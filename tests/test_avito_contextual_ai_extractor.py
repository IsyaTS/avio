from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from libs.core.services.avito_context_extractor import extract_context
from libs.core.services.avito_contextual_ai_extractor import (
    AvitoContextualAIExtractor,
    AvitoContextualAIExtractorConfig,
    PROMPT_VERSION,
    extract_cases,
)
from libs.core.services.avito_contextual_case_builder import build_contextual_case_candidates
from libs.core.services.avito_dialog_filter import AvitoDialogMessage


pytestmark = pytest.mark.unit


class FakeResponses:
    def __init__(self, payload: dict[str, Any] | str | BaseException) -> None:
        self.payload = payload
        self.calls = 0

    def create(self, **_kwargs: Any) -> SimpleNamespace:
        self.calls += 1
        if isinstance(self.payload, BaseException):
            raise self.payload
        if isinstance(self.payload, str):
            return SimpleNamespace(output_text=self.payload)
        return SimpleNamespace(output_text=json.dumps(self.payload, ensure_ascii=False))


class FakeClient:
    def __init__(self, payload: dict[str, Any] | str | BaseException) -> None:
        self.responses = FakeResponses(payload)


def _candidate(client_text: str = "Здравствуйте, я из Уфы, где посмотреть двери?", reply: str = "Менделеева 80"):
    result = build_contextual_case_candidates(
        [[AvitoDialogMessage("client", client_text), AvitoDialogMessage("manager", reply)]],
        tenant_id=101,
        created_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
    )
    assert result.candidates
    return result.candidates[0]


def test_ai_config_defaults_to_gpt_5_2() -> None:
    assert AvitoContextualAIExtractorConfig().model == "gpt-5.2"
    assert PROMPT_VERSION == "avito_contextual_domain_extractor_v2"


def test_build_default_extractor_silences_openai_debug_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    from libs.core.services import avito_contextual_ai_extractor

    logging.getLogger("openai._base_client").setLevel(logging.DEBUG)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    assert avito_contextual_ai_extractor.build_default_extractor() is None
    assert logging.getLogger("openai._base_client").level >= logging.WARNING


@pytest.mark.asyncio
async def test_ai_structured_direct_extraction() -> None:
    candidate = _candidate("Доставка бесплатная?", "Доставка и установка бесплатная")
    payload = {
        "context": {"intent": "delivery_installation", "stage": "first_touch", "missing_facts": []},
        "reply_facts": {"mentions_delivery": True, "mentions_installation": True},
        "applicability": {"mode": "direct_example", "requires": []},
        "quality": {"confidence": 0.91, "reason_code": "universal_delivery"},
    }
    extractor = AvitoContextualAIExtractor(
        client=FakeClient(payload),
        config=AvitoContextualAIExtractorConfig(model="gpt-5.2", timeout_seconds=1),
    )

    result = await extractor.extract_case(candidate, rule_extraction=extract_context(candidate))

    assert result["applicability"]["mode"] == "direct_example"
    assert result["quality"]["reason_code"] == "universal_delivery"


@pytest.mark.asyncio
async def test_ai_structured_context_bound_extraction() -> None:
    candidate = _candidate()
    payload = {
        "context": {"intent": "store_location", "client_city": "Уфа", "business_city": "Уфа"},
        "reply_facts": {"mentions_address": True, "city_specific": True},
        "applicability": {"mode": "context_bound", "requires": ["slots.client_city"], "same_city_required": True},
        "quality": {"confidence": 0.87, "reason_code": "context_bound_address_answer"},
    }
    extractor = AvitoContextualAIExtractor(
        client=FakeClient(payload),
        config=AvitoContextualAIExtractorConfig(model="gpt-5.2", timeout_seconds=1),
    )

    result = await extractor.extract_case(candidate, rule_extraction=extract_context(candidate))

    assert result["applicability"]["mode"] == "context_bound"
    assert result["applicability"]["same_city_required"] is True


@pytest.mark.asyncio
async def test_invalid_json_goes_to_failed_result() -> None:
    candidate = _candidate()
    extractor = AvitoContextualAIExtractor(
        client=FakeClient("{not json"),
        config=AvitoContextualAIExtractorConfig(model="gpt-5.2", timeout_seconds=1),
    )

    result = await extract_cases([candidate], extractor=extractor, rule_extractions={candidate.case_id: extract_context(candidate)})

    assert result.extractions == {}
    assert result.failed_count == 1
    assert result.errors


@pytest.mark.asyncio
async def test_timeout_goes_to_failed_result() -> None:
    candidate = _candidate()
    extractor = AvitoContextualAIExtractor(
        client=FakeClient(TimeoutError("deadline")),
        config=AvitoContextualAIExtractorConfig(model="gpt-5.2", timeout_seconds=1),
    )

    result = await extract_cases([candidate], extractor=extractor, rule_extractions={candidate.case_id: extract_context(candidate)})

    assert result.extracted_count == 0
    assert result.failed_count == 1
    assert result.errors["TimeoutError"] == 1


@pytest.mark.asyncio
async def test_cache_prevents_duplicate_calls() -> None:
    candidate = _candidate()
    client = FakeClient({"context": {}, "reply_facts": {}, "applicability": {}, "quality": {}})
    extractor = AvitoContextualAIExtractor(
        client=client,
        config=AvitoContextualAIExtractorConfig(model="gpt-5.2", timeout_seconds=1),
    )

    await extractor.extract_case(candidate, rule_extraction=extract_context(candidate))
    await extractor.extract_case(candidate, rule_extraction=extract_context(candidate))

    assert client.responses.calls == 1


@pytest.mark.asyncio
async def test_progress_callback_reports_ai_counts() -> None:
    candidates = [
        _candidate("Здравствуйте, я из Уфы, где посмотреть двери?", "Менделеева 80"),
        _candidate("Сколько стоит дверь?", "Стоимость от 35000 рублей"),
    ]
    client = FakeClient({"context": {}, "reply_facts": {}, "applicability": {}, "quality": {}})
    extractor = AvitoContextualAIExtractor(
        client=client,
        config=AvitoContextualAIExtractorConfig(model="gpt-5.2", timeout_seconds=1),
    )
    snapshots = []

    async def progress(snapshot):
        snapshots.append(snapshot)

    result = await extract_cases(candidates, extractor=extractor, progress_callback=progress, progress_interval=1)

    assert result.extracted_count == 2
    assert [item.extracted_count for item in snapshots] == [1, 2]


@pytest.mark.asyncio
async def test_no_raw_logs_on_failure(caplog: pytest.LogCaptureFixture) -> None:
    raw_text = "raw customer secret"
    candidate = _candidate(raw_text, "Менделеева 80")
    extractor = AvitoContextualAIExtractor(
        client=FakeClient(RuntimeError("failed")),
        config=AvitoContextualAIExtractorConfig(model="gpt-5.2", timeout_seconds=1),
    )

    await extract_cases([candidate], extractor=extractor, rule_extractions={candidate.case_id: extract_context(candidate)})

    assert raw_text not in caplog.text
