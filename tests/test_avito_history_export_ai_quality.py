from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Sequence

import pytest

from libs.core.services import avito_history_export
from tests.test_avito_history_export import FakeAvitoApi, _deps, _good_messages


pytestmark = pytest.mark.unit


class FakeContextualAIModule:
    @staticmethod
    async def extract_cases(
        candidates: Sequence[Any],
        *,
        rule_extractions: dict[str, dict[str, Any]],
        extractor: Any,
        enabled: bool = True,
        progress_callback: Any = None,
        **_kwargs: Any,
    ) -> SimpleNamespace:
        extractions: dict[str, dict[str, Any]] = {}
        for candidate in candidates:
            text = candidate.manager_reply.text
            if "Менделеева" in text:
                extractions[candidate.case_id] = {
                    "context": {"intent": "store_location", "client_city": None, "business_city": "Уфа"},
                    "reply_facts": {"mentions_address": True, "city_specific": True},
                    "applicability": {"mode": "direct_example"},
                    "quality": {"confidence": 0.95, "reason_code": "ai_direct_address"},
                }
            else:
                extractions[candidate.case_id] = {
                    "context": {},
                    "reply_facts": {},
                    "applicability": {"mode": "direct_example"},
                    "quality": {"confidence": 0.9, "reason_code": "ai_direct"},
                }
        result = SimpleNamespace(
            extractions=extractions,
            extracted_count=len(extractions),
            failed_count=0,
            errors={},
        )
        if progress_callback is not None:
            await progress_callback(result)
        return result


def _messages_with_replies() -> list[dict[str, Any]]:
    return [
        {"created": "2026-05-01T10:00:00+00:00", "direction": "in", "text": "Нужна дверь"},
        {"created": "2026-05-01T10:01:00+00:00", "direction": "out", "text": "Подскажите размер?"},
        {"created": "2026-05-01T10:02:00+00:00", "direction": "in", "text": "90 на 200"},
        {"created": "2026-05-01T10:03:00+00:00", "direction": "out", "text": "Отправили по ватсап"},
    ]


@pytest.mark.asyncio
async def test_contextual_ai_creates_cases_review_and_summary(tmp_path: Path) -> None:
    api = FakeAvitoApi(chats=[{"id": "chat-1"}], messages={"chat-1": _messages_with_replies()})
    deps = _deps(api, tmp_path, max_candidates_multiplier=10)
    deps = avito_history_export.AvitoHistoryExportDeps(
        **{
            **deps.__dict__,
            "contextual_ai_client": object(),
            "contextual_ai_extractor_module": FakeContextualAIModule,
            "legacy_contextual_cases_enabled": True,
        }
    )

    result = await avito_history_export.run_export(1, target_dialogs=1, job_id="job-1", deps=deps)

    assert result.status == "completed"
    assert result.contextual_file_path
    assert result.review_cases_file_path
    assert result.rejected_cases_summary_path
    contextual_rows = [
        json.loads(line) for line in Path(result.contextual_file_path).read_text(encoding="utf-8").splitlines()
    ]
    review_rows = [
        json.loads(line) for line in Path(result.review_cases_file_path).read_text(encoding="utf-8").splitlines()
    ]
    summary = json.loads(Path(result.rejected_cases_summary_path).read_text(encoding="utf-8"))
    assert len(contextual_rows) == 1
    assert len(review_rows) == 1
    assert summary["contextual_mode"] == "ai_selective"
    assert summary["ai_extracted_count"] == 1
    assert result.training_file_path is None
    assert "Нужна дверь" not in json.dumps(summary, ensure_ascii=False)


@pytest.mark.asyncio
async def test_ai_direct_address_without_city_is_forced_to_review(tmp_path: Path) -> None:
    api = FakeAvitoApi(
        chats=[{"id": "chat-1"}],
        messages={
            "chat-1": [
                {"created": "2026-05-01T10:00:00+00:00", "direction": "in", "text": "Где посмотреть двери?"},
                {"created": "2026-05-01T10:01:00+00:00", "direction": "out", "text": "В Уфе Менделеева 80"},
            ]
        },
    )
    deps = _deps(api, tmp_path, max_candidates_multiplier=10)
    deps = avito_history_export.AvitoHistoryExportDeps(
        **{
            **deps.__dict__,
            "contextual_ai_client": object(),
            "contextual_ai_extractor_module": FakeContextualAIModule,
            "legacy_contextual_cases_enabled": True,
        }
    )

    result = await avito_history_export.run_export(1, target_dialogs=1, job_id="job-1", deps=deps)

    assert result.contextual_cases_count == 0
    assert result.review_cases_count == 1
    assert result.review_cases_file_path
    assert result.direct_example_count == 0


@pytest.mark.asyncio
async def test_ai_unavailable_uses_rule_fallback(tmp_path: Path) -> None:
    api = FakeAvitoApi(chats=[{"id": "chat-1"}], messages={"chat-1": _messages_with_replies()})

    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=avito_history_export.AvitoHistoryExportDeps(
            **{
                **_deps(api, tmp_path).__dict__,
                "quality_review_enabled": False,
                "legacy_contextual_cases_enabled": True,
            }
        ),
    )

    assert result.contextual_mode == "disabled"
    assert result.contextual_cases_count == 1
    assert result.review_cases_count == 1
    assert result.training_examples_count == 0


@pytest.mark.asyncio
async def test_cancel_creates_no_contextual_artifacts(tmp_path: Path) -> None:
    async def cancelled() -> bool:
        return True

    api = FakeAvitoApi(chats=[{"id": "chat-1"}], messages={"chat-1": _good_messages("1")})
    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(api, tmp_path, cancel_callback=cancelled),
    )

    assert result.status == "cancelled"
    assert not list(tmp_path.rglob("*.jsonl"))
    export_json_files = [
        path for path in tmp_path.rglob("*.json") if "checkpoints" not in path.parts
    ]
    assert not export_json_files
