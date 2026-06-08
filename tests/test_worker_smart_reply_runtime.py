from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from apps.worker.services import smart_reply_runtime


pytestmark = pytest.mark.unit


@dataclass
class PipelineResult:
    reply_text: str
    source: str = "llm"


@dataclass
class ReplyWithPlan:
    llm_plan: dict[str, Any]
    llm_raw_answer: str

    def __str__(self) -> str:
        return "final answer"


class FakeRedis:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int | None]] = []

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.calls.append((key, value, ex))
        return True


def test_merge_reply_context_keeps_existing_values_and_normalizes_tg_slot() -> None:
    result = smart_reply_runtime.merge_reply_context(
        "telegram",
        {"tg_slot": "bad", "peer": "old", "keep": "yes"},
        {"tg_slot": "2", "peer": "   ", "extra": 5, "none": None},
        normalize_tg_slot_fn=lambda value: int(value or 1),
    )

    assert result == {"tg_slot": 2, "peer": "old", "keep": "yes", "extra": 5}


def test_can_generate_reply_for_channel_uses_channel_specific_gate() -> None:
    deps = smart_reply_runtime.SmartReplyChannelDeps(
        telegram_reply_enabled_fn=lambda tenant: tenant == 1,
        max_reply_enabled_fn=lambda _tenant: False,
        max_personal_reply_enabled_fn=lambda _tenant: True,
        avito_smart_reply_enabled_fn=lambda _tenant: True,
        smart_reply_enabled_fn=lambda tenant: tenant in {1, 3},
    )

    assert smart_reply_runtime.can_generate_reply_for_channel(1, "telegram", deps=deps) is True
    assert smart_reply_runtime.can_generate_reply_for_channel(3, "telegram", deps=deps) is False
    assert smart_reply_runtime.can_generate_reply_for_channel(3, "avito", deps=deps) is True
    assert smart_reply_runtime.can_generate_reply_for_channel(3, "max", deps=deps) is False
    assert smart_reply_runtime.can_generate_reply_for_channel(9, "whatsapp", deps=deps) is False


def test_log_smart_reply_diag_serializes_plan_without_raw_payloads() -> None:
    logs: list[str] = []

    smart_reply_runtime.log_smart_reply_diag(
        "avito",
        1,
        22,
        ReplyWithPlan(
            llm_plan={"next_questions": ["размер?", None], "cta": "уточнить"},
            llm_raw_answer="сырой ответ",
        ),
        log_fn=logs.append,
    )

    assert len(logs) == 1
    assert "event=smart_reply_diag" in logs[0]
    assert "plan_next_questions=[\"размер?\"]" in logs[0]
    assert "plan_cta=\"уточнить\"" in logs[0]
    assert "answer=\"сырой ответ\"" in logs[0]


@pytest.mark.anyio
async def test_maybe_set_waiting_photo_state_sets_state_when_marker_matches() -> None:
    redis = FakeRedis()
    logs: list[str] = []
    deps = smart_reply_runtime.WaitingPhotoDeps(
        redis_client=redis,
        handoff_silence_ttl_seconds=600,
        photo_expectation_config_fn=lambda _tenant: (["пришлите фото"], "", 15),
        log_fn=logs.append,
    )

    await smart_reply_runtime.maybe_set_waiting_photo_state(
        tenant_id=1,
        lead_id=2,
        channel="telegram",
        reply_text="Пришлите фото проема",
        deps=deps,
    )

    assert redis.calls == [("conv:state:1:2", "waiting_photo", 15)]
    assert any("event=photo_expected_set" in item for item in logs)


@pytest.mark.anyio
async def test_maybe_set_waiting_photo_state_ignores_non_supported_channel() -> None:
    redis = FakeRedis()
    deps = smart_reply_runtime.WaitingPhotoDeps(
        redis_client=redis,
        handoff_silence_ttl_seconds=600,
        photo_expectation_config_fn=lambda _tenant: (["фото"], "", 15),
        log_fn=lambda *_args: None,
    )

    await smart_reply_runtime.maybe_set_waiting_photo_state(
        tenant_id=1,
        lead_id=2,
        channel="avito",
        reply_text="пришлите фото",
        deps=deps,
    )

    assert redis.calls == []


@pytest.mark.anyio
async def test_generate_reply_text_returns_pipeline_text_and_logs_fallback_source() -> None:
    logs: list[str] = []
    diag: list[tuple[str, int, int | None, Any]] = []
    calls: list[dict[str, Any]] = []

    async def run_pipeline(**kwargs: Any) -> PipelineResult:
        calls.append(dict(kwargs))
        return PipelineResult(" ответ по делу ", source="fallback")

    deps = smart_reply_runtime.SmartReplyGenerateDeps(
        run_response_pipeline_fn=run_pipeline,
        default_fallback_reply_fn=lambda _tenant: "fallback text",
        strip_instruction_leaks_fn=lambda text: text,
        log_smart_reply_diag_fn=lambda *args: diag.append(args),
        log_fn=logs.append,
        timeout_seconds=12.5,
    )

    reply_text, reply = await smart_reply_runtime.generate_reply_text(
        tenant_id=1,
        lead_id=22,
        refer_id=33,
        channel="avito",
        user_text="нужна дверь",
        deps=deps,
    )

    assert reply_text == "ответ по делу"
    assert reply == " ответ по делу "
    assert calls[0]["contact_id"] == 33
    assert calls[0]["timeout_seconds"] == 12.5
    assert any("event=smart_reply_quality_signal" in item for item in logs)
    assert diag == [("avito", 1, 22, " ответ по делу ")]


@pytest.mark.anyio
async def test_generate_reply_text_uses_fallback_on_pipeline_error() -> None:
    logs: list[str] = []

    async def run_pipeline(**_kwargs: Any) -> PipelineResult:
        raise RuntimeError("boom")

    deps = smart_reply_runtime.SmartReplyGenerateDeps(
        run_response_pipeline_fn=run_pipeline,
        default_fallback_reply_fn=lambda tenant: f"fallback {tenant}",
        strip_instruction_leaks_fn=lambda text: text,
        log_smart_reply_diag_fn=lambda *_args: None,
        log_fn=logs.append,
        timeout_seconds=5.0,
    )

    reply_text, reply = await smart_reply_runtime.generate_reply_text(
        tenant_id=7,
        lead_id=8,
        refer_id=0,
        channel="telegram",
        user_text="тест",
        deps=deps,
    )

    assert reply_text == "fallback 7"
    assert reply == "fallback 7"
    assert any("event=smart_reply_failed" in item and "stage=pipeline" in item for item in logs)
