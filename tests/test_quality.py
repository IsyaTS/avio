from __future__ import annotations

import pytest

from libs.core.brain import quality
from libs.core.brain.planner import GeneratedPlan
from scripts import dialog_regression


def test_enforce_plan_alignment_deduplicates_questions():
    plan = GeneratedPlan(
        analysis="",
        stage="",
        next_questions=["Какой бюджет?", "Какой бюджет?"],
        cta="",
        tone="",
        raw={},
    )
    ctx = quality.EnforcementContext(max_questions=2)
    reply = (
        "Здравствуйте!\n\n"
        "Какой бюджет?\n"
        "Какой бюджет?\n"
        "Уточните, пожалуйста?\n"
        "Уточните, пожалуйста?"
    )

    result = quality.enforce_plan_alignment(reply, plan, context=ctx)

    assert result.count("Какой бюджет?") == 1
    assert result.count("Уточните, пожалуйста?") == 1


@pytest.mark.asyncio
async def test_dialog_regression_eval_lite_does_not_call_live_llm(monkeypatch):
    async def _build_messages(**_kwargs):
        return [{"role": "user", "content": "здравствуйте"}]

    async def _fail_live_llm(*_args, **_kwargs):
        raise AssertionError("dialog regression eval-lite must not call live LLM")

    monkeypatch.setenv("SALES_EVAL_LITE", "1")
    monkeypatch.setattr(dialog_regression.core, "build_llm_messages", _build_messages)
    monkeypatch.setattr(dialog_regression.core, "ask_llm", _fail_live_llm)
    monkeypatch.setattr(dialog_regression.core, "reset_sales_state", lambda *_args, **_kwargs: None)

    convo, violations = await dialog_regression._run_case(
        case={
            "name": "eval_lite_no_live_llm",
            "tenant": 101,
            "channel": "telegram",
            "messages": ["здравствуйте", "уфа"],
            "rules": {"max_questions_per_reply": 1},
        },
        iteration=1,
        contact_base=123000,
        tenant_override=None,
        channel_override=None,
    )

    assert violations == []
    assert len(convo) == 2
    assert "город" in convo[0][1].lower()
