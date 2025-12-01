from __future__ import annotations

from libs.core.brain import quality
from libs.core.brain.planner import GeneratedPlan


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
