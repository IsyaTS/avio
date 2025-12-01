import pytest

from libs.core.brain import planner, quality


def test_enforce_plan_alignment_filters_channel_switch_questions():
    plan = planner.GeneratedPlan(
        next_questions=["В каком канале вам удобнее общаться?"],
        cta="Готов оформить заказ?",
    )
    ctx = quality.EnforcementContext(channel="whatsapp", disable_channel_switch_prompts=True)

    result = quality.enforce_plan_alignment("Ответ клиента", plan, None, context=ctx)

    assert "канале" not in result.lower()
    assert plan.next_questions == []
    assert ctx.applied_questions == []


def test_enforce_plan_alignment_blocks_cta_when_disallowed():
    question = "Нужна ли доставка завтра?"
    fingerprint = quality.question_fingerprint(question)
    plan = planner.GeneratedPlan(
        next_questions=[question],
        cta="Готов забронировать условия?",
    )
    ctx = quality.EnforcementContext(
        channel="whatsapp",
        allow_cta=False,
        asked_fingerprints={fingerprint},
        recent_cta="Готов забронировать условия?",
        recent_cta_ts=0.0,
    )

    result = quality.enforce_plan_alignment("Ответ клиента", plan, None, context=ctx)

    assert question not in result
    assert ctx.applied_cta == ""
    assert not ctx.applied_questions


def test_enforce_plan_alignment_appends_cta_when_allowed():
    plan = planner.GeneratedPlan(
        next_questions=["Сколько комплектов требуется?"],
        cta="Зафиксирую цену — подтверждаем?",
    )
    ctx = quality.EnforcementContext(channel="whatsapp", allow_cta=True)

    result = quality.enforce_plan_alignment("Ответ клиента", plan, None, context=ctx)

    assert "Зафиксирую цену" in result
    assert ctx.applied_cta == "Зафиксирую цену — подтверждаем?"


def test_question_present_handles_paraphrased_phrases():
    base = "В каком городе установка?"
    reply = "Подскажите, в каком городе планируете установку двери?"
    assert quality._question_present(base, reply)


def test_question_present_handles_partial_overlap():
    base = "Сориентируйте, пожалуйста, по бюджету — чтобы предложить наиболее подходящие варианты."
    reply = "Сориентируйте, пожалуйста, по бюджету — чтобы я мог предложить оптимальные варианты из каталога."
    assert quality._question_present(base, reply)


def test_enforce_plan_alignment_removes_channel_switch_lines_from_reply():
    plan = planner.GeneratedPlan()
    ctx = quality.EnforcementContext(channel="whatsapp", disable_channel_switch_prompts=True)
    reply = "Где удобнее общаться — в WhatsApp или Telegram?"

    result = quality.enforce_plan_alignment(reply, plan, None, context=ctx)

    assert "удобнее" not in result.lower()


def test_enforce_plan_alignment_strips_forbidden_cta():
    plan = planner.GeneratedPlan()
    ctx = quality.EnforcementContext(channel="whatsapp", allow_cta=False)

    result = quality.enforce_plan_alignment("Готов зафиксировать цену, подтверждаем?", plan, None, context=ctx)

    assert not result
