from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from libs.core.learning.actions import classify_action
from libs.core.learning.outcomes import compute_episode_outcome
from libs.core.learning.policy import select_runtime_policy, should_activate_candidate
from libs.core.learning.replay import evaluate_replay_case
from libs.core.learning.service import capture_intervention_episode, prepare_runtime_policy_hint
from libs.core.learning.state_snapshot import build_dialogue_state_snapshot
from libs.core.learning.stitching import stitch_messages
from libs.core import response_pipeline
from libs.core.learning import service as service_mod


pytestmark = pytest.mark.unit


@pytest.fixture
def sample_state() -> SimpleNamespace:
    return SimpleNamespace(
        facts={"city": "Уфа", "model": "Delta 100"},
        known_slots={"city": "Уфа"},
        pending_fact_key="",
        last_plan={"action": "ask_missing_fact", "intent": "qualification"},
        last_bot_reply="Могу предложить несколько вариантов и дать цену.",
        last_items=[{"title": "Delta 100", "price": "25000"}],
        catalog_sent=True,
        catalog_delivery_mode="pdf",
    )


def test_stitch_messages_merges_same_role_within_window() -> None:
    turns = stitch_messages(
        [
            {"id": 1, "direction": 0, "text": "нужны двери", "created_at": "2026-04-13T10:00:00+00:00"},
            {"id": 2, "direction": 0, "text": "белые", "created_at": "2026-04-13T10:00:20+00:00"},
            {"id": 3, "direction": 1, "is_bot": True, "source": "bot", "text": "Есть варианты", "created_at": "2026-04-13T10:02:00+00:00"},
        ],
        within_seconds=45,
    )
    assert len(turns) == 2
    assert turns[0].is_stitched is True
    assert turns[0].raw_count == 2
    assert "белые" in turns[0].text


def test_build_dialogue_state_snapshot_sets_core_flags(sample_state: SimpleNamespace) -> None:
    turns = stitch_messages(
        [
            {"id": 1, "direction": 0, "text": "Сколько стоит?", "created_at": "2026-04-13T10:00:00+00:00"},
            {"id": 2, "direction": 1, "is_bot": True, "source": "bot", "text": "Цена зависит от модели", "created_at": "2026-04-13T10:00:10+00:00"},
        ]
    )
    snapshot = build_dialogue_state_snapshot(
        tenant_id=101,
        lead_id=500,
        contact_id=500,
        channel="telegram",
        state=sample_state,
        stitched_history=turns,
        current_user_text="Сколько стоит?",
    )
    assert snapshot.user_intent == "price"
    assert snapshot.has_price_context is True
    assert snapshot.after_catalog is True
    assert snapshot.after_pdf is True
    assert snapshot.direct_question is True
    assert snapshot.fingerprint


def test_classify_action_detects_price_range_and_cta() -> None:
    action = classify_action("Цена от 20 000 до 25 000 ₽. Если удобно, оставьте номер и я перезвоню.")
    assert action["action"] == "schedule_cta"
    assert action["style_hints"]["cta_density"] == "high"


def test_select_runtime_policy_skips_direct_question_guard(sample_state: SimpleNamespace) -> None:
    turns = stitch_messages(
        [
            {"id": 1, "direction": 0, "text": "Сколько стоит Delta 100?", "created_at": "2026-04-13T10:00:00+00:00"},
        ]
    )
    snapshot = build_dialogue_state_snapshot(
        tenant_id=101,
        lead_id=500,
        contact_id=500,
        channel="telegram",
        state=sample_state,
        stitched_history=turns,
        current_user_text="Сколько стоит Delta 100?",
    )
    decision = select_runtime_policy(
        snapshot=snapshot,
        rules=[
            {
                "id": 1,
                "fingerprint_payload": dict(snapshot.fingerprint_payload),
                "recommended_action": "ask_missing_fact",
                "avoid_action": "answer_direct",
                "style_hints": {},
                "confidence": 0.95,
            }
        ],
        settings={"min_similarity": 0.5, "min_confidence": 0.5, "apply_mode": False, "shadow_mode": True},
    )
    assert decision.status == "skipped"
    assert decision.reason == "direct_question_guard"


def test_select_runtime_policy_accepts_stringified_fingerprint_payload(sample_state: SimpleNamespace) -> None:
    turns = stitch_messages(
        [
            {"id": 1, "direction": 0, "text": "Сколько стоит Delta 100?", "created_at": "2026-04-13T10:00:00+00:00"},
        ]
    )
    snapshot = build_dialogue_state_snapshot(
        tenant_id=101,
        lead_id=501,
        contact_id=501,
        channel="telegram",
        state=sample_state,
        stitched_history=turns,
        current_user_text="Сколько стоит Delta 100?",
    )
    decision = select_runtime_policy(
        snapshot=snapshot,
        rules=[
            {
                "id": 2,
                "fingerprint_payload": json.dumps(dict(snapshot.fingerprint_payload), ensure_ascii=False),
                "recommended_action": "answer_direct",
                "avoid_action": "ask_missing_fact",
                "style_hints": {},
                "confidence": 0.95,
            }
        ],
        settings={"min_similarity": 0.5, "min_confidence": 0.5, "apply_mode": True, "shadow_mode": False},
    )
    assert decision.status == "eligible"
    assert decision.reason == "matched"


def test_should_activate_candidate_respects_thresholds() -> None:
    assert should_activate_candidate(
        {"evidence_count": 4, "distinct_leads_count": 3, "reward_delta": 0.4, "negative_evidence": 1},
        min_evidence=3,
        min_distinct_leads=2,
        min_reward_delta=0.15,
        max_negative_evidence=2,
    ) is True
    assert should_activate_candidate(
        {"evidence_count": 2, "distinct_leads_count": 1, "reward_delta": 0.4, "negative_evidence": 1},
        min_evidence=3,
        min_distinct_leads=2,
        min_reward_delta=0.15,
        max_negative_evidence=2,
    ) is False


def test_replay_cases_fixture() -> None:
    fixture_path = Path("tests/fixtures/learning_v2_replay_cases.json")
    cases = json.loads(fixture_path.read_text(encoding="utf-8"))
    ok = evaluate_replay_case("Цена есть, покажу варианты и задам один следующий шаг.", cases[0])
    assert ok.ok is True
    failed = evaluate_replay_case("Подскажите ваш бюджет и в каком городе?", cases[0])
    assert failed.ok is False
    assert any(item.startswith("banned:") for item in failed.failures)


def test_compute_episode_outcome_rewards_progress() -> None:
    turns = stitch_messages(
        [
            {"id": 10, "direction": 0, "text": "а есть дешевле?", "created_at": "2026-04-13T10:04:00+00:00"},
            {"id": 11, "direction": 1, "text": "Да, есть еще вариант", "is_bot": False, "source": "manager", "created_at": "2026-04-13T10:04:20+00:00"},
            {"id": 12, "direction": 0, "text": "да, покажите", "created_at": "2026-04-13T10:05:00+00:00"},
        ]
    )
    outcome = compute_episode_outcome(
        trigger_user_text="а есть дешевле?",
        subsequent_turns=turns[1:],
        horizon_reached=False,
    )
    assert outcome.finalized is True
    assert outcome.signals["user_continued_dialogue"] is True
    assert outcome.reward > 0


@pytest.mark.asyncio
async def test_prepare_runtime_policy_hint_shadow_only(monkeypatch, sample_state: SimpleNamespace) -> None:
    monkeypatch.setattr(
        service_mod,
        "read_tenant_config",
        lambda _tenant: {"learning": {"enabled": True, "intervention_policy": {"enabled": True, "runtime_enabled": True, "shadow_mode": True, "apply_mode": False}}},
    )
    monkeypatch.setattr(service_mod, "load_sales_state", lambda *_a, **_k: sample_state)

    async def _noop(*_a, **_k):
        return 0

    async def _list_rules(*_a, **_k):
        return [
            {
                "id": 7,
                "fingerprint_payload": {
                    "intent": "price",
                    "pending_fact_key": "",
                    "last_plan_action": "ask_missing_fact",
                    "known_fact_keys": ["city", "model"],
                    "has_price_context": True,
                    "has_shortlist": True,
                    "has_cta": False,
                    "has_handoff": False,
                    "frustration_signal": False,
                    "repair_signal": False,
                    "complaint_signal": False,
                    "after_catalog": True,
                },
                "recommended_action": "give_price",
                "avoid_action": "ask_missing_fact",
                "style_hints": {"answer_length": "short"},
                "confidence": 0.93,
            }
        ]

    async def _create_snapshot(_snapshot):
        return 11

    async def _create_decision(**kwargs):
        return 22

    monkeypatch.setattr(service_mod, "finalize_pending_episode_outcomes", _noop)
    monkeypatch.setattr(service_mod.db, "list_tenant_policy_rules", _list_rules)
    monkeypatch.setattr(service_mod.db, "create_dialogue_state_snapshot", _create_snapshot)
    monkeypatch.setattr(service_mod.db, "create_policy_decision", _create_decision)

    ctx = await prepare_runtime_policy_hint(
        tenant_id=101,
        lead_id=500,
        channel="telegram",
        user_text="Сколько стоит Delta 100?",
        normalized_history=[{"role": "assistant", "content": "Могу помочь с выбором."}],
    )
    assert ctx["enabled"] is True
    assert ctx["mode"] == "shadow"
    assert ctx["policy_block"] == ""
    assert ctx["decision"].status == "eligible"


@pytest.mark.asyncio
async def test_run_response_pipeline_injects_policy_block_in_apply_mode(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(messages, **_kwargs):
        captured["messages"] = messages
        return "ok"

    async def _policy_ctx(**_kwargs):
        return {"enabled": True, "policy_block": "Intervention policy hint for this tenant:\n- recommended_action: give_price"}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="сколько стоит",
        contact_id=500,
        history=[{"role": "assistant", "content": "чем помочь"}],
    )
    assert result.reply_text == "ok"
    sent_messages = captured["messages"]
    assert isinstance(sent_messages, list)
    assert "Intervention policy hint" in sent_messages[0]["content"]


@pytest.mark.asyncio
async def test_run_response_pipeline_injects_training_examples(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(messages, **_kwargs):
        captured["messages"] = messages
        return "ok"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return "Проверенные примеры ответов менеджера.\nКлиент: цена?\nМенеджер: Цена от 20000 ₽."

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(
        response_pipeline,
        "read_tenant_config",
        lambda _tenant: {"learning": {"legacy_pairs_enabled": True}},
    )

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="цена?",
        contact_id=500,
    )
    assert result.reply_text == "ok"
    sent_messages = captured["messages"]
    assert isinstance(sent_messages, list)
    assert "Проверенные примеры ответов менеджера" in sent_messages[0]["content"]
    assert "Цена от 20000" in sent_messages[0]["content"]


@pytest.mark.asyncio
async def test_run_response_pipeline_injects_dialog_training_block(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(messages, **_kwargs):
        captured["messages"] = messages
        return "ok"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(
        response_pipeline,
        "_build_dialog_training_block",
        lambda **_kwargs: "Похожие реальные диалоги менеджера.\nКлиент: где магазин?\nМенеджер: уточните город",
    )

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="где магазин?",
        contact_id=500,
    )

    assert result.reply_text == "ok"
    sent_messages = captured["messages"]
    assert isinstance(sent_messages, list)
    assert "Похожие реальные диалоги менеджера" in sent_messages[0]["content"]


@pytest.mark.asyncio
async def test_run_response_pipeline_guards_location_reply_without_city(monkeypatch) -> None:
    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(_messages, **_kwargs):
        return "В Ишимбае двери можно посмотреть по адресу Стерлитамак, Коммунистическая 38"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="где можно посмотреть двери?",
        contact_id=500,
    )

    assert result.source == "guarded_location_context"
    assert result.reply_text == "Секунду, сейчас позову менеджера"
    assert "Коммунистическая" not in result.reply_text


@pytest.mark.asyncio
async def test_run_response_pipeline_guards_contact_reply_for_location_without_city(monkeypatch) -> None:
    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(_messages, **_kwargs):
        return "Напишите в Telegram @example или по телефону 89999999999"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="где можно посмотреть двери?",
        contact_id=500,
    )

    assert result.source == "guarded_location_context"
    assert result.reply_text == "Секунду, сейчас позову менеджера"
    assert "@example" not in result.reply_text
    assert "89999999999" not in result.reply_text


@pytest.mark.asyncio
async def test_run_response_pipeline_keeps_city_clarification_for_location_without_city(monkeypatch) -> None:
    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(_messages, **_kwargs):
        return "Здравствуйте. Подскажите, пожалуйста, в каком городе хотите посмотреть каталог?"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="где можно посмотреть каталог?",
        contact_id=500,
    )

    assert result.source == "llm"
    assert result.reply_text == "Здравствуйте. Подскажите, пожалуйста, в каком городе хотите посмотреть каталог?"


@pytest.mark.asyncio
async def test_run_response_pipeline_replaces_low_value_location_reply_with_city_clarification(monkeypatch) -> None:
    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(_messages, **_kwargs):
        return "Гермес. Айдар - двери."

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="где можно посмотреть каталог?",
        contact_id=500,
    )

    assert result.source == "guarded_location_context"
    assert result.reply_text == "Здравствуйте. Подскажите, пожалуйста, в каком городе хотите посмотреть каталог?"


@pytest.mark.asyncio
async def test_run_response_pipeline_replaces_catalog_promise_without_city_with_city_clarification(monkeypatch) -> None:
    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(_messages, **_kwargs):
        return "Здравствуйте каталог могу отправить здесь в сообщениях или показать в магазине"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="где можно посмотреть каталог?",
        contact_id=500,
    )

    assert result.source == "guarded_location_context"
    assert result.reply_text == "Здравствуйте. Подскажите, пожалуйста, в каком городе хотите посмотреть каталог?"
    assert "магазине" not in result.reply_text


@pytest.mark.asyncio
async def test_run_response_pipeline_removes_catalog_promise_even_when_city_question_present(monkeypatch) -> None:
    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(_messages, **_kwargs):
        return "Здравствуйте. Каталог могу отправить здесь, есть варианты для квартиры. Подскажите, пожалуйста, в каком городе планируете установку?"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="где можно посмотреть каталог?",
        contact_id=500,
    )

    assert result.source == "guarded_location_context"
    assert result.reply_text == "Здравствуйте. Подскажите, пожалуйста, в каком городе хотите посмотреть каталог?"
    assert "варианты" not in result.reply_text


@pytest.mark.asyncio
async def test_run_response_pipeline_without_policy_keeps_base_prompt(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _build_llm_messages(*_a, **_k):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(messages, **_kwargs):
        captured["messages"] = messages
        return "ok"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    async def _examples_block(*_a, **_k):
        return ""

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", _examples_block)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="сколько стоит",
        contact_id=500,
    )
    assert result.reply_text == "ok"
    sent_messages = captured["messages"]
    assert isinstance(sent_messages, list)
    assert sent_messages[0]["content"] == "base-system"


@pytest.mark.asyncio
async def test_finalize_positive_episode_records_training_example(monkeypatch) -> None:
    monkeypatch.setattr(
        service_mod,
        "read_tenant_config",
        lambda _tenant: {
            "learning": {
                "enabled": True,
                "intervention_policy": {
                    "enabled": True,
                    "outcome_horizon_minutes": 5,
                    "stitch_window_seconds": 45,
                },
            }
        },
    )

    async def _episodes(*_a, **_k):
        return [
            {
                "id": 77,
                "tenant_id": 101,
                "lead_id": 500,
                "manager_message_id": 3,
                "trigger_user_text": "Сколько стоит?",
                "manager_reply_text": "Цена от 20 000 до 25 000 ₽, зависит от модели.",
                "created_at": None,
            }
        ]

    async def _messages(*_a, **_k):
        return [
            {"id": 1, "lead_id": 500, "direction": 0, "text": "Сколько стоит?", "created_at": "2026-04-13T10:00:00+00:00", "source": "user", "is_bot": False},
            {"id": 2, "lead_id": 500, "direction": 1, "text": "Подскажите город", "created_at": "2026-04-13T10:00:30+00:00", "source": "bot", "is_bot": True},
            {"id": 3, "lead_id": 500, "direction": 1, "text": "Цена от 20 000 до 25 000 ₽, зависит от модели.", "created_at": "2026-04-13T10:02:00+00:00", "source": "manager", "is_bot": False},
            {"id": 4, "lead_id": 500, "direction": 0, "text": "Покажите варианты", "created_at": "2026-04-13T10:03:00+00:00", "source": "user", "is_bot": False},
        ]

    recorded: dict[str, object] = {}

    async def _finalize(*_a, **_k):
        return None

    async def _record_training(*_a, **kwargs):
        recorded.update(kwargs)
        return 123

    async def _candidate(*_a, **_k):
        return None

    monkeypatch.setattr(service_mod.db, "list_open_intervention_episodes", _episodes)
    monkeypatch.setattr(service_mod.db, "get_recent_lead_messages", _messages)
    monkeypatch.setattr(service_mod.db, "finalize_intervention_episode", _finalize)
    monkeypatch.setattr(service_mod.db, "record_training_example", _record_training)
    monkeypatch.setattr(service_mod.db, "upsert_policy_candidate_from_episode", _candidate)

    finalized = await service_mod.finalize_pending_episode_outcomes(
        tenant_id=101,
        lead_id=500,
    )
    assert finalized == 1
    assert recorded["source"] == "correction"
    assert recorded["q_text"] == "Сколько стоит?"
    assert "20 000" in recorded["a_text"]
    assert recorded["embedding_status"] == "pending"


@pytest.mark.asyncio
async def test_capture_intervention_episode_smoke(monkeypatch) -> None:
    state = SimpleNamespace(
        facts={"city": "Уфа"},
        known_slots={"city": "Уфа"},
        pending_fact_key="city",
        last_plan={"action": "ask_missing_fact"},
        last_bot_reply="Подскажите, пожалуйста, в каком городе нужна установка?",
        last_items=[],
        catalog_sent=False,
        catalog_delivery_mode="",
    )
    monkeypatch.setattr(
        service_mod,
        "read_tenant_config",
        lambda _tenant: {"learning": {"enabled": True, "intervention_policy": {"enabled": True, "capture_enabled": True}}},
    )
    monkeypatch.setattr(service_mod, "load_sales_state", lambda *_a, **_k: state)

    async def _noop(*_a, **_k):
        return 0

    async def _messages(*_a, **_k):
        return [
            {"id": 1, "lead_id": 500, "direction": 0, "text": "Сколько стоит?", "created_at": "2026-04-13T10:00:00+00:00", "source": "user", "is_bot": False},
            {"id": 2, "lead_id": 500, "direction": 1, "text": "Подскажите, пожалуйста, в каком городе нужна установка?", "created_at": "2026-04-13T10:00:30+00:00", "source": "bot", "is_bot": True},
            {"id": 3, "lead_id": 500, "direction": 1, "text": "Цена от 20 000 до 25 000 ₽, зависит от модели.", "created_at": "2026-04-13T10:02:00+00:00", "source": "manager", "is_bot": False},
        ]

    async def _create_snapshot(_snapshot):
        return 10

    captured: dict[str, object] = {}

    async def _create_episode(**kwargs):
        captured.update(kwargs)
        return 99

    async def _labels(*_a, **_k):
        return None

    async def _recent_decision(*_a, **_k):
        return {"id": 71, "recommended_action": "give_price"}

    async def _create_policy_outcome(**kwargs):
        captured["policy_outcome"] = kwargs
        return 1

    monkeypatch.setattr(service_mod, "finalize_pending_episode_outcomes", _noop)
    monkeypatch.setattr(service_mod.db, "get_recent_lead_messages", _messages)
    monkeypatch.setattr(service_mod.db, "create_dialogue_state_snapshot", _create_snapshot)
    monkeypatch.setattr(service_mod.db, "create_intervention_episode", _create_episode)
    monkeypatch.setattr(service_mod.db, "insert_episode_labels", _labels)
    monkeypatch.setattr(service_mod.db, "get_recent_policy_decision_for_lead", _recent_decision)
    monkeypatch.setattr(service_mod.db, "create_policy_outcome", _create_policy_outcome)

    episode_id = await capture_intervention_episode(
        tenant_id=101,
        lead_id=500,
        channel="telegram",
        source_event="manager_outgoing",
        manager_message_id=3,
    )
    assert episode_id == 99
    assert captured["source_event"] == "manager_outgoing"
    assert captured["manager_action"]["action"] == "give_price_range"
    assert captured["bot_action"]["action"] == "ask_missing_fact"
