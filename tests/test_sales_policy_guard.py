import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from libs.core import sales_core as core


def _patch_minimal_fallback_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    state: core.SalesState,
    persona: str = "",
    catalog_url: str = "",
) -> None:
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: persona, raising=False)
    monkeypatch.setattr(core, "load_persona_hints", lambda tenant, channel: core.PersonaHints(), raising=False)
    monkeypatch.setattr(
        core,
        "_branding_for_tenant",
        lambda tenant, channel: {"CATALOG_URL": catalog_url},
        raising=False,
    )


def test_safe_minimal_fallback_no_catalog_pdf_loop(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=1)
    _patch_minimal_fallback_env(monkeypatch, state=state, persona="", catalog_url="https://example.com/cat")
    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="avito",
        contact_ref=1,
        last_user_message="подскажите пожалуйста",
    )
    low = out.lower()
    assert "каталог pdf" not in low
    assert "что важно в первую очередь" not in low


def test_safe_minimal_fallback_catalog_only_on_explicit_request(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=2)
    _patch_minimal_fallback_env(monkeypatch, state=state, persona="", catalog_url="https://example.com/cat")

    out_regular = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="avito",
        contact_ref=2,
        last_user_message="здравствуйте",
    )
    out_catalog = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="avito",
        contact_ref=2,
        last_user_message="скиньте каталог",
    )
    assert "https://example.com/cat" not in out_regular
    assert "https://example.com/cat" in out_catalog


def test_safe_minimal_fallback_catalog_intent_does_not_jump_to_contact_request(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=21)
    persona = (
        "## Диалог-скрипт\n"
        "1) Уточнить город\n"
        "2) Подскажите его номер и предложите написать ему первым\n"
    )
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")
    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=21,
        last_user_message="самая дорогая дверь в квартиру",
    )
    low = out.lower()
    assert "номер" not in low
    assert "написать ему первым" not in low


def test_safe_minimal_fallback_catalog_intent_uses_full_catalog_when_grounding_empty(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=22)
    _patch_minimal_fallback_env(monkeypatch, state=state, persona="", catalog_url="")
    monkeypatch.setattr(core, "_build_reply_grounding", lambda **_: {}, raising=False)
    monkeypatch.setattr(
        core,
        "read_all_catalog",
        lambda cfg=None, tenant=None: [{"title": "X", "price": "36900"}],
        raising=False,
    )
    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=22,
        last_user_message="самая дорогая дверь",
    )
    assert "36 900" in out or "36900" in out


def test_safe_minimal_fallback_offtopic_does_not_force_qualification(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=3)
    persona = "## Диалог-скрипт\n1) Уточнить город\n2) Уточнить тип объекта\n"
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")
    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=3,
        last_user_message="как дела?",
    )
    low = out.lower()
    assert "в каком городе" not in low
    assert "город?" not in low


def test_safe_minimal_fallback_updates_memory_and_avoids_repeat_persona_injections(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=31)
    state.facts = {"city": "Уфа"}
    persona = (
        "Если клиент дал город `Уфа`: "
        "в этом же ответе обязательно добавьте: "
        "\"Адрес магазина: Менделеева 80\" и "
        "\"При заказе в течение недели действует скидка 2000 ₽\""
    )
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")
    out1 = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=31,
        last_user_message="уфа",
    )
    out2 = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=31,
        last_user_message="покажите варианты",
    )
    low1 = out1.lower()
    low2 = out2.lower()
    assert "менделеева 80" in low1
    assert "2000" in low1
    assert "менделеева 80" not in low2
    assert "2000" not in low2


def test_persona_direct_reply_does_not_override_selected_model_attribute_followup():
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    out = core._persona_direct_reply_for_user_turn(
        persona,
        last_user_message="а как они по шумке?",
        known_facts={"model": "гарда зеркало"},
        state=core.SalesState(tenant=101, contact_id=5001),
    )
    assert out == ""


def test_fallback_contextual_question_skips_uncertain_reply_for_selected_model_attribute_followup():
    state = core.SalesState(tenant=101, contact_id=5002)
    state.facts["model"] = "гарда зеркало"
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    out = core._fallback_contextual_question(
        "а как они по шумке?",
        state=state,
        persona_context=persona,
    )
    assert out == ""


def test_persona_sequence_obligations_do_not_override_substantive_selected_model_attribute_reply():
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    out = core._apply_persona_sequence_obligations(
        "Наполнение: ПЕНОПОЛИСТИРОЛ. Контуров уплотнения: 2. Толщина полотна: 75 ММ.",
        persona_context=persona,
        last_user_message="а как они по шумке?",
        known_facts={"model": "гарда зеркало"},
        state=core.SalesState(tenant=101, contact_id=5003),
    )
    low = out.lower()
    assert "уточню" not in low
    assert "город" not in low
    assert "квартира" not in low
    assert "пенополистирол" in low


def test_selected_item_attribute_answer_handles_thickness_skepticism():
    item = {
        "title": "ГАРДА ЗЕРКАЛО",
        "Толщина полотна": "75 ММ",
        "Наполнение двери": "ПЕНОПОЛИСТИРОЛ",
    }
    out = core._selected_item_attribute_answer("это которые как жестянная банка?", item)
    low = out.lower()
    assert "75" in out
    assert "пенополистирол" in low


def test_selected_item_attribute_answer_handles_insulation_without_question_mark():
    item = {
        "title": "ГАРДА ЗЕРКАЛО",
        "Наполнение двери": "ПЕНОПОЛИСТИРОЛ",
        "Количество контуров уплотнений": "2",
        "Толщина полотна": "75 ММ",
    }
    out = core._selected_item_attribute_answer("какое утепление там", item)
    low = out.lower()
    assert "пенополистирол" in low


def test_selected_item_attribute_answer_handles_inside_color_yes_no_question():
    item = {
        "title": "ГАРДА ЗЕРКАЛО",
        "Цвет внутренней панели": "БЕЛЫЙ ЯСЕНЬ",
        "Цвет покраски": "МУАР ЧЕРНЫЙ",
    }
    out = core._selected_item_attribute_answer("она белая внутри?", item)
    low = out.lower()
    assert "бел" in low or "ясень" in low


def test_selected_item_brief_answer_avoids_broken_lowercasing_for_uppercase_model():
    out = core._selected_item_brief_answer(
        {
            "title": "ГАРДА ЗЕРКАЛО",
            "price": "29500",
            "Цвет внутренней панели": "БЕЛЫЙ ЯСЕНЬ",
        }
    )
    assert "Гарда Зеркало" in out
    assert "гАРДА" not in out


def test_safe_minimal_fallback_catalog_unavailable_keeps_model_stage_and_offers_variants(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5004)
    state.facts = {"city": "уфа", "object_type": "apartment", "address": "пугачева 7"}
    state.pending_fact_key = "model"
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")
    monkeypatch.setattr(
        core,
        "search_catalog",
        lambda needs, limit=2, tenant=None, query="": [
            {"title": "ГАРДА ЗЕРКАЛО", "price": "29500"},
            {"title": "ГАРДА 7 СМ ЗЕРКАЛО", "price": "29000"},
        ],
        raising=False,
    )
    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=5004,
        last_user_message="грузится ещё",
    )
    low = out.lower()
    assert "гарда зеркало" in low
    assert "29 500" in out or "29500" in out
    assert state.pending_fact_key == "model"


def test_humanize_reply_text_normalizes_question_dot_artifacts():
    state = core.SalesState(tenant=101, contact_id=5005)
    out = core._humanize_reply_text(
        "Адрес магазина: Менделеева 80. Для квартиры или частного дома подбираете?.",
        state=state,
        persona_hints=core.PersonaHints(),
    )
    assert "?." not in out


def test_prioritize_missing_facts_follows_persona_qualification_order():
    ordered = core._prioritize_missing_facts(["address", "object_type", "model", "city"])
    assert ordered[:4] == ["city", "object_type", "address", "model"]


def test_classify_turn_intent_detects_why_question():
    assert core._classify_turn_intent("зачем вам мой адрес?") == "why_question"


def test_classify_turn_intent_detects_repair_complaint():
    assert core._classify_turn_intent("я же говорил уже") == "repair"


def test_safe_minimal_fallback_answers_why_question_before_repeating_slot(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5008)
    state.facts = {"city": "уфа", "object_type": "apartment"}
    state.pending_fact_key = "address"
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")

    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=5008,
        last_user_message="зачем вам мой адрес?",
    )
    low = out.lower()
    assert "адрес нужен" in low
    assert "квартир" not in low
    assert state.pending_fact_key == "address"


def test_safe_minimal_fallback_repair_turn_keeps_current_step(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5009)
    state.facts = {"city": "уфа", "object_type": "apartment"}
    state.pending_fact_key = "address"
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")

    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=5009,
        last_user_message="чего?",
    )
    low = out.lower()
    assert "адрес" in low
    assert "город" not in low
    assert state.pending_fact_key == "address"


def test_safe_minimal_fallback_unresolved_model_followup_stays_on_model_step(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5011)
    state.facts = {"city": "уфа", "object_type": "apartment", "address": "пугачева 7"}
    state.pending_fact_key = "model"
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")
    monkeypatch.setattr(core, "_build_reply_grounding", lambda **_: {}, raising=False)

    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=5011,
        last_user_message="какое утепление там?",
    )
    low = out.lower()
    assert "модель" in low or "каталога" in low
    assert "город" not in low
    assert "квартир" not in low
    assert state.pending_fact_key == "model"


def test_safe_minimal_fallback_repeated_model_question_does_not_fall_back_to_old_script_step(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5012)
    state.facts = {"city": "уфа", "object_type": "apartment", "address": "пугачева 7"}
    state.pending_fact_key = "model"
    state.asked_questions = ["Подскажите, какая модель/тип из каталога интересует?"]
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")
    monkeypatch.setattr(core, "_build_reply_grounding", lambda **_: {}, raising=False)

    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=5012,
        last_user_message="я же говорил уже",
    )
    low = out.lower()
    assert "тип объекта" not in low
    assert "город" not in low
    assert state.pending_fact_key == "model"


def test_safe_minimal_fallback_unresolved_model_followup_uses_last_offered_shortlist(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5013)
    state.facts = {"city": "уфа", "object_type": "apartment", "address": "космонавтов 76"}
    state.pending_fact_key = "model"
    state.last_items = [
        {
            "title": "ГЕРМЕС ГОСТ МЕТ/МЕТ",
            "price": "17500",
            "Толщина полотна": "75 ММ",
            "Толщина металла": "1,2 ММ",
            "Наполнение двери": "ПЕНОПОЛИСТИРОЛ",
        },
        {
            "title": "ГЕРМЕС ГОСТ МЕТ/МДФ",
            "price": "17500",
            "Толщина полотна": "75 ММ",
            "Толщина металла": "1,2 ММ",
            "Наполнение двери": "ПЕНОПОЛИСТИРОЛ",
        },
    ]
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")
    monkeypatch.setattr(core, "_build_reply_grounding", lambda **_: {}, raising=False)

    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=5013,
        last_user_message="это которые как жестяные банки?",
    )
    low = out.lower()
    assert "75" in out or "пенополистирол" in low
    assert "какая модель" not in low
    assert state.pending_fact_key == "model"


@pytest.mark.asyncio
async def test_ask_llm_routes_selected_model_followup_to_single_llm_path(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5006)
    state.facts = {"model": "гарда зеркало"}
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "_get_openai_client", lambda: object(), raising=False)
    monkeypatch.setattr(core, "load_persona_hints", lambda tenant, channel: core.PersonaHints(), raising=False)
    monkeypatch.setattr(core, "load_tenant", lambda tenant: {}, raising=False)
    monkeypatch.setattr(core, "_single_llm_reply", AsyncMock(return_value="FACTUAL"), raising=False)

    out = await core.ask_llm(
        [{"role": "user", "content": "а как они по шумке?"}],
        tenant=101,
        contact_id=5006,
        channel="telegram",
    )
    assert str(out) == "FACTUAL"


@pytest.mark.asyncio
async def test_ask_llm_routes_contextual_short_followup_with_last_items_to_llm(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5014)
    state.facts = {"city": "уфа", "object_type": "apartment", "address": "пугачева 7"}
    state.pending_fact_key = "model"
    state.last_items = [
        {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
        {"title": "ГЕРМЕС ГОСТ МЕТ/МДФ", "price": "17500"},
    ]
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "_get_openai_client", lambda: object(), raising=False)
    monkeypatch.setattr(core, "load_persona_hints", lambda tenant, channel: core.PersonaHints(), raising=False)
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: "", raising=False)
    monkeypatch.setattr(core, "load_tenant", lambda tenant: {}, raising=False)
    monkeypatch.setattr(core, "_build_reply_grounding", lambda **kwargs: {}, raising=False)
    monkeypatch.setattr(core, "_safe_minimal_fallback_reply", lambda **kwargs: "FALLBACK", raising=False)
    monkeypatch.setattr(core, "_resolve_brain_mode", lambda tenant, cfg=None: "smart", raising=False)
    monkeypatch.setattr(core, "openai", SimpleNamespace(api_key=None), raising=False)

    async def fake_single(*args, **kwargs):
        return "LLM_HANDLED"

    monkeypatch.setattr(core, "_single_llm_reply", fake_single, raising=False)

    out = await core.ask_llm(
        [{"role": "user", "content": "Давайте"}],
        tenant=101,
        contact_id=5014,
        channel="telegram",
    )
    assert str(out) == "LLM_HANDLED"


def test_safe_minimal_fallback_contextual_short_followup_uses_active_shortlist(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5015)
    state.facts = {"city": "уфа", "object_type": "apartment", "address": "космонавтов 76"}
    state.pending_fact_key = "model"
    state.last_items = [
        {
            "title": "ГАРДА 8 ММ",
            "price": "23900",
            "Толщина полотна": "90 ММ",
            "Толщина металла": "1,5 ММ",
        },
        {
            "title": "ЦАРСКОЕ",
            "price": "27500",
            "Толщина полотна": "100 ММ",
            "Толщина металла": "1,8 ММ",
        },
    ]
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")
    monkeypatch.setattr(core, "_build_reply_grounding", lambda **_: {}, raising=False)

    out = core._safe_minimal_fallback_reply(
        tenant=101,
        channel_name="telegram",
        contact_ref=5015,
        last_user_message="Давайте",
    )
    low = out.lower()
    assert "какая модель" not in low
    assert "гарда 8 мм" in low or "царское" in low


@pytest.mark.asyncio
async def test_ask_llm_routes_qualification_turn_to_single_llm_path(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5007)
    state.pending_fact_key = "address"
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "_get_openai_client", lambda: object(), raising=False)
    monkeypatch.setattr(core, "load_persona_hints", lambda tenant, channel: core.PersonaHints(), raising=False)
    monkeypatch.setattr(core, "load_tenant", lambda tenant: {}, raising=False)
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: "## Диалог-скрипт\n1) Уточнить город\n2) Уточнить адрес установки\n", raising=False)
    monkeypatch.setattr(core, "_single_llm_reply", AsyncMock(return_value="ADDRESS_FLOW"), raising=False)

    out = await core.ask_llm(
        [{"role": "user", "content": "пугачева 7"}],
        tenant=101,
        contact_id=5007,
        channel="telegram",
    )
    assert str(out) == "ADDRESS_FLOW"


@pytest.mark.asyncio
async def test_single_llm_reply_applies_humanize_postprocess(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=5099)
    state.user_message_count = 2

    monkeypatch.setenv("SALES_EVAL_LITE", "1")
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "record_bot_reply", lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(core, "_build_human_mode_messages", lambda messages: list(messages), raising=False)
    monkeypatch.setattr(core, "_build_reply_grounding", lambda **kwargs: {}, raising=False)
    monkeypatch.setattr(core, "_state_facts_snapshot", lambda _state: {}, raising=False)
    monkeypatch.setattr(core, "_resolve_persona_rules_context", lambda **kwargs: "", raising=False)
    monkeypatch.setattr(
        core,
        "_apply_persona_sequence_obligations",
        lambda reply, **kwargs: str(reply or "").strip(),
        raising=False,
    )
    monkeypatch.setattr(
        core,
        "_apply_persona_delivery_obligations",
        lambda reply, **kwargs: str(reply or "").strip(),
        raising=False,
    )
    monkeypatch.setattr(
        core,
        "_enforce_next_required_fact_question",
        lambda reply, **kwargs: (str(reply or "").strip(), ""),
        raising=False,
    )
    monkeypatch.setattr(
        core,
        "_stabilize_followup_price_reference",
        lambda reply, **kwargs: str(reply or "").strip(),
        raising=False,
    )
    monkeypatch.setattr(
        core,
        "_apply_base_answer_quality_floor",
        lambda reply, **kwargs: str(reply or "").strip(),
        raising=False,
    )
    monkeypatch.setattr(
        core,
        "_ensure_dialog_greeting_on_first_reply",
        lambda reply, *_args, **_kwargs: str(reply or "").strip(),
        raising=False,
    )
    monkeypatch.setattr(
        core,
        "_humanize_reply_text",
        lambda reply, **kwargs: f"humanized::{str(reply or '').strip()}",
        raising=False,
    )
    monkeypatch.setattr(core, "_resolve_chat_completion_callable", lambda client: object(), raising=False)

    async def _fake_llm_call(_create_fn, **kwargs):
        if kwargs.get("response_format"):
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            content=(
                                '{"action":"respond","intent":"general","intent_tags":[],'
                                '"question_strategy":{"should_ask":false,"question_goal":"","question_fact_key":""},'
                                '"claims":[],"fact_updates":[],"selected_item_ref":"",'
                                '"reply_plan":{"tone":"persona","brief":true,"ack":true}}'
                            )
                        )
                    )
                ]
            )
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="Тестовый ответ"))]
        )

    monkeypatch.setattr(core, "_llm_call_with_deadline", _fake_llm_call, raising=False)

    out = await core._single_llm_reply(
        object(),
        [{"role": "system", "content": "persona"}, {"role": "user", "content": "здравствуйте"}],
        core.PersonaHints(language="ru"),
        state,
        "telegram",
        5099,
        101,
        "здравствуйте",
    )
    assert str(out).startswith("humanized::Тестовый ответ")


def test_safe_minimal_fallback_replays_core_persona_dialog_without_slot_loop(
    monkeypatch: pytest.MonkeyPatch,
):
    contact = 5010
    tenant = 101
    persona = (ROOT / "data/tenants/101/persona_telegram.md").read_text(encoding="utf-8")
    state = core.SalesState(tenant=tenant, contact_id=contact)
    _patch_minimal_fallback_env(monkeypatch, state=state, persona=persona, catalog_url="")

    reply0 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="здравствуйте",
    )
    assert "город" in reply0.lower()

    core._capture_pending_fact_answer(state, "уфа")
    reply1 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="уфа",
    )
    low1 = reply1.lower()
    assert "менделеева 80" in low1
    assert "2000" in low1
    assert "квартир" in low1 or "частного дома" in low1

    core._capture_pending_fact_answer(state, "квартира")
    reply2 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="квартира",
    )
    assert "адрес установки" in reply2.lower()
    assert state.pending_fact_key == "address"

    reply3 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="зачем вам мой адрес?",
    )
    assert "адрес нужен" in reply3.lower()
    assert state.pending_fact_key == "address"

    core._capture_pending_fact_answer(state, "пугачева 7")
    reply4 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="пугачева 7",
    )
    assert "модель" in reply4.lower() or "каталога" in reply4.lower()

    core._maybe_store_model_slot(state, tenant, "гарда зеркало")
    state.facts["model"] = "гарда зеркало"
    selected_item = {
        "title": "ГАРДА ЗЕРКАЛО",
        "price": "29500",
        "Цвет внутренней панели": "БЕЛЫЙ ЯСЕНЬ",
        "Толщина полотна": "75 ММ",
        "Толщина металла": "1,2 ММ",
        "Наполнение двери": "ПЕНОПОЛИСТИРОЛ",
        "Количество контуров уплотнений": "2",
        "Количество замков": "3",
        "Тип замков": "СУВАЛЬДНЫЙ, ЦИЛИНДРОВЫЙ И НОЧНАЯ ЗАДВИЖКА",
    }
    monkeypatch.setattr(
        core,
        "_build_reply_grounding",
        lambda **_: {"selected_item": dict(selected_item), "catalog_items": [dict(selected_item)]},
        raising=False,
    )
    reply5 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="гарда зеркало",
    )
    assert "гарда зеркало" in reply5.lower()

    reply6 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="это которые как жестянные банки?",
    )
    assert "75" in reply6 or "пенополистирол" in reply6.lower()

    reply7 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="а как они по шумке?",
    )
    assert "пенополистирол" in reply7.lower()

    reply8 = core._safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name="telegram",
        contact_ref=contact,
        last_user_message="какие там замки?",
    )
    low8 = reply8.lower()
    assert "замков" in low8 or "сувальд" in low8 or "цилиндров" in low8

    joined = " ".join(
        part.lower() for part in (reply3, reply4, reply5, reply6, reply7, reply8)
    )
    assert "подскажите город" not in joined


def test_persona_primary_script_question_from_freeform_imperative():
    persona = (
        "Вы администратор автосервиса\n"
        "Общение на Вы\n"
        "Уточни марку авто, проблему, желаемое время визита\n"
    )
    q = core._persona_primary_script_question(persona)
    assert "марку авто" in q.lower()
    assert "?" in q


def test_persona_script_questions_skip_operator_like_multi_action_lines():
    persona = (
        "## Диалог-скрипт\n"
        "1) Уточни город\n"
        "2) Подскажите его номер и предложите написать ему первым\n"
        "3) Уточни тип объекта\n"
    )
    questions = core._persona_script_questions(persona)
    low = " | ".join(questions).lower()
    assert "город" in low
    assert "тип объекта" in low
    assert "предложите написать ему первым" not in low


def test_capture_pending_budget_ignores_non_budget_text():
    state = core.SalesState(tenant=101, contact_id=17)
    state.pending_fact_key = "budget"
    core._capture_pending_fact_answer(state, "шкаф в спальню")
    assert str(state.facts.get("budget") or "").strip() == ""
    assert state.pending_fact_key == "budget"


def test_capture_pending_dimensions_ignores_irrelevant_text():
    state = core.SalesState(tenant=101, contact_id=18)
    state.pending_fact_key = "dimensions"
    core._capture_pending_fact_answer(state, "квартира")
    assert str(state.facts.get("dimensions") or "").strip() == ""
    assert state.pending_fact_key == "dimensions"


def test_capture_pending_model_requires_catalog_match_and_ignores_complaint(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=19)
    state.pending_fact_key = "model"
    monkeypatch.setattr(
        core,
        "_read_catalog",
        lambda tenant: [{"title": "ГАРДА ЗЕРКАЛО", "price": "29500"}],
        raising=False,
    )
    core._capture_pending_fact_answer(state, "я же говорил уже")
    assert str(state.facts.get("model") or "").strip() == ""
    assert state.pending_fact_key == "model"


def test_capture_pending_model_saves_canonical_catalog_label(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=20)
    state.pending_fact_key = "model"
    monkeypatch.setattr(
        core,
        "_read_catalog",
        lambda tenant: [{"title": "ГАРДА ЗЕРКАЛО", "price": "29500"}],
        raising=False,
    )
    core._capture_pending_fact_answer(state, "гарда зеркало")
    assert str(state.facts.get("model") or "").strip() == "ГАРДА ЗЕРКАЛО"
    assert str((state.known_slots or {}).get("model") or "").strip() == "ГАРДА ЗЕРКАЛО"
    assert state.pending_fact_key == ""


def test_classify_turn_intent_offtopic_smalltalk():
    assert core._classify_turn_intent("как дела?") == "offtopic"
    assert core._classify_turn_intent("скиньте каталог") == "catalog_request"


def test_semantic_guard_drops_repeated_topic_when_fact_confirmed():
    state = core.SalesState(tenant=1, contact_id=42)
    state.facts["city"] = "Уфа"
    core._remember_question_state(state, "В каком городе планируете установку?")
    plan = {
        "question": "В каком городе нужна установка?",
        "question_slot": "location",
        "required_facts": [],
    }
    guarded = core._enforce_semantic_plan_guards(plan, state=state, grounding={})
    assert guarded.get("question") == ""
    assert guarded.get("question_slot") == "none"


def test_merge_fact_updates_rejects_unconfirmed_core_facts():
    state = core.SalesState(tenant=101, contact_id=4201)
    core._merge_fact_updates(
        state,
        {"city": "Уфа", "object_type": "частный дом"},
        user_text="здравствуйте",
    )
    assert str(state.facts.get("city") or "").strip() == ""
    assert str(state.facts.get("object_type") or "").strip() == ""


def test_load_sales_state_does_not_resurrect_deleted_cache(monkeypatch: pytest.MonkeyPatch):
    key = core._state_key(101, 909001)
    cached = core.SalesState(tenant=101, contact_id=909001)
    cached.facts["city"] = "Уфа"
    core._STATE_CACHE[key] = cached
    monkeypatch.setattr(core, "_state_store_read", lambda _key: None, raising=False)
    state = core.load_sales_state(101, 909001)
    assert state is not cached
    assert str(state.facts.get("city") or "").strip() == ""
    assert key not in core._STATE_CACHE


def test_merge_fact_updates_accepts_core_facts_when_user_confirmed():
    state = core.SalesState(tenant=101, contact_id=4202)
    core._merge_fact_updates(
        state,
        {"city": "Уфа", "object_type": "частный дом"},
        user_text="город уфа, для частного дома",
    )
    assert str(state.facts.get("city") or "").lower() == "уфа"
    assert str(state.facts.get("object_type") or "") == "house"


def test_maybe_store_model_slot_ignores_greeting(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=909002)
    monkeypatch.setattr(
        core,
        "_read_catalog",
        lambda tenant: [{"title": "Гарда 8 мм", "price": "23900"}],
        raising=False,
    )
    monkeypatch.setattr(
        core,
        "_best_catalog_item_match",
        lambda text, items: {"title": "Гарда 8 мм"},
        raising=False,
    )
    core._maybe_store_model_slot(state, 101, "Здравствуйте")
    assert str(state.known_slots.get("model") or "").strip() == ""


def test_enforce_next_required_fact_question_replaces_unconfirmed_fact_claim():
    state = core.SalesState(tenant=101, contact_id=4203)
    persona = "## Диалог-скрипт\n1) Уточнить город\n2) Уточнить тип объекта\n"
    reply = "Для частного дома в Уфе можем предложить терморазрыв."
    out, key = core._enforce_next_required_fact_question(
        reply,
        state=state,
        persona_context=persona,
        known_facts={},
        user_text="здравствуйте",
        grounding={},
    )
    assert key == "city"
    assert "город" in out.lower()
    assert "уф" not in out.lower()


@pytest.mark.anyio
async def test_build_llm_messages_persists_city_without_pending(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=11)
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: "", raising=False)
    monkeypatch.setattr(core, "summarize_sales_state", lambda *args, **kwargs: "", raising=False)
    monkeypatch.setattr(core, "_branding_for_tenant", lambda tenant, channel: {"CHANNEL": "Avito", "CURRENCY": "₽"}, raising=False)
    monkeypatch.setattr(core, "search_catalog", lambda *args, **kwargs: [], raising=False)

    await core.build_llm_messages(
        11,
        "город уфа",
        channel="avito",
        tenant=101,
    )
    assert str(state.facts.get("city") or "").lower() == "уфа"


@pytest.mark.anyio
async def test_build_llm_messages_persists_standalone_city_reply(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=13)
    state.pending_fact_key = "city"
    state.last_bot_reply = "Подскажите, пожалуйста, в каком городе нужна установка?"
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: "", raising=False)
    monkeypatch.setattr(core, "summarize_sales_state", lambda *args, **kwargs: "", raising=False)
    monkeypatch.setattr(
        core,
        "_branding_for_tenant",
        lambda tenant, channel: {"CHANNEL": "Telegram", "CURRENCY": "₽"},
        raising=False,
    )
    monkeypatch.setattr(core, "search_catalog", lambda *args, **kwargs: [], raising=False)

    await core.build_llm_messages(
        13,
        "уфа",
        channel="telegram",
        tenant=101,
    )
    assert str(state.facts.get("city") or "").lower() == "уфа"


@pytest.mark.anyio
async def test_build_llm_messages_does_not_store_non_city_word_as_city(monkeypatch: pytest.MonkeyPatch):
    state = core.SalesState(tenant=101, contact_id=12)
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: "", raising=False)
    monkeypatch.setattr(core, "summarize_sales_state", lambda *args, **kwargs: "", raising=False)
    monkeypatch.setattr(
        core,
        "_branding_for_tenant",
        lambda tenant, channel: {"CHANNEL": "Avito", "CURRENCY": "₽"},
        raising=False,
    )
    monkeypatch.setattr(core, "search_catalog", lambda *args, **kwargs: [], raising=False)

    await core.build_llm_messages(
        12,
        "летать",
        channel="avito",
        tenant=101,
    )
    assert str(state.facts.get("city") or "").strip() == ""


@pytest.mark.anyio
async def test_build_llm_messages_does_not_store_greeting_typo_as_city(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=14)
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: "", raising=False)
    monkeypatch.setattr(core, "summarize_sales_state", lambda *args, **kwargs: "", raising=False)
    monkeypatch.setattr(
        core,
        "_branding_for_tenant",
        lambda tenant, channel: {"CHANNEL": "Telegram", "CURRENCY": "₽"},
        raising=False,
    )
    monkeypatch.setattr(core, "search_catalog", lambda *args, **kwargs: [], raising=False)

    await core.build_llm_messages(
        14,
        "здравствуйтек",
        channel="telegram",
        tenant=101,
    )
    assert str(state.facts.get("city") or "").strip() == ""


@pytest.mark.anyio
async def test_build_llm_messages_does_not_store_question_phrase_as_city(
    monkeypatch: pytest.MonkeyPatch,
):
    state = core.SalesState(tenant=101, contact_id=15)
    state.pending_fact_key = "city"
    monkeypatch.setattr(core, "load_sales_state", lambda tenant, contact_id: state, raising=False)
    monkeypatch.setattr(core, "save_sales_state", lambda current: None, raising=False)
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: "", raising=False)
    monkeypatch.setattr(core, "summarize_sales_state", lambda *args, **kwargs: "", raising=False)
    monkeypatch.setattr(
        core,
        "_branding_for_tenant",
        lambda tenant, channel: {"CHANNEL": "Telegram", "CURRENCY": "₽"},
        raising=False,
    )
    monkeypatch.setattr(core, "search_catalog", lambda *args, **kwargs: [], raising=False)

    await core.build_llm_messages(
        15,
        "зачем вам адресм",
        channel="telegram",
        tenant=101,
    )
    assert str(state.facts.get("city") or "").strip() == ""


def test_apply_persona_sequence_obligations_does_not_append_raw_action_text():
    persona = "Если клиент из Уфы: Напишите служебную фразу для теста"
    base = "Подберу варианты по вашему запросу"
    out = core._apply_persona_sequence_obligations(
        base,
        persona_context=persona,
        last_user_message="Я из Уфы",
        known_facts={"city": "Уфа"},
        state=core.SalesState(tenant=1, contact_id=1),
    )
    assert out == base


def test_apply_persona_sequence_obligations_appends_conditional_discount_fact():
    persona = "Если клиент из Уфы: предлагайте скидку 2000 ₽ при заказе в течение недели"
    base = "Адрес магазина: Менделеева 80."
    out = core._apply_persona_sequence_obligations(
        base,
        persona_context=persona,
        last_user_message="уфа",
        known_facts={"city": "Уфа"},
        state=core.SalesState(tenant=1, contact_id=2),
    )
    assert "2000" in out
    assert "скидк" in out.lower()


def test_apply_persona_sequence_obligations_does_not_repeat_same_fact_block():
    persona = "Если клиент из Уфы: Адрес магазина: Менделеева 80. При заказе в течение недели действует скидка 2000 ₽."
    state = core.SalesState(tenant=1, contact_id=22)
    state.history.append(
        {
            "role": "assistant",
            "content": "Адрес магазина: Менделеева 80. При заказе в течение недели действует скидка 2000 ₽.",
        }
    )
    out = core._apply_persona_sequence_obligations(
        "Подберу пару вариантов по вашему запросу.",
        persona_context=persona,
        last_user_message="нужна дверь с зеркалом",
        known_facts={"city": "Уфа"},
        state=state,
    )
    low = out.lower()
    assert "подберу пару вариантов" in low
    assert "менделеева 80" not in low
    assert "2000" not in low


def test_apply_persona_sequence_obligations_respects_recent_fact_memory_for_rephrased_block():
    persona = "Если клиент из Уфы: Адрес магазина: Менделеева 80. Действует скидка 2000 ₽ при заказе в течение недели."
    state = core.SalesState(tenant=1, contact_id=221)
    state.recent_fact_fingerprints = [
        core._fact_fingerprint("При заказе в течение недели действует скидка 2000 ₽."),
        core._fact_fingerprint("Адрес магазина: Менделеева 80."),
    ]
    out = core._apply_persona_sequence_obligations(
        "Подберу варианты под ваш запрос.",
        persona_context=persona,
        last_user_message="покажите варианты",
        known_facts={"city": "Уфа"},
        state=state,
    )
    low = out.lower()
    assert "подберу варианты" in low
    assert "менделеева 80" not in low
    assert "2000" not in low


def test_apply_persona_sequence_obligations_allows_short_explicit_followup_for_discount():
    persona = "Если назван город Уфа: При заказе в течение недели действует скидка 2000 ₽."
    state = core.SalesState(tenant=1, contact_id=222)
    state.recent_fact_fingerprints = [
        core._fact_fingerprint("При заказе в течение недели действует скидка 2000 ₽.")
    ]
    out = core._apply_persona_sequence_obligations(
        "Понял.",
        persona_context=persona,
        last_user_message="а скидка?",
        known_facts={"city": "Уфа"},
        state=state,
    )
    assert "2000" in out


def test_dedupe_repeated_fact_sentences_removes_repeated_discount_line():
    state = core.SalesState(tenant=1, contact_id=23)
    sentence = "При заказе в течение недели действует скидка 2000 ₽."
    fp = core._fact_fingerprint(sentence)
    assert fp
    state.recent_fact_fingerprints = [fp]
    out = core._dedupe_repeated_fact_sentences(
        f"{sentence} Подскажите адрес установки.",
        state,
    )
    low = out.lower()
    assert "скидка 2000" not in low
    assert "адрес установки" in low


def test_dedupe_repeated_fact_sentences_removes_near_duplicate_sentence():
    state = core.SalesState(tenant=1, contact_id=24)
    state.recent_fact_fingerprints = [
        core._fact_fingerprint("Действует скидка 2000 ₽ при заказе в течение недели.")
    ]
    out = core._dedupe_repeated_fact_sentences(
        "При заказе в течение недели действует скидка 2000 ₽. Подскажите объект установки.",
        state,
    )
    low = out.lower()
    assert "скидка 2000" not in low
    assert "объект установки" in low


def test_apply_persona_sequence_obligations_supports_multiline_condition_rule():
    persona = (
        "- Если клиент из Уфы:\n"
        "  предлагайте скидку 2000 ₽ при заказе в течение недели\n"
    )
    base = "Адрес магазина: Менделеева 80."
    out = core._apply_persona_sequence_obligations(
        base,
        persona_context=persona,
        last_user_message="уфа",
        known_facts={"city": "Уфа"},
        state=core.SalesState(tenant=1, contact_id=3),
    )
    assert "2000" in out
    assert "скидк" in out.lower()


def test_apply_persona_sequence_obligations_meta_rule_does_not_erase_discount():
    persona = (
        "Если назван город Уфа: в этом же ответе дать адрес магазина и условие скидки, только потом переходить к уточнениям.\n"
        "Если клиент из Уфы, Стерлитамака, Оренбурга или любого города Республики Башкортостан: "
        "предлагайте скидку 2000 ₽ при заказе в течение недели.\n"
        "Если клиент дал город `Уфа`: "
        "в этом же ответе обязательно добавьте: "
        "\"Адрес магазина: Менделеева 80\" и "
        "\"При заказе в течение недели действует скидка 2000 ₽\"\n"
    )
    base = "Понял. Подскажите адрес установки."
    out = core._apply_persona_sequence_obligations(
        base,
        persona_context=persona,
        last_user_message="уфа",
        known_facts={"city": "Уфа"},
        state=core.SalesState(tenant=1, contact_id=4),
    )
    low = out.lower()
    assert "2000" in out
    assert "скидк" in low
    assert "в этом же ответе" not in low


def test_apply_persona_sequence_obligations_ignores_quoted_placeholder_template():
    persona = (
        "Если пользователь пишет в формате \"Город <название>\": "
        "\"<город>, понял. Подскажите, пожалуйста, адрес установки.\""
    )
    out = core._apply_persona_sequence_obligations(
        "Для начала уточню пару деталей.",
        persona_context=persona,
        last_user_message="зачем вам адрес",
        known_facts={},
        state=core.SalesState(tenant=1, contact_id=6),
    )
    assert "<город>" not in out
    assert ", понял" not in out.lower()


def test_apply_persona_sequence_obligations_strips_embedded_operator_tail_in_action():
    persona = (
        "Если клиент просит каталог: "
        "работаем по каталогу и выездом, без адресов магазинов "
        "поздоровайтесь, скажите что для квартир в наличии около 45 моделей"
    )
    out = core._apply_persona_sequence_obligations(
        "В каком городе планируете установку?",
        persona_context=persona,
        last_user_message="нужен каталог",
        known_facts={},
        state=core.SalesState(tenant=1, contact_id=8),
    )
    low = out.lower()
    assert "работаем по каталогу и выездом" in low
    assert "поздоровайтесь" not in low
    assert "скажите что" not in low


def test_strip_instruction_leaks_removes_trailing_unbalanced_quote():
    out = core._strip_instruction_leaks('Уточню и сразу вернусь с точным ответом".')
    assert out == "Уточню и сразу вернусь с точным ответом."


def test_strip_instruction_leaks_fixes_catalog_price_phrase():
    out = core._strip_instruction_leaks("Самая доступная — от цена по каталогу.")
    assert "от цена по каталогу" not in out.lower()
    assert "цена по каталогу" in out.lower()


def test_fallback_plan_does_not_emit_technical_phrase():
    plan = core._fallback_semantic_plan("гарда зеркало")
    blocks = plan.get("blocks") or []
    texts = " ".join(str(item.get("text") or "") for item in blocks if isinstance(item, dict))
    assert "Отвечаю по запросу" not in texts
    assert plan.get("question_slot") == "none"


def test_pending_address_rejects_non_address_text():
    state = core.SalesState(tenant=101, contact_id=1)
    state.pending_fact_key = "address"
    core._capture_pending_fact_answer(state, "гарда зеркало")
    assert "address" not in (state.facts or {})
    assert state.pending_fact_key == "address"

    core._capture_pending_fact_answer(state, "Авроры 5/3")
    assert (state.facts or {}).get("address") == "Авроры 5/3"
    assert state.pending_fact_key == ""


@pytest.mark.parametrize(
    "persona,expected",
    [
        (
            "## Диалог-скрипт\n"
            "1) Узнать город и адрес.\n"
            "2) Уточнить тип помещения.\n"
            "3) Спросить, что из каталога приглянулось.\n",
            ["city", "address", "object_type", "model"],
        ),
        (
            "## Диалог-скрипт\n"
            "1) Узнать город.\n"
            "2) Узнать адрес.\n"
            "3) Если клиент назвал источник — применить скидку.\n",
            ["city", "address"],
        ),
    ],
)
def test_required_facts_from_persona_text(persona: str, expected: list[str]):
    assert core._required_facts_from_persona_text(persona) == expected


def test_line_to_question_prefers_quoted_question_fragment() -> None:
    line = 'Спросить, что из каталога приглянулось - "Что из каталога приглянулось?"'
    out = core._line_to_question(line)
    assert out == "Что из каталога приглянулось?"


def test_compose_reply_skips_already_asked_question_block():
    state = core.SalesState(tenant=1, contact_id=42)
    core._remember_question_state(state, "В каком городе нужна установка?")

    plan = {
        "blocks": [
            {
                "type": "question",
                "text": "В каком городе нужна установка?",
                "requires": [],
                "question_key": "city",
            },
            {
                "type": "info",
                "text": "Покажу варианты после уточнения бюджета.",
                "requires": [],
            },
        ]
    }

    reply, next_key = core._compose_reply_from_policy_blocks(
        plan,
        state=state,
        known_facts={},
        required_facts=[],
    )
    assert "В каком городе" not in reply
    assert "Покажу варианты" in reply
    assert next_key == ""


def test_humanize_drops_neighbor_claim_without_address():
    state = core.SalesState(tenant=1, contact_id=7)
    text = "Недавно ставили дверь в соседнем доме. Что из каталога вам приглянулось?"

    out = core._humanize_reply_text(text, state=state, persona_hints=None)

    assert "соседнем доме" not in out.lower()
    assert "каталога" in out.lower()


def test_conversational_phrasing_normalizes_city_echo_ack():
    out = core._apply_conversational_phrasing("уфа, понял. Для квартиры или дома нужна дверь?")
    assert out == "Для квартиры или дома нужна дверь?"
    assert "уфа, понял" not in out.lower()


def test_conversational_phrasing_normalizes_city_echo_prinyato():
    out = core._apply_conversational_phrasing("уфа, принято. Для квартиры или дома нужна дверь?")
    assert out == "Для квартиры или дома нужна дверь?"
    assert "уфа, принято" not in out.lower()


def test_conversational_phrasing_normalizes_city_echo_prinyato_with_dash():
    out = core._apply_conversational_phrasing("уфа — принято. Для квартиры или дома нужна дверь?")
    assert out == "Для квартиры или дома нужна дверь?"
    assert "уфа — принято" not in out.lower()


def test_conversational_phrasing_rewrites_hey_opening():
    out = core._apply_conversational_phrasing("Привет! В каком городе нужна установка?")
    assert out.lower().startswith("здравствуйте")


def test_drop_repeated_questions_from_reply_skips_known_fact_question():
    state = core.SalesState(tenant=1, contact_id=31)
    state.facts["object_type"] = "apartment"
    out = core._drop_repeated_questions_from_reply(
        "Для квартиры или частного дома выбираете? Подберу варианты по каталогу.",
        state,
    )
    assert "Для квартиры или частного дома" not in out
    assert "Подберу варианты" in out


def test_unsubscribe_intent_helper():
    assert core._is_unsubscribe_intent("не пишите мне больше")
    assert core._is_unsubscribe_intent("стоп")
    assert not core._is_unsubscribe_intent("подскажите цену")


def test_is_price_intent_handles_plural_stoyat():
    assert core._is_price_intent("сколько стоят эти модели?")


def test_ensure_concrete_variants_in_reply_keeps_llm_text_when_promised():
    grounding = {
        "items": [
            {"title": "ГАРДА ЗЕРКАЛО", "price": "29500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9СМ", "price": "33900"},
        ]
    }
    reply = "Сейчас скину варианты и цены"
    out = core._ensure_concrete_variants_in_reply(
        reply,
        grounding=grounding,
        user_text="ну так где варианты?",
    )
    assert out == reply


def test_ensure_concrete_variants_in_reply_does_not_duplicate_when_model_already_present():
    grounding = {"items": [{"title": "ГАРДА ЗЕРКАЛО", "price": "29500"}]}
    reply = "Могу предложить ГАРДА ЗЕРКАЛО — 29 500 ₽"
    out = core._ensure_concrete_variants_in_reply(
        reply,
        grounding=grounding,
        user_text="подберите вариант",
    )
    assert out == reply


def test_ensure_concrete_variants_in_reply_overrides_model_question_on_variants_request():
    grounding = {
        "items": [
            {"title": "ГАРДА ЗЕРКАЛО", "price": "29500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9СМ", "price": "33900"},
        ]
    }
    reply = "Какой вариант показать подробнее?"
    out = core._ensure_concrete_variants_in_reply(
        reply,
        grounding=grounding,
        user_text="дайте 2-3 варианта с ценой",
    )
    assert out == reply


def test_catalog_truth_guard_rewrites_invalid_prices_with_shortlist():
    grounding = {
        "items": [
            {"title": "ГАРДА ЗЕРКАЛО", "price": "29500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9СМ", "price": "33900"},
        ],
        "catalog_items": [
            {"title": "ГАРДА ЗЕРКАЛО", "price": "29500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9СМ", "price": "33900"},
        ],
    }
    out = core._enforce_catalog_truth_guard(
        "В ваш бюджет подойдут: гарда зеркало — 34 900 ₽ и эмалит зеркало 9см — 33 900 ₽",
        grounding=grounding,
        user_text="покажите 2-3 модели с ценой",
    )
    low = out.lower()
    assert "34 900" not in low
    assert "29 500" in low
    assert "33 900" in low


def test_filter_items_by_object_type_need_uses_object_type_hints():
    items = [
        {"title": "A", "object_type": "квартира"},
        {"title": "B", "object_type": "частный дом"},
    ]
    out = core._filter_items_by_object_type_need(items, {"object_type": "house"})
    assert len(out) == 1
    assert out[0]["title"] == "B"


def test_extract_expected_tokens_from_condition_ignores_generic_city_words():
    tokens = core._extract_expected_tokens_from_condition(
        "Если клиент из Уфы, Стерлитамака или любого города Республики Башкортостан"
    )
    assert "любого" not in tokens
    assert "города" not in tokens
    assert "республики" not in tokens
    assert "уфы" in tokens
    assert "стерлитамака" in tokens


def test_extract_price_spans_supports_narrow_nbsp_grouping():
    spans = core._extract_price_spans("цена 34 900 ₽ и 33 900 ₽")
    values = [item[2] for item in spans]
    assert 34900 in values
    assert 33900 in values


def test_catalog_claim_coverage_flags_catalog_text_without_claims():
    items = [{"title": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ", "price": 26500}]
    issues = core._catalog_claim_coverage_issues(
        "Могу предложить Гарда 7.5 Бетон снежный — 26 500 ₽",
        policy={},
        grounding_items=items,
    )
    assert "catalog_item_mentioned_without_validated_claims" in issues


def test_catalog_claim_coverage_allows_identity_and_price_claims():
    item = {"title": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ", "price": 26500}
    item_id = core._catalog_item_identity(item)
    policy = {
        "claims": [
            {
                "type": "catalog_item_identity",
                "subject": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ",
                "item_id": item_id,
                "attribute": "",
                "value": "",
            },
            {
                "type": "catalog_price",
                "subject": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ",
                "item_id": item_id,
                "attribute": "price",
                "value": "26 500 ₽",
            },
        ]
    }
    issues = core._catalog_claim_coverage_issues(
        "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ — 26 500 ₽",
        policy=policy,
        grounding_items=[item],
    )
    assert issues == []


def test_catalog_claim_coverage_flags_attribute_like_details_without_attribute_claim():
    item = {"title": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ", "price": 26500}
    item_id = core._catalog_item_identity(item)
    policy = {
        "claims": [
            {
                "type": "catalog_item_identity",
                "subject": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ",
                "item_id": item_id,
                "attribute": "",
                "value": "",
            },
            {
                "type": "catalog_price",
                "subject": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ",
                "item_id": item_id,
                "attribute": "price",
                "value": "26 500 ₽",
            },
        ]
    }
    issues = core._catalog_claim_coverage_issues(
        "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ — 26 500 ₽, у нее две панели и современный дизайн с максимальной шумоизоляцией.",
        policy=policy,
        grounding_items=[item],
    )
    assert "attribute_like_details_without_validated_claim" in issues


def test_ensure_dialog_greeting_not_injected_mid_dialog():
    state = core.SalesState(tenant=1, contact_id=123)
    state.user_message_count = 3
    state.history = [
        {"role": "user", "content": "здравствуйте"},
        {"role": "assistant", "content": "Добрый день! В каком городе установка?"},
    ]
    persona = "Первое сообщение в диалоге всегда начинайте с приветствия."
    source = "Если дверь не открывается, могу предложить варианты из каталога."
    out = core._ensure_dialog_greeting_on_first_reply(source, state, persona_context=persona)
    assert out == source


def test_rewrite_loses_context_anchors_detects_loss():
    candidate = "На Гоголя 31 ставили недавно, проём обычно 90 см. Что из каталога приглянулось?"
    rewrite = "Что из каталога приглянулось, уже что-то выбрали или подсказать варианты?"
    dialogue = [{"role": "user", "content": "гоголя 33"}]
    assert core._rewrite_loses_context_anchors(candidate, rewrite, dialogue)


@pytest.mark.anyio
async def test_audit_persona_reply_keeps_contact_artifacts():
    answer = "Telegram: @dverigermes\nТелефон: 89866666133"
    out = await core._audit_and_rewrite_persona_reply(
        lambda **_: None,
        model="gpt-4.1",
        timeout_seconds=5.0,
        prepared_messages=[{"role": "user", "content": "скиньте контакт"}],
        answer=answer,
        last_user_message="скиньте контакт",
        state=core.SalesState(tenant=1, contact_id=1),
    )
    assert "@dverigermes" in out
    assert "89866666133" in out


@pytest.mark.anyio
async def test_audit_persona_reply_does_not_downgrade_to_generic_clarify():
    answer = "Telegram: @dverigermes Телефон: 89866666133"
    out = await core._audit_and_rewrite_persona_reply(
        lambda **_: None,
        model="gpt-4.1",
        timeout_seconds=5.0,
        prepared_messages=[{"role": "user", "content": "оставьте контакт"}],
        answer=answer,
        last_user_message="оставьте контакт",
        state=core.SalesState(tenant=1, contact_id=2),
    )
    assert "что именно нужно" not in out.lower()


def test_extract_questions_from_text_detects_question_cues_without_qmark():
    questions = core._extract_questions_from_text("подскажите, пожалуйста адрес установки")
    assert questions == ["подскажите, пожалуйста адрес установки?"]


def test_limit_questions_drops_extra_question_sentence_with_cue():
    text = (
        "Спасибо, адрес записал. Есть ли у вас предпочтения по цвету или отделке двери? "
        "Могу сразу предложить пару популярных моделей для квартиры или отправить каталог с фото — как удобнее?"
    )

    out = core._limit_questions(text, max_questions=1)

    assert out.count("?") == 1
    assert "как удобнее" not in out.lower()


def test_wrap_llm_reply_applies_final_one_question_guard():
    from libs.core.sales_core.reply_runtime import ReplyRuntime, ReplyRuntimeDeps

    runtime = ReplyRuntime(ReplyRuntimeDeps(style_guard=""))
    out = runtime.wrap_llm_reply(
        "Спасибо, адрес записал. Есть ли предпочтения по цвету? Когда вам удобнее на замер?"
    )

    assert str(out).count("?") == 1
    assert "когда вам удобнее" not in str(out).lower()


def test_compose_reply_keeps_info_block_when_required_missing():
    state = core.SalesState(tenant=1, contact_id=10)
    plan = {
        "blocks": [
            {
                "type": "info",
                "text": "Недавно ставили дверь рядом, обычно проём 90 см.",
                "requires": ["city", "address", "object_type"],
            },
            {
                "type": "question",
                "text": "Что из каталога приглянулось?",
                "requires": [],
                "question_key": "model",
            },
        ]
    }
    reply, next_key = core._compose_reply_from_policy_blocks(
        plan,
        state=state,
        known_facts={"city": "уфа", "address": "менделеева 153", "object_type": "квартира"},
        required_facts=["city", "address", "object_type", "model"],
    )
    assert "Недавно ставили дверь рядом" in reply
    assert next_key == "model"


def test_enforce_next_required_fact_question_keeps_substantive_offer_with_required_question():
    state = core.SalesState(tenant=1, contact_id=501)
    persona = (
        "## Диалог-скрипт\n"
        "1) Узнать город.\n"
        "2) Узнать адрес.\n"
        "3) Уточнить тип помещения.\n"
        "4) Спросить, что из каталога приглянулось.\n"
    )
    known_facts = {"city": "уфа", "address": "пугачева 7", "object_type": "квартира"}
    candidate = (
        "Здравствуйте. Ранее вы интересовались входными дверьми, поэтому можем предложить скидку 2000 ₽ "
        "на любую дверь из каталога и рассрочку 50/50 без банка. "
        "Хотите посмотреть варианты и узнать подробнее об условиях?"
    )
    out, key = core._enforce_next_required_fact_question(
        candidate,
        state=state,
        persona_context=persona,
        known_facts=known_facts,
        user_text="интересуют варианты",
        grounding={},
    )
    assert "скидк" in out.lower()
    assert "рассроч" in out.lower()
    assert "вариант" in out.lower()
    assert key == "model"


def test_enforce_next_required_fact_question_does_not_replace_substantive_text_with_generic_question():
    state = core.SalesState(tenant=1, contact_id=502)
    persona = "## Диалог-скрипт\n1) Узнать город.\n2) Уточнить тип помещения.\n"
    known_facts = {}
    candidate = (
        "Под ваш запрос подходят модели с хорошей шумоизоляцией и усиленными замками, "
        "могу сориентировать по ценам и срокам."
    )
    out, key = core._enforce_next_required_fact_question(
        candidate,
        state=state,
        persona_context=persona,
        known_facts=known_facts,
        user_text="нужна тихая дверь",
        grounding={},
    )
    assert "шумоизоля" in out.lower()
    assert "цен" in out.lower()
    assert key == ""


def test_infer_user_needs_marks_insulation_and_object_type():
    needs = core.infer_user_needs("для квартиры, зимой дует и нужна тихая дверь")
    assert needs.get("object_type") == "apartment"
    assert needs.get("insulation_priority") is True
    assert needs.get("noise_priority") is True


def test_infer_user_needs_marks_insulation_for_thermal_break_phrase():
    needs = core.infer_user_needs("нужна дверь с терморазрывом")
    assert needs.get("insulation_priority") is True


def test_infer_user_needs_does_not_extract_color_from_address_turn():
    needs = core.infer_user_needs("Дубцова 76")
    assert "color" not in needs
    assert "type" not in needs
    assert "focus" not in needs
    assert "keywords" not in needs


def test_shortlist_comparison_followup_plan_answers_price_objection_without_model_reset():
    shortlist = [
        {"title": "ИЗОТЕРМА ДУБ ПАЦИФИК", "price": "37900", "Толщина полотна": "115 ММ"},
        {"title": "СЕВЕР ТЕРМО ЭКО ДУБ", "price": "40400", "Толщина полотна": "117 ММ"},
    ]
    reply, retained = core._shortlist_comparison_followup_plan(
        "А почему так дорого?",
        shortlist,
        tenant=101,
    )
    low = reply.lower()
    assert "диапазон" in low or "цена" in low
    assert "модель из каталога интересует" not in low
    assert len(retained) >= 1


def test_catalog_price_grounding_fixes_thousand_prices_per_model():
    grounding = {
        "items": [
            {"title": "ЧЕРНЫЙ КВАРЦ", "price": "33900"},
            {"title": "ЭЛИТ 100", "price": "33200"},
        ],
        "catalog_items": [
            {"title": "ЧЕРНЫЙ КВАРЦ", "price": "33900"},
            {"title": "ЭЛИТ 100", "price": "33200"},
        ],
        "model_aliases": ["ЧЕРНЫЙ КВАРЦ", "ЭЛИТ 100"],
    }
    text = "«Чёрный кварц» 38 тысяч с установкой, «Элит 100» 44 тысячи"
    out = core._enforce_catalog_price_grounding(text, grounding=grounding)
    assert "33 900 ₽" in out
    assert "33 200 ₽" in out
    assert "44 тысячи" not in out


def test_catalog_price_grounding_rewrites_unanchored_question_price_with_selected_item():
    grounding = {
        "items": [
            {"title": "ПРОТЕРМО", "price": "37400"},
            {"title": "ТЕРМО СТАНДАРТ", "price": "39900"},
        ],
        "catalog_items": [
            {"title": "ПРОТЕРМО", "price": "37400"},
            {"title": "ТЕРМО СТАНДАРТ", "price": "39900"},
        ],
        "selected_item": {"title": "ПРОТЕРМО", "price": "37400"},
    }
    text = "28 000 рублей с установкой подходит такой вариант?"
    out = core._enforce_catalog_price_grounding(text, grounding=grounding)
    assert "37 400 ₽" in out
    assert "28 000" not in out


def test_catalog_price_grounding_blocks_unknown_unanchored_price_without_catalog_match():
    grounding = {
        "items": [
            {"title": "МОДЕЛЬ A", "price": "33000"},
            {"title": "МОДЕЛЬ B", "price": "35000"},
        ],
        "catalog_items": [
            {"title": "МОДЕЛЬ A", "price": "33000"},
            {"title": "МОДЕЛЬ B", "price": "35000"},
        ],
    }
    text = "есть вариант за 28 000 рублей, подойдет?"
    out = core._enforce_catalog_price_grounding(text, grounding=grounding)
    assert "28 000" not in out
    assert "цена по каталогу" in out.lower()


def test_apply_persona_sequence_obligations_skips_infinitive_meta_instruction():
    persona = "Если в частный дом просят без терморазрыва: предупредить о риске промерзания и предложить терморазрыв."
    base = "Для частного дома лучше смотреть модели с терморазрывом."
    out = core._apply_persona_sequence_obligations(
        base,
        persona_context=persona,
        last_user_message="для частного дома без терморазрыва",
        known_facts={"object_type": "house"},
        state=core.SalesState(tenant=1, contact_id=5),
    )
    assert "предупредить" not in out.lower()
    assert out == base


def test_strip_instruction_leaks_removes_angle_placeholders():
    raw = "<город>, понял. Подскажите адрес установки."
    out = core._strip_instruction_leaks(raw)
    assert "<город>" not in out
    assert "адрес" in out.lower()


def test_strip_instruction_leaks_drops_imperative_meta_sentence():
    raw = "понял, ждёте конкретику. честно сообщайте, что в продаже только наружное открывание."
    out = core._strip_instruction_leaks(raw)
    assert "честно сообщайте" not in out.lower()


def test_items_with_attribute_uses_nonstandard_csv_columns_when_populated():
    items = [
        {"title": "Модель A", "price": "25000", "Полный терморазрыв по полотну и коробу": ""},
        {
            "title": "Модель B",
            "price": "36900",
            "Полный терморазрыв по полотну и коробу": "Полиамидный",
        },
    ]
    out = core._items_with_attribute(items, "терморазрыв")
    assert len(out) == 1
    assert str(out[0].get("title") or "") == "Модель B"


def test_items_with_attribute_semantic_fallback_matches_termo_family_without_exact_field():
    items = [
        {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
        {"title": "ПРОТЕРМО", "price": "37400"},
        {"title": "ТЕРМО МДФ/МДФ ВИНАРИТ", "price": "55900"},
        {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
    ]
    out = core._items_with_attribute(items, "терморазрыв")
    titles = [str(item.get("title") or "") for item in out]
    assert "ИЗОТЕРМА АСТАНА МИЛКИ" in titles
    assert "ПРОТЕРМО" in titles
    assert "ТЕРМО МДФ/МДФ ВИНАРИТ" in titles
    assert "ГЕРМЕС ГОСТ МЕТ/МЕТ" not in titles


def test_catalog_truth_guard_price_min_uses_semantic_termo_subset():
    grounding = {
        "items": [
            {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
            {"title": "ПРОТЕРМО", "price": "37400"},
            {"title": "ТЕРМО МДФ/МДФ ВИНАРИТ", "price": "55900"},
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
        ],
        "catalog_items": [
            {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
            {"title": "ПРОТЕРМО", "price": "37400"},
            {"title": "ТЕРМО МДФ/МДФ ВИНАРИТ", "price": "55900"},
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
        ],
        "needs": {"keywords": ["терморазрывом"]},
    }
    out = core._enforce_catalog_truth_guard(
        "Для частного дома с терморазрывом цены от 55 900 ₽",
        grounding=grounding,
        user_text="сколько стоит дверь с терморазрывом?",
    )
    assert "36 900 ₽" in out
    assert "28 000" not in out


def test_catalog_truth_guard_price_max_uses_semantic_termo_subset():
    grounding = {
        "items": [
            {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
            {"title": "ПРОТЕРМО", "price": "37400"},
            {"title": "ТЕРМО МДФ/МДФ ВИНАРИТ", "price": "55900"},
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
        ],
        "catalog_items": [
            {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
            {"title": "ПРОТЕРМО", "price": "37400"},
            {"title": "ТЕРМО МДФ/МДФ ВИНАРИТ", "price": "55900"},
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
        ],
        "needs": {"keywords": ["терморазрывом"]},
    }
    out = core._enforce_catalog_truth_guard(
        "Самая дорогая с терморазрывом — 33 900",
        grounding=grounding,
        user_text="какая самая дорогая дверь с терморазрывом?",
    )
    assert "55 900 ₽" in out
    assert "33 900" not in out


def test_catalog_truth_guard_price_min_respects_attribute_from_user_text():
    grounding = {
        "items": [
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
            {"title": "ГАРДА ЗЕРКАЛО", "price": "29500"},
            {"title": "ЧЕРНОЕ ЗЕРКАЛО", "price": "33900"},
        ],
        "catalog_items": [
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
            {"title": "ГАРДА ЗЕРКАЛО", "price": "29500"},
            {"title": "ЧЕРНОЕ ЗЕРКАЛО", "price": "33900"},
        ],
    }
    out = core._enforce_catalog_truth_guard(
        (
            "Самая доступная дверь с зеркалом для квартиры — ГЕРМЕС ГОСТ МЕТ/МДФ, "
            "цена 17 500 ₽. Могу предложить варианты зеркала на выбор."
        ),
        grounding=grounding,
        user_text="нужна самая дешевая дверь с зеркалом для квартиры",
    )
    assert "ГАРДА ЗЕРКАЛО" in out or "ЧЕРНОЕ ЗЕРКАЛО" in out
    assert "17 500" not in out


def test_two_panel_reply_rewrites_invalid_model_mentions():
    grounding = {
        "items": [
            {"title": "ЧЕРНЫЙ КВАРЦ", "price": "33900", "Толщина МДФ панели снаружи": "10 ММ"},
            {"title": "9005 МУАР", "price": "32000", "Толщина МДФ панели снаружи": "10 ММ"},
            {"title": "ЭЛИТ 100", "price": "33200"},
        ],
        "catalog_items": [
            {"title": "ЧЕРНЫЙ КВАРЦ", "price": "33900", "Толщина МДФ панели снаружи": "10 ММ"},
            {"title": "9005 МУАР", "price": "32000", "Толщина МДФ панели снаружи": "10 ММ"},
            {"title": "ЭЛИТ 100", "price": "33200"},
        ],
    }
    bad = "тогда советую двухпанельные двери: ЧЕРНЫЙ КВАРЦ или ЭЛИТ 100"
    out = core._ensure_concrete_variants_in_reply(
        bad,
        grounding=grounding,
        user_text="нужна дверь от которой не дует",
    )
    assert out == bad


def test_compile_persona_rules_extracts_delivery_rules_and_artifacts():
    persona = (
        "## Главный режим Avito\n"
        "1) Сначала ответьте по делу.\n"
        "2) Потом предлагайте продолжить в Telegram: @dverigermes, номер 89866666133.\n"
    )
    compiled = core._compile_persona_rules(persona)
    assert compiled.delivery_rules
    assert "@dverigermes" in compiled.contact_artifacts
    assert any("89866666133" in item for item in compiled.contact_artifacts)


def test_resolve_persona_rules_context_prefers_clean_tenant_persona(monkeypatch: pytest.MonkeyPatch):
    clean_persona = "Продолжим в Telegram: @dverigermes, номер 89866666133"
    noisy_system = (
        "Контекст персоны:\nПродолжим в Telegram: @dverigermes, номер 89866666133\n\n"
        "Идентификатор контакта: 7780893623\n"
        "Идентификатор лида: 988085679616094772"
    )
    monkeypatch.setattr(core, "load_persona", lambda tenant, channel: clean_persona, raising=False)
    resolved = core._resolve_persona_rules_context(
        tenant=101,
        channel_name="avito",
        fallback_context=noisy_system,
    )
    assert resolved == clean_persona


def test_extract_contact_artifacts_ignores_numeric_service_ids():
    text = (
        "Продолжим в Telegram: @dverigermes, номер 89866666133\n"
        "Идентификатор контакта: 7780893623\n"
        "Идентификатор лида: 988085679616094772"
    )
    artifacts = core._extract_contact_artifacts(text)
    assert "@dverigermes" in artifacts
    assert any("89866666133" in item for item in artifacts)
    assert not any("7780893623" in item for item in artifacts)
    assert not any("988085679616094772" in item for item in artifacts)


def test_infer_user_needs_detects_price_order_intent():
    desc = core.infer_user_needs("покажите самую дорогую модель")
    asc = core.infer_user_needs("нужна самая дешевая дверь")
    assert desc.get("price_order") == "desc"
    assert asc.get("price_order") == "asc"


def test_narrow_catalog_items_by_user_text_avoids_quartz_false_match_for_apartment():
    items = [
        {"title": "ЧЕРНЫЙ КВАРЦ", "price": "33900"},
        {"title": "LUXOR 2МДФ 3D", "price": "49500"},
        {"title": "ПРЕМИУМ ПАНОРАМА", "price": "40400"},
    ]
    narrowed = core._narrow_catalog_items_by_user_text(
        items,
        "самую дорогую дверь в квартиру покажите",
    )
    titles = [str(item.get("title") or "") for item in narrowed]
    assert "LUXOR 2МДФ 3D" in titles
    assert len(titles) > 1


def test_narrow_catalog_items_by_user_text_respects_without_attribute():
    items = [
        {"title": "ГАРДА ЗЕРКАЛО", "price": "29500"},
        {"title": "ЧЕРНОЕ ЗЕРКАЛО", "price": "33900"},
        {"title": "LUXOR 2МДФ 3D", "price": "49500", "Цвет внутренней панели": "ВЕЛЮР БЕЛЫЙ СОФТ"},
    ]
    narrowed = core._narrow_catalog_items_by_user_text(
        items,
        "нужна белая панель без зеркала, что самое дорогое есть?",
    )
    titles = [str(item.get("title") or "") for item in narrowed]
    assert "LUXOR 2МДФ 3D" in titles
    assert "ГАРДА ЗЕРКАЛО" not in titles
    assert "ЧЕРНОЕ ЗЕРКАЛО" not in titles


def test_items_with_attribute_direct_does_not_match_quartz_for_apartment_word():
    items = [
        {"title": "ЧЕРНЫЙ КВАРЦ", "price": "33900"},
        {"title": "LUXOR 2МДФ 3D", "price": "49500", "description": "для квартиры"},
    ]
    out = core._items_with_attribute_direct(items, "квартиру")
    titles = [str(item.get("title") or "") for item in out]
    assert "ЧЕРНЫЙ КВАРЦ" not in titles
    assert "LUXOR 2МДФ 3D" in titles


def test_catalog_truth_guard_price_max_for_apartment_keeps_real_max():
    grounding = {
        "items": [
            {"title": "ЧЕРНЫЙ КВАРЦ", "price": "33900"},
            {"title": "LUXOR 2МДФ 3D", "price": "49500", "description": "для квартиры"},
            {"title": "ПРЕМИУМ ПАНОРАМА", "price": "40400", "description": "для квартиры"},
        ],
        "catalog_items": [
            {"title": "ЧЕРНЫЙ КВАРЦ", "price": "33900"},
            {"title": "LUXOR 2МДФ 3D", "price": "49500", "description": "для квартиры"},
            {"title": "ПРЕМИУМ ПАНОРАМА", "price": "40400", "description": "для квартиры"},
        ],
        "needs": {"object_type": "apartment", "keywords": ["квартиру"]},
    }
    out = core._enforce_catalog_truth_guard(
        "самая дорогая для квартиры — черный кварц 33 900",
        grounding=grounding,
        user_text="самую дорогую дверь в квартиру покажите",
    )
    assert "49 500 ₽" in out
    assert "33 900" not in out


def test_filter_items_by_object_type_need_excludes_house_ready_for_apartment():
    items = [
        {"title": "VITRA ВИНАРИТ", "price": "71000", "object_type": "house", "tags": ["house_ready"]},
        {"title": "LUXOR 2МДФ 3D", "price": "49500", "tags": []},
    ]
    out = core._filter_items_by_object_type_need(items, {"object_type": "apartment"})
    titles = [str(item.get("title") or "") for item in out]
    assert "VITRA ВИНАРИТ" not in titles
    assert "LUXOR 2МДФ 3D" in titles


def test_catalog_condition_matches_contains_cross_script():
    item = {"title": "LUXOR TERMO РЕЙКА"}
    condition = {"field": "title", "contains": ["термо"]}
    assert core._catalog_condition_matches(item, condition) is True


def test_catalog_attribute_and_sales_rules_handle_latin_termo_for_apartment():
    items = [
        {"title": "LUXOR TERMO РЕЙКА", "price": "68000"},
        {"title": "LUXOR 2МДФ 3D", "price": "49500"},
    ]
    persona_meta = {
        "catalog_tags": [
            {
                "name": "thermo",
                "any": [{"field": "title", "contains": ["термо", "терма", "арктик"]}],
                "tags": ["house_ready"],
                "set": {"object_type": "house"},
            }
        ],
        "sales_rules": [
            {"needs": {"object_type": "apartment"}, "forbid_tags": ["house_ready"]},
        ],
    }
    core._apply_catalog_attribute_rules(items, persona_meta)
    out = core._filter_catalog_items_by_rules(items, {"object_type": "apartment"}, persona_meta)
    titles = [str(item.get("title") or "") for item in out]
    assert "LUXOR TERMO РЕЙКА" not in titles
    assert "LUXOR 2МДФ 3D" in titles


def test_build_reply_grounding_merges_object_type_from_facts(monkeypatch):
    state = core.SalesState(tenant=101, contact_id=9001)
    state.needs = {"keywords": ["дорогую"]}
    state.facts = {"object_type": "apartment"}

    monkeypatch.setattr(
        core,
        "_collect_grounding_items",
        lambda tenant, state, user_text: [{"title": "X", "price": "100"}],
    )
    monkeypatch.setattr(core, "_read_catalog", lambda tenant: [{"title": "X", "price": "100"}])

    out = core._build_reply_grounding(tenant=101, state=state, user_text="самую дорогую")
    assert out["needs"].get("object_type") == "apartment"


def test_build_reply_grounding_restores_object_type_from_history_when_needs_empty(monkeypatch):
    state = core.SalesState(tenant=101, contact_id=9002)
    state.needs = {}
    state.facts = {}
    state.history = [
        {"role": "user", "content": "квартира"},
        {"role": "assistant", "content": "принял"},
    ]

    monkeypatch.setattr(
        core,
        "_collect_grounding_items",
        lambda tenant, state, user_text: [{"title": "X", "price": "100"}],
    )
    monkeypatch.setattr(core, "_read_catalog", lambda tenant: [{"title": "X", "price": "100"}])

    out = core._build_reply_grounding(tenant=101, state=state, user_text="самую дорогую")
    assert out["needs"].get("object_type") == "apartment"


def test_catalog_truth_guard_price_max_respects_without_mirror():
    grounding = {
        "items": [
            {"title": "ИЗОТЕРМА ЗЕРКАЛО", "price": "41900"},
            {"title": "LUXOR 2МДФ 3D", "price": "49500", "Цвет внутренней панели": "ВЕЛЮР БЕЛЫЙ СОФТ"},
            {"title": "ПРЕМИУМ ПАНОРАМА", "price": "40400"},
        ],
        "catalog_items": [
            {"title": "ИЗОТЕРМА ЗЕРКАЛО", "price": "41900"},
            {"title": "LUXOR 2МДФ 3D", "price": "49500", "Цвет внутренней панели": "ВЕЛЮР БЕЛЫЙ СОФТ"},
            {"title": "ПРЕМИУМ ПАНОРАМА", "price": "40400"},
        ],
        "needs": {"object_type": "apartment", "keywords": ["белая", "без", "зеркала"]},
    }
    out = core._enforce_catalog_truth_guard(
        "Самая дорогая белая без зеркала — ИЗОТЕРМА ЗЕРКАЛО 41 900 ₽",
        grounding=grounding,
        user_text="нужна белая панель без зеркала, что самое дорогое есть?",
    )
    assert "49 500 ₽" in out
    assert "ЗЕРКАЛО" not in out


def test_catalog_truth_guard_does_not_reintroduce_house_items_after_apartment_filter():
    grounding = {
        "items": [
            {"title": "VITRA ВИНАРИТ ПАТИНА 13 СМ", "price": "71000", "tags": ["house_ready"], "object_type": "house"},
            {"title": "LUXOR 2МДФ 3D", "price": "49500", "Цвет внутренней панели": "ВЕЛЮР БЕЛЫЙ СОФТ"},
            {"title": "ПРЕМИУМ ПАНОРАМА", "price": "40400"},
        ],
        "catalog_items": [
            {"title": "VITRA ВИНАРИТ ПАТИНА 13 СМ", "price": "71000", "tags": ["house_ready"], "object_type": "house"},
            {"title": "LUXOR 2МДФ 3D", "price": "49500", "Цвет внутренней панели": "ВЕЛЮР БЕЛЫЙ СОФТ"},
            {"title": "ПРЕМИУМ ПАНОРАМА", "price": "40400"},
        ],
        "needs": {
            "object_type": "apartment",
            "keywords": ["белая", "панель", "без", "зеркала", "дорогое"],
            "price_order": "desc",
        },
    }
    out = core._enforce_catalog_truth_guard(
        "Самый дорогой вариант по каталогу — VITRA ВИНАРИТ ПАТИНА 13 СМ за 71 000 ₽.",
        grounding=grounding,
        user_text="нужна белая панель без зеркала, что самое дорогое есть?",
    )
    assert "LUXOR 2МДФ 3D" in out
    assert "VITRA" not in out


def test_catalog_truth_guard_turn_object_type_overrides_stale_needs_and_neutralizes_when_unknown():
    grounding = {
        "items": [
            {"title": "VITRA ВИНАРИТ ПАТИНА 13 СМ", "price": "71000"},
            {"title": "LUXOR 2МДФ 3D", "price": "49500"},
        ],
        "catalog_items": [
            {"title": "VITRA ВИНАРИТ ПАТИНА 13 СМ", "price": "71000"},
            {"title": "LUXOR 2МДФ 3D", "price": "49500"},
        ],
        # stale state from previous turn: apartment
        "needs": {"object_type": "apartment", "price_order": "desc"},
    }
    out = core._enforce_catalog_truth_guard(
        "Самый дорогой вариант по каталогу — LUXOR 2МДФ 3D за 49 500 ₽.",
        grounding=grounding,
        user_text="самая дорогая дверь в частный дом",
    )
    low = out.lower()
    assert "luxor" not in low
    assert "vitra" not in low
    assert "цена по каталогу" in low or "по каталогу" in low


def test_catalog_truth_guard_price_min_applies_even_if_model_is_already_mentioned():
    grounding = {
        "items": [
            {"title": "ТЕРМО МДФ/МДФ ВИНАРИТ", "price": "55900"},
            {"title": "ПРОТЕРМО", "price": "37400"},
            {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
        ],
        "catalog_items": [
            {"title": "ТЕРМО МДФ/МДФ ВИНАРИТ", "price": "55900"},
            {"title": "ПРОТЕРМО", "price": "37400"},
            {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
        ],
        "needs": {"keywords": ["терморазрывом"]},
    }
    out = core._enforce_catalog_truth_guard(
        "самая доступная дверь с терморазрывом — термо мдф/мдф винарит, могу уточнить",
        grounding=grounding,
        user_text="какая самая дешевая дверь с терморазрывом?",
    )
    assert "36 900 ₽" in out
    assert "55 900" not in out


def test_catalog_truth_guard_price_max_uses_current_turn_keywords_not_stale_state_keywords():
    grounding = {
        "items": [
            {
                "title": "VITRA ВИНАРИТ ПАТИНА 13 СМ",
                "price": "71000",
                "object_type": "house",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
            {
                "title": "ТЕРМО МДФ/МДФ ВИНАРИТ",
                "price": "55900",
                "object_type": "house",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
                "description": "для частного дома",
            },
        ],
        "catalog_items": [
            {
                "title": "VITRA ВИНАРИТ ПАТИНА 13 СМ",
                "price": "71000",
                "object_type": "house",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
            {
                "title": "ТЕРМО МДФ/МДФ ВИНАРИТ",
                "price": "55900",
                "object_type": "house",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
                "description": "для частного дома",
            },
        ],
        # Simulates stale conversation state keywords from previous turns.
        "needs": {"object_type": "house", "keywords": ["частного", "дома"]},
    }
    out = core._enforce_catalog_truth_guard(
        "Самый дорогой вариант по каталогу — термо мдф/мдф винарит за 55 900 ₽.",
        grounding=grounding,
        user_text="самая дорогая с терморазрывом",
    )
    assert "VITRA" in out
    assert "71 000 ₽" in out


def test_catalog_truth_guard_price_min_keeps_specific_previous_keyword_when_current_turn_generic():
    grounding = {
        "items": [
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
            {
                "title": "ТЕРМО МДФ/МДФ ВИНАРИТ",
                "price": "55900",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
            {
                "title": "ИЗОТЕРМА АСТАНА МИЛКИ",
                "price": "36900",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
        ],
        "catalog_items": [
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
            {
                "title": "ТЕРМО МДФ/МДФ ВИНАРИТ",
                "price": "55900",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
            {
                "title": "ИЗОТЕРМА АСТАНА МИЛКИ",
                "price": "36900",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
        ],
        # Previous turn context should stay active when current turn is generic.
        "needs": {"keywords": ["терморазрывом"], "object_type": "house"},
    }
    out = core._enforce_catalog_truth_guard(
        "Самая доступная — ГЕРМЕС ГОСТ МЕТ/МЕТ за 17 500 ₽.",
        grounding=grounding,
        user_text="какая самая дешевая?",
    )
    assert "17 500" not in out
    assert ("36 900 ₽" in out) or ("55 900 ₽" in out)


def test_catalog_truth_guard_house_without_evidence_neutralizes_model_and_price():
    grounding = {
        "items": [
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
            {"title": "ОПТИМА ЛАЙТ", "price": "21100"},
        ],
        "catalog_items": [
            {"title": "ГЕРМЕС ГОСТ МЕТ/МЕТ", "price": "17500"},
            {"title": "ОПТИМА ЛАЙТ", "price": "21100"},
        ],
        "needs": {"object_type": "house"},
    }
    out = core._enforce_catalog_truth_guard(
        "Для частного дома самая недорогая — ГЕРМЕС ГОСТ МЕТ/МЕТ за 17 500 ₽.",
        grounding=grounding,
        user_text="какая самая дешевая дверь для частного дома?",
    )
    low = out.lower()
    assert "гермес" not in low
    assert "17 500" not in low
    assert "цена по каталогу" in low


def test_catalog_truth_guard_price_max_for_house_not_forced_to_two_panel_subset():
    grounding = {
        "items": [
            {
                "title": "VITRA ВИНАРИТ ПАТИНА 13 СМ",
                "price": "71000",
                "object_type": "house",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
            {
                "title": "ТЕРМО МДФ/МДФ ВИНАРИТ",
                "price": "55900",
                "object_type": "house",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
        ],
        "catalog_items": [
            {
                "title": "VITRA ВИНАРИТ ПАТИНА 13 СМ",
                "price": "71000",
                "object_type": "house",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
            {
                "title": "ТЕРМО МДФ/МДФ ВИНАРИТ",
                "price": "55900",
                "object_type": "house",
                "Полный терморазрыв по полотну и коробу": "ПОЛИАМИДНЫЙ",
            },
        ],
        # insulation_priority should not force two-panel-only subset for house max-price request
        "needs": {"object_type": "house", "insulation_priority": True, "keywords": ["терморазрывом"]},
    }
    out = core._enforce_catalog_truth_guard(
        "Самый дорогой вариант по каталогу — термо мдф/мдф винарит за 55 900 ₽.",
        grounding=grounding,
        user_text="самая дорогая с терморазрывом",
    )
    assert "VITRA" in out
    assert "71 000 ₽" in out


def test_strict_catalog_item_match_does_not_accept_fuzzy_hallucinated_name():
    items = [
        {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
        {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
    ]
    assert core._strict_catalog_item_match("гарда белая", items) is None
    assert core._strict_catalog_item_match("эмалит белая", items) is None
    assert core._strict_catalog_item_match("гарда 7.5 белый ясень", items) is not None


def test_catalog_truth_guard_rewrites_unknown_quoted_model_without_model_prefix():
    grounding = {
        "items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
        ],
        "catalog_items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
        ],
        "needs": {"object_type": "apartment"},
    }
    out = core._enforce_catalog_truth_guard(
        'тогда могу предложить "Гарда Белая" с шумоизоляцией за 17 500 ₽. Подходит такой вариант?',
        grounding=grounding,
        user_text="да конечно",
    )
    assert "Гарда Белая" not in out
    assert "17 500" not in out
    assert out.strip()
    assert "подходит" in out.lower()


def test_catalog_truth_guard_does_not_rewrite_attribute_quotes_as_models():
    grounding = {
        "items": [
            {"title": "ОПТИМА ЛАЙТ БЕЛОЕ ДЕРЕВО", "price": "21100"},
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
        ],
        "catalog_items": [
            {"title": "ОПТИМА ЛАЙТ БЕЛОЕ ДЕРЕВО", "price": "21100"},
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
        ],
    }
    text = 'Из светлых есть варианты в цвете "белое дерево" и "белый ясень".'
    out = core._enforce_catalog_truth_guard(
        text,
        grounding=grounding,
        user_text="есть светлые цвета?",
    )
    assert out == text


def test_catalog_truth_guard_blocks_price_hallucination_when_catalog_unavailable():
    out = core._enforce_catalog_truth_guard(
        "Есть вариант за 28 000 ₽, могу оформить сегодня.",
        grounding={"items": [], "catalog_items": []},
        user_text="сколько стоит?",
    )
    assert "28 000" not in out
    assert "цена по каталогу" in out.lower()


def test_catalog_truth_guard_does_not_treat_phone_as_price():
    grounding = {
        "items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
        ],
        "catalog_items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
        ],
    }
    text = "Пишите в Telegram: @dveri, телефон 89866666133"
    out = core._enforce_catalog_truth_guard(
        text,
        grounding=grounding,
        user_text="оставьте контакт",
    )
    assert out == text


def test_catalog_truth_guard_keeps_generic_variant_cta_with_contact_tail():
    grounding = {
        "items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
        ],
        "catalog_items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
        ],
    }
    text = (
        "Для квартир в наличии около 45 моделей, цены стартуют от 17 500 ₽. "
        "Замер, доставка, установка и монтажные материалы — бесплатно. "
        "Удобнее будет выбрать вариант и обсудить детали в Telegram: @dverigermes."
    )
    out = core._enforce_catalog_truth_guard(
        text,
        grounding=grounding,
        user_text="для квартиры",
    )
    low = out.lower()
    assert "обсудить детали" in low
    assert "telegram: @dverigermes" in low


def test_catalog_truth_guard_variants_intent_rewrites_unknown_plain_model_names():
    grounding = {
        "items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
        ],
        "catalog_items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "33900"},
        ],
    }
    out = core._enforce_catalog_truth_guard(
        "Могу предложить гарда белая и эмалит белая, обе хорошие.",
        grounding=grounding,
        user_text="какие варианты есть",
    )
    low = out.lower()
    assert "гарда белая" not in low
    assert "эмалит белая" not in low
    assert out.strip()


def test_prefer_refined_answer_keeps_base_when_refined_is_worse():
    state = core.SalesState(tenant=101, contact_id=77)
    answer = (
        'Для частного дома советую двери с терморазрывом. '
        'Из светлых есть варианты в цвете "белый ясень" и "белое дерево".'
    )
    refined = "Для частного дома советую двери с терморазрывом. Из светлых есть варианты в цвете вариант и белое дерево."
    out = core._prefer_refined_answer(
        answer=answer,
        refined=refined,
        state=state,
        persona_hints=None,
        grounding={
            "items": [
                {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
                {"title": "ОПТИМА ЛАЙТ БЕЛОЕ ДЕРЕВО", "price": "21100"},
            ],
            "catalog_items": [
                {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
                {"title": "ОПТИМА ЛАЙТ БЕЛОЕ ДЕРЕВО", "price": "21100"},
            ],
        },
        user_text="нужна светлая в частный дом",
    )
    low = out.lower()
    assert "белый ясень" in low
    assert "вариант и белое дерево" not in low


def test_humanize_reply_text_returns_empty_for_pure_instruction_leak():
    state = core.SalesState(tenant=101, contact_id=78)
    out = core._humanize_reply_text(
        "После приветствия последовательно уточни:",
        state=state,
        persona_hints=None,
    )
    assert out == ""


def test_catalog_truth_guard_blocks_unverified_price_labels_even_when_price_exists_in_catalog():
    grounding = {
        "items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "29500"},
        ],
        "catalog_items": [
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "29500"},
        ],
    }
    out = core._enforce_catalog_truth_guard(
        "Эмалит белая — 29 500 ₽, Гарда белая — 29 500 ₽. Обе есть.",
        grounding=grounding,
        user_text="сколько стоят эмалит белая и гарда белая?",
    )
    low = out.lower()
    assert "эмалит белая" not in low
    assert "гарда белая" not in low
    assert out.strip()


def test_catalog_truth_guard_blocks_unknown_explicit_model_probe():
    grounding = {
        "items": [
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "29500"},
            {"title": "ГАРДА 8 ММ", "price": "23900"},
        ],
        "catalog_items": [
            {"title": "ЭМАЛИТ ЗЕРКАЛО 9 СМ", "price": "29500"},
            {"title": "ГАРДА 8 ММ", "price": "23900"},
        ],
    }
    out = core._enforce_catalog_truth_guard(
        "Да, есть в наличии.",
        grounding=grounding,
        user_text="гарда белая есть?",
    )
    assert out.strip()
    assert "сверю по каталогу" not in out.lower()


def test_catalog_truth_guard_drops_unknown_model_sentence_without_marker_phrase():
    grounding = {
        "items": [
            {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
            {"title": "АРКТИК КЛАССИКА", "price": "40400"},
        ],
        "catalog_items": [
            {"title": "ИЗОТЕРМА АСТАНА МИЛКИ", "price": "36900"},
            {"title": "АРКТИК КЛАССИКА", "price": "40400"},
        ],
    }
    out = core._enforce_catalog_truth_guard(
        'Вам подойдёт дверь "Аргус ДА-2" с терморазрывом. Могу отправить фото и цены?',
        grounding=grounding,
        user_text="что посоветуете?",
    )
    assert "Аргус" not in out
    assert "модель из каталога" not in out
    assert out


def test_catalog_truth_guard_does_not_replace_discount_amount_with_catalog_placeholder():
    grounding = {
        "items": [
            {"title": "ГАРДА 8 ММ", "price": "23900"},
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
        ],
        "catalog_items": [
            {"title": "ГАРДА 8 ММ", "price": "23900"},
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500"},
        ],
    }
    text = "Для Уфы действует скидка 2000 ₽ при заказе в течение недели."
    out = core._enforce_catalog_truth_guard(
        text,
        grounding=grounding,
        user_text="я из уфы, что по скидке?",
    )
    assert "2000" in out
    assert "цена по каталогу" not in out.lower()


def test_strip_instruction_leaks_rewrites_catalog_placeholder_and_drops_order_question():
    text = "С учётом скидки дверь с установкой обойдётся примерно в цена по каталогу. Оформляем заказ?"
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "цена по каталогу" not in low
    assert "оформляем заказ" not in low
    assert "уточню" in low or "точную цену" in low


def test_enforce_next_required_model_replaces_unsolicited_catalog_option_list_question():
    state = core.SalesState(tenant=101, contact_id=3)
    state.facts["city"] = "уфа"
    state.facts["address"] = "космонавтов 3"
    state.facts["object_type"] = "apartment"
    persona = (
        "## Диалог-скрипт\n"
        "1) Уточнить город\n"
        "2) Уточнить адрес\n"
        "3) Уточнить тип помещения\n"
        "4) Спросить, какая модель из каталога интересует\n"
    )
    reply = "Спасибо, адрес записал. Какой цвет или модель интересует — например, белый ясень или бетон снежный?"
    grounding = {
        "items": [
            {"title": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ", "price": "26500", "color": "БЕТОН СНЕЖНЫЙ"},
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500", "color": "БЕЛЫЙ ЯСЕНЬ"},
        ],
        "catalog_items": [
            {"title": "ГАРДА 7.5 БЕТОН СНЕЖНЫЙ", "price": "26500", "color": "БЕТОН СНЕЖНЫЙ"},
            {"title": "ГАРДА 7.5 БЕЛЫЙ ЯСЕНЬ", "price": "26500", "color": "БЕЛЫЙ ЯСЕНЬ"},
        ],
    }
    out, pending = core._enforce_next_required_fact_question(
        reply,
        state=state,
        persona_context=persona,
        known_facts=dict(state.facts),
        user_text="квартира",
        grounding=grounding,
    )
    low = out.lower()
    assert "белый ясень" not in low
    assert "бетон снежный" not in low
    assert "модел" in low or "каталог" in low
    assert pending == "model"


def test_operator_instruction_sentence_detects_delivery_meta():
    sentence = "Сначала короткий текст, затем отдельным сообщением только ссылку каталога."
    assert core._is_operator_instruction_sentence(sentence) is True
    assert core._is_operator_instruction_sentence("Вот каталог: https://disk.yandex.ru/d/test") is False


def test_response_format_instruction_sentence_detects_meta():
    assert core._is_response_format_instruction_sentence("Отвечайте развернуто.") is True
    assert core._is_response_format_instruction_sentence("Не одной строкой") is True
    assert core._is_response_format_instruction_sentence("Напишите, пожалуйста, номер телефона.") is False


def test_sequence_process_instruction_sentence_detects_meta():
    sentence = "Сначала уточняйте город, затем давайте ответ строго по географии."
    assert core._is_sequence_process_instruction_sentence(sentence) is True
    assert (
        core._is_sequence_process_instruction_sentence(
            "Сначала уточню параметры, затем предложу варианты."
        )
        is False
    )


def test_prefer_refined_answer_drops_instructional_meta_leak():
    state = core.SalesState(tenant=101, contact_id=79)
    answer = (
        "Вот полный каталог с фото моделей: https://disk.yandex.ru/d/TN2KZxBcWySYVA "
        "@dverigermes 89866666133"
    )
    refined = (
        "Вот полный каталог с фото моделей: https://disk.yandex.ru/d/TN2KZxBcWySYVA "
        "@dverigermes 89866666133. сначала короткий текст, затем отдельным сообщением только ссылку каталога."
    )
    out = core._prefer_refined_answer(
        answer=answer,
        refined=refined,
        state=state,
        persona_hints=None,
        grounding={},
        user_text="каталог можно посмотреть",
    )
    low = out.lower()
    assert "сначала короткий текст" not in low
    assert "отдельным сообщением" not in low


def test_prefer_refined_answer_rejects_unasked_eta_suffix() -> None:
    state = core.SalesState(tenant=101, contact_id=79)
    answer = "Да, могу написать в Max. Оставьте контакт для связи."
    refined = (
        "Да, могу написать в Max. Оставьте контакт для связи. "
        "Ориентир по времени: напишу интервал после подтверждения адреса."
    )
    out = core._prefer_refined_answer(
        answer=answer,
        refined=refined,
        state=state,
        persona_hints=None,
        grounding={},
        user_text="можете написать в max",
    )
    assert out == answer


def test_eta_intent_does_not_trigger_on_generic_smozhete() -> None:
    assert core._ETA_INTENT_RE.search("сможете написать в Max?") is None
    assert core._ETA_INTENT_RE.search("можете завтра утром написать?") is not None


def test_enforce_sentence_budget_clips_overlong_single_sentence() -> None:
    raw = " ".join(["очень"] * 220)
    out = core._enforce_sentence_budget(raw, max_sentences=3)
    assert len(out) <= 420
    assert out.endswith(".")


def test_strip_instruction_leaks_removes_formatting_meta_tails():
    text = (
        "Расскажу подробнее и помогу выбрать, если удобно, напишите ваш номер, "
        "чтобы я мог перезвонить, отвечайте развернуто. Не одной строкой."
    )
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "отвечайте" not in low
    assert "не одной строкой" not in low
    assert "напишите ваш номер" in low


def test_strip_instruction_leaks_keeps_business_tail_after_format_meta_with_yo() -> None:
    text = "отвечайте развёрнуто, не одной строкой работаем по каталогу и выездом"
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "отвечайте" not in low
    assert "не одной строкой" not in low
    assert "работаем по каталогу и выездом" in low


def test_strip_instruction_leaks_removes_sequence_process_meta_tail():
    text = (
        "Понял, что Telegram не подходит. Удобно получить каталог на ваш номер телефона? "
        "сначала уточняйте город, затем давайте ответ строго по географии."
    )
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "сначала уточняйте город" not in low
    assert "ответ строго по географии" not in low
    assert "удобно получить каталог" in low


def test_strip_instruction_leaks_drops_manager_imperative_discount_meta():
    text = "Для клиентов из Уфы действует скидка 2000 ₽. предложите скидку 2000 ₽ при заказе в течение недели."
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "предложите скидку" not in low
    assert "действует скидка 2000" in low


def test_strip_instruction_leaks_removes_embedded_operator_process_tail():
    text = "работаем по каталогу и выездом без адресов магазинов поздоровайтесь, скажите что для квартир в наличии около 45 моделей"
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "поздоровайтесь" not in low
    assert "скажите что" not in low
    assert "работаем по каталогу" in low


def test_strip_instruction_leaks_removes_meta_fragment_without_store_addresses():
    text = "работаем по каталогу и выездом, без адресов магазинов"
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "без адресов магазинов" not in low
    assert "работаем по каталогу и выездом" in low


def test_strip_instruction_leaks_removes_standalone_sequence_meta_phrase():
    text = "сначала короткий текст затем отдельным сообщением только ссылку каталога"
    out = core._strip_instruction_leaks(text)
    assert out.strip() == ""


def test_strip_instruction_leaks_removes_separate_message_tail_with_backtick():
    text = "в каком городе планируете установку двери работаем по каталогу и выездом отдельным сообщением `"
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "отдельным сообщением" not in low
    assert "`" not in out
    assert "в каком городе планируете установку" in low


def test_strip_instruction_leaks_drops_non_question_imperative_greeting_directive() -> None:
    text = (
        "Понял. В каком городе планируете установку? работаем по каталогу и выездом. "
        "поздоровайтесь, скажите что для квартир в наличии около 45 моделей."
    )
    out = core._strip_instruction_leaks(text)
    low = out.lower()
    assert "поздоровайтесь" not in low
    assert "скажите что" not in low
    assert "в каком городе планируете установку" in low


def test_enforce_next_required_fact_question_keeps_substantive_question_reply():
    state = core.SalesState(tenant=101, contact_id=1)
    persona = (
        "## Диалог-скрипт\n"
        "1) Уточнить город\n"
        "2) Уточнить тип помещения\n"
        "3) Уточнить адрес установки\n"
    )
    reply = (
        "Для частного дома подойдут двери с терморазрывом. "
        "Напишите, пожалуйста, адрес установки — подберу подходящие варианты и сроки."
    )
    out, pending = core._enforce_next_required_fact_question(
        reply,
        state=state,
        persona_context=persona,
        known_facts={},
        user_text="для частного дома",
        grounding={},
    )
    assert out == reply
    assert pending == ""


def test_enforce_next_required_fact_question_appends_model_step_when_reply_skips_it() -> None:
    state = core.SalesState(tenant=101, contact_id=2)
    persona = (
        "## Диалог-скрипт\n"
        "1) Уточнить город\n"
        "2) Уточнить тип объекта\n"
        "3) Уточнить адрес установки\n"
        "4) Спросить, что из каталога приглянулось - \"Что из каталога приглянулось?\"\n"
    )
    known_facts = {
        "city": "уфа",
        "object_type": "квартира",
        "address": "космонавтов 87",
    }
    reply = "Из популярных вариантов могу предложить гермес гост мет/мдф 17 500 ₽."
    out, pending = core._enforce_next_required_fact_question(
        reply,
        state=state,
        persona_context=persona,
        known_facts=known_facts,
        user_text="космонавтов 87",
        grounding={},
    )
    assert "что из каталога приглянулось" in out.lower()
    assert pending == "model"


def test_has_substantive_non_question_payload_treats_ack_stub_as_non_substantive() -> None:
    assert core._has_substantive_non_question_payload("Понял.") is False
    assert core._has_substantive_non_question_payload("Космонавтов 76, понял.") is False


def test_enforce_next_required_fact_question_does_not_keep_address_ack_stub_without_object_type() -> None:
    state = core.SalesState(tenant=101, contact_id=778)
    persona = (
        "## Диалог-скрипт\n"
        "1) Уточнить город\n"
        "2) Уточнить адрес установки\n"
        "3) Уточнить тип объекта (квартира или частный дом)\n"
    )
    known_facts = {
        "city": "уфа",
        "address": "космонавтов 76",
    }
    reply = "Космонавтов 76, понял."
    out, pending = core._enforce_next_required_fact_question(
        reply,
        state=state,
        persona_context=persona,
        known_facts=known_facts,
        user_text="космонавтов 76",
        grounding={},
    )
    out_low = out.lower()
    assert "квартир" in out_low or "частн" in out_low
    assert pending == "object_type"


def test_stabilize_followup_price_reference_keeps_previous_anchor() -> None:
    state = core.SalesState(tenant=101, contact_id=3)
    state.last_bot_reply = "Да, всё верно: с учётом скидки будет 24 500 ₽."
    out = core._stabilize_followup_price_reference(
        "Да, это финальная цена 23 900 ₽ с установкой.",
        state=state,
        user_text="это цена с установкой или ещё не окончательная?",
        grounding={"items": []},
    )
    assert "24 500" in out
    assert "23 900" not in out


def test_normalize_shouting_case_softens_long_uppercase_words() -> None:
    out = core._normalize_shouting_case("Ещё могу предложить вариант ГАРДА 7,5 БЕТОН СНЕЖНЫЙ и ПВХ панель.")
    assert "гарда" in out
    assert "ПВХ" in out


def test_catalog_truth_guard_does_not_pick_extreme_by_object_without_object_evidence():
    grounding = {
        "items": [
            {"title": "МОДЕЛЬ A", "price": "30000"},
            {"title": "МОДЕЛЬ B", "price": "60000"},
        ],
        "catalog_items": [
            {"title": "МОДЕЛЬ A", "price": "30000"},
            {"title": "МОДЕЛЬ B", "price": "60000"},
        ],
        "needs": {"object_type": "apartment"},
    }
    out = core._enforce_catalog_truth_guard(
        "Самый дорогой вариант по каталогу — МОДЕЛЬ B за 60 000 ₽.",
        grounding=grounding,
        user_text="какая самая дорогая для квартиры?",
    )
    assert out.strip()
    assert "сверю по каталогу" not in out.lower()


def test_extract_contact_artifacts_normalizes_duplicate_urls_with_trailing_quote():
    text = (
        "Ссылка: https://disk.yandex.ru/d/TN2KZxBcWySYVA\n"
        "Ссылка дубль: https://disk.yandex.ru/d/TN2KZxBcWySYVA\""
    )
    artifacts = core._extract_contact_artifacts(text)
    assert artifacts.count("https://disk.yandex.ru/d/TN2KZxBcWySYVA") == 1
    assert not any(item.endswith('"') for item in artifacts)


def test_apply_persona_delivery_obligations_appends_contacts():
    persona = (
        "## Главный режим Avito\n"
        "2) Потом предлагайте продолжить в Telegram: @dverigermes, номер 89866666133.\n"
    )
    state = core.SalesState(tenant=101, contact_id=77)
    base = "Да, такая модель есть"
    out = core._apply_persona_delivery_obligations(
        base,
        persona_context=persona,
        channel_name="avito",
        last_user_message="Сколько стоит?",
        known_facts={},
        state=state,
    )
    assert "@dverigermes" in out
    assert "89866666133" in out
    assert out.startswith(base)


def test_apply_persona_delivery_obligations_no_continue_phrase_inside_telegram():
    persona = (
        "В Telegram отправляйте ссылку на Яндекс.Диск: "
        "https://disk.yandex.ru/d/TN2KZxBcWySYVA\n"
    )
    state = core.SalesState(tenant=101, contact_id=80)
    out = core._apply_persona_delivery_obligations(
        "Отправил PDF-каталог",
        persona_context=persona,
        channel_name="telegram",
        last_user_message="каталог",
        known_facts={},
        state=state,
    )
    assert "продолжим в telegram" not in out.lower()
    assert "https://disk.yandex.ru/d/TN2KZxBcWySYVA" in out


def test_apply_persona_delivery_obligations_does_not_spam_each_turn():
    persona = (
        "## Главный режим Avito\n"
        "2) Потом предлагайте продолжить в Telegram: @dverigermes, номер 89866666133.\n"
    )
    state = core.SalesState(tenant=101, contact_id=78)
    state.history = [
        {"role": "assistant", "content": "Если удобно, продолжим в Telegram\n@dverigermes\n89866666133"}
    ]
    base = "Подберу варианты под ваш бюджет"
    out = core._apply_persona_delivery_obligations(
        base,
        persona_context=persona,
        channel_name="avito",
        last_user_message="Ок",
        known_facts={},
        state=state,
    )
    assert out == base


def test_apply_persona_delivery_obligations_respects_conditional_rule():
    persona = (
        "Если клиент просит каталог: отправляйте ссылку https://disk.yandex.ru/d/example\n"
    )
    state = core.SalesState(tenant=101, contact_id=79)
    base = "Сейчас подскажу"
    out_positive = core._apply_persona_delivery_obligations(
        base,
        persona_context=persona,
        channel_name="avito",
        last_user_message="Скиньте каталог",
        known_facts={},
        state=state,
    )
    out_negative = core._apply_persona_delivery_obligations(
        base,
        persona_context=persona,
        channel_name="avito",
        last_user_message="Здравствуйте",
        known_facts={},
        state=state,
    )
    assert "https://disk.yandex.ru/d/example" in out_positive
    assert out_negative == base


def test_delivery_rule_from_telegram_catalog_line_prefers_link_not_handle():
    rule = core._delivery_rule_from_line(
        source_line="Если в Telegram отправляете PDF-каталог, добавляйте ссылку на Яндекс.Диск",
        channel_scope=[],
    )
    assert rule.wants_link is True
    assert rule.wants_handle is False
