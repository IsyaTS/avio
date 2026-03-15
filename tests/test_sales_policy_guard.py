import sys
from pathlib import Path

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
    assert "давайте вернемся" in low


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
        "уфа",
        channel="avito",
        tenant=101,
    )
    assert str(state.facts.get("city") or "").lower() == "уфа"


def test_fallback_plan_does_not_emit_technical_phrase():
    plan = core._fallback_semantic_plan("гарда зеркало")
    blocks = plan.get("blocks") or []
    texts = " ".join(str(item.get("text") or "") for item in blocks if isinstance(item, dict))
    assert "Отвечаю по запросу" not in texts
    assert plan.get("question_slot") == "model"


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
    assert out.lower().startswith("понял.")
    assert "уфа, понял" not in out.lower()


def test_unsubscribe_intent_helper():
    assert core._is_unsubscribe_intent("не пишите мне больше")
    assert core._is_unsubscribe_intent("стоп")
    assert not core._is_unsubscribe_intent("подскажите цену")


def test_ensure_concrete_variants_in_reply_appends_catalog_items_when_promised():
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
    assert "ГАРДА ЗЕРКАЛО" in out
    assert "ЭМАЛИТ ЗЕРКАЛО 9СМ" in out


def test_ensure_concrete_variants_in_reply_does_not_duplicate_when_model_already_present():
    grounding = {"items": [{"title": "ГАРДА ЗЕРКАЛО", "price": "29500"}]}
    reply = "Могу предложить ГАРДА ЗЕРКАЛО — 29 500 ₽"
    out = core._ensure_concrete_variants_in_reply(
        reply,
        grounding=grounding,
        user_text="подберите вариант",
    )
    assert out == reply


def test_rewrite_loses_context_anchors_detects_loss():
    candidate = "На Гоголя 31 ставили недавно, проём обычно 90 см. Что из каталога приглянулось?"
    rewrite = "Что из каталога приглянулось, уже что-то выбрали или подсказать варианты?"
    dialogue = [{"role": "user", "content": "гоголя 33"}]
    assert core._rewrite_loses_context_anchors(candidate, rewrite, dialogue)


def test_extract_questions_from_text_detects_question_cues_without_qmark():
    questions = core._extract_questions_from_text("подскажите, пожалуйста адрес установки")
    assert questions == ["подскажите, пожалуйста адрес установки?"]


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


def test_infer_user_needs_marks_insulation_and_object_type():
    needs = core.infer_user_needs("для квартиры, зимой дует и нужна тихая дверь")
    assert needs.get("object_type") == "apartment"
    assert needs.get("insulation_priority") is True
    assert needs.get("noise_priority") is True


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
    assert "ЭЛИТ 100" not in out
    assert "двухпанельные варианты" in out.lower()


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
