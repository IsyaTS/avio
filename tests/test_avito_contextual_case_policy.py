from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.core.services.avito_context_extractor import extract_context
from libs.core.services.avito_contextual_case_builder import (
    AvitoContextualCaseCandidate,
    AvitoContextualMessage,
    build_contextual_case_candidates,
)
from libs.core.services.avito_contextual_case_policy import classify_cases
from libs.core.services.avito_dialog_filter import AvitoDialogMessage


pytestmark = pytest.mark.unit


def _m(role: str, text: str) -> AvitoDialogMessage:
    return AvitoDialogMessage(role=role, text=text)


def _candidate(client_text: str, manager_reply: str) -> AvitoContextualCaseCandidate:
    result = build_contextual_case_candidates(
        [[_m("client", client_text), _m("manager", manager_reply)]],
        tenant_id=101,
        created_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
    )
    assert result.candidates
    return result.candidates[0]


def _classified(candidate: AvitoContextualCaseCandidate, *, ai_data: dict | None = None):
    rule_data = extract_context(candidate)
    return classify_cases(
        [candidate],
        rule_extractions={candidate.case_id: rule_data},
        ai_extractions={candidate.case_id: ai_data} if ai_data else None,
    )


def _first_case(result):
    return (result.contextual_cases or result.review_cases)[0]


def test_address_with_known_same_city_is_context_bound() -> None:
    result = _classified(_candidate("Я из Уфы, где посмотреть двери?", "В Уфе магазин на Менделеева 80"))

    case = result.contextual_cases[0]
    assert case["applicability"]["mode"] == "context_bound"
    assert case["applicability"]["same_city_required"] is True
    assert "slots.client_city" in case["applicability"]["requires"]


def test_address_without_client_city_is_never_direct() -> None:
    result = _classified(_candidate("Где находится магазин?", "В Уфе магазин на Менделеева 80"))

    assert result.contextual_cases == []
    case = result.review_cases[0]
    assert case["applicability"]["mode"] == "review"
    assert case["quality"]["reason_code"] == "address_without_client_city"


def test_unknown_city_location_question_with_city_clarifier_is_clarify_first() -> None:
    result = _classified(_candidate("Где находитесь?", "А вы в каком городе проживаете?"))

    case = result.contextual_cases[0]
    assert case["applicability"]["mode"] == "clarify_first"
    assert case["quality"]["reason_code"] == "clarify_city"


def test_universal_delivery_answer_is_direct_example() -> None:
    result = _classified(_candidate("Доставка и установка платная?", "Доставка и установка бесплатная"))

    case = result.contextual_cases[0]
    assert case["applicability"]["mode"] == "direct_example"
    assert case["quality"]["reason_code"] == "useful_conditions_answer"


def test_price_specific_answer_is_context_bound() -> None:
    result = _classified(_candidate("Сколько стоит дверь?", "Стоимость от 35000 рублей"))

    case = result.contextual_cases[0]
    assert case["applicability"]["mode"] == "context_bound"
    assert "slots.product_type" in case["applicability"]["requires"]
    assert "slots.product_type" in case["applicability"]["do_not_use_directly_without"]
    assert case["applicability"]["same_product_required"] is True
    assert case["reply_facts"]["mentions_price"] is True


def test_ai_context_bound_with_conditional_facts_gets_explicit_requirements() -> None:
    candidate = _candidate("Доставка и установка платная?", "Доставка и установка бесплатная")
    ai_data = {
        "context": {"intent": "delivery_installation", "missing_facts": []},
        "reply_facts": {"mentions_delivery": True, "mentions_installation": True},
        "applicability": {"mode": "context_bound", "requires": []},
        "quality": {"confidence": 0.9, "reason_code": "ai_context_bound"},
    }

    result = _classified(candidate, ai_data=ai_data)
    case = result.contextual_cases[0]

    assert case["applicability"]["mode"] == "context_bound"
    assert case["applicability"]["requires"]
    assert "slots.client_city" in case["applicability"]["requires"]
    assert "slots.product_type" in case["applicability"]["requires"]


def test_contact_only_reply_is_rejected() -> None:
    candidate = AvitoContextualCaseCandidate(
        source="avito",
        tenant_id=101,
        dialog_id="dialog",
        case_id="case",
        turn_index=1,
        channel="avito",
        history=[AvitoContextualMessage(role="client", text="Здравствуйте, нужна дверь")],
        manager_reply=AvitoContextualMessage(role="manager", text="[PHONE]"),
        created_at="2026-05-22T00:00:00Z",
    )

    result = _classified(candidate)

    assert result.contextual_cases == []
    assert result.review_cases == []
    assert result.reject_reasons["contact_only_reply"] == 1


def test_messenger_transfer_only_goes_to_review() -> None:
    result = _classified(_candidate("Можно каталог?", "Отправили по ватсап"))

    assert result.contextual_cases == []
    assert result.review_cases[0]["applicability"]["mode"] == "review"
    assert result.review_cases[0]["quality"]["reason_code"] == "messenger_transfer_only"


def test_short_useful_clarifier_not_rejected() -> None:
    result = _classified(_candidate("Нужна дверь в дом", "Размер?"))

    assert result.review_cases == []
    assert result.contextual_cases[0]["applicability"]["mode"] in {"direct_example", "clarify_first"}


def test_repeated_manager_phrasing_not_rejected_by_itself() -> None:
    candidate = _candidate("Нужна входная дверь", "Здравствуйте, подскажите размер проема?")
    result = _classified(candidate)

    assert result.review_cases == []
    assert result.contextual_cases
    assert result.reject_reasons == {}


def test_ai_mode_is_honored_but_policy_still_blocks_unsafe_direct_address() -> None:
    candidate = _candidate("Где посмотреть двери?", "В Уфе магазин на Менделеева 80")
    ai_data = {
        "context": {"intent": "store_location", "client_city": None, "business_city": "Уфа", "missing_facts": []},
        "reply_facts": {"mentions_address": True, "city_specific": True},
        "applicability": {"mode": "direct_example"},
        "quality": {"confidence": 0.95, "reason_code": "ai_direct"},
    }

    result = _classified(candidate, ai_data=ai_data)
    case = _first_case(result)

    assert result.contextual_cases == []
    assert case["applicability"]["mode"] == "review"
    assert case["quality"]["extractor"] == "ai_gpt_5_2"
    assert case["quality"]["reason_code"] == "address_without_client_city"
