from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.core.services.avito_contextual_case_builder import build_contextual_case_candidates
from libs.core.services.avito_contextual_case_policy import classify_cases
from libs.core.services.avito_dialog_filter import AvitoDialogMessage
from libs.core.services.avito_domain_context_extractor import extract_context


pytestmark = pytest.mark.unit


def _m(role: str, text: str) -> AvitoDialogMessage:
    return AvitoDialogMessage(role=role, text=text)


def _candidate(client_text: str, manager_reply: str):
    result = build_contextual_case_candidates(
        [[_m("client", client_text), _m("manager", manager_reply)]],
        tenant_id=101,
        created_at=datetime(2026, 5, 25, tzinfo=timezone.utc),
    )
    assert result.candidates
    return result.candidates[0]


def _schema(**overrides):
    base = {
        "domain": "lawn_mowing",
        "domain_label": "покос травы",
        "required_slots": ["area_size", "grass_height", "location"],
        "optional_slots": ["waste_removal", "urgency", "access"],
        "slot_definitions": {
            "area_size": "площадь участка",
            "grass_height": "высота травы",
            "location": "адрес или район",
            "waste_removal": "вывоз травы",
            "urgency": "срочность",
            "access": "доступ к участку",
        },
        "price_depends_on": ["area_size", "grass_height", "location"],
        "location_depends_on": ["location"],
        "service_depends_on": ["grass_height", "access"],
        "availability_depends_on": ["location", "urgency"],
    }
    base.update(overrides)
    return base


def _classified(client_text: str, manager_reply: str, *, domain_schema: dict | None = None):
    schema = domain_schema or _schema()
    candidate = _candidate(client_text, manager_reply)
    rule_data = extract_context(candidate, domain_schema=schema)
    return classify_cases([candidate], rule_extractions={candidate.case_id: rule_data}, domain_schema=schema)


def _first(result):
    return (result.contextual_cases or result.review_cases)[0]


def test_lawn_mowing_price_requires_domain_slots() -> None:
    result = _classified("Сколько стоит покосить траву?", "Стоимость зависит от площади, высоты травы и района")

    case = result.contextual_cases[0]
    assert case["applicability"]["mode"] == "context_bound"
    assert {"slots.area_size", "slots.grass_height", "slots.location"}.issubset(
        set(case["applicability"]["requires"])
    )


def test_lawn_mowing_service_area_requires_location() -> None:
    result = _classified("Выезжаете за город?", "Работаем по району, адрес уточните")

    case = _first(result)
    assert "slots.location" in case["applicability"]["requires"]


def test_cleaning_price_requires_schema_slots() -> None:
    cleaning_schema = _schema(
        domain="cleaning",
        domain_label="клининг",
        required_slots=["area_size", "cleaning_type"],
        optional_slots=["location"],
        slot_definitions={
            "area_size": "площадь помещения",
            "cleaning_type": "тип уборки",
            "location": "адрес или район",
        },
        price_depends_on=["area_size", "cleaning_type"],
        location_depends_on=["location"],
        service_depends_on=["cleaning_type"],
    )

    result = _classified(
        "Сколько стоит уборка?",
        "Цена зависит от площади и типа уборки",
        domain_schema=cleaning_schema,
    )

    requires = set(result.contextual_cases[0]["applicability"]["requires"])
    assert {"slots.area_size", "slots.cleaning_type"}.issubset(requires)


def test_door_address_requires_client_city_slot() -> None:
    door_schema = _schema(
        domain="entrance_doors",
        domain_label="входные двери",
        required_slots=["client_city", "door_type"],
        optional_slots=["premise_type"],
        slot_definitions={"client_city": "город клиента", "door_type": "тип двери"},
        price_depends_on=["door_type"],
        location_depends_on=["client_city"],
        service_depends_on=["door_type"],
    )

    result = _classified("Я из Уфы, где посмотреть двери?", "В Уфе магазин на Менделеева 80", domain_schema=door_schema)

    case = result.contextual_cases[0]
    assert case["applicability"]["mode"] == "context_bound"
    assert "slots.client_city" in case["applicability"]["requires"]
    assert case["applicability"]["same_city_required"] is True


def test_context_bound_without_requirements_goes_to_review() -> None:
    schema = _schema(required_slots=[], price_depends_on=[], location_depends_on=[], service_depends_on=[])
    candidate = _candidate("Сколько стоит?", "Стоимость обсуждается отдельно")
    rule_data = {
        "context": {"intent": "price_question", "domain": "generic_sales", "slots": {}, "missing_slots": []},
        "reply_facts": {"mentions_price": True},
        "applicability": {"mode": "context_bound", "requires": []},
    }

    result = classify_cases([candidate], rule_extractions={candidate.case_id: rule_data}, domain_schema=schema)

    assert result.contextual_cases == []
    assert result.review_cases[0]["quality"]["reason_code"] == "context_bound_missing_requirements"


def test_direct_example_with_conditional_facts_becomes_context_bound() -> None:
    result = _classified("Сколько стоит покосить?", "Цена зависит от площади участка")

    assert result.contextual_cases[0]["applicability"]["mode"] == "context_bound"


def test_short_clarifier_remains_clarify_first() -> None:
    result = _classified("Сколько стоит покос?", "Сколько соток?")

    assert result.review_cases == []
    assert result.contextual_cases[0]["applicability"]["mode"] in {"direct_example", "clarify_first"}


def test_repeated_manager_phrasing_not_rejected() -> None:
    result = _classified("Нужно покосить участок", "Здравствуйте, подскажите площадь участка?")

    assert result.reject_reasons == {}
    assert result.contextual_cases
