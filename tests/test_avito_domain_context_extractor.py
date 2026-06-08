from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.core.services.avito_contextual_case_builder import build_contextual_case_candidates
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


def _lawn_schema() -> dict:
    return {
        "domain": "lawn_mowing",
        "domain_label": "покос травы",
        "required_slots": ["area_size", "grass_height", "location"],
        "optional_slots": ["waste_removal"],
        "slot_definitions": {
            "area_size": "площадь участка",
            "grass_height": "высота травы",
            "location": "адрес или район",
            "waste_removal": "вывоз травы",
        },
        "price_depends_on": ["area_size", "grass_height", "location"],
        "location_depends_on": ["location"],
        "service_depends_on": ["grass_height"],
        "availability_depends_on": ["location"],
    }


def test_lawn_mowing_schema_extracts_known_slots() -> None:
    data = extract_context(
        _candidate("Нужно покосить 10 соток, трава по пояс, участок в Казани", "Цена зависит от высоты травы"),
        domain_schema=_lawn_schema(),
    )

    assert data["context"]["domain"] == "lawn_mowing"
    assert data["context"]["slots"]["area_size"]
    assert data["context"]["slots"]["grass_height"]
    assert data["context"]["slots"]["location"] == "Казань"
    assert {"area_size", "grass_height", "location"}.issubset(set(data["context"]["known_slots"]))


def test_price_reply_facts_include_price_specific() -> None:
    data = extract_context(
        _candidate("Сколько стоит покос?", "Стоимость зависит от площади участка"),
        domain_schema=_lawn_schema(),
    )

    assert data["reply_facts"]["mentions_price"] is True
    assert data["reply_facts"]["price_specific"] is True


def test_location_and_service_area_facts() -> None:
    data = extract_context(
        _candidate("Выезжаете за город?", "По району работаем, адрес уточните"),
        domain_schema=_lawn_schema(),
    )

    assert data["reply_facts"]["mentions_location"] is True
    assert data["reply_facts"]["mentions_service_area"] is True


def test_legacy_door_fields_still_present() -> None:
    data = extract_context(_candidate("Нужна входная дверь в частный дом", "Подскажите размер проема"))

    assert data["context"]["product_type"] == "входная дверь"
    assert data["context"]["premise_type"] == "частный дом"
    assert "product_type" in data["context"]["known_facts"]
    assert "premise_type" in data["context"]["known_facts"]
    assert "slots" in data["context"]
    assert "known_slots" in data["context"]
    assert "missing_slots" in data["context"]


def test_missing_slots_derived_from_required_slots() -> None:
    data = extract_context(
        _candidate("Сколько стоит покосить траву?", "Стоимость зависит от площади и высоты травы"),
        domain_schema=_lawn_schema(),
    )

    assert "area_size" in data["context"]["missing_slots"]
    assert "grass_height" in data["context"]["missing_slots"]
    assert "location" in data["context"]["missing_slots"]


def test_no_raw_metadata_fields() -> None:
    data = extract_context(_candidate("Здравствуйте", "Здравствуйте"), domain_schema=_lawn_schema())

    assert "chat_id" not in data["context"]
    assert "item_id" not in data["context"]
    assert "account_id" not in data["context"]
