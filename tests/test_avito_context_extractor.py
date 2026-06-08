from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.core.services.avito_context_extractor import extract_context
from libs.core.services.avito_contextual_case_builder import build_contextual_case_candidates
from libs.core.services.avito_dialog_filter import AvitoDialogMessage


pytestmark = pytest.mark.unit


def _m(role: str, text: str) -> AvitoDialogMessage:
    return AvitoDialogMessage(role=role, text=text)


def _candidate(client_text: str, manager_reply: str):
    result = build_contextual_case_candidates(
        [[_m("client", client_text), _m("manager", manager_reply)]],
        tenant_id=101,
        created_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
    )
    assert result.candidates
    return result.candidates[0]


def test_store_location_intent_and_city_extraction() -> None:
    data = extract_context(_candidate("Здравствуйте, я из Уфы, где посмотреть двери?", "Менделеева 80"))

    assert data["context"]["intent"] == "store_location"
    assert data["context"]["client_city"] == "Уфа"
    assert data["context"]["business_city"] is None
    assert data["reply_facts"]["mentions_address"] is True
    assert data["context"]["missing_facts"] == []


def test_store_location_missing_city_detected() -> None:
    data = extract_context(_candidate("Здравствуйте, где находится магазин?", "В Уфе магазин на Менделеева 80"))

    assert data["context"]["intent"] == "store_location"
    assert data["context"]["client_city"] is None
    assert data["context"]["business_city"] == "Уфа"
    assert "client_city" in data["context"]["missing_facts"]


def test_price_intent_and_reply_facts() -> None:
    data = extract_context(_candidate("Сколько стоит входная дверь?", "Стоимость от 35000 рублей"))

    assert data["context"]["intent"] == "price_question"
    assert data["context"]["product_type"] == "входная дверь"
    assert data["reply_facts"]["mentions_price"] is True
    assert data["reply_facts"]["price_specific"] is True


def test_delivery_installation_intent() -> None:
    data = extract_context(_candidate("В район выезжаете на установку?", "Доставка и установка бесплатная"))

    assert data["context"]["intent"] == "delivery_installation"
    assert data["reply_facts"]["mentions_delivery"] is True
    assert data["reply_facts"]["mentions_installation"] is True


def test_catalog_request_intent_and_contact_facts() -> None:
    data = extract_context(_candidate("Можно каталог дверей?", "Отправим каталог в ватсап"))

    assert data["context"]["intent"] == "catalog_request"
    assert data["reply_facts"]["mentions_contact"] is True


def test_product_type_and_premise_type_extraction() -> None:
    data = extract_context(
        _candidate("Нужна дверь с терморазрывом и зеркалом в частный дом", "Подскажите размер проема?")
    )

    assert data["context"]["product_type"] == "дверь с терморазрывом"
    assert data["context"]["premise_type"] == "частный дом"
    assert "product_type" in data["context"]["known_facts"]
    assert "premise_type" in data["context"]["known_facts"]
