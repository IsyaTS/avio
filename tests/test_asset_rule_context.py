from __future__ import annotations

import pytest

from libs.core.services.asset_rule_context import build_asset_rule_context


pytestmark = pytest.mark.unit


def test_asset_rule_context_uses_avito_item_city_and_product_text() -> None:
    context = build_asset_rule_context(
        tenant_id=101,
        lead_id=1,
        channel="avito",
        user_text="Здравствуйте, нужна дверь с зеркалом",
        avito_item_city="Казань",
    )

    assert context["slots"]["city"] == "Казань"
    assert context["slots"]["product"] == "дверь с зеркалом"
    assert context["intent"] in {"catalog_request", "general"}


def test_asset_rule_context_respects_without_mirror() -> None:
    context = build_asset_rule_context(
        tenant_id=101,
        channel="avito",
        user_text="интересуют квартирные двери без зеркала",
        avito_item_city="Уфа",
    )

    assert context["slots"]["product"] == "квартирная дверь без зеркала"


def test_asset_rule_context_extracts_city_written_by_client() -> None:
    context = build_asset_rule_context(
        tenant_id=101,
        channel="avito",
        user_text="Екатеринбург",
        known_facts={"account_label": "Гермес"},
    )

    assert context["slots"]["city"] == "Екатеринбург"
    assert context["slots"]["account_brand"] == "germes"


def test_asset_rule_context_does_not_call_ai_or_mutate_state() -> None:
    facts = {"city": "Уфа"}
    context = build_asset_rule_context(
        tenant_id=1,
        channel="telegram",
        user_text="нужна дверь",
        known_facts=facts,
    )

    assert facts == {"city": "Уфа"}
    assert context["slots"]["city"] == "Уфа"


def test_asset_rule_context_prefers_avito_item_city_over_text_city() -> None:
    context = build_asset_rule_context(
        tenant_id=101,
        channel="avito",
        user_text="Здравствуйте, я из Москвы, каталог можно?",
        known_facts={"account_label": "Гермес"},
        avito_item_city="Нефтекамск",
    )

    assert context["slots"]["city"] == "Нефтекамск"
    assert context["slots"]["account_brand"] == "germes"
