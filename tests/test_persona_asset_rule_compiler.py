from __future__ import annotations

import pytest

from libs.core.services.persona_asset_rule_compiler import build_persona_asset_rules


pytestmark = pytest.mark.unit


PERSONA_TEXT = """
отправляй фото исходя от аккаунта на который написали: Гермес или Айдар.
Фото где написано Уфа отправляй только в перечисленные города: Уфа, Нефтекамск, Оренбург.
Во всех остальных населенных пунктах нужно отправлять фотографии где написано Казань.
Город узнавай по городу в объявление на которое написали.
"""


def test_build_persona_asset_rules_routes_by_account_and_ufa_city_group() -> None:
    rules = build_persona_asset_rules(
        tenant_id=101,
        persona_text=PERSONA_TEXT,
        assets=[
            {
                "tenant_id": 101,
                "asset_id": "photo-ufa",
                "asset_type": "photo",
                "title": "квартирные двери без зеркала гермес уфа",
                "status": "active",
            }
        ],
    )

    assert len(rules) == 1
    assert rules[0]["source"] == "persona"
    assert rules[0]["status"] == "active"
    assert rules[0]["needs_review"] is False
    assert rules[0]["conditions"]["all"][0] == {
        "slot": "account_brand",
        "operator": "equals",
        "value": "germes",
    }
    assert rules[0]["conditions"]["all"][1]["operator"] == "in"
    assert "нефтекамск" in rules[0]["conditions"]["all"][1]["value"]


def test_build_persona_asset_rules_routes_kazan_assets_to_other_cities() -> None:
    rules = build_persona_asset_rules(
        tenant_id=101,
        persona_text=PERSONA_TEXT,
        assets=[
            {
                "tenant_id": 101,
                "asset_id": "photo-kazan",
                "asset_type": "photo",
                "title": "квартирные двери с зеркалом айдар казань",
                "status": "active",
            }
        ],
    )

    assert len(rules) == 1
    assert rules[0]["conditions"]["all"][0]["value"] == "aidar"
    assert rules[0]["conditions"]["all"][1]["operator"] == "not_in"


def test_build_persona_asset_rules_ignores_assets_without_account_or_region() -> None:
    rules = build_persona_asset_rules(
        tenant_id=101,
        persona_text=PERSONA_TEXT,
        assets=[
            {
                "tenant_id": 101,
                "asset_id": "photo-common",
                "asset_type": "photo",
                "title": "общий каталог",
                "status": "active",
            }
        ],
    )

    assert rules == []
