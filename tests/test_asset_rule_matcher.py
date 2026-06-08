from __future__ import annotations

import pytest

from libs.core.services.asset_rule_matcher import match_asset_rules


pytestmark = pytest.mark.unit


def _rule(**patch):
    base = {
        "rule_id": "r1",
        "asset_id": "a1",
        "status": "active",
        "needs_review": False,
        "conditions": {
            "all": [
                {"slot": "city", "operator": "equals", "value": "Казань"},
                {"slot": "product", "operator": "contains", "value": "дверь с зеркалом"},
            ]
        },
        "guards": {"requires_known_slots": ["city", "product"], "allowed_channels": ["avito"]},
        "action": {"type": "send_asset", "asset_id": "a1", "asset_type": "photo"},
    }
    base.update(patch)
    return base


def test_match_asset_rules_requires_exact_city_and_product() -> None:
    result = match_asset_rules(
        [_rule()],
        {"channel": "avito", "slots": {"city": "Казань", "product": "дверь с зеркалом"}},
    )

    assert len(result.matched_actions) == 1


def test_match_asset_rules_blocks_wrong_city() -> None:
    result = match_asset_rules(
        [_rule()],
        {"channel": "avito", "slots": {"city": "Уфа", "product": "дверь с зеркалом"}},
    )

    assert not result.matched_actions
    assert result.blocked_actions[0]["reason"] == "condition_not_matched"


def test_match_asset_rules_blocks_missing_required_slot() -> None:
    result = match_asset_rules([_rule()], {"channel": "avito", "slots": {"product": "дверь с зеркалом"}})

    assert not result.matched_actions
    assert result.missing_slots == ["city"]


def test_match_asset_rules_ignores_needs_review() -> None:
    result = match_asset_rules(
        [_rule(needs_review=True, status="needs_review")],
        {"channel": "avito", "slots": {"city": "Казань", "product": "дверь с зеркалом"}},
    )

    assert not result.matched_actions


def test_match_asset_rules_supports_city_group_and_account_brand() -> None:
    rule = _rule(
        conditions={
            "all": [
                {"slot": "account_brand", "operator": "equals", "value": "germes"},
                {"slot": "city", "operator": "in", "value": ["Уфа", "Нефтекамск"]},
            ]
        },
        guards={"requires_known_slots": ["account_brand", "city"], "allowed_channels": ["avito"]},
    )

    result = match_asset_rules(
        [rule],
        {"channel": "avito", "slots": {"account_brand": "germes", "city": "Нефтекамск"}},
    )

    assert len(result.matched_actions) == 1


def test_match_asset_rules_supports_kazan_fallback_not_in_group() -> None:
    rule = _rule(
        conditions={
            "all": [
                {"slot": "account_brand", "operator": "equals", "value": "germes"},
                {"slot": "city", "operator": "not_in", "value": ["Уфа", "Нефтекамск"]},
            ]
        },
        guards={"requires_known_slots": ["account_brand", "city"], "allowed_channels": ["avito"]},
    )

    matched = match_asset_rules(
        [rule],
        {"channel": "avito", "slots": {"account_brand": "germes", "city": "Москва"}},
    )
    blocked = match_asset_rules(
        [rule],
        {"channel": "avito", "slots": {"account_brand": "germes", "city": "Уфа"}},
    )

    assert len(matched.matched_actions) == 1
    assert not blocked.matched_actions
