from __future__ import annotations

import pytest

from libs.core.repo import tenant_asset_rules


pytestmark = pytest.mark.unit


@pytest.mark.anyio
async def test_upsert_rule_without_db_returns_rule(monkeypatch) -> None:
    monkeypatch.setattr(tenant_asset_rules.db_module, "_fetchrow", None, raising=False)
    monkeypatch.setattr(tenant_asset_rules.db_module, "_exec", None, raising=False)

    rule = await tenant_asset_rules.upsert_rule(
        1,
        "r1",
        asset_id="a1",
        source="asset_title",
        status="active",
        conditions={"all": [{"slot": "city", "operator": "equals", "value": "Казань"}]},
        action={"type": "send_asset", "asset_id": "a1"},
        guards={"requires_known_slots": ["city"]},
        confidence=0.9,
        needs_review=False,
    )

    assert rule is not None
    assert rule["status"] == "active"
    assert rule["needs_review"] is False


def test_stable_rule_id_is_deterministic() -> None:
    first = tenant_asset_rules.stable_rule_id(1, "a1", "asset_title", {"all": []})
    second = tenant_asset_rules.stable_rule_id(1, "a1", "asset_title", {"all": []})
    assert first == second
