from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from apps.worker.services import asset_action_runtime


pytestmark = pytest.mark.unit


@pytest.mark.anyio
async def test_asset_runtime_context_uses_sales_state_city(monkeypatch) -> None:
    async def _no_item_context(_tenant_id: int, _lead_id: int):
        return None

    monkeypatch.setattr(asset_action_runtime.avito_item_contexts, "get_context_for_lead", _no_item_context)
    monkeypatch.setattr(
        asset_action_runtime.sales_core,
        "load_sales_state",
        lambda _tenant_id, _lead_id: SimpleNamespace(facts={"city": "Екатеринбург", "product": "дверь"}),
    )

    result = await asset_action_runtime._asset_runtime_context(
        tenant_id=101,
        lead_id=55,
        context={},
    )

    assert result["known_facts"]["city"] == "Екатеринбург"
    assert result["known_facts"]["product"] == "дверь"


@pytest.mark.anyio
async def test_asset_runtime_context_prefers_resolved_item_city(monkeypatch) -> None:
    async def _item_context(_tenant_id: int, _lead_id: int):
        return {"status": "resolved", "city": "Казань", "account_id": 222}

    async def _account(_tenant_id: int, _account_id: int) -> dict[str, Any]:
        return {"account_id": _account_id, "display_name": "Гермес", "account_login": "Двери"}

    monkeypatch.setattr(asset_action_runtime.avito_item_contexts, "get_context_for_lead", _item_context)
    monkeypatch.setattr(asset_action_runtime.avito_accounts, "get_account", _account)
    monkeypatch.setattr(
        asset_action_runtime.sales_core,
        "load_sales_state",
        lambda _tenant_id, _lead_id: SimpleNamespace(facts={"city": "Екатеринбург"}),
    )

    result = await asset_action_runtime._asset_runtime_context(
        tenant_id=101,
        lead_id=55,
        context={},
    )

    assert result["known_facts"]["city"] == "Екатеринбург"
    assert result["avito_item_city"] == "Казань"
    assert result["known_facts"]["account_label"] == "Гермес"
