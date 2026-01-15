import pytest

from apps.worker import followups


@pytest.mark.asyncio
async def test_condition_allows_eq_no(monkeypatch):
    async def fake_get_fact(tenant_id, lead_id, key):
        return "no"

    monkeypatch.setattr(followups, "_get_fact", fake_get_fact)

    job = {
        "tenant_id": 1,
        "lead_id": 2,
        "condition": {"key": "order_done", "op": "eq", "value": "no"},
    }
    assert await followups._condition_allows(job) is True

    job["condition"]["value"] = "yes"
    assert await followups._condition_allows(job) is False


@pytest.mark.asyncio
async def test_condition_allows_not_exists(monkeypatch):
    async def fake_get_fact(tenant_id, lead_id, key):
        return None

    monkeypatch.setattr(followups, "_get_fact", fake_get_fact)

    job = {
        "tenant_id": 1,
        "lead_id": 2,
        "condition": {"key": "order_done", "op": "not_exists"},
    }
    assert await followups._condition_allows(job) is True
