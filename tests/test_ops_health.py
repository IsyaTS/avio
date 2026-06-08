import pytest

from libs.core.services import ops_health


class DummyRedis:
    def ping(self):
        return True


@pytest.mark.asyncio
async def test_deep_health_reports_tenant_avito_and_learning(monkeypatch):
    async def _fetchrow(sql, *args):
        if "SELECT 1 AS ok" in sql:
            return {"ok": 1}
        if "FROM training_examples" in sql:
            return {"active": 7, "ready": 5}
        raise AssertionError(sql)

    cfg = {
        "behavior": {"avito_smart_reply_enabled": True},
        "follow_up": [{"text": "later"}],
        "integrations": {
            "avito": {
                "access_token": "access",
                "refresh_token": "refresh",
                "expires_at": 4_000_000_000,
                "account_id": 123,
                "account_login": "shop",
            }
        },
        "learning": {
            "enabled": True,
            "intervention_policy": {
                "enabled": True,
                "capture_enabled": True,
                "runtime_enabled": True,
                "apply_mode": True,
                "shadow_mode": False,
            },
        },
    }

    monkeypatch.setattr(ops_health.db, "_fetchrow", _fetchrow, raising=False)
    monkeypatch.setattr(ops_health, "read_tenant_config", lambda tenant: cfg)

    payload = await ops_health.build_deep_health(redis_client=DummyRedis(), tenants="1")

    assert payload["ok"] is True
    tenant = payload["tenants"][0]
    assert tenant["settings"]["avito_smart_reply_enabled"] is True
    assert tenant["settings"]["follow_up_count"] == 1
    assert tenant["avito"]["has_access_token"] is True
    assert tenant["avito"]["has_refresh_token"] is True
    assert tenant["learning"]["apply_mode"] is True
    assert tenant["learning"]["training_examples"] == {
        "available": True,
        "active": 7,
        "ready": 5,
    }


@pytest.mark.asyncio
async def test_deep_health_degrades_when_redis_is_unavailable(monkeypatch):
    async def _fetchrow(sql, *args):
        return {"ok": 1}

    monkeypatch.setattr(ops_health.db, "_fetchrow", _fetchrow, raising=False)

    payload = await ops_health.build_deep_health(redis_client=None, tenants="")

    assert payload["ok"] is False
    assert payload["status"] == "degraded"
    assert payload["redis"]["error"] == "redis_client_unavailable"
