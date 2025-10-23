from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.internal import tenant as tenant_module


def _build_app():
    app = FastAPI()
    app.include_router(tenant_module.router)
    return app


def test_internal_ensure_reuses_existing_token(monkeypatch):
    created = []

    async def _fake_get_by_tenant(tenant_id: int):
        assert tenant_id == 11
        return SimpleNamespace(token="existing-token")

    async def _fake_create(tenant_id: int, token: str):
        created.append((tenant_id, token))
        return SimpleNamespace(token=token)

    async def _noop_schema() -> None:
        return None

    monkeypatch.setattr(tenant_module.common_module, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(tenant_module.provider_tokens, "ensure_schema", _noop_schema, raising=False)
    monkeypatch.setattr(tenant_module.provider_tokens, "get_by_tenant", _fake_get_by_tenant)
    monkeypatch.setattr(tenant_module.provider_tokens, "create_for_tenant", _fake_create)
    monkeypatch.setattr(tenant_module.settings, "WEBHOOK_SECRET", "secret", raising=False)

    app = _build_app()
    client = TestClient(app)

    resp = client.post("/internal/tenant/11/ensure", headers={"X-Auth-Token": "secret"})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["tenant"] == 11
    assert payload["provider_token"] == "existing-token"
    assert created == []


def test_internal_ensure_generates_new_token(monkeypatch):
    captured = {}

    async def _fake_get_by_tenant(tenant_id: int):
        return None

    async def _fake_create(tenant_id: int, token: str):
        captured["tenant_id"] = tenant_id
        captured["token"] = token
        return SimpleNamespace(token=token)

    async def _noop_schema() -> None:
        return None

    monkeypatch.setattr(tenant_module.common_module, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(tenant_module.provider_tokens, "ensure_schema", _noop_schema, raising=False)
    monkeypatch.setattr(tenant_module.provider_tokens, "get_by_tenant", _fake_get_by_tenant)
    monkeypatch.setattr(tenant_module.provider_tokens, "create_for_tenant", _fake_create)
    monkeypatch.setattr(tenant_module.settings, "WEBHOOK_SECRET", "secret", raising=False)

    app = _build_app()
    client = TestClient(app)

    resp = client.post("/internal/tenant/9/ensure", headers={"X-Auth-Token": "secret"})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["tenant"] == 9
    assert isinstance(payload["provider_token"], str)
    assert len(payload["provider_token"]) >= 10
    assert captured["tenant_id"] == 9
    assert captured["token"] == payload["provider_token"]


def test_internal_ensure_db_error(monkeypatch):
    async def _boom(*_: object, **__: object):
        raise RuntimeError("boom")

    async def _noop_schema() -> None:
        return None

    monkeypatch.setattr(tenant_module.common_module, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(tenant_module.provider_tokens, "ensure_schema", _noop_schema, raising=False)
    monkeypatch.setattr(tenant_module.provider_tokens, "get_by_tenant", _boom)
    monkeypatch.setattr(tenant_module.settings, "WEBHOOK_SECRET", "secret", raising=False)

    app = _build_app()
    client = TestClient(app)

    resp = client.post("/internal/tenant/42/ensure", headers={"X-Auth-Token": "secret"})
    assert resp.status_code == 500
    assert resp.json()["detail"] == "db_error"


def test_internal_ensure_accepts_admin_token(monkeypatch):
    async def _fake_get_by_tenant(tenant_id: int):
        return SimpleNamespace(token="token-from-db")

    async def _noop_schema() -> None:
        return None

    monkeypatch.setattr(tenant_module.common_module, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(tenant_module.provider_tokens, "ensure_schema", _noop_schema, raising=False)
    monkeypatch.setattr(tenant_module.provider_tokens, "get_by_tenant", _fake_get_by_tenant)
    monkeypatch.setattr(tenant_module.settings, "WEBHOOK_SECRET", "", raising=False)
    monkeypatch.setattr(tenant_module.settings, "ADMIN_TOKEN", "admin-secret", raising=False)

    app = _build_app()
    client = TestClient(app)

    resp = client.post("/internal/tenant/7/ensure", headers={"X-Auth-Token": "admin-secret"})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["tenant"] == 7
    assert payload["provider_token"] == "token-from-db"
