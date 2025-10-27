from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.web import public as public_module


def _build_app() -> TestClient:
    app = FastAPI()
    app.include_router(public_module.router)
    return TestClient(app)


def test_settings_get_accepts_cookie_key(monkeypatch):
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: tenant == 7 and key == "cookie-secret")
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: {"tenant": tenant})
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant: "persona")

    client = _build_app()
    client.cookies.set("client_key", "cookie-secret")
    response = client.get(
        "/pub/settings/get",
        params={"tenant": 7},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True, "cfg": {"tenant": 7}, "persona": "persona"}

    client.close()


def test_settings_get_accepts_query_key(monkeypatch):
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: tenant == 5 and key == "query-secret")
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: {"tenant": tenant})
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant: "persona")

    client = _build_app()
    response = client.get(
        "/pub/settings/get",
        params={"tenant": 5, "k": "query-secret"},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True, "cfg": {"tenant": 5}, "persona": "persona"}

    client.close()


def test_settings_get_accepts_global_and_tenant_keys(monkeypatch):
    monkeypatch.setattr(public_module.settings, "PUBLIC_KEY", "GLOBAL")
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    config = {"passport": {"public_key": "TENANT_KEY"}, "tenant": 1}
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: dict(config))
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant: "persona")
    monkeypatch.setattr(public_module.common, "get_tenant_pubkey", lambda tenant: "")
    monkeypatch.setattr(
        public_module.common,
        "valid_key",
        lambda tenant, key: key in {"GLOBAL", "TENANT_KEY"},
    )

    client = _build_app()

    global_resp = client.get("/pub/settings/get", params={"tenant": 1, "k": "GLOBAL"})
    assert global_resp.status_code == 200

    tenant_resp = client.get("/pub/settings/get", params={"tenant": 1, "k": "TENANT_KEY"})
    assert tenant_resp.status_code == 200

    denied_resp = client.get("/pub/settings/get", params={"tenant": 1, "k": "BAD"})
    assert denied_resp.status_code == 401

    client.close()


def test_settings_get_sets_no_cache_headers(monkeypatch):
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: tenant == 3 and key == "token")
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: {"tenant": tenant})
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant: "persona")

    client = _build_app()
    response = client.get("/pub/settings/get", params={"tenant": 3, "k": "token"})

    assert response.status_code == 200
    headers = response.headers
    assert headers.get("cache-control") == "no-store, must-revalidate"
    assert headers.get("pragma") == "no-cache"
    assert headers.get("expires") == "0"

    client.close()
