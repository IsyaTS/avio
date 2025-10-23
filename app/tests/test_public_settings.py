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
