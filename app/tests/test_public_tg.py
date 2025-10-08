from __future__ import annotations

import json
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from app.web import public as public_module


def _base_app(monkeypatch):
    app = FastAPI()
    app.include_router(public_module.router)

    dummy = APIRouter()

    @dummy.get("/client/{tenant}/settings", name="client_settings")
    def _client_settings_stub(tenant: int):  # pragma: no cover - smoke helper
        return {"ok": True, "tenant": tenant}

    app.include_router(dummy)

    monkeypatch.setattr(public_module.C, "valid_key", lambda tenant, key: True)
    monkeypatch.setattr(public_module.C, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(
        public_module.C,
        "read_tenant_config",
        lambda tenant: {"passport": {"brand": "Test Brand"}, "integrations": {}},
    )
    monkeypatch.setattr(public_module.C, "read_persona", lambda tenant: "Persona\nLine2")
    monkeypatch.setattr(public_module.C, "public_base_url", lambda request=None: "https://example.test")
    monkeypatch.setattr(public_module.C, "public_url", lambda request, url: str(url))

    return app


def test_connect_tg_renders(monkeypatch):
    app = _base_app(monkeypatch)
    client = TestClient(app)

    response = client.get("/connect/tg", params={"tenant": 7, "k": "abc123"})

    assert response.status_code == 200
    body = response.text
    assert "Подключение Telegram" in body
    assert "/pub/tg/status" in body
    assert "Test Brand" in body
    assert "Persona" in body


def test_tg_status_success(monkeypatch):
    app = _base_app(monkeypatch)
    called = {"start": 0, "status": []}

    async def _fake_start(path: str, payload: dict, timeout: float = 8.0):
        called["start"] += 1
        assert payload["tenant_id"] == 3

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        called["status"].append((method, path))
        payload = {"status": "waiting_qr", "tenant_id": 3, "qr_id": "qr-test"}
        return 200, json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(public_module.C, "tg_post", _fake_start)
    monkeypatch.setattr(public_module.C, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/status", params={"tenant": 3, "k": "key"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "waiting_qr"
    assert data["qr_id"] == "qr-test"
    assert called["start"] == 1
    assert called["status"] == [("GET", "/session/status?tenant=3")]
