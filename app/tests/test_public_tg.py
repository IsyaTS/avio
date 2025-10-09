from __future__ import annotations

import json

import httpx
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


def test_tg_start_filters_payload(monkeypatch):
    app = _base_app(monkeypatch)
    called: dict[str, object] = {}

    async def _fake_start(path: str, payload: dict, timeout: float = 8.0):
        called["path"] = path
        called["payload"] = payload
        return httpx.Response(
            200,
            json={"status": "waiting_qr", "qr_id": "qr-1", "needs_2fa": None, "extra": "ignore"},
        )

    monkeypatch.setattr(public_module.C, "tg_post", _fake_start)

    client = TestClient(app)
    resp = client.post("/pub/tg/start", params={"tenant": 11, "k": "secret"})

    assert resp.status_code == 200
    assert resp.json() == {"status": "waiting_qr", "qr_id": "qr-1"}
    assert called["path"] == "/session/start"
    assert called["payload"] == {"tenant_id": 11}


def test_tg_status_success(monkeypatch):
    app = _base_app(monkeypatch)
    called = {"start": 0, "status": []}

    async def _fake_start(path: str, payload: dict, timeout: float = 8.0):
        called["start"] += 1
        assert payload["tenant_id"] == 3
        return httpx.Response(200, json={"ok": True})

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


def test_tg_qr_png_proxy(monkeypatch):
    app = _base_app(monkeypatch)

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        return 200, b"png-bytes"

    monkeypatch.setattr(public_module.C, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.png", params={"tenant": 5, "k": "secret", "qr_id": "qr-1"})

    assert resp.status_code == 200
    assert resp.headers.get("cache-control") == "no-store"
    assert resp.content == b"png-bytes"


def test_tg_qr_png_expired(monkeypatch):
    app = _base_app(monkeypatch)

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        return 404, b""

    monkeypatch.setattr(public_module.C, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.png", params={"tenant": 5, "k": "secret", "qr_id": "qr-1"})

    assert resp.status_code == 404
    assert resp.json() == {"error": "qr_expired"}
