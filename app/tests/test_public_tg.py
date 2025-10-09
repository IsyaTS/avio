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
    assert "window.__tgConnectConfig" in body
    assert '"tenant": "7"' in body
    assert '"key": "abc123"' in body
    assert "Test Brand" in body
    assert "Persona" in body


def test_tg_start_passthrough(monkeypatch):
    app = _base_app(monkeypatch)
    called: dict[str, object] = {}

    async def _fake_start(path: str, payload: dict, timeout: float = 8.0):
        called["path"] = path
        called["payload"] = payload
        called["timeout"] = timeout
        return httpx.Response(
            200,
            json={"status": "waiting_qr", "qr_id": "qr-1", "needs_2fa": None, "extra": "ignore"},
        )

    monkeypatch.setattr(public_module.C, "tg_post", _fake_start)

    client = TestClient(app)
    resp = client.post("/pub/tg/start", params={"tenant": 11, "k": "secret"})

    assert resp.status_code == 200
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    data = resp.json()
    assert data == {
        "status": "waiting_qr",
        "qr_id": "qr-1",
        "needs_2fa": None,
        "extra": "ignore",
    }
    assert data["status"] is not None
    assert data["qr_id"] is not None
    assert called["path"] == "http://tgworker:8085/session/start"
    assert called["payload"] == {"tenant_id": 11}
    assert called["timeout"] == 15.0


def test_tg_status_success(monkeypatch):
    app = _base_app(monkeypatch)
    called = {"status": []}

    def _unexpected(*args, **kwargs):  # pragma: no cover - safeguard
        raise AssertionError("tg_post should not be called")

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        called["status"].append((method, path, timeout))
        payload = {"status": "waiting_qr", "tenant_id": 3, "qr_id": "qr-test"}
        return 200, json.dumps(payload).encode("utf-8"), {"Content-Type": "application/json"}

    monkeypatch.setattr(public_module.C, "tg_post", _unexpected)
    monkeypatch.setattr(public_module.C, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/status", params={"tenant": 3, "k": "key"})

    assert resp.status_code == 200
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    data = resp.json()
    assert data["status"] == "waiting_qr"
    assert data["qr_id"] == "qr-test"
    assert called["status"] == [
        ("GET", "http://tgworker:8085/session/status?tenant=3", 15.0)
    ]


def test_tg_qr_png_proxy(monkeypatch):
    app = _base_app(monkeypatch)

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        assert path == "http://tgworker:8085/session/qr/qr-1.png"
        return 200, b"png-bytes", {"Content-Type": "image/png"}

    monkeypatch.setattr(public_module.C, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.png", params={"qr_id": "qr-1"})

    assert resp.status_code == 200
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    assert resp.headers.get("pragma") == "no-cache"
    assert resp.headers.get("expires") == "0"
    assert resp.headers.get("content-type") == "image/png"
    assert resp.content == b"png-bytes"


def test_tg_qr_png_expired(monkeypatch):
    app = _base_app(monkeypatch)

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        assert path == "http://tgworker:8085/session/qr/qr-1.png"
        payload = json.dumps({"detail": "qr_expired"}).encode("utf-8")
        return 404, payload, {"Content-Type": "application/json"}

    monkeypatch.setattr(public_module.C, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.png", params={"qr_id": "qr-1"})

    assert resp.status_code == 404
    assert resp.json() == {"detail": "qr_expired"}
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    assert resp.headers.get("pragma") == "no-cache"
    assert resp.headers.get("expires") == "0"
    assert resp.headers.get("x-telegram-upstream-status") == "404"


def test_tg_qr_txt_proxy(monkeypatch):
    app = _base_app(monkeypatch)

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        assert path == "http://tgworker:8085/session/qr/qr-1.txt"
        return 200, b"tg://login?token=abc", {"Content-Type": "text/plain"}

    monkeypatch.setattr(public_module.C, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.txt", params={"qr_id": "qr-1"})

    assert resp.status_code == 200
    assert resp.text == "tg://login?token=abc"
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    assert resp.headers.get("pragma") == "no-cache"
    assert resp.headers.get("expires") == "0"


def test_tg_qr_txt_expired(monkeypatch):
    app = _base_app(monkeypatch)

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        assert path == "http://tgworker:8085/session/qr/qr-1.txt"
        payload = json.dumps({"detail": "qr_expired"}).encode("utf-8")
        return 404, payload, {"Content-Type": "application/json"}

    monkeypatch.setattr(public_module.C, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.txt", params={"qr_id": "qr-1"})

    assert resp.status_code == 404
    assert resp.json() == {"detail": "qr_expired"}
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    assert resp.headers.get("pragma") == "no-cache"
    assert resp.headers.get("expires") == "0"
    assert resp.headers.get("x-telegram-upstream-status") == "404"
