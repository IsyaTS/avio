from __future__ import annotations

import json

import httpx
import pytest
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

    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: True)
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(
        public_module.common,
        "read_tenant_config",
        lambda tenant: {"passport": {"brand": "Test Brand"}, "integrations": {}},
    )
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant: "Persona\nLine2")
    monkeypatch.setattr(public_module.common, "public_base_url", lambda request=None: "https://example.test")
    monkeypatch.setattr(public_module.common, "public_url", lambda request, url: str(url))
    monkeypatch.setattr(public_module.settings, "ADMIN_TOKEN", "admin-token")
    monkeypatch.setattr(public_module.settings, "PUBLIC_KEY", "public-key")
    public_module._LOCAL_PASSWORD_ATTEMPTS.clear()

    return app


def test_connect_tg_renders(monkeypatch):
    app = _base_app(monkeypatch)
    client = TestClient(app)

    response = client.get("/connect/tg", params={"tenant": 7, "k": "abc123"})

    assert response.status_code == 200
    body = response.text
    assert "Подключение Telegram" in body
    assert "window.__tgConnectConfig" in body
    assert '"tenant": 7' in body
    assert '"key": "abc123"' in body
    assert "Test Brand" in body
    assert "Persona" in body
    assert 'id="tg-qr-image"' in body
    assert 'id="tg-2fa-block"' in body


def test_tg_start_passthrough(monkeypatch):
    app = _base_app(monkeypatch)
    called: dict[str, object] = {}

    async def _fake_start(path: str, payload: dict, timeout: float = 5.0):
        called["path"] = path
        called["payload"] = payload
        called["timeout"] = timeout
        return httpx.Response(
            200,
            json={
                "status": "waiting_qr",
                "qr_id": "qr-1",
                "qr_valid_until": 1700000000,
            },
        )

    monkeypatch.setattr(public_module.C, "tg_post", _fake_start)

    client = TestClient(app)
    resp = client.get(
        "/pub/tg/start",
        params={"tenant": 11},
        headers={"X-Admin-Token": "admin-token"},
    )

    assert resp.status_code == 200
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    assert resp.headers.get("x-telegram-upstream-status") == "200"
    data = resp.json()
    assert data == {
        "status": "waiting_qr",
        "qr_id": "qr-1",
        "qr_valid_until": 1700000000,
    }
    assert called["path"] == "/rpc/start"
    assert called["payload"] == {"tenant_id": 11, "force": False}
    assert called["timeout"] == 5.0


def test_tg_status_success(monkeypatch):
    app = _base_app(monkeypatch)
    called = {"status": []}

    async def _fake_get(path: str, payload: dict | None = None, timeout: float = 8.0):
        called["status"].append((path, payload, timeout))
        body = {
            "status": "waiting_qr",
            "tenant_id": 3,
            "qr_id": "qr-test",
            "needs_2fa": False,
            "twofa_pending": False,
            "twofa_since": None,
            "qr_valid_until": 1700000000,
            "last_error": None,
            "stats": {"authorized": 0, "waiting": 1, "needs_2fa": 0},
        }
        return httpx.Response(200, json=body)

    monkeypatch.setattr(public_module.C, "tg_get", _fake_get)

    client = TestClient(app)
    resp = client.get(
        "/pub/tg/status",
        params={"tenant": 3},
        headers={"X-Admin-Token": "admin-token"},
    )

    assert resp.status_code == 200
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    assert resp.headers.get("x-telegram-upstream-status") == "200"
    data = resp.json()
    assert data["status"] == "waiting_qr"
    assert data["qr_id"] == "qr-test"
    assert data["needs_2fa"] is False
    assert data["twofa_pending"] is False
    assert data["twofa_since"] is None
    assert data["qr_valid_until"] == 1700000000
    assert data["last_error"] is None
    assert "stats" in data
    assert called["status"] == [("/rpc/status", {"tenant_id": 3}, 5.0)]


def test_tg_password_proxies_json_payload(monkeypatch):
    app = _base_app(monkeypatch)
    captured: dict[str, object] = {}

    async def _fake_post(path: str, payload: dict, timeout: float = 8.0):
        captured["path"] = path
        captured["payload"] = payload
        captured["timeout"] = timeout
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(public_module.C, "tg_post", _fake_post)

    client = TestClient(app)
    response = client.post(
        "/pub/tg/password",
        params={"tenant": 5},
        json={"password": "pass123"},
        headers={"X-Admin-Token": "admin-token"},
    )

    assert response.status_code == 200
    assert captured["path"] == "/rpc/twofa.submit"
    assert captured["payload"] == {"tenant_id": 5, "password": "pass123"}
    assert captured["timeout"] == 5.0
    assert response.headers.get("x-telegram-upstream-status") == "200"


def test_tg_password_accepts_form_payload(monkeypatch):
    app = _base_app(monkeypatch)
    captured: dict[str, object] = {}

    async def _fake_post(path: str, payload: dict, timeout: float = 8.0):
        captured["path"] = path
        captured["payload"] = payload
        captured["timeout"] = timeout
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(public_module.C, "tg_post", _fake_post)

    client = TestClient(app)
    response = client.post(
        "/pub/tg/password",
        params={"tenant": 6, "k": "public-key"},
        data={"password": "form-pass"},
    )

    assert response.status_code == 200
    assert captured["path"] == "/rpc/twofa.submit"
    assert captured["payload"] == {"tenant_id": 6, "password": "form-pass"}
    assert captured["timeout"] == 5.0


@pytest.mark.parametrize(
    ("tenant_id", "status_code", "body", "headers"),
    [
        (41, 401, {"error": "password_invalid"}, {}),
        (42, 409, {"error": "srp_invalid"}, {}),
        (43, 429, {"error": "flood_wait", "retry_after": 17}, {"Retry-After": "17"}),
    ],
)
def test_tg_password_error_passthrough(monkeypatch, tenant_id, status_code, body, headers):
    app = _base_app(monkeypatch)

    async def _fake_post(path: str, payload: dict, timeout: float = 8.0):
        assert path == "/rpc/twofa.submit"
        assert payload["tenant_id"] == tenant_id
        return httpx.Response(status_code, json=body, headers=headers)

    monkeypatch.setattr(public_module.C, "tg_post", _fake_post)

    client = TestClient(app)
    response = client.post(
        "/pub/tg/password",
        params={"tenant": tenant_id},
        json={"password": "pass"},
        headers={"X-Admin-Token": "admin-token"},
    )

    assert response.status_code == status_code
    assert response.json() == body
    assert response.headers.get("x-telegram-upstream-status") == str(status_code)
    if status_code == 429:
        assert response.headers.get("retry-after") == headers.get("Retry-After")


def test_tg_qr_png_proxy(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_get(path: str, payload: dict | None = None, timeout: float = 8.0):
        assert path == "/session/qr/qr-1.png"
        assert payload is None
        assert timeout == 5.0
        return httpx.Response(200, content=b"png-bytes", headers={"Content-Type": "image/png"})

    monkeypatch.setattr(public_module.C, "tg_get", _fake_get)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.png", params={"qr_id": "qr-1"})

    assert resp.status_code == 200
    cache_header = resp.headers.get("cache-control", "")
    assert cache_header.startswith("no-store")
    assert resp.headers.get("x-telegram-upstream-status") == "200"
    assert resp.headers.get("content-type") == "image/png"
    assert resp.content == b"png-bytes"


def test_tg_qr_png_expired(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_get(path: str, payload: dict | None = None, timeout: float = 8.0):
        assert path == "/session/qr/qr-1.png"
        assert payload is None
        assert timeout == 5.0
        payload_bytes = json.dumps({"error": "qr_expired"}).encode("utf-8")
        return httpx.Response(404, content=payload_bytes, headers={"Content-Type": "application/json"})

    monkeypatch.setattr(public_module.C, "tg_get", _fake_get)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.png", params={"qr_id": "qr-1"})

    assert resp.status_code == 404
    assert resp.json() == {"error": "qr_expired"}
    cache_header = resp.headers.get("cache-control", "")
    assert cache_header.startswith("no-store")
    assert resp.headers.get("x-telegram-upstream-status") == "404"


def test_tg_qr_txt_proxy(monkeypatch):
    app = _base_app(monkeypatch)

    def _fake_http(method: str, path: str, body: bytes | None = None, timeout: float = 8.0):
        assert path == "http://tgworker:8085/session/qr/qr-1.txt"
        return 200, b"tg://login?token=abc", {"Content-Type": "text/plain"}

    monkeypatch.setattr(public_module.common, "tg_http", _fake_http)

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

    monkeypatch.setattr(public_module.common, "tg_http", _fake_http)

    client = TestClient(app)
    resp = client.get("/pub/tg/qr.txt", params={"qr_id": "qr-1"})

    assert resp.status_code == 404
    assert resp.json() == {"detail": "qr_expired"}
    cache_header = resp.headers.get("cache-control", "")
    assert "no-store" in cache_header
    assert resp.headers.get("pragma") == "no-cache"
    assert resp.headers.get("expires") == "0"
    assert resp.headers.get("x-telegram-upstream-status") == "404"
