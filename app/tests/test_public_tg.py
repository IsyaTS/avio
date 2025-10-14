from __future__ import annotations

import httpx
import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from app.web import public as public_module


def _base_app(monkeypatch, public_key: str = "public-key"):
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
    monkeypatch.setattr(public_module.settings, "PUBLIC_KEY", public_key)
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
    assert '"key": "public-key"' in body
    assert '"public_key": "public-key"' in body
    assert '"tg_qr_png": "/pub/tg/qr.png?k=public-key"' in body
    assert '"tg_status_url": "/pub/tg/status?k=public-key"' in body
    assert '"tg_start_url": "/pub/tg/start?k=public-key"' in body
    assert '"tg_2fa_url": "/pub/tg/2fa?k=public-key"' in body
    assert '"tg_password": "/pub/tg/2fa"' in body
    assert "Test Brand" in body
    assert "Persona" in body
    assert 'id="tg-qr-image"' in body
    assert 'id="tg-2fa-block"' in body


def test_tg_start_returns_qr_metadata(monkeypatch):
    app = _base_app(monkeypatch)
    captured: dict[str, object] = {}

    async def _fake_start(path: str, payload: dict, timeout: float = 5.0):
        captured["path"] = path
        captured["payload"] = payload
        captured["timeout"] = timeout
        return httpx.Response(
            200,
            json={
                "ok": True,
                "state": "waiting_qr",
                "authorized": False,
                "expires_at": 1700000000,
                "last_error": None,
            },
        )

    monkeypatch.setattr(public_module.C, "tg_post", _fake_start)

    client = TestClient(app)
    response = client.get("/pub/tg/start", params={"tenant": 11, "k": "public-key"})

    assert response.status_code == 200
    assert response.headers.get("cache-control") == "no-store, no-cache, must-revalidate"
    assert response.headers.get("x-telegram-upstream-status") == "200"
    payload = response.json()
    assert payload["state"] == "waiting_qr"
    assert payload["authorized"] is False
    assert payload["qr_url"] == "/pub/tg/qr.png?tenant=11&k=public-key"
    assert payload["expires_at"] == 1700000000
    assert captured == {"path": "/qr/start", "payload": {"tenant": 11}, "timeout": 5.0}


def test_tg_start_renders_html(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_start(path: str, payload: dict, timeout: float = 5.0):
        return httpx.Response(200, json={"state": "waiting_qr", "authorized": False})

    monkeypatch.setattr(public_module.C, "tg_post", _fake_start)

    client = TestClient(app)
    response = client.get(
        "/pub/tg/start",
        params={"tenant": 2, "k": "public-key"},
        headers={"accept": "text/html"},
    )

    assert response.status_code == 200
    assert "<img" in response.text
    assert "/pub/tg/qr.png?tenant=2&k=public-key" in response.text
    assert response.headers.get("content-type") == "text/html; charset=utf-8"


def test_tg_start_conflict_maps_to_409(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_start(path: str, payload: dict, timeout: float = 5.0):
        return httpx.Response(409, json={"ok": False, "error": "already_authorized", "state": "authorized"})

    monkeypatch.setattr(public_module.C, "tg_post", _fake_start)

    client = TestClient(app)
    response = client.get("/pub/tg/start", params={"tenant": 3, "k": "public-key"})

    assert response.status_code == 409
    assert response.json()["error"] == "already_authorized"
    assert response.headers.get("x-telegram-upstream-status") == "409"


def test_tg_start_failed_maps_to_gateway(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_start(path: str, payload: dict, timeout: float = 5.0):
        return httpx.Response(410, json={"ok": False, "error": "qr_expired", "state": "failed", "last_error": "qr_expired"})

    monkeypatch.setattr(public_module.C, "tg_post", _fake_start)

    client = TestClient(app)
    response = client.get("/pub/tg/start", params={"tenant": 5, "k": "public-key"})

    assert response.status_code == 502
    body = response.json()
    assert body["error"] == "qr_expired"
    assert body["state"] == "failed"
    assert response.headers.get("x-telegram-upstream-status") == "410"


def test_tg_status_success(monkeypatch):
    app = _base_app(monkeypatch)
    captured: dict[str, object] = {}

    async def _fake_status(path: str, payload: dict | None = None, timeout: float = 5.0, stream: bool = False):
        captured["path"] = path
        captured["payload"] = payload
        captured["timeout"] = timeout
        return httpx.Response(200, json={"state": "need_2fa", "authorized": False, "needs_2fa": True})

    monkeypatch.setattr(public_module.C, "tg_get", _fake_status)

    client = TestClient(app)
    response = client.get("/pub/tg/status", params={"tenant": 9, "k": "public-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["authorized"] is False
    assert payload["state"] == "need_2fa"
    assert payload["needs_2fa"] is True
    assert captured == {"path": "/status", "payload": {"tenant": 9}, "timeout": 5.0}


def test_tg_status_requires_key(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_status(path: str, payload: dict | None = None, timeout: float = 5.0, stream: bool = False):
        raise AssertionError("should not reach tgworker without key")

    monkeypatch.setattr(public_module.C, "tg_get", _fake_status)

    client = TestClient(app)
    response = client.get("/pub/tg/status", params={"tenant": 4})

    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}


def test_tg_qr_png_streams(monkeypatch):
    app = _base_app(monkeypatch)
    captured: dict[str, object] = {}

    async def _fake_qr(path: str, payload: dict | None = None, timeout: float = 5.0, stream: bool = False):
        captured["path"] = path
        captured["payload"] = payload
        captured["timeout"] = timeout
        captured["stream"] = stream
        return httpx.Response(200, headers={"Content-Type": "image/png"}, content=b"png-bytes")

    monkeypatch.setattr(public_module.C, "tg_get", _fake_qr)

    client = TestClient(app)
    response = client.get("/pub/tg/qr.png", params={"tenant": 12, "k": "public-key"})

    assert response.status_code == 200
    assert response.content == b"png-bytes"
    assert response.headers.get("content-type") == "image/png"
    assert captured == {
        "path": "/qr.png",
        "payload": {"tenant": 12},
        "timeout": 5.0,
        "stream": True,
    }


def test_tg_qr_png_expired(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_qr(path: str, payload: dict | None = None, timeout: float = 5.0, stream: bool = False):
        return httpx.Response(410, json={"error": "qr_expired"})

    monkeypatch.setattr(public_module.C, "tg_get", _fake_qr)

    client = TestClient(app)
    response = client.get("/pub/tg/qr.png", params={"tenant": 6, "k": "public-key"})

    assert response.status_code == 410
    assert response.json() == {"error": "qr_expired"}


def test_tg_qr_png_not_found(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_qr(path: str, payload: dict | None = None, timeout: float = 5.0, stream: bool = False):
        return httpx.Response(404, json={"error": "qr_not_found"})

    monkeypatch.setattr(public_module.C, "tg_get", _fake_qr)

    client = TestClient(app)
    response = client.get("/pub/tg/qr.png", params={"tenant": 7, "k": "public-key"})

    assert response.status_code == 404
    assert response.json() == {"error": "qr_not_found"}


def test_tg_twofa_success(monkeypatch):
    app = _base_app(monkeypatch)
    captured: dict[str, object] = {}

    async def _fake_post(path: str, payload: dict, timeout: float = 5.0):
        captured["path"] = path
        captured["payload"] = payload
        captured["timeout"] = timeout
        return httpx.Response(200, json={"authorized": True, "state": "authorized", "needs_2fa": False})

    monkeypatch.setattr(public_module.C, "tg_post", _fake_post)

    client = TestClient(app)
    response = client.post(
        "/pub/tg/2fa",
        params={"tenant": 8, "k": "public-key"},
        json={"password": "secret"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["authorized"] is True
    assert payload["state"] == "authorized"
    assert captured == {"path": "/2fa", "payload": {"tenant": 8, "password": "secret"}, "timeout": 5.0}


def test_tg_twofa_bad_password(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fake_post(path: str, payload: dict, timeout: float = 5.0):
        return httpx.Response(401, json={"error": "bad_password"})

    monkeypatch.setattr(public_module.C, "tg_post", _fake_post)

    client = TestClient(app)
    response = client.post(
        "/pub/tg/2fa",
        params={"tenant": 10, "k": "public-key"},
        json={"password": "wrong"},
    )

    assert response.status_code == 401
    assert response.json()["error"] == "bad_password"


def test_tg_twofa_rate_limited(monkeypatch):
    app = _base_app(monkeypatch)

    monkeypatch.setattr(public_module, "_register_password_attempt", lambda *args, **kwargs: (False, 30))

    client = TestClient(app)
    response = client.post(
        "/pub/tg/2fa",
        params={"tenant": 1, "k": "public-key"},
        json={"password": "secret"},
    )

    assert response.status_code == 429
    payload = response.json()
    assert payload["error"] == "flood_wait"
    assert payload["retry_after"] == 30


@pytest.mark.parametrize("route", ["/pub/tg/start", "/pub/tg/status", "/pub/tg/qr.png"])
def test_public_tg_endpoints_require_key(monkeypatch, route):
    app = _base_app(monkeypatch, public_key="public-key")

    async def _fail(*args, **kwargs):
        raise AssertionError("should not call upstream")

    monkeypatch.setattr(public_module.C, "tg_post", _fail)
    monkeypatch.setattr(public_module.C, "tg_get", _fail)

    client = TestClient(app)
    response = client.get(route, params={"tenant": 123})
    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}
