import httpx
import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from app.web import public as public_module


def _build_app(monkeypatch: pytest.MonkeyPatch, public_key: str = "public-key") -> FastAPI:
    app = FastAPI()
    app.include_router(public_module.router)

    dummy = APIRouter()

    @dummy.get("/client/{tenant}/settings", name="client_settings")
    def _client_settings_stub(tenant: int):  # pragma: no cover - support endpoint
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


def test_pub_start_produces_qr_id(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch)
    captured: dict[str, object] = {}

    async def _fake_call(
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        timeout: float = 5.0,
    ):
        captured.update({
            "method": method,
            "path": path,
            "payload": json,
            "params": params,
            "timeout": timeout,
        })
        return 200, httpx.Response(
            200,
            json={
                "state": "need_qr",
                "authorized": False,
                "qr_id": "abc123",
                "qr_url": "https://example.test/pub/tg/qr.png?qr_id=abc123",
                "expires_at": 1700000000,
            },
        )

    monkeypatch.setattr(public_module, "_tg_call", _fake_call)

    client = TestClient(app)
    response = client.get("/pub/tg/start", params={"tenant": 1, "k": "public-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["qr_id"] == "abc123"
    assert payload["qr_url"].endswith("qr_id=abc123")
    assert captured == {
        "method": "POST",
        "path": "/qr/start",
        "payload": {"tenant": 1},
        "params": None,
        "timeout": 5.0,
    }


def test_pub_qr_png_proxy(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch)
    captured: dict[str, object] = {}

    async def _fake_call(
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        timeout: float = 5.0,
    ):
        captured.update(
            {
                "method": method,
                "path": path,
                "params": params,
                "payload": json,
                "timeout": timeout,
            }
        )
        return 200, httpx.Response(
            200,
            content=b"png-bytes",
            headers={"Content-Type": "image/png"},
        )

    monkeypatch.setattr(public_module, "_tg_call", _fake_call)

    client = TestClient(app)
    response = client.get(
        "/pub/tg/qr.png",
        params={"tenant": 2, "k": "public-key", "qr_id": "abc123"},
    )

    assert response.status_code == 200
    assert response.content == b"png-bytes"
    assert response.headers.get("content-type") == "image/png"
    assert captured == {
        "method": "GET",
        "path": "/qr/png",
        "payload": None,
        "params": {"tenant": 2, "qr_id": "abc123"},
        "timeout": 5.0,
    }


def test_pub_status_proxy(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch)

    async def _fake_call(
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        timeout: float = 5.0,
    ):
        return 200, httpx.Response(
            200,
            json={
                "state": "need_qr",
                "authorized": False,
                "qr_id": "abc123",
                "qr_url": "https://example.test/pub/tg/qr.png?qr_id=abc123",
            },
        )

    monkeypatch.setattr(public_module, "_tg_call", _fake_call)

    client = TestClient(app)
    response = client.get("/pub/tg/status", params={"tenant": 4, "k": "public-key"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["state"] == "need_qr"
    assert payload["qr_url"].endswith("qr_id=abc123")


def test_connect_tg_does_not_call_wa(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch)
    was_called = False

    def _wa_post(*args, **kwargs):  # pragma: no cover - should not run
        nonlocal was_called
        was_called = True
        raise AssertionError("wa_post should not be called for /connect/tg")

    monkeypatch.setattr(public_module.common, "wa_post", _wa_post)

    client = TestClient(app)
    response = client.get("/connect/tg", params={"tenant": 5, "k": "public-key"})

    assert response.status_code == 200
    assert was_called is False
