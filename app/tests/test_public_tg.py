from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
import httpx
import pytest

from app.web import public as public_module
from app.web.public import TgWorkerCallError


def _base_app(monkeypatch, public_key: str = "public-key") -> FastAPI:
    app = FastAPI()
    app.include_router(public_module.router)

    dummy = APIRouter()

    @dummy.get("/client/{tenant}/settings", name="client_settings")
    def _client_settings_stub(tenant: int):  # pragma: no cover - smoke helper
        return {"ok": True, "tenant": tenant}

    app.include_router(dummy)

    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(
        public_module.common,
        "read_tenant_config",
        lambda tenant: {"passport": {"brand": "Test Brand"}, "integrations": {}},
    )
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant: "Persona\nLine2")
    monkeypatch.setattr(public_module.common, "public_base_url", lambda request=None: "https://example.test")
    monkeypatch.setattr(public_module.common, "public_url", lambda request, url: str(url))
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: key == public_key)
    monkeypatch.setattr(public_module.settings, "ADMIN_TOKEN", "admin-token")
    monkeypatch.setattr(public_module.settings, "PUBLIC_KEY", public_key)
    monkeypatch.setattr(public_module.settings, "TGWORKER_BASE_URL", "http://tgworker:9000")
    monkeypatch.setenv("ADMIN_TOKEN", "admin-token")
    monkeypatch.setenv("PUBLIC_KEY", public_key)
    public_module._LOCAL_PASSWORD_ATTEMPTS.clear()

    return app


def _make_response(method: str, path: str, *, status_code: int, json: dict | None = None, content: bytes | None = None, headers: dict[str, str] | None = None) -> httpx.Response:
    request = httpx.Request(method.upper(), f"http://tgworker.test{path}")
    if json is not None:
        return httpx.Response(status_code, json=json, headers=headers, request=request)
    return httpx.Response(status_code, content=content, headers=headers, request=request)


def test_connect_tg_renders(monkeypatch):
    app = _base_app(monkeypatch)
    client = TestClient(app)

    response = client.get("/connect/tg", params={"tenant": 7, "k": "public-key"})

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


def test_tg_happy_path_flow(monkeypatch):
    app = _base_app(monkeypatch)
    calls: list[tuple[str, str]] = []

    async def _fake_call(method: str, path: str, **kwargs):
        calls.append((method.upper(), path))
        if path == "/qr/start":
            return 200, _make_response(
                "POST",
                path,
                status_code=200,
                json={"qr_id": "abc123", "state": "waiting", "tenant": 7},
                headers={"Content-Type": "application/json"},
            )
        if path == "/status":
            return 200, _make_response(
                "GET",
                path,
                status_code=200,
                json={"state": "waiting", "qr_id": "abc123", "tenant": 7},
                headers={"Content-Type": "application/json"},
            )
        if path == "/qr/png":
            return 200, _make_response(
                "GET",
                path,
                status_code=200,
                content=b"\x89PNG",
                headers={"Content-Type": "image/png"},
            )
        raise AssertionError(f"unexpected call {method} {path}")

    monkeypatch.setattr(public_module, "_tg_call", _fake_call)

    client = TestClient(app)
    start = client.post("/pub/tg/start", params={"tenant": 7, "k": "public-key"})
    assert start.status_code == 200
    assert start.headers.get("x-telegram-upstream-status") == "200"
    assert start.json()["qr_id"] == "abc123"

    status = client.get("/pub/tg/status", params={"tenant": 7, "k": "public-key"})
    assert status.status_code == 200
    body = status.json()
    assert body["state"] in {"need_qr", "waiting"}
    assert body["qr_id"] == "abc123"

    qr = client.get(
        "/pub/tg/qr.png",
        params={"tenant": 7, "k": "public-key", "qr_id": "abc123"},
    )
    assert qr.status_code == 200
    assert qr.headers.get("content-type") == "image/png"
    assert qr.headers.get("cache-control") == "no-store"
    assert qr.content.startswith(b"\x89PNG")

    assert calls == [
        ("POST", "/qr/start"),
        ("GET", "/status"),
        ("GET", "/qr/png"),
    ]


def test_tg_status_fallback(monkeypatch):
    app = _base_app(monkeypatch)
    calls: list[tuple[str, str]] = []

    async def _fake_call(method: str, path: str, **kwargs):
        calls.append((method.upper(), path))
        if path == "/status":
            return 404, _make_response(
                "GET",
                path,
                status_code=404,
                content=b"missing",
                headers={"Content-Type": "text/plain"},
            )
        if path == "/rpc/status":
            return 200, _make_response(
                "GET",
                path,
                status_code=200,
                json={"state": "waiting", "qr_id": "fallback"},
                headers={"Content-Type": "application/json"},
            )
        if path == "/session/status":
            pytest.fail("/session/status should not be called once /rpc/status succeeds")
        raise AssertionError(f"unexpected call {method} {path}")

    monkeypatch.setattr(public_module, "_tg_call", _fake_call)

    client = TestClient(app)
    response = client.get("/pub/tg/status", params={"tenant": 4, "k": "public-key"})
    assert response.status_code == 200
    assert response.json()["qr_id"] == "fallback"
    assert calls == [("GET", "/status"), ("GET", "/rpc/status")]


def test_tg_worker_unavailable(monkeypatch):
    app = _base_app(monkeypatch)

    async def _fail_call(method: str, path: str, **kwargs):
        raise TgWorkerCallError(f"http://tgworker.test{path}", "ECONNREFUSED")

    monkeypatch.setattr(public_module, "_tg_call", _fail_call)

    client = TestClient(app)
    response = client.get("/pub/tg/status", params={"tenant": 2, "k": "public-key"})
    assert response.status_code == 502
    payload = response.json()
    assert payload["error"] == "tg_unavailable"
    assert "ECONNREFUSED" in payload["detail"]
