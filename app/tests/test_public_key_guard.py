import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.web import public as public_module


def _build_public_app() -> FastAPI:
    app = FastAPI()
    app.include_router(public_module.router)
    return app


@pytest.fixture(autouse=True)
def _clear_attempts():
    public_module._LOCAL_PASSWORD_ATTEMPTS.clear()


def test_public_key_checker_allows_valid_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PUBLIC_KEY", "test-public-key")
    monkeypatch.setattr(public_module.settings, "PUBLIC_KEY", "test-public-key")

    async def _fake_status_impl(tenant: int) -> dict:
        return {"ok": True, "tenant": tenant}

    async def _fake_wa_post(*args, **kwargs):  # pragma: no cover - not used in assertions
        class _Resp:
            status_code = 200

        return _Resp()

    app = _build_public_app()
    monkeypatch.setattr(public_module, "_wa_status_impl", _fake_status_impl)
    monkeypatch.setattr(public_module.common, "wa_post", _fake_wa_post)

    client = TestClient(app)

    ok_response = client.get(
        "/pub/wa/status",
        params={"tenant": 7, "k": "test-public-key"},
    )
    assert ok_response.status_code == 200
    assert ok_response.json()["tenant"] == 7

    denied = client.get(
        "/pub/wa/status",
        params={"tenant": 7, "k": "invalid"},
    )
    assert denied.status_code == 401
    assert denied.json() == {"error": "invalid_key"}


def test_wa_status_integration_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PUBLIC_KEY", "integration-key")
    monkeypatch.setattr(public_module.settings, "PUBLIC_KEY", "integration-key")
    monkeypatch.setattr(public_module.common, "webhook_url", lambda: "https://example.test/webhook")

    async def _fake_wa_post(*args, **kwargs):  # pragma: no cover - start side effect
        class _Resp:
            status_code = 200

        return _Resp()

    observed: list[str] = []

    def _fake_http(method: str, url: str, body=None, timeout: float = 8.0):
        observed.append(url)
        payload = {"ready": True, "last": {"status": "online"}}
        return 200, json.dumps(payload)

    app = _build_public_app()
    monkeypatch.setattr(public_module.common, "wa_post", _fake_wa_post)
    monkeypatch.setattr(public_module.common, "http", _fake_http)

    client = TestClient(app)
    response = client.get(
        "/pub/wa/status",
        params={"tenant": 1, "k": "integration-key"},
    )

    assert response.status_code == 200
    assert response.json()["ready"] is True
    assert any(url.endswith("/session/1/status") for url in observed)
