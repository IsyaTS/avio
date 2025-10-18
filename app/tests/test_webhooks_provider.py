import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.web import webhooks as webhooks_module


class _DummyAsyncRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.queue: list[tuple[str, str]] = []

    async def set(self, key: str, value: str, **kwargs):  # pragma: no cover - helper
        self.store[key] = value

    async def lpush(self, key: str, value: str):  # pragma: no cover - helper
        self.queue.append((key, value))


def _build_app():
    app = FastAPI()
    app.include_router(webhooks_module.router)
    return app


def test_provider_webhook_caches_qr(monkeypatch):
    dummy = _DummyAsyncRedis()
    monkeypatch.setattr(webhooks_module, "_redis_queue", dummy, raising=False)
    async def _fake_get_by_tenant(tenant_id: int):
        assert tenant_id == 7
        return type("_T", (), {"token": "provider-secret"})()

    monkeypatch.setattr(
        webhooks_module.provider_tokens_repo,
        "get_by_tenant",
        _fake_get_by_tenant,
        raising=False,
    )

    app = _build_app()
    client = TestClient(app)

    payload = {
        "provider": "whatsapp",
        "event": "qr",
        "tenant": 7,
        "qr_id": "1234567890",
        "svg": "<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>",
    }

    resp = client.post(
        "/webhook?token=provider-secret",
        json=payload,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body == {"ok": True, "queued": False, "event": "qr", "qr_id": "1234567890"}
    cached_entry = json.loads(dummy.store[f"wa:qr:7:1234567890"])
    assert cached_entry["tenant"] == 7
    assert cached_entry["qr_id"] == "1234567890"
    assert cached_entry["qr_svg"].startswith("<svg")
    assert cached_entry["provider"] == "whatsapp"
    assert cached_entry["event"] == "qr"
    assert isinstance(cached_entry["updated_at"], int)
    assert dummy.store[f"wa:qr:last:7"] == "1234567890"


def test_provider_webhook_messages_incoming(monkeypatch):
    dummy = _DummyAsyncRedis()
    monkeypatch.setattr(webhooks_module, "_redis_queue", dummy, raising=False)

    inserted: list[tuple[str, str, int | None, dict]] = []

    async def _fake_insert(provider: str, event_type: str, lead_id: int | None, payload: dict):
        inserted.append((provider, event_type, lead_id, payload))

    async def _fake_get_by_tenant(tenant_id: int):
        return type("_T", (), {"token": "provider-secret"})()

    monkeypatch.setattr(
        webhooks_module.provider_tokens_repo,
        "get_by_tenant",
        _fake_get_by_tenant,
        raising=False,
    )
    monkeypatch.setattr(
        webhooks_module,
        "insert_webhook_event",
        _fake_insert,
        raising=False,
    )

    app = _build_app()
    client = TestClient(app)

    payload = {
        "provider": "whatsapp",
        "event": "messages.incoming",
        "tenant": 5,
        "channel": "whatsapp",
        "message_id": "ABCDEF",
        "from": "+7 (999) 123-45-67",
        "text": "Hello",
        "ts": 1716800000,
    }

    resp = client.post(
        "/webhook?token=provider-secret",
        json=payload,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body == {"ok": True, "queued": True}
    assert inserted
    assert inserted[0][0] == "whatsapp"
    assert inserted[0][1] == "messages.incoming"
    assert dummy.queue
    key, raw_item = dummy.queue[0]
    assert key == "inbox:message_in"
    stored = json.loads(raw_item)
    assert stored["event"] == "messages.incoming"
    assert stored["tenant"] == 5
    assert stored["from"] == "79991234567"
    assert stored["from_jid"].endswith("@c.us")
    assert stored["text"] == "Hello"
    assert stored["message_id"] == "ABCDEF"


def test_provider_webhook_rejects_bad_token(monkeypatch):
    dummy = _DummyAsyncRedis()
    monkeypatch.setattr(webhooks_module, "_redis_queue", dummy, raising=False)

    async def _fake_get_by_tenant(tenant_id: int):
        return type("_T", (), {"token": "provider-secret"})()

    monkeypatch.setattr(
        webhooks_module.provider_tokens_repo,
        "get_by_tenant",
        _fake_get_by_tenant,
        raising=False,
    )

    app = _build_app()
    client = TestClient(app)

    payload = {
        "provider": "whatsapp",
        "event": "ready",
        "tenant": 3,
        "channel": "whatsapp",
    }

    resp = client.post("/webhook?token=wrong", json=payload)
    assert resp.status_code == 401
    assert resp.json()["detail"] == "unauthorized"
    assert not dummy.queue


def test_provider_webhook_db_error(monkeypatch):
    dummy = _DummyAsyncRedis()
    monkeypatch.setattr(webhooks_module, "_redis_queue", dummy, raising=False)

    async def _boom(*_: object, **__: object):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        webhooks_module.provider_tokens_repo,
        "get_by_tenant",
        _boom,
        raising=False,
    )

    app = _build_app()
    client = TestClient(app)

    payload = {
        "provider": "whatsapp",
        "event": "ready",
        "tenant": 2,
        "channel": "whatsapp",
    }

    resp = client.post("/webhook?token=fake", json=payload)
    assert resp.status_code == 500
    assert resp.json()["detail"] == "db_error"
    assert not dummy.queue
