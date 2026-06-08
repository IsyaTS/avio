import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi.responses import JSONResponse
from starlette.requests import Request

from apps.api.web import webhooks as webhooks_module


pytestmark = pytest.mark.integration


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


def _json_request(
    payload: dict,
    *,
    tenant: int,
    token: str | None = None,
    headers: dict[str, str] | None = None,
) -> Request:
    query_items = [f"tenant={int(tenant)}"]
    if token is not None:
        query_items.append(f"token={token}")
    raw_headers = [(b"content-type", b"application/json")]
    for key, value in (headers or {}).items():
        raw_headers.append((key.lower().encode("latin-1"), str(value).encode("latin-1")))
    body = json.dumps(payload).encode("utf-8")

    async def _receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/webhook/max_personal",
            "headers": raw_headers,
            "query_string": "&".join(query_items).encode("ascii"),
            "server": ("dev.avio.website", 443),
            "scheme": "https",
            "client": ("testclient", 1),
        },
        _receive,
    )


@pytest.mark.asyncio
async def test_process_incoming_treats_max_personal_manager_origin_as_outgoing(monkeypatch):
    dummy = _DummyAsyncRedis()
    monkeypatch.setattr(webhooks_module, "_redis_queue", dummy, raising=False)

    inserted_in: list[tuple] = []
    inserted_out: list[tuple] = []
    enqueued: list[tuple] = []

    async def _fake_upsert_lead(lead_id, **_kwargs):
        return lead_id

    async def _fake_resolve_contact(**_kwargs):
        return 777

    async def _fake_link_contact(*_args, **_kwargs):
        return None

    async def _fake_insert_in(*args, **kwargs):
        inserted_in.append((args, kwargs))
        return 11

    async def _fake_insert_out(*args, **kwargs):
        inserted_out.append((args, kwargs))
        return 22

    async def _fake_amocrm(*_args, **_kwargs):
        return None

    async def _fake_capture(*_args, **_kwargs):
        return None

    async def _fake_lpush(key, value):
        enqueued.append((key, value))

    dummy.lpush = _fake_lpush
    monkeypatch.setattr(webhooks_module, "upsert_lead", _fake_upsert_lead, raising=False)
    monkeypatch.setattr(webhooks_module, "resolve_or_create_contact", _fake_resolve_contact, raising=False)
    monkeypatch.setattr(webhooks_module, "link_lead_contact", _fake_link_contact, raising=False)
    monkeypatch.setattr(webhooks_module, "insert_message_in", _fake_insert_in, raising=False)
    monkeypatch.setattr(webhooks_module, "insert_message_out", _fake_insert_out, raising=False)
    monkeypatch.setattr(
        webhooks_module.amocrm_service,
        "amocrm_on_outbound_message",
        _fake_amocrm,
        raising=False,
    )
    monkeypatch.setattr(webhooks_module, "_capture_manager_intervention", _fake_capture, raising=False)

    response = await webhooks_module.process_incoming(
        {
            "source": {"type": "max_personal", "tenant": 101},
            "message": {
                "message_id": "m-manager-1",
                "chat_id": "chat-1",
                "peer": "chat-1",
                "text": "менеджер отвечает",
                "max_user_id": "9001",
            },
            "origin": "max_personal:manager",
        },
        None,
    )

    assert response.status_code == 200
    assert inserted_out, "manager MAX message must be stored as outgoing"
    assert not inserted_in, "manager MAX message must not be stored as client incoming"
    assert not enqueued, "manager MAX message must not trigger worker/bot queue"


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
    cached_entry = json.loads(dummy.store["wa:qr:7:1234567890"])
    assert cached_entry["tenant"] == 7
    assert cached_entry["qr_id"] == "1234567890"
    assert cached_entry["qr_svg"].startswith("<svg")
    assert cached_entry["provider"] == "whatsapp"
    assert cached_entry["event"] == "qr"
    assert isinstance(cached_entry["updated_at"], int)
    assert dummy.store["wa:qr:last:7"] == "1234567890"


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
    assert stored["lead_id"]
    assert stored["from_jid"].endswith("@c.us")
    assert stored["from"].endswith("@c.us")
    assert stored["from_digits"] == "79991234567"
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


@pytest.mark.anyio
async def test_max_personal_webhook_requires_auth_token(monkeypatch):
    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "get_integration",
        lambda *_args, **_kwargs: {"event_secret": "secret-1"},
        raising=False,
    )
    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "max_personal_worker_token",
        lambda: "",
        raising=False,
    )

    with pytest.raises(webhooks_module.HTTPException) as exc:
        await webhooks_module.max_personal_webhook(
            _json_request(
                {"message": {"chat_id": "c1", "message_id": "m1", "text": "hi"}},
                tenant=1,
            )
        )
    assert exc.value.status_code == 401
    assert exc.value.detail == "unauthorized"


@pytest.mark.anyio
async def test_max_personal_webhook_normalizes_payload(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_process_incoming(body, _request):
        captured["body"] = body
        return JSONResponse({"ok": True})

    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "get_integration",
        lambda *_args, **_kwargs: {"event_secret": "secret-2"},
        raising=False,
    )
    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "max_personal_worker_token",
        lambda: "",
        raising=False,
    )
    monkeypatch.setattr(webhooks_module, "process_incoming", _fake_process_incoming, raising=False)

    payload = {
        "tenant": 2,
        "message": {
            "chat_id": "chat-22",
            "message_id": "msg-22",
            "text": "тест",
            "max_user_id": "777",
            "max_username": "isyyaa",
            "display_name": "Manager",
        },
        "manager": True,
        "out": True,
        "origin": "max_personal:manager",
    }

    resp = await webhooks_module.max_personal_webhook(
        _json_request(payload, tenant=2, token="secret-2")
    )
    assert resp.status_code == 200
    assert json.loads(resp.body)["ok"] is True
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["source"]["type"] == "max_personal"
    assert body["source"]["tenant"] == 2
    assert body["message"]["chat_id"] == "chat-22"
    assert body["message"]["peer"] == "chat-22"
    assert body["message"]["message_id"] == "msg-22"
    assert body["message"]["text"] == "тест"
    assert body["manager"] is True
    assert body["out"] is True
    assert body["origin"] == "max_personal:manager"


@pytest.mark.anyio
async def test_max_personal_webhook_extracts_nested_text(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_process_incoming(body, _request):
        captured["body"] = body
        return JSONResponse({"ok": True})

    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "get_integration",
        lambda *_args, **_kwargs: {"event_secret": "secret-3"},
        raising=False,
    )
    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "max_personal_worker_token",
        lambda: "",
        raising=False,
    )
    monkeypatch.setattr(webhooks_module, "process_incoming", _fake_process_incoming, raising=False)

    payload = {
        "tenant": 3,
        "message": {
            "chat_id": "chat-33",
            "message_id": "msg-33",
            "text": {"text": "здравствуйте"},
        },
    }

    resp = await webhooks_module.max_personal_webhook(
        _json_request(payload, tenant=3, token="secret-3")
    )
    assert resp.status_code == 200
    assert json.loads(resp.body)["ok"] is True
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["message"]["text"] == "здравствуйте"


@pytest.mark.anyio
async def test_max_personal_webhook_accepts_worker_token_when_query_stale(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_process_incoming(body, _request):
        captured["body"] = body
        return JSONResponse({"ok": True})

    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "get_integration",
        lambda *_args, **_kwargs: {"event_secret": "fresh-secret"},
        raising=False,
    )
    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "max_personal_worker_token",
        lambda: "worker-secret",
        raising=False,
    )
    monkeypatch.setattr(webhooks_module, "process_incoming", _fake_process_incoming, raising=False)

    payload = {
        "tenant": 4,
        "message": {
            "chat_id": "chat-44",
            "message_id": "msg-44",
            "text": "ping",
        },
    }

    resp = await webhooks_module.max_personal_webhook(
        _json_request(
            payload,
            tenant=4,
            token="stale-token",
            headers={"X-Auth-Token": "worker-secret"},
        )
    )
    assert resp.status_code == 200
    assert json.loads(resp.body)["ok"] is True
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["source"]["tenant"] == 4
    assert body["message"]["text"] == "ping"


@pytest.mark.anyio
async def test_max_personal_webhook_binds_secret_when_missing_and_worker_auth(monkeypatch):
    captured_updates: list[tuple[int, dict[str, str]]] = []

    async def _fake_process_incoming(body, _request):
        return JSONResponse({"ok": True, "tenant": body["source"]["tenant"]})

    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "get_integration",
        lambda *_args, **_kwargs: {},
        raising=False,
    )
    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "max_personal_worker_token",
        lambda: "worker-secret",
        raising=False,
    )

    def _fake_update(tenant_id: int, data: dict[str, str]):
        captured_updates.append((tenant_id, dict(data)))
        return data

    monkeypatch.setattr(
        webhooks_module.max_personal_service,
        "update_integration",
        _fake_update,
        raising=False,
    )
    monkeypatch.setattr(webhooks_module, "process_incoming", _fake_process_incoming, raising=False)

    payload = {
        "tenant": 5,
        "message": {
            "chat_id": "chat-55",
            "message_id": "msg-55",
            "text": "hello",
        },
    }

    resp = await webhooks_module.max_personal_webhook(
        _json_request(
            payload,
            tenant=5,
            token="rebind-secret",
            headers={"X-Auth-Token": "worker-secret"},
        )
    )
    assert resp.status_code == 200
    assert json.loads(resp.body)["ok"] is True
    assert captured_updates == [(5, {"event_secret": "rebind-secret"})]
