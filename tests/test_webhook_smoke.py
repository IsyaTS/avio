import json
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main
from app.web import public as public_module


class _DummyRedis:
    def __init__(self) -> None:
        self.items: list[tuple[str, str]] = []

    async def lpush(self, key: str, value: str) -> None:
        self.items.append((key, value))

    async def incrby(self, key: str, value: int) -> None:  # pragma: no cover - unused safety
        return None

    async def setnx(self, key: str, value: int) -> int:  # pragma: no cover - unused safety
        return 1

    async def expire(self, key: str, ttl: int) -> None:  # pragma: no cover - unused safety
        return None


@pytest.fixture
def app_client(monkeypatch: pytest.MonkeyPatch) -> tuple[TestClient, _DummyRedis]:
    redis_stub = _DummyRedis()
    monkeypatch.setattr(public_module.common, "redis_client", lambda: redis_stub)
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: True)
    monkeypatch.setattr(public_module.settings, "ADMIN_TOKEN", "admin-token")
    monkeypatch.setattr(main, "_r", redis_stub)
    if hasattr(main, "_webhooks_mod"):
        monkeypatch.setattr(main._webhooks_mod, "_redis_queue", redis_stub)
        monkeypatch.setattr(
            public_module,
            "INBOX_MESSAGE_KEY",
            getattr(main._webhooks_mod, "INCOMING_QUEUE_KEY", "messages:incoming"),
            raising=False,
        )
    with TestClient(main.app) as client:
        yield client, redis_stub


def test_webhook_smoke_and_outgoing(
    monkeypatch: pytest.MonkeyPatch, app_client: tuple[TestClient, _DummyRedis]
) -> None:
    client, redis_stub = app_client
    inbound_payload = {
        "tenant": 1,
        "channel": "telegram",
        "from_id": 12345,
        "to": 67890,
        "text": "hello",
        "attachments": [],
        "ts": 1_700_000_000,
        "provider_raw": {
            "date": "2024-05-01T12:00:00Z",
            "nested": {"edit_date": "2024-05-01T12:05:00Z"},
        },
    }

    headers = {"X-Admin-Token": "admin-token"}

    response = client.post("/webhook/provider", json=inbound_payload, headers=headers)
    assert response.status_code == 200

    assert redis_stub.items, "webhook payload was not stored"
    key, raw_payload = redis_stub.items[0]
    assert key == public_module.INBOX_MESSAGE_KEY
    stored_payload = json.loads(raw_payload)
    assert isinstance(stored_payload["ts"], int)
    provider_raw = stored_payload.get("provider_raw", {})
    assert isinstance(provider_raw, dict)
    assert provider_raw.get("date") == "2024-05-01T12:00:00Z"
    assert provider_raw.get("nested", {}).get("edit_date") == "2024-05-01T12:05:00Z"

    captured: dict[str, Any] = {"calls": []}

    class _DummyAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            captured["init_kwargs"] = kwargs

        async def __aenter__(self) -> "_DummyAsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
            return False

        async def post(self, url: str, json: Any = None, **kwargs: Any) -> httpx.Response:
            captured["calls"].append({"url": url, "json": json})
            return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(main.httpx, "AsyncClient", _DummyAsyncClient)

    outbound_payload = {
        "tenant": 1,
        "channel": "telegram",
        "to": 67890,
        "text": "ping",
        "attachments": [],
        "meta": {},
    }

    send_response = client.post("/send", json=outbound_payload, headers=headers)
    assert send_response.status_code == 200
    assert captured["calls"], "outgoing request to tgworker was not captured"

    sent_payload = captured["calls"][0]["json"]
    json.dumps(sent_payload)


def test_app_send_to_me(monkeypatch: pytest.MonkeyPatch, app_client):
    client, _ = app_client
    captured: dict[str, Any] = {"calls": []}

    class _DummyAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> "_DummyAsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
            return False

        async def post(self, url: str, json: Any = None, **kwargs: Any) -> httpx.Response:
            captured["calls"].append({"url": url, "json": json})
            return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(main.httpx, "AsyncClient", _DummyAsyncClient)

    payload = {"tenant": 1, "channel": "telegram", "to": "me", "text": "ping"}
    headers = {"X-Admin-Token": "admin-token"}
    response = client.post("/send", json=payload, headers=headers)
    assert response.status_code == 200
    assert captured["calls"], "expected request to tgworker"
    forwarded = captured["calls"][0]["json"]
    assert forwarded["to"] == "me"


def test_app_send_to_me_not_authorized(monkeypatch: pytest.MonkeyPatch, app_client):
    client, _ = app_client

    class _DummyAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> "_DummyAsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
            return False

        async def post(self, url: str, json: Any = None, **kwargs: Any) -> httpx.Response:
            return httpx.Response(409, json={"error": "not_authorized"})

    monkeypatch.setattr(main.httpx, "AsyncClient", _DummyAsyncClient)

    payload = {"tenant": 1, "channel": "telegram", "to": "me", "text": "ping"}
    response = client.post("/send", json=payload)
    assert response.status_code == 409
    body = response.json()
    assert body.get("detail") == '{"error": "not_authorized"}'
