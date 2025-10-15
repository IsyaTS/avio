from typing import Any, Dict, Tuple

import pytest

from app.web import webhooks


class _RedisProbe:
    def __init__(self) -> None:
        self.items: list[Tuple[str, str]] = []

    async def lpush(self, key: str, value: str) -> None:
        self.items.append((key, value))

    async def incrby(self, key: str, value: int) -> None:  # pragma: no cover - not used in tests
        return None

    async def setnx(self, key: str, value: int) -> int:  # pragma: no cover - duplicate guard
        return 1

    async def expire(self, key: str, ttl: int) -> None:  # pragma: no cover - duplicate guard
        return None


async def _async_reply(*_args: Any, **_kwargs: Any) -> str:
    return "auto"


async def _async_messages(*_args: Any, **_kwargs: Any) -> list:
    return []


def _sample_body() -> Dict[str, Any]:
    return {
        "source": {"type": "telegram", "tenant": 1},
        "provider": "telegram",
        "message": {
            "text": "hello",
            "telegram_user_id": 12345,
            "message_id": "msg-1",
            "attachments": [],
        },
        "telegram": {"peer_id": 12345},
    }


def _prepare_module(monkeypatch: pytest.MonkeyPatch, redis_stub: _RedisProbe) -> None:
    monkeypatch.setattr(webhooks, "_redis_queue", redis_stub)
    monkeypatch.setattr(webhooks, "_catalog_sent_cache", {})
    monkeypatch.setattr(webhooks, "ask_llm", _async_reply)
    monkeypatch.setattr(webhooks, "build_llm_messages", _async_messages)
    monkeypatch.setattr(webhooks.core, "load_tenant", lambda tenant: {})
    monkeypatch.setattr(webhooks.core, "record_bot_reply", lambda *a, **k: None)
    monkeypatch.setattr(webhooks.settings, "WEBHOOK_SECRET", "")
    monkeypatch.setattr(webhooks.settings, "APP_INTERNAL_URL", "")
    monkeypatch.setattr(webhooks.settings, "APP_PUBLIC_URL", "")


@pytest.mark.anyio
async def test_process_incoming_stores_message(monkeypatch: pytest.MonkeyPatch) -> None:
    redis_stub = _RedisProbe()
    _prepare_module(monkeypatch, redis_stub)

    captured: list[Dict[str, Any]] = []

    async def fake_upsert(lead_id: int, **kwargs: Any) -> int:
        captured.append({"kind": "upsert", "lead_id": lead_id, "kwargs": kwargs})
        return lead_id

    async def fake_insert_message_in(
        lead_id: int,
        text: str,
        status: str = "received",
        tenant_id: int | None = None,
        telegram_user_id: int | None = None,
        provider_msg_id: str | None = None,
    ) -> int:
        captured.append(
            {
                "kind": "message",
                "lead_id": lead_id,
                "text": text,
                "status": status,
                "tenant_id": tenant_id,
                "telegram_user_id": telegram_user_id,
                "provider_msg_id": provider_msg_id,
            }
        )
        return 101

    async def fake_resolve_or_create_contact(**_kwargs: Any) -> int:
        captured.append({"kind": "contact"})
        return 0

    async def fake_link_lead_contact(*_args: Any, **_kwargs: Any) -> None:
        captured.append({"kind": "link"})
        return None

    monkeypatch.setattr(webhooks, "upsert_lead", fake_upsert)
    monkeypatch.setattr(webhooks, "insert_message_in", fake_insert_message_in)
    monkeypatch.setattr(webhooks, "resolve_or_create_contact", fake_resolve_or_create_contact)
    monkeypatch.setattr(webhooks, "link_lead_contact", fake_link_lead_contact)

    response = await webhooks.process_incoming(_sample_body())
    assert response.status_code == 200

    messages = [item for item in captured if item.get("kind") == "message"]
    assert messages, "insert_message_in was not called"
    assert messages[0]["telegram_user_id"] == 12345
    assert any(key == "outbox:send" for key, _ in redis_stub.items)
    assert any(key == webhooks.INCOMING_QUEUE_KEY for key, _ in redis_stub.items)


@pytest.mark.anyio
async def test_process_incoming_upsert_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    redis_stub = _RedisProbe()
    _prepare_module(monkeypatch, redis_stub)

    async def failing_upsert(*_args: Any, **_kwargs: Any) -> int:
        raise RuntimeError("db down")

    async def guard_insert(*_args: Any, **_kwargs: Any) -> int:
        raise AssertionError("insert_message_in should not run on failure")

    monkeypatch.setattr(webhooks, "upsert_lead", failing_upsert)
    monkeypatch.setattr(webhooks, "insert_message_in", guard_insert)
    monkeypatch.setattr(webhooks, "resolve_or_create_contact", lambda **_: 0)
    monkeypatch.setattr(webhooks, "link_lead_contact", lambda *_a, **_kw: None)

    with pytest.raises(webhooks.HTTPException) as excinfo:
        await webhooks.process_incoming(_sample_body())
    assert excinfo.value.status_code == 500
    assert not any(key == "outbox:send" for key, _ in redis_stub.items)
    # inbound queue still receives the message normalization payload
    assert any(key == webhooks.INCOMING_QUEUE_KEY for key, _ in redis_stub.items)
