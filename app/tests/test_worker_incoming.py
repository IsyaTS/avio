from __future__ import annotations
import json

import pytest

from app import worker as worker_module


@pytest.mark.anyio
async def test_worker_handles_whatsapp_event(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict]] = []

    class FakeRedis:
        async def lpush(self, key: str, value: str) -> None:
            events.append((key, json.loads(value)))

    inserted: list[tuple[int, str, str, int | None]] = []

    async def fake_insert_message_in(
        lead_id: int,
        text: str,
        *,
        status: str = "received",
        tenant_id: int | None = None,
        telegram_user_id: int | None = None,
    ) -> None:
        inserted.append((lead_id, text, status, tenant_id))

    async def fake_upsert_lead(lead_id: int, **kwargs: object) -> int:
        return lead_id

    async def fake_resolve_contact(**kwargs: object) -> int:
        return 777

    async def fake_link_contact(lead_id: int, contact_id: int) -> None:
        return None

    async def fake_build_llm_messages(*args: object, **kwargs: object) -> list[str]:
        return ["context"]

    async def fake_ask_llm(*args: object, **kwargs: object) -> str:
        return "auto-reply"

    monkeypatch.setattr(worker_module, "r", FakeRedis(), raising=False)
    monkeypatch.setattr(worker_module, "insert_message_in", fake_insert_message_in, raising=False)
    monkeypatch.setattr(worker_module, "upsert_lead", fake_upsert_lead, raising=False)
    monkeypatch.setattr(
        worker_module,
        "resolve_or_create_contact",
        fake_resolve_contact,
        raising=False,
    )
    monkeypatch.setattr(worker_module, "link_lead_contact", fake_link_contact, raising=False)
    monkeypatch.setattr(worker_module, "smart_reply_enabled", lambda *_: True, raising=False)
    monkeypatch.setattr(worker_module, "build_llm_messages", fake_build_llm_messages, raising=False)
    monkeypatch.setattr(worker_module, "ask_llm", fake_ask_llm, raising=False)

    event = {
        "channel": "whatsapp",
        "tenant": 9,
        "message_id": "MSG-1",
        "from": "79991234567",
        "text": "hello",
        "lead_id": 1234,
    }

    await worker_module._handle_incoming_event(event)

    assert inserted == [(1234, "hello", "received", 9)]
    assert events, "expected outgoing reply to be enqueued"
    queue, payload = events[0]
    assert queue == worker_module.OUTBOX_QUEUE_KEY
    assert payload["provider"] == "whatsapp"
    assert payload["to"] == "79991234567"
    assert payload["text"] == "auto-reply"
