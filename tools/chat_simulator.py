#!/usr/bin/env python3
"""Interactive console that exercises Avio bot flows without WhatsApp/Avito."""

from __future__ import annotations

import asyncio
import fnmatch
import json
import time
from pathlib import Path
from typing import Any

import yaml

from app import core
from app.common import OUTBOX_QUEUE_KEY
from app import worker as worker_module

WHATSAPP_JID = "79991112233@c.us"
AVITO_CHAT_ID = "demo-chat-1"


class _InMemoryRedis:
    def __init__(self) -> None:
        self._store: dict[str, tuple[str, float | None]] = {}
        self._hashes: dict[str, dict[str, str]] = {}

    def _purge(self, key: str) -> None:
        entry = self._store.get(key)
        if entry:
            _, expires = entry
            if expires is not None and time.time() >= expires:
                self._store.pop(key, None)

    def get(self, key: str) -> str | None:
        self._purge(key)
        entry = self._store.get(key)
        return entry[0] if entry else None

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        self._store[key] = (value, time.time() + ttl_seconds)

    def delete(self, *keys: str) -> None:
        for key in keys:
            self._store.pop(key, None)
            self._hashes.pop(key, None)

    def scan(self, cursor: int = 0, match: str | None = None, count: int = 100) -> tuple[int, list[str]]:
        keys = sorted(k for k in self._store if self._match(k, match))
        return 0, keys[cursor : cursor + count]

    def hget(self, name: str, key: str) -> str | None:
        return self._hashes.get(name, {}).get(key)

    def hset(self, name: str, key: str, value: str) -> None:
        self._hashes.setdefault(name, {})[key] = value

    def hdel(self, name: str, key: str) -> None:
        bucket = self._hashes.get(name)
        if bucket:
            bucket.pop(key, None)

    @staticmethod
    def _match(key: str, pattern: str | None) -> bool:
        if pattern is None or pattern == "":
            return True
        return fnmatch.fnmatch(key, pattern)


class _FakeQueue:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    async def lpush(self, key: str, value: str) -> None:
        # keep latest entries in append order
        self.entries.append((key, value))

    async def rpush(self, key: str, value: str) -> None:
        self.entries.append((key, value))

    def pop_outgoing(self) -> dict[str, Any] | None:
        for idx in range(len(self.entries) - 1, -1, -1):
            key, raw = self.entries[idx]
            if key == OUTBOX_QUEUE_KEY:
                self.entries = self.entries[:idx]
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return None
        return None


_redis_client = _InMemoryRedis()


def _fake_with_sync_redis(func: Any, default: Any = None) -> Any:
    try:
        return func(_redis_client)
    except Exception:
        return default


async def _stub_get_or_create_by_peer(
    *,
    channel: str,
    lead_id_hint: int | None = None,
    source_real_id: int | None = None,
    **_: Any,
) -> int:
    if lead_id_hint:
        return lead_id_hint
    mapping = {"whatsapp": _lead_id_for("whatsapp", TENANT_ID_DEFAULT), "avito": _lead_id_for("avito", TENANT_ID_DEFAULT)}
    return mapping.get(channel.lower(), mapping["whatsapp"])


async def _stub_async(*_: Any, **__: Any) -> None:
    return None


async def _stub_contact(**kwargs: Any) -> int:
    if "whatsapp_phone" in kwargs:
        return WHATSAPP_CONTACT_ID
    if "avito_user_id" in kwargs or "avito_login" in kwargs:
        return AVITO_CONTACT_ID
    return WHATSAPP_CONTACT_ID


def _setup_local_patches(queue: _FakeQueue) -> None:
    core._with_sync_redis = _fake_with_sync_redis
    core._sync_redis_client = None  # type: ignore[attr-defined]
    core._STATE_CACHE.clear()

    worker_module.r = queue
    worker_module.get_or_create_by_peer = _stub_get_or_create_by_peer  # type: ignore[assignment]
    worker_module.resolve_or_create_contact = _stub_contact  # type: ignore[assignment]
    worker_module.link_lead_contact = _stub_async  # type: ignore[assignment]
    worker_module.insert_message_in = _stub_async  # type: ignore[assignment]
    worker_module.smart_reply_enabled = lambda *_: True  # type: ignore[assignment]


def _reset_session(channel: str) -> None:
    lead_id = WHATSAPP_LEAD_ID if channel == "whatsapp" else AVITO_LEAD_ID
    contact_id = WHATSAPP_CONTACT_ID if channel == "whatsapp" else AVITO_CONTACT_ID
    core.reset_sales_state(TENANT_ID, lead_id)
    core.reset_sales_state(TENANT_ID, contact_id)
    worker_module._catalog_sent_cache.clear()
    _redis_client.delete(core._state_key(TENANT_ID, lead_id))
    _redis_client.delete(core._state_key(TENANT_ID, contact_id))


def _build_event(channel: str, text: str) -> dict[str, Any]:
    base: dict[str, Any] = {
        "tenant": TENANT_ID,
        "channel": channel,
        "provider": channel,
        "message_id": f"{channel}-{int(time.time() * 1000)}",
    }
    if channel == "whatsapp":
        base.update(
            {
                "from": PHONE_JID,
                "from_jid": PHONE_JID,
                "lead_id": WHATSAPP_LEAD_ID,
                "text": text,
                "conversation_id": WHATSAPP_LEAD_ID,
            }
        )
    else:  # avito
        base.update(
            {
                "chat_id": AVITO_CHAT_ID,
                "lead_id": AVITO_LEAD_ID,
                "message": text,
                "text": text,
                "author": {"id": 1, "login": "demo"},
                "account_id": 40001,
                "user_id": 77,
            }
        )
    return base


def _display_reply(payload: dict[str, Any] | None) -> None:
    if not payload:
        print("бот: (ответ не сформирован)")
        return
    text = payload.get("text") or ""
    extra = []
    if payload.get("attachments"):
        extra.append(f"attachments={len(payload['attachments'])}")
    if payload.get("provider"):
        extra.append(f"provider={payload['provider']}")
    info = f" [{', '.join(extra)}]" if extra else ""
    print(f"бот:{info} {text}")


def main() -> None:
    queue = _FakeQueue()
    _setup_local_patches(queue)

    channels = {"1": "whatsapp", "2": "avito"}
    selected = "1"
    print(
        "Живой эмулятор чата (без WhatsApp/Avito).\n"
        "Команды: /reset — сбросить контекст, /channel — сменить канал, /quit — выход."
    )
    while True:
        print("\nВыберите канал:")
        for key, name in channels.items():
            print(f"  {key}. {name.title()}")
        choice = input("Номер канала > ").strip()
        if choice in channels:
            selected = choice
            _reset_session(channels[selected])
            break
        if choice == "/quit":
            return
    channel_name = channels[selected]
    while True:
        prompt = f"[{channel_name}] > "
        text = input(prompt).strip()
        if not text:
            continue
        if text == "/quit":
            return
        if text == "/reset":
            _reset_session(channel_name)
            print("контекст сброшен")
            continue
        if text == "/channel":
            return main()

        event = _build_event(channel_name, text)
        asyncio.run(worker_module._handle_incoming_event(event))
        payload = queue.pop_outgoing()
        _display_reply(payload)


if __name__ == "__main__":
    main()
