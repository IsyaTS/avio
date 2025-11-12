#!/usr/bin/env python3
"""Interactive console that exercises Avio bot flows without WhatsApp/Avito."""

from __future__ import annotations

import asyncio
import fnmatch
import json
import sys
import time
from pathlib import Path
from typing import Any

import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import core
from app.common import OUTBOX_QUEUE_KEY
from app import worker as worker_module

WHATSAPP_JID = "79991112233@c.us"
AVITO_CHAT_ID = "demo-chat-1"
CHANNEL_OPTIONS = {"1": "whatsapp", "2": "avito"}
AVAILABLE_COMMANDS = "/reset /channel /tenant /quit"


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

    def clear(self) -> None:
        self._store.clear()
        self._hashes.clear()

    @staticmethod
    def _match(key: str, pattern: str | None) -> bool:
        if pattern is None or pattern == "":
            return True
        return fnmatch.fnmatch(key, pattern)


class _FakeQueue:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    async def lpush(self, key: str, value: str) -> None:
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
_CURRENT_TENANT_ID = 1


def _current_tenant_id() -> int:
    return int(_CURRENT_TENANT_ID)


def _set_current_tenant_id(tenant_id: int) -> None:
    global _CURRENT_TENANT_ID

    _CURRENT_TENANT_ID = int(tenant_id)


def _lead_id_for(channel: str, tenant_id: int) -> int:
    base = int(tenant_id) * 1000
    return base + (11 if channel == "whatsapp" else 22)


def _contact_id_for(channel: str, tenant_id: int) -> int:
    base = int(tenant_id) * 1000
    return base + (111 if channel == "whatsapp" else 222)


def _fake_with_sync_redis(func: Any, default: Any = None) -> Any:
    try:
        return func(_redis_client)
    except Exception:
        return default


async def _stub_get_or_create_by_peer(
    *,
    channel: str,
    lead_id_hint: int | None = None,
    **_: Any,
) -> int:
    if lead_id_hint:
        return lead_id_hint
    return _lead_id_for(channel.lower(), _current_tenant_id())


async def _stub_async(*_: Any, **__: Any) -> None:
    return None


async def _stub_contact(**kwargs: Any) -> int:
    if "whatsapp_phone" in kwargs:
        return _contact_id_for("whatsapp", _current_tenant_id())
    if "avito_user_id" in kwargs or "avito_login" in kwargs:
        return _contact_id_for("avito", _current_tenant_id())
    return _contact_id_for("whatsapp", _current_tenant_id())


def _setup_local_patches(queue: _FakeQueue) -> None:
    core._with_sync_redis = _fake_with_sync_redis
    core._sync_redis_client = None  # type: ignore[attr-defined]
    core._STATE_CACHE.clear()
    _redis_client.clear()

    worker_module.r = queue
    worker_module.get_or_create_by_peer = _stub_get_or_create_by_peer  # type: ignore[assignment]
    worker_module.resolve_or_create_contact = _stub_contact  # type: ignore[assignment]
    worker_module.link_lead_contact = _stub_async  # type: ignore[assignment]
    worker_module.insert_message_in = _stub_async  # type: ignore[assignment]
    worker_module.smart_reply_enabled = lambda *_: True  # type: ignore[assignment]


def _reset_session(channel: str, tenant_id: int) -> None:
    lead_id = _lead_id_for(channel, tenant_id)
    contact_id = _contact_id_for(channel, tenant_id)
    core.reset_sales_state(tenant_id, lead_id)
    core.reset_sales_state(tenant_id, contact_id)
    _redis_client.delete(core._state_key(tenant_id, lead_id))
    _redis_client.delete(core._state_key(tenant_id, contact_id))


def _reset_all_sessions(tenant_id: int) -> None:
    for channel in ("whatsapp", "avito"):
        _reset_session(channel, tenant_id)


def _build_event(channel: str, text: str, tenant_id: int) -> dict[str, Any]:
    base: dict[str, Any] = {
        "tenant": tenant_id,
        "channel": channel,
        "provider": channel,
        "message_id": f"{channel}-{int(time.time() * 1000)}",
    }
    if channel == "whatsapp":
        base.update(
            {
                "from": WHATSAPP_JID,
                "from_jid": WHATSAPP_JID,
                "lead_id": _lead_id_for(channel, tenant_id),
                "text": text,
                "conversation_id": _lead_id_for(channel, tenant_id),
            }
        )
    else:
        base.update(
            {
                "chat_id": AVITO_CHAT_ID,
                "lead_id": _lead_id_for(channel, tenant_id),
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


def _available_tenants() -> list[tuple[int, str]]:
    seen: dict[int, str] = {}
    config_path = core.TENANTS_CONFIG_PATH
    if config_path.exists():
        try:
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            for entry in (raw or {}).get("tenants", []):
                if not isinstance(entry, dict):
                    continue
                tenant_id = entry.get("id")
                name = entry.get("name") or f"Tenant {tenant_id}"
                if isinstance(tenant_id, int):
                    seen[tenant_id] = str(name)
        except Exception:
            pass

    tenants_dir = core.TENANTS_DIR
    if tenants_dir.exists():
        for child in sorted(tenants_dir.iterdir()):
            if not child.is_dir():
                continue
            if not child.name.isdigit():
                continue
            tenant_id = int(child.name)
            seen.setdefault(tenant_id, f"Tenant {tenant_id}")

    if not seen:
        seen[1] = "Tenant 1"
    return sorted(seen.items())


def _choose_tenant(default: int | None = None) -> tuple[int, str]:
    tenants = _available_tenants()
    defaults = {tid for tid, _ in tenants}
    if default not in defaults:
        default = tenants[0][0]
    prompt = f"Tenant id (available {', '.join(str(tid) for tid in defaults)}; Enter=use {default}) > "
    while True:
        for tid, name in tenants:
            print(f"  {tid}: {name}")
        value = input(prompt).strip()
        if not value:
            chosen = default
        else:
            try:
                chosen = int(value)
            except ValueError:
                print("некорректный id, попробуй ещё")
                continue
        if chosen in defaults:
            name = dict(tenants)[chosen]
            return chosen, name
        print("такого тенанта нет, попробуй ещё")


def _choose_channel(current: str | None = None) -> str:
    while True:
        print("\nВыберите канал:")
        for key, name in CHANNEL_OPTIONS.items():
            print(f"  {key}. {name.title()}")
        if current:
            prompt = f"Номер канала (Enter = {current}) > "
        else:
            prompt = "Номер канала > "
        value = input(prompt).strip()
        if not value and current:
            return current
        if value in CHANNEL_OPTIONS:
            return CHANNEL_OPTIONS[value]
        print("Неправильный выбор, попробуй ещё")


def main() -> None:
    queue = _FakeQueue()
    _setup_local_patches(queue)

    tenant_id: int | None = None
    tenant_name = ""
    while True:  # tenant loop
        tenant_id, tenant_name = _choose_tenant(default=tenant_id or 1)
        _set_current_tenant_id(tenant_id)
        core.ensure_tenant_files(tenant_id)
        _reset_all_sessions(tenant_id)

        while True:  # channel loop
            channel_name = _choose_channel()
            print(
                f"\nДобро пожаловать в тестовый чат tenant={tenant_name}({tenant_id}) channel={channel_name}."
            )
            print(f"Доступные команды: {AVAILABLE_COMMANDS}")

            while True:  # conversation loop
                prompt = f"[tenant={tenant_name} channel={channel_name}] > "
                text = input(prompt).strip()
                if not text:
                    continue
                if text == "/quit":
                    return
                if text == "/reset":
                    _reset_session(channel_name, tenant_id)
                    print("Контекст сброшен")
                    continue
                if text == "/channel":
                    break
                if text == "/tenant":
                    break  # bubble up to tenant loop

                event = _build_event(channel_name, text, tenant_id)
                asyncio.run(worker_module._handle_incoming_event(event))
                payload = queue.pop_outgoing()
                _display_reply(payload)

            if text == "/tenant":
                break


if __name__ == "__main__":
    main()
