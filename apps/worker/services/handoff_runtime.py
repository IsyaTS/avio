from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ManagerHandoffDeps:
    redis_client: Any
    handoff_silence_ttl_seconds: int
    notify_event_manager: str
    log_fn: SyncFn
    notification_event_enabled_fn: SyncFn
    notification_chat_ids_fn: SyncFn
    get_contact_phone_by_lead_fn: AsyncFn
    process_notification_fn: AsyncFn
    handoff_silence_key_fn: SyncFn
    handoff_silence_meta_key_fn: SyncFn
    time_fn: Callable[[], float] = time.time


def notification_lead_title(lead_id: int, contact_phone: str | None) -> str:
    if contact_phone:
        return f"Лид {contact_phone}"
    return f"Лид {lead_id}"


def build_chat_link(username: str | None, phone: str | None, peer: str | None) -> str | None:
    if phone:
        digits = "".join(ch for ch in phone if ch.isdigit())
        if digits:
            return f"https://t.me/+{digits}"
    if username:
        return f"https://t.me/{username.lstrip('@')}"
    if peer:
        digits = "".join(ch for ch in peer if ch.isdigit())
        if digits:
            return f"tg://user?id={digits}"
    return None


async def notify_manager_handoff(
    tenant_id: int,
    lead_id: int,
    reason: str | None,
    *,
    contact_hint: str | None = None,
    username_hint: str | None = None,
    deps: ManagerHandoffDeps,
) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    if not deps.notification_event_enabled_fn(tenant_id, deps.notify_event_manager):
        return
    chat_ids = deps.notification_chat_ids_fn(tenant_id, deps.notify_event_manager)
    if not chat_ids:
        return

    reason_hint = handoff_reason_hint(reason)
    chat_phone = await _lead_chat_phone(lead_id, deps=deps)
    peer_hint = _peer_hint(contact_hint)
    chat_link = build_chat_link(username_hint, chat_phone, chat_phone or peer_hint)
    text = _manager_handoff_text(lead_id, chat_phone, chat_link, reason_hint)
    deps.log_fn(
        f"event=notify_prepare tenant={tenant_id} lead_id={lead_id} reason={reason_hint} chat_ids={chat_ids}"
    )
    await deps.process_notification_fn(
        {
            "type": "notify",
            "event": deps.notify_event_manager,
            "tenant": int(tenant_id),
            "tenant_id": int(tenant_id),
            "lead_id": int(lead_id),
            "chat_ids": chat_ids,
            "text": text.strip(),
        }
    )


async def mark_handoff_silence(
    tenant_id: int,
    lead_id: int,
    *,
    reason: str | None = None,
    contact_hint: str | None = None,
    username_hint: str | None = None,
    notify: bool = True,
    deps: ManagerHandoffDeps,
) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    timestamp = int(deps.time_fn())
    try:
        await _write_handoff_silence(
            tenant_id,
            lead_id,
            reason=reason,
            timestamp=timestamp,
            deps=deps,
        )
    except Exception:
        deps.log_fn(f"event=handoff_flag_set_failed tenant={tenant_id} lead_id={lead_id}")
        return

    if notify:
        await notify_manager_handoff(
            int(tenant_id),
            int(lead_id),
            reason,
            contact_hint=contact_hint,
            username_hint=username_hint,
            deps=deps,
        )


async def is_handoff_silenced(tenant_id: int, lead_id: int, *, deps: ManagerHandoffDeps) -> bool:
    if tenant_id <= 0 or lead_id <= 0:
        return False
    try:
        return bool(await deps.redis_client.exists(deps.handoff_silence_key_fn(int(tenant_id), int(lead_id))))
    except Exception:
        return False


def handoff_reason_hint(reason: str | None) -> str:
    return "прислал файл" if reason == "photo_received" else (reason or "требуется участие менеджера")


async def _lead_chat_phone(lead_id: int, *, deps: ManagerHandoffDeps) -> str | None:
    try:
        lead_phone = await deps.get_contact_phone_by_lead_fn(int(lead_id))
    except Exception:
        lead_phone = None
    return lead_phone if isinstance(lead_phone, str) else None


def _peer_hint(contact_hint: str | None) -> str | None:
    if not contact_hint:
        return None
    digits_hint = "".join(ch for ch in str(contact_hint) if ch.isdigit())
    return digits_hint or None


def _manager_handoff_text(
    lead_id: int,
    chat_phone: str | None,
    chat_link: str | None,
    reason_hint: str,
) -> str:
    title = notification_lead_title(lead_id, chat_phone)
    if chat_link:
        return f'{title}: <a href="{chat_link}">ссылка</a> - {reason_hint}'
    return f"{title}: {reason_hint}"


async def _write_handoff_silence(
    tenant_id: int,
    lead_id: int,
    *,
    reason: str | None,
    timestamp: int,
    deps: ManagerHandoffDeps,
) -> None:
    silence_key = deps.handoff_silence_key_fn(int(tenant_id), int(lead_id))
    meta_key = deps.handoff_silence_meta_key_fn(int(tenant_id), int(lead_id))
    await deps.redis_client.set(silence_key, str(timestamp), ex=deps.handoff_silence_ttl_seconds)
    if meta_key:
        meta_payload = {"reason": reason or "unknown", "ts": timestamp}
        await deps.redis_client.set(
            meta_key,
            json.dumps(meta_payload, ensure_ascii=False),
            ex=deps.handoff_silence_ttl_seconds,
        )
