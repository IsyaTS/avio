from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from libs.core.common import normalize_echo_text
from libs.core.lib.numbers import coerce_int
from libs.core.transport import WhatsAppAddressError, normalize_whatsapp_recipient


@dataclass(frozen=True)
class OutboxSendContext:
    channel: str
    tenant_id: int
    lead_id: int


@dataclass(frozen=True)
class OutboxWriteResultContext:
    lead_id: int
    tenant_id: int
    channel: str
    text: str
    telegram_user_id: int | None
    peer_value: str | None
    username: str | None
    stored_message_id: int | None


@dataclass(frozen=True)
class LeadAvailabilityPlan:
    lead_ref: int
    available: bool
    needs_exists_check: bool
    exists_check_lead_id: int | None = None
    missing_reason: str | None = None


@dataclass(frozen=True)
class LearningCaptureContext:
    tenant_id: int
    lead_id: int
    channel: str
    source_event: str
    manager_message_id: int


@dataclass(frozen=True)
class OutboxAttachments:
    primary: dict[str, Any] | None
    all_items: list[dict[str, Any]]


@dataclass(frozen=True)
class SendOutcome:
    status: str
    reason: str


@dataclass(frozen=True)
class EchoTargetPlan:
    chat_hint: str | None
    needs_peer_lookup: bool


@dataclass(frozen=True)
class WhatsAppRecipientPlan:
    recipient: str
    source: str | None
    missing: bool


@dataclass(frozen=True)
class AuthRetryPlan:
    attempt: int
    body_hint: str
    should_dlq: bool
    payload: dict[str, Any]


@dataclass(frozen=True)
class WhatsAppPreparedAttachments:
    primary: dict[str, Any] | None
    all_items: list[dict[str, Any]]


@dataclass(frozen=True)
class ChannelSendResult:
    status_code: int
    body: str


@dataclass(frozen=True)
class OutboxItemPlan:
    channel: str
    text: str
    lead_id: int
    phone: str
    raw_to: Any
    to_peer_raw: Any
    peer_raw: Any
    peer_value: str | None
    username: str | None
    raw_telegram: Any
    item_tg_slot: Any
    telegram_user_id: int | None
    primary_telegram_user_id: int | None
    tenant_id: int
    attachment: dict[str, Any] | None
    attachments: list[dict[str, Any]]
    reply_to: str | None
    avito_account_id: int | None
    avito_chat_id_hint: Any
    max_user_id: int | None
    max_chat_id_hint: Any


def resolve_outbox_channel(item: Mapping[str, Any]) -> str:
    raw_channel = item.get("provider") or item.get("ch") or item.get("channel")
    channel = ""
    if isinstance(raw_channel, str):
        channel = raw_channel.strip().lower()
    elif raw_channel is not None:
        channel = str(raw_channel).strip().lower()
    if channel:
        return channel
    if item.get("max_user_id") is not None:
        return "max"
    if item.get("telegram_user_id") is not None or item.get("peer_id") is not None:
        return "telegram"
    return "whatsapp"


def resolve_outbox_tenant_id(item: Mapping[str, Any], default_tenant_id: int = 1) -> int:
    tenant_id = coerce_int(item.get("tenant_id") or item.get("tenant"))
    if tenant_id is not None:
        return int(tenant_id)
    return int(default_tenant_id)


def coerce_meta_flag(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def is_manager_message(item: Mapping[str, Any]) -> bool:
    origin_raw = item.get("origin")
    origin = origin_raw.strip().lower() if isinstance(origin_raw, str) else ""
    if origin in {"app.send", "dialogs.ui", "client.dialog"}:
        return True

    if coerce_meta_flag(item.get("manager")):
        return True

    meta = item.get("meta")
    if isinstance(meta, Mapping):
        return coerce_meta_flag(meta.get("manager"))
    return False


def is_followup_message(item: Mapping[str, Any]) -> bool:
    origin_raw = item.get("origin")
    origin = origin_raw.strip().lower() if isinstance(origin_raw, str) else ""
    if origin == "followup":
        return True

    meta = item.get("meta")
    if isinstance(meta, Mapping):
        return coerce_meta_flag(meta.get("followup"))
    return False


def build_send_context(
    item: Mapping[str, Any],
    *,
    default_tenant_id: int = 1,
) -> OutboxSendContext:
    lead_id = coerce_int(item.get("lead_id")) or 0
    return OutboxSendContext(
        channel=resolve_outbox_channel(item),
        tenant_id=resolve_outbox_tenant_id(item, default_tenant_id=default_tenant_id),
        lead_id=int(lead_id),
    )


def collect_outbox_attachments(item: Mapping[str, Any]) -> OutboxAttachments:
    primary = item.get("attachment") if isinstance(item.get("attachment"), dict) else None
    raw_attachments = item.get("attachments") if isinstance(item.get("attachments"), list) else []
    attachments: list[dict[str, Any]] = [
        dict(blob) for blob in raw_attachments if isinstance(blob, dict)
    ]
    if primary:
        attachments.append(dict(primary))
    return OutboxAttachments(primary=dict(primary) if primary else None, all_items=attachments)


def build_outbox_item_plan(
    item: Mapping[str, Any],
    *,
    default_tenant_id: int,
    normalize_tg_slot_fn: Callable[[Any], Any],
) -> OutboxItemPlan:
    channel = resolve_outbox_channel(item)
    text = str(item.get("text") or "").strip()
    lead_candidate = coerce_int(item.get("lead_id"))
    lead_id = lead_candidate if lead_candidate and lead_candidate > 0 else 0
    raw_to = item.get("to")
    to_peer_raw = item.get("to_peer")
    peer_field = item.get("peer")
    peer_raw = item.get("peer_id")
    peer_value: str | None = None
    for candidate in (to_peer_raw, peer_field, peer_raw):
        if candidate is not None and peer_value is None:
            peer_value = str(candidate).strip() or None
    if peer_raw is None and peer_value is not None:
        peer_raw = peer_value
    username_raw = item.get("username")
    username = (str(username_raw).strip() or None) if username_raw is not None else None
    raw_telegram = item.get("telegram_user_id")
    if raw_telegram is None and peer_raw is not None:
        raw_telegram = peer_raw
    telegram_user_id: int | None = None
    if raw_telegram is not None:
        try:
            candidate_id = int(raw_telegram)
        except Exception:
            telegram_user_id = None
        else:
            telegram_user_id = candidate_id if candidate_id > 0 else None
    tenant_id = resolve_outbox_tenant_id(item, default_tenant_id=default_tenant_id)
    attachment_context = collect_outbox_attachments(item)
    return OutboxItemPlan(
        channel=channel,
        text=text,
        lead_id=int(lead_id),
        phone=digits_only(raw_to),
        raw_to=raw_to,
        to_peer_raw=to_peer_raw,
        peer_raw=peer_raw,
        peer_value=peer_value,
        username=username,
        raw_telegram=raw_telegram,
        item_tg_slot=normalize_tg_slot_fn(item.get("tg_slot")),
        telegram_user_id=telegram_user_id,
        primary_telegram_user_id=telegram_user_id,
        tenant_id=int(tenant_id),
        attachment=attachment_context.primary,
        attachments=attachment_context.all_items,
        reply_to=item.get("reply_to") if isinstance(item.get("reply_to"), str) else None,
        avito_account_id=coerce_int(item.get("account_id")),
        avito_chat_id_hint=item.get("chat_id") or item.get("peer") or item.get("peer_id"),
        max_user_id=coerce_int(item.get("max_user_id") or item.get("user_id")),
        max_chat_id_hint=item.get("chat_id") or item.get("peer") or item.get("peer_id"),
    )


def build_write_result_context(
    item: Mapping[str, Any],
    *,
    default_tenant_id: int = 1,
) -> OutboxWriteResultContext:
    lead_id = int(item.get("lead_id") or 0)
    tenant_id = resolve_outbox_tenant_id(item, default_tenant_id=default_tenant_id)

    attachment = item.get("attachment") if isinstance(item.get("attachment"), dict) else None
    text = str(item.get("text") or "").strip()
    if not text and attachment:
        filename = str(attachment.get("filename") or "")
        text = f"[attachment] {filename}".strip()

    peer_value: str | None = None
    for candidate in (
        item.get("to_peer"),
        item.get("peer"),
        item.get("telegram_user_id"),
        item.get("peer_id"),
    ):
        if candidate is not None and peer_value is None:
            peer_value = str(candidate).strip() or None

    telegram_user_id = coerce_int(item.get("telegram_user_id") or item.get("peer_id"))
    if telegram_user_id is None and peer_value is not None:
        telegram_user_id = coerce_int(peer_value)

    username = item.get("username") if isinstance(item.get("username"), str) else None
    stored_message_id = coerce_int(item.get("_message_db_id"))
    if stored_message_id is not None and stored_message_id <= 0:
        stored_message_id = None

    resolved_lead_override = item.get("_resolved_lead_id")
    if isinstance(resolved_lead_override, int) and resolved_lead_override > 0:
        lead_id = resolved_lead_override

    return OutboxWriteResultContext(
        lead_id=lead_id,
        tenant_id=tenant_id,
        channel=resolve_outbox_channel(item),
        text=text,
        telegram_user_id=telegram_user_id,
        peer_value=peer_value,
        username=username,
        stored_message_id=stored_message_id,
    )


def build_status_echo_payload(
    *,
    lead_id: int,
    reply_text: str,
    status: str,
    version: str,
    item: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "lead_id": int(lead_id),
        "reply": str(reply_text or ""),
        "status": str(status or ""),
        "version": str(version or ""),
        "ch": item.get("ch") or item.get("provider") or "whatsapp",
    }


def build_send_outcome(status_code: int) -> SendOutcome:
    if 200 <= int(status_code) < 300:
        return SendOutcome(status="sent", reason="ok")
    if int(status_code) in {401, 403}:
        return SendOutcome(status="unauthorized", reason=f"status_{int(status_code)}")
    if int(status_code) == 422:
        return SendOutcome(status="skipped", reason="validation")
    if int(status_code) == 0:
        return SendOutcome(status="skipped", reason="network")
    return SendOutcome(status="skipped", reason=f"status_{int(status_code)}")


def build_telegram_chat_candidates(
    *,
    primary_telegram_user_id: int | None = None,
    db_lookup_result: int | None = None,
    from_candidate: int | None = None,
    peer_value: str | None = None,
) -> list[int]:
    candidates: list[int] = []
    for value in (primary_telegram_user_id, db_lookup_result, from_candidate):
        if value is not None and int(value) > 0:
            candidates.append(int(value))
    peer_candidate = coerce_int(peer_value)
    if peer_candidate is not None and peer_candidate > 0:
        candidates.append(int(peer_candidate))
    return candidates


def first_positive_candidate(candidates: list[int]) -> int | None:
    for candidate in candidates:
        if int(candidate) > 0:
            return int(candidate)
    return None


def resolve_telegram_peer_id(*, peer_value: str | None, peer_raw: Any) -> int | None:
    for value in (peer_value, peer_raw):
        candidate = coerce_int(value)
        if candidate is not None:
            return int(candidate)
    return None


def normalize_optional_chat_hint(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip() or None


def plan_echo_target(*, chat_hint: Any, lead_id: int) -> EchoTargetPlan:
    normalized = normalize_optional_chat_hint(chat_hint)
    return EchoTargetPlan(
        chat_hint=normalized,
        needs_peer_lookup=not bool(normalized) and int(lead_id or 0) > 0,
    )


def plan_baileys_recipient(
    *,
    explicit_to_jid: Any = None,
    cached_whatsapp_jid: Any = None,
    raw_to: Any = None,
    phone: str = "",
    normalize_jid_fn: Any,
) -> WhatsAppRecipientPlan:
    fallback = str(raw_to).strip() if isinstance(raw_to, str) and str(raw_to).strip() else str(phone or "")
    jid_sources: tuple[tuple[str, Any], ...] = (
        ("task", explicit_to_jid),
        ("cache", cached_whatsapp_jid),
        ("raw_to", raw_to if isinstance(raw_to, str) and raw_to.strip() else None),
        ("phone", phone),
    )
    for label, source in jid_sources:
        candidate_jid = normalize_jid_fn(source)
        if candidate_jid:
            return WhatsAppRecipientPlan(
                recipient=str(candidate_jid),
                source=label,
                missing=False,
            )
    if fallback and "@" not in fallback:
        normalized_fallback = normalize_jid_fn(fallback)
        if normalized_fallback:
            return WhatsAppRecipientPlan(
                recipient=str(normalized_fallback),
                source=None,
                missing=False,
            )
    return WhatsAppRecipientPlan(
        recipient=fallback,
        source=None,
        missing=not bool(fallback),
    )


def digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def normalize_baileys_jid(candidate: Any) -> str:
    if candidate is None:
        return ""
    text = str(candidate).strip()
    if not text:
        return ""
    lowered = text.lower()
    if "@" in lowered:
        return lowered
    try:
        digits, _ = normalize_whatsapp_recipient(text)
    except WhatsAppAddressError:
        digits = digits_only(text)
        if not digits:
            return ""
    return f"{digits}@s.whatsapp.net"


async def resolve_cached_whatsapp_jid(redis_client: Any, tenant_id: int, lead_id: int) -> str | None:
    if int(tenant_id or 0) <= 0 or int(lead_id or 0) <= 0:
        return None
    try:
        cached = await redis_client.hget(f"wa:jid:{int(tenant_id)}", str(int(lead_id)))
    except Exception:
        return None
    if not cached:
        return None
    text = str(cached).strip()
    return text or None


def normalize_whatsapp_peer(raw: Any) -> str | None:
    if raw is None:
        return None
    peer = str(raw).strip()
    if not peer:
        return None
    return peer.lower()


def max_bot_echo_key(tenant_id: int, channel: str, chat_key: str) -> str:
    channel_norm = str(channel or "max").strip().lower() or "max"
    chat_norm = str(chat_key or "").strip()
    return f"{channel_norm}:bot_echo:{int(tenant_id)}:{chat_norm}"


async def cache_max_bot_echo(
    redis_client: Any,
    *,
    tenant_id: int,
    channel: str,
    chat_key: str | None,
    text: str | None,
    ttl_seconds: int,
) -> bool:
    normalized = normalize_echo_text(text or "")
    key_chat = str(chat_key or "").strip()
    if not normalized or not key_chat:
        return False
    payload = {"text": normalized, "ts": int(time.time())}
    await redis_client.set(
        max_bot_echo_key(int(tenant_id), str(channel or ""), key_chat),
        json.dumps(payload, ensure_ascii=False),
        ex=int(ttl_seconds),
    )
    return True


async def is_recent_max_bot_echo(
    redis_client: Any,
    *,
    tenant_id: int,
    channel: str,
    chat_key: str | None,
    text: str | None,
) -> bool:
    normalized = normalize_echo_text(text or "")
    key_chat = str(chat_key or "").strip()
    if not normalized or not key_chat:
        return False
    try:
        raw = await redis_client.get(max_bot_echo_key(int(tenant_id), str(channel or ""), key_chat))
    except Exception:
        return False
    if not raw:
        return False
    try:
        payload = json.loads(raw) if isinstance(raw, str) else {}
    except Exception:
        payload = {}
    cached = normalize_echo_text(str((payload or {}).get("text") or ""))
    return bool(cached and cached == normalized)


def plan_waweb_auth_retry(
    item: Mapping[str, Any],
    *,
    body: str,
    max_attempts: int = 3,
) -> AuthRetryPlan:
    try:
        retry_count = int(item.get("_waweb_auth_retry") or 0)
    except Exception:
        retry_count = 0
    attempt = retry_count + 1
    body_hint = str(body or "").strip()
    if len(body_hint) > 400:
        body_hint = f"{body_hint[:400]}…"
    payload = dict(item)
    payload["_waweb_auth_retry"] = attempt
    return AuthRetryPlan(
        attempt=attempt,
        body_hint=body_hint,
        should_dlq=attempt >= int(max_attempts),
        payload=payload,
    )


async def prepare_whatsapp_attachments(
    *,
    primary: Mapping[str, Any] | None,
    attachments: list[dict[str, Any]],
    prepare_attachment_fn: Callable[[Mapping[str, Any]], Awaitable[dict[str, Any]]],
) -> WhatsAppPreparedAttachments:
    prepared_primary = await prepare_attachment_fn(primary) if primary else None
    prepared_items: list[dict[str, Any]] = []
    for blob in attachments:
        if not isinstance(blob, Mapping):
            continue
        if primary is not None and blob is primary:
            prepared_items.append(dict(prepared_primary or blob))
            continue
        prepared_blob = await prepare_attachment_fn(blob)
        prepared_items.append(dict(prepared_blob))
    return WhatsAppPreparedAttachments(
        primary=dict(prepared_primary) if prepared_primary else None,
        all_items=prepared_items,
    )


def build_outbound_attachment_snapshot(
    *,
    primary: Mapping[str, Any] | None,
    attachments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    snapshot: list[dict[str, Any]] = []
    for item in attachments:
        if isinstance(item, Mapping):
            snapshot.append(dict(item))
    if isinstance(primary, Mapping):
        snapshot.append(dict(primary))
    return snapshot


async def run_max_send_with_echo(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    text: str,
    chat_hint: Any,
    manager_message: bool,
    send_fn: Callable[[str | None], Awaitable[tuple[int, str]]],
    get_lead_peer_fn: Callable[[int, str], Awaitable[Any]],
    cache_echo_fn: Callable[[int, str, str | None, str], Awaitable[None]],
) -> ChannelSendResult:
    echo_plan = plan_echo_target(chat_hint=chat_hint, lead_id=lead_id)
    normalized_chat_hint = echo_plan.chat_hint

    async def _resolve_echo_chat() -> str | None:
        if not echo_plan.needs_peer_lookup:
            return normalized_chat_hint
        try:
            return str(await get_lead_peer_fn(int(lead_id), channel) or "").strip() or None
        except Exception:
            return None

    if not manager_message and text:
        await cache_echo_fn(int(tenant_id), channel, await _resolve_echo_chat(), text)

    status_code, body = await send_fn(normalized_chat_hint)

    if 200 <= int(status_code) < 300 and not manager_message and text:
        await cache_echo_fn(int(tenant_id), channel, await _resolve_echo_chat(), text)

    return ChannelSendResult(status_code=int(status_code), body=str(body or ""))


async def run_avito_send_with_echo(
    *,
    lead_id: int,
    text: str,
    chat_hint: Any,
    has_attachments: bool,
    manager_message: bool,
    send_fn: Callable[[str | None], Awaitable[tuple[int, str]]],
    get_lead_peer_fn: Callable[[int, str], Awaitable[Any]],
    cache_echo_fn: Callable[[str, dict[str, Any], str], Awaitable[None]],
) -> ChannelSendResult:
    normalized_chat_hint = normalize_optional_chat_hint(chat_hint)
    payload = None
    if not manager_message:
        payload = build_echo_cache_payload(
            normalized_text=normalize_echo_text(text or ""),
            has_attachments=has_attachments,
            timestamp=int(time.time()),
        )

    async def _resolve_chat_key() -> str:
        if normalized_chat_hint:
            return normalized_chat_hint
        try:
            return str(await get_lead_peer_fn(int(lead_id), "avito") or "").strip()
        except Exception:
            return ""

    if payload is not None:
        chat_key = await _resolve_chat_key()
        if chat_key:
            await cache_echo_fn(chat_key, payload, "pre")

    status_code, body = await send_fn(normalized_chat_hint)

    if 200 <= int(status_code) < 300 and payload is not None:
        chat_key = await _resolve_chat_key()
        if chat_key:
            await cache_echo_fn(chat_key, payload, "post")

    return ChannelSendResult(status_code=int(status_code), body=str(body or ""))


def normalize_telegram_title(
    *,
    title: Any,
    username: str | None,
    telegram_user_id: int,
    normalize_username_fn: Any,
) -> str:
    title_hint: str | None = None
    if isinstance(title, str):
        normalized_title = title.strip() or ""
        if normalized_title:
            import re

            legacy_username = re.fullmatch(r"(?i)tg:\s*@?([a-z0-9_]{3,})", normalized_title)
            if legacy_username:
                title_hint = f"@{legacy_username.group(1)}"
            elif re.fullmatch(r"(?i)tg:id\s+\d+", normalized_title):
                title_hint = None
            else:
                title_hint = normalized_title
    normalized_username = normalize_username_fn(username)
    if title_hint:
        return title_hint
    if normalized_username:
        return str(normalized_username)
    return f"tg:id {int(telegram_user_id)}"


def build_echo_cache_payload(
    *,
    normalized_text: str,
    has_attachments: bool = False,
    timestamp: int,
) -> dict[str, Any] | None:
    text_value = str(normalized_text or "").strip()
    variants: list[str] = []
    if text_value:
        variants.append(text_value)
    if has_attachments:
        variants.append("__image__")
    if not text_value and has_attachments:
        text_value = "__image__"
    if not text_value:
        return None
    return {
        "text": text_value,
        "extra": variants,
        "ts": int(timestamp),
    }


def build_lead_upsert_kwargs(context: OutboxWriteResultContext) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "channel": context.channel,
        "source_real_id": None,
        "tenant_id": context.tenant_id,
        "telegram_username": context.username,
        "peer_id": context.telegram_user_id,
        "peer": context.peer_value,
        "contact": context.username,
    }
    if context.telegram_user_id is not None:
        kwargs["telegram_user_id"] = int(context.telegram_user_id)
    return kwargs


def build_outgoing_message_source(
    *,
    channel: str,
    tg_slot: Any = None,
    is_manager: bool = False,
    is_followup: bool = False,
) -> str:
    role = "followup" if is_followup else ("manager" if is_manager else "bot")
    channel_name = str(channel or "").strip().lower()
    if channel_name != "telegram":
        return role
    slot_value = coerce_int(tg_slot)
    if slot_value is None or slot_value < 1:
        slot_value = 1
    if slot_value > 3:
        slot_value = 3
    return f"{role}:tg_slot:{slot_value}"


def build_insert_message_out_kwargs(
    *,
    context: OutboxWriteResultContext,
    status: str,
    is_manager: bool = False,
    is_followup: bool = False,
    attachments: list[dict[str, Any]] | None = None,
    tg_slot: Any = None,
) -> dict[str, Any]:
    return {
        "status": str(status or ""),
        "tenant_id": context.tenant_id,
        "channel": context.channel,
        "telegram_user_id": context.telegram_user_id,
        "telegram_username": context.username,
        "is_bot": not (is_manager or is_followup),
        "attachments": attachments or None,
        "source": build_outgoing_message_source(
            channel=context.channel,
            tg_slot=tg_slot,
            is_manager=is_manager,
            is_followup=is_followup,
        ),
    }


def plan_lead_availability(
    *,
    lead_id: int,
    resolved_lead_id: int | None,
    stored_message_id: int | None,
) -> LeadAvailabilityPlan:
    lead_ref = int(resolved_lead_id or lead_id)
    if stored_message_id:
        return LeadAvailabilityPlan(
            lead_ref=lead_ref,
            available=True,
            needs_exists_check=False,
        )
    if resolved_lead_id:
        return LeadAvailabilityPlan(
            lead_ref=lead_ref,
            available=False,
            needs_exists_check=True,
            exists_check_lead_id=int(resolved_lead_id),
        )
    return LeadAvailabilityPlan(
        lead_ref=lead_ref,
        available=False,
        needs_exists_check=False,
        missing_reason="lead_upsert_missing",
    )


def build_learning_capture_context(
    *,
    tenant_id: int,
    lead_ref: int | None,
    channel: str,
    is_manager: bool,
    stored_message_id: int | None,
) -> LearningCaptureContext | None:
    if not is_manager or not lead_ref or not stored_message_id:
        return None
    return LearningCaptureContext(
        tenant_id=int(tenant_id),
        lead_id=int(lead_ref),
        channel=str(channel or ""),
        source_event="manager_outgoing",
        manager_message_id=int(stored_message_id),
    )


def base_channel_reply_payload(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    context: Mapping[str, Any],
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    channel_name = str(channel or "").strip().lower()
    if not channel_name:
        return None
    payload: dict[str, Any] = {
        "lead_id": int(lead_id),
        "tenant": int(tenant_id),
        "tenant_id": int(tenant_id),
        "provider": channel_name,
        "ch": channel_name,
        "channel": channel_name,
        "attachments": attachments or [],
    }

    if channel_name == "telegram":
        message_id = context.get("message_id")
        telegram_user_id = coerce_int(context.get("telegram_user_id"))
        peer_id = coerce_int(context.get("peer_id"))
        username = context.get("username")
        tg_slot = context.get("tg_slot")
        if tg_slot is not None:
            payload["tg_slot"] = tg_slot
        if message_id:
            payload["message_id"] = str(message_id)
        if telegram_user_id is not None:
            payload["telegram_user_id"] = str(telegram_user_id)
        if peer_id is not None:
            payload["peer_id"] = int(peer_id)
        if isinstance(username, str) and username.strip():
            payload["username"] = username.strip()
    elif channel_name in {"max", "max_personal"}:
        message_id = context.get("message_id")
        max_user_id = coerce_int(context.get("max_user_id"))
        peer_value = context.get("peer")
        if message_id:
            payload["message_id"] = str(message_id)
        if max_user_id is not None:
            payload["max_user_id"] = max_user_id
        if isinstance(peer_value, str) and peer_value.strip():
            payload["peer"] = peer_value.strip()
            payload["peer_id"] = peer_value.strip()
    elif channel_name == "whatsapp":
        message_id = context.get("message_id")
        to_value = str(context.get("to") or "").strip()
        to_jid = str(context.get("to_jid") or "").strip()
        payload["to"] = to_value
        if to_jid:
            payload["to_jid"] = to_jid
        if message_id:
            payload["message_id"] = str(message_id)
    elif channel_name == "avito":
        chat_id = str(context.get("chat_id") or "").strip()
        if not chat_id:
            return None
        payload["chat_id"] = chat_id
        payload["peer"] = chat_id
        payload["peer_id"] = chat_id
        account_id = coerce_int(context.get("account_id"))
        message_id = context.get("message_id")
        avito_user_id = coerce_int(context.get("avito_user_id"))
        avito_login = context.get("avito_login")
        if account_id is not None:
            payload["account_id"] = account_id
        if message_id:
            payload["message_id"] = str(message_id)
        if avito_user_id is not None:
            payload["avito_user_id"] = avito_user_id
        if isinstance(avito_login, str) and avito_login.strip():
            payload["avito_login"] = avito_login.strip()
    return payload


def avito_auto_reply_payload(
    *,
    tenant_id: int,
    lead_id: int,
    chat_id: str,
    text: str,
    account_id: int | None = None,
    user_id: int | None = None,
    login: str | None = None,
    message_id: str | None = None,
) -> dict[str, Any] | None:
    text_value = str(text or "").strip()
    chat_value = str(chat_id or "").strip()
    if not text_value or not chat_value:
        return None
    payload = base_channel_reply_payload(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        channel="avito",
        context={
            "chat_id": chat_value,
            "account_id": account_id,
            "message_id": message_id,
            "avito_user_id": user_id,
            "avito_login": login,
        },
        attachments=[],
    )
    if payload is None:
        return None
    payload["text"] = text_value
    return payload


__all__ = [
    "OutboxSendContext",
    "OutboxWriteResultContext",
    "LeadAvailabilityPlan",
    "LearningCaptureContext",
    "EchoTargetPlan",
    "AuthRetryPlan",
    "OutboxAttachments",
    "WhatsAppRecipientPlan",
    "WhatsAppPreparedAttachments",
    "ChannelSendResult",
    "OutboxItemPlan",
    "avito_auto_reply_payload",
    "base_channel_reply_payload",
    "build_echo_cache_payload",
    "build_send_context",
    "build_send_outcome",
    "build_telegram_chat_candidates",
    "build_lead_upsert_kwargs",
    "first_positive_candidate",
    "build_insert_message_out_kwargs",
    "build_learning_capture_context",
    "build_outbox_item_plan",
    "build_outgoing_message_source",
    "build_outbound_attachment_snapshot",
    "build_status_echo_payload",
    "build_write_result_context",
    "cache_max_bot_echo",
    "collect_outbox_attachments",
    "digits_only",
    "plan_lead_availability",
    "is_recent_max_bot_echo",
    "max_bot_echo_key",
    "normalize_baileys_jid",
    "normalize_telegram_title",
    "normalize_optional_chat_hint",
    "normalize_whatsapp_peer",
    "plan_echo_target",
    "plan_baileys_recipient",
    "plan_waweb_auth_retry",
    "prepare_whatsapp_attachments",
    "resolve_cached_whatsapp_jid",
    "resolve_outbox_channel",
    "resolve_outbox_tenant_id",
    "resolve_telegram_peer_id",
    "run_avito_send_with_echo",
    "run_max_send_with_echo",
]
