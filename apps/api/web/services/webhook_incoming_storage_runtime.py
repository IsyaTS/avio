from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from fastapi import HTTPException

from .webhook_incoming_runtime import ParsedIncoming


SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class IncomingStorageContext:
    body: Mapping[str, Any]
    msg: Mapping[str, Any]
    normalized_event: dict[str, Any]
    tenant: int
    lead_id: int
    provider: str
    message_id: str | None
    text: str
    attachments: list[dict[str, Any]]
    has_photo: bool
    whatsapp_phone: str
    contact_value: str | None
    peer_value: str | None
    peer_id: int | None
    telegram_user_id: int | None
    telegram_username: str
    telegram_display_name: str
    max_user_id: int | None
    max_username: str
    avito_account_id: int | None
    avito_chat_id: str
    avito_user_id: int | None
    avito_login: str
    avito_system_message: bool


@dataclass(frozen=True)
class IncomingStorageResult:
    lead_id: int
    contact_id: int
    message_db_id: int
    stored_incoming: bool
    refer_id: int


@dataclass(frozen=True)
class IncomingStorageDeps:
    redis_queue: Any
    json_module: Any
    upsert_lead_fn: SyncFn
    resolve_or_create_contact_fn: SyncFn
    link_lead_contact_fn: SyncFn
    insert_message_in_fn: SyncFn
    remember_whatsapp_jid_fn: SyncFn
    text_or_placeholder_fn: SyncFn
    extract_tg_slot_fn: SyncFn
    has_contact_identifiers_fn: SyncFn
    handoff_silence_key_fn: SyncFn
    handoff_silence_meta_key_fn: SyncFn
    handoff_silence_ttl_seconds: int
    logger: Any


def storage_context_from_parsed(
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    parsed: ParsedIncoming,
    *,
    tenant: int,
    normalized_event: dict[str, Any],
    peer_value: str | None = None,
) -> IncomingStorageContext:
    return IncomingStorageContext(
        body=body,
        msg=msg,
        normalized_event=normalized_event,
        tenant=tenant,
        lead_id=parsed.lead_id,
        provider=parsed.resolved_provider,
        message_id=parsed.message_id,
        text=parsed.text,
        attachments=parsed.attachments,
        has_photo=parsed.has_photo,
        whatsapp_phone=parsed.whatsapp_phone,
        contact_value=parsed.contact_value,
        peer_value=peer_value if peer_value is not None else parsed.peer_value,
        peer_id=parsed.peer_id,
        telegram_user_id=parsed.telegram_user_id,
        telegram_username=parsed.telegram_username,
        telegram_display_name=parsed.telegram_display_name,
        max_user_id=parsed.max_user_id,
        max_username=parsed.max_username,
        avito_account_id=parsed.avito_account_id,
        avito_chat_id=parsed.avito_chat_id,
        avito_user_id=parsed.avito_user_id,
        avito_login=parsed.avito_login,
        avito_system_message=parsed.avito_system_message,
    )


async def persist_incoming_side_effects(
    ctx: IncomingStorageContext,
    *,
    deps: IncomingStorageDeps,
) -> IncomingStorageResult:
    lead_id = int(ctx.lead_id)
    contact_id = 0
    stored_incoming = False
    message_db_id = 0

    resolved_lead = await _upsert_lead(ctx, deps=deps)
    if resolved_lead:
        try:
            lead_id = int(resolved_lead)
        except Exception:
            pass
        else:
            ctx.normalized_event["lead_id"] = lead_id
    deps.logger.info(
        "lead_upsert_ok tenant=%s lead_id=%s resolved=%s",
        ctx.tenant,
        lead_id,
        resolved_lead,
    )

    if ctx.provider == "whatsapp" and ctx.normalized_event.get("from_jid") and ctx.tenant and lead_id:
        await deps.remember_whatsapp_jid_fn(
            int(ctx.tenant),
            int(lead_id),
            str(ctx.normalized_event["from_jid"]),
        )

    try:
        contact_id, message_db_id, stored_incoming = await _resolve_contact_and_store(
            ctx,
            lead_id=lead_id,
            deps=deps,
        )
    except Exception:
        deps.logger.exception("contact_upsert_err tenant=%s lead_id=%s", ctx.tenant, lead_id)

    incoming_text = deps.text_or_placeholder_fn(ctx.text, ctx.attachments)
    if incoming_text and not stored_incoming:
        try:
            message_db_id = await _store_incoming_message(
                ctx,
                lead_id=lead_id,
                incoming_text=incoming_text,
                deps=deps,
            )
            if message_db_id:
                ctx.normalized_event["_message_db_id"] = message_db_id
                ctx.normalized_event["_incoming_stored"] = True
        except Exception:
            pass

    if ctx.has_photo:
        await _set_photo_handoff(ctx, lead_id=lead_id, deps=deps)
        ctx.normalized_event["handoff"] = True

    return IncomingStorageResult(
        lead_id=lead_id,
        contact_id=contact_id,
        message_db_id=message_db_id,
        stored_incoming=stored_incoming,
        refer_id=contact_id or lead_id,
    )


async def _upsert_lead(ctx: IncomingStorageContext, *, deps: IncomingStorageDeps) -> Any:
    try:
        upsert_kwargs: dict[str, Any] = {
            "channel": ctx.provider or "whatsapp",
            "tenant_id": ctx.tenant,
            "telegram_username": ctx.telegram_username,
            "title": ctx.telegram_display_name,
            "peer_id": ctx.peer_id,
            "peer": ctx.peer_value,
        }
        if not (ctx.provider == "avito" and ctx.avito_system_message):
            upsert_kwargs["contact"] = ctx.contact_value
        else:
            deps.logger.info(
                "avito_system_message_skip_metadata_update tenant=%s lead_id=%s message_id=%s",
                ctx.tenant,
                ctx.lead_id,
                ctx.message_id or "",
            )
        if ctx.telegram_user_id is not None:
            upsert_kwargs["telegram_user_id"] = int(ctx.telegram_user_id)
        if ctx.provider == "avito":
            if ctx.avito_chat_id:
                upsert_kwargs["peer"] = ctx.avito_chat_id
            if ctx.avito_account_id is not None:
                upsert_kwargs["source_real_id"] = ctx.avito_account_id
            if ctx.avito_login and not upsert_kwargs.get("title") and not ctx.avito_system_message:
                upsert_kwargs["title"] = f"Avito · {ctx.avito_login}"
        return await deps.upsert_lead_fn(ctx.lead_id, **upsert_kwargs)
    except Exception as exc:
        deps.logger.exception(
            "lead_upsert_err:db_error tenant=%s lead_id=%s message_in_lead_upsert_fail",
            ctx.tenant,
            ctx.lead_id,
        )
        raise HTTPException(status_code=500, detail="lead_upsert_failed") from exc


async def _resolve_contact_and_store(
    ctx: IncomingStorageContext,
    *,
    lead_id: int,
    deps: IncomingStorageDeps,
) -> tuple[int, int, bool]:
    telegram_phone = await _lookup_telegram_phone(ctx, lead_id=lead_id, deps=deps)
    if deps.has_contact_identifiers_fn(
        phone=telegram_phone or None,
        whatsapp_phone=ctx.whatsapp_phone or None,
        avito_user_id=ctx.avito_user_id,
        avito_login=ctx.avito_login,
        telegram_user_id=ctx.telegram_user_id,
        telegram_username=ctx.telegram_username,
        max_user_id=ctx.max_user_id,
        max_username=ctx.max_username,
    ):
        contact_id = await deps.resolve_or_create_contact_fn(
            tenant_id=ctx.tenant,
            phone=telegram_phone or None,
            whatsapp_phone=ctx.whatsapp_phone or None,
            avito_user_id=ctx.avito_user_id,
            avito_login=ctx.avito_login,
            telegram_user_id=ctx.telegram_user_id,
            telegram_username=ctx.telegram_username,
            max_user_id=ctx.max_user_id,
            max_username=ctx.max_username,
        )
    else:
        contact_id = 0
    message_db_id = 0
    stored_incoming = False
    if contact_id:
        await deps.link_lead_contact_fn(
            lead_id,
            contact_id,
            channel=ctx.provider,
            peer=ctx.peer_value if ctx.provider in {"telegram", "avito", "max"} else None,
        )
        incoming_text = deps.text_or_placeholder_fn(ctx.text, ctx.attachments)
        if incoming_text:
            message_db_id = await _store_incoming_message(
                ctx,
                lead_id=lead_id,
                incoming_text=incoming_text,
                deps=deps,
            )
            stored_incoming = True
            if message_db_id:
                ctx.normalized_event["_message_db_id"] = message_db_id
                ctx.normalized_event["_incoming_stored"] = True
    deps.logger.info(
        "stage=contact_resolved tenant=%s lead_id=%s contact_id=%s has_photo=%s text_len=%s attachments=%s",
        ctx.tenant,
        lead_id,
        contact_id,
        int(bool(ctx.has_photo)),
        len(ctx.text or ""),
        len(ctx.attachments),
    )
    return int(contact_id or 0), int(message_db_id or 0), stored_incoming


async def _lookup_telegram_phone(
    ctx: IncomingStorageContext,
    *,
    lead_id: int,
    deps: IncomingStorageDeps,
) -> str | None:
    if ctx.provider != "telegram":
        return None
    telegram_phone: str | None = None
    try:
        if lead_id:
            phone_candidate = await deps.redis_queue.get(f"cache:lead_phone:{ctx.tenant}:{lead_id}")
            if phone_candidate:
                telegram_phone = _decode_redis_value(phone_candidate)
    except Exception:
        telegram_phone = None
    if not telegram_phone and ctx.peer_value:
        try:
            phone_candidate = await deps.redis_queue.get(
                f"cache:avito_phone:{ctx.tenant}:{ctx.peer_value}"
            )
            if phone_candidate:
                telegram_phone = _decode_redis_value(phone_candidate)
        except Exception:
            telegram_phone = None
    return telegram_phone


async def _store_incoming_message(
    ctx: IncomingStorageContext,
    *,
    lead_id: int,
    incoming_text: str,
    deps: IncomingStorageDeps,
) -> int:
    incoming_source = "incoming"
    if ctx.provider == "telegram":
        slot_value = deps.extract_tg_slot_fn(ctx.msg, ctx.body)
        incoming_source = f"incoming:tg_slot:{slot_value}"
    return int(
        await deps.insert_message_in_fn(
            lead_id,
            incoming_text,
            status="received",
            tenant_id=ctx.tenant,
            telegram_user_id=ctx.telegram_user_id,
            attachments=ctx.attachments or None,
            source=incoming_source,
        )
        or 0
    )


async def _set_photo_handoff(
    ctx: IncomingStorageContext,
    *,
    lead_id: int,
    deps: IncomingStorageDeps,
) -> None:
    try:
        timestamp = int(time.time())
        await deps.redis_queue.set(
            deps.handoff_silence_key_fn(int(ctx.tenant), int(lead_id)),
            str(timestamp),
            ex=deps.handoff_silence_ttl_seconds,
        )
        meta_key = deps.handoff_silence_meta_key_fn(int(ctx.tenant), int(lead_id))
        if meta_key:
            payload = {"reason": "photo_received", "ts": timestamp}
            await deps.redis_queue.set(
                meta_key,
                deps.json_module.dumps(payload, ensure_ascii=False),
                ex=deps.handoff_silence_ttl_seconds,
            )
    except Exception:
        deps.logger.debug(
            "handoff_flag_set_failed tenant=%s lead_id=%s",
            ctx.tenant,
            lead_id,
            exc_info=True,
        )


def _decode_redis_value(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode()
    return str(value).strip()
