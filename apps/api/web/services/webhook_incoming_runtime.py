from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import HTTPException


SyncFn = Callable[..., Any]
AsyncFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class IncomingBase:
    src: Mapping[str, Any]
    provider: str
    tenant: int
    msg: dict[str, Any]
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class ManagerFlags:
    manager: bool
    out: bool
    provider_raw: Mapping[str, Any]


@dataclass(frozen=True)
class IncomingRuntimeDeps:
    coerce_int_fn: SyncFn
    getenv_fn: SyncFn
    as_mapping_fn: SyncFn
    logger: Any


@dataclass(frozen=True)
class IncomingParseDeps:
    json_module: Any
    normalize_attachments_fn: SyncFn
    has_photo_attachment_fn: SyncFn
    as_mapping_fn: SyncFn
    sanitize_display_name_fn: SyncFn
    is_human_readable_name_fn: SyncFn
    is_manager_telegram_fn: SyncFn
    is_manager_whatsapp_fn: SyncFn
    is_avito_system_message_fn: SyncFn
    coerce_int_fn: SyncFn
    digits_fn: SyncFn
    avito_module: Any
    logger: Any


@dataclass(frozen=True)
class ParsedIncoming:
    message_id: str
    text: str
    attachments: list[dict[str, Any]]
    has_photo: bool
    manager_flag: bool
    out_flag: bool
    provider_raw: Mapping[str, Any]
    whatsapp_phone: str
    telegram_user_id: int | None
    telegram_username: str
    telegram_display_name: str
    max_user_id: int | None
    max_username: str
    max_display_name: str
    peer_id: int | None
    peer_value: str | None
    contact_value: str | None
    avito_user_id: int | None
    avito_login: str
    avito_chat_id: str
    avito_account_id: int | None
    avito_item_id: int | None
    avito_system_message: bool
    lead_id: int
    channel: str
    resolved_provider: str
    peer_for_log: str


@dataclass(frozen=True)
class IncomingEnvelopeContext:
    body: Mapping[str, Any]
    msg: Mapping[str, Any]
    tenant: int
    lead_id: int
    channel: str
    provider: str
    message_id: str | None
    text: str
    attachments: list[dict[str, Any]]
    manager_flag: bool
    out_flag: bool
    provider_raw: Mapping[str, Any]
    peer_value: str | None
    contact_value: str | None
    telegram_user_id: int | None
    telegram_username: str
    telegram_display_name: str
    max_user_id: int | None
    max_username: str
    max_display_name: str
    peer_id: int | None
    whatsapp_phone: str
    avito_account_id: int | None
    avito_item_id: int | None
    avito_chat_id: str
    avito_user_id: int | None
    avito_login: str


@dataclass(frozen=True)
class IncomingEnvelope:
    event: dict[str, Any]
    peer_value: str | None


@dataclass(frozen=True)
class IncomingEnvelopeDeps:
    build_envelope_fn: SyncFn
    extract_tg_slot_fn: SyncFn
    digits_fn: SyncFn


@dataclass(frozen=True)
class IncomingEventQueueDeps:
    redis_queue: Any
    incoming_queue_key: str
    push_json_left_fn: Callable[..., Any]
    json_safe_fn: SyncFn
    logger: Any


@dataclass(frozen=True)
class TextReplyContext:
    body: Mapping[str, Any]
    msg: Mapping[str, Any]
    lead_id: int
    tenant: int
    provider: str
    resolved_provider: str
    message_id: str | None
    attachments: list[dict[str, Any]]
    telegram_user_id: int | None
    peer_value: str | None
    peer_id: int | None
    whatsapp_phone: str
    sender_jid_value: str | None
    refer_id: int
    cfg: Mapping[str, Any] | None


@dataclass(frozen=True)
class TextReplyDeps:
    redis_queue: Any
    outbox_queue_key: str
    push_json_left_fn: Callable[..., Any]
    extract_tg_slot_fn: SyncFn
    assign_whatsapp_to_jid_fn: SyncFn
    core_module: Any


@dataclass(frozen=True)
class PhotoHandoffEnqueueContext:
    channel: str
    tenant: int
    lead_id: int
    attachments: list[dict[str, Any]]
    normalized_event: Mapping[str, Any]


@dataclass(frozen=True)
class PhotoHandoffEnqueueDeps:
    redis_queue: Any
    incoming_queue_key: str
    push_json_left_fn: Callable[..., Any]
    json_safe_fn: SyncFn
    logger: Any


@dataclass(frozen=True)
class IncomingGuardDeps:
    content_fingerprint_fn: SyncFn
    is_duplicate_fn: AsyncFn
    logger: Any


@dataclass(frozen=True)
class IncomingPostCatalogContext:
    body: Mapping[str, Any]
    msg: Mapping[str, Any]
    parsed: ParsedIncoming
    tenant: int
    lead_id: int
    refer_id: int
    cfg: Mapping[str, Any] | None
    sender_jid_value: str | None
    normalized_event: Mapping[str, Any]
    channel: str
    peer_value: str | None = None


@dataclass(frozen=True)
class IncomingPostCatalogDeps:
    redis_queue: Any
    handoff_silence_key_fn: SyncFn
    default_fallback_reply_fn: SyncFn
    smart_reply_enabled_fn: SyncFn
    queue_text_reply_deps: TextReplyDeps
    photo_handoff_enqueue_deps: PhotoHandoffEnqueueDeps
    logger: Any


@dataclass(frozen=True)
class IncomingPostCatalogResult:
    response_payload: dict[str, Any]
    auto_reply_handled: bool = False
    enqueue_regular: bool = False


def envelope_context_from_parsed(
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    parsed: ParsedIncoming,
    *,
    tenant: int,
) -> IncomingEnvelopeContext:
    return IncomingEnvelopeContext(
        body=body,
        msg=msg,
        tenant=tenant,
        lead_id=parsed.lead_id,
        channel=parsed.channel,
        provider=parsed.resolved_provider,
        message_id=parsed.message_id,
        text=parsed.text,
        attachments=parsed.attachments,
        manager_flag=parsed.manager_flag,
        out_flag=parsed.out_flag,
        provider_raw=parsed.provider_raw,
        peer_value=parsed.peer_value,
        contact_value=parsed.contact_value,
        telegram_user_id=parsed.telegram_user_id,
        telegram_username=parsed.telegram_username,
        telegram_display_name=parsed.telegram_display_name,
        max_user_id=parsed.max_user_id,
        max_username=parsed.max_username,
        max_display_name=parsed.max_display_name,
        peer_id=parsed.peer_id,
        whatsapp_phone=parsed.whatsapp_phone,
        avito_account_id=parsed.avito_account_id,
        avito_item_id=parsed.avito_item_id,
        avito_chat_id=parsed.avito_chat_id,
        avito_user_id=parsed.avito_user_id,
        avito_login=parsed.avito_login,
    )


def text_reply_context_from_parsed(
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    parsed: ParsedIncoming,
    *,
    tenant: int,
    lead_id: int | None = None,
    refer_id: int,
    cfg: Mapping[str, Any] | None,
    sender_jid_value: str | None,
    peer_value: str | None = None,
) -> TextReplyContext:
    return TextReplyContext(
        body=body,
        msg=msg,
        lead_id=lead_id if lead_id is not None else parsed.lead_id,
        tenant=tenant,
        provider=parsed.resolved_provider,
        resolved_provider=parsed.resolved_provider,
        message_id=parsed.message_id,
        attachments=parsed.attachments,
        telegram_user_id=parsed.telegram_user_id,
        peer_value=peer_value if peer_value is not None else parsed.peer_value,
        peer_id=parsed.peer_id,
        whatsapp_phone=parsed.whatsapp_phone,
        sender_jid_value=sender_jid_value,
        refer_id=refer_id,
        cfg=cfg,
    )


async def pre_reply_guard(
    parsed: ParsedIncoming,
    *,
    tenant: int,
    deps: IncomingGuardDeps,
) -> dict[str, Any] | None:
    if not parsed.text and not parsed.has_photo and parsed.resolved_provider != "telegram":
        return {"skipped": True, "reason": "no_text"}
    incoming_fp = deps.content_fingerprint_fn(parsed.text, parsed.attachments)
    provider = parsed.resolved_provider
    if provider not in {"telegram", "avito", "max", "max_personal"}:
        _log_pre_reply(parsed, tenant=tenant, deps=deps)
        return None
    if await deps.is_duplicate_fn(provider, tenant, parsed.message_id or None, fingerprint=incoming_fp):
        deps.logger.info(
            "stage=incoming_duplicate ch=%s tenant=%s message_id=%s",
            provider,
            tenant,
            parsed.message_id,
        )
        return {"skipped": True, "reason": "duplicate"}
    _log_pre_reply(parsed, tenant=tenant, deps=deps)
    return None


async def run_post_catalog_flow(
    ctx: IncomingPostCatalogContext,
    *,
    deps: IncomingPostCatalogDeps,
) -> IncomingPostCatalogResult:
    handoff_result = await _handoff_silence_result(ctx, deps=deps)
    if handoff_result is not None:
        return handoff_result
    if ctx.parsed.resolved_provider in {"max", "max_personal"}:
        return IncomingPostCatalogResult(
            {"queued": True, "leadId": ctx.lead_id, "smartReply": False},
            enqueue_regular=True,
        )
    if not deps.smart_reply_enabled_fn(ctx.tenant):
        return await _fallback_reply_result(ctx, deps=deps)
    if ctx.parsed.resolved_provider == "telegram":
        deps.logger.info(
            "event=smart_reply_deferred tenant=%s channel=%s lead_id=%s",
            ctx.tenant,
            ctx.parsed.resolved_provider,
            ctx.lead_id,
        )
    if ctx.parsed.has_photo:
        return await _photo_handoff_result(ctx, deps=deps)
    return IncomingPostCatalogResult(
        {"queued": False, "leadId": ctx.lead_id, "smartReply": True},
        enqueue_regular=True,
    )


async def _handoff_silence_result(
    ctx: IncomingPostCatalogContext,
    *,
    deps: IncomingPostCatalogDeps,
) -> IncomingPostCatalogResult | None:
    try:
        if await deps.redis_queue.exists(deps.handoff_silence_key_fn(int(ctx.tenant), int(ctx.lead_id))):
            deps.logger.info(
                "event=smart_reply_silenced tenant=%s channel=%s lead_id=%s",
                ctx.tenant,
                ctx.parsed.resolved_provider,
                ctx.lead_id,
            )
            return IncomingPostCatalogResult(
                {"queued": False, "leadId": ctx.lead_id, "smartReply": False, "handoff": True},
                enqueue_regular=True,
            )
    except Exception:
        deps.logger.debug(
            "handoff_check_failed tenant=%s lead_id=%s",
            ctx.tenant,
            ctx.lead_id,
            exc_info=True,
        )
    return None


async def _fallback_reply_result(
    ctx: IncomingPostCatalogContext,
    *,
    deps: IncomingPostCatalogDeps,
) -> IncomingPostCatalogResult:
    deps.logger.info(
        "event=smart_reply_disabled tenant=%s channel=%s lead_id=%s",
        ctx.tenant,
        ctx.parsed.resolved_provider,
        ctx.lead_id,
    )
    fallback_sent = await queue_text_reply(
        deps.default_fallback_reply_fn(),
        text_reply_context_from_parsed(
            ctx.body,
            ctx.msg,
            ctx.parsed,
            tenant=ctx.tenant,
            lead_id=ctx.lead_id,
            refer_id=ctx.refer_id,
            cfg=ctx.cfg,
            sender_jid_value=ctx.sender_jid_value,
            peer_value=ctx.peer_value,
        ),
        deps=deps.queue_text_reply_deps,
    )
    return IncomingPostCatalogResult(
        {"queued": bool(fallback_sent), "leadId": ctx.lead_id, "smartReply": False},
        auto_reply_handled=True,
        enqueue_regular=True,
    )


async def _photo_handoff_result(
    ctx: IncomingPostCatalogContext,
    *,
    deps: IncomingPostCatalogDeps,
) -> IncomingPostCatalogResult:
    await enqueue_photo_handoff_event(
        PhotoHandoffEnqueueContext(
            channel=ctx.channel,
            tenant=ctx.tenant,
            lead_id=ctx.lead_id,
            attachments=ctx.parsed.attachments,
            normalized_event=ctx.normalized_event,
        ),
        deps=deps.photo_handoff_enqueue_deps,
    )
    return IncomingPostCatalogResult(
        {"queued": True, "leadId": ctx.lead_id, "smartReply": False, "handoff": True}
    )


def post_catalog_context_from_parsed(
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    parsed: ParsedIncoming,
    *,
    tenant: int,
    lead_id: int,
    refer_id: int,
    cfg: Mapping[str, Any] | None,
    sender_jid_value: str | None,
    normalized_event: Mapping[str, Any],
    channel: str,
    peer_value: str | None,
) -> IncomingPostCatalogContext:
    return IncomingPostCatalogContext(
        body=body,
        msg=msg,
        parsed=parsed,
        tenant=tenant,
        lead_id=lead_id,
        refer_id=refer_id,
        cfg=cfg,
        sender_jid_value=sender_jid_value,
        normalized_event=normalized_event,
        channel=channel,
        peer_value=peer_value,
    )


class IncomingEventEnqueuer:
    def __init__(
        self,
        *,
        normalized_event: Mapping[str, Any],
        channel: str,
        tenant: int,
        deps: IncomingEventQueueDeps,
    ) -> None:
        self._normalized_event = normalized_event
        self._channel = channel
        self._tenant = tenant
        self._deps = deps
        self.enqueued = False

    async def enqueue(self, *, auto_reply_handled: bool = False) -> None:
        if self.enqueued:
            return
        payload = dict(self._normalized_event)
        if auto_reply_handled:
            payload["auto_reply_handled"] = True
        else:
            payload.pop("auto_reply_handled", None)
        try:
            payload = self._deps.json_safe_fn(payload)
            self._deps.logger.info(
                "stage=incoming_enqueue_attempt ch=%s tenant=%s message_id=%s attachments=%s",
                self._channel,
                self._tenant,
                payload.get("message_id") or "",
                len(payload.get("attachments") or []),
            )
            await self._deps.push_json_left_fn(
                self._deps.redis_queue,
                self._deps.incoming_queue_key,
                payload,
            )
            if self._channel in {"telegram", "whatsapp", "avito", "max", "max_personal"}:
                await self._deps.redis_queue.incrby(f"metrics:{self._channel}:incoming", 1)
            self._deps.logger.info(
                "stage=incoming_enqueued ch=%s tenant=%s message_id=%s",
                self._channel,
                self._tenant,
                payload.get("message_id") or "",
            )
            self.enqueued = True
        except Exception:
            self._deps.logger.exception(
                "stage=incoming_enqueue_failed ch=%s tenant=%s",
                self._channel,
                self._tenant,
            )


def resolve_incoming_base(body: Mapping[str, Any], *, deps: IncomingRuntimeDeps) -> IncomingBase:
    src = body.get("source") if isinstance(body.get("source"), Mapping) else {}
    provider = (
        src.get("type")
        or body.get("provider")
        or body.get("channel")
        or body.get("ch")
        or "whatsapp"
    )
    provider = str(provider).lower()
    raw_tenant = src.get("tenant") or body.get("tenant_id") or deps.getenv_fn("TENANT_ID", "1")
    tenant_candidate = deps.coerce_int_fn(raw_tenant)
    if tenant_candidate is None:
        deps.logger.warning(
            "lead_upsert_err:invalid_tenant message_in_lead_upsert_fail tenant_raw=%s",
            raw_tenant,
        )
        raise HTTPException(status_code=400, detail="invalid_tenant")
    msg_raw = body.get("message")
    msg = dict(msg_raw) if isinstance(msg_raw, Mapping) else {}
    payload = body.get("payload") if isinstance(body.get("payload"), Mapping) else {}
    return IncomingBase(
        src=src,
        provider=provider,
        tenant=int(tenant_candidate),
        msg=msg,
        payload=payload,
    )


def _log_pre_reply(parsed: ParsedIncoming, *, tenant: int, deps: IncomingGuardDeps) -> None:
    deps.logger.info(
        "stage=pre_reply_checks ch=%s tenant=%s lead_id=%s msg=%s has_photo=%s attachments=%s text_len=%s",
        parsed.channel,
        tenant,
        parsed.lead_id,
        parsed.message_id or "",
        int(bool(parsed.has_photo)),
        len(parsed.attachments),
        len(parsed.text or ""),
    )


async def queue_text_reply(
    text: str,
    ctx: TextReplyContext,
    *,
    deps: TextReplyDeps,
    attachments: list[dict[str, Any]] | None = None,
) -> bool:
    cleaned = str(text or "").strip()
    if not cleaned:
        return False
    out: dict[str, Any] = {
        "lead_id": ctx.lead_id,
        "text": cleaned,
        "provider": ctx.resolved_provider,
        "ch": ctx.resolved_provider,
        "tenant_id": int(ctx.tenant),
        "tenant": int(ctx.tenant),
        "message_id": ctx.message_id or str(ctx.lead_id),
        "attachments": attachments or [],
    }
    if ctx.resolved_provider == "telegram":
        out["tg_slot"] = deps.extract_tg_slot_fn(ctx.msg, ctx.body)
        if ctx.telegram_user_id:
            out["telegram_user_id"] = int(ctx.telegram_user_id)
        if ctx.peer_value:
            out["peer"] = ctx.peer_value
        if ctx.peer_id is not None:
            out["peer_id"] = int(ctx.peer_id)
        if not out.get("telegram_user_id") and not out.get("peer"):
            return False
    else:
        if not ctx.whatsapp_phone:
            return False
        out["to"] = ctx.whatsapp_phone
        deps.assign_whatsapp_to_jid_fn(out, ctx.resolved_provider, ctx.sender_jid_value)
    await deps.push_json_left_fn(deps.redis_queue, deps.outbox_queue_key, out)
    try:
        deps.core_module.record_bot_reply(
            ctx.refer_id,
            ctx.tenant,
            ctx.provider,
            cleaned,
            tenant_cfg=ctx.cfg,
        )
    except Exception:
        pass
    return True


async def enqueue_photo_handoff_event(
    ctx: PhotoHandoffEnqueueContext,
    *,
    deps: PhotoHandoffEnqueueDeps,
) -> None:
    deps.logger.info(
        "event=handoff_enqueue_has_photo ch=%s tenant=%s lead_id=%s attachments=%s keys=%s",
        ctx.channel,
        ctx.tenant,
        ctx.lead_id,
        len(ctx.attachments),
        list(ctx.normalized_event.keys()),
    )
    try:
        await deps.push_json_left_fn(
            deps.redis_queue,
            deps.incoming_queue_key,
            deps.json_safe_fn(ctx.normalized_event),
        )
        if ctx.channel in {"telegram", "max", "max_personal", "avito", "whatsapp"}:
            await deps.redis_queue.incrby(f"metrics:{ctx.channel}:incoming", 1)
        deps.logger.info(
            "stage=incoming_enqueued_photo ch=%s tenant=%s message_id=%s",
            ctx.channel,
            ctx.tenant,
            ctx.normalized_event.get("message_id") or "",
        )
    except Exception as exc:
        deps.logger.exception(
            "stage=incoming_enqueue_photo_failed ch=%s tenant=%s",
            ctx.channel,
            ctx.tenant,
        )
        raise HTTPException(status_code=500, detail="queue_error") from exc


def detect_manager_flags(
    body: Mapping[str, Any],
    base: IncomingBase,
    *,
    deps: IncomingRuntimeDeps,
) -> ManagerFlags:
    manager_flag, out_flag = _top_level_manager_out(body)
    if base.provider == "telegram":
        deps.logger.info(
            "manager_diag provider=telegram tenant=%s body_keys=%s msg_keys=%s manager=%s out=%s origin=%s",
            base.tenant,
            ",".join(sorted(body.keys())),
            ",".join(sorted(base.msg.keys())) if isinstance(base.msg, Mapping) else "",
            body.get("manager") or base.msg.get("manager"),
            body.get("out") or base.msg.get("out"),
            body.get("origin") or base.msg.get("origin"),
        )
    manager_flag = manager_flag or _has_truthy(
        body.get("manager"),
        base.msg.get("manager"),
        base.src.get("manager"),
    )
    origin_raw = body.get("origin") or base.msg.get("origin") or base.src.get("origin")
    if isinstance(origin_raw, str) and origin_raw.startswith(("telegram:manager", "max_personal:manager")):
        manager_flag = True

    provider_raw = (
        deps.as_mapping_fn(base.msg.get("provider_raw"))
        or deps.as_mapping_fn(body.get("provider_raw"))
        or {}
    )
    manager_flag, out_flag = _provider_raw_manager_flags(provider_raw, manager_flag, out_flag)
    message_obj = base.msg.get("message") if isinstance(base.msg.get("message"), Mapping) else {}
    manager_flag, out_flag = _message_obj_manager_flags(message_obj, manager_flag, out_flag)
    if not manager_flag:
        manager_flag = _has_truthy(
            body.get("out"),
            base.msg.get("out"),
            base.src.get("out"),
            base.src.get("outgoing"),
        )
    return ManagerFlags(manager=manager_flag, out=out_flag, provider_raw=provider_raw)


def parse_incoming_payload(
    body: dict[str, Any],
    base: IncomingBase,
    flags: ManagerFlags,
    *,
    deps: IncomingParseDeps,
) -> ParsedIncoming:
    msg = base.msg
    provider = base.provider
    tenant = base.tenant
    payload = base.payload
    manager_flag = flags.manager
    out_flag = flags.out
    provider_raw = flags.provider_raw

    message_id = _message_id(body, msg)
    text = (msg.get("text") or msg.get("body") or body.get("text") or "").strip()
    avito_account_id = deps.coerce_int_fn(body.get("account_id") or base.src.get("account_id"))
    avito_item_id = _extract_avito_item_id(body, msg, base.src, deps=deps)
    attachments = _collect_attachments(body, msg, provider_raw)
    if provider == "telegram":
        attachments = _force_telegram_media_attachments(
            attachments,
            provider_raw=provider_raw,
            tenant=tenant,
            msg=msg,
            body=body,
            deps=deps,
        )
    attachments = deps.normalize_attachments_fn(attachments)
    msg["attachments"] = attachments
    body["attachments"] = attachments
    has_photo = _detect_has_photo(
        provider=provider,
        provider_raw=provider_raw,
        msg=msg,
        body=body,
        attachments=attachments,
        deps=deps,
    )
    parsed_provider = _parse_provider_identity(
        provider=provider,
        text=text,
        body=body,
        msg=msg,
        payload=payload,
        provider_raw=provider_raw,
        tenant=tenant,
        manager_flag=manager_flag,
        avito_account_id=avito_account_id,
        deps=deps,
    )
    lead_id = _resolve_lead_id(
        provider=provider,
        body=body,
        tenant=tenant,
        avito_account_id=avito_account_id,
        parsed=parsed_provider,
        deps=deps,
    )
    channel = provider or "whatsapp"
    peer_for_log = _peer_for_log(provider, parsed_provider)
    return _make_parsed_incoming(
        message_id=message_id,
        text=text,
        attachments=attachments,
        has_photo=has_photo,
        out_flag=out_flag,
        provider_raw=provider_raw,
        parsed_provider=parsed_provider,
        avito_account_id=avito_account_id,
        avito_item_id=avito_item_id,
        lead_id=lead_id,
        channel=channel,
        peer_for_log=peer_for_log,
    )


def _extract_avito_item_id(
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    source: Mapping[str, Any],
    *,
    deps: IncomingParseDeps,
) -> int | None:
    avito_payload = body.get("avito") if isinstance(body.get("avito"), Mapping) else {}
    return deps.coerce_int_fn(
        body.get("item_id") or avito_payload.get("item_id") or source.get("item_id") or msg.get("item_id")
    )


def _make_parsed_incoming(
    *,
    message_id: str,
    text: str,
    attachments: list[dict[str, Any]],
    has_photo: bool,
    out_flag: bool,
    provider_raw: Mapping[str, Any],
    parsed_provider: Mapping[str, Any],
    avito_account_id: int | None,
    avito_item_id: int | None,
    lead_id: int,
    channel: str,
    peer_for_log: str,
) -> ParsedIncoming:
    return ParsedIncoming(
        message_id=message_id,
        text=text,
        attachments=attachments,
        has_photo=has_photo,
        manager_flag=parsed_provider["manager_flag"],
        out_flag=out_flag,
        provider_raw=provider_raw,
        whatsapp_phone=parsed_provider["whatsapp_phone"],
        telegram_user_id=parsed_provider["telegram_user_id"],
        telegram_username=parsed_provider["telegram_username"] or "",
        telegram_display_name=parsed_provider["telegram_display_name"] or "",
        max_user_id=parsed_provider["max_user_id"],
        max_username=parsed_provider["max_username"] or "",
        max_display_name=parsed_provider["max_display_name"] or "",
        peer_id=parsed_provider["peer_id"],
        peer_value=parsed_provider["peer_value"],
        contact_value=parsed_provider["contact_value"],
        avito_user_id=parsed_provider["avito_user_id"],
        avito_login=parsed_provider["avito_login"] or "",
        avito_chat_id=parsed_provider["avito_chat_id"] or "",
        avito_account_id=avito_account_id,
        avito_item_id=avito_item_id,
        avito_system_message=parsed_provider["avito_system_message"],
        lead_id=lead_id,
        channel=channel,
        resolved_provider=channel,
        peer_for_log=peer_for_log,
    )


def _message_id(body: Mapping[str, Any], msg: Mapping[str, Any]) -> str:
    raw_message_id = (
        msg.get("message_id")
        or msg.get("id")
        or (msg.get("key") or {}).get("id")
        or body.get("message_id")
        or body.get("id")
    )
    return str(raw_message_id) if raw_message_id is not None else ""


def build_normalized_incoming_event(
    ctx: IncomingEnvelopeContext,
    *,
    deps: IncomingEnvelopeDeps,
) -> IncomingEnvelope:
    from_addr, to_addr = _incoming_addresses(ctx, deps=deps)
    peer_value = ctx.peer_value
    normalized_event: dict[str, Any] = deps.build_envelope_fn(
        tenant_id=int(ctx.tenant),
        lead_id=int(ctx.lead_id),
        source_channel=ctx.channel,
        dialog_channel=ctx.channel,
        direction="incoming",
        author_kind="lead",
        provider_message_id=ctx.message_id or str(ctx.lead_id),
        text=ctx.text,
        attachments=ctx.attachments,
        trigger_bot=not ctx.manager_flag,
        peer=peer_value,
        extra={
            "event": "messages.incoming",
            "from": from_addr,
            "to": to_addr,
            "ts": int(time.time() * 1000),
        },
    )
    if ctx.manager_flag:
        normalized_event["manager"] = True
    if ctx.out_flag:
        normalized_event["out"] = True
    if ctx.provider_raw:
        normalized_event["provider_raw"] = ctx.provider_raw
    if isinstance(ctx.msg, Mapping):
        normalized_event["message"] = ctx.msg
    _apply_common_identity(normalized_event, ctx)
    peer_value = _apply_channel_identity(normalized_event, ctx, deps=deps, peer_value=peer_value)
    return IncomingEnvelope(event=normalized_event, peer_value=peer_value)


def _apply_common_identity(normalized_event: dict[str, Any], ctx: IncomingEnvelopeContext) -> None:
    if ctx.telegram_user_id is not None:
        normalized_event["telegram_user_id"] = ctx.telegram_user_id
    if ctx.telegram_username:
        normalized_event["username"] = ctx.telegram_username
    if ctx.telegram_display_name:
        normalized_event["display_name"] = ctx.telegram_display_name
    if ctx.provider in {"max", "max_personal"}:
        if ctx.max_user_id is not None:
            normalized_event["max_user_id"] = ctx.max_user_id
        if ctx.max_username:
            normalized_event["max_username"] = ctx.max_username
        if ctx.max_display_name:
            normalized_event["display_name"] = ctx.max_display_name
    if ctx.peer_id is not None:
        normalized_event["peer_id"] = ctx.peer_id


def _apply_channel_identity(
    normalized_event: dict[str, Any],
    ctx: IncomingEnvelopeContext,
    *,
    deps: IncomingEnvelopeDeps,
    peer_value: str | None,
) -> str | None:
    if ctx.provider == "telegram":
        normalized_event["tg_slot"] = deps.extract_tg_slot_fn(ctx.msg, ctx.body)
        if peer_value is None and ctx.telegram_user_id is not None:
            peer_value = str(ctx.telegram_user_id)
        if peer_value is not None:
            normalized_event["peer"] = peer_value
            lead_contacts = normalized_event.setdefault("lead_contacts", {})
            telegram_contact: dict[str, Any] = {"peer": peer_value}
            if ctx.contact_value:
                telegram_contact["contact"] = ctx.contact_value
            lead_contacts["telegram"] = telegram_contact
    if ctx.provider in {"max", "max_personal"}:
        if peer_value is None and ctx.max_user_id is not None:
            peer_value = str(ctx.max_user_id)
        if peer_value is not None:
            normalized_event["peer"] = peer_value
            lead_contacts = normalized_event.setdefault("lead_contacts", {})
            max_contact: dict[str, Any] = {"peer": peer_value}
            if ctx.contact_value:
                max_contact["contact"] = ctx.contact_value
            lead_contacts[ctx.provider] = max_contact
    if ctx.provider == "avito":
        if ctx.avito_chat_id:
            normalized_event["peer"] = ctx.avito_chat_id
            lead_contacts = normalized_event.setdefault("lead_contacts", {})
            avito_contact: dict[str, Any] = {"peer": ctx.avito_chat_id}
            if ctx.contact_value:
                avito_contact["contact"] = ctx.contact_value
            lead_contacts["avito"] = avito_contact
        if ctx.avito_account_id is not None:
            normalized_event["account_id"] = ctx.avito_account_id
        if ctx.avito_item_id is not None:
            normalized_event["item_id"] = ctx.avito_item_id
            source = normalized_event.setdefault("source", {})
            if isinstance(source, dict):
                source["item_id"] = ctx.avito_item_id
            if isinstance(normalized_event.get("message"), dict):
                normalized_event["message"]["item_id"] = ctx.avito_item_id
        normalized_event["avito"] = {
            "account_id": ctx.avito_account_id,
            "item_id": ctx.avito_item_id,
            "chat_id": ctx.avito_chat_id,
            "user_id": ctx.avito_user_id,
            "login": ctx.avito_login,
        }
    return peer_value


def _top_level_manager_out(body: Mapping[str, Any]) -> tuple[bool, bool]:
    manager_flag = False
    out_flag = False
    for candidate in (body.get("manager"), body.get("out")):
        candidate_bool = _truthy(candidate)
        if candidate_bool:
            manager_flag = True
            if body.get("out") is not None:
                out_flag = bool(candidate_bool)
            break
    return manager_flag, out_flag


def _has_truthy(*candidates: Any) -> bool:
    return any(_truthy(candidate) for candidate in candidates)


def _truthy(candidate: Any) -> bool:
    if isinstance(candidate, str):
        return candidate.strip().lower() in {"1", "true", "yes", "on"}
    return bool(candidate)


def _provider_raw_manager_flags(
    provider_raw: Mapping[str, Any],
    manager_flag: bool,
    out_flag: bool,
) -> tuple[bool, bool]:
    if not isinstance(provider_raw, Mapping):
        return manager_flag, out_flag
    key_obj = provider_raw.get("key") if isinstance(provider_raw.get("key"), Mapping) else {}
    if key_obj.get("fromMe") or provider_raw.get("fromMe"):
        manager_flag = True
    if provider_raw.get("out") or provider_raw.get("outgoing"):
        manager_flag = True
        out_flag = True
    return manager_flag, out_flag


def _message_obj_manager_flags(
    message_obj: Mapping[str, Any],
    manager_flag: bool,
    out_flag: bool,
) -> tuple[bool, bool]:
    if not isinstance(message_obj, Mapping):
        return manager_flag, out_flag
    if message_obj.get("out") or message_obj.get("outgoing"):
        manager_flag = True
        out_flag = True
    if message_obj.get("fromMe"):
        manager_flag = True
    meta_obj = message_obj.get("meta") if isinstance(message_obj.get("meta"), Mapping) else {}
    if isinstance(meta_obj, Mapping) and meta_obj.get("manager"):
        manager_flag = True
    return manager_flag, out_flag


def _collect_attachments(
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    provider_raw: Mapping[str, Any],
) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    raw_attachments = msg.get("attachments") or body.get("attachments")
    if isinstance(raw_attachments, list):
        attachments = [item for item in raw_attachments if isinstance(item, dict)]
    extra_media: list[Mapping[str, Any]] = []
    for candidate in (
        msg.get("media"),
        body.get("media"),
        (msg.get("message") or {}).get("media") if isinstance(msg.get("message"), Mapping) else None,
        (msg.get("provider_raw") or {}).get("media")
        if isinstance(msg.get("provider_raw"), Mapping)
        else None,
        (body.get("provider_raw") or {}).get("media")
        if isinstance(body.get("provider_raw"), Mapping)
        else None,
    ):
        if isinstance(candidate, list):
            extra_media.extend(item for item in candidate if isinstance(item, Mapping))
        elif isinstance(candidate, Mapping):
            extra_media.append(candidate)
    for candidate in (
        msg.get("photo"),
        body.get("photo"),
        (msg.get("message") or {}).get("photo") if isinstance(msg.get("message"), Mapping) else None,
        (msg.get("provider_raw") or {}).get("photo")
        if isinstance(msg.get("provider_raw"), Mapping)
        else None,
        (body.get("provider_raw") or {}).get("photo")
        if isinstance(body.get("provider_raw"), Mapping)
        else None,
    ):
        if isinstance(candidate, list):
            extra_media.extend(item for item in candidate if isinstance(item, Mapping))
        elif isinstance(candidate, Mapping):
            extra_media.append(candidate)
    if extra_media:
        attachments.extend([dict(item) for item in extra_media if isinstance(item, Mapping)])
    return attachments


def _force_telegram_media_attachments(
    attachments: list[dict[str, Any]],
    *,
    provider_raw: Mapping[str, Any],
    tenant: int,
    msg: dict[str, Any],
    body: dict[str, Any],
    deps: IncomingParseDeps,
) -> list[dict[str, Any]]:
    if attachments or not isinstance(provider_raw, Mapping):
        return attachments
    media_obj = deps.as_mapping_fn(provider_raw.get("media"))
    photo_obj = deps.as_mapping_fn(provider_raw.get("photo"))
    forced = []
    for obj in (media_obj, photo_obj):
        if not obj:
            continue
        kind = str(obj.get("_") or obj.get("type") or "photo")
        photo_id = obj.get("id") or ((obj.get("photo") or {}).get("id") if isinstance(obj, Mapping) else None)
        attachment = {"type": kind}
        if photo_id:
            attachment["photo_id"] = photo_id
            attachment["url"] = f"telegram://{tenant}/{photo_id}"
        forced.append(attachment)
    if forced:
        msg["attachments"] = forced
        body["attachments"] = forced
        return forced
    return attachments


def _detect_has_photo(
    *,
    provider: str,
    provider_raw: Mapping[str, Any],
    msg: Mapping[str, Any],
    body: Mapping[str, Any],
    attachments: list[dict[str, Any]],
    deps: IncomingParseDeps,
) -> bool:
    has_photo = deps.has_photo_attachment_fn(attachments)
    deps.logger.debug(
        "webhook_photo_probe provider=%s tenant=%s lead_hint=%s has_photo_initial=%s attachments_len=%s provider_raw_keys=%s",
        provider,
        body.get("tenant_id") or body.get("tenant") or "-",
        None,
        int(has_photo),
        len(attachments),
        list(provider_raw.keys()) if isinstance(provider_raw, Mapping) else None,
    )
    if not has_photo and isinstance(provider_raw, Mapping):
        media_obj = deps.as_mapping_fn(provider_raw.get("media"))
        photo_obj = deps.as_mapping_fn(provider_raw.get("photo"))
        if media_obj or photo_obj:
            if deps.has_photo_attachment_fn([obj for obj in (media_obj, photo_obj) if obj]):
                has_photo = True
        if not has_photo:
            try:
                raw_dump = deps.json_module.dumps(provider_raw, ensure_ascii=False)
            except Exception:
                raw_dump = str(provider_raw)
            lowered_dump = raw_dump.lower()
            if "messagemediaphoto" in lowered_dump or '"photo"' in lowered_dump:
                has_photo = True
    if provider == "telegram" and not has_photo:
        telegram_media_candidates = [
            deps.as_mapping_fn(msg.get("media")),
            deps.as_mapping_fn(body.get("media")),
            deps.as_mapping_fn((msg.get("message") or {}).get("media"))
            if isinstance(msg.get("message"), Mapping)
            else None,
            deps.as_mapping_fn(msg.get("photo")),
            deps.as_mapping_fn(body.get("photo")),
            deps.as_mapping_fn((msg.get("message") or {}).get("photo"))
            if isinstance(msg.get("message"), Mapping)
            else None,
        ]
        if any(candidate for candidate in telegram_media_candidates if candidate is not None):
            has_photo = True
    if not has_photo:
        provider_raw_alt = deps.as_mapping_fn(msg.get("provider_raw")) or deps.as_mapping_fn(
            body.get("provider_raw")
        )
        if isinstance(provider_raw_alt, Mapping):
            marker = str(provider_raw_alt.get("_") or "").lower()
            if marker and ("photo" in marker or "image" in marker):
                has_photo = True
            elif any(
                key.lower().startswith("photo")
                for key in provider_raw_alt.keys()
                if isinstance(key, str)
            ):
                has_photo = True
    return has_photo


def _parse_provider_identity(
    *,
    provider: str,
    text: str,
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    payload: Mapping[str, Any],
    provider_raw: Mapping[str, Any],
    tenant: int,
    manager_flag: bool,
    avito_account_id: int | None,
    deps: IncomingParseDeps,
) -> dict[str, Any]:
    parsed = {
        "whatsapp_phone": "",
        "telegram_user_id": None,
        "telegram_username": None,
        "telegram_display_name": None,
        "max_user_id": None,
        "max_username": None,
        "max_display_name": None,
        "peer_id": None,
        "peer_value": None,
        "contact_value": None,
        "avito_user_id": None,
        "avito_login": None,
        "avito_chat_id": None,
        "avito_system_message": False,
        "manager_flag": manager_flag,
    }
    if provider == "telegram":
        _parse_telegram_identity(parsed, body=body, msg=msg, deps=deps)
    elif provider in {"max", "max_personal"}:
        _parse_max_identity(parsed, body=body, msg=msg, deps=deps)
    elif provider == "avito":
        _parse_avito_identity(
            parsed,
            body=body,
            msg=msg,
            payload=payload,
            text=text,
            avito_account_id=avito_account_id,
            deps=deps,
        )
    else:
        from_id = msg.get("from") or msg.get("author") or body.get("from") or ""
        whatsapp_phone = deps.digits_fn(from_id.split("@", 1)[0] if from_id else "")
        parsed["whatsapp_phone"] = whatsapp_phone
        parsed["contact_value"] = whatsapp_phone or None
        if deps.is_manager_whatsapp_fn(whatsapp_phone):
            parsed["manager_flag"] = True
    return parsed


def _parse_telegram_identity(parsed: dict[str, Any], *, body: Mapping[str, Any], msg: Mapping[str, Any], deps: IncomingParseDeps) -> None:
    raw_id = msg.get("telegram_user_id") or body.get("telegram_user_id") or body.get("user_id")
    if raw_id is not None:
        try:
            parsed["telegram_user_id"] = int(raw_id)
        except Exception:
            parsed["telegram_user_id"] = None
    raw_username = msg.get("telegram_username") or body.get("username")
    parsed["telegram_username"] = raw_username.strip() if isinstance(raw_username, str) and raw_username.strip() else None
    parsed["telegram_display_name"] = deps.sanitize_display_name_fn(
        msg.get("display_name") or body.get("display_name")
    )
    parsed["contact_value"] = parsed["telegram_display_name"] or parsed["telegram_username"]
    if deps.is_manager_telegram_fn(parsed["telegram_user_id"]):
        parsed["manager_flag"] = True
    peer_id, peer_value = _parse_peer(msg, body)
    if parsed["telegram_user_id"] is None and peer_id is not None:
        parsed["telegram_user_id"] = peer_id
        if deps.is_manager_telegram_fn(parsed["telegram_user_id"]):
            parsed["manager_flag"] = True
    parsed["peer_id"] = peer_id
    parsed["peer_value"] = peer_value


def _parse_max_identity(parsed: dict[str, Any], *, body: Mapping[str, Any], msg: Mapping[str, Any], deps: IncomingParseDeps) -> None:
    raw_id = msg.get("max_user_id") or body.get("max_user_id") or msg.get("user_id") or body.get("user_id")
    if raw_id is not None:
        try:
            parsed["max_user_id"] = int(raw_id)
        except Exception:
            parsed["max_user_id"] = None
    raw_username = msg.get("max_username") or body.get("max_username") or msg.get("username") or body.get("username")
    parsed["max_username"] = raw_username.strip() if isinstance(raw_username, str) and raw_username.strip() else None
    raw_display_name = msg.get("display_name") or body.get("display_name") or msg.get("name") or body.get("name")
    parsed["max_display_name"] = deps.sanitize_display_name_fn(raw_display_name) if isinstance(raw_display_name, str) else None
    if parsed["max_display_name"] and not deps.is_human_readable_name_fn(parsed["max_display_name"]):
        parsed["max_display_name"] = None
    if parsed["max_username"] and not deps.is_human_readable_name_fn(parsed["max_username"]):
        parsed["max_username"] = None
    parsed["contact_value"] = parsed["max_display_name"] or parsed["max_username"]
    parsed["peer_id"], parsed["peer_value"] = _parse_peer(msg, body)


def _parse_avito_identity(
    parsed: dict[str, Any],
    *,
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    payload: Mapping[str, Any],
    text: str,
    avito_account_id: int | None,
    deps: IncomingParseDeps,
) -> None:
    parsed["avito_system_message"] = deps.is_avito_system_message_fn(text, msg, payload)
    chat_candidate = (
        msg.get("chat_id")
        or body.get("chat_id")
        or msg.get("conversation_id")
        or payload.get("chat_id")
        or payload.get("conversation_id")
    )
    if isinstance(chat_candidate, dict):
        chat_candidate = chat_candidate.get("id")
    if chat_candidate is not None:
        chat_text = str(chat_candidate).strip()
        parsed["avito_chat_id"] = chat_text or None
        if parsed["avito_chat_id"]:
            parsed["peer_value"] = parsed["avito_chat_id"]
    author_info = msg.get("author") or msg.get("sender") or body.get("author") or {}
    if not isinstance(author_info, Mapping):
        author_info = {}
    parsed["avito_user_id"] = deps.coerce_int_fn(
        author_info.get("id")
        or author_info.get("user_id")
        or msg.get("author_id")
        or body.get("avito_user_id")
    )
    login_candidate = (
        author_info.get("login")
        or author_info.get("username")
        or msg.get("author_login")
        or body.get("avito_login")
    )
    if isinstance(login_candidate, str):
        login_candidate = deps.sanitize_display_name_fn(login_candidate.strip())
    parsed["avito_login"] = login_candidate or None
    parsed["contact_value"] = parsed["avito_login"] or (
        str(parsed["avito_user_id"]) if parsed["avito_user_id"] else None
    )


def _parse_peer(msg: Mapping[str, Any], body: Mapping[str, Any]) -> tuple[int | None, str | None]:
    peer_candidate = (
        msg.get("peer")
        or body.get("peer")
        or msg.get("peer_id")
        or body.get("peer_id")
        or msg.get("chat_id")
        or body.get("chat_id")
    )
    if peer_candidate is None:
        return None, None
    peer_value = str(peer_candidate).strip() or None
    if peer_value is None:
        return None, None
    try:
        return int(peer_value), peer_value
    except Exception:
        return None, peer_value


def _resolve_lead_id(
    *,
    provider: str,
    body: Mapping[str, Any],
    tenant: int,
    avito_account_id: int | None,
    parsed: Mapping[str, Any],
    deps: IncomingParseDeps,
) -> int:
    lead_id_value = deps.coerce_int_fn(body.get("leadId") or body.get("lead_id"))
    if provider == "telegram":
        if parsed["telegram_user_id"] is not None:
            lead_id_value = parsed["telegram_user_id"]
        elif parsed["peer_id"] is not None:
            lead_id_value = parsed["peer_id"]
    elif provider in {"max", "max_personal"}:
        if parsed["max_user_id"] is not None:
            lead_id_value = parsed["max_user_id"]
        elif parsed["peer_id"] is not None:
            lead_id_value = parsed["peer_id"]
    elif provider == "avito":
        account_hint = avito_account_id if avito_account_id is not None else tenant
        if parsed["avito_chat_id"]:
            lead_id_value = deps.avito_module.stable_lead_id(account_hint, parsed["avito_chat_id"])
    if lead_id_value in (None, 0):
        lead_id_value = int(time.time() * 1000)
    return int(lead_id_value)


def _peer_for_log(provider: str, parsed: Mapping[str, Any]) -> str:
    if provider == "telegram":
        return str(parsed["peer_value"] or parsed["peer_id"] or parsed["telegram_user_id"] or "")
    if provider in {"max", "max_personal"}:
        return str(parsed["peer_value"] or parsed["peer_id"] or parsed["max_user_id"] or "")
    return str(parsed["whatsapp_phone"] or "")


def _incoming_addresses(
    ctx: IncomingEnvelopeContext,
    *,
    deps: IncomingEnvelopeDeps,
) -> tuple[str, str]:
    if ctx.provider == "telegram":
        from_addr = str(ctx.telegram_user_id or "")
        if ctx.telegram_user_id is not None:
            return from_addr, str(ctx.telegram_user_id)
        if ctx.peer_id is not None:
            return from_addr, str(ctx.peer_id)
        return from_addr, ""
    if ctx.provider in {"max", "max_personal"}:
        from_addr = str(ctx.max_user_id or ctx.peer_value or "")
        if ctx.max_user_id is not None:
            return from_addr, str(ctx.max_user_id)
        if ctx.peer_id is not None:
            return from_addr, str(ctx.peer_id)
        return from_addr, ""
    if ctx.provider == "avito":
        from_addr = ctx.avito_login or (str(ctx.avito_user_id) if ctx.avito_user_id else "")
        return from_addr, ""
    to_candidate = (
        ctx.msg.get("to")
        or ctx.body.get("to")
        or (ctx.body.get("destination") if isinstance(ctx.body.get("destination"), str) else "")
    )
    return ctx.whatsapp_phone, deps.digits_fn(to_candidate)
