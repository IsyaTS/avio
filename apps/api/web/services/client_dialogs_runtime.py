from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Mapping

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ClientDialogsDeps:
    resolve_tenant_and_key_fn: SyncFn
    db_module: Any
    isoformat_fn: SyncFn
    normalize_attachments_fn: SyncFn
    parse_tg_slot_fn: SyncFn
    load_silence_status_fn: SyncFn
    load_telegram_slot_profiles_fn: SyncFn
    common_module: Any
    is_technical_max_title_fn: SyncFn
    run_response_pipeline_fn: AsyncFn
    default_fallback_reply_fn: SyncFn
    apply_custom_punctuation_style_fn: SyncFn
    split_reply_for_test_send_fn: SyncFn
    delay_seconds_value_fn: SyncFn
    smart_reply_delay_min_seconds: int
    smart_reply_delay_max_seconds: int
    smart_reply_split_part_delay_enabled: bool
    smart_reply_split_channels: set[str]
    smart_reply_split_part_delay_min_seconds: int
    smart_reply_split_part_delay_max_seconds: int
    read_photo_manifest_fn: SyncFn
    photo_public_url_fn: SyncFn
    tg_slot_min: int
    tg_slot_max: int
    outbox_queue_key: str
    time_module: Any
    json_module: Any
    asyncio_module: Any
    dialogs_logger: Any
    avito_accounts_repo: Any = None
    avito_item_contexts_repo: Any = None


@dataclass
class _DialogSendContext:
    tenant_id: int
    key: str
    lead_id: int
    payload: Mapping[str, Any]
    text: str
    attachment: dict[str, Any] | None
    display_text: str
    lead_meta: Mapping[str, Any]
    channel: str
    telegram_user_id: int | None
    tg_slot: int
    message_id: Any = None


async def list_dialogs_api(
    request: Request,
    *,
    tenant: int | str | None,
    limit: int,
    deps: ClientDialogsDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _key = auth

    try:
        limit_val = int(limit)
    except Exception:
        limit_val = 200
    if limit_val <= 0:
        limit_val = 50
    limit_val = min(limit_val, 500)

    dialogs_raw = await deps.db_module.fetch_dialogs_for_tenant(tenant_id, limit=limit_val)
    avito_meta = await _load_avito_dialog_metadata(tenant_id, dialogs_raw, deps=deps)
    dialogs: list[dict[str, Any]] = []
    for entry in dialogs_raw:
        channel_name = (entry.get("channel") or "").strip().lower() or "unknown"
        if channel_name not in {"telegram", "avito", "whatsapp", "max", "max_personal", "unknown"}:
            continue
        channel_ui = "max" if channel_name == "max_personal" else channel_name
        lead_ref = _coerce_int(entry.get("id"))
        title = _dialog_title(entry, channel_name, deps=deps)
        if not title:
            title = f"Лид {lead_ref}" if lead_ref else "Лид"
        lead_ref_str = str(lead_ref)
        avito_entry_meta = avito_meta.get(int(lead_ref or 0), {}) if channel_name == "avito" else {}
        dialogs.append(
            {
                "id": lead_ref_str,
                "id_num": lead_ref,
                "id_str": lead_ref_str,
                "channel": channel_ui,
                "title": title,
                "contact": entry.get("contact"),
                "last_message": entry.get("last_message"),
                "last_ts": deps.isoformat_fn(entry.get("last_ts")),
                "unread": 0,
                **_avito_dialog_public_meta(avito_entry_meta),
            }
        )
    return {"ok": True, "dialogs": dialogs}


async def get_dialog_messages_api(
    lead_id: int,
    request: Request,
    *,
    tenant: int | str | None,
    limit: int,
    before: str | None,
    deps: ClientDialogsDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, key = auth

    lead_meta = await deps.db_module.get_lead_dialog_metadata(lead_id)
    if not lead_meta or int(lead_meta.get("tenant_id") or 0) != int(tenant_id):
        return JSONResponse({"detail": "not_found"}, status_code=404)

    limit_val = _normalize_limit(limit, default=50, minimum=20, maximum=200)
    before_dt = _parse_before(before)
    messages = await deps.db_module.list_messages_for_lead(
        tenant_id, lead_id, limit=limit_val, before=before_dt
    )
    if messages:
        messages = sorted(messages, key=_message_sort_key)
    message_ids = [msg.get("id") for msg in messages if msg.get("id")]
    feedback_ids = await deps.db_module.list_feedback_message_ids(tenant_id, message_ids)
    formatted = []
    for msg in messages:
        msg_id = msg.get("id")
        attachments = deps.normalize_attachments_fn(
            request,
            tenant_id,
            key or "",
            msg.get("attachments"),
        )
        formatted.append(
            {
                "id": msg_id,
                "direction": msg.get("direction") or 0,
                "text": msg.get("text") or "",
                "ts": deps.isoformat_fn(msg.get("created_at")),
                "status": msg.get("status") or "",
                "from_bot": bool(msg.get("is_bot")),
                "feedbacked": bool(msg_id and msg_id in feedback_ids),
                "attachments": attachments,
                "source": msg.get("source") or "",
                "tg_slot": deps.parse_tg_slot_fn(msg.get("source")),
            }
        )

    channel_raw = (lead_meta.get("channel") or "").strip().lower()
    silence = deps.load_silence_status_fn(tenant_id, lead_id, channel_raw)
    telegram_accounts: list[dict[str, Any]] = []
    selected_tg_slot: int | None = None
    if channel_raw == "telegram":
        telegram_accounts = deps.load_telegram_slot_profiles_fn(tenant_id)
        try:
            redis_client = deps.common_module.redis_client()
            raw_slot = redis_client.get(f"tg:lead_slot:{int(tenant_id)}:{int(lead_id)}")
            selected_tg_slot = int(raw_slot) if raw_slot is not None else None
        except Exception:
            selected_tg_slot = None

    channel_ui = "max" if channel_raw == "max_personal" else channel_raw
    title_value = _message_title(lead_meta, channel_raw, deps=deps)
    avito_detail_meta = {}
    if channel_raw == "avito":
        avito_detail_meta = await _load_avito_detail_metadata(tenant_id, lead_id, lead_meta, deps=deps)
    return {
        "ok": True,
        "dialog_id": lead_id,
        "channel": channel_ui,
        "title": title_value,
        "messages": formatted,
        "telegram_accounts": telegram_accounts,
        "selected_tg_slot": selected_tg_slot,
        "silence": silence,
        **_avito_dialog_public_meta(avito_detail_meta),
    }


async def test_dialog_api(
    request: Request,
    *,
    tenant: int | str | None,
    deps: ClientDialogsDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    text = str(payload.get("text") or "").strip()
    if not text:
        return JSONResponse({"detail": "empty_text"}, status_code=400)

    channel = str(payload.get("channel") or "telegram").strip().lower() or "telegram"
    if channel not in {"telegram", "avito", "whatsapp", "max", "max_personal"}:
        channel = "telegram"
    delay_enabled = bool(payload.get("delay_enabled", True))
    force_delay = bool(payload.get("force_delay", False))
    history = _normalize_history(payload.get("history") or [])
    had_assistant_before = any(item.get("role") == "assistant" for item in history)
    emulate_channels = bool(payload.get("emulate_channels", True))
    contact_id = _coerce_nonnegative_int(payload.get("contact_id"))
    lead_id = _coerce_nonnegative_int(payload.get("lead_id"))
    if contact_id <= 0 and lead_id > 0:
        try:
            resolved_contact = await deps.db_module.get_contact_id_by_lead(int(lead_id))
            contact_id = int(resolved_contact or 0)
        except Exception:
            contact_id = 0

    try:
        result = await deps.run_response_pipeline_fn(
            tenant_id=tenant_id,
            channel=channel,
            user_text=text,
            history=[] if emulate_channels else history,
            contact_id=contact_id,
            enable_photos=False,
        )
        reply_text = result.reply_text
    except Exception:
        reply_text = deps.default_fallback_reply_fn()
    reply_text = deps.apply_custom_punctuation_style_fn(str(reply_text or "").strip())
    reply_parts = deps.split_reply_for_test_send_fn(reply_text, channel)
    if not reply_parts:
        reply_parts = [deps.default_fallback_reply_fn()]
    timeline = _test_dialog_timeline(
        reply_parts,
        channel,
        delay_enabled,
        had_assistant_before,
        force_delay,
        deps,
    )
    return {
        "ok": True,
        "reply": (timeline[0]["text"] if timeline else ""),
        "replies": timeline,
        "delay_enabled": delay_enabled,
    }


def _test_dialog_timeline(
    reply_parts: list[str],
    channel: str,
    delay_enabled: bool,
    had_assistant_before: bool,
    force_delay: bool,
    deps: ClientDialogsDeps,
) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []
    at_ms = _initial_test_dialog_delay_ms(
        delay_enabled,
        had_assistant_before,
        force_delay,
        deps,
    )
    for idx, part in enumerate(reply_parts):
        at_ms += _split_test_dialog_delay_ms(idx, channel, delay_enabled, deps)
        timeline.append({"text": str(part or "").strip(), "at_ms": at_ms})
    return timeline


def _initial_test_dialog_delay_ms(
    delay_enabled: bool,
    had_assistant_before: bool,
    force_delay: bool,
    deps: ClientDialogsDeps,
) -> int:
    if not delay_enabled or not (had_assistant_before or force_delay):
        return 0
    return int(
        deps.delay_seconds_value_fn(
            deps.smart_reply_delay_min_seconds,
            deps.smart_reply_delay_max_seconds,
        )
        * 1000
    )


def _split_test_dialog_delay_ms(
    idx: int,
    channel: str,
    delay_enabled: bool,
    deps: ClientDialogsDeps,
) -> int:
    if (
        not delay_enabled
        or idx <= 0
        or not deps.smart_reply_split_part_delay_enabled
        or channel not in deps.smart_reply_split_channels
        or deps.smart_reply_split_part_delay_max_seconds <= 0
    ):
        return 0
    return int(
        deps.delay_seconds_value_fn(
            deps.smart_reply_split_part_delay_min_seconds,
            deps.smart_reply_split_part_delay_max_seconds,
        )
        * 1000
    )


async def send_dialog_message_api(
    lead_id: int,
    request: Request,
    *,
    tenant: int | str | None,
    deps: ClientDialogsDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, key = auth
    payload = await _dialog_send_payload(request)
    attachment_or_response = _dialog_send_attachment(request, tenant_id, key, payload, deps)
    if isinstance(attachment_or_response, Response):
        return attachment_or_response
    text = str(payload.get("text") or "").strip()
    if not text and not attachment_or_response:
        return JSONResponse({"detail": "empty_text"}, status_code=400)
    lead_meta = await deps.db_module.get_lead_dialog_metadata(lead_id)
    if not _dialog_send_lead_allowed(lead_meta, tenant_id):
        return JSONResponse({"detail": "not_found"}, status_code=404)
    channel = str(lead_meta.get("channel") or "").strip().lower()
    if not _dialog_send_channel_supported(channel):
        return JSONResponse({"detail": "unsupported_channel"}, status_code=400)
    ctx = _build_dialog_send_context(
        tenant_id,
        key,
        lead_id,
        payload,
        text,
        attachment_or_response,
        lead_meta,
        channel,
        deps,
    )
    insert_error = await _insert_dialog_send_message(ctx, deps)
    if insert_error is not None:
        return insert_error
    queue_item = _dialog_send_queue_item(ctx, deps)
    enqueue_error = await _enqueue_dialog_send(queue_item, ctx, deps)
    if enqueue_error is not None:
        return enqueue_error
    return {"ok": True, "queued": True, "message": _dialog_send_response_message(ctx)}


async def _dialog_send_payload(request: Request) -> Mapping[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return payload if isinstance(payload, Mapping) else {}


def _dialog_send_attachment(
    request: Request,
    tenant_id: int,
    key: str,
    payload: Mapping[str, Any],
    deps: ClientDialogsDeps,
) -> dict[str, Any] | Response | None:
    photo_id = str(payload.get("photo_id") or "").strip()
    if not photo_id:
        return None
    entries = deps.read_photo_manifest_fn(tenant_id)
    photo_entry = next((item for item in entries if str(item.get("id") or "") == photo_id), None)
    if not photo_entry:
        return JSONResponse({"detail": "photo_not_found"}, status_code=404)
    return {
        "type": "image",
        "path": photo_entry.get("path"),
        "filename": photo_entry.get("original") or photo_entry.get("filename"),
        "mime": photo_entry.get("mime"),
        "size": photo_entry.get("size"),
        "url": deps.photo_public_url_fn(request, tenant_id, key, photo_id),
    }


def _dialog_send_lead_allowed(lead_meta: Any, tenant_id: int) -> bool:
    return bool(lead_meta and int(lead_meta.get("tenant_id") or 0) == int(tenant_id))


def _dialog_send_channel_supported(channel: str) -> bool:
    return channel in {"telegram", "avito", "max", "max_personal"}


def _build_dialog_send_context(
    tenant_id: int,
    key: str,
    lead_id: int,
    payload: Mapping[str, Any],
    text: str,
    attachment: dict[str, Any] | None,
    lead_meta: Mapping[str, Any],
    channel: str,
    deps: ClientDialogsDeps,
) -> _DialogSendContext:
    telegram_user_id, tg_slot = _dialog_send_telegram_target(payload, lead_meta, channel, deps)
    return _DialogSendContext(
        tenant_id=tenant_id,
        key=key,
        lead_id=lead_id,
        payload=payload,
        text=text,
        attachment=attachment,
        display_text=text or "Фото",
        lead_meta=lead_meta,
        channel=channel,
        telegram_user_id=telegram_user_id,
        tg_slot=tg_slot,
    )


def _dialog_send_telegram_target(
    payload: Mapping[str, Any],
    lead_meta: Mapping[str, Any],
    channel: str,
    deps: ClientDialogsDeps,
) -> tuple[int | None, int]:
    telegram_user_id: int | None = None
    tg_slot = deps.tg_slot_min
    if channel != "telegram":
        return telegram_user_id, tg_slot
    tg_raw = lead_meta.get("telegram_user_id") or lead_meta.get("peer")
    try:
        telegram_user_id = int(tg_raw)
    except Exception:
        telegram_user_id = None
    if telegram_user_id is not None and telegram_user_id <= 0:
        telegram_user_id = None
    return telegram_user_id, _normalize_tg_slot(payload.get("tg_slot"), deps=deps)


async def _insert_dialog_send_message(
    ctx: _DialogSendContext,
    deps: ClientDialogsDeps,
) -> Response | None:
    try:
        ctx.message_id = await deps.db_module.insert_message_out(
            ctx.lead_id,
            ctx.display_text,
            None,
            status="queued",
            tenant_id=ctx.tenant_id,
            channel=ctx.channel,
            telegram_user_id=ctx.telegram_user_id,
            telegram_username=ctx.lead_meta.get("telegram_username"),
            title=ctx.lead_meta.get("title"),
            attachments=[ctx.attachment] if ctx.attachment else None,
            source=f"manager:tg_slot:{ctx.tg_slot}" if ctx.channel == "telegram" else "manager",
        )
    except Exception:
        deps.dialogs_logger.exception(
            "dialog_send_insert_failed tenant=%s lead=%s",
            ctx.tenant_id,
            ctx.lead_id,
        )
        return JSONResponse({"detail": "db_error"}, status_code=500)
    if not ctx.message_id:
        return JSONResponse({"detail": "db_error"}, status_code=500)
    return None


def _dialog_send_queue_item(
    ctx: _DialogSendContext,
    deps: ClientDialogsDeps,
) -> dict[str, Any]:
    queue_item: dict[str, Any] = {
        "lead_id": ctx.lead_id,
        "tenant_id": ctx.tenant_id,
        "tenant": ctx.tenant_id,
        "provider": ctx.channel,
        "ch": ctx.channel,
        "channel": ctx.channel,
        "text": ctx.text,
        "origin": "dialogs.ui",
        "_message_db_id": ctx.message_id,
        "_resolved_lead_id": ctx.lead_id,
        "queued_at": deps.time_module.time(),
    }
    if ctx.attachment:
        queue_item["attachment"] = ctx.attachment
    if ctx.telegram_user_id:
        queue_item["telegram_user_id"] = ctx.telegram_user_id
        queue_item["peer"] = ctx.telegram_user_id
        queue_item["tg_slot"] = ctx.tg_slot
    elif ctx.lead_meta.get("peer"):
        queue_item["peer"] = ctx.lead_meta.get("peer")
    if ctx.lead_meta.get("contact"):
        queue_item["contact"] = ctx.lead_meta.get("contact")
    if ctx.lead_meta.get("title"):
        queue_item["title"] = ctx.lead_meta.get("title")
    return queue_item


async def _enqueue_dialog_send(
    queue_item: dict[str, Any],
    ctx: _DialogSendContext,
    deps: ClientDialogsDeps,
) -> Response | None:
    try:
        redis_client = deps.common_module.redis_client()
    except Exception:
        return JSONResponse({"detail": "queue_unavailable"}, status_code=503)
    try:
        queue_payload = deps.json_module.dumps(queue_item, ensure_ascii=False)
    except Exception:
        queue_payload = None
    if not queue_payload:
        return JSONResponse({"detail": "queue_error"}, status_code=502)
    try:
        lpush_fn = getattr(redis_client, "lpush", None)
        if callable(lpush_fn):
            if deps.asyncio_module.iscoroutinefunction(lpush_fn):  # pragma: no cover
                await lpush_fn(deps.outbox_queue_key, queue_payload)
            else:
                lpush_fn(deps.outbox_queue_key, queue_payload)
        else:
            raise RuntimeError("redis_lpush_missing")
    except Exception:
        deps.dialogs_logger.exception(
            "dialog_send_enqueue_failed tenant=%s lead=%s channel=%s",
            ctx.tenant_id,
            ctx.lead_id,
            ctx.channel,
        )
        return JSONResponse({"detail": "queue_error"}, status_code=502)
    return None


def _dialog_send_response_message(ctx: _DialogSendContext) -> dict[str, Any]:
    return {
        "id": ctx.message_id,
        "direction": 1,
        "text": ctx.display_text,
        "ts": datetime.now(timezone.utc).isoformat(),
        "status": "queued",
        "from_bot": False,
    }


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _coerce_nonnegative_int(value: Any) -> int:
    result = _coerce_int(value if value is not None else 0)
    return result if result > 0 else 0


def _normalize_tg_slot(value: Any, *, deps: ClientDialogsDeps) -> int:
    try:
        slot = int(value)
    except Exception:
        slot = deps.tg_slot_min
    if slot < deps.tg_slot_min or slot > deps.tg_slot_max:
        slot = deps.tg_slot_min
    return slot


def _title_is_numeric(value: str) -> bool:
    return value.strip().isdigit() if isinstance(value, str) else False


async def _load_avito_dialog_metadata(
    tenant_id: int,
    dialogs_raw: list[Mapping[str, Any]],
    *,
    deps: ClientDialogsDeps,
) -> dict[int, dict[str, Any]]:
    avito_entries = [
        entry for entry in dialogs_raw if str(entry.get("channel") or "").strip().lower() == "avito"
    ]
    if not avito_entries:
        return {}
    lead_ids = [_coerce_int(entry.get("id")) for entry in avito_entries]
    contexts = await _safe_list_avito_contexts(tenant_id, lead_ids, deps=deps)
    accounts = await _safe_load_avito_accounts(tenant_id, deps=deps)
    result: dict[int, dict[str, Any]] = {}
    for entry in avito_entries:
        lead_id = _coerce_int(entry.get("id"))
        account_id = _coerce_int(entry.get("source_real_id") or contexts.get(lead_id, {}).get("account_id"))
        account = accounts.get(account_id, {}) if account_id else {}
        result[lead_id] = {
            "account_id": account_id or None,
            "display_name": account.get("display_name"),
            "account_login": account.get("account_login"),
            "item_context": contexts.get(lead_id, {}),
        }
    return result


async def _load_avito_detail_metadata(
    tenant_id: int,
    lead_id: int,
    lead_meta: Mapping[str, Any],
    *,
    deps: ClientDialogsDeps,
) -> dict[str, Any]:
    context = await _safe_get_avito_context_for_lead(tenant_id, lead_id, deps=deps)
    account_id = _coerce_int(lead_meta.get("source_real_id") or context.get("account_id"))
    account = {}
    if account_id and deps.avito_accounts_repo is not None:
        try:
            account = await deps.avito_accounts_repo.get_account(tenant_id, account_id) or {}
        except Exception:
            account = {}
    return {
        "account_id": account_id or None,
        "display_name": account.get("display_name"),
        "account_login": account.get("account_login"),
        "item_context": context,
    }


async def _safe_load_avito_accounts(
    tenant_id: int,
    *,
    deps: ClientDialogsDeps,
) -> dict[int, Mapping[str, Any]]:
    if deps.avito_accounts_repo is None:
        return {}
    try:
        rows = await deps.avito_accounts_repo.list_accounts(tenant_id, include_disconnected=True)
    except Exception:
        return {}
    result: dict[int, Mapping[str, Any]] = {}
    for row in rows or []:
        account_id = _coerce_int(row.get("account_id"))
        if account_id:
            result[account_id] = row
    return result


async def _safe_list_avito_contexts(
    tenant_id: int,
    lead_ids: list[int],
    *,
    deps: ClientDialogsDeps,
) -> dict[int, dict[str, Any]]:
    if deps.avito_item_contexts_repo is None:
        return {}
    try:
        return await deps.avito_item_contexts_repo.list_contexts_for_leads(tenant_id, lead_ids)
    except Exception:
        return {}


async def _safe_get_avito_context_for_lead(
    tenant_id: int,
    lead_id: int,
    *,
    deps: ClientDialogsDeps,
) -> dict[str, Any]:
    if deps.avito_item_contexts_repo is None:
        return {}
    try:
        return await deps.avito_item_contexts_repo.get_context_for_lead(tenant_id, lead_id) or {}
    except Exception:
        return {}


def _avito_dialog_public_meta(meta: Mapping[str, Any]) -> dict[str, Any]:
    item_context = meta.get("item_context") if isinstance(meta.get("item_context"), Mapping) else {}
    city = str(item_context.get("city") or "").strip() or None
    status = str(item_context.get("status") or "").strip() or None
    return {
        "avito_account_id": meta.get("account_id"),
        "avito_account_display_name": meta.get("display_name"),
        "avito_account_login": meta.get("account_login"),
        "avito_item_id": item_context.get("item_id"),
        "avito_item_city": city,
        "avito_item_city_status": status,
    }


def _dialog_title(entry: Mapping[str, Any], channel_name: str, *, deps: ClientDialogsDeps) -> Any:
    raw_title = entry.get("title")
    raw_contact = entry.get("contact")
    raw_peer = entry.get("peer")
    if channel_name == "avito":
        title = entry.get("avito_login") or raw_title or raw_contact
        if title and _title_is_numeric(str(title)):
            title = entry.get("avito_login")
        return title or "Avito · клиент"
    if channel_name == "telegram":
        title = raw_title or entry.get("telegram_username") or raw_contact
        if title and _title_is_numeric(str(title)):
            title = entry.get("telegram_username")
        return title or "Telegram · клиент"
    if channel_name in {"max", "max_personal"}:
        raw_title_str = str(raw_title).strip() if raw_title is not None else ""
        if deps.is_technical_max_title_fn(raw_title_str):
            title = raw_contact or entry.get("max_username") or raw_peer
        else:
            title = raw_title or entry.get("max_username") or raw_contact or raw_peer
        if title and _title_is_numeric(str(title)):
            title = (
                entry.get("max_username")
                or raw_contact
                or (str(entry.get("max_user_id")) if entry.get("max_user_id") is not None else "")
            )
        if title and _title_is_numeric(str(title)):
            title = None
        return title or "MAX · клиент"
    return raw_title or raw_contact or raw_peer


def _message_title(lead_meta: Mapping[str, Any], channel_raw: str, *, deps: ClientDialogsDeps) -> Any:
    title_value = lead_meta.get("title") or lead_meta.get("contact")
    if channel_raw not in {"max", "max_personal"}:
        return title_value
    max_username = lead_meta.get("max_username")
    max_user_id = lead_meta.get("max_user_id")
    raw_title = lead_meta.get("title")
    if deps.is_technical_max_title_fn(raw_title):
        title_value = lead_meta.get("contact") or max_username or lead_meta.get("peer")
    else:
        title_value = title_value or max_username or lead_meta.get("peer")
    if isinstance(title_value, str) and title_value.strip().isdigit():
        title_value = lead_meta.get("contact") or max_username or (
            str(max_user_id) if max_user_id is not None else title_value
        )
    if isinstance(title_value, str) and title_value.strip().isdigit():
        title_value = None
    return title_value or "MAX · клиент"


def _normalize_limit(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        result = int(value)
    except Exception:
        result = default
    if result <= 0:
        result = minimum
    return min(result, maximum)


def _parse_before(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        try:
            return datetime.fromtimestamp(float(value))
        except Exception:
            return None


def _message_sort_key(item: dict) -> tuple[float, int]:
    ts_value = item.get("created_at")
    ts_num = 0.0
    if isinstance(ts_value, datetime):
        try:
            ts_num = ts_value.timestamp()
        except Exception:
            ts_num = 0.0
    elif isinstance(ts_value, str):
        try:
            ts_num = datetime.fromisoformat(ts_value).timestamp()
        except Exception:
            ts_num = 0.0
    msg_id = _coerce_int(item.get("id"))
    return (ts_num, msg_id)


def _normalize_history(raw: Any) -> list[dict[str, str]]:
    history: list[dict[str, str]] = []
    if isinstance(raw, list):
        for item in raw[-12:]:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "").strip().lower()
            if role not in {"user", "assistant"}:
                continue
            content = str(item.get("text") or item.get("content") or "").strip()
            if not content:
                continue
            history.append({"role": role, "content": content})
    return history
