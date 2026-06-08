from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class AvitoWebhookDeps:
    avito_webhook_events_module: Any
    logger: Any
    json_module: Any
    avito_module: Any
    coerce_int_fn: SyncFn
    find_lead_by_peer_fn: AsyncFn
    redis_queue: Any
    content_fingerprint_fn: SyncFn
    avito_bot_echo_key_fn: SyncFn
    normalize_echo_text_fn: SyncFn
    is_recent_bot_echo_fn: AsyncFn
    time_module: Any
    handoff_silence_key_fn: SyncFn
    handoff_silence_meta_key_fn: SyncFn
    handoff_silence_ttl_seconds: int
    db_module: Any
    insert_message_out_fn: AsyncFn
    capture_manager_intervention_fn: AsyncFn
    amocrm_service_module: Any
    process_incoming_fn: AsyncFn


@dataclass
class _AvitoWebhookContext:
    event: Mapping[str, Any]
    normalized: Any
    payload: Mapping[str, Any]
    value: Mapping[str, Any]
    content_raw: Mapping[str, Any]
    tenant: int | None
    account_id: int | None
    item_id: int | None
    chat_id: str
    message_type: str
    text: str
    attachments: list[dict[str, Any]]
    unresolved_voice: Mapping[str, Any] | None
    message_id_str: str
    avito_user_id: int | None
    avito_login: str
    manager_outgoing: bool = False


async def handle_avito_webhook_event(
    event: Mapping[str, Any],
    request: Any,
    *,
    deps: AvitoWebhookDeps,
) -> bool:
    ctx = _build_avito_webhook_context(event, deps)
    deps.logger.warning("avito_webhook_received raw_event=%s", deps.json_module.dumps(event, ensure_ascii=False))

    if not ctx.value:
        deps.logger.warning(
            "avito_webhook_skip reason=no_value tenant=%s account_id=%s raw_event=%s",
            ctx.tenant,
            ctx.account_id,
            deps.json_module.dumps(event, ensure_ascii=False),
        )
        return False

    if not ctx.chat_id:
        deps.logger.warning(
            "avito_webhook_skip reason=no_chat account_id=%s tenant=%s raw_event=%s",
            ctx.account_id,
            ctx.tenant,
            deps.json_module.dumps(event, ensure_ascii=False),
        )
        return False

    await _resolve_avito_tenant(ctx, request, deps)
    if ctx.tenant is None or ctx.tenant <= 0:
        deps.logger.warning(
            "avito_webhook_skip reason=unknown_tenant account_id=%s chat_id=%s raw_event=%s",
            ctx.account_id,
            ctx.chat_id,
            deps.json_module.dumps(event, ensure_ascii=False),
        )
        return False

    ctx.tenant = int(ctx.tenant)
    if ctx.account_id is not None:
        try:
            from libs.core.repo import avito_accounts

            await avito_accounts.mark_webhook_seen(int(ctx.account_id))
        except Exception:
            deps.logger.debug(
                "avito_webhook_mark_seen_failed tenant=%s account_id=%s",
                ctx.tenant,
                ctx.account_id,
                exc_info=True,
            )
    _resolve_account_from_integration(ctx, deps)
    await _resolve_voice_attachment(ctx, deps)
    if await _handle_avito_manager_outgoing(ctx, deps):
        return False

    if not ctx.text and not ctx.attachments:
        deps.logger.info(
            "avito_webhook_skip reason=empty_message tenant=%s account_id=%s chat_id=%s raw_event=%s",
            ctx.tenant,
            ctx.account_id,
            ctx.chat_id,
            deps.json_module.dumps(event, ensure_ascii=False),
        )
        return False

    await deps.process_incoming_fn(_build_avito_incoming_body(ctx, deps), request)
    return True


def _build_avito_webhook_context(
    event: Mapping[str, Any],
    deps: AvitoWebhookDeps,
) -> _AvitoWebhookContext:
    normalized = deps.avito_webhook_events_module.normalize_public_webhook_event(event)
    return _AvitoWebhookContext(
        event=event,
        normalized=normalized,
        payload=normalized.payload,
        value=normalized.value,
        content_raw=normalized.content,
        tenant=None,
        account_id=normalized.account_id,
        item_id=getattr(normalized, "item_id", None),
        chat_id=normalized.chat_id,
        message_type=normalized.message_type,
        text=normalized.text,
        attachments=[dict(item) for item in normalized.attachments],
        unresolved_voice=normalized.unresolved_voice,
        message_id_str=normalized.message_id,
        avito_user_id=normalized.avito_user_id,
        avito_login=normalized.avito_login,
    )


async def _resolve_avito_tenant(
    ctx: _AvitoWebhookContext,
    request: Any,
    deps: AvitoWebhookDeps,
) -> None:
    if ctx.account_id is None:
        resolved_tenant, resolved_account = await deps.avito_module.resolve_tenant_by_chat(ctx.chat_id)
        if resolved_tenant is not None and resolved_account is not None:
            ctx.tenant = int(resolved_tenant)
            ctx.account_id = int(resolved_account)
        else:
            ctx.account_id = deps.coerce_int_fn(
                (ctx.payload.get("value") or {}).get("user_id")
                or ctx.payload.get("user_id")
                or ctx.event.get("user_id")
            )
    if ctx.account_id is not None and ctx.tenant is None:
        try:
            from libs.core.repo import avito_accounts

            account = await avito_accounts.find_active_by_account_id(int(ctx.account_id))
            if account and account.get("tenant_id") is not None:
                ctx.tenant = int(account["tenant_id"])
        except Exception:
            ctx.tenant = None
        if ctx.tenant is None:
            ctx.tenant = deps.avito_module.find_tenant_by_account(ctx.account_id)
    if ctx.tenant is None:
        ctx.tenant = deps.coerce_int_fn(ctx.payload.get("tenant") or ctx.event.get("tenant"))
    if ctx.tenant is None:
        ctx.tenant = deps.coerce_int_fn(
            request.query_params.get("tenant") or request.query_params.get("t")
        )


def _resolve_account_from_integration(
    ctx: _AvitoWebhookContext,
    deps: AvitoWebhookDeps,
) -> None:
    if ctx.account_id is not None or ctx.tenant is None:
        return
    integration = deps.avito_module.get_integration(int(ctx.tenant))
    if integration:
        ctx.account_id = deps.coerce_int_fn(integration.get("account_id"))


async def _resolve_voice_attachment(
    ctx: _AvitoWebhookContext,
    deps: AvitoWebhookDeps,
) -> None:
    if not ctx.unresolved_voice:
        return
    voice_id = str(ctx.unresolved_voice.get("voice_id") or "")
    resolved_voice_url = ""
    try:
        resolved_voice_url = await deps.avito_module.resolve_voice_url(
            int(ctx.tenant or 0),
            voice_id,
            account_id=ctx.account_id,
        )
    except Exception as exc:
        deps.logger.info(
            "avito_voice_url_resolve_failed tenant=%s account_id=%s chat_id=%s voice_id=%s error=%s",
            ctx.tenant,
            ctx.account_id,
            ctx.chat_id,
            voice_id,
            exc,
        )
    ctx.attachments.append(
        {
            "type": "voice",
            "url": str(resolved_voice_url or voice_id),
            "name": str(ctx.unresolved_voice.get("name") or "voice.mp4"),
            "mime": str(ctx.unresolved_voice.get("mime") or "audio/mp4"),
            "voice_id": voice_id,
        }
    )


async def _handle_avito_manager_outgoing(
    ctx: _AvitoWebhookContext,
    deps: AvitoWebhookDeps,
) -> bool:
    if (
        ctx.account_id is None
        or ctx.avito_user_id is None
        or ctx.avito_user_id != ctx.account_id
    ):
        return False
    lead_id = await _resolve_avito_manager_lead_id(ctx, deps)
    dedup_token = _manager_dedup_token(ctx, deps)
    if dedup_token and await _avito_manager_dedup_seen(ctx, lead_id, dedup_token, deps):
        deps.logger.info(
            "avito_webhook_manager_outgoing_dedup tenant=%s account_id=%s chat_id=%s lead_id=%s dedup_id=%s",
            ctx.tenant,
            ctx.account_id,
            ctx.chat_id,
            lead_id,
            dedup_token,
        )
        return True
    echo_detected = await _set_avito_manager_handoff(ctx, lead_id, deps)
    deps.logger.info(
        "avito_webhook_manager_outgoing tenant=%s account_id=%s chat_id=%s",
        ctx.tenant,
        ctx.account_id,
        ctx.chat_id,
    )
    ctx.manager_outgoing = True
    if not ctx.text and not ctx.attachments:
        return False
    await _process_avito_manager_content(ctx, lead_id, echo_detected, deps)
    return True


async def _resolve_avito_manager_lead_id(
    ctx: _AvitoWebhookContext,
    deps: AvitoWebhookDeps,
) -> int:
    lead_id = deps.avito_module.stable_lead_id(ctx.account_id, ctx.chat_id)
    try:
        resolved = await deps.find_lead_by_peer_fn(
            int(ctx.tenant or 0),
            "avito",
            _avito_peer_key(ctx.account_id, ctx.chat_id),
        )
    except Exception:
        resolved = None
    if resolved and resolved.get("id"):
        lead_id = int(resolved["id"])
    return int(lead_id)


def _avito_peer_key(account_id: int | None, chat_id: str) -> str:
    chat_text = str(chat_id or "").strip()
    return chat_text


def _manager_dedup_token(
    ctx: _AvitoWebhookContext,
    deps: AvitoWebhookDeps,
) -> str:
    dedup_message_id = str(ctx.message_id_str or "").strip()
    if dedup_message_id:
        return dedup_message_id
    created_hint = str(
        ctx.value.get("created")
        or ctx.content_raw.get("created")
        or ctx.payload.get("created")
        or ctx.value.get("published_at")
        or ctx.payload.get("published_at")
        or ""
    ).strip()
    return f"fp:{deps.content_fingerprint_fn(ctx.text, ctx.attachments)}:{created_hint}"


async def _avito_manager_dedup_seen(
    ctx: _AvitoWebhookContext,
    lead_id: int,
    dedup_token: str,
    deps: AvitoWebhookDeps,
) -> bool:
    dedup_message_id = str(ctx.message_id_str or "").strip()
    dedup_seen = await _avito_manager_dedup_seen_in_redis(ctx, dedup_token, deps)
    if not dedup_seen and dedup_message_id:
        dedup_seen = await _avito_manager_dedup_seen_in_db(
            ctx,
            lead_id,
            dedup_message_id,
            deps,
        )
    return dedup_seen


async def _avito_manager_dedup_seen_in_redis(
    ctx: _AvitoWebhookContext,
    dedup_token: str,
    deps: AvitoWebhookDeps,
) -> bool:
    if deps.redis_queue is None:
        return False
    try:
        dedup_ttl = 7 * 24 * 3600 if str(ctx.message_id_str or "").strip() else 180
        dedup_key = "avito:manager:outgoing:dedup:%s:%s:%s" % (
            int(ctx.tenant or 0),
            str(ctx.chat_id),
            dedup_token,
        )
        accepted = await deps.redis_queue.set(dedup_key, "1", ex=dedup_ttl, nx=True)
        return not bool(accepted)
    except Exception:
        deps.logger.debug(
            "avito_outgoing_dedup_cache_failed tenant=%s chat_id=%s",
            ctx.tenant,
            ctx.chat_id,
            exc_info=True,
        )
        return False


async def _avito_manager_dedup_seen_in_db(
    ctx: _AvitoWebhookContext,
    lead_id: int,
    dedup_message_id: str,
    deps: AvitoWebhookDeps,
) -> bool:
    fetchrow = getattr(deps.db_module, "_fetchrow", None)
    if not fetchrow:
        return False
    try:
        row = await fetchrow(
            """
            SELECT 1
            FROM messages
            WHERE tenant_id = $1
              AND lead_id = $2
              AND direction = 1
              AND provider_msg_id = $3
              AND COALESCE(source, '') = 'manager'
            LIMIT 1
            """,
            int(ctx.tenant or 0),
            int(lead_id),
            dedup_message_id,
        )
        return bool(row)
    except Exception:
        deps.logger.debug(
            "avito_outgoing_dedup_db_check_failed tenant=%s chat_id=%s lead_id=%s",
            ctx.tenant,
            ctx.chat_id,
            lead_id,
            exc_info=True,
        )
        return False


async def _set_avito_manager_handoff(
    ctx: _AvitoWebhookContext,
    lead_id: int,
    deps: AvitoWebhookDeps,
) -> bool:
    if deps.redis_queue is None:
        deps.logger.debug(
            "handoff_flag_set_skipped_no_redis tenant=%s lead_id=%s",
            ctx.tenant,
            lead_id,
        )
        return False
    try:
        echo_detected = await _avito_manager_echo_detected(ctx, lead_id, deps)
        if not echo_detected:
            timestamp = int(deps.time_module.time())
            await deps.redis_queue.set(
                deps.handoff_silence_key_fn(int(ctx.tenant or 0), int(lead_id)),
                str(timestamp),
                ex=deps.handoff_silence_ttl_seconds,
            )
            meta_key = deps.handoff_silence_meta_key_fn(int(ctx.tenant or 0), int(lead_id))
            if meta_key:
                handoff_payload = {"reason": "manager_outgoing", "ts": timestamp}
                await deps.redis_queue.set(
                    meta_key,
                    deps.json_module.dumps(handoff_payload, ensure_ascii=False),
                    ex=deps.handoff_silence_ttl_seconds,
                )
        return echo_detected
    except Exception:
        deps.logger.debug(
            "handoff_flag_set_failed tenant=%s chat_id=%s",
            ctx.tenant,
            ctx.chat_id,
            exc_info=True,
        )
        return False


async def _avito_manager_echo_detected(
    ctx: _AvitoWebhookContext,
    lead_id: int,
    deps: AvitoWebhookDeps,
) -> bool:
    echo_detected = await _matches_cached_avito_echo(ctx, deps)
    if not echo_detected and ctx.text:
        echo_detected = await deps.is_recent_bot_echo_fn(
            int(ctx.tenant or 0),
            int(lead_id),
            ctx.text,
        )
    return bool(echo_detected)


async def _matches_cached_avito_echo(
    ctx: _AvitoWebhookContext,
    deps: AvitoWebhookDeps,
) -> bool:
    echo_key = deps.avito_bot_echo_key_fn(int(ctx.tenant or 0), ctx.chat_id)
    echo_payload = await deps.redis_queue.get(echo_key)
    if not echo_payload:
        return False
    try:
        payload = deps.json_module.loads(echo_payload)
    except Exception:
        payload = {}
    cached_text = ""
    cached_extra: list[str] = []
    if isinstance(payload, Mapping):
        cached_text = deps.normalize_echo_text_fn(str(payload.get("text") or ""))
        extra_raw = payload.get("extra")
        if isinstance(extra_raw, list):
            cached_extra = [
                deps.normalize_echo_text_fn(str(entry or ""))
                for entry in extra_raw
                if str(entry or "").strip()
            ]
    incoming_markers: list[str] = []
    incoming_text = deps.normalize_echo_text_fn(ctx.text)
    if incoming_text:
        incoming_markers.append(incoming_text)
    if ctx.attachments:
        incoming_markers.append("__image__")
    if not incoming_markers:
        return False
    cached_values = [cached_text, *cached_extra]
    return any(candidate and candidate in cached_values for candidate in incoming_markers)


async def _process_avito_manager_content(
    ctx: _AvitoWebhookContext,
    lead_id: int,
    echo_detected: bool,
    deps: AvitoWebhookDeps,
) -> None:
    deps.logger.info(
        "avito_outgoing_eval tenant=%s chat_id=%s echo=%s text_len=%s attachments=%s",
        ctx.tenant,
        ctx.chat_id,
        int(bool(echo_detected)),
        len(ctx.text or ""),
        len(ctx.attachments),
    )
    if not echo_detected:
        echo_detected = await _matches_amocrm_manager_echo(ctx, lead_id, deps)
    if not echo_detected:
        await _store_avito_manager_outgoing(ctx, lead_id, deps)
    else:
        deps.logger.info(
            "avito_outgoing_skipped_echo tenant=%s chat_id=%s",
            ctx.tenant,
            ctx.chat_id,
        )


async def _matches_amocrm_manager_echo(
    ctx: _AvitoWebhookContext,
    lead_id: int,
    deps: AvitoWebhookDeps,
) -> bool:
    if deps.redis_queue is None:
        return False
    try:
        fp = deps.content_fingerprint_fn(ctx.text, ctx.attachments)
        amo_echo_key = "amocrm:manager:echo:%s:%s:%s" % (
            int(ctx.tenant or 0),
            int(lead_id),
            fp,
        )
        if await deps.redis_queue.get(amo_echo_key):
            deps.logger.info(
                "avito_outgoing_skipped_amocrm_echo tenant=%s chat_id=%s lead_id=%s",
                ctx.tenant,
                ctx.chat_id,
                lead_id,
            )
            return True
        amo_echo_chat_key = "amocrm:manager:echo:chat:%s:%s:%s" % (
            int(ctx.tenant or 0),
            str(ctx.chat_id),
            fp,
        )
        if await deps.redis_queue.get(amo_echo_chat_key):
            deps.logger.info(
                "avito_outgoing_skipped_amocrm_echo_chat tenant=%s chat_id=%s lead_id=%s",
                ctx.tenant,
                ctx.chat_id,
                lead_id,
            )
            return True
    except Exception:
        deps.logger.debug(
            "avito_outgoing_amocrm_echo_check_failed tenant=%s chat_id=%s",
            ctx.tenant,
            ctx.chat_id,
            exc_info=True,
        )
    return False


async def _store_avito_manager_outgoing(
    ctx: _AvitoWebhookContext,
    lead_id: int,
    deps: AvitoWebhookDeps,
) -> None:
    display_text = ctx.text or ("Вложение" if ctx.attachments else "")
    try:
        stored_id = await deps.insert_message_out_fn(
            lead_id,
            display_text,
            ctx.message_id_str,
            status="sent",
            tenant_id=int(ctx.tenant or 0),
            channel="avito",
            is_bot=False,
            attachments=ctx.attachments or None,
            source="manager",
        )
        deps.logger.info(
            "avito_outgoing_stored tenant=%s chat_id=%s lead_id=%s msg_id=%s",
            ctx.tenant,
            ctx.chat_id,
            lead_id,
            stored_id,
        )
        await deps.capture_manager_intervention_fn(
            tenant_id=int(ctx.tenant or 0),
            lead_id=int(lead_id),
            channel="avito",
            manager_message_id=int(stored_id) if stored_id else None,
            source_event="avito_webhook_outgoing",
        )
    except Exception as exc:
        deps.logger.warning(
            "avito_outgoing_store_failed tenant=%s chat_id=%s error=%s",
            ctx.tenant,
            ctx.chat_id,
            exc,
        )
    try:
        await deps.amocrm_service_module.amocrm_on_outbound_message(
            int(ctx.tenant or 0),
            int(lead_id),
            text=display_text or "",
            channel="avito",
            attachments=ctx.attachments or None,
            source_role="manager",
        )
    except Exception:
        deps.logger.exception(
            "avito_outgoing_amocrm_sync_failed tenant=%s chat_id=%s lead_id=%s",
            ctx.tenant,
            ctx.chat_id,
            lead_id,
        )


def _build_avito_incoming_body(
    ctx: _AvitoWebhookContext,
    deps: AvitoWebhookDeps,
) -> dict[str, Any]:
    lead_id = deps.avito_module.stable_lead_id(ctx.account_id, ctx.chat_id)
    incoming_body: dict[str, Any] = {
        "provider": "avito",
        "channel": "avito",
        "tenant": ctx.tenant,
        "tenant_id": ctx.tenant,
        "manager": ctx.manager_outgoing,
        "out": ctx.manager_outgoing,
        "account_id": ctx.account_id,
        "item_id": ctx.item_id,
        "chat_id": ctx.chat_id,
        "lead_id": lead_id,
        "avito_user_id": ctx.avito_user_id,
        "avito_login": ctx.avito_login,
        "source": {
            "type": "avito",
            "tenant": ctx.tenant,
            "account_id": ctx.account_id,
            "item_id": ctx.item_id,
            "chat_id": ctx.chat_id,
        },
        "message": {
            "id": ctx.message_id_str,
            "message_id": ctx.message_id_str,
            "text": ctx.text,
            "chat_id": ctx.chat_id,
            "item_id": ctx.item_id,
            "direction": ctx.message_type,
            "attachments": ctx.attachments,
            "author_id": ctx.avito_user_id,
        },
        "attachments": ctx.attachments,
        "peer": ctx.chat_id,
        "auto_reply_handled": False,
        "avito": {
            "account_id": ctx.account_id,
            "item_id": ctx.item_id,
            "chat_id": ctx.chat_id,
            "user_id": ctx.avito_user_id,
            "login": ctx.avito_login,
        },
    }
    if ctx.normalized.created_at is not None:
        incoming_body["message"]["created_at"] = ctx.normalized.created_at
    if ctx.normalized.published_at is not None:
        incoming_body["message"]["published_at"] = ctx.normalized.published_at
    lead_contacts = {"avito": {"peer": ctx.chat_id}}
    if ctx.avito_login:
        lead_contacts["avito"]["contact"] = ctx.avito_login
    incoming_body["lead_contacts"] = lead_contacts
    return incoming_body
