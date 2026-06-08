from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Optional


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
LogFn = Callable[[str], None]


@dataclass(frozen=True)
class OutboxSendDeps:
    default_tenant_id: int
    outbox_enabled: bool
    outbox_enabled_raw: str
    send_enabled: bool
    redis_client: Any
    json_module: Any
    log_fn: LogFn
    db_errors_counter: Any
    outbox_payloads_module: Any
    queue_contract_module: Any
    outbox_queue_key: str
    outbox_dlq_key: str
    normalize_tg_slot_fn: SyncFn
    whitelist_allows_fn: AsyncFn
    lead_exists_fn: AsyncFn
    coerce_int_fn: SyncFn
    get_lead_peer_fn: AsyncFn
    get_telegram_user_id_by_lead_fn: AsyncFn
    find_lead_by_telegram_fn: AsyncFn
    normalize_username_fn: SyncFn
    upsert_lead_fn: AsyncFn
    get_lead_tg_slot_fn: AsyncFn
    telegram_slot_is_enabled_fn: SyncFn
    is_manager_message_fn: SyncFn
    is_followup_message_fn: SyncFn
    mark_handoff_silence_fn: AsyncFn
    collect_outgoing_attachments_fn: SyncFn
    insert_message_out_fn: AsyncFn
    prepare_internal_attachment_fn: AsyncFn
    tenant_whatsapp_provider_fn: SyncFn
    send_whatsapp_baileys_fn: AsyncFn
    send_whatsapp_fn: AsyncFn
    avito_bot_echo_key_fn: SyncFn
    avito_bot_echo_ttl_seconds: int
    send_avito_fn: AsyncFn
    send_telegram_fn: AsyncFn
    send_max_fn: AsyncFn
    send_max_personal_fn: AsyncFn
    cache_max_bot_echo_fn: AsyncFn
    update_message_status_fn: AsyncFn
    amocrm_service_module: Any


@dataclass
class OutboxSendState:
    item: dict[str, Any]
    deps: OutboxSendDeps
    channel: str
    text: str | None
    lead_id: int
    phone: str | None
    raw_to: Any
    to_peer_raw: Any
    peer_raw: Any
    peer_value: str | None
    username: str | None
    item_tg_slot: int
    telegram_user_id: int | None
    primary_telegram_user_id: int | None
    tenant: int
    attachment: dict[str, Any] | None
    attachments: list[dict[str, Any]] | None
    reply_to: str | None
    avito_account_id: int | None
    avito_chat_id_hint: str | None
    max_user_id: Any
    max_chat_id_hint: str | None
    cached_whatsapp_jid: str | None = None
    explicit_to_jid: Any = None
    message_db_id: int | None = None
    actual_lead_id: int = 0
    manager_message: bool = False


async def do_send(item: dict, deps: OutboxSendDeps) -> tuple[str, str, str, int]:
    send_plan = deps.outbox_payloads_module.build_outbox_item_plan(
        item,
        default_tenant_id=deps.default_tenant_id,
        normalize_tg_slot_fn=deps.normalize_tg_slot_fn,
    )
    state = _outbox_send_state(item, send_plan, deps)
    preflight = await _preflight_outbox_send(state)
    if preflight is not None:
        return preflight
    telegram_result = await _prepare_telegram_if_needed(state)
    if telegram_result is not None:
        return telegram_result
    await _mark_manager_outgoing_if_needed(state)
    st, body = await _send_by_channel_state(state)
    status_str, reason_str = await _handle_outbox_send_result(state, st)
    deps.log_fn(
        f"event=send_result status={status_str} reason={reason_str} "
        f"channel={state.channel} lead_id={state.actual_lead_id} code={st}"
    )
    return (status_str, reason_str, body, st)


def _outbox_send_state(item: dict[str, Any], send_plan: Any, deps: OutboxSendDeps) -> OutboxSendState:
    message_db_id_raw = deps.coerce_int_fn(item.get("_message_db_id"))
    message_db_id: Optional[int] = (
        message_db_id_raw if message_db_id_raw and message_db_id_raw > 0 else None
    )
    return OutboxSendState(
        item=item,
        deps=deps,
        channel=send_plan.channel,
        text=send_plan.text,
        lead_id=send_plan.lead_id,
        phone=send_plan.phone,
        raw_to=send_plan.raw_to,
        to_peer_raw=send_plan.to_peer_raw,
        peer_raw=send_plan.peer_raw,
        peer_value=send_plan.peer_value,
        username=send_plan.username,
        item_tg_slot=send_plan.item_tg_slot,
        telegram_user_id=send_plan.telegram_user_id,
        primary_telegram_user_id=send_plan.primary_telegram_user_id,
        tenant=send_plan.tenant_id,
        attachment=send_plan.attachment,
        attachments=send_plan.attachments,
        reply_to=send_plan.reply_to,
        avito_account_id=send_plan.avito_account_id,
        avito_chat_id_hint=send_plan.avito_chat_id_hint,
        max_user_id=send_plan.max_user_id,
        max_chat_id_hint=send_plan.max_chat_id_hint,
        explicit_to_jid=item.get("to_jid") if send_plan.channel == "whatsapp" else None,
        message_db_id=message_db_id,
        actual_lead_id=send_plan.lead_id,
    )


async def _preflight_outbox_send(state: OutboxSendState) -> tuple[str, str, str, int] | None:
    empty_result = _empty_outbox_send_result(state)
    if empty_result is not None:
        return empty_result
    if state.channel == "whatsapp" and state.lead_id > 0:
        state.cached_whatsapp_jid = await state.deps.outbox_payloads_module.resolve_cached_whatsapp_jid(
            state.deps.redis_client, state.tenant, state.lead_id
        )
    if state.channel != "telegram" and state.lead_id <= 0:
        state.deps.log_fn(
            f"event=send_result status=skipped reason=missing_lead channel={state.channel} lead_id={state.lead_id}"
        )
        return ("skipped", "missing_lead", "", 0)
    if not state.deps.outbox_enabled:
        env_hint = state.deps.outbox_enabled_raw or "1"
        state.deps.log_fn(
            "event=send_result status=skipped reason=outbox_disabled "
            f"channel={state.channel} lead_id={state.lead_id} outbox_enabled_env={env_hint}"
        )
        return ("skipped", "outbox_disabled", "", 0)
    _fill_max_raw_to(state)
    whitelist = await _check_outbox_whitelist(state)
    if whitelist is not None:
        return whitelist
    await _warn_if_outbox_lead_unknown(state)
    if not state.deps.send_enabled:
        state.deps.log_fn(
            f"event=send_result status=dry-run reason=send_disabled channel={state.channel} lead_id={state.lead_id}"
        )
        return ("skipped", "dry-run", "", 0)
    return None


def _empty_outbox_send_result(state: OutboxSendState) -> tuple[str, str, str, int] | None:
    if state.text or state.attachment or state.attachments:
        return None
    state.deps.log_fn(
        f"event=send_result status=skipped reason=empty channel={state.channel} lead_id={state.lead_id}"
    )
    return ("skipped", "empty", "", 0)


def _fill_max_raw_to(state: OutboxSendState) -> None:
    if state.channel not in {"max", "max_personal"} or state.raw_to is not None:
        return
    if state.peer_value:
        state.raw_to = state.peer_value
    elif state.max_user_id is not None:
        state.raw_to = state.max_user_id


async def _check_outbox_whitelist(state: OutboxSendState) -> tuple[str, str, str, int] | None:
    if state.channel == "telegram":
        return None
    allowed, whitelist_reason = await state.deps.whitelist_allows_fn(
        telegram_user_id=state.telegram_user_id,
        username=state.username,
        raw_to=state.raw_to,
        lead_id=state.lead_id,
        tenant_id=state.tenant,
        channel=state.channel,
    )
    if allowed:
        return None
    state.deps.log_fn(
        "event=send_result status=skipped reason=whitelist_miss "
        f"channel={state.channel} lead_id={state.lead_id} telegram_user_id={state.telegram_user_id} "
        f"username={state.username} raw_to={state.raw_to} whitelist_reason={whitelist_reason}"
    )
    return ("skipped", "whitelist", "", 0)


async def _warn_if_outbox_lead_unknown(state: OutboxSendState) -> None:
    if state.channel == "telegram":
        return
    lead_known = False
    try:
        lead_known = await state.deps.lead_exists_fn(state.lead_id, tenant_id=state.tenant)
    except Exception as exc:
        state.deps.db_errors_counter.labels("lead_exists").inc()
        state.deps.log_fn(
            "event=send_result status=warning reason=db_error operation=lead_exists "
            f"channel={state.channel} lead_id={state.lead_id} error={exc}"
        )
    if not lead_known:
        state.deps.log_fn(
            f"event=send_result status=warning reason=err:no_lead channel={state.channel} lead_id={state.lead_id}"
        )


async def _prepare_telegram_if_needed(state: OutboxSendState) -> tuple[str, str, str, int] | None:
    if state.channel != "telegram":
        return None
    telegram_state = await _prepare_telegram_outbox_send(
        item=state.item,
        tenant=state.tenant,
        lead_id=state.lead_id,
        primary_telegram_user_id=state.primary_telegram_user_id,
        telegram_user_id=state.telegram_user_id,
        username=state.username,
        peer_value=state.peer_value,
        peer_raw=state.peer_raw,
        to_peer_raw=state.to_peer_raw,
        item_tg_slot=state.item_tg_slot,
        text=state.text,
        attachments=state.attachments,
        message_db_id=state.message_db_id,
        deps=state.deps,
    )
    if telegram_state.result is not None:
        return telegram_state.result
    state.message_db_id = telegram_state.message_db_id
    state.actual_lead_id = telegram_state.actual_lead_id
    state.telegram_user_id = telegram_state.telegram_user_id
    state.item["tg_slot"] = telegram_state.tg_slot
    state.peer_value = telegram_state.peer_value
    return None


async def _mark_manager_outgoing_if_needed(state: OutboxSendState) -> None:
    state.manager_message = state.deps.is_manager_message_fn(state.item)
    if not state.manager_message or state.actual_lead_id <= 0:
        return
    await state.deps.mark_handoff_silence_fn(
        state.tenant,
        state.actual_lead_id,
        reason="manager_outgoing",
        contact_hint=state.item.get("peer") or state.item.get("contact"),
        username_hint=state.item.get("username"),
    )


async def _send_by_channel_state(state: OutboxSendState) -> tuple[int, str]:
    if state.channel == "whatsapp":
        return await _send_whatsapp_outbox_channel(state)
    if state.channel == "avito":
        return await _send_avito_outbox_channel(state)
    if state.channel == "telegram":
        return await _send_telegram_outbox_channel(state)
    if state.channel in {"max", "max_personal"}:
        return await _send_max_outbox_channel(state)
    recipient_value = state.raw_to if isinstance(state.raw_to, str) and state.raw_to.strip() else state.phone
    return await state.deps.send_whatsapp_fn(
        state.tenant,
        recipient_value or "",
        state.text or None,
        state.attachment,
        state.attachments or None,
    )


async def _handle_outbox_send_result(state: OutboxSendState, status_code: int) -> tuple[str, str]:
    outcome = state.deps.outbox_payloads_module.build_send_outcome(status_code)
    if state.message_db_id:
        await _update_outbox_message_status(state, status_code)
    if 200 <= status_code < 300 and state.actual_lead_id and state.actual_lead_id > 0:
        await _notify_amocrm_outbound(state)
    return str(outcome.status), str(outcome.reason)


async def _update_outbox_message_status(state: OutboxSendState, status_code: int) -> None:
    new_status = "sent" if 200 <= status_code < 300 else "failed"
    try:
        await state.deps.update_message_status_fn(state.message_db_id, new_status)
    except Exception as exc:
        state.deps.log_fn(
            "event=send_result status=warning reason=update_message_status_failed "
            f"channel={state.channel} message_id={state.message_db_id} error={exc}"
        )


async def _notify_amocrm_outbound(state: OutboxSendState) -> None:
    outbound_attachments = state.deps.outbox_payloads_module.build_outbound_attachment_snapshot(
        primary=state.attachment,
        attachments=state.attachments,
    )
    try:
        await state.deps.amocrm_service_module.amocrm_on_outbound_message(
            int(state.tenant),
            int(state.actual_lead_id),
            text=state.text or "",
            channel=str(state.channel),
            attachments=outbound_attachments or None,
            source_role=("manager" if state.manager_message else "bot"),
        )
    except Exception as exc:
        state.deps.log_fn(
            "event=amocrm_outbound_note_failed "
            f"channel={state.channel} tenant={state.tenant} lead_id={state.actual_lead_id} error={exc}"
        )


@dataclass(frozen=True)
class TelegramOutboxState:
    result: tuple[str, str, str, int] | None
    message_db_id: int | None
    actual_lead_id: int
    telegram_user_id: int | None
    title_hint: str | None
    tg_slot: int
    peer_value: str | None


@dataclass
class TelegramPrepareContext:
    item: dict[str, Any]
    tenant: int
    lead_id: int
    primary_telegram_user_id: int | None
    telegram_user_id: int | None
    username: str | None
    peer_value: str | None
    peer_raw: Any
    to_peer_raw: Any
    item_tg_slot: int
    text: str | None
    attachments: list[dict[str, Any]] | None
    message_db_id: int | None
    deps: OutboxSendDeps
    title_hint: str | None = None
    chat_id: int | None = None
    resolved_lead_id: int | None = None
    resolved_tg_slot: int = 0


async def _prepare_telegram_outbox_send(
    *,
    item: dict[str, Any],
    tenant: int,
    lead_id: int,
    primary_telegram_user_id: int | None,
    telegram_user_id: int | None,
    username: str | None,
    peer_value: str | None,
    peer_raw: Any,
    to_peer_raw: Any,
    item_tg_slot: int,
    text: str | None,
    attachments: list[dict[str, Any]] | None,
    message_db_id: int | None,
    deps: OutboxSendDeps,
) -> TelegramOutboxState:
    ctx = TelegramPrepareContext(
        item=item,
        tenant=tenant,
        lead_id=lead_id,
        primary_telegram_user_id=primary_telegram_user_id,
        telegram_user_id=telegram_user_id,
        username=username,
        peer_value=peer_value,
        peer_raw=peer_raw,
        to_peer_raw=to_peer_raw,
        item_tg_slot=item_tg_slot,
        text=text,
        attachments=attachments,
        message_db_id=message_db_id,
        deps=deps,
        resolved_tg_slot=item_tg_slot,
    )
    result = await _resolve_telegram_prepare_target(ctx)
    if result is not None:
        return result
    result = await _upsert_telegram_prepare_lead(ctx)
    if result is not None:
        return result
    result = await _resolve_telegram_prepare_slot(ctx)
    if result is not None:
        return result
    result = await _insert_telegram_prepare_message(ctx)
    if result is not None:
        return result
    if ctx.message_db_id:
        ctx.item["_message_db_id"] = ctx.message_db_id
        ctx.item["_resolved_lead_id"] = ctx.resolved_lead_id
    return TelegramOutboxState(
        None,
        ctx.message_db_id,
        int(ctx.resolved_lead_id or 0),
        ctx.telegram_user_id,
        ctx.title_hint,
        ctx.resolved_tg_slot,
        ctx.peer_value,
    )


async def _resolve_telegram_prepare_target(
    ctx: TelegramPrepareContext,
) -> TelegramOutboxState | None:
    from_candidate = ctx.deps.coerce_int_fn(ctx.item.get("from"))
    if from_candidate is not None and from_candidate <= 0:
        from_candidate = None
    await _fill_telegram_prepare_peer(ctx)
    db_lookup_result = await _lookup_telegram_user_id_for_send(ctx)
    if isinstance(db_lookup_result, TelegramOutboxState):
        return db_lookup_result
    chat_candidates = ctx.deps.outbox_payloads_module.build_telegram_chat_candidates(
        primary_telegram_user_id=ctx.primary_telegram_user_id,
        db_lookup_result=db_lookup_result,
        from_candidate=from_candidate,
        peer_value=ctx.peer_value,
    )
    ctx.chat_id = ctx.deps.outbox_payloads_module.first_positive_candidate(chat_candidates)
    if ctx.chat_id is None or ctx.chat_id <= 0:
        ctx.deps.log_fn(
            "event=send_result status=skipped reason=missing_peer channel=telegram lead_id=%s"
            % ctx.lead_id
        )
        return _telegram_prepare_result(ctx, ("skipped", "missing_peer", "", 0))
    ctx.telegram_user_id = ctx.chat_id
    return None


async def _fill_telegram_prepare_peer(ctx: TelegramPrepareContext) -> None:
    if ctx.peer_value is None and ctx.lead_id > 0:
        try:
            stored_peer = await ctx.deps.get_lead_peer_fn(ctx.lead_id, channel="telegram")
        except Exception as exc:
            ctx.deps.db_errors_counter.labels("get_lead_peer").inc()
            ctx.deps.log_fn(
                "event=send_peer_lookup_failed channel=telegram lead_id=%s error=%s"
                % (ctx.lead_id, exc)
            )
            stored_peer = None
        if stored_peer:
            ctx.peer_value = stored_peer
    if ctx.peer_value and not ctx.to_peer_raw:
        ctx.item["to_peer"] = ctx.peer_value


async def _lookup_telegram_user_id_for_send(
    ctx: TelegramPrepareContext,
) -> int | None | TelegramOutboxState:
    if ctx.primary_telegram_user_id is not None or ctx.lead_id <= 0:
        return None
    try:
        return await ctx.deps.get_telegram_user_id_by_lead_fn(ctx.lead_id)
    except Exception as exc:
        ctx.deps.db_errors_counter.labels("get_telegram_user_id_by_lead").inc()
        ctx.deps.log_fn(
            "event=send_result status=skipped reason=db_error operation=get_telegram_user_id_by_lead "
            f"channel=telegram lead_id={ctx.lead_id} error={exc}"
        )
        return _telegram_prepare_result(ctx, ("skipped", "db_error", "", 0))


async def _upsert_telegram_prepare_lead(
    ctx: TelegramPrepareContext,
) -> TelegramOutboxState | None:
    ctx.resolved_lead_id = ctx.lead_id if ctx.lead_id > 0 else None
    if ctx.resolved_lead_id is None:
        result = await _find_telegram_prepare_lead(ctx)
        if result is not None:
            return result
    ctx.title_hint = ctx.deps.outbox_payloads_module.normalize_telegram_title(
        title=ctx.item.get("title"),
        username=ctx.username,
        telegram_user_id=int(ctx.telegram_user_id),
        normalize_username_fn=ctx.deps.normalize_username_fn,
    )
    result = await _upsert_telegram_prepare_lead_row(ctx)
    if result is not None:
        return result
    if ctx.resolved_lead_id is None and ctx.telegram_user_id is not None:
        ctx.resolved_lead_id = int(ctx.telegram_user_id)
    if ctx.resolved_lead_id is None or ctx.resolved_lead_id <= 0:
        ctx.deps.log_fn(
            "event=send_result status=skipped reason=missing_lead "
            f"channel=telegram tenant={ctx.tenant} telegram_user_id={ctx.telegram_user_id}"
        )
        return _telegram_prepare_result(ctx, ("skipped", "missing_lead", "", 0))
    return None


async def _find_telegram_prepare_lead(ctx: TelegramPrepareContext) -> TelegramOutboxState | None:
    try:
        found_lead = await ctx.deps.find_lead_by_telegram_fn(ctx.tenant, int(ctx.telegram_user_id))
    except Exception as exc:
        ctx.deps.db_errors_counter.labels("find_lead_by_telegram").inc()
        ctx.deps.log_fn(
            "event=send_result status=skipped reason=db_error operation=find_lead_by_telegram "
            f"channel=telegram telegram_user_id={ctx.telegram_user_id} error={exc}"
        )
        return _telegram_prepare_result(ctx, ("skipped", "db_error", "", 0))
    if found_lead and found_lead > 0:
        ctx.resolved_lead_id = int(found_lead)
    return None


async def _upsert_telegram_prepare_lead_row(
    ctx: TelegramPrepareContext,
) -> TelegramOutboxState | None:
    normalized_username = ctx.deps.normalize_username_fn(ctx.username)
    upsert_kwargs = {
        "channel": "telegram",
        "tenant_id": ctx.tenant,
        "telegram_username": ctx.username,
        "title": ctx.title_hint,
        "peer_id": ctx.telegram_user_id,
        "peer": ctx.peer_value,
        "contact": normalized_username or ctx.username,
    }
    if ctx.telegram_user_id is not None:
        upsert_kwargs["telegram_user_id"] = int(ctx.telegram_user_id)
    try:
        upsert_result = await ctx.deps.upsert_lead_fn(
            ctx.resolved_lead_id if ctx.resolved_lead_id else None,
            **upsert_kwargs,
        )
    except Exception as exc:
        ctx.deps.db_errors_counter.labels("upsert_lead").inc()
        ctx.deps.log_fn(
            "event=send_result status=skipped reason=db_error operation=upsert_lead "
            f"channel=telegram lead_id={ctx.resolved_lead_id or 0} error={exc}"
        )
        return _telegram_prepare_result(ctx, ("skipped", "db_error", "", 0))
    if upsert_result is not None:
        try:
            ctx.resolved_lead_id = int(upsert_result)
        except Exception:
            pass
    return None


async def _resolve_telegram_prepare_slot(
    ctx: TelegramPrepareContext,
) -> TelegramOutboxState | None:
    if ctx.resolved_lead_id and ctx.resolved_lead_id > 0:
        stored_slot = await ctx.deps.get_lead_tg_slot_fn(ctx.tenant, ctx.resolved_lead_id)
        if stored_slot is not None:
            ctx.resolved_tg_slot = stored_slot
    if ctx.deps.telegram_slot_is_enabled_fn(ctx.tenant, ctx.resolved_tg_slot):
        return None
    ctx.deps.log_fn(
        "event=send_result status=skipped reason=tg_slot_disabled "
        f"channel=telegram tenant={ctx.tenant} slot={ctx.resolved_tg_slot} lead_id={ctx.resolved_lead_id}"
    )
    return _telegram_prepare_result(ctx, ("skipped", "tg_slot_disabled", "", 0))


async def _insert_telegram_prepare_message(
    ctx: TelegramPrepareContext,
) -> TelegramOutboxState | None:
    ctx.deps.log_fn(
        "event=send_attempt channel=telegram tenant=%s slot=%s lead_id=%s send_target=%s"
        % (ctx.tenant, ctx.resolved_tg_slot, ctx.resolved_lead_id, ctx.chat_id)
    )
    if ctx.message_db_id is not None:
        return None
    try:
        ctx.message_db_id = await ctx.deps.insert_message_out_fn(
            ctx.resolved_lead_id,
            ctx.text,
            None,
            **_telegram_prepare_insert_kwargs(ctx),
        )
    except Exception as exc:
        ctx.deps.db_errors_counter.labels("insert_message_out").inc()
        ctx.deps.log_fn(
            "event=send_result status=skipped reason=db_error operation=insert_message_out "
            f"channel=telegram lead_id={ctx.resolved_lead_id} error={exc}"
        )
        return _telegram_prepare_result(ctx, ("skipped", "db_error", "", 0))
    return None


def _telegram_prepare_insert_kwargs(ctx: TelegramPrepareContext) -> dict[str, Any]:
    insert_context = ctx.deps.outbox_payloads_module.OutboxWriteResultContext(
        lead_id=ctx.resolved_lead_id,
        tenant_id=ctx.tenant,
        channel="telegram",
        text=ctx.text,
        telegram_user_id=ctx.telegram_user_id,
        peer_value=str(ctx.chat_id),
        username=ctx.username,
        stored_message_id=None,
    )
    insert_kwargs = ctx.deps.outbox_payloads_module.build_insert_message_out_kwargs(
        context=insert_context,
        status="queued",
        is_manager=ctx.deps.is_manager_message_fn(ctx.item),
        is_followup=ctx.deps.is_followup_message_fn(ctx.item),
        attachments=ctx.deps.collect_outgoing_attachments_fn(ctx.item, ctx.tenant) or None,
        tg_slot=ctx.item.get("tg_slot"),
    )
    insert_kwargs["title"] = ctx.title_hint
    return insert_kwargs


def _telegram_prepare_result(
    ctx: TelegramPrepareContext,
    result: tuple[str, str, str, int],
) -> TelegramOutboxState:
    return TelegramOutboxState(
        result,
        ctx.message_db_id,
        int(ctx.resolved_lead_id or ctx.lead_id),
        ctx.telegram_user_id,
        ctx.title_hint,
        ctx.resolved_tg_slot,
        ctx.peer_value,
    )


async def _send_whatsapp_outbox_channel(state: OutboxSendState) -> tuple[int, str]:
    deps = state.deps
    prepared_whatsapp = await deps.outbox_payloads_module.prepare_whatsapp_attachments(
        primary=state.attachment,
        attachments=state.attachments,
        prepare_attachment_fn=deps.prepare_internal_attachment_fn,
    )
    recipient_value = state.raw_to if isinstance(state.raw_to, str) and state.raw_to.strip() else state.phone
    if deps.tenant_whatsapp_provider_fn(state.tenant) == "baileys":
        return await _send_baileys_outbox_channel(
            state=state,
            prepared_attachments=prepared_whatsapp.all_items,
        )
    st, body = await deps.send_whatsapp_fn(
        state.tenant,
        recipient_value or "",
        state.text or None,
        prepared_whatsapp.primary,
        prepared_whatsapp.all_items or None,
    )
    if st == 401:
        await _handle_waweb_auth_retry(state, st, body)
    return st, body


async def _send_baileys_outbox_channel(
    *,
    state: OutboxSendState,
    prepared_attachments: list[dict[str, Any]],
) -> tuple[int, str]:
    deps = state.deps
    recipient_plan = deps.outbox_payloads_module.plan_baileys_recipient(
        explicit_to_jid=state.explicit_to_jid,
        cached_whatsapp_jid=state.cached_whatsapp_jid,
        raw_to=state.raw_to,
        phone=state.phone,
        normalize_jid_fn=deps.outbox_payloads_module.normalize_baileys_jid,
    )
    _log_baileys_outbox_plan(state, recipient_plan)
    if recipient_plan.missing:
        return (0, "missing_recipient")
    meta_payload = state.item.get("meta") if isinstance(state.item.get("meta"), Mapping) else None
    return await deps.send_whatsapp_baileys_fn(
        state.tenant,
        recipient_plan.recipient or "",
        state.text or None,
        prepared_attachments or None,
        meta_payload,
    )


def _log_baileys_outbox_plan(state: OutboxSendState, recipient_plan: Any) -> None:
    parts = [
        "[BAILEYS OUTBOUND]",
        f"tenant={state.tenant}",
        f"lead_id={state.actual_lead_id}",
        f"raw_to={state.raw_to or '-'}",
        f"to_jid={state.explicit_to_jid or '-'}",
        f"cached_jid={state.cached_whatsapp_jid or '-'}",
    ]
    if recipient_plan.missing:
        state.deps.log_fn(" ".join(parts + ["final_jid=-", "status=skipped_missing_recipient"]))
        return
    state.deps.log_fn(
        " ".join(parts + [f"final_jid={recipient_plan.recipient or '-'}", f"source={recipient_plan.source or '-'}"])
    )


async def _handle_waweb_auth_retry(
    state: OutboxSendState,
    status: int,
    body: str,
) -> None:
    deps = state.deps
    retry_plan = deps.outbox_payloads_module.plan_waweb_auth_retry(state.item, body=body)
    deps.log_fn(
        f"event=waweb_auth_error tenant={state.tenant} lead_id={state.actual_lead_id} "
        f"phone={state.phone or '-'} attempt={retry_plan.attempt} code={status} body={retry_plan.body_hint or '-'}"
    )
    target_queue = deps.outbox_dlq_key if retry_plan.should_dlq else deps.outbox_queue_key
    try:
        await deps.queue_contract_module.push_json_left(
            deps.redis_client,
            target_queue,
            retry_plan.payload,
        )
    except Exception:
        if not retry_plan.should_dlq:
            deps.log_fn(
                f"event=waweb_auth_error action=requeue_failed tenant={state.tenant} lead_id={state.actual_lead_id}"
            )


async def _send_avito_outbox_channel(state: OutboxSendState) -> tuple[int, str]:
    deps = state.deps

    async def _cache_avito_echo(chat_key: str, payload: dict[str, Any], phase: str) -> None:
        try:
            echo_key = deps.avito_bot_echo_key_fn(state.tenant, chat_key)
            cached_payload: dict[str, Any] = {}
            try:
                cached_raw = await deps.redis_client.get(echo_key)
                if cached_raw:
                    cached_payload = deps.json_module.loads(cached_raw)
            except Exception:
                cached_payload = {}
            if isinstance(cached_payload, dict):
                variants: list[str] = []
                for source in (cached_payload, payload):
                    raw_text = str(source.get("text") or "").strip()
                    if raw_text and raw_text not in variants:
                        variants.append(raw_text)
                    for item in source.get("extra") or []:
                        text = str(item or "").strip()
                        if text and text not in variants:
                            variants.append(text)
                if variants:
                    payload = dict(payload)
                    payload["extra"] = variants[-12:]
            await deps.redis_client.set(
                echo_key,
                deps.json_module.dumps(payload, ensure_ascii=False),
                ex=deps.avito_bot_echo_ttl_seconds,
            )
        except Exception as exc:
            suffix = "_pre" if phase == "pre" else ""
            deps.log_fn(
                "event=avito_echo_cache_failed%s tenant=%s lead_id=%s error=%s"
                % (suffix, state.tenant, state.lead_id, exc)
            )

    avito_result = await deps.outbox_payloads_module.run_avito_send_with_echo(
        lead_id=state.lead_id,
        text=state.text,
        chat_hint=state.avito_chat_id_hint,
        has_attachments=bool(state.attachments),
        manager_message=state.manager_message,
        send_fn=lambda chat_hint: deps.send_avito_fn(
            state.tenant,
            state.lead_id,
            state.text,
            chat_id=chat_hint,
            account_id=state.avito_account_id,
            attachments=state.attachments or None,
        ),
        get_lead_peer_fn=deps.get_lead_peer_fn,
        cache_echo_fn=_cache_avito_echo,
    )
    return avito_result.status_code, avito_result.body


async def _send_telegram_outbox_channel(state: OutboxSendState) -> tuple[int, str]:
    deps = state.deps
    peer_id = deps.outbox_payloads_module.resolve_telegram_peer_id(
        peer_value=state.peer_value,
        peer_raw=state.peer_raw,
    )
    return await deps.send_telegram_fn(
        state.tenant,
        tg_slot=deps.normalize_tg_slot_fn(state.item.get("tg_slot")),
        chat_id=int(state.telegram_user_id or 0),
        peer_id=peer_id,
        peer=state.peer_value,
        telegram_user_id=state.telegram_user_id,
        username=state.username,
        text=state.text or None,
        attachments=state.attachments or None,
        reply_to=state.reply_to,
        lead_id=state.actual_lead_id,
    )


async def _send_max_outbox_channel(state: OutboxSendState) -> tuple[int, str]:
    deps = state.deps
    send_fn = deps.send_max_fn if state.channel == "max" else deps.send_max_personal_fn
    message_id = str(state.item.get("message_id") or "") or None

    def _send(chat_id: str | None) -> Awaitable[tuple[int, str]]:
        kwargs = {
            "chat_id": chat_id,
            "user_id": state.max_user_id,
            "attachments": state.attachments or None,
        }
        if state.channel == "max_personal":
            kwargs["message_id"] = message_id
        return send_fn(state.tenant, state.lead_id, state.text, **kwargs)

    max_result = await deps.outbox_payloads_module.run_max_send_with_echo(
        tenant_id=state.tenant,
        lead_id=state.lead_id,
        channel=state.channel,
        text=state.text,
        chat_hint=state.max_chat_id_hint,
        manager_message=state.manager_message,
        send_fn=_send,
        get_lead_peer_fn=deps.get_lead_peer_fn,
        cache_echo_fn=deps.cache_max_bot_echo_fn,
    )
    return max_result.status_code, max_result.body
