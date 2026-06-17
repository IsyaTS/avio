from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from libs.core.integrations import avito as avito_integration
from libs.core.message_envelope import normalize_attachments as normalize_message_attachments
from libs.core.message_envelope import text_or_placeholder
from libs.core.services import incoming_events
from libs.core.services.avito_incoming import build_avito_incoming_context


_AVITO_INCOMING_DEDUP_TTL_SECONDS = 7 * 24 * 3600

LogFn = Callable[[str], None]
AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
IncDbErrorFn = Callable[[str], None]


@dataclass(frozen=True)
class AvitoIncomingDeps:
    avito_chat_cache: dict[int, str]
    redis_client: Any
    phone_tg_ttl_seconds: int
    auto_reply_ttl_seconds: int
    testing_mode: bool
    log_fn: LogFn
    coerce_int_fn: SyncFn
    extract_ru_phone_fn: SyncFn
    extract_tg_username_fn: SyncFn
    avito_phone_tg_template_fn: SyncFn
    avito_auto_reply_text_fn: SyncFn
    resolve_avito_user_name_fn: AsyncFn
    get_or_create_by_peer_fn: AsyncFn
    lead_exists_fn: AsyncFn
    upsert_lead_fn: AsyncFn
    handle_followup_opt_out_fn: AsyncFn
    capture_followup_answer_fn: AsyncFn
    schedule_followups_fn: AsyncFn
    cancel_pending_smart_reply_fn: AsyncFn
    resolve_or_create_contact_fn: AsyncFn
    update_contact_phone_fn: AsyncFn
    update_contact_avito_login_fn: AsyncFn
    link_lead_contact_fn: AsyncFn
    insert_message_in_fn: AsyncFn
    maybe_amocrm_inbound_fn: AsyncFn
    match_behavior_trigger_fn: SyncFn
    mark_handoff_silence_fn: AsyncFn
    send_telegram_to_phone_fn: AsyncFn
    send_telegram_to_username_fn: AsyncFn
    enqueue_avito_auto_reply_fn: AsyncFn
    is_handoff_silenced_fn: AsyncFn
    avito_smart_reply_enabled_fn: SyncFn
    smart_reply_enabled_fn: SyncFn
    try_handle_smart_reply_with_delay_fn: AsyncFn
    produce_and_enqueue_smart_reply_fn: AsyncFn
    inc_db_error_fn: IncDbErrorFn
    resolve_avito_item_city_fn: AsyncFn | None = None
    resolve_avito_contact_identity_fn: AsyncFn | None = None


@dataclass(frozen=True)
class AvitoAutoReplyEnqueueDeps:
    redis_client: Any
    outbox_queue_key: str
    outbox_payloads_module: Any
    queue_contract_module: Any
    log_fn: Callable[..., None]


async def enqueue_avito_auto_reply(
    *,
    tenant_id: int,
    lead_id: int,
    chat_id: str,
    account_id: int | None,
    user_id: int | None,
    login: str | None,
    message_id: str,
    text: str,
    deps: AvitoAutoReplyEnqueueDeps,
) -> bool:
    out_payload = deps.outbox_payloads_module.avito_auto_reply_payload(
        tenant_id=tenant_id,
        lead_id=lead_id,
        chat_id=chat_id,
        account_id=account_id,
        user_id=user_id,
        login=login,
        message_id=message_id,
        text=(text or "").strip(),
    )
    if out_payload is None:
        return False
    try:
        await deps.queue_contract_module.push_json_left(
            deps.redis_client,
            deps.outbox_queue_key,
            out_payload,
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_auto_reply_enqueue_failed tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )
        return False
    deps.log_fn(
        "event=avito_auto_reply_enqueued tenant=%s lead_id=%s chat_id=%s",
        tenant_id,
        lead_id,
        chat_id,
    )
    return True


@dataclass
class AvitoIncomingState:
    tenant_id: int
    tenant_raw: Any
    chat_id: str
    message_id: str
    text: str
    attachments: list[dict[str, Any]]
    has_photo: bool
    auto_reply_text: str
    account_id: int | None
    item_id: int | None
    user_id: int | None
    login: str | None
    phone_value: str
    tg_username: str
    bridge_template: str
    lead_id: int = 0
    contact_id: int = 0


async def handle_avito_incoming(
    event: Mapping[str, Any],
    *,
    deps: AvitoIncomingDeps,
) -> None:
    state = await _prepare_incoming_state(event, deps=deps)
    if state is None:
        return
    if not _message_has_content(state, deps=deps):
        return
    if not _integration_connected(state, deps=deps):
        return
    if await _is_duplicate_avito_incoming(state, deps=deps):
        return

    await _resolve_and_cache_account_metadata(state, deps=deps)
    if not await _resolve_and_ensure_lead(state, event, deps=deps):
        return
    await _resolve_and_store_item_city(state, event, deps=deps)
    if await _handle_followup_text(state, deps=deps):
        return
    await _schedule_avito_followups(state, deps=deps)
    await _cache_phone_values(state, deps=deps)
    await _resolve_and_link_contact(state, deps=deps)
    await _resolve_and_store_contact_identity(state, deps=deps)
    await _store_incoming_and_forward_amocrm(state, event, deps=deps)

    if await _handle_behavior_trigger(state, deps=deps):
        return
    if await _handle_avito_telegram_bridge(state, deps=deps):
        return
    if await _handle_photo_handoff(state, deps=deps):
        return
    if await _handle_static_auto_reply(state, deps=deps):
        return
    await _maybe_enqueue_smart_reply(state, deps=deps)


async def _prepare_incoming_state(
    event: Mapping[str, Any],
    *,
    deps: AvitoIncomingDeps,
) -> AvitoIncomingState | None:
    tenant_hint = deps.coerce_int_fn(event.get("tenant") or event.get("tenant_id")) or 0
    cached_chat = deps.avito_chat_cache.get(int(tenant_hint)) if tenant_hint > 0 else None
    context = build_avito_incoming_context(event, cached_chat_id=cached_chat)
    tenant_id = context.tenant_id
    if tenant_id <= 0:
        deps.log_fn("event=skip_invalid_tenant channel=avito tenant_raw=%s" % context.tenant_raw)
        return None

    chat_id = context.chat_id
    if chat_id:
        deps.avito_chat_cache[int(tenant_id)] = chat_id
    if not chat_id:
        deps.log_fn(f"event=skip_invalid_chat channel=avito tenant={tenant_id}")
        return None

    text = context.text
    phone_value = deps.extract_ru_phone_fn(text)
    tg_username = deps.extract_tg_username_fn(text) if text and not phone_value else ""
    bridge_template = _resolve_bridge_template(
        tenant_id,
        text=text,
        phone_value=phone_value,
        tg_username=tg_username,
        deps=deps,
    )
    _log_bridge_detection(
        tenant_id,
        phone_value=phone_value,
        tg_username=tg_username,
        bridge_template=bridge_template,
        deps=deps,
    )

    attachments = normalize_message_attachments(incoming_events.collect_event_attachment_items(event))
    return AvitoIncomingState(
        tenant_id=int(tenant_id),
        tenant_raw=context.tenant_raw,
        chat_id=chat_id,
        message_id=context.message_id,
        text=text,
        attachments=attachments,
        has_photo=incoming_events.has_image_attachment(attachments),
        auto_reply_text=deps.avito_auto_reply_text_fn(tenant_id),
        account_id=context.account_id,
        item_id=context.item_id,
        user_id=context.user_id,
        login=context.login,
        phone_value=phone_value,
        tg_username=tg_username,
        bridge_template=bridge_template,
    )


def _resolve_bridge_template(
    tenant_id: int,
    *,
    text: str,
    phone_value: str,
    tg_username: str,
    deps: AvitoIncomingDeps,
) -> str:
    bridge_template = deps.avito_phone_tg_template_fn(tenant_id) if (phone_value or tg_username) else ""
    if deps.testing_mode and (phone_value or tg_username) and not bridge_template:
        bridge_template = (text or "").strip() or "Продолжим в Telegram"
    return bridge_template


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _safe_error_code(exc: BaseException) -> str:
    status = getattr(exc, "status", None)
    if status:
        return f"{type(exc).__name__}:{status}"
    return type(exc).__name__


async def _is_duplicate_avito_incoming(
    state: AvitoIncomingState,
    *,
    deps: AvitoIncomingDeps,
) -> bool:
    message_id = str(state.message_id or "").strip()
    if not message_id:
        return False
    account_part = str(state.account_id if state.account_id is not None else "no-account")
    key = "avito:incoming:dedup:%s:%s:%s:%s" % (
        state.tenant_id,
        account_part,
        state.chat_id,
        message_id,
    )
    try:
        if await deps.redis_client.get(key):
            deps.log_fn(
                "event=avito_incoming_dedup tenant=%s account_id=%s chat_id=%s message_id=%s"
                % (state.tenant_id, state.account_id, state.chat_id, message_id)
            )
            return True
        await deps.redis_client.set(key, "1", ex=_AVITO_INCOMING_DEDUP_TTL_SECONDS)
    except Exception as exc:
        deps.log_fn(
            "event=avito_incoming_dedup_cache_failed tenant=%s account_id=%s chat_id=%s error=%s"
            % (state.tenant_id, state.account_id, state.chat_id, _safe_error_code(exc))
        )
    return False


async def _resolve_and_store_item_city(
    state: AvitoIncomingState,
    event: Mapping[str, Any],
    *,
    deps: AvitoIncomingDeps,
) -> None:
    if state.item_id is None or state.account_id is None or deps.resolve_avito_item_city_fn is None:
        return
    source = _mapping(event.get("source"))
    avito = _mapping(event.get("avito"))
    message = _mapping(event.get("message"))
    try:
        await deps.resolve_avito_item_city_fn(
            tenant_id=state.tenant_id,
            account_id=state.account_id,
            item_id=state.item_id,
            lead_id=state.lead_id,
            url_hint=event.get("item_url") or avito.get("item_url") or source.get("item_url") or message.get("item_url"),
            address_hint=event.get("item_address")
            or avito.get("item_address")
            or source.get("item_address")
            or message.get("item_address"),
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_item_city_resolve_failed tenant=%s account_id=%s item_id=%s error=%s"
            % (state.tenant_id, state.account_id, state.item_id, _safe_error_code(exc))
        )


def _log_bridge_detection(
    tenant_id: int,
    *,
    phone_value: str,
    tg_username: str,
    bridge_template: str,
    deps: AvitoIncomingDeps,
) -> None:
    if phone_value:
        deps.log_fn(f"event=avito_phone_detected tenant={tenant_id} phone={phone_value}")
        if not bridge_template:
            deps.log_fn(
                f"event=avito_phone_tg_skip reason=empty_template channel=avito tenant={tenant_id} phone={phone_value}"
            )
    if tg_username:
        deps.log_fn("event=avito_username_detected tenant=%s username=%s" % (tenant_id, tg_username))
        if not bridge_template:
            deps.log_fn(
                "event=avito_username_tg_skip reason=empty_template channel=avito tenant=%s username=%s"
                % (tenant_id, tg_username)
            )


def _message_has_content(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    if state.text or state.attachments:
        return True
    deps.log_fn(
        f"event=skip_empty_message channel=avito tenant={state.tenant_id} chat_id={state.chat_id}"
    )
    return False


def _integration_connected(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    if state.account_id is not None:
        try:
            account = _get_connected_account_sync(int(state.tenant_id), int(state.account_id))
        except Exception:
            account = None
        if account and (account.get("access_token") or account.get("refresh_token")):
            return True
    integration = avito_integration.get_integration(int(state.tenant_id)) or {}
    token_value = str(integration.get("access_token") or "").strip()
    refresh_value = str(integration.get("refresh_token") or "").strip()
    if token_value or refresh_value:
        return True
    deps.log_fn(
        "event=avito_incoming_skip reason=disconnected tenant=%s chat_id=%s"
        % (state.tenant_id, state.chat_id)
    )
    return False


def _avito_peer_key(account_id: int | None, chat_id: str) -> str:
    chat_text = str(chat_id or "").strip()
    return chat_text


def _get_connected_account_sync(tenant_id: int, account_id: int) -> dict[str, Any] | None:
    import asyncio
    from libs.core.repo import avito_accounts

    async def _load() -> dict[str, Any] | None:
        account = await avito_accounts.get_account(int(tenant_id), int(account_id))
        if account and str(account.get("status") or "") == "active":
            return account
        return None

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_load())
    # Worker incoming runs in async context; avoid nested loop and rely on legacy mirror there.
    return None


async def _resolve_and_cache_account_metadata(
    state: AvitoIncomingState,
    *,
    deps: AvitoIncomingDeps,
) -> None:
    if not state.login:
        try:
            state.login = await deps.resolve_avito_user_name_fn(
                int(state.tenant_id),
                account_id=state.account_id,
                chat_id=state.chat_id,
                author_id=state.user_id,
            )
        except Exception as exc:
            deps.log_fn(
                "event=avito_user_name_failed tenant=%s chat_id=%s error=%s"
                % (state.tenant_id, state.chat_id, exc)
            )
    if state.account_id is not None:
        try:
            from libs.core.repo import avito_accounts

            await avito_accounts.mark_webhook_seen(int(state.account_id))
            deps.avito_chat_cache[int(state.tenant_id)] = state.chat_id
        except Exception as exc:
            deps.log_fn(
                "event=avito_account_cache_failed tenant=%s account_id=%s error=%s"
                % (state.tenant_id, state.account_id, exc)
            )
    if state.account_id is not None and state.login:
        try:
            account = await avito_integration.get_account_integration(
                int(state.tenant_id),
                int(state.account_id),
            )
            if account and not account.get("account_login"):
                from libs.core.repo import avito_accounts

                await avito_accounts.upsert_account_tokens(
                    int(state.tenant_id),
                    int(state.account_id),
                    account,
                    account_login=state.login,
                    is_primary=bool(account.get("is_primary")),
                )
        except Exception:
            pass


async def _resolve_and_ensure_lead(
    state: AvitoIncomingState,
    event: Mapping[str, Any],
    *,
    deps: AvitoIncomingDeps,
) -> bool:
    account_hint = state.account_id if state.account_id is not None else state.tenant_id
    provided_lead_id = deps.coerce_int_fn(event.get("lead_id"))
    derived_lead_id = avito_integration.stable_lead_id(account_hint, state.chat_id)
    if provided_lead_id and provided_lead_id != derived_lead_id:
        deps.log_fn(
            f"event=avito_lead_id_override tenant={state.tenant_id} provided_lead_id={provided_lead_id} derived_lead_id={derived_lead_id} chat_id={state.chat_id}"
        )
    lead_id_hint = derived_lead_id
    state.lead_id = await _get_or_create_avito_lead(lead_id_hint, state, deps=deps)
    if not await _ensure_avito_lead_exists(state, deps=deps):
        deps.log_fn(
            "event=skip_missing_lead channel=avito tenant=%s chat_id=%s lead_id=%s"
            % (state.tenant_id, state.chat_id, state.lead_id)
        )
        return False
    return True


async def _get_or_create_avito_lead(
    lead_id_hint: int,
    state: AvitoIncomingState,
    *,
    deps: AvitoIncomingDeps,
) -> int:
    try:
        lead_id = await deps.get_or_create_by_peer_fn(
            tenant_id=state.tenant_id,
            channel="avito",
            peer=_avito_peer_key(state.account_id, state.chat_id),
            lead_id_hint=lead_id_hint,
            source_real_id=state.account_id,
            contact=state.login,
        )
        lead_id = int(lead_id)
        deps.log_fn(
            f"event=avito_lead_resolved tenant={state.tenant_id} lead_id={lead_id} chat_id={state.chat_id}"
        )
        return lead_id
    except Exception as exc:
        deps.inc_db_error_fn("get_or_create_lead_peer")
        deps.log_fn(
            "event=warning reason=db_error operation=get_or_create_lead_peer channel=avito tenant=%s chat_id=%s error=%s"
            % (state.tenant_id, state.chat_id, exc)
        )
        return int(lead_id_hint or avito_integration.stable_lead_id(state.tenant_id, state.chat_id))


async def _ensure_avito_lead_exists(
    state: AvitoIncomingState,
    *,
    deps: AvitoIncomingDeps,
) -> bool:
    try:
        exists = await deps.lead_exists_fn(state.lead_id, state.tenant_id)
    except Exception:
        exists = True
    if exists:
        return True
    try:
        await deps.upsert_lead_fn(
            state.lead_id,
            channel="avito",
            tenant_id=state.tenant_id,
            peer=_avito_peer_key(state.account_id, state.chat_id),
            source_real_id=state.account_id,
            contact=state.login,
        )
        return bool(await deps.lead_exists_fn(state.lead_id, state.tenant_id))
    except Exception as exc:
        deps.inc_db_error_fn("upsert_lead_retry")
        deps.log_fn(
            "event=warning reason=db_error operation=ensure_lead channel=avito tenant=%s chat_id=%s lead_id=%s error=%s"
            % (state.tenant_id, state.chat_id, state.lead_id, exc)
        )
        return False


async def _handle_followup_text(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    if not state.text:
        return False
    try:
        if await deps.handle_followup_opt_out_fn(state.tenant_id, state.lead_id, state.text):
            await deps.cancel_pending_smart_reply_fn(
                state.tenant_id,
                "avito",
                state.lead_id,
                reason="followup_optout",
            )
            deps.log_fn(
                "event=followup_optout channel=avito tenant=%s lead_id=%s",
                state.tenant_id,
                state.lead_id,
            )
            return True
        await deps.capture_followup_answer_fn(state.tenant_id, state.lead_id, state.text, "avito")
    except Exception as exc:
        deps.log_fn(
            "event=followup_capture_warn channel=avito tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )
    return False


async def _schedule_avito_followups(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> None:
    try:
        await deps.schedule_followups_fn(state.tenant_id, state.lead_id, "avito")
    except Exception as exc:
        deps.log_fn(
            f"event=followup_schedule_warn channel=avito tenant={state.tenant_id} lead_id={state.lead_id} error={exc}"
        )


async def _cache_phone_values(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> None:
    if not state.phone_value:
        return
    try:
        await deps.redis_client.set(
            f"cache:avito_phone:{state.tenant_id}:{state.chat_id}",
            state.phone_value,
            ex=3600 * 24 * 7,
        )
        await deps.redis_client.set(
            f"cache:lead_phone:{state.tenant_id}:{state.lead_id}",
            state.phone_value,
            ex=3600 * 24 * 7,
        )
    except Exception:
        pass


async def _resolve_and_link_contact(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> None:
    await _resolve_avito_contact(state, deps=deps)
    if not state.contact_id:
        return
    try:
        await deps.link_lead_contact_fn(
            state.lead_id,
            state.contact_id,
            channel="avito",
            peer=_avito_peer_key(state.account_id, state.chat_id),
        )
    except Exception as exc:
        deps.inc_db_error_fn("link_lead_contact")
        deps.log_fn(
            "event=link_lead_contact_failed channel=avito tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )


async def _resolve_avito_contact(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> None:
    try:
        state.contact_id = await deps.resolve_or_create_contact_fn(
            tenant_id=state.tenant_id,
            avito_user_id=state.user_id,
            avito_login=state.login,
            phone=state.phone_value,
            whatsapp_phone=state.phone_value,
        )
        if state.contact_id and state.phone_value:
            await _update_avito_contact_phone(state, deps=deps)
        if state.contact_id and state.login:
            try:
                await deps.update_contact_avito_login_fn(state.contact_id, state.login)
            except Exception:
                pass
    except Exception as exc:
        deps.inc_db_error_fn("resolve_contact")
        deps.log_fn(
            "event=contact_resolve_failed channel=avito tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )


async def _update_avito_contact_phone(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> None:
    try:
        await deps.update_contact_phone_fn(state.contact_id, state.phone_value)
        deps.log_fn(
            f"event=contact_phone_updated channel=avito tenant={state.tenant_id} lead_id={state.lead_id} contact_id={state.contact_id} phone={state.phone_value}"
        )
    except Exception:
        pass


async def _resolve_and_store_contact_identity(
    state: AvitoIncomingState,
    *,
    deps: AvitoIncomingDeps,
) -> None:
    if deps.resolve_avito_contact_identity_fn is None:
        return
    try:
        result = await deps.resolve_avito_contact_identity_fn(
            tenant_id=state.tenant_id,
            lead_id=state.lead_id,
            contact_id=state.contact_id,
            account_id=state.account_id,
            chat_id=state.chat_id,
            author_id=state.user_id,
            current_login=state.login,
            current_contact=None,
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_contact_identity_failed tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, type(exc).__name__)
        )
        return
    name = getattr(result, "name", None)
    if name:
        state.login = str(name).strip() or state.login


async def _store_incoming_and_forward_amocrm(
    state: AvitoIncomingState,
    event: Mapping[str, Any],
    *,
    deps: AvitoIncomingDeps,
) -> None:
    incoming_stored = bool(event.get("_incoming_stored"))
    stored_message_id = deps.coerce_int_fn(event.get("_message_db_id"))
    if stored_message_id:
        incoming_stored = True
    try:
        incoming_text = text_or_placeholder(state.text, state.attachments)
        if not incoming_stored:
            await deps.insert_message_in_fn(
                state.lead_id,
                incoming_text,
                status="received",
                tenant_id=state.tenant_id,
                provider_msg_id=state.message_id or None,
                source="incoming",
            )
        await deps.maybe_amocrm_inbound_fn(
            state.tenant_id,
            state.lead_id,
            state.text,
            "avito",
            attachments=state.attachments,
        )
    except Exception as exc:
        deps.inc_db_error_fn("insert_message_in")
        deps.log_fn(
            "event=store_incoming_failed channel=avito tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )


async def _handle_behavior_trigger(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    trigger_rule = deps.match_behavior_trigger_fn(state.tenant_id, "avito", state.text)
    if not (trigger_rule and trigger_rule.get("silence", True)):
        return False
    notify_flag = bool(trigger_rule.get("notify"))
    await deps.mark_handoff_silence_fn(
        state.tenant_id,
        state.lead_id,
        reason="trigger_match",
        contact_hint=state.chat_id,
        username_hint=state.login,
        notify=notify_flag,
    )
    deps.log_fn(
        f"event=trigger_match channel=avito tenant={state.tenant_id} lead_id={state.lead_id} notify={int(notify_flag)} phrases={trigger_rule.get('phrases')}"
    )
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        "avito",
        state.lead_id,
        reason="trigger_silence",
    )
    return True


async def _handle_avito_telegram_bridge(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    if state.phone_value and state.bridge_template and state.lead_id > 0:
        return await _send_avito_bridge_to_phone(state, deps=deps)
    if state.tg_username and state.bridge_template and state.lead_id > 0:
        return await _send_avito_bridge_to_username(state, deps=deps)
    return False


async def _send_avito_bridge_to_phone(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    dedup_key = f"avito:phone_tg_sent:{state.tenant_id}:{state.lead_id}"
    already_sent = None
    avito_phone_tg_dedup_enabled_local = False
    if avito_phone_tg_dedup_enabled_local:
        try:
            already_sent = await deps.redis_client.get(dedup_key)
        except Exception:
            already_sent = None
    if already_sent:
        deps.log_fn(
            f"event=avito_phone_tg_skip reason=dedup channel=avito tenant={state.tenant_id} lead_id={state.lead_id} phone={state.phone_value}"
        )
        return False
    try:
        status_code, body = await deps.send_telegram_to_phone_fn(
            tenant_id=state.tenant_id,
            phone=state.phone_value,
            text=state.bridge_template,
            lead_id=state.lead_id,
            contact_id=state.contact_id or None,
        )
    except Exception as exc:
        deps.log_fn(
            f"event=avito_phone_tg_fail channel=avito tenant={state.tenant_id} lead_id={state.lead_id} phone={state.phone_value} error={exc}"
        )
        return True
    if 200 <= status_code < 300:
        await _mark_phone_bridge_sent(dedup_key, state, deps=deps, enabled=avito_phone_tg_dedup_enabled_local)
        deps.log_fn(
            f"event=avito_phone_tg_sent channel=avito tenant={state.tenant_id} lead_id={state.lead_id} phone={state.phone_value} status={status_code}"
        )
    else:
        deps.log_fn(
            f"event=avito_phone_tg_fail channel=avito tenant={state.tenant_id} lead_id={state.lead_id} phone={state.phone_value} status={status_code} body={body}"
        )
    return False


async def _mark_phone_bridge_sent(
    dedup_key: str,
    state: AvitoIncomingState,
    *,
    deps: AvitoIncomingDeps,
    enabled: bool,
) -> None:
    if not enabled:
        return
    try:
        await deps.redis_client.set(dedup_key, "1", ex=deps.phone_tg_ttl_seconds)
    except Exception:
        pass


async def _send_avito_bridge_to_username(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    try:
        status_code, body = await deps.send_telegram_to_username_fn(
            tenant_id=state.tenant_id,
            username=state.tg_username,
            text=state.bridge_template,
            lead_id=state.lead_id,
            contact_id=state.contact_id or None,
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_username_tg_fail channel=avito tenant=%s lead_id=%s username=%s error=%s"
            % (state.tenant_id, state.lead_id, state.tg_username, exc)
        )
        return True
    if 200 <= status_code < 300:
        deps.log_fn(
            "event=avito_username_tg_sent channel=avito tenant=%s lead_id=%s username=%s status=%s"
            % (state.tenant_id, state.lead_id, state.tg_username, status_code)
        )
    else:
        deps.log_fn(
            "event=avito_username_tg_fail channel=avito tenant=%s lead_id=%s username=%s status=%s body=%s"
            % (state.tenant_id, state.lead_id, state.tg_username, status_code, body)
        )
    return False


async def _handle_photo_handoff(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    if not state.has_photo:
        return False
    await deps.mark_handoff_silence_fn(state.tenant_id, state.lead_id, reason="photo_received")
    deps.log_fn(
        f"event=handoff_marked channel=avito tenant={state.tenant_id} lead_id={state.lead_id} reason=photo_received"
    )
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        "avito",
        state.lead_id,
        reason="photo_received",
    )
    return True


async def _handle_static_auto_reply(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    if not state.auto_reply_text:
        deps.log_fn(
            f"event=avito_static_auto_reply_skip reason=no_text tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        return False
    auto_reply_dedup_key = f"avito:auto_reply_sent:{state.tenant_id}:{state.lead_id}"
    try:
        already_replied = await deps.redis_client.get(auto_reply_dedup_key)
    except Exception:
        already_replied = None
    if already_replied:
        deps.log_fn(
            f"event=avito_auto_reply_skip reason=dedup tenant={state.tenant_id} lead_id={state.lead_id} chat_id={state.chat_id}"
        )
        return False
    sent = await deps.enqueue_avito_auto_reply_fn(
        tenant_id=state.tenant_id,
        lead_id=state.lead_id,
        chat_id=state.chat_id,
        account_id=state.account_id,
        user_id=state.user_id,
        login=state.login,
        message_id=state.message_id,
        text=state.auto_reply_text,
    )
    if not sent:
        return False
    try:
        await deps.redis_client.set(auto_reply_dedup_key, "1", ex=deps.auto_reply_ttl_seconds)
    except Exception:
        pass
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        "avito",
        state.lead_id,
        reason="avito_auto_reply",
    )
    deps.log_fn(
        f"event=avito_static_auto_reply_applied tenant={state.tenant_id} lead_id={state.lead_id} "
        f"chat_id={state.chat_id} channel=avito"
    )
    return True


async def _maybe_enqueue_smart_reply(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> None:
    if not state.text:
        deps.log_fn(
            f"event=smart_reply_skip tenant={state.tenant_id} lead_id={state.lead_id} reason=empty_text channel=avito"
        )
        return
    if await deps.is_handoff_silenced_fn(state.tenant_id, state.lead_id):
        deps.log_fn(
            f"event=smart_reply_silenced channel=avito tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        await deps.cancel_pending_smart_reply_fn(
            state.tenant_id,
            "avito",
            state.lead_id,
            reason="silenced",
        )
        return
    deps.log_fn(
        f"event=smart_reply_allowed channel=avito tenant={state.tenant_id} lead_id={state.lead_id}"
    )
    if not _smart_reply_allowed(state, deps=deps):
        deps.log_fn(
            f"event=smart_reply_not_allowed channel=avito tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        return
    refer_id = state.contact_id if state.contact_id and state.contact_id > 0 else state.lead_id
    reply_context = _smart_reply_context(state)
    delayed = await deps.try_handle_smart_reply_with_delay_fn(
        tenant_id=state.tenant_id,
        lead_id=state.lead_id,
        channel="avito",
        refer_id=refer_id,
        user_text=state.text,
        context=reply_context,
    )
    if delayed:
        deps.log_fn(
            f"event=smart_reply_enqueue_result channel=avito tenant={state.tenant_id} lead_id={state.lead_id} result=delayed"
        )
        return
    await deps.produce_and_enqueue_smart_reply_fn(
        tenant_id=state.tenant_id,
        lead_id=state.lead_id,
        channel="avito",
        refer_id=refer_id,
        user_text=state.text,
        context=reply_context,
        delayed=False,
    )
    deps.log_fn(
        f"event=smart_reply_enqueue_result channel=avito tenant={state.tenant_id} lead_id={state.lead_id} result=immediate"
    )


def _smart_reply_allowed(state: AvitoIncomingState, *, deps: AvitoIncomingDeps) -> bool:
    if not deps.avito_smart_reply_enabled_fn(state.tenant_id):
        deps.log_fn(
            f"event=smart_reply_disabled reason=avito_disabled channel=avito tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        return False
    if not deps.smart_reply_enabled_fn(state.tenant_id):
        deps.log_fn(
            f"event=smart_reply_disabled channel=avito tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        return False
    return True


def _smart_reply_context(state: AvitoIncomingState) -> dict[str, Any]:
    return {
        "chat_id": state.chat_id,
        "account_id": state.account_id,
        "item_id": state.item_id,
        "message_id": state.message_id,
        "avito_user_id": state.user_id,
        "avito_login": state.login,
    }
