from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Mapping

from libs.core.message_envelope import (
    detect_message_kind,
    normalize_attachments as normalize_message_attachments,
    sanitize_display_name,
)
from libs.core.services import incoming_events
from libs.core.services import queue_contract


LogFn = Callable[[str], None]
AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
IncDbErrorFn = Callable[[str], None]


@dataclass(frozen=True)
class TelegramIncomingDeps:
    redis_client: Any
    outbox_queue_key: str
    notify_bot_id: int | None
    log_fn: LogFn
    normalize_tg_slot_fn: SyncFn
    coerce_int_fn: SyncFn
    is_duplicate_telegram_incoming_fn: AsyncFn
    find_lead_by_telegram_fn: AsyncFn
    normalize_username_fn: SyncFn
    upsert_lead_fn: AsyncFn
    store_lead_tg_slot_fn: AsyncFn
    telegram_slot_is_enabled_fn: SyncFn
    looks_like_manager_outgoing_fn: SyncFn
    is_manager_message_fn: SyncFn
    handle_followup_opt_out_fn: AsyncFn
    capture_followup_answer_fn: AsyncFn
    maybe_amocrm_inbound_fn: AsyncFn
    schedule_followups_fn: AsyncFn
    get_contact_id_by_lead_fn: AsyncFn
    get_contact_id_by_phone_fn: AsyncFn
    resolve_or_create_contact_fn: AsyncFn
    link_lead_contact_fn: AsyncFn
    update_contact_telegram_fn: AsyncFn
    update_contact_phone_fn: AsyncFn
    match_behavior_trigger_fn: SyncFn
    mark_handoff_silence_fn: AsyncFn
    cancel_pending_smart_reply_fn: AsyncFn
    photo_expectation_config_fn: SyncFn
    notify_manager_handoff_fn: AsyncFn
    is_handoff_silenced_fn: AsyncFn
    read_tenant_config_fn: SyncFn
    get_contact_phone_by_lead_fn: AsyncFn
    telegram_reply_enabled_fn: SyncFn
    smart_reply_enabled_fn: SyncFn
    try_handle_smart_reply_with_delay_fn: AsyncFn
    produce_and_enqueue_smart_reply_fn: AsyncFn
    catalog_flow_service: Any
    inc_db_error_fn: IncDbErrorFn


@dataclass
class TelegramIncomingState:
    tenant_id: int
    tenant_raw: Any
    tg_slot: int
    text: str
    attachments: list[dict[str, Any]]
    message_kind: str
    has_photo: bool
    message_id: str
    message_id_int: int | None
    telegram_user_id: int | None
    peer_id: int | None
    peer_value: str | None
    username: str | None
    display_name: str | None
    lead_id: int = 0
    peer_log_hint: str | None = None
    contact_hint: str | None = None
    manager_outgoing: bool = False
    trigger_bot: bool = True
    first_catalog_sent_now: bool = False


async def handle_telegram_incoming(
    event: Mapping[str, Any],
    *,
    deps: TelegramIncomingDeps,
) -> None:
    state = _parse_telegram_state(event, deps=deps)
    if state is None:
        return
    if await _is_duplicate_incoming(state, deps=deps):
        return
    if not await _resolve_telegram_lead(state, event, deps=deps):
        return
    if not _telegram_incoming_allowed(state, deps=deps):
        return

    _resolve_trigger_context(state, event, deps=deps)
    if await _capture_followup_and_amocrm(state, deps=deps):
        return
    if not state.trigger_bot:
        deps.log_fn(
            "event=incoming_skip_trigger channel=telegram tenant=%s lead_id=%s author_kind=%s",
            state.tenant_id,
            state.lead_id,
            str(event.get("author_kind") or ""),
        )
        return

    await _schedule_telegram_followups(state, deps=deps)
    await _link_telegram_contact_by_cached_phone(state, deps=deps)
    if await _handle_behavior_trigger(state, deps=deps):
        return
    if await _handle_expected_photo_reply(state, deps=deps):
        return
    if await _handle_attachment_or_manager_handoff(state, deps=deps):
        return
    if await _is_smart_reply_silenced(state, deps=deps):
        return
    await _run_catalog_flow(state, deps=deps)
    if getattr(state, "_catalog_stopped", False):
        return
    if await _ensure_phone_contact_if_missing(state, deps=deps):
        pass
    await _maybe_enqueue_telegram_smart_reply(state, event, deps=deps)


def _parse_telegram_state(
    event: Mapping[str, Any],
    *,
    deps: TelegramIncomingDeps,
) -> TelegramIncomingState | None:
    tenant_raw = event.get("tenant") or event.get("tenant_id")
    try:
        tenant_id = int(tenant_raw) if tenant_raw is not None else 0
    except Exception:
        tenant_id = 0
    if tenant_id <= 0:
        deps.log_fn("event=skip_invalid_tenant channel=telegram tenant_raw=%s" % tenant_raw)
        return None

    nested_message = event.get("message") if isinstance(event.get("message"), Mapping) else {}
    tg_slot = deps.normalize_tg_slot_fn(
        event.get("tg_slot") or nested_message.get("tg_slot") or event.get("slot")
    )
    text = incoming_events.normalize_event_text(event)
    attachments = normalize_message_attachments(incoming_events.collect_event_attachment_items(event))
    message_kind = str(event.get("message_kind") or detect_message_kind(text, attachments)).strip().lower() or "text"
    message_id_raw = event.get("message_id")
    message_id = str(message_id_raw) if message_id_raw is not None else ""
    try:
        message_id_int = int(message_id) if message_id else None
    except Exception:
        message_id_int = None
    peer_id = deps.coerce_int_fn(event.get("peer_id"))
    peer_value = _peer_value(event.get("peer"))
    if peer_value and peer_id is None:
        try:
            peer_id = int(peer_value)
        except Exception:
            peer_id = None
    username_raw = event.get("username")
    username = str(username_raw).strip() or None if username_raw is not None else None
    display_name_raw = event.get("display_name")
    if display_name_raw is None and isinstance(nested_message, Mapping):
        display_name_raw = nested_message.get("display_name")
    return TelegramIncomingState(
        tenant_id=tenant_id,
        tenant_raw=tenant_raw,
        tg_slot=tg_slot,
        text=text,
        attachments=attachments,
        message_kind=message_kind,
        has_photo=incoming_events.has_image_attachment(attachments),
        message_id=message_id,
        message_id_int=message_id_int,
        telegram_user_id=deps.coerce_int_fn(event.get("telegram_user_id")),
        peer_id=peer_id,
        peer_value=peer_value,
        username=username,
        display_name=sanitize_display_name(display_name_raw),
    )


def _peer_value(raw: Any) -> str | None:
    if isinstance(raw, str):
        return raw.strip() or None
    if raw is not None:
        return str(raw).strip() or None
    return None


async def _is_duplicate_incoming(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> bool:
    duplicate = await deps.is_duplicate_telegram_incoming_fn(
        tenant_id=state.tenant_id,
        message_id=state.message_id_int,
        telegram_user_id=state.telegram_user_id,
        peer=state.peer_value,
    )
    if duplicate:
        deps.log_fn(
            f"event=skip_duplicate_incoming channel=telegram tenant={state.tenant_id} "
            f"message_id={state.message_id or '-'} peer={state.peer_value or '-'}"
        )
    return bool(duplicate)


async def _resolve_telegram_lead(
    state: TelegramIncomingState,
    event: Mapping[str, Any],
    *,
    deps: TelegramIncomingDeps,
) -> bool:
    lead_candidate = deps.coerce_int_fn(event.get("lead_id"))
    state.lead_id = lead_candidate if lead_candidate and lead_candidate > 0 else 0
    normalized_username = deps.normalize_username_fn(state.username)
    state.contact_hint = state.display_name or normalized_username or state.username
    resolved_lead_id = await _find_existing_telegram_lead(state, normalized_username, deps=deps)
    resolved_lead_id = await _upsert_telegram_lead(state, resolved_lead_id, normalized_username, deps=deps)
    if resolved_lead_id is None and state.telegram_user_id is not None:
        resolved_lead_id = int(state.telegram_user_id)
    state.lead_id = resolved_lead_id if resolved_lead_id is not None else 0
    await deps.store_lead_tg_slot_fn(state.tenant_id, state.lead_id, state.tg_slot)
    state.peer_log_hint = _peer_log_hint(state)
    deps.log_fn(
        f"event=inbox_lead_resolved channel=telegram tenant={state.tenant_id} slot={state.tg_slot} lead_id={state.lead_id} peer={state.peer_log_hint or '-'}"
    )
    if state.lead_id > 0:
        return True
    deps.log_fn(
        f"event=skip_missing_lead channel=telegram tenant={state.tenant_id} message_id={state.message_id}"
    )
    return False


async def _find_existing_telegram_lead(
    state: TelegramIncomingState,
    normalized_username: str | None,
    *,
    deps: TelegramIncomingDeps,
) -> int | None:
    if state.lead_id > 0:
        return state.lead_id
    if state.telegram_user_id is None:
        return None
    try:
        found_lead = await deps.find_lead_by_telegram_fn(state.tenant_id, int(state.telegram_user_id))
    except Exception as exc:
        deps.inc_db_error_fn("find_lead_by_telegram")
        deps.log_fn(
            "event=inbox_lead_lookup_failed channel=telegram tenant=%s error=%s"
            % (state.tenant_id, exc)
        )
        found_lead = None
    return int(found_lead) if found_lead and found_lead > 0 else None


async def _upsert_telegram_lead(
    state: TelegramIncomingState,
    resolved_lead_id: int | None,
    normalized_username: str | None,
    *,
    deps: TelegramIncomingDeps,
) -> int | None:
    title_hint = _telegram_title_hint(state, normalized_username)
    upsert_kwargs: Dict[str, Any] = {
        "channel": "telegram",
        "tenant_id": state.tenant_id,
        "peer_id": state.peer_id,
        "peer": state.peer_value,
        "contact": state.contact_hint,
        "title": title_hint,
        "telegram_username": state.username,
    }
    if state.telegram_user_id is not None:
        upsert_kwargs["telegram_user_id"] = int(state.telegram_user_id)
    try:
        upsert_result = await deps.upsert_lead_fn(resolved_lead_id if resolved_lead_id else None, **upsert_kwargs)
    except Exception as exc:
        deps.inc_db_error_fn("upsert_lead")
        deps.log_fn(
            "event=inbox_lead_upsert_failed channel=telegram tenant=%s error=%s" % (state.tenant_id, exc)
        )
        return None
    if upsert_result is None:
        return resolved_lead_id
    try:
        return int(upsert_result)
    except Exception:
        return None


def _telegram_title_hint(state: TelegramIncomingState, normalized_username: str | None) -> str | None:
    if normalized_username:
        return normalized_username
    if state.display_name:
        return state.display_name
    if state.telegram_user_id is not None:
        return f"tg:id {state.telegram_user_id}"
    if state.peer_id is not None:
        return f"tg:id {state.peer_id}"
    return None


def _peer_log_hint(state: TelegramIncomingState) -> str | None:
    if state.peer_value:
        return state.peer_value
    if state.peer_id is not None:
        return str(state.peer_id)
    if state.telegram_user_id is not None:
        return str(state.telegram_user_id)
    return None


def _telegram_incoming_allowed(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> bool:
    if not deps.telegram_slot_is_enabled_fn(state.tenant_id, state.tg_slot):
        deps.log_fn(
            f"event=telegram_slot_disabled channel=telegram tenant={state.tenant_id} slot={state.tg_slot} lead_id={state.lead_id}"
        )
        return False
    if deps.notify_bot_id and state.telegram_user_id and int(state.telegram_user_id) == int(deps.notify_bot_id):
        deps.log_fn(
            f"event=skip_notify_bot channel=telegram tenant={state.tenant_id} lead_id={state.lead_id} peer={state.peer_log_hint or '-'}"
        )
        return False
    return True


def _resolve_trigger_context(
    state: TelegramIncomingState,
    event: Mapping[str, Any],
    *,
    deps: TelegramIncomingDeps,
) -> None:
    state.manager_outgoing = deps.looks_like_manager_outgoing_fn(event) or deps.is_manager_message_fn(event)
    state.trigger_bot = bool(event.get("trigger_bot")) if "trigger_bot" in event else not state.manager_outgoing


async def _capture_followup_and_amocrm(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> bool:
    if state.text and not state.manager_outgoing:
        if await _handle_followup_text(state, deps=deps):
            return True
    if not state.manager_outgoing:
        await _forward_telegram_amocrm(state, deps=deps)
    return False


async def _handle_followup_text(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> bool:
    try:
        if await deps.handle_followup_opt_out_fn(state.tenant_id, state.lead_id, state.text):
            await deps.cancel_pending_smart_reply_fn(
                state.tenant_id,
                "telegram",
                state.lead_id,
                reason="followup_optout",
            )
            deps.log_fn(
                "event=followup_optout channel=telegram tenant=%s lead_id=%s"
                % (state.tenant_id, state.lead_id)
            )
            return True
        await deps.capture_followup_answer_fn(state.tenant_id, state.lead_id, state.text, "telegram")
    except Exception as exc:
        deps.log_fn(
            "event=followup_capture_warn channel=telegram tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )
    return False


async def _forward_telegram_amocrm(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> None:
    try:
        await deps.maybe_amocrm_inbound_fn(
            state.tenant_id,
            state.lead_id,
            state.text,
            "telegram",
            attachments=state.attachments,
            message_id=state.message_id_int,
        )
    except Exception as exc:
        deps.log_fn(
            "event=amocrm_inbound_failed channel=telegram tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )


async def _schedule_telegram_followups(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> None:
    try:
        await deps.schedule_followups_fn(state.tenant_id, state.lead_id, "telegram")
    except Exception as exc:
        deps.log_fn(
            f"event=followup_schedule_warn channel=telegram tenant={state.tenant_id} lead_id={state.lead_id} error={exc}"
        )


async def _link_telegram_contact_by_cached_phone(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> None:
    existing_contact_id = await _existing_contact_id(state, deps=deps)
    phone_norm = await _cached_phone_for_telegram(state, deps=deps)
    if _notify_bot_phone_link_forbidden(phone_norm, state, deps=deps):
        deps.log_fn(
            f"event=telegram_contact_link_skip reason=notify_bot tenant={state.tenant_id} lead_id={state.lead_id} phone={phone_norm}"
        )
        phone_norm = None
    if phone_norm:
        await _link_telegram_phone_contact(state, phone_norm, existing_contact_id, deps=deps)
    elif existing_contact_id:
        try:
            await deps.update_contact_telegram_fn(existing_contact_id, state.telegram_user_id, state.username)
        except Exception:
            pass


async def _existing_contact_id(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> int | None:
    try:
        return await deps.get_contact_id_by_lead_fn(state.lead_id)
    except Exception:
        return None


async def _cached_phone_for_telegram(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> str | None:
    phone_norm = await _redis_str(deps.redis_client, f"cache:lead_phone:{state.tenant_id}:{state.lead_id}")
    if not phone_norm and state.peer_value:
        phone_norm = await _redis_str(deps.redis_client, f"cache:avito_phone:{state.tenant_id}:{state.peer_value}")
    return phone_norm


async def _redis_str(redis_client: Any, key: str) -> str | None:
    try:
        raw = await redis_client.get(key)
    except Exception:
        return None
    if not raw or not str(raw).strip():
        return None
    return raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw).strip()


def _notify_bot_phone_link_forbidden(
    phone_norm: str | None,
    state: TelegramIncomingState,
    *,
    deps: TelegramIncomingDeps,
) -> bool:
    return bool(
        phone_norm
        and deps.notify_bot_id
        and state.telegram_user_id
        and int(state.telegram_user_id) == int(deps.notify_bot_id)
    )


async def _link_telegram_phone_contact(
    state: TelegramIncomingState,
    phone_norm: str,
    existing_contact_id: int | None,
    *,
    deps: TelegramIncomingDeps,
) -> None:
    try:
        phone_owner_id = await _contact_id_by_phone(phone_norm, state, deps=deps)
        target_contact_id = phone_owner_id or await deps.resolve_or_create_contact_fn(
            tenant_id=state.tenant_id,
            phone=phone_norm,
            whatsapp_phone=phone_norm,
            telegram_user_id=state.telegram_user_id,
            telegram_username=state.username,
        )
        if existing_contact_id and existing_contact_id != target_contact_id:
            await _relink_telegram_contact(state, existing_contact_id, target_contact_id, phone_norm, deps=deps)
            return
        if existing_contact_id:
            await deps.update_contact_telegram_fn(existing_contact_id, state.telegram_user_id, state.username)
        await deps.update_contact_phone_fn(target_contact_id, phone_norm)
        await deps.link_lead_contact_fn(
            state.lead_id,
            target_contact_id,
            channel="telegram",
            peer=state.peer_value or state.peer_log_hint,
        )
        deps.log_fn(
            f"event=telegram_contact_linked_by_phone tenant={state.tenant_id} lead_id={state.lead_id} contact_id={target_contact_id} phone={phone_norm}"
        )
    except Exception as exc:
        deps.log_fn(
            f"event=telegram_contact_link_failed tenant={state.tenant_id} lead_id={state.lead_id} phone={phone_norm} error={exc}"
        )


async def _contact_id_by_phone(
    phone_norm: str,
    state: TelegramIncomingState,
    *,
    deps: TelegramIncomingDeps,
) -> int | None:
    try:
        return await deps.get_contact_id_by_phone_fn(phone_norm, tenant_id=state.tenant_id)
    except Exception:
        return None


async def _relink_telegram_contact(
    state: TelegramIncomingState,
    existing_contact_id: int,
    target_contact_id: int,
    phone_norm: str,
    *,
    deps: TelegramIncomingDeps,
) -> None:
    await deps.link_lead_contact_fn(
        state.lead_id,
        target_contact_id,
        channel="telegram",
        peer=state.peer_value or state.peer_log_hint,
    )
    await deps.update_contact_telegram_fn(target_contact_id, state.telegram_user_id, state.username)
    deps.log_fn(
        f"event=telegram_contact_relinked_by_phone tenant={state.tenant_id} lead_id={state.lead_id} from_contact={existing_contact_id} to_contact={target_contact_id} phone={phone_norm}"
    )


async def _handle_behavior_trigger(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> bool:
    if not state.text:
        return False
    trigger_rule = deps.match_behavior_trigger_fn(state.tenant_id, "telegram", state.text)
    if not (trigger_rule and trigger_rule.get("silence", True)):
        return False
    notify_flag = bool(trigger_rule.get("notify"))
    await deps.mark_handoff_silence_fn(
        state.tenant_id,
        state.lead_id,
        reason="trigger_match",
        contact_hint=state.peer_log_hint or state.peer_value or state.contact_hint,
        username_hint=state.username,
        notify=notify_flag,
    )
    deps.log_fn(
        f"event=trigger_match channel=telegram tenant={state.tenant_id} lead_id={state.lead_id} notify={int(notify_flag)} phrases={trigger_rule.get('phrases')}"
    )
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        "telegram",
        state.lead_id,
        reason="trigger_silence",
    )
    return True


async def _handle_expected_photo_reply(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> bool:
    if not _has_media_for_handoff(state):
        return False
    _markers, photo_reply, _photo_ttl = deps.photo_expectation_config_fn(state.tenant_id)
    state_key = f"conv:state:{state.tenant_id}:{state.lead_id}"
    if not await _waiting_photo_state(state_key, deps=deps):
        return False
    if photo_reply and photo_reply.strip():
        await _enqueue_photo_expected_reply(state, photo_reply.strip(), deps=deps)
    try:
        await deps.notify_manager_handoff_fn(
            int(state.tenant_id),
            int(state.lead_id),
            reason="photo_received",
            contact_hint=state.peer_log_hint or state.peer_value or state.contact_hint,
            username_hint=state.username,
        )
    except Exception:
        pass
    try:
        await deps.redis_client.delete(state_key)
    except Exception:
        pass
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        "telegram",
        state.lead_id,
        reason="photo_expected_reply",
    )
    return True


def _has_media_for_handoff(state: TelegramIncomingState) -> bool:
    return bool(state.has_photo or state.message_kind in {"image", "video", "voice", "file", "mixed"})


async def _waiting_photo_state(state_key: str, *, deps: TelegramIncomingDeps) -> bool:
    try:
        state_val = await deps.redis_client.get(state_key)
    except Exception:
        return False
    if isinstance(state_val, (bytes, bytearray)):
        state_val = state_val.decode("utf-8", errors="ignore")
    return isinstance(state_val, str) and state_val == "waiting_photo"


async def _enqueue_photo_expected_reply(
    state: TelegramIncomingState,
    photo_reply: str,
    *,
    deps: TelegramIncomingDeps,
) -> None:
    out_payload = {
        "lead_id": int(state.lead_id),
        "tenant": int(state.tenant_id),
        "tenant_id": int(state.tenant_id),
        "provider": "telegram",
        "ch": "telegram",
        "channel": "telegram",
        "text": photo_reply,
        "attachments": [],
        "peer": state.peer_value or state.peer_log_hint,
        "peer_id": state.peer_value or state.peer_log_hint,
        "tg_slot": state.tg_slot,
    }
    try:
        await queue_contract.push_json_left(deps.redis_client, deps.outbox_queue_key, out_payload)
        deps.log_fn(
            f"event=photo_expected_reply_sent tenant={state.tenant_id} lead_id={state.lead_id} peer={state.peer_log_hint or '-'}"
        )
    except Exception as exc:
        deps.log_fn(
            f"event=photo_expected_reply_failed tenant={state.tenant_id} lead_id={state.lead_id} error={exc}"
        )


async def _handle_attachment_or_manager_handoff(
    state: TelegramIncomingState,
    *,
    deps: TelegramIncomingDeps,
) -> bool:
    if state.attachments:
        deps.log_fn(
            f"event=incoming_attachments channel=telegram tenant={state.tenant_id} lead_id={state.lead_id} count={len(state.attachments)} has_photo={int(state.has_photo)}"
        )
    if _has_media_for_handoff(state):
        await _mark_telegram_handoff(state, "photo_received", deps=deps)
        return True
    if state.manager_outgoing:
        await _mark_telegram_handoff(state, "manager_outgoing", deps=deps)
        return True
    return False


async def _mark_telegram_handoff(
    state: TelegramIncomingState,
    reason: str,
    *,
    deps: TelegramIncomingDeps,
) -> None:
    await deps.mark_handoff_silence_fn(
        state.tenant_id,
        state.lead_id,
        reason=reason,
        contact_hint=state.peer_log_hint or state.peer_value or state.contact_hint,
        username_hint=state.username,
    )
    deps.log_fn(
        f"event=handoff_marked channel=telegram tenant={state.tenant_id} lead_id={state.lead_id} reason={reason}"
    )
    await deps.cancel_pending_smart_reply_fn(state.tenant_id, "telegram", state.lead_id, reason=reason)


async def _is_smart_reply_silenced(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> bool:
    if not await deps.is_handoff_silenced_fn(state.tenant_id, state.lead_id):
        return False
    deps.log_fn(f"event=smart_reply_silenced channel=telegram tenant={state.tenant_id} lead_id={state.lead_id}")
    await deps.cancel_pending_smart_reply_fn(state.tenant_id, "telegram", state.lead_id, reason="silenced")
    return True


async def _run_catalog_flow(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> None:
    setattr(state, "_catalog_stopped", False)
    if deps.catalog_flow_service is None:
        return
    try:
        tenant_cfg = deps.read_tenant_config_fn(int(state.tenant_id))
    except Exception:
        tenant_cfg = None
    try:
        result = await deps.catalog_flow_service.handle_catalog_flow(
            tenant=int(state.tenant_id),
            lead_id=int(state.lead_id),
            refer_id=int(state.lead_id),
            text=state.text,
            provider="telegram",
            resolved_provider="telegram",
            message_id=state.message_id or str(state.lead_id),
            cache_key=_catalog_cache_key(state),
            telegram_user_id=state.telegram_user_id,
            peer_value=state.peer_value or state.peer_log_hint,
            peer_id=state.peer_id,
            redis_conn=deps.redis_client,
            tenant_cfg=tenant_cfg if isinstance(tenant_cfg, dict) else None,
        )
        if result.catalog_sent:
            state.first_catalog_sent_now = not bool(result.catalog_already_sent)
            deps.log_fn(
                "event=catalog_flow_worker_sent channel=telegram tenant=%s lead_id=%s"
                % (state.tenant_id, state.lead_id)
            )
        if result.stop_processing:
            setattr(state, "_catalog_stopped", True)
            deps.log_fn(
                "event=catalog_flow_worker_stop channel=telegram tenant=%s lead_id=%s reason=%s"
                % (state.tenant_id, state.lead_id, result.stop_reason or "-")
            )
    except Exception as exc:
        deps.log_fn(
            "event=catalog_flow_worker_failed channel=telegram tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )


def _catalog_cache_key(state: TelegramIncomingState) -> tuple[int, str] | None:
    if state.telegram_user_id:
        return int(state.tenant_id), f"tg:{int(state.telegram_user_id)}"
    if state.peer_value:
        return int(state.tenant_id), f"tg:peer:{state.peer_value}"
    if state.lead_id > 0:
        return int(state.tenant_id), f"lead:{int(state.lead_id)}"
    return None


async def _ensure_phone_contact_if_missing(state: TelegramIncomingState, *, deps: TelegramIncomingDeps) -> bool:
    try:
        existing_phone = await deps.get_contact_phone_by_lead_fn(state.lead_id)
    except Exception:
        existing_phone = None
    if existing_phone:
        return False
    cached_phone_norm = await _cached_phone_for_telegram(state, deps=deps)
    if _notify_bot_phone_link_forbidden(cached_phone_norm, state, deps=deps):
        cached_phone_norm = None
    if not cached_phone_norm:
        return False
    try:
        contact_id = await deps.resolve_or_create_contact_fn(tenant_id=state.tenant_id, phone=cached_phone_norm)
        await deps.link_lead_contact_fn(
            state.lead_id,
            contact_id,
            channel="telegram",
            peer=state.peer_value or state.peer_log_hint,
        )
        await deps.update_contact_phone_fn(contact_id, cached_phone_norm)
        deps.log_fn(
            f"event=telegram_contact_linked_by_phone tenant={state.tenant_id} lead_id={state.lead_id} contact_id={contact_id} phone={cached_phone_norm}"
        )
        return True
    except Exception:
        return False


async def _maybe_enqueue_telegram_smart_reply(
    state: TelegramIncomingState,
    event: Mapping[str, Any],
    *,
    deps: TelegramIncomingDeps,
) -> None:
    if not state.text:
        deps.log_fn(f"event=skip_no_text channel=telegram tenant={state.tenant_id} lead_id={state.lead_id}")
        return
    if not deps.telegram_reply_enabled_fn(state.tenant_id):
        deps.log_fn(
            f"event=telegram_reply_disabled channel=telegram tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        return
    if not deps.smart_reply_enabled_fn(state.tenant_id):
        deps.log_fn(
            f"event=smart_reply_disabled channel=telegram tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        return
    contact_id = deps.coerce_int_fn(event.get("contact_id"))
    refer_id = contact_id if contact_id and contact_id > 0 else state.lead_id
    reply_context = _telegram_reply_context(state)
    delayed = await deps.try_handle_smart_reply_with_delay_fn(
        tenant_id=state.tenant_id,
        lead_id=state.lead_id,
        channel="telegram",
        refer_id=refer_id,
        user_text=state.text,
        context=reply_context,
        bypass_delay=state.first_catalog_sent_now,
    )
    if delayed:
        return
    await deps.produce_and_enqueue_smart_reply_fn(
        tenant_id=state.tenant_id,
        lead_id=state.lead_id,
        channel="telegram",
        refer_id=refer_id,
        user_text=state.text,
        context=reply_context,
        delayed=False,
    )


def _telegram_reply_context(state: TelegramIncomingState) -> dict[str, Any]:
    return {
        "tg_slot": state.tg_slot,
        "message_id": state.message_id,
        "telegram_user_id": state.telegram_user_id,
        "peer_id": state.peer_id,
        "username": state.username,
    }
