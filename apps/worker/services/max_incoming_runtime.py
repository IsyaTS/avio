from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from libs.core.message_envelope import normalize_attachments as normalize_message_attachments
from libs.core.message_envelope import text_or_placeholder
from libs.core.services import incoming_events
from libs.core.services import queue_contract


LogFn = Callable[[str], None]
AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
IncDbErrorFn = Callable[[str], None]


@dataclass(frozen=True)
class MaxIncomingDeps:
    redis_client: Any
    log_fn: LogFn
    coerce_int_fn: SyncFn
    normalize_max_human_name_fn: SyncFn
    get_or_create_by_peer_fn: AsyncFn
    upsert_lead_fn: AsyncFn
    looks_like_manager_outgoing_fn: SyncFn
    is_manager_message_fn: SyncFn
    handle_followup_opt_out_fn: AsyncFn
    capture_followup_answer_fn: AsyncFn
    maybe_amocrm_inbound_fn: AsyncFn
    schedule_followups_fn: AsyncFn
    resolve_or_create_contact_fn: AsyncFn
    link_lead_contact_fn: AsyncFn
    update_contact_max_fn: AsyncFn
    insert_message_in_fn: AsyncFn
    match_behavior_trigger_fn: SyncFn
    mark_handoff_silence_fn: AsyncFn
    cancel_pending_smart_reply_fn: AsyncFn
    photo_expectation_config_fn: SyncFn
    notify_manager_handoff_fn: AsyncFn
    is_handoff_silenced_fn: AsyncFn
    read_tenant_config_fn: SyncFn
    max_reply_enabled_fn: SyncFn
    max_personal_reply_enabled_fn: SyncFn
    smart_reply_enabled_fn: SyncFn
    try_handle_smart_reply_with_delay_fn: AsyncFn
    produce_and_enqueue_smart_reply_fn: AsyncFn
    is_recent_max_bot_echo_fn: AsyncFn
    catalog_flow_service: Any
    outbox_queue_key: str
    inc_db_error_fn: IncDbErrorFn


@dataclass
class MaxIncomingState:
    channel_name: str
    tenant_id: int
    tenant_raw: Any
    text: str
    attachments: list[dict[str, Any]]
    has_photo: bool
    message_id: str
    max_user_id: int | None
    peer_value: str | None
    peer_id: int | None
    username: str | None
    display_name: str | None
    contact_hint: str | None
    title_hint: str | None
    lead_id: int = 0
    peer_log_hint: str | None = None
    contact_id: int = 0
    first_catalog_sent_now: bool = False
    catalog_stopped: bool = False


async def handle_max_incoming(event: Mapping[str, Any], *, deps: MaxIncomingDeps) -> None:
    state = await _prepare_max_state(event, deps=deps)
    if state is None:
        return
    if not await _resolve_max_lead(state, event, deps=deps):
        return
    if await _handle_manager_outgoing(state, event, deps=deps):
        return
    if await _handle_followup_text(state, deps=deps):
        return

    await _forward_max_amocrm(state, deps=deps)
    await _schedule_max_followups(state, deps=deps)
    await _resolve_and_link_max_contact(state, deps=deps)
    await _store_max_incoming(state, event, deps=deps)

    if await _handle_behavior_trigger(state, deps=deps):
        return
    if await _handle_expected_photo_reply(state, deps=deps):
        return
    if await _handle_attachment_handoff(state, deps=deps):
        return
    if await _is_smart_reply_silenced(state, deps=deps):
        return
    await _run_catalog_flow(state, deps=deps)
    if state.catalog_stopped:
        return
    await _maybe_enqueue_max_smart_reply(state, deps=deps)


async def _prepare_max_state(event: Mapping[str, Any], *, deps: MaxIncomingDeps) -> MaxIncomingState | None:
    channel_name = _max_channel_name(event)
    tenant_raw = event.get("tenant") or event.get("tenant_id")
    try:
        tenant_id = int(tenant_raw) if tenant_raw is not None else 0
    except Exception:
        tenant_id = 0
    if tenant_id <= 0:
        deps.log_fn("event=skip_invalid_tenant channel=%s tenant_raw=%s" % (channel_name, tenant_raw))
        return None

    text = incoming_events.normalize_event_text(event)
    attachments = normalize_message_attachments(incoming_events.collect_event_attachment_items(event))
    message_id_raw = event.get("message_id") or event.get("id")
    max_user_id = deps.coerce_int_fn(event.get("max_user_id") or event.get("user_id"))
    peer_value = _max_peer_value(event, max_user_id)
    peer_id = deps.coerce_int_fn(peer_value)
    if text:
        echo_chat = peer_value or (str(max_user_id) if max_user_id is not None else "")
        if await deps.is_recent_max_bot_echo_fn(tenant_id, channel_name, echo_chat, text):
            deps.log_fn(
                "event=skip_bot_echo channel=%s tenant=%s chat=%s text=%r"
                % (channel_name, tenant_id, echo_chat or "-", text[:160])
            )
            return None
    username = deps.normalize_max_human_name_fn(
        event.get("max_username") or event.get("username"),
        peer_value=peer_value,
        max_user_id=max_user_id,
    )
    display_name = deps.normalize_max_human_name_fn(
        event.get("display_name") or event.get("name"),
        peer_value=peer_value,
        max_user_id=max_user_id,
    )
    return MaxIncomingState(
        channel_name=channel_name,
        tenant_id=tenant_id,
        tenant_raw=tenant_raw,
        text=text,
        attachments=attachments,
        has_photo=incoming_events.has_image_attachment(attachments),
        message_id=str(message_id_raw) if message_id_raw is not None else "",
        max_user_id=max_user_id,
        peer_value=peer_value,
        peer_id=peer_id,
        username=username,
        display_name=display_name,
        contact_hint=display_name or username,
        title_hint=display_name or username,
    )


def _max_channel_name(event: Mapping[str, Any]) -> str:
    raw = event.get("channel") or event.get("ch") or event.get("provider")
    channel_name = str(raw or "max").strip().lower() or "max"
    return channel_name if channel_name in {"max", "max_personal"} else "max"


def _max_peer_value(event: Mapping[str, Any], max_user_id: int | None) -> str | None:
    peer_raw = event.get("peer") or event.get("chat_id") or event.get("peer_id")
    if isinstance(peer_raw, str):
        peer_value = peer_raw.strip() or None
    elif peer_raw is not None:
        peer_value = str(peer_raw).strip() or None
    else:
        peer_value = None
    if not peer_value and max_user_id is not None:
        peer_value = str(max_user_id)
    return peer_value


async def _resolve_max_lead(
    state: MaxIncomingState,
    event: Mapping[str, Any],
    *,
    deps: MaxIncomingDeps,
) -> bool:
    lead_candidate = deps.coerce_int_fn(event.get("lead_id"))
    lead_hint = lead_candidate if lead_candidate and lead_candidate > 0 else None
    resolved_lead_id = await _lookup_max_lead(state, lead_hint, deps=deps)
    if resolved_lead_id is None and state.max_user_id is not None:
        resolved_lead_id = int(state.max_user_id)
    if resolved_lead_id is None:
        resolved_lead_id = int(time.time() * 1000)
    state.lead_id = int(resolved_lead_id)
    if not await _upsert_max_lead(state, deps=deps):
        return False
    state.peer_log_hint = state.peer_value or (str(state.peer_id) if state.peer_id is not None else None)
    deps.log_fn(
        f"event=inbox_lead_resolved channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id} peer={state.peer_log_hint or '-'}"
    )
    return True


async def _lookup_max_lead(
    state: MaxIncomingState,
    lead_hint: int | None,
    *,
    deps: MaxIncomingDeps,
) -> int | None:
    if lead_hint:
        return lead_hint
    if not state.peer_value:
        return None
    try:
        lead_lookup = await deps.get_or_create_by_peer_fn(
            tenant_id=state.tenant_id,
            channel=state.channel_name,
            peer=state.peer_value,
            lead_id_hint=lead_hint,
        )
        return int(lead_lookup)
    except Exception as exc:
        deps.inc_db_error_fn("get_or_create_lead_peer")
        deps.log_fn(
            "event=inbox_lead_resolve_failed channel=%s tenant=%s error=%s"
            % (state.channel_name, state.tenant_id, exc)
        )
        return None


async def _upsert_max_lead(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> bool:
    try:
        await deps.upsert_lead_fn(
            state.lead_id,
            channel=state.channel_name,
            tenant_id=state.tenant_id,
            peer=state.peer_value,
            contact=state.contact_hint,
            title=state.title_hint,
        )
        return True
    except Exception as exc:
        deps.inc_db_error_fn("upsert_lead")
        deps.log_fn(
            "event=inbox_lead_upsert_failed channel=%s tenant=%s error=%s"
            % (state.channel_name, state.tenant_id, exc)
        )
        return False


async def _handle_manager_outgoing(
    state: MaxIncomingState,
    event: Mapping[str, Any],
    *,
    deps: MaxIncomingDeps,
) -> bool:
    if not (deps.looks_like_manager_outgoing_fn(event) or deps.is_manager_message_fn(event)):
        return False
    await _mark_max_handoff(state, "manager_outgoing", deps=deps)
    return True


async def _handle_followup_text(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> bool:
    if not state.text:
        return False
    try:
        if await deps.handle_followup_opt_out_fn(state.tenant_id, state.lead_id, state.text):
            await deps.cancel_pending_smart_reply_fn(
                state.tenant_id,
                state.channel_name,
                state.lead_id,
                reason="followup_optout",
            )
            deps.log_fn(
                "event=followup_optout channel=%s tenant=%s lead_id=%s"
                % (state.channel_name, state.tenant_id, state.lead_id)
            )
            return True
        await deps.capture_followup_answer_fn(state.tenant_id, state.lead_id, state.text, state.channel_name)
    except Exception as exc:
        deps.log_fn(
            "event=followup_capture_warn channel=%s tenant=%s lead_id=%s error=%s"
            % (state.channel_name, state.tenant_id, state.lead_id, exc)
        )
    return False


async def _forward_max_amocrm(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> None:
    try:
        await deps.maybe_amocrm_inbound_fn(
            state.tenant_id,
            state.lead_id,
            state.text,
            state.channel_name,
            attachments=state.attachments,
            message_id=None,
        )
    except Exception as exc:
        deps.log_fn(
            "event=amocrm_inbound_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (state.channel_name, state.tenant_id, state.lead_id, exc)
        )


async def _schedule_max_followups(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> None:
    try:
        await deps.schedule_followups_fn(state.tenant_id, state.lead_id, state.channel_name)
    except Exception as exc:
        deps.log_fn(
            f"event=followup_schedule_warn channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id} error={exc}"
        )


async def _resolve_and_link_max_contact(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> None:
    try:
        state.contact_id = await deps.resolve_or_create_contact_fn(
            tenant_id=state.tenant_id,
            max_user_id=state.max_user_id,
            max_username=state.username,
        )
    except Exception as exc:
        deps.inc_db_error_fn("resolve_or_create_contact")
        deps.log_fn(
            "event=contact_resolve_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (state.channel_name, state.tenant_id, state.lead_id, exc)
        )
        state.contact_id = 0
    if not state.contact_id:
        return
    try:
        await deps.link_lead_contact_fn(
            state.lead_id,
            state.contact_id,
            channel=state.channel_name,
            peer=state.peer_value or "",
        )
        await deps.update_contact_max_fn(state.contact_id, state.max_user_id, state.username)
    except Exception as exc:
        deps.inc_db_error_fn("link_lead_contact")
        deps.log_fn(
            "event=link_lead_contact_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (state.channel_name, state.tenant_id, state.lead_id, exc)
        )


async def _store_max_incoming(
    state: MaxIncomingState,
    event: Mapping[str, Any],
    *,
    deps: MaxIncomingDeps,
) -> None:
    incoming_text = text_or_placeholder(state.text, state.attachments)
    incoming_stored = bool(event.get("_incoming_stored"))
    if incoming_text and not incoming_stored:
        try:
            await deps.insert_message_in_fn(
                state.lead_id,
                incoming_text,
                status="received",
                tenant_id=state.tenant_id,
            )
        except Exception as exc:
            deps.inc_db_error_fn("insert_message_in")
            deps.log_fn(
                "event=store_incoming_failed channel=%s tenant=%s lead_id=%s error=%s"
                % (state.channel_name, state.tenant_id, state.lead_id, exc)
            )
    elif incoming_text and incoming_stored:
        deps.log_fn(
            "event=store_incoming_skip_already_stored channel=%s tenant=%s lead_id=%s message_id=%s"
            % (state.channel_name, state.tenant_id, state.lead_id, state.message_id or "-")
        )


async def _handle_behavior_trigger(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> bool:
    if not state.text:
        return False
    trigger_rule = deps.match_behavior_trigger_fn(state.tenant_id, state.channel_name, state.text)
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
        f"event=trigger_match channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id} notify={int(notify_flag)} phrases={trigger_rule.get('phrases')}"
    )
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        state.channel_name,
        state.lead_id,
        reason="trigger_silence",
    )
    return True


async def _handle_expected_photo_reply(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> bool:
    if not (state.has_photo or state.attachments):
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
        state.channel_name,
        state.lead_id,
        reason="photo_expected_reply",
    )
    return True


async def _waiting_photo_state(state_key: str, *, deps: MaxIncomingDeps) -> bool:
    try:
        state_val = await deps.redis_client.get(state_key)
    except Exception:
        return False
    if isinstance(state_val, (bytes, bytearray)):
        state_val = state_val.decode("utf-8", errors="ignore")
    return isinstance(state_val, str) and state_val == "waiting_photo"


async def _enqueue_photo_expected_reply(
    state: MaxIncomingState,
    photo_reply: str,
    *,
    deps: MaxIncomingDeps,
) -> None:
    out_payload = {
        "lead_id": int(state.lead_id),
        "tenant": int(state.tenant_id),
        "tenant_id": int(state.tenant_id),
        "provider": state.channel_name,
        "ch": state.channel_name,
        "channel": state.channel_name,
        "text": photo_reply,
        "attachments": [],
        "peer": state.peer_value or state.peer_log_hint,
        "peer_id": state.peer_value or state.peer_log_hint,
    }
    try:
        await queue_contract.push_json_left(deps.redis_client, deps.outbox_queue_key, out_payload)
        deps.log_fn(
            f"event=photo_expected_reply_sent tenant={state.tenant_id} lead_id={state.lead_id} peer={state.peer_log_hint or '-'}"
        )
    except Exception as exc:
        deps.log_fn(
            f"event=photo_expected_reply_failed channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id} error={exc}"
        )


async def _handle_attachment_handoff(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> bool:
    if state.attachments:
        deps.log_fn(
            f"event=incoming_attachments channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id} count={len(state.attachments)} has_photo={int(state.has_photo)}"
        )
    if not (state.has_photo or state.attachments):
        return False
    await _mark_max_handoff(state, "photo_received", deps=deps)
    if state.attachments:
        await deps.maybe_amocrm_inbound_fn(
            state.tenant_id,
            state.lead_id,
            state.text,
            state.channel_name,
            attachments=state.attachments,
        )
    return True


async def _mark_max_handoff(state: MaxIncomingState, reason: str, *, deps: MaxIncomingDeps) -> None:
    await deps.mark_handoff_silence_fn(
        state.tenant_id,
        state.lead_id,
        reason=reason,
        contact_hint=state.peer_log_hint or state.peer_value or state.contact_hint,
        username_hint=state.username,
    )
    deps.log_fn(
        f"event=handoff_marked channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id} reason={reason}"
    )
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        state.channel_name,
        state.lead_id,
        reason=reason,
    )


async def _is_smart_reply_silenced(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> bool:
    if not await deps.is_handoff_silenced_fn(state.tenant_id, state.lead_id):
        return False
    deps.log_fn(
        f"event=smart_reply_silenced channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id}"
    )
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        state.channel_name,
        state.lead_id,
        reason="silenced",
    )
    return True


async def _run_catalog_flow(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> None:
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
            provider=state.channel_name,
            resolved_provider=state.channel_name,
            message_id=state.message_id or str(state.lead_id),
            cache_key=_catalog_cache_key(state),
            telegram_user_id=None,
            peer_value=state.peer_value or state.peer_log_hint,
            peer_id=state.peer_id,
            redis_conn=deps.redis_client,
            tenant_cfg=tenant_cfg if isinstance(tenant_cfg, dict) else None,
        )
        if result.catalog_sent:
            state.first_catalog_sent_now = not bool(result.catalog_already_sent)
            deps.log_fn(
                "event=catalog_flow_worker_sent channel=%s tenant=%s lead_id=%s"
                % (state.channel_name, state.tenant_id, state.lead_id)
            )
        if result.stop_processing:
            state.catalog_stopped = True
            deps.log_fn(
                "event=catalog_flow_worker_stop channel=%s tenant=%s lead_id=%s reason=%s"
                % (state.channel_name, state.tenant_id, state.lead_id, result.stop_reason or "-")
            )
    except Exception as exc:
        deps.log_fn(
            "event=catalog_flow_worker_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (state.channel_name, state.tenant_id, state.lead_id, exc)
        )


def _catalog_cache_key(state: MaxIncomingState) -> tuple[int, str] | None:
    if state.max_user_id is not None:
        return int(state.tenant_id), f"{state.channel_name}:{int(state.max_user_id)}"
    if state.peer_value:
        return int(state.tenant_id), f"{state.channel_name}:peer:{state.peer_value}"
    if state.lead_id > 0:
        return int(state.tenant_id), f"lead:{int(state.lead_id)}"
    return None


async def _maybe_enqueue_max_smart_reply(state: MaxIncomingState, *, deps: MaxIncomingDeps) -> None:
    if not state.text:
        deps.log_fn(f"event=skip_no_text channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id}")
        return
    reply_enabled = (
        deps.max_reply_enabled_fn(state.tenant_id)
        if state.channel_name == "max"
        else deps.max_personal_reply_enabled_fn(state.tenant_id)
    )
    if not reply_enabled:
        deps.log_fn(
            f"event=max_reply_disabled channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        return
    if not deps.smart_reply_enabled_fn(state.tenant_id):
        deps.log_fn(
            f"event=smart_reply_disabled channel={state.channel_name} tenant={state.tenant_id} lead_id={state.lead_id}"
        )
        return
    refer_id = state.contact_id if state.contact_id and state.contact_id > 0 else state.lead_id
    reply_context = {
        "message_id": state.message_id,
        "max_user_id": state.max_user_id,
        "peer": state.peer_value or state.peer_log_hint,
    }
    delayed = await deps.try_handle_smart_reply_with_delay_fn(
        tenant_id=state.tenant_id,
        lead_id=state.lead_id,
        channel=state.channel_name,
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
        channel=state.channel_name,
        refer_id=refer_id,
        user_text=state.text,
        context=reply_context,
        delayed=False,
    )
