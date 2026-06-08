from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from libs.core.message_envelope import normalize_attachments as normalize_message_attachments
from libs.core.message_envelope import text_or_placeholder
from libs.core.services import incoming_events
from libs.core.services import outbox_payloads


LogFn = Callable[[str], None]
AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
IncDbErrorFn = Callable[[str], None]


@dataclass(frozen=True)
class WhatsAppIncomingDeps:
    default_tenant_id: int
    log_fn: LogFn
    coerce_int_fn: SyncFn
    is_whatsapp_group_fn: SyncFn
    digits_fn: SyncFn
    get_or_create_by_peer_fn: AsyncFn
    handle_followup_opt_out_fn: AsyncFn
    capture_followup_answer_fn: AsyncFn
    schedule_followups_fn: AsyncFn
    cancel_pending_smart_reply_fn: AsyncFn
    resolve_or_create_contact_fn: AsyncFn
    link_lead_contact_fn: AsyncFn
    insert_message_in_fn: AsyncFn
    maybe_amocrm_inbound_fn: AsyncFn
    match_behavior_trigger_fn: SyncFn
    mark_handoff_silence_fn: AsyncFn
    is_handoff_silenced_fn: AsyncFn
    smart_reply_enabled_fn: SyncFn
    try_handle_smart_reply_with_delay_fn: AsyncFn
    produce_and_enqueue_smart_reply_fn: AsyncFn
    inc_db_error_fn: IncDbErrorFn


@dataclass
class WhatsAppIncomingState:
    event: Mapping[str, Any]
    deps: WhatsAppIncomingDeps
    tenant_id: int
    message_id: str = ""
    sender_raw: Any = None
    sender_peer: str = ""
    sender_digits: str = ""
    attachments: list[dict[str, Any]] | None = None
    has_photo: bool = False
    text: str = ""
    conversation_id: int | None = None
    lead_hint: int | None = None
    source_real_id: int | None = None
    db_available: bool = True
    lead_id: int = 0
    contact_id: int = 0
    stored_incoming: bool = False

    @property
    def refer_id(self) -> int:
        return self.contact_id if self.contact_id and self.contact_id > 0 else self.lead_id


def _coerce_tenant(event: Mapping[str, Any], deps: WhatsAppIncomingDeps) -> int:
    tenant_raw = event.get("tenant") or event.get("tenant_id") or deps.default_tenant_id
    try:
        return int(tenant_raw)
    except Exception:
        return int(deps.default_tenant_id)


async def handle_whatsapp_incoming(
    event: Mapping[str, Any],
    *,
    deps: WhatsAppIncomingDeps,
) -> None:
    state = WhatsAppIncomingState(event=event, deps=deps, tenant_id=_coerce_tenant(event, deps))
    if event.get("auto_reply_handled"):
        deps.log_fn(f"event=incoming_skip_auto_handled channel=whatsapp tenant={state.tenant_id}")
        return
    if not _normalize_sender(state):
        return
    _normalize_message(state)
    if not await _resolve_lead(state):
        return
    if await _handle_followup_answer(state):
        return
    await _schedule_followups(state)
    await _resolve_contact(state)
    await _store_incoming_message(state)
    if await _maybe_trigger_handoff(state):
        return
    if await _maybe_attachment_handoff(state):
        return
    if await _should_skip_smart_reply(state):
        return
    await _dispatch_smart_reply(state)


def _normalize_sender(state: WhatsAppIncomingState) -> bool:
    event = state.event
    deps = state.deps
    state.message_id = str(event.get("message_id")) if event.get("message_id") is not None else ""
    state.sender_raw = (
        event.get("from_jid") or event.get("from") or event.get("from_raw") or event.get("sender")
    )
    if deps.is_whatsapp_group_fn(state.sender_raw):
        deps.log_fn(
            f"event=skip_group_message channel=whatsapp tenant={state.tenant_id} "
            f"message_id={state.message_id or '-'} from={state.sender_raw or '-'}"
        )
        return False
    state.sender_peer = outbox_payloads.normalize_whatsapp_peer(state.sender_raw)
    if not state.sender_peer:
        deps.log_fn(
            f"event=skip_invalid_sender channel=whatsapp tenant={state.tenant_id} "
            f"message_id={state.message_id}"
        )
        return False
    peer_local = state.sender_peer.split("@", 1)[0]
    state.sender_digits = str(event.get("from_digits") or "").strip()
    if not state.sender_digits:
        state.sender_digits = deps.digits_fn(peer_local)
    return True


def _normalize_message(state: WhatsAppIncomingState) -> None:
    event = state.event
    deps = state.deps
    state.attachments = normalize_message_attachments(
        incoming_events.collect_event_attachment_items(event)
    )
    state.has_photo = incoming_events.has_image_attachment(state.attachments)
    state.text = incoming_events.normalize_event_text(event)
    state.conversation_id = deps.coerce_int_fn(event.get("conversation_id"))
    state.lead_hint = deps.coerce_int_fn(event.get("lead_id"))
    if state.lead_hint is not None and state.lead_hint <= 0:
        state.lead_hint = None
    if state.lead_hint is None and state.conversation_id and state.conversation_id > 0:
        state.lead_hint = state.conversation_id
    if state.conversation_id and state.conversation_id > 0:
        state.source_real_id = state.conversation_id


async def _resolve_lead(state: WhatsAppIncomingState) -> bool:
    deps = state.deps
    fallback_lead = incoming_events.fallback_lead_id(
        lead_hint=state.lead_hint,
        numeric_identity=state.sender_digits,
        fallback_value=int(time.time() * 1000),
    )
    try:
        lead_lookup = await deps.get_or_create_by_peer_fn(
            tenant_id=state.tenant_id,
            channel="whatsapp",
            peer=state.sender_peer,
            lead_id_hint=state.lead_hint,
            source_real_id=state.source_real_id,
        )
        state.lead_id = int(lead_lookup)
    except Exception as exc:
        deps.inc_db_error_fn("get_or_create_lead_peer")
        deps.log_fn(
            "event=inbox_lead_resolve_failed channel=whatsapp tenant=%s error=%s fallback=%s"
            % (state.tenant_id, exc, fallback_lead)
        )
        state.db_available = False
        state.lead_id = int(fallback_lead or int(time.time() * 1000))
    if state.lead_id <= 0:
        deps.log_fn(
            f"event=skip_missing_lead channel=whatsapp tenant={state.tenant_id} "
            f"message_id={state.message_id}"
        )
        return False
    deps.log_fn(
        f"event=inbox_lead_resolved channel=whatsapp tenant={state.tenant_id} lead_id={state.lead_id}"
    )
    return True


async def _handle_followup_answer(state: WhatsAppIncomingState) -> bool:
    if not state.text:
        return False
    deps = state.deps
    try:
        if await deps.handle_followup_opt_out_fn(state.tenant_id, state.lead_id, state.text):
            await deps.cancel_pending_smart_reply_fn(
                state.tenant_id,
                "whatsapp",
                state.lead_id,
                reason="followup_optout",
            )
            deps.log_fn(
                "event=followup_optout channel=whatsapp tenant=%s lead_id=%s"
                % (state.tenant_id, state.lead_id)
            )
            return True
        await deps.capture_followup_answer_fn(state.tenant_id, state.lead_id, state.text, "whatsapp")
    except Exception as exc:
        deps.log_fn(
            "event=followup_capture_warn channel=whatsapp tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )
    return False


async def _schedule_followups(state: WhatsAppIncomingState) -> None:
    try:
        await state.deps.schedule_followups_fn(state.tenant_id, state.lead_id, "whatsapp")
    except Exception as exc:
        state.deps.log_fn(
            f"event=followup_schedule_warn channel=whatsapp tenant={state.tenant_id} "
            f"lead_id={state.lead_id} error={exc}"
        )


async def _resolve_contact(state: WhatsAppIncomingState) -> None:
    if not state.sender_digits or not state.db_available:
        return
    deps = state.deps
    try:
        state.contact_id = await deps.resolve_or_create_contact_fn(
            tenant_id=state.tenant_id,
            whatsapp_phone=state.sender_digits,
        )
    except Exception as exc:
        deps.inc_db_error_fn("resolve_or_create_contact")
        deps.log_fn(
            "event=contact_resolve_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )
        state.contact_id = 0


async def _store_incoming_message(state: WhatsAppIncomingState) -> None:
    if state.contact_id and state.db_available:
        await _link_contact_and_store(state)
    if (state.text or state.attachments) and not state.stored_incoming and state.db_available:
        await _store_message_and_amocrm(state)


async def _link_contact_and_store(state: WhatsAppIncomingState) -> None:
    deps = state.deps
    try:
        await deps.link_lead_contact_fn(
            state.lead_id,
            state.contact_id,
            channel="whatsapp",
            peer=state.sender_peer,
        )
    except Exception as exc:
        deps.inc_db_error_fn("link_lead_contact")
        deps.log_fn(
            "event=link_lead_contact_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )
    await _store_message_and_amocrm(state, mark_stored=True)


async def _store_message_and_amocrm(
    state: WhatsAppIncomingState,
    *,
    mark_stored: bool = False,
) -> None:
    deps = state.deps
    incoming_text = text_or_placeholder(state.text, state.attachments or [])
    if not incoming_text:
        return
    try:
        await deps.insert_message_in_fn(
            state.lead_id,
            incoming_text,
            status="received",
            tenant_id=state.tenant_id,
        )
        if mark_stored:
            state.stored_incoming = True
        await deps.maybe_amocrm_inbound_fn(
            state.tenant_id,
            state.lead_id,
            state.text,
            "whatsapp",
            attachments=state.attachments,
        )
    except Exception as exc:
        deps.inc_db_error_fn("insert_message_in")
        deps.log_fn(
            "event=store_incoming_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
            % (state.tenant_id, state.lead_id, exc)
        )


async def _maybe_trigger_handoff(state: WhatsAppIncomingState) -> bool:
    if not state.text:
        return False
    deps = state.deps
    trigger_rule = deps.match_behavior_trigger_fn(state.tenant_id, "whatsapp", state.text)
    if not trigger_rule or not trigger_rule.get("silence", True):
        return False
    notify_flag = bool(trigger_rule.get("notify"))
    await deps.mark_handoff_silence_fn(
        state.tenant_id,
        state.lead_id,
        reason="trigger_match",
        contact_hint=state.event.get("peer") or state.event.get("contact"),
        username_hint=state.event.get("username"),
        notify=notify_flag,
    )
    deps.log_fn(
        f"event=trigger_match channel=whatsapp tenant={state.tenant_id} "
        f"lead_id={state.lead_id} notify={int(notify_flag)} phrases={trigger_rule.get('phrases')}"
    )
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        "whatsapp",
        state.lead_id,
        reason="trigger_silence",
    )
    return True


async def _maybe_attachment_handoff(state: WhatsAppIncomingState) -> bool:
    deps = state.deps
    attachments = state.attachments or []
    if attachments:
        deps.log_fn(
            f"event=incoming_attachments channel=whatsapp tenant={state.tenant_id} "
            f"lead_id={state.lead_id} count={len(attachments)} has_photo={int(state.has_photo)}"
        )
    if not state.has_photo and not attachments:
        return False
    await deps.mark_handoff_silence_fn(
        state.tenant_id,
        state.lead_id,
        reason="photo_received",
        contact_hint=state.event.get("peer") or state.event.get("contact"),
        username_hint=state.event.get("username"),
    )
    if attachments:
        await deps.maybe_amocrm_inbound_fn(
            state.tenant_id,
            state.lead_id,
            state.text,
            "whatsapp",
            attachments=attachments,
        )
    deps.log_fn(
        f"event=handoff_marked channel=whatsapp tenant={state.tenant_id} "
        f"lead_id={state.lead_id} reason=photo_received"
    )
    await deps.cancel_pending_smart_reply_fn(
        state.tenant_id,
        "whatsapp",
        state.lead_id,
        reason="photo_received",
    )
    return True


async def _should_skip_smart_reply(state: WhatsAppIncomingState) -> bool:
    deps = state.deps
    if await deps.is_handoff_silenced_fn(state.tenant_id, state.lead_id):
        deps.log_fn(
            f"event=smart_reply_silenced channel=whatsapp tenant={state.tenant_id} "
            f"lead_id={state.lead_id}"
        )
        await deps.cancel_pending_smart_reply_fn(
            state.tenant_id,
            "whatsapp",
            state.lead_id,
            reason="silenced",
        )
        return True
    if not state.text:
        deps.log_fn(f"event=skip_no_text channel=whatsapp tenant={state.tenant_id} lead_id={state.lead_id}")
        return True
    if not deps.smart_reply_enabled_fn(state.tenant_id):
        deps.log_fn(
            f"event=smart_reply_disabled channel=whatsapp tenant={state.tenant_id} "
            f"lead_id={state.lead_id}"
        )
        return True
    return False


async def _dispatch_smart_reply(state: WhatsAppIncomingState) -> None:
    deps = state.deps
    sender_jid = outbox_payloads.normalize_baileys_jid(
        state.event.get("from_jid") or state.event.get("from_raw")
    )
    reply_context = {
        "message_id": state.message_id,
        "to": state.sender_digits,
        "to_jid": sender_jid,
    }
    delayed = await deps.try_handle_smart_reply_with_delay_fn(
        tenant_id=state.tenant_id,
        lead_id=state.lead_id,
        channel="whatsapp",
        refer_id=state.refer_id,
        user_text=state.text,
        context=reply_context,
    )
    if delayed:
        return
    await deps.produce_and_enqueue_smart_reply_fn(
        tenant_id=state.tenant_id,
        lead_id=state.lead_id,
        channel="whatsapp",
        refer_id=state.refer_id,
        user_text=state.text,
        context=reply_context,
        delayed=False,
    )
