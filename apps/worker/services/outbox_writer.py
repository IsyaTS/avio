from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from libs.core.db import insert_message_out, lead_exists, upsert_lead
from libs.core.learning.service import capture_intervention_episode
from libs.core.services import outbox_payloads, queue_contract


@dataclass(frozen=True)
class OutboxWriterDeps:
    redis_client: Any
    outbox_queue_key: str
    app_version: str
    default_tenant_id: int
    log_fn: Callable[..., None]
    collect_outgoing_attachments_fn: Callable[[Mapping[str, Any], int], list[dict[str, Any]]]
    is_manager_message_fn: Callable[[Mapping[str, Any]], bool]
    is_followup_message_fn: Callable[[Mapping[str, Any]], bool]
    db_error_labels_fn: Callable[[str], Any]


@dataclass
class WriteResultState:
    item: dict[str, Any]
    deps: OutboxWriterDeps
    context: Any
    lead_id: int
    tenant_id: int
    text: str
    manager_message: bool
    sent_status: str
    channel_name: str
    stored_message_id: int | None
    lead_ref: int


async def write_result(
    item: dict[str, Any],
    status: str,
    status_code: int,
    reason: str,
    *,
    deps: OutboxWriterDeps,
) -> None:
    _ = (status, status_code, reason)
    context = outbox_payloads.build_write_result_context(
        item,
        default_tenant_id=int(deps.default_tenant_id),
    )
    state = WriteResultState(
        item=item,
        deps=deps,
        context=context,
        lead_id=context.lead_id,
        tenant_id=context.tenant_id,
        text=context.text,
        manager_message=deps.is_manager_message_fn(item),
        sent_status="sent",
        channel_name=context.channel,
        stored_message_id=context.stored_message_id,
        lead_ref=context.lead_id,
    )
    if not await _ensure_outbox_lead_and_message(state):
        return
    await _enqueue_status_echo(state)
    await _capture_learning_episode(state)


async def _ensure_outbox_lead_and_message(state: WriteResultState) -> bool:
    if state.channel_name == "telegram" and state.stored_message_id:
        state.lead_ref = state.lead_id
        return True
    lead_plan = await _build_outbox_lead_plan(state)
    if lead_plan is None:
        return False
    state.lead_ref = lead_plan.lead_ref
    if not await _lead_available_for_outbox_message(state, lead_plan):
        return False
    if not state.stored_message_id:
        await _insert_outbox_message(state)
    return True


async def _build_outbox_lead_plan(state: WriteResultState) -> Any | None:
    try:
        resolved_lead_id = await upsert_lead(
            state.lead_id,
            **outbox_payloads.build_lead_upsert_kwargs(state.context),
        )
    except Exception as exc:
        state.deps.log_fn(
            "event=send_result status=skipped reason=lead_upsert_error "
            f"channel={state.channel_name} lead_id={state.lead_id} tenant={state.tenant_id} error={exc}"
        )
        return None
    return outbox_payloads.plan_lead_availability(
        lead_id=state.lead_id,
        resolved_lead_id=resolved_lead_id,
        stored_message_id=state.stored_message_id,
    )


async def _lead_available_for_outbox_message(state: WriteResultState, lead_plan: Any) -> bool:
    lead_available = lead_plan.available
    if lead_plan.needs_exists_check and lead_plan.exists_check_lead_id is not None:
        lead_available = await _check_outbox_lead_exists(state, lead_plan.exists_check_lead_id)
    elif lead_plan.missing_reason == "lead_upsert_missing":
        state.deps.log_fn(
            "event=send_result status=skipped reason=lead_upsert_missing "
            f"channel={state.channel_name} lead_id={state.lead_id} tenant={state.tenant_id}"
        )
    if lead_available:
        return True
    state.deps.log_fn(
        "event=send_result status=skipped reason=lead_missing_for_message "
        f"channel={state.channel_name} lead_id={lead_plan.lead_ref} tenant={state.tenant_id}"
    )
    return False


async def _check_outbox_lead_exists(state: WriteResultState, lead_id: int) -> bool:
    try:
        return await lead_exists(lead_id, tenant_id=state.tenant_id)
    except Exception as exc:
        state.deps.db_error_labels_fn("lead_exists").inc()
        state.deps.log_fn(
            "event=send_result status=skipped reason=lead_check_error "
            f"channel={state.channel_name} lead_id={lead_id} tenant={state.tenant_id} error={exc}"
        )
        return False


async def _insert_outbox_message(state: WriteResultState) -> None:
    try:
        attachments = state.deps.collect_outgoing_attachments_fn(state.item, state.tenant_id)
        is_followup = state.deps.is_followup_message_fn(state.item)
        state.stored_message_id = await insert_message_out(
            state.lead_ref,
            state.text,
            None,
            **outbox_payloads.build_insert_message_out_kwargs(
                context=state.context,
                status=state.sent_status,
                is_manager=state.manager_message,
                is_followup=is_followup,
                attachments=attachments,
                tg_slot=state.item.get("tg_slot"),
            ),
        )
    except Exception as exc:
        state.deps.log_fn(f"[worker] insert_message_out err: {exc}")


async def _enqueue_status_echo(state: WriteResultState) -> None:
    out = outbox_payloads.build_status_echo_payload(
        lead_id=state.lead_id,
        reply_text=state.text,
        status=state.sent_status,
        version=state.deps.app_version,
        item=state.item,
    )
    await queue_contract.push_json_right(state.deps.redis_client, state.deps.outbox_queue_key, out)
    state.deps.log_fn(
        f"event=enqueue_outbox queue={state.deps.outbox_queue_key} "
        f"lead_id={state.lead_id} channel={out['ch']} status={state.sent_status}"
    )
    state.deps.log_fn(f"[worker] reply -> lead {state.lead_id}: {state.text[:160]} ({state.sent_status})")


async def _capture_learning_episode(state: WriteResultState) -> None:
    capture_context = outbox_payloads.build_learning_capture_context(
        tenant_id=state.tenant_id,
        lead_ref=state.lead_ref,
        channel=state.channel_name,
        is_manager=state.manager_message,
        stored_message_id=state.stored_message_id,
    )
    if capture_context is None:
        return
    try:
        await capture_intervention_episode(
            tenant_id=capture_context.tenant_id,
            lead_id=capture_context.lead_id,
            channel=capture_context.channel,
            source_event=capture_context.source_event,
            manager_message_id=capture_context.manager_message_id,
            log_fn=state.deps.log_fn,
        )
    except Exception as exc:
        state.deps.log_fn(
            "event=learning_v2_capture_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (state.channel_name, state.tenant_id, state.lead_ref, exc)
        )


__all__ = ["OutboxWriterDeps", "write_result"]
