from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from . import webhook_catalog_runtime
from . import webhook_incoming_runtime
from . import webhook_incoming_storage_runtime


@dataclass(frozen=True)
class ProcessPrepDeps:
    incoming_envelope_deps: webhook_incoming_runtime.IncomingEnvelopeDeps
    incoming_event_queue_deps: webhook_incoming_runtime.IncomingEventQueueDeps
    incoming_storage_deps: webhook_incoming_storage_runtime.IncomingStorageDeps


@dataclass(frozen=True)
class ProcessPrepResult:
    normalized_event: dict[str, Any]
    peer_value: str | None
    lead_id: int
    refer_id: int
    sender_jid_value: str | None
    enqueuer: webhook_incoming_runtime.IncomingEventEnqueuer


@dataclass(frozen=True)
class ProcessFlowDeps:
    catalog_flow_deps: webhook_catalog_runtime.CatalogFlowDeps
    incoming_post_catalog_deps: webhook_incoming_runtime.IncomingPostCatalogDeps


async def prepare_incoming_after_guard(
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    parsed: webhook_incoming_runtime.ParsedIncoming,
    *,
    tenant: int,
    deps: ProcessPrepDeps,
) -> ProcessPrepResult:
    envelope = webhook_incoming_runtime.build_normalized_incoming_event(
        webhook_incoming_runtime.envelope_context_from_parsed(
            body,
            msg,
            parsed,
            tenant=tenant,
        ),
        deps=deps.incoming_envelope_deps,
    )
    normalized_event = envelope.event
    peer_value = envelope.peer_value
    enqueuer = webhook_incoming_runtime.IncomingEventEnqueuer(
        normalized_event=normalized_event,
        channel=parsed.channel,
        tenant=tenant,
        deps=deps.incoming_event_queue_deps,
    )
    storage_result = await webhook_incoming_storage_runtime.persist_incoming_side_effects(
        webhook_incoming_storage_runtime.storage_context_from_parsed(
            body,
            msg,
            parsed,
            tenant=tenant,
            normalized_event=normalized_event,
            peer_value=peer_value,
        ),
        deps=deps.incoming_storage_deps,
    )
    return ProcessPrepResult(
        normalized_event=normalized_event,
        peer_value=peer_value,
        lead_id=storage_result.lead_id,
        refer_id=storage_result.refer_id,
        sender_jid_value=normalized_event.get("from_jid"),
        enqueuer=enqueuer,
    )


async def run_after_prepare_flow(
    request: Any,
    body: Mapping[str, Any],
    msg: Mapping[str, Any],
    parsed: webhook_incoming_runtime.ParsedIncoming,
    prep: ProcessPrepResult,
    *,
    tenant: int,
    deps: ProcessFlowDeps,
) -> dict[str, Any]:
    auto_reply_handled = False

    async def enqueue_incoming_event() -> None:
        await prep.enqueuer.enqueue(auto_reply_handled=auto_reply_handled)

    catalog_result = await webhook_catalog_runtime.run_catalog_flow_from_parsed(
        request,
        parsed,
        tenant=tenant,
        lead_id=prep.lead_id,
        refer_id=prep.refer_id,
        sender_jid_value=prep.sender_jid_value,
        peer_value=prep.peer_value,
        deps=deps.catalog_flow_deps,
    )
    if catalog_result.auto_reply_handled:
        auto_reply_handled = True
    if catalog_result.response_payload is not None:
        if catalog_result.enqueue_incoming:
            await enqueue_incoming_event()
        return catalog_result.response_payload

    post_result = await webhook_incoming_runtime.run_post_catalog_flow(
        webhook_incoming_runtime.post_catalog_context_from_parsed(
            body,
            msg,
            parsed,
            tenant=tenant,
            lead_id=prep.lead_id,
            refer_id=prep.refer_id,
            cfg=catalog_result.cfg,
            sender_jid_value=prep.sender_jid_value,
            normalized_event=prep.normalized_event,
            channel=parsed.channel,
            peer_value=prep.peer_value,
        ),
        deps=deps.incoming_post_catalog_deps,
    )
    if post_result.auto_reply_handled:
        auto_reply_handled = True
    if post_result.enqueue_regular:
        await enqueue_incoming_event()
    return post_result.response_payload
