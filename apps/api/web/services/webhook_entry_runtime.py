from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ProcessIncomingEntryDeps:
    incoming_runtime_module: Any
    manager_outgoing_runtime_module: Any
    process_runtime_module: Any
    incoming_runtime_deps_fn: SyncFn
    incoming_parse_deps_fn: SyncFn
    manager_outgoing_deps_fn: SyncFn
    incoming_guard_deps_fn: SyncFn
    process_prep_deps_fn: SyncFn
    process_flow_deps_fn: SyncFn
    ok_response_fn: SyncFn
    logger: Any


async def process_incoming_entry(
    body: dict,
    request: Request | None,
    *,
    deps: ProcessIncomingEntryDeps,
) -> JSONResponse:
    incoming_base = deps.incoming_runtime_module.resolve_incoming_base(
        body,
        deps=deps.incoming_runtime_deps_fn(),
    )
    tenant = incoming_base.tenant
    msg = incoming_base.msg
    manager_flags = deps.incoming_runtime_module.detect_manager_flags(
        body,
        incoming_base,
        deps=deps.incoming_runtime_deps_fn(),
    )
    parsed = deps.incoming_runtime_module.parse_incoming_payload(
        body,
        incoming_base,
        manager_flags,
        deps=deps.incoming_parse_deps_fn(),
    )
    _log_incoming(parsed, tenant, deps)
    if parsed.manager_flag:
        return await _handle_manager_outgoing(parsed, tenant, deps)

    guard_payload = await deps.incoming_runtime_module.pre_reply_guard(
        parsed,
        tenant=tenant,
        deps=deps.incoming_guard_deps_fn(),
    )
    if guard_payload is not None:
        return deps.ok_response_fn(guard_payload)

    prep = await deps.process_runtime_module.prepare_incoming_after_guard(
        body,
        msg,
        parsed,
        tenant=tenant,
        deps=deps.process_prep_deps_fn(),
    )
    response_payload = await deps.process_runtime_module.run_after_prepare_flow(
        request,
        body,
        msg,
        parsed,
        tenant=tenant,
        prep=prep,
        deps=deps.process_flow_deps_fn(),
    )
    return deps.ok_response_fn(response_payload)


def _log_incoming(parsed: Any, tenant: int, deps: ProcessIncomingEntryDeps) -> None:
    deps.logger.info(
        "webhook_received channel=%s tenant=%s lead_id=%s message_id=%s peer=%s",
        parsed.channel,
        tenant,
        parsed.lead_id,
        parsed.message_id or "",
        parsed.peer_for_log or "-",
    )


async def _handle_manager_outgoing(
    parsed: Any,
    tenant: int,
    deps: ProcessIncomingEntryDeps,
) -> Response:
    return await deps.manager_outgoing_runtime_module.handle_manager_outgoing(
        deps.manager_outgoing_runtime_module.manager_context_from_parsed(
            parsed,
            tenant=tenant,
            lead_id=parsed.lead_id,
        ),
        deps=deps.manager_outgoing_deps_fn(),
    )
