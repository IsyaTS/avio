from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.responses import JSONResponse

from apps.api.web.services import webhook_entry_runtime


pytestmark = pytest.mark.unit


class _Logger:
    def __init__(self):
        self.records = []

    def info(self, message, *args):
        self.records.append((message, args))


def _ok(payload=None):
    body = {"ok": True}
    if payload:
        body.update(payload)
    return JSONResponse(body)


def _deps(*, manager: bool = False, guard_payload=None):
    parsed = SimpleNamespace(
        manager_flag=manager,
        lead_id=42,
        channel="telegram",
        message_id="m1",
        peer_for_log="peer",
    )
    base = SimpleNamespace(tenant=7, msg={"text": "hello"})

    async def _pre_reply_guard(*_args, **_kwargs):
        return guard_payload

    async def _prepare(*_args, **_kwargs):
        return SimpleNamespace(prepared=True)

    async def _run(*_args, **_kwargs):
        return {"queued": True}

    async def _handle_manager(ctx, deps):
        return JSONResponse({"ok": True, "manager": True, "lead_id": ctx.lead_id})

    incoming = SimpleNamespace(
        resolve_incoming_base=lambda body, deps: base,
        detect_manager_flags=lambda body, incoming_base, deps: SimpleNamespace(manager=manager),
        parse_incoming_payload=lambda body, incoming_base, manager_flags, deps: parsed,
        pre_reply_guard=_pre_reply_guard,
    )
    manager_module = SimpleNamespace(
        manager_context_from_parsed=lambda parsed, tenant, lead_id: SimpleNamespace(
            parsed=parsed,
            tenant=tenant,
            lead_id=lead_id,
        ),
        handle_manager_outgoing=_handle_manager,
    )
    process = SimpleNamespace(
        prepare_incoming_after_guard=_prepare,
        run_after_prepare_flow=_run,
    )
    logger = _Logger()
    deps = webhook_entry_runtime.ProcessIncomingEntryDeps(
        incoming_runtime_module=incoming,
        manager_outgoing_runtime_module=manager_module,
        process_runtime_module=process,
        incoming_runtime_deps_fn=lambda: object(),
        incoming_parse_deps_fn=lambda: object(),
        manager_outgoing_deps_fn=lambda: object(),
        incoming_guard_deps_fn=lambda: object(),
        process_prep_deps_fn=lambda: object(),
        process_flow_deps_fn=lambda: object(),
        ok_response_fn=_ok,
        logger=logger,
    )
    return deps, logger


@pytest.mark.asyncio
async def test_process_incoming_entry_runs_normal_flow() -> None:
    deps, logger = _deps()

    response = await webhook_entry_runtime.process_incoming_entry({"tenant": 7}, None, deps=deps)

    assert response.status_code == 200
    assert response.body.decode("utf-8") == '{"ok":true,"queued":true}'
    assert logger.records


@pytest.mark.asyncio
async def test_process_incoming_entry_returns_guard_payload() -> None:
    deps, _logger = _deps(guard_payload={"reply": "ignored"})

    response = await webhook_entry_runtime.process_incoming_entry({"tenant": 7}, None, deps=deps)

    assert response.body.decode("utf-8") == '{"ok":true,"reply":"ignored"}'


@pytest.mark.asyncio
async def test_process_incoming_entry_routes_manager_outgoing() -> None:
    deps, _logger = _deps(manager=True)

    response = await webhook_entry_runtime.process_incoming_entry({"tenant": 7}, None, deps=deps)

    assert response.body.decode("utf-8") == '{"ok":true,"manager":true,"lead_id":42}'
