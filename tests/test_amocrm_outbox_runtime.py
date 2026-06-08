from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

from apps.worker.services import amocrm_outbox_runtime


pytestmark = pytest.mark.unit


def _deps(logs: list[str]) -> amocrm_outbox_runtime.AmoCrmOutboxDeps:
    return amocrm_outbox_runtime.AmoCrmOutboxDeps(
        enabled=True,
        outbox_limit=10,
        outbox_max_attempts=3,
        log_fn=lambda message, *args: logs.append(str(message % args if args else message)),
        read_tenant_config_fn=lambda _tenant: {},
        download_file_fn=lambda _url: (None, "", None),
    )


def test_parse_amocrm_payload_and_backoff() -> None:
    assert amocrm_outbox_runtime.parse_amocrm_payload({"a": 1}) == {"a": 1}
    assert amocrm_outbox_runtime.parse_amocrm_payload('{"a": 1}') == {"a": 1}
    assert amocrm_outbox_runtime.parse_amocrm_payload("bad") == {}
    assert amocrm_outbox_runtime.amocrm_backoff_seconds(1) == 5
    assert amocrm_outbox_runtime.amocrm_backoff_seconds(2) == 10
    assert amocrm_outbox_runtime.amocrm_backoff_seconds(99) == 300
    assert amocrm_outbox_runtime.is_amocrm_lead_not_found_error(
        Exception("amocrm_http_error:400 Lead not found")
    )


@pytest.mark.asyncio
async def test_process_amocrm_outbox_marks_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs: list[str] = []
    calls: list[tuple[str, Any]] = []

    async def fake_take_pending(limit: int) -> list[dict[str, Any]]:
        calls.append(("take", limit))
        if len(calls) == 1:
            return [{"id": 11, "tenant_id": 101, "lead_id": 202, "event_type": "noop"}]
        raise RuntimeError("stop-loop")

    async def fake_handle(event: dict[str, Any], *, deps: Any) -> None:
        calls.append(("handle", (event["id"], deps.enabled)))

    async def fake_mark_done(event_id: int) -> None:
        calls.append(("done", event_id))

    monkeypatch.setattr(amocrm_outbox_runtime.crm_outbox, "take_pending", fake_take_pending)
    monkeypatch.setattr(amocrm_outbox_runtime, "handle_amocrm_event", fake_handle)
    monkeypatch.setattr(amocrm_outbox_runtime.crm_outbox, "mark_done", fake_mark_done)

    task = asyncio.create_task(amocrm_outbox_runtime.process_amocrm_outbox(deps=_deps(logs)))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert ("handle", (11, True)) in calls
    assert ("done", 11) in calls
    assert any("amocrm_event_done tenant=101 lead_id=202 event=noop" in row for row in logs)


@pytest.mark.asyncio
async def test_process_amocrm_outbox_retries_and_marks_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs: list[str] = []
    retry_calls: list[tuple[str, Any]] = []
    dead_calls: list[tuple[str, Any]] = []
    events = [
        {"id": 21, "tenant_id": 101, "lead_id": 202, "event_type": "x", "attempts": 1},
        {"id": 22, "tenant_id": 101, "lead_id": 202, "event_type": "x", "attempts": 2},
    ]

    async def fake_take_pending(limit: int) -> list[dict[str, Any]]:
        _ = limit
        if events:
            return [events.pop(0)]
        raise RuntimeError("stop-loop")

    async def fake_handle(event: dict[str, Any], *, deps: Any) -> None:
        _ = deps
        raise RuntimeError(f"boom-{event['id']}")

    async def fake_mark_retry(
        event_id: int,
        attempts: int,
        next_retry: datetime,
        error: str,
    ) -> None:
        retry_calls.append((str(event_id), attempts, next_retry.tzinfo, error))

    async def fake_mark_dead(event_id: int, error: str) -> None:
        dead_calls.append((str(event_id), error))

    monkeypatch.setattr(amocrm_outbox_runtime.crm_outbox, "take_pending", fake_take_pending)
    monkeypatch.setattr(amocrm_outbox_runtime, "handle_amocrm_event", fake_handle)
    monkeypatch.setattr(amocrm_outbox_runtime.crm_outbox, "mark_retry", fake_mark_retry)
    monkeypatch.setattr(amocrm_outbox_runtime.crm_outbox, "mark_dead", fake_mark_dead)

    task = asyncio.create_task(amocrm_outbox_runtime.process_amocrm_outbox(deps=_deps(logs)))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert retry_calls and retry_calls[0][0] == "21"
    assert retry_calls[0][1] == 2
    assert retry_calls[0][2] == timezone.utc
    assert "boom-21" in retry_calls[0][3]
    assert dead_calls == [("22", "boom-22")]
    assert any("amocrm_event_retry tenant=101 lead_id=202 event=x attempts=2" in row for row in logs)
    assert any("amocrm_event_dead tenant=101 lead_id=202 event=x error=boom-22" in row for row in logs)
