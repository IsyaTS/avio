from __future__ import annotations

import asyncio
import json
from typing import Any, Mapping

import pytest

from apps.worker.services import queue_loops


pytestmark = pytest.mark.unit


class _FakeRedisIncoming:
    def __init__(self, values: list[Any]) -> None:
        self.values = list(values)

    async def brpop(self, _queue: str, timeout: int = 0) -> Any:
        _ = timeout
        if self.values:
            value = self.values.pop(0)
            if isinstance(value, BaseException):
                raise value
            return ("inbox:message_in", value)
        raise RuntimeError("stop-loop")


class _FakeRedisOutbox:
    def __init__(self, values: list[Any]) -> None:
        self.values = list(values)
        self.requeued: list[tuple[str, dict[str, Any]]] = []
        self.dlq: list[tuple[str, dict[str, Any]]] = []
        self.metrics: list[tuple[str, int]] = []

    async def brpop(self, _queues: list[str], timeout: int = 0) -> Any:
        _ = timeout
        if self.values:
            value = self.values.pop(0)
            if isinstance(value, BaseException):
                raise value
            return ("outbox:send", value)
        raise RuntimeError("stop-loop")

    async def lpush(self, key: str, value: str) -> int:
        payload = json.loads(value)
        if key == "outbox:dlq":
            self.dlq.append((key, payload))
        else:
            self.requeued.append((key, payload))
        return 1

    async def incrby(self, key: str, amount: int) -> int:
        self.metrics.append((key, amount))
        return amount


@pytest.mark.asyncio
async def test_process_incoming_queue_handles_payload_and_stops() -> None:
    logs: list[str] = []
    handled: list[dict[str, Any]] = []

    async def handle(event: Mapping[str, Any]) -> None:
        handled.append(dict(event))

    deps = queue_loops.IncomingLoopDeps(
        redis_client=_FakeRedisIncoming(['{"channel":"whatsapp","text":"hello"}']),
        queue_key="inbox:message_in",
        block_timeout=1,
        enabled=True,
        log_fn=lambda message, *args: logs.append(str(message % args if args else message)),
        handle_incoming_event_fn=handle,
    )

    task = asyncio.create_task(queue_loops.process_incoming_queue(deps))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert handled == [{"channel": "whatsapp", "text": "hello"}]
    assert logs[0] == "[worker] inbox loop start enabled=1 queue=inbox:message_in"


@pytest.mark.asyncio
async def test_process_outbox_queue_sends_and_writes_result() -> None:
    logs: list[str] = []
    sent: list[dict[str, Any]] = []
    written: list[tuple[dict[str, Any], str, int, str]] = []
    redis = _FakeRedisOutbox(['{"provider":"telegram","tenant":101,"lead_id":202,"text":"hello"}'])

    async def do_send(item: dict[str, Any]) -> tuple[str, str, str, int]:
        sent.append(dict(item))
        return "sent", "ok", "body", 200

    async def write_result(item: dict[str, Any], status: str, code: int, reason: str) -> None:
        written.append((dict(item), status, code, reason))

    deps = queue_loops.OutboxLoopDeps(
        redis_client=redis,
        queue_keys=["outbox:send"],
        outbox_queue_key="outbox:send",
        outbox_dlq_key="outbox:dlq",
        enabled=True,
        default_tenant_id=1,
        log_fn=lambda message, *args: logs.append(str(message % args if args else message)),
        process_notification_fn=lambda item: (_ for _ in ()).throw(AssertionError(item)),
        resolve_channel_fn=lambda item: "telegram",
        is_status_echo_fn=lambda item: False,
        parse_send_not_before_ts_fn=lambda item: 0.0,
        coerce_int_fn=lambda value: int(value) if value is not None else None,
        do_send_fn=do_send,
        write_result_fn=write_result,
    )

    task = asyncio.create_task(queue_loops.process_outbox_queue(deps))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert sent == [{"provider": "telegram", "tenant": 101, "lead_id": 202, "text": "hello"}]
    assert written == [({"provider": "telegram", "tenant": 101, "lead_id": 202, "text": "hello"}, "sent", 200, "ok")]
    assert ("metrics:telegram:outgoing", 1) in redis.metrics
    assert any("event=send_success channel=telegram tenant=101 lead_id=202 reason=ok code=200" in row for row in logs)


@pytest.mark.asyncio
async def test_process_outbox_queue_requeues_delayed_item() -> None:
    logs: list[str] = []
    redis = _FakeRedisOutbox(['{"provider":"avito","tenant":101,"lead_id":202,"text":"hello","send_not_before_ts":9999999999}'])

    deps = queue_loops.OutboxLoopDeps(
        redis_client=redis,
        queue_keys=["outbox:send"],
        outbox_queue_key="outbox:send",
        outbox_dlq_key="outbox:dlq",
        enabled=True,
        default_tenant_id=1,
        log_fn=lambda message, *args: logs.append(str(message % args if args else message)),
        process_notification_fn=lambda item: (_ for _ in ()).throw(AssertionError(item)),
        resolve_channel_fn=lambda item: "avito",
        is_status_echo_fn=lambda item: False,
        parse_send_not_before_ts_fn=lambda item: float(item["send_not_before_ts"]),
        coerce_int_fn=lambda value: int(value) if value is not None else None,
        do_send_fn=lambda item: (_ for _ in ()).throw(AssertionError(item)),
        write_result_fn=lambda *_args: (_ for _ in ()).throw(AssertionError("write_result")),
    )

    task = asyncio.create_task(queue_loops.process_outbox_queue(deps))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert redis.requeued
    assert redis.requeued[0][1]["lead_id"] == 202
    assert any("event=send_wait_deferred channel=avito tenant=101 lead_id=202" in row for row in logs)
