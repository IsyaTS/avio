from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Sequence

from redis import exceptions as redis_ex

from libs.core.services import outbox_payloads, queue_contract


@dataclass(frozen=True)
class IncomingLoopDeps:
    redis_client: Any
    queue_key: str
    block_timeout: int
    enabled: bool
    log_fn: Callable[..., None]
    handle_incoming_event_fn: Callable[[Mapping[str, Any]], Awaitable[None]]


@dataclass(frozen=True)
class OutboxLoopDeps:
    redis_client: Any
    queue_keys: Sequence[str]
    outbox_queue_key: str
    outbox_dlq_key: str
    enabled: bool
    default_tenant_id: int
    log_fn: Callable[..., None]
    process_notification_fn: Callable[[Mapping[str, Any]], Awaitable[None]]
    resolve_channel_fn: Callable[[Mapping[str, Any]], str]
    is_status_echo_fn: Callable[[Mapping[str, Any]], bool]
    parse_send_not_before_ts_fn: Callable[[Mapping[str, Any]], float]
    coerce_int_fn: Callable[[Any], int | None]
    do_send_fn: Callable[[dict[str, Any]], Awaitable[tuple[str, str, str, int]]]
    write_result_fn: Callable[[dict[str, Any], str, int, str], Awaitable[None]]


async def process_incoming_queue(deps: IncomingLoopDeps) -> None:
    deps.log_fn(f"[worker] inbox loop start enabled={int(deps.enabled)} queue={deps.queue_key}")
    if not deps.enabled:
        return
    while True:
        try:
            try:
                popped = await deps.redis_client.brpop(deps.queue_key, timeout=deps.block_timeout)
            except redis_ex.ConnectionError:
                await asyncio.sleep(1.0)
                continue

            if not popped:
                continue

            _, raw_item = popped
            parsed = queue_contract.parse_queue_payload(raw_item)
            if not parsed.ok or parsed.payload is None:
                if parsed.error == "json_decode":
                    deps.log_fn(
                        f"event=incoming_parse_error queue={deps.queue_key} preview={parsed.preview}"
                    )
                else:
                    deps.log_fn(
                        f"event=incoming_skip reason={parsed.error or 'invalid_payload'} queue={deps.queue_key}"
                    )
                continue
            event = parsed.payload

            try:
                await deps.handle_incoming_event_fn(event)
            except Exception as exc:
                channel_hint = event.get("channel") or event.get("ch") or event.get("provider") or "-"
                deps.log_fn("event=incoming_unhandled channel=%s error=%s" % (channel_hint, exc))
                await asyncio.sleep(0)

        except Exception as exc:
            deps.log_fn(f"event=incoming_loop_error error={exc}")
            await asyncio.sleep(0.5)


async def process_outbox_queue(deps: OutboxLoopDeps) -> None:
    deps.log_fn(f"[worker] loop start, queues={list(deps.queue_keys)}")
    if not deps.enabled:
        env_hint = (os.getenv("OUTBOX_ENABLED") or "").strip().lower() or "1"
        deps.log_fn(f"[worker] outbox loop disabled outbox_enabled_env={env_hint}")
        return
    while True:
        item: dict[str, Any] | None = None
        try:
            item = await _pop_outbox_item(deps)
            if item is None:
                continue
            await _handle_outbox_item(deps, item)

        except Exception as exc:
            try:
                await queue_contract.push_json_left(deps.redis_client, deps.outbox_dlq_key, item or {})
            except Exception:
                pass
            deps.log_fn(f"[worker] err: {exc}")
            await asyncio.sleep(0.5)


async def _pop_outbox_item(deps: OutboxLoopDeps) -> dict[str, Any] | None:
    try:
        popped = await deps.redis_client.brpop(list(deps.queue_keys), timeout=5)
    except redis_ex.ConnectionError:
        await asyncio.sleep(1.0)
        return None
    if not popped:
        return None
    _, raw_item = popped
    parsed = queue_contract.parse_queue_payload(raw_item)
    if parsed.ok and parsed.payload is not None:
        return parsed.payload
    if parsed.error == "json_decode":
        deps.log_fn(f"[worker] json decode err: {parsed.preview[:200]}")
    else:
        deps.log_fn(
            "[worker] skip invalid outbox payload "
            f"reason={parsed.error or 'invalid_payload'}"
        )
    return None


async def _handle_outbox_item(deps: OutboxLoopDeps, item: dict[str, Any]) -> None:
    if isinstance(item, Mapping) and item.get("type") == "notify":
        await _handle_notification_item(deps, item)
        return
    if deps.is_status_echo_fn(item):
        _log_status_echo_skip(deps, item)
        return
    send_context = outbox_payloads.build_send_context(
        item,
        default_tenant_id=int(deps.default_tenant_id),
    )
    deps.log_fn(
        f"event=send_attempt channel={send_context.channel or '-'} "
        f"tenant={send_context.tenant_id} lead_id={send_context.lead_id}"
    )
    if await _defer_outbox_item_if_needed(deps, item, send_context):
        return
    await _send_outbox_item(deps, item, send_context)


async def _handle_notification_item(deps: OutboxLoopDeps, item: Mapping[str, Any]) -> None:
    deps.log_fn(
        f"event=notify_queue_item tenant={item.get('tenant') or item.get('tenant_id') or '-'} "
        f"lead_id={item.get('lead_id') or '-'} event={item.get('event') or 'notify'}"
    )
    try:
        await deps.process_notification_fn(item)
    except Exception:
        deps.log_fn(
            "event=notify_unhandled tenant=%s lead_id=%s event=%s"
            % (
                item.get("tenant_id") or item.get("tenant") or "-",
                item.get("lead_id") or "-",
                item.get("event") or "notify",
            )
        )


def _log_status_echo_skip(deps: OutboxLoopDeps, item: Mapping[str, Any]) -> None:
    channel_hint = deps.resolve_channel_fn(item)
    tenant_raw = item.get("tenant_id") or item.get("tenant") or deps.default_tenant_id
    try:
        tenant_id = int(tenant_raw)
    except Exception:
        tenant_id = int(deps.default_tenant_id)
    status = str(item.get("status") or "").strip() or "-"
    deps.log_fn(
        f"event=outbox_status_echo_skip channel={channel_hint or '-'} tenant={tenant_id} status={status}"
    )


async def _defer_outbox_item_if_needed(
    deps: OutboxLoopDeps,
    item: dict[str, Any],
    send_context: Any,
) -> bool:
    not_before_ts = deps.parse_send_not_before_ts_fn(item)
    if not_before_ts <= 0:
        return False
    wait_seconds = max(0.0, not_before_ts - time.time())
    if wait_seconds <= 0:
        return False
    _log_outbox_deferred(deps, item, send_context, wait_seconds)
    try:
        await queue_contract.push_json_left(deps.redis_client, deps.outbox_queue_key, item)
    except Exception as exc:
        deps.log_fn(
            "event=send_wait_requeue_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (send_context.channel or "-", send_context.tenant_id, send_context.lead_id, exc)
        )
    await asyncio.sleep(min(wait_seconds, 1.0))
    return True


def _log_outbox_deferred(
    deps: OutboxLoopDeps,
    item: Mapping[str, Any],
    send_context: Any,
    wait_seconds: float,
) -> None:
    split_idx = deps.coerce_int_fn(item.get("split_part_index")) or 0
    split_total = deps.coerce_int_fn(item.get("split_part_total")) or 0
    deps.log_fn(
        "event=send_wait_deferred channel=%s tenant=%s lead_id=%s wait=%.2fs part=%s/%s"
        % (
            send_context.channel or "-",
            send_context.tenant_id,
            send_context.lead_id,
            wait_seconds,
            split_idx or "-",
            split_total or "-",
        )
    )


async def _send_outbox_item(
    deps: OutboxLoopDeps,
    item: dict[str, Any],
    send_context: Any,
) -> None:
    status, reason, body, code = await deps.do_send_fn(item)
    status_str = str(status)
    reason_str = str(reason)
    deps.log_fn(
        f"[worker] send ch={send_context.channel or '-'} status={status_str} "
        f"reason={reason_str} code={code} body={body[:200]}"
    )
    lead_for_status = _lead_for_outbox_status(item, send_context.lead_id)
    _log_outbox_send_result(deps, send_context, lead_for_status, status_str, reason_str, code)
    if send_context.channel == "telegram":
        await _increment_telegram_outbox_metric(deps)
    if status_str == "sent":
        await deps.write_result_fn(item, status_str, code, reason_str)


def _lead_for_outbox_status(item: Mapping[str, Any], fallback: int) -> int:
    resolved_lead_for_log = item.get("_resolved_lead_id")
    if isinstance(resolved_lead_for_log, int) and resolved_lead_for_log > 0:
        return resolved_lead_for_log
    return fallback


def _log_outbox_send_result(
    deps: OutboxLoopDeps,
    send_context: Any,
    lead_for_status: int,
    status_str: str,
    reason_str: str,
    code: int,
) -> None:
    if status_str == "sent":
        deps.log_fn(
            f"event=send_success channel={send_context.channel or '-'} tenant={send_context.tenant_id} "
            f"lead_id={lead_for_status} reason={reason_str} code={code}"
        )
        return
    deps.log_fn(
        "event=send_failed "
        f"channel={send_context.channel or '-'} tenant={send_context.tenant_id} "
        f"lead_id={lead_for_status} reason={reason_str or status_str} code={code}"
    )


async def _increment_telegram_outbox_metric(deps: OutboxLoopDeps) -> None:
    try:
        await deps.redis_client.incrby("metrics:telegram:outgoing", 1)
    except Exception:
        pass


__all__ = [
    "IncomingLoopDeps",
    "OutboxLoopDeps",
    "process_incoming_queue",
    "process_outbox_queue",
]
