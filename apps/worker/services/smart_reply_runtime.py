from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class SmartReplyEnqueueDeps:
    redis_client: Any
    outbox_queue_key: str
    split_part_delay_min_seconds: int
    split_part_delay_max_seconds: int
    log_fn: SyncFn
    select_auto_photos_fn: AsyncFn
    normalize_tg_slot_fn: SyncFn
    base_channel_reply_payload_fn: SyncFn
    split_reply_for_send_fn: SyncFn
    apply_custom_punctuation_style_fn: SyncFn
    is_punctuation_only_chunk_fn: SyncFn
    split_part_delay_enabled_fn: SyncFn
    split_part_delay_seconds_value_fn: SyncFn
    queue_contract_module: Any
    time_fn: Callable[[], float] = time.time


@dataclass(frozen=True)
class SmartReplyProduceDeps:
    log_fn: SyncFn
    generate_reply_text_fn: AsyncFn
    maybe_set_waiting_photo_state_fn: AsyncFn
    enqueue_channel_reply_payload_fn: AsyncFn
    mark_thread_bot_reply_fn: AsyncFn


@dataclass(frozen=True)
class SmartReplyFlushDeps:
    pending_replies: dict[str, dict[str, Any]]
    pending_lock: Any
    log_fn: SyncFn
    sleep_fn: AsyncFn
    is_handoff_silenced_fn: AsyncFn
    can_generate_reply_for_channel_fn: SyncFn
    compose_burst_user_text_fn: SyncFn
    produce_and_enqueue_smart_reply_fn: AsyncFn
    time_fn: Callable[[], float] = time.time


@dataclass(frozen=True)
class SmartReplyScheduleDeps:
    pending_replies: dict[str, dict[str, Any]]
    pending_lock: Any
    burst_max_messages: int
    log_fn: SyncFn
    smart_reply_pending_key_fn: SyncFn
    delay_seconds_value_fn: SyncFn
    merge_reply_context_fn: SyncFn
    flush_pending_smart_reply_fn: AsyncFn
    create_task_fn: Callable[..., asyncio.Task[Any]] = asyncio.create_task
    time_fn: Callable[[], float] = time.time


@dataclass(frozen=True)
class SmartReplyChannelDeps:
    telegram_reply_enabled_fn: SyncFn
    max_reply_enabled_fn: SyncFn
    max_personal_reply_enabled_fn: SyncFn
    avito_smart_reply_enabled_fn: SyncFn
    smart_reply_enabled_fn: SyncFn


@dataclass(frozen=True)
class SmartReplyGenerateDeps:
    run_response_pipeline_fn: AsyncFn
    default_fallback_reply_fn: SyncFn
    strip_instruction_leaks_fn: SyncFn
    log_smart_reply_diag_fn: SyncFn
    log_fn: SyncFn
    timeout_seconds: float


@dataclass(frozen=True)
class WaitingPhotoDeps:
    redis_client: Any
    handoff_silence_ttl_seconds: int
    photo_expectation_config_fn: SyncFn
    log_fn: SyncFn


def log_smart_reply_diag(
    channel: str,
    tenant_id: int,
    lead_id: int | None,
    reply: Any,
    *,
    log_fn: SyncFn,
) -> None:
    """Emit debug info about planner output for downstream analysis."""

    try:
        plan_data = getattr(reply, "llm_plan", None)
        next_questions: list[str] = []
        plan_cta = None
        if isinstance(plan_data, Mapping):
            raw_questions = plan_data.get("next_questions")
            if isinstance(raw_questions, (list, tuple)):
                next_questions = [str(q) for q in raw_questions if q]
            plan_cta = plan_data.get("cta")
        raw_answer = getattr(reply, "llm_raw_answer", None)
        final_answer = str(reply or "")
        log_fn(
            "event=smart_reply_diag channel=%s tenant=%s lead_id=%s plan_next_questions=%s plan_cta=%s answer=%s"
            % (
                channel,
                tenant_id,
                lead_id if lead_id is not None else 0,
                json.dumps(next_questions, ensure_ascii=False),
                json.dumps(plan_cta or "", ensure_ascii=False),
                json.dumps(raw_answer or final_answer, ensure_ascii=False),
            )
        )
    except Exception as exc:
        log_fn(
            "event=smart_reply_diag_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (channel, tenant_id, lead_id if lead_id is not None else 0, exc)
        )


def sanitize_outbound_reply_text(reply_text: str, *, strip_instruction_leaks_fn: SyncFn) -> str:
    candidate = re.sub(r"\s+", " ", str(reply_text or "")).strip()
    if not candidate:
        return ""
    candidate = candidate.replace("`", " ")
    try:
        cleaned = str(strip_instruction_leaks_fn(candidate) or "").strip()
    except Exception:
        cleaned = candidate
    if cleaned:
        return cleaned
    # If core leak-stripper intentionally removed the whole line, never resurrect
    # the original text via salvage fallback: this is almost always operator/meta text.
    if candidate:
        return ""
    # Defensive salvage for format/meta leaks in a single line.
    salvage = re.sub(
        r"(?iu)[,;:\-\s]*\bотвечайт\w*\s+(?:разв[её]рнут\w*|подробн\w*|не\s+односложн\w*)\b[.!?]*",
        "",
        candidate,
    )
    salvage = re.sub(
        r"(?iu)[,;:\-\s]*\bне\s+одн\w*\s+строк\w*\b[.!?]*",
        "",
        salvage,
    )
    salvage = re.sub(r"\s{2,}", " ", salvage).strip(" ,.;:-")
    return salvage


def merge_reply_context(
    channel: str,
    base: Mapping[str, Any],
    incoming: Mapping[str, Any],
    *,
    normalize_tg_slot_fn: SyncFn,
) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                merged[key] = value
            continue
        merged[key] = value
    if channel == "telegram":
        merged["tg_slot"] = normalize_tg_slot_fn(merged.get("tg_slot"))
    return merged


def can_generate_reply_for_channel(
    tenant_id: int,
    channel: str,
    *,
    deps: SmartReplyChannelDeps,
) -> bool:
    ch = str(channel).strip().lower()
    if ch == "telegram":
        return bool(deps.telegram_reply_enabled_fn(tenant_id)) and bool(
            deps.smart_reply_enabled_fn(tenant_id)
        )
    if ch == "max":
        return bool(deps.max_reply_enabled_fn(tenant_id)) and bool(
            deps.smart_reply_enabled_fn(tenant_id)
        )
    if ch == "max_personal":
        return bool(deps.max_personal_reply_enabled_fn(tenant_id)) and bool(
            deps.smart_reply_enabled_fn(tenant_id)
        )
    if ch == "avito":
        return bool(deps.avito_smart_reply_enabled_fn(tenant_id)) and bool(
            deps.smart_reply_enabled_fn(tenant_id)
        )
    if ch == "whatsapp":
        return bool(deps.smart_reply_enabled_fn(tenant_id))
    return False


async def generate_reply_text(
    *,
    tenant_id: int,
    lead_id: int,
    refer_id: int,
    channel: str,
    user_text: str,
    deps: SmartReplyGenerateDeps,
) -> tuple[str, Any]:
    reply: Any = ""
    reply_text = ""
    try:
        result = await deps.run_response_pipeline_fn(
            tenant_id=tenant_id,
            channel=channel,
            user_text=user_text,
            contact_id=refer_id if refer_id > 0 else 0,
            enable_photos=False,
            timeout_seconds=deps.timeout_seconds,
            log_fn=deps.log_fn,
        )
        reply_text = str(result.reply_text or "").strip()
        reply = result.reply_text
        source = str(getattr(result, "source", "llm") or "llm").strip().lower()
        if source != "llm":
            deps.log_fn(
                "event=smart_reply_quality_signal channel=%s tenant=%s lead_id=%s signal=pipeline_fallback source=%s"
                % (channel, tenant_id, lead_id, source)
            )
    except Exception as exc:
        deps.log_fn(
            "event=smart_reply_failed channel=%s tenant=%s lead_id=%s stage=pipeline error=%s"
            % (channel, tenant_id, lead_id, exc)
        )
        reply_text = deps.default_fallback_reply_fn(tenant_id)
        reply = reply_text

    reply_text = str(reply_text or "").strip()
    if channel == "telegram" and reply_text:
        # For Telegram, catalog links are handled by the file-send branch.
        reply_text = re.sub(r"https?://\\S*/pub/catalog/file/\\S*", "", reply_text).strip()
    if reply_text:
        reply_text = sanitize_outbound_reply_text(
            reply_text,
            strip_instruction_leaks_fn=deps.strip_instruction_leaks_fn,
        )
        if not reply_text:
            reply_text = deps.default_fallback_reply_fn(tenant_id)
    deps.log_smart_reply_diag_fn(channel, tenant_id, lead_id, reply)
    return reply_text, reply


async def maybe_set_waiting_photo_state(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    reply_text: str,
    deps: WaitingPhotoDeps,
) -> None:
    if channel not in {"telegram", "max", "max_personal"}:
        return
    markers, _, photo_ttl = deps.photo_expectation_config_fn(tenant_id)
    if not markers:
        return
    lowered = reply_text.lower()
    for marker in markers:
        if not isinstance(marker, str) or not marker.strip():
            continue
        if marker.strip().lower() not in lowered:
            continue
        ttl = int(photo_ttl) if int(photo_ttl or 0) > 0 else deps.handoff_silence_ttl_seconds
        state_key = f"conv:state:{tenant_id}:{lead_id}"
        try:
            await deps.redis_client.set(state_key, "waiting_photo", ex=ttl)
            deps.log_fn(
                "event=photo_expected_set channel=%s tenant=%s lead_id=%s ttl=%s marker=%s"
                % (channel, tenant_id, lead_id, ttl, marker)
            )
        except Exception as exc:
            deps.log_fn(
                "event=photo_expected_set_failed channel=%s tenant=%s lead_id=%s error=%s"
                % (channel, tenant_id, lead_id, exc)
            )
        break


async def enqueue_channel_reply_payload(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    reply_text: str,
    user_text: str,
    context: Mapping[str, Any],
    deps: SmartReplyEnqueueDeps,
) -> bool:
    attachments = await _auto_reply_attachments(
        tenant_id,
        lead_id,
        channel,
        user_text,
        reply_text,
        context,
        deps=deps,
    )
    base_payload = _base_reply_payload(
        tenant_id,
        lead_id,
        channel,
        context,
        attachments,
        deps=deps,
    )
    if base_payload is None:
        return False

    prepared_parts = _prepared_reply_parts(reply_text, channel, deps=deps)
    if not prepared_parts:
        return False
    if len(prepared_parts) > 1:
        deps.log_fn(
            "event=smart_reply_split channel=%s tenant=%s lead_id=%s parts=%s"
            % (channel, tenant_id, lead_id, len(prepared_parts))
        )

    try:
        await _push_reply_parts(
            base_payload,
            prepared_parts,
            tenant_id=tenant_id,
            lead_id=lead_id,
            channel=channel,
            deps=deps,
        )
    except Exception as exc:
        deps.log_fn(
            "event=smart_reply_enqueue_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (channel, tenant_id, lead_id, exc)
        )
        return False
    return True


async def produce_and_enqueue_smart_reply(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    refer_id: int,
    user_text: str,
    context: Mapping[str, Any],
    delayed: bool = False,
    deps: SmartReplyProduceDeps,
) -> bool:
    reply_text, _ = await deps.generate_reply_text_fn(
        tenant_id=tenant_id,
        lead_id=lead_id,
        refer_id=refer_id,
        channel=channel,
        user_text=user_text,
    )
    if not reply_text:
        deps.log_fn(
            "event=smart_reply_empty channel=%s tenant=%s lead_id=%s delayed=%s"
            % (channel, tenant_id, lead_id, int(delayed))
        )
        return False
    await deps.maybe_set_waiting_photo_state_fn(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        reply_text=reply_text,
    )
    enqueued = await deps.enqueue_channel_reply_payload_fn(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        reply_text=reply_text,
        user_text=user_text,
        context=context,
    )
    if not enqueued:
        return False
    await deps.mark_thread_bot_reply_fn(tenant_id, channel, lead_id)
    deps.log_fn(
        "event=smart_reply_enqueued channel=%s tenant=%s lead_id=%s delayed=%s"
        % (channel, tenant_id, lead_id, int(delayed))
    )
    return True


async def flush_pending_smart_reply(key: str, *, deps: SmartReplyFlushDeps) -> None:
    payload: dict[str, Any] | None = None
    try:
        async with deps.pending_lock:
            payload = deps.pending_replies.get(key)
            if not payload:
                return
            due_at = float(payload.get("due_at") or 0.0)
        sleep_for = max(0.0, due_at - deps.time_fn())
        if sleep_for > 0:
            await deps.sleep_fn(sleep_for)
        async with deps.pending_lock:
            payload = deps.pending_replies.pop(key, None)
        if not payload:
            return
        await _flush_pending_payload(payload, deps=deps)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _log_flush_failure(payload, exc, deps=deps)


async def schedule_delayed_smart_reply(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    refer_id: int,
    user_text: str,
    context: Mapping[str, Any],
    deps: SmartReplyScheduleDeps,
) -> None:
    key = deps.smart_reply_pending_key_fn(tenant_id, channel, lead_id)
    now_ts = deps.time_fn()
    async with deps.pending_lock:
        payload = deps.pending_replies.get(key)
        if payload:
            _append_pending_payload(
                payload,
                channel=channel,
                refer_id=refer_id,
                user_text=user_text,
                context=context,
                now_ts=now_ts,
                deps=deps,
            )
            return
        payload = _new_pending_payload(
            tenant_id=tenant_id,
            lead_id=lead_id,
            channel=channel,
            refer_id=refer_id,
            user_text=user_text,
            context=context,
            now_ts=now_ts,
            deps=deps,
        )
        task = deps.create_task_fn(
            deps.flush_pending_smart_reply_fn(key),
            name=f"smart-reply-delay:{channel}:{tenant_id}:{lead_id}",
        )
        payload["task"] = task
        deps.pending_replies[key] = payload
        deps.log_fn(
            "event=smart_reply_burst_scheduled channel=%s tenant=%s lead_id=%s delay=%.1fs"
            % (channel, tenant_id, lead_id, max(0.0, float(payload["due_at"]) - now_ts))
        )


async def _auto_reply_attachments(
    tenant_id: int,
    lead_id: int,
    channel: str,
    user_text: str,
    reply_text: str,
    context: Mapping[str, Any],
    *,
    deps: SmartReplyEnqueueDeps,
) -> list[dict[str, Any]]:
    if channel not in {"telegram", "max", "max_personal", "avito"}:
        return []
    return list(
        await deps.select_auto_photos_fn(
            tenant_id,
            channel,
            user_text,
            reply_text,
            lead_id=lead_id,
            context=context,
        )
        or []
    )


def _base_reply_payload(
    tenant_id: int,
    lead_id: int,
    channel: str,
    context: Mapping[str, Any],
    attachments: list[dict[str, Any]],
    *,
    deps: SmartReplyEnqueueDeps,
) -> dict[str, Any] | None:
    payload_context = dict(context)
    if channel == "telegram":
        payload_context["tg_slot"] = deps.normalize_tg_slot_fn(context.get("tg_slot"))
    return deps.base_channel_reply_payload_fn(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        context=payload_context,
        attachments=attachments or [],
    )


def _prepared_reply_parts(
    reply_text: str,
    channel: str,
    *,
    deps: SmartReplyEnqueueDeps,
) -> list[str]:
    reply_parts = list(deps.split_reply_for_send_fn(reply_text, channel) or [])
    prepared_parts: list[str] = []
    for part in reply_parts:
        styled_part = deps.apply_custom_punctuation_style_fn(part)
        final_text = (styled_part or part).strip()
        if final_text and not deps.is_punctuation_only_chunk_fn(final_text):
            prepared_parts.append(final_text)
    return prepared_parts


async def _push_reply_parts(
    base_payload: Mapping[str, Any],
    prepared_parts: list[str],
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    deps: SmartReplyEnqueueDeps,
) -> None:
    part_due_ts = deps.time_fn()
    use_part_delay = len(prepared_parts) > 1 and bool(deps.split_part_delay_enabled_fn(channel))
    if use_part_delay:
        deps.log_fn(
            "event=smart_reply_split_delay channel=%s tenant=%s lead_id=%s min=%s max=%s"
            % (
                channel,
                tenant_id,
                lead_id,
                deps.split_part_delay_min_seconds,
                deps.split_part_delay_max_seconds,
            )
        )
    for idx, part in enumerate(prepared_parts):
        payload = dict(base_payload)
        payload["text"] = part
        if idx > 0:
            payload["attachments"] = []
            if use_part_delay:
                part_due_ts += float(deps.split_part_delay_seconds_value_fn())
            payload["send_not_before_ts"] = float(part_due_ts)
            payload["split_part_index"] = int(idx + 1)
            payload["split_part_total"] = int(len(prepared_parts))
        await deps.queue_contract_module.push_json_left(
            deps.redis_client,
            deps.outbox_queue_key,
            payload,
        )


async def _flush_pending_payload(payload: Mapping[str, Any], *, deps: SmartReplyFlushDeps) -> None:
    tenant_id = int(payload.get("tenant_id") or 0)
    lead_id = int(payload.get("lead_id") or 0)
    channel = str(payload.get("channel") or "").strip().lower()
    if tenant_id <= 0 or lead_id <= 0 or not channel:
        return
    if await deps.is_handoff_silenced_fn(tenant_id, lead_id):
        deps.log_fn(
            "event=smart_reply_burst_drop channel=%s tenant=%s lead_id=%s reason=silenced"
            % (channel, tenant_id, lead_id)
        )
        return
    if not deps.can_generate_reply_for_channel_fn(tenant_id, channel):
        deps.log_fn(
            "event=smart_reply_burst_drop channel=%s tenant=%s lead_id=%s reason=disabled"
            % (channel, tenant_id, lead_id)
        )
        return
    parts = payload.get("parts") if isinstance(payload.get("parts"), list) else []
    user_text = deps.compose_burst_user_text_fn([str(item or "") for item in parts])
    if not user_text:
        return
    refer_id = int(payload.get("refer_id") or lead_id)
    context = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
    deps.log_fn(
        "event=smart_reply_burst_flush channel=%s tenant=%s lead_id=%s messages=%s"
        % (channel, tenant_id, lead_id, len(parts))
    )
    await deps.produce_and_enqueue_smart_reply_fn(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        refer_id=refer_id,
        user_text=user_text,
        context=dict(context),
        delayed=True,
    )


def _log_flush_failure(
    payload: Mapping[str, Any] | None,
    exc: Exception,
    *,
    deps: SmartReplyFlushDeps,
) -> None:
    if payload:
        channel = str(payload.get("channel") or "")
        tenant_id = int(payload.get("tenant_id") or 0)
        lead_id = int(payload.get("lead_id") or 0)
    else:
        channel = "-"
        tenant_id = 0
        lead_id = 0
    deps.log_fn(
        "event=smart_reply_burst_flush_failed channel=%s tenant=%s lead_id=%s error=%s"
        % (channel, tenant_id, lead_id, exc)
    )


def _append_pending_payload(
    payload: dict[str, Any],
    *,
    channel: str,
    refer_id: int,
    user_text: str,
    context: Mapping[str, Any],
    now_ts: float,
    deps: SmartReplyScheduleDeps,
) -> None:
    parts = payload.get("parts")
    if not isinstance(parts, list):
        parts = []
    if user_text.strip():
        parts.append(user_text.strip())
    if len(parts) > deps.burst_max_messages:
        parts = parts[-deps.burst_max_messages:]
    payload["parts"] = parts
    payload["updated_at"] = now_ts
    payload["refer_id"] = int(refer_id or payload.get("refer_id") or 0)
    base_ctx = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
    payload["context"] = deps.merge_reply_context_fn(channel, base_ctx, context)
    due_at = float(payload.get("due_at") or now_ts)
    deps.log_fn(
        "event=smart_reply_burst_append channel=%s tenant=%s lead_id=%s messages=%s due_in=%.1fs"
        % (
            channel,
            int(payload.get("tenant_id") or 0),
            int(payload.get("lead_id") or 0),
            len(parts),
            max(0.0, due_at - now_ts),
        )
    )


def _new_pending_payload(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    refer_id: int,
    user_text: str,
    context: Mapping[str, Any],
    now_ts: float,
    deps: SmartReplyScheduleDeps,
) -> dict[str, Any]:
    return {
        "tenant_id": int(tenant_id),
        "lead_id": int(lead_id),
        "channel": channel,
        "refer_id": int(refer_id or 0),
        "parts": [user_text.strip()] if user_text.strip() else [],
        "context": deps.merge_reply_context_fn(channel, {}, context),
        "created_at": now_ts,
        "updated_at": now_ts,
        "due_at": now_ts + float(deps.delay_seconds_value_fn()),
    }
