from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from libs.core.services import incoming_events
from libs.core.services.queue_contract import dumps_queue_payload, push_json_left


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ProviderWebhookDeps:
    json_module: Any
    redis_queue: Any
    incoming_queue_key: str
    provider_tokens_repo: Any
    webhook_provider_counter: Any
    db_errors_counter: Any
    extract_provider_token_fn: SyncFn
    coerce_int_fn: SyncFn
    digits_fn: SyncFn
    sanitize_media_item_fn: SyncFn
    insert_webhook_event_fn: AsyncFn
    logger: Any
    wa_qr_cache_ttl_min: int
    wa_qr_cache_ttl_max: int
    random_module: Any = random
    time_module: Any = time


def normalize_whatsapp_incoming(
    payload: Mapping[str, Any],
    tenant: int,
    lead_hint: int | None = None,
    *,
    deps: ProviderWebhookDeps,
) -> dict[str, Any]:
    channel_value = (
        str(payload.get("channel") or payload.get("provider") or "whatsapp").strip().lower()
    )
    if channel_value and channel_value not in {"whatsapp", "wa"}:
        raise ValueError("invalid_channel")

    message_id_raw = payload.get("message_id") or payload.get("id")
    message_id = str(message_id_raw).strip() if message_id_raw is not None else ""
    if not message_id:
        raise ValueError("missing_message_id")

    message_node = payload.get("message")
    if not isinstance(message_node, Mapping):
        message_node = {}
    sender_str = _first_non_empty(
        payload.get("from_jid"),
        payload.get("from_raw"),
        message_node.get("from_jid"),
        message_node.get("from_raw"),
        payload.get("from"),
        payload.get("from_id"),
        message_node.get("from"),
        message_node.get("from_id"),
        payload.get("fromAddress"),
    )
    if not sender_str:
        raise ValueError("missing_from")
    sender_digits = deps.digits_fn(sender_str)
    if not sender_digits:
        raise ValueError("invalid_from")
    sender_jid = _normalize_whatsapp_jid(sender_str, sender_digits)

    text_raw = payload.get("text") or payload.get("body")
    text = str(text_raw).strip() if isinstance(text_raw, str) else ""
    media = _normalized_media(payload, deps=deps)
    normalized: dict[str, Any] = {
        "event": "messages.incoming",
        "tenant": int(tenant),
        "channel": "whatsapp",
        "provider": "whatsapp",
        "message_id": message_id,
        "from": sender_jid,
        "from_jid": sender_jid,
        "from_raw": sender_str,
        "from_digits": sender_digits,
    }
    if text:
        normalized["text"] = text
    if media:
        normalized["media"] = media

    ts_value = payload.get("ts") or payload.get("timestamp")
    if ts_value is not None:
        normalized["ts"] = ts_value
    for optional_key in ("to", "wa_id", "conversation_id"):
        if optional_key in payload:
            normalized[optional_key] = payload[optional_key]
    normalized["lead_id"] = int(_resolve_lead_id(payload, lead_hint, sender_digits, ts_value, deps=deps))
    return normalized


async def queue_incoming_event(event_payload: dict[str, Any], *, deps: ProviderWebhookDeps) -> None:
    try:
        dumps_queue_payload(event_payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="invalid_payload") from exc

    try:
        await push_json_left(deps.redis_queue, deps.incoming_queue_key, event_payload)
        hints = incoming_events.build_incoming_event_log_hints(event_payload)
        deps.logger.info(
            "incoming_enqueued ch=%s tenant=%s message_id=%s",
            hints.channel,
            hints.tenant,
            hints.message_id,
        )
    except Exception as exc:  # pragma: no cover - Redis connectivity issues
        deps.logger.exception("webhook_provider_queue_failed tenant=%s", event_payload.get("tenant"))
        raise HTTPException(status_code=500, detail="queue_error") from exc


async def cache_whatsapp_qr(
    payload: Mapping[str, Any],
    tenant: int,
    provider: str,
    event_name: str,
    *,
    deps: ProviderWebhookDeps,
) -> dict[str, Any]:
    qr_id_raw = (
        payload.get("qr_id") or payload.get("qrId") or payload.get("id") or payload.get("qr")
    )
    svg_raw = payload.get("svg") or payload.get("qr") or payload.get("data")
    if svg_raw is None:
        nested_payload = payload.get("payload")
        if isinstance(nested_payload, Mapping):
            svg_raw = nested_payload.get("svg")

    qr_id = str(qr_id_raw).strip() if qr_id_raw is not None else ""
    if not qr_id:
        raise HTTPException(status_code=422, detail="invalid_qr")
    svg_value = svg_raw.strip() if isinstance(svg_raw, str) else ""
    if not svg_value or not svg_value.lstrip().startswith("<svg"):
        raise HTTPException(status_code=422, detail="invalid_qr")

    ttl = deps.random_module.randint(deps.wa_qr_cache_ttl_min, deps.wa_qr_cache_ttl_max)
    cache_key = f"wa:qr:{tenant}:{qr_id}"
    svg_key = f"{cache_key}:svg"
    last_key = f"wa:qr:last:{tenant}"
    entry = {
        "tenant": int(tenant),
        "qr_id": qr_id,
        "qr_svg": svg_value,
        "provider": provider,
        "event": event_name,
        "updated_at": int(deps.time_module.time()),
    }
    try:
        serialized_entry = deps.json_module.dumps(entry, ensure_ascii=False)
    except Exception:
        serialized_entry = None

    try:
        await deps.redis_queue.set(svg_key, svg_value, ex=ttl)
        await deps.redis_queue.set(last_key, qr_id, ex=ttl)
        if serialized_entry is not None:
            await deps.redis_queue.set(cache_key, serialized_entry, ex=ttl)
    except Exception as exc:  # pragma: no cover - Redis failures
        deps.logger.exception("wa_qr_cache_write_failed tenant=%s qr_id=%s", tenant, qr_id)
        raise HTTPException(status_code=500, detail="cache_error") from exc

    deps.logger.info("wa_qr_cached tenant=%s qr_id=%s ttl=%s", tenant, qr_id, ttl)
    return {"qr_id": qr_id}


async def provider_webhook(request: Request, *, deps: ProviderWebhookDeps) -> JSONResponse:
    channel_label = "whatsapp"
    payload = await _request_json(request, deps=deps, channel_label=channel_label)
    tenant = _tenant_from_payload_or_query(payload, request, deps=deps, channel_label=channel_label)
    provider_value = (
        str(payload.get("provider") or payload.get("channel") or channel_label).strip().lower()
    )
    if provider_value and provider_value not in {"whatsapp", "wa"}:
        deps.webhook_provider_counter.labels("ignored", channel_label).inc()
        return JSONResponse({"ok": True, "queued": False, "event": provider_value or "ignored"})

    await _authorize_provider(request, tenant, deps=deps, channel_label=channel_label)
    event = _event_name(payload, deps=deps, channel_label=channel_label)
    if event == "messages.incoming":
        return await _handle_messages_incoming(payload, tenant, deps=deps, channel_label=channel_label)
    if event == "ready":
        return await _handle_ready(payload, tenant, deps=deps, channel_label=channel_label)
    if event == "qr":
        return await _handle_qr(payload, tenant, deps=deps, channel_label=channel_label)

    deps.webhook_provider_counter.labels("ignored", channel_label).inc()
    return JSONResponse({"ok": True, "queued": False, "event": event})


async def _request_json(
    request: Request,
    *,
    deps: ProviderWebhookDeps,
    channel_label: str,
) -> dict[str, Any]:
    try:
        payload = await request.json()
    except deps.json_module.JSONDecodeError:
        deps.webhook_provider_counter.labels("invalid_json", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_json")
    except Exception:
        deps.webhook_provider_counter.labels("invalid_payload", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_payload")
    if not isinstance(payload, dict):
        deps.webhook_provider_counter.labels("invalid_payload", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_payload")
    return payload


def _tenant_from_payload_or_query(
    payload: Mapping[str, Any],
    request: Request,
    *,
    deps: ProviderWebhookDeps,
    channel_label: str,
) -> int:
    raw_tenant = (
        payload.get("tenant") or request.query_params.get("tenant") or request.query_params.get("t")
    )
    tenant_candidate = deps.coerce_int_fn(raw_tenant)
    if tenant_candidate is None:
        deps.webhook_provider_counter.labels("invalid_tenant", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_tenant")
    return int(tenant_candidate)


async def _authorize_provider(
    request: Request,
    tenant: int,
    *,
    deps: ProviderWebhookDeps,
    channel_label: str,
) -> None:
    token = deps.extract_provider_token_fn(request)
    if not token:
        deps.webhook_provider_counter.labels("unauthorized", channel_label).inc()
        raise HTTPException(status_code=401, detail="unauthorized")
    try:
        stored = await deps.provider_tokens_repo.get_by_tenant(tenant)
    except Exception as exc:
        deps.db_errors_counter.labels("provider_token_get").inc()
        deps.webhook_provider_counter.labels("error", channel_label).inc()
        deps.logger.exception(
            "provider_token_lookup_failed channel=%s tenant=%s",
            channel_label,
            tenant,
        )
        raise HTTPException(status_code=500, detail="db_error") from exc
    if not stored or stored.token != token:
        deps.webhook_provider_counter.labels("unauthorized", channel_label).inc()
        raise HTTPException(status_code=401, detail="unauthorized")


def _event_name(payload: Mapping[str, Any], *, deps: ProviderWebhookDeps, channel_label: str) -> str:
    raw_event = str(payload.get("event") or "").strip().lower()
    event = "qr" if raw_event == "wa_qr" else raw_event
    if not event:
        deps.webhook_provider_counter.labels("invalid_payload", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_event")
    return event


async def _handle_messages_incoming(
    payload: dict[str, Any],
    tenant: int,
    *,
    deps: ProviderWebhookDeps,
    channel_label: str,
) -> JSONResponse:
    lead_hint = deps.coerce_int_fn(payload.get("lead_id") or payload.get("leadId"))
    try:
        normalized_event = normalize_whatsapp_incoming(payload, tenant, lead_hint, deps=deps)
    except ValueError as exc:
        deps.webhook_provider_counter.labels("invalid_payload", channel_label).inc()
        raise HTTPException(status_code=422, detail=str(exc) or "invalid_payload") from exc
    text_value = normalized_event.get("text", "")
    text_value = text_value.strip() if isinstance(text_value, str) else ""
    media_items = normalized_event.get("media") if isinstance(normalized_event.get("media"), list) else []
    if not text_value and not media_items:
        deps.webhook_provider_counter.labels("invalid_payload", channel_label).inc()
        raise HTTPException(status_code=422, detail="empty_message")
    await _insert_provider_event(payload, lead_hint, deps=deps, channel_label=channel_label)
    try:
        await queue_incoming_event(normalized_event, deps=deps)
    except HTTPException as exc:
        status_label = "invalid_payload" if exc.status_code < 500 else "queue_error"
        deps.webhook_provider_counter.labels(status_label, channel_label).inc()
        raise
    deps.webhook_provider_counter.labels("ok", channel_label).inc()
    sender_for_log = normalized_event.get("from_jid") or normalized_event.get("from") or "-"
    message_id = normalized_event.get("message_id") or "-"
    deps.logger.info(
        "event=webhook_received channel=%s tenant=%s from=%s msg=%s",
        channel_label,
        tenant,
        sender_for_log,
        message_id,
    )
    return JSONResponse({"ok": True, "queued": True})


async def _insert_provider_event(
    payload: Mapping[str, Any],
    lead_hint: int | None,
    *,
    deps: ProviderWebhookDeps,
    channel_label: str,
) -> None:
    try:
        await deps.insert_webhook_event_fn("whatsapp", "messages.incoming", lead_hint, payload)
    except Exception as exc:
        deps.db_errors_counter.labels("webhook_event_insert").inc()
        deps.webhook_provider_counter.labels("error", channel_label).inc()
        deps.logger.exception(
            "webhook_event_store_failed channel=%s tenant=%s",
            channel_label,
            payload.get("tenant"),
        )
        raise HTTPException(status_code=500, detail="db_error") from exc


async def _handle_ready(
    payload: Mapping[str, Any],
    tenant: int,
    *,
    deps: ProviderWebhookDeps,
    channel_label: str,
) -> JSONResponse:
    ready_event: dict[str, Any] = {
        "event": "ready",
        "tenant": tenant,
        "channel": channel_label,
        "provider": channel_label,
        "state": str(payload.get("state") or payload.get("status") or "ready"),
    }
    ts_value = payload.get("ts") or payload.get("timestamp")
    if ts_value is not None:
        ready_event["ts"] = ts_value
    try:
        await queue_incoming_event(ready_event, deps=deps)
    except HTTPException as exc:
        status_label = "invalid_payload" if exc.status_code < 500 else "queue_error"
        deps.webhook_provider_counter.labels(status_label, channel_label).inc()
        raise
    deps.webhook_provider_counter.labels("ok", channel_label).inc()
    deps.logger.info(
        "event=webhook_received channel=%s tenant=%s state=%s",
        channel_label,
        tenant,
        ready_event["state"],
    )
    return JSONResponse({"ok": True, "queued": True, "event": "ready"})


async def _handle_qr(
    payload: Mapping[str, Any],
    tenant: int,
    *,
    deps: ProviderWebhookDeps,
    channel_label: str,
) -> JSONResponse:
    try:
        qr_meta = await cache_whatsapp_qr(payload, tenant, channel_label, "qr", deps=deps)
    except HTTPException as exc:
        status_label = "invalid_payload" if exc.status_code < 500 else "error"
        deps.webhook_provider_counter.labels(status_label, channel_label).inc()
        raise
    deps.webhook_provider_counter.labels("ok", channel_label).inc()
    response_payload: dict[str, Any] = {"ok": True, "queued": False, "event": "qr"}
    response_payload.update(qr_meta)
    return JSONResponse(response_payload)


def _first_non_empty(*candidates: Any) -> str:
    for candidate in candidates:
        if candidate is None:
            continue
        candidate_text = str(candidate).strip()
        if candidate_text:
            return candidate_text
    return ""


def _normalize_whatsapp_jid(sender_str: str, sender_digits: str) -> str:
    formatted_lower = sender_str.strip().lower()
    if "@" not in formatted_lower:
        return f"{sender_digits}@c.us"
    if formatted_lower.endswith(("@c.us", "@s.whatsapp.net", "@lid", "@g.us")):
        return formatted_lower
    return f"{sender_digits}@c.us"


def _normalized_media(payload: Mapping[str, Any], *, deps: ProviderWebhookDeps) -> list[dict[str, Any]]:
    raw_media = payload.get("media") or payload.get("attachments") or []
    media: list[dict[str, Any]] = []
    if isinstance(raw_media, list):
        for item in raw_media:
            if isinstance(item, dict):
                media.append(deps.sanitize_media_item_fn(item))
    return media


def _resolve_lead_id(
    payload: Mapping[str, Any],
    lead_hint: int | None,
    sender_digits: str,
    ts_value: Any,
    *,
    deps: ProviderWebhookDeps,
) -> int:
    if isinstance(lead_hint, int) and lead_hint > 0:
        return int(lead_hint)
    conversation_hint = deps.coerce_int_fn(payload.get("conversation_id"))
    if conversation_hint and conversation_hint > 0:
        return int(conversation_hint)
    if sender_digits:
        try:
            return int(sender_digits)
        except Exception:
            pass
    ts_hint = deps.coerce_int_fn(ts_value)
    if ts_hint and ts_hint > 0:
        return int(ts_hint)
    return int(deps.time_module.time() * 1000)
