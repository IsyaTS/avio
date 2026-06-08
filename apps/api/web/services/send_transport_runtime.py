from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import httpx
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]


@dataclass
class SendTransportDeps:
    admin_token_fn: Callable[[], str]
    channel_endpoints: dict[str, str]
    message_to_dict_fn: Callable[[Any], dict[str, Any]]
    normalize_whatsapp_recipient_fn: Callable[[Any], tuple[str, str]]
    whatsapp_address_error: type[Exception]
    tenant_whatsapp_provider_fn: Callable[[int], str]
    whatsapp_send_url_fn: Callable[[str | None, int], str]
    prepare_whatsapp_payload_fn: Callable[[dict[str, Any], int], dict[str, Any]]
    outbox_enabled_fn: Callable[[], bool]
    get_outbox_whitelist_fn: Callable[[], Any]
    whitelist_contains_number_fn: Callable[[Any, str], bool]
    get_redis_client_fn: Callable[[], Any]
    outbox_queue_key: str
    send_strategy: str
    mark_handoff_silence_fn: AsyncFn
    ensure_worker_healthy_fn: AsyncFn
    transport_client_fn: Callable[[str, str | None], httpx.AsyncClient]
    resolve_or_create_contact_fn: AsyncFn
    upsert_lead_fn: AsyncFn
    link_lead_contact_fn: AsyncFn
    message_out_counter: Any
    send_fail_counter: Any
    logger: Any


@dataclass
class PreparedSend:
    payload: dict[str, Any]
    channel: str
    endpoint: str
    wa_provider: str | None = None
    normalized_to: Any = None
    whitelist_number: str | None = None
    normalized_e164: str | None = None
    raw_to: Any = None


def request_bool_param(request: Request, name: str) -> bool:
    try:
        return (request.query_params.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _safe_meta(message: Any) -> dict[str, Any]:
    if not isinstance(getattr(message, "meta", None), dict):
        return {}
    try:
        return json.loads(json.dumps(message.meta, ensure_ascii=False))
    except Exception:
        return dict(message.meta)


def _manager_flag_from_meta(meta: dict[str, Any], fallback: bool) -> bool:
    if fallback:
        return True
    raw_manager = meta.get("manager")
    if isinstance(raw_manager, str):
        return raw_manager.strip().lower() in {"1", "true", "yes", "on"}
    return bool(raw_manager)


def _lead_id_from_meta(meta: dict[str, Any]) -> int:
    lead_hint = meta.get("lead_id") or meta.get("leadId")
    try:
        return int(lead_hint) if lead_hint is not None else 0
    except Exception:
        return 0


def _inc(counter: Any, channel: str, label: str) -> None:
    counter.labels(channel, label).inc()


def _log_message_out(deps: SendTransportDeps, level: str, **fields: Any) -> None:
    deps.logger.log(
        getattr(__import__("logging"), level.upper(), 20),
        "event=message_out channel=%s tenant=%s to=%s status=%s",
        fields.get("channel"),
        fields.get("tenant"),
        fields.get("to") or "-",
        fields.get("status"),
    )


def _resolve_channel(message: Any, deps: SendTransportDeps) -> tuple[str, str | None, str]:
    channel = message.channel
    wa_provider: str | None = None
    endpoint = deps.channel_endpoints.get(channel) or ""
    if channel == "whatsapp":
        wa_provider = deps.tenant_whatsapp_provider_fn(message.tenant)
        endpoint = deps.whatsapp_send_url_fn(wa_provider, message.tenant)
    if not endpoint:
        raise HTTPException(status_code=400, detail="channel_unknown")
    return channel, wa_provider, endpoint


def _invalid_whatsapp_response(message: Any, payload: dict[str, Any], deps: SendTransportDeps, reason: str) -> JSONResponse:
    explanations = {
        "empty": "empty",
        "invalid_length": "expected 10-15 digits",
        "invalid_domain": "expected @c.us jid",
    }
    message_text = explanations.get(reason, reason)
    _inc(deps.message_out_counter, "whatsapp", "invalid_to")
    deps.logger.warning(
        "event=message_out channel=%s tenant=%s to=%s status=%s reason=%s",
        "whatsapp",
        message.tenant,
        payload.get("to") or "-",
        "invalid_to",
        message_text,
    )
    return JSONResponse({"error": f"invalid_to: {message_text}"}, status_code=400)


def _prepare_send(message: Any, deps: SendTransportDeps) -> tuple[PreparedSend, JSONResponse | None]:
    payload = deps.message_to_dict_fn(message)
    channel, wa_provider, endpoint = _resolve_channel(message, deps)
    state = PreparedSend(payload=payload, channel=channel, endpoint=endpoint, wa_provider=wa_provider)
    state.raw_to = payload.get("to")
    state.normalized_to = state.raw_to
    if channel != "whatsapp":
        return state, None
    try:
        digits, jid = deps.normalize_whatsapp_recipient_fn(state.raw_to)
    except deps.whatsapp_address_error as exc:
        return state, _invalid_whatsapp_response(message, payload, deps, str(exc) or "invalid")
    payload["to"] = jid
    state.normalized_to = jid
    state.whitelist_number = digits
    state.normalized_e164 = f"+{digits}"
    return state, None


def _reject_if_outbox_disabled(message: Any, state: PreparedSend, deps: SendTransportDeps) -> JSONResponse | None:
    if deps.outbox_enabled_fn():
        return None
    _inc(deps.message_out_counter, state.channel, "outbox_disabled")
    _log_message_out(
        deps,
        "warning",
        channel=state.channel,
        tenant=message.tenant,
        to=state.normalized_to or state.payload.get("to"),
        status="outbox_disabled",
    )
    return JSONResponse({"error": "outbox_disabled"}, status_code=403)


def _reject_if_not_whitelisted(message: Any, state: PreparedSend, deps: SendTransportDeps) -> JSONResponse | None:
    if state.channel != "whatsapp" or state.whitelist_number is None:
        return None
    whitelist = deps.get_outbox_whitelist_fn()
    if whitelist.allow_all or deps.whitelist_contains_number_fn(whitelist, state.whitelist_number):
        return None
    _inc(deps.message_out_counter, state.channel, "not_whitelisted")
    deps.logger.warning(
        "event=message_out channel=%s tenant=%s to=%s status=%s normalized_to=%s raw_to=%s whitelist=%s reason=%s",
        state.channel,
        message.tenant,
        state.normalized_to or "-",
        "not_whitelisted",
        state.normalized_e164 or f"+{state.whitelist_number}",
        state.raw_to or "-",
        whitelist.raw_value,
        "not_found",
    )
    return JSONResponse({"error": "not_whitelisted"}, status_code=403)


async def _resolve_queue_lead(message: Any, state: PreparedSend, deps: SendTransportDeps, meta: dict[str, Any]) -> tuple[int, int]:
    lead_from_meta = _lead_id_from_meta(meta)
    base_lead_id = lead_from_meta if lead_from_meta > 0 else int(time.time() * 1000)
    digits_only = state.normalized_to.split("@", 1)[0] if isinstance(state.normalized_to, str) else ""
    contact_id = 0
    if digits_only:
        try:
            contact_id = await deps.resolve_or_create_contact_fn(tenant_id=int(message.tenant), whatsapp_phone=digits_only)
        except Exception:
            contact_id = 0
    lead_resolved = await _upsert_queue_lead(message, state, deps, base_lead_id, digits_only)
    await _link_queue_contact(state, deps, lead_resolved, contact_id, digits_only)
    return (lead_resolved if lead_resolved > 0 else base_lead_id), contact_id


async def _upsert_queue_lead(message: Any, state: PreparedSend, deps: SendTransportDeps, lead_id: int, digits: str) -> int:
    try:
        return int(
            await deps.upsert_lead_fn(
                lead_id,
                channel="whatsapp",
                tenant_id=int(message.tenant),
                peer=state.normalized_to or digits or None,
                contact=digits or None,
                title=(f"WhatsApp {digits}" if digits else None),
            )
        )
    except Exception:
        return lead_id


async def _link_queue_contact(state: PreparedSend, deps: SendTransportDeps, lead_id: int, contact_id: int, digits: str) -> None:
    if not lead_id or not contact_id:
        return
    try:
        await deps.link_lead_contact_fn(lead_id, contact_id, channel="whatsapp", peer=state.normalized_to or digits)
    except Exception:
        return


def _build_queue_item(message: Any, state: PreparedSend, meta: dict[str, Any], lead_id: int, contact_id: int) -> dict[str, Any]:
    meta.setdefault("lead_id", lead_id)
    queue_message_id = (
        state.payload.get("message_id")
        or state.payload.get("meta", {}).get("message_id")
        or getattr(message, "message_id", None)
        or str(uuid.uuid4())
    )
    item = {
        "lead_id": lead_id,
        "tenant_id": int(message.tenant),
        "tenant": int(message.tenant),
        "provider": "whatsapp",
        "ch": "whatsapp",
        "channel": "whatsapp",
        "to": state.payload.get("to"),
        "text": state.payload.get("text") or "",
        "attachments": state.payload.get("attachments", []),
        "attachment": state.payload.get("attachment"),
        "meta": meta,
        "message_id": queue_message_id,
        "queued_at": time.time(),
        "origin": "app.send",
    }
    if state.wa_provider == "baileys" and isinstance(state.payload.get("to_jid"), str):
        item["to_jid"] = state.payload["to_jid"].strip()
    if contact_id:
        item["contact_id"] = contact_id
    return item


async def _queue_whatsapp_message(request: Request, message: Any, state: PreparedSend, deps: SendTransportDeps, manager: bool) -> JSONResponse | None:
    strategy_override = (request.query_params.get("strategy") or "").strip().lower()
    if deps.send_strategy != "redis" or strategy_override == "direct":
        return None
    redis_client = deps.get_redis_client_fn()
    if redis_client is None:
        _log_message_out(deps, "error", channel=state.channel, tenant=message.tenant, to=state.normalized_to, status="queue_unavailable")
        raise HTTPException(status_code=502, detail="queue_unavailable")
    meta = _safe_meta(message)
    manager = _manager_flag_from_meta(meta, manager)
    lead_id, contact_id = await _resolve_queue_lead(message, state, deps, meta)
    await deps.mark_handoff_silence_fn(tenant=message.tenant, lead_id=lead_id, manager_flag=manager)
    item = _build_queue_item(message, state, meta, lead_id, contact_id)
    try:
        await redis_client.lpush(deps.outbox_queue_key, json.dumps(item, ensure_ascii=False))
    except Exception as exc:
        deps.logger.error("event=message_out channel=%s tenant=%s to=%s status=queue_push_failed error=%s", state.channel, message.tenant, state.normalized_to or "-", exc)
        raise HTTPException(status_code=502, detail="queue_push_failed") from exc
    _inc(deps.message_out_counter, state.channel, "queued")
    _log_message_out(deps, "info", channel=state.channel, tenant=message.tenant, to=state.normalized_to, status="queued")
    return JSONResponse({"ok": True, "queued": True, "strategy": "redis"})


async def _post_remote(message: Any, state: PreparedSend, deps: SendTransportDeps, request_headers: dict[str, str] | None) -> httpx.Response:
    client = deps.transport_client_fn(state.channel, state.wa_provider if state.channel == "whatsapp" else None)
    request_kwargs: dict[str, Any] = {"json": state.payload, "timeout": httpx.Timeout(12.0)}
    if state.channel == "telegram":
        attachments = state.payload.get("attachments")
        request_kwargs["timeout"] = httpx.Timeout(90.0 if isinstance(attachments, list) and attachments else 25.0)
    if state.channel == "whatsapp":
        request_kwargs["timeout"] = httpx.Timeout(300.0 if (state.wa_provider or "waweb") != "baileys" else 60.0)
    if request_headers:
        request_kwargs["headers"] = request_headers
    try:
        return await client.post(state.endpoint, **request_kwargs)
    except httpx.HTTPError as exc:
        _inc(deps.send_fail_counter, state.channel, "http_error")
        _inc(deps.message_out_counter, state.channel, "http_error")
        deps.logger.error("event=message_out channel=%s tenant=%s to=%s status=http_error error=%s", state.channel, message.tenant, state.normalized_to or state.payload.get("to") or "-", exc)
        raise HTTPException(status_code=502, detail="worker_unreachable") from exc


async def _remote_response(message: Any, state: PreparedSend, deps: SendTransportDeps, response: httpx.Response, manager: bool) -> Response:
    if response.status_code == 409 and response.headers.get("X-Reauth", "").strip() == "1":
        _inc(deps.message_out_counter, state.channel, "reauth")
        _log_message_out(deps, "warning", channel=state.channel, tenant=message.tenant, to=state.normalized_to, status="reauth")
        headers = {"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache", "Expires": "0", "X-Reauth": "1"}
        return JSONResponse({"ok": False, "state": "need_qr", "error": "relogin_required"}, status_code=409, headers=headers)
    if not 200 <= response.status_code < 300:
        reason = f"status_{response.status_code}"
        _inc(deps.send_fail_counter, state.channel, reason)
        _inc(deps.message_out_counter, state.channel, "remote_error")
        _log_message_out(deps, "warning", channel=state.channel, tenant=message.tenant, to=state.normalized_to, status="remote_error")
        return Response(content=response.content, status_code=response.status_code, media_type=response.headers.get("Content-Type") or "application/json")
    _inc(deps.message_out_counter, state.channel, "success")
    _log_message_out(deps, "info", channel=state.channel, tenant=message.tenant, to=state.normalized_to, status="success")
    await deps.mark_handoff_silence_fn(tenant=message.tenant, lead_id=0, manager_flag=manager)
    try:
        body = response.json()
    except Exception:
        body = {"ok": True}
    return JSONResponse(body, status_code=response.status_code)


async def handle_send_transport_message(request: Request, message: Any, deps: SendTransportDeps) -> Response:
    admin_token = deps.admin_token_fn()
    if admin_token and (request.headers.get("X-Admin-Token") or "").strip() != admin_token:
        raise HTTPException(status_code=401, detail="unauthorized")
    if not message.has_content:
        raise HTTPException(status_code=400, detail="empty_message")
    manager = request_bool_param(request, "manager")
    state, reject = _prepare_send(message, deps)
    if reject is not None:
        return reject
    reject = _reject_if_outbox_disabled(message, state, deps) or _reject_if_not_whitelisted(message, state, deps)
    if reject is not None:
        return reject
    request_headers: dict[str, str] | None = None
    if state.channel == "whatsapp":
        state.payload = deps.prepare_whatsapp_payload_fn(state.payload, message.tenant)
        if state.wa_provider == "baileys":
            state.payload["tenant"] = int(message.tenant)
            state.payload["tenant_id"] = int(message.tenant)
        queued = await _queue_whatsapp_message(request, message, state, deps, manager)
        if queued is not None:
            return queued
        if state.wa_provider != "baileys":
            request_headers = {"X-Auth-Token": deps.admin_token_fn()}
            await deps.ensure_worker_healthy_fn()
    response = await _post_remote(message, state, deps, request_headers)
    return await _remote_response(message, state, deps, response, manager)
