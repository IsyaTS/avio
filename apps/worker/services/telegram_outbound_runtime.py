from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
LogFn = Callable[[str], None]


@dataclass(frozen=True)
class TelegramOutboundDeps:
    tg_slot_min: int
    notify_bot_id: int | None
    tg_worker_token: str
    admin_token: str
    log_fn: LogFn
    normalize_tg_slot_fn: SyncFn
    virtual_tg_tenant_fn: SyncFn
    normalize_attachments_fn: SyncFn
    prepare_tg_attachments_for_send_fn: SyncFn
    wait_until_authorized_fn: AsyncFn
    telegram_transport_module: Any
    message_out_counter: Any
    sleep_fn: AsyncFn


@dataclass
class TelegramSendState:
    tenant_id: int
    tg_slot: int
    target: int
    send_tenant_id: int
    peer_hint: str | int | None
    text_value: str
    normalized_attachments: list[dict[str, Any]] | None
    meta: Dict[str, Any]
    headers: Dict[str, str]
    peer_id: int | None
    username: str | None
    lead_id: int | None
    retry_unknown: bool
    deps: TelegramOutboundDeps
    last_status: int = 0
    last_body: str = ""
    last_error: Optional[str] = None
    unauthorized_checked: bool = False


async def send_telegram(
    tenant_id: int,
    *,
    tg_slot: int,
    chat_id: int,
    peer_id: int | None,
    peer: str | None,
    telegram_user_id: int | None,
    username: str | None,
    text: str | None,
    attachments: list[dict[str, Any]] | None = None,
    reply_to: str | None = None,
    lead_id: int | None = None,
    deps: TelegramOutboundDeps,
) -> tuple[int, str]:
    target = int(chat_id)
    normalized_slot = deps.normalize_tg_slot_fn(tg_slot)
    send_tenant_id = deps.virtual_tg_tenant_fn(int(tenant_id), normalized_slot)

    if deps.notify_bot_id and int(target) == int(deps.notify_bot_id):
        deps.log_fn(f"event=telegram_send_skip reason=notify_bot tenant={tenant_id} target={target}")
        return (0, "skip_notify_bot")
    state = _build_telegram_send_state(
        tenant_id=int(tenant_id),
        normalized_slot=normalized_slot,
        target=target,
        send_tenant_id=send_tenant_id,
        peer=peer,
        peer_id=peer_id,
        username=username,
        text=text,
        attachments=attachments,
        reply_to=reply_to,
        lead_id=lead_id,
        deps=deps,
    )
    _log_telegram_send_payload(state)
    await _send_telegram_attempts(state)
    deps.log_fn(f"[worker] telegram response status={state.last_status} body={state.last_body[:400]}")
    fallback = await _maybe_send_telegram_text_fallback(state)
    if fallback is not None:
        return fallback
    if state.last_status == 422 and not state.last_error:
        state.last_body = json.dumps({"error": "validation_error"}, ensure_ascii=False)
    return state.last_status, state.last_body


def _build_telegram_send_state(
    *,
    tenant_id: int,
    normalized_slot: int,
    target: int,
    send_tenant_id: int,
    peer: str | None,
    peer_id: int | None,
    username: str | None,
    text: str | None,
    attachments: list[dict[str, Any]] | None,
    reply_to: str | None,
    lead_id: int | None,
    deps: TelegramOutboundDeps,
) -> TelegramSendState:
    normalized_attachments = deps.normalize_attachments_fn(attachments or [])
    if normalized_attachments:
        normalized_attachments = deps.prepare_tg_attachments_for_send_fn(tenant_id, normalized_attachments)
    return TelegramSendState(
        tenant_id=tenant_id,
        tg_slot=normalized_slot,
        target=target,
        send_tenant_id=send_tenant_id,
        peer_hint=peer or str(target),
        text_value=str(text or "").strip(),
        normalized_attachments=normalized_attachments,
        meta=_telegram_meta(reply_to=reply_to, peer_id=peer_id),
        headers=_telegram_headers(deps),
        peer_id=peer_id,
        username=username,
        lead_id=lead_id,
        retry_unknown=_telegram_retry_unknown(bool(normalized_attachments)),
        deps=deps,
    )


def _telegram_meta(*, reply_to: str | None, peer_id: int | None) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    if reply_to:
        meta["reply_to"] = reply_to
    if peer_id is not None:
        meta["peer_id"] = peer_id
    return meta


def _telegram_headers(deps: TelegramOutboundDeps) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if deps.tg_worker_token:
        headers["X-Auth-Token"] = deps.tg_worker_token
    headers["X-Admin-Token"] = deps.admin_token
    return headers


def _telegram_retry_unknown(has_attachments: bool) -> bool:
    retry_unknown = (os.getenv("TG_SEND_RETRY_ON_UNKNOWN") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return retry_unknown or has_attachments


def _log_telegram_send_payload(state: TelegramSendState) -> None:
    payload_preview = {
        "tenant": state.tenant_id,
        "tg_slot": state.tg_slot,
        "send_tenant": state.send_tenant_id,
        "peer": state.peer_hint,
        "text": state.text_value,
        "has_attachments": bool(state.normalized_attachments),
        "meta": state.meta,
    }
    state.deps.log_fn(f"[worker] telegram send target send_target={state.target}")
    state.deps.log_fn(
        f"[worker] telegram send payload={json.dumps(payload_preview, ensure_ascii=False)}"
    )


async def _send_telegram_attempts(state: TelegramSendState) -> None:
    for attempt in range(3):
        timeout = _telegram_send_timeout(bool(state.normalized_attachments))
        state.deps.log_fn(
            "[worker] telegram send attempt=%s timeout=%.1f has_attachments=%s"
            % (attempt + 1, timeout, bool(state.normalized_attachments))
        )
        state.last_status, state.last_body = await state.deps.telegram_transport_module.send(
            tenant=state.send_tenant_id,
            text=state.text_value,
            peer=state.peer_hint,
            attachments=state.normalized_attachments or None,
            meta=state.meta or None,
            headers=state.headers,
            lead_id=state.lead_id,
            timeout=timeout,
        )
        if 200 <= state.last_status < 300:
            state.deps.message_out_counter.labels("telegram", "success").inc()
            break
        if await _handle_telegram_send_failure(state, attempt):
            continue
        break


def _telegram_send_timeout(has_attachments: bool) -> float:
    if has_attachments:
        return float(os.getenv("TG_SEND_ATTACH_TIMEOUT", "300") or 300.0)
    return float(os.getenv("TG_SEND_TEXT_TIMEOUT", "40") or 40.0)


async def _handle_telegram_send_failure(state: TelegramSendState, attempt: int) -> bool:
    parsed_error, forbidden_peer = _parse_telegram_error(state)
    if state.last_status in {401, 403}:
        return await _handle_telegram_auth_failure(state, parsed_error, forbidden_peer)
    if state.last_status == 422:
        state.last_error = parsed_error or "validation_error"
        return False
    if state.last_status == 0:
        if state.retry_unknown:
            await _telegram_network_retry(state, attempt)
            return True
        state.last_error = parsed_error or "network_unknown"
        return False
    if state.last_status == 429 or state.last_status >= 500:
        await _telegram_network_retry(state, attempt)
        return True
    state.last_error = parsed_error
    return False


def _parse_telegram_error(state: TelegramSendState) -> tuple[Optional[str], bool]:
    try:
        parsed = json.loads(state.last_body) if state.last_body else {}
    except json.JSONDecodeError:
        parsed = {}
    if not isinstance(parsed, dict):
        return None, False
    parsed_error = str(parsed.get("error")) if parsed.get("error") else None
    forbidden_peer = parsed_error == "forbidden_peer_type"
    if parsed_error == "send_failed":
        _log_telegram_send_failed(state, parsed)
    return parsed_error, forbidden_peer


def _log_telegram_send_failed(state: TelegramSendState, parsed: Dict[str, Any]) -> None:
    details = parsed.get("details")
    error_type = ""
    state.peer_hint = state.peer_id
    if isinstance(details, dict):
        error_type = str(details.get("type") or "")
        if details.get("peer_id") is not None:
            state.peer_hint = details.get("peer_id")
    state.deps.log_fn(
        f"[worker] telegram send_failed error_type={error_type or 'unknown'} "
        f"peer_id={state.peer_hint or state.username or state.target}"
    )


async def _handle_telegram_auth_failure(
    state: TelegramSendState,
    parsed_error: Optional[str],
    forbidden_peer: bool,
) -> bool:
    if forbidden_peer:
        state.last_error = parsed_error or "forbidden_peer_type"
        state.deps.log_fn(
            f"[worker] telegram unauthorized_peer peer={state.peer_id or state.username or state.target}"
        )
        return False
    if state.unauthorized_checked:
        return False
    authorized = await state.deps.wait_until_authorized_fn(int(state.send_tenant_id))
    state.unauthorized_checked = True
    if authorized:
        return True
    state.last_error = parsed_error or "not_authorized"
    return False


async def _telegram_network_retry(state: TelegramSendState, attempt: int) -> None:
    delay = min(2**attempt, 8.0)
    state.deps.log_fn(
        f"[worker] telegram network_retry attempt={attempt + 1} status={state.last_status} delay={delay}"
    )
    await state.deps.sleep_fn(delay)


async def _maybe_send_telegram_text_fallback(
    state: TelegramSendState,
) -> tuple[int, str] | None:
    if not _should_send_telegram_fallback(state):
        return None
    fallback_timeout = float(os.getenv("TG_SEND_TEXT_TIMEOUT", "40") or 40.0)
    state.deps.log_fn(
        "[worker] telegram attachment_send_failed fallback=text_only "
        f"status={state.last_status} timeout={fallback_timeout}"
    )
    fb_status, fb_body = await state.deps.telegram_transport_module.send(
        tenant=state.send_tenant_id,
        text=state.text_value,
        peer=state.peer_hint,
        attachments=None,
        meta=state.meta or None,
        headers=state.headers,
        lead_id=state.lead_id,
        timeout=fallback_timeout,
    )
    state.deps.log_fn(f"[worker] telegram fallback response status={fb_status} body={fb_body[:400]}")
    if 200 <= fb_status < 300:
        return fb_status, fb_body
    return None


def _should_send_telegram_fallback(state: TelegramSendState) -> bool:
    return (
        not (200 <= state.last_status < 300)
        and bool(state.normalized_attachments)
        and bool(state.text_value)
        and '"missing_peer"' not in str(state.last_body or "")
    )
