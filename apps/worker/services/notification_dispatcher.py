from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

import httpx

from libs.core.services import notifications as notification_service
from libs.core.transport import telegram as telegram_transport


@dataclass(frozen=True)
class NotificationDispatcherDeps:
    default_tenant_id: int
    admin_token: str
    notify_bot_enabled: bool
    log_fn: Callable[..., None]
    notification_chat_ids_fn: Callable[[int, str], list[int]]
    send_notify_bot_fn: Callable[[int, str], Awaitable[tuple[bool, int, str]]]


async def send_notify_bot(
    chat_id: int,
    text: str,
    *,
    token: str,
    httpx_module: Any = httpx,
) -> tuple[bool, int, str]:
    if not token:
        return False, 0, "notify_bot_token_missing"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": int(chat_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    status, error = await _post_notify_bot(httpx_module, url, payload)
    if 200 <= status < 300:
        return True, status, ""
    return False, status, error or "send_failed"


async def process_notification(
    item: Mapping[str, Any],
    *,
    deps: NotificationDispatcherDeps,
) -> None:
    event_name = str(item.get("event") or "notify").strip() or "notify"
    tenant_hint = _coerce_int(item.get("tenant_id") or item.get("tenant")) or deps.default_tenant_id
    configured_chat_ids = deps.notification_chat_ids_fn(tenant_hint, event_name)
    context = notification_service.build_notification_context(
        item,
        default_tenant_id=deps.default_tenant_id,
        configured_chat_ids=configured_chat_ids,
    )
    if not context.has_text:
        deps.log_fn(
            f"event=notify_skip reason=empty_text tenant={context.tenant_id} "
            f"lead_id={context.lead_id} event={context.event_name}"
        )
        return
    if not context.has_targets:
        deps.log_fn(
            f"event=notify_skip reason=missing_chat_ids tenant={context.tenant_id} "
            f"lead_id={context.lead_id} event={context.event_name}"
        )
        return

    deps.log_fn(
        f"event=notify_dispatch tenant={context.tenant_id} lead_id={context.lead_id} "
        f"event={context.event_name} chat_ids={context.chat_ids}"
    )
    for chat_id in context.chat_ids:
        try:
            target = int(chat_id)
        except Exception:
            continue
        deps.log_fn(
            f"event=notify_send_attempt tenant={context.tenant_id} lead_id={context.lead_id} "
            f"event={context.event_name} chat_id={target}"
        )
        send_ok = False
        send_status = 0
        send_error = ""
        if deps.notify_bot_enabled:
            send_ok, send_status, send_error = await deps.send_notify_bot_fn(target, context.text)
            if not send_ok:
                _log_notify_send_failed(deps, context, target, send_status, send_error)
                continue
        if not send_ok:
            headers = {}
            if deps.admin_token:
                headers["X-Admin-Token"] = deps.admin_token
            status_code, body_text = await telegram_transport.send(
                tenant=context.tenant_id,
                peer=str(target),
                text=context.text,
                headers=headers or None,
            )
            send_status = status_code
            if status_code == 200:
                send_ok = True
            else:
                send_error = body_text
        if send_ok:
            deps.log_fn(
                f"event=notify_send_success tenant={context.tenant_id} "
                f"lead_id={context.lead_id} event={context.event_name} "
                f"chat_id={target} status={send_status}"
            )
            continue
        _log_notify_send_failed(deps, context, target, send_status, send_error)


def _coerce_int(value: Any) -> int | None:
    try:
        result = int(value)
    except Exception:
        return None
    return result


def _log_notify_send_failed(
    deps: NotificationDispatcherDeps,
    context: Any,
    target: int,
    send_status: int,
    send_error: str,
) -> None:
    deps.log_fn(
        "event=notify_send_failed tenant=%s lead_id=%s event=%s chat_id=%s status=%s error=%s"
        % (
            context.tenant_id,
            context.lead_id,
            context.event_name,
            target,
            send_status,
            send_error or "-",
        )
    )


async def _post_notify_bot(
    httpx_module: Any,
    url: str,
    payload: Mapping[str, Any],
) -> tuple[int, str]:
    try:
        async with httpx_module.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json={k: v for k, v in payload.items() if v is not None})
    except httpx_module.HTTPError as exc:
        return 0, str(exc)
    if 200 <= resp.status_code < 300:
        return resp.status_code, ""
    try:
        data = resp.json()
        err = data.get("description") or data.get("error") or resp.text
    except Exception:
        err = resp.text
    return resp.status_code, err or "send_failed"


__all__ = ["NotificationDispatcherDeps", "process_notification", "send_notify_bot"]
