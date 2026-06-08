from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class MaxPublicDeps:
    authorize_fn: AsyncFn
    max_integration: Any
    logger: Any
    public_url_fn: SyncFn
    secrets_module: Any
    time_module: Any


@dataclass(frozen=True)
class MaxPersonalDeps:
    authorize_fn: AsyncFn
    service: Any
    transport: Any
    refresh_status_fn: AsyncFn
    callback_url_fn: SyncFn


def max_webhook_url(request: Request, tenant_id: int, secret: str, *, public_url_fn: SyncFn) -> str:
    token_param = secret.strip()
    tail = f"/webhook/max?tenant={int(tenant_id)}"
    if token_param:
        tail = f"{tail}&token={token_param}"
    return public_url_fn(request, tail)


def max_personal_callback_url(
    request: Request,
    tenant_id: int,
    secret: str,
    *,
    public_url_fn: SyncFn,
) -> str:
    token_param = secret.strip()
    tail = f"/webhook/max_personal?tenant={int(tenant_id)}"
    if token_param:
        tail = f"{tail}&token={token_param}"
    return public_url_fn(request, tail)


async def max_status(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPublicDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    integration = deps.max_integration.get_integration(int(tenant_id)) or {}
    token = str(integration.get("bot_token") or integration.get("token") or "").strip()
    secret = str(integration.get("webhook_secret") or "").strip()
    webhook_url = (
        max_webhook_url(request, int(tenant_id), secret, public_url_fn=deps.public_url_fn)
        if secret
        else ""
    )
    return JSONResponse(
        {
            "connected": bool(token),
            "webhook_url": webhook_url,
            "webhook_secret_set": bool(secret),
            "webhook_registered": bool(integration.get("webhook_registered")),
        }
    )


async def max_connect(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPublicDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    payload = await _json_payload(request)
    token = _max_token_from_payload(payload)
    if not token:
        return JSONResponse({"detail": "token_required"}, status_code=400)

    integration = deps.max_integration.get_integration(int(tenant_id)) or {}
    secret = str(integration.get("webhook_secret") or "").strip()
    if not secret:
        secret = deps.secrets_module.token_urlsafe(18)

    deps.max_integration.update_integration(
        int(tenant_id),
        {
            "bot_token": token,
            "webhook_secret": secret,
            "connected_at": int(deps.time_module.time()),
        },
    )

    webhook_url = max_webhook_url(request, int(tenant_id), secret, public_url_fn=deps.public_url_fn)
    webhook_ok = await _ensure_max_webhook(tenant_id, webhook_url, deps)
    _store_max_webhook_state(tenant_id, webhook_ok, deps)
    return JSONResponse(
        {
            "ok": True,
            "connected": True,
            "webhook_url": webhook_url,
            "webhook_ok": bool(webhook_ok),
        }
    )


async def max_disconnect(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPublicDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    integration = deps.max_integration.get_integration(int(tenant_id)) or {}
    secret = str(integration.get("webhook_secret") or "").strip()
    webhook_url = (
        max_webhook_url(request, int(tenant_id), secret, public_url_fn=deps.public_url_fn)
        if secret
        else ""
    )
    if webhook_url:
        try:
            await deps.max_integration.delete_webhook(int(tenant_id), webhook_url)
        except Exception as exc:
            deps.logger.warning("max_webhook_delete_failed tenant=%s error=%s", tenant_id, exc)
    deps.max_integration.update_integration(
        int(tenant_id),
        {
            "bot_token": None,
            "token": None,
            "webhook_registered": False,
            "webhook_registered_at": None,
            "webhook_secret": None,
        },
    )
    return JSONResponse({"ok": True})


async def refresh_max_personal_status(tenant_id: int, deps: MaxPersonalDeps) -> dict[str, Any] | None:
    status_code, payload = await deps.transport.get_status(int(tenant_id))
    if not (200 <= status_code < 300) or not isinstance(payload, Mapping):
        return None
    status_value = str(payload.get("status") or "idle")
    account_raw = payload.get("account")
    account_payload = dict(account_raw) if isinstance(account_raw, Mapping) else {}
    deps.service.update_integration(
        int(tenant_id),
        {
            "session_status": status_value,
            "session_last_error": payload.get("last_error"),
            "account": account_payload,
            "last_heartbeat": payload.get("last_heartbeat"),
        },
    )
    return dict(payload)


async def max_personal_status(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPersonalDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    refreshed = await deps.refresh_status_fn(int(tenant_id))
    payload = deps.service.build_state_payload(int(tenant_id), refreshed)
    if refreshed and isinstance(refreshed, Mapping):
        payload["worker"] = dict(refreshed)
        payload["qr_required"] = str(refreshed.get("status") or "") in _QR_REQUIRED_STATUSES
    else:
        payload["worker"] = None
        payload["qr_required"] = payload["status"] in _QR_REQUIRED_STATUSES
    return JSONResponse(payload)


async def max_personal_connect(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPersonalDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    payload = await _json_payload(request)
    force = bool(payload.get("force", False))

    event_secret = deps.service.ensure_event_secret(int(tenant_id))
    callback_url = deps.callback_url_fn(request, int(tenant_id), event_secret)
    status_code, upstream = await deps.transport.start_session(
        int(tenant_id),
        callback_url=callback_url,
        webhook_token=event_secret,
        force=force,
    )
    if not (200 <= status_code < 300):
        detail = upstream.get("error") if isinstance(upstream, Mapping) else "max_personal_unavailable"
        return JSONResponse({"detail": detail or "max_personal_unavailable"}, status_code=502)

    deps.service.update_integration(
        int(tenant_id),
        {
            "enabled": True,
            "outbound_enabled": True,
            "session_status": (upstream or {}).get("status"),
            "session_last_error": (upstream or {}).get("last_error"),
            "account": (upstream or {}).get("account") if isinstance(upstream, Mapping) else {},
        },
    )
    response_payload = deps.service.build_state_payload(
        int(tenant_id),
        upstream if isinstance(upstream, Mapping) else None,
    )
    response_payload["worker"] = upstream
    response_payload["callback_url"] = callback_url
    return JSONResponse(response_payload)


async def max_personal_session_qr(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPersonalDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    status_code, payload = await deps.transport.get_qr(int(tenant_id))
    if status_code == 404:
        return JSONResponse({"detail": "qr_not_available"}, status_code=404)
    if status_code == 410:
        detail = payload.get("error") if isinstance(payload, Mapping) else "qr_expired"
        status = (payload or {}).get("status") if isinstance(payload, Mapping) else None
        return JSONResponse({"detail": detail or "qr_expired", "status": status}, status_code=410)
    if not (200 <= status_code < 300):
        detail = payload.get("error") if isinstance(payload, Mapping) else "max_personal_unavailable"
        return JSONResponse({"detail": detail or "max_personal_unavailable"}, status_code=502)
    return JSONResponse(dict(payload) if isinstance(payload, Mapping) else {"ok": True})


async def max_personal_session_logout(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPersonalDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    status_code, payload = await deps.transport.logout_session(int(tenant_id))
    if not (200 <= status_code < 300):
        detail = payload.get("error") if isinstance(payload, Mapping) else "max_personal_unavailable"
        return JSONResponse({"detail": detail or "max_personal_unavailable"}, status_code=502)
    deps.service.update_integration(
        int(tenant_id),
        {
            "session_status": "disconnected",
            "session_last_error": None,
            "account": {},
        },
    )
    return JSONResponse({"ok": True, "status": "disconnected"})


async def max_personal_disconnect(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPersonalDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    await deps.transport.logout_session(int(tenant_id))
    deps.service.update_integration(
        int(tenant_id),
        {
            "enabled": False,
            "outbound_enabled": False,
            "session_status": "disconnected",
            "session_last_error": None,
            "account": {},
        },
    )
    return JSONResponse({"ok": True, "enabled": False})


async def max_personal_send(
    request: Request,
    tenant: int | None,
    key: str | None,
    deps: MaxPersonalDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    payload = await _json_payload(request)
    to_value = payload.get("to") or payload.get("chat_id") or payload.get("peer")
    text_value = str(payload.get("text") or "").strip()
    if not to_value:
        return JSONResponse({"detail": "to_required"}, status_code=400)
    if not text_value:
        return JSONResponse({"detail": "text_required"}, status_code=400)
    status_code, upstream = await deps.transport.send_message(
        int(tenant_id),
        chat_id=to_value,
        text=text_value,
        dedupe_key=str(payload.get("dedupe_key") or ""),
        idempotency_key=str(payload.get("idempotency_key") or ""),
    )
    if not (200 <= status_code < 300):
        detail = upstream.get("error") if isinstance(upstream, Mapping) else "send_failed"
        return JSONResponse({"detail": detail or "send_failed"}, status_code=502)
    return JSONResponse(dict(upstream) if isinstance(upstream, Mapping) else {"ok": True})


async def _json_payload(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _max_token_from_payload(payload: Mapping[str, Any]) -> str:
    raw_token = payload.get("token") or payload.get("bot_token") or payload.get("access_token") or ""
    return str(raw_token or "").strip()


async def _ensure_max_webhook(tenant_id: int, webhook_url: str, deps: MaxPublicDeps) -> bool:
    try:
        return bool(await deps.max_integration.ensure_webhook(int(tenant_id), webhook_url))
    except Exception as exc:
        deps.logger.warning("max_webhook_register_failed tenant=%s error=%s", tenant_id, exc)
        return False


def _store_max_webhook_state(tenant_id: int, webhook_ok: bool, deps: MaxPublicDeps) -> None:
    try:
        deps.max_integration.update_integration(
            int(tenant_id),
            {
                "webhook_registered": bool(webhook_ok),
                "webhook_registered_at": int(deps.time_module.time()) if webhook_ok else None,
            },
        )
    except Exception:
        deps.logger.exception("max_webhook_state_update_failed tenant=%s", tenant_id)


_QR_REQUIRED_STATUSES = {"waiting_qr", "authorizing", "reauth_required"}
