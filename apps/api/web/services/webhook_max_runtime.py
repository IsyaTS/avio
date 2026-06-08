from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import HTTPException, Request


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class MaxWebhookDeps:
    json_module: Any
    extract_token_fn: SyncFn
    max_integration_module: Any
    max_personal_service_module: Any
    process_incoming_fn: AsyncFn
    getenv_fn: SyncFn
    logger: Any


async def max_webhook(request: Request, *, deps: MaxWebhookDeps) -> Any:
    token = deps.extract_token_fn(request)
    payload = await _request_json(request, deps=deps)
    tenant = _tenant_from_query_or_payload(request, payload)
    integration = deps.max_integration_module.get_integration(int(tenant)) or {}
    secret_expected = str(integration.get("webhook_secret") or "").strip()
    if secret_expected and token != secret_expected:
        raise HTTPException(status_code=401, detail="unauthorized")
    message = _normalize_max_message(payload)
    body = {
        "source": {"type": "max", "tenant": tenant},
        "message": message,
        "provider_raw": payload,
        "max": payload,
    }
    _copy_flags(payload, body, ("manager", "out", "outgoing"))
    return await deps.process_incoming_fn(body, request)


async def max_personal_webhook(request: Request, *, deps: MaxWebhookDeps) -> Any:
    query_token = (request.query_params.get("token") or "").strip()
    token_candidates = _token_candidates(request, query_token)
    payload = await _request_json(request, deps=deps)
    tenant = _tenant_from_query_or_payload(request, payload)

    integration = deps.max_personal_service_module.get_integration(int(tenant)) or {}
    expected_secret = str(integration.get("event_secret") or "").strip()
    worker_token = (
        deps.getenv_fn("MAX_PERSONAL_WEBHOOK_TOKEN")
        or deps.max_personal_service_module.max_personal_worker_token()
    )
    worker_token = str(worker_token or "").strip()
    if not _authorized_max_personal(expected_secret, worker_token, token_candidates):
        raise HTTPException(status_code=401, detail="unauthorized")
    _bind_missing_max_personal_secret(
        tenant=tenant,
        expected_secret=expected_secret,
        worker_token=worker_token,
        query_token=query_token,
        token_candidates=token_candidates,
        deps=deps,
    )
    message = _normalize_max_personal_message(payload)
    body = {
        "source": {"type": "max_personal", "tenant": tenant},
        "message": message,
        "provider_raw": payload,
        "max_personal": payload,
    }
    _copy_flags(payload, body, ("manager", "out", "outgoing", "origin"))
    return await deps.process_incoming_fn(body, request)


async def _request_json(request: Request, *, deps: MaxWebhookDeps) -> dict[str, Any]:
    try:
        payload = await request.json()
    except deps.json_module.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid_json")
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_payload")
    return payload if isinstance(payload, dict) else {}


def _tenant_from_query_or_payload(request: Request, payload: Mapping[str, Any]) -> int:
    tenant_raw = (
        request.query_params.get("tenant") or payload.get("tenant") or payload.get("tenant_id")
    )
    try:
        tenant = int(tenant_raw) if tenant_raw is not None else 0
    except Exception:
        tenant = 0
    if tenant <= 0:
        raise HTTPException(status_code=400, detail="invalid_tenant")
    return tenant


def _normalize_max_message(payload: Mapping[str, Any]) -> dict[str, Any]:
    msg_raw = payload.get("message") or payload.get("data") or payload
    message = dict(msg_raw) if isinstance(msg_raw, Mapping) else {}
    text_value = message.get("text") or message.get("body") or payload.get("text") or ""
    if not isinstance(text_value, str):
        text_value = str(text_value)
    message["text"] = text_value.strip()
    _copy_message_id(payload, message)
    user_id = _mapping_value(message, "from", "id") if isinstance(message.get("from"), Mapping) else None
    user_id = message.get("user_id") or message.get("from_id") or user_id or payload.get("user_id")
    if user_id is not None:
        message.setdefault("max_user_id", user_id)
    username = _mapping_value(message, "from", "username") if isinstance(message.get("from"), Mapping) else None
    username = message.get("username") or username or payload.get("username")
    if username:
        message.setdefault("max_username", username)
    display_name = _mapping_value(message, "from", "name") if isinstance(message.get("from"), Mapping) else None
    display_name = message.get("display_name") or message.get("name") or display_name
    if display_name:
        message.setdefault("display_name", display_name)
    chat_id = _mapping_value(message, "chat", "id") if isinstance(message.get("chat"), Mapping) else None
    chat_id = message.get("chat_id") or chat_id or payload.get("chat_id")
    if chat_id is not None and "peer" not in message:
        message["peer"] = chat_id
    _copy_attachments(payload, message)
    return message


def _normalize_max_personal_message(payload: Mapping[str, Any]) -> dict[str, Any]:
    msg_raw = payload.get("message") or payload.get("data") or payload
    message = dict(msg_raw) if isinstance(msg_raw, Mapping) else {}
    text_value = (
        _coerce_max_personal_text(message.get("text"))
        or _coerce_max_personal_text(message.get("body"))
        or _coerce_max_personal_text(payload.get("text"))
    )
    if not isinstance(text_value, str):
        text_value = str(text_value)
    message["text"] = text_value.strip()
    _copy_message_id(payload, message)
    user_id = (
        message.get("max_user_id")
        or message.get("user_id")
        or message.get("from_id")
        or (_mapping_value(message, "from", "id") if isinstance(message.get("from"), Mapping) else None)
        or payload.get("max_user_id")
    )
    if user_id is not None:
        message.setdefault("max_user_id", user_id)
    username = (
        message.get("max_username")
        or message.get("username")
        or (_mapping_value(message, "from", "username") if isinstance(message.get("from"), Mapping) else None)
        or payload.get("username")
    )
    if username:
        message.setdefault("max_username", username)
    display_name = (
        message.get("display_name")
        or message.get("name")
        or (_mapping_value(message, "from", "name") if isinstance(message.get("from"), Mapping) else None)
        or payload.get("display_name")
    )
    if display_name:
        message.setdefault("display_name", display_name)
    chat_id = (
        message.get("chat_id")
        or message.get("peer")
        or message.get("peer_id")
        or (_mapping_value(message, "chat", "id") if isinstance(message.get("chat"), Mapping) else None)
        or payload.get("chat_id")
    )
    if chat_id is not None:
        message.setdefault("chat_id", chat_id)
        message.setdefault("peer", chat_id)
    _copy_attachments(payload, message)
    return message


def _copy_message_id(payload: Mapping[str, Any], message: dict[str, Any]) -> None:
    message_id = (
        message.get("message_id")
        or message.get("id")
        or payload.get("message_id")
        or payload.get("id")
    )
    if message_id is not None:
        message["message_id"] = message_id


def _copy_attachments(payload: Mapping[str, Any], message: dict[str, Any]) -> None:
    if "attachments" not in message and isinstance(payload.get("attachments"), list):
        message["attachments"] = payload.get("attachments")
    if "attachments" not in message and isinstance(message.get("media"), list):
        message["attachments"] = message.get("media")


def _mapping_value(source: Mapping[str, Any], key: str, nested_key: str) -> Any:
    nested = source.get(key)
    if isinstance(nested, Mapping):
        return nested.get(nested_key)
    return None


def _copy_flags(payload: Mapping[str, Any], body: dict[str, Any], keys: tuple[str, ...]) -> None:
    for flag_key in keys:
        if payload.get(flag_key) is not None:
            body[flag_key] = payload.get(flag_key)


def _coerce_max_personal_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("text", "body", "message", "caption"):
            nested = value.get(key)
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
    return ""


def _token_candidates(request: Request, query_token: str) -> list[str]:
    headers = getattr(request, "headers", {}) or {}
    header_tokens = []
    for key in ("X-Webhook-Token", "X-Auth-Token", "Authorization"):
        raw = str(headers.get(key) or "").strip()
        if not raw:
            continue
        if key == "Authorization" and raw.lower().startswith("bearer "):
            raw = raw[7:].strip()
        if raw:
            header_tokens.append(raw)
    token_candidates = []
    if query_token:
        token_candidates.append(query_token)
    token_candidates.extend(header_tokens)
    return token_candidates


def _authorized_max_personal(
    expected_secret: str,
    worker_token: str,
    token_candidates: list[str],
) -> bool:
    if expected_secret and expected_secret in token_candidates:
        return True
    return bool(worker_token and worker_token in token_candidates)


def _bind_missing_max_personal_secret(
    *,
    tenant: int,
    expected_secret: str,
    worker_token: str,
    query_token: str,
    token_candidates: list[str],
    deps: MaxWebhookDeps,
) -> None:
    if expected_secret or not worker_token or worker_token not in token_candidates or not query_token:
        return
    try:
        deps.max_personal_service_module.update_integration(int(tenant), {"event_secret": query_token})
    except Exception:
        deps.logger.exception("max_personal_webhook_secret_bind_failed tenant=%s", tenant)
