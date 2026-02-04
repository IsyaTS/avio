"""MAX Bot API helpers (official bot integration)."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Mapping, Optional

import httpx

from libs.core import sales_core as core_module

settings = core_module.settings  # type: ignore[attr-defined]
read_tenant_config = core_module.read_tenant_config  # type: ignore[attr-defined]
write_tenant_config = core_module.write_tenant_config  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)

MAX_API_BASE = (os.getenv("MAX_API_BASE") or "https://platform-api.max.ru").strip().rstrip("/")
MAX_TIMEOUT = float(os.getenv("MAX_TIMEOUT", "10.0") or 10.0)
MAX_TOKEN_HEADER = (os.getenv("MAX_TOKEN_HEADER") or "Authorization").strip() or "Authorization"
MAX_TOKEN_PREFIX = (os.getenv("MAX_TOKEN_PREFIX") or "Bearer").strip()
MAX_WEBHOOK_ENDPOINT = (os.getenv("MAX_WEBHOOK_ENDPOINT") or "/webhook").strip() or "/webhook"
MAX_WEBHOOK_METHOD = (os.getenv("MAX_WEBHOOK_METHOD") or "POST").strip().upper()
MAX_WEBHOOK_DELETE_ENDPOINT = (
    os.getenv("MAX_WEBHOOK_DELETE_ENDPOINT") or MAX_WEBHOOK_ENDPOINT
).strip()
MAX_WEBHOOK_DELETE_METHOD = (os.getenv("MAX_WEBHOOK_DELETE_METHOD") or "DELETE").strip().upper()
MAX_SEND_ENDPOINT = (os.getenv("MAX_SEND_ENDPOINT") or "/messages").strip() or "/messages"
MAX_UPLOAD_ENDPOINT = (os.getenv("MAX_UPLOAD_ENDPOINT") or "").strip()
MAX_ATTACHMENT_MODE = (os.getenv("MAX_ATTACHMENT_MODE") or "url").strip().lower()


def _headers(token: str) -> dict[str, str]:
    if not token:
        return {}
    if MAX_TOKEN_PREFIX:
        auth_value = f"{MAX_TOKEN_PREFIX} {token}".strip()
    else:
        auth_value = token
    return {MAX_TOKEN_HEADER: auth_value}


def get_integration(tenant: int) -> Optional[dict[str, Any]]:
    cfg = read_tenant_config(int(tenant))
    if not isinstance(cfg, Mapping):
        return None
    integrations = cfg.get("integrations")
    if not isinstance(integrations, Mapping):
        return None
    max_cfg = integrations.get("max")
    if isinstance(max_cfg, Mapping):
        return dict(max_cfg)
    return None


def update_integration(tenant: int, data: Mapping[str, Any]) -> dict[str, Any]:
    cfg = read_tenant_config(int(tenant))
    if not isinstance(cfg, dict):
        cfg = {}
    integrations = cfg.setdefault("integrations", {})
    existing = integrations.get("max") if isinstance(integrations.get("max"), Mapping) else {}
    max_cfg = dict(existing)
    max_cfg.update(data)
    integrations["max"] = max_cfg
    write_tenant_config(int(tenant), cfg)
    return max_cfg


def get_token(tenant: int) -> str:
    integration = get_integration(int(tenant)) or {}
    token = integration.get("bot_token") or integration.get("token") or ""
    return str(token).strip()


async def ensure_webhook(tenant: int, url: str) -> bool:
    token = get_token(int(tenant))
    if not token:
        return False
    endpoint = MAX_WEBHOOK_ENDPOINT
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    target = f"{MAX_API_BASE}{endpoint}"
    payload = {"url": url}
    try:
        async with httpx.AsyncClient(timeout=MAX_TIMEOUT) as client:
            response = await client.request(
                MAX_WEBHOOK_METHOD, target, json=payload, headers=_headers(token)
            )
    except httpx.HTTPError as exc:
        logger.warning("max_webhook_register_failed tenant=%s error=%s", tenant, exc)
        return False
    if 200 <= response.status_code < 300:
        return True
    logger.warning(
        "max_webhook_register_failed tenant=%s status=%s body=%s",
        tenant,
        response.status_code,
        response.text[:500],
    )
    return False


async def delete_webhook(tenant: int, url: str) -> bool:
    token = get_token(int(tenant))
    if not token:
        return False
    endpoint = MAX_WEBHOOK_DELETE_ENDPOINT
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    target = f"{MAX_API_BASE}{endpoint}"
    payload = {"url": url}
    try:
        async with httpx.AsyncClient(timeout=MAX_TIMEOUT) as client:
            response = await client.request(
                MAX_WEBHOOK_DELETE_METHOD,
                target,
                json=payload,
                headers=_headers(token),
            )
    except httpx.HTTPError as exc:
        logger.warning("max_webhook_delete_failed tenant=%s error=%s", tenant, exc)
        return False
    if 200 <= response.status_code < 300:
        return True
    logger.warning(
        "max_webhook_delete_failed tenant=%s status=%s body=%s",
        tenant,
        response.status_code,
        response.text[:500],
    )
    return False


async def send_message(
    tenant: int,
    *,
    chat_id: str | int | None,
    user_id: str | int | None,
    text: str | None,
    attachments: list[Mapping[str, Any]] | None = None,
) -> tuple[int, str]:
    token = get_token(int(tenant))
    if not token:
        return 0, "token_missing"
    endpoint = MAX_SEND_ENDPOINT
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    target = f"{MAX_API_BASE}{endpoint}"

    payload: dict[str, Any] = {}
    if chat_id is not None:
        payload["chat_id"] = chat_id
    elif user_id is not None:
        payload["user_id"] = user_id
    if text:
        payload["text"] = text
    if attachments:
        payload["attachments"] = [dict(item) for item in attachments]

    try:
        async with httpx.AsyncClient(timeout=MAX_TIMEOUT) as client:
            response = await client.post(target, json=payload, headers=_headers(token))
    except httpx.HTTPError as exc:
        return 0, str(exc)
    return response.status_code, response.text


async def upload_file(
    tenant: int,
    *,
    filename: str,
    content: bytes,
    mime: str | None = None,
) -> tuple[int, dict[str, Any] | None, str]:
    if not MAX_UPLOAD_ENDPOINT:
        return 0, None, "upload_endpoint_missing"
    token = get_token(int(tenant))
    if not token:
        return 0, None, "token_missing"
    endpoint = MAX_UPLOAD_ENDPOINT
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    target = f"{MAX_API_BASE}{endpoint}"
    files = {"file": (filename, content, mime or "application/octet-stream")}
    try:
        async with httpx.AsyncClient(timeout=MAX_TIMEOUT) as client:
            response = await client.post(target, files=files, headers=_headers(token))
    except httpx.HTTPError as exc:
        return 0, None, str(exc)
    if response.status_code < 200 or response.status_code >= 300:
        return response.status_code, None, response.text
    try:
        data = response.json()
    except Exception:
        data = None
    if isinstance(data, dict):
        return response.status_code, data, ""
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            return response.status_code, parsed, ""
    return response.status_code, None, response.text

