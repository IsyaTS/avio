"""Telegram worker transport helpers."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Tuple

import httpx

try:  # pragma: no cover - optional during bootstrap
    from app.core import settings as core_settings  # type: ignore
except Exception:  # pragma: no cover - fallback when settings unavailable
    core_settings = None  # type: ignore[assignment]

logger = logging.getLogger("app.providers.telegram_bot")

_DEFAULT_TIMEOUT = float(os.getenv("TGWORKER_TIMEOUT", "10"))
_DEFAULT_WORKER_BASE = (
    getattr(core_settings, "DEFAULT_WORKER_BASE_URL", "http://worker:8000")
    if core_settings is not None
    else "http://worker:8000"
)

_client_lock = asyncio.Lock()
_client: httpx.AsyncClient | None = None


def _resolve_base_url() -> str:
    """Resolve TG worker base URL without enforcing Bot API tokens."""

    for env_key in ("WORKER_BASE_URL", "TGWORKER_BASE_URL", "TG_WORKER_URL", "TGWORKER_URL"):
        raw = os.getenv(env_key)
        if raw:
            candidate = raw.strip()
            if candidate:
                return candidate.rstrip("/")

    if core_settings is not None:
        candidate = getattr(core_settings, "WORKER_BASE_URL", "") or ""
        if candidate:
            return str(candidate).strip().rstrip("/")

    try:
        from config import tg_worker_url  # type: ignore
    except Exception:  # pragma: no cover - config import may fail in bootstrap
        return ""

    try:
        base = tg_worker_url()
    except Exception:  # pragma: no cover - defensive around dynamic config
        return ""
    resolved = str(base).strip().rstrip("/")
    return resolved or _DEFAULT_WORKER_BASE


def _resolve_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    worker_token = (os.getenv("TG_WORKER_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
    if worker_token:
        headers["X-Auth-Token"] = worker_token

    admin_token = (os.getenv("ADMIN_TOKEN") or "").strip()
    if not admin_token and core_settings is not None:
        admin_token = str(getattr(core_settings, "ADMIN_TOKEN", "") or "").strip()
    if admin_token:
        headers["X-Admin-Token"] = admin_token
    return headers


def is_configured() -> bool:
    """Return True when TG worker URL is configured."""

    return bool(_resolve_base_url())


async def _get_client() -> httpx.AsyncClient:
    global _client
    async with _client_lock:
        if _client is None or _client.is_closed:
            timeout = httpx.Timeout(_DEFAULT_TIMEOUT)
            _client = httpx.AsyncClient(timeout=timeout)
    return _client


async def send_message(
    *,
    tenant_id: int,
    telegram_user_id: int,
    text: str,
) -> Tuple[bool, int, str]:
    """Send message via TG worker service.

    Returns tuple of (success, status_code, error_message).
    """

    if tenant_id <= 0:
        logger.warning("event=tgworker_send_skip reason=invalid_tenant tenant=%s", tenant_id)
        return False, 0, "invalid_tenant"

    base_url = _resolve_base_url()
    if not base_url:
        logger.warning("event=tgworker_send_skip reason=missing_base_url")
        return False, 0, "tgworker_base_url_missing"

    payload: dict[str, Any] = {
        "tenant": int(tenant_id),
        "channel": "telegram",
        "to": int(telegram_user_id),
    }
    text_value = (text or "").strip()
    if text_value:
        payload["text"] = text_value

    headers = _resolve_headers()
    send_url = f"{base_url}/send"

    try:
        client = await _get_client()
        response = await client.post(send_url, json=payload, headers=headers)
    except httpx.HTTPError as exc:
        logger.error("event=tgworker_send_error error=%s", exc)
        return False, 0, str(exc)

    status_code = response.status_code
    if 200 <= status_code < 300:
        return True, status_code, ""

    error_text = ""
    try:
        body = response.json()
    except Exception:
        body = None

    if isinstance(body, dict):
        raw_error = body.get("error")
        if isinstance(raw_error, str) and raw_error:
            error_text = raw_error
        elif raw_error is not None:
            error_text = str(raw_error)
        elif body.get("details"):
            error_text = str(body.get("details"))

    if not error_text:
        error_text = response.text or "send_failed"

    logger.warning(
        "event=tgworker_send_fail status=%s error=%s tenant=%s chat_id=%s",
        status_code,
        error_text,
        tenant_id,
        telegram_user_id,
    )
    return False, status_code, error_text


async def aclose() -> None:
    global _client
    async with _client_lock:
        client = _client
        _client = None
    if client is not None and not client.is_closed:
        await client.aclose()


__all__ = [
    "send_message",
    "is_configured",
    "aclose",
]
