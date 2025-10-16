"""Telegram bot transport helpers."""

from __future__ import annotations

"""Telegram bot transport helpers."""

import asyncio
import logging
import os
from typing import Any, Tuple

import httpx

logger = logging.getLogger("app.providers.telegram_bot")

_DEFAULT_TIMEOUT = float(os.getenv("TELEGRAM_BOT_TIMEOUT", "10"))
_API_BASE_URL = (os.getenv("TELEGRAM_API_BASE_URL") or "https://api.telegram.org").rstrip("/")

_client_lock = asyncio.Lock()
_client: httpx.AsyncClient | None = None


def _bot_token() -> str:
    return (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()


def is_configured() -> bool:
    return bool(_bot_token())


async def _get_client() -> httpx.AsyncClient:
    global _client
    async with _client_lock:
        if _client is None or _client.is_closed:
            timeout = httpx.Timeout(_DEFAULT_TIMEOUT)
            _client = httpx.AsyncClient(timeout=timeout)
    return _client


def _build_send_url(token: str) -> str:
    return f"{_API_BASE_URL}/bot{token}/sendMessage"


async def send_message(
    telegram_user_id: int,
    text: str,
    *,
    disable_web_page_preview: bool = True,
) -> Tuple[bool, int, str]:
    """Send a message via Telegram Bot API.

    Returns a tuple of (success, status_code, error_message).
    """

    token = _bot_token()
    if not token:
        logger.warning("event=telegram_bot_send_skip reason=missing_token")
        return False, 0, "telegram_bot_token_missing"

    try:
        chat_id = int(telegram_user_id)
    except Exception:
        logger.warning("event=telegram_bot_send_skip reason=invalid_chat_id value=%s", telegram_user_id)
        return False, 0, "invalid_telegram_user_id"

    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
    }
    if disable_web_page_preview:
        payload["disable_web_page_preview"] = True

    url = _build_send_url(token)

    try:
        client = await _get_client()
        response = await client.post(url, json=payload)
    except httpx.HTTPError as exc:
        logger.error("event=telegram_bot_send_error error=%s", exc)
        return False, 0, str(exc)

    status_code = response.status_code
    if status_code != 200:
        body_text = response.text
        logger.warning(
            "event=telegram_bot_send_fail status=%s body=%s", status_code, body_text[:200]
        )
        return False, status_code, body_text

    try:
        body = response.json()
    except Exception:
        logger.warning("event=telegram_bot_send_invalid_json status=%s", status_code)
        return False, status_code, response.text

    if not bool(body.get("ok")):
        description = str(body.get("description") or response.text or "send_failed")
        logger.warning(
            "event=telegram_bot_send_api_error status=%s description=%s", status_code, description
        )
        return False, status_code, description

    return True, status_code, ""


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
