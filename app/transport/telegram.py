from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Mapping

import httpx

from app.db import get_lead_peer

logger = logging.getLogger("app.transport.telegram")

_client_lock = asyncio.Lock()
_client: httpx.AsyncClient | None = None


def _resolve_base_url() -> str:
    raw = os.getenv("TGWORKER_URL") or os.getenv("TG_WORKER_URL") or "http://tgworker:9000"
    base = str(raw).strip() or "http://tgworker:9000"
    return base.rstrip("/") or "http://tgworker:9000"


async def _get_client(timeout: float) -> httpx.AsyncClient:
    global _client
    async with _client_lock:
        if _client is None or _client.is_closed:
            _client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))
    return _client


async def send(
    *,
    tenant: int,
    text: str | None = None,
    peer: str | None = None,
    attachments: list[dict[str, Any]] | None = None,
    meta: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    lead_id: int | None = None,
    timeout: float = 15.0,
) -> tuple[int, str]:
    """Send a Telegram message via worker service.

    Parameters
    ----------
    tenant:
        Tenant identifier used by the worker.
    text:
        Optional message text.
    peer:
        Preferred peer identifier (chat_id or username) as a string.
    attachments:
        Optional list of attachment descriptors already normalized for the worker.
    meta:
        Additional metadata to include in the payload.
    headers:
        Optional HTTP headers; callers are expected to include auth tokens.
    lead_id:
        Lead reference for peer lookups when ``peer`` is absent.
    timeout:
        Request timeout in seconds.

    Returns
    -------
    tuple[int, str]
        Response status code and body text from the worker.
    """

    peer_value = (peer or "").strip()
    if not peer_value and lead_id is not None and lead_id > 0:
        try:
            stored_peer = await get_lead_peer(lead_id, channel="telegram")
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "event=tg_transport_peer_lookup_failed tenant=%s lead_id=%s error=%s",
                tenant,
                lead_id,
                exc,
            )
            stored_peer = None
        if stored_peer:
            peer_value = str(stored_peer).strip()

    if not peer_value:
        # Surface consistent error payload for callers to handle gracefully.
        return 0, json.dumps({"error": "missing_peer"}, ensure_ascii=False)

    payload: dict[str, Any] = {
        "tenant": int(tenant),
        "channel": "telegram",
        "peer": peer_value,
    }

    try:
        to_candidate = int(peer_value)
    except (TypeError, ValueError):
        payload["to"] = peer_value
    else:
        payload["to"] = to_candidate

    text_value = (text or "").strip()
    if text_value:
        payload["text"] = text_value

    if attachments:
        payload["attachments"] = attachments

    if meta:
        payload["meta"] = dict(meta)

    url = f"{_resolve_base_url()}/send"
    request_headers = dict(headers or {})

    try:
        client = await _get_client(timeout)
        response = await client.post(
            url,
            json=payload,
            headers=request_headers,
            timeout=httpx.Timeout(timeout),
        )
    except httpx.HTTPError as exc:  # pragma: no cover - network issues
        logger.warning(
            "event=tg_transport_http_error tenant=%s error=%s",
            tenant,
            exc,
        )
        return 0, str(exc)

    return response.status_code, response.text


async def aclose() -> None:
    global _client
    async with _client_lock:
        client = _client
        _client = None
    if client is not None and not client.is_closed:
        await client.aclose()


__all__ = ["send", "aclose"]
