from __future__ import annotations

import os
from typing import Any, Mapping

import httpx

_DEFAULT_BASE = "http://tgworker:8085"


class TGWorkerError(Exception):
    """Base error for Telegram worker HTTP helpers."""


class TGWorkerConnectionError(TGWorkerError):
    """Raised when the Telegram worker cannot be reached."""


def _base_url() -> str:
    raw = os.getenv("TGWORKER_URL") or os.getenv("TG_WORKER_URL")
    candidate = (raw or _DEFAULT_BASE).strip()
    return candidate.rstrip("/") or _DEFAULT_BASE


def _resolve_url(path: str) -> str:
    if not path:
        return _base_url()
    lowered = path.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{_base_url()}{path}"


def _http_timeout(value: float | httpx.Timeout | None) -> httpx.Timeout:
    if isinstance(value, httpx.Timeout):
        return value
    total = float(value) if value is not None else 5.0
    connect = min(3.0, total)
    return httpx.Timeout(total=total, connect=connect, read=total)


async def tg_post(
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = 5.0,
) -> httpx.Response:
    try:
        async with httpx.AsyncClient(timeout=_http_timeout(timeout)) as client:
            return await client.post(_resolve_url(path), json=payload)
    except httpx.RequestError as exc:  # pragma: no cover - network errors
        raise TGWorkerConnectionError(str(exc)) from exc


async def tg_get(
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = 5.0,
) -> httpx.Response:
    params = None if payload is None else dict(payload)
    try:
        async with httpx.AsyncClient(timeout=_http_timeout(timeout)) as client:
            return await client.get(_resolve_url(path), params=params)
    except httpx.RequestError as exc:  # pragma: no cover - network errors
        raise TGWorkerConnectionError(str(exc)) from exc


__all__ = ["tg_post", "tg_get", "TGWorkerError", "TGWorkerConnectionError"]
