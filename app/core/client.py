from __future__ import annotations

import os
from typing import Any, Mapping

import httpx

from config import tg_worker_url

_MAX_TIMEOUT = 5.0


class TGWorkerError(Exception):
    """Base error for Telegram worker HTTP helpers."""


class TGWorkerConnectionError(TGWorkerError):
    """Raised when the Telegram worker cannot be reached."""


def _base_url() -> str:
    raw = os.getenv("TGWORKER_URL") or os.getenv("TG_WORKER_URL")
    if raw:
        candidate = raw.strip()
        if candidate:
            return candidate.rstrip("/")
    return tg_worker_url()


def _resolve_url(path: str) -> str:
    if not path:
        return _base_url()
    lowered = path.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{_base_url()}{path}"


def _clamp_timeout(value: float | None) -> float:
    try:
        numeric = float(value) if value is not None else _MAX_TIMEOUT
    except (TypeError, ValueError):
        numeric = _MAX_TIMEOUT
    if numeric <= 0:
        return _MAX_TIMEOUT
    return min(numeric, _MAX_TIMEOUT)


def _http_timeout(value: float | httpx.Timeout | None) -> httpx.Timeout:
    if isinstance(value, httpx.Timeout):
        total = _clamp_timeout(getattr(value, "total", None))
        connect = _clamp_timeout(getattr(value, "connect", None))
        read = _clamp_timeout(getattr(value, "read", None))
        write = _clamp_timeout(getattr(value, "write", None))
        pool = getattr(value, "pool", None)
        return httpx.Timeout(
            total=total,
            connect=min(connect, total),
            read=min(read, total),
            write=min(write, total),
            pool=pool,
        )
    total = _clamp_timeout(value if isinstance(value, (int, float)) else None)
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
