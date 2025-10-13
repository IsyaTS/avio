from __future__ import annotations

import os
from typing import Any, Mapping

import httpx

from config import tg_worker_url

_MAX_TIMEOUT = 5.0


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


def _http_timeout(value: float | httpx.Timeout | None) -> httpx.Timeout | float:
    if isinstance(value, httpx.Timeout):
        connect = _clamp_timeout(getattr(value, "connect", None))
        read = _clamp_timeout(getattr(value, "read", None))
        write = _clamp_timeout(getattr(value, "write", None))
        pool = _clamp_timeout(getattr(value, "pool", None))
        return httpx.Timeout(
            connect=min(connect, 3.0),
            read=min(read, _MAX_TIMEOUT),
            write=min(write, _MAX_TIMEOUT),
            pool=min(pool, 3.0),
        )
    numeric = _clamp_timeout(value if isinstance(value, (int, float)) else None)
    if numeric == _MAX_TIMEOUT:
        return _MAX_TIMEOUT
    return httpx.Timeout(
        connect=min(3.0, numeric),
        read=numeric,
        write=numeric,
        pool=min(3.0, numeric),
    )


async def tg_post(
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = 5.0,
) -> httpx.Response:
    async with httpx.AsyncClient(timeout=_http_timeout(timeout)) as client:
        return await client.post(_resolve_url(path), json=payload)


async def tg_get(
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = 5.0,
) -> httpx.Response:
    params = None if payload is None else dict(payload)
    async with httpx.AsyncClient(timeout=_http_timeout(timeout)) as client:
        return await client.get(_resolve_url(path), params=params)


__all__ = ["tg_post", "tg_get"]
