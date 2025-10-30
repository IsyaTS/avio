from __future__ import annotations

import os
from typing import Any, Mapping

import httpx

try:  # pragma: no cover - fallback for early imports
    from app.core import settings as core_settings  # type: ignore
except Exception:  # pragma: no cover
    core_settings = None  # type: ignore[assignment]

_DEFAULT_TIMEOUT_SECONDS = 5.0
_DEFAULT_WORKER_BASE = (
    getattr(core_settings, "DEFAULT_WORKER_BASE_URL", "http://worker:8000")
    if core_settings is not None
    else "http://worker:8000"
)


def _default_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=3.0,
        read=_DEFAULT_TIMEOUT_SECONDS,
        write=_DEFAULT_TIMEOUT_SECONDS,
        pool=3.0,
    )


def _normalize_timeout(value: float | httpx.Timeout | None) -> float | httpx.Timeout:
    if value is None:
        return _default_timeout()
    if isinstance(value, httpx.Timeout):
        connect = value.connect if value.connect is not None else 3.0
        read = value.read if value.read is not None else _DEFAULT_TIMEOUT_SECONDS
        write = value.write if value.write is not None else _DEFAULT_TIMEOUT_SECONDS
        pool = value.pool if value.pool is not None else 3.0
        return httpx.Timeout(
            connect=min(connect, 3.0),
            read=min(read, _DEFAULT_TIMEOUT_SECONDS),
            write=min(write, _DEFAULT_TIMEOUT_SECONDS),
            pool=min(pool, 3.0),
        )
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = _DEFAULT_TIMEOUT_SECONDS
    if numeric <= 0:
        numeric = _DEFAULT_TIMEOUT_SECONDS
    return min(numeric, _DEFAULT_TIMEOUT_SECONDS)


def _base_url() -> str:
    if core_settings is not None:
        candidate = getattr(core_settings, "WORKER_BASE_URL", "") or ""
        cleaned = str(candidate).strip()
        if cleaned:
            return cleaned.rstrip("/") or _DEFAULT_WORKER_BASE

    for env_key in ("WORKER_BASE_URL", "TGWORKER_BASE_URL", "TG_WORKER_URL", "TGWORKER_URL"):
        raw = os.getenv(env_key)
        if raw:
            cleaned = str(raw).strip()
            if cleaned:
                return cleaned.rstrip("/")

    return _DEFAULT_WORKER_BASE


def _resolve_url(path: str) -> str:
    if not path:
        return _base_url()
    lowered = path.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{_base_url()}{path}"


def _admin_token() -> str:
    if core_settings is not None:
        token = getattr(core_settings, "ADMIN_TOKEN", "") or ""
        if token:
            return token.strip()
    return (os.getenv("ADMIN_TOKEN") or "").strip()


def _client_headers() -> dict[str, str]:
    token = _admin_token()
    if token:
        return {"X-Admin-Token": token}
    return {}


async def tg_post(
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = 5.0,
) -> httpx.Response:
    async with httpx.AsyncClient(
        timeout=_normalize_timeout(timeout), headers=_client_headers()
    ) as client:
        return await client.post(_resolve_url(path), json=payload)


async def tg_get(
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = 5.0,
    stream: bool = False,
) -> httpx.Response:
    params = None if payload is None else dict(payload)
    async with httpx.AsyncClient(
        timeout=_normalize_timeout(timeout), headers=_client_headers()
    ) as client:
        url = _resolve_url(path)
        if stream:
            async with client.stream("GET", url, params=params) as response:
                content = await response.aread()
                return httpx.Response(
                    status_code=response.status_code,
                    headers=response.headers,
                    content=content,
                    request=response.request,
                    extensions=response.extensions,
                )
        return await client.get(url, params=params)


__all__ = ["tg_post", "tg_get"]
