from __future__ import annotations

import os
from typing import Any, Mapping

import httpx

_DEFAULT_BASE = "http://tgworker:8085"


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


async def tg_post(
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = 8.0,
) -> httpx.Response:
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.post(_resolve_url(path), json=payload)


async def tg_get(
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = 8.0,
) -> httpx.Response:
    params = None if payload is None else dict(payload)
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.get(_resolve_url(path), params=params)


__all__ = ["tg_post", "tg_get"]
