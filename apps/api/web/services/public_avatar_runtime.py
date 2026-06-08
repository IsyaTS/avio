from __future__ import annotations

import hashlib
import html
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

import httpx
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class PublicAvatarDeps:
    get_lead_dialog_metadata_fn: AsyncFn
    resolve_avito_profile_fn: AsyncFn
    no_store_headers_fn: SyncFn
    http_client_cls: Any = httpx.AsyncClient


async def chat_avatar_response(
    *,
    tenant_id: int,
    lead_id: int,
    deps: PublicAvatarDeps,
) -> Response:
    meta = await deps.get_lead_dialog_metadata_fn(lead_id)
    if not meta or int(meta.get("tenant_id") or 0) != int(tenant_id):
        return JSONResponse({"error": "not_found"}, status_code=404, headers=deps.no_store_headers_fn())

    display_name = _display_name(meta, lead_id)
    channel = str(meta.get("channel") or "").strip().lower()
    if channel == "avito":
        upstream = await _fetch_avito_avatar(tenant_id=tenant_id, meta=meta, deps=deps)
        if upstream is not None:
            return upstream
    return _fallback_avatar_response(display_name=display_name, channel=channel, deps=deps)


def avatar_initials(label: str) -> str:
    tokens = [chunk for chunk in re.split(r"\s+", str(label or "").strip()) if chunk]
    if not tokens:
        return "A"
    if len(tokens) == 1:
        value = tokens[0][:2]
    else:
        value = f"{tokens[0][:1]}{tokens[1][:1]}"
    return value.upper()


def avatar_fill(seed: str) -> str:
    palette = (
        "#2563eb",
        "#0f766e",
        "#c2410c",
        "#475569",
        "#7c3aed",
        "#0f172a",
    )
    digest = hashlib.sha1(str(seed or "avatar").encode("utf-8")).hexdigest()
    return palette[int(digest[:2], 16) % len(palette)]


def avatar_text_for_channel(label: str, channel: str) -> str:
    if str(channel or "").strip().lower() == "avito":
        clean = str(label or "").strip()
        if clean.startswith("@"):
            clean = clean[1:]
        return (clean[:1] or "A").upper()
    return avatar_initials(label)


def avatar_fill_for_channel(label: str, channel: str) -> str:
    if str(channel or "").strip().lower() == "avito":
        return "#7cc35b"
    return avatar_fill(str(channel or "") + ":" + str(label or ""))


def _display_name(meta: Mapping[str, Any], lead_id: int) -> str:
    return (
        str(meta.get("contact") or "").strip()
        or str(meta.get("title") or "").strip()
        or str(meta.get("avito_login") or "").strip()
        or str(meta.get("telegram_username") or "").strip()
        or f"Lead {lead_id}"
    )


async def _fetch_avito_avatar(
    *,
    tenant_id: int,
    meta: Mapping[str, Any],
    deps: PublicAvatarDeps,
) -> Response | None:
    try:
        live_profile = await deps.resolve_avito_profile_fn(
            int(tenant_id),
            account_id=int(str(meta.get("source_real_id") or "").strip() or 0) or None,
            chat_id=str(meta.get("peer") or "").strip(),
            author_id=int(str(meta.get("avito_user_id") or "").strip() or 0) or None,
        )
    except Exception:
        live_profile = {}
    avatar_url = str((live_profile or {}).get("avatar") or "").strip()
    if not avatar_url:
        return None
    try:
        async with deps.http_client_cls(timeout=15.0, follow_redirects=True) as client:
            upstream = await client.get(avatar_url)
        if upstream.status_code == 200 and upstream.content:
            headers = deps.no_store_headers_fn()
            headers["Content-Type"] = upstream.headers.get("content-type", "image/png")
            return Response(content=upstream.content, status_code=200, headers=headers)
    except Exception:
        return None
    return None


def _fallback_avatar_response(
    *,
    display_name: str,
    channel: str,
    deps: PublicAvatarDeps,
) -> Response:
    initials = html.escape(avatar_text_for_channel(display_name, channel))
    fill = avatar_fill_for_channel(display_name, channel)
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">'
        f'<rect width="128" height="128" rx="64" fill="{fill}"/>'
        f'<text x="50%" y="54%" dominant-baseline="middle" text-anchor="middle" '
        f'font-family="Inter, Arial, sans-serif" font-size="42" font-weight="700" fill="#ffffff">{initials}</text>'
        f"</svg>"
    )
    headers = deps.no_store_headers_fn()
    headers["Content-Type"] = "image/svg+xml"
    return Response(content=svg, status_code=200, headers=headers)
