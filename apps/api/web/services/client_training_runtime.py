from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class ClientTrainingDeps:
    authorize_client_settings_request_fn: AsyncFn
    db_module: Any
    settings_module: Any
    logger: Any
    log_prefix: str
    httpx_module: Any
    time_module: Any = time


async def training_tg_harvest(
    tenant: int,
    request: Request,
    *,
    deps: ClientTrainingDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    payload = await _request_json(request)
    limit_dialogs = _parse_int(payload.get("limit_dialogs"), 15, minimum=1)
    limit_messages = _parse_int(payload.get("limit_messages"), 300, minimum=50)
    if not getattr(deps.settings_module, "ADMIN_TOKEN", ""):
        return JSONResponse({"detail": "tgworker_admin_missing"}, status_code=500)
    status, data = await tgworker_request(
        "POST",
        "/tg/qa",
        json_body={
            "tenant": int(tenant_id),
            "limit_dialogs": limit_dialogs,
            "limit_messages": limit_messages,
        },
        deps=deps,
    )
    if status >= 400:
        return JSONResponse(data or {"detail": "tgworker_error"}, status_code=status)
    return {"ok": True, "items": data.get("items", []), "meta": data.get("meta", {})}


async def training_tg_accept(
    tenant: int,
    request: Request,
    *,
    deps: ClientTrainingDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    payload = await _request_json(request)
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return JSONResponse({"detail": "invalid_items"}, status_code=400)
    saved = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        q_text = sanitize_training_text(str(item.get("q_text") or ""))
        a_text = sanitize_training_text(str(item.get("a_text") or ""))
        if not q_text or not a_text:
            continue
        await deps.db_module.record_training_example(
            int(tenant_id),
            lead_id=None,
            message_id=None,
            source="tg_harvest",
            source_feedback_id=None,
            q_text=q_text,
            a_text=a_text,
            is_active=True,
        )
        saved += 1
    return {"ok": True, "saved": saved}


async def tgworker_request(
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None,
    deps: ClientTrainingDeps,
) -> tuple[int, Any]:
    url = f"{tgworker_base_url(deps=deps)}{path}"
    headers = {"X-Admin-Token": deps.settings_module.ADMIN_TOKEN}
    async with deps.httpx_module.AsyncClient(timeout=30.0) as client:
        resp = await client.request(method, url, headers=headers, json=json_body)
        try:
            data = resp.json()
        except Exception:
            data = {"error": "invalid_json"}
        return resp.status_code, data


def tgworker_base_url(*, deps: ClientTrainingDeps) -> str:
    cleaned = str(getattr(deps.settings_module, "TGWORKER_BASE_URL", "") or "").strip()
    return cleaned.rstrip("/") or "http://tgworker:8000"


def _parse_int(raw: Any, default: int, *, minimum: int) -> int:
    if raw is None:
        raw = default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value >= minimum else minimum


async def _request_json(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def sanitize_training_text(text: str) -> str:
    if not text:
        return ""
    cleaned = str(text).replace("\r", " ").replace("\n", " ").strip()
    while "  " in cleaned:
        cleaned = cleaned.replace("  ", " ")
    return cleaned
