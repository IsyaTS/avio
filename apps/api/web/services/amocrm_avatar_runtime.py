from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class AmoCRMAvatarDeps:
    read_tenant_config_fn: SyncFn
    amocrm_chat_service_module: Any
    hmac_module: Any
    tg_call_fn: AsyncFn
    tg_worker_call_error_type: type[Exception]
    no_store_headers_fn: SyncFn
    chat_avatar_fn: AsyncFn
    get_tenant_pubkey_fn: SyncFn


async def chat_avatar_proxy(
    request: Request,
    tenant_id: int,
    peer_id: str,
    token: str,
    *,
    deps: AmoCRMAvatarDeps,
) -> Response:
    if tenant_id <= 0:
        return JSONResponse({"ok": False, "detail": "bad_tenant"}, status_code=400)
    cfg = deps.read_tenant_config_fn(int(tenant_id))
    expected = deps.amocrm_chat_service_module.build_avatar_path_token(cfg, int(tenant_id), peer_id)
    if not _valid_token(token, expected, deps):
        return JSONResponse({"ok": False, "detail": "invalid_token"}, status_code=403)
    peer_val = _positive_int(peer_id)
    if peer_val is None:
        return JSONResponse({"ok": False, "detail": "bad_peer"}, status_code=400)
    try:
        status_code, response = await deps.tg_call_fn(
            "GET",
            f"/avatar/{int(tenant_id)}/{int(peer_val)}",
            timeout=15.0,
        )
    except deps.tg_worker_call_error_type as exc:
        return JSONResponse({"ok": False, "detail": exc.detail}, status_code=502)
    return _avatar_response(status_code, response, deps)


async def lead_avatar_proxy(
    request: Request,
    tenant_id: int,
    lead_id: int,
    token: str,
    *,
    deps: AmoCRMAvatarDeps,
) -> Response:
    if tenant_id <= 0 or lead_id <= 0:
        return JSONResponse({"ok": False, "detail": "bad_params"}, status_code=400)
    cfg = deps.read_tenant_config_fn(int(tenant_id))
    expected = deps.amocrm_chat_service_module.build_lead_avatar_path_token(
        cfg,
        int(tenant_id),
        int(lead_id),
    )
    if not _valid_token(token, expected, deps):
        return JSONResponse({"ok": False, "detail": "invalid_token"}, status_code=403)
    tenant_key = str(deps.get_tenant_pubkey_fn(int(tenant_id)) or "").strip()
    if not tenant_key:
        return JSONResponse({"ok": False, "detail": "tenant_key_missing"}, status_code=404)
    return await deps.chat_avatar_fn(request, str(int(lead_id)), tenant=int(tenant_id), k=tenant_key)


def _valid_token(token: str, expected: str, deps: AmoCRMAvatarDeps) -> bool:
    token_value = str(token or "").strip()
    return bool(token_value and deps.hmac_module.compare_digest(token_value, expected))


def _positive_int(value: Any) -> int | None:
    try:
        result = int(str(value))
    except Exception:
        return None
    return result if result > 0 else None


def _avatar_response(status_code: int, response: Any, deps: AmoCRMAvatarDeps) -> Response:
    body = bytes(response.content or b"")
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": str(status_code)})
    content_type = response.headers.get("content-type")
    if content_type:
        headers["Content-Type"] = content_type
    return Response(content=body, status_code=status_code, headers=headers)
