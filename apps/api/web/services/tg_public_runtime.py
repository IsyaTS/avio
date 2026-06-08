from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class TgPublicDeps:
    log_deprecated_fn: SyncFn
    resolve_tenant_and_key_fn: AsyncFn
    authorize_public_settings_request_fn: AsyncFn
    tg_slot_tenant_fn: SyncFn
    log_public_tg_request_fn: SyncFn
    client_identifier_fn: SyncFn
    log_tg_proxy_fn: SyncFn
    no_store_headers_fn: SyncFn
    register_password_attempt_fn: SyncFn
    tg_call_fn: AsyncFn
    tg_worker_call_error_type: type[Exception]
    resolve_qr_identifier_fn: SyncFn
    quote_fn: SyncFn
    common_module: Any
    resolve_tg_base_fn: SyncFn
    extract_json_detail_fn: SyncFn
    stringify_detail_fn: SyncFn
    proxy_headers_fn: SyncFn
    json_module: Any


@dataclass(frozen=True)
class TgConnectDeps:
    common_module: Any
    settings_module: Any
    render_template_fn: SyncFn
    quote_plus_fn: SyncFn


def connect_tg(
    tenant: int,
    request: Request,
    *,
    k: str | None,
    key: str | None,
    deps: TgConnectDeps,
) -> Response:
    tenant_id = int(tenant)
    access_key = _connect_access_key(request, k=k, key=key)
    if not deps.common_module.valid_key(tenant_id, access_key):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    deps.common_module.ensure_tenant_files(tenant_id)
    cfg = deps.common_module.read_tenant_config(tenant_id)
    passport = cfg.get("passport", {}) if isinstance(cfg, dict) else {}
    public_key = str(getattr(deps.settings_module, "PUBLIC_KEY", "") or "")
    resolved_key = _resolved_tenant_key(tenant_id, access_key, deps=deps)
    return deps.render_template_fn(
        "connect/tg.html",
        _tg_connect_context(
            request,
            tenant_id,
            access_key,
            public_key,
            resolved_key,
            passport,
            deps=deps,
        ),
    )


def _connect_access_key(request: Request, *, k: str | None, key: str | None) -> str:
    return (k or key or request.query_params.get("k") or request.query_params.get("key") or "").strip()


def _resolved_tenant_key(tenant_id: int, access_key: str, *, deps: TgConnectDeps) -> str:
    primary_key = (deps.common_module.get_tenant_pubkey(tenant_id) or "").strip()
    return primary_key or access_key


def _tg_connect_context(
    request: Request,
    tenant_id: int,
    access_key: str,
    public_key: str,
    resolved_key: str,
    passport: Any,
    *,
    deps: TgConnectDeps,
) -> dict[str, Any]:
    public_or_tenant_key = public_key or resolved_key
    return {
        "request": request,
        "tenant": tenant_id,
        "key": public_or_tenant_key,
        "tenant_key": access_key,
        "subtitle": _passport_brand(passport),
        "persona_preview": _persona_preview(deps.common_module.read_persona(tenant_id)),
        "tg_connect_config": _tg_connect_config(tenant_id, public_or_tenant_key, public_key, deps),
    }


def _tg_connect_config(
    tenant_id: int,
    key: str,
    public_key: str,
    deps: TgConnectDeps,
) -> dict[str, Any]:
    encoded_public_key = deps.quote_plus_fn(public_key)
    return {
        "tenant": tenant_id,
        "key": key,
        "urls": {
            "public_key": public_key,
            "tg_status": "/pub/tg/status",
            "tg_status_url": _tg_public_path("/pub/tg/status", encoded_public_key),
            "tg_start": "/pub/tg/start",
            "tg_start_url": _tg_public_path("/pub/tg/start", encoded_public_key),
            "tg_qr_png": _tg_public_path("/pub/tg/qr.png", encoded_public_key),
            "tg_2fa": "/pub/tg/2fa",
            "tg_2fa_url": _tg_public_path("/pub/tg/2fa", encoded_public_key),
            "tg_password": "/pub/tg/2fa",
        },
    }


def _tg_public_path(path: str, encoded_public_key: str) -> str:
    return f"{path}?k={encoded_public_key}" if encoded_public_key else path


def _passport_brand(passport: Any) -> str:
    if not isinstance(passport, dict):
        return ""
    return str(passport.get("brand") or "").strip()


def _persona_preview(persona_text: Any) -> str:
    if not persona_text:
        return ""
    return "\n".join(str(persona_text).splitlines()[:6]).strip()


async def handle_twofa(
    route: str,
    request: Request,
    tenant: int | str | None,
    key: str | None,
    *,
    slot: int,
    deps: TgPublicDeps,
) -> Response:
    deps.log_deprecated_fn(route)
    tenant_candidate, key_candidate = await deps.resolve_tenant_and_key_fn(
        request, tenant, key
    )
    auth = await deps.authorize_public_settings_request_fn(
        request, tenant_candidate, key_candidate
    )
    if isinstance(auth, Response):
        return auth
    tenant_id, validated_key = auth
    tg_tenant_id = deps.tg_slot_tenant_fn(tenant_id, slot)
    deps.log_public_tg_request_fn(route, tenant_id, validated_key or "session")

    client_token = deps.client_identifier_fn(request)
    password_text = (await _twofa_password_value(request)).strip()
    if not password_text:
        deps.log_tg_proxy_fn(route, tenant_id, 400, None, error="password_required")
        return JSONResponse(
            {"error": "password_required"},
            status_code=400,
            headers=deps.no_store_headers_fn(),
        )

    rate_limit = _twofa_rate_limit_response(route, tenant_id, tg_tenant_id, client_token, deps)
    if rate_limit is not None:
        return rate_limit
    upstream_result = await _submit_twofa_to_worker(tg_tenant_id, password_text, deps)
    if isinstance(upstream_result, Response):
        deps.log_tg_proxy_fn(route, tenant_id, 502, None, error="tg_unavailable")
        return upstream_result
    last_status, upstream = upstream_result
    status_code = int(last_status or upstream.status_code)
    body_bytes = bytes(getattr(upstream, "content", b"") or b"")
    try:
        payload = upstream.json()
    except ValueError:
        payload = {}
    headers = deps.no_store_headers_fn(
        {"X-Telegram-Upstream-Status": str(status_code or "-")}
    )
    error_response = _twofa_error_response(route, tenant_id, status_code, body_bytes, payload, headers, deps)
    if error_response is not None:
        return error_response
    return _twofa_success_response(route, tenant_id, status_code, body_bytes, payload, headers, deps)


async def _twofa_password_value(request: Request) -> str:
    try:
        data = await request.json()
    except Exception:
        data = None
    if isinstance(data, dict) and isinstance(data.get("password"), str):
        return data["password"]
    try:
        form = await request.form()
    except Exception:
        form = None
    if form is not None and isinstance(form.get("password"), str):
        return form.get("password") or ""
    return ""


def _twofa_rate_limit_response(
    route: str,
    tenant_id: int,
    tg_tenant_id: int,
    client_token: str,
    deps: TgPublicDeps,
) -> Response | None:
    allowed, retry_after = deps.register_password_attempt_fn(tg_tenant_id, client_token)
    if allowed:
        return None
    headers = deps.no_store_headers_fn()
    if retry_after and retry_after > 0:
        headers["Retry-After"] = str(int(retry_after))
    deps.log_tg_proxy_fn(route, tenant_id, 429, None, error="flood_wait")
    body = {"error": "flood_wait"}
    if retry_after and retry_after > 0:
        body["retry_after"] = int(retry_after)
    return JSONResponse(body, status_code=429, headers=headers)


async def _submit_twofa_to_worker(
    tg_tenant_id: int,
    password_text: str,
    deps: TgPublicDeps,
) -> tuple[int, Any] | Response:
    last_error: str | None = None
    payload_body = {"tenant": tg_tenant_id, "password": password_text}
    for candidate in ["/2fa", "/rpc/twofa.submit"]:
        try:
            status_code, response = await deps.tg_call_fn(
                "POST",
                candidate,
                json=payload_body,
                timeout=5.0,
            )
            return status_code, response
        except deps.tg_worker_call_error_type as exc:
            last_error = getattr(exc, "detail", "") or str(exc)
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": "-"})
    return JSONResponse(
        {"error": "tg_unavailable", "detail": last_error or "tg_unavailable"},
        status_code=502,
        headers=headers,
    )


def _twofa_error_response(
    route: str,
    tenant_id: int,
    status_code: int,
    body_bytes: bytes,
    payload: dict[str, Any],
    headers: dict[str, str],
    deps: TgPublicDeps,
) -> Response | None:
    error_code = str(payload.get("error") or "").strip()
    if status_code <= 0:
        deps.log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error="tg_unavailable")
        headers["Content-Type"] = "application/json"
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers=headers)
    if status_code == 401 or error_code == "bad_password":
        response_payload = {"error": "bad_password"}
        detail = payload.get("detail")
        if detail:
            response_payload["detail"] = detail
        deps.log_tg_proxy_fn(route, tenant_id, 401, body_bytes, error="bad_password")
        headers["Content-Type"] = "application/json"
        return JSONResponse(response_payload, status_code=401, headers=headers)

    if status_code == 409 and error_code:
        deps.log_tg_proxy_fn(route, tenant_id, 409, body_bytes, error=error_code)
        headers["Content-Type"] = "application/json"
        return JSONResponse({"error": error_code}, status_code=409, headers=headers)

    if not (200 <= status_code < 300):
        failure = error_code or payload.get("detail") or f"status_{status_code}"
        deps.log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error=failure)
        headers["Content-Type"] = "application/json"
        return JSONResponse({"error": failure}, status_code=502, headers=headers)
    return None


def _twofa_success_response(
    route: str,
    tenant_id: int,
    status_code: int,
    body_bytes: bytes,
    payload: dict[str, Any],
    headers: dict[str, str],
    deps: TgPublicDeps,
) -> Response:
    state_value = str(payload.get("state") or payload.get("status") or "").strip()
    needs_twofa = bool(state_value == "need_2fa" or payload.get("needs_2fa"))
    response_payload = {
        "authorized": bool(payload.get("authorized")),
        "state": state_value,
        "needs_2fa": needs_twofa,
        "last_error": payload.get("last_error"),
        "expires_at": payload.get("expires_at"),
        "ok": bool(payload.get("ok", True)),
    }
    deps.log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error=None)
    headers["Content-Type"] = "application/json"
    return JSONResponse(response_payload, headers=headers)


async def start(
    route: str,
    request: Request,
    tenant: int | str | None,
    key: str | None,
    *,
    slot: int,
    allow_body: bool,
    deps: TgPublicDeps,
) -> Response:
    tenant_candidate, key_candidate = await deps.resolve_tenant_and_key_fn(
        request,
        tenant,
        key,
        query_keys=("k",),
        allow_body=allow_body,
    )
    auth = await deps.authorize_public_settings_request_fn(request, tenant_candidate, key_candidate)
    if isinstance(auth, Response):
        return auth
    tenant_id, validated_key = auth
    tg_tenant_id = deps.tg_slot_tenant_fn(tenant_id, slot)
    deps.log_public_tg_request_fn(route, tenant_id, validated_key or "session")
    upstream_result = await _start_tg_worker(tg_tenant_id, deps)
    if isinstance(upstream_result, Response):
        deps.log_tg_proxy_fn(route, tenant_id, 502, None, error="tg_unavailable")
        return upstream_result
    last_status, upstream = upstream_result
    return _start_response(route, tenant_id, int(last_status or upstream.status_code), upstream, deps)


async def _start_tg_worker(tg_tenant_id: int, deps: TgPublicDeps) -> tuple[int, Any] | Response:
    last_error: str | None = None
    for candidate in ["/qr/start", "/rpc/start", "/session/start"]:
        try:
            return await deps.tg_call_fn(
                "POST",
                candidate,
                json={"tenant": tg_tenant_id},
                timeout=5.0,
            )
        except deps.tg_worker_call_error_type as exc:
            last_error = getattr(exc, "detail", "") or str(exc)
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": "-"})
    return JSONResponse(
        {"error": "tg_unavailable", "detail": last_error or "tg_unavailable"},
        status_code=502,
        headers=headers,
    )


def _start_response(
    route: str,
    tenant_id: int,
    status_code: int,
    upstream: Any,
    deps: TgPublicDeps,
) -> Response:
    body_bytes = bytes(getattr(upstream, "content", b"") or b"")
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": str(status_code)})
    if 200 <= status_code < 300:
        content_type = upstream.headers.get("content-type")
        if content_type:
            headers["Content-Type"] = content_type
        deps.log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error=None)
        return Response(content=body_bytes, status_code=status_code, headers=headers)
    detail_text = upstream.text if hasattr(upstream, "text") else ""
    reason = detail_text.strip() or f"status_{status_code}"
    deps.log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error=reason)
    headers["Content-Type"] = "application/json"
    return JSONResponse(
        {"error": "tg_upstream", "detail": reason},
        status_code=status_code,
        headers=headers,
    )


async def status(
    route: str,
    request: Request,
    tenant: int | str | None,
    key: str | None,
    *,
    slot: int,
    deps: TgPublicDeps,
) -> Response:
    tenant_candidate, key_candidate = await deps.resolve_tenant_and_key_fn(
        request,
        tenant,
        key,
        query_keys=("k",),
        allow_body=False,
    )
    auth = await deps.authorize_public_settings_request_fn(request, tenant_candidate, key_candidate)
    if isinstance(auth, Response):
        return auth
    tenant_id, validated_key = auth
    tg_tenant_id = deps.tg_slot_tenant_fn(tenant_id, slot)
    deps.log_public_tg_request_fn(route, tenant_id, validated_key or "session")
    upstream_result = await _status_tg_worker(tg_tenant_id, deps)
    if isinstance(upstream_result, Response):
        deps.log_tg_proxy_fn(route, tenant_id, 502, None, error="tg_unavailable")
        return upstream_result
    last_status, upstream = upstream_result
    status_code = int(last_status or upstream.status_code)
    body_bytes = bytes(upstream.content or b"")
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": str(status_code)})
    content_type = upstream.headers.get("content-type")
    if content_type:
        headers["Content-Type"] = content_type
    deps.log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error=None)
    return Response(content=body_bytes, status_code=status_code, headers=headers)


async def _status_tg_worker(tg_tenant_id: int, deps: TgPublicDeps) -> tuple[int, Any] | Response:
    last_error: str | None = None
    for candidate in ["/status", "/rpc/status", "/session/status"]:
        try:
            status_code, response = await deps.tg_call_fn(
                "GET",
                candidate,
                params={"tenant": tg_tenant_id},
                timeout=5.0,
            )
        except deps.tg_worker_call_error_type as exc:
            last_error = getattr(exc, "detail", "") or str(exc)
            continue
        if not (200 <= status_code < 300):
            detail_text = response.text if hasattr(response, "text") else ""
            last_error = detail_text.strip() or f"status_{status_code}"
            continue
        return status_code, response
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": "-"})
    return JSONResponse(
        {"error": "tg_unavailable", "detail": last_error or "tg_unavailable"},
        status_code=502,
        headers=headers,
    )


async def qr_png(
    route: str,
    request: Request,
    tenant: int | str | None,
    qr_id: str | None,
    key: str | None,
    *,
    slot: int,
    deps: TgPublicDeps,
) -> Response:
    tenant_candidate, key_candidate = await deps.resolve_tenant_and_key_fn(
        request,
        tenant,
        key,
        query_keys=("k",),
        allow_body=False,
    )
    auth = await deps.authorize_public_settings_request_fn(request, tenant_candidate, key_candidate)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    tg_tenant_id = deps.tg_slot_tenant_fn(tenant_id, slot)
    qr_identifier = deps.resolve_qr_identifier_fn(qr_id, request.query_params.get("id"))
    if not qr_identifier:
        deps.log_tg_proxy_fn(route, tenant_id, 400, None, error="missing_qr_id")
        return JSONResponse(
            {"error": "missing_qr_id"},
            status_code=400,
            headers=deps.no_store_headers_fn(),
        )
    upstream_result = await _qr_png_tg_worker(tg_tenant_id, qr_identifier, deps)
    if isinstance(upstream_result, Response):
        deps.log_tg_proxy_fn(route, tenant_id, 502, None, error="tg_unavailable")
        return upstream_result
    last_status, upstream = upstream_result
    status_code = int(last_status or upstream.status_code)
    body_bytes = bytes(upstream.content or b"")
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": str(status_code)})
    headers["Cache-Control"] = "no-store"
    headers["Content-Type"] = upstream.headers.get("content-type") or "image/png"
    deps.log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error=None)
    return Response(content=body_bytes, status_code=status_code, headers=headers)


async def proxy_tg_resource(
    route: str,
    request: Request,
    tenant: int | str | None,
    key: str | None,
    *,
    resource_path_fn: Callable[[int], str],
    deps: TgPublicDeps,
    timeout: float = 15.0,
) -> Response:
    tenant_candidate, key_candidate = await deps.resolve_tenant_and_key_fn(
        request,
        tenant,
        key,
        query_keys=("k",),
        allow_body=False,
    )
    auth = await deps.authorize_public_settings_request_fn(request, tenant_candidate, key_candidate)
    if isinstance(auth, Response):
        return auth
    tenant_id, validated_key = auth
    deps.log_public_tg_request_fn(route, tenant_id, validated_key or "session")
    try:
        resource_path = resource_path_fn(int(tenant_id))
    except Exception:
        return JSONResponse({"error": "bad_params"}, status_code=400, headers=deps.no_store_headers_fn())

    try:
        status_code, response = await deps.tg_call_fn("GET", resource_path, timeout=timeout)
    except deps.tg_worker_call_error_type as exc:
        detail = getattr(exc, "detail", "") or str(exc)
        deps.log_tg_proxy_fn(route, tenant_id, 502, None, error=detail)
        return JSONResponse(
            {"error": "tg_unavailable", "detail": detail},
            status_code=502,
            headers=deps.no_store_headers_fn(),
        )

    body_bytes = bytes(response.content or b"")
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": str(status_code)})
    content_type = response.headers.get("content-type")
    if content_type:
        headers["Content-Type"] = content_type
    deps.log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error=None)
    return Response(content=body_bytes, status_code=status_code, headers=headers)


async def _qr_png_tg_worker(
    tg_tenant_id: int,
    qr_identifier: str,
    deps: TgPublicDeps,
) -> tuple[int, Any] | Response:
    safe_qr = deps.quote_fn(qr_identifier, safe="")
    fallback_paths: list[tuple[str, dict[str, Any]]] = [
        ("/qr/png", {"tenant": tg_tenant_id, "qr_id": qr_identifier}),
        (f"/session/qr/{safe_qr}.png", {"tenant": tg_tenant_id}),
    ]
    last_error: str | None = None
    for candidate, params in fallback_paths:
        try:
            status_code, response = await deps.tg_call_fn(
                "GET",
                candidate,
                params=params,
                timeout=5.0,
            )
        except deps.tg_worker_call_error_type as exc:
            last_error = getattr(exc, "detail", "") or str(exc)
            continue
        if not (200 <= status_code < 300):
            detail_text = response.text if hasattr(response, "text") else ""
            last_error = detail_text.strip() or f"status_{status_code}"
            continue
        return status_code, response
    headers = deps.no_store_headers_fn({"X-Telegram-Upstream-Status": "-"})
    return JSONResponse(
        {"error": "tg_unavailable", "detail": last_error or "tg_unavailable"},
        status_code=502,
        headers=headers,
    )


async def qr_txt(
    route: str,
    request: Request,
    tenant: int | str | None,
    qr_id: str | None,
    key: str | None,
    *,
    slot: int,
    deps: TgPublicDeps,
) -> Response:
    tenant_candidate, key_candidate = await deps.resolve_tenant_and_key_fn(
        request,
        tenant,
        key,
        query_keys=("k", "key"),
        allow_body=False,
    )
    auth = await deps.authorize_public_settings_request_fn(
        request, tenant_candidate, key_candidate
    )
    if isinstance(auth, Response):
        return auth
    tenant_id, validated_key = auth
    tg_tenant_id = deps.tg_slot_tenant_fn(tenant_id, slot)
    deps.log_public_tg_request_fn(route, tenant_id, validated_key or "session")

    qr_value = deps.resolve_qr_identifier_fn(qr_id, request.query_params.get("id"))
    if not qr_value:
        deps.log_tg_proxy_fn(route, tenant_id, 400, None, error="missing_qr_id")
        return JSONResponse(
            {"error": "missing_qr_id"},
            status_code=400,
            headers=deps.no_store_headers_fn(),
        )

    safe_qr = deps.quote_fn(qr_value, safe="")
    status_code, body, headers = deps.common_module.tg_http(
        "GET",
        f"{deps.resolve_tg_base_fn()}/session/qr/{safe_qr}.txt?tenant={tg_tenant_id}",
        timeout=15.0,
    )
    body_bytes = (
        body
        if isinstance(body, (bytes, bytearray))
        else ("" if body is None else str(body)).encode("utf-8")
    )
    detail_from_json = deps.extract_json_detail_fn(body_bytes)
    detail = _qr_txt_error_detail(status_code, body_bytes, detail_from_json, deps)
    deps.log_tg_proxy_fn(route, None, status_code, body_bytes, error=detail)
    error_response = _qr_txt_error_response(
        status_code,
        body_bytes,
        headers or {},
        detail_from_json,
        deps,
    )
    if error_response is not None:
        return error_response
    return _qr_txt_success_response(status_code, body_bytes, headers or {}, deps)


def _qr_txt_error_detail(
    status_code: int,
    body_bytes: bytes,
    detail_from_json: str | None,
    deps: TgPublicDeps,
) -> str | None:
    if status_code == 200:
        return None
    if detail_from_json:
        return detail_from_json
    return deps.stringify_detail_fn(body_bytes) or f"status_{status_code}"


def _qr_txt_error_response(
    status_code: int,
    body_bytes: bytes,
    headers: dict[str, Any],
    detail_from_json: str | None,
    deps: TgPublicDeps,
) -> Response | None:
    if status_code <= 0:
        return JSONResponse(
            {"error": "tg_unavailable"},
            status_code=502,
            headers=deps.no_store_headers_fn({"X-Telegram-Upstream-Status": str(status_code)}),
        )
    if status_code == 404:
        return _qr_txt_not_found_response(status_code, body_bytes, headers, detail_from_json, deps)
    if status_code != 200:
        headers_out = deps.proxy_headers_fn(headers, status_code)
        headers_out.update(deps.no_store_headers_fn())
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers=headers_out)
    return None


def _qr_txt_not_found_response(
    status_code: int,
    body_bytes: bytes,
    headers: dict[str, Any],
    detail_from_json: str | None,
    deps: TgPublicDeps,
) -> Response:
    detail_value = detail_from_json or "qr_not_found"
    headers_out = deps.proxy_headers_fn(headers, status_code)
    headers_out.update(deps.no_store_headers_fn())
    if not body_bytes:
        body_bytes = deps.json_module.dumps({"detail": detail_value}).encode("utf-8")
    media_type = headers_out.get("Content-Type") or "application/json"
    return Response(
        content=body_bytes,
        status_code=status_code,
        headers=headers_out,
        media_type=media_type,
    )


def _qr_txt_success_response(
    status_code: int,
    body_bytes: bytes,
    headers: dict[str, Any],
    deps: TgPublicDeps,
) -> Response:
    response_headers = deps.proxy_headers_fn(headers, status_code)
    response_headers.update(deps.no_store_headers_fn())
    response_headers.setdefault("Content-Type", "text/plain; charset=utf-8")
    return Response(content=body_bytes, status_code=status_code, headers=response_headers)
