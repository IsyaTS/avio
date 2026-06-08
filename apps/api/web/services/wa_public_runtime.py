from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class WaPublicDeps:
    ensure_valid_qr_request_fn: SyncFn
    invalid_key_response_fn: SyncFn
    as_head_response_fn: SyncFn
    common_module: Any
    proxy_baileys_qr_fn: SyncFn
    normalize_qr_id_fn: SyncFn
    get_last_qr_id_fn: SyncFn
    no_store_headers_fn: SyncFn
    load_cached_svg_fn: SyncFn
    httpx_module: Any
    wa_logger: Any
    qr_expired_response_fn: SyncFn
    cache_qr_payload_fn: SyncFn


@dataclass(frozen=True)
class WaRestartDeps:
    ensure_valid_qr_request_fn: SyncFn
    invalid_key_response_fn: SyncFn
    common_module: Any
    json_module: Any
    wa_logger: Any


@dataclass(frozen=True)
class WaStartDeps:
    ensure_valid_qr_request_fn: SyncFn
    invalid_key_response_fn: SyncFn
    common_module: Any
    get_last_qr_id_fn: SyncFn
    normalize_qr_id_fn: SyncFn
    derive_state_fn: SyncFn
    status_fn: AsyncFn
    baileys_status_fn: AsyncFn
    build_qr_url_fn: SyncFn
    no_store_headers_fn: SyncFn
    wa_logger: Any


@dataclass(frozen=True)
class WaStatusDeps:
    ensure_valid_qr_request_fn: SyncFn
    invalid_key_response_fn: SyncFn
    as_head_response_fn: SyncFn
    common_module: Any
    get_last_qr_id_fn: SyncFn
    normalize_qr_id_fn: SyncFn
    status_fn: AsyncFn
    baileys_status_fn: AsyncFn
    compose_response_fn: SyncFn
    build_qr_url_fn: SyncFn
    no_store_headers_fn: SyncFn
    wa_logger: Any


@dataclass(frozen=True)
class WaResponseDeps:
    normalize_qr_id_fn: SyncFn
    derive_state_fn: SyncFn
    build_qr_url_fn: SyncFn


@dataclass(frozen=True)
class WaConnectDeps:
    ensure_valid_qr_request_fn: SyncFn
    invalid_key_response_fn: SyncFn
    common_module: Any
    render_template_fn: SyncFn
    quote_plus_fn: SyncFn
    time_module: Any


@dataclass(frozen=True)
class WaStatusImplDeps:
    common_module: Any
    json_module: Any
    get_last_qr_id_fn: SyncFn
    normalize_qr_id_fn: SyncFn
    derive_state_fn: SyncFn
    truthy_flag_fn: SyncFn


def connect_wa(
    tenant: int,
    request: Request,
    *,
    k: str | None,
    deps: WaConnectDeps,
) -> Response:
    query_candidate = k or request.query_params.get("k") or ""
    guard = deps.ensure_valid_qr_request_fn(tenant, query_candidate, request, query_param_only=True)
    if guard is None:
        return deps.invalid_key_response_fn()

    tenant_id, resolved_key = guard
    resolved_key = _wa_connect_key(int(tenant_id), resolved_key or "", deps=deps)
    deps.common_module.ensure_tenant_files(int(tenant_id))
    cfg = deps.common_module.read_tenant_config(int(tenant_id))
    passport = cfg.get("passport", {}) if isinstance(cfg, Mapping) else {}
    persona = deps.common_module.read_persona(int(tenant_id))
    return deps.render_template_fn(
        "connect/wa.html",
        _wa_connect_context(
            request,
            int(tenant_id),
            resolved_key,
            passport,
            persona,
            deps=deps,
        ),
    )


def _wa_connect_key(tenant_id: int, resolved_key: str, *, deps: WaConnectDeps) -> str:
    if resolved_key:
        return resolved_key
    items = deps.common_module.list_keys(tenant_id)
    if items:
        return items[0].get("key", "")
    return ""


def _wa_connect_context(
    request: Request,
    tenant_id: int,
    resolved_key: str,
    passport: Any,
    persona: Any,
    *,
    deps: WaConnectDeps,
) -> dict[str, Any]:
    return {
        "request": request,
        "tenant": tenant_id,
        "key": resolved_key,
        "k": resolved_key,
        "timestamp": int(deps.time_module.time()),
        "passport": passport,
        "persona_preview": "\n".join((persona or "").splitlines()[:6]),
        "title": "Подключение WhatsApp",
        "subtitle": _wa_connect_subtitle(passport),
        "settings_link": _wa_settings_link(request, tenant_id, resolved_key, deps=deps),
        "public_base": deps.common_module.public_base_url(request),
    }


def _wa_connect_subtitle(passport: Any) -> str:
    if isinstance(passport, Mapping) and passport:
        return passport.get("brand") or "Подключение WhatsApp"
    return "Подключение WhatsApp"


def _wa_settings_link(
    request: Request,
    tenant_id: int,
    resolved_key: str,
    *,
    deps: WaConnectDeps,
) -> str:
    if not resolved_key:
        return ""
    raw_settings = request.url_for("client_settings", tenant=str(tenant_id))
    return deps.common_module.public_url(
        request,
        f"{raw_settings}?k={deps.quote_plus_fn(resolved_key)}",
    )


async def legacy_status_impl(tenant: int, *, deps: WaStatusImplDeps) -> dict[str, Any]:
    cached_qr_id, redis_failed = deps.get_last_qr_id_fn(int(tenant))
    code, raw = deps.common_module.http(
        "GET",
        f"{deps.common_module.wa_base_url(int(tenant))}/session/{int(tenant)}/status",
        timeout=3.0,
    )
    data = _json_dict(raw, deps)
    state_value, need_qr_flag = deps.derive_state_fn(data)
    qr_id_value = deps.normalize_qr_id_fn(data.get("qr_id") or data.get("qrId"))
    if qr_id_value is None and cached_qr_id and not redis_failed:
        qr_id_value = cached_qr_id
    payload = _legacy_status_payload(
        int(code or 0),
        data,
        state_value,
        need_qr_flag,
        redis_failed,
        deps,
    )
    if qr_id_value is not None:
        payload["qr_id"] = qr_id_value
    return payload


async def baileys_status_impl(tenant: int, *, deps: WaStatusImplDeps) -> dict[str, Any]:
    _code, raw = deps.common_module.wabaileys_http(
        "GET",
        f"/sessions/status?tenant={int(tenant)}",
        timeout=3.0,
    )
    data = _json_dict(raw, deps)
    session = data.get("session")
    if not isinstance(session, Mapping):
        session = {}
    qr_block = session.get("qr") if isinstance(session.get("qr"), Mapping) else {}
    connected_flag = bool(session.get("connected"))
    normalized: dict[str, Any] = {
        "ok": bool(data.get("ok")),
        "state": str(session.get("status") or "") or None,
        "connected": connected_flag,
        "ready": connected_flag,
        "qr": bool(qr_block),
        "need_qr": not connected_flag,
        "qr_id": session.get("qr_id") or qr_block.get("id") or qr_block.get("raw"),
        "raw": session,
    }
    if qr_block:
        normalized["qr_meta"] = qr_block
    return normalized


def _json_dict(raw: Any, deps: WaStatusImplDeps) -> dict[str, Any]:
    try:
        data = deps.json_module.loads(raw)
    except Exception:
        data = {}
    return data if isinstance(data, dict) else {}


def _legacy_status_payload(
    status_code: int,
    data: Mapping[str, Any],
    state_value: str | None,
    need_qr_flag: bool,
    redis_failed: bool,
    deps: WaStatusImplDeps,
) -> dict[str, Any]:
    ready_flag = deps.truthy_flag_fn(data.get("ready"))
    connected_flag = deps.truthy_flag_fn(data.get("connected"))
    qr_flag = deps.truthy_flag_fn(data.get("qr"))
    payload: dict[str, Any] = {
        "ok": True,
        "state": state_value,
        "status_code": status_code,
        "raw": dict(data),
        "need_qr": need_qr_flag,
        "ready": bool(data.get("ready")) if "ready" in data else ready_flag,
        "connected": bool(data.get("connected")) if "connected" in data else connected_flag or ready_flag,
        "qr": bool(data.get("qr")) if "qr" in data else qr_flag,
    }
    if redis_failed:
        payload["qr_cache_unavailable"] = True
    if data.get("last") is not None:
        payload["last"] = data.get("last")
    return payload


async def wa_status(
    request: Request,
    *,
    tenant: int,
    key: str,
    deps: WaStatusDeps,
) -> Response:
    ok = deps.ensure_valid_qr_request_fn(tenant, key, request, query_param_only=True)
    if ok is None:
        response = deps.invalid_key_response_fn()
        return deps.as_head_response_fn(response, request)
    tenant_id, validated_key = ok

    cached_qr_id, redis_failed = deps.get_last_qr_id_fn(int(tenant_id))
    qr_id_override = None if redis_failed else cached_qr_id
    if redis_failed:
        deps.wa_logger.info("wa_qr_cache_unavailable tenant=%s", tenant_id)

    provider = deps.common_module.whatsapp_provider(int(tenant_id))
    if provider == "baileys":
        snapshot = await deps.baileys_status_fn(int(tenant_id))
    else:
        snapshot = await deps.status_fn(int(tenant_id))

    result = deps.compose_response_fn(
        int(tenant_id),
        validated_key,
        status_snapshot=snapshot,
        qr_id_override=qr_id_override,
    )
    _apply_status_qr_url(
        result,
        tenant_id=int(tenant_id),
        key=validated_key,
        qr_id_override=qr_id_override,
        deps=deps,
    )
    return JSONResponse(result, headers=deps.no_store_headers_fn())


def _apply_status_qr_url(
    result: dict[str, Any],
    *,
    tenant_id: int,
    key: str,
    qr_id_override: str | None,
    deps: WaStatusDeps,
) -> None:
    effective_qr_id = deps.normalize_qr_id_fn(qr_id_override) if qr_id_override else None
    if not effective_qr_id:
        effective_qr_id = deps.normalize_qr_id_fn(result.get("qr_id"))

    if effective_qr_id:
        result["qr_id"] = effective_qr_id
        if key:
            result["qr_url"] = deps.build_qr_url_fn(tenant_id, key, effective_qr_id)
    else:
        result.pop("qr_id", None)
        if result.get("need_qr") and key:
            result.setdefault("state", "qr")
            result["qr_url"] = deps.build_qr_url_fn(tenant_id, key)
        elif not result.get("need_qr"):
            result.pop("qr_url", None)


async def wa_start(
    request: Request,
    *,
    tenant: int,
    key: str,
    deps: WaStartDeps,
) -> Response:
    ok = deps.ensure_valid_qr_request_fn(tenant, key, request, query_param_only=True)
    if ok is None:
        return deps.invalid_key_response_fn()
    tenant_id, validated_key = ok
    provider = deps.common_module.whatsapp_provider(int(tenant_id))
    response_data = await _start_wa_session(int(tenant_id), provider, deps)
    if isinstance(response_data, Response):
        return response_data
    qr_id_value = _wa_start_qr_id(int(tenant_id), response_data, deps)
    status_snapshot = await _wa_status_snapshot(int(tenant_id), provider, deps)
    result = compose_public_wa_response(
        int(tenant_id),
        validated_key,
        status_snapshot=status_snapshot,
        qr_id_override=qr_id_value,
        deps=WaResponseDeps(
            normalize_qr_id_fn=deps.normalize_qr_id_fn,
            derive_state_fn=deps.derive_state_fn,
            build_qr_url_fn=deps.build_qr_url_fn,
        ),
    )
    if result.get("need_qr") and not result.get("qr_url"):
        result["qr_url"] = deps.build_qr_url_fn(int(tenant_id), validated_key)
    if result.get("need_qr") and result.get("state") != "qr":
        result["state"] = "qr"
    return JSONResponse(result, headers=deps.no_store_headers_fn())


async def _start_wa_session(tenant_id: int, provider: str, deps: WaStartDeps) -> dict[str, Any] | Response:
    webhook = deps.common_module.webhook_url()
    if provider == "baileys":
        return await _start_baileys_session(tenant_id, webhook, deps)
    return await _start_legacy_wa_session(tenant_id, webhook, deps)


async def _start_baileys_session(tenant_id: int, webhook: str, deps: WaStartDeps) -> dict[str, Any] | Response:
    try:
        response = await deps.common_module.wabaileys_post(
            "/sessions/start",
            {"tenant": int(tenant_id), "webhookUrl": webhook},
        )
    except Exception:
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)
    if int(getattr(response, "status_code", 0) or 0) < 200 or int(getattr(response, "status_code", 0) or 0) >= 400:
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)
    return {}


async def _start_legacy_wa_session(tenant_id: int, webhook: str, deps: WaStartDeps) -> dict[str, Any] | Response:
    try:
        response = await deps.common_module.wa_post(
            f"/session/{int(tenant_id)}/start",
            {"tenant_id": int(tenant_id), "webhook_url": webhook},
            tenant=int(tenant_id),
        )
    except Exception:
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code < 200 or status_code >= 400:
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)
    try:
        data = response.json()
    except Exception:
        data = {}
    return data if isinstance(data, dict) else {}


def _wa_start_qr_id(tenant_id: int, response_data: Mapping[str, Any], deps: WaStartDeps) -> str | None:
    qr_id_value, redis_failed = deps.get_last_qr_id_fn(int(tenant_id))
    if redis_failed:
        deps.wa_logger.info("wa_qr_cache_unavailable tenant=%s", tenant_id)
        return None
    return qr_id_value or deps.normalize_qr_id_fn(response_data.get("qr_id") or response_data.get("qrId"))


async def _wa_status_snapshot(tenant_id: int, provider: str, deps: WaStartDeps) -> Mapping[str, Any]:
    if provider == "baileys":
        return await deps.baileys_status_fn(int(tenant_id))
    return await deps.status_fn(int(tenant_id))


def compose_public_wa_response(
    tenant: int,
    key: str | None,
    *,
    status_snapshot: Mapping[str, Any] | None = None,
    qr_id_override: str | None = None,
    deps: WaResponseDeps,
) -> dict[str, Any]:
    state_value: str | None = None
    need_qr_flag = False
    qr_id_value = qr_id_override
    raw_snapshot: Mapping[str, Any] | None = None
    result: dict[str, Any] = {"ok": True}
    if isinstance(status_snapshot, Mapping):
        raw_snapshot = _wa_raw_snapshot(status_snapshot)
        state_candidate = status_snapshot.get("state")
        if state_candidate is not None:
            state_value = str(state_candidate)
        need_qr_flag = bool(status_snapshot.get("need_qr"))
        if qr_id_value is None:
            qr_id_value = deps.normalize_qr_id_fn(status_snapshot.get("qr_id"))
        _copy_wa_snapshot_fields(result, status_snapshot)
    state_value, need_qr_flag, qr_id_value = _resolve_wa_state_and_qr(
        state_value,
        need_qr_flag,
        qr_id_value,
        raw_snapshot,
        deps,
    )
    _apply_wa_response_fields(
        result,
        tenant=tenant,
        key=key,
        state_value=state_value,
        need_qr_flag=need_qr_flag,
        qr_id_value=qr_id_value,
        deps=deps,
    )
    return result


def _wa_raw_snapshot(status_snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    candidate = status_snapshot.get("raw")
    return candidate if isinstance(candidate, Mapping) else status_snapshot


def _copy_wa_snapshot_fields(result: dict[str, Any], status_snapshot: Mapping[str, Any]) -> None:
    for snapshot_key, value in status_snapshot.items():
        if snapshot_key == "raw":
            continue
        if snapshot_key == "ok":
            result["ok"] = bool(value)
            continue
        result[snapshot_key] = value


def _resolve_wa_state_and_qr(
    state_value: str | None,
    need_qr_flag: bool,
    qr_id_value: str | None,
    raw_snapshot: Mapping[str, Any] | None,
    deps: WaResponseDeps,
) -> tuple[str | None, bool, str | None]:
    derived_state, derived_need_qr = deps.derive_state_fn(raw_snapshot)
    if state_value is None:
        state_value = derived_state
    if not need_qr_flag:
        need_qr_flag = derived_need_qr
    if qr_id_value is None and raw_snapshot is not None:
        qr_id_value = deps.normalize_qr_id_fn(raw_snapshot.get("qr_id") or raw_snapshot.get("qrId"))
    if need_qr_flag and state_value != "qr":
        state_value = "qr"
    if state_value is not None:
        state_value = str(state_value)
    return state_value, need_qr_flag, qr_id_value


def _apply_wa_response_fields(
    result: dict[str, Any],
    *,
    tenant: int,
    key: str | None,
    state_value: str | None,
    need_qr_flag: bool,
    qr_id_value: str | None,
    deps: WaResponseDeps,
) -> None:
    result.setdefault("tenant", int(tenant))
    if state_value is not None:
        result["state"] = state_value
    elif "state" in result and result["state"] is None:
        result.pop("state", None)
    result["need_qr"] = bool(need_qr_flag)
    _apply_wa_qr_fields(result, tenant=tenant, key=key, qr_id_value=qr_id_value, need_qr_flag=need_qr_flag, deps=deps)


def _apply_wa_qr_fields(
    result: dict[str, Any],
    *,
    tenant: int,
    key: str | None,
    qr_id_value: str | None,
    need_qr_flag: bool,
    deps: WaResponseDeps,
) -> None:
    if qr_id_value is not None:
        result["qr_id"] = qr_id_value
    else:
        result.pop("qr_id", None)
    if key and qr_id_value:
        result["qr_url"] = deps.build_qr_url_fn(int(tenant), key, qr_id_value)
    elif key and need_qr_flag:
        result["qr_url"] = deps.build_qr_url_fn(int(tenant), key)
    else:
        result.pop("qr_url", None)


async def wa_qr_svg(
    request: Request,
    *,
    tenant: int,
    key: str,
    qr_id: str | None,
    deps: WaPublicDeps,
) -> Response:
    ok = deps.ensure_valid_qr_request_fn(tenant, key, request, query_param_only=True)
    if ok is None:
        response = deps.invalid_key_response_fn()
        return deps.as_head_response_fn(response, request)
    tenant_id, _ = ok
    provider = deps.common_module.whatsapp_provider(int(tenant_id))
    if provider == "baileys":
        response = deps.proxy_baileys_qr_fn(int(tenant_id))
        return deps.as_head_response_fn(response, request)
    requested_id = deps.normalize_qr_id_fn(qr_id) if qr_id is not None else None
    bypass_cache = _wa_qr_bypass_cache(request)
    requested_or_response = _resolve_wa_qr_id(int(tenant_id), requested_id, deps)
    if isinstance(requested_or_response, Response):
        return deps.as_head_response_fn(requested_or_response, request)
    requested_id = requested_or_response
    cached_or_response = _load_wa_cached_svg(int(tenant_id), requested_id, bypass_cache, deps)
    if isinstance(cached_or_response, Response):
        return deps.as_head_response_fn(cached_or_response, request)
    svg_value = cached_or_response
    if bypass_cache and not svg_value:
        upstream = await _fetch_wa_upstream_svg(int(tenant_id), requested_id, deps)
        if isinstance(upstream, Response):
            return deps.as_head_response_fn(upstream, request)
        requested_id, svg_value = upstream
    if not svg_value:
        response = deps.qr_expired_response_fn(requested_id)
        return deps.as_head_response_fn(response, request)
    response = _wa_svg_response(svg_value, requested_id, deps)
    return deps.as_head_response_fn(response, request)


def _wa_qr_bypass_cache(request: Request) -> bool:
    query_params = request.query_params
    if "t" in query_params:
        return True
    force_value = query_params.get("force")
    if force_value is None:
        return False
    return str(force_value).strip().lower() not in ("", "0", "false")


def _resolve_wa_qr_id(
    tenant_id: int,
    requested_id: str | None,
    deps: WaPublicDeps,
) -> str | None | Response:
    redis_failed = False
    if not requested_id:
        requested_id, redis_failed = deps.get_last_qr_id_fn(tenant_id)
    if not redis_failed:
        return requested_id
    headers = deps.no_store_headers_fn()
    if requested_id:
        headers["X-WA-QR-ID"] = str(requested_id)
    return JSONResponse({"error": "wa_cache_error"}, status_code=500, headers=headers)


def _load_wa_cached_svg(
    tenant_id: int,
    requested_id: str | None,
    bypass_cache: bool,
    deps: WaPublicDeps,
) -> str | None | Response:
    if not requested_id or bypass_cache:
        return None
    cached_svg, redis_failed = deps.load_cached_svg_fn(tenant_id, requested_id)
    if not redis_failed:
        return cached_svg
    headers = deps.no_store_headers_fn({"X-WA-QR-ID": str(requested_id)})
    return JSONResponse({"error": "wa_cache_error"}, status_code=500, headers=headers)


async def _fetch_wa_upstream_svg(
    tenant_id: int,
    requested_id: str | None,
    deps: WaPublicDeps,
) -> tuple[str | None, str] | Response:
    response = await _request_wa_upstream_svg(tenant_id, deps)
    if isinstance(response, Response):
        return response
    status_error = _wa_upstream_status_error(tenant_id, requested_id, response, deps)
    if status_error is not None:
        return status_error
    svg_value = response.text.strip()
    if not svg_value or not svg_value.lstrip().startswith("<svg"):
        deps.wa_logger.info("wa_qr_upstream_invalid tenant=%s", tenant_id)
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)
    upstream_qr_id = deps.normalize_qr_id_fn(
        response.headers.get("X-WA-QR-ID")
        or response.headers.get("X-Wa-Qr-Id")
        or requested_id
    )
    requested_id = upstream_qr_id or requested_id
    _cache_wa_upstream_svg(tenant_id, requested_id, svg_value, deps)
    return requested_id, svg_value


async def _request_wa_upstream_svg(
    tenant_id: int,
    deps: WaPublicDeps,
) -> Any | Response:
    fallback_headers: dict[str, str] = {}
    if getattr(deps.common_module, "WA_INTERNAL_TOKEN", ""):
        fallback_headers["X-Auth-Token"] = deps.common_module.WA_INTERNAL_TOKEN
    timeout = deps.httpx_module.Timeout(3.0, connect=2.0)
    try:
        async with deps.httpx_module.AsyncClient(timeout=timeout) as client:
            return await client.get(
                f"{deps.common_module.wa_base_url(tenant_id)}/session/{tenant_id}/qr.svg",
                headers=fallback_headers,
            )
    except deps.httpx_module.HTTPError as exc:
        deps.wa_logger.info(
            "wa_qr_upstream_error tenant=%s reason=%s",
            tenant_id,
            getattr(exc, "__class__", type(exc)).__name__,
        )
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)


def _wa_upstream_status_error(
    tenant_id: int,
    requested_id: str | None,
    response: Any,
    deps: WaPublicDeps,
) -> Response | None:
    status_code = int(response.status_code or 0)
    if status_code == 404:
        return deps.qr_expired_response_fn(requested_id)
    if 400 <= status_code < 500:
        deps.wa_logger.info("wa_qr_upstream_status tenant=%s status=%s", tenant_id, status_code)
        return deps.qr_expired_response_fn(requested_id)
    if status_code < 200 or status_code >= 300:
        deps.wa_logger.info("wa_qr_upstream_status tenant=%s status=%s", tenant_id, status_code)
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)
    return None


def _cache_wa_upstream_svg(
    tenant_id: int,
    requested_id: str | None,
    svg_value: str,
    deps: WaPublicDeps,
) -> None:
    if not requested_id:
        return
    try:
        deps.cache_qr_payload_fn(tenant_id, requested_id, {"qr_svg": svg_value}, include_last=True)
    except Exception:
        deps.wa_logger.info("wa_qr_cache_store_failed tenant=%s qr_id=%s", tenant_id, requested_id)


def _wa_svg_response(
    svg_value: str,
    requested_id: str | None,
    deps: WaPublicDeps,
) -> Response:
    headers = {"Content-Type": "image/svg+xml"}
    headers.update(deps.no_store_headers_fn())
    if requested_id:
        headers["X-WA-QR-ID"] = str(requested_id)
    return Response(content=svg_value, media_type="image/svg+xml", headers=headers)


async def wa_restart(
    request: Request,
    *,
    tenant: int,
    key: str,
    deps: WaRestartDeps,
) -> Response:
    ok = deps.ensure_valid_qr_request_fn(tenant, key, request)
    if ok is None:
        return deps.invalid_key_response_fn()
    tenant_id, _ = ok

    deps.wa_logger.info("wa_restart click tenant=%s", tenant_id)
    try:
        webhook = deps.common_module.webhook_url()
        provider = deps.common_module.whatsapp_provider(int(tenant_id))
        if provider == "baileys":
            return await _wa_restart_baileys(int(tenant_id), webhook, deps)
        return _wa_restart_legacy(int(tenant_id), webhook, deps)
    except Exception as exc:  # pragma: no cover
        try:
            deps.wa_logger.exception("wa_restart_failed: %s", exc)
        except Exception:
            pass
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)


async def _wa_restart_baileys(
    tenant_id: int,
    webhook: str,
    deps: WaRestartDeps,
) -> Response:
    start_payload = {"tenant": tenant_id, "webhookUrl": webhook, "force": True}
    response = await deps.common_module.wabaileys_post("/sessions/start", start_payload)
    if 200 <= response.status_code < 400:
        return JSONResponse({"ok": True})
    return JSONResponse({"error": "wa_unavailable"}, status_code=502)


def _wa_restart_legacy(
    tenant_id: int,
    webhook: str,
    deps: WaRestartDeps,
) -> Response:
    start_payload = deps.json_module.dumps(
        {"tenant_id": tenant_id, "webhook_url": webhook},
        ensure_ascii=False,
    ).encode("utf-8")
    empty_payload = deps.json_module.dumps({}, ensure_ascii=False).encode("utf-8")
    codes: dict[str, Any] = {}
    code_restart = _wa_http_code(
        tenant_id,
        f"/session/{tenant_id}/restart",
        start_payload,
        deps,
    )
    codes["tenant_restart"] = code_restart
    if _wa_restart_success(tenant_id, "tenant_restart", code_restart, deps):
        return JSONResponse({"ok": True})
    codes["tenant_logout"] = _wa_http_code(
        tenant_id,
        f"/session/{tenant_id}/logout",
        empty_payload,
        deps,
    )
    codes["tenant_start"] = _wa_http_code(
        tenant_id,
        f"/session/{tenant_id}/start",
        start_payload,
        deps,
    )
    if _wa_restart_success(tenant_id, "tenant_logout_start", codes["tenant_start"], deps, codes):
        return JSONResponse({"ok": True})
    codes["global_restart"] = _wa_http_code(None, "/session/restart", start_payload, deps)
    if _wa_restart_success(tenant_id, "global_restart", codes["global_restart"], deps):
        return JSONResponse({"ok": True})
    codes["global_start"] = _wa_http_code(None, "/session/start", start_payload, deps)
    if _wa_restart_success(tenant_id, "global_start", codes["global_start"], deps):
        return JSONResponse({"ok": True})
    deps.wa_logger.info("wa_restart failed tenant=%s codes=%s", tenant_id, codes)
    return JSONResponse({"error": "wa_unavailable"}, status_code=502)


def _wa_http_code(
    tenant_id: int | None,
    path: str,
    body: bytes,
    deps: WaRestartDeps,
) -> Any:
    code, _ = deps.common_module.http(
        "POST",
        f"{deps.common_module.wa_base_url(tenant_id)}{path}",
        body=body,
    )
    return code


def _wa_restart_success(
    tenant_id: int,
    stage: str,
    code: Any,
    deps: WaRestartDeps,
    codes: dict[str, Any] | None = None,
) -> bool:
    if not (200 <= int(code or 0) < 300):
        return False
    if codes and stage == "tenant_logout_start":
        deps.wa_logger.info(
            "wa_restart success tenant=%s stage=%s logout=%s start=%s",
            tenant_id,
            stage,
            codes.get("tenant_logout"),
            code,
        )
    else:
        deps.wa_logger.info("wa_restart success tenant=%s stage=%s code=%s", tenant_id, stage, code)
    return True
