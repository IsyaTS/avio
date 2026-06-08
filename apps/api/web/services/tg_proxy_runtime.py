from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from fastapi.responses import JSONResponse, Response


SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class TgProxyCallDeps:
    make_url_fn: SyncFn
    admin_headers_fn: SyncFn
    client_fn: SyncFn
    httpx_module: Any
    logger: Any
    worker_call_error_type: type[Exception]


def base_url(os_module: Any, settings_module: Any) -> str:
    candidates = [
        os_module.getenv("TG_WORKER_URL"),
        os_module.getenv("TGWORKER_URL"),
        getattr(settings_module, "TG_WORKER_URL", None),
        getattr(settings_module, "TGWORKER_BASE_URL", None),
        getattr(settings_module, "WORKER_BASE_URL", None),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        cleaned = str(candidate).strip()
        if cleaned:
            return cleaned.rstrip("/") or getattr(settings_module, "DEFAULT_WORKER_BASE_URL", "http://worker:8000")
    return getattr(settings_module, "DEFAULT_WORKER_BASE_URL", "http://worker:8000")


def resolve_base(current_base: str | None, base: str) -> str:
    return base if current_base != base else current_base


def make_url(path: str, *, base: str) -> str:
    if not path:
        return base
    lowered = path.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}"


def admin_headers(os_module: Any, settings_module: Any) -> dict[str, str]:
    token = (
        os_module.getenv("ADMIN_TOKEN")
        or os_module.getenv("TGWORKER_ADMIN_TOKEN")
        or getattr(settings_module, "ADMIN_TOKEN", "")
        or ""
    ).strip()
    return {"X-Admin-Token": token} if token else {}


def client(current_client: Any, httpx_module: Any) -> Any:
    if current_client is None or current_client.is_closed:
        return httpx_module.AsyncClient(timeout=httpx_module.Timeout(10.0))
    return current_client


async def call(
    method: str,
    path: str,
    *,
    params: Mapping[str, Any] | None,
    json_payload: Mapping[str, Any] | None,
    timeout: float,
    route: str | None,
    peer: Any | None,
    deps: TgProxyCallDeps,
) -> tuple[int, Any]:
    url = deps.make_url_fn(path)
    request_kwargs: dict[str, Any] = {
        "params": dict(params or {}),
        "headers": deps.admin_headers_fn(),
        "follow_redirects": False,
        "timeout": deps.httpx_module.Timeout(timeout),
    }
    if json_payload is not None:
        request_kwargs["json"] = dict(json_payload)
    try:
        response = await deps.client_fn().request(method.upper(), url, **request_kwargs)
    except deps.httpx_module.HTTPError as exc:
        detail = str(exc)
        deps.logger.warning(
            "event=tg_proxy_error route=%s url=%s status=error detail=%s",
            route or path,
            url,
            detail,
        )
        raise deps.worker_call_error_type(url, detail) from exc

    status_code = int(getattr(response, "status_code", 0) or 0)
    _log_tg_response(deps.logger, route or path, url, status_code, peer)
    return status_code, response


def _log_tg_response(logger: Any, route: str, url: str, status_code: int, peer: Any | None) -> None:
    peer_info = "-" if peer is None else str(peer)
    log_args = (route, url, status_code, peer_info)
    if status_code == 401:
        logger.warning(
            "event=tg_proxy_response route=%s url=%s status=%s peer=%s unauthorized",
            *log_args,
        )
        return
    logger.info("event=tg_proxy_response route=%s url=%s status=%s peer=%s", *log_args)


def stringify_detail(value: bytes | bytearray | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(value)


_JSON_DBL_PASSWORD = re.compile(r'("password"\s*:\s*")([^"\\]*)(")', re.IGNORECASE)
_JSON_SGL_PASSWORD = re.compile(r"('password'\s*:\s*')([^'\\]*)(')", re.IGNORECASE)
_QUERY_PASSWORD = re.compile(r"(password\s*=\s*)([^&\s]+)", re.IGNORECASE)


def mask_sensitive_detail(detail: str | None) -> str:
    if not detail:
        return ""
    masked = str(detail)
    masked = _JSON_DBL_PASSWORD.sub(lambda m: f"{m.group(1)}******{m.group(3)}", masked)
    masked = _JSON_SGL_PASSWORD.sub(lambda m: f"{m.group(1)}******{m.group(3)}", masked)
    return _QUERY_PASSWORD.sub(r"\1******", masked)


def extract_json_detail(body: bytes | bytearray | str | None, *, json_module: Any) -> str | None:
    payload = _json_payload(body, json_module=json_module)
    if isinstance(payload, dict):
        detail = payload.get("detail")
        if isinstance(detail, str):
            return detail
    return None


def _json_payload(body: bytes | bytearray | str | None, *, json_module: Any) -> Any:
    if body is None:
        return None
    payload: Any = body
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = payload.decode("utf-8")
        except Exception:
            return None
    if not isinstance(payload, str):
        return payload
    payload = payload.strip()
    if not payload:
        return None
    try:
        return json_module.loads(payload)
    except Exception:
        return None


def log_tg_proxy(
    logger: Any,
    route: str,
    tenant: int | str | None,
    status: int,
    body: bytes | bytearray | str | None,
    *,
    error: str | None,
    force: bool | None,
) -> None:
    detail_raw = error if error is not None else stringify_detail(body)
    detail = mask_sensitive_detail(detail_raw)
    log_fn = logger.info if 200 <= int(status or 0) < 300 else logger.warning
    tenant_value = "-" if tenant is None else tenant
    if route == "/pub/tg/password":
        log_fn("tg_proxy route=%s tenant=%s tg_code=%s", route, tenant_value, status)
        return
    force_fragment = " force=%s" % ("1" if force else "0") if force is not None else ""
    log_fn(
        "tg_proxy route=%s tenant=%s tg_code=%s%s detail=%s",
        route,
        tenant_value,
        status,
        force_fragment,
        detail or "",
    )


_UPSTREAM_HEADER_MAP = {"content-type": "Content-Type", "retry-after": "Retry-After"}


def passthrough_upstream_response(
    route: str,
    tenant_id: int | str | None,
    upstream: Any,
    *,
    no_store_headers_fn: SyncFn,
    log_tg_proxy_fn: SyncFn,
    success_content_type: str | None = "application/json",
    error_content_type: str | None = "application/json",
    include_no_store: bool = True,
    force: bool | None = None,
) -> Response:
    status_code = int(getattr(upstream, "status_code", 0) or 0)
    body_bytes = bytes(getattr(upstream, "content", b"") or b"")
    detail = _passthrough_error_detail(status_code, body_bytes, upstream)
    log_tg_proxy_fn(route, tenant_id, status_code, body_bytes, error=detail, force=force)
    if status_code <= 0:
        headers = no_store_headers_fn({"X-Telegram-Upstream-Status": "-"})
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers=headers)
    headers = _passthrough_headers(
        upstream,
        status_code,
        success_content_type=success_content_type,
        error_content_type=error_content_type,
        include_no_store=include_no_store,
        no_store_headers_fn=no_store_headers_fn,
    )
    return Response(content=body_bytes, status_code=status_code, headers=headers)


def _passthrough_error_detail(status_code: int, body_bytes: bytes, upstream: Any) -> str | None:
    if 200 <= status_code < 300:
        return None
    return stringify_detail(body_bytes) or stringify_detail(getattr(upstream, "text", "")) or f"status_{status_code}"


def _passthrough_headers(
    upstream: Any,
    status_code: int,
    *,
    success_content_type: str | None,
    error_content_type: str | None,
    include_no_store: bool,
    no_store_headers_fn: SyncFn,
) -> dict[str, str]:
    headers: dict[str, str] = {}
    if include_no_store:
        headers.update(no_store_headers_fn())
    headers["X-Telegram-Upstream-Status"] = str(status_code)
    _copy_allowed_upstream_headers(headers, getattr(upstream, "headers", {}) or {})
    default_content_type = success_content_type if 200 <= status_code < 300 else error_content_type
    if default_content_type and "Content-Type" not in headers:
        headers["Content-Type"] = default_content_type
    return headers


def _copy_allowed_upstream_headers(target: dict[str, str], upstream_headers: Mapping[str, str]) -> None:
    for name, value in upstream_headers.items():
        if not value:
            continue
        mapped = _UPSTREAM_HEADER_MAP.get(name.lower())
        if mapped:
            target[mapped] = value


def proxy_headers(headers: Mapping[str, str] | None, status_code: int, *, no_store_value: str) -> dict[str, str]:
    allowed = {"content-type", "cache-control"}
    result: dict[str, str] = {}
    for name, value in (headers or {}).items():
        if value and name.lower() in allowed:
            result[name] = value
    result["Cache-Control"] = no_store_value
    result["Pragma"] = "no-cache"
    result["Expires"] = "0"
    result["X-Telegram-Upstream-Status"] = str(status_code)
    return result
