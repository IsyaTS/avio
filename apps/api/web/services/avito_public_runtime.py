from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.parse import urlencode, urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, Response


SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class AvitoConnectDeps:
    common_module: Any
    avito_module: Any
    logger: Any
    render_template_fn: SyncFn
    quote_plus_fn: SyncFn


def state_key(state: str, *, prefix: str) -> str:
    return f"{prefix}{state}"


def state_secret(settings_module: Any) -> str:
    return (
        str(getattr(settings_module, "WEBHOOK_SECRET", "") or "").strip()
        or str(getattr(settings_module, "ADMIN_TOKEN", "") or "").strip()
        or str(getattr(settings_module, "AVITO_CLIENT_SECRET", "") or "").strip()
    )


def b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def b64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("ascii"))


def build_oauth_state(
    tenant_id: int,
    *,
    settings_module: Any,
    time_module: Any = time,
    secrets_module: Any = secrets,
) -> str:
    tenant_hex = f"{int(tenant_id):08x}"
    issued_hex = f"{int(time_module.time()):08x}"
    nonce_hex = secrets_module.token_hex(16)
    body = f"a1{tenant_hex}{issued_hex}{nonce_hex}"
    secret = state_secret(settings_module).encode("utf-8")
    signature = hmac.new(secret, body.encode("ascii"), hashlib.sha256).hexdigest()[:32]
    return f"{body}{signature}"


def state_cookie_domain(settings_module: Any) -> str | None:
    redirect_url = str(getattr(settings_module, "AVITO_REDIRECT_URL", "") or "").strip()
    try:
        host = urlsplit(redirect_url).hostname or ""
    except Exception:
        host = ""
    if host == "avio.website" or host.endswith(".avio.website"):
        return ".avio.website"
    return None


def oauth_public_origin(
    request: Request,
    *,
    settings_module: Any,
    public_base_url_fn: SyncFn,
) -> str:
    redirect_url = str(getattr(settings_module, "AVITO_REDIRECT_URL", "") or "").strip()
    try:
        parsed = urlsplit(redirect_url)
    except Exception:
        parsed = None
    if parsed and parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return public_base_url_fn(request).rstrip("/")


def oauth_redirect_entry_url(
    request: Request,
    tenant_id: int,
    key: str | None,
    *,
    settings_module: Any,
    public_base_url_fn: SyncFn,
) -> str:
    params: dict[str, Any] = {"tenant": int(tenant_id), "redirect": "1"}
    if key:
        params["k"] = str(key)
    origin = oauth_public_origin(
        request,
        settings_module=settings_module,
        public_base_url_fn=public_base_url_fn,
    )
    path = f"/v1/oauth/avito/authorize?{urlencode(params)}"
    return f"{origin}{path}" if origin else path


def set_state_cookie(
    response: Response,
    request: Request,
    state: str,
    *,
    settings_module: Any,
    cookie_name: str,
    ttl_seconds: int,
) -> None:
    forwarded_proto = str(getattr(request, "headers", {}).get("x-forwarded-proto") or "").lower()
    request_url = str(getattr(request, "url", "") or "")
    response.set_cookie(
        cookie_name,
        state,
        max_age=int(ttl_seconds),
        httponly=True,
        secure=forwarded_proto == "https" or request_url.startswith("https://"),
        samesite="lax",
        path="/",
        domain=state_cookie_domain(settings_module),
    )


def clear_state_cookie(
    response: Response,
    *,
    settings_module: Any,
    cookie_name: str,
) -> None:
    response.delete_cookie(cookie_name, path="/")
    cookie_domain = state_cookie_domain(settings_module)
    if cookie_domain:
        response.delete_cookie(cookie_name, path="/", domain=cookie_domain)


def verify_oauth_state(
    state: str,
    *,
    settings_module: Any,
    ttl_seconds: int,
    coerce_int_fn: SyncFn,
    time_module: Any = time,
) -> dict[str, Any] | None:
    hex_payload = _verify_hex_oauth_state(
        state,
        settings_module=settings_module,
        ttl_seconds=ttl_seconds,
        time_module=time_module,
    )
    if hex_payload is not None:
        return hex_payload
    return _verify_v1_oauth_state(
        state,
        settings_module=settings_module,
        ttl_seconds=ttl_seconds,
        coerce_int_fn=coerce_int_fn,
        time_module=time_module,
    )


def _verify_hex_oauth_state(
    state: str,
    *,
    settings_module: Any,
    ttl_seconds: int,
    time_module: Any,
) -> dict[str, Any] | None:
    if not state.startswith("a1") or len(state) != 82:
        return None
    if not all(char in "0123456789abcdefABCDEF" for char in state):
        return None
    body = state[:50].lower()
    signature = state[50:].lower()
    secret = state_secret(settings_module).encode("utf-8")
    expected = hmac.new(secret, body.encode("ascii"), hashlib.sha256).hexdigest()[:32]
    if not hmac.compare_digest(expected, signature):
        return None
    try:
        tenant_id = int(body[2:10], 16)
        issued_at = int(body[10:18], 16)
    except Exception:
        return None
    if _state_expired(issued_at, ttl_seconds=ttl_seconds, time_module=time_module):
        return None
    return {"tenant": tenant_id, "iat": issued_at}


def _verify_v1_oauth_state(
    state: str,
    *,
    settings_module: Any,
    ttl_seconds: int,
    coerce_int_fn: SyncFn,
    time_module: Any,
) -> dict[str, Any] | None:
    if not state.startswith("v1."):
        return None
    parts = state.split(".", 2)
    if len(parts) != 3:
        return None
    _, body, signature = parts
    secret = state_secret(settings_module).encode("utf-8")
    expected = b64url_encode(hmac.new(secret, body.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(expected, signature):
        return None
    payload = _decode_v1_payload(body)
    if payload is None:
        return None
    tenant_id = coerce_int_fn(payload.get("tenant"))
    issued_at = coerce_int_fn(payload.get("iat"))
    if tenant_id is None or issued_at is None:
        return None
    if _state_expired(issued_at, ttl_seconds=ttl_seconds, time_module=time_module):
        return None
    payload["tenant"] = tenant_id
    return payload


def _decode_v1_payload(body: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(b64url_decode(body).decode("utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _state_expired(issued_at: int, *, ttl_seconds: int, time_module: Any) -> bool:
    now = int(time_module.time())
    return issued_at > now + 60 or now - issued_at > int(ttl_seconds)


def delete_states_for_tenant(
    client: Any,
    tenant_id: int,
    *,
    prefix: str,
    coerce_int_fn: SyncFn,
    json_module: Any = json,
) -> int:
    deleted = 0
    try:
        keys = client.scan_iter(f"{prefix}*")
    except Exception:
        return deleted
    for key in keys:
        payload = _state_payload_from_redis(client, key, json_module=json_module)
        if isinstance(payload, Mapping) and coerce_int_fn(payload.get("tenant")) == int(tenant_id):
            deleted += _delete_redis_key(client, key)
    return deleted


def _state_payload_from_redis(client: Any, key: Any, *, json_module: Any) -> Any:
    try:
        raw_value = client.get(key)
    except Exception:
        return None
    if isinstance(raw_value, bytes):
        try:
            raw_value = raw_value.decode("utf-8")
        except Exception:
            return None
    if not isinstance(raw_value, str):
        return None
    try:
        return json_module.loads(raw_value)
    except Exception:
        return None


def _delete_redis_key(client: Any, key: Any) -> int:
    try:
        return int(client.delete(key) or 0)
    except Exception:
        return 0


def public_payload(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return {"connected": False}
    info = {
        "connected": bool(str(raw.get("access_token") or "").strip()),
        "expires_at": _optional_int(raw.get("expires_at")),
        "obtained_at": _optional_int(raw.get("obtained_at")),
    }
    scope = raw.get("scope")
    if isinstance(scope, str) and scope.strip():
        info["scope"] = scope.strip()
    account_id = raw.get("account_id")
    if account_id is not None:
        info["account_id"] = _coerce_account_id(account_id)
    return info


def connect_avito(
    tenant: int,
    request: Request,
    *,
    k: str | None,
    key: str | None,
    deps: AvitoConnectDeps,
) -> Response:
    tenant_id = int(tenant)
    access_key = _connect_access_key(request, k=k, key=key)
    if not deps.common_module.valid_key(tenant_id, access_key):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    deps.common_module.ensure_tenant_files(tenant_id)
    cfg = deps.common_module.read_tenant_config(tenant_id) or {}
    passport = cfg.get("passport", {}) if isinstance(cfg, dict) else {}
    avito_info = public_payload(deps.avito_module.get_integration(tenant_id))
    _enable_avito_auto_reply(tenant_id, cfg, deps=deps)
    resolved_key = _connect_resolved_key(tenant_id, access_key, deps=deps)
    return deps.render_template_fn(
        "connect/avito.html",
        _connect_context(
            request,
            tenant_id,
            resolved_key,
            access_key,
            passport,
            avito_info,
            deps=deps,
        ),
    )


def _connect_access_key(request: Request, *, k: str | None, key: str | None) -> str:
    return (k or key or request.query_params.get("k") or request.query_params.get("key") or "").strip()


def _enable_avito_auto_reply(tenant_id: int, cfg: Any, *, deps: AvitoConnectDeps) -> None:
    if not isinstance(cfg, dict):
        return
    behavior = cfg.setdefault("behavior", {})
    if not isinstance(behavior, dict):
        return
    changed = False
    for flag in ("auto_reply", "auto_reply_enabled"):
        if behavior.get(flag) is not True:
            behavior[flag] = True
            changed = True
    if not changed:
        return
    try:
        deps.common_module.write_tenant_config(tenant_id, cfg)
    except Exception:
        deps.logger.exception("avito_behavior_update_failed tenant=%s", tenant_id)


def _connect_resolved_key(tenant_id: int, access_key: str, *, deps: AvitoConnectDeps) -> str:
    primary_key = (deps.common_module.get_tenant_pubkey(tenant_id) or "").strip()
    return primary_key or access_key


def _connect_context(
    request: Request,
    tenant_id: int,
    resolved_key: str,
    access_key: str,
    passport: Any,
    avito_info: Mapping[str, Any],
    *,
    deps: AvitoConnectDeps,
) -> dict[str, Any]:
    passport_payload = passport if isinstance(passport, Mapping) else {}
    return {
        "request": request,
        "tenant": tenant_id,
        "key": resolved_key,
        "tenant_key": access_key,
        "subtitle": str(passport_payload.get("brand") or "").strip(),
        "passport": passport_payload,
        "avito": dict(avito_info),
        "settings_link": _settings_link(request, tenant_id, resolved_key, deps=deps),
    }


def _settings_link(
    request: Request,
    tenant_id: int,
    resolved_key: str,
    *,
    deps: AvitoConnectDeps,
) -> str:
    try:
        raw_settings = request.url_for("client_settings", tenant=str(tenant_id))
        if resolved_key:
            return deps.common_module.public_url(
                request,
                f"{raw_settings}?k={deps.quote_plus_fn(resolved_key)}",
            )
    except Exception:
        return ""
    return ""


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _coerce_account_id(value: Any) -> int | str:
    try:
        return int(value)
    except Exception:
        return str(value)
