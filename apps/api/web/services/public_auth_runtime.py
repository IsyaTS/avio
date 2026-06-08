from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class PublicAuthDeps:
    get_current_user_fn: AsyncFn
    coerce_tenant_fn: SyncFn
    resolve_public_settings_key_fn: SyncFn
    get_tenant_pubkey_fn: SyncFn
    list_keys_fn: SyncFn
    magic_link_enabled_fn: SyncFn
    valid_key_fn: SyncFn
    settings: Any


@dataclass(frozen=True)
class PublicAccessDeps:
    coerce_tenant_fn: SyncFn
    admin_token_valid_fn: SyncFn
    list_keys_fn: SyncFn
    get_tenant_pubkey_fn: SyncFn
    resolve_public_key_candidate_fn: SyncFn
    expected_public_key_value_fn: SyncFn
    valid_key_fn: SyncFn


def normalize_public_token(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def expected_public_key_value(settings: Any) -> str:
    env_public = normalize_public_token(os.getenv("PUBLIC_KEY"))
    if env_public:
        return env_public

    env_admin = normalize_public_token(os.getenv("ADMIN_TOKEN"))
    if env_admin:
        return env_admin

    return normalize_public_token(getattr(settings, "ADMIN_TOKEN", ""))


def resolve_public_key_candidate(
    key_candidate: str | None,
    request: Request | None = None,
    *,
    query_param_only: bool = False,
) -> str:
    candidate = normalize_public_token(key_candidate)
    if request is None:
        return candidate
    if query_param_only:
        return normalize_public_token(request.query_params.get("k"))
    if not candidate:
        return normalize_public_token(request.query_params.get("k"))
    return candidate


def ensure_public_key(
    key_candidate: str | None,
    request: Request | None,
    *,
    query_param_only: bool,
    expected_key_fn: SyncFn,
) -> str | None:
    candidate = resolve_public_key_candidate(
        key_candidate,
        request,
        query_param_only=query_param_only,
    )
    expected = expected_key_fn()
    if expected and candidate and candidate == expected:
        return candidate
    return None


def resolve_public_settings_key(request: Request, key_candidate: str | None) -> str:
    candidate = (key_candidate or "").strip()
    if candidate:
        return candidate
    query_value = (request.query_params.get("k") or "").strip()
    if query_value:
        return query_value
    cookies = getattr(request, "cookies", None) or {}
    return (cookies.get("client_key") or "").strip()


async def authorize_public_settings_request(
    request: Request,
    tenant: int | str | None,
    key_candidate: str | None,
    deps: PublicAuthDeps,
) -> tuple[int, str] | Response:
    user = await deps.get_current_user_fn(request)
    session_tenant = int(user.get("tenant_id") or 0) if isinstance(user, dict) else 0
    if session_tenant > 0:
        session_result = _authorize_session_tenant(
            request,
            tenant,
            key_candidate,
            session_tenant,
            deps,
        )
        if session_result is not None:
            return session_result

    try:
        tenant_id = deps.coerce_tenant_fn(tenant)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)

    resolved_key = deps.resolve_public_settings_key_fn(request, key_candidate)
    if not deps.magic_link_enabled_fn():
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    if not resolved_key or not deps.valid_key_fn(tenant_id, resolved_key):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)
    return tenant_id, resolved_key


def _authorize_session_tenant(
    request: Request,
    tenant: int | str | None,
    key_candidate: str | None,
    session_tenant: int,
    deps: PublicAuthDeps,
) -> tuple[int, str] | Response | None:
    if tenant is None or str(tenant).strip() == "":
        return session_tenant, _session_public_key(request, session_tenant, key_candidate, deps)
    try:
        tenant_id = deps.coerce_tenant_fn(tenant)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    if int(tenant_id) != int(session_tenant):
        return None
    return tenant_id, _session_public_key(request, tenant_id, key_candidate, deps)


def _session_public_key(
    request: Request,
    tenant_id: int,
    key_candidate: str | None,
    deps: PublicAuthDeps,
) -> str:
    resolved_key = deps.resolve_public_settings_key_fn(request, key_candidate)
    if resolved_key:
        return resolved_key
    resolved_key = (deps.get_tenant_pubkey_fn(int(tenant_id)) or "").strip()
    if resolved_key:
        return resolved_key
    keys = deps.list_keys_fn(int(tenant_id))
    return (keys[0].get("key") if keys else "") or ""


def ensure_valid_public_access(
    raw_tenant: int | str | None,
    raw_key: str | None,
    request: Request | None,
    *,
    query_param_only: bool,
    deps: PublicAccessDeps,
) -> tuple[int, str] | None:
    try:
        tenant_id = deps.coerce_tenant_fn(raw_tenant)
    except ValueError:
        return None

    if request is not None and deps.admin_token_valid_fn(request):
        return _tenant_key_from_config(tenant_id, "", deps)

    candidate = deps.resolve_public_key_candidate_fn(
        raw_key,
        request,
        query_param_only=query_param_only,
    )
    if not candidate:
        return None

    expected = deps.expected_public_key_value_fn()
    if expected and candidate == expected:
        return _tenant_key_from_config(tenant_id, candidate, deps)

    if deps.valid_key_fn(tenant_id, candidate):
        return _tenant_key_from_config(tenant_id, candidate, deps, fallback_to_config=False)

    return None


def _tenant_key_from_config(
    tenant_id: int,
    fallback: str,
    deps: PublicAccessDeps,
    *,
    fallback_to_config: bool = True,
) -> tuple[int, str]:
    items = deps.list_keys_fn(tenant_id)
    if items:
        return tenant_id, items[0].get("key", fallback) or fallback
    primary_key = (deps.get_tenant_pubkey_fn(tenant_id) or "").strip()
    if primary_key or fallback_to_config:
        return tenant_id, primary_key or fallback
    return tenant_id, fallback
