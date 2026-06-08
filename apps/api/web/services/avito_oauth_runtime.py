from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class AvitoOAuthDeps:
    authorize_public_settings_request_fn: AsyncFn
    coerce_int_fn: SyncFn
    avito_module: Any
    logger: Any
    common_module: Any
    json_module: Any
    redis_error_type: type[Exception]
    avito_state_ttl: int
    avito_state_cookie: str
    avito_state_key_fn: SyncFn
    build_avito_oauth_state_fn: SyncFn
    avito_oauth_redirect_entry_url_fn: SyncFn
    set_avito_state_cookie_fn: SyncFn
    avito_callback_html_fn: SyncFn
    clear_avito_state_cookie_fn: SyncFn
    verify_avito_oauth_state_fn: SyncFn
    resolve_tenant_from_state_fn: SyncFn
    build_token_update_payload_fn: SyncFn
    avito_token_payload_error: type[Exception]


async def oauth_status(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth

    tenant_id, _ = auth
    integration = deps.avito_module.get_integration(int(tenant_id)) or {}
    authorized = False
    if integration:
        try:
            _, integration = await deps.avito_module.ensure_access_token(int(tenant_id))
            authorized = True
        except deps.avito_module.AvitoOAuthError:
            authorized = False
        except Exception:
            deps.logger.exception("avito_oauth_status_failed tenant=%s", tenant_id)
            integration = deps.avito_module.get_integration(int(tenant_id)) or {}
    account_id = deps.coerce_int_fn(integration.get("account_id")) if integration else None
    account_login_raw = integration.get("account_login") if integration else None
    account_login = account_login_raw.strip() if isinstance(account_login_raw, str) and account_login_raw.strip() else None
    accounts = await _public_accounts(int(tenant_id), deps)

    access_token = str(integration.get("access_token") or "").strip()
    expires_value = deps.coerce_int_fn(integration.get("expires_at")) if integration else None
    configured_flag = bool(access_token)
    if not configured_flag and integration:
        configured_flag = deps.coerce_int_fn(integration.get("account_id")) is not None
    body = {
        "authorized": bool(authorized),
        "connected": bool(authorized),
        "configured": configured_flag,
        "expires_at": expires_value,
        "account_id": account_id,
        "account_login": account_login,
        "primary_account_id": account_id,
        "accounts": accounts,
    }
    return JSONResponse(body)


def _public_account(account: Any, deps: AvitoOAuthDeps) -> dict[str, Any]:
    data = dict(account or {})
    account_id = deps.coerce_int_fn(data.get("account_id"))
    login = data.get("account_login")
    display_name = data.get("display_name")
    return {
        "account_id": account_id,
        "account_login": login.strip() if isinstance(login, str) and login.strip() else None,
        "display_name": display_name.strip() if isinstance(display_name, str) and display_name.strip() else None,
        "is_primary": bool(data.get("is_primary")),
        "status": str(data.get("status") or "active"),
        "expires_at": deps.coerce_int_fn(data.get("expires_at")),
        "last_webhook_at": data.get("last_webhook_at").isoformat() if hasattr(data.get("last_webhook_at"), "isoformat") else data.get("last_webhook_at"),
        "webhook_status": "unknown",
    }


async def _public_accounts(tenant_id: int, deps: AvitoOAuthDeps) -> list[dict[str, Any]]:
    list_fn = getattr(deps.avito_module, "list_accounts", None)
    if not callable(list_fn):
        return []
    try:
        accounts = await list_fn(int(tenant_id), include_disconnected=True)
    except TypeError:
        accounts = await list_fn(int(tenant_id))
    except Exception:
        deps.logger.exception("avito_accounts_status_failed tenant=%s", tenant_id)
        return []
    return [_public_account(item, deps) for item in accounts]


async def oauth_authorize(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    redirect: bool,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth

    tenant_id, _ = auth
    state = deps.build_avito_oauth_state_fn(int(tenant_id))
    state_payload = deps.json_module.dumps({"tenant": tenant_id})
    state_key = deps.avito_state_key_fn(state)
    try:
        client = deps.common_module.redis_client()
        client.setex(state_key, deps.avito_state_ttl, state_payload)
    except deps.redis_error_type:
        deps.logger.exception("avito_oauth_state_store_failed tenant=%s", tenant_id)
        deps.logger.warning("avito_oauth_state_store_fallback tenant=%s", tenant_id)

    avito_authorize_url = deps.avito_module.build_authorize_url(state=state)
    deps.logger.info(
        "avito_oauth_authorize tenant=%s state=%s ttl=%s redirect=%s state_in_url=%s",
        tenant_id,
        state,
        deps.avito_state_ttl,
        redirect,
        True,
    )
    if redirect:
        response = RedirectResponse(avito_authorize_url, status_code=303)
        deps.set_avito_state_cookie_fn(response, request, state)
        return response
    return JSONResponse(
        {"authorize_url": deps.avito_oauth_redirect_entry_url_fn(request, int(tenant_id), key)}
    )


async def oauth_callback(
    request: Request,
    *,
    code: str | None,
    state: str | None,
    error: str | None,
    deps: AvitoOAuthDeps,
) -> Response:
    if error:
        deps.logger.warning("avito_oauth_callback_error error=%s state=%s", error, state)
        return HTMLResponse(deps.avito_callback_html_fn(False, error, {}))
    state_info = _resolve_callback_state(request, state, deps)
    if isinstance(state_info, Response):
        return state_info
    state, state_source = state_info
    raw_value = _pop_avito_oauth_state(state, deps)
    tenant_id = _tenant_from_avito_oauth_state(state, raw_value, state_source, deps)
    if tenant_id is None:
        return _avito_oauth_html(False, "invalid_state", {}, deps)

    if not code:
        deps.logger.warning("avito_oauth_missing_code tenant=%s state=%s", tenant_id, state)
        return _avito_oauth_html(False, "missing_code", {"tenant": tenant_id}, deps)

    token_payload = await _exchange_avito_oauth_code(int(tenant_id), code, deps)
    if isinstance(token_payload, Response):
        return token_payload
    store_result = await _store_avito_oauth_payload(int(tenant_id), token_payload, deps)
    if isinstance(store_result, Response):
        return store_result
    account_id = None
    if isinstance(store_result, dict):
        account_id = deps.coerce_int_fn(store_result.get("account_id"))
    await _sync_avito_oauth_side_effects(request, int(tenant_id), deps, account_id=account_id)
    return _avito_oauth_html(True, "ok", {"tenant": tenant_id}, deps)


def _resolve_callback_state(
    request: Request,
    state: str | None,
    deps: AvitoOAuthDeps,
) -> tuple[str, str] | Response:
    if state:
        return state, "query"
    cookie_state = (request.cookies.get(deps.avito_state_cookie) or "").strip()
    if cookie_state:
        return cookie_state, "cookie"
    deps.logger.warning("avito_oauth_callback_missing_state")
    return _avito_oauth_html(False, "missing_state", {}, deps)


def _pop_avito_oauth_state(
    state: str,
    deps: AvitoOAuthDeps,
) -> Any:
    client = None
    try:
        client = deps.common_module.redis_client()
    except deps.redis_error_type:
        deps.logger.exception("avito_oauth_state_fetch_failed state=%s", state)
    if client is None:
        return None
    state_key = deps.avito_state_key_fn(state)
    try:
        return client.get(state_key)
    except deps.redis_error_type:
        deps.logger.exception("avito_oauth_state_fetch_failed state=%s", state)
        return None
    finally:
        try:
            client.delete(state_key)
        except Exception:
            pass


def _tenant_from_avito_oauth_state(
    state: str,
    raw_value: Any,
    state_source: str,
    deps: AvitoOAuthDeps,
) -> int | None:
    tenant_id = deps.resolve_tenant_from_state_fn(
        raw_value=raw_value,
        state=state,
        verify_signed_state=deps.verify_avito_oauth_state_fn,
    )
    if tenant_id is None:
        deps.logger.warning(
            "avito_oauth_invalid_state state=%s state_source=%s raw_present=%s",
            state,
            state_source,
            raw_value is not None,
        )
    return tenant_id


async def _exchange_avito_oauth_code(
    tenant_id: int,
    code: str,
    deps: AvitoOAuthDeps,
) -> Any | Response:
    try:
        return await deps.avito_module.exchange_code_for_token(tenant_id, code)
    except deps.avito_module.AvitoOAuthError as exc:
        message = str(exc) or "token_exchange_failed"
        return _avito_oauth_html(False, message, {"tenant": tenant_id}, deps)
    except Exception:
        deps.logger.exception("avito_oauth_exchange_failed tenant=%s", tenant_id)
        return _avito_oauth_html(False, "token_exchange_failed", {"tenant": tenant_id}, deps)


async def _store_avito_oauth_payload(
    tenant_id: int,
    token_payload: Any,
    deps: AvitoOAuthDeps,
) -> dict[str, Any] | Response:
    try:
        update_payload = deps.build_token_update_payload_fn(token_payload)
    except deps.avito_token_payload_error:
        return _avito_oauth_html(False, "access_token_missing", {"tenant": tenant_id}, deps)
    try:
        deps.common_module.ensure_tenant_files(tenant_id)
        upsert_fn = getattr(deps.avito_module, "upsert_oauth_account_from_payload", None)
        if callable(upsert_fn):
            try:
                account = await upsert_fn(tenant_id, update_payload)
                if isinstance(account, dict):
                    return account
            except deps.avito_module.AvitoOAuthError:
                return deps.avito_module.update_integration(tenant_id, update_payload)
        else:
            return deps.avito_module.update_integration(tenant_id, update_payload)
        return dict(update_payload)
    except Exception:
        deps.logger.exception("avito_oauth_store_failed tenant=%s", tenant_id)
        return _avito_oauth_html(False, "token_store_failed", {"tenant": tenant_id}, deps)


async def _sync_avito_oauth_side_effects(
    request: Request,
    tenant_id: int,
    deps: AvitoOAuthDeps,
    *,
    account_id: int | None = None,
) -> None:
    if account_id is None:
        await _sync_avito_account_info(tenant_id, deps)
    await _ensure_avito_callback_webhook(request, tenant_id, deps, account_id=account_id)


async def _sync_avito_account_info(
    tenant_id: int,
    deps: AvitoOAuthDeps,
) -> None:
    try:
        await deps.avito_module.sync_account_info(tenant_id)
    except deps.avito_module.AvitoOAuthError as exc:
        deps.logger.warning("avito_account_sync_failed tenant=%s error=%s", tenant_id, exc)
    except Exception:
        deps.logger.exception("avito_account_sync_failed tenant=%s", tenant_id)


async def _ensure_avito_callback_webhook(
    request: Request,
    tenant_id: int,
    deps: AvitoOAuthDeps,
    *,
    account_id: int | None = None,
) -> None:
    try:
        target_url = deps.common_module.public_url(request, "/webhook/avito")
        success = await deps.avito_module.ensure_webhook(
            tenant_id,
            target_url,
            account_id=account_id,
        )
        if not success:
            deps.logger.warning(
                "avito_webhook_register_failed tenant=%s error=unexpected_response",
                tenant_id,
            )
    except deps.avito_module.AvitoOAuthError as exc:
        deps.logger.warning("avito_webhook_register_failed tenant=%s error=%s", tenant_id, exc)
    except Exception:
        deps.logger.exception("avito_webhook_register_failed tenant=%s", tenant_id)


def _avito_oauth_html(
    ok: bool,
    message: str,
    payload: dict[str, Any],
    deps: AvitoOAuthDeps,
) -> HTMLResponse:
    response = HTMLResponse(deps.avito_callback_html_fn(ok, message, payload))
    deps.clear_avito_state_cookie_fn(response)
    return response


async def oauth_disconnect(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth

    tenant_id, _ = auth
    account_id = None
    try:
        primary = await deps.avito_module.get_primary_account(int(tenant_id))
        account_id = deps.coerce_int_fn((primary or {}).get("account_id"))
    except Exception:
        primary = None
    try:
        target_url = deps.common_module.public_url(request, "/webhook/avito")
        await deps.avito_module.delete_webhook(int(tenant_id), target_url, account_id=account_id)
        legacy_url = deps.common_module.public_url(request, f"/webhook/avito?tenant={int(tenant_id)}")
        if legacy_url != target_url:
            await deps.avito_module.delete_webhook(int(tenant_id), legacy_url, account_id=account_id)
    except deps.avito_module.AvitoOAuthError:
        deps.logger.warning("avito_webhook_delete_failed tenant=%s reason=oauth", tenant_id)
    except Exception:
        deps.logger.exception("avito_webhook_delete_failed tenant=%s", tenant_id)
    try:
        deps.common_module.ensure_tenant_files(int(tenant_id))
    except Exception:
        deps.logger.exception("avito_oauth_disconnect_failed tenant=%s", tenant_id)
        return JSONResponse({"detail": "disconnect_failed"}, status_code=500)
    try:
        if account_id is not None:
            await deps.avito_module.disconnect_account(int(tenant_id), int(account_id))
        else:
            deps.avito_module.update_integration(
                int(tenant_id),
                {
                    "access_token": None,
                    "refresh_token": None,
                    "expires_at": None,
                    "obtained_at": None,
                    "account_id": None,
                    "account_login": None,
                },
            )
    except Exception:
        deps.logger.exception("avito_oauth_disconnect_failed tenant=%s", tenant_id)
        return JSONResponse({"detail": "disconnect_failed"}, status_code=500)
    return JSONResponse({"ok": True})


async def oauth_accounts(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    accounts = await _public_accounts(int(tenant_id), deps)
    return JSONResponse({"accounts": accounts})


async def oauth_account_primary(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    account_id: int,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    account = await deps.avito_module.set_primary_account(int(tenant_id), int(account_id))
    if not account:
        return JSONResponse({"detail": "account_not_found"}, status_code=404)
    return JSONResponse({"ok": True, "account": _public_account(account, deps)})


async def oauth_account_rename(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    account_id: int,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    display_name = str((payload or {}).get("display_name") or "").strip()
    if len(display_name) > 120:
        return JSONResponse({"detail": "display_name_too_long"}, status_code=400)
    update_fn = getattr(deps.avito_module, "update_account_display_name", None)
    if not callable(update_fn):
        return JSONResponse({"detail": "unsupported"}, status_code=501)
    account = await update_fn(int(tenant_id), int(account_id), display_name or None)
    if not account:
        return JSONResponse({"detail": "account_not_found"}, status_code=404)
    return JSONResponse({"ok": True, "account": _public_account(account, deps)})


async def oauth_account_disconnect(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    account_id: int,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    try:
        target_url = deps.common_module.public_url(request, "/webhook/avito")
        await deps.avito_module.delete_webhook(int(tenant_id), target_url, account_id=int(account_id))
    except Exception:
        deps.logger.warning("avito_webhook_delete_failed tenant=%s account_id=%s", tenant_id, account_id)
    account = await deps.avito_module.disconnect_account(int(tenant_id), int(account_id))
    if not account:
        return JSONResponse({"detail": "account_not_found"}, status_code=404)
    return JSONResponse({"ok": True})


async def oauth_account_webhook(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    account_id: int,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    target_url = deps.common_module.public_url(request, "/webhook/avito")
    try:
        success = await deps.avito_module.ensure_webhook(
            int(tenant_id),
            target_url,
            account_id=int(account_id),
        )
    except deps.avito_module.AvitoOAuthError as exc:
        return JSONResponse({"detail": "webhook_register_failed", "reason": str(exc)}, status_code=400)
    except Exception:
        deps.logger.exception(
            "avito_webhook_register_failed tenant=%s account_id=%s",
            tenant_id,
            account_id,
        )
        return JSONResponse({"detail": "webhook_register_failed"}, status_code=500)
    if not success:
        return JSONResponse({"detail": "webhook_register_failed"}, status_code=502)
    return JSONResponse({"ok": True})


async def oauth_webhook(
    request: Request,
    *,
    tenant: int | None,
    key: str | None,
    deps: AvitoOAuthDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth

    tenant_id, _ = auth
    try:
        await deps.avito_module.ensure_access_token(int(tenant_id))
    except deps.avito_module.AvitoOAuthError as exc:
        return JSONResponse(
            {"detail": "oauth_not_authorized", "reason": str(exc) or "oauth_error"},
            status_code=400,
        )
    except Exception:
        deps.logger.exception("avito_oauth_token_check_failed tenant=%s", tenant_id)
        return JSONResponse({"detail": "oauth_error"}, status_code=500)

    target_url = deps.common_module.public_url(request, "/webhook/avito")
    try:
        success = await deps.avito_module.ensure_webhook(int(tenant_id), target_url)
    except deps.avito_module.AvitoOAuthError as exc:
        deps.logger.warning("avito_webhook_register_failed tenant=%s error=%s", tenant_id, exc)
        return JSONResponse({"detail": "webhook_register_failed"}, status_code=400)
    except Exception:
        deps.logger.exception("avito_webhook_register_failed tenant=%s", tenant_id)
        return JSONResponse({"detail": "webhook_register_failed"}, status_code=500)

    if not success:
        return JSONResponse({"detail": "webhook_register_failed"}, status_code=502)
    return JSONResponse({"ok": True})
