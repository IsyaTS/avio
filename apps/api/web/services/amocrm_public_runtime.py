from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class AmoCRMPublicDeps:
    authorize_public_settings_request_fn: AsyncFn
    read_tenant_config_fn: SyncFn
    write_tenant_config_fn: SyncFn
    amocrm_service_module: Any
    amocrm_integration_module: Any
    amocrm_tokens_module: Any
    amocrm_chat_service_module: Any
    common_module: Any
    logger: Any
    uuid_module: Any
    time_module: Any
    urlencode_fn: SyncFn
    state_secret_fn: SyncFn
    httpx_module: Any
    os_module: Any
    json_module: Any
    datetime_cls: Any
    timezone_utc: Any
    timedelta_cls: Any
    quote_plus_fn: SyncFn
    no_store_headers_fn: SyncFn
    read_amocrm_webhook_payload_fn: AsyncFn
    extract_amocrm_uninstall_info_fn: SyncFn
    crm_chat_links_module: Any
    crm_links_module: Any
    db_module: Any
    get_lead_dialog_metadata_fn: AsyncFn
    get_lead_peer_fn: AsyncFn
    content_fingerprint_fn: SyncFn
    text_or_placeholder_fn: SyncFn
    redis_queue: Any
    settings_module: Any
    avito_bot_echo_key_fn: SyncFn
    avito_bot_echo_ttl_seconds: int
    normalize_echo_text_fn: SyncFn
    telegram_transport_module: Any
    insert_message_out_fn: AsyncFn
    capture_manager_intervention_fn: AsyncFn
    handoff_silence_key_fn: SyncFn
    handoff_silence_meta_key_fn: SyncFn
    handoff_silence_ttl_seconds: int
    redis_error_type: type[Exception]
    send_avito_fn: AsyncFn


@dataclass
class _AmoChatWebhookContext:
    request: Request
    payload: Mapping[str, Any]
    tenant_id: int
    cfg: Mapping[str, Any]
    message: Mapping[str, Any]
    text: str
    attachment_list: list[dict[str, Any]]
    external_message_id: str
    dedup_message_id: str
    link: Mapping[str, Any] | None = None
    lead_id: int = 0
    outbound_lead_id: int = 0
    outbound_meta: Mapping[str, Any] | None = None
    dialog_channel: str = ""
    peer_value: str = ""


async def oauth_start(
    request: Request,
    *,
    tenant_id: int | None,
    tenant: int | None,
    key: str | None,
    deps: AmoCRMPublicDeps,
) -> Response:
    tenant_val = tenant_id or tenant
    auth = await deps.authorize_public_settings_request_fn(request, tenant_val, key)
    if isinstance(auth, Response):
        return auth
    tenant_resolved, validated_key = auth
    cfg = deps.read_tenant_config_fn(tenant_resolved)
    amocrm_cfg = deps.amocrm_service_module.get_amocrm_cfg(cfg) or {}
    oauth_cfg = deps.amocrm_service_module.resolve_oauth_cfg(amocrm_cfg, int(tenant_resolved))
    client_id = str(oauth_cfg.get("client_id") or "").strip()
    client_secret = str(oauth_cfg.get("client_secret") or "").strip()
    if not client_id or not client_secret:
        return JSONResponse({"detail": "oauth_not_configured"}, status_code=400)
    auth_base_url = deps.amocrm_service_module.resolve_auth_url(amocrm_cfg, int(tenant_resolved))
    redirect_url = str(oauth_cfg.get("redirect_url") or "").strip()
    if not redirect_url:
        redirect_url = str(request.url_for("amocrm_oauth_callback"))
    state_payload = {
        "tenant_id": int(tenant_resolved),
        "k": validated_key,
        "nonce": deps.uuid_module.uuid4().hex,
        "ts": int(deps.time_module.time()),
    }
    state = deps.amocrm_integration_module.build_oauth_state(
        state_payload, deps.state_secret_fn()
    )
    params = {"client_id": client_id, "state": state, "mode": "post_message"}
    if redirect_url:
        params["redirect_uri"] = redirect_url
    authorize_url = f"{auth_base_url}/oauth?{deps.urlencode_fn(params)}"
    return RedirectResponse(authorize_url)


async def oauth_callback(
    request: Request,
    *,
    code: str | None,
    state: str | None,
    deps: AmoCRMPublicDeps,
) -> Response:
    if not code:
        return HTMLResponse("AmoCRM OAuth error: missing_code")
    payload = deps.amocrm_integration_module.verify_oauth_state(
        state or "", deps.state_secret_fn()
    )
    if not payload:
        return HTMLResponse("AmoCRM OAuth error: invalid_state")
    tenant_id = payload.get("tenant_id")
    key = payload.get("k")
    auth = await deps.authorize_public_settings_request_fn(request, tenant_id, key)
    if isinstance(auth, Response):
        return auth
    tenant_resolved, validated_key = auth
    cfg = deps.read_tenant_config_fn(tenant_resolved)
    amocrm_cfg = deps.amocrm_service_module.get_amocrm_cfg(cfg) or {}
    oauth_cfg = deps.amocrm_service_module.resolve_oauth_cfg(amocrm_cfg, int(tenant_resolved))
    if not _amocrm_oauth_credentials_present(oauth_cfg):
        return HTMLResponse("AmoCRM OAuth error: oauth_not_configured")
    base_url = deps.amocrm_service_module.resolve_base_url(amocrm_cfg, int(tenant_resolved))
    auth_base_url, subdomain_hint = _resolve_callback_auth_base(
        request,
        amocrm_cfg,
        int(tenant_resolved),
        deps,
    )
    token_payload = await _exchange_amocrm_oauth_token(
        code,
        oauth_cfg,
        base_url,
        auth_base_url,
        deps,
    )
    if isinstance(token_payload, Response):
        return token_payload
    store_error = await _store_amocrm_oauth_token(int(tenant_resolved), token_payload, deps)
    if store_error is not None:
        return store_error
    cfg, amocrm_cfg = _enable_amocrm_oauth_config(
        int(tenant_resolved),
        cfg,
        token_payload,
        subdomain_hint,
        deps,
    )
    await _sync_amocrm_oauth_account(
        request,
        int(tenant_resolved),
        cfg,
        amocrm_cfg,
        token_payload,
        base_url,
        deps,
    )
    redirect_url = request.url_for("client_settings", tenant=str(tenant_resolved))
    redirect = deps.common_module.public_url(
        request,
        f"{redirect_url}?k={deps.quote_plus_fn(str(validated_key))}#/channels?amocrm=1",
    )
    return RedirectResponse(redirect)


def _amocrm_oauth_credentials_present(oauth_cfg: Mapping[str, Any]) -> bool:
    return bool(
        str(oauth_cfg.get("client_id") or "").strip()
        and str(oauth_cfg.get("client_secret") or "").strip()
    )


def _resolve_callback_auth_base(
    request: Request,
    amocrm_cfg: Mapping[str, Any],
    tenant_id: int,
    deps: AmoCRMPublicDeps,
) -> tuple[str, str]:
    auth_base_url = deps.amocrm_service_module.resolve_auth_url(amocrm_cfg, tenant_id)
    subdomain_hint = (
        request.query_params.get("referer")
        or request.query_params.get("account")
        or request.query_params.get("subdomain")
        or request.headers.get("referer")
        or request.headers.get("origin")
        or ""
    )
    subdomain_hint = deps.amocrm_service_module._extract_subdomain(str(subdomain_hint))
    if subdomain_hint:
        auth_base_url = f"https://{subdomain_hint}.amocrm.ru"
    return auth_base_url, subdomain_hint


async def _exchange_amocrm_oauth_token(
    code: str,
    oauth_cfg: Mapping[str, Any],
    base_url: str,
    auth_base_url: str,
    deps: AmoCRMPublicDeps,
) -> Mapping[str, Any] | Response:
    payload_data = _amocrm_oauth_token_payload(code, oauth_cfg)
    token_url = f"{auth_base_url}/oauth2/access_token"
    fallback_url = f"{base_url}/oauth2/access_token" if base_url else ""
    async with deps.httpx_module.AsyncClient(timeout=10.0) as client:
        response = await client.post(token_url, json=payload_data)
        if _should_retry_amocrm_token_exchange(response, fallback_url, auth_base_url, base_url):
            response = await client.post(fallback_url, json=payload_data)
    if response.status_code >= 400:
        return HTMLResponse(
            f"AmoCRM OAuth error: token_exchange_failed{_token_exchange_error_suffix(response)}"
        )
    try:
        token_payload = response.json()
    except json.JSONDecodeError:
        return HTMLResponse("AmoCRM OAuth error: token_invalid_json")
    return token_payload if isinstance(token_payload, Mapping) else {}


def _amocrm_oauth_token_payload(
    code: str,
    oauth_cfg: Mapping[str, Any],
) -> dict[str, Any]:
    payload_data = {
        "client_id": str(oauth_cfg.get("client_id") or "").strip(),
        "client_secret": str(oauth_cfg.get("client_secret") or "").strip(),
        "grant_type": "authorization_code",
        "code": code,
    }
    redirect_url = str(oauth_cfg.get("redirect_url") or "").strip()
    if redirect_url:
        payload_data["redirect_uri"] = redirect_url
    return payload_data


def _should_retry_amocrm_token_exchange(
    response: Any,
    fallback_url: str,
    auth_base_url: str,
    base_url: str,
) -> bool:
    return bool(
        response.status_code >= 400
        and fallback_url
        and auth_base_url.rstrip("/") != base_url.rstrip("/")
    )


def _token_exchange_error_suffix(response: Any) -> str:
    detail = ""
    try:
        data = response.json()
        if isinstance(data, Mapping):
            detail = str(
                data.get("hint")
                or data.get("detail")
                or data.get("title")
                or data.get("error")
                or ""
            ).strip()
    except Exception:
        detail = (response.text or "").strip()
    detail = detail[:200]
    return f": {detail}" if detail else ""


async def _store_amocrm_oauth_token(
    tenant_id: int,
    token_payload: Mapping[str, Any],
    deps: AmoCRMPublicDeps,
) -> Response | None:
    access_token = str(token_payload.get("access_token") or "").strip()
    refresh_token = str(token_payload.get("refresh_token") or "").strip()
    expires_at = _amocrm_oauth_expires_at(token_payload, deps)
    obtained_at = deps.datetime_cls.now(tz=deps.timezone_utc)
    try:
        await deps.amocrm_tokens_module.ensure_schema()
        await deps.amocrm_tokens_module.upsert(
            tenant_id,
            access_token=access_token,
            refresh_token=refresh_token or None,
            expires_at=expires_at,
            obtained_at=obtained_at,
            raw_payload=token_payload,
        )
        return None
    except Exception:
        deps.logger.exception("amocrm_oauth_token_store_failed tenant=%s", tenant_id)
        return HTMLResponse("AmoCRM OAuth error: token_store_failed")


def _amocrm_oauth_expires_at(
    token_payload: Mapping[str, Any],
    deps: AmoCRMPublicDeps,
) -> Any:
    expires_in = token_payload.get("expires_in")
    if not isinstance(expires_in, (int, float)):
        return None
    return deps.datetime_cls.now(tz=deps.timezone_utc) + deps.timedelta_cls(
        seconds=int(expires_in)
    )


def _enable_amocrm_oauth_config(
    tenant_id: int,
    cfg: Any,
    token_payload: Mapping[str, Any],
    subdomain_hint: str,
    deps: AmoCRMPublicDeps,
) -> tuple[Any, Mapping[str, Any]]:
    if not isinstance(cfg, dict):
        return cfg, {}
    integrations = cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg = integrations.get("amocrm")
    if not isinstance(amocrm_cfg, dict):
        amocrm_cfg = {}
    amocrm_cfg["enabled"] = True
    amocrm_cfg["mode"] = "oauth"
    api_domain = token_payload.get("api_domain")
    if api_domain:
        amocrm_cfg["api_domain"] = api_domain
    if subdomain_hint:
        amocrm_cfg["subdomain"] = amocrm_cfg.get("subdomain") or subdomain_hint
    integrations["amocrm"] = amocrm_cfg
    cfg["integrations"] = integrations
    cfg = deps.amocrm_chat_service_module.ensure_chat_cfg_in_tenant(cfg, tenant_id) or cfg
    deps.write_tenant_config_fn(tenant_id, cfg)
    return cfg, amocrm_cfg


async def _sync_amocrm_oauth_account(
    request: Request,
    tenant_id: int,
    cfg: Any,
    amocrm_cfg: Mapping[str, Any],
    token_payload: Mapping[str, Any],
    base_url: str,
    deps: AmoCRMPublicDeps,
) -> None:
    try:
        api_base = _amocrm_oauth_api_base(token_payload, base_url)
        if not api_base:
            return
        oauth_cfg = deps.amocrm_service_module.resolve_oauth_cfg(amocrm_cfg, tenant_id)
        client = deps.amocrm_integration_module.AmoCRMClient(
            tenant_id=tenant_id,
            base_url=api_base,
            client_id=str(oauth_cfg.get("client_id") or ""),
            client_secret=str(oauth_cfg.get("client_secret") or ""),
            redirect_url=str(oauth_cfg.get("redirect_url") or ""),
        )
        account_payload = await client.get_account()
        if isinstance(account_payload, Mapping):
            cfg = await _persist_amocrm_account_metadata(
                request,
                tenant_id,
                cfg,
                account_payload,
                client,
                deps,
            )
            del cfg
    except Exception:
        deps.logger.exception("amocrm_account_fetch_failed tenant=%s", tenant_id)


def _amocrm_oauth_api_base(
    token_payload: Mapping[str, Any],
    base_url: str,
) -> str:
    api_domain = str(token_payload.get("api_domain") or "").strip()
    return base_url or (f"https://{api_domain}" if api_domain else "")


async def _persist_amocrm_account_metadata(
    request: Request,
    tenant_id: int,
    cfg: Any,
    account_payload: Mapping[str, Any],
    client: Any,
    deps: AmoCRMPublicDeps,
) -> Any:
    if not isinstance(cfg, dict):
        return cfg
    integrations = cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg = integrations.get("amocrm")
    if not isinstance(amocrm_cfg, dict):
        amocrm_cfg = {}
    _apply_amocrm_account_fields(amocrm_cfg, account_payload)
    integrations["amocrm"] = amocrm_cfg
    cfg["integrations"] = integrations
    cfg = deps.amocrm_chat_service_module.ensure_chat_cfg_in_tenant(cfg, tenant_id) or cfg
    deps.write_tenant_config_fn(tenant_id, cfg)
    await deps.amocrm_service_module.ensure_pipeline_config(tenant_id, cfg, client)
    await deps.amocrm_service_module.ensure_lead_phone_field_id(tenant_id, cfg, client)
    await _ensure_amocrm_chat_connected_after_oauth(request, tenant_id, cfg, deps)
    return cfg


def _apply_amocrm_account_fields(
    amocrm_cfg: dict[str, Any],
    account_payload: Mapping[str, Any],
) -> None:
    amocrm_cfg["enabled"] = True
    amocrm_cfg["mode"] = "oauth"
    account_id = account_payload.get("id")
    subdomain = account_payload.get("subdomain")
    if account_id is not None:
        amocrm_cfg["account_id"] = account_id
    if subdomain:
        amocrm_cfg["subdomain"] = amocrm_cfg.get("subdomain") or subdomain


async def _ensure_amocrm_chat_connected_after_oauth(
    request: Request,
    tenant_id: int,
    cfg: Mapping[str, Any],
    deps: AmoCRMPublicDeps,
) -> None:
    try:
        await deps.amocrm_chat_service_module.ensure_connected(
            tenant_id,
            cfg=cfg,
            webhook_base_url=str(deps.common_module.public_base_url(request) or "").rstrip("/"),
        )
    except Exception:
        deps.logger.exception("amocrm_chat_connect_after_oauth_failed tenant=%s", tenant_id)


async def oauth_status(
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: AmoCRMPublicDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    cfg = deps.read_tenant_config_fn(tenant_id)
    amocrm_cfg = deps.amocrm_service_module.get_amocrm_cfg(cfg) or {}
    entry = await deps.amocrm_tokens_module.get(int(tenant_id))
    connected, expires_at_ts, base_url = await _amocrm_oauth_connection_status(
        int(tenant_id),
        amocrm_cfg,
        entry,
        deps,
    )
    pipelines_cache = await _amocrm_status_pipeline_cache(
        int(tenant_id),
        cfg,
        amocrm_cfg,
        entry,
        connected,
        base_url,
        deps,
    )

    payload = {
        "connected": connected,
        "expires_at": expires_at_ts,
        "last_error": entry.last_error if entry else None,
        "chat": deps.amocrm_chat_service_module.mask_chat_cfg(cfg, tenant_id),
        "pipelines": pipelines_cache,
    }
    payload["chat"]["connected"] = bool(payload["chat"].get("scope_id"))
    if (
        payload["chat"].get("enabled")
        or payload["chat"].get("env_configured")
        or payload["chat"].get("channel_id")
    ):
        payload["chat"]["webhook_url"] = (
            deps.amocrm_chat_service_module.build_webhook_url(
                str(deps.common_module.public_base_url(request) or "").rstrip("/"),
                cfg,
                tenant_id,
            )
        )
    return JSONResponse(payload, headers=deps.no_store_headers_fn())


async def _amocrm_oauth_connection_status(
    tenant_id: int,
    amocrm_cfg: Mapping[str, Any],
    entry: Any,
    deps: AmoCRMPublicDeps,
) -> tuple[bool, int | None, str | None]:
    connected = bool(entry and entry.access_token)
    expires_at_ts = int(entry.expires_at.timestamp()) if entry and entry.expires_at else None
    base_url = None
    if entry and entry.expires_at and entry.expires_at <= deps.datetime_cls.now(tz=deps.timezone_utc):
        base_url = await _resolve_amocrm_status_base_url(tenant_id, amocrm_cfg, entry, deps)
        if base_url and entry.refresh_token:
            connected, expires_at_ts = await _refresh_amocrm_status_token(
                tenant_id,
                amocrm_cfg,
                entry,
                base_url,
                expires_at_ts,
                deps,
            )
        else:
            connected = False
    if connected and amocrm_cfg and not base_url:
        base_url = await _resolve_amocrm_status_base_url(tenant_id, amocrm_cfg, entry, deps)
    return connected, expires_at_ts, base_url


async def _resolve_amocrm_status_base_url(
    tenant_id: int,
    amocrm_cfg: Mapping[str, Any],
    entry: Any,
    deps: AmoCRMPublicDeps,
) -> str | None:
    if not amocrm_cfg:
        return None
    try:
        return await deps.amocrm_service_module.resolve_api_base_url(amocrm_cfg, tenant_id, entry)
    except Exception:
        return None


async def _refresh_amocrm_status_token(
    tenant_id: int,
    amocrm_cfg: Mapping[str, Any],
    entry: Any,
    base_url: str,
    expires_at_ts: int | None,
    deps: AmoCRMPublicDeps,
) -> tuple[bool, int | None]:
    del entry
    oauth_cfg = deps.amocrm_service_module.resolve_oauth_cfg(amocrm_cfg or {}, tenant_id)
    client = deps.amocrm_service_module.amocrm_core.AmoCRMClient(
        tenant_id=tenant_id,
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )
    try:
        refreshed = await client.refresh_tokens()
        if refreshed and refreshed.expires_at:
            expires_at_ts = int(refreshed.expires_at.timestamp())
        return bool(refreshed and refreshed.access_token), expires_at_ts
    except Exception:
        return False, expires_at_ts


async def _amocrm_status_pipeline_cache(
    tenant_id: int,
    cfg: Mapping[str, Any],
    amocrm_cfg: Mapping[str, Any],
    entry: Any,
    connected: bool,
    base_url: str | None,
    deps: AmoCRMPublicDeps,
) -> list[dict[str, Any]]:
    del entry
    pipelines_cache = _cached_amocrm_pipeline_options(amocrm_cfg)
    if connected and not pipelines_cache and amocrm_cfg and base_url:
        pipelines_cache = await _fetch_and_cache_amocrm_status_pipelines(
            tenant_id,
            cfg,
            amocrm_cfg,
            base_url,
            deps,
        )
    return pipelines_cache


def _cached_amocrm_pipeline_options(
    amocrm_cfg: Mapping[str, Any],
) -> list[dict[str, Any]]:
    raw = amocrm_cfg.get("pipelines_cache")
    return _amocrm_pipeline_options(raw if isinstance(raw, list) else [])


async def _fetch_and_cache_amocrm_status_pipelines(
    tenant_id: int,
    cfg: Mapping[str, Any],
    amocrm_cfg: Mapping[str, Any],
    base_url: str,
    deps: AmoCRMPublicDeps,
) -> list[dict[str, Any]]:
    try:
        oauth_cfg = deps.amocrm_service_module.resolve_oauth_cfg(amocrm_cfg, tenant_id)
        client = deps.amocrm_service_module.amocrm_core.AmoCRMClient(
            tenant_id=tenant_id,
            base_url=base_url,
            client_id=str(oauth_cfg.get("client_id") or ""),
            client_secret=str(oauth_cfg.get("client_secret") or ""),
            redirect_url=str(oauth_cfg.get("redirect_url") or ""),
        )
        pipelines_payload = await client.get_pipelines()
        pipelines = deps.amocrm_service_module._extract_embedded_list(
            pipelines_payload,
            "pipelines",
        )
        options = _amocrm_pipeline_options(pipelines)
        if options:
            _save_amocrm_status_pipeline_cache(tenant_id, cfg, amocrm_cfg, options, deps)
        return options
    except Exception:
        deps.logger.exception("amocrm_status_pipelines_fetch_failed tenant=%s", tenant_id)
        return []


def _save_amocrm_status_pipeline_cache(
    tenant_id: int,
    cfg: Mapping[str, Any],
    amocrm_cfg: Mapping[str, Any],
    pipelines_cache: list[dict[str, Any]],
    deps: AmoCRMPublicDeps,
) -> None:
    updated_cfg = dict(cfg) if isinstance(cfg, Mapping) else {}
    integrations = updated_cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg_copy = dict(amocrm_cfg)
    amocrm_cfg_copy["pipelines_cache"] = pipelines_cache
    amocrm_cfg_copy["pipelines_cached_at"] = int(deps.time_module.time())
    integrations["amocrm"] = amocrm_cfg_copy
    updated_cfg["integrations"] = integrations
    deps.write_tenant_config_fn(tenant_id, updated_cfg)


async def chat_webhook(
    request: Request,
    *,
    token: str | None = None,
    scope_id: str | None = None,
    deps: AmoCRMPublicDeps,
) -> Response:
    payload = await deps.read_amocrm_webhook_payload_fn(request)
    _log_chat_webhook_received(payload, token, scope_id, deps)
    tenant_id = await _resolve_chat_webhook_tenant(payload, token, scope_id, deps)
    if tenant_id is None:
        return JSONResponse({"ok": False, "detail": "tenant_not_found"}, status_code=404)
    cfg = deps.read_tenant_config_fn(int(tenant_id))
    if _chat_webhook_token_invalid(cfg, int(tenant_id), token, deps):
        return JSONResponse({"ok": False, "detail": "invalid_token"}, status_code=403)
    message = deps.amocrm_chat_service_module.extract_webhook_message(payload)
    _log_chat_webhook_message(int(tenant_id), message, deps)
    if str(message.get("event_type") or "").strip().lower() not in {"", "new_message"}:
        return JSONResponse({"ok": True, "skipped": "unsupported_event"})
    ctx = _build_chat_webhook_context(request, payload, int(tenant_id), cfg, message, deps)
    if await _chat_webhook_seen_dedup(ctx, deps):
        return JSONResponse({"ok": True, "dedup": True})
    if not ctx.text and not ctx.attachment_list:
        _log_chat_webhook_empty(ctx, deps)
        return JSONResponse({"ok": True, "skipped": "empty_text"})
    ctx.link = await _find_chat_webhook_link(ctx, deps)
    if not ctx.link:
        return JSONResponse({"ok": False, "detail": "chat_link_not_found"}, status_code=404)
    if str(ctx.link.get("last_outbound_message_id") or "").strip() == ctx.dedup_message_id:
        return JSONResponse({"ok": True, "dedup": True})
    await _resolve_chat_webhook_route(ctx, deps)
    await _preflag_chat_webhook_echo(ctx, deps)
    await _cache_chat_webhook_avito_echo(ctx, deps)
    status_code, body = await _send_chat_webhook_message(ctx, deps)
    if status_code < 200 or status_code >= 300:
        deps.logger.warning(
            "amocrm_chat_webhook_send_failed tenant=%s lead_id=%s status=%s body=%s",
            ctx.tenant_id,
            ctx.lead_id,
            status_code,
            body[:300],
        )
        return JSONResponse(
            {
                "ok": False,
                "detail": "telegram_send_failed",
                "status_code": status_code,
                "body": body[:300],
            },
            status_code=502,
        )
    await _store_chat_webhook_manager_message(ctx, deps)
    await _evaluate_chat_webhook_stage(ctx, deps)
    await _set_chat_webhook_handoff(ctx, deps)
    await _mark_chat_webhook_echo_and_touch(ctx, deps)
    return JSONResponse({"ok": True})


def _log_chat_webhook_received(
    payload: Mapping[str, Any],
    token: str | None,
    scope_id: str | None,
    deps: AmoCRMPublicDeps,
) -> None:
    try:
        deps.logger.info(
            "amocrm_chat_webhook_received keys=%s token_present=%s scope_present=%s",
            sorted(payload.keys()) if isinstance(payload, Mapping) else [],
            int(bool(str(token or "").strip())),
            int(bool(str(scope_id or "").strip())),
        )
    except Exception:
        pass


async def _resolve_chat_webhook_tenant(
    payload: Mapping[str, Any],
    token: str | None,
    scope_id: str | None,
    deps: AmoCRMPublicDeps,
) -> int | None:
    tenant_id = deps.amocrm_chat_service_module.find_tenant_by_webhook_token(token)
    if tenant_id is None:
        tenant_id = deps.amocrm_chat_service_module.find_tenant_by_scope_id(scope_id)
    if tenant_id is None and scope_id:
        scope_link = await deps.crm_chat_links_module.find_by_scope_id(
            deps.amocrm_chat_service_module.AMOCRM_CHAT_PROVIDER,
            scope_id,
        )
        if scope_link:
            tenant_id = int(scope_link.get("tenant_id") or 0) or None
    if tenant_id is None:
        account_id, subdomain = deps.extract_amocrm_uninstall_info_fn(payload)
        tenant_id = deps.amocrm_service_module.find_tenant_by_account(account_id, subdomain)
    return int(tenant_id) if tenant_id is not None else None


def _chat_webhook_token_invalid(
    cfg: Mapping[str, Any],
    tenant_id: int,
    token: str | None,
    deps: AmoCRMPublicDeps,
) -> bool:
    expected = deps.amocrm_chat_service_module.build_webhook_path_token(cfg, int(tenant_id))
    token_value = str(token or "").strip()
    return bool(expected and token_value and token_value != expected)


def _log_chat_webhook_message(
    tenant_id: int,
    message: Mapping[str, Any],
    deps: AmoCRMPublicDeps,
) -> None:
    deps.logger.info(
        "amocrm_chat_webhook_message tenant=%s event=%s chat=%s conversation=%s message=%s text_len=%s",
        tenant_id,
        str(message.get("event_type") or ""),
        str(message.get("external_chat_id") or ""),
        str(message.get("external_conversation_id") or ""),
        str(message.get("external_message_id") or ""),
        len(str(message.get("text") or "")),
    )


def _build_chat_webhook_context(
    request: Request,
    payload: Mapping[str, Any],
    tenant_id: int,
    cfg: Mapping[str, Any],
    message: Mapping[str, Any],
    deps: AmoCRMPublicDeps,
) -> _AmoChatWebhookContext:
    text = str(message.get("text") or "").strip()
    attachments = message.get("attachments") if isinstance(message, Mapping) else None
    attachment_list = [att for att in (attachments or []) if isinstance(att, dict)]
    external_message_id = str(message.get("external_message_id") or "").strip()
    dedup_message_id = external_message_id or f"fp:{deps.content_fingerprint_fn(text, attachment_list)}"
    return _AmoChatWebhookContext(
        request=request,
        payload=payload,
        tenant_id=int(tenant_id),
        cfg=cfg,
        message=message,
        text=text,
        attachment_list=attachment_list,
        external_message_id=external_message_id,
        dedup_message_id=dedup_message_id,
    )


async def _chat_webhook_seen_dedup(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> bool:
    if deps.redis_queue is None:
        return False
    try:
        dedup_scope = (
            str(ctx.message.get("external_conversation_id") or "").strip()
            or str(ctx.message.get("external_chat_id") or "").strip()
            or "global"
        )
        dedup_ttl = 86400 if ctx.external_message_id else 180
        dedup_key = f"amocrm:chat:webhook:{ctx.tenant_id}:{dedup_scope}:{ctx.dedup_message_id}"
        accepted = await deps.redis_queue.set(dedup_key, "1", ex=dedup_ttl, nx=True)
        return not bool(accepted)
    except Exception:
        deps.logger.debug(
            "amocrm_chat_webhook_dedup_check_failed tenant=%s",
            ctx.tenant_id,
            exc_info=True,
        )
        return False


def _log_chat_webhook_empty(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> None:
    try:
        raw_message = ctx.payload.get("message") if isinstance(ctx.payload, Mapping) else None
        deps.logger.warning(
            "amocrm_chat_webhook_empty_text tenant=%s message_type=%s message_preview=%s",
            ctx.tenant_id,
            type(raw_message).__name__,
            str(raw_message)[:500],
        )
    except Exception:
        pass


async def _find_chat_webhook_link(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> Mapping[str, Any] | None:
    return await deps.crm_chat_links_module.find_by_external_chat(
        deps.amocrm_chat_service_module.AMOCRM_CHAT_PROVIDER,
        external_chat_id=str(ctx.message.get("external_chat_id") or ""),
        external_conversation_id=str(ctx.message.get("external_conversation_id") or ""),
    )


async def _resolve_chat_webhook_route(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> None:
    ctx.lead_id = int((ctx.link or {}).get("lead_id") or 0)
    meta = await deps.get_lead_dialog_metadata_fn(int(ctx.lead_id))
    ctx.outbound_lead_id = await _resolve_preferred_outbound_lead(
        ctx.tenant_id,
        int(ctx.lead_id),
        ctx.link or {},
        deps,
    )
    ctx.outbound_meta = meta
    if ctx.outbound_lead_id != ctx.lead_id:
        candidate_meta = await deps.get_lead_dialog_metadata_fn(int(ctx.outbound_lead_id))
        candidate_peer = (
            str(candidate_meta.get("peer") or "").strip()
            if isinstance(candidate_meta, Mapping)
            else ""
        )
        if candidate_peer:
            ctx.outbound_meta = candidate_meta
        else:
            ctx.outbound_lead_id = int(ctx.lead_id)
    ctx.dialog_channel = (
        str((ctx.outbound_meta or {}).get("channel") or "").strip().lower()
        if isinstance(ctx.outbound_meta, Mapping)
        else ""
    )
    ctx.peer_value = (
        str((ctx.outbound_meta or {}).get("peer") or "").strip()
        if isinstance(ctx.outbound_meta, Mapping)
        else ""
    )
    if ctx.dialog_channel == "avito" and not ctx.peer_value:
        try:
            peer_fallback = await deps.get_lead_peer_fn(int(ctx.outbound_lead_id), channel="avito")
        except Exception:
            peer_fallback = ""
        ctx.peer_value = str(peer_fallback or "").strip()
    deps.logger.info(
        "amocrm_chat_webhook_route tenant=%s link_lead_id=%s outbound_lead_id=%s channel=%s peer_present=%s",
        ctx.tenant_id,
        ctx.lead_id,
        ctx.outbound_lead_id,
        ctx.dialog_channel or "-",
        int(bool(ctx.peer_value)),
    )


async def _resolve_preferred_outbound_lead(
    tenant_value: int,
    default_lead_id: int,
    link_row: Mapping[str, Any],
    deps: AmoCRMPublicDeps,
) -> int:
    provider_lead_id = _provider_lead_id_from_chat_link(link_row)
    if provider_lead_id is None:
        provider_lead_id = await _provider_lead_id_from_crm_link(
            tenant_value,
            default_lead_id,
            deps,
        )
    if provider_lead_id is None:
        return int(default_lead_id)
    return await _telegram_lead_for_provider_lead(
        tenant_value,
        provider_lead_id,
        default_lead_id,
        deps,
    )


def _provider_lead_id_from_chat_link(link_row: Mapping[str, Any]) -> int | None:
    try:
        if link_row.get("external_lead_id") is not None:
            return int(link_row.get("external_lead_id"))
    except Exception:
        return None
    return None


async def _provider_lead_id_from_crm_link(
    tenant_value: int,
    default_lead_id: int,
    deps: AmoCRMPublicDeps,
) -> int | None:
    crm_link = await deps.crm_links_module.get_link(
        int(tenant_value),
        int(default_lead_id),
        deps.amocrm_service_module.AMOCRM_PROVIDER,
    )
    try:
        if isinstance(crm_link, Mapping) and crm_link.get("provider_lead_id") is not None:
            return int(crm_link.get("provider_lead_id"))
    except Exception:
        return None
    return None


async def _telegram_lead_for_provider_lead(
    tenant_value: int,
    provider_lead_id: int,
    default_lead_id: int,
    deps: AmoCRMPublicDeps,
) -> int:
    fetchrow = getattr(deps.db_module, "_fetchrow", None)
    if not fetchrow:
        return int(default_lead_id)
    row = await fetchrow(
        """
        SELECT l.id
        FROM crm_links cl
        JOIN leads l ON l.id = cl.lead_id
        WHERE cl.tenant_id = $1
          AND cl.provider = $2
          AND cl.provider_lead_id = $3
          AND l.tenant_id = $1
          AND l.channel = 'telegram'
          AND COALESCE(NULLIF(l.peer, ''), '') <> ''
        ORDER BY cl.updated_at DESC, l.updated_at DESC
        LIMIT 1
        """,
        int(tenant_value),
        deps.amocrm_service_module.AMOCRM_PROVIDER,
        int(provider_lead_id),
    )
    try:
        resolved = int((row or {}).get("id") or 0)
    except Exception:
        resolved = 0
    return int(resolved) if resolved > 0 else int(default_lead_id)


async def _preflag_chat_webhook_echo(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> None:
    try:
        echo_fingerprints = _chat_webhook_echo_fingerprints(ctx, deps)
        for target_lead in {int(ctx.lead_id), int(ctx.outbound_lead_id)}:
            if target_lead <= 0:
                continue
            for fp in echo_fingerprints:
                pre_key = f"amocrm:manager:echo:{ctx.tenant_id}:{int(target_lead)}:{fp}"
                await deps.settings_module.r.set(pre_key, "1", ex=180)
                if ctx.dialog_channel == "avito" and ctx.peer_value:
                    chat_pre_key = (
                        f"amocrm:manager:echo:chat:{ctx.tenant_id}:{str(ctx.peer_value)}:{fp}"
                    )
                    await deps.settings_module.r.set(chat_pre_key, "1", ex=180)
    except Exception:
        deps.logger.debug(
            "amocrm_chat_echo_pre_flag_set_failed tenant=%s lead_id=%s outbound_lead_id=%s",
            ctx.tenant_id,
            ctx.lead_id,
            ctx.outbound_lead_id,
            exc_info=True,
        )


def _chat_webhook_echo_fingerprints(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> set[str]:
    echo_fingerprints: set[str] = {deps.content_fingerprint_fn(ctx.text, ctx.attachment_list)}
    if ctx.attachment_list and not ctx.text:
        echo_fingerprints.add(deps.content_fingerprint_fn("__image__", ctx.attachment_list))
        placeholder_text = deps.text_or_placeholder_fn("", ctx.attachment_list)
        if placeholder_text:
            echo_fingerprints.add(deps.content_fingerprint_fn(placeholder_text, ctx.attachment_list))
    return echo_fingerprints


async def _cache_chat_webhook_avito_echo(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> None:
    if ctx.dialog_channel != "avito" or not ctx.peer_value:
        return
    try:
        variants = _chat_webhook_avito_echo_variants(ctx, deps)
        primary = variants[0] if variants else deps.normalize_echo_text_fn(ctx.text)
        if not primary:
            return
        payload = {"text": primary, "extra": variants, "ts": int(deps.time_module.time())}
        await deps.settings_module.r.set(
            deps.avito_bot_echo_key_fn(ctx.tenant_id, str(ctx.peer_value)),
            deps.json_module.dumps(payload, ensure_ascii=False),
            ex=deps.avito_bot_echo_ttl_seconds,
        )
    except Exception:
        deps.logger.debug(
            "amocrm_chat_avito_echo_pre_cache_failed tenant=%s lead_id=%s outbound_lead_id=%s",
            ctx.tenant_id,
            ctx.lead_id,
            ctx.outbound_lead_id,
            exc_info=True,
        )


def _chat_webhook_avito_echo_variants(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> list[str]:
    variants: list[str] = []
    for candidate in (
        ctx.text,
        "__image__" if ctx.attachment_list else "",
        deps.text_or_placeholder_fn(ctx.text, ctx.attachment_list),
        "Голосовое сообщение",
        "Вложение",
    ):
        normalized = deps.normalize_echo_text_fn(candidate)
        if normalized and normalized not in variants:
            variants.append(normalized)
    return variants


async def _send_chat_webhook_message(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> tuple[int, str]:
    if ctx.dialog_channel == "avito":
        return await _send_chat_webhook_avito(ctx, deps)
    return await _send_chat_webhook_telegram(ctx, deps)


async def _send_chat_webhook_avito(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> tuple[int, str]:
    account_id = None
    if isinstance(ctx.outbound_meta, Mapping):
        try:
            account_id = (
                int(ctx.outbound_meta.get("source_real_id"))
                if ctx.outbound_meta.get("source_real_id") is not None
                else None
            )
        except Exception:
            account_id = None
    return await deps.send_avito_fn(
        ctx.tenant_id,
        int(ctx.outbound_lead_id),
        ctx.text,
        chat_id=ctx.peer_value or None,
        account_id=account_id,
        attachments=ctx.attachment_list or None,
    )


async def _send_chat_webhook_telegram(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> tuple[int, str]:
    headers = _chat_webhook_telegram_headers(deps)
    return await deps.telegram_transport_module.send(
        tenant=ctx.tenant_id,
        text=ctx.text,
        peer=ctx.peer_value or None,
        attachments=ctx.attachment_list or None,
        lead_id=ctx.outbound_lead_id if ctx.outbound_lead_id > 0 else None,
        headers=headers or None,
        meta={"origin": "amocrm:manager"},
    )


def _chat_webhook_telegram_headers(deps: AmoCRMPublicDeps) -> dict[str, str]:
    headers: dict[str, str] = {}
    worker_token = (
        deps.os_module.getenv("TG_WORKER_TOKEN")
        or deps.os_module.getenv("WEBHOOK_SECRET")
        or ""
    ).strip()
    admin_token = (deps.settings_module.ADMIN_TOKEN or "").strip()
    if worker_token:
        headers["X-Auth-Token"] = worker_token
    if admin_token:
        headers["X-Admin-Token"] = admin_token
    return headers


async def _store_chat_webhook_manager_message(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> str:
    telegram_user_id = (
        ctx.outbound_meta.get("telegram_user_id") if isinstance(ctx.outbound_meta, Mapping) else None
    )
    telegram_username = (
        ctx.outbound_meta.get("telegram_username") if isinstance(ctx.outbound_meta, Mapping) else None
    )
    stored_text = deps.text_or_placeholder_fn(ctx.text, ctx.attachment_list)
    stored_manager_message_id = await deps.insert_message_out_fn(
        int(ctx.outbound_lead_id),
        stored_text,
        provider_msg_id=ctx.dedup_message_id,
        status="sent",
        tenant_id=ctx.tenant_id,
        channel=ctx.dialog_channel or "telegram",
        telegram_user_id=int(telegram_user_id) if telegram_user_id is not None else None,
        telegram_username=str(telegram_username or "") or None,
        is_bot=False,
        attachments=ctx.attachment_list or None,
        source="manager",
    )
    await deps.capture_manager_intervention_fn(
        tenant_id=ctx.tenant_id,
        lead_id=int(ctx.outbound_lead_id),
        channel=(ctx.dialog_channel or "telegram"),
        manager_message_id=int(stored_manager_message_id) if stored_manager_message_id else None,
        source_event="amocrm_chat_webhook_outgoing",
    )
    return stored_text


async def _evaluate_chat_webhook_stage(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> None:
    try:
        stored_text = deps.text_or_placeholder_fn(ctx.text, ctx.attachment_list)
        await deps.amocrm_service_module.amocrm_on_outbound_message(
            ctx.tenant_id,
            int(ctx.outbound_lead_id),
            text=stored_text or "",
            channel=ctx.dialog_channel or "telegram",
            attachments=ctx.attachment_list or None,
            sync_chat=False,
            source_role="manager",
        )
    except Exception:
        deps.logger.exception(
            "amocrm_chat_webhook_stage_eval_failed tenant=%s lead_id=%s channel=%s",
            ctx.tenant_id,
            ctx.outbound_lead_id,
            ctx.dialog_channel or "telegram",
        )


async def _set_chat_webhook_handoff(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> None:
    silence_targets = await _chat_webhook_silence_targets(ctx, deps)
    for silence_lead_id in silence_targets:
        if silence_lead_id <= 0:
            continue
        try:
            await deps.settings_module.r.set(
                deps.handoff_silence_key_fn(ctx.tenant_id, int(silence_lead_id)),
                "1",
                ex=deps.handoff_silence_ttl_seconds,
            )
            await _set_chat_webhook_handoff_meta(ctx, silence_lead_id, deps)
        except deps.redis_error_type:
            deps.logger.debug(
                "amocrm_chat_handoff_flag_set_failed tenant=%s lead_id=%s",
                ctx.tenant_id,
                silence_lead_id,
                exc_info=True,
            )


async def _set_chat_webhook_handoff_meta(
    ctx: _AmoChatWebhookContext,
    silence_lead_id: int,
    deps: AmoCRMPublicDeps,
) -> None:
    meta_payload = {
        "reason": "manager_outgoing",
        "source": "amocrm_inbox",
        "ts": int(deps.time_module.time()),
        "channel": ctx.dialog_channel or "telegram",
    }
    if ctx.peer_value:
        meta_payload["peer"] = ctx.peer_value
    await deps.settings_module.r.set(
        deps.handoff_silence_meta_key_fn(ctx.tenant_id, int(silence_lead_id)),
        deps.json_module.dumps(meta_payload, ensure_ascii=False),
        ex=deps.handoff_silence_ttl_seconds,
    )


async def _chat_webhook_silence_targets(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> set[int]:
    silence_targets = {int(ctx.lead_id), int(ctx.outbound_lead_id)}
    provider_lead = await _chat_webhook_provider_lead_for_silence(ctx, deps)
    if provider_lead is not None:
        silence_targets.update(await _chat_webhook_linked_leads(provider_lead, ctx, deps))
    return silence_targets


async def _chat_webhook_provider_lead_for_silence(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> int | None:
    provider_lead = _provider_lead_id_from_chat_link(ctx.link or {})
    if provider_lead is not None:
        return provider_lead
    try:
        outbound_crm_link = await deps.crm_links_module.get_link(
            ctx.tenant_id,
            int(ctx.outbound_lead_id),
            deps.amocrm_service_module.AMOCRM_PROVIDER,
        )
        if (
            isinstance(outbound_crm_link, Mapping)
            and outbound_crm_link.get("provider_lead_id") is not None
        ):
            return int(outbound_crm_link.get("provider_lead_id"))
    except Exception:
        return None
    return None


async def _chat_webhook_linked_leads(
    provider_lead_id: int,
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> set[int]:
    fetch = getattr(deps.db_module, "_fetch", None)
    if not fetch:
        return set()
    try:
        rows = await fetch(
            """
            SELECT lead_id
            FROM crm_links
            WHERE tenant_id = $1
              AND provider = $2
              AND provider_lead_id = $3
            """,
            ctx.tenant_id,
            deps.amocrm_service_module.AMOCRM_PROVIDER,
            int(provider_lead_id),
        )
    except Exception:
        deps.logger.debug(
            "amocrm_chat_handoff_targets_resolve_failed tenant=%s provider_lead_id=%s",
            ctx.tenant_id,
            provider_lead_id,
            exc_info=True,
        )
        return set()
    return {_positive_int((row or {}).get("lead_id")) for row in rows or []} - {0}


async def _mark_chat_webhook_echo_and_touch(
    ctx: _AmoChatWebhookContext,
    deps: AmoCRMPublicDeps,
) -> None:
    try:
        echo_key = (
            f"amocrm:manager:echo:{ctx.tenant_id}:{int(ctx.outbound_lead_id)}:"
            f"{deps.content_fingerprint_fn(ctx.text, ctx.attachment_list)}"
        )
        await deps.settings_module.r.set(echo_key, "1", ex=180)
    except Exception:
        deps.logger.debug(
            "amocrm_chat_echo_flag_set_failed tenant=%s lead_id=%s",
            ctx.tenant_id,
            ctx.outbound_lead_id,
            exc_info=True,
        )
    await deps.crm_chat_links_module.touch_message_ids(
        ctx.tenant_id,
        int(ctx.lead_id),
        deps.amocrm_chat_service_module.AMOCRM_CHAT_PROVIDER,
        outbound_message_id=ctx.dedup_message_id,
    )


def _positive_int(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except Exception:
        return 0
    return parsed if parsed > 0 else 0


async def disconnect(
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: AmoCRMPublicDeps,
) -> Response:
    tenant_id = None
    if tenant is not None or key is not None:
        auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
        if isinstance(auth, Response):
            return auth
        tenant_id, _ = auth
    else:
        payload = await deps.read_amocrm_webhook_payload_fn(request)
        account_id, subdomain = deps.extract_amocrm_uninstall_info_fn(payload)
        tenant_id = deps.amocrm_service_module.find_tenant_by_account(account_id, subdomain)
        if tenant_id is None:
            return JSONResponse({"ok": False, "detail": "tenant_not_found"}, status_code=404)
    cfg = deps.read_tenant_config_fn(tenant_id)
    if isinstance(cfg, dict):
        integrations = cfg.get("integrations")
        if not isinstance(integrations, dict):
            integrations = {}
        amocrm_cfg = integrations.get("amocrm")
        if isinstance(amocrm_cfg, dict):
            amocrm_cfg["enabled"] = False
            manual_cfg = amocrm_cfg.get("manual")
            if isinstance(manual_cfg, dict):
                manual_cfg.pop("access_token", None)
            amocrm_cfg.pop("tokens", None)
            integrations["amocrm"] = amocrm_cfg
            cfg["integrations"] = integrations
            deps.write_tenant_config_fn(tenant_id, cfg)
    try:
        await deps.amocrm_tokens_module.delete(int(tenant_id))
    except Exception:
        deps.logger.exception("amocrm_disconnect_failed tenant=%s", tenant_id)
        return JSONResponse({"ok": False, "detail": "amocrm_disconnect_failed"}, status_code=500)
    return JSONResponse({"ok": True})


async def pipeline(
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    apply: int | None,
    pipeline_id: int | None,
    deps: AmoCRMPublicDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    cfg = deps.read_tenant_config_fn(tenant_id)
    amocrm_cfg = deps.amocrm_service_module.get_amocrm_cfg(cfg)
    if not amocrm_cfg or not bool(amocrm_cfg.get("enabled")):
        return JSONResponse({"ok": False, "detail": "amocrm_not_enabled"}, status_code=400)
    client_or_response = await _amocrm_pipeline_client(int(tenant_id), amocrm_cfg, deps)
    if isinstance(client_or_response, Response):
        return client_or_response
    client = client_or_response
    pipelines = await _fetch_amocrm_pipeline_list(client, deps)
    if not pipelines:
        return JSONResponse({"ok": False, "detail": "amocrm_pipeline_empty"}, status_code=400)
    pipeline_options = _amocrm_pipeline_options(pipelines)
    if not pipeline_options:
        return JSONResponse({"ok": False, "detail": "amocrm_pipeline_empty"}, status_code=400)
    resolved_pipeline_id = _resolve_amocrm_pipeline_id(pipeline_id, amocrm_cfg, pipeline_options)
    statuses = await _amocrm_pipeline_statuses(client, pipelines, resolved_pipeline_id, deps)
    if not statuses:
        return JSONResponse({"ok": False, "detail": "amocrm_statuses_empty"}, status_code=400)
    stages = _build_amocrm_pipeline_stages(statuses, amocrm_cfg, resolved_pipeline_id, deps)
    if not stages:
        return JSONResponse({"ok": False, "detail": "amocrm_stage_build_failed"}, status_code=400)
    if apply:
        _save_amocrm_pipeline_config(
            int(tenant_id),
            cfg,
            amocrm_cfg,
            resolved_pipeline_id,
            stages,
            pipeline_options,
            deps,
        )
    return JSONResponse(
        {
            "ok": True,
            "pipeline_id": resolved_pipeline_id,
            "stages": stages,
            "pipelines": pipeline_options,
        },
        headers=deps.no_store_headers_fn(),
    )


async def test_connection(
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: AmoCRMPublicDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    cfg = deps.read_tenant_config_fn(tenant_id)
    amocrm_cfg = deps.amocrm_service_module.get_amocrm_cfg(cfg)
    if not amocrm_cfg or not bool(amocrm_cfg.get("enabled")):
        return JSONResponse({"ok": False, "detail": "amocrm_not_enabled"}, status_code=400)
    client_or_response = await _amocrm_test_client(int(tenant_id), amocrm_cfg, deps)
    if isinstance(client_or_response, Response):
        return client_or_response
    try:
        await client_or_response.get_pipelines()
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "detail": str(exc) or "amocrm_unreachable"},
            status_code=400,
        )
    return JSONResponse({"ok": True})


async def _amocrm_test_client(
    tenant_id: int,
    amocrm_cfg: Mapping[str, Any],
    deps: AmoCRMPublicDeps,
) -> Any | Response:
    base_url = await deps.amocrm_service_module.resolve_api_base_url(amocrm_cfg, int(tenant_id))
    if not base_url:
        return JSONResponse({"ok": False, "detail": "base_url_missing"}, status_code=400)
    oauth_cfg = deps.amocrm_service_module.resolve_oauth_cfg(amocrm_cfg, int(tenant_id))
    return deps.amocrm_integration_module.AmoCRMClient(
        tenant_id=int(tenant_id),
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )


async def _amocrm_pipeline_client(
    tenant_id: int,
    amocrm_cfg: Mapping[str, Any],
    deps: AmoCRMPublicDeps,
) -> Any | Response:
    token_entry = await deps.amocrm_tokens_module.get(int(tenant_id))
    if not token_entry or not token_entry.access_token:
        return JSONResponse({"ok": False, "detail": "amocrm_token_missing"}, status_code=400)
    base_url = await deps.amocrm_service_module.resolve_api_base_url(
        amocrm_cfg,
        int(tenant_id),
        token_entry,
    )
    if not base_url:
        return JSONResponse({"ok": False, "detail": "base_url_missing"}, status_code=400)
    oauth_cfg = deps.amocrm_service_module.resolve_oauth_cfg(amocrm_cfg, int(tenant_id))
    return deps.amocrm_integration_module.AmoCRMClient(
        tenant_id=int(tenant_id),
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )


async def _fetch_amocrm_pipeline_list(
    client: Any,
    deps: AmoCRMPublicDeps,
) -> list[Mapping[str, Any]]:
    payload = await client.get_pipelines()
    return deps.amocrm_service_module._extract_embedded_list(payload, "pipelines")


def _amocrm_pipeline_options(pipelines: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for item in pipelines:
        if not isinstance(item, Mapping):
            continue
        item_id = _positive_int(item.get("id"))
        if item_id <= 0:
            continue
        item_name = str(item.get("name") or "").strip() or f"Воронка {item_id}"
        options.append({"id": item_id, "name": item_name})
    return options


def _resolve_amocrm_pipeline_id(
    pipeline_id: int | None,
    amocrm_cfg: Mapping[str, Any],
    pipeline_options: list[dict[str, Any]],
) -> int:
    try:
        resolved = int(pipeline_id or amocrm_cfg.get("pipeline_id") or 0)
    except Exception:
        resolved = 0
    if resolved <= 0:
        resolved = int(pipeline_options[0]["id"])
    return resolved


async def _amocrm_pipeline_statuses(
    client: Any,
    pipelines: list[Mapping[str, Any]],
    resolved_pipeline_id: int,
    deps: AmoCRMPublicDeps,
) -> list[Mapping[str, Any]]:
    statuses = await _fetch_amocrm_pipeline_statuses(client, resolved_pipeline_id, deps)
    if statuses:
        return statuses
    return _cached_amocrm_pipeline_statuses(pipelines, resolved_pipeline_id, deps)


async def _fetch_amocrm_pipeline_statuses(
    client: Any,
    resolved_pipeline_id: int,
    deps: AmoCRMPublicDeps,
) -> list[Mapping[str, Any]]:
    if resolved_pipeline_id <= 0:
        return []
    try:
        payload = await client.get_pipeline_stages(
            resolved_pipeline_id,
            with_descriptions=True,
        )
        return deps.amocrm_service_module._extract_embedded_list(payload, "statuses")
    except Exception:
        return []


def _cached_amocrm_pipeline_statuses(
    pipelines: list[Mapping[str, Any]],
    resolved_pipeline_id: int,
    deps: AmoCRMPublicDeps,
) -> list[Mapping[str, Any]]:
    for item in pipelines:
        if not isinstance(item, Mapping):
            continue
        if _positive_int(item.get("id")) != resolved_pipeline_id:
            continue
        return deps.amocrm_service_module._extract_embedded_list(item, "statuses")
    return []


def _build_amocrm_pipeline_stages(
    statuses: list[Mapping[str, Any]],
    amocrm_cfg: Mapping[str, Any],
    resolved_pipeline_id: int,
    deps: AmoCRMPublicDeps,
) -> list[dict[str, Any]]:
    stages = deps.amocrm_service_module.build_stages_from_statuses(statuses)
    return deps.amocrm_service_module._merge_stages_for_pipeline(
        stages,
        amocrm_cfg,
        resolved_pipeline_id,
    )


def _save_amocrm_pipeline_config(
    tenant_id: int,
    cfg: Mapping[str, Any],
    amocrm_cfg: Mapping[str, Any],
    resolved_pipeline_id: int,
    stages: list[dict[str, Any]],
    pipeline_options: list[dict[str, Any]],
    deps: AmoCRMPublicDeps,
) -> None:
    updated_cfg = dict(cfg) if isinstance(cfg, Mapping) else {}
    integrations = updated_cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg_copy = dict(amocrm_cfg)
    amocrm_cfg_copy["pipeline_id"] = resolved_pipeline_id
    amocrm_cfg_copy["stages"] = stages
    stages_by_pipeline = amocrm_cfg_copy.get("stages_by_pipeline")
    if not isinstance(stages_by_pipeline, dict):
        stages_by_pipeline = {}
    stages_by_pipeline[str(int(resolved_pipeline_id))] = {
        "stages": stages,
        "synced_at": int(deps.time_module.time()),
    }
    amocrm_cfg_copy["stages_by_pipeline"] = stages_by_pipeline
    amocrm_cfg_copy["pipelines_cache"] = pipeline_options
    amocrm_cfg_copy["pipelines_cached_at"] = int(deps.time_module.time())
    integrations["amocrm"] = amocrm_cfg_copy
    updated_cfg["integrations"] = integrations
    deps.write_tenant_config_fn(tenant_id, updated_cfg)
