from __future__ import annotations

import json
import logging
import uuid
import base64
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from redis import exceptions as redis_ex

from libs.core import sales_core as core_module
from libs.core.crypto import EncryptionError
from libs.core.integrations import avito_analytics as avito_analytics_client
from libs.core.repo import avito_analytics_tokens as tokens_repo
from libs.core.services import avito_public_analytics as analytics_service

from .public import _authorize_public_settings_request
from .ui import render_template
from . import common

router = APIRouter(tags=["analytics"])
logger = logging.getLogger("app.web.analytics_avito")
_STATE_TTL = 600


async def _ensure_schema() -> None:
    try:
        await tokens_repo.ensure_schema()
    except Exception:
        logger.exception("avito_analytics_schema_failed")
        raise


async def _ensure_schema_or_503() -> JSONResponse | None:
    try:
        await _ensure_schema()
    except Exception as exc:
        return JSONResponse({"detail": "db_unavailable", "error": str(exc)}, status_code=503)
    return None


def _state_secret() -> str:
    return (
        (core_module.settings.WEBHOOK_SECRET or "").strip()
        or (core_module.settings.ADMIN_TOKEN or "").strip()
        or (core_module.settings.AVITO_CLIENT_SECRET or "").strip()
    )


def _build_state_token(tenant_id: int, k: str | None) -> str:
    payload = {"tenant": tenant_id, "k": k, "ts": int(datetime.now(tz=timezone.utc).timestamp())}
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    secret = _state_secret().encode("utf-8")
    sig = hmac.new(secret, body, hashlib.sha256).hexdigest()
    body_b64 = base64.urlsafe_b64encode(body).decode("utf-8").rstrip("=")
    return f"a1.{body_b64}.{sig}"


def _analytics_redirect(request: Request) -> str:
    fallback = (getattr(core_module.settings, "AVITO_REDIRECT_URL", "") or "").strip()
    if fallback:
        return fallback
    base = str(request.base_url).rstrip("/")
    return f"{base}/v1/oauth/avito/callback"


def _parse_calc_params(request: Request) -> dict[str, Any]:
    params = request.query_params
    result: dict[str, Any] = {}
    for key in (
        "avg_check",
        "gross_margin",
        "close_rate_chat",
        "close_rate_call",
        "loss_factor_slow_response",
        "value_per_lead",
        "workday_start",
        "workday_end",
        "weekend_days",
    ):
        if key in params:
            result[key] = params.get(key)
    return result


def _get_analytics_account_id(tenant_id: int) -> tuple[int | None, Mapping[str, Any]]:
    try:
        common.ensure_tenant_files(int(tenant_id))
    except Exception:
        logger.exception("avito_analytics_tenant_files_failed tenant=%s", tenant_id)
    try:
        cfg = core_module.read_tenant_config(int(tenant_id))
    except Exception:
        logger.exception("avito_analytics_read_tenant_failed tenant=%s", tenant_id)
        return None, {}
    if not isinstance(cfg, Mapping):
        return None, {}
    integrations = cfg.get("integrations")
    if not isinstance(integrations, Mapping):
        return None, {}
    avito_cfg = integrations.get("avito_analytics")
    if not isinstance(avito_cfg, Mapping):
        return None, {}
    account_raw = avito_cfg.get("account_id")
    try:
        account_id = int(account_raw) if account_raw is not None else None
    except Exception:
        account_id = None
    return account_id, avito_cfg


def _set_analytics_account(tenant_id: int, account_id: int, display_name: str | None) -> None:
    cfg = core_module.read_tenant_config(int(tenant_id))
    if not isinstance(cfg, dict):
        cfg = {}
    integrations = cfg.setdefault("integrations", {})
    avito_cfg = integrations.get("avito_analytics") if isinstance(integrations.get("avito_analytics"), Mapping) else {}
    updated = dict(avito_cfg)
    updated["account_id"] = int(account_id)
    if display_name:
        updated["display_name"] = display_name
    updated["connected_at"] = int(datetime.now(tz=timezone.utc).timestamp())
    integrations["avito_analytics"] = updated
    core_module.write_tenant_config(int(tenant_id), cfg)


@router.get("/v1/oauth/avito-analytics/status")
async def avito_analytics_status(request: Request, tenant: int, k: str | None = None):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    schema_err = await _ensure_schema_or_503()
    if schema_err:
        return schema_err
    tenant_id, _ = auth
    account_id, cfg = _get_analytics_account_id(tenant_id)
    connected = False
    if account_id:
        token = await tokens_repo.get(int(account_id))
        connected = bool(token and token.refresh_token)
    return {"ok": True, "connected": connected, "account_id": account_id, "display_name": cfg.get("display_name")}


@router.get("/v1/oauth/avito-analytics/authorize")
async def avito_analytics_authorize(request: Request, tenant: int, k: str | None = None):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    schema_err = await _ensure_schema_or_503()
    if schema_err:
        return schema_err
    tenant_id, _ = auth
    state = _build_state_token(int(tenant_id), k)
    payload = json.dumps({"tenant": tenant_id, "k": k})
    state_key = f"oauth:avito:analytics:state:{state}"
    try:
        client = common.redis_client()
        client.setex(state_key, _STATE_TTL, payload)
    except redis_ex.RedisError:
        logger.exception("avito_analytics_state_store_failed tenant=%s", tenant_id)
        # Allow stateless flow even if redis is down.

    redirect_uri = _analytics_redirect(request)
    authorize_url = avito_analytics_client.build_authorize_url(
        state=state,
        scope=avito_analytics_client.DEFAULT_SCOPES,
        redirect_uri=redirect_uri,
    )
    return JSONResponse({"authorize_url": authorize_url})


@router.get("/v1/oauth/avito-analytics/callback")
async def avito_analytics_callback(request: Request, code: str | None = None, state: str | None = None, error: str | None = None):
    if error:
        return HTMLResponse("Avito OAuth error: " + str(error))
    if not state:
        return HTMLResponse("Avito OAuth error: missing_state")
    try:
        client = common.redis_client()
        raw_state = client.get(f"oauth:avito:analytics:state:{state}")
        if raw_state:
            client.delete(f"oauth:avito:analytics:state:{state}")
    except redis_ex.RedisError:
        logger.exception("avito_analytics_state_fetch_failed state=%s", state)
        return HTMLResponse("Avito OAuth error: state_unavailable")
    if not raw_state:
        return HTMLResponse("Avito OAuth error: state_missing")
    try:
        payload = json.loads(raw_state.decode("utf-8") if isinstance(raw_state, (bytes, bytearray)) else raw_state)
    except Exception:
        payload = {}
    tenant_id = payload.get("tenant")
    if not tenant_id:
        return HTMLResponse("Avito OAuth error: invalid_state")
    if not code:
        return HTMLResponse("Avito OAuth error: missing_code")
    schema_err = await _ensure_schema_or_503()
    if schema_err:
        return HTMLResponse("Avito OAuth error: db_unavailable")

    try:
        token_payload = await avito_analytics_client.exchange_code_for_token(
            code, redirect_uri=_analytics_redirect(request)
        )
    except Exception:
        logger.exception("avito_analytics_token_exchange_failed")
        return HTMLResponse("Avito OAuth error: token_exchange_failed")

    access_token = str(token_payload.get("access_token") or "").strip()
    refresh_token = str(token_payload.get("refresh_token") or "").strip()
    token_type = token_payload.get("token_type")
    scopes = token_payload.get("scope") or avito_analytics_client.DEFAULT_SCOPES
    expires_in = token_payload.get("expires_in")
    obtained_at = datetime.now(tz=timezone.utc)
    expires_at = None
    if expires_in:
        try:
            expires_at = obtained_at + timedelta(seconds=int(expires_in))
        except Exception:
            expires_at = None
    try:
        user_info = await avito_analytics_client.get_user_me(access_token)
    except Exception:
        user_info = {}
    account_candidate = None
    display_name = None
    if isinstance(user_info, Mapping):
        account_candidate = (
            user_info.get("id")
            or user_info.get("account_id")
            or user_info.get("accountId")
            or user_info.get("account")
        )
        display_name = (
            user_info.get("login")
            or user_info.get("name")
            or user_info.get("title")
            or user_info.get("username")
        )
    try:
        account_id = int(account_candidate) if account_candidate is not None else None
    except Exception:
        account_id = None
    if account_id is None or not refresh_token:
        return HTMLResponse("Avito OAuth error: account_unknown")

    sanitized_payload = dict(token_payload)
    sanitized_payload.pop("access_token", None)
    sanitized_payload.pop("refresh_token", None)
    sanitized_payload["user"] = user_info
    try:
        await tokens_repo.upsert(
            int(account_id),
            display_name=display_name,
            scopes=scopes,
            token_type=token_type,
            access_token=access_token or None,
            refresh_token=refresh_token,
            expires_at=expires_at,
            obtained_at=obtained_at,
            raw_payload=sanitized_payload,
        )
    except EncryptionError as exc:
        return HTMLResponse("Avito OAuth error: encryption_error")
    except Exception:
        logger.exception("avito_analytics_token_store_failed account_id=%s", account_id)
        return HTMLResponse("Avito OAuth error: token_store_failed")

    _set_analytics_account(int(tenant_id), int(account_id), display_name)
    redirect_key = payload.get("k") or ""
    return RedirectResponse(url=f"/pub/analytics/avito?tenant={tenant_id}&k={redirect_key}", status_code=303)


@router.post("/v1/oauth/avito-analytics/disconnect")
async def avito_analytics_disconnect(request: Request, tenant: int, k: str | None = None):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    account_id, _ = _get_analytics_account_id(tenant_id)
    if account_id:
        try:
            await tokens_repo.delete(int(account_id))
        except Exception:
            logger.exception("avito_analytics_disconnect_failed account_id=%s", account_id)
            return JSONResponse({"detail": "disconnect_failed"}, status_code=500)
    cfg = core_module.read_tenant_config(int(tenant_id))
    if isinstance(cfg, dict):
        integrations = cfg.get("integrations")
        if isinstance(integrations, dict):
            integrations.pop("avito_analytics", None)
            cfg["integrations"] = integrations
        core_module.write_tenant_config(int(tenant_id), cfg)
    return {"ok": True}


@router.get("/v1/analytics/avito/report")
async def avito_report(request: Request, tenant: int, k: str | None = None, period: int = 7, sla: int = 15, fast: int = 1, force: int = 0):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    schema_err = await _ensure_schema_or_503()
    if schema_err:
        return schema_err
    tenant_id, _ = auth
    account_id, _ = _get_analytics_account_id(int(tenant_id))
    if not account_id:
        return JSONResponse({"detail": "analytics_not_authorized"}, status_code=401)
    calc_params = _parse_calc_params(request)
    try:
        report = await analytics_service.build_report(
            int(account_id),
            tenant_id=int(tenant_id),
            period_days=int(period),
            sla_minutes=int(sla),
            fast=bool(fast),
            calc_params=calc_params,
            force_refresh=bool(force),
        )
    except avito_analytics_client.AvitoOAuthError as exc:
        return JSONResponse({"detail": "oauth_error", "error": str(exc)}, status_code=401)
    except Exception as exc:
        logger.exception("avito_public_report_failed tenant=%s", tenant)
        return JSONResponse({"detail": "report_failed", "error": str(exc)}, status_code=500)
    return JSONResponse({"ok": True, "report": jsonable_encoder(report)})


@router.get("/v1/analytics/avito/items")
async def avito_items(request: Request, tenant: int, k: str | None = None, period: int = 7, fast: int = 1):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    schema_err = await _ensure_schema_or_503()
    if schema_err:
        return schema_err
    tenant_id, _ = auth
    account_id, _ = _get_analytics_account_id(int(tenant_id))
    if not account_id:
        return JSONResponse({"detail": "analytics_not_authorized"}, status_code=401)
    report = await analytics_service.build_report(int(account_id), tenant_id=int(tenant_id), period_days=int(period), fast=bool(fast))
    return JSONResponse({"ok": True, "items": jsonable_encoder((report.get("listings") or {}).get("items") or [])})


@router.get("/v1/analytics/avito/stats")
async def avito_stats(request: Request, tenant: int, k: str | None = None, period: int = 7, fast: int = 1):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    schema_err = await _ensure_schema_or_503()
    if schema_err:
        return schema_err
    tenant_id, _ = auth
    account_id, _ = _get_analytics_account_id(int(tenant_id))
    if not account_id:
        return JSONResponse({"detail": "analytics_not_authorized"}, status_code=401)
    report = await analytics_service.build_report(int(account_id), tenant_id=int(tenant_id), period_days=int(period), fast=bool(fast))
    return JSONResponse({"ok": True, "stats": jsonable_encoder(report.get("stats") or {})})


@router.get("/v1/analytics/avito/messenger")
async def avito_messenger(request: Request, tenant: int, k: str | None = None, period: int = 7, sla: int = 15, fast: int = 1):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    schema_err = await _ensure_schema_or_503()
    if schema_err:
        return schema_err
    tenant_id, _ = auth
    account_id, _ = _get_analytics_account_id(int(tenant_id))
    if not account_id:
        return JSONResponse({"detail": "analytics_not_authorized"}, status_code=401)
    report = await analytics_service.build_report(int(account_id), tenant_id=int(tenant_id), period_days=int(period), sla_minutes=int(sla), fast=bool(fast))
    return JSONResponse({"ok": True, "messenger": jsonable_encoder(report.get("messaging") or {})})


@router.get("/v1/analytics/avito/spend")
async def avito_spend(request: Request, tenant: int, k: str | None = None, period: int = 7, fast: int = 1):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    schema_err = await _ensure_schema_or_503()
    if schema_err:
        return schema_err
    tenant_id, _ = auth
    account_id, _ = _get_analytics_account_id(int(tenant_id))
    if not account_id:
        return JSONResponse({"detail": "analytics_not_authorized"}, status_code=401)
    report = await analytics_service.build_report(int(account_id), tenant_id=int(tenant_id), period_days=int(period), fast=bool(fast))
    return JSONResponse({"ok": True, "spend": jsonable_encoder(report.get("spend") or {})})


@router.get("/v1/analytics/avito/calls")
async def avito_calls(request: Request, tenant: int, k: str | None = None, period: int = 7, fast: int = 1):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    await _ensure_schema()
    tenant_id, _ = auth
    account_id, _ = _get_analytics_account_id(int(tenant_id))
    if not account_id:
        return JSONResponse({"detail": "analytics_not_authorized"}, status_code=401)
    report = await analytics_service.build_report(int(account_id), tenant_id=int(tenant_id), period_days=int(period), fast=bool(fast))
    return JSONResponse({"ok": True, "calls": jsonable_encoder(report.get("calls") or {})})


@router.get("/pub/analytics/avito", response_class=HTMLResponse)
async def avito_ui(request: Request, tenant: int, k: str | None = None):
    auth = _authorize_public_settings_request(request, tenant, k)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    context = {
        "request": request,
        "tenant": tenant_id,
        "k": k,
        "title": "Avito Analytics",
    }
    return render_template("analytics/avito.html", context)
