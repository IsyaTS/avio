from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from libs.core.crypto import EncryptionError
from libs.core.integrations import avito_analytics as avito_api
from libs.core.repo import avito_analytics_tokens as tokens_repo
from libs.core.repo import avito_job_applications as job_repo
from libs.core.services import avito_analytics as analytics_service
from libs.core.sales_core import settings, ADMIN_COOKIE

from .admin import _auth_ok, _require_admin
from .ui import render_template

router = APIRouter()
logger = logging.getLogger("app.web.admin_avito_analytics")

STATE_PREFIX = "oauth:avito:analytics:state:"
STATE_TTL = 600


def _session_hint(request: Request) -> str:
    return (request.cookies.get(ADMIN_COOKIE) or "") or (request.headers.get("X-Admin-Token") or "")


async def _state_set(state: str, payload: dict[str, Any]) -> bool:
    r = getattr(settings, "r", None)
    if not r:
        return False
    try:
        await r.set(f"{STATE_PREFIX}{state}", json.dumps(payload, ensure_ascii=False), ex=STATE_TTL)
    except Exception:
        logger.exception("avito_analytics_state_store_failed state=%s", state)
        return False
    return True


async def _state_pop(state: str) -> dict[str, Any] | None:
    r = getattr(settings, "r", None)
    if not r:
        return None
    key = f"{STATE_PREFIX}{state}"
    try:
        raw = await r.get(key)
        await r.delete(key)
    except Exception:
        logger.exception("avito_analytics_state_fetch_failed state=%s", state)
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _redirect_login() -> RedirectResponse:
    return RedirectResponse(url="/admin/login", status_code=303)


async def _ensure_schema() -> None:
    try:
        await tokens_repo.ensure_schema()
        await job_repo.ensure_schema()
    except Exception:
        logger.exception("avito_analytics_schema_failed")
        raise


@router.get("/admin/avito-analytics")
async def avito_analytics_page(request: Request, account_id: int | None = None):
    if not _auth_ok(request):
        return _redirect_login()
    await _ensure_schema()
    tokens = await tokens_repo.list_tokens()
    summary = tokens_repo.summary_from_tokens(tokens)
    context = {
        "request": request,
        "title": "Avito Analytics",
        "subtitle": "OAuth аналитика по Avito-аккаунтам",
        "accounts": summary,
        "default_account": account_id or (summary[0]["account_id"] if summary else None),
        "scopes": avito_api.DEFAULT_SCOPES,
        "redirect_uri": avito_api.ANALYTICS_REDIRECT,
    }
    return render_template("admin/avito_analytics.html", context)


@router.get("/admin/avito-analytics/oauth/start")
async def avito_analytics_oauth_start(request: Request):
    guard = _require_admin(request)
    if guard:
        return guard
    state = uuid.uuid4().hex
    payload = {
        "session": _session_hint(request),
        "ts": int(time.time()),
    }
    stored = await _state_set(state, payload)
    if not stored:
        return JSONResponse({"detail": "state_store_failed"}, status_code=500)
    authorize_url = avito_api.build_authorize_url(state)
    return RedirectResponse(url=authorize_url, status_code=303)


@router.get("/admin/avito-analytics/oauth/callback")
async def avito_analytics_oauth_callback(request: Request, code: str | None = None, state: str | None = None):
    if not state:
        return JSONResponse({"detail": "state_missing"}, status_code=400)
    payload = await _state_pop(state)
    if not payload:
        return JSONResponse({"detail": "state_invalid_or_expired"}, status_code=400)
    if not _auth_ok(request):
        session_hint = _session_hint(request)
        stored_hint = payload.get("session")
        if not stored_hint or session_hint != stored_hint:
            return _redirect_login()
    if not code:
        return JSONResponse({"detail": "code_missing"}, status_code=400)

    await _ensure_schema()
    try:
        token_payload = await avito_api.exchange_code_for_token(code, redirect_uri=avito_api.ANALYTICS_REDIRECT or None)
    except avito_api.AvitoOAuthError as exc:
        return JSONResponse({"detail": "oauth_failed", "error": str(exc)}, status_code=400)

    access_token = str(token_payload.get("access_token") or "").strip()
    refresh_token = str(token_payload.get("refresh_token") or "").strip()
    token_type = token_payload.get("token_type")
    expires_in = token_payload.get("expires_in")
    scopes = token_payload.get("scope") or avito_api.DEFAULT_SCOPES
    obtained_at = datetime.now(tz=timezone.utc)
    expires_at = None
    if expires_in:
        try:
            expires_at = obtained_at + timedelta(seconds=int(expires_in))
        except Exception:
            expires_at = None

    if not refresh_token:
        return JSONResponse({"detail": "refresh_token_missing"}, status_code=400)

    try:
        user_info = await avito_api.get_user_me(access_token)
    except Exception:
        user_info = {}

    account_candidate = None
    display_name = None
    if isinstance(user_info, dict):
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
    if account_id is None:
        return JSONResponse({"detail": "account_unknown", "raw": user_info}, status_code=400)

    sanitized_token_payload = dict(token_payload)
    sanitized_token_payload.pop("access_token", None)
    sanitized_token_payload.pop("refresh_token", None)
    sanitized_token_payload["user"] = user_info

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
            raw_payload=sanitized_token_payload,
        )
    except EncryptionError as exc:
        return JSONResponse({"detail": "encryption_error", "error": str(exc)}, status_code=500)
    except Exception:
        logger.exception("avito_analytics_token_store_failed account_id=%s", account_id)
        return JSONResponse({"detail": "store_failed"}, status_code=500)

    redirect_url = f"/admin/avito-analytics?{urlencode({'account_id': account_id, 'auth': 'ok'})}"
    return RedirectResponse(url=redirect_url, status_code=303)


@router.get("/admin/avito-analytics/api/accounts")
async def avito_analytics_accounts(request: Request):
    guard = _require_admin(request)
    if guard:
        return guard
    await _ensure_schema()
    summary = await analytics_service.accounts_summary()
    return {"ok": True, "accounts": summary}


@router.get("/admin/avito-analytics/api/report")
async def avito_analytics_report(request: Request, account_id: int | None = None, period: int = 30):
    guard = _require_admin(request)
    if guard:
        return guard
    await _ensure_schema()
    accounts = await tokens_repo.list_tokens()
    if not accounts:
        return JSONResponse({"detail": "no_accounts"}, status_code=404)
    target_account = account_id or accounts[0].account_id
    if period not in (7, 30, 90):
        try:
            period = max(1, min(365, int(period)))
        except Exception:
            period = 30
    try:
        report = await analytics_service.build_report(int(target_account), int(period))
    except avito_api.AvitoOAuthError as exc:
        await tokens_repo.mark_error(int(target_account), str(exc))
        return JSONResponse({"detail": "oauth_error", "error": str(exc)}, status_code=401)
    except EncryptionError as exc:
        return JSONResponse({"detail": "encryption_error", "error": str(exc)}, status_code=500)
    except Exception as exc:
        logger.exception("avito_analytics_report_failed account_id=%s", target_account)
        return JSONResponse({"detail": "report_failed", "error": str(exc)}, status_code=500)
    return {"ok": True, "account_id": target_account, "report": report}


@router.get("/admin/avito-analytics/api/export.json")
async def avito_analytics_export_json(request: Request, account_id: int | None = None, period: int = 30):
    resp = await avito_analytics_report(request, account_id=account_id, period=period)
    if isinstance(resp, Response) and resp.status_code != 200:
        return resp
    payload = resp["report"] if isinstance(resp, dict) else {}
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    headers = {"Content-Disposition": f'attachment; filename="avito-analytics-{account_id or "account"}.json"'}
    return Response(content, media_type="application/json", headers=headers)


@router.get("/admin/avito-analytics/api/export.csv")
async def avito_analytics_export_csv(request: Request, account_id: int | None = None, period: int = 30, kind: str | None = None):
    resp = await avito_analytics_report(request, account_id=account_id, period=period)
    if isinstance(resp, Response) and resp.status_code != 200:
        return resp
    report = resp["report"] if isinstance(resp, dict) else {}
    data_kind = (kind or "items").strip().lower()
    import io
    import csv

    def _build_rows(header: list[str], rows: list[list[Any]], filename: str) -> Response:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
        content = buffer.getvalue()
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return Response(content, media_type="text/csv", headers=headers)

    if data_kind == "operations":
        operations = report.get("operations") or []
        rows = []
        for op in operations:
            rows.append(
                [
                    op.get("id") or "",
                    op.get("date") or op.get("created_at") or "",
                    op.get("type") or "",
                    op.get("description") or op.get("title") or "",
                    op.get("amount") or op.get("sum") or "",
                ]
            )
        return _build_rows(
            ["id", "date", "type", "description", "amount"],
            rows,
            f"avito-operations-{account_id or 'account'}.csv",
        )
    if data_kind == "job_applications":
        jobs = (report.get("job_applications") or {}).get("table") or []
        rows = []
        for row in jobs:
            rows.append(
                [
                    row.get("id") or "",
                    row.get("status") or "",
                    row.get("created_at") or "",
                    row.get("vacancy_id") or "",
                    row.get("resume_id") or "",
                    row.get("applicant") or "",
                ]
            )
        return _build_rows(
            ["id", "status", "created_at", "vacancy_id", "resume_id", "applicant"],
            rows,
            f"avito-job-applications-{account_id or 'account'}.csv",
        )
    if data_kind == "vas_prices":
        services = []
        prices_raw = (report.get("vas") or {}).get("raw", {}).get("prices")
        if isinstance(prices_raw, list):
            services = prices_raw
        elif isinstance(prices_raw, Mapping):
            services = prices_raw.get("services") or prices_raw.get("result") or []
        rows = []
        if isinstance(services, list):
            for svc in services:
                if not isinstance(svc, Mapping):
                    continue
                rows.append(
                    [
                        svc.get("name") or svc.get("service") or "",
                        svc.get("price") or svc.get("amount") or "",
                        svc.get("duration") or svc.get("period") or "",
                    ]
                )
        return _build_rows(
            ["name", "price", "duration"],
            rows,
            f"avito-vas-prices-{account_id or 'account'}.csv",
        )
    if data_kind == "vas_packages":
        packages = []
        packages_raw = (report.get("vas") or {}).get("raw", {}).get("packages")
        if isinstance(packages_raw, list):
            packages = packages_raw
        elif isinstance(packages_raw, Mapping):
            packages = packages_raw.get("packages") or packages_raw.get("result") or []
        rows = []
        if isinstance(packages, list):
            for pkg in packages:
                if not isinstance(pkg, Mapping):
                    continue
                rows.append(
                    [
                        pkg.get("name") or "",
                        pkg.get("price") or pkg.get("amount") or "",
                        pkg.get("duration") or pkg.get("period") or "",
                    ]
                )
        return _build_rows(
            ["name", "price", "duration"],
            rows,
            f"avito-vas-packages-{account_id or 'account'}.csv",
        )

    # default: items
    items = report.get("items_table") or []
    rows = []
    for row in items:
        rows.append(
            [
                row.get("id") or "",
                row.get("title") or "",
                row.get("status") or "",
                row.get("price") or "",
                row.get("views") or "",
                row.get("contacts") or "",
                row.get("calls") or "",
                row.get("url") or "",
            ]
        )
    return _build_rows(
        ["id", "title", "status", "price", "views", "contacts", "calls", "url"],
        rows,
        f"avito-items-{account_id or 'account'}.csv",
    )


@router.post("/admin/avito-analytics/api/refresh")
async def avito_analytics_refresh(request: Request, account_id: int | None = None, period: int = 30):
    guard = _require_admin(request)
    if guard:
        return guard
    await _ensure_schema()
    accounts = await tokens_repo.list_tokens()
    if not accounts:
        return JSONResponse({"detail": "no_accounts"}, status_code=404)
    target_account = account_id or accounts[0].account_id
    await analytics_service.drop_cache(int(target_account), period)
    try:
        report = await analytics_service.build_report(int(target_account), int(period), force_refresh=True)
    except Exception as exc:
        logger.exception("avito_analytics_refresh_failed account_id=%s", target_account)
        return JSONResponse({"detail": "refresh_failed", "error": str(exc)}, status_code=500)
    return {"ok": True, "report": report}


@router.post("/admin/avito-analytics/api/job/application/add")
async def avito_analytics_job_app_add(request: Request):
    guard = _require_admin(request)
    if guard:
        return guard
    payload = await request.json()
    account_id = payload.get("account_id")
    application_id = payload.get("application_id")
    if not account_id or not application_id:
        return JSONResponse({"detail": "missing_account_or_id"}, status_code=400)
    try:
        await job_repo.store_event(int(account_id), str(application_id), source="manual", payload=payload)
    except Exception:
        logger.exception("avito_job_app_manual_add_failed account_id=%s", account_id)
        return JSONResponse({"detail": "store_failed"}, status_code=500)
    return {"ok": True}


@router.post("/admin/avito-analytics/api/disconnect")
async def avito_analytics_disconnect(request: Request, account_id: int):
    guard = _require_admin(request)
    if guard:
        return guard
    await _ensure_schema()
    try:
        await tokens_repo.delete(int(account_id))
        await analytics_service.drop_cache(int(account_id))
    except Exception:
        logger.exception("avito_analytics_disconnect_failed account_id=%s", account_id)
        return JSONResponse({"detail": "disconnect_failed"}, status_code=500)
    return {"ok": True}
