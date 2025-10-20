import os
import json
import logging
from typing import Any
from urllib.parse import quote_plus
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from core import ADMIN_COOKIE, settings, get_tenant_pubkey, set_tenant_pubkey
from . import common as C
from .ui import templates
import secrets

from app.repo import provider_tokens as provider_tokens_repo

router = APIRouter()
_log = logging.getLogger("app.web.admin")


def _auth_ok(request: Request) -> bool:
    token = (request.query_params.get("token") or request.headers.get("X-Admin-Token") or "").strip()
    if token and token == settings.ADMIN_TOKEN:
        return True
    cookie = (request.cookies.get(ADMIN_COOKIE) or "").strip()
    return bool(cookie) and cookie == settings.ADMIN_TOKEN


@router.get("/admin/login")
def login(request: Request, token: str | None = None):
    cookie_value = (request.cookies.get(ADMIN_COOKIE) or "").strip()
    if cookie_value and cookie_value == settings.ADMIN_TOKEN:
        return RedirectResponse(url="/admin")

    admin_token = settings.ADMIN_TOKEN
    error = None

    if token:
        token = token.strip()
        if token and token == admin_token:
            resp = RedirectResponse(url="/admin", status_code=303)
            resp.set_cookie(
                ADMIN_COOKIE,
                admin_token,
                max_age=60 * 60 * 24 * 14,
                httponly=True,
                secure=True,
                samesite="lax",
            )
            return resp
        error = "Неверный токен доступа"

    context = {
        "request": request,
        "title": "Avio · Вход",
        "subtitle": "Доступ для команды",
        "error": error,
    }
    return templates.TemplateResponse("admin/login.html", context)


@router.get("/admin")
def dashboard(request: Request, tenant: int = 1):
    if not _auth_ok(request):
        return RedirectResponse(url="/admin/login")
    tenant = int(tenant)
    keys = C.list_keys(tenant)
    primary = next((item for item in keys if item.get("primary")), None)
    public_base = C.public_base_url(request)

    context = {
        "request": request,
        "tenant": tenant,
        "keys": keys,
        "primary_key": primary,
        "subtitle": f"Tenant {tenant}",
        "title": f"Админка · Tenant {tenant}",
        "public_base": public_base,
    }
    return templates.TemplateResponse("admin/dashboard.html", context)


@router.get("/admin/keys/list")
async def keys_list(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    token_entry = await provider_tokens_repo.get_by_tenant(int(tenant))
    provider_token = token_entry.token if token_entry else ""
    return {
        "ok": True,
        "items": C.list_keys(int(tenant)),
        "provider_token": provider_token,
    }


@router.get("/admin/provider-token/{tenant}")
async def provider_token_get(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    try:
        token_entry = await provider_tokens_repo.get_by_tenant(int(tenant))
    except Exception:
        _log.exception("provider_token_fetch_failed tenant=%s", tenant)
        return JSONResponse({"detail": "db_error"}, status_code=500)
    if not token_entry or not token_entry.token:
        new_token = secrets.token_urlsafe(32)
        try:
            token_entry = await provider_tokens_repo.upsert(int(tenant), new_token)
        except Exception:
            _log.exception("provider_token_upsert_failed tenant=%s", tenant)
            return JSONResponse({"detail": "db_error"}, status_code=500)
        if not token_entry:
            return JSONResponse({"detail": "db_error"}, status_code=500)
    return {
        "ok": True,
        "tenant": int(tenant),
        "provider_token": token_entry.token,
        "created_at": token_entry.created_at.isoformat(),
    }


@router.post("/admin/keys/generate")
async def keys_generate(request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    payload = await request.json()
    tenant = int(payload.get("tenant"))
    label = (payload.get("label") or "").strip()
    existing = (get_tenant_pubkey(tenant) or "").strip()
    if existing:
        return JSONResponse({"error": "key_already_exists"}, status_code=409)
    key = os.urandom(16).hex()
    C.add_key(tenant, key, label)
    C.ensure_tenant_files(tenant)
    C.set_primary(tenant, key)
    encoded = quote_plus(key)
    link = f"/connect/wa?tenant={tenant}&k={encoded}"
    settings_link = f"/client/{tenant}/settings?k={encoded}"
    return {"ok": True, "key": key, "link": link, "settings_link": settings_link}


@router.post("/admin/keys/save")
async def keys_save(request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    payload = await request.json()
    tenant = int(payload.get("tenant"))
    key = (payload.get("key") or "").strip()
    label = (payload.get("label") or "").strip()
    if not key:
        return {"ok": False, "error": "empty_key"}
    current = (get_tenant_pubkey(tenant) or "").strip()
    if current and current.lower() != key.lower():
        return JSONResponse({"error": "key_already_exists"}, status_code=409)
    C.add_key(tenant, key, label)
    C.ensure_tenant_files(tenant)
    C.set_primary(tenant, key)
    encoded = quote_plus(key)
    link = f"/connect/wa?tenant={tenant}&k={encoded}"
    settings_link = f"/client/{tenant}/settings?k={encoded}"
    return {"ok": True, "key": key, "link": link, "settings_link": settings_link}


@router.post("/admin/keys/delete")
async def keys_delete(request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    payload = await request.json()
    tenant = int(payload.get("tenant"))
    key = (payload.get("key") or "").strip()
    if not key:
        return {"ok": False, "error": "empty_key"}
    C.del_key(tenant, key)
    if (get_tenant_pubkey(tenant) or "").strip().lower() == key.lower():
        set_tenant_pubkey(tenant, "")
    return {"ok": True}


# совместимость REST-хендлеров v1
@router.get("/admin/key/get")
def admin_key_get(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    tenant_id = int(tenant)
    items = C.list_keys(tenant_id)
    if items:
        key_value = items[0].get("key", "")
    else:
        existing = (get_tenant_pubkey(tenant_id) or "").strip()
        key_value = existing
        if not existing:
            key_value = os.urandom(16).hex()
            C.add_key(tenant_id, key_value, "primary")
            C.ensure_tenant_files(tenant_id)
            C.set_primary(tenant_id, key_value)
            items = C.list_keys(tenant_id)
            if items:
                key_value = items[0].get("key", key_value)
    return {"ok": True, "tenant": tenant_id, "key": key_value}


@router.post("/admin/key/generate")
def admin_key_generate(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    tenant_id = int(tenant)
    existing = (get_tenant_pubkey(tenant_id) or "").strip()
    if existing:
        return JSONResponse({"error": "key_already_exists"}, status_code=409)
    key = os.urandom(16).hex()
    C.add_key(tenant_id, key, "primary")
    C.set_primary(tenant_id, key)
    C.ensure_tenant_files(tenant_id)
    return {"ok": True, "tenant": tenant_id, "key": key}


@router.post("/admin/key/save")
async def admin_key_save(
    request: Request,
    tenant: int | str | None = None,
    key: str | None = None,
    k: str | None = None,
):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    raw_tenant: int | str | None = tenant
    raw_key: str | None = key or k

    payload: dict[str, Any] = {}
    if raw_tenant is None or not raw_key:
        try:
            data = await request.json()
            if isinstance(data, dict):
                payload.update(data)
        except Exception:
            payload = {}
        if not payload:
            try:
                form = await request.form()
            except Exception:
                form = None
            if form is not None:
                payload = {}
                for form_key, value in form.multi_items():
                    if form_key not in payload:
                        payload[form_key] = value

        if raw_tenant is None:
            raw_tenant = payload.get("tenant")
        if not raw_key:
            raw_key = payload.get("key") or payload.get("k")

    if raw_tenant is None:
        qp = request.query_params
        raw_tenant = qp.get("tenant")
    if not raw_key:
        qp = request.query_params
        raw_key = qp.get("key") or qp.get("k")

    try:
        tenant_id = int(raw_tenant)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return {"ok": False, "error": "invalid_tenant"}

    key_value = "" if raw_key is None else str(raw_key).strip()
    if not key_value:
        return {"ok": False, "error": "empty_key"}

    current = (get_tenant_pubkey(tenant_id) or "").strip()
    if current and current.lower() != key_value.lower():
        return JSONResponse({"error": "key_already_exists"}, status_code=409)

    C.add_key(tenant_id, key_value, "manual")
    C.set_primary(tenant_id, key_value)
    C.ensure_tenant_files(tenant_id)
    return {"ok": True, "tenant": tenant_id, "key": key_value}


@router.get("/admin/wa/status")
async def admin_wa_status(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    # Prefer tenant-specific status; fall back to global for legacy setups
    code, raw = C.http("GET", f"{C.WA_WEB_URL}/session/{int(tenant)}/status")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{C.WA_WEB_URL}/session/status")
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    # Normalize minimal shape for the admin panel
    state = (data.get("last") or data.get("state") or ("no_session" if int(code or 0) == 404 else "unknown")).strip() if isinstance(data, dict) else "unknown"
    resp = {
        "ok": bool(data.get("ok", True)) if isinstance(data, dict) else True,
        "tenant": int(tenant),
        "ready": bool(data.get("ready")) if isinstance(data, dict) else False,
        "qr": bool(data.get("qr")) if isinstance(data, dict) else False,
        "state": state,
    }
    # Always return 200 to avoid noisy 404s in the admin UI
    return JSONResponse(resp, status_code=200, headers={"X-Debug-Stage": f"admin_status_{'tenant' if int(code or 0)!=404 else 'global'}"})


@router.get("/admin/wa/qr.svg")
def admin_wa_qr(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    # Prefer tenant-scoped QR endpoints; fallback to legacy global paths
    code, raw = C.http("GET", f"{C.WA_WEB_URL}/session/{int(tenant)}/qr.svg")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{C.WA_WEB_URL}/session/{int(tenant)}/qr.png")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{C.WA_WEB_URL}/session/qr?format=svg")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{C.WA_WEB_URL}/session/qr.svg")
    headers = {"Cache-Control": "no-store", "X-Debug-Stage": f"admin_qr_{code or 0}"}
    if code == 200 and raw and "<svg" in raw:
        return Response(raw.encode("utf-8"), media_type="image/svg+xml", headers=headers)
    return Response(b"", media_type="image/svg+xml", status_code=404, headers=headers)
