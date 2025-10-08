import os
import json
from urllib.parse import quote_plus
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from core import ADMIN_COOKIE, settings, get_tenant_pubkey, set_tenant_pubkey
from . import common as C
from .ui import templates

router = APIRouter()


def _auth_ok(request: Request) -> bool:
    token = (request.query_params.get("token") or request.headers.get("X-Admin-Token") or "").strip()
    if token and token == settings.ADMIN_TOKEN:
        return True
    cookie = (request.cookies.get("admin") or "").strip()
    return bool(cookie) and cookie == ADMIN_COOKIE


@router.get("/admin/login")
def login(request: Request, token: str | None = None):
    cookie_value = (request.cookies.get("admin") or "").strip()
    if cookie_value == ADMIN_COOKIE:
        return RedirectResponse(url="/admin")

    admin_token = settings.ADMIN_TOKEN
    error = None

    if token:
        token = token.strip()
        if token and token == admin_token:
            resp = RedirectResponse(url="/admin", status_code=303)
            resp.set_cookie("admin", ADMIN_COOKIE, max_age=86400 * 7, httponly=True, samesite="lax")
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
def keys_list(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    return {"ok": True, "items": C.list_keys(int(tenant))}


@router.post("/admin/keys/generate")
async def keys_generate(request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    payload = await request.json()
    tenant = int(payload.get("tenant"))
    label = (payload.get("label") or "").strip()
    key = os.urandom(16).hex()
    C.add_key(tenant, key, label)
    C.ensure_tenant_files(tenant)
    if not (get_tenant_pubkey(tenant) or ""):
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
    primary = bool(payload.get("primary"))
    if not key:
        return {"ok": False, "error": "empty_key"}
    C.add_key(tenant, key, label)
    C.ensure_tenant_files(tenant)
    if primary or not (get_tenant_pubkey(tenant) or ""):
        C.set_primary(tenant, key)
    encoded = quote_plus(key)
    link = f"/connect/wa?tenant={tenant}&k={encoded}"
    settings_link = f"/client/{tenant}/settings?k={encoded}"
    return {"ok": True, "key": key, "link": link, "settings_link": settings_link}


@router.post("/admin/keys/set_primary")
async def keys_set_primary(request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    payload = await request.json()
    tenant = int(payload.get("tenant"))
    key = (payload.get("key") or "").strip()
    if not key:
        return {"ok": False, "error": "empty_key"}
    C.set_primary(tenant, key)
    return {"ok": True}


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
    key = get_tenant_pubkey(int(tenant)) or ""
    return {"ok": True, "tenant": int(tenant), "key": key}


@router.post("/admin/key/generate")
def admin_key_generate(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    key = os.urandom(16).hex()
    C.add_key(int(tenant), key, "primary")
    C.set_primary(int(tenant), key)
    C.ensure_tenant_files(int(tenant))
    return {"ok": True, "tenant": int(tenant), "key": key}


@router.post("/admin/key/save")
def admin_key_save(tenant: int, key: str, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    key = key.strip()
    C.add_key(int(tenant), key, "manual")
    C.set_primary(int(tenant), key)
    C.ensure_tenant_files(int(tenant))
    return {"ok": True, "tenant": int(tenant), "key": key}


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
