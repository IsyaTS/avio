import os
import json
import base64
import logging
import pathlib
import secrets
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, quote_plus

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
import httpx

from libs.constants import ADMIN_TENANT_ID
from libs.core import sales_core as core
from libs.core.sales_core import ADMIN_COOKIE, settings, get_tenant_pubkey, set_tenant_pubkey
from libs.core.common import OUTBOX_QUEUE_KEY, OUTBOX_DLQ_KEY
from libs.core.repo import provider_tokens as provider_tokens_repo
from . import common as C
from .ui import render_template

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
            # In dev (http) secure cookies are not stored, so only mark secure when using https.
            secure_flag = request.url.scheme == "https"
            resp.set_cookie(
                ADMIN_COOKIE,
                admin_token,
                max_age=60 * 60 * 24 * 14,
                httponly=True,
                secure=secure_flag,
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
    return render_template("admin/login.html", context)


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
    return render_template("admin/dashboard.html", context)


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


@router.get("/admin/queue-stats")
def queue_stats(request: Request, sample: int = 500):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    try:
        redis_client = C.redis_client()
    except Exception:
        return JSONResponse({"detail": "redis_unavailable"}, status_code=503)

    sample_limit = max(0, min(int(sample or 0), 2000))
    try:
        outbox_len = int(redis_client.llen(OUTBOX_QUEUE_KEY))
    except Exception:
        outbox_len = 0
    try:
        dlq_len = int(redis_client.llen(OUTBOX_DLQ_KEY))
    except Exception:
        dlq_len = 0
    try:
        followup_len = int(redis_client.zcard("followup:schedule"))
    except Exception:
        followup_len = 0

    tenant_counts: dict[int, int] = {}
    sampled = 0
    if sample_limit > 0:
        try:
            items = redis_client.lrange(OUTBOX_QUEUE_KEY, 0, sample_limit - 1)
        except Exception:
            items = []
        sampled = len(items)
        for raw in items:
            try:
                payload = json.loads(raw) if raw else {}
            except Exception:
                payload = {}
            tenant_raw = payload.get("tenant_id") or payload.get("tenant")
            try:
                tenant_id = int(tenant_raw)
            except Exception:
                continue
            if tenant_id <= 0:
                continue
            tenant_counts[tenant_id] = tenant_counts.get(tenant_id, 0) + 1

    top_tenants = [
        {"tenant_id": tid, "count": count}
        for tid, count in sorted(tenant_counts.items(), key=lambda item: item[1], reverse=True)[:20]
    ]

    return {
        "ok": True,
        "outbox_len": outbox_len,
        "dlq_len": dlq_len,
        "followup_scheduled_len": followup_len,
        "sampled": sampled,
        "outbox_by_tenant": top_tenants,
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
            set_tenant_pubkey(tenant_id, key_value)
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
    provider = C.whatsapp_provider(int(tenant))
    if provider == "baileys":
        code, raw = C.wabaileys_http("GET", f"/sessions/status?tenant={int(tenant)}", timeout=3.0)
        try:
            data = json.loads(raw)
        except Exception:
            data = {}
        session = data.get("session") if isinstance(data.get("session"), dict) else {}
        state = str(session.get("status") or "").strip() or "unknown"
        resp = {
            "ok": bool(data.get("ok", True)),
            "tenant": int(tenant),
            "ready": bool(session.get("connected")),
            "qr": bool(session.get("qr")),
            "state": state,
        }
        return JSONResponse(resp, status_code=200, headers={"X-Debug-Stage": "admin_status_baileys"})

    base_url = C.wa_base_url(int(tenant))
    code, raw = C.http("GET", f"{base_url}/session/{int(tenant)}/status")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{base_url}/session/status")
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    state = (data.get("last") or data.get("state") or ("no_session" if int(code or 0) == 404 else "unknown")).strip() if isinstance(data, dict) else "unknown"
    resp = {
        "ok": bool(data.get("ok", True)) if isinstance(data, dict) else True,
        "tenant": int(tenant),
        "ready": bool(data.get("ready")) if isinstance(data, dict) else False,
        "qr": bool(data.get("qr")) if isinstance(data, dict) else False,
        "state": state,
    }
    return JSONResponse(resp, status_code=200, headers={"X-Debug-Stage": f"admin_status_{'tenant' if int(code or 0)!=404 else 'global'}"})


@router.get("/admin/wa/qr.svg")
def admin_wa_qr(tenant: int, request: Request):
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    if C.whatsapp_provider(int(tenant)) == "baileys":
        return _admin_baileys_qr_response(int(tenant))
    # Prefer tenant-scoped QR endpoints; fallback to legacy global paths
    base_url = C.wa_base_url(int(tenant))
    code, raw = C.http("GET", f"{base_url}/session/{int(tenant)}/qr.svg")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{base_url}/session/{int(tenant)}/qr.png")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{base_url}/session/qr?format=svg")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{base_url}/session/qr.svg")
    headers = {"Cache-Control": "no-store", "X-Debug-Stage": f"admin_qr_{code or 0}"}
    if code == 200 and raw and "<svg" in raw:
        return Response(raw.encode("utf-8"), media_type="image/svg+xml", headers=headers)
    return Response(b"", media_type="image/svg+xml", status_code=404, headers=headers)


def _admin_baileys_qr_response(tenant: int) -> Response:
    code, raw = C.wabaileys_http("GET", f"/sessions/status?tenant={int(tenant)}", timeout=3.0)
    if int(code or 0) < 200 or int(code or 0) >= 300:
        return Response(b"", media_type="image/svg+xml", status_code=int(code or 0) or 502)
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    session = data.get("session") if isinstance(data.get("session"), dict) else {}
    qr_block = session.get("qr") if isinstance(session.get("qr"), dict) else {}
    if not qr_block:
        return Response(b"", media_type="image/svg+xml", status_code=404)
    qr_id = str(qr_block.get("id") or qr_block.get("raw") or "")
    headers = {"Cache-Control": "no-store", "X-Debug-Stage": "admin_qr_baileys"}
    if qr_id:
        headers["X-WA-QR-ID"] = qr_id
    svg_blob = qr_block.get("svg")
    if isinstance(svg_blob, str) and svg_blob.strip():
        return Response(svg_blob.encode("utf-8"), media_type="image/svg+xml", headers=headers)
    png_blob = qr_block.get("png")
    if isinstance(png_blob, str) and png_blob.strip():
        try:
            binary = base64.b64decode(png_blob, validate=True)
        except Exception:
            binary = b""
        if binary:
            return Response(binary, media_type="image/png", headers=headers)
    return Response(b"", media_type="image/svg+xml", status_code=404, headers=headers)


def _require_admin(request: Request) -> JSONResponse | None:
    if not settings.ADMIN_TOKEN:
        return JSONResponse({"detail": "admin_token_missing"}, status_code=500)
    if not _auth_ok(request):
        return JSONResponse({"detail": "unauthorized"}, status_code=403)
    return None


def _tgworker_base_url() -> str:
    base = (
        getattr(settings, "TGWORKER_BASE_URL", "")
        or getattr(settings, "WORKER_BASE_URL", "")
        or "http://tgworker:8000"
    )
    cleaned = str(base).strip()
    return cleaned.rstrip("/") or "http://tgworker:8000"


def _tgworker_url(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{_tgworker_base_url()}{path}"


def _admin_exports_dir() -> pathlib.Path:
    env_dir = (os.getenv("APP_DATA_DIR") or "").strip()
    if env_dir:
        base = pathlib.Path(env_dir)
    elif pathlib.Path("/data").is_dir():
        base = pathlib.Path("/data")
    else:
        base = pathlib.Path(__file__).resolve().parents[3] / "data"
    export_dir = base / "admin_exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir


def _parse_usernames_from_text(raw_text: str) -> list[str]:
    usernames: list[str] = []
    seen: set[str] = set()
    for line in raw_text.splitlines():
        cleaned = line.strip()
        if not cleaned:
            continue
        token = cleaned.split()[0].strip()
        if not token:
            continue
        token = token.lstrip("@")
        if not token:
            continue
        normalized = f"@{token}"
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        usernames.append(normalized)
    return usernames


async def _tgworker_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> httpx.Response:
    url = _tgworker_url(path)
    headers = {"X-Admin-Token": settings.ADMIN_TOKEN}
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        return await client.request(
            method.upper(),
            url,
            params=params,
            json=payload,
            headers=headers,
        )


@router.get("/admin/_secret/tgexport")
def admin_tgexport_page(request: Request):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    context = {
        "request": request,
        "title": "TG Export",
        "admin_tenant_id": ADMIN_TENANT_ID,
    }
    return render_template("admin/tgexport.html", context)


@router.post("/admin/_secret/tgexport/start")
async def admin_tgexport_start(request: Request):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    resp = await _tgworker_request(
        "POST",
        "/session/start",
        payload={"tenant": ADMIN_TENANT_ID, "force": False},
    )
    try:
        data = resp.json()
    except Exception:
        data = {}
    if resp.status_code >= 400:
        return JSONResponse(data or {"error": "tgworker_error"}, status_code=resp.status_code)
    qr_id = data.get("qr_id") if isinstance(data, dict) else None
    status = data.get("status") if isinstance(data, dict) else None
    return JSONResponse({"qr_id": qr_id, "status": status}, status_code=200)


@router.get("/admin/_secret/tgexport/qr")
async def admin_tgexport_qr(request: Request, qr_id: str):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    safe_qr = quote(qr_id, safe="")
    resp = await _tgworker_request("GET", f"/session/qr/{safe_qr}.png")
    headers = {"Cache-Control": "no-store"}
    media_type = resp.headers.get("Content-Type") or "image/png"
    return Response(resp.content, status_code=resp.status_code, headers=headers, media_type=media_type)


@router.get("/admin/_secret/tgexport/status")
async def admin_tgexport_status(request: Request):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    resp = await _tgworker_request("GET", "/session/status", params={"tenant": ADMIN_TENANT_ID})
    try:
        data = resp.json()
    except Exception:
        data = {}
    if resp.status_code >= 400:
        return JSONResponse(data or {"error": "tgworker_error"}, status_code=resp.status_code)
    status = data.get("status") if isinstance(data, dict) else None
    need_2fa = bool(
        data.get("needs_2fa")
        or data.get("twofa_pending")
        or data.get("status") in {"needs_2fa", "need_2fa"}
    )
    payload = {
        "status": status,
        "authorized": status == "authorized",
        "need_2fa": need_2fa,
        "qr_id": data.get("qr_id") if isinstance(data, dict) else None,
        "qr_valid_until": data.get("qr_valid_until") if isinstance(data, dict) else None,
        "last_error": data.get("last_error") if isinstance(data, dict) else None,
    }
    return JSONResponse(payload, status_code=200)


@router.post("/admin/_secret/tgexport/password")
async def admin_tgexport_password(request: Request):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"detail": "invalid_json"}, status_code=400)
    password = (payload.get("password") or "").strip()
    resp = await _tgworker_request(
        "POST",
        "/session/password",
        payload={"tenant": ADMIN_TENANT_ID, "password": password},
    )
    try:
        data = resp.json()
    except Exception:
        data = {}
    return JSONResponse(data or {"error": "tgworker_error"}, status_code=resp.status_code)


@router.get("/admin/_secret/tgexport/dialogs")
async def admin_tgexport_dialogs(request: Request):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    resp = await _tgworker_request("GET", "/admin/tg/dialogs", params={"tenant": ADMIN_TENANT_ID})
    try:
        data = resp.json()
    except Exception:
        data = {}
    if resp.status_code >= 400:
        return JSONResponse(data or {"error": "tgworker_error"}, status_code=resp.status_code)
    return JSONResponse(data, status_code=200)


@router.get("/admin/_secret/tgexport/export")
async def admin_tgexport_export(request: Request, chat_id: int):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    resp = await _tgworker_request(
        "POST",
        "/admin/tg/members",
        payload={"tenant": ADMIN_TENANT_ID, "chat_id": int(chat_id)},
        timeout=30.0,
    )
    try:
        data = resp.json()
    except Exception:
        data = {}
    if resp.status_code >= 400:
        return JSONResponse(data or {"error": "tgworker_error"}, status_code=resp.status_code)
    usernames = data.get("usernames") if isinstance(data, dict) else None
    if not isinstance(usernames, list):
        return JSONResponse({"error": "invalid_upstream_payload"}, status_code=502)
    export_dir = _admin_exports_dir()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{int(chat_id)}.txt"
    export_path = export_dir / filename
    export_text = "\n".join(str(item) for item in usernames if item)
    export_path.write_text(f"{export_text}\n" if export_text else "", encoding="utf-8")
    return FileResponse(
        export_path,
        media_type="text/plain",
        filename=filename,
    )


@router.post("/admin/_secret/tgexport/logout")
async def admin_tgexport_logout(request: Request):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    resp = await _tgworker_request(
        "POST",
        "/session/logout",
        payload={"tenant": ADMIN_TENANT_ID, "force": False},
    )
    try:
        data = resp.json()
    except Exception:
        data = {}
    return JSONResponse(data or {"error": "tgworker_error"}, status_code=resp.status_code)


@router.post("/admin/_secret/tgexport/broadcast")
async def admin_tgexport_broadcast(
    request: Request,
    file: UploadFile = File(...),
    message: str = Form(...),
    limit: int = Form(1000),
    pause_min_s: float = Form(4.0),
    pause_max_s: float = Form(7.0),
):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    raw_message = (message or "").strip()
    if not raw_message:
        return JSONResponse({"error": "empty_message"}, status_code=400)
    if limit <= 0:
        return JSONResponse({"error": "invalid_limit"}, status_code=400)
    try:
        payload = await file.read()
    except Exception:
        return JSONResponse({"error": "file_read_failed"}, status_code=400)
    try:
        text = payload.decode("utf-8", errors="ignore")
    except Exception:
        text = ""
    usernames = _parse_usernames_from_text(text)
    if not usernames:
        return JSONResponse({"error": "empty_usernames"}, status_code=400)
    max_limit = 5000
    safe_limit = min(int(limit), max_limit)
    if safe_limit < len(usernames):
        usernames = usernames[:safe_limit]
    pause_min = max(float(pause_min_s or 0.0), 0.0)
    pause_max = max(float(pause_max_s or 0.0), 0.0)
    resp = await _tgworker_request(
        "POST",
        "/admin/tg/broadcast",
        payload={
            "tenant": ADMIN_TENANT_ID,
            "message": raw_message,
            "usernames": usernames,
            "limit": safe_limit,
            "pause_min_s": pause_min,
            "pause_max_s": pause_max,
        },
        timeout=20.0,
    )
    try:
        data = resp.json()
    except Exception:
        data = {}
    return JSONResponse(data or {"error": "tgworker_error"}, status_code=resp.status_code)


@router.get("/admin/_secret/tgexport/broadcast/status")
async def admin_tgexport_broadcast_status(request: Request, job_id: str):
    guard = _require_admin(request)
    if guard is not None:
        return guard
    resp = await _tgworker_request(
        "GET",
        "/admin/tg/broadcast/status",
        params={"job_id": job_id},
        timeout=10.0,
    )
    try:
        data = resp.json()
    except Exception:
        data = {}
    return JSONResponse(data or {"error": "tgworker_error"}, status_code=resp.status_code)
