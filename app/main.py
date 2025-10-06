from __future__ import annotations

import pathlib
import os, json, re, time, mimetypes
from urllib.parse import quote

import importlib
import sys

from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import logging
from logging import StreamHandler

if __package__ in (None, ""):
    core = importlib.import_module("core")  # type: ignore[assignment]
    ask_llm = core.ask_llm  # type: ignore[attr-defined]
    build_llm_messages = core.build_llm_messages  # type: ignore[attr-defined]
    settings = core.settings  # type: ignore[attr-defined]
    _common_mod = importlib.import_module("web.common")  # type: ignore[assignment]
    _admin_mod = importlib.import_module("web.admin")  # type: ignore[assignment]
    _public_mod = importlib.import_module("web.public")  # type: ignore[assignment]
    _client_mod = importlib.import_module("web.client")  # type: ignore[assignment]
else:
    core = importlib.import_module("app.core")
    sys.modules.setdefault("core", core)

    def _import_with_alias(name: str):
        if name in sys.modules:
            return sys.modules[name]
        module = importlib.import_module(f"app.{name}")
        sys.modules[name] = module
        return module

    sys.modules.setdefault("web", importlib.import_module("app.web"))
    _common_mod = _import_with_alias("web.common")
    _admin_mod = _import_with_alias("web.admin")
    _public_mod = _import_with_alias("web.public")
    _client_mod = _import_with_alias("web.client")

    ask_llm = core.ask_llm  # type: ignore[attr-defined]
    build_llm_messages = core.build_llm_messages  # type: ignore[attr-defined]
    settings = core.settings  # type: ignore[attr-defined]

C = _common_mod  # type: ignore[assignment]
admin_router = _admin_mod.router  # type: ignore[attr-defined]
public_router = _public_mod.router  # type: ignore[attr-defined]
client_router = _client_mod.router  # type: ignore[attr-defined]

import importlib.util as _importlib_util

ROOT = pathlib.Path(__file__).resolve().parent

try:  # рабочие БД-хелперы; при отсутствии БД заменяются заглушками
    from .db import (
        resolve_or_create_contact,
        link_lead_contact,
        insert_message_in,
        upsert_lead,
    )
except ImportError:  # pragma: no cover - фоллбек для окружений без БД
    async def resolve_or_create_contact(**_: object) -> int:  # type: ignore[override]
        return 0

    async def link_lead_contact(*_: object, **__: object) -> None:  # type: ignore[override]
        return None

    async def insert_message_in(*_: object, **__: object) -> None:  # type: ignore[override]
        return None

    async def upsert_lead(*_: object, **__: object) -> None:  # type: ignore[override]
        return None

def _init_logging():
    level_name = (os.getenv("LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt)

    # Explicit stdout handlers for custom loggers
    for name in ("training", "wa"):
        lg = logging.getLogger(name)
        lg.setLevel(level)
        # Avoid duplicate handlers on reload
        if not any(isinstance(h, StreamHandler) for h in lg.handlers):
            h = StreamHandler()
            h.setFormatter(logging.Formatter(fmt))
            lg.addHandler(h)

    # Ensure uvicorn access logs are enabled and formatted
    for logger_name in ("uvicorn", "uvicorn.access"):
        lg = logging.getLogger(logger_name)
        lg.setLevel(level)
        if not lg.handlers:
            handler = StreamHandler()
            handler.setFormatter(logging.Formatter(fmt))
            lg.addHandler(handler)


_init_logging()

# Module-level logger for request access
_access_logger = logging.getLogger("app.access")

app = FastAPI(title="avio-api")
static_dir = ROOT / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
webhook = APIRouter()

_r = settings.r

_catalog_sent_cache: dict[tuple[int, str], float] = {}

def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _resolve_catalog_attachment(
    cfg: dict | None,
    tenant: int,
    request: Request | None = None,
) -> tuple[dict | None, str]:
    if not isinstance(cfg, dict):
        return None, ""
    integrations = cfg.get("integrations", {}) if isinstance(cfg.get("integrations"), dict) else {}
    meta = integrations.get("uploaded_catalog")
    if not isinstance(meta, dict):
        return None, ""
    if (meta.get("type") or "").lower() != "pdf":
        return None, ""
    raw_path = (meta.get("path") or "").replace("\\", "/")
    if not raw_path:
        return None, ""
    try:
        safe = pathlib.PurePosixPath(raw_path)
    except Exception:
        return None, ""
    if safe.is_absolute() or ".." in safe.parts:
        return None, ""

    safe_path = str(safe)
    try:
        tenant_root = core.tenant_dir(tenant)
        target = tenant_root / safe_path
    except Exception:
        return None, ""

    if not target.exists() or not target.is_file():
        return None, ""

    if request is not None:
        base = str(request.url_for("internal_catalog_file", tenant=str(tenant)))
    else:
        base_root = settings.APP_INTERNAL_URL or settings.APP_PUBLIC_URL or ""
        if not base_root:
            base_root = "http://app:8000"
        base = f"{base_root.rstrip('/')}/internal/tenant/{tenant}/catalog-file"

    url = f"{base}?path={quote(str(safe), safe='/')}"
    token = settings.WEBHOOK_SECRET or ""
    if token:
        url += f"&token={quote(token)}"

    filename = meta.get("original") or safe.name
    mime = meta.get("mime") or "application/pdf"
    caption = f"Каталог в PDF: {filename}"

    attachment = {
        "url": url,
        "filename": filename,
        "mime_type": mime,
    }
    return attachment, caption

def _ok(data: dict | None = None, status: int = 200):
    return JSONResponse({"ok": True, **(data or {})}, status_code=status)

def _err(msg: str, status: int = 400):
    return JSONResponse({"ok": False, "error": msg}, status_code=status)


@app.get("/health")
def healthcheck():
    """Lightweight container health endpoint."""
    return JSONResponse({"ok": True})

async def _handle(req: Request):
    token = (req.query_params.get("token") or "").strip()
    secret = settings.WEBHOOK_SECRET
    if secret and token != secret:
        return _err("unauthorized", 401)

    try: body = await req.json()
    except Exception: body = {}

    src = body.get("source") or {}
    provider = (src.get("type") or "whatsapp").lower()
    tenant = int(src.get("tenant") or os.getenv("TENANT_ID", "1"))

    msg = body.get("message") or {}
    text = (msg.get("text") or msg.get("body") or "").strip()
    from_id = msg.get("from") or msg.get("author") or ""
    phone = _digits((from_id.split("@", 1)[0]) if from_id else "")

    lead_id = body.get("leadId") or body.get("lead_id") or int(time.time() * 1000)
    try: lead_id = int(str(lead_id))
    except Exception: lead_id = int(time.time() * 1000)

    if not text:
        return _ok({"skipped": True, "reason": "no_text"})

    # контакт в БД
    contact_id = 0
    stored_incoming = False
    try:
        await upsert_lead(lead_id, channel=provider or "whatsapp", tenant_id=tenant)
    except Exception:
        pass
    try:
        contact_id = await resolve_or_create_contact(whatsapp_phone=phone)
        if contact_id:
            await link_lead_contact(lead_id, contact_id)
            await insert_message_in(lead_id, text, status="received", tenant_id=tenant)
            stored_incoming = True
    except Exception:
        pass

    if not stored_incoming:
        try:
            await insert_message_in(lead_id, text, status="received", tenant_id=tenant)
        except Exception:
            pass

    refer_id = contact_id or lead_id

    state = None
    catalog_already_sent = False
    cache_key: tuple[int, str] | None = None
    now_ts = time.time()
    try:
        state = core.load_sales_state(tenant, refer_id)
        catalog_already_sent = bool(getattr(state, "catalog_sent", False))
    except Exception:
        state = None
    if phone:
        cache_key = (tenant, phone)
        cached_ts = _catalog_sent_cache.get(cache_key)
        if cached_ts and now_ts - cached_ts < core.STATE_TTL_SECONDS:
            catalog_already_sent = True
        elif cached_ts:
            _catalog_sent_cache.pop(cache_key, None)

    cfg = None
    behavior: dict[str, object] = {}
    attachment, caption = None, ""
    try:
        cfg = core.load_tenant(tenant)
        if isinstance(cfg, dict):
            raw_behavior = cfg.get("behavior")
            if isinstance(raw_behavior, dict):
                behavior = raw_behavior
        attachment, caption = _resolve_catalog_attachment(cfg, tenant, req)
    except Exception:
        cfg = None
        behavior = {}
        attachment, caption = None, ""

    if attachment and not catalog_already_sent:
        catalog_text = (caption or "Каталог во вложении (PDF).").strip()
        catalog_out = {
            "lead_id": lead_id,
            "text": catalog_text,
            "provider": provider or "whatsapp",
            "to": phone,
            "tenant_id": tenant,
            "attachment": attachment,
        }
        await _r.lpush("outbox:send", json.dumps(catalog_out, ensure_ascii=False))
        print(f"[outq] ENQ outbox:send lead={lead_id} tenant={tenant} to={phone} attachment={attachment.get('filename')}")
        try:
            core.record_bot_reply(refer_id, tenant, provider, catalog_text, tenant_cfg=cfg)
        except Exception:
            pass
        if state is not None:
            state.catalog_sent = True
            state.catalog_sent_at = time.time()
            state.catalog_delivery_mode = "pdf"
            try:
                core.save_sales_state(state)
            except Exception:
                pass
        if cache_key:
            _catalog_sent_cache[cache_key] = time.time()
        return _ok({"queued": True, "leadId": lead_id})

    # ответ
    try:
        msgs = await build_llm_messages(refer_id, text, provider, tenant=tenant)
        reply = await ask_llm(msgs, tenant=tenant, contact_id=refer_id, channel=provider)
    except Exception:
        reply = "Принял запрос. Скидываю весь каталог. Если нужно PDF — напишите «каталог pdf»."

    out = {
        "lead_id": lead_id,
        "text": reply,
        "provider": provider or "whatsapp",
        "to": phone,
        "tenant_id": tenant,
    }
    await _r.lpush("outbox:send", json.dumps(out, ensure_ascii=False))
    print(f"[outq] ENQ outbox:send lead={lead_id} tenant={tenant} to={phone} len={len(reply)}")

    always_full = bool(behavior.get("always_full_catalog")) if behavior else False
    send_pages_pref = bool(behavior.get("send_catalog_as_pages")) if behavior else False
    should_send_catalog_pages = (always_full or send_pages_pref) and not catalog_already_sent

    if should_send_catalog_pages:
        try:
            items = core.read_all_catalog(cfg)
            pages = core.paginate_catalog_text(items, cfg, int(os.getenv("CATALOG_PAGE_SIZE", "10")))
        except Exception as e:
            print(f"[outq] pages error: {e}")
        else:
            sent_any = False
            for p in pages:
                outp = {
                    "lead_id": lead_id,
                    "text": p,
                    "provider": provider or "whatsapp",
                    "to": phone,
                    "tenant_id": tenant,
                }
                await _r.lpush("outbox:send", json.dumps(outp, ensure_ascii=False))
                print(f"[outq] ENQ outbox:send lead={lead_id} tenant={tenant} to={phone} len={len(p)}")
                sent_any = True
                try:
                    core.record_bot_reply(refer_id, tenant, provider, p, tenant_cfg=cfg)
                except Exception:
                    pass
            if sent_any:
                if state is None:
                    try:
                        state = core.load_sales_state(tenant, refer_id)
                    except Exception:
                        state = None
                if state is not None:
                    state.catalog_sent = True
                    state.catalog_sent_at = time.time()
                    state.catalog_delivery_mode = "pages"
                    try:
                        core.save_sales_state(state)
                    except Exception:
                        pass
                if cache_key:
                    _catalog_sent_cache[cache_key] = time.time()
                catalog_already_sent = True

    return _ok({"queued": True, "leadId": lead_id})

@webhook.post("/webhook")
async def webhook_in(req: Request): return await _handle(req)

@webhook.post("/webhook/provider")
async def webhook_provider(req: Request): return await _handle(req)


@webhook.get("/internal/tenant/{tenant}/catalog-file")
async def internal_catalog_file(tenant: int, path: str, token: str = ""):
    if settings.WEBHOOK_SECRET and token != settings.WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    if not path:
        raise HTTPException(status_code=400, detail="invalid_path")
    try:
        normalized = str(path).replace("\\", "/")
        safe = pathlib.PurePosixPath(normalized)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_path")
    if safe.is_absolute() or ".." in safe.parts:
        raise HTTPException(status_code=400, detail="invalid_path")

    target = core.tenant_dir(tenant) / str(safe)
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="not_found")

    display_name = target.name
    mime, _ = mimetypes.guess_type(str(target))

    try:
        cfg = core.load_tenant(tenant)
        integrations = cfg.get("integrations", {}) if isinstance(cfg, dict) else {}
        uploaded_meta = integrations.get("uploaded_catalog") if isinstance(integrations, dict) else {}
        if isinstance(uploaded_meta, dict):
            meta_path = (uploaded_meta.get("path") or "").replace("\\", "/")
            if meta_path == str(safe):
                display_name = uploaded_meta.get("original") or display_name
                mime = uploaded_meta.get("mime") or mime
    except Exception:
        pass

    return FileResponse(
        target,
        media_type=mime or "application/octet-stream",
        filename=display_name,
    )

# монтирование роутеров
app.include_router(admin_router)
app.include_router(public_router)
app.include_router(client_router)
app.include_router(webhook)

@app.get("/")
def root(): return RedirectResponse(url="/admin")

# Internal: ensure per-tenant files exist (called by waweb)
@app.post("/internal/tenant/{tenant}/ensure")
async def internal_tenant_ensure(tenant: int, request: Request):
    # Authenticate using either header or query token
    token_hdr = (request.headers.get("X-Auth-Token") or "").strip()
    token_qs = (request.query_params.get("token") or "").strip()
    allowed = (settings.WEBHOOK_SECRET or "") or (os.getenv("WA_WEB_TOKEN") or "")
    if allowed and token_hdr != allowed and token_qs != allowed:
        return _err("unauthorized", 401)
    try:
        C.ensure_tenant_files(int(tenant))
        return _ok({"tenant": int(tenant)})
    except Exception:
        return _err("failed")

# Basic health endpoint for Docker healthcheck
@app.get("/health")
async def health():
    return JSONResponse({"ok": True, "status": "healthy"}, status_code=200)

# Mount Ops admin under /admin if available
try:
    OPS_BASE = ROOT.parent / "ops" / "app"
    ops_main_file = OPS_BASE / "main.py"
    if ops_main_file.exists():
        spec = _importlib_util.spec_from_file_location("ops_panel", str(ops_main_file))
        mod = _importlib_util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)
        ops_app = getattr(mod, "app", None)
        if ops_app is not None:
            app.mount("/ops", ops_app)
except Exception as _e:
    # fail-safe: do not block API if ops cannot be mounted
    pass

# Simple request logging middleware. Tests can stub FastAPI with lightweight
# stand-ins, so register the middleware only if the instance exposes the
# decorator method.
async def _log_requests(request: Request, call_next):
    start = time.time()
    response = None
    exc: BaseException | None = None
    try:
        response = await call_next(request)
        return response
    except BaseException as err:
        exc = err
        raise
    finally:
        took = (time.time() - start) * 1000.0
        try:
            if exc is not None:
                _access_logger.error(
                    "%s %s -> 500 %.1fms",
                    request.method,
                    request.url.path,
                    took,
                    exc_info=exc,
                )
            elif response is not None:
                _access_logger.info(
                    "%s %s -> %s %.1fms",
                    request.method,
                    request.url.path,
                    response.status_code,
                    took,
                )
            else:
                _access_logger.info(
                    "%s %s -> %s %.1fms",
                    request.method,
                    request.url.path,
                    0,
                    took,
                )
        except Exception:
            pass


if hasattr(app, "middleware"):
    app.middleware("http")(_log_requests)
