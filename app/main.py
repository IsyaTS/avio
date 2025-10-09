from __future__ import annotations

import pathlib
import os, json, re, time, mimetypes
from urllib.parse import quote

import importlib
import sys

from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
import logging
from logging import StreamHandler

if __package__ in (None, ""):
    project_root = pathlib.Path(__file__).resolve().parent.parent
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

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
_webhooks_mod = _import_with_alias("web.webhooks")

ask_llm = core.ask_llm  # type: ignore[attr-defined]
build_llm_messages = core.build_llm_messages  # type: ignore[attr-defined]
settings = core.settings  # type: ignore[attr-defined]

C = _common_mod  # type: ignore[assignment]
admin_router = _admin_mod.router  # type: ignore[attr-defined]
public_router = _public_mod.router  # type: ignore[attr-defined]
client_router = _client_mod.router  # type: ignore[attr-defined]
webhooks_router = _webhooks_mod.router  # type: ignore[attr-defined]
process_incoming = _webhooks_mod.process_incoming  # type: ignore[attr-defined]

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


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def _ok(data: dict | None = None, status: int = 200):
    return JSONResponse({"ok": True, **(data or {})}, status_code=status)

def _err(msg: str, status: int = 400):
    return JSONResponse({"ok": False, "error": msg}, status_code=status)


@app.get("/health")
def healthcheck():
    """Lightweight container health endpoint."""
    return JSONResponse({"ok": True})

async def _handle(request: Request):
    query_token = (request.query_params.get("token") or "").strip()
    headers = getattr(request, "headers", {}) or {}
    header_token = headers.get("X-Webhook-Token") or ""
    auth_header = headers.get("Authorization") or ""
    if auth_header and auth_header.lower().startswith("bearer "):
        auth_token = auth_header[7:]
    else:
        auth_token = auth_header
    header_token = (header_token or auth_token).strip()
    token = query_token or header_token

    if not token:
        secret = settings.WEBHOOK_SECRET
        if secret:
            return _err("unauthorized", 401)
        return Response(status_code=204)

    secret = settings.WEBHOOK_SECRET
    if secret and token != secret:
        return _err("unauthorized", 401)

    try:
        raw_body = await request.body()
    except Exception:
        raw_body = b""

    if raw_body:
        try:
            decoded = raw_body.decode("utf-8")
        except UnicodeDecodeError:
            return _err("invalid_json", 400)
        try:
            body = json.loads(decoded)
        except json.JSONDecodeError:
            return _err("invalid_json", 400)
        except Exception:
            return _err("invalid_payload", 400)
        if not isinstance(body, dict):
            body = {}
    else:
        body = {}

    return await process_incoming(body, request)

@webhook.post("/webhook")
async def webhook_in(request: Request):
    return await _handle(request)

@webhook.post("/webhook/provider")
async def webhook_provider(request: Request):
    return await _handle(request)


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
app.include_router(webhooks_router)

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
    try:
        response = await call_next(request)
    except BaseException:
        took = (time.time() - start) * 1000.0
        _access_logger.exception(
            "%s %s -> 500 %.1fms",
            request.method,
            request.url.path,
            took,
        )
        return JSONResponse({"detail": "internal_error"}, status_code=500)

    took = (time.time() - start) * 1000.0
    try:
        _access_logger.info(
            "%s %s -> %s %.1fms",
            request.method,
            request.url.path,
            response.status_code,
            took,
        )
    except Exception:
        pass
    return response


if hasattr(app, "middleware"):
    app.middleware("http")(_log_requests)
