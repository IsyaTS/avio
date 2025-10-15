from __future__ import annotations

import pathlib
import os, json, re, time, mimetypes
from urllib.parse import quote

import importlib
import importlib.machinery
import importlib.util
import sys
from types import ModuleType

from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from fastapi.staticfiles import StaticFiles
import logging
from logging import StreamHandler
import httpx

project_root = pathlib.Path(__file__).resolve().parent.parent
if __package__ in (None, ""):
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

core = importlib.import_module("app.core")
sys.modules.setdefault("core", core)
_EXPECTED_WEB_ATTRS: dict[str, tuple[str, ...]] = {
    "common": ("router",),
    "admin": ("router",),
    "public": ("router", "templates"),
    "client": ("router",),
    "webhooks": ("router", "process_incoming", "_resolve_catalog_attachment"),
}


def _load_web_module_from_source(module_name: str, full_name: str) -> ModuleType:
    module_path = project_root / "app" / "web" / f"{module_name}.py"
    loader = importlib.machinery.SourceFileLoader(full_name, str(module_path))
    spec = importlib.util.spec_from_loader(full_name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    sys.modules[full_name] = module
    parent_pkg = sys.modules.get("app.web")
    if parent_pkg is not None:
        setattr(parent_pkg, module_name, module)
    return module


def _import_web_module(module_name: str) -> ModuleType:
    full_name = f"app.web.{module_name}"
    module = importlib.import_module(full_name)
    expected = _EXPECTED_WEB_ATTRS.get(module_name, ())
    if expected and not all(hasattr(module, attr) for attr in expected):
        if getattr(module, "__avio_fallback_failed__", False):
            return module
        try:
            module = _load_web_module_from_source(module_name, full_name)
        except Exception:
            logging.getLogger(__name__).warning(
                "fallback_import_failed module=%s", full_name, exc_info=True
            )
            setattr(module, "__avio_fallback_failed__", True)
            if module_name == "public" and not hasattr(module, "templates"):
                module.templates = object()  # type: ignore[attr-defined]
            return module
    return module


_common_mod = _import_web_module("common")
_admin_mod = _import_web_module("admin")
_public_mod = _import_web_module("public")
_client_mod = _import_web_module("client")
_webhooks_mod = _import_web_module("webhooks")
_catalog_sent_cache = getattr(_webhooks_mod, "_catalog_sent_cache", {})
_r = getattr(_webhooks_mod, "_redis_queue", None)

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
    from . import db as db_module  # type: ignore
    resolve_or_create_contact = db_module.resolve_or_create_contact
    link_lead_contact = db_module.link_lead_contact
    insert_message_in = db_module.insert_message_in
    upsert_lead = db_module.upsert_lead
except ImportError:  # pragma: no cover - фоллбек для окружений без БД
    db_module = None  # type: ignore[assignment]

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


from config import CHANNEL_ENDPOINTS
from app.schemas import TransportMessage
from app.lib.transport_utils import transport_message_asdict
from app.metrics import MESSAGE_OUT_COUNTER, SEND_FAIL_COUNTER
from app.starlette_ext import register_transport_validation


_init_logging()

# Module-level logger for request access
_access_logger = logging.getLogger("app.access")
transport_logger = logging.getLogger("app.transport")

_transport_clients: dict[str, httpx.AsyncClient] = {}


def _transport_client(channel: str) -> httpx.AsyncClient:
    key = (channel or "").lower()
    client = _transport_clients.get(key)
    admin_token = getattr(settings, "ADMIN_TOKEN", "") or ""
    if client is None or client.is_closed:
        headers: dict[str, str] = {}
        if key == "telegram":
            headers["X-Admin-Token"] = admin_token
        client = httpx.AsyncClient(timeout=httpx.Timeout(12.0), headers=headers)
        _transport_clients[key] = client
    elif key == "telegram":
        client.headers.update({"X-Admin-Token": admin_token})
    return client

app = FastAPI(title="avio-api")
static_dir = ROOT / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
webhook = APIRouter()

register_transport_validation(app)


async def _log_alembic_revision_on_startup() -> None:
    logger = logging.getLogger("app.alembic")
    module = globals().get("db_module")
    revision_getter = getattr(module, "current_alembic_revision", None)
    if revision_getter is None:
        logger.info("alembic_revision=unavailable (db module missing)")
        return
    try:
        revision = await revision_getter()  # type: ignore[misc]
    except Exception:
        logger.exception("failed to query Alembic revision")
        return
    if revision:
        logger.info("alembic_revision=%s", revision)
    else:
        logger.warning("alembic_revision=unavailable")


@app.on_event("startup")
async def _startup_log_revision() -> None:
    await _log_alembic_revision_on_startup()


@app.get("/metrics")
async def metrics_endpoint() -> Response:
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.post("/send")
async def send_transport_message(request: Request, message: TransportMessage) -> JSONResponse:
    admin_token = getattr(settings, "ADMIN_TOKEN", "") or ""
    header_token = (request.headers.get("X-Admin-Token") or "").strip()
    if admin_token and header_token != admin_token:
        raise HTTPException(status_code=401, detail="unauthorized")

    if not message.has_content:
        raise HTTPException(status_code=400, detail="empty_message")

    endpoint = CHANNEL_ENDPOINTS.get(message.channel)
    if not endpoint:
        raise HTTPException(status_code=400, detail="channel_unknown")

    payload = transport_message_asdict(message)
    transport_logger.info(
        "event=message_out stage=dispatch_request channel=%s tenant=%s endpoint=%s",
        message.channel,
        message.tenant,
        endpoint,
    )
    peer_value = payload.get("to")
    try:
        client = _transport_client(message.channel)
        response = await client.post(
            endpoint,
            json=payload,
            timeout=httpx.Timeout(12.0),
        )
    except httpx.HTTPError as exc:
        SEND_FAIL_COUNTER.labels(message.channel, "http_error").inc()
        transport_logger.error(
            "event=message_out stage=dispatch_error channel=%s tenant=%s error=%s",
            message.channel,
            message.tenant,
            exc,
        )
        raise HTTPException(status_code=502, detail="worker_unreachable") from exc

    transport_logger.info(
        "event=message_out stage=dispatch_response channel=%s tenant=%s status=%s peer=%s",
        message.channel,
        message.tenant,
        response.status_code,
        peer_value or "-",
    )

    if (
        response.status_code == 409
        and response.headers.get("X-Reauth", "").strip() == "1"
    ):
        transport_logger.warning(
            "event=message_out stage=dispatch_reauth channel=%s tenant=%s", 
            message.channel,
            message.tenant,
        )
        reauth_headers = {
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Reauth": "1",
        }
        return JSONResponse(
            {"ok": False, "state": "need_qr", "error": "relogin_required"},
            status_code=409,
            headers=reauth_headers,
        )

    if not (200 <= response.status_code < 300):
        reason = f"status_{response.status_code}"
        SEND_FAIL_COUNTER.labels(message.channel, reason).inc()
        transport_logger.warning(
            "event=message_out stage=dispatch_fail channel=%s tenant=%s status=%s",
            message.channel,
            message.tenant,
            response.status_code,
        )
        detail = response.text
        raise HTTPException(status_code=response.status_code, detail=detail or "worker_error")

    MESSAGE_OUT_COUNTER.labels(message.channel).inc()
    transport_logger.info(
        "event=message_out stage=dispatch_ok channel=%s tenant=%s",
        message.channel,
        message.tenant,
    )
    try:
        body = response.json()
    except Exception:
        body = {"ok": True}
    return JSONResponse(body, status_code=response.status_code)


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def _ok(data: dict | None = None, status: int = 200):
    return JSONResponse({"ok": True, **(data or {})}, status_code=status)

def _err(msg: str, status: int = 400):
    return JSONResponse({"ok": False, "error": msg}, status_code=status)


def _resolve_catalog_attachment(cfg, tenant, request=None):
    return _webhooks_mod._resolve_catalog_attachment(cfg, tenant, request)


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

    if hasattr(_webhooks_mod, "_redis_queue"):
        setattr(_webhooks_mod, "_redis_queue", _r)
    for attr in ("ask_llm", "build_llm_messages", "settings"):
        if hasattr(_webhooks_mod, attr) and attr in globals():
            setattr(_webhooks_mod, attr, globals()[attr])

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
