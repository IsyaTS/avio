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
from redis import exceptions as redis_ex

project_root = pathlib.Path(__file__).resolve().parent.parent

# Ensure JavaScript assets are served with the correct MIME type even if the
# underlying system defaults to ``text/plain``.
mimetypes.add_type("application/javascript", ".js")
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

from app.internal.tenant import router as internal_tenant_router

import importlib.util as _importlib_util

from app import outbox_worker

OUTBOX_DB_WORKER_ENABLED = (os.getenv("OUTBOX_DB_WORKER") or "0").strip().lower() in {
    "1",
    "true",
    "yes",
}

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
from app.metrics import (
    MESSAGE_OUT_COUNTER,
    SEND_FAIL_COUNTER,
    WA_QR_CALLBACK_ERRORS_COUNTER,
    WA_QR_RECEIVED_COUNTER,
)
from app.transport import WhatsAppAddressError, normalize_whatsapp_recipient
from app.common import get_outbox_whitelist, whitelist_contains_number


_FALSE_OUTBOX_VALUES = {"0", "false", "no", "off", "disabled"}


def _outbox_enabled() -> bool:
    raw = (os.getenv("OUTBOX_ENABLED") or "").strip().lower()
    return raw not in _FALSE_OUTBOX_VALUES


from app.starlette_ext import register_transport_validation


_init_logging()

# Module-level logger for request access
_access_logger = logging.getLogger("app.access")
transport_logger = logging.getLogger("app.transport")
wa_logger = logging.getLogger("wa")

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


@app.get("/")
async def root_ping() -> dict[str, bool]:
    return {"ok": True}


@app.head("/")
async def root_head() -> Response:
    return Response(status_code=200)
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
    except Exception as exc:  # pragma: no cover - startup logging
        asyncpg_module = getattr(module, "asyncpg", None)
        undefined_table_error = getattr(asyncpg_module, "UndefinedTableError", None)
        if undefined_table_error and isinstance(exc, undefined_table_error):
            logger.info(
                "alembic_revision=unavailable (alembic_version table missing)"
            )
            return
        logger.exception("failed to query Alembic revision")
        return
    if revision:
        logger.info("alembic_revision=%s", revision)
    else:
        logger.warning("alembic_revision=unavailable")


@app.on_event("startup")
async def _startup_run_provider_token_migration() -> None:
    module = globals().get("db_module")
    runner = getattr(module, "ensure_provider_tokens_schema", None)
    if runner is None:
        logging.getLogger("app.migrations").info(
            "provider_tokens_migration_skip reason=no_db_module",
        )
        return
    try:
        await runner()  # type: ignore[misc]
    except Exception:
        logging.getLogger("app.migrations").exception(
            "provider_tokens_migration_failed",
        )
        raise


@app.on_event("startup")
async def _startup_log_revision() -> None:
    await _log_alembic_revision_on_startup()


@app.on_event("startup")
async def _startup_outbox_worker() -> None:
    if not OUTBOX_DB_WORKER_ENABLED:
        logging.getLogger("app.outbox_worker").info(
            "event=outbox_worker_disabled"
        )
        return
    try:
        await outbox_worker.start()
    except Exception:
        logging.getLogger("app.outbox_worker").exception(
            "event=outbox_worker_start_failed"
        )


@app.on_event("shutdown")
async def _shutdown_outbox_worker() -> None:
    if not OUTBOX_DB_WORKER_ENABLED:
        return
    try:
        await outbox_worker.stop()
    except Exception:
        logging.getLogger("app.outbox_worker").exception(
            "event=outbox_worker_stop_failed"
        )


@app.get("/metrics")
async def metrics_endpoint() -> Response:
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.post("/send")
async def send_transport_message(request: Request, message: TransportMessage) -> Response:
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
    channel = message.channel
    raw_to_value = payload.get("to")
    normalized_to = raw_to_value
    whitelist_number: str | None = None
    normalized_e164: str | None = None

    if channel == "whatsapp":
        try:
            digits, jid = normalize_whatsapp_recipient(raw_to_value)
        except WhatsAppAddressError as exc:
            reason = str(exc) or "invalid"
            explanations = {
                "empty": "empty",
                "invalid_length": "expected 10-15 digits",
                "invalid_domain": "expected @c.us jid",
            }
            message_text = explanations.get(reason, reason)
            status_label = "invalid_to"
            MESSAGE_OUT_COUNTER.labels(channel, status_label).inc()
            transport_logger.warning(
                "event=message_out channel=%s tenant=%s to=%s status=%s reason=%s",
                channel,
                message.tenant,
                payload.get("to") or "-",
                status_label,
                message_text,
            )
            return JSONResponse(
                {"error": f"invalid_to: {message_text}"},
                status_code=400,
            )
        payload["to"] = jid
        normalized_to = jid
        whitelist_number = digits
        normalized_e164 = f"+{digits}"

    if not _outbox_enabled():
        status_label = "outbox_disabled"
        MESSAGE_OUT_COUNTER.labels(channel, status_label).inc()
        transport_logger.warning(
            "event=message_out channel=%s tenant=%s to=%s status=%s",
            channel,
            message.tenant,
            normalized_to or payload.get("to") or "-",
            status_label,
        )
        return JSONResponse({"error": "outbox_disabled"}, status_code=403)

    if channel == "whatsapp" and whitelist_number is not None:
        whitelist = get_outbox_whitelist()
        if not whitelist.allow_all and not whitelist_contains_number(
            whitelist, whitelist_number
        ):
            status_label = "not_whitelisted"
            MESSAGE_OUT_COUNTER.labels(channel, status_label).inc()
            transport_logger.warning(
                "event=message_out channel=%s tenant=%s to=%s status=%s normalized_to=%s raw_to=%s "
                "whitelist=%s reason=%s",
                channel,
                message.tenant,
                normalized_to or "-",
                status_label,
                normalized_e164 or (whitelist_number and f"+{whitelist_number}") or "-",
                raw_to_value or "-",
                whitelist.raw_value,
                "not_found",
            )
            return JSONResponse({"error": "not_whitelisted"}, status_code=403)

    try:
        client = _transport_client(channel)
        response = await client.post(
            endpoint,
            json=payload,
            timeout=httpx.Timeout(12.0),
        )
    except httpx.HTTPError as exc:
        status_label = "http_error"
        SEND_FAIL_COUNTER.labels(channel, status_label).inc()
        MESSAGE_OUT_COUNTER.labels(channel, status_label).inc()
        transport_logger.error(
            "event=message_out channel=%s tenant=%s to=%s status=%s error=%s",
            channel,
            message.tenant,
            normalized_to or payload.get("to") or "-",
            status_label,
            exc,
        )
        raise HTTPException(status_code=502, detail="worker_unreachable") from exc

    if (
        response.status_code == 409
        and response.headers.get("X-Reauth", "").strip() == "1"
    ):
        status_label = "reauth"
        MESSAGE_OUT_COUNTER.labels(channel, status_label).inc()
        transport_logger.warning(
            "event=message_out channel=%s tenant=%s to=%s status=%s",
            channel,
            message.tenant,
            normalized_to or "-",
            status_label,
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
        status_label = "remote_error"
        reason = f"status_{response.status_code}"
        SEND_FAIL_COUNTER.labels(channel, reason).inc()
        MESSAGE_OUT_COUNTER.labels(channel, status_label).inc()
        transport_logger.warning(
            "event=message_out channel=%s tenant=%s to=%s status=%s http_status=%s",
            channel,
            message.tenant,
            normalized_to or "-",
            status_label,
            response.status_code,
        )
        media_type = response.headers.get("Content-Type") or "application/json"
        return Response(
            content=response.content,
            status_code=response.status_code,
            media_type=media_type,
        )

    status_label = "success"
    MESSAGE_OUT_COUNTER.labels(channel, status_label).inc()
    transport_logger.info(
        "event=message_out channel=%s tenant=%s to=%s status=%s",
        channel,
        message.tenant,
        normalized_to or "-",
        status_label,
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

@webhook.api_route("/internal/tenant/{tenant}/catalog-file", methods=["GET", "HEAD"])
async def internal_catalog_file(
    tenant: int, path: str, request: Request, token: str = ""
):
    if not C.is_internal_request_authorized(request, token=token):
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

    response = FileResponse(
        target,
        media_type=mime or "application/octet-stream",
        filename=display_name,
    )

    if request.method.upper() == "HEAD":
        response.body_iterator = iter(())

    return response

# монтирование роутеров
app.include_router(admin_router)
app.include_router(public_router)
app.include_router(client_router)
app.include_router(internal_tenant_router)
app.include_router(webhook)
app.include_router(webhooks_router)

@app.get("/")
def root(): return RedirectResponse(url="/admin")

@app.post("/internal/tenant/{tenant}/wa/qr")
async def internal_tenant_wa_qr(tenant: int, request: Request):
    admin_token = (request.headers.get("X-Admin-Token") or "").strip()
    if not admin_token or admin_token != (settings.ADMIN_TOKEN or ""):
        return _err("unauthorized", 401)

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    tenant_value = payload.get("tenant", tenant)
    try:
        tenant_id = int(tenant_value)
    except (TypeError, ValueError):
        WA_QR_CALLBACK_ERRORS_COUNTER.labels(reason="invalid_tenant").inc()
        wa_logger.warning("wa_qr_callback_invalid tenant=%s reason=invalid_tenant", tenant_value)
        return _err("invalid_tenant", 400)

    raw_svg = payload.get("qr_svg")
    raw_png = payload.get("qr_png")
    ts_raw = payload.get("ts")
    ts_value = str(ts_raw or int(time.time()))

    svg_value = raw_svg if isinstance(raw_svg, str) and raw_svg.strip() else ""
    png_value = raw_png if isinstance(raw_png, str) and raw_png.strip() else ""

    if not svg_value and not png_value:
        WA_QR_CALLBACK_ERRORS_COUNTER.labels(reason="empty_payload").inc()
        wa_logger.warning("wa_qr_callback_invalid tenant=%s reason=empty_payload", tenant_id)
        return _err("invalid_payload", 400)

    entry = {"tenant": tenant_id, "ts": ts_value}
    if svg_value:
        entry["qr_svg"] = svg_value
    if png_value:
        entry["qr_png"] = png_value

    cache_key = f"wa:qr:{tenant_id}:{ts_value}"
    last_key = f"wa:qr:last:{tenant_id}"

    try:
        client = C.redis_client()
        client.setex(cache_key, 120, json.dumps(entry, ensure_ascii=False))
        client.set(last_key, ts_value)
    except redis_ex.RedisError as exc:
        WA_QR_CALLBACK_ERRORS_COUNTER.labels(reason="redis").inc()
        wa_logger.warning(
            "wa_qr_callback_redis_error tenant=%s ts=%s detail=%s",
            tenant_id,
            ts_value,
            exc,
        )
        return _err("redis_error", 500)
    except Exception:
        WA_QR_CALLBACK_ERRORS_COUNTER.labels(reason="unexpected").inc()
        wa_logger.exception("wa_qr_callback_exception tenant=%s ts=%s", tenant_id, ts_value)
        return _err("internal_error", 500)

    WA_QR_RECEIVED_COUNTER.labels(tenant=str(tenant_id)).inc()
    wa_logger.info("saved_wa_qr tenant=%s ts=%s", tenant_id, ts_value)
    return _ok({"tenant": tenant_id, "ts": ts_value})

# Basic health endpoint for Docker healthcheck
@app.get("/health")
async def health():
    return JSONResponse(
        {"ok": True, "status": "healthy", "version": C.asset_version()},
        status_code=200,
    )

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
