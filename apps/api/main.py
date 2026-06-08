from __future__ import annotations

import pathlib
import os
import json
import re
import time
import mimetypes
from urllib.parse import parse_qsl, urlencode, urlparse
from typing import Any

import importlib
import importlib.machinery
import importlib.util
import sys
from contextlib import asynccontextmanager
from types import ModuleType

from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, FileResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from fastapi.staticfiles import StaticFiles
import logging
from logging import StreamHandler
import httpx
from redis import exceptions as redis_ex

from libs.core import sales_core as core

IS_TESTING = (os.getenv("TESTING") or "").strip() == "1"

project_root = pathlib.Path(__file__).resolve().parent.parent

# Ensure JavaScript assets are served with the correct MIME type even if the
# underlying system defaults to ``text/plain``.
mimetypes.add_type("application/javascript", ".js")
if __package__ in (None, ""):
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

sys.modules.setdefault("core", core)
_EXPECTED_WEB_ATTRS: dict[str, tuple[str, ...]] = {
    "common": ("router",),
    "admin": ("router",),
    "auth": ("router",),
    "public": ("router", "templates"),
    "analytics_avito": ("router",),
    "client": ("router",),
    "webhooks": ("router", "process_incoming"),
}

try:
    from libs.core.services import catalog_flow as catalog_flow_service
except ImportError:
    catalog_flow_service = None  # type: ignore

from libs.core.services import ops_health


def _load_web_module_from_source(module_name: str, full_name: str) -> ModuleType:
    module_path = pathlib.Path(__file__).resolve().parent / "web" / f"{module_name}.py"
    loader = importlib.machinery.SourceFileLoader(full_name, str(module_path))
    spec = importlib.util.spec_from_loader(full_name, loader)
    existing = sys.modules.get(full_name)
    if existing is not None:
        module = existing
        module.__loader__ = loader
        module.__spec__ = spec
        module.__file__ = str(module_path)
        loader.exec_module(module)
    else:
        module = importlib.util.module_from_spec(spec)
        sys.modules[full_name] = module
        loader.exec_module(module)
    parent_pkg = sys.modules.get("apps.api.web")
    if parent_pkg is not None:
        setattr(parent_pkg, module_name, module)
    return module


def _import_web_module(module_name: str) -> ModuleType:
    full_name = f"apps.api.web.{module_name}"
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
_auth_mod = _import_web_module("auth")
_public_mod = _import_web_module("public")
_analytics_avito_mod = _import_web_module("analytics_avito")
_client_mod = _import_web_module("client")
_webhooks_mod = _import_web_module("webhooks")
if catalog_flow_service is not None:
    _catalog_sent_cache = catalog_flow_service._catalog_sent_cache
else:
    _catalog_sent_cache = getattr(_webhooks_mod, "_catalog_sent_cache", {})
_r = getattr(_webhooks_mod, "_redis_queue", None)

ask_llm = core.ask_llm  # type: ignore[attr-defined]
build_llm_messages = core.build_llm_messages  # type: ignore[attr-defined]
settings = core.settings  # type: ignore[attr-defined]
tenant_whatsapp_provider = getattr(core, "tenant_whatsapp_provider", lambda tenant: "waweb")

C = _common_mod  # type: ignore[assignment]
admin_router = _admin_mod.router  # type: ignore[attr-defined]
auth_router = _auth_mod.router  # type: ignore[attr-defined]
public_router = _public_mod.router  # type: ignore[attr-defined]
analytics_avito_router = _analytics_avito_mod.router  # type: ignore[attr-defined]
client_router = _client_mod.router  # type: ignore[attr-defined]
webhooks_router = _webhooks_mod.router  # type: ignore[attr-defined]
process_incoming = _webhooks_mod.process_incoming  # type: ignore[attr-defined]

from libs.core.internal.tenant import router as internal_tenant_router
from libs.core.repo import tenant_configs


from apps.worker import outbox as outbox_worker

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
    for logger_name in ("httpx", "httpcore"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

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


from libs.config import CHANNEL_ENDPOINTS
from libs.core.schemas import TransportMessage
from libs.core.lib.transport_utils import transport_message_asdict
from libs.core.metrics import (
    MESSAGE_OUT_COUNTER,
    SEND_FAIL_COUNTER,
    WA_QR_CALLBACK_ERRORS_COUNTER,
    WA_QR_RECEIVED_COUNTER,
)
from libs.core.transport import WhatsAppAddressError, normalize_whatsapp_recipient
from libs.core.common import (
    OUTBOX_QUEUE_KEY,
    HANDOFF_SILENCE_TTL_SECONDS,
    get_outbox_whitelist,
    handoff_silence_key,
    handoff_silence_meta_key,
    whitelist_contains_number,
)
from apps.api.web.services.send_transport_runtime import (
    SendTransportDeps,
    handle_send_transport_message,
)

_FALSE_OUTBOX_VALUES = {"0", "false", "no", "off", "disabled"}


def _outbox_enabled() -> bool:
    raw = (os.getenv("OUTBOX_ENABLED") or "").strip().lower()
    return raw not in _FALSE_OUTBOX_VALUES


async def _mark_handoff_silence(
    *,
    tenant: int,
    lead_id: int | None,
    manager_flag: bool,
) -> None:
    if not manager_flag or not lead_id or lead_id <= 0:
        return
    redis_client = _r or getattr(settings, "r", None)
    if redis_client is None:
        return
    try:
        timestamp = int(time.time())
        await redis_client.set(
            handoff_silence_key(int(tenant), int(lead_id)),
            str(timestamp),
            ex=HANDOFF_SILENCE_TTL_SECONDS,
        )
        meta_key = handoff_silence_meta_key(int(tenant), int(lead_id))
        if meta_key:
            payload = {"reason": "manager_outgoing", "ts": timestamp}
            await redis_client.set(
                meta_key,
                json.dumps(payload, ensure_ascii=False),
                ex=HANDOFF_SILENCE_TTL_SECONDS,
            )
    except Exception:
        transport_logger.debug(
            "handoff_flag_set_failed tenant=%s lead_id=%s", tenant, lead_id, exc_info=True
        )


SEND_STRATEGY = (os.getenv("SEND_STRATEGY") or "").strip().lower()


from .starlette_ext import register_transport_validation


_init_logging()

# Module-level logger for request access
_access_logger = logging.getLogger("app.access")
transport_logger = logging.getLogger("app.transport")
wa_logger = logging.getLogger("wa")

_transport_clients: dict[str, httpx.AsyncClient] = {}


def _admin_token() -> str:
    return (getattr(settings, "ADMIN_TOKEN", "") or "").strip()


def _waweb_base_url(tenant: int) -> str:
    try:
        base = str(C.wa_base_url(int(tenant)))
    except Exception:
        base = ""
    if base:
        return base.rstrip("/")
    endpoint = CHANNEL_ENDPOINTS.get("whatsapp") or ""
    if endpoint.endswith("/send"):
        endpoint = endpoint[: -len("/send")]
    return (endpoint or "http://waweb:9001").rstrip("/")


def _waweb_send_url(tenant: int) -> str:
    base = _waweb_base_url(int(tenant)).rstrip("/")
    query = urlencode({"tenant": int(tenant)})
    return f"{base}/send?{query}"


def _wabaileys_base_url() -> str:
    base = (
        getattr(settings, "BAILEYS_URL", "") or os.getenv("BAILEYS_URL") or "http://wabaileys:9002"
    )
    return str(base).rstrip("/")


def _wabaileys_send_url() -> str:
    return f"{_wabaileys_base_url()}/messages/send"


def _whatsapp_send_url(provider: str, tenant: int) -> str:
    if (provider or "").strip().lower() == "baileys":
        return _wabaileys_send_url()
    return _waweb_send_url(tenant)


def _normalize_internal_attachment_url(raw_url: str) -> str:
    url_value = str(raw_url or "").strip()
    if not url_value:
        return url_value

    internal_base = (
        settings.APP_INTERNAL_URL or os.getenv("APP_INTERNAL_URL") or "http://app:8000"
    ).rstrip("/")
    base_netloc = urlparse(internal_base).netloc
    token = (getattr(C, "WA_INTERNAL_TOKEN", "") or "").strip()

    path = ""
    query = ""

    if url_value.startswith("/internal/"):
        parsed = urlparse(url_value)
        path = parsed.path
        query = parsed.query
    else:
        parsed = urlparse(url_value)
        if not (parsed.scheme and parsed.netloc and parsed.path.startswith("/internal/")):
            return url_value
        if parsed.netloc not in {base_netloc, "app:8000"}:
            return url_value
        path = parsed.path
        query = parsed.query

    base_url = f"{internal_base}{path}"
    if token:
        query_pairs = [
            (key, value)
            for key, value in parse_qsl(query, keep_blank_values=True)
            if key.lower() != "token"
        ]
        query_pairs.append(("token", token))
        query = urlencode(query_pairs)
    return f"{base_url}?{query}" if query else base_url


def _prepare_whatsapp_attachment(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        return {}
    prepared = {key: value for key, value in item.items() if value is not None}

    url_value = prepared.get("url")
    if isinstance(url_value, str):
        prepared["url"] = _normalize_internal_attachment_url(url_value)

    name = prepared.get("name") or prepared.get("filename") or prepared.get("title")
    if name is not None:
        prepared["name"] = str(name)
    prepared.pop("filename", None)

    mime = (
        prepared.get("mime")
        or prepared.get("mime_type")
        or prepared.get("mimetype")
        or prepared.get("content_type")
    )
    if mime is not None:
        prepared["mime"] = str(mime)
    prepared.pop("mime_type", None)
    prepared.pop("mimetype", None)
    prepared.pop("content_type", None)

    caption = prepared.get("caption") or prepared.get("description")
    if caption is not None:
        prepared["caption"] = str(caption)

    for nested_key in ("document", "image", "video", "audio", "voice", "thumbnail"):
        nested_value = prepared.get(nested_key)
        if isinstance(nested_value, dict):
            nested_prepared = {k: v for k, v in nested_value.items() if v is not None}
            nested_url = nested_prepared.get("url")
            if isinstance(nested_url, str):
                nested_prepared["url"] = _normalize_internal_attachment_url(nested_url)
            prepared[nested_key] = nested_prepared

    return prepared


def _prepare_whatsapp_payload(payload: dict[str, Any], tenant: int) -> dict[str, Any]:
    cleaned = dict(payload)
    for key in ("tenant", "tenant_id", "tenantId"):
        cleaned.pop(key, None)

    attachments = cleaned.get("attachments")
    if isinstance(attachments, list):
        normalized: list[dict[str, Any]] = []
        for item in attachments:
            prepared = _prepare_whatsapp_attachment(item)
            if prepared:
                normalized.append(prepared)
        cleaned["attachments"] = normalized

    attachment_single = cleaned.get("attachment")
    if isinstance(attachment_single, dict):
        cleaned["attachment"] = _prepare_whatsapp_attachment(attachment_single)

    return cleaned


def _transport_client(channel: str, provider: str | None = None) -> httpx.AsyncClient:
    key = (channel or "").lower()
    client_key = key if not (key == "whatsapp" and provider) else f"{key}:{provider}"
    client = _transport_clients.get(client_key)
    admin_token = _admin_token()
    wa_header_value = admin_token or ""
    if client is None or client.is_closed:
        headers: dict[str, str] = {}
        timeout = httpx.Timeout(20.0)
        if key == "telegram" and admin_token:
            headers["X-Admin-Token"] = admin_token
        if key == "whatsapp":
            provider_key = (provider or "waweb").strip().lower()
            if provider_key != "baileys":
                headers["X-Auth-Token"] = wa_header_value
                # Large WhatsApp documents (e.g. catalog PDFs) may require extra time for waweb
                timeout = httpx.Timeout(300.0)
            else:
                timeout = httpx.Timeout(60.0)
        client = httpx.AsyncClient(timeout=timeout, headers=headers)
        _transport_clients[client_key] = client
    elif key == "telegram":
        if admin_token:
            client.headers.update({"X-Admin-Token": admin_token})
        else:
            client.headers.pop("X-Admin-Token", None)
    elif key == "whatsapp":
        provider_key = (provider or "waweb").strip().lower()
        if provider_key != "baileys":
            if wa_header_value:
                client.headers.update({"X-Auth-Token": wa_header_value})
            else:
                client.headers.pop("X-Auth-Token", None)
        else:
            client.headers.pop("X-Auth-Token", None)
    return client


_WORKER_HEALTH_URL = "http://worker:8000/health"
_WORKER_HEALTH_TIMEOUT = httpx.Timeout(0.75)
_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if not value:
        return default
    return value not in _FALSE_VALUES


async def _ensure_worker_healthy() -> None:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                _WORKER_HEALTH_URL,
                timeout=_WORKER_HEALTH_TIMEOUT,
            )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="worker_unreachable") from exc
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail="worker_unreachable")


_docs_enabled = _env_flag("ENABLE_API_DOCS", default=True)

@asynccontextmanager
async def _app_lifespan(_app: FastAPI):  # pragma: no cover - lifecycle wiring
    await _startup_run_provider_token_migration()
    await _startup_run_auth_migration()
    await _startup_run_tenant_config_migration()
    await _startup_log_revision()
    await _client_mod.client_avito_history_export_runtime.startup_resume_exports(runtime_module=_client_mod.client_avito_history_export_runtime, common_module=C, logger=logging.getLogger("app.avito_history_exports"), enabled=not IS_TESTING)
    await _startup_outbox_worker()
    try:
        yield
    finally:
        await _shutdown_outbox_worker()


app = FastAPI(
    title="avio-api",
    docs_url="/docs" if _docs_enabled else None,
    redoc_url="/redoc" if _docs_enabled else None,
    openapi_url="/openapi.json" if _docs_enabled else None,
    lifespan=_app_lifespan,
)


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
            logger.info("alembic_revision=unavailable (alembic_version table missing)")
            return
        logger.exception("failed to query Alembic revision")
        return
    if revision:
        logger.info("alembic_revision=%s", revision)
    else:
        logger.warning("alembic_revision=unavailable")


async def _startup_run_provider_token_migration() -> None:
    if IS_TESTING:
        return
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


async def _startup_run_auth_migration() -> None:
    if IS_TESTING:
        return
    module = globals().get("db_module")
    runner = getattr(module, "ensure_auth_schema", None)
    if runner is None:
        logging.getLogger("app.migrations").info(
            "auth_migration_skip reason=no_db_module",
        )
        return
    try:
        await runner()  # type: ignore[misc]
    except Exception:
        logging.getLogger("app.migrations").exception(
            "auth_migration_failed",
        )
        raise


async def _startup_run_tenant_config_migration() -> None:
    if IS_TESTING:
        return
    try:
        tenant_configs.ensure_schema()
    except Exception:
        logging.getLogger("app.migrations").exception(
            "tenant_config_migration_failed",
        )


async def _startup_log_revision() -> None:
    if IS_TESTING:
        return
    await _log_alembic_revision_on_startup()


async def _startup_outbox_worker() -> None:
    if IS_TESTING:
        return
    if not OUTBOX_DB_WORKER_ENABLED:
        logging.getLogger("app.outbox_worker").info("event=outbox_worker_disabled")
        return
    try:
        await outbox_worker.start()
    except Exception:
        logging.getLogger("app.outbox_worker").exception("event=outbox_worker_start_failed")


async def _shutdown_outbox_worker() -> None:
    if not OUTBOX_DB_WORKER_ENABLED:
        return
    try:
        await outbox_worker.stop()
    except Exception:
        logging.getLogger("app.outbox_worker").exception("event=outbox_worker_stop_failed")


def _send_transport_deps() -> SendTransportDeps:
    return SendTransportDeps(
        admin_token_fn=_admin_token,
        channel_endpoints=CHANNEL_ENDPOINTS,
        message_to_dict_fn=transport_message_asdict,
        normalize_whatsapp_recipient_fn=normalize_whatsapp_recipient,
        whatsapp_address_error=WhatsAppAddressError,
        tenant_whatsapp_provider_fn=tenant_whatsapp_provider,
        whatsapp_send_url_fn=_whatsapp_send_url,
        prepare_whatsapp_payload_fn=_prepare_whatsapp_payload,
        outbox_enabled_fn=_outbox_enabled,
        get_outbox_whitelist_fn=get_outbox_whitelist,
        whitelist_contains_number_fn=whitelist_contains_number,
        get_redis_client_fn=lambda: _r or getattr(settings, "r", None),
        outbox_queue_key=OUTBOX_QUEUE_KEY,
        send_strategy=SEND_STRATEGY,
        mark_handoff_silence_fn=_mark_handoff_silence,
        ensure_worker_healthy_fn=_ensure_worker_healthy,
        transport_client_fn=_transport_client,
        resolve_or_create_contact_fn=resolve_or_create_contact,
        upsert_lead_fn=upsert_lead,
        link_lead_contact_fn=link_lead_contact,
        message_out_counter=MESSAGE_OUT_COUNTER,
        send_fail_counter=SEND_FAIL_COUNTER,
        logger=transport_logger,
    )


@app.get("/metrics")
async def metrics_endpoint() -> Response:
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.post("/send")
async def send_transport_message(request: Request, message: TransportMessage) -> Response:
    return await handle_send_transport_message(request, message, _send_transport_deps())


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _ok(data: dict | None = None, status: int = 200):
    return JSONResponse({"ok": True, **(data or {})}, status_code=status)


def _err(msg: str, status: int = 400):
    return JSONResponse({"ok": False, "error": msg}, status_code=status)


def _resolve_catalog_attachment(cfg, tenant, request=None):
    if catalog_flow_service is not None:
        attachment, caption = catalog_flow_service._resolve_catalog_attachment(
            cfg,
            tenant,
            request,
        )
        if attachment is not None:
            return attachment, caption
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
async def internal_catalog_file(tenant: int, path: str, request: Request, token: str = ""):
    if not C.is_internal_request_authorized(request, token=token):
        internal_token = (getattr(C, "WA_INTERNAL_TOKEN", "") or "").strip()
        query_token = ""
        try:
            query_token = (request.query_params.get("token") or "").strip()
        except Exception:
            query_token = ""
        if not (internal_token and query_token == internal_token):
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
        uploaded_meta = (
            integrations.get("uploaded_catalog") if isinstance(integrations, dict) else {}
        )
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

    inline_flag = request.query_params.get("inline", "")
    if str(inline_flag).strip().lower() not in {"", "0", "false", "no", "off"}:
        disposition = f'inline; filename="{display_name}"'
        response.headers["Content-Disposition"] = disposition

    if request.method.upper() == "HEAD":
        response.body_iterator = iter(())

    return response


# монтирование роутеров
app.include_router(admin_router)
app.include_router(auth_router)
app.include_router(public_router)
app.include_router(analytics_avito_router)
app.include_router(client_router)
app.include_router(internal_tenant_router)
app.include_router(webhook)
app.include_router(webhooks_router)


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


@app.get("/internal/health/deep")
async def internal_deep_health(request: Request, tenants: str = "1,3") -> JSONResponse:
    if not C.is_internal_request_authorized(request):
        raise HTTPException(status_code=401, detail="unauthorized")
    redis_client = None
    try:
        redis_client = C.redis_client()
    except Exception:
        redis_client = None
    payload = await ops_health.build_deep_health(redis_client=redis_client, tenants=tenants)
    return JSONResponse(payload, status_code=200 if payload.get("ok") else 503)


async def _bypass_client_settings_cache(request: Request, call_next):
    response = await call_next(request)
    try:
        if request.url.path == "/static/js/client-settings.js":
            response.headers["Cache-Control"] = "no-store, max-age=0"
            response.headers["Pragma"] = "no-cache"
            for header_name in ("etag", "ETag", "last-modified", "Last-Modified"):
                if header_name in response.headers:
                    del response.headers[header_name]
    finally:
        return response


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


async def _security_headers(request: Request, call_next):
    response = await call_next(request)
    headers = response.headers
    headers.setdefault("X-Content-Type-Options", "nosniff")
    headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    return response


if hasattr(app, "middleware"):
    app.middleware("http")(_bypass_client_settings_cache)
    app.middleware("http")(_log_requests)
    app.middleware("http")(_security_headers)
