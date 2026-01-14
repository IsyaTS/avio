from __future__ import annotations

import pathlib
import os, json, re, time, mimetypes, uuid
from urllib.parse import quote, parse_qsl, urlencode, urlparse
from typing import Any

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
    "public": ("router", "templates"),
    "analytics_avito": ("router",),
    "client": ("router",),
    "webhooks": ("router", "process_incoming"),
}

try:
    from libs.core.services import catalog_flow as catalog_flow_service
except ImportError:
    catalog_flow_service = None  # type: ignore


def _load_web_module_from_source(module_name: str, full_name: str) -> ModuleType:
    module_path = pathlib.Path(__file__).resolve().parent / "web" / f"{module_name}.py"
    loader = importlib.machinery.SourceFileLoader(full_name, str(module_path))
    spec = importlib.util.spec_from_loader(full_name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    sys.modules[full_name] = module
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
public_router = _public_mod.router  # type: ignore[attr-defined]
analytics_avito_router = _analytics_avito_mod.router  # type: ignore[attr-defined]
client_router = _client_mod.router  # type: ignore[attr-defined]
webhooks_router = _webhooks_mod.router  # type: ignore[attr-defined]
process_incoming = _webhooks_mod.process_incoming  # type: ignore[attr-defined]

from libs.core.internal.tenant import router as internal_tenant_router

import importlib.util as _importlib_util

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
    whitelist_contains_number,
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
        await redis_client.set(
            handoff_silence_key(int(tenant), int(lead_id)),
            str(int(time.time())),
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
    base = getattr(settings, "BAILEYS_URL", "") or os.getenv("BAILEYS_URL") or "http://wabaileys:9002"
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
        settings.APP_INTERNAL_URL
        or os.getenv("APP_INTERNAL_URL")
        or "http://app:8000"
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
        timeout = httpx.Timeout(12.0)
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
        raise


@app.on_event("startup")
async def _startup_log_revision() -> None:
    if IS_TESTING:
        return
    await _log_alembic_revision_on_startup()


@app.on_event("startup")
async def _startup_outbox_worker() -> None:
    if IS_TESTING:
        return
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
    admin_token = _admin_token()
    header_token = (request.headers.get("X-Admin-Token") or "").strip()
    if admin_token and header_token != admin_token:
        raise HTTPException(status_code=401, detail="unauthorized")

    if not message.has_content:
        raise HTTPException(status_code=400, detail="empty_message")

    payload = transport_message_asdict(message)
    manager_flag = False
    lead_from_meta = 0
    try:
        manager_flag = (request.query_params.get("manager") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
    except Exception:
        manager_flag = False
    channel = message.channel
    wa_provider: str | None = None
    endpoint = CHANNEL_ENDPOINTS.get(channel)
    if channel == "whatsapp":
        wa_provider = tenant_whatsapp_provider(message.tenant)
        endpoint = _whatsapp_send_url(wa_provider, message.tenant)
    if not endpoint:
        raise HTTPException(status_code=400, detail="channel_unknown")

    request_headers: dict[str, str] | None = None
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

    if channel == "whatsapp":
        payload = _prepare_whatsapp_payload(payload, message.tenant)
        if wa_provider == "baileys":
            payload["tenant"] = int(message.tenant)
            payload["tenant_id"] = int(message.tenant)

        strategy_override = ""
        try:
            strategy_override = (request.query_params.get("strategy") or "").strip().lower()
        except Exception:
            strategy_override = ""

        use_queue = SEND_STRATEGY == "redis" and strategy_override != "direct"

        if use_queue:
            redis_client = _r or getattr(settings, "r", None)
            if redis_client is None:
                transport_logger.error(
                    "event=message_out channel=%s tenant=%s to=%s status=queue_unavailable",
                    channel,
                    message.tenant,
                    normalized_to or "-",
                )
                raise HTTPException(status_code=502, detail="queue_unavailable")

            meta_payload: dict[str, Any] = {}
            if isinstance(message.meta, dict):
                try:
                    meta_payload = json.loads(
                        json.dumps(message.meta, ensure_ascii=False)
                    )
                except Exception:
                    meta_payload = dict(message.meta)
            if not manager_flag and isinstance(meta_payload, dict):
                raw_manager = meta_payload.get("manager")
                if isinstance(raw_manager, str):
                    manager_flag = raw_manager.strip().lower() in {"1", "true", "yes", "on"}
                else:
                    manager_flag = bool(raw_manager)

            lead_hint = None
            if isinstance(meta_payload, dict):
                lead_hint = meta_payload.get("lead_id") or meta_payload.get("leadId")
            try:
                lead_from_meta = int(lead_hint) if lead_hint is not None else 0
            except Exception:
                lead_from_meta = 0

            base_lead_id = (
                lead_from_meta if lead_from_meta and lead_from_meta > 0 else int(time.time() * 1000)
            )

            digits_only = ""
            if isinstance(normalized_to, str):
                digits_only = normalized_to.split("@", 1)[0]

            contact_id = 0
            if digits_only:
                try:
                    contact_id = await resolve_or_create_contact(whatsapp_phone=digits_only)
                except Exception:
                    contact_id = 0

            lead_resolved = base_lead_id
            try:
                lead_resolved = await upsert_lead(
                    base_lead_id,
                    channel="whatsapp",
                    tenant_id=int(message.tenant),
                    peer=normalized_to or digits_only or None,
                    contact=digits_only or None,
                    title=(f"WhatsApp {digits_only}" if digits_only else None),
                )
            except Exception:
                lead_resolved = base_lead_id

            if lead_resolved and contact_id:
                try:
                    await link_lead_contact(
                        lead_resolved,
                        contact_id,
                        channel="whatsapp",
                        peer=normalized_to or digits_only,
                    )
                except Exception:
                    pass

            lead_for_queue = lead_resolved if lead_resolved and lead_resolved > 0 else base_lead_id
            if isinstance(meta_payload, dict):
                meta_payload.setdefault("lead_id", lead_for_queue)

            queue_message_id = (
                payload.get("message_id")
                or payload.get("meta", {}).get("message_id")
                or getattr(message, "message_id", None)
                or str(uuid.uuid4())
            )

            queue_item: dict[str, Any] = {
                "lead_id": lead_for_queue,
                "tenant_id": int(message.tenant),
                "tenant": int(message.tenant),
                "provider": "whatsapp",
                "ch": "whatsapp",
                "channel": "whatsapp",
                "to": payload.get("to"),
                "text": payload.get("text") or "",
                "attachments": payload.get("attachments", []),
                "attachment": payload.get("attachment"),
                "meta": meta_payload,
                "message_id": queue_message_id,
                "queued_at": time.time(),
                "origin": "app.send",
            }
            if wa_provider == "baileys":
                raw_to_jid = payload.get("to_jid")
                if isinstance(raw_to_jid, str) and raw_to_jid.strip():
                    queue_item["to_jid"] = raw_to_jid.strip()
            if contact_id:
                queue_item["contact_id"] = contact_id
            await _mark_handoff_silence(
                tenant=message.tenant,
                lead_id=lead_for_queue,
                manager_flag=manager_flag,
            )

            try:
                await redis_client.lpush(
                    OUTBOX_QUEUE_KEY, json.dumps(queue_item, ensure_ascii=False)
                )
            except Exception as exc:
                transport_logger.error(
                    "event=message_out channel=%s tenant=%s to=%s status=queue_push_failed error=%s",
                    channel,
                    message.tenant,
                    normalized_to or "-",
                    exc,
                )
                raise HTTPException(status_code=502, detail="queue_push_failed") from exc

            MESSAGE_OUT_COUNTER.labels(channel, "queued").inc()
            transport_logger.info(
                "event=message_out channel=%s tenant=%s to=%s status=queued strategy=redis",
                channel,
                message.tenant,
                normalized_to or "-",
            )
            return JSONResponse({"ok": True, "queued": True, "strategy": "redis"})

        if wa_provider != "baileys":
            token = _admin_token()
            request_headers = {"X-Auth-Token": token}
            await _ensure_worker_healthy()

    try:
        client = _transport_client(channel, wa_provider if channel == "whatsapp" else None)
        request_kwargs: dict[str, Any] = {
            "json": payload,
            "timeout": httpx.Timeout(12.0),
        }
        if channel == "whatsapp":
            timeout_value = 300.0 if (wa_provider or "waweb") != "baileys" else 60.0
            request_kwargs["timeout"] = httpx.Timeout(timeout_value)
        if request_headers:
            request_kwargs["headers"] = request_headers
        response = await client.post(endpoint, **request_kwargs)
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
    await _mark_handoff_silence(
        tenant=message.tenant,
        lead_id=lead_from_meta,
        manager_flag=manager_flag,
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
async def internal_catalog_file(
    tenant: int, path: str, request: Request, token: str = ""
):
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

    inline_flag = request.query_params.get("inline", "")
    if str(inline_flag).strip().lower() not in {"", "0", "false", "no", "off"}:
        disposition = f'inline; filename="{display_name}"'
        response.headers["Content-Disposition"] = disposition

    if request.method.upper() == "HEAD":
        response.body_iterator = iter(())

    return response

# монтирование роутеров
app.include_router(admin_router)
app.include_router(public_router)
app.include_router(analytics_avito_router)
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


if hasattr(app, "middleware"):
    app.middleware("http")(_bypass_client_settings_cache)
    app.middleware("http")(_log_requests)
