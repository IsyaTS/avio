from __future__ import annotations
import os
import json
import time
import asyncio
import random
import urllib.request
import urllib.error
import logging
from logging import StreamHandler
import pathlib
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, Optional
from urllib.parse import (
    urljoin,
    urlparse,
)

import httpx

import redis.asyncio as redis
from libs.core.sales_core import (
    settings as core_settings,
    tenant_waweb_url,
    tenant_whatsapp_provider,
    _strip_instruction_leaks,
    persona_meta_config,
    read_tenant_config,
    tenant_dir,
)
from libs.core.response_pipeline import run_response_pipeline

from libs.core.db import (
    init_db,
    insert_message_out,
    insert_message_in,
    upsert_lead,
    lead_exists,
    find_lead_by_telegram,
    get_telegram_user_id_by_lead,
    get_lead_peer,
    update_message_status,
    has_recent_incoming_message,
    get_contact_id_by_lead,
    get_contact_id_by_phone,
    get_contact_phone_by_lead,
    resolve_or_create_contact,
    link_lead_contact,
    update_contact_phone,
    update_contact_avito_login,
    update_contact_telegram,
    update_contact_max,
    fetch_pending_training_examples,
    set_training_embedding,
)
from libs.core.dao.leads import get_or_create_by_peer
from libs.core.metrics import MESSAGE_OUT_COUNTER, DB_ERRORS_COUNTER
from libs.core.message_envelope import (
    content_fingerprint,
    normalize_attachments as normalize_message_attachments,
    normalize_attachment as normalize_message_attachment,
    sanitize_display_name,
)
from libs.core.common import (
    OUTBOX_QUEUE_KEY,
    OUTBOX_DLQ_KEY,
    get_outbox_whitelist,
    normalize_username,
    smart_reply_enabled,
    notification_chat_ids,
    notification_event_enabled,
    default_fallback_reply,
    HANDOFF_SILENCE_TTL_SECONDS,
    handoff_silence_key,
    handoff_silence_meta_key,
    AVITO_BOT_ECHO_TTL_SECONDS,
    avito_bot_echo_key,
)
from libs.core.integrations import avito as avito_integration
from libs.core.integrations import max as max_integration
from libs.core.integrations import amocrm as amocrm_integration
from libs.core.transport import max_personal as max_personal_transport
from libs.core.services import max_personal_service
from libs.core.services import amocrm as amocrm_service
from libs.core.services import amocrm_chat as amocrm_chat_service
from libs.core.services import avito_behavior, avito_item_city
from libs.core.services import incoming_events
from libs.core.services import notifications as notification_service
from libs.core.services import outbox_payloads
from libs.core.services import queue_contract
try:
    from libs.core.services import catalog_flow as catalog_flow_service
except Exception:  # pragma: no cover - defensive import for worker boot
    catalog_flow_service = None  # type: ignore[assignment]
from libs.core.repo import crm_chat_links, crm_links, crm_outbox
from libs.core.transport import (
    WhatsAppAddressError,
    normalize_e164_digits,
    normalize_whatsapp_recipient,
)
from libs.core.transport import telegram as telegram_transport
from libs.core.training import embeddings as training_embeddings
from libs.core.lib.numbers import coerce_int as _coerce_int_shared
from libs.core.lib.tg_slots import (
    TG_SLOT_MAX,
    TG_SLOT_MIN,
    normalize_tg_slot as _normalize_tg_slot_shared,
    virtual_tenant_id as _virtual_tenant_id_shared,
)
from apps.api.web.common import WA_INTERNAL_TOKEN as COMMON_WA_INTERNAL_TOKEN
from apps.worker import followups
from apps.worker.services import amocrm_outbox_runtime
from apps.worker.services import attachment_runtime
from apps.worker.services import amocrm_bridge_runtime
from apps.worker.services import avito_contact_identity_runtime, avito_incoming_runtime
from apps.worker.services import notification_dispatcher
from apps.worker.services import outbox_writer
from apps.worker.services import queue_loops
from apps.worker.services import training_embeddings_runtime
from apps.worker.services import max_incoming_runtime
from apps.worker.services import max_outbound_runtime
from apps.worker.services import avito_outbound_runtime
from apps.worker.services import incoming_dispatcher
from apps.worker.services import outbox_send_runtime
from apps.worker.services import telegram_incoming_runtime
from apps.worker.services import reply_splitter
from apps.worker.services import auto_photos
from apps.worker.services import outbox_whitelist
from apps.worker.services import smart_reply_runtime
from apps.worker.services import handoff_runtime
from apps.worker.services import amocrm_inbound_runtime
from apps.worker.services import channel_config_runtime
from apps.worker.services import telegram_bridge_runtime
from apps.worker.services import telegram_outbound_runtime
from apps.worker.services import text_extract_runtime
from apps.worker.services import whatsapp_incoming_runtime
from apps.worker.services import whatsapp_outbound_runtime

# Guard against attribute absence when the worker boots before settings load
_default_version = getattr(core_settings, "APP_VERSION", "v21.0")

APP_VERSION = os.getenv("APP_VERSION", _default_version)


# ==== Logging ====
def _init_logging() -> None:
    level_name = (os.getenv("LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt)
    for name in ("training", "libs.core.sales_core"):
        lg = logging.getLogger(name)
        lg.setLevel(level)
        if not any(isinstance(h, StreamHandler) for h in lg.handlers):
            h = StreamHandler()
            h.setFormatter(logging.Formatter(fmt))
            lg.addHandler(h)


_init_logging()

# ==== ENV ====
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
# TTL for deduplicating Telegram outreach triggered by Avito phone detection.
AVITO_PHONE_TG_TTL_SECONDS = int(os.getenv("AVITO_PHONE_TG_TTL", "86400"))
# Allow disabling phone→tg deduplication (for diagnostics/edge cases).
AVITO_PHONE_TG_DEDUP_ENABLED = (
    os.getenv("AVITO_PHONE_TG_DEDUP_ENABLED") or ""
).strip().lower() in {"1", "true", "yes"}
# TTL for suppressing repeated Avito auto-replies in the same chat/lead.
AVITO_AUTO_REPLY_TTL_SECONDS = int(os.getenv("AVITO_AUTO_REPLY_TTL", "86400"))
# Match waweb INTERNAL_SYNC_TOKEN resolution (shared with the web layer)
WA_INTERNAL_TOKEN = COMMON_WA_INTERNAL_TOKEN
_DEFAULT_WORKER_BASE = getattr(core_settings, "DEFAULT_WORKER_BASE_URL", "http://worker:8000")
AMOCRM_OUTBOX_ENABLED = (os.getenv("AMOCRM_OUTBOX_ENABLED") or "").strip().lower() not in {
    "0",
    "false",
    "no",
}
AMOCRM_OUTBOX_LIMIT = int(os.getenv("AMOCRM_OUTBOX_LIMIT", "10") or 10)
AMOCRM_OUTBOX_MAX_ATTEMPTS = int(os.getenv("AMOCRM_OUTBOX_MAX_ATTEMPTS", "6") or 6)
_WORKER_BASE_RAW = (
    os.getenv("WORKER_BASE_URL")
    or os.getenv("TGWORKER_URL")
    or os.getenv("TGWORKER_BASE_URL")
    or os.getenv("TG_WORKER_URL")
    or getattr(core_settings, "WORKER_BASE_URL", "")
)
TGWORKER_BASE_URL = str(_WORKER_BASE_RAW).strip().rstrip("/") or _DEFAULT_WORKER_BASE
APP_BASE_URL = (
    (os.getenv("APP_BASE_URL") or os.getenv("APP_INTERNAL_URL") or os.getenv("APP_URL") or "")
    .strip()
    .rstrip("/")
)
TG_WORKER_TOKEN = (os.getenv("TG_WORKER_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
SEND = os.getenv("SEND_ENABLED", "true").lower() == "true"
TGWORKER_STATUS_URL = f"{TGWORKER_BASE_URL}/status"
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()
_OUTBOX_ENABLED_RAW = (os.getenv("OUTBOX_ENABLED") or "").strip().lower()
OUTBOX_ENABLED = _OUTBOX_ENABLED_RAW not in {"0", "false"}
LEARNING_EMBEDDINGS_ENABLED = (
    os.getenv("LEARNING_EMBEDDINGS_ENABLED") or "1"
).strip().lower() not in {
    "",
    "0",
    "false",
    "no",
    "off",
}
EMBEDDING_MODEL = (
    os.getenv("EMBEDDING_MODEL")
    or getattr(core_settings, "EMBEDDING_MODEL", "")
    or "text-embedding-3-small"
)
AVITO_TIMEOUT = getattr(core_settings, "AVITO_TIMEOUT", 10.0)
AVITO_IMAGE_MAX_BYTES = 24 * 1024 * 1024
AVITO_FILE_MAX_BYTES = 100 * 1024 * 1024
MAX_BOT_ECHO_TTL_SECONDS = max(30, int(os.getenv("MAX_BOT_ECHO_TTL_SECONDS", "120") or 120))
_INBOX_ENABLED_RAW = (os.getenv("INBOX_ENABLED") or "").strip().lower()
INBOX_ENABLED = _INBOX_ENABLED_RAW not in {"", "0", "false", "no", "off"}
INCOMING_QUEUE_KEY = (
    os.getenv("INCOMING_QUEUE_KEY") or os.getenv("INBOX_QUEUE_KEY") or "inbox:message_in"
)
FOLLOWUPS_ENABLED = (os.getenv("FOLLOWUPS_ENABLED") or "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
try:
    INBOX_BLOCK_TIMEOUT = max(1, int(os.getenv("INBOX_BLOCK_TIMEOUT", "5")))
except Exception:
    INBOX_BLOCK_TIMEOUT = 5
SMART_REPLY_PUNCT_STYLE_ENABLED = (
    os.getenv("SMART_REPLY_PUNCT_STYLE_ENABLED") or "1"
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SMART_REPLY_SPLIT_ENABLED = (os.getenv("SMART_REPLY_SPLIT_ENABLED") or "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
try:
    SMART_REPLY_SPLIT_MIN_LEN = max(40, int(os.getenv("SMART_REPLY_SPLIT_MIN_LEN", "70")))
except Exception:
    SMART_REPLY_SPLIT_MIN_LEN = 70
try:
    SMART_REPLY_SPLIT_MAX_LEN = max(
        SMART_REPLY_SPLIT_MIN_LEN + 20, int(os.getenv("SMART_REPLY_SPLIT_MAX_LEN", "120"))
    )
except Exception:
    SMART_REPLY_SPLIT_MAX_LEN = max(SMART_REPLY_SPLIT_MIN_LEN + 20, 120)
try:
    SMART_REPLY_SPLIT_MAX_PARTS = max(2, int(os.getenv("SMART_REPLY_SPLIT_MAX_PARTS", "6")))
except Exception:
    SMART_REPLY_SPLIT_MAX_PARTS = 6
_SMART_REPLY_SPLIT_CHANNELS_RAW = (
    os.getenv("SMART_REPLY_SPLIT_CHANNELS") or "telegram,avito,whatsapp,max,max_personal"
).strip()
SMART_REPLY_SPLIT_CHANNELS = {
    part.strip().lower() for part in _SMART_REPLY_SPLIT_CHANNELS_RAW.split(",") if part.strip()
}
if not SMART_REPLY_SPLIT_CHANNELS:
    SMART_REPLY_SPLIT_CHANNELS = {"telegram", "avito", "whatsapp", "max", "max_personal"}
SMART_REPLY_SPLIT_PART_DELAY_ENABLED = (
    os.getenv("SMART_REPLY_SPLIT_PART_DELAY_ENABLED") or "1"
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
try:
    SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS = max(
        0,
        int(os.getenv("SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS", "5")),
    )
except Exception:
    SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS = 5
try:
    SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS = max(
        SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS,
        int(os.getenv("SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS", "10")),
    )
except Exception:
    SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS = max(
        SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS,
        10,
    )
SMART_REPLY_BURST_ENABLED = (os.getenv("SMART_REPLY_BURST_ENABLED") or "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
try:
    SMART_REPLY_DELAY_MIN_SECONDS = max(0, int(os.getenv("SMART_REPLY_DELAY_MIN_SECONDS", "40")))
except Exception:
    SMART_REPLY_DELAY_MIN_SECONDS = 40
try:
    SMART_REPLY_DELAY_MAX_SECONDS = max(
        SMART_REPLY_DELAY_MIN_SECONDS,
        int(os.getenv("SMART_REPLY_DELAY_MAX_SECONDS", "120")),
    )
except Exception:
    SMART_REPLY_DELAY_MAX_SECONDS = max(120, SMART_REPLY_DELAY_MIN_SECONDS)
try:
    SMART_REPLY_FIRST_TTL_SECONDS = max(
        300, int(os.getenv("SMART_REPLY_FIRST_TTL_SECONDS", "1800"))
    )
except Exception:
    SMART_REPLY_FIRST_TTL_SECONDS = 1800
try:
    SMART_REPLY_BURST_MAX_MESSAGES = max(2, int(os.getenv("SMART_REPLY_BURST_MAX_MESSAGES", "8")))
except Exception:
    SMART_REPLY_BURST_MAX_MESSAGES = 8
_SMART_REPLY_DELAY_CHANNELS_RAW = (
    os.getenv("SMART_REPLY_DELAY_CHANNELS") or "telegram,avito,whatsapp,max,max_personal"
).strip()
SMART_REPLY_DELAY_CHANNELS = {
    part.strip().lower() for part in _SMART_REPLY_DELAY_CHANNELS_RAW.split(",") if part.strip()
}
if not SMART_REPLY_DELAY_CHANNELS:
    SMART_REPLY_DELAY_CHANNELS = {"telegram", "avito", "whatsapp", "max", "max_personal"}
SMART_REPLY_FIRST_KEY_PREFIX = "smart_reply:first_sent"
TENANT_ID = int(os.getenv("TENANT_ID", "1"))
QUEUES = [OUTBOX_QUEUE_KEY]

_PENDING_SMART_REPLIES: Dict[str, Dict[str, Any]] = {}
_PENDING_SMART_REPLY_LOCK = asyncio.Lock()

r = redis.from_url(REDIS_URL, decode_responses=True)
NOTIFY_EVENT_MANAGER = "manager_requested"
NOTIFY_BOT_TOKEN = (os.getenv("NOTIFY_BOT_TOKEN") or "").strip()
NOTIFY_BOT_PARSE_MODE = (os.getenv("NOTIFY_BOT_PARSE_MODE") or "Markdown").strip()
try:
    NOTIFY_BOT_ID = int(NOTIFY_BOT_TOKEN.split(":")[0]) if NOTIFY_BOT_TOKEN else None
except Exception:
    NOTIFY_BOT_ID = None


def _notification_link(tenant_id: int, lead_id: int) -> str:
    base = APP_BASE_URL or getattr(core_settings, "APP_PUBLIC_URL", "").strip().rstrip("/")
    if not base:
        return ""
    return f"{base}/admin?tenant={tenant_id}&lead={lead_id}"


def _notification_lead_title(lead_id: int, contact_phone: str | None) -> str:
    return handoff_runtime.notification_lead_title(lead_id, contact_phone)


def _build_chat_link(username: str | None, phone: str | None, peer: str | None) -> str | None:
    return handoff_runtime.build_chat_link(username, phone, peer)


def _handoff_deps() -> handoff_runtime.ManagerHandoffDeps:
    return handoff_runtime.ManagerHandoffDeps(
        redis_client=r,
        handoff_silence_ttl_seconds=HANDOFF_SILENCE_TTL_SECONDS,
        notify_event_manager=NOTIFY_EVENT_MANAGER,
        log_fn=log,
        notification_event_enabled_fn=notification_event_enabled,
        notification_chat_ids_fn=notification_chat_ids,
        get_contact_phone_by_lead_fn=get_contact_phone_by_lead,
        process_notification_fn=_process_notification,
        handoff_silence_key_fn=handoff_silence_key,
        handoff_silence_meta_key_fn=handoff_silence_meta_key,
    )


async def _enqueue_notification_payload(payload: Mapping[str, Any]) -> None:
    try:
        await queue_contract.push_json_left(r, OUTBOX_QUEUE_KEY, payload)
    except Exception:
        log(
            f"event=notify_enqueue_failed tenant={payload.get('tenant_id') or payload.get('tenant')} "
            f"lead_id={payload.get('lead_id') or '-'}"
        )


async def _notify_manager_handoff(
    tenant_id: int,
    lead_id: int,
    reason: str | None,
    contact_hint: str | None = None,
    username_hint: str | None = None,
) -> None:
    await handoff_runtime.notify_manager_handoff(
        tenant_id,
        lead_id,
        reason,
        contact_hint=contact_hint,
        username_hint=username_hint,
        deps=_handoff_deps(),
    )


async def _mark_handoff_silence(
    tenant_id: int,
    lead_id: int,
    reason: str | None = None,
    contact_hint: str | None = None,
    username_hint: str | None = None,
    notify: bool = True,
) -> None:
    await handoff_runtime.mark_handoff_silence(
        tenant_id,
        lead_id,
        reason=reason,
        contact_hint=contact_hint,
        username_hint=username_hint,
        notify=notify,
        deps=_handoff_deps(),
    )


async def _is_handoff_silenced(tenant_id: int, lead_id: int) -> bool:
    return await handoff_runtime.is_handoff_silenced(tenant_id, lead_id, deps=_handoff_deps())


def _coerce_chat_ids(raw: Any) -> list[int]:
    return notification_service.coerce_chat_ids(raw)


async def _send_notify_bot(chat_id: int, text: str) -> tuple[bool, int, str]:
    return await notification_dispatcher.send_notify_bot(
        chat_id,
        text,
        token=NOTIFY_BOT_TOKEN,
        httpx_module=httpx,
    )


def _looks_like_manager_outgoing(event: Mapping[str, Any]) -> bool:
    return incoming_events.looks_like_manager_outgoing(event)


# ==== Utils ====
def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except Exception:
        return default
    return value if value > 0 else default


WA_SEND_BASE_TIMEOUT = _env_float("WA_SEND_TIMEOUT_BASE", 120.0)
WA_SEND_TIMEOUT_PER_MIB = _env_float("WA_SEND_TIMEOUT_PER_MIB", 75.0)
WA_SEND_TIMEOUT_MAX = _env_float("WA_SEND_TIMEOUT_MAX", 1800.0)

_FALSE_VALUES = {"", "0", "false", "no", "off", "disabled"}


def _bool_env(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in _FALSE_VALUES


SMART_REPLY_TIMEOUT_SECONDS = _env_float("SMART_REPLY_TIMEOUT_SECONDS", 25.0)
if SMART_REPLY_TIMEOUT_SECONDS < 5.0:
    SMART_REPLY_TIMEOUT_SECONDS = 5.0

FALLBACK_REPLY_TEXT = (default_fallback_reply() or "").strip()

PDF_COMPRESS_ENABLED = _bool_env("WA_PDF_COMPRESS", True)
PDF_COMPRESS_SETTINGS = (os.getenv("WA_PDF_COMPRESS_SETTINGS") or "/screen").strip() or "/screen"
PDF_COMPRESS_TIMEOUT = max(10, int(os.getenv("WA_PDF_COMPRESS_TIMEOUT", "120")))
PDF_COMPRESS_BIN = os.getenv("WA_PDF_COMPRESS_BIN") or "gs"
TG_PDF_FAST_ENABLED = _bool_env("TG_PDF_FAST_ENABLED", True)
try:
    TG_PDF_FAST_MIN_MB = float(os.getenv("TG_PDF_FAST_MIN_MB", "12") or "12")
except Exception:
    TG_PDF_FAST_MIN_MB = 12.0
if TG_PDF_FAST_MIN_MB < 1.0:
    TG_PDF_FAST_MIN_MB = 1.0
try:
    TG_PDF_FAST_TARGET_MB = float(os.getenv("TG_PDF_FAST_TARGET_MB", "8") or "8")
except Exception:
    TG_PDF_FAST_TARGET_MB = 8.0
if TG_PDF_FAST_TARGET_MB < 1.0:
    TG_PDF_FAST_TARGET_MB = 1.0
TG_PDF_FAST_SUFFIX = (os.getenv("TG_PDF_FAST_SUFFIX") or ".tg.fast.pdf").strip() or ".tg.fast.pdf"
TG_PDF_FAST_WARMUP_ENABLED = _bool_env("TG_PDF_FAST_WARMUP_ENABLED", True)
try:
    TG_PDF_FAST_WARMUP_DELAY_SECONDS = float(
        os.getenv("TG_PDF_FAST_WARMUP_DELAY_SECONDS", "5") or "5"
    )
except Exception:
    TG_PDF_FAST_WARMUP_DELAY_SECONDS = 5.0
if TG_PDF_FAST_WARMUP_DELAY_SECONDS < 0.0:
    TG_PDF_FAST_WARMUP_DELAY_SECONDS = 0.0
TENANTS_ROOT = pathlib.Path((os.getenv("TENANTS_DIR") or "/data/tenants").strip())


def _resolve_gs_path() -> str | None:
    return attachment_runtime.resolve_gs_path(
        compress_bin=PDF_COMPRESS_BIN,
        env_bin=os.getenv("WA_PDF_COMPRESS_BIN"),
    )


def _waweb_base_url(tenant: Optional[int]) -> str:
    base = ""
    if tenant is not None:
        try:
            base = tenant_waweb_url(int(tenant))
        except Exception:
            base = ""
    if not base:
        base = getattr(core_settings, "WA_WEB_URL", "http://waweb:9001")
    return str(base).rstrip("/")


def _wabaileys_base_url() -> str:
    base = getattr(core_settings, "BAILEYS_URL", "http://wabaileys:9002")
    return str(base).rstrip("/")


def log(*parts: object):
    if len(parts) == 1:
        print(parts[0], flush=True)
    else:
        print(" ".join(str(p) for p in parts), flush=True)


async def _maybe_amocrm_inbound(
    tenant_id: int,
    lead_id: int,
    text: str,
    channel: str,
    attachments: Iterable[Mapping[str, Any]] | None = None,
    message_id: int | None = None,
) -> None:
    await amocrm_inbound_runtime.maybe_amocrm_inbound(
        tenant_id,
        lead_id,
        text,
        channel,
        attachments=attachments,
        message_id=message_id,
        deps=amocrm_inbound_runtime.AmoCrmInboundDeps(
            redis_client=r,
            log_fn=log,
            normalize_attachments_fn=normalize_message_attachments,
            content_fingerprint_fn=content_fingerprint,
            amocrm_service_module=amocrm_service,
        ),
    )


async def _is_duplicate_telegram_incoming(
    *,
    tenant_id: int,
    message_id: int | None,
    telegram_user_id: int | None,
    peer: str | None,
) -> bool:
    if tenant_id <= 0 or message_id is None:
        return False
    scope = ""
    if telegram_user_id is not None:
        scope = str(int(telegram_user_id))
    elif isinstance(peer, str) and peer.strip():
        scope = peer.strip()
    else:
        scope = "unknown"
    dedup_key = f"inbox:telegram:incoming:{tenant_id}:{scope}:{int(message_id)}"
    try:
        fresh = await r.set(dedup_key, "1", ex=86400, nx=True)
    except Exception:
        return False
    return not bool(fresh)


def _log_smart_reply_diag(channel: str, tenant_id: int, lead_id: int | None, reply: Any) -> None:
    smart_reply_runtime.log_smart_reply_diag(
        channel,
        tenant_id,
        lead_id,
        reply,
        log_fn=log,
    )


AVITO_CHAT_CACHE: Dict[int, str] = {}


def _avito_auto_reply_text(tenant_id: int) -> str:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None
    return avito_behavior.extract_avito_auto_reply_text(cfg)


def _avito_phone_tg_template(tenant_id: int) -> str:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None
    try:
        persona_meta = persona_meta_config(int(tenant_id))
    except Exception:
        persona_meta = {}
    return avito_behavior.extract_avito_phone_tg_template(cfg, persona_meta)


def _avito_smart_reply_enabled(tenant_id: int) -> bool:
    """Per-tenant gate for Avito smart-reply; default disabled."""

    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None
    return avito_behavior.avito_smart_reply_enabled(cfg)


def _smart_reply_first_key(tenant_id: int, channel: str, lead_id: int) -> str:
    return f"{SMART_REPLY_FIRST_KEY_PREFIX}:{int(tenant_id)}:{channel}:{int(lead_id)}"


def _smart_reply_pending_key(tenant_id: int, channel: str, lead_id: int) -> str:
    return f"{int(tenant_id)}:{channel}:{int(lead_id)}"


def _channel_delay_enabled(channel: str) -> bool:
    if not SMART_REPLY_BURST_ENABLED:
        return False
    if SMART_REPLY_DELAY_MAX_SECONDS <= 0:
        return False
    return str(channel).strip().lower() in SMART_REPLY_DELAY_CHANNELS


async def _thread_has_recent_bot_reply(tenant_id: int, channel: str, lead_id: int) -> bool:
    if tenant_id <= 0 or lead_id <= 0:
        return False
    try:
        return bool(await r.exists(_smart_reply_first_key(tenant_id, channel, lead_id)))
    except Exception:
        return False


async def _mark_thread_bot_reply(tenant_id: int, channel: str, lead_id: int) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    try:
        await r.set(
            _smart_reply_first_key(tenant_id, channel, lead_id),
            str(int(time.time())),
            ex=SMART_REPLY_FIRST_TTL_SECONDS,
        )
    except Exception:
        pass


def _delay_seconds_value() -> float:
    if SMART_REPLY_DELAY_MAX_SECONDS <= SMART_REPLY_DELAY_MIN_SECONDS:
        return float(SMART_REPLY_DELAY_MIN_SECONDS)
    return float(random.randint(SMART_REPLY_DELAY_MIN_SECONDS, SMART_REPLY_DELAY_MAX_SECONDS))


def _split_part_delay_enabled(channel: str) -> bool:
    if not SMART_REPLY_SPLIT_PART_DELAY_ENABLED:
        return False
    if SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS <= 0:
        return False
    ch = str(channel).strip().lower()
    return ch in SMART_REPLY_SPLIT_CHANNELS


def _split_part_delay_seconds_value() -> float:
    if SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS <= SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS:
        return float(SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS)
    return float(
        random.randint(
            SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS,
            SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS,
        )
    )


def _auto_photo_deps() -> auto_photos.AutoPhotoDeps:
    return auto_photos.AutoPhotoDeps(
        app_base_url=APP_BASE_URL,
        read_tenant_config_fn=read_tenant_config,
        tenant_dir_fn=tenant_dir,
        log_fn=log,
    )


def _apply_custom_punctuation_style(text: str) -> str:
    return reply_splitter.apply_custom_punctuation_style(
        text,
        punct_style_enabled=SMART_REPLY_PUNCT_STYLE_ENABLED,
    )


def _split_long_segment(text: str, max_len: int) -> list[str]:
    return reply_splitter.split_long_segment(text, max_len)


def _merge_short_split_parts(parts: list[str], max_len: int) -> list[str]:
    return reply_splitter.merge_short_split_parts(parts, max_len)


def _is_punctuation_only_chunk(text: str) -> bool:
    return reply_splitter.is_punctuation_only_chunk(text)


def _split_reply_for_send(reply_text: str, channel: str) -> list[str]:
    return reply_splitter.split_reply_for_send(
        reply_text,
        channel,
        config=reply_splitter.ReplySplitConfig(
            enabled=SMART_REPLY_SPLIT_ENABLED,
            channels=SMART_REPLY_SPLIT_CHANNELS,
            min_len=SMART_REPLY_SPLIT_MIN_LEN,
            max_len=SMART_REPLY_SPLIT_MAX_LEN,
            max_parts=SMART_REPLY_SPLIT_MAX_PARTS,
        ),
    )


def _sanitize_outbound_reply_text(reply_text: str) -> str:
    return smart_reply_runtime.sanitize_outbound_reply_text(
        reply_text,
        strip_instruction_leaks_fn=_strip_instruction_leaks,
    )


def _clip_text(value: str, limit: int = 1200) -> str:
    return reply_splitter.clip_text(value, limit)


def _compose_burst_user_text(parts: list[str]) -> str:
    return reply_splitter.compose_burst_user_text(parts)


def _parse_send_not_before_ts(item: Mapping[str, Any]) -> float:
    raw = item.get("send_not_before_ts")
    if raw is None:
        return 0.0
    try:
        ts = float(raw)
    except Exception:
        return 0.0
    if ts <= 0:
        return 0.0
    return ts


def _merge_reply_context(
    channel: str, base: Mapping[str, Any], incoming: Mapping[str, Any]
) -> dict[str, Any]:
    return smart_reply_runtime.merge_reply_context(
        channel,
        base,
        incoming,
        normalize_tg_slot_fn=_normalize_tg_slot,
    )


async def _cancel_pending_smart_reply(
    tenant_id: int,
    channel: str,
    lead_id: int,
    *,
    reason: str,
) -> None:
    key = _smart_reply_pending_key(tenant_id, channel, lead_id)
    task: asyncio.Task[Any] | None = None
    async with _PENDING_SMART_REPLY_LOCK:
        payload = _PENDING_SMART_REPLIES.pop(key, None)
        if payload:
            task = payload.get("task")
    if task and not task.done():
        task.cancel()
    if task:
        log(
            "event=smart_reply_burst_cancel channel=%s tenant=%s lead_id=%s reason=%s"
            % (channel, tenant_id, lead_id, reason)
        )


def _can_generate_reply_for_channel(tenant_id: int, channel: str) -> bool:
    return smart_reply_runtime.can_generate_reply_for_channel(
        tenant_id,
        channel,
        deps=smart_reply_runtime.SmartReplyChannelDeps(
            telegram_reply_enabled_fn=_telegram_reply_enabled,
            max_reply_enabled_fn=_max_reply_enabled,
            max_personal_reply_enabled_fn=_max_personal_reply_enabled,
            avito_smart_reply_enabled_fn=_avito_smart_reply_enabled,
            smart_reply_enabled_fn=smart_reply_enabled,
        ),
    )


async def _generate_reply_text(
    *,
    tenant_id: int,
    lead_id: int,
    refer_id: int,
    channel: str,
    user_text: str,
) -> tuple[str, Any]:
    return await smart_reply_runtime.generate_reply_text(
        tenant_id=tenant_id,
        lead_id=lead_id,
        refer_id=refer_id,
        channel=channel,
        user_text=user_text,
        deps=smart_reply_runtime.SmartReplyGenerateDeps(
            run_response_pipeline_fn=run_response_pipeline,
            default_fallback_reply_fn=default_fallback_reply,
            strip_instruction_leaks_fn=_strip_instruction_leaks,
            log_smart_reply_diag_fn=_log_smart_reply_diag,
            log_fn=log,
            timeout_seconds=SMART_REPLY_TIMEOUT_SECONDS,
        ),
    )


async def _maybe_set_waiting_photo_state(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    reply_text: str,
) -> None:
    await smart_reply_runtime.maybe_set_waiting_photo_state(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        reply_text=reply_text,
        deps=smart_reply_runtime.WaitingPhotoDeps(
            redis_client=r,
            handoff_silence_ttl_seconds=HANDOFF_SILENCE_TTL_SECONDS,
            photo_expectation_config_fn=_photo_expectation_config,
            log_fn=log,
        ),
    )


async def _enqueue_channel_reply_payload(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    reply_text: str,
    user_text: str,
    context: Mapping[str, Any],
) -> bool:
    return await smart_reply_runtime.enqueue_channel_reply_payload(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        reply_text=reply_text,
        user_text=user_text,
        context=context,
        deps=smart_reply_runtime.SmartReplyEnqueueDeps(
            redis_client=r,
            outbox_queue_key=OUTBOX_QUEUE_KEY,
            split_part_delay_min_seconds=SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS,
            split_part_delay_max_seconds=SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS,
            log_fn=log,
            select_auto_photos_fn=_select_auto_photos,
            normalize_tg_slot_fn=_normalize_tg_slot,
            base_channel_reply_payload_fn=outbox_payloads.base_channel_reply_payload,
            split_reply_for_send_fn=_split_reply_for_send,
            apply_custom_punctuation_style_fn=_apply_custom_punctuation_style,
            is_punctuation_only_chunk_fn=_is_punctuation_only_chunk,
            split_part_delay_enabled_fn=_split_part_delay_enabled,
            split_part_delay_seconds_value_fn=_split_part_delay_seconds_value,
            queue_contract_module=queue_contract,
        ),
    )


async def _produce_and_enqueue_smart_reply(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    refer_id: int,
    user_text: str,
    context: Mapping[str, Any],
    delayed: bool = False,
) -> bool:
    return await smart_reply_runtime.produce_and_enqueue_smart_reply(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        refer_id=refer_id,
        user_text=user_text,
        context=context,
        delayed=delayed,
        deps=smart_reply_runtime.SmartReplyProduceDeps(
            log_fn=log,
            generate_reply_text_fn=_generate_reply_text,
            maybe_set_waiting_photo_state_fn=_maybe_set_waiting_photo_state,
            enqueue_channel_reply_payload_fn=_enqueue_channel_reply_payload,
            mark_thread_bot_reply_fn=_mark_thread_bot_reply,
        ),
    )


async def _flush_pending_smart_reply(key: str) -> None:
    await smart_reply_runtime.flush_pending_smart_reply(
        key,
        deps=smart_reply_runtime.SmartReplyFlushDeps(
            pending_replies=_PENDING_SMART_REPLIES,
            pending_lock=_PENDING_SMART_REPLY_LOCK,
            log_fn=log,
            sleep_fn=asyncio.sleep,
            is_handoff_silenced_fn=_is_handoff_silenced,
            can_generate_reply_for_channel_fn=_can_generate_reply_for_channel,
            compose_burst_user_text_fn=_compose_burst_user_text,
            produce_and_enqueue_smart_reply_fn=_produce_and_enqueue_smart_reply,
        ),
    )


async def _schedule_delayed_smart_reply(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    refer_id: int,
    user_text: str,
    context: Mapping[str, Any],
) -> None:
    await smart_reply_runtime.schedule_delayed_smart_reply(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        refer_id=refer_id,
        user_text=user_text,
        context=context,
        deps=smart_reply_runtime.SmartReplyScheduleDeps(
            pending_replies=_PENDING_SMART_REPLIES,
            pending_lock=_PENDING_SMART_REPLY_LOCK,
            burst_max_messages=SMART_REPLY_BURST_MAX_MESSAGES,
            log_fn=log,
            smart_reply_pending_key_fn=_smart_reply_pending_key,
            delay_seconds_value_fn=_delay_seconds_value,
            merge_reply_context_fn=_merge_reply_context,
            flush_pending_smart_reply_fn=_flush_pending_smart_reply,
        ),
    )


async def _try_handle_smart_reply_with_delay(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    refer_id: int,
    user_text: str,
    context: Mapping[str, Any],
    bypass_delay: bool = False,
) -> bool:
    if not _channel_delay_enabled(channel):
        return False
    if not user_text.strip():
        return False
    if bypass_delay:
        log(
            "event=smart_reply_delay_bypass channel=%s tenant=%s lead_id=%s reason=first_catalog"
            % (channel, tenant_id, lead_id)
        )
        return False
    if not await _thread_has_recent_bot_reply(tenant_id, channel, lead_id):
        return False
    await _schedule_delayed_smart_reply(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        refer_id=refer_id,
        user_text=user_text,
        context=context,
    )
    return True


def _extract_avito_user_name(
    payload: Mapping[str, Any], *, author_id: int | None, account_id: int | None
) -> str:
    return channel_config_runtime.extract_avito_user_name(
        payload,
        author_id=author_id,
        account_id=account_id,
        deps=channel_config_runtime.AvitoUserNameDeps(
            redis_client=r,
            avito_integration_module=avito_integration,
            log_fn=log,
            coerce_int_fn=_coerce_int,
        ),
    )


async def _resolve_avito_user_name(
    tenant_id: int,
    *,
    account_id: int | None,
    chat_id: str,
    author_id: int | None,
) -> str:
    return await channel_config_runtime.resolve_avito_user_name(
        tenant_id,
        account_id=account_id,
        chat_id=chat_id,
        author_id=author_id,
        deps=channel_config_runtime.AvitoUserNameDeps(
            redis_client=r,
            avito_integration_module=avito_integration,
            log_fn=log,
            coerce_int_fn=_coerce_int,
        ),
    )


def _coerce_bool_value(value: Any) -> bool | None:
    return channel_config_runtime.coerce_bool_value(value)


def _channel_config_deps() -> channel_config_runtime.ChannelConfigDeps:
    return channel_config_runtime.ChannelConfigDeps(
        read_tenant_config_fn=read_tenant_config,
        max_personal_service_module=max_personal_service,
    )


def _telegram_reply_enabled(tenant_id: int) -> bool:
    return channel_config_runtime.telegram_reply_enabled(tenant_id, deps=_channel_config_deps())


def _normalize_tg_slot(value: Any) -> int:
    return _normalize_tg_slot_shared(value)


def _virtual_tg_tenant(tenant_id: int, slot: int) -> int:
    return _virtual_tenant_id_shared(tenant_id, slot)


def _telegram_slot_settings(tenant_id: int) -> tuple[bool, dict[int, bool]]:
    multi_enabled = True
    enabled_map = {slot: True for slot in range(TG_SLOT_MIN, TG_SLOT_MAX + 1)}
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None
    if not isinstance(cfg, Mapping):
        return multi_enabled, enabled_map
    tg_cfg = cfg.get("telegram")
    if not isinstance(tg_cfg, Mapping):
        return multi_enabled, enabled_map
    raw_multi = _coerce_bool_value(tg_cfg.get("multi_slot_enabled"))
    if raw_multi is not None:
        multi_enabled = bool(raw_multi)
    slot_enabled = tg_cfg.get("slot_enabled")
    if isinstance(slot_enabled, Mapping):
        for idx in range(TG_SLOT_MIN, TG_SLOT_MAX + 1):
            raw_flag = slot_enabled.get(str(idx), slot_enabled.get(idx))
            normalized_flag = _coerce_bool_value(raw_flag)
            if normalized_flag is not None:
                enabled_map[idx] = bool(normalized_flag)
    return multi_enabled, enabled_map


def _telegram_slot_is_enabled(tenant_id: int, slot: int) -> bool:
    multi_mode, slot_enabled = _telegram_slot_settings(tenant_id)
    normalized = _normalize_tg_slot(slot)
    if not multi_mode and normalized != TG_SLOT_MIN:
        return False
    return bool(slot_enabled.get(normalized, True))


def _lead_tg_slot_key(tenant_id: int, lead_id: int) -> str:
    return f"tg:lead_slot:{int(tenant_id)}:{int(lead_id)}"


async def _store_lead_tg_slot(tenant_id: int, lead_id: int, slot: int) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    try:
        await r.set(
            _lead_tg_slot_key(tenant_id, lead_id),
            str(_normalize_tg_slot(slot)),
            ex=60 * 60 * 24 * 30,
        )
    except Exception:
        pass


async def _get_lead_tg_slot(tenant_id: int, lead_id: int) -> int | None:
    if tenant_id <= 0 or lead_id <= 0:
        return None
    try:
        raw = await r.get(_lead_tg_slot_key(tenant_id, lead_id))
    except Exception:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    try:
        value = int(str(raw).strip())
    except Exception:
        return None
    if TG_SLOT_MIN <= value <= TG_SLOT_MAX:
        return value
    return None


def _max_reply_enabled(tenant_id: int) -> bool:
    return channel_config_runtime.max_reply_enabled(tenant_id, deps=_channel_config_deps())


def _max_personal_reply_enabled(tenant_id: int) -> bool:
    return channel_config_runtime.max_personal_reply_enabled(
        tenant_id,
        deps=_channel_config_deps(),
    )


def _behavior_triggers(tenant_id: int) -> list[dict[str, Any]]:
    return channel_config_runtime.behavior_triggers(tenant_id, deps=_channel_config_deps())


def _match_behavior_trigger(tenant_id: int, channel: str, text: str) -> dict[str, Any] | None:
    return channel_config_runtime.match_behavior_trigger(
        tenant_id,
        channel,
        text,
        deps=_channel_config_deps(),
    )


def _photo_expectation_config(tenant_id: int) -> tuple[list[str], str, int]:
    return channel_config_runtime.photo_expectation_config(
        tenant_id,
        deps=_channel_config_deps(),
    )


def _photo_auto_config(tenant_id: int) -> tuple[bool, int]:
    return auto_photos.photo_auto_config(tenant_id, deps=_auto_photo_deps())


def _load_photo_manifest(tenant_id: int) -> list[dict[str, Any]]:
    return auto_photos.load_photo_manifest(tenant_id, deps=_auto_photo_deps())


def _tenant_public_key(tenant_id: int) -> str:
    return auto_photos.tenant_public_key(tenant_id, deps=_auto_photo_deps())


def _build_photo_public_url(tenant_id: int, photo_id: str) -> str:
    return auto_photos.build_photo_public_url(tenant_id, photo_id, deps=_auto_photo_deps())


def _build_photo_public_path(tenant_id: int, photo_id: str) -> str:
    return auto_photos.build_photo_public_path(tenant_id, photo_id, deps=_auto_photo_deps())


def _collect_outgoing_attachments(item: Mapping[str, Any], tenant_id: int) -> list[dict[str, Any]]:
    return auto_photos.collect_outgoing_attachments(item, tenant_id, deps=_auto_photo_deps())


async def _cache_max_bot_echo(
    tenant_id: int,
    channel: str,
    chat_key: str | None,
    text: str | None,
) -> None:
    try:
        await outbox_payloads.cache_max_bot_echo(
            r,
            tenant_id=tenant_id,
            channel=channel,
            chat_key=chat_key,
            text=text,
            ttl_seconds=MAX_BOT_ECHO_TTL_SECONDS,
        )
    except Exception as exc:
        log(
            "event=max_echo_cache_failed channel=%s tenant=%s chat=%s error=%s"
            % (channel, tenant_id, str(chat_key or "").strip(), exc)
        )


async def _is_recent_max_bot_echo(
    tenant_id: int,
    channel: str,
    chat_key: str | None,
    text: str | None,
) -> bool:
    return await outbox_payloads.is_recent_max_bot_echo(
        r,
        tenant_id=tenant_id,
        channel=channel,
        chat_key=chat_key,
        text=text,
    )


def _normalize_max_human_name(
    value: Any,
    *,
    peer_value: str | None = None,
    max_user_id: int | None = None,
) -> str | None:
    return text_extract_runtime.normalize_max_human_name(
        value,
        sanitize_display_name_fn=sanitize_display_name,
        peer_value=peer_value,
        max_user_id=max_user_id,
    )


def _normalize_photo_candidates(tenant_id: int, channel: str) -> list[dict[str, Any]]:
    return auto_photos.normalize_photo_candidates(tenant_id, channel, deps=_auto_photo_deps())


def _score_photo_candidate(candidate: Mapping[str, Any], text: str) -> int:
    return auto_photos.score_photo_candidate(candidate, text)


def _select_photos_by_tags(
    candidates: list[dict[str, Any]],
    user_text: str,
    reply_text: str,
    max_count: int,
) -> list[dict[str, Any]]:
    return auto_photos.select_photos_by_tags(candidates, user_text, reply_text, max_count)


def _guess_photo_mime(photo: Mapping[str, Any]) -> str:
    return auto_photos.guess_photo_mime(photo)


def _extract_photo_ids(reply_text: str, allowed: set[str], max_count: int) -> list[str]:
    return auto_photos.extract_photo_ids(reply_text, allowed, max_count)


async def _select_auto_photos(tenant_id: int, channel: str, user_text: str, reply_text: str, *, lead_id: int = 0, context: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    return await auto_photos.select_auto_photos(
        tenant_id,
        channel,
        user_text,
        reply_text,
        lead_id=lead_id,
        context=context,
        deps=_auto_photo_deps(),
    )


def _extract_ru_phone(text: str) -> str:
    return text_extract_runtime.extract_ru_phone(text)


def _extract_tg_username(text: str) -> str:
    return text_extract_runtime.extract_tg_username(
        text,
        normalize_username_fn=normalize_username,
    )


def _amocrm_bridge_deps() -> amocrm_bridge_runtime.AmoCrmBridgeDeps:
    return amocrm_bridge_runtime.AmoCrmBridgeDeps(
        sleep_fn=asyncio.sleep,
        normalize_e164_digits_fn=normalize_e164_digits,
        read_tenant_config_fn=read_tenant_config,
        amocrm_service_module=amocrm_service,
        amocrm_integration_module=amocrm_integration,
        crm_links_repo=crm_links,
        crm_chat_links_repo=crm_chat_links,
        crm_outbox_repo=crm_outbox,
        amocrm_chat_service_module=amocrm_chat_service,
    )


async def _resolve_live_amocrm_target_by_phone(
    tenant_id: int,
    *,
    phone: str | None,
    origin_lead_id: int | None = None,
) -> tuple[int | None, int | None]:
    return await amocrm_bridge_runtime.resolve_live_amocrm_target_by_phone(
        tenant_id,
        phone=phone,
        origin_lead_id=origin_lead_id,
        deps=_amocrm_bridge_deps(),
    )


async def _wait_for_amocrm_link_ready(
    tenant_id: int,
    lead_id: int,
    *,
    timeout_seconds: float = 8.0,
    poll_seconds: float = 0.4,
) -> Mapping[str, Any] | None:
    return await amocrm_bridge_runtime.wait_for_amocrm_link_ready(
        tenant_id,
        lead_id,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
        deps=_amocrm_bridge_deps(),
    )


async def _enqueue_amocrm_cleanup_event(
    tenant_id: int,
    lead_id: int,
    *,
    event_type: str,
    payload: Mapping[str, Any],
) -> None:
    await amocrm_bridge_runtime.enqueue_amocrm_cleanup_event(
        tenant_id,
        lead_id,
        event_type=event_type,
        payload=payload,
        deps=_amocrm_bridge_deps(),
    )


async def _reconcile_avito_bridge_amocrm_links(
    *,
    tenant_id: int,
    origin_lead_id: int,
    tg_lead_id: int,
    keep_provider_lead_id: int,
    keep_provider_contact_id: int | None,
) -> None:
    await amocrm_bridge_runtime.reconcile_avito_bridge_amocrm_links(
        tenant_id=tenant_id,
        origin_lead_id=origin_lead_id,
        tg_lead_id=tg_lead_id,
        keep_provider_lead_id=keep_provider_lead_id,
        keep_provider_contact_id=keep_provider_contact_id,
        deps=_amocrm_bridge_deps(),
    )


async def _send_telegram_to_target(
    tenant_id: int,
    text: str,
    *,
    phone: str | None = None,
    username: str | None = None,
    lead_id: int | None = None,
    contact_id: int | None = None,
) -> tuple[int, str]:
    return await telegram_bridge_runtime.send_telegram_to_target(
        tenant_id,
        text,
        phone=phone,
        username=username,
        lead_id=lead_id,
        contact_id=contact_id,
        deps=telegram_bridge_runtime.TelegramBridgeDeps(
            tg_worker_token=TG_WORKER_TOKEN,
            admin_token=ADMIN_TOKEN,
            log_fn=log,
            telegram_transport_module=telegram_transport,
            json_module=json,
            redis_client=r,
            normalize_username_fn=normalize_username,
            sanitize_display_name_fn=sanitize_display_name,
            find_lead_by_telegram_fn=find_lead_by_telegram,
            upsert_lead_fn=upsert_lead,
            crm_links_repo=crm_links,
            amocrm_service_module=amocrm_service,
            wait_for_amocrm_link_ready_fn=_wait_for_amocrm_link_ready,
            link_lead_contact_fn=link_lead_contact,
            update_contact_telegram_fn=update_contact_telegram,
            resolve_live_amocrm_target_by_phone_fn=_resolve_live_amocrm_target_by_phone,
            crm_outbox_repo=crm_outbox,
            crm_chat_links_repo=crm_chat_links,
            amocrm_chat_service_module=amocrm_chat_service,
            read_tenant_config_fn=read_tenant_config,
            reconcile_avito_bridge_amocrm_links_fn=_reconcile_avito_bridge_amocrm_links,
            create_task_fn=asyncio.create_task,
            insert_message_out_fn=insert_message_out,
        ),
    )


async def _send_telegram_to_phone(
    tenant_id: int,
    phone: str,
    text: str,
    *,
    lead_id: int | None = None,
    contact_id: int | None = None,
) -> tuple[int, str]:
    return await _send_telegram_to_target(
        tenant_id=tenant_id,
        text=text,
        phone=phone,
        lead_id=lead_id,
        contact_id=contact_id,
    )


async def _send_telegram_to_username(
    tenant_id: int,
    username: str,
    text: str,
    *,
    lead_id: int | None = None,
    contact_id: int | None = None,
) -> tuple[int, str]:
    return await _send_telegram_to_target(
        tenant_id=tenant_id,
        text=text,
        username=username,
        lead_id=lead_id,
        contact_id=contact_id,
    )


async def _enqueue_avito_auto_reply(
    *,
    tenant_id: int,
    lead_id: int,
    chat_id: str,
    account_id: Optional[int],
    user_id: Optional[int],
    login: Optional[str],
    message_id: str,
    text: str,
) -> bool:
    return await avito_incoming_runtime.enqueue_avito_auto_reply(
        tenant_id=tenant_id,
        lead_id=lead_id,
        chat_id=chat_id,
        account_id=account_id,
        user_id=user_id,
        login=login,
        message_id=message_id,
        text=text,
        deps=avito_incoming_runtime.AvitoAutoReplyEnqueueDeps(
            redis_client=r,
            outbox_queue_key=OUTBOX_QUEUE_KEY,
            outbox_payloads_module=outbox_payloads,
            queue_contract_module=queue_contract,
            log_fn=log,
        ),
    )


def _digits(s: str) -> str:
    return outbox_payloads.digits_only(s)


def _compress_pdf_bytes(data: bytes, filename: str, target_bytes: int) -> bytes | None:
    return attachment_runtime.compress_pdf_bytes(
        data,
        filename,
        target_bytes,
        enabled=PDF_COMPRESS_ENABLED,
        compress_bin=PDF_COMPRESS_BIN,
        settings=PDF_COMPRESS_SETTINGS,
        timeout=PDF_COMPRESS_TIMEOUT,
        env_bin=os.getenv("WA_PDF_COMPRESS_BIN"),
    )


def _attachment_deps() -> attachment_runtime.AttachmentRuntimeDeps:
    return attachment_runtime.AttachmentRuntimeDeps(
        tg_pdf_fast_enabled=TG_PDF_FAST_ENABLED,
        tg_pdf_fast_min_mb=TG_PDF_FAST_MIN_MB,
        tg_pdf_fast_target_mb=TG_PDF_FAST_TARGET_MB,
        tg_pdf_fast_suffix=TG_PDF_FAST_SUFFIX,
        is_internal_path_fn=_is_internal_path,
        normalize_internal_urls_fn=_normalize_internal_urls,
        download_internal_attachment_fn=_download_internal_attachment,
        resolve_attachment_filename_fn=_resolve_attachment_filename,
        resolve_attachment_mime_fn=_resolve_attachment_mime,
        compress_pdf_bytes_fn=_compress_pdf_bytes,
    )


def _tg_fast_pdf_cache_path(source_path: pathlib.Path) -> pathlib.Path:
    return attachment_runtime.tg_fast_pdf_cache_path(
        source_path,
        deps=_attachment_deps(),
    )


def _prepare_tg_attachment_fast_pdf(
    attachment: Mapping[str, Any],
) -> dict[str, Any]:
    return attachment_runtime.prepare_tg_attachment_fast_pdf(
        attachment,
        deps=_attachment_deps(),
    )


def _prepare_tg_attachments_for_send(
    tenant_id: int,
    attachments: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return attachment_runtime.prepare_tg_attachments_for_send(
        tenant_id,
        attachments,
        deps=_attachment_deps(),
    )


def _iter_tenants_with_catalog_pdf() -> list[tuple[int, pathlib.Path]]:
    return attachment_runtime.iter_tenants_with_catalog_pdf(TENANTS_ROOT)


def _warmup_single_tg_fast_pdf(tenant_id: int, catalog_pdf: pathlib.Path) -> bool:
    return attachment_runtime.warmup_single_tg_fast_pdf(
        tenant_id,
        catalog_pdf,
        deps=_attachment_deps(),
        log_fn=log,
    )


async def _warmup_tg_fast_pdf_cache_once() -> None:
    await attachment_runtime.warmup_tg_fast_pdf_cache_once(
        enabled=TG_PDF_FAST_ENABLED,
        warmup_enabled=TG_PDF_FAST_WARMUP_ENABLED,
        delay_seconds=TG_PDF_FAST_WARMUP_DELAY_SECONDS,
        tenants_root=TENANTS_ROOT,
        deps=_attachment_deps(),
        log_fn=log,
        sleep_fn=asyncio.sleep,
        to_thread_fn=asyncio.to_thread,
    )


def _coerce_int(value: Any) -> Optional[int]:
    return _coerce_int_shared(value)


def _is_whatsapp_group(identifier: Any) -> bool:
    """Return True if the sender belongs to a WhatsApp group chat."""

    if identifier is None:
        return False
    try:
        normalized = str(identifier).strip().lower()
    except Exception:
        return False
    return normalized.endswith("@g.us")


OUTBOX_WHITELIST = get_outbox_whitelist()

RECENT_INCOMING_TTL_SECONDS = 24 * 60 * 60


def _is_status_echo(item: Mapping[str, Any]) -> bool:
    return queue_contract.is_status_echo_payload(item)


async def _whitelist_allows(
    *,
    telegram_user_id: Optional[int],
    username: Optional[str],
    raw_to: Any,
    lead_id: Optional[int],
    tenant_id: Optional[int],
    channel: str,
) -> tuple[bool, str]:
    return await outbox_whitelist.whitelist_allows(
        telegram_user_id=telegram_user_id,
        username=username,
        raw_to=raw_to,
        lead_id=lead_id,
        tenant_id=tenant_id,
        channel=channel,
        deps=outbox_whitelist.OutboxWhitelistDeps(
            whitelist=OUTBOX_WHITELIST,
            recent_incoming_ttl_seconds=RECENT_INCOMING_TTL_SECONDS,
            normalize_e164_digits_fn=normalize_e164_digits,
            coerce_int_fn=_coerce_int,
            has_recent_incoming_message_fn=has_recent_incoming_message,
            db_error_labels_fn=DB_ERRORS_COUNTER.labels,
            log_fn=log,
        ),
    )


def _resolve_channel(item: Mapping[str, Any]) -> str:
    return outbox_payloads.resolve_outbox_channel(item)


def _is_manager_message(item: Mapping[str, Any]) -> bool:
    return outbox_payloads.is_manager_message(item)


def _is_followup_message(item: Mapping[str, Any]) -> bool:
    return outbox_payloads.is_followup_message(item)


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    parsed = urlparse(cleaned)
    if parsed.scheme:
        return cleaned
    if APP_BASE_URL:
        base = f"{APP_BASE_URL}/"
        return urljoin(base, cleaned.lstrip("/"))
    return cleaned


def _normalize_attachment(blob: dict[str, Any]) -> Optional[dict[str, Any]]:
    prepared = dict(blob)
    prepared["url"] = _normalize_url(str(blob.get("url") or ""))
    return normalize_message_attachment(prepared)


def _normalize_attachments(blobs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for blob in blobs:
        item = _normalize_attachment(blob)
        if item:
            prepared.append(item)
    return normalize_message_attachments(prepared)


def _has_photo_attachment(blobs: Iterable[Mapping[str, Any]] | None) -> bool:
    return attachment_runtime.has_photo_attachment(
        blobs,
        normalize_attachments_fn=normalize_message_attachments,
        normalize_attachment_fn=normalize_message_attachment,
    )


def _internal_base_url() -> str:
    return "http://app:8000"


def _is_internal_path(value: str) -> bool:
    return attachment_runtime.is_internal_path(value)


def _inject_internal_token(query: str) -> str:
    return attachment_runtime.inject_internal_token(query, token_value=WA_INTERNAL_TOKEN)


def _ensure_inline_hint(url: str) -> str:
    return attachment_runtime.ensure_inline_hint(url)


def _normalize_internal_urls(relative_url: str) -> tuple[str, str]:
    return attachment_runtime.normalize_internal_urls(
        relative_url,
        token_value=WA_INTERNAL_TOKEN,
        internal_base_url=_internal_base_url(),
    )


def _parse_disposition_filename(header: str | None) -> str:
    return attachment_runtime.parse_disposition_filename(header)


def _resolve_attachment_filename(
    attachment: Mapping[str, Any],
    headers: Mapping[str, str] | None,
    absolute_url: str,
) -> str:
    return attachment_runtime.resolve_attachment_filename(attachment, headers, absolute_url)


def _resolve_attachment_mime(
    attachment: Mapping[str, Any], headers: Mapping[str, str] | None
) -> str:
    return attachment_runtime.resolve_attachment_mime(attachment, headers)


async def _download_internal_attachment(
    relative_url: str,
) -> tuple[bytes | None, Mapping[str, str] | None, str]:
    return await attachment_runtime.download_internal_attachment(
        relative_url,
        token_value=WA_INTERNAL_TOKEN,
        normalize_internal_urls_fn=_normalize_internal_urls,
        log_fn=log,
    )


def _prepare_whatsapp_attachment_url(url: str) -> str:
    return attachment_runtime.prepare_whatsapp_attachment_url(
        url,
        deps=attachment_runtime.AttachmentRuntimeDeps(
            tg_pdf_fast_enabled=TG_PDF_FAST_ENABLED,
            tg_pdf_fast_min_mb=TG_PDF_FAST_MIN_MB,
            tg_pdf_fast_target_mb=TG_PDF_FAST_TARGET_MB,
            tg_pdf_fast_suffix=TG_PDF_FAST_SUFFIX,
            is_internal_path_fn=_is_internal_path,
            normalize_internal_urls_fn=_normalize_internal_urls,
            download_internal_attachment_fn=_download_internal_attachment,
            resolve_attachment_filename_fn=_resolve_attachment_filename,
            resolve_attachment_mime_fn=_resolve_attachment_mime,
            compress_pdf_bytes_fn=_compress_pdf_bytes,
        ),
    )


def _tokenize_attachment_mapping(attachment: Mapping[str, Any]) -> dict[str, Any]:
    return attachment_runtime.tokenize_attachment_mapping(
        attachment,
        deps=attachment_runtime.AttachmentRuntimeDeps(
            tg_pdf_fast_enabled=TG_PDF_FAST_ENABLED,
            tg_pdf_fast_min_mb=TG_PDF_FAST_MIN_MB,
            tg_pdf_fast_target_mb=TG_PDF_FAST_TARGET_MB,
            tg_pdf_fast_suffix=TG_PDF_FAST_SUFFIX,
            is_internal_path_fn=_is_internal_path,
            normalize_internal_urls_fn=_normalize_internal_urls,
            download_internal_attachment_fn=_download_internal_attachment,
            resolve_attachment_filename_fn=_resolve_attachment_filename,
            resolve_attachment_mime_fn=_resolve_attachment_mime,
            compress_pdf_bytes_fn=_compress_pdf_bytes,
        ),
    )


async def _prepare_internal_attachment(attachment: Mapping[str, Any]) -> dict[str, Any]:
    return await attachment_runtime.prepare_internal_attachment(
        attachment,
        deps=attachment_runtime.AttachmentRuntimeDeps(
            tg_pdf_fast_enabled=TG_PDF_FAST_ENABLED,
            tg_pdf_fast_min_mb=TG_PDF_FAST_MIN_MB,
            tg_pdf_fast_target_mb=TG_PDF_FAST_TARGET_MB,
            tg_pdf_fast_suffix=TG_PDF_FAST_SUFFIX,
            is_internal_path_fn=_is_internal_path,
            normalize_internal_urls_fn=_normalize_internal_urls,
            download_internal_attachment_fn=_download_internal_attachment,
            resolve_attachment_filename_fn=_resolve_attachment_filename,
            resolve_attachment_mime_fn=_resolve_attachment_mime,
            compress_pdf_bytes_fn=_compress_pdf_bytes,
        ),
    )


def _build_wa_document_payload(
    attachment: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    return attachment_runtime.build_wa_document_payload(attachment)


async def _handle_telegram_incoming(event: Mapping[str, Any]) -> None:
    await telegram_incoming_runtime.handle_telegram_incoming(
        event,
        deps=telegram_incoming_runtime.TelegramIncomingDeps(
            redis_client=r,
            outbox_queue_key=OUTBOX_QUEUE_KEY,
            notify_bot_id=NOTIFY_BOT_ID,
            log_fn=log,
            normalize_tg_slot_fn=_normalize_tg_slot,
            coerce_int_fn=_coerce_int,
            is_duplicate_telegram_incoming_fn=_is_duplicate_telegram_incoming,
            find_lead_by_telegram_fn=find_lead_by_telegram,
            normalize_username_fn=normalize_username,
            upsert_lead_fn=upsert_lead,
            store_lead_tg_slot_fn=_store_lead_tg_slot,
            telegram_slot_is_enabled_fn=_telegram_slot_is_enabled,
            looks_like_manager_outgoing_fn=_looks_like_manager_outgoing,
            is_manager_message_fn=_is_manager_message,
            handle_followup_opt_out_fn=followups.handle_opt_out,
            capture_followup_answer_fn=followups.capture_followup_answer,
            maybe_amocrm_inbound_fn=_maybe_amocrm_inbound,
            schedule_followups_fn=followups.schedule_followups,
            get_contact_id_by_lead_fn=get_contact_id_by_lead,
            get_contact_id_by_phone_fn=get_contact_id_by_phone,
            resolve_or_create_contact_fn=resolve_or_create_contact,
            link_lead_contact_fn=link_lead_contact,
            update_contact_telegram_fn=update_contact_telegram,
            update_contact_phone_fn=update_contact_phone,
            match_behavior_trigger_fn=_match_behavior_trigger,
            mark_handoff_silence_fn=_mark_handoff_silence,
            cancel_pending_smart_reply_fn=_cancel_pending_smart_reply,
            photo_expectation_config_fn=_photo_expectation_config,
            notify_manager_handoff_fn=_notify_manager_handoff,
            is_handoff_silenced_fn=_is_handoff_silenced,
            read_tenant_config_fn=read_tenant_config,
            get_contact_phone_by_lead_fn=get_contact_phone_by_lead,
            telegram_reply_enabled_fn=_telegram_reply_enabled,
            smart_reply_enabled_fn=smart_reply_enabled,
            try_handle_smart_reply_with_delay_fn=_try_handle_smart_reply_with_delay,
            produce_and_enqueue_smart_reply_fn=_produce_and_enqueue_smart_reply,
            catalog_flow_service=catalog_flow_service,
            inc_db_error_fn=lambda label: DB_ERRORS_COUNTER.labels(label).inc(),
        ),
    )


async def _handle_max_incoming(event: Mapping[str, Any]) -> None:
    await max_incoming_runtime.handle_max_incoming(
        event,
        deps=max_incoming_runtime.MaxIncomingDeps(
            redis_client=r,
            log_fn=log,
            coerce_int_fn=_coerce_int,
            normalize_max_human_name_fn=_normalize_max_human_name,
            get_or_create_by_peer_fn=get_or_create_by_peer,
            upsert_lead_fn=upsert_lead,
            looks_like_manager_outgoing_fn=_looks_like_manager_outgoing,
            is_manager_message_fn=_is_manager_message,
            handle_followup_opt_out_fn=followups.handle_opt_out,
            capture_followup_answer_fn=followups.capture_followup_answer,
            maybe_amocrm_inbound_fn=_maybe_amocrm_inbound,
            schedule_followups_fn=followups.schedule_followups,
            resolve_or_create_contact_fn=resolve_or_create_contact,
            link_lead_contact_fn=link_lead_contact,
            update_contact_max_fn=update_contact_max,
            insert_message_in_fn=insert_message_in,
            match_behavior_trigger_fn=_match_behavior_trigger,
            mark_handoff_silence_fn=_mark_handoff_silence,
            cancel_pending_smart_reply_fn=_cancel_pending_smart_reply,
            photo_expectation_config_fn=_photo_expectation_config,
            notify_manager_handoff_fn=_notify_manager_handoff,
            is_handoff_silenced_fn=_is_handoff_silenced,
            read_tenant_config_fn=read_tenant_config,
            max_reply_enabled_fn=_max_reply_enabled,
            max_personal_reply_enabled_fn=_max_personal_reply_enabled,
            smart_reply_enabled_fn=smart_reply_enabled,
            try_handle_smart_reply_with_delay_fn=_try_handle_smart_reply_with_delay,
            produce_and_enqueue_smart_reply_fn=_produce_and_enqueue_smart_reply,
            is_recent_max_bot_echo_fn=_is_recent_max_bot_echo,
            catalog_flow_service=catalog_flow_service,
            outbox_queue_key=OUTBOX_QUEUE_KEY,
            inc_db_error_fn=lambda label: DB_ERRORS_COUNTER.labels(label).inc(),
        ),
    )


async def _handle_max_personal_incoming(event: Mapping[str, Any]) -> None:
    normalized = dict(event)
    normalized.setdefault("channel", "max_personal")
    normalized.setdefault("ch", "max_personal")
    normalized.setdefault("provider", "max_personal")
    await _handle_max_incoming(normalized)


async def _handle_whatsapp_incoming(event: Mapping[str, Any]) -> None:
    await whatsapp_incoming_runtime.handle_whatsapp_incoming(
        event,
        deps=whatsapp_incoming_runtime.WhatsAppIncomingDeps(
            default_tenant_id=int(os.getenv("TENANT_ID", "1")),
            log_fn=log,
            coerce_int_fn=_coerce_int,
            is_whatsapp_group_fn=_is_whatsapp_group,
            digits_fn=_digits,
            get_or_create_by_peer_fn=get_or_create_by_peer,
            handle_followup_opt_out_fn=followups.handle_opt_out,
            capture_followup_answer_fn=followups.capture_followup_answer,
            schedule_followups_fn=followups.schedule_followups,
            cancel_pending_smart_reply_fn=_cancel_pending_smart_reply,
            resolve_or_create_contact_fn=resolve_or_create_contact,
            link_lead_contact_fn=link_lead_contact,
            insert_message_in_fn=insert_message_in,
            maybe_amocrm_inbound_fn=_maybe_amocrm_inbound,
            match_behavior_trigger_fn=_match_behavior_trigger,
            mark_handoff_silence_fn=_mark_handoff_silence,
            is_handoff_silenced_fn=_is_handoff_silenced,
            smart_reply_enabled_fn=smart_reply_enabled,
            try_handle_smart_reply_with_delay_fn=_try_handle_smart_reply_with_delay,
            produce_and_enqueue_smart_reply_fn=_produce_and_enqueue_smart_reply,
            inc_db_error_fn=lambda label: DB_ERRORS_COUNTER.labels(label).inc(),
        ),
    )


async def _handle_avito_incoming(event: Mapping[str, Any]) -> None:
    await avito_incoming_runtime.handle_avito_incoming(
        event,
        deps=avito_incoming_runtime.AvitoIncomingDeps(
            avito_chat_cache=AVITO_CHAT_CACHE,
            redis_client=r,
            phone_tg_ttl_seconds=AVITO_PHONE_TG_TTL_SECONDS,
            auto_reply_ttl_seconds=AVITO_AUTO_REPLY_TTL_SECONDS,
            testing_mode=(os.getenv("TESTING") or "").strip() == "1",
            log_fn=log,
            coerce_int_fn=_coerce_int,
            extract_ru_phone_fn=_extract_ru_phone,
            extract_tg_username_fn=_extract_tg_username,
            avito_phone_tg_template_fn=_avito_phone_tg_template,
            avito_auto_reply_text_fn=_avito_auto_reply_text,
            resolve_avito_user_name_fn=_resolve_avito_user_name,
            get_or_create_by_peer_fn=get_or_create_by_peer,
            lead_exists_fn=lead_exists,
            upsert_lead_fn=upsert_lead,
            handle_followup_opt_out_fn=followups.handle_opt_out,
            capture_followup_answer_fn=followups.capture_followup_answer,
            schedule_followups_fn=followups.schedule_followups,
            cancel_pending_smart_reply_fn=_cancel_pending_smart_reply,
            resolve_or_create_contact_fn=resolve_or_create_contact,
            update_contact_phone_fn=update_contact_phone,
            update_contact_avito_login_fn=update_contact_avito_login,
            link_lead_contact_fn=link_lead_contact,
            insert_message_in_fn=insert_message_in,
            maybe_amocrm_inbound_fn=_maybe_amocrm_inbound,
            match_behavior_trigger_fn=_match_behavior_trigger,
            mark_handoff_silence_fn=_mark_handoff_silence,
            send_telegram_to_phone_fn=_send_telegram_to_phone,
            send_telegram_to_username_fn=_send_telegram_to_username,
            enqueue_avito_auto_reply_fn=_enqueue_avito_auto_reply,
            is_handoff_silenced_fn=_is_handoff_silenced,
            avito_smart_reply_enabled_fn=_avito_smart_reply_enabled,
            smart_reply_enabled_fn=smart_reply_enabled,
            try_handle_smart_reply_with_delay_fn=_try_handle_smart_reply_with_delay,
            produce_and_enqueue_smart_reply_fn=_produce_and_enqueue_smart_reply,
            inc_db_error_fn=lambda label: DB_ERRORS_COUNTER.labels(label).inc(),
            resolve_avito_item_city_fn=avito_item_city.resolve_and_store_avito_item_city,
            resolve_avito_contact_identity_fn=lambda **kw: avito_contact_identity_runtime.resolve_avito_contact_identity(redis_client=r, update_contact_avito_login_fn=update_contact_avito_login, log_fn=log, **kw),
        ),
    )


_INCOMING_EVENT_HANDLERS: dict[str, Callable[[Mapping[str, Any]], Awaitable[None]]] = {
    "telegram": _handle_telegram_incoming,
    "whatsapp": _handle_whatsapp_incoming,
    "avito": _handle_avito_incoming,
    "max": _handle_max_incoming,
    "max_personal": _handle_max_personal_incoming,
}


async def _handle_incoming_event(event: Mapping[str, Any]) -> None:
    await incoming_dispatcher.handle_incoming_event(
        event,
        deps=incoming_dispatcher.IncomingDispatcherDeps(
            handlers=_INCOMING_EVENT_HANDLERS,
            log_fn=log,
        ),
    )


def _http_json(
    method: str,
    url: str,
    data: dict | None = None,
    timeout: float = 10.0,
    headers: Dict[str, str] | None = None,
) -> tuple[int, str]:
    body: bytes | None = None
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json; charset=utf-8")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            txt = raw.decode("utf-8", errors="ignore")
            return resp.status, txt
    except urllib.error.HTTPError as e:
        raw = e.read()
        txt = raw.decode("utf-8", errors="ignore") if raw else ""
        return e.code, txt
    except Exception as e:
        return 0, str(e)


def _download_file(
    url: str,
    *,
    timeout: float = 15.0,
    max_size: int = 20 * 1024 * 1024,
) -> tuple[bytes | None, str | None, str | None]:
    return attachment_runtime.download_file(
        url,
        timeout=timeout,
        max_size=max_size,
        tgworker_url=os.getenv("TGWORKER_URL", "http://tgworker:8000"),
        admin_token=str(getattr(core_settings, "ADMIN_TOKEN", "") or os.getenv("ADMIN_TOKEN", "")),
    )


async def send_whatsapp(
    tenant_id: int,
    phone: str,
    text: str | None = None,
    attachment: Mapping[str, Any] | None = None,
    attachments: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[int, str]:
    return await whatsapp_outbound_runtime.send_whatsapp(
        tenant_id,
        phone,
        text=text,
        attachment=attachment,
        attachments=attachments,
        deps=whatsapp_outbound_runtime.WhatsAppOutboundDeps(
            log_fn=log,
            waweb_base_url_fn=_waweb_base_url,
            wabaileys_base_url_fn=_wabaileys_base_url,
            normalize_whatsapp_recipient_fn=normalize_whatsapp_recipient,
            whatsapp_address_error=WhatsAppAddressError,
            digits_fn=_digits,
            tokenize_attachment_mapping_fn=_tokenize_attachment_mapping,
            build_wa_document_payload_fn=_build_wa_document_payload,
            http_json_fn=_http_json,
            sleep_fn=asyncio.sleep,
            asyncio_to_thread_fn=asyncio.to_thread,
            json_module=json,
            wa_send_base_timeout=WA_SEND_BASE_TIMEOUT,
            wa_send_timeout_per_mib=WA_SEND_TIMEOUT_PER_MIB,
            wa_send_timeout_max=WA_SEND_TIMEOUT_MAX,
            wa_internal_token=WA_INTERNAL_TOKEN,
            admin_token=ADMIN_TOKEN,
            core_settings_module=core_settings,
        ),
    )


async def send_whatsapp_baileys(
    tenant_id: int,
    phone: str,
    text: str | None = None,
    attachments: Iterable[Mapping[str, Any]] | None = None,
    meta: Mapping[str, Any] | None = None,
) -> tuple[int, str]:
    return await whatsapp_outbound_runtime.send_whatsapp_baileys(
        tenant_id,
        phone,
        text=text,
        attachments=attachments,
        meta=meta,
        deps=whatsapp_outbound_runtime.WhatsAppOutboundDeps(
            log_fn=log,
            waweb_base_url_fn=_waweb_base_url,
            wabaileys_base_url_fn=_wabaileys_base_url,
            normalize_whatsapp_recipient_fn=normalize_whatsapp_recipient,
            whatsapp_address_error=WhatsAppAddressError,
            digits_fn=_digits,
            tokenize_attachment_mapping_fn=_tokenize_attachment_mapping,
            build_wa_document_payload_fn=_build_wa_document_payload,
            http_json_fn=_http_json,
            sleep_fn=asyncio.sleep,
            asyncio_to_thread_fn=asyncio.to_thread,
            json_module=json,
            wa_send_base_timeout=WA_SEND_BASE_TIMEOUT,
            wa_send_timeout_per_mib=WA_SEND_TIMEOUT_PER_MIB,
            wa_send_timeout_max=WA_SEND_TIMEOUT_MAX,
            wa_internal_token=WA_INTERNAL_TOKEN,
            admin_token=ADMIN_TOKEN,
            core_settings_module=core_settings,
        ),
    )


async def send_avito(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: Optional[str] = None,
    account_id: Optional[int] = None,
    attachments: list[dict[str, Any]] | None = None,
) -> tuple[int, str]:
    return await avito_outbound_runtime.send_avito(
        tenant_id,
        lead_id,
        text,
        chat_id=chat_id,
        account_id=account_id,
        attachments=attachments,
        deps=avito_outbound_runtime.AvitoOutboundDeps(
            avito_timeout=AVITO_TIMEOUT,
            avito_image_max_bytes=AVITO_IMAGE_MAX_BYTES,
            avito_file_max_bytes=AVITO_FILE_MAX_BYTES,
            log_fn=log,
            prepare_tg_attachments_for_send_fn=_prepare_tg_attachments_for_send,
            avito_integration_module=avito_integration,
            coerce_int_fn=_coerce_int,
            get_lead_peer_fn=get_lead_peer,
            tenant_dir_fn=tenant_dir,
            message_out_counter=MESSAGE_OUT_COUNTER,
            avito_chat_cache=AVITO_CHAT_CACHE,
            httpx_module=httpx,
        ),
    )


async def send_max(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: str | int | None = None,
    user_id: str | int | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> tuple[int, str]:
    return await max_outbound_runtime.send_max(
        tenant_id,
        lead_id,
        text,
        chat_id=chat_id,
        user_id=user_id,
        attachments=attachments,
        deps=max_outbound_runtime.MaxOutboundDeps(
            log_fn=log,
            prepare_tg_attachments_for_send_fn=_prepare_tg_attachments_for_send,
            get_lead_peer_fn=get_lead_peer,
            tenant_dir_fn=tenant_dir,
            is_internal_path_fn=_is_internal_path,
            download_internal_attachment_fn=_download_internal_attachment,
            resolve_attachment_filename_fn=_resolve_attachment_filename,
            resolve_attachment_mime_fn=_resolve_attachment_mime,
            download_file_fn=_download_file,
            max_integration_module=max_integration,
            max_personal_service_module=max_personal_service,
            max_personal_transport_module=max_personal_transport,
            message_out_counter=MESSAGE_OUT_COUNTER,
        ),
    )


async def send_max_personal(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: str | int | None = None,
    user_id: str | int | None = None,
    attachments: list[dict[str, Any]] | None = None,
    message_id: str | None = None,
) -> tuple[int, str]:
    return await max_outbound_runtime.send_max_personal(
        tenant_id,
        lead_id,
        text,
        chat_id=chat_id,
        user_id=user_id,
        attachments=attachments,
        message_id=message_id,
        deps=max_outbound_runtime.MaxOutboundDeps(
            log_fn=log,
            prepare_tg_attachments_for_send_fn=_prepare_tg_attachments_for_send,
            get_lead_peer_fn=get_lead_peer,
            tenant_dir_fn=tenant_dir,
            is_internal_path_fn=_is_internal_path,
            download_internal_attachment_fn=_download_internal_attachment,
            resolve_attachment_filename_fn=_resolve_attachment_filename,
            resolve_attachment_mime_fn=_resolve_attachment_mime,
            download_file_fn=_download_file,
            max_integration_module=max_integration,
            max_personal_service_module=max_personal_service,
            max_personal_transport_module=max_personal_transport,
            message_out_counter=MESSAGE_OUT_COUNTER,
        ),
    )


async def _fetch_authorized_status(tenant_id: int) -> Optional[bool]:
    try:
        status_url = f"{TGWORKER_STATUS_URL}?tenant={tenant_id}"
        code, body = await asyncio.to_thread(_http_json, "GET", status_url, None, 8.0, None)
    except Exception as exc:  # pragma: no cover - defensive
        log(f"[worker] status_check err: {exc}")
        return None
    if not (200 <= code < 300):
        log(f"[worker] status_check code={code} body={body[:160]}")
        return None
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return None
    return bool(data.get("authorized"))


async def _wait_until_authorized(tenant_id: int, attempts: int = 3) -> bool:
    for attempt in range(attempts):
        authorized = await _fetch_authorized_status(tenant_id)
        if authorized:
            return True
        await asyncio.sleep(min(2**attempt, 8.0))
    return False


async def send_telegram(
    tenant_id: int,
    *,
    tg_slot: int = TG_SLOT_MIN,
    chat_id: int,
    peer_id: int | None,
    peer: str | None,
    telegram_user_id: int | None,
    username: str | None,
    text: str | None,
    attachments: list[dict[str, Any]] | None = None,
    reply_to: str | None = None,
    lead_id: int | None = None,
) -> tuple[int, str]:
    return await telegram_outbound_runtime.send_telegram(
        tenant_id,
        tg_slot=tg_slot,
        chat_id=chat_id,
        peer_id=peer_id,
        peer=peer,
        telegram_user_id=telegram_user_id,
        username=username,
        text=text,
        attachments=attachments,
        reply_to=reply_to,
        lead_id=lead_id,
        deps=telegram_outbound_runtime.TelegramOutboundDeps(
            tg_slot_min=TG_SLOT_MIN,
            notify_bot_id=NOTIFY_BOT_ID,
            tg_worker_token=TG_WORKER_TOKEN,
            admin_token=ADMIN_TOKEN,
            log_fn=log,
            normalize_tg_slot_fn=_normalize_tg_slot,
            virtual_tg_tenant_fn=_virtual_tg_tenant,
            normalize_attachments_fn=_normalize_attachments,
            prepare_tg_attachments_for_send_fn=_prepare_tg_attachments_for_send,
            wait_until_authorized_fn=_wait_until_authorized,
            telegram_transport_module=telegram_transport,
            message_out_counter=MESSAGE_OUT_COUNTER,
            sleep_fn=asyncio.sleep,
        ),
    )


# ==== Core send ====
async def do_send(item: dict) -> tuple[str, str, str, int]:
    return await outbox_send_runtime.do_send(
        item,
        deps=outbox_send_runtime.OutboxSendDeps(
            default_tenant_id=int(os.getenv("TENANT_ID", "1")),
            outbox_enabled=OUTBOX_ENABLED,
            outbox_enabled_raw=_OUTBOX_ENABLED_RAW,
            send_enabled=SEND,
            redis_client=r,
            json_module=json,
            log_fn=log,
            db_errors_counter=DB_ERRORS_COUNTER,
            outbox_payloads_module=outbox_payloads,
            queue_contract_module=queue_contract,
            outbox_queue_key=OUTBOX_QUEUE_KEY,
            outbox_dlq_key=OUTBOX_DLQ_KEY,
            normalize_tg_slot_fn=_normalize_tg_slot,
            whitelist_allows_fn=_whitelist_allows,
            lead_exists_fn=lead_exists,
            coerce_int_fn=_coerce_int,
            get_lead_peer_fn=get_lead_peer,
            get_telegram_user_id_by_lead_fn=get_telegram_user_id_by_lead,
            find_lead_by_telegram_fn=find_lead_by_telegram,
            normalize_username_fn=normalize_username,
            upsert_lead_fn=upsert_lead,
            get_lead_tg_slot_fn=_get_lead_tg_slot,
            telegram_slot_is_enabled_fn=_telegram_slot_is_enabled,
            is_manager_message_fn=_is_manager_message,
            is_followup_message_fn=_is_followup_message,
            mark_handoff_silence_fn=_mark_handoff_silence,
            collect_outgoing_attachments_fn=_collect_outgoing_attachments,
            insert_message_out_fn=insert_message_out,
            prepare_internal_attachment_fn=_prepare_internal_attachment,
            tenant_whatsapp_provider_fn=tenant_whatsapp_provider,
            send_whatsapp_baileys_fn=send_whatsapp_baileys,
            send_whatsapp_fn=send_whatsapp,
            avito_bot_echo_key_fn=avito_bot_echo_key,
            avito_bot_echo_ttl_seconds=AVITO_BOT_ECHO_TTL_SECONDS,
            send_avito_fn=send_avito,
            send_telegram_fn=send_telegram,
            send_max_fn=send_max,
            send_max_personal_fn=send_max_personal,
            cache_max_bot_echo_fn=_cache_max_bot_echo,
            update_message_status_fn=update_message_status,
            amocrm_service_module=amocrm_service,
        ),
    )


# ==== Writer ====
async def write_result(item: dict, status: str, status_code: int, reason: str):
    await outbox_writer.write_result(
        item,
        status,
        status_code,
        reason,
        deps=outbox_writer.OutboxWriterDeps(
            redis_client=r,
            outbox_queue_key=OUTBOX_QUEUE_KEY,
            app_version=APP_VERSION,
            default_tenant_id=int(os.getenv("TENANT_ID", "1")),
            log_fn=log,
            collect_outgoing_attachments_fn=_collect_outgoing_attachments,
            is_manager_message_fn=_is_manager_message,
            is_followup_message_fn=_is_followup_message,
            db_error_labels_fn=DB_ERRORS_COUNTER.labels,
        ),
    )


# Notification dispatcher
async def _process_notification(item: Mapping[str, Any]) -> None:
    await notification_dispatcher.process_notification(
        item,
        deps=notification_dispatcher.NotificationDispatcherDeps(
            default_tenant_id=int(os.getenv("TENANT_ID", "1")),
            admin_token=ADMIN_TOKEN,
            notify_bot_enabled=bool(NOTIFY_BOT_TOKEN),
            log_fn=log,
            notification_chat_ids_fn=notification_chat_ids,
            send_notify_bot_fn=_send_notify_bot,
        ),
    )


# Debug helper: log when notify type payload is seen in queue.


# ==== Loop ====
async def process_incoming_queue() -> None:
    await queue_loops.process_incoming_queue(
        queue_loops.IncomingLoopDeps(
            redis_client=r,
            queue_key=INCOMING_QUEUE_KEY,
            block_timeout=INBOX_BLOCK_TIMEOUT,
            enabled=bool(INBOX_ENABLED),
            log_fn=log,
            handle_incoming_event_fn=_handle_incoming_event,
        )
    )


async def process_queue():
    await queue_loops.process_outbox_queue(
        queue_loops.OutboxLoopDeps(
            redis_client=r,
            queue_keys=QUEUES,
            outbox_queue_key=OUTBOX_QUEUE_KEY,
            outbox_dlq_key=OUTBOX_DLQ_KEY,
            enabled=bool(OUTBOX_ENABLED),
            default_tenant_id=int(os.getenv("TENANT_ID", "1")),
            log_fn=log,
            process_notification_fn=_process_notification,
            resolve_channel_fn=_resolve_channel,
            is_status_echo_fn=_is_status_echo,
            parse_send_not_before_ts_fn=_parse_send_not_before_ts,
            coerce_int_fn=_coerce_int,
            do_send_fn=do_send,
            write_result_fn=write_result,
        )
    )


def _amocrm_backoff_seconds(attempts: int) -> int:
    return amocrm_outbox_runtime.amocrm_backoff_seconds(attempts)


def _parse_amocrm_payload(raw: Any) -> dict[str, Any]:
    return amocrm_outbox_runtime.parse_amocrm_payload(raw)


def _amocrm_stage_id_from_cfg(amocrm_cfg: Mapping[str, Any] | None, stage_index: int) -> int | None:
    return amocrm_outbox_runtime.amocrm_stage_id_from_cfg(amocrm_cfg, stage_index)


def _is_amocrm_lead_not_found_error(exc: Exception) -> bool:
    return amocrm_outbox_runtime.is_amocrm_lead_not_found_error(exc)


async def _amocrm_entity_exists_in_worker(
    client: Any, *, entity_type: str, entity_id: int | None
) -> bool | None:
    return await amocrm_outbox_runtime.amocrm_entity_exists(
        client,
        entity_type=entity_type,
        entity_id=entity_id,
    )


async def _recover_amocrm_missing_lead(
    *,
    tenant_id: int,
    lead_id: int,
    payload: Mapping[str, Any],
    amocrm_cfg: Mapping[str, Any],
    client: Any,
    link: Mapping[str, Any] | None,
) -> int | None:
    return await amocrm_outbox_runtime.recover_amocrm_missing_lead(
        tenant_id=tenant_id,
        lead_id=lead_id,
        payload=payload,
        amocrm_cfg=amocrm_cfg,
        client=client,
        link=link,
    )


async def _handle_amocrm_event(event: Mapping[str, Any]) -> None:
    await amocrm_outbox_runtime.handle_amocrm_event(
        event,
        deps=amocrm_outbox_runtime.AmoCrmOutboxDeps(
            enabled=AMOCRM_OUTBOX_ENABLED,
            outbox_limit=AMOCRM_OUTBOX_LIMIT,
            outbox_max_attempts=AMOCRM_OUTBOX_MAX_ATTEMPTS,
            log_fn=log,
            read_tenant_config_fn=read_tenant_config,
            download_file_fn=_download_file,
        ),
    )


async def process_amocrm_outbox() -> None:
    await amocrm_outbox_runtime.process_amocrm_outbox(
        deps=amocrm_outbox_runtime.AmoCrmOutboxDeps(
            enabled=AMOCRM_OUTBOX_ENABLED,
            outbox_limit=AMOCRM_OUTBOX_LIMIT,
            outbox_max_attempts=AMOCRM_OUTBOX_MAX_ATTEMPTS,
            log_fn=log,
            read_tenant_config_fn=read_tenant_config,
            download_file_fn=_download_file,
        )
    )


async def process_training_embeddings() -> None:
    await training_embeddings_runtime.process_training_embeddings_loop(
        training_embeddings_runtime.TrainingEmbeddingsDeps(
            enabled=LEARNING_EMBEDDINGS_ENABLED,
            embedding_model=EMBEDDING_MODEL,
            fetch_pending_examples_fn=fetch_pending_training_examples,
            set_training_embedding_fn=set_training_embedding,
            embed_texts_fn=training_embeddings.embed_texts,
            sleep_fn=asyncio.sleep,
            log_fn=log,
        )
    )


async def main():
    log(f"[worker] boot {APP_VERSION}")
    await init_db()
    tasks = [
        asyncio.create_task(process_queue(), name="outbox-loop"),
    ]
    tasks.append(asyncio.create_task(process_amocrm_outbox(), name="amocrm-outbox-loop"))
    if FOLLOWUPS_ENABLED:
        tasks.append(asyncio.create_task(followups.run_loop(), name="followups-loop"))
    if INBOX_ENABLED:
        tasks.append(asyncio.create_task(process_incoming_queue(), name="inbox-loop"))
    if LEARNING_EMBEDDINGS_ENABLED:
        tasks.append(
            asyncio.create_task(process_training_embeddings(), name="training-embeddings-loop")
        )
    tasks.append(asyncio.create_task(_warmup_tg_fast_pdf_cache_once(), name="tg-pdf-fast-warmup"))
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())

AVITO_CHAT_CACHE: Dict[int, str] = {}
