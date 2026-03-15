from __future__ import annotations
import os
import base64
import re
import json
import time
import asyncio
import random
import heapq
import itertools
import cgi
import mimetypes
from datetime import datetime, timedelta, timezone
import urllib.request
import urllib.error
import tempfile
import subprocess
import shutil
import urllib.parse
import logging
from logging import StreamHandler
import pathlib
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, Optional
from urllib.parse import (
    urljoin,
    urlparse,
    urlsplit,
    urlunsplit,
    parse_qsl,
    urlencode,
    unquote,
    quote,
)

import httpx

import redis.asyncio as redis
from redis import exceptions as redis_ex

from libs.core import db as db_module
from libs.core.sales_core import (
    settings as core_settings,
    tenant_waweb_url,
    tenant_whatsapp_provider,
    build_llm_messages,
    ask_llm,
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
    find_lead_by_peer,
    get_telegram_user_id_by_lead,
    get_lead_peer,
    update_message_status,
    has_recent_incoming_message,
    get_contact_id_by_lead,
    get_contact_id_by_phone,
    get_contact_phone_by_lead,
    get_lead_dialog_metadata,
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
    detect_message_kind,
    normalize_attachments as normalize_message_attachments,
    normalize_attachment as normalize_message_attachment,
    sanitize_display_name,
    text_or_placeholder,
)
from libs.core.common import (
    OUTBOX_QUEUE_KEY,
    OUTBOX_DLQ_KEY,
    get_outbox_whitelist,
    normalize_username,
    normalize_echo_text,
    smart_reply_enabled,
    notification_chat_ids,
    notification_event_enabled,
    whitelist_contains_number,
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
from libs.core.services import amocrm as amocrm_service
from libs.core.services import amocrm_chat as amocrm_chat_service
from libs.core.repo import crm_chat_links, crm_links, crm_outbox
from libs.core.transport import (
    WhatsAppAddressError,
    normalize_e164_digits,
    normalize_whatsapp_recipient,
)
from libs.core.transport import telegram as telegram_transport
from libs.core.training import embeddings as training_embeddings
from apps.api.web.common import WA_INTERNAL_TOKEN as COMMON_WA_INTERNAL_TOKEN
from apps.worker import followups
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
REDIS_URL  = os.getenv("REDIS_URL", "redis://redis:6379/0")
# TTL for deduplicating Telegram outreach triggered by Avito phone detection.
AVITO_PHONE_TG_TTL_SECONDS = int(os.getenv("AVITO_PHONE_TG_TTL", "86400"))
# Allow disabling phone→tg deduplication (for diagnostics/edge cases).
AVITO_PHONE_TG_DEDUP_ENABLED = (os.getenv("AVITO_PHONE_TG_DEDUP_ENABLED") or "").strip().lower() in {"1", "true", "yes"}
# TTL for suppressing repeated Avito auto-replies in the same chat/lead.
AVITO_AUTO_REPLY_TTL_SECONDS = int(os.getenv("AVITO_AUTO_REPLY_TTL", "86400"))
TG_SLOT_MIN = 1
TG_SLOT_MAX = 5
TG_SLOT_MULTIPLIER = 1000
# Match waweb INTERNAL_SYNC_TOKEN resolution (shared with the web layer)
WA_INTERNAL_TOKEN = COMMON_WA_INTERNAL_TOKEN
_DEFAULT_WORKER_BASE = getattr(core_settings, "DEFAULT_WORKER_BASE_URL", "http://worker:8000")
AMOCRM_OUTBOX_ENABLED = (os.getenv("AMOCRM_OUTBOX_ENABLED") or "").strip().lower() not in {"0", "false", "no"}
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
    os.getenv("APP_BASE_URL")
    or os.getenv("APP_INTERNAL_URL")
    or os.getenv("APP_URL")
    or ""
).strip().rstrip("/")
TG_WORKER_TOKEN = (os.getenv("TG_WORKER_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
SEND       = (os.getenv("SEND_ENABLED","true").lower() == "true")
TGWORKER_STATUS_URL = f"{TGWORKER_BASE_URL}/status"
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()
_OUTBOX_ENABLED_RAW = (os.getenv("OUTBOX_ENABLED") or "").strip().lower()
OUTBOX_ENABLED = _OUTBOX_ENABLED_RAW not in {"0", "false"}
LEARNING_EMBEDDINGS_ENABLED = (os.getenv("LEARNING_EMBEDDINGS_ENABLED") or "1").strip().lower() not in {
    "",
    "0",
    "false",
    "no",
    "off",
}
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL") or getattr(core_settings, "EMBEDDING_MODEL", "") or "text-embedding-3-small"
AVITO_TIMEOUT = getattr(core_settings, "AVITO_TIMEOUT", 10.0)
AVITO_IMAGE_MAX_BYTES = 24 * 1024 * 1024
AVITO_FILE_MAX_BYTES = 100 * 1024 * 1024
_INBOX_ENABLED_RAW = (os.getenv("INBOX_ENABLED") or "").strip().lower()
INBOX_ENABLED = _INBOX_ENABLED_RAW not in {"", "0", "false", "no", "off"}
INCOMING_QUEUE_KEY = (
    os.getenv("INCOMING_QUEUE_KEY")
    or os.getenv("INBOX_QUEUE_KEY")
    or "inbox:message_in"
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
SMART_REPLY_PUNCT_STYLE_ENABLED = (os.getenv("SMART_REPLY_PUNCT_STYLE_ENABLED") or "1").strip().lower() in {
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
    SMART_REPLY_SPLIT_MAX_LEN = max(SMART_REPLY_SPLIT_MIN_LEN + 20, int(os.getenv("SMART_REPLY_SPLIT_MAX_LEN", "120")))
except Exception:
    SMART_REPLY_SPLIT_MAX_LEN = max(SMART_REPLY_SPLIT_MIN_LEN + 20, 120)
try:
    SMART_REPLY_SPLIT_MAX_PARTS = max(2, int(os.getenv("SMART_REPLY_SPLIT_MAX_PARTS", "6")))
except Exception:
    SMART_REPLY_SPLIT_MAX_PARTS = 6
_SMART_REPLY_SPLIT_CHANNELS_RAW = (
    os.getenv("SMART_REPLY_SPLIT_CHANNELS") or "telegram,avito,whatsapp,max"
).strip()
SMART_REPLY_SPLIT_CHANNELS = {
    part.strip().lower()
    for part in _SMART_REPLY_SPLIT_CHANNELS_RAW.split(",")
    if part.strip()
}
if not SMART_REPLY_SPLIT_CHANNELS:
    SMART_REPLY_SPLIT_CHANNELS = {"telegram", "avito", "whatsapp", "max"}
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
    SMART_REPLY_FIRST_TTL_SECONDS = max(300, int(os.getenv("SMART_REPLY_FIRST_TTL_SECONDS", "1800")))
except Exception:
    SMART_REPLY_FIRST_TTL_SECONDS = 1800
try:
    SMART_REPLY_BURST_MAX_MESSAGES = max(2, int(os.getenv("SMART_REPLY_BURST_MAX_MESSAGES", "8")))
except Exception:
    SMART_REPLY_BURST_MAX_MESSAGES = 8
_SMART_REPLY_DELAY_CHANNELS_RAW = (
    os.getenv("SMART_REPLY_DELAY_CHANNELS") or "telegram,avito,whatsapp,max"
).strip()
SMART_REPLY_DELAY_CHANNELS = {
    part.strip().lower()
    for part in _SMART_REPLY_DELAY_CHANNELS_RAW.split(",")
    if part.strip()
}
if not SMART_REPLY_DELAY_CHANNELS:
    SMART_REPLY_DELAY_CHANNELS = {"telegram", "avito", "whatsapp", "max"}
SMART_REPLY_FIRST_KEY_PREFIX = "smart_reply:first_sent"
TENANT_ID  = int(os.getenv("TENANT_ID","1"))
QUEUES = [OUTBOX_QUEUE_KEY]

_PENDING_SMART_REPLIES: Dict[str, Dict[str, Any]] = {}
_PENDING_SMART_REPLY_LOCK = asyncio.Lock()

# Outbox items with send_not_before_ts should not block the whole queue.
# Keep them in a small in-memory min-heap and release when due.
_DEFERRED_OUTBOX_HEAP: list[tuple[float, int, dict[str, Any]]] = []
_DEFERRED_OUTBOX_SEQ = itertools.count(1)

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
    if contact_phone:
        return f"Лид {contact_phone}"
    return f"Лид {lead_id}"


def _build_chat_link(username: str | None, phone: str | None, peer: str | None) -> str | None:
    """
    Build a link to chat with priority:
    1) phone (digits) -> https://t.me/+<phone> (Telegram phone search)
    2) username -> https://t.me/username
    3) peer/id -> tg://user?id=<id> (best effort)
    """
    if phone:
        digits = "".join(ch for ch in phone if ch.isdigit())
        if digits:
            return f"https://t.me/+{digits}"
    if username:
        return f"https://t.me/{username.lstrip('@')}"
    if peer:
        digits = "".join(ch for ch in peer if ch.isdigit())
        if digits:
            return f"tg://user?id={digits}"
    return None


async def _enqueue_notification_payload(payload: Mapping[str, Any]) -> None:
    try:
        await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
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
    if tenant_id <= 0 or lead_id <= 0:
        return
    if not notification_event_enabled(tenant_id, NOTIFY_EVENT_MANAGER):
        return
    chat_ids = notification_chat_ids(tenant_id, NOTIFY_EVENT_MANAGER)
    if not chat_ids:
        return
    link = _notification_link(tenant_id, lead_id)
    reason_hint = "прислал файл" if reason == "photo_received" else (reason or "требуется участие менеджера")
    lead_phone = None
    try:
        lead_phone = await get_contact_phone_by_lead(int(lead_id))
    except Exception:
        lead_phone = None
    # Prefer numeric peer for link/title when available.
    peer_hint = None
    if contact_hint:
        digits_hint = "".join(ch for ch in str(contact_hint) if ch.isdigit())
        if digits_hint:
            peer_hint = digits_hint
    chat_phone = lead_phone if isinstance(lead_phone, str) else None

    # Prefer phone as target; fallback to peer.
    chat_target = chat_phone or peer_hint
    title = _notification_lead_title(lead_id, chat_phone)
    chat_link = _build_chat_link(username_hint, chat_phone, chat_target or peer_hint)
    if chat_link:
        text = f"{title}: <a href=\"{chat_link}\">ссылка</a> - {reason_hint}"
    else:
        text = f"{title}: {reason_hint}"
    log(
        f"event=notify_prepare tenant={tenant_id} lead_id={lead_id} reason={reason_hint} chat_ids={chat_ids}"
    )
    payload = {
        "type": "notify",
        "event": NOTIFY_EVENT_MANAGER,
        "tenant": int(tenant_id),
        "tenant_id": int(tenant_id),
        "lead_id": int(lead_id),
        "chat_ids": chat_ids,
        "text": text.strip(),
    }
    # Send immediately (do not rely on queue, which is shared with outbox).
    await _process_notification(payload)

async def _mark_handoff_silence(
    tenant_id: int,
    lead_id: int,
    reason: str | None = None,
    contact_hint: str | None = None,
    username_hint: str | None = None,
    notify: bool = True,
) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    silence_key = handoff_silence_key(int(tenant_id), int(lead_id))
    meta_key = handoff_silence_meta_key(int(tenant_id), int(lead_id))
    timestamp = int(time.time())
    try:
        await r.set(
            silence_key,
            str(timestamp),
            ex=HANDOFF_SILENCE_TTL_SECONDS,
        )
        if meta_key:
            meta_payload = {"reason": reason or "unknown", "ts": timestamp}
            await r.set(
                meta_key,
                json.dumps(meta_payload, ensure_ascii=False),
                ex=HANDOFF_SILENCE_TTL_SECONDS,
            )
    except Exception:
        log(
            f"event=handoff_flag_set_failed tenant={tenant_id} lead_id={lead_id}"  # noqa: G004
        )
        return

    if notify:
        await _notify_manager_handoff(
            int(tenant_id),
            int(lead_id),
            reason,
            contact_hint=contact_hint,
            username_hint=username_hint,
        )


async def _is_handoff_silenced(tenant_id: int, lead_id: int) -> bool:
    if tenant_id <= 0 or lead_id <= 0:
        return False
    try:
        return bool(await r.exists(handoff_silence_key(int(tenant_id), int(lead_id))))
    except Exception:
        return False


def _coerce_chat_ids(raw: Any) -> list[int]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        result: list[int] = []
        for item in raw:
            try:
                val = int(item)
            except Exception:
                continue
            if val:
                result.append(val)
        return result
    try:
        candidate = int(raw)
    except Exception:
        return []
    return [candidate] if candidate else []


async def _send_notify_bot(chat_id: int, text: str) -> tuple[bool, int, str]:
    token = NOTIFY_BOT_TOKEN
    if not token:
        return False, 0, "notify_bot_token_missing"
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    async def _post(payload: dict[str, Any]) -> tuple[int, str]:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json={k: v for k, v in payload.items() if v is not None})
        except httpx.HTTPError as exc:
            return 0, str(exc)
        if 200 <= resp.status_code < 300:
            return resp.status_code, ""
        try:
            data = resp.json()
            err = data.get("description") or data.get("error") or resp.text
        except Exception:
            err = resp.text
        return resp.status_code, err or "send_failed"

    base_payload = {
        "chat_id": int(chat_id),
        "text": text,
        "parse_mode": "HTML",  # используем HTML для ссылки
        "disable_web_page_preview": True,
    }
    status, error = await _post(base_payload)
    if 200 <= status < 300:
        return True, status, ""
    return False, status, error or "send_failed"


def _looks_like_manager_outgoing(event: Mapping[str, Any]) -> bool:
    """Best-effort check whether telegram event came from manager account."""

    def _has_flag(blob: Any) -> bool:
        if not isinstance(blob, Mapping):
            return False
        key_obj = blob.get("key") if isinstance(blob.get("key"), Mapping) else {}
        return bool(
            blob.get("manager")
            or blob.get("out")
            or blob.get("outgoing")
            or blob.get("fromMe")
            or (isinstance(key_obj, Mapping) and key_obj.get("fromMe"))
        )

    origin = event.get("origin")
    if isinstance(origin, str) and origin.startswith("telegram:manager"):
        return True
    if bool(event.get("manager")) or bool(event.get("out")):
        return True

    provider_raw = event.get("provider_raw")
    message_obj = event.get("message") if isinstance(event.get("message"), Mapping) else {}
    meta_obj = (
        message_obj.get("meta")
        if isinstance(message_obj, Mapping) and isinstance(message_obj.get("meta"), Mapping)
        else {}
    )
    return _has_flag(provider_raw) or _has_flag(message_obj) or _has_flag(meta_obj)

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


def _resolve_gs_path() -> str | None:
    candidates = []
    raw = os.getenv("WA_PDF_COMPRESS_BIN")
    if raw:
        candidates.append(raw)
    if PDF_COMPRESS_BIN:
        candidates.append(PDF_COMPRESS_BIN)
    candidates.extend(["/usr/bin/gs", "/usr/local/bin/gs"])
    for candidate in candidates:
        if not candidate:
            continue
        if os.path.isabs(candidate):
            if os.path.exists(candidate) and os.access(candidate, os.X_OK):
                return candidate
        else:
            found = shutil.which(candidate)
            if found:
                return found
    return None


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
    normalized_attachments = normalize_message_attachments(attachments or [])
    if not text and not normalized_attachments:
        return
    fingerprint = content_fingerprint(text, normalized_attachments)
    if message_id is not None:
        try:
            dedup_key = f"amocrm:inbound:{tenant_id}:{lead_id}:{channel}:{int(message_id)}"
            deduped = await _redis_queue.set(dedup_key, "1", ex=86400, nx=True)
            if not deduped:
                return
        except Exception:
            pass
    else:
        try:
            dedup_key = f"amocrm:inbound:{tenant_id}:{lead_id}:{channel}:fp:{fingerprint}"
            deduped = await _redis_queue.set(dedup_key, "1", ex=180, nx=True)
            if not deduped:
                return
        except Exception:
            pass
    try:
        await amocrm_service.amocrm_on_inbound_message(
            int(tenant_id),
            int(lead_id),
            text=text,
            channel=channel,
            attachments=normalized_attachments or None,
            source_role="lead",
        )
    except Exception as exc:
        log(
            f"event=amocrm_inbound_failed channel={channel} tenant={tenant_id} "
            f"lead_id={lead_id} error={exc}"
        )


def _log_smart_reply_diag(channel: str, tenant_id: int, lead_id: int | None, reply: Any) -> None:
    """Emit debug info about planner output for downstream analysis."""

    try:
        plan_data = getattr(reply, "llm_plan", None)
        next_questions: list[str] = []
        plan_cta = None
        if isinstance(plan_data, Mapping):
            raw_questions = plan_data.get("next_questions")
            if isinstance(raw_questions, (list, tuple)):
                next_questions = [str(q) for q in raw_questions if q]
            plan_cta = plan_data.get("cta")
        raw_answer = getattr(reply, "llm_raw_answer", None)
        refined = str(reply or "")
        log(
            "event=smart_reply_diag channel=%s tenant=%s lead_id=%s plan_next_questions=%s plan_cta=%s answer=%s refined=%s"
            % (
                channel,
                tenant_id,
                lead_id if lead_id is not None else 0,
                json.dumps(next_questions, ensure_ascii=False),
                json.dumps(plan_cta or "", ensure_ascii=False),
                json.dumps(raw_answer or "", ensure_ascii=False),
                json.dumps(refined, ensure_ascii=False),
            )
        )
    except Exception as exc:
        log(
            "event=smart_reply_diag_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (channel, tenant_id, lead_id if lead_id is not None else 0, exc)
        )

async def _ask_llm_with_fallback(
    messages: list[dict[str, Any]],
    *,
    tenant_id: int,
    contact_id: int | None,
    channel: str,
) -> str:
    try:
        return await asyncio.wait_for(
            ask_llm(
                messages,
                tenant=tenant_id,
                contact_id=contact_id,
                channel=channel,
            ),
            timeout=SMART_REPLY_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        log(
            "event=smart_reply_timeout channel=%s tenant=%s contact=%s timeout=%.1f"
            % (channel, tenant_id, contact_id or 0, SMART_REPLY_TIMEOUT_SECONDS)
        )
    except Exception as exc:
        log(
            "event=smart_reply_failed channel=%s tenant=%s contact=%s stage=ask_llm error=%s fallback=1"
            % (channel, tenant_id, contact_id or 0, exc)
        )
    return FALLBACK_REPLY_TEXT

AVITO_CHAT_CACHE: Dict[int, str] = {}


def _avito_auto_reply_text(tenant_id: int) -> str:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None
    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            auto_flag = behavior.get("auto_reply")
            if auto_flag is not None and not bool(auto_flag):
                return ""
            text_value = behavior.get("auto_reply_text")
            if isinstance(text_value, str) and text_value.strip():
                return text_value.strip()
    return ""


def _avito_phone_tg_template(tenant_id: int) -> str:
    # 1) Explicit behavior config
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None
    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            txt = behavior.get("avito_phone_tg_template")
            if isinstance(txt, str) and txt.strip():
                return txt.strip()

    # 2) Fallback to persona meta for backward compatibility
    try:
        persona_meta = persona_meta_config(int(tenant_id))
    except Exception:
        persona_meta = {}
    if not isinstance(persona_meta, Mapping):
        return ""
    for key in (
        "avito_phone_tg_template",
        "meta.avito_phone_tg_template",
        "persona.meta.avito_phone_tg_template",
    ):
        value = persona_meta.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _avito_smart_reply_enabled(tenant_id: int) -> bool:
    """Per-tenant gate for Avito smart-reply; default disabled."""

    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None

    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            for key in ("avito_smart_reply_enabled", "avito_ai_enabled"):
                flag = behavior.get(key)
                if flag is not None:
                    return bool(flag)
    return False


def _response_pipeline_enabled() -> bool:
    flag = str(os.getenv("RESPONSE_PIPELINE_ENABLED", "")).strip().lower()
    return flag in {"1", "true", "yes", "on"}


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
    return str(channel).strip().lower() in SMART_REPLY_SPLIT_CHANNELS


def _split_part_delay_seconds_value() -> float:
    if SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS <= SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS:
        return float(SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS)
    return float(
        random.randint(
            SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS,
            SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS,
        )
    )


_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_EOS_MARKER = "<<eos>>"
_ACK_CAP_NEXT_WORD_RE = re.compile(
    r"(?iu)\b(ок|понял|принял|услышал|ладно|хорошо)\s+([А-ЯЁA-Z][А-Яа-яЁёA-Za-z\-]{0,40})\b"
)


def _punct_style_segment(text: str, comma_index: int) -> tuple[str, int]:
    out_chars: list[str] = []
    idx = int(comma_index or 0)
    eos_pending = False
    for ch in text:
        if ch in {".", "!"}:
            if not eos_pending:
                out_chars.append(_EOS_MARKER)
                eos_pending = True
            continue
        if ch == ",":
            idx += 1
            # Remove each second comma -> 50% commas removed.
            if idx % 2 == 0:
                continue
        if not ch.isspace():
            eos_pending = False
        out_chars.append(ch)
    return "".join(out_chars), idx


def _lowercase_after_removed_sentence_endings(text: str) -> str:
    candidate = str(text or "")
    if not candidate:
        return ""
    if _EOS_MARKER not in candidate:
        return candidate
    parts = candidate.split(_EOS_MARKER)
    merged = parts[0].rstrip()
    for part in parts[1:]:
        chunk = part.lstrip()
        if chunk:
            first = chunk[0]
            if first.isalpha():
                chunk = first.lower() + chunk[1:]
        if merged and chunk:
            merged = f"{merged} {chunk}"
        elif chunk:
            merged = chunk
    return merged.strip()


def _lowercase_after_acknowledgement(text: str) -> str:
    candidate = str(text or "")
    if not candidate:
        return ""

    def _repl(match: re.Match[str]) -> str:
        head = match.group(1)
        word = match.group(2)
        if not word:
            return match.group(0)
        return f"{head} {word[0].lower()}{word[1:]}"

    return _ACK_CAP_NEXT_WORD_RE.sub(_repl, candidate)


def _apply_custom_punctuation_style(text: str) -> str:
    candidate = str(text or "")
    if not candidate:
        return ""
    if not SMART_REPLY_PUNCT_STYLE_ENABLED:
        return candidate.strip()

    parts: list[str] = []
    pos = 0
    comma_idx = 0
    for match in _URL_RE.finditer(candidate):
        if match.start() > pos:
            segment, comma_idx = _punct_style_segment(candidate[pos : match.start()], comma_idx)
            parts.append(segment)
        parts.append(match.group(0))
        pos = match.end()
    if pos < len(candidate):
        tail, comma_idx = _punct_style_segment(candidate[pos:], comma_idx)
        parts.append(tail)

    styled = "".join(parts)
    styled = re.sub(r"[ \t]{2,}", " ", styled)
    styled = re.sub(r"[ \t]+\n", "\n", styled)
    styled = re.sub(r"\n{3,}", "\n\n", styled)
    styled = re.sub(r"\s+([,?])", r"\1", styled)
    styled = re.sub(r",{2,}", ",", styled)
    styled = re.sub(r"\?{2,}", "?", styled)
    styled = _lowercase_after_removed_sentence_endings(styled)
    styled = _lowercase_after_acknowledgement(styled)
    return styled.strip()


_GREETING_PREFIX_RE = re.compile(
    r"^\s*(здравствуйте|добрый(?:й|е)|доброго|привет|салам|доброе утро|добрый вечер)\b",
    re.IGNORECASE,
)
_QUESTION_START_RE = re.compile(
    r"\b(в каком|какой|какая|какие|где|когда|сколько|что|как|подскажите|уточните|нужен ли|нужна ли)\b",
    re.IGNORECASE,
)
_SEGMENT_CONNECTOR_RE = re.compile(
    r"\s+(?:но|а|если|когда|чтобы|потом|также|при этом|после этого)\s+",
    re.IGNORECASE,
)


def _split_long_segment_by_words(text: str, max_len: int) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return []
    if len(clean) <= max_len:
        return [clean]
    words = clean.split(" ")
    out: list[str] = []
    current = ""
    for word in words:
        if not word:
            continue
        candidate = f"{current} {word}".strip() if current else word
        if current and len(candidate) > max_len:
            # Avoid splitting model name and its price into different bubbles.
            if re.match(r"^\d", word) and len(candidate) <= max_len + 14:
                current = candidate
                continue
            out.append(current.strip())
            current = word
        else:
            current = candidate
    if current.strip():
        out.append(current.strip())
    # Avoid tiny tail fragments ("установку") when long sentence is split by length.
    if len(out) >= 2 and len(out[-1]) < 12:
        combined = f"{out[-2]} {out[-1]}".strip()
        if len(combined) <= max_len + 20:
            out[-2] = combined
            out.pop()
    return [part for part in out if part]


def _split_long_segment_by_connectors(text: str, max_len: int) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return []
    if len(clean) <= max_len:
        return [clean]

    out: list[str] = []
    remaining = clean
    min_cut = max(48, int(max_len * 0.5))
    while len(remaining) > max_len:
        window = remaining[: max_len + 1]
        split_pos = -1
        for match in _SEGMENT_CONNECTOR_RE.finditer(window):
            if match.start() >= min_cut:
                split_pos = int(match.start())
        if split_pos <= 0:
            break
        head = remaining[:split_pos].strip(" ,")
        if len(head) < min_cut:
            break
        if head:
            out.append(head)
        remaining = remaining[split_pos:].strip(" ,")
        if not remaining:
            break
    if remaining:
        out.append(remaining)
    return [part for part in out if part]


def _merge_short_split_parts(parts: list[str], max_len: int) -> list[str]:
    if not parts:
        return []
    def _is_atomic_contact_or_link(chunk: str) -> bool:
        raw = re.sub(r"\s+", " ", str(chunk or "")).strip(" ,")
        if not raw:
            return False
        if re.fullmatch(r"https?://\S+", raw, flags=re.IGNORECASE):
            return True
        if re.fullmatch(r"@[\w\d_]{4,}", raw):
            return True
        if re.fullmatch(r"(?:\+?\d[\d\-\s()]{8,}\d)", raw):
            return True
        return False

    merged: list[str] = []
    min_part = max(36, int(max_len * 0.33))
    for part in parts:
        candidate = re.sub(r"\s+", " ", str(part or "")).strip(" ,")
        if not candidate:
            continue
        if _is_atomic_contact_or_link(candidate):
            merged.append(candidate)
            continue
        if merged and len(candidate) < min_part:
            prev = merged[-1]
            if _is_atomic_contact_or_link(prev):
                merged.append(candidate)
                continue
            if _GREETING_PREFIX_RE.match(prev) and _QUESTION_START_RE.match(candidate):
                merged.append(candidate)
                continue
            combined = f"{merged[-1]} {candidate}".strip()
            if len(combined) <= max_len + 6:
                merged[-1] = combined
                continue
        merged.append(candidate)
    if len(merged) >= 2 and len(merged[0]) < min_part:
        if not (_GREETING_PREFIX_RE.match(merged[0]) and _QUESTION_START_RE.match(merged[1])):
            combined = f"{merged[0]} {merged[1]}".strip()
            if len(combined) <= max_len + 6:
                merged[1] = combined
                merged = merged[1:]

    tail_connectors = ("и", "но", "а", "или", "если", "чтобы", "потом", "также")
    idx = 0
    while idx < len(merged) - 1:
        last_word = merged[idx].split(" ")[-1].lower()
        if last_word in tail_connectors:
            combined = f"{merged[idx]} {merged[idx + 1]}".strip()
            if len(combined) <= max_len + 6:
                merged[idx] = combined
                del merged[idx + 1]
                continue
        # Keep "модель ... 33 900 ₽" in one message for readability.
        if re.search(r'[»"]\s*$', merged[idx]) and re.match(r"^\d", merged[idx + 1]):
            combined = f"{merged[idx]} {merged[idx + 1]}".strip()
            if len(combined) <= max_len + 20:
                merged[idx] = combined
                del merged[idx + 1]
                continue
        if "—" in merged[idx] and re.match(r"^\d", merged[idx + 1]):
            combined = f"{merged[idx]} {merged[idx + 1]}".strip()
            if len(combined) <= max_len + 20:
                merged[idx] = combined
                del merged[idx + 1]
                continue
        idx += 1
    return merged


def _split_long_segment(text: str, max_len: int) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return []
    if len(clean) <= max_len:
        return [clean]

    comma_parts = [part.strip() for part in clean.split(",") if part.strip()]
    if len(comma_parts) > 1 and any(len(part) < 8 for part in comma_parts):
        # Tiny comma prefixes like "ок," degrade split quality; prefer connector-based split.
        conn_parts = _split_long_segment_by_connectors(clean, max_len)
        out_parts: list[str] = []
        for part in conn_parts:
            out_parts.extend(_split_long_segment_by_words(part, max_len))
        normalized = [part for part in out_parts if part]
        if len(normalized) >= 2:
            return normalized
    if len(comma_parts) <= 1:
        dash_parts = [part.strip() for part in re.split(r"\s*[—–;]\s*", clean) if part.strip()]
        if len(dash_parts) > 1:
            expanded: list[str] = []
            for part in dash_parts:
                expanded.extend(_split_long_segment_by_connectors(part, max_len))
            out_parts: list[str] = []
            for part in expanded:
                out_parts.extend(_split_long_segment_by_words(part, max_len))
            return [part for part in out_parts if part]
        conn_parts = _split_long_segment_by_connectors(clean, max_len)
        out_parts: list[str] = []
        for part in conn_parts:
            out_parts.extend(_split_long_segment_by_words(part, max_len))
        return [part for part in out_parts if part]

    out: list[str] = []
    current = ""
    for idx, part in enumerate(comma_parts):
        suffix = "," if idx < len(comma_parts) - 1 else ""
        piece = f"{part}{suffix}".strip()
        candidate = f"{current} {piece}".strip() if current else piece
        if current and len(candidate) > max_len:
            out.extend(_split_long_segment_by_words(current, max_len))
            current = piece
        else:
            current = candidate
    if current.strip():
        out.extend(_split_long_segment_by_words(current, max_len))
    return [part.strip() for part in out if part.strip()]


def _split_greeting_question_combo(text: str) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return [clean] if clean else []
    if not _GREETING_PREFIX_RE.search(clean):
        return [clean]
    match = _QUESTION_START_RE.search(clean)
    if not match:
        return [clean]
    split_at = int(match.start())
    if split_at <= 6:
        return [clean]
    head = clean[:split_at].strip(" ,")
    tail = clean[split_at:].strip(" ,")
    if not head or not tail:
        return [clean]
    if len(head) > 56:
        return [clean]
    # For no-question-mark phrasing ("Здравствуйте в каком городе..."), still split.
    if "?" not in clean and len(tail) < 10:
        return [clean]
    return [head, tail]


_URL_TOKEN_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_TG_HANDLE_RE = re.compile(r"(?<!\w)@[\w\d_]{4,}")
_PHONE_TOKEN_RE = re.compile(r"(?<!\d)(?:\+?\d[\d\-\s()]{8,}\d)(?!\d)")


def _extract_standalone_tokens(text: str) -> list[str]:
    candidate = str(text or "").strip()
    if not candidate:
        return []
    tokens: list[tuple[int, int, str]] = []
    for rx in (_URL_TOKEN_RE, _TG_HANDLE_RE, _PHONE_TOKEN_RE):
        for match in rx.finditer(candidate):
            token = str(match.group(0) or "").strip()
            if token:
                tokens.append((match.start(), match.end(), token))
    if not tokens:
        return [candidate]
    tokens.sort(key=lambda item: (item[0], item[1]))
    merged: list[tuple[int, int, str]] = []
    for start, end, token in tokens:
        if merged and start < merged[-1][1]:
            continue
        merged.append((start, end, token))
    out: list[str] = []
    cursor = 0
    for start, end, token in merged:
        prefix = candidate[cursor:start].strip(" ,")
        if prefix:
            out.append(prefix)
        out.append(token)
        cursor = end
    tail = candidate[cursor:].strip(" ,")
    if tail:
        out.append(tail)
    return [item for item in out if item]


def _force_isolate_contact_tokens(parts: list[str]) -> list[str]:
    if not parts:
        return []
    out: list[str] = []
    for raw in parts:
        part = re.sub(r"\s+", " ", str(raw or "")).strip()
        if not part:
            continue
        if not (
            _URL_TOKEN_RE.search(part)
            or _TG_HANDLE_RE.search(part)
            or _PHONE_TOKEN_RE.search(part)
        ):
            out.append(part)
            continue
        expanded = _extract_standalone_tokens(part)
        if expanded:
            out.extend(expanded)
        else:
            out.append(part)
    return [item for item in out if item]


def _has_contact_intro(parts: list[str]) -> bool:
    if not parts:
        return False
    combined = " ".join(str(p or "") for p in parts).lower()
    markers = (
        "контакт",
        "для связи",
        "напишите",
        "пишите",
        "позвон",
        "связаться",
        "telegram",
        "телеграм",
        "whatsapp",
        "ватсап",
        "вотсап",
    )
    return any(marker in combined for marker in markers)


def _split_reply_for_send(reply_text: str, channel: str) -> list[str]:
    clean = re.sub(r"\s+", " ", str(reply_text or "")).strip()
    if not clean:
        return []
    ch = str(channel or "").strip().lower()
    has_contact_tokens = bool(
        _URL_TOKEN_RE.search(clean)
        or _TG_HANDLE_RE.search(clean)
        or _PHONE_TOKEN_RE.search(clean)
    )
    if not SMART_REPLY_SPLIT_ENABLED or ch not in SMART_REPLY_SPLIT_CHANNELS:
        if has_contact_tokens:
            tokenized = _extract_standalone_tokens(clean)
            if tokenized:
                return _force_isolate_contact_tokens(tokenized)
        return [clean]
    greeting_combo = _split_greeting_question_combo(clean)
    has_multi_questions = clean.count("?") > 1
    has_paragraphs = "\n\n" in clean
    if (
        len(clean) < SMART_REPLY_SPLIT_MIN_LEN
        and len(greeting_combo) <= 1
        and not has_multi_questions
        and not has_paragraphs
        and not has_contact_tokens
    ):
        return [clean]

    parts: list[str] = []
    blocks = [blk.strip() for blk in re.split(r"\n{2,}", clean) if blk.strip()]
    if not blocks:
        blocks = [clean]

    for block in blocks:
        greeting_split = _split_greeting_question_combo(block)
        for segment in greeting_split:
            seg = segment.strip()
            if not seg:
                continue
            dash_chunks = [part.strip() for part in re.split(r"\s*[—–;]\s*", seg) if part.strip()]
            if not dash_chunks:
                dash_chunks = [seg]
            q_chunks: list[str] = []
            for dash_chunk in dash_chunks:
                q_chunks.extend(
                    [q.strip() for q in re.findall(r"[^?]+(?:\?|$)", dash_chunk) if q.strip()]
                )
            if not q_chunks:
                q_chunks = [seg]
            for chunk in q_chunks:
                parts.extend(_split_long_segment(chunk, SMART_REPLY_SPLIT_MAX_LEN))

    deduped: list[str] = []
    prev_norm = ""
    for part in parts:
        line = re.sub(r"\s+", " ", part).strip(" ,")
        if not line:
            continue
        if re.fullmatch(r"[.!,;:()\-\s]+", line):
            continue
        norm = line.casefold()
        if norm == prev_norm:
            continue
        deduped.append(line)
        prev_norm = norm

    if not deduped:
        return [clean]
    tokenized: list[str] = []
    for part in deduped:
        tokenized.extend(_extract_standalone_tokens(part))
    deduped = tokenized or deduped
    deduped = _merge_short_split_parts(deduped, SMART_REPLY_SPLIT_MAX_LEN)
    if len(deduped) <= SMART_REPLY_SPLIT_MAX_PARTS:
        return deduped

    head = deduped[: SMART_REPLY_SPLIT_MAX_PARTS - 1]
    tail = " ".join(deduped[SMART_REPLY_SPLIT_MAX_PARTS - 1 :]).strip()
    if tail:
        head.append(tail)
    final_parts = [part for part in head if part]
    if has_contact_tokens:
        final_parts = _force_isolate_contact_tokens(final_parts)
    return final_parts


def _clip_text(value: str, limit: int = 1200) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _compose_burst_user_text(parts: list[str]) -> str:
    cleaned: list[str] = []
    last_norm = ""
    for raw in parts:
        text = str(raw or "").strip()
        if not text:
            continue
        norm = re.sub(r"\s+", " ", text).strip().lower()
        if norm == last_norm:
            continue
        cleaned.append(_clip_text(text, 700))
        last_norm = norm
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    lines = [f"{idx + 1}. {item}" for idx, item in enumerate(cleaned)]
    return (
        "Клиент отправил несколько сообщений подряд одним блоком. "
        "Ответьте единым сообщением, учтите все пункты и не повторяйтесь.\n"
        + "\n".join(lines)
    )


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


def _defer_outbox_item(item: Mapping[str, Any], due_ts: float) -> None:
    if due_ts <= 0:
        return
    payload = dict(item)
    heapq.heappush(
        _DEFERRED_OUTBOX_HEAP,
        (float(due_ts), next(_DEFERRED_OUTBOX_SEQ), payload),
    )


def _pop_ready_deferred_outbox(now_ts: float | None = None) -> dict[str, Any] | None:
    if now_ts is None:
        now_ts = time.time()
    if not _DEFERRED_OUTBOX_HEAP:
        return None
    due_ts, _, item = _DEFERRED_OUTBOX_HEAP[0]
    if due_ts > now_ts:
        return None
    heapq.heappop(_DEFERRED_OUTBOX_HEAP)
    return item


def _next_deferred_outbox_wait(now_ts: float | None = None) -> float | None:
    if now_ts is None:
        now_ts = time.time()
    if not _DEFERRED_OUTBOX_HEAP:
        return None
    due_ts = float(_DEFERRED_OUTBOX_HEAP[0][0])
    return max(0.0, due_ts - now_ts)


def _merge_reply_context(channel: str, base: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                merged[key] = value
            continue
        merged[key] = value
    if channel == "telegram":
        merged["tg_slot"] = _normalize_tg_slot(merged.get("tg_slot"))
    return merged


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
    ch = str(channel).strip().lower()
    if ch == "telegram":
        return _telegram_reply_enabled(tenant_id) and smart_reply_enabled(tenant_id)
    if ch == "max":
        return _max_reply_enabled(tenant_id) and smart_reply_enabled(tenant_id)
    if ch == "avito":
        return _avito_smart_reply_enabled(tenant_id) and smart_reply_enabled(tenant_id)
    if ch == "whatsapp":
        return smart_reply_enabled(tenant_id)
    return False


async def _generate_reply_text(
    *,
    tenant_id: int,
    lead_id: int,
    refer_id: int,
    channel: str,
    user_text: str,
) -> tuple[str, Any]:
    reply: Any = ""
    reply_text = ""
    if _response_pipeline_enabled():
        try:
            result = await run_response_pipeline(
                tenant_id=tenant_id,
                channel=channel,
                user_text=user_text,
                contact_id=refer_id if refer_id > 0 else 0,
                enable_photos=False,
                timeout_seconds=SMART_REPLY_TIMEOUT_SECONDS,
                log_fn=log,
            )
            reply_text = str(result.reply_text or "").strip()
            reply = result.reply_text
        except Exception as exc:
            log(
                "event=smart_reply_failed channel=%s tenant=%s lead_id=%s stage=pipeline error=%s"
                % (channel, tenant_id, lead_id, exc)
            )
            reply_text = default_fallback_reply(tenant_id)
            reply = reply_text
    else:
        try:
            messages = await build_llm_messages(refer_id, user_text, channel, tenant=tenant_id)
        except Exception as exc:
            log(
                "event=smart_reply_failed channel=%s tenant=%s lead_id=%s stage=build_messages error=%s"
                % (channel, tenant_id, lead_id, exc)
            )
            return "", ""

        reply = await _ask_llm_with_fallback(
            messages,
            tenant_id=tenant_id,
            contact_id=refer_id if refer_id > 0 else None,
            channel=channel,
        )
        reply_text = (reply or "").strip()
    reply_text = str(reply_text or "").strip()
    if channel == "telegram" and reply_text:
        # Для Telegram ссылки на каталог уводим в file-send логику.
        reply_text = re.sub(r"https?://\\S*/pub/catalog/file/\\S*", "", reply_text).strip()
    if reply_text:
        reply_text = _apply_custom_punctuation_style(reply_text)
    _log_smart_reply_diag(channel, tenant_id, lead_id, reply)
    return reply_text, reply


async def _maybe_set_waiting_photo_state(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    reply_text: str,
) -> None:
    if channel not in {"telegram", "max"}:
        return
    markers, _, photo_ttl = _photo_expectation_config(tenant_id)
    if not markers:
        return
    lowered = reply_text.lower()
    for marker in markers:
        if not isinstance(marker, str) or not marker.strip():
            continue
        if marker.strip().lower() not in lowered:
            continue
        ttl = photo_ttl if photo_ttl > 0 else HANDOFF_SILENCE_TTL_SECONDS
        state_key = f"conv:state:{tenant_id}:{lead_id}"
        try:
            await r.set(state_key, "waiting_photo", ex=ttl)
            log(
                "event=photo_expected_set channel=%s tenant=%s lead_id=%s ttl=%s marker=%s"
                % (channel, tenant_id, lead_id, ttl, marker)
            )
        except Exception as exc:
            log(
                "event=photo_expected_set_failed channel=%s tenant=%s lead_id=%s error=%s"
                % (channel, tenant_id, lead_id, exc)
            )
        break


async def _enqueue_channel_reply_payload(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    reply_text: str,
    user_text: str,
    context: Mapping[str, Any],
) -> bool:
    attachments: list[dict[str, Any]] = []
    if channel in {"telegram", "max", "avito"}:
        attachments = await _select_auto_photos(tenant_id, channel, user_text, reply_text)

    base_payload: Dict[str, Any] = {
        "lead_id": int(lead_id),
        "tenant": int(tenant_id),
        "tenant_id": int(tenant_id),
        "provider": channel,
        "ch": channel,
        "channel": channel,
        "attachments": attachments or [],
    }

    if channel == "telegram":
        base_payload["tg_slot"] = _normalize_tg_slot(context.get("tg_slot"))
        message_id = context.get("message_id")
        telegram_user_id = _coerce_int(context.get("telegram_user_id"))
        peer_id = _coerce_int(context.get("peer_id"))
        username = context.get("username")
        if message_id:
            base_payload["message_id"] = str(message_id)
        if telegram_user_id is not None:
            base_payload["telegram_user_id"] = str(telegram_user_id)
        if peer_id is not None:
            base_payload["peer_id"] = int(peer_id)
        if isinstance(username, str) and username.strip():
            base_payload["username"] = username.strip()
    elif channel == "max":
        message_id = context.get("message_id")
        max_user_id = _coerce_int(context.get("max_user_id"))
        peer_value = context.get("peer")
        if message_id:
            base_payload["message_id"] = str(message_id)
        if max_user_id is not None:
            base_payload["max_user_id"] = max_user_id
        if isinstance(peer_value, str) and peer_value.strip():
            base_payload["peer"] = peer_value.strip()
            base_payload["peer_id"] = peer_value.strip()
    elif channel == "whatsapp":
        message_id = context.get("message_id")
        to_value = str(context.get("to") or "").strip()
        to_jid = str(context.get("to_jid") or "").strip()
        base_payload["to"] = to_value
        if to_jid:
            base_payload["to_jid"] = to_jid
        if message_id:
            base_payload["message_id"] = str(message_id)
    elif channel == "avito":
        chat_id = str(context.get("chat_id") or "").strip()
        if not chat_id:
            return False
        base_payload["chat_id"] = chat_id
        base_payload["peer"] = chat_id
        base_payload["peer_id"] = chat_id
        account_id = _coerce_int(context.get("account_id"))
        message_id = context.get("message_id")
        avito_user_id = _coerce_int(context.get("avito_user_id"))
        avito_login = context.get("avito_login")
        if account_id is not None:
            base_payload["account_id"] = account_id
        if message_id:
            base_payload["message_id"] = str(message_id)
        if avito_user_id is not None:
            base_payload["avito_user_id"] = avito_user_id
        if isinstance(avito_login, str) and avito_login.strip():
            base_payload["avito_login"] = avito_login.strip()

    reply_parts = _split_reply_for_send(reply_text, channel)
    if not reply_parts:
        return False
    if len(reply_parts) > 1:
        log(
            "event=smart_reply_split channel=%s tenant=%s lead_id=%s parts=%s"
            % (channel, tenant_id, lead_id, len(reply_parts))
        )

    try:
        part_due_ts = time.time()
        use_part_delay = len(reply_parts) > 1 and _split_part_delay_enabled(channel)
        if use_part_delay:
            log(
                "event=smart_reply_split_delay channel=%s tenant=%s lead_id=%s min=%s max=%s"
                % (
                    channel,
                    tenant_id,
                    lead_id,
                    SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS,
                    SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS,
                )
            )
        for idx, part in enumerate(reply_parts):
            payload = dict(base_payload)
            payload["text"] = part
            if idx > 0:
                payload["attachments"] = []
            if idx > 0:
                if use_part_delay:
                    part_due_ts += _split_part_delay_seconds_value()
                payload["send_not_before_ts"] = float(part_due_ts)
                payload["split_part_index"] = int(idx + 1)
                payload["split_part_total"] = int(len(reply_parts))
            await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        log(
            "event=smart_reply_enqueue_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (channel, tenant_id, lead_id, exc)
        )
        return False
    return True


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
    reply_text, _ = await _generate_reply_text(
        tenant_id=tenant_id,
        lead_id=lead_id,
        refer_id=refer_id,
        channel=channel,
        user_text=user_text,
    )
    if not reply_text:
        log(
            "event=smart_reply_empty channel=%s tenant=%s lead_id=%s delayed=%s"
            % (channel, tenant_id, lead_id, int(delayed))
        )
        return False
    await _maybe_set_waiting_photo_state(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        reply_text=reply_text,
    )
    enqueued = await _enqueue_channel_reply_payload(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel=channel,
        reply_text=reply_text,
        user_text=user_text,
        context=context,
    )
    if not enqueued:
        return False
    await _mark_thread_bot_reply(tenant_id, channel, lead_id)
    log(
        "event=smart_reply_enqueued channel=%s tenant=%s lead_id=%s delayed=%s"
        % (channel, tenant_id, lead_id, int(delayed))
    )
    return True


async def _flush_pending_smart_reply(key: str) -> None:
    payload: Dict[str, Any] | None = None
    try:
        async with _PENDING_SMART_REPLY_LOCK:
            payload = _PENDING_SMART_REPLIES.get(key)
            if not payload:
                return
            due_at = float(payload.get("due_at") or 0.0)
        sleep_for = max(0.0, due_at - time.time())
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
        async with _PENDING_SMART_REPLY_LOCK:
            payload = _PENDING_SMART_REPLIES.pop(key, None)
        if not payload:
            return
        tenant_id = int(payload.get("tenant_id") or 0)
        lead_id = int(payload.get("lead_id") or 0)
        channel = str(payload.get("channel") or "").strip().lower()
        if tenant_id <= 0 or lead_id <= 0 or not channel:
            return
        if await _is_handoff_silenced(tenant_id, lead_id):
            log(
                "event=smart_reply_burst_drop channel=%s tenant=%s lead_id=%s reason=silenced"
                % (channel, tenant_id, lead_id)
            )
            return
        if not _can_generate_reply_for_channel(tenant_id, channel):
            log(
                "event=smart_reply_burst_drop channel=%s tenant=%s lead_id=%s reason=disabled"
                % (channel, tenant_id, lead_id)
            )
            return
        parts = payload.get("parts") if isinstance(payload.get("parts"), list) else []
        user_text = _compose_burst_user_text([str(item or "") for item in parts])
        if not user_text:
            return
        refer_id = int(payload.get("refer_id") or lead_id)
        context = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
        log(
            "event=smart_reply_burst_flush channel=%s tenant=%s lead_id=%s messages=%s"
            % (channel, tenant_id, lead_id, len(parts))
        )
        await _produce_and_enqueue_smart_reply(
            tenant_id=tenant_id,
            lead_id=lead_id,
            channel=channel,
            refer_id=refer_id,
            user_text=user_text,
            context=dict(context),
            delayed=True,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if payload:
            channel = str(payload.get("channel") or "")
            tenant_id = int(payload.get("tenant_id") or 0)
            lead_id = int(payload.get("lead_id") or 0)
        else:
            channel = "-"
            tenant_id = 0
            lead_id = 0
        log(
            "event=smart_reply_burst_flush_failed channel=%s tenant=%s lead_id=%s error=%s"
            % (channel, tenant_id, lead_id, exc)
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
    key = _smart_reply_pending_key(tenant_id, channel, lead_id)
    now_ts = time.time()
    async with _PENDING_SMART_REPLY_LOCK:
        payload = _PENDING_SMART_REPLIES.get(key)
        if payload:
            parts = payload.get("parts")
            if not isinstance(parts, list):
                parts = []
            if user_text.strip():
                parts.append(user_text.strip())
            if len(parts) > SMART_REPLY_BURST_MAX_MESSAGES:
                parts = parts[-SMART_REPLY_BURST_MAX_MESSAGES:]
            payload["parts"] = parts
            payload["updated_at"] = now_ts
            payload["refer_id"] = int(refer_id or payload.get("refer_id") or 0)
            base_ctx = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
            payload["context"] = _merge_reply_context(channel, base_ctx, context)
            _PENDING_SMART_REPLIES[key] = payload
            due_at = float(payload.get("due_at") or now_ts)
            log(
                "event=smart_reply_burst_append channel=%s tenant=%s lead_id=%s messages=%s due_in=%.1fs"
                % (channel, tenant_id, lead_id, len(parts), max(0.0, due_at - now_ts))
            )
            return
        due_at = now_ts + _delay_seconds_value()
        parts = [user_text.strip()] if user_text.strip() else []
        payload = {
            "tenant_id": int(tenant_id),
            "lead_id": int(lead_id),
            "channel": channel,
            "refer_id": int(refer_id or 0),
            "parts": parts,
            "context": _merge_reply_context(channel, {}, context),
            "created_at": now_ts,
            "updated_at": now_ts,
            "due_at": due_at,
        }
        task = asyncio.create_task(
            _flush_pending_smart_reply(key),
            name=f"smart-reply-delay:{channel}:{tenant_id}:{lead_id}",
        )
        payload["task"] = task
        _PENDING_SMART_REPLIES[key] = payload
        log(
            "event=smart_reply_burst_scheduled channel=%s tenant=%s lead_id=%s delay=%.1fs"
            % (channel, tenant_id, lead_id, max(0.0, due_at - now_ts))
        )


async def _try_handle_smart_reply_with_delay(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    refer_id: int,
    user_text: str,
    context: Mapping[str, Any],
) -> bool:
    if not _channel_delay_enabled(channel):
        return False
    if not user_text.strip():
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


def _extract_avito_user_name(payload: Mapping[str, Any], *, author_id: int | None, account_id: int | None) -> str:
    users = payload.get("users")
    if not isinstance(users, list):
        return ""

    def user_id_value(user: Mapping[str, Any]) -> int | None:
        return _coerce_int(
            user.get("id")
            or user.get("user_id")
            or (user.get("public_user_profile") or {}).get("user_id")
        )

    def user_name_value(user: Mapping[str, Any]) -> str:
        name = user.get("name") or user.get("username") or user.get("login")
        return str(name).strip() if name else ""

    if author_id is not None:
        for user in users:
            if not isinstance(user, Mapping):
                continue
            if user_id_value(user) == author_id:
                name = user_name_value(user)
                if name:
                    return name

    for user in users:
        if not isinstance(user, Mapping):
            continue
        uid = user_id_value(user)
        if account_id is not None and uid == account_id:
            continue
        name = user_name_value(user)
        if name:
            return name

    for user in users:
        if not isinstance(user, Mapping):
            continue
        name = user_name_value(user)
        if name:
            return name

    return ""


async def _resolve_avito_user_name(
    tenant_id: int,
    *,
    account_id: int | None,
    chat_id: str,
    author_id: int | None,
) -> str:
    if not chat_id:
        return ""

    cache_key = None
    if author_id is not None:
        cache_key = f"cache:avito_user_name:{tenant_id}:{author_id}"
        try:
            cached = await r.get(cache_key)
        except Exception:
            cached = None
        if isinstance(cached, str) and cached.strip():
            return cached.strip()

    try:
        info = await avito_integration.resolve_chat_participant_profile(
            int(tenant_id),
            account_id=account_id,
            chat_id=chat_id,
            author_id=author_id,
        )
    except Exception as exc:
        log(
            "event=avito_user_name_request_failed tenant=%s chat_id=%s error=%s"
            % (tenant_id, chat_id, exc)
        )
        return ""
    name = str((info or {}).get("name") or "").strip()
    if name and cache_key:
        try:
            await r.set(cache_key, name, ex=3600 * 24 * 7)
        except Exception:
            pass
    return name


def _coerce_bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes", "y", "on"}:
            return True
        if token in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _telegram_reply_enabled(tenant_id: int) -> bool:
    """Per-tenant gate for Telegram auto replies; default enabled."""

    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None

    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            for key in ("telegram_reply_enabled", "telegram_smart_reply_enabled", "telegram_ai_enabled"):
                flag = _coerce_bool_value(behavior.get(key))
                if flag is not None:
                    return bool(flag)
        root_flag = _coerce_bool_value(cfg.get("telegram_reply_enabled"))
        if root_flag is not None:
            return bool(root_flag)
    return True


def _normalize_tg_slot(value: Any) -> int:
    try:
        slot = int(value)
    except Exception:
        return TG_SLOT_MIN
    if slot < TG_SLOT_MIN:
        return TG_SLOT_MIN
    if slot > TG_SLOT_MAX:
        return TG_SLOT_MAX
    return slot


def _virtual_tg_tenant(tenant_id: int, slot: int) -> int:
    normalized = _normalize_tg_slot(slot)
    if normalized == TG_SLOT_MIN:
        return int(tenant_id)
    return int(tenant_id) * TG_SLOT_MULTIPLIER + normalized


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
        await r.set(_lead_tg_slot_key(tenant_id, lead_id), str(_normalize_tg_slot(slot)), ex=60 * 60 * 24 * 30)
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
    """Per-tenant gate for MAX auto replies; default enabled."""

    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None

    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            for key in ("max_reply_enabled", "max_smart_reply_enabled", "max_ai_enabled"):
                flag = _coerce_bool_value(behavior.get(key))
                if flag is not None:
                    return bool(flag)
        root_flag = _coerce_bool_value(cfg.get("max_reply_enabled"))
        if root_flag is not None:
            return bool(root_flag)
    return True


def _behavior_triggers(tenant_id: int) -> list[dict[str, Any]]:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = None
    if not isinstance(cfg, Mapping):
        return []
    behavior = cfg.get("behavior")
    if not isinstance(behavior, Mapping):
        return []
    triggers = behavior.get("triggers")
    if not isinstance(triggers, list):
        return []
    result: list[dict[str, Any]] = []
    for item in triggers:
        if not isinstance(item, Mapping):
            continue
        phrases = [p.strip() for p in item.get("phrases", []) if isinstance(p, str) and p.strip()]
        if not phrases:
            continue
        channels_raw = item.get("channels") or ["telegram", "avito", "whatsapp", "max"]
        channels: list[str] = []
        if isinstance(channels_raw, (list, tuple, set)):
            for ch in channels_raw:
                if isinstance(ch, str) and ch.strip():
                    channels.append(ch.strip().lower())
        elif isinstance(channels_raw, str) and channels_raw.strip():
            channels.append(channels_raw.strip().lower())
        if not channels:
            channels = ["telegram", "avito", "whatsapp", "max"]
        result.append(
            {
                "phrases": phrases,
                "channels": channels,
                "silence": bool(item.get("silence", True)),
                "notify": bool(item.get("notify", False)),
            }
        )
    return result


def _match_behavior_trigger(tenant_id: int, channel: str, text: str) -> dict[str, Any] | None:
    if not text or not channel:
        return None
    candidates = _behavior_triggers(tenant_id)
    if not candidates:
        return None
    lowered = text.lower()
    channel_norm = channel.strip().lower()
    for rule in candidates:
        channels = rule.get("channels") or []
        if channels and channel_norm not in channels:
            continue
        phrases = rule.get("phrases") or []
        for phrase in phrases:
            if isinstance(phrase, str) and phrase.strip() and phrase.strip().lower() in lowered:
                return rule
    return None


def _photo_expectation_config(tenant_id: int) -> tuple[list[str], str, int]:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        return [], "", 0
    if not isinstance(cfg, Mapping):
        return [], "", 0
    behavior = cfg.get("behavior")
    if not isinstance(behavior, Mapping):
        return [], "", 0
    markers_raw = behavior.get("photo_expected_markers") or []
    markers: list[str] = []
    if isinstance(markers_raw, (list, tuple, set)):
        for ph in markers_raw:
            if isinstance(ph, str) and ph.strip():
                markers.append(ph.strip())
    elif isinstance(markers_raw, str) and markers_raw.strip():
        for ph in markers_raw.split(","):
            if ph.strip():
                markers.append(ph.strip())
    reply_text = behavior.get("photo_expected_reply")
    reply = reply_text if isinstance(reply_text, str) else str(reply_text or "")
    try:
        ttl_val = int(behavior.get("photo_expected_ttl") or 0)
    except Exception:
        ttl_val = 0
    ttl = ttl_val if ttl_val > 0 else 0
    return markers, reply, ttl


def _photo_auto_config(tenant_id: int) -> tuple[bool, int]:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = {}
    if not isinstance(cfg, Mapping):
        return False, 1
    behavior = cfg.get("behavior")
    if not isinstance(behavior, Mapping):
        return False, 1
    enabled = bool(behavior.get("auto_photo_enabled"))
    try:
        max_count = int(behavior.get("auto_photo_max") or 0)
    except Exception:
        max_count = 0
    if max_count <= 0:
        max_count = 1
    return enabled, max_count


def _load_photo_manifest(tenant_id: int) -> list[dict[str, Any]]:
    try:
        path = tenant_dir(int(tenant_id)) / "uploads" / "photos" / "manifest.json"
    except Exception:
        return []
    if not path.exists() or not path.is_file():
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception:
        return []
    if isinstance(raw, list):
        return [entry for entry in raw if isinstance(entry, dict)]
    return []


def _tenant_public_key(tenant_id: int) -> str:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        return ""
    if not isinstance(cfg, Mapping):
        return ""
    passport = cfg.get("passport")
    if isinstance(passport, Mapping):
        key = str(passport.get("public_key") or "").strip()
        if key:
            return key
    return str(cfg.get("public_key") or "").strip()


def _build_photo_public_url(tenant_id: int, photo_id: str) -> str:
    base = (APP_BASE_URL or "").strip().rstrip("/")
    if not base:
        return ""
    key = _tenant_public_key(tenant_id)
    if not key:
        return ""
    return f"{base}/pub/files/photos/{photo_id}?tenant={tenant_id}&k={key}"


def _build_photo_public_path(tenant_id: int, photo_id: str) -> str:
    key = _tenant_public_key(tenant_id)
    if not key:
        return ""
    return f"/pub/files/photos/{photo_id}?tenant={tenant_id}&k={key}"


def _collect_outgoing_attachments(item: Mapping[str, Any], tenant_id: int) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    raw_attachment = item.get("attachment")
    if isinstance(raw_attachment, Mapping):
        entry = dict(raw_attachment)
        if not entry.get("url"):
            photo_id = str(entry.get("photo_id") or entry.get("id") or "").strip()
            if photo_id:
                entry["url"] = _build_photo_public_url(tenant_id, photo_id) or _build_photo_public_path(tenant_id, photo_id)
        attachments.append(entry)
    raw_list = item.get("attachments")
    if isinstance(raw_list, list):
        for att in raw_list:
            if not isinstance(att, Mapping):
                continue
            entry = dict(att)
            if not entry.get("url"):
                photo_id = str(entry.get("photo_id") or entry.get("id") or "").strip()
                if photo_id:
                    entry["url"] = _build_photo_public_url(tenant_id, photo_id) or _build_photo_public_path(tenant_id, photo_id)
                elif entry.get("path"):
                    entry["url"] = str(entry.get("path"))
            attachments.append(entry)
    photo_id = str(item.get("photo_id") or "").strip()
    if photo_id:
        attachments.append(
            {
                "type": "image",
                "photo_id": photo_id,
                "url": _build_photo_public_url(tenant_id, photo_id) or _build_photo_public_path(tenant_id, photo_id),
            }
        )
    raw_ids = item.get("photo_ids")
    if isinstance(raw_ids, list):
        for pid in raw_ids:
            pid_str = str(pid or "").strip()
            if not pid_str:
                continue
            attachments.append(
                {
                    "type": "image",
                    "photo_id": pid_str,
                    "url": _build_photo_public_url(tenant_id, pid_str) or _build_photo_public_path(tenant_id, pid_str),
                }
            )
    return attachments


def _normalize_photo_candidates(tenant_id: int, channel: str) -> list[dict[str, Any]]:
    entries = _load_photo_manifest(tenant_id)
    normalized: list[dict[str, Any]] = []
    channel_norm = channel.strip().lower()
    for entry in entries:
        photo_id = str(entry.get("id") or "").strip()
        if not photo_id:
            continue
        if not entry.get("auto"):
            continue
        channels_raw = entry.get("channels") if isinstance(entry.get("channels"), list) else []
        channels = [str(ch).strip().lower() for ch in channels_raw if str(ch).strip()]
        if channels and channel_norm not in channels:
            continue
        try:
            priority = int(entry.get("priority") or 0)
        except Exception:
            priority = 0
        normalized.append(
            {
                "id": photo_id,
                "title": entry.get("title") or entry.get("original") or entry.get("filename") or photo_id,
                "filename": entry.get("filename") or entry.get("original") or "",
                "tags": entry.get("tags") or [],
                "usage": entry.get("usage") or "",
                "priority": priority,
                "path": entry.get("path"),
            }
        )
    normalized.sort(key=lambda item: int(item.get("priority") or 0), reverse=True)
    return normalized


def _score_photo_candidate(candidate: Mapping[str, Any], text: str) -> int:
    hay = (text or "").lower()
    if not hay:
        return 0
    tokens: list[str] = []
    for key in ("title", "usage"):
        value = candidate.get(key)
        if isinstance(value, str) and value.strip():
            tokens.extend(re.split(r"[,\n;]+", value.lower()))
    tags = candidate.get("tags")
    if isinstance(tags, list):
        tokens.extend(str(tag).lower() for tag in tags if str(tag).strip())
    score = 0
    for token in tokens:
        clean = token.strip()
        if clean and clean in hay:
            score += 1
    return score


def _select_photos_by_tags(
    candidates: list[dict[str, Any]],
    user_text: str,
    reply_text: str,
    max_count: int,
) -> list[dict[str, Any]]:
    scored: list[tuple[int, int, dict[str, Any]]] = []
    combined = f"{user_text}\n{reply_text}".strip()
    for item in candidates:
        score = _score_photo_candidate(item, combined)
        if score <= 0:
            continue
        try:
            priority = int(item.get("priority") or 0)
        except Exception:
            priority = 0
        scored.append((score, priority, item))
    if not scored:
        return []
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [item[2] for item in scored[:max_count]]


def _guess_photo_mime(photo: Mapping[str, Any]) -> str:
    candidate = str(photo.get("path") or photo.get("url") or photo.get("name") or "")
    if candidate:
        mime, _ = mimetypes.guess_type(candidate)
        if mime:
            return mime
    return "image/jpeg"


def _extract_photo_ids(reply_text: str, allowed: set[str], max_count: int) -> list[str]:
    cleaned = (reply_text or "").strip()
    if not cleaned:
        return []
    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        return []
    try:
        payload = json.loads(match.group(0))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    raw_ids = payload.get("photo_ids")
    if not isinstance(raw_ids, list):
        return []
    seen: list[str] = []
    for item in raw_ids:
        candidate = str(item).strip()
        if not candidate or candidate not in allowed:
            continue
        if candidate in seen:
            continue
        seen.append(candidate)
        if len(seen) >= max_count:
            break
    return seen


async def _select_auto_photos(
    tenant_id: int,
    channel: str,
    user_text: str,
    reply_text: str,
) -> list[dict[str, Any]]:
    enabled, max_count = _photo_auto_config(tenant_id)
    if not enabled:
        return []
    candidates = _normalize_photo_candidates(tenant_id, channel)
    if not candidates:
        return []
    allowed_ids = {item["id"] for item in candidates if item.get("id")}
    tag_selected = _select_photos_by_tags(candidates, user_text, reply_text, max_count)
    if tag_selected:
        attachments: list[dict[str, Any]] = []
        for photo in tag_selected:
            url = _build_photo_public_url(tenant_id, photo["id"])
            if channel == "telegram" and not url:
                continue
            if not url:
                url = _build_photo_public_path(tenant_id, photo["id"])
            attachments.append(
                {
                    "type": "image",
                    "url": url,
                    "path": photo.get("path"),
                    "name": photo.get("filename") or photo.get("path") or photo.get("title"),
                    "mime": _guess_photo_mime(photo),
                }
            )
            if len(attachments) >= max_count:
                break
        if attachments:
            log(
                "event=auto_photo_selected tenant=%s channel=%s method=tags count=%s ids=%s",
                tenant_id,
                channel,
                len(attachments),
                [att.get("path") or att.get("url") for att in attachments],
            )
            return attachments
    log(
        "event=auto_photo_candidates tenant=%s channel=%s count=%s",
        tenant_id,
        channel,
        len(candidates),
    )
    return []


def _extract_ru_phone(text: str) -> str:
    if not text:
        return ""
    digits = re.sub(r"\D", "", text)
    if digits.startswith("8") and len(digits) == 11:
        digits = f"7{digits[1:]}"
    elif digits.startswith("7") and len(digits) == 10:
        digits = f"7{digits}"
    if len(digits) == 11 and digits.startswith("7"):
        return f"+{digits}"
    return ""


_TG_USERNAME_URL_RE = re.compile(
    r"(?iu)(?:https?://)?(?:t(?:elegram)?\.me)/([a-z][a-z0-9_]{4,31})"
)
_TG_USERNAME_AT_RE = re.compile(r"(?iu)(?<![\w.])@([a-z][a-z0-9_]{4,31})(?![\w])")
_TG_USERNAME_RESERVED = {
    "joinchat",
    "addstickers",
    "addemoji",
    "share",
    "s",
    "iv",
    "proxy",
    "login",
    "c",
}


def _extract_tg_username(text: str) -> str:
    if not text:
        return ""
    raw = str(text).strip()
    if not raw:
        return ""

    for match in _TG_USERNAME_URL_RE.finditer(raw):
        candidate = str(match.group(1) or "").strip()
        if not candidate:
            continue
        lowered = candidate.lower()
        if lowered in _TG_USERNAME_RESERVED:
            continue
        normalized = normalize_username(candidate)
        if normalized:
            return normalized

    for match in _TG_USERNAME_AT_RE.finditer(raw):
        candidate = str(match.group(1) or "").strip()
        if not candidate:
            continue
        lowered = candidate.lower()
        if lowered in _TG_USERNAME_RESERVED:
            continue
        normalized = normalize_username(candidate)
        if normalized:
            return normalized
    return ""


async def _resolve_live_amocrm_target_by_phone(
    tenant_id: int,
    *,
    phone: str | None,
    origin_lead_id: int | None = None,
) -> tuple[int | None, int | None]:
    phone_value = normalize_e164_digits(phone or "")
    cfg = read_tenant_config(int(tenant_id))
    amocrm_cfg = amocrm_service.get_amocrm_cfg(cfg)
    if not phone_value or not amocrm_cfg or not bool(amocrm_cfg.get("enabled")):
        return None, None
    base_url = await amocrm_service.resolve_api_base_url(amocrm_cfg, int(tenant_id))
    if not base_url:
        return None, None
    oauth_cfg = amocrm_service.resolve_oauth_cfg(amocrm_cfg, int(tenant_id))
    client = amocrm_integration.AmoCRMClient(
        tenant_id=int(tenant_id),
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )

    async def _is_live(contact_id: int | None, lead_id: int | None) -> bool:
        if not contact_id or not lead_id:
            return False
        try:
            await client.get_contact(int(contact_id))
            await client.get_lead(int(lead_id))
            return True
        except Exception:
            return False

    async def _contact_id_from_lead(lead_id: int | None) -> int | None:
        if not lead_id:
            return None
        try:
            lead_payload = await client.get_lead(int(lead_id))
        except Exception:
            return None
        embedded = lead_payload.get("_embedded") if isinstance(lead_payload, Mapping) else None
        contacts = embedded.get("contacts") if isinstance(embedded, Mapping) else None
        if not isinstance(contacts, list):
            return None
        for item in contacts:
            if not isinstance(item, Mapping):
                continue
            try:
                contact_id = int(item.get("id"))
            except Exception:
                continue
            if contact_id > 0:
                return contact_id
        return None

    if origin_lead_id and int(origin_lead_id) > 0:
        try:
            origin_link = await crm_links.get_link(int(tenant_id), int(origin_lead_id), amocrm_service.AMOCRM_PROVIDER)
        except Exception:
            origin_link = None
        try:
            existing_contact_id = (
                int(origin_link.get("provider_contact_id"))
                if isinstance(origin_link, Mapping) and origin_link.get("provider_contact_id") is not None
                else None
            )
        except Exception:
            existing_contact_id = None
        try:
            existing_lead_id = (
                int(origin_link.get("provider_lead_id"))
                if isinstance(origin_link, Mapping) and origin_link.get("provider_lead_id") is not None
                else None
            )
        except Exception:
            existing_lead_id = None
        if await _is_live(existing_contact_id, existing_lead_id):
            return existing_contact_id, existing_lead_id
        if existing_lead_id:
            lead_contact_id = await _contact_id_from_lead(existing_lead_id)
            if await _is_live(lead_contact_id, existing_lead_id):
                return lead_contact_id, existing_lead_id

    try:
        contacts = await client.search_contacts(phone_value)
    except Exception:
        return None, None
    candidates: list[tuple[int, int]] = []
    for item in contacts:
        try:
            contact_id = int(item.get("id"))
        except Exception:
            continue
        try:
            full_contact = await client.get_contact(contact_id, with_leads=True)
        except Exception:
            continue
        embedded = full_contact.get("_embedded") if isinstance(full_contact, Mapping) else None
        leads = embedded.get("leads") if isinstance(embedded, Mapping) else None
        if not isinstance(leads, list):
            continue
        for lead_item in leads:
            if not isinstance(lead_item, Mapping):
                continue
            try:
                lead_id = int(lead_item.get("id"))
            except Exception:
                continue
            if await _is_live(contact_id, lead_id):
                candidates.append((contact_id, lead_id))
    if not candidates:
        return None, None
    candidates.sort(key=lambda pair: pair[1], reverse=True)
    return candidates[0]


async def _wait_for_amocrm_link_ready(
    tenant_id: int,
    lead_id: int,
    *,
    timeout_seconds: float = 8.0,
    poll_seconds: float = 0.4,
) -> Mapping[str, Any] | None:
    deadline = time.monotonic() + max(0.5, float(timeout_seconds))
    last_link: Mapping[str, Any] | None = None
    while True:
        try:
            link = await crm_links.get_link(
                int(tenant_id),
                int(lead_id),
                amocrm_service.AMOCRM_PROVIDER,
            )
        except Exception:
            link = None
        if isinstance(link, Mapping):
            last_link = link
            if link.get("provider_lead_id") is not None or link.get("provider_contact_id") is not None:
                return link
        if time.monotonic() >= deadline:
            return last_link
        await asyncio.sleep(max(0.1, float(poll_seconds)))


async def _enqueue_amocrm_cleanup_event(
    tenant_id: int,
    lead_id: int,
    *,
    event_type: str,
    payload: Mapping[str, Any],
) -> None:
    # amoCRM v4 API on current account rejects DELETE leads/contacts (HTTP 405),
    # so we keep merge logic idempotent by link rebinding only.
    if str(event_type).strip().lower() in {"delete_lead", "delete_contact"}:
        return
    try:
        already = await crm_outbox.has_recent_event(
            int(tenant_id),
            amocrm_service.AMOCRM_PROVIDER,
            int(lead_id),
            str(event_type),
            dict(payload),
            window_seconds=900,
        )
    except Exception:
        already = False
    if already:
        return
    await crm_outbox.enqueue(
        int(tenant_id),
        amocrm_service.AMOCRM_PROVIDER,
        int(lead_id),
        str(event_type),
        dict(payload),
    )


async def _reconcile_avito_bridge_amocrm_links(
    *,
    tenant_id: int,
    origin_lead_id: int,
    tg_lead_id: int,
    keep_provider_lead_id: int,
    keep_provider_contact_id: int | None,
) -> None:
    try:
        await crm_outbox.cancel_pending_events(
            int(tenant_id),
            amocrm_service.AMOCRM_PROVIDER,
            int(origin_lead_id),
            "create_lead",
            reason="cancelled_by_avito_tg_merge",
        )
    except Exception:
        pass

    stable_hits = 0
    for _ in range(25):
        changed = False
        try:
            origin_link = await crm_links.get_link(
                int(tenant_id),
                int(origin_lead_id),
                amocrm_service.AMOCRM_PROVIDER,
            )
        except Exception:
            origin_link = None
        try:
            current_provider_lead = (
                int(origin_link.get("provider_lead_id"))
                if isinstance(origin_link, Mapping) and origin_link.get("provider_lead_id") is not None
                else None
            )
        except Exception:
            current_provider_lead = None
        try:
            current_provider_contact = (
                int(origin_link.get("provider_contact_id"))
                if isinstance(origin_link, Mapping) and origin_link.get("provider_contact_id") is not None
                else None
            )
        except Exception:
            current_provider_contact = None

        if current_provider_lead is not None and int(current_provider_lead) != int(keep_provider_lead_id):
            try:
                await _enqueue_amocrm_cleanup_event(
                    int(tenant_id),
                    int(tg_lead_id),
                    event_type="delete_lead",
                    payload={"amo_lead_id": int(current_provider_lead)},
                )
            except Exception:
                pass
            try:
                await crm_links.update_provider_lead_id(
                    int(tenant_id),
                    int(origin_lead_id),
                    amocrm_service.AMOCRM_PROVIDER,
                    int(keep_provider_lead_id),
                )
            except Exception:
                pass
            changed = True

        if (
            keep_provider_contact_id is not None
            and current_provider_contact is not None
            and int(current_provider_contact) != int(keep_provider_contact_id)
        ):
            try:
                await _enqueue_amocrm_cleanup_event(
                    int(tenant_id),
                    int(tg_lead_id),
                    event_type="delete_contact",
                    payload={"amo_contact_id": int(current_provider_contact)},
                )
            except Exception:
                pass
            try:
                await crm_links.update_provider_contact_id(
                    int(tenant_id),
                    int(origin_lead_id),
                    amocrm_service.AMOCRM_PROVIDER,
                    int(keep_provider_contact_id),
                )
            except Exception:
                pass
            changed = True

        try:
            await crm_links.update_provider_lead_id(
                int(tenant_id),
                int(tg_lead_id),
                amocrm_service.AMOCRM_PROVIDER,
                int(keep_provider_lead_id),
            )
        except Exception:
            pass
        if keep_provider_contact_id is not None:
            try:
                await crm_links.update_provider_contact_id(
                    int(tenant_id),
                    int(tg_lead_id),
                    amocrm_service.AMOCRM_PROVIDER,
                    int(keep_provider_contact_id),
                )
            except Exception:
                pass

        try:
            origin_chat_link = await crm_chat_links.get_link(
                int(tenant_id),
                int(origin_lead_id),
                amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
            )
        except Exception:
            origin_chat_link = None
        try:
            tg_chat_link = await crm_chat_links.get_link(
                int(tenant_id),
                int(tg_lead_id),
                amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
            )
        except Exception:
            tg_chat_link = None
        external_chat_id = (
            str((origin_chat_link or {}).get("external_chat_id") or "").strip()
            or str((tg_chat_link or {}).get("external_chat_id") or "").strip()
            or f"avio:{int(tenant_id)}:avito:{int(origin_lead_id)}"
        )
        external_conversation_id = (
            str((origin_chat_link or {}).get("external_conversation_id") or "").strip()
            or str((tg_chat_link or {}).get("external_conversation_id") or "").strip()
            or external_chat_id
        )
        try:
            external_chat_id, external_conversation_id = await amocrm_chat_service._canonical_chat_identity(
                int(tenant_id),
                provider_lead_id=int(keep_provider_lead_id),
                fallback_chat_id=external_chat_id,
                fallback_conversation_id=external_conversation_id,
            )
        except Exception:
            pass
        try:
            await crm_chat_links.upsert_link(
                int(tenant_id),
                int(origin_lead_id),
                amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
                external_chat_id=external_chat_id,
                external_conversation_id=external_conversation_id,
                external_contact_id=int(keep_provider_contact_id)
                if keep_provider_contact_id is not None
                else None,
                external_lead_id=int(keep_provider_lead_id),
            )
        except Exception:
            pass
        try:
            await crm_chat_links.upsert_link(
                int(tenant_id),
                int(tg_lead_id),
                amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
                external_chat_id=external_chat_id,
                external_conversation_id=external_conversation_id,
                external_contact_id=int(keep_provider_contact_id)
                if keep_provider_contact_id is not None
                else None,
                external_lead_id=int(keep_provider_lead_id),
            )
        except Exception:
            pass

        if not changed:
            stable_hits += 1
        else:
            stable_hits = 0
        if stable_hits >= 3:
            break
        await asyncio.sleep(0.8)


async def _send_telegram_to_target(
    tenant_id: int,
    text: str,
    *,
    phone: str | None = None,
    username: str | None = None,
    lead_id: int | None = None,
    contact_id: int | None = None,
) -> tuple[int, str]:
    phone_value = (phone or "").strip()
    username_target = normalize_username(username) if username else None
    headers: dict[str, str] = {}
    if TG_WORKER_TOKEN:
        headers["X-Auth-Token"] = TG_WORKER_TOKEN
    if ADMIN_TOKEN:
        headers["X-Admin-Token"] = ADMIN_TOKEN

    status_code, body_text = await telegram_transport.send(
        tenant=tenant_id,
        phone=phone_value or None,
        peer=username_target or None,
        text=text,
        lead_id=lead_id,
        meta={"contact_id": contact_id} if contact_id else None,
        headers=headers,
    )
    # If tgworker returned resolved peer_id, remember phone scoped to that peer for subsequent inbound messages.
    if status_code and status_code < 500:
        try:
            parsed = json.loads(body_text)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            peer_id_value = parsed.get("peer_id")
            message_id_value = parsed.get("message_id")
            username_value = str(parsed.get("username") or "").strip()
            display_name_value = sanitize_display_name(parsed.get("display_name"))
            resolved_peer_id: int | None = None
            try:
                if peer_id_value is not None:
                    resolved_peer_id = int(peer_id_value)
                    if phone_value:
                        await r.set(
                            f"cache:avito_phone:{tenant_id}:{peer_id_value}",
                            phone_value,
                            ex=3600 * 24 * 7,
                        )
            except Exception:
                pass
            if status_code >= 200 and status_code < 300 and resolved_peer_id and text.strip():
                resolved_lead_id: int | None = None
                title_hint = f"tg:id {resolved_peer_id}"
                contact_hint = phone_value or (username_target or "").strip()
                try:
                    found_lead = await find_lead_by_telegram(int(tenant_id), int(resolved_peer_id))
                except Exception as exc:
                    log(
                        "event=avito_phone_tg_find_lead_failed tenant=%s peer_id=%s error=%s"
                        % (tenant_id, resolved_peer_id, exc)
                    )
                    found_lead = None
                if found_lead and int(found_lead) > 0:
                    resolved_lead_id = int(found_lead)
                try:
                    normalized_username = normalize_username(username_value) if username_value else None
                    lead_title = normalized_username or display_name_value or title_hint
                    lead_contact = normalized_username or display_name_value or contact_hint
                    upsert_result = await upsert_lead(
                        resolved_lead_id if resolved_lead_id and resolved_lead_id > 0 else None,
                        channel="telegram",
                        tenant_id=int(tenant_id),
                        telegram_user_id=int(resolved_peer_id),
                        telegram_username=(normalized_username or "").lstrip("@") or None,
                        title=lead_title,
                        peer_id=int(resolved_peer_id),
                        peer=str(resolved_peer_id),
                        contact=lead_contact,
                    )
                    if upsert_result is not None:
                        resolved_lead_id = int(upsert_result)
                except Exception as exc:
                    log(
                        "event=avito_phone_tg_upsert_lead_failed tenant=%s peer_id=%s error=%s"
                        % (tenant_id, resolved_peer_id, exc)
                    )
                    resolved_lead_id = resolved_lead_id or int(resolved_peer_id)
                if resolved_lead_id and resolved_lead_id > 0:
                    bridge_from_origin = bool(lead_id and int(lead_id) > 0)
                    origin_crm_link = None
                    original_origin_provider_lead_id: int | None = None
                    origin_provider_lead_before_rebind: int | None = None
                    tg_provider_lead_id: int | None = None
                    tg_provider_contact_id: int | None = None
                    origin_provider_lead_id: int | None = None
                    origin_provider_contact_id: int | None = None
                    provider_lead_id: int | None = None
                    provider_contact_id: int | None = None
                    try:
                        existing_tg_link = await crm_links.get_link(
                            int(tenant_id),
                            int(resolved_lead_id),
                            amocrm_service.AMOCRM_PROVIDER,
                        )
                    except Exception:
                        existing_tg_link = None
                    if isinstance(existing_tg_link, Mapping):
                        try:
                            tg_provider_lead_id = (
                                int(existing_tg_link.get("provider_lead_id"))
                                if existing_tg_link.get("provider_lead_id") is not None
                                else None
                            )
                        except Exception:
                            tg_provider_lead_id = None
                        try:
                            tg_provider_contact_id = (
                                int(existing_tg_link.get("provider_contact_id"))
                                if existing_tg_link.get("provider_contact_id") is not None
                                else None
                            )
                        except Exception:
                            tg_provider_contact_id = None
                        if tg_provider_lead_id is None or tg_provider_contact_id is None:
                            tg_provider_lead_id = None
                            tg_provider_contact_id = None
                    if lead_id and int(lead_id) > 0:
                        try:
                            origin_crm_link = await _wait_for_amocrm_link_ready(
                                int(tenant_id),
                                int(lead_id),
                                timeout_seconds=8.0,
                                poll_seconds=0.4,
                            )
                        except Exception:
                            origin_crm_link = None
                    if isinstance(origin_crm_link, Mapping):
                        try:
                            origin_provider_lead_id = (
                                int(origin_crm_link.get("provider_lead_id"))
                                if origin_crm_link.get("provider_lead_id") is not None
                                else None
                            )
                        except Exception:
                            origin_provider_lead_id = None
                        try:
                            origin_provider_contact_id = (
                                int(origin_crm_link.get("provider_contact_id"))
                                if origin_crm_link.get("provider_contact_id") is not None
                                else None
                            )
                        except Exception:
                            origin_provider_contact_id = None
                    if origin_provider_lead_id is not None:
                        original_origin_provider_lead_id = int(origin_provider_lead_id)
                    if contact_id and contact_id > 0:
                        try:
                            await link_lead_contact(
                                int(resolved_lead_id),
                                int(contact_id),
                                channel="telegram",
                                peer=str(resolved_peer_id),
                            )
                        except Exception:
                            log(
                                "event=avito_phone_tg_link_contact_failed tenant=%s lead_id=%s contact_id=%s"
                                % (tenant_id, resolved_lead_id, contact_id)
                            )
                        try:
                            await update_contact_telegram(
                                int(contact_id),
                                int(resolved_peer_id),
                                (normalized_username or "").lstrip("@") or None,
                            )
                        except Exception:
                            log(
                                "event=avito_phone_tg_contact_update_failed tenant=%s lead_id=%s contact_id=%s"
                                % (tenant_id, resolved_lead_id, contact_id)
                            )
                    # Canonical priority:
                    # - For Avito->TG bridge we must keep current origin Avito lead as canonical.
                    #   Do not reuse arbitrary old Telegram/phone matches from previous dialogs.
                    # - For standalone TG flow keep existing fallback behavior.
                    if origin_provider_lead_id is not None and origin_provider_contact_id is not None:
                        provider_lead_id = int(origin_provider_lead_id)
                        provider_contact_id = int(origin_provider_contact_id)
                    elif (
                        not bridge_from_origin
                        and tg_provider_lead_id is not None
                        and tg_provider_contact_id is not None
                    ):
                        provider_lead_id = int(tg_provider_lead_id)
                        provider_contact_id = int(tg_provider_contact_id)
                    if (
                        not bridge_from_origin
                        and (provider_lead_id is None or provider_contact_id is None)
                    ):
                        live_contact_id, live_lead_id = None, None
                        if phone_value:
                            try:
                                live_contact_id, live_lead_id = await _resolve_live_amocrm_target_by_phone(
                                    int(tenant_id),
                                    phone=phone_value,
                                    origin_lead_id=int(lead_id) if lead_id and int(lead_id) > 0 else None,
                                )
                            except Exception:
                                live_contact_id, live_lead_id = None, None
                        if provider_contact_id is None and live_contact_id is not None:
                            provider_contact_id = int(live_contact_id)
                        if provider_lead_id is None and live_lead_id is not None:
                            provider_lead_id = int(live_lead_id)
                    # One amo deal for avito+telegram bridge:
                    # if origin lead already has amo lead, always reuse it.
                    if (provider_lead_id is None or provider_contact_id is None) and lead_id and int(lead_id) > 0:
                        try:
                            refreshed_origin_link = await _wait_for_amocrm_link_ready(
                                int(tenant_id),
                                int(lead_id),
                                timeout_seconds=6.0,
                                poll_seconds=0.4,
                            )
                        except Exception:
                            refreshed_origin_link = None
                        if isinstance(refreshed_origin_link, Mapping):
                            if provider_contact_id is None and refreshed_origin_link.get("provider_contact_id") is not None:
                                try:
                                    provider_contact_id = int(refreshed_origin_link.get("provider_contact_id"))
                                except Exception:
                                    provider_contact_id = provider_contact_id
                            if provider_lead_id is None and refreshed_origin_link.get("provider_lead_id") is not None:
                                try:
                                    provider_lead_id = int(refreshed_origin_link.get("provider_lead_id"))
                                except Exception:
                                    provider_lead_id = provider_lead_id
                    if provider_lead_id and lead_id and int(lead_id) > 0:
                        try:
                            await crm_outbox.cancel_pending_events(
                                int(tenant_id),
                                amocrm_service.AMOCRM_PROVIDER,
                                int(lead_id),
                                "create_lead",
                                reason="cancelled_by_avito_tg_merge",
                            )
                        except Exception:
                            pass
                    pipeline_id = None
                    stage_index = 0
                    inbound_count = 0
                    if isinstance(origin_crm_link, Mapping):
                        try:
                            pipeline_id = int(origin_crm_link.get("pipeline_id")) if origin_crm_link.get("pipeline_id") is not None else None
                        except Exception:
                            pipeline_id = None
                        try:
                            stage_index = int(origin_crm_link.get("stage_index") or 0)
                        except Exception:
                            stage_index = 0
                        try:
                            inbound_count = int(origin_crm_link.get("inbound_count") or 0)
                        except Exception:
                            inbound_count = 0
                    if provider_lead_id or provider_contact_id:
                        try:
                            if lead_id and int(lead_id) > 0:
                                existing_origin_link = await crm_links.get_link(
                                    int(tenant_id),
                                    int(lead_id),
                                    amocrm_service.AMOCRM_PROVIDER,
                                )
                                if (
                                    isinstance(existing_origin_link, Mapping)
                                    and existing_origin_link.get("provider_lead_id") is not None
                                ):
                                    try:
                                        origin_provider_lead_before_rebind = int(
                                            existing_origin_link.get("provider_lead_id")
                                        )
                                    except Exception:
                                        origin_provider_lead_before_rebind = None
                                if not existing_origin_link:
                                    await crm_links.create_link(
                                        int(tenant_id),
                                        int(lead_id),
                                        amocrm_service.AMOCRM_PROVIDER,
                                        pipeline_id=pipeline_id,
                                        stage_index=stage_index,
                                        inbound_count=inbound_count,
                                    )
                                if provider_contact_id is not None:
                                    await crm_links.update_provider_contact_id(
                                        int(tenant_id),
                                        int(lead_id),
                                        amocrm_service.AMOCRM_PROVIDER,
                                        int(provider_contact_id),
                                    )
                                if provider_lead_id is not None:
                                    await crm_links.update_provider_lead_id(
                                        int(tenant_id),
                                        int(lead_id),
                                        amocrm_service.AMOCRM_PROVIDER,
                                        int(provider_lead_id),
                                    )
                                if (
                                    origin_provider_lead_before_rebind
                                    and int(origin_provider_lead_before_rebind) != int(provider_lead_id or 0)
                                ):
                                    original_origin_provider_lead_id = int(origin_provider_lead_before_rebind)
                            existing_tg_crm_link = await crm_links.get_link(
                                int(tenant_id),
                                int(resolved_lead_id),
                                amocrm_service.AMOCRM_PROVIDER,
                            )
                            if not existing_tg_crm_link:
                                await crm_links.create_link(
                                    int(tenant_id),
                                    int(resolved_lead_id),
                                    amocrm_service.AMOCRM_PROVIDER,
                                    pipeline_id=pipeline_id,
                                    stage_index=stage_index,
                                    inbound_count=inbound_count,
                                )
                            if provider_contact_id is not None:
                                await crm_links.update_provider_contact_id(
                                    int(tenant_id),
                                    int(resolved_lead_id),
                                    amocrm_service.AMOCRM_PROVIDER,
                                    int(provider_contact_id),
                                )
                            if provider_lead_id is not None:
                                await crm_links.update_provider_lead_id(
                                    int(tenant_id),
                                    int(resolved_lead_id),
                                    amocrm_service.AMOCRM_PROVIDER,
                                    int(provider_lead_id),
                                )
                            origin_chat_link = None
                            if lead_id and int(lead_id) > 0:
                                try:
                                    origin_chat_link = await crm_chat_links.get_link(
                                        int(tenant_id),
                                        int(lead_id),
                                        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
                                    )
                                except Exception:
                                    origin_chat_link = None
                            external_chat_id = (
                                str((origin_chat_link or {}).get("external_chat_id") or "").strip()
                                or f"avio:{int(tenant_id)}:telegram:{int(resolved_lead_id)}"
                            )
                            external_conversation_id = (
                                str((origin_chat_link or {}).get("external_conversation_id") or "").strip()
                                or external_chat_id
                            )
                            if provider_lead_id is not None:
                                try:
                                    external_chat_id, external_conversation_id = await amocrm_chat_service._canonical_chat_identity(
                                        int(tenant_id),
                                        provider_lead_id=int(provider_lead_id),
                                        fallback_chat_id=external_chat_id,
                                        fallback_conversation_id=external_conversation_id,
                                    )
                                except Exception:
                                    pass
                            if lead_id and int(lead_id) > 0:
                                try:
                                    await crm_chat_links.upsert_link(
                                        int(tenant_id),
                                        int(lead_id),
                                        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
                                        external_chat_id=external_chat_id,
                                        external_conversation_id=external_conversation_id,
                                        external_contact_id=int(provider_contact_id)
                                        if provider_contact_id is not None
                                        else None,
                                        external_lead_id=int(provider_lead_id)
                                        if provider_lead_id is not None
                                        else None,
                                    )
                                except Exception:
                                    pass
                            await crm_chat_links.upsert_link(
                                int(tenant_id),
                                int(resolved_lead_id),
                                amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
                                external_chat_id=external_chat_id,
                                external_conversation_id=external_conversation_id,
                                external_contact_id=int(provider_contact_id) if provider_contact_id is not None else None,
                                external_lead_id=int(provider_lead_id) if provider_lead_id is not None else None,
                            )
                            try:
                                await amocrm_chat_service.sync_chat_profile(
                                    int(tenant_id),
                                    int(resolved_lead_id),
                                    cfg=read_tenant_config(int(tenant_id)),
                                )
                            except Exception:
                                log(
                                    "event=avito_phone_tg_sync_profile_failed tenant=%s lead_id=%s"
                                    % (tenant_id, resolved_lead_id)
                                )
                            if lead_id and int(lead_id) > 0:
                                try:
                                    await amocrm_chat_service.sync_chat_profile(
                                        int(tenant_id),
                                        int(lead_id),
                                        cfg=read_tenant_config(int(tenant_id)),
                                    )
                                except Exception:
                                    log(
                                        "event=avito_phone_avito_sync_profile_failed tenant=%s lead_id=%s"
                                        % (tenant_id, lead_id)
                                    )

                            preferred_identity = (
                                (normalized_username or "").strip()
                                or (display_name_value or "").strip()
                                or phone_value
                                or (username_target or "").strip()
                            )
                            if preferred_identity:
                                # Keep amo lead/contact labels aligned with Telegram identity
                                # after Avito->Telegram bridge so Inbox titles are meaningful.
                                target_leads: list[int] = [int(resolved_lead_id)]
                                if lead_id and int(lead_id) > 0 and int(lead_id) not in target_leads:
                                    target_leads.append(int(lead_id))
                                for target_lead in target_leads:
                                    try:
                                        await crm_outbox.enqueue(
                                            int(tenant_id),
                                            amocrm_service.AMOCRM_PROVIDER,
                                            int(target_lead),
                                            "update_fields",
                                            {"lead_name": preferred_identity},
                                        )
                                    except Exception:
                                        pass
                                    try:
                                        await crm_outbox.enqueue(
                                            int(tenant_id),
                                            amocrm_service.AMOCRM_PROVIDER,
                                            int(target_lead),
                                            "update_contact_fields",
                                            {"contact_name": preferred_identity},
                                        )
                                    except Exception:
                                        pass
                        except Exception as exc:
                            log(
                                "event=avito_phone_tg_clone_amocrm_link_failed tenant=%s lead_id=%s origin_lead_id=%s error=%s"
                                % (tenant_id, resolved_lead_id, lead_id, exc)
                            )
                    if (
                        provider_lead_id
                        and lead_id
                        and int(lead_id) > 0
                        and original_origin_provider_lead_id is None
                    ):
                        try:
                            current_origin_link = await crm_links.get_link(
                                int(tenant_id),
                                int(lead_id),
                                amocrm_service.AMOCRM_PROVIDER,
                            )
                        except Exception:
                            current_origin_link = None
                        if (
                            isinstance(current_origin_link, Mapping)
                            and current_origin_link.get("provider_lead_id") is not None
                        ):
                            try:
                                current_origin_lead_id = int(current_origin_link.get("provider_lead_id"))
                            except Exception:
                                current_origin_lead_id = None
                            if (
                                current_origin_lead_id
                                and int(current_origin_lead_id) != int(provider_lead_id)
                            ):
                                original_origin_provider_lead_id = int(current_origin_lead_id)
                    if (
                        original_origin_provider_lead_id
                        and provider_lead_id
                        and int(original_origin_provider_lead_id) != int(provider_lead_id)
                    ):
                        cleanup_payload = {"amo_lead_id": int(original_origin_provider_lead_id)}
                        try:
                            already_cleanup = await crm_outbox.has_recent_event(
                                int(tenant_id),
                                amocrm_service.AMOCRM_PROVIDER,
                                int(resolved_lead_id),
                                "delete_lead",
                                cleanup_payload,
                                window_seconds=900,
                            )
                        except Exception:
                            already_cleanup = False
                        if not already_cleanup:
                            try:
                                await crm_outbox.enqueue(
                                    int(tenant_id),
                                    amocrm_service.AMOCRM_PROVIDER,
                                    int(resolved_lead_id),
                                    "delete_lead",
                                    cleanup_payload,
                                )
                            except Exception as exc:
                                log(
                                    "event=avito_phone_tg_delete_old_lead_enqueue_failed tenant=%s old_lead=%s keep_lead=%s error=%s"
                                    % (
                                        tenant_id,
                                        original_origin_provider_lead_id,
                                        provider_lead_id,
                                        exc,
                                    )
                                )
                    if provider_lead_id and lead_id and int(lead_id) > 0:
                        try:
                            asyncio.create_task(
                                _reconcile_avito_bridge_amocrm_links(
                                    tenant_id=int(tenant_id),
                                    origin_lead_id=int(lead_id),
                                    tg_lead_id=int(resolved_lead_id),
                                    keep_provider_lead_id=int(provider_lead_id),
                                    keep_provider_contact_id=int(provider_contact_id)
                                    if provider_contact_id is not None
                                    else None,
                                )
                            )
                        except Exception:
                            pass
                    try:
                        provider_msg_id = None
                        try:
                            provider_msg_id = str(int(message_id_value)) if message_id_value is not None else None
                        except Exception:
                            provider_msg_id = str(message_id_value or "").strip() or None
                        await insert_message_out(
                            int(resolved_lead_id),
                            text.strip(),
                            provider_msg_id=provider_msg_id,
                            status="sent",
                            tenant_id=int(tenant_id),
                            channel="telegram",
                            telegram_user_id=int(resolved_peer_id),
                            telegram_username=(normalized_username or "").lstrip("@") or None,
                            title=lead_title,
                            is_bot=True,
                            source="bot",
                        )
                    except Exception as exc:
                        log(
                            "event=avito_phone_tg_store_out_failed tenant=%s lead_id=%s error=%s"
                            % (tenant_id, resolved_lead_id, exc)
                        )
                    # Mirror bridge bootstrap message into amo chat so Inbox shows
                    # continuity (Avito history + first Telegram handoff message).
                    if provider_lead_id is None:
                        log(
                            "event=avito_phone_tg_amocrm_sync_skipped tenant=%s lead_id=%s reason=provider_lead_missing"
                            % (tenant_id, resolved_lead_id)
                        )
                    elif text.strip():
                        try:
                            await amocrm_chat_service.enqueue_message(
                                int(tenant_id),
                                int(resolved_lead_id),
                                direction="out",
                                text=text.strip(),
                                channel="telegram",
                                attachments=None,
                            )
                            log(
                                "event=avito_phone_tg_amocrm_sync_enqueued tenant=%s lead_id=%s"
                                % (tenant_id, resolved_lead_id)
                            )
                        except Exception as exc:
                            log(
                                "event=avito_phone_tg_amocrm_sync_failed tenant=%s lead_id=%s error=%s"
                                % (tenant_id, resolved_lead_id, exc)
                            )

    return status_code, body_text


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
    text_value = (text or "").strip()
    if not text_value:
        return False
    out_payload: Dict[str, Any] = {
        "lead_id": int(lead_id),
        "tenant": int(tenant_id),
        "tenant_id": int(tenant_id),
        "provider": "avito",
        "ch": "avito",
        "channel": "avito",
        "text": text_value,
        "attachments": [],
        "chat_id": chat_id,
        "peer": chat_id,
        "peer_id": chat_id,
    }
    if account_id is not None:
        out_payload["account_id"] = account_id
    if user_id is not None:
        out_payload["avito_user_id"] = user_id
    if login:
        out_payload["avito_login"] = login
    if message_id:
        out_payload["message_id"] = message_id
    try:
        await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(out_payload, ensure_ascii=False))
    except Exception as exc:
        log(
            "event=avito_auto_reply_enqueue_failed tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )
        return False
    log(
        "event=avito_auto_reply_enqueued tenant=%s lead_id=%s chat_id=%s",
        tenant_id,
        lead_id,
        chat_id,
    )
    return True

def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _normalize_baileys_jid(candidate: Any) -> str:
    if candidate is None:
        return ""
    text = str(candidate).strip()
    if not text:
        return ""
    lowered = text.lower()
    if "@" in lowered:
        return lowered
    try:
        digits, _ = normalize_whatsapp_recipient(text)
    except WhatsAppAddressError:
        digits = _digits(text)
        if not digits:
            return ""
    return f"{digits}@s.whatsapp.net"


async def _resolve_cached_whatsapp_jid(tenant: int, lead_id: int) -> str | None:
    if tenant <= 0 or lead_id <= 0:
        return None
    cache_key = f"wa:jid:{tenant}"
    try:
        cached = await r.hget(cache_key, str(lead_id))
    except Exception:
        return None
    if not cached:
        return None
    text = cached.strip()
    return text or None


def _normalize_whatsapp_peer(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raw = str(raw)
    peer = raw.strip()
    if not peer:
        return None
    return peer.lower()


def _compress_pdf_bytes(data: bytes, filename: str, target_bytes: int) -> bytes | None:
    if not PDF_COMPRESS_ENABLED:
        return None
    if not data or target_bytes <= 0:
        return None
    gs_path = _resolve_gs_path()
    if not gs_path:
        return None
    src_path = out_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as src_file:
            src_file.write(data)
            src_path = src_file.name
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as out_file:
            out_path = out_file.name
        cmd = [
            gs_path,
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS={PDF_COMPRESS_SETTINGS}",
            "-dNOPAUSE",
            "-dQUIET",
            "-dBATCH",
            f"-sOutputFile={out_path}",
            src_path,
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=PDF_COMPRESS_TIMEOUT)
        with open(out_path, "rb") as fh:
            compressed = fh.read()
        if not compressed:
            return None
        if len(compressed) >= len(data):
            return None
        return compressed
    except (subprocess.SubprocessError, OSError, ValueError):
        return None
    finally:
        if src_path:
            try:
                os.remove(src_path)
            except OSError:
                pass
        if out_path:
            try:
                os.remove(out_path)
            except OSError:
                pass


def _coerce_int(value: Any) -> Optional[int]:
    try:
        result = int(str(value).strip())
    except Exception:
        return None
    return result


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
    """Return True if the queue payload looks like a status echo we produced."""

    if not isinstance(item, Mapping):
        return False

    status = item.get("status")
    if not status:
        return False

    # Real outgoing jobs always carry either text or attachments to deliver.
    if item.get("text") or item.get("attachment") or item.get("attachments"):
        return False

    # Status echoes from write_result contain a reply preview and version tag.
    reply = item.get("reply")
    version = item.get("version")
    if isinstance(reply, str) and version:
        return True

    return False


async def _whitelist_allows(
    *,
    telegram_user_id: Optional[int],
    username: Optional[str],
    raw_to: Any,
    lead_id: Optional[int],
    tenant_id: Optional[int],
    channel: str,
) -> tuple[bool, str]:
    if OUTBOX_WHITELIST.allow_all:
        return True, "allow_all"

    candidate_ids: set[int] = set()
    if telegram_user_id is not None:
        candidate_ids.add(int(telegram_user_id))
    raw_id = _coerce_int(raw_to)
    if raw_id is not None:
        candidate_ids.add(raw_id)
    for candidate in candidate_ids:
        if candidate in OUTBOX_WHITELIST.ids:
            return True, "id"

    candidate_names: set[str] = set()
    normalized = normalize_username(username)
    if normalized:
        lowered = normalized.lower()
        candidate_names.add(lowered)
        candidate_names.add(lowered.lstrip("@"))
    if isinstance(raw_to, str):
        alt = normalize_username(raw_to)
        if alt:
            lowered_alt = alt.lower()
            candidate_names.add(lowered_alt)
            candidate_names.add(lowered_alt.lstrip("@"))
    for name in candidate_names:
        if name in OUTBOX_WHITELIST.usernames:
            return True, "username"

    number_candidates: set[str] = set()
    format_error = False
    if raw_to is not None:
        try:
            number_candidates.add(normalize_e164_digits(raw_to))
        except WhatsAppAddressError:
            format_error = True
        except Exception:
            format_error = True

    for digits in number_candidates:
        if whitelist_contains_number(OUTBOX_WHITELIST, digits):
            return True, "number"

    if channel == "whatsapp":
        if lead_id and lead_id > 0:
            try:
                recent = await has_recent_incoming_message(
                    int(lead_id),
                    tenant_id=int(tenant_id) if tenant_id is not None else None,
                    within_seconds=RECENT_INCOMING_TTL_SECONDS,
                )
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("recent_incoming_check").inc()
                log(
                    "event=whitelist_bypass_check status=error reason=db "
                    f"lead_id={lead_id} tenant_id={tenant_id} error={exc}"
                )
            else:
                if recent:
                    log(
                        "event=whitelist_bypass status=allow reason=recent_incoming "
                        f"lead_id={lead_id} tenant_id={tenant_id}"
                    )
                    return True, "recent_incoming"
        if format_error:
            return False, "format"

    return False, "not_found"


def _resolve_channel(item: Mapping[str, Any]) -> str:
    raw_channel = item.get("provider") or item.get("ch") or item.get("channel")
    channel = ""
    if isinstance(raw_channel, str):
        channel = raw_channel.strip().lower()
    elif raw_channel is not None:
        channel = str(raw_channel).strip().lower()
    if channel:
        return channel
    if item.get("max_user_id") is not None:
        return "max"
    if item.get("telegram_user_id") is not None or item.get("peer_id") is not None:
        return "telegram"
    return "whatsapp"


def _is_manager_message(item: Mapping[str, Any]) -> bool:
    origin_raw = item.get("origin")
    origin = origin_raw.strip().lower() if isinstance(origin_raw, str) else ""
    if origin in {"app.send", "dialogs.ui", "client.dialog"}:
        return True

    manager_flag = item.get("manager")
    if isinstance(manager_flag, str):
        manager_flag = manager_flag.strip().lower() in {"1", "true", "yes", "on"}
    if manager_flag:
        return True

    meta = item.get("meta")
    if isinstance(meta, Mapping):
        meta_flag = meta.get("manager")
        if isinstance(meta_flag, str):
            meta_flag = meta_flag.strip().lower() in {"1", "true", "yes", "on"}
        if meta_flag:
            return True
    return False


def _is_followup_message(item: Mapping[str, Any]) -> bool:
    origin_raw = item.get("origin")
    origin = origin_raw.strip().lower() if isinstance(origin_raw, str) else ""
    if origin == "followup":
        return True
    meta = item.get("meta")
    if isinstance(meta, Mapping):
        meta_flag = meta.get("followup")
        if isinstance(meta_flag, str):
            meta_flag = meta_flag.strip().lower() in {"1", "true", "yes", "on"}
        if meta_flag:
            return True
    return False


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
    """
    Detect whether payload carries photo/image attachments.

    We normalize attachments and inspect type/mime to keep channel handlers simple.
    """
    raw_items = list(blobs or [])
    normalized = _normalize_attachments(raw_items)
    for item in normalized:
        type_raw = str(item.get("type") or item.get("kind") or "").strip().lower()
        mime_raw = str(
            item.get("mime")
            or item.get("mime_type")
            or item.get("mimetype")
            or ""
        ).strip().lower()
        if type_raw in {"image", "photo", "picture"} or "photo" in type_raw:
            return True
        if mime_raw.startswith("image/"):
            return True
    # Fallback: detect photo intent even if attachment lacks URL (e.g., temp IDs)
    for blob in raw_items:
        if not isinstance(blob, Mapping):
            continue
        type_raw = str(blob.get("type") or blob.get("kind") or "").strip().lower()
        mime_raw = str(
            blob.get("mime")
            or blob.get("mime_type")
            or blob.get("mimetype")
            or ""
        ).strip().lower()
        if type_raw in {"image", "photo", "picture"} or "photo" in type_raw:
            return True
        if mime_raw.startswith("image/"):
            return True
    return False


def _internal_base_url() -> str:
    return "http://app:8000"


def _is_internal_path(value: str) -> bool:
    trimmed = (value or "").strip()
    if not trimmed:
        return False
    if trimmed.startswith("/internal/"):
        return True
    parsed = urlsplit(trimmed)
    path = parsed.path or ""
    return path.startswith("/internal/")


def _inject_internal_token(query: str) -> str:
    token_value = WA_INTERNAL_TOKEN
    if not token_value:
        return query

    filtered: list[str] = []
    for chunk in query.split("&"):
        if not chunk:
            continue
        key, sep, value = chunk.partition("=")
        if key.lower() == "token":
            continue
        if sep:
            filtered.append(f"{key}{sep}{value}")
        else:
            filtered.append(key)

    filtered.append(f"token={quote(token_value, safe='')}")
    return "&".join(filtered)


def _ensure_inline_hint(url: str) -> str:
    if not url:
        return url
    try:
        parsed = urlsplit(url)
    except Exception:
        return url
    path = (parsed.path or "").lower()
    if "/catalog-file" not in path:
        return url
    existing = parse_qsl(parsed.query, keep_blank_values=True)
    if any(key.lower() == "inline" for key, _ in existing):
        return url
    existing.append(("inline", "1"))
    new_query = urlencode(existing, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))


def _normalize_internal_urls(relative_url: str) -> tuple[str, str]:
    parsed = urlsplit(relative_url)
    query = _inject_internal_token(parsed.query)
    fragment = parsed.fragment

    if parsed.scheme and parsed.netloc:
        absolute = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, fragment))
        path = parsed.path or ""
        relative = urlunsplit(("", "", path, query, fragment))
        if not relative.startswith("/"):
            relative = f"/{relative.lstrip('/')}"
        relative = _ensure_inline_hint(relative)
        absolute = _ensure_inline_hint(absolute)
        return relative, absolute

    path = parsed.path or ""
    if not path.startswith("/"):
        path = f"/{path}"
    relative = urlunsplit(("", "", path, query, fragment))
    relative = _ensure_inline_hint(relative)
    absolute = _ensure_inline_hint(f"{_internal_base_url()}{relative}")
    return relative, absolute


def _parse_disposition_filename(header: str | None) -> str:
    if not header:
        return ""
    match = re.search(r"filename\*=UTF-8''([^;]+)", header, flags=re.IGNORECASE)
    if match and match.group(1):
        try:
            return unquote(match.group(1))
        except Exception:
            return match.group(1)
    match = re.search(r'filename="?([^";]+)"?', header, flags=re.IGNORECASE)
    if match and match.group(1):
        return match.group(1)
    return ""


def _resolve_attachment_filename(
    attachment: Mapping[str, Any],
    headers: Mapping[str, str] | None,
    absolute_url: str,
) -> str:
    for key in ("filename", "name"):
        candidate = attachment.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    disposition = ""
    if headers:
        disposition = headers.get("Content-Disposition") or headers.get("content-disposition") or ""
    candidate = _parse_disposition_filename(disposition)
    if candidate:
        return candidate
    path = urlparse(absolute_url).path
    if path:
        tail = path.rstrip("/").split("/")[-1]
        if tail:
            return unquote(tail)
    return ""


def _resolve_attachment_mime(
    attachment: Mapping[str, Any], headers: Mapping[str, str] | None
) -> str:
    for key in ("mime", "mime_type", "mimetype"):
        candidate = attachment.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    if headers:
        content_type = headers.get("Content-Type") or headers.get("content-type")
        if content_type:
            return content_type.split(";", 1)[0].strip()
    return ""


async def _download_internal_attachment(
    relative_url: str,
) -> tuple[bytes | None, Mapping[str, str] | None, str]:
    normalized_relative, absolute_url = _normalize_internal_urls(relative_url)
    token_value = WA_INTERNAL_TOKEN
    timeout = httpx.Timeout(20.0, connect=5.0)
    final_headers: Mapping[str, str] | None = None
    final_status: int | None = None
    error_label: str | None = None

    header_attempts: list[tuple[str, Mapping[str, str] | None]] = []
    if token_value:
        header_attempts.append(("X-Auth-Token", {"X-Auth-Token": token_value}))
        header_attempts.append(("X-Internal-Token", {"X-Internal-Token": token_value}))
    else:
        header_attempts.append(("", None))

    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt_index, (header_label, headers) in enumerate(header_attempts, start=1):
            log(
                "event=internal_download level=info action=request "
                f"attempt={attempt_index} url={normalized_relative} header={header_label or 'none'}"
            )
            try:
                response = await client.get(absolute_url, headers=headers)
            except httpx.HTTPError as exc:
                error_label = exc.__class__.__name__
                log(
                    "event=internal_download level=info action=error "
                    f"attempt={attempt_index} url={normalized_relative} error={error_label}"
                )
                continue

            final_status = response.status_code
            final_headers = response.headers
            log(
                "event=internal_download level=info action=response "
                f"attempt={attempt_index} url={normalized_relative} status={final_status}"
            )

            if 200 <= response.status_code < 300:
                return response.content, response.headers, absolute_url

            if not (
                token_value
                and response.status_code in {401, 403}
                and header_label == "X-Auth-Token"
            ):
                break

    if final_status is not None or error_label:
        status_hint = error_label or final_status or "error"
        log(
            "event=internal_download level=info action=fetch "
            f"url={normalized_relative} status={status_hint}"
        )

    return None, final_headers, absolute_url


def _prepare_whatsapp_attachment_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    if _is_internal_path(cleaned):
        _, absolute = _normalize_internal_urls(cleaned)
        return absolute
    return cleaned


def _tokenize_attachment_mapping(attachment: Mapping[str, Any]) -> dict[str, Any]:
    prepared = dict(attachment)
    path_value = prepared.get("path")
    if isinstance(path_value, str) and path_value.strip():
        try:
            resolved_path = os.path.abspath(path_value)
            size = os.path.getsize(resolved_path)
            if size >= 0:
                prepared.setdefault("path", resolved_path)
                prepared.setdefault("size", size)
                url_value = prepared.get("url")
                if isinstance(url_value, str) and _is_internal_path(url_value):
                    prepared["internal_url"] = url_value
                    prepared.pop("url", None)
        except OSError:
            pass

    url_value = prepared.get("url")
    if isinstance(url_value, str):
        prepared["url"] = _prepare_whatsapp_attachment_url(url_value)
    for nested_key in ("document", "image", "video", "audio", "voice", "thumbnail"):
        nested_value = prepared.get(nested_key)
        if isinstance(nested_value, Mapping):
            nested_copy = dict(nested_value)
            nested_url = nested_copy.get("url")
            if isinstance(nested_url, str):
                nested_copy["url"] = _prepare_whatsapp_attachment_url(nested_url)
            prepared[nested_key] = nested_copy
    return prepared


async def _prepare_internal_attachment(
    attachment: Mapping[str, Any]
) -> dict[str, Any]:
    if not isinstance(attachment, Mapping):
        return dict(attachment)
    url = attachment.get("url")
    if not isinstance(url, str):
        return dict(attachment)
    trimmed = url.strip()
    if not _is_internal_path(trimmed):
        return _tokenize_attachment_mapping(attachment)

    data, headers, absolute_url = await _download_internal_attachment(trimmed)
    prepared = dict(attachment)
    prepared["url"] = absolute_url

    if data is None:
        return _tokenize_attachment_mapping(prepared)

    filename = _resolve_attachment_filename(prepared, headers, absolute_url)
    if filename:
        prepared["filename"] = filename
        prepared.setdefault("name", filename)

    mime = _resolve_attachment_mime(prepared, headers)
    if mime:
        prepared["mime"] = mime
        prepared["mime_type"] = mime
        prepared["mimetype"] = mime

    prepared["type"] = str(prepared.get("type") or "document")
    prepared["sendMediaAsDocument"] = True
    prepared.setdefault("size", len(data))

    inline_limit_mb = float(os.getenv("WA_INLINE_ATTACHMENT_LIMIT_MB", "8") or "0")
    inline_limit_bytes = 0
    if inline_limit_mb > 0:
        inline_limit_bytes = int(inline_limit_mb * 1024 * 1024)

    working_data = data
    if (
        inline_limit_bytes
        and len(working_data) > inline_limit_bytes
        and isinstance(mime, str)
        and "pdf" in mime.lower()
    ):
        compressed = _compress_pdf_bytes(working_data, filename or "catalog.pdf", inline_limit_bytes)
        if compressed and len(compressed) < len(working_data):
            working_data = compressed
            prepared["size"] = len(working_data)

    if inline_limit_bytes and len(working_data) > inline_limit_bytes:
        # too large for inlining; let waweb download via URL
        document_meta = {
            "url": absolute_url,
        }
        if filename:
            document_meta["filename"] = filename
        fallback_mime = "application/octet-stream"
        prepared["mime"] = fallback_mime
        prepared["mime_type"] = fallback_mime
        prepared["mimetype"] = fallback_mime
        document_meta["mime"] = fallback_mime
        caption_value = prepared.get("caption") or prepared.get("text")
        if isinstance(caption_value, str) and caption_value.strip():
            document_meta["caption"] = caption_value.strip()
        prepared.pop("b64", None)
        prepared["document"] = document_meta
        prepared["source"] = "url"
        return _tokenize_attachment_mapping(prepared)

    prepared["size"] = len(working_data)
    prepared["b64"] = base64.b64encode(working_data).decode("ascii")
    return _tokenize_attachment_mapping(prepared)


def _build_wa_document_payload(
    attachment: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not isinstance(attachment, Mapping):
        return None, None

    path_value = attachment.get("path")
    if isinstance(path_value, str) and path_value.strip():
        # Если передан локальный путь к файлу, оставляем вложение без изменений,
        # чтобы waweb использовал доступ к файлу напрямую (без скачивания по URL).
        return None, None

    attachment_type = str(attachment.get("type") or attachment.get("kind") or "").strip().lower()
    if attachment_type and attachment_type not in {"document", "file"}:
        return None, None

    def _first_text(*keys: str) -> str:
        for key in keys:
            value = attachment.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    url = _first_text("url", "href", "document", "file", "path")
    if not url:
        return None, None

    filename = _first_text("filename", "name", "title")
    mime = _first_text("mime", "mime_type", "mimetype", "content_type")
    caption = _first_text("caption", "text", "description")

    document_block: dict[str, Any] = {"url": url}
    if filename:
        document_block["filename"] = filename
    if mime:
        document_block["mime"] = mime
    if caption:
        document_block["caption"] = caption

    wa_attachment: dict[str, Any] = {
        "type": "document",
        "document": dict(document_block),
        "url": url,
    }

    if filename:
        wa_attachment["filename"] = filename
        wa_attachment.setdefault("name", filename)
    if mime:
        wa_attachment["mime"] = mime
        wa_attachment.setdefault("mime_type", mime)
        wa_attachment.setdefault("mimetype", mime)
    if caption:
        wa_attachment["caption"] = caption

    if attachment.get("b64"):
        wa_attachment["b64"] = attachment.get("b64")
    if attachment.get("sendMediaAsDocument") is not None:
        wa_attachment["sendMediaAsDocument"] = attachment.get("sendMediaAsDocument")
    if attachment.get("source"):
        wa_attachment["source"] = attachment.get("source")

    size_value = attachment.get("size")
    try:
        size_int = int(size_value) if size_value is not None else None
    except Exception:
        size_int = None
    if size_int is not None and size_int >= 0:
        wa_attachment["size"] = size_int

    return wa_attachment, document_block


async def _handle_telegram_incoming(event: Mapping[str, Any]) -> None:
    tenant_raw = event.get("tenant") or event.get("tenant_id")
    try:
        tenant_id = int(tenant_raw) if tenant_raw is not None else 0
    except Exception:
        tenant_id = 0

    if tenant_id <= 0:
        log("event=skip_invalid_tenant channel=telegram tenant_raw=%s" % tenant_raw)
        return
    nested_message = event.get("message") if isinstance(event.get("message"), Mapping) else {}
    tg_slot = _normalize_tg_slot(
        event.get("tg_slot")
        or nested_message.get("tg_slot")
        or event.get("slot")
    )

    text_raw = event.get("text")
    text = "" if text_raw is None else str(text_raw)
    text = text.strip()

    raw_attachment_items: list[Mapping[str, Any]] = []
    raw_attachments = event.get("attachments") if isinstance(event.get("attachments"), list) else []
    raw_attachment_items.extend(item for item in raw_attachments if isinstance(item, Mapping))
    single_attachment = event.get("attachment")
    if isinstance(single_attachment, Mapping):
        raw_attachment_items.append(single_attachment)
    media_field = event.get("media")
    if isinstance(media_field, list):
        raw_attachment_items.extend(item for item in media_field if isinstance(item, Mapping))
    elif isinstance(media_field, Mapping):
        raw_attachment_items.append(media_field)
    photo_field = event.get("photo")
    if isinstance(photo_field, list):
        raw_attachment_items.extend(item for item in photo_field if isinstance(item, Mapping))
    elif isinstance(photo_field, Mapping):
        raw_attachment_items.append(photo_field)
    attachments = normalize_message_attachments(raw_attachment_items)
    message_kind = str(event.get("message_kind") or detect_message_kind(text, attachments)).strip().lower() or "text"
    has_photo = any(str(item.get("type") or "").strip().lower() == "image" for item in attachments)

    message_id_raw = event.get("message_id")
    message_id = str(message_id_raw) if message_id_raw is not None else ""
    try:
        message_id_int = int(message_id) if message_id else None
    except Exception:
        message_id_int = None

    telegram_user_id = _coerce_int(event.get("telegram_user_id"))
    peer_id = _coerce_int(event.get("peer_id"))
    peer_raw = event.get("peer")
    peer_value: Optional[str] = None
    if isinstance(peer_raw, str):
        peer_value = peer_raw.strip() or None
    elif peer_raw is not None:
        peer_value = str(peer_raw).strip() or None
    if peer_value and peer_id is None:
        try:
            peer_id = int(peer_value)
        except Exception:
            peer_id = None
    username_raw = event.get("username")
    username = None
    if username_raw is not None:
        username = str(username_raw).strip() or None
    display_name_raw = event.get("display_name")
    if display_name_raw is None and isinstance(nested_message, Mapping):
        display_name_raw = nested_message.get("display_name")
    display_name = sanitize_display_name(display_name_raw)

    lead_candidate = _coerce_int(event.get("lead_id"))
    lead_id = lead_candidate if lead_candidate and lead_candidate > 0 else 0

    title_hint: Optional[str] = None
    normalized_username = normalize_username(username)
    if normalized_username:
        title_hint = normalized_username
    elif display_name:
        title_hint = display_name
    elif telegram_user_id is not None:
        title_hint = f"tg:id {telegram_user_id}"
    elif peer_id is not None:
        title_hint = f"tg:id {peer_id}"

    resolved_lead_id: Optional[int] = lead_id if lead_id > 0 else None
    if resolved_lead_id is None and telegram_user_id is not None:
        try:
            found_lead = await find_lead_by_telegram(tenant_id, int(telegram_user_id))
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("find_lead_by_telegram").inc()
            log(
                "event=inbox_lead_lookup_failed channel=telegram tenant=%s error=%s"
                % (tenant_id, exc)
            )
            found_lead = None
        if found_lead and found_lead > 0:
            resolved_lead_id = int(found_lead)

    contact_hint = display_name or normalized_username or username

    upsert_kwargs: Dict[str, Any] = {
        "channel": "telegram",
        "tenant_id": tenant_id,
        "peer_id": peer_id,
        "peer": peer_value,
        "contact": contact_hint,
        "title": title_hint,
        "telegram_username": username,
    }
    if telegram_user_id is not None:
        upsert_kwargs["telegram_user_id"] = int(telegram_user_id)

    try:
        upsert_key: Optional[int] = resolved_lead_id if resolved_lead_id else None
        upsert_result = await upsert_lead(upsert_key, **upsert_kwargs)
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("upsert_lead").inc()
        log(
            "event=inbox_lead_upsert_failed channel=telegram tenant=%s error=%s"
            % (tenant_id, exc)
        )
        return

    if upsert_result is not None:
        try:
            resolved_lead_id = int(upsert_result)
        except Exception:
            resolved_lead_id = None

    if resolved_lead_id is None and telegram_user_id is not None:
        resolved_lead_id = int(telegram_user_id)

    lead_id = resolved_lead_id if resolved_lead_id is not None else 0
    await _store_lead_tg_slot(tenant_id, lead_id, tg_slot)

    peer_log_hint = peer_value or (str(peer_id) if peer_id is not None else None)
    if peer_log_hint is None and telegram_user_id is not None:
        peer_log_hint = str(telegram_user_id)
    log(
        f"event=inbox_lead_resolved channel=telegram tenant={tenant_id} slot={tg_slot} lead_id={lead_id} peer={peer_log_hint or '-'}"
    )

    if lead_id <= 0:
        log(
            f"event=skip_missing_lead channel=telegram tenant={tenant_id} message_id={message_id}"
        )
        return

    if not _telegram_slot_is_enabled(tenant_id, tg_slot):
        log(
            f"event=telegram_slot_disabled channel=telegram tenant={tenant_id} slot={tg_slot} lead_id={lead_id}"
        )
        return

    # Никогда не отвечаем и не обрабатываем сообщения от бота уведомлений.
    if NOTIFY_BOT_ID and telegram_user_id and int(telegram_user_id) == int(NOTIFY_BOT_ID):
        log(
            f"event=skip_notify_bot channel=telegram tenant={tenant_id} lead_id={lead_id} peer={peer_log_hint or '-'}"
        )
        return

    manager_outgoing = _looks_like_manager_outgoing(event) or _is_manager_message(event)
    trigger_bot = bool(event.get("trigger_bot")) if "trigger_bot" in event else not manager_outgoing

    if text and not manager_outgoing:
        try:
            if await followups.handle_opt_out(tenant_id, lead_id, text):
                await _cancel_pending_smart_reply(
                    tenant_id,
                    "telegram",
                    lead_id,
                    reason="followup_optout",
                )
                log(
                    "event=followup_optout channel=telegram tenant=%s lead_id=%s",
                    tenant_id,
                    lead_id,
                )
                return
            await followups.capture_followup_answer(tenant_id, lead_id, text, "telegram")
        except Exception as exc:
            log(
                "event=followup_capture_warn channel=telegram tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    if not manager_outgoing:
        try:
            await _maybe_amocrm_inbound(
                tenant_id,
                lead_id,
                text,
                "telegram",
                attachments=attachments,
                message_id=message_id_int,
            )
        except Exception as exc:
            log(
                "event=amocrm_inbound_failed channel=telegram tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    if not trigger_bot:
        log(
            "event=incoming_skip_trigger channel=telegram tenant=%s lead_id=%s author_kind=%s",
            tenant_id,
            lead_id,
            str(event.get("author_kind") or ""),
        )
        return

    try:
        await followups.schedule_followups(tenant_id, lead_id, "telegram")
    except Exception as exc:
        log(
            f"event=followup_schedule_warn channel=telegram tenant={tenant_id} lead_id={lead_id} error={exc}"
        )

    # Try to merge with existing contact by phone (from cache or linked leads).
    existing_contact_id = None
    try:
        existing_contact_id = await get_contact_id_by_lead(lead_id)
    except Exception:
        existing_contact_id = None

    # Phone scoped to lead/chat to avoid races.
    phone_norm: str | None = None
    if lead_id:
        try:
            phone_candidate = await r.get(f"cache:lead_phone:{tenant_id}:{lead_id}")
            if phone_candidate and str(phone_candidate).strip():
                phone_norm = (
                    phone_candidate.decode()
                    if isinstance(phone_candidate, (bytes, bytearray))
                    else str(phone_candidate).strip()
                )
        except Exception:
            phone_norm = None
    if not phone_norm and peer_value:
        try:
            phone_candidate = await r.get(f"cache:avito_phone:{tenant_id}:{peer_value}")
            if phone_candidate and str(phone_candidate).strip():
                phone_norm = (
                    phone_candidate.decode()
                    if isinstance(phone_candidate, (bytes, bytearray))
                    else str(phone_candidate).strip()
                )
        except Exception:
            phone_norm = None

    # Skip phone linking for the notify bot itself.
    if phone_norm and NOTIFY_BOT_ID and telegram_user_id and int(telegram_user_id) == int(NOTIFY_BOT_ID):
        log(
            f"event=telegram_contact_link_skip reason=notify_bot tenant={tenant_id} lead_id={lead_id} phone={phone_norm}"
        )
        phone_norm = None

    merged_contact_id = None
    if phone_norm:
        try:
            phone_owner_id = None
            try:
                phone_owner_id = await get_contact_id_by_phone(phone_norm)
            except Exception:
                phone_owner_id = None
            target_contact_id = phone_owner_id or await resolve_or_create_contact(
                phone=phone_norm,
                whatsapp_phone=phone_norm,
                telegram_user_id=telegram_user_id,
                telegram_username=username,
            )
            if existing_contact_id and existing_contact_id != target_contact_id:
                await link_lead_contact(
                    lead_id,
                    target_contact_id,
                    channel="telegram",
                    peer=peer_value or peer_log_hint,
                )
                await update_contact_telegram(target_contact_id, telegram_user_id, username)
                log(
                    f"event=telegram_contact_relinked_by_phone tenant={tenant_id} lead_id={lead_id} from_contact={existing_contact_id} to_contact={target_contact_id} phone={phone_norm}"
                )
            else:
                if existing_contact_id:
                    await update_contact_telegram(existing_contact_id, telegram_user_id, username)
                await update_contact_phone(target_contact_id, phone_norm)
                await link_lead_contact(
                    lead_id,
                    target_contact_id,
                    channel="telegram",
                    peer=peer_value or peer_log_hint,
                )
                log(
                    f"event=telegram_contact_linked_by_phone tenant={tenant_id} lead_id={lead_id} contact_id={target_contact_id} phone={phone_norm}"
                )
        except Exception as exc:
            log(
                f"event=telegram_contact_link_failed tenant={tenant_id} lead_id={lead_id} phone={phone_norm} error={exc}"
            )
            merged_contact_id = None
    elif existing_contact_id:
        try:
            await update_contact_telegram(existing_contact_id, telegram_user_id, username)
        except Exception:
            pass

    # Поведение по триггерам (фразы → тишина/уведомление).
    if text:
        trigger_rule = _match_behavior_trigger(tenant_id, "telegram", text)
        if trigger_rule and trigger_rule.get("silence", True):
            notify_flag = bool(trigger_rule.get("notify"))
            await _mark_handoff_silence(
                tenant_id,
                lead_id,
                reason="trigger_match",
                contact_hint=peer_log_hint or peer_value or contact_hint,
                username_hint=username,
                notify=notify_flag,
            )
            log(
                f"event=trigger_match channel=telegram tenant={tenant_id} lead_id={lead_id} notify={int(notify_flag)} phrases={trigger_rule.get('phrases')}"
            )
            await _cancel_pending_smart_reply(
                tenant_id,
                "telegram",
                lead_id,
                reason="trigger_silence",
            )
            return

    # Фото-ожидание: если пришло фото/вложение и мы ждали — отвечаем и не ставим тишину.
    if has_photo or message_kind in {"image", "video", "voice", "file", "mixed"}:
        markers, photo_reply, photo_ttl = _photo_expectation_config(tenant_id)
        state_key = f"conv:state:{tenant_id}:{lead_id}"
        waiting_photo = False
        try:
            state_val = await r.get(state_key)
            if isinstance(state_val, str) and state_val == "waiting_photo":
                waiting_photo = True
        except Exception:
            waiting_photo = False
        if waiting_photo:
            if photo_reply and photo_reply.strip():
                out_payload = {
                    "lead_id": int(lead_id),
                    "tenant": int(tenant_id),
                    "tenant_id": int(tenant_id),
                    "provider": "telegram",
                    "ch": "telegram",
                    "channel": "telegram",
                    "text": photo_reply.strip(),
                    "attachments": [],
                    "peer": peer_value or peer_log_hint,
                    "peer_id": peer_value or peer_log_hint,
                    "tg_slot": tg_slot,
                }
                try:
                    await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(out_payload, ensure_ascii=False))
                    log(
                        f"event=photo_expected_reply_sent tenant={tenant_id} lead_id={lead_id} peer={peer_log_hint or '-'}"
                    )
                except Exception as exc:
                    log(
                        f"event=photo_expected_reply_failed tenant={tenant_id} lead_id={lead_id} error={exc}"
                    )
            # Уведомим менеджера, но без постановки тишины.
            try:
                await _notify_manager_handoff(
                    int(tenant_id),
                    int(lead_id),
                    reason="photo_received",
                    contact_hint=peer_log_hint or peer_value or contact_hint,
                    username_hint=username,
                )
            except Exception:
                pass
            try:
                await r.delete(state_key)
            except Exception:
                pass
            await _cancel_pending_smart_reply(
                tenant_id,
                "telegram",
                lead_id,
                reason="photo_expected_reply",
            )
            return

    if attachments:
        log(
            f"event=incoming_attachments channel=telegram tenant={tenant_id} lead_id={lead_id} count={len(attachments)} has_photo={int(has_photo)}"
        )

    if has_photo or message_kind in {"image", "video", "voice", "file", "mixed"}:
        await _mark_handoff_silence(
            tenant_id,
            lead_id,
            reason="photo_received",
            contact_hint=peer_log_hint or peer_value or contact_hint,
            username_hint=username,
        )
        log(
            f"event=handoff_marked channel=telegram tenant={tenant_id} lead_id={lead_id} reason=photo_received"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "telegram",
            lead_id,
            reason="photo_received",
        )
        return

    if manager_outgoing:
        await _mark_handoff_silence(
            tenant_id,
            lead_id,
            reason="manager_outgoing",
            contact_hint=peer_log_hint or peer_value or contact_hint,
            username_hint=username,
        )
        log(
            f"event=handoff_marked channel=telegram tenant={tenant_id} lead_id={lead_id} reason=manager_outgoing"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "telegram",
            lead_id,
            reason="manager_outgoing",
        )
        return

    silenced = await _is_handoff_silenced(tenant_id, lead_id)
    if silenced:
        log(
            f"event=smart_reply_silenced channel=telegram tenant={tenant_id} lead_id={lead_id}"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "telegram",
            lead_id,
            reason="silenced",
        )
        return

    # Link telegram lead to existing contact by phone (if known from other channels/cache).
    try:
        existing_phone = await get_contact_phone_by_lead(lead_id)
    except Exception:
        existing_phone = None
    merged_contact_id = None
    if not existing_phone:
        cached_phone_norm: str | None = None
        try:
            cached_phone = await r.get(f"cache:lead_phone:{tenant_id}:{lead_id}")
            if cached_phone and str(cached_phone).strip():
                cached_phone_norm = (
                    cached_phone.decode()
                    if isinstance(cached_phone, (bytes, bytearray))
                    else str(cached_phone).strip()
                )
        except Exception:
            cached_phone_norm = None
        if not cached_phone_norm and peer_value:
            try:
                cached_phone = await r.get(f"cache:avito_phone:{tenant_id}:{peer_value}")
                if cached_phone and str(cached_phone).strip():
                    cached_phone_norm = (
                        cached_phone.decode()
                        if isinstance(cached_phone, (bytes, bytearray))
                        else str(cached_phone).strip()
                    )
            except Exception:
                cached_phone_norm = None
        if (
            cached_phone_norm
            and NOTIFY_BOT_ID
            and telegram_user_id
            and int(telegram_user_id) == int(NOTIFY_BOT_ID)
        ):
            cached_phone_norm = None
        if cached_phone_norm:
            try:
                merged_contact_id = await resolve_or_create_contact(phone=cached_phone_norm)
                await link_lead_contact(
                    lead_id,
                    merged_contact_id,
                    channel="telegram",
                    peer=peer_value or peer_log_hint,
                )
                await update_contact_phone(merged_contact_id, cached_phone_norm)
                log(
                    f"event=telegram_contact_linked_by_phone tenant={tenant_id} lead_id={lead_id} contact_id={merged_contact_id} phone={cached_phone_norm}"
                )
            except Exception:
                merged_contact_id = None


    if not text:
        log(
            f"event=skip_no_text channel=telegram tenant={tenant_id} lead_id={lead_id}"
        )
        return

    if not _telegram_reply_enabled(tenant_id):
        log(
            f"event=telegram_reply_disabled channel=telegram tenant={tenant_id} lead_id={lead_id}"
        )
        return
    if not smart_reply_enabled(tenant_id):
        log(
            f"event=smart_reply_disabled channel=telegram tenant={tenant_id} lead_id={lead_id}"
        )
        return

    contact_id = _coerce_int(event.get("contact_id"))
    refer_id = contact_id if contact_id and contact_id > 0 else lead_id

    reply_context = {
        "tg_slot": tg_slot,
        "message_id": message_id,
        "telegram_user_id": telegram_user_id,
        "peer_id": peer_id,
        "username": username,
    }
    delayed = await _try_handle_smart_reply_with_delay(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel="telegram",
        refer_id=refer_id,
        user_text=text,
        context=reply_context,
    )
    if delayed:
        return
    await _produce_and_enqueue_smart_reply(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel="telegram",
        refer_id=refer_id,
        user_text=text,
        context=reply_context,
        delayed=False,
    )


async def _handle_max_incoming(event: Mapping[str, Any]) -> None:
    tenant_raw = event.get("tenant") or event.get("tenant_id")
    try:
        tenant_id = int(tenant_raw) if tenant_raw is not None else 0
    except Exception:
        tenant_id = 0

    if tenant_id <= 0:
        log("event=skip_invalid_tenant channel=max tenant_raw=%s" % tenant_raw)
        return

    text_raw = event.get("text")
    text = "" if text_raw is None else str(text_raw)
    text = text.strip()

    raw_attachment_items: list[Mapping[str, Any]] = []
    raw_attachments = event.get("attachments") if isinstance(event.get("attachments"), list) else []
    raw_attachment_items.extend(item for item in raw_attachments if isinstance(item, Mapping))
    single_attachment = event.get("attachment")
    if isinstance(single_attachment, Mapping):
        raw_attachment_items.append(single_attachment)
    media_field = event.get("media")
    if isinstance(media_field, list):
        raw_attachment_items.extend(item for item in media_field if isinstance(item, Mapping))
    elif isinstance(media_field, Mapping):
        raw_attachment_items.append(media_field)
    photo_field = event.get("photo")
    if isinstance(photo_field, list):
        raw_attachment_items.extend(item for item in photo_field if isinstance(item, Mapping))
    elif isinstance(photo_field, Mapping):
        raw_attachment_items.append(photo_field)
    attachments = normalize_message_attachments(raw_attachment_items)
    has_photo = any(str(item.get("type") or "").strip().lower() == "image" for item in attachments)

    message_id_raw = event.get("message_id") or event.get("id")
    message_id = str(message_id_raw) if message_id_raw is not None else ""

    max_user_id = _coerce_int(event.get("max_user_id") or event.get("user_id"))
    peer_raw = event.get("peer") or event.get("chat_id") or event.get("peer_id")
    peer_value: Optional[str] = None
    if isinstance(peer_raw, str):
        peer_value = peer_raw.strip() or None
    elif peer_raw is not None:
        peer_value = str(peer_raw).strip() or None
    if not peer_value and max_user_id is not None:
        peer_value = str(max_user_id)
    peer_id = _coerce_int(peer_value)

    username_raw = event.get("max_username") or event.get("username")
    username = None
    if username_raw is not None:
        username = str(username_raw).strip() or None
    display_name_raw = event.get("display_name") or event.get("name")
    display_name = sanitize_display_name(display_name_raw)

    lead_candidate = _coerce_int(event.get("lead_id"))
    lead_hint = lead_candidate if lead_candidate and lead_candidate > 0 else None

    title_hint: Optional[str] = None
    normalized_username = normalize_username(username)
    if display_name:
        title_hint = display_name
    elif normalized_username:
        title_hint = f"max:{normalized_username}"
    elif max_user_id is not None:
        title_hint = f"max:id {max_user_id}"

    contact_hint = display_name or normalized_username or username

    resolved_lead_id: Optional[int] = lead_hint
    if not resolved_lead_id and peer_value:
        try:
            lead_lookup = await get_or_create_by_peer(
                tenant_id=tenant_id,
                channel="max",
                peer=peer_value,
                lead_id_hint=lead_hint,
            )
            resolved_lead_id = int(lead_lookup)
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("get_or_create_lead_peer").inc()
            log(
                "event=inbox_lead_resolve_failed channel=max tenant=%s error=%s"
                % (tenant_id, exc)
            )
            resolved_lead_id = None

    if resolved_lead_id is None and max_user_id is not None:
        resolved_lead_id = int(max_user_id)
    if resolved_lead_id is None:
        resolved_lead_id = int(time.time() * 1000)

    lead_id = int(resolved_lead_id)

    try:
        await upsert_lead(
            lead_id,
            channel="max",
            tenant_id=tenant_id,
            peer=peer_value,
            contact=contact_hint,
            title=title_hint,
        )
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("upsert_lead").inc()
        log(
            "event=inbox_lead_upsert_failed channel=max tenant=%s error=%s"
            % (tenant_id, exc)
        )
        return

    peer_log_hint = peer_value or (str(peer_id) if peer_id is not None else None)
    log(
        f"event=inbox_lead_resolved channel=max tenant={tenant_id} lead_id={lead_id} peer={peer_log_hint or '-'}"
    )

    manager_outgoing = _looks_like_manager_outgoing(event) or _is_manager_message(event)

    if text and not manager_outgoing:
        try:
            if await followups.handle_opt_out(tenant_id, lead_id, text):
                await _cancel_pending_smart_reply(
                    tenant_id,
                    "max",
                    lead_id,
                    reason="followup_optout",
                )
                log(
                    "event=followup_optout channel=max tenant=%s lead_id=%s",
                    tenant_id,
                    lead_id,
                )
                return
            await followups.capture_followup_answer(tenant_id, lead_id, text, "max")
        except Exception as exc:
            log(
                "event=followup_capture_warn channel=max tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    if not manager_outgoing:
        try:
            await _maybe_amocrm_inbound(
                tenant_id,
                lead_id,
                text,
                "max",
                attachments=attachments,
                message_id=message_id if isinstance(message_id, int) else None,
            )
        except Exception as exc:
            log(
                "event=amocrm_inbound_failed channel=max tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    try:
        await followups.schedule_followups(tenant_id, lead_id, "max")
    except Exception as exc:
        log(
            f"event=followup_schedule_warn channel=max tenant={tenant_id} lead_id={lead_id} error={exc}"
        )

    contact_id = 0
    try:
        contact_id = await resolve_or_create_contact(
            max_user_id=max_user_id,
            max_username=username,
        )
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("resolve_or_create_contact").inc()
        log(
            "event=contact_resolve_failed channel=max tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )
        contact_id = 0

    if contact_id:
        try:
            await link_lead_contact(
                lead_id,
                contact_id,
                channel="max",
                peer=peer_value or "",
            )
            await update_contact_max(contact_id, max_user_id, username)
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("link_lead_contact").inc()
            log(
                "event=link_lead_contact_failed channel=max tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    incoming_text = text_or_placeholder(text, attachments)
    if incoming_text:
        try:
            await insert_message_in(
                lead_id,
                incoming_text,
                status="received",
                tenant_id=tenant_id,
            )
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("insert_message_in").inc()
            log(
                "event=store_incoming_failed channel=max tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    # Поведение по триггерам (фразы → тишина/уведомление).
    if text:
        trigger_rule = _match_behavior_trigger(tenant_id, "max", text)
        if trigger_rule and trigger_rule.get("silence", True):
            notify_flag = bool(trigger_rule.get("notify"))
            await _mark_handoff_silence(
                tenant_id,
                lead_id,
                reason="trigger_match",
                contact_hint=peer_log_hint or peer_value or contact_hint,
                username_hint=username,
                notify=notify_flag,
            )
            log(
                f"event=trigger_match channel=max tenant={tenant_id} lead_id={lead_id} notify={int(notify_flag)} phrases={trigger_rule.get('phrases')}"
            )
            await _cancel_pending_smart_reply(
                tenant_id,
                "max",
                lead_id,
                reason="trigger_silence",
            )
            return

    # Фото-ожидание: если пришло фото/вложение и мы ждали — отвечаем и не ставим тишину.
    if has_photo or attachments:
        markers, photo_reply, photo_ttl = _photo_expectation_config(tenant_id)
        state_key = f"conv:state:{tenant_id}:{lead_id}"
        waiting_photo = False
        try:
            state_val = await r.get(state_key)
            if isinstance(state_val, str) and state_val == "waiting_photo":
                waiting_photo = True
        except Exception:
            waiting_photo = False
        if waiting_photo:
            if photo_reply and photo_reply.strip():
                out_payload = {
                    "lead_id": int(lead_id),
                    "tenant": int(tenant_id),
                    "tenant_id": int(tenant_id),
                    "provider": "max",
                    "ch": "max",
                    "channel": "max",
                    "text": photo_reply.strip(),
                    "attachments": [],
                    "peer": peer_value or peer_log_hint,
                    "peer_id": peer_value or peer_log_hint,
                }
                try:
                    await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(out_payload, ensure_ascii=False))
                    log(
                        f"event=photo_expected_reply_sent tenant={tenant_id} lead_id={lead_id} peer={peer_log_hint or '-'}"
                    )
                except Exception as exc:
                    log(
                        f"event=photo_expected_reply_failed channel=max tenant={tenant_id} lead_id={lead_id} error={exc}"
                    )
            try:
                await _notify_manager_handoff(
                    int(tenant_id),
                    int(lead_id),
                    reason="photo_received",
                    contact_hint=peer_log_hint or peer_value or contact_hint,
                    username_hint=username,
                )
            except Exception:
                pass
            try:
                await r.delete(state_key)
            except Exception:
                pass
            await _cancel_pending_smart_reply(
                tenant_id,
                "max",
                lead_id,
                reason="photo_expected_reply",
            )
            return

    if attachments:
        log(
            f"event=incoming_attachments channel=max tenant={tenant_id} lead_id={lead_id} count={len(attachments)} has_photo={int(has_photo)}"
        )

    if has_photo or attachments:
        await _mark_handoff_silence(
            tenant_id,
            lead_id,
            reason="photo_received",
            contact_hint=peer_log_hint or peer_value or contact_hint,
            username_hint=username,
        )
        if attachments:
            await _maybe_amocrm_inbound(tenant_id, lead_id, text, "max", attachments=attachments)
        log(
            f"event=handoff_marked channel=max tenant={tenant_id} lead_id={lead_id} reason=photo_received"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "max",
            lead_id,
            reason="photo_received",
        )
        return

    if manager_outgoing:
        await _mark_handoff_silence(
            tenant_id,
            lead_id,
            reason="manager_outgoing",
            contact_hint=peer_log_hint or peer_value or contact_hint,
            username_hint=username,
        )
        log(
            f"event=handoff_marked channel=max tenant={tenant_id} lead_id={lead_id} reason=manager_outgoing"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "max",
            lead_id,
            reason="manager_outgoing",
        )
        return

    silenced = await _is_handoff_silenced(tenant_id, lead_id)
    if silenced:
        log(
            f"event=smart_reply_silenced channel=max tenant={tenant_id} lead_id={lead_id}"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "max",
            lead_id,
            reason="silenced",
        )
        return

    if not text:
        log(
            f"event=skip_no_text channel=max tenant={tenant_id} lead_id={lead_id}"
        )
        return

    if not _max_reply_enabled(tenant_id):
        log(
            f"event=max_reply_disabled channel=max tenant={tenant_id} lead_id={lead_id}"
        )
        return
    if not smart_reply_enabled(tenant_id):
        log(
            f"event=smart_reply_disabled channel=max tenant={tenant_id} lead_id={lead_id}"
        )
        return

    refer_id = contact_id if contact_id and contact_id > 0 else lead_id

    reply_context = {
        "message_id": message_id,
        "max_user_id": max_user_id,
        "peer": peer_value or peer_log_hint,
    }
    delayed = await _try_handle_smart_reply_with_delay(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel="max",
        refer_id=refer_id,
        user_text=text,
        context=reply_context,
    )
    if delayed:
        return
    await _produce_and_enqueue_smart_reply(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel="max",
        refer_id=refer_id,
        user_text=text,
        context=reply_context,
        delayed=False,
    )


async def _handle_whatsapp_incoming(event: Mapping[str, Any]) -> None:
    tenant_raw = event.get("tenant") or event.get("tenant_id") or os.getenv("TENANT_ID", "1")
    try:
        tenant_id = int(tenant_raw)
    except Exception:
        tenant_id = int(os.getenv("TENANT_ID", "1"))

    if event.get("auto_reply_handled"):
        log(
            f"event=incoming_skip_auto_handled channel=whatsapp tenant={tenant_id}"
        )
        return

    message_id_raw = event.get("message_id")
    message_id = str(message_id_raw) if message_id_raw is not None else ""

    sender_raw = (
        event.get("from_jid")
        or event.get("from")
        or event.get("from_raw")
        or event.get("sender")
    )

    if _is_whatsapp_group(sender_raw):
        log(
            f"event=skip_group_message channel=whatsapp tenant={tenant_id} message_id={message_id or '-'} from={sender_raw or '-'}"
        )
        return

    sender_peer = _normalize_whatsapp_peer(sender_raw)
    if not sender_peer:
        log(
            f"event=skip_invalid_sender channel=whatsapp tenant={tenant_id} message_id={message_id}"
        )
        return

    peer_local = sender_peer.split("@", 1)[0]
    sender_digits = str(event.get("from_digits") or "").strip()
    if not sender_digits:
        sender_digits = _digits(peer_local)

    raw_attachment_items: list[Mapping[str, Any]] = []
    raw_attachments = event.get("attachments") if isinstance(event.get("attachments"), list) else []
    raw_attachment_items.extend(item for item in raw_attachments if isinstance(item, Mapping))
    single_attachment = event.get("attachment")
    if isinstance(single_attachment, Mapping):
        raw_attachment_items.append(single_attachment)
    media_field = event.get("media")
    if isinstance(media_field, list):
        raw_attachment_items.extend(item for item in media_field if isinstance(item, Mapping))
    elif isinstance(media_field, Mapping):
        raw_attachment_items.append(media_field)
    photo_field = event.get("photo")
    if isinstance(photo_field, list):
        raw_attachment_items.extend(item for item in photo_field if isinstance(item, Mapping))
    elif isinstance(photo_field, Mapping):
        raw_attachment_items.append(photo_field)
    attachments = normalize_message_attachments(raw_attachment_items)
    has_photo = any(str(item.get("type") or "").strip().lower() == "image" for item in attachments)

    text_raw = event.get("text")
    text = "" if text_raw is None else str(text_raw)
    text = text.strip()

    conversation_id = _coerce_int(event.get("conversation_id"))
    lead_hint = _coerce_int(event.get("lead_id"))
    if lead_hint is not None and lead_hint <= 0:
        lead_hint = None
    if lead_hint is None and conversation_id and conversation_id > 0:
        lead_hint = conversation_id
    source_real_id = conversation_id if conversation_id and conversation_id > 0 else None

    db_available = True
    fallback_lead = None
    if lead_hint and lead_hint > 0:
        fallback_lead = lead_hint
    elif sender_digits:
        try:
            fallback_lead = int(sender_digits)
        except Exception:
            fallback_lead = None
    if fallback_lead is None:
        fallback_lead = int(time.time() * 1000)

    try:
        lead_lookup = await get_or_create_by_peer(
            tenant_id=tenant_id,
            channel="whatsapp",
            peer=sender_peer,
            lead_id_hint=lead_hint,
            source_real_id=source_real_id,
        )
        lead_id = int(lead_lookup)
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("get_or_create_lead_peer").inc()
        log(
            "event=inbox_lead_resolve_failed channel=whatsapp tenant=%s error=%s fallback=%s"
            % (tenant_id, exc, fallback_lead)
        )
        db_available = False
        lead_id = int(fallback_lead or int(time.time() * 1000))

    if lead_id <= 0:
        log(
            f"event=skip_missing_lead channel=whatsapp tenant={tenant_id} message_id={message_id}"
        )
        return

    log(
        f"event=inbox_lead_resolved channel=whatsapp tenant={tenant_id} lead_id={lead_id}"
    )

    if text:
        try:
            if await followups.handle_opt_out(tenant_id, lead_id, text):
                await _cancel_pending_smart_reply(
                    tenant_id,
                    "whatsapp",
                    lead_id,
                    reason="followup_optout",
                )
                log(
                    "event=followup_optout channel=whatsapp tenant=%s lead_id=%s",
                    tenant_id,
                    lead_id,
                )
                return
            await followups.capture_followup_answer(tenant_id, lead_id, text, "whatsapp")
        except Exception as exc:
            log(
                "event=followup_capture_warn channel=whatsapp tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    try:
        await followups.schedule_followups(tenant_id, lead_id, "whatsapp")
    except Exception as exc:
        log(f"event=followup_schedule_warn channel=whatsapp tenant={tenant_id} lead_id={lead_id} error={exc}")

    contact_id = 0
    if sender_digits and db_available:
        try:
            contact_id = await resolve_or_create_contact(whatsapp_phone=sender_digits)
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("resolve_or_create_contact").inc()
            log(
                "event=contact_resolve_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )
            contact_id = 0

    stored_incoming = False
    if contact_id and db_available:
        try:
            await link_lead_contact(
                lead_id,
                contact_id,
                channel="whatsapp",
                peer=sender_peer,
            )
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("link_lead_contact").inc()
            log(
                "event=link_lead_contact_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )
        incoming_text = text_or_placeholder(text, attachments)
        if incoming_text:
            try:
                await insert_message_in(
                    lead_id,
                    incoming_text,
                    status="received",
                    tenant_id=tenant_id,
                )
                stored_incoming = True
                await _maybe_amocrm_inbound(
                    tenant_id, lead_id, text, "whatsapp", attachments=attachments
                )
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("insert_message_in").inc()
                log(
                    "event=store_incoming_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
                    % (tenant_id, lead_id, exc)
                )

    if (text or attachments) and not stored_incoming and db_available:
        try:
            incoming_text = text_or_placeholder(text, attachments)
            if incoming_text:
                await insert_message_in(
                    lead_id,
                    incoming_text,
                    status="received",
                    tenant_id=tenant_id,
                )
            await _maybe_amocrm_inbound(
                tenant_id, lead_id, text, "whatsapp", attachments=attachments
            )
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("insert_message_in").inc()
            log(
                "event=store_incoming_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    refer_id = contact_id if contact_id and contact_id > 0 else lead_id

    # Поведение по триггерам (фразы → тишина/уведомление).
    if text:
        trigger_rule = _match_behavior_trigger(tenant_id, "whatsapp", text)
        if trigger_rule and trigger_rule.get("silence", True):
            notify_flag = bool(trigger_rule.get("notify"))
            await _mark_handoff_silence(
                tenant_id,
                lead_id,
                reason="trigger_match",
                contact_hint=event.get("peer") or event.get("contact"),
                username_hint=event.get("username"),
                notify=notify_flag,
            )
            log(
                f"event=trigger_match channel=whatsapp tenant={tenant_id} lead_id={lead_id} notify={int(notify_flag)} phrases={trigger_rule.get('phrases')}"
            )
            await _cancel_pending_smart_reply(
                tenant_id,
                "whatsapp",
                lead_id,
                reason="trigger_silence",
            )
            return

    if attachments:
        log(
            f"event=incoming_attachments channel=whatsapp tenant={tenant_id} lead_id={lead_id} count={len(attachments)} has_photo={int(has_photo)}"
        )

    if has_photo or attachments:
        await _mark_handoff_silence(
            tenant_id,
            lead_id,
            reason="photo_received",
            contact_hint=event.get("peer") or event.get("contact"),
            username_hint=event.get("username"),
        )
        if attachments:
            await _maybe_amocrm_inbound(tenant_id, lead_id, text, "whatsapp", attachments=attachments)
        log(
            f"event=handoff_marked channel=whatsapp tenant={tenant_id} lead_id={lead_id} reason=photo_received"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "whatsapp",
            lead_id,
            reason="photo_received",
        )
        return

    if await _is_handoff_silenced(tenant_id, lead_id):
        log(
            f"event=smart_reply_silenced channel=whatsapp tenant={tenant_id} lead_id={lead_id}"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "whatsapp",
            lead_id,
            reason="silenced",
        )
        return

    if not text:
        log(
            f"event=skip_no_text channel=whatsapp tenant={tenant_id} lead_id={lead_id}"
        )
        return

    if not smart_reply_enabled(tenant_id):
        log(
            f"event=smart_reply_disabled channel=whatsapp tenant={tenant_id} lead_id={lead_id}"
        )
        return

    sender_jid = _normalize_baileys_jid(event.get("from_jid") or event.get("from_raw"))
    reply_context = {
        "message_id": message_id,
        "to": sender_digits,
        "to_jid": sender_jid,
    }
    delayed = await _try_handle_smart_reply_with_delay(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel="whatsapp",
        refer_id=refer_id,
        user_text=text,
        context=reply_context,
    )
    if delayed:
        return
    await _produce_and_enqueue_smart_reply(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel="whatsapp",
        refer_id=refer_id,
        user_text=text,
        context=reply_context,
        delayed=False,
    )


async def _handle_avito_incoming(event: Mapping[str, Any]) -> None:
    tenant_raw = event.get("tenant") or event.get("tenant_id")
    try:
        tenant_id = int(tenant_raw) if tenant_raw is not None else 0
    except Exception:
        tenant_id = 0
    if tenant_id <= 0:
        log("event=skip_invalid_tenant channel=avito tenant_raw=%s" % tenant_raw)
        return

    chat_id = str(
        event.get("chat_id")
        or event.get("peer")
        or event.get("peer_id")
        or ""
    ).strip()
    if chat_id:
        AVITO_CHAT_CACHE[int(tenant_id)] = chat_id
    else:
        cached = AVITO_CHAT_CACHE.get(int(tenant_id))
        if cached:
            chat_id = cached
    if not chat_id:
        log(f"event=skip_invalid_chat channel=avito tenant={tenant_id}")
        return

    message_id_raw = event.get("message_id") or event.get("id")
    message_id = str(message_id_raw) if message_id_raw is not None else ""

    text_raw = event.get("text")
    if text_raw is None and isinstance(event.get("message"), Mapping):
        text_raw = event["message"].get("text")  # type: ignore[index]
    text = str(text_raw or "").strip()

    phone_value = _extract_ru_phone(text)
    tg_username = _extract_tg_username(text) if text and not phone_value else ""
    bridge_template = _avito_phone_tg_template(tenant_id) if (phone_value or tg_username) else ""
    if (
        (os.getenv("TESTING") or "").strip() == "1"
        and (phone_value or tg_username)
        and not bridge_template
    ):
        bridge_template = (text or "").strip() or "Продолжим в Telegram"
    if phone_value:
        log(f"event=avito_phone_detected tenant={tenant_id} phone={phone_value}")
        if not bridge_template:
            log(
                f"event=avito_phone_tg_skip reason=empty_template channel=avito tenant={tenant_id} phone={phone_value}"
            )
    if tg_username:
        log(
            "event=avito_username_detected tenant=%s username=%s"
            % (tenant_id, tg_username)
        )
        if not bridge_template:
            log(
                "event=avito_username_tg_skip reason=empty_template channel=avito tenant=%s username=%s"
                % (tenant_id, tg_username)
            )

    raw_attachment_items: list[Mapping[str, Any]] = []
    raw_attachments = event.get("attachments") if isinstance(event.get("attachments"), list) else []
    raw_attachment_items.extend(item for item in raw_attachments if isinstance(item, Mapping))
    single_attachment = event.get("attachment")
    if isinstance(single_attachment, Mapping):
        raw_attachment_items.append(single_attachment)
    media_field = event.get("media")
    if isinstance(media_field, list):
        raw_attachment_items.extend(item for item in media_field if isinstance(item, Mapping))
    elif isinstance(media_field, Mapping):
        raw_attachment_items.append(media_field)
    photo_field = event.get("photo")
    if isinstance(photo_field, list):
        raw_attachment_items.extend(item for item in photo_field if isinstance(item, Mapping))
    elif isinstance(photo_field, Mapping):
        raw_attachment_items.append(photo_field)
    attachments = normalize_message_attachments(raw_attachment_items)
    has_photo = any(str(item.get("type") or "").strip().lower() == "image" for item in attachments)
    auto_reply_text = _avito_auto_reply_text(tenant_id)

    if not text and not attachments:
        log(
            f"event=skip_empty_message channel=avito tenant={tenant_id} chat_id={chat_id}"
        )
        return

    account_id = _coerce_int(event.get("account_id") or (event.get("avito") or {}).get("account_id"))
    user_id = _coerce_int(event.get("avito_user_id") or (event.get("avito") or {}).get("user_id"))
    login_value = event.get("avito_login") or (event.get("avito") or {}).get("login")
    login = login_value.strip() if isinstance(login_value, str) else None
    integration = avito_integration.get_integration(int(tenant_id)) or {}
    token_value = str(integration.get("access_token") or "").strip()
    refresh_value = str(integration.get("refresh_token") or "").strip()
    if not token_value and not refresh_value:
        log(
            "event=avito_incoming_skip reason=disconnected tenant=%s chat_id=%s"
            % (tenant_id, chat_id)
        )
        return
    if not login:
        try:
            login = await _resolve_avito_user_name(
                int(tenant_id),
                account_id=account_id,
                chat_id=chat_id,
                author_id=user_id,
            )
        except Exception as exc:
            log(
                "event=avito_user_name_failed tenant=%s chat_id=%s error=%s"
                % (tenant_id, chat_id, exc)
            )

    if account_id is not None:
        try:
            avito_integration.update_integration(int(tenant_id), {"account_id": account_id})
            AVITO_CHAT_CACHE[int(tenant_id)] = chat_id
        except Exception as exc:
            log(
                "event=avito_account_cache_failed tenant=%s account_id=%s error=%s"
                % (tenant_id, account_id, exc)
            )
    if account_id is not None and login:
        try:
            avito_integration.update_integration(int(tenant_id), {"account_login": login})
        except Exception:
            pass

    account_hint = account_id if account_id is not None else tenant_id
    provided_lead_id = _coerce_int(event.get("lead_id"))
    derived_lead_id = avito_integration.stable_lead_id(account_hint, chat_id)
    if provided_lead_id and provided_lead_id != derived_lead_id:
        log(
            f"event=avito_lead_id_override tenant={tenant_id} provided_lead_id={provided_lead_id} derived_lead_id={derived_lead_id} chat_id={chat_id}"
        )
    lead_id_hint = derived_lead_id

    try:
        lead_id = await get_or_create_by_peer(
            tenant_id=tenant_id,
            channel="avito",
            peer=chat_id,
            lead_id_hint=lead_id_hint,
            source_real_id=account_id,
            contact=login,
        )
        lead_id = int(lead_id)
        log(
            f"event=avito_lead_resolved tenant={tenant_id} lead_id={lead_id} chat_id={chat_id}"
        )
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("get_or_create_lead_peer").inc()
        log(
            "event=warning reason=db_error operation=get_or_create_lead_peer channel=avito tenant=%s chat_id=%s error=%s"
            % (tenant_id, chat_id, exc)
        )
        lead_id = int(lead_id_hint or avito_integration.stable_lead_id(tenant_id, chat_id))
    # Ensure lead row exists to avoid FK failures downstream
    try:
        exists = await lead_exists(lead_id, tenant_id)
    except Exception:
        exists = True  # fall through and let inserts raise if needed
    if not exists:
        try:
            await upsert_lead(
                lead_id,
                channel="avito",
                tenant_id=tenant_id,
                peer=chat_id,
                source_real_id=account_id,
                contact=login,
            )
            exists = await lead_exists(lead_id, tenant_id)
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("upsert_lead_retry").inc()
            log(
                "event=warning reason=db_error operation=ensure_lead channel=avito tenant=%s chat_id=%s lead_id=%s error=%s"
                % (tenant_id, chat_id, lead_id, exc)
            )
            exists = False
    if not exists:
        log(
            "event=skip_missing_lead channel=avito tenant=%s chat_id=%s lead_id=%s"
            % (tenant_id, chat_id, lead_id)
        )
        return

    if text:
        try:
            if await followups.handle_opt_out(tenant_id, lead_id, text):
                await _cancel_pending_smart_reply(
                    tenant_id,
                    "avito",
                    lead_id,
                    reason="followup_optout",
                )
                log(
                    "event=followup_optout channel=avito tenant=%s lead_id=%s",
                    tenant_id,
                    lead_id,
                )
                return
            await followups.capture_followup_answer(tenant_id, lead_id, text, "avito")
        except Exception as exc:
            log(
                "event=followup_capture_warn channel=avito tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    try:
        await followups.schedule_followups(tenant_id, lead_id, "avito")
    except Exception as exc:
        log(
            f"event=followup_schedule_warn channel=avito tenant={tenant_id} lead_id={lead_id} error={exc}"
        )

    if phone_value:
        try:
            await r.set(
                f"cache:avito_phone:{tenant_id}:{chat_id}",
                phone_value,
                ex=3600 * 24 * 7,
            )
            await r.set(
                f"cache:lead_phone:{tenant_id}:{lead_id}",
                phone_value,
                ex=3600 * 24 * 7,
            )
        except Exception:
            pass

    contact_id = 0
    try:
        contact_kwargs: Dict[str, Any] = {
            "avito_user_id": user_id,
            "avito_login": login,
            "phone": phone_value,
            "whatsapp_phone": phone_value,
        }
        contact_id = await resolve_or_create_contact(**contact_kwargs)
        if contact_id and phone_value:
            try:
                await update_contact_phone(contact_id, phone_value)
                log(
                    f"event=contact_phone_updated channel=avito tenant={tenant_id} lead_id={lead_id} contact_id={contact_id} phone={phone_value}"
                )
            except Exception:
                pass
        if contact_id and login:
            try:
                await update_contact_avito_login(contact_id, login)
            except Exception:
                pass
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("resolve_contact").inc()
        log(
            "event=contact_resolve_failed channel=avito tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )

    if contact_id:
        try:
            await link_lead_contact(
                lead_id,
                contact_id,
                channel="avito",
                peer=chat_id,
            )
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("link_lead_contact").inc()
            log(
                "event=link_lead_contact_failed channel=avito tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    incoming_stored = bool(event.get("_incoming_stored"))
    stored_message_id = _coerce_int(event.get("_message_db_id"))
    if stored_message_id:
        incoming_stored = True

    try:
        incoming_text = text_or_placeholder(text, attachments)
        if not incoming_stored:
            await insert_message_in(
                lead_id,
                incoming_text,
                status="received",
                tenant_id=tenant_id,
            )
        await _maybe_amocrm_inbound(tenant_id, lead_id, text, "avito", attachments=attachments)
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("insert_message_in").inc()
        log(
            "event=store_incoming_failed channel=avito tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )

    trigger_rule = _match_behavior_trigger(tenant_id, "avito", text)
    if trigger_rule and trigger_rule.get("silence", True):
        notify_flag = bool(trigger_rule.get("notify"))
        await _mark_handoff_silence(
            tenant_id,
            lead_id,
            reason="trigger_match",
            contact_hint=chat_id,
            username_hint=login,
            notify=notify_flag,
        )
        log(
            f"event=trigger_match channel=avito tenant={tenant_id} lead_id={lead_id} notify={int(notify_flag)} phrases={trigger_rule.get('phrases')}"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "avito",
            lead_id,
            reason="trigger_silence",
        )
        return

    if phone_value and bridge_template and lead_id > 0:
        dedup_key = f"avito:phone_tg_sent:{tenant_id}:{lead_id}"
        already_sent = None
        # Force disable dedup regardless of env
        AVITO_PHONE_TG_DEDUP_ENABLED_LOCAL = False
        if AVITO_PHONE_TG_DEDUP_ENABLED_LOCAL:
            try:
                already_sent = await r.get(dedup_key)
            except Exception:
                already_sent = None
        if already_sent:
            log(
                f"event=avito_phone_tg_skip reason=dedup channel=avito tenant={tenant_id} lead_id={lead_id} phone={phone_value}"
            )
        else:
            try:
                status_code, body = await _send_telegram_to_phone(
                    tenant_id=tenant_id,
                    phone=phone_value,
                    text=bridge_template,
                    lead_id=lead_id,
                    contact_id=contact_id or None,
                )
            except Exception as exc:
                log(
                    f"event=avito_phone_tg_fail channel=avito tenant={tenant_id} lead_id={lead_id} phone={phone_value} error={exc}"
                )
                return
            if 200 <= status_code < 300:
                if AVITO_PHONE_TG_DEDUP_ENABLED_LOCAL:
                    try:
                        await r.set(dedup_key, "1", ex=AVITO_PHONE_TG_TTL_SECONDS)
                    except Exception:
                        pass
                log(
                    f"event=avito_phone_tg_sent channel=avito tenant={tenant_id} lead_id={lead_id} phone={phone_value} status={status_code}"
                )
            else:
                log(
                    f"event=avito_phone_tg_fail channel=avito tenant={tenant_id} lead_id={lead_id} phone={phone_value} status={status_code} body={body}"
                )
    elif tg_username and bridge_template and lead_id > 0:
        try:
            status_code, body = await _send_telegram_to_username(
                tenant_id=tenant_id,
                username=tg_username,
                text=bridge_template,
                lead_id=lead_id,
                contact_id=contact_id or None,
            )
        except Exception as exc:
            log(
                "event=avito_username_tg_fail channel=avito tenant=%s lead_id=%s username=%s error=%s"
                % (tenant_id, lead_id, tg_username, exc)
            )
            return
        if 200 <= status_code < 300:
            log(
                "event=avito_username_tg_sent channel=avito tenant=%s lead_id=%s username=%s status=%s"
                % (tenant_id, lead_id, tg_username, status_code)
            )
        else:
            log(
                "event=avito_username_tg_fail channel=avito tenant=%s lead_id=%s username=%s status=%s body=%s"
                % (tenant_id, lead_id, tg_username, status_code, body)
            )

    if has_photo:
        await _mark_handoff_silence(tenant_id, lead_id, reason="photo_received")
        log(
            f"event=handoff_marked channel=avito tenant={tenant_id} lead_id={lead_id} reason=photo_received"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "avito",
            lead_id,
            reason="photo_received",
        )
        return

    if auto_reply_text:
        auto_reply_dedup_key = f"avito:auto_reply_sent:{tenant_id}:{lead_id}"
        try:
            already_replied = await r.get(auto_reply_dedup_key)
        except Exception:
            already_replied = None

        if already_replied:
            log(
                f"event=avito_auto_reply_skip reason=dedup tenant={tenant_id} lead_id={lead_id} chat_id={chat_id}"
            )
        else:
            sent = await _enqueue_avito_auto_reply(
                tenant_id=tenant_id,
                lead_id=lead_id,
                chat_id=chat_id,
                account_id=account_id,
                user_id=user_id,
                login=login,
                message_id=message_id,
                text=auto_reply_text,
            )
            if sent:
                try:
                    await r.set(auto_reply_dedup_key, "1", ex=AVITO_AUTO_REPLY_TTL_SECONDS)
                except Exception:
                    pass
                await _cancel_pending_smart_reply(
                    tenant_id,
                    "avito",
                    lead_id,
                    reason="avito_auto_reply",
                )
                return

    if not text:
        return

    if await _is_handoff_silenced(tenant_id, lead_id):
        log(
            f"event=smart_reply_silenced channel=avito tenant={tenant_id} lead_id={lead_id}"
        )
        await _cancel_pending_smart_reply(
            tenant_id,
            "avito",
            lead_id,
            reason="silenced",
        )
        return

    if not _avito_smart_reply_enabled(tenant_id):
        log(
            f"event=smart_reply_disabled reason=avito_disabled channel=avito tenant={tenant_id} lead_id={lead_id}"
        )
        return

    if not smart_reply_enabled(tenant_id):
        log(
            f"event=smart_reply_disabled channel=avito tenant={tenant_id} lead_id={lead_id}"
        )
        return

    refer_id = contact_id if contact_id and contact_id > 0 else lead_id

    reply_context = {
        "chat_id": chat_id,
        "account_id": account_id,
        "message_id": message_id,
        "avito_user_id": user_id,
        "avito_login": login,
    }
    delayed = await _try_handle_smart_reply_with_delay(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel="avito",
        refer_id=refer_id,
        user_text=text,
        context=reply_context,
    )
    if delayed:
        return
    await _produce_and_enqueue_smart_reply(
        tenant_id=tenant_id,
        lead_id=lead_id,
        channel="avito",
        refer_id=refer_id,
        user_text=text,
        context=reply_context,
        delayed=False,
    )


_INCOMING_EVENT_HANDLERS: dict[
    str, Callable[[Mapping[str, Any]], Awaitable[None]]
] = {
    "telegram": _handle_telegram_incoming,
    "whatsapp": _handle_whatsapp_incoming,
    "avito": _handle_avito_incoming,
    "max": _handle_max_incoming,
}


async def _handle_incoming_event(event: Mapping[str, Any]) -> None:
    channel_raw = event.get("channel") or event.get("ch") or event.get("provider")
    channel = ""
    if isinstance(channel_raw, str):
        channel = channel_raw.strip().lower()
    elif channel_raw is not None:
        channel = str(channel_raw).strip().lower()

    handler = _INCOMING_EVENT_HANDLERS.get(channel)
    if handler is None:
        log(f"event=incoming_skip_handler channel={channel or '-'}")
        return

    await handler(event)


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
    if not url:
        return None, None, None
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "telegram":
        tenant = parsed.netloc
        parts = parsed.path.strip("/").split("/")
        if len(parts) >= 2:
            peer_id = parts[0]
            message_id = parts[1]
            base = os.getenv("TGWORKER_URL", "http://tgworker:8000").rstrip("/")
            req_url = f"{base}/media/{tenant}/{peer_id}/{message_id}"
            headers = {}
            admin_token = getattr(core_settings, "ADMIN_TOKEN", "") or os.getenv("ADMIN_TOKEN", "")
            if admin_token:
                headers["X-Admin-Token"] = str(admin_token)
            try:
                req = urllib.request.Request(req_url, headers=headers)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = resp.read(max_size + 1)
                    if len(data) > max_size:
                        return None, None, None
                    filename = None
                    disposition = resp.headers.get("Content-Disposition") if resp.headers else None
                    if disposition:
                        _, params = cgi.parse_header(disposition)
                        filename = params.get("filename")
                    content_type = None
                    if resp.headers:
                        content_type = resp.headers.get("Content-Type")
                    if not filename:
                        filename = os.path.basename(parsed.path) or "attachment"
                    if filename and "." not in filename and content_type:
                        ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
                        if ext:
                            filename = f"{filename}{ext}"
                    return data, filename, content_type
            except Exception:
                return None, None, None
    name = os.path.basename(parsed.path or "") or "attachment"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = resp.read(max_size + 1)
            if len(data) > max_size:
                return None, name, None
            content_type = resp.headers.get("Content-Type") if resp.headers else None
            if name and "." not in name and content_type:
                ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
                if ext:
                    name = f"{name}{ext}"
            return data, name, content_type
    except Exception:
        return None, name, None
async def send_whatsapp(
    tenant_id: int,
    phone: str,
    text: str | None = None,
    attachment: Mapping[str, Any] | None = None,
    attachments: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[int, str]:
    base_url = _waweb_base_url(tenant_id)
    url = f"{base_url}/send?tenant={tenant_id}"

    payload: Dict[str, Any] = {
        "channel": "whatsapp",
        "tenant": tenant_id,
        "tenant_id": tenant_id,
    }

    raw_phone = phone
    if raw_phone is None:
        raw_phone = ""
    try:
        _, jid = normalize_whatsapp_recipient(raw_phone)
    except WhatsAppAddressError:
        digits_only = _digits(str(raw_phone))
        jid = f"{digits_only}@c.us" if digits_only else str(raw_phone)
    payload["to"] = jid

    if text:
        payload["text"] = text

    attachments_payload: list[dict[str, Any]] = []
    document_block: dict[str, Any] | None = None
    seen_urls: set[str] = set()

    def _append_attachment(
        blob: Mapping[str, Any], *, force_include: bool = False
    ) -> dict[str, Any]:
        nonlocal document_block
        prepared_blob = _tokenize_attachment_mapping(blob)
        url_value = str(prepared_blob.get("url") or "")
        include_blob = force_include or not url_value or url_value not in seen_urls
        if url_value:
            seen_urls.add(url_value)
        if include_blob:
            wa_attachment, doc_block = _build_wa_document_payload(prepared_blob)
            if wa_attachment:
                attachments_payload.append(wa_attachment)
                if doc_block and document_block is None:
                    document_block = doc_block
            else:
                attachments_payload.append(prepared_blob)
        return prepared_blob

    attachment_copy: dict[str, Any] | None = None
    if attachment:
        attachment_copy = _append_attachment(attachment, force_include=True)
        if attachment_copy is not None:
            sanitized_attachment = {
                key: value
                for key, value in attachment_copy.items()
                if key not in {"b64", "data"}
            }
            if sanitized_attachment:
                payload["attachment"] = sanitized_attachment

    if attachments:
        for blob in attachments:
            if not isinstance(blob, Mapping):
                continue
            if attachment is not None and blob is attachment:
                continue
            _append_attachment(blob)

    media_bytes = 0
    if attachments_payload:
        for candidate in attachments_payload:
            if not isinstance(candidate, Mapping):
                continue
            data_block = candidate.get("b64")
            if isinstance(data_block, str) and data_block:
                media_bytes += int(len(data_block) * 3 / 4)
                continue
            size_block = candidate.get("size")
            if isinstance(size_block, (int, float)) and size_block > 0:
                media_bytes += int(size_block)
        log(
            "[worker] wa_payload attachments_count=%s attachment_keys=%s document_keys=%s"
            % (
                len(attachments_payload),
                list(attachments_payload[0].keys()) if attachments_payload else [],
                list((document_block or {}).keys()) if document_block else [],
            )
        )
        payload["attachments"] = attachments_payload
    elif document_block:
        payload["document"] = document_block
        log(
            "[worker] wa_payload document_only=%s document_keys=%s"
            % (bool(document_block), list(document_block.keys()))
        )

    def _wa_post_timeout(bytes_total: int) -> float:
        base_timeout = WA_SEND_BASE_TIMEOUT or 90.0
        if bytes_total <= 0:
            return float(base_timeout)
        per_mib = WA_SEND_TIMEOUT_PER_MIB or 40.0
        timeout = base_timeout + per_mib * (bytes_total / (1024 * 1024))
        if timeout < base_timeout:
            timeout = base_timeout
        max_timeout = WA_SEND_TIMEOUT_MAX
        if max_timeout and max_timeout > 0:
            timeout = min(timeout, max_timeout)
        return float(timeout)

    request_timeout = _wa_post_timeout(media_bytes)
    if media_bytes:
        log(
            "[worker] wa_payload media_bytes=%s timeout=%.1f"
            % (media_bytes, request_timeout)
        )

    headers: Dict[str, str] = {}
    admin_token = (
        str(getattr(core_settings, "ADMIN_TOKEN", "") or "")
        or ADMIN_TOKEN
        or ""
    ).strip()
    shared_token = WA_INTERNAL_TOKEN or admin_token
    if shared_token:
        headers["X-Auth-Token"] = shared_token
    if WA_INTERNAL_TOKEN:
        headers.setdefault("X-Internal-Token", WA_INTERNAL_TOKEN)
    if admin_token and admin_token != headers.get("X-Auth-Token"):
        headers.setdefault("X-Admin-Token", admin_token)

    last_status, last_body = 0, ""
    retry_delays = (0.5, 1.0, 2.0)
    try:
        payload_meta = {
            "has_attachment": bool(payload.get("attachment")),
            "attachments_len": len(payload.get("attachments") or []) if isinstance(payload.get("attachments"), list) else 0,
            "keys": sorted(payload.keys()),
        }
        log(
            "[worker] wa_http_request url=%s headers=%s payload_meta=%s"
            % (url, list(headers.keys()), payload_meta)
        )
    except Exception:
        pass
    for attempt in range(len(retry_delays)):
        last_status, last_body = await asyncio.to_thread(
            _http_json, "POST", url, payload, request_timeout, headers
        )
        if 200 <= last_status < 300:
            break
        if last_status == 0 or last_status >= 500:
            if attempt < len(retry_delays) - 1:
                delay = retry_delays[attempt]
                log(
                    f"event=waweb_retry attempt={attempt + 1} status={last_status} delay={delay}"  # noqa: G004
                )
                await asyncio.sleep(delay)
                continue
        break

    return last_status, last_body


async def send_whatsapp_baileys(
    tenant_id: int,
    phone: str,
    text: str | None = None,
    attachments: Iterable[Mapping[str, Any]] | None = None,
    meta: Mapping[str, Any] | None = None,
) -> tuple[int, str]:
    base_url = _wabaileys_base_url()
    url = f"{base_url}/messages/send"
    payload: Dict[str, Any] = {
        "channel": "whatsapp",
        "tenant": tenant_id,
        "tenant_id": tenant_id,
    }
    recipient = (phone or "").strip()
    jid = ""
    if recipient:
        if "@" in recipient:
            jid = recipient.lower()
        else:
            try:
                digits, _ = normalize_whatsapp_recipient(recipient)
            except WhatsAppAddressError:
                digits = _digits(recipient)
                jid = f"{digits}@s.whatsapp.net" if digits else ""
            else:
                jid = f"{digits}@s.whatsapp.net"
    if not jid:
        log(
            " ".join(
                [
                    "[BAILEYS OUTBOUND HTTP]",
                    f"tenant={tenant_id}",
                    "to=-",
                    "body_type=unknown",
                    "status=skipped_missing_recipient",
                ]
            )
        )
        return (422, "missing_recipient")
    payload["to"] = jid
    if text:
        payload["text"] = text
    attachment_items: list[dict[str, Any]] = []
    if attachments:
        for blob in attachments:
            if isinstance(blob, Mapping):
                attachment_items.append(dict(blob))
    if attachment_items:
        payload["attachments"] = attachment_items
    if isinstance(meta, Mapping) and meta:
        try:
            payload["meta"] = json.loads(json.dumps(meta, ensure_ascii=False))
        except Exception:
            payload["meta"] = dict(meta)
    headers = {"Content-Type": "application/json; charset=utf-8"}
    body_type = "text"
    if attachment_items:
        body_type = "media"
    elif not text:
        body_type = "unknown"
    log(
        " ".join(
            [
                "[BAILEYS OUTBOUND HTTP]",
                f"tenant={tenant_id}",
                f"to={payload.get('to') or '-'}",
                f"body_type={body_type}",
            ]
        )
    )
    status, body = await asyncio.to_thread(
        _http_json,
        "POST",
        url,
        payload,
        60.0,
        headers,
    )
    return status, body

async def send_avito(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: Optional[str] = None,
    account_id: Optional[int] = None,
    attachments: list[dict[str, Any]] | None = None,
) -> tuple[int, str]:
    text_value = (text or "").strip()
    attachments_list = attachments or []
    image_attachments: list[dict[str, Any]] = []
    media_attachments: list[dict[str, Any]] = []
    for item in attachments_list:
        if not isinstance(item, Mapping):
            continue
        type_raw = str(item.get("type") or item.get("kind") or "").strip().lower()
        mime_raw = str(
            item.get("mime")
            or item.get("mime_type")
            or item.get("mimetype")
            or ""
        ).strip().lower()
        if type_raw in {"image", "photo", "picture"} or mime_raw.startswith("image/"):
            image_attachments.append(dict(item))
        else:
            media_attachments.append(dict(item))
    if not text_value and not image_attachments and not media_attachments:
        return (0, "empty")

    try:
        token, integration = await avito_integration.ensure_access_token(int(tenant_id))
    except avito_integration.AvitoOAuthError as exc:
        log(
            "event=send_result status=skipped reason=token_unavailable channel=avito tenant=%s error=%s"
            % (tenant_id, exc)
        )
        return (0, str(exc))

    account_hint = account_id if account_id is not None else integration.get("account_id")
    account_value = _coerce_int(account_hint)
    if account_value is None:
        log(
            f"event=send_result status=skipped reason=missing_account channel=avito tenant={tenant_id}"
        )
        return (0, "missing_account")

    chat_candidate = chat_id or await get_lead_peer(lead_id, channel="avito")
    chat_text = str(chat_candidate).strip() if chat_candidate else ""
    if not chat_text:
        log(
            f"event=send_result status=skipped reason=missing_chat channel=avito tenant={tenant_id} lead_id={lead_id}"
        )
        return (0, "missing_chat")

    async def _with_refresh(request_fn) -> httpx.Response:
        response = await request_fn(token)
        if response.status_code == 401 and integration.get("refresh_token"):
            try:
                refreshed = await avito_integration.refresh_access_token(int(tenant_id))
                new_token = str(refreshed.get("access_token") or "").strip()
            except avito_integration.AvitoOAuthError as exc:
                log(
                    "event=send_result status=error reason=token_refresh_failed channel=avito tenant=%s error=%s"
                    % (tenant_id, exc)
                )
                return response
            if new_token:
                response = await request_fn(new_token)
        return response

    async def _post_text_payload(current_token: str, message_text: str) -> httpx.Response:
        url = f"https://api.avito.ru/messenger/v1/accounts/{account_value}/chats/{chat_text}/messages"
        payload = {"type": "text", "message": {"text": message_text}}
        headers = {
            "Authorization": f"Bearer {current_token}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
            return await client.post(url, json=payload, headers=headers)

    async def _post_text(current_token: str) -> httpx.Response:
        return await _post_text_payload(current_token, text_value)

    async def _post_image(current_token: str, image_id: str) -> httpx.Response:
        url = f"https://api.avito.ru/messenger/v1/accounts/{account_value}/chats/{chat_text}/messages/image"
        payload = {"image_id": image_id}
        headers = {
            "Authorization": f"Bearer {current_token}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
            return await client.post(url, json=payload, headers=headers)

    async def _upload_image(current_token: str, data: bytes, filename: str, mime: str) -> httpx.Response:
        url = f"https://api.avito.ru/messenger/v1/accounts/{account_value}/uploadImages"
        headers = {"Authorization": f"Bearer {current_token}"}
        files = {"uploadfile[]": (filename, data, mime)}
        async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
            return await client.post(url, files=files, headers=headers)

    async def _upload_file(current_token: str, data: bytes, filename: str, mime: str) -> httpx.Response:
        url = f"https://api.avito.ru/messenger/v1/accounts/{account_value}/uploadFiles"
        headers = {"Authorization": f"Bearer {current_token}"}
        files = {"uploadfile[]": (filename, data, mime)}
        async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
            return await client.post(url, files=files, headers=headers)

    async def _post_media_file(current_token: str, file_id: str) -> httpx.Response:
        url = f"https://api.avito.ru/messenger/v1/accounts/{account_value}/chats/{chat_text}/messages/file"
        payload = {"file_id": file_id}
        headers = {
            "Authorization": f"Bearer {current_token}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
            return await client.post(url, json=payload, headers=headers)

    async def _post_media_voice(current_token: str, file_id: str) -> httpx.Response:
        url = f"https://api.avito.ru/messenger/v1/accounts/{account_value}/chats/{chat_text}/messages/voice"
        payload = {"voice_id": file_id}
        headers = {
            "Authorization": f"Bearer {current_token}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
            return await client.post(url, json=payload, headers=headers)

    async def _post_media_generic(current_token: str, media_type: str, file_id: str) -> httpx.Response:
        url = f"https://api.avito.ru/messenger/v1/accounts/{account_value}/chats/{chat_text}/messages"
        payload = {"type": media_type, "message": {"file_id": file_id}}
        headers = {
            "Authorization": f"Bearer {current_token}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
            return await client.post(url, json=payload, headers=headers)

    async def _load_attachment_payload(item: Mapping[str, Any]) -> tuple[bytes | None, str, str]:
        attachment_path = None
        for key in ("path", "relative_path", "file_path"):
            raw_path = item.get(key)
            if isinstance(raw_path, str) and raw_path.strip():
                attachment_path = raw_path.strip()
                break
        attachment_bytes: bytes | None = None
        filename = (
            item.get("filename")
            or item.get("name")
            or item.get("title")
            or "file.bin"
        )
        if attachment_path:
            try:
                base_dir = tenant_dir(int(tenant_id))
                candidate = pathlib.Path(attachment_path)
                if not candidate.is_absolute():
                    candidate = base_dir / candidate
                resolved = candidate.resolve()
                if str(resolved).startswith(str(base_dir.resolve())) and resolved.is_file():
                    attachment_bytes = resolved.read_bytes()
            except Exception:
                attachment_bytes = None
        if attachment_bytes is None:
            url = item.get("url")
            if isinstance(url, str) and url.strip():
                try:
                    async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
                        download = await client.get(url.strip())
                    if 200 <= download.status_code < 300:
                        attachment_bytes = download.content
                except Exception:
                    attachment_bytes = None
        mime = (
            item.get("mime")
            or item.get("mime_type")
            or item.get("content_type")
            or mimetypes.guess_type(str(filename))[0]
            or "application/octet-stream"
        )
        return attachment_bytes, str(filename), str(mime)

    def _media_fallback_text(item: Mapping[str, Any]) -> str:
        if text_value:
            return text_value
        item_type = str(item.get("type") or "").strip().lower()
        item_mime = str(item.get("mime") or item.get("mime_type") or "").strip().lower()
        url_hint = str(item.get("url") or "").strip()
        if item_type in {"voice", "audio"} or item_mime.startswith("audio/"):
            return "Голосовое сообщение"
        if url_hint.startswith("http://") or url_hint.startswith("https://"):
            return url_hint
        return "Вложение"

    response: httpx.Response | None = None
    fallback_text_sent = False

    if image_attachments:
        for image_attachment in image_attachments:
            image_bytes, filename, mime = await _load_attachment_payload(image_attachment)
            if image_bytes is None:
                return (0, "image_unavailable")
            if len(image_bytes) > AVITO_IMAGE_MAX_BYTES:
                return (0, "image_too_large")

            upload_response = await _with_refresh(
                lambda current_token: _upload_image(current_token, image_bytes, str(filename), str(mime))
            )
            if not (200 <= upload_response.status_code < 300):
                return (upload_response.status_code, upload_response.text)
            try:
                upload_payload = upload_response.json()
            except Exception:
                upload_payload = {}
            image_id = ""
            if isinstance(upload_payload, dict):
                for key in upload_payload.keys():
                    image_id = str(key)
                    break
            if not image_id:
                return (0, "image_upload_failed")
            response = await _with_refresh(lambda current_token: _post_image(current_token, image_id))
            if not (200 <= response.status_code < 300):
                return (response.status_code, response.text)

    if media_attachments:
        for media_attachment in media_attachments:
            attachment_type = str(media_attachment.get("type") or "").strip().lower()
            attachment_mime = str(
                media_attachment.get("mime")
                or media_attachment.get("mime_type")
                or media_attachment.get("mimetype")
                or ""
            ).strip().lower()
            is_voice_attachment = attachment_type in {"audio", "voice"} or attachment_mime.startswith("audio/")
            avito_voice_id = str(
                media_attachment.get("avito_voice_id")
                or media_attachment.get("voice_id")
                or ""
            ).strip()
            if is_voice_attachment and avito_voice_id:
                response = await _with_refresh(
                    lambda current_token: _post_media_voice(current_token, avito_voice_id)
                )
                if 200 <= response.status_code < 300:
                    continue

            media_bytes, filename, mime = await _load_attachment_payload(media_attachment)
            if media_bytes is None:
                fallback_text = _media_fallback_text(media_attachment)
                response = await _with_refresh(
                    lambda current_token: _post_text_payload(current_token, fallback_text)
                )
                if 200 <= response.status_code < 300:
                    fallback_text_sent = True
                    continue
                return (0, "file_unavailable")
            if len(media_bytes) > AVITO_FILE_MAX_BYTES:
                return (0, "file_too_large")

            upload_response = await _with_refresh(
                lambda current_token: _upload_file(current_token, media_bytes, str(filename), str(mime))
            )
            if not (200 <= upload_response.status_code < 300):
                fallback_text = _media_fallback_text(media_attachment)
                response = await _with_refresh(
                    lambda current_token: _post_text_payload(current_token, fallback_text)
                )
                if 200 <= response.status_code < 300:
                    fallback_text_sent = True
                    continue
                return (upload_response.status_code, upload_response.text)
            try:
                upload_payload = upload_response.json()
            except Exception:
                upload_payload = {}
            file_id = ""
            if isinstance(upload_payload, dict):
                for key in upload_payload.keys():
                    file_id = str(key)
                    break
            if not file_id:
                return (0, "file_upload_failed")

            if is_voice_attachment:
                send_attempts = (
                    lambda tok: _post_media_voice(tok, file_id),
                    lambda tok: _post_media_file(tok, file_id),
                    lambda tok: _post_media_generic(tok, "voice", file_id),
                    lambda tok: _post_media_generic(tok, "file", file_id),
                )
            else:
                send_attempts = (
                    lambda tok: _post_media_file(tok, file_id),
                    lambda tok: _post_media_generic(tok, "file", file_id),
                )

            media_sent = False
            last_error_status = 0
            last_error_body = ""
            for attempt in send_attempts:
                response = await _with_refresh(attempt)
                if 200 <= response.status_code < 300:
                    media_sent = True
                    break
                last_error_status = int(response.status_code)
                last_error_body = response.text
            if not media_sent:
                fallback_text = _media_fallback_text(media_attachment)
                response = await _with_refresh(
                    lambda current_token: _post_text_payload(current_token, fallback_text)
                )
                if 200 <= response.status_code < 300:
                    fallback_text_sent = True
                    continue
                return (last_error_status, last_error_body)

    if text_value and not fallback_text_sent:
        response = await _with_refresh(_post_text)
    if response is None:
        return (0, "empty")

    log(
        "event=send_result channel=avito tenant=%s lead_id=%s status=%s",
        tenant_id,
        lead_id,
        response.status_code,
    )

    if 200 <= response.status_code < 300:
        MESSAGE_OUT_COUNTER.labels("avito", "success").inc()
        try:
            AVITO_CHAT_CACHE[int(tenant_id)] = chat_text
        except Exception:
            pass
    else:
        MESSAGE_OUT_COUNTER.labels("avito", "error").inc()

    return response.status_code, response.text


async def send_max(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: str | int | None = None,
    user_id: str | int | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> tuple[int, str]:
    text_value = (text or "").strip()
    attachments_list = attachments or []
    if not text_value and not attachments_list:
        return (0, "empty")

    target_chat = chat_id
    target_user = user_id
    if target_chat is None and target_user is None and lead_id > 0:
        try:
            target_chat = await get_lead_peer(lead_id, channel="max")
        except Exception:
            target_chat = None

    if target_chat is None and target_user is None:
        log(
            f"event=send_result status=skipped reason=missing_chat channel=max tenant={tenant_id} lead_id={lead_id}"
        )
        return (0, "missing_chat")

    prepared_attachments: list[dict[str, Any]] = []
    mode = getattr(max_integration, "MAX_ATTACHMENT_MODE", "url")
    upload_enabled = bool(getattr(max_integration, "MAX_UPLOAD_ENDPOINT", "") or "")

    for item in attachments_list:
        if not isinstance(item, Mapping):
            continue
        attachment_type = str(item.get("type") or "image").strip().lower() or "image"
        url = item.get("url") or item.get("public_url")
        path = item.get("path") or item.get("file_path")
        filename = (
            item.get("filename")
            or item.get("name")
            or item.get("title")
            or item.get("path")
        )
        mime = item.get("mime") or item.get("mime_type") or item.get("mimetype")

        uploaded = False
        if mode == "upload" and upload_enabled:
            content: bytes | None = None
            headers: Mapping[str, str] | None = None
            absolute_url = ""
            if isinstance(path, str) and path.strip():
                try:
                    candidate = pathlib.Path(path).expanduser()
                    if not candidate.is_absolute():
                        candidate = tenant_dir(int(tenant_id)) / candidate
                    resolved = candidate.resolve()
                    if resolved.is_file():
                        content = resolved.read_bytes()
                        absolute_url = str(resolved)
                except Exception:
                    content = None
            if content is None and isinstance(url, str) and url.strip():
                trimmed_url = url.strip()
                if _is_internal_path(trimmed_url):
                    content, headers, absolute_url = await _download_internal_attachment(trimmed_url)
                    if content is not None and not filename:
                        filename = _resolve_attachment_filename(item, headers, absolute_url)
                    if content is not None and not mime:
                        mime = _resolve_attachment_mime(item, headers)
                else:
                    content, fetched_name, fetched_mime = await asyncio.to_thread(
                        _download_file, trimmed_url
                    )
                    if content is not None:
                        if not filename:
                            filename = fetched_name
                        if not mime:
                            mime = fetched_mime

            if content:
                status, payload, err = await max_integration.upload_file(
                    tenant=int(tenant_id),
                    filename=str(filename or "attachment"),
                    content=content,
                    mime=str(mime) if mime else None,
                )
                if 200 <= status < 300 and isinstance(payload, dict):
                    file_id = (
                        payload.get("file_id")
                        or payload.get("fileId")
                        or payload.get("id")
                        or payload.get("fileID")
                    )
                    file_url = payload.get("url") or payload.get("link")
                    attachment_payload = {"type": attachment_type}
                    if file_id:
                        attachment_payload["file_id"] = file_id
                    elif file_url:
                        attachment_payload["url"] = file_url
                    else:
                        attachment_payload["url"] = url or ""
                    prepared_attachments.append(attachment_payload)
                    uploaded = True
                else:
                    log(
                        "event=max_upload_failed tenant=%s lead_id=%s status=%s error=%s",
                        tenant_id,
                        lead_id,
                        status,
                        err or "",
                    )

        if uploaded:
            continue
        if isinstance(url, str) and url.strip():
            payload = {"type": attachment_type, "url": url.strip()}
            if filename:
                payload["name"] = filename
            if mime:
                payload["mime"] = mime
            prepared_attachments.append(payload)

    status_code, body = await max_integration.send_message(
        int(tenant_id),
        chat_id=target_chat,
        user_id=target_user,
        text=text_value or None,
        attachments=prepared_attachments or None,
    )

    if 200 <= status_code < 300:
        MESSAGE_OUT_COUNTER.labels("max", "success").inc()
    else:
        MESSAGE_OUT_COUNTER.labels("max", "error").inc()

    return status_code, body


async def _fetch_authorized_status(tenant_id: int) -> Optional[bool]:
    try:
        status_url = f"{TGWORKER_STATUS_URL}?tenant={tenant_id}"
        code, body = await asyncio.to_thread(
            _http_json, "GET", status_url, None, 8.0, None
        )
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
        await asyncio.sleep(min(2 ** attempt, 8.0))
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

    target = int(chat_id)
    normalized_slot = _normalize_tg_slot(tg_slot)
    send_tenant_id = _virtual_tg_tenant(int(tenant_id), normalized_slot)

    if NOTIFY_BOT_ID and int(target) == int(NOTIFY_BOT_ID):
        log(f"event=telegram_send_skip reason=notify_bot tenant={tenant_id} target={target}")
        return (0, "skip_notify_bot")

    normalized_attachments = _normalize_attachments(attachments or [])
    text_value = str(text or "").strip()

    meta: Dict[str, Any] = {}
    if reply_to:
        meta["reply_to"] = reply_to
    if peer_id is not None:
        meta["peer_id"] = peer_id

    headers: Dict[str, str] = {}
    if TG_WORKER_TOKEN:
        headers["X-Auth-Token"] = TG_WORKER_TOKEN
    headers["X-Admin-Token"] = ADMIN_TOKEN

    peer_hint = peer or str(target)
    payload_preview = {
        "tenant": tenant_id,
        "tg_slot": normalized_slot,
        "send_tenant": send_tenant_id,
        "peer": peer_hint,
        "text": text_value,
        "has_attachments": bool(normalized_attachments),
        "meta": meta,
    }
    log(f"[worker] telegram send target send_target={target}")
    log(f"[worker] telegram send payload={json.dumps(payload_preview, ensure_ascii=False)}")

    last_status, last_body = 0, ""
    last_error: Optional[str] = None
    unauthorized_checked = False

    retry_unknown = (os.getenv("TG_SEND_RETRY_ON_UNKNOWN") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    for attempt in range(3):
        timeout = float(os.getenv("TG_SEND_TEXT_TIMEOUT", "40") or 40.0)
        if normalized_attachments:
            timeout = float(os.getenv("TG_SEND_ATTACH_TIMEOUT", "90") or 90.0)
        last_status, last_body = await telegram_transport.send(
            tenant=send_tenant_id,
            text=text_value,
            peer=peer_hint,
            attachments=normalized_attachments or None,
            meta=meta or None,
            headers=headers,
            lead_id=lead_id,
            timeout=timeout,
        )
        if 200 <= last_status < 300:
            MESSAGE_OUT_COUNTER.labels("telegram", "success").inc()
            break

        parsed_error: Optional[str] = None
        forbidden_peer = False
        try:
            parsed = json.loads(last_body) if last_body else {}
        except json.JSONDecodeError:
            parsed = {}
        if isinstance(parsed, dict):
            raw_error = parsed.get("error")
            if raw_error:
                parsed_error = str(raw_error)
                if parsed_error == "forbidden_peer_type":
                    forbidden_peer = True
            if parsed_error == "send_failed":
                details = parsed.get("details")
                error_type = ""
                peer_hint = peer_id
                if isinstance(details, dict):
                    error_type = str(details.get("type") or "")
                    if details.get("peer_id") is not None:
                        peer_hint = details.get("peer_id")
                log(
                    f"[worker] telegram send_failed error_type={error_type or 'unknown'} "
                    f"peer_id={peer_hint or username or target}"
                )

        if last_status in {401, 403}:
            if forbidden_peer:
                last_error = parsed_error or "forbidden_peer_type"
                log(
                    f"[worker] telegram unauthorized_peer peer={peer_id or username or target}"
                )
                break
            if unauthorized_checked:
                break
            authorized = await _wait_until_authorized(int(send_tenant_id))
            unauthorized_checked = True
            if authorized:
                continue
            last_error = parsed_error or "not_authorized"
            break

        if last_status == 422:
            last_error = parsed_error or "validation_error"
            break

        if last_status == 0:
            if retry_unknown:
                delay = min(2 ** attempt, 8.0)
                log(
                    f"[worker] telegram network_retry attempt={attempt + 1} status={last_status} delay={delay}"  # noqa: G004
                )
                await asyncio.sleep(delay)
                continue
            last_error = parsed_error or "network_unknown"
            break

        if last_status == 429 or last_status >= 500:
            delay = min(2 ** attempt, 8.0)
            log(
                f"[worker] telegram network_retry attempt={attempt + 1} status={last_status} delay={delay}"  # noqa: G004
            )
            await asyncio.sleep(delay)
            continue

        last_error = parsed_error
        break

    log(
        f"[worker] telegram response status={last_status} body={last_body[:400]}"  # noqa: G004
    )

    if not (200 <= last_status < 300) and normalized_attachments and text_value:
        fallback_timeout = float(os.getenv("TG_SEND_TEXT_TIMEOUT", "40") or 40.0)
        log(
            "[worker] telegram attachment_send_failed fallback=text_only "
            f"status={last_status} timeout={fallback_timeout}"
        )
        fb_status, fb_body = await telegram_transport.send(
            tenant=send_tenant_id,
            text=text_value,
            peer=peer_hint,
            attachments=None,
            meta=meta or None,
            headers=headers,
            lead_id=lead_id,
            timeout=fallback_timeout,
        )
        log(
            f"[worker] telegram fallback response status={fb_status} body={fb_body[:400]}"  # noqa: G004
        )
        if 200 <= fb_status < 300:
            return fb_status, fb_body

    if last_status == 422 and not last_error:
        last_body = json.dumps({"error": "validation_error"}, ensure_ascii=False)

    return last_status, last_body

# ==== Core send ====
async def do_send(item: dict) -> tuple[str, str, str, int]:
    channel = _resolve_channel(item)
    text = (item.get("text") or "").strip()
    lead_candidate = _coerce_int(item.get("lead_id"))
    lead_id = lead_candidate if lead_candidate and lead_candidate > 0 else 0
    phone = _digits(item.get("to") or "")
    raw_to = item.get("to")
    to_peer_raw = item.get("to_peer")
    peer_field = item.get("peer")
    peer_raw = item.get("peer_id")
    peer_value: Optional[str] = None
    for candidate in (to_peer_raw, peer_field, peer_raw):
        if candidate is not None and peer_value is None:
            peer_value = str(candidate).strip() or None
    if peer_raw is None and peer_value is not None:
        peer_raw = peer_value
    username_raw = item.get("username")
    username = None
    if username_raw is not None:
        username = str(username_raw).strip() or None
    raw_telegram = item.get("telegram_user_id")
    if raw_telegram is None and peer_raw is not None:
        raw_telegram = peer_raw
    item_tg_slot = _normalize_tg_slot(item.get("tg_slot"))
    telegram_user_id: Optional[int] = None
    if raw_telegram is not None:
        try:
            candidate_id = int(raw_telegram)
        except Exception:
            telegram_user_id = None
        else:
            telegram_user_id = candidate_id if candidate_id > 0 else None
    primary_telegram_user_id = telegram_user_id
    tenant_raw = item.get("tenant_id") or item.get("tenant") or os.getenv("TENANT_ID", "1")
    try:
        tenant = int(tenant_raw)
    except Exception:
        tenant = int(os.getenv("TENANT_ID", "1"))
    attachment = item.get("attachment") if isinstance(item.get("attachment"), dict) else None
    raw_attachments = item.get("attachments") if isinstance(item.get("attachments"), list) else []
    attachments: list[dict[str, Any]] = []
    for blob in raw_attachments:
        if isinstance(blob, dict):
            attachments.append(blob)
    if attachment:
        attachments.append(attachment)
    reply_to = item.get("reply_to") if isinstance(item.get("reply_to"), str) else None
    avito_account_id = _coerce_int(item.get("account_id"))
    avito_chat_id_hint = item.get("chat_id") or item.get("peer") or item.get("peer_id")
    max_user_id = _coerce_int(item.get("max_user_id") or item.get("user_id"))
    max_chat_id_hint = item.get("chat_id") or item.get("peer") or item.get("peer_id")

    if not text and not attachment and not attachments:
        log(
            f"event=send_result status=skipped reason=empty channel={channel} lead_id={lead_id}"
        )
        return ("skipped", "empty", "", 0)

    cached_whatsapp_jid: Optional[str] = None
    if channel == "whatsapp" and lead_id > 0:
        cached_whatsapp_jid = await _resolve_cached_whatsapp_jid(tenant, lead_id)
    explicit_to_jid = item.get("to_jid") if channel == "whatsapp" else None

    if channel != "telegram" and lead_id <= 0:
        log(
            f"event=send_result status=skipped reason=missing_lead channel={channel} lead_id={lead_id}"
        )
        return ("skipped", "missing_lead", "", 0)

    if not OUTBOX_ENABLED:
        env_hint = _OUTBOX_ENABLED_RAW or "1"
        log(
            "event=send_result status=skipped reason=outbox_disabled "
            f"channel={channel} lead_id={lead_id} outbox_enabled_env={env_hint}"
        )
        return ("skipped", "outbox_disabled", "", 0)

    if channel == "max" and raw_to is None:
        if peer_value:
            raw_to = peer_value
        elif max_user_id is not None:
            raw_to = max_user_id

    if channel != "telegram":
        allowed, whitelist_reason = await _whitelist_allows(
            telegram_user_id=telegram_user_id,
            username=username,
            raw_to=raw_to,
            lead_id=lead_id,
            tenant_id=tenant,
            channel=channel,
        )
        if not allowed:
            log(
                "event=send_result status=skipped reason=whitelist_miss "
                f"channel={channel} lead_id={lead_id} telegram_user_id={telegram_user_id} "
                f"username={username} raw_to={raw_to} whitelist_reason={whitelist_reason}"
            )
            return ("skipped", "whitelist", "", 0)

    if channel != "telegram":
        lead_known = False
        try:
            lead_known = await lead_exists(lead_id, tenant_id=tenant)
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("lead_exists").inc()
            log(
                "event=send_result status=warning reason=db_error operation=lead_exists "
                f"channel={channel} lead_id={lead_id} error={exc}"
            )

        if not lead_known:
            log(
                f"event=send_result status=warning reason=err:no_lead channel={channel} lead_id={lead_id}"
            )

    if not SEND:
        log(
            f"event=send_result status=dry-run reason=send_disabled channel={channel} lead_id={lead_id}"
        )
        return ("skipped", "dry-run", "", 0)

    message_db_id_raw = _coerce_int(item.get("_message_db_id"))
    message_db_id: Optional[int] = message_db_id_raw if message_db_id_raw and message_db_id_raw > 0 else None
    title_hint: Optional[str] = None
    actual_lead_id = lead_id

    if channel == "telegram":
        from_candidate = _coerce_int(item.get("from"))
        if from_candidate is not None and from_candidate <= 0:
            from_candidate = None

        if peer_value is None and lead_id > 0:
            try:
                stored_peer = await get_lead_peer(lead_id, channel="telegram")
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("get_lead_peer").inc()
                log(
                    "event=send_peer_lookup_failed channel=%s lead_id=%s error=%s"
                    % (channel, lead_id, exc)
                )
                stored_peer = None
            if stored_peer:
                peer_value = stored_peer
        if peer_value and not to_peer_raw:
            item["to_peer"] = peer_value

        db_lookup_result: Optional[int] = None
        if primary_telegram_user_id is None and lead_id > 0:
            try:
                db_lookup_result = await get_telegram_user_id_by_lead(lead_id)
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("get_telegram_user_id_by_lead").inc()
                log(
                    "event=send_result status=skipped reason=db_error operation=get_telegram_user_id_by_lead "
                    f"channel={channel} lead_id={lead_id} error={exc}"
                )
                return ("skipped", "db_error", "", 0)
        chat_candidates: list[int] = []
        if primary_telegram_user_id is not None and primary_telegram_user_id > 0:
            chat_candidates.append(int(primary_telegram_user_id))
        if db_lookup_result is not None and db_lookup_result > 0:
            chat_candidates.append(int(db_lookup_result))
        if from_candidate is not None and from_candidate > 0:
            chat_candidates.append(int(from_candidate))
        if peer_value:
            try:
                peer_candidate = int(peer_value)
            except Exception:
                peer_candidate = None
            else:
                if peer_candidate and peer_candidate > 0:
                    chat_candidates.append(int(peer_candidate))

        chat_id: Optional[int] = None
        for candidate in chat_candidates:
            if candidate > 0:
                chat_id = int(candidate)
                break

        if chat_id is None or chat_id <= 0:
            log(
                "event=send_result status=skipped reason=missing_peer "
                f"channel={channel} lead_id={lead_id}"
            )
            return ("skipped", "missing_peer", "", 0)

        telegram_user_id = chat_id

        resolved_lead_id: Optional[int] = lead_id if lead_id > 0 else None
        if resolved_lead_id is None:
            try:
                found_lead = await find_lead_by_telegram(tenant, int(telegram_user_id))
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("find_lead_by_telegram").inc()
                log(
                    "event=send_result status=skipped reason=db_error operation=find_lead_by_telegram "
                    f"channel={channel} telegram_user_id={telegram_user_id} error={exc}"
                )
                return ("skipped", "db_error", "", 0)
            if found_lead and found_lead > 0:
                resolved_lead_id = int(found_lead)

        title_raw = item.get("title")
        title_hint = None
        if isinstance(title_raw, str):
            normalized_title = title_raw.strip() or ""
            if normalized_title:
                legacy_username = re.fullmatch(r"(?i)tg:\s*@?([a-z0-9_]{3,})", normalized_title)
                if legacy_username:
                    title_hint = f"@{legacy_username.group(1)}"
                elif re.fullmatch(r"(?i)tg:id\s+\d+", normalized_title):
                    title_hint = None
                else:
                    title_hint = normalized_title

        normalized_username = normalize_username(username)
        if not title_hint:
            if normalized_username:
                title_hint = normalized_username
            else:
                title_hint = f"tg:id {telegram_user_id}"

        upsert_kwargs = {
            "channel": "telegram",
            "tenant_id": tenant,
            "telegram_username": username,
            "title": title_hint,
            "peer_id": telegram_user_id,
            "peer": peer_value,
            "contact": normalized_username or username,
        }
        if telegram_user_id is not None:
            upsert_kwargs["telegram_user_id"] = int(telegram_user_id)

        try:
            upsert_result = await upsert_lead(
                resolved_lead_id if resolved_lead_id else None,
                **upsert_kwargs,
            )
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("upsert_lead").inc()
            log(
                "event=send_result status=skipped reason=db_error operation=upsert_lead "
                f"channel={channel} lead_id={resolved_lead_id or 0} error={exc}"
            )
            return ("skipped", "db_error", "", 0)

        if upsert_result is not None:
            try:
                resolved_lead_id = int(upsert_result)
            except Exception:
                pass

        if resolved_lead_id is None and telegram_user_id is not None:
            resolved_lead_id = int(telegram_user_id)

        if resolved_lead_id is None or resolved_lead_id <= 0:
            log(
                "event=send_result status=skipped reason=missing_lead "
                f"channel={channel} tenant={tenant} telegram_user_id={telegram_user_id}"
            )
            return ("skipped", "missing_lead", "", 0)

        actual_lead_id = resolved_lead_id
        resolved_tg_slot = item_tg_slot
        if actual_lead_id > 0:
            stored_slot = await _get_lead_tg_slot(tenant, actual_lead_id)
            if stored_slot is not None:
                resolved_tg_slot = stored_slot
        if not _telegram_slot_is_enabled(tenant, resolved_tg_slot):
            log(
                f"event=send_result status=skipped reason=tg_slot_disabled channel=telegram tenant={tenant} slot={resolved_tg_slot} lead_id={actual_lead_id}"
            )
            return ("skipped", "tg_slot_disabled", "", 0)
        item["tg_slot"] = resolved_tg_slot
        log(
            f"event=send_attempt channel=telegram tenant={tenant} slot={resolved_tg_slot} lead_id={actual_lead_id} send_target={chat_id}"
        )
        if message_db_id is None:
            try:
                message_db_id = await insert_message_out(
                    actual_lead_id,
                    text,
                    None,
                    status="queued",
                    tenant_id=tenant,
                    channel="telegram",
                    telegram_user_id=telegram_user_id,
                    telegram_username=username,
                    title=title_hint,
                    is_bot=not (_is_manager_message(item) or _is_followup_message(item)),
                    attachments=_collect_outgoing_attachments(item, tenant) or None,
                    source=(
                        (
                            f"followup:tg_slot:{_normalize_tg_slot(item.get('tg_slot'))}"
                            if _is_followup_message(item)
                            else (f"manager:tg_slot:{_normalize_tg_slot(item.get('tg_slot'))}" if _is_manager_message(item) else f"bot:tg_slot:{_normalize_tg_slot(item.get('tg_slot'))}")
                        )
                        if channel == "telegram"
                        else ("followup" if _is_followup_message(item) else ("manager" if _is_manager_message(item) else "bot"))
                    ),
                )
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("insert_message_out").inc()
                log(
                    "event=send_result status=skipped reason=db_error operation=insert_message_out "
                    f"channel={channel} lead_id={actual_lead_id} error={exc}"
                )
                return ("skipped", "db_error", "", 0)
        if message_db_id:
            item["_message_db_id"] = message_db_id
            item["_resolved_lead_id"] = actual_lead_id

    manager_message = _is_manager_message(item)
    if manager_message and actual_lead_id > 0:
        await _mark_handoff_silence(
            tenant,
            actual_lead_id,
            reason="manager_outgoing",
            contact_hint=item.get("peer") or item.get("contact"),
            username_hint=item.get("username"),
        )

    if channel == "whatsapp":
        prepared_attachment = (
            await _prepare_internal_attachment(attachment)
            if attachment
            else attachment
        )
        prepared_attachments: list[dict[str, Any]] = []
        for blob in attachments:
            if not isinstance(blob, Mapping):
                continue
            if attachment is not None and blob is attachment:
                if prepared_attachment is not None:
                    prepared_attachments.append(dict(prepared_attachment))
                else:
                    prepared_attachments.append(dict(blob))
                continue
            prepared_blob = await _prepare_internal_attachment(blob)
            prepared_attachments.append(prepared_blob)
        recipient_value = raw_to if isinstance(raw_to, str) and raw_to.strip() else phone
        provider = tenant_whatsapp_provider(tenant)
        meta_payload = item.get("meta") if isinstance(item.get("meta"), Mapping) else None
        if provider == "baileys":
            recipient_jid = ""
            source_used = None
            jid_sources: tuple[tuple[str, Any], ...] = (
                ("task", explicit_to_jid),
                ("cache", cached_whatsapp_jid),
                ("raw_to", raw_to if isinstance(raw_to, str) and raw_to.strip() else None),
                ("phone", phone),
            )
            for label, source in jid_sources:
                candidate_jid = _normalize_baileys_jid(source)
                if candidate_jid:
                    recipient_jid = candidate_jid
                    source_used = label
                    break
            if recipient_jid:
                recipient_value = recipient_jid
            if not recipient_value:
                log(
                    " ".join(
                        [
                            "[BAILEYS OUTBOUND]",
                            f"tenant={tenant}",
                            f"lead_id={actual_lead_id}",
                            f"raw_to={raw_to or '-'}",
                            f"to_jid={explicit_to_jid or '-'}",
                            f"cached_jid={cached_whatsapp_jid or '-'}",
                            "final_jid=-",
                            "status=skipped_missing_recipient",
                        ]
                    )
                )
                return ("skipped", "missing_recipient", "", 0)
            if recipient_value and "@" not in recipient_value:
                normalized_fallback = _normalize_baileys_jid(recipient_value)
                if normalized_fallback:
                    recipient_value = normalized_fallback
            log(
                " ".join(
                    [
                        "[BAILEYS OUTBOUND]",
                        f"tenant={tenant}",
                        f"lead_id={actual_lead_id}",
                        f"raw_to={raw_to or '-'}",
                        f"to_jid={explicit_to_jid or '-'}",
                        f"cached_jid={cached_whatsapp_jid or '-'}",
                        f"final_jid={recipient_value or '-'}",
                        f"source={source_used or '-'}",
                    ]
                )
            )
            st, body = await send_whatsapp_baileys(
                tenant,
                recipient_value or "",
                text or None,
                prepared_attachments or None,
                meta_payload,
            )
        else:
            st, body = await send_whatsapp(
                tenant,
                recipient_value or "",
                text or None,
                prepared_attachment,
                prepared_attachments or None,
            )
        if st == 401 and provider != "baileys":
            retry_count = 0
            try:
                retry_count = int(item.get("_waweb_auth_retry") or 0)
            except Exception:
                retry_count = 0
            attempt = retry_count + 1
            body_hint = (body or "").strip()
            if len(body_hint) > 400:
                body_hint = f"{body_hint[:400]}…"
            log(
                f"event=waweb_auth_error tenant={tenant} lead_id={actual_lead_id} "
                f"phone={phone or '-'} attempt={attempt} code={st} body={body_hint or '-'}"
            )
            retry_payload = dict(item)
            retry_payload["_waweb_auth_retry"] = attempt
            if attempt >= 3:
                try:
                    await r.lpush(OUTBOX_DLQ_KEY, json.dumps(retry_payload, ensure_ascii=False))
                except Exception:
                    pass
                return ("failed", "waweb_auth", body, st)
            try:
                await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(retry_payload, ensure_ascii=False))
            except Exception:
                log(
                    f"event=waweb_auth_error action=requeue_failed tenant={tenant} lead_id={actual_lead_id}"
                )
            return ("retry", "waweb_auth", body, st)
    elif channel == "avito":
        chat_hint = avito_chat_id_hint
        if chat_hint is not None:
            chat_hint = str(chat_hint).strip() or None
        if not manager_message:
            echo_text = normalize_echo_text(text or "")
            echo_variants: list[str] = []
            if echo_text:
                echo_variants.append(echo_text)
            if attachments:
                echo_variants.append("__image__")
            if not echo_text and attachments:
                echo_text = "__image__"
            if echo_text:
                chat_key = chat_hint or (str(avito_chat_id_hint).strip() if avito_chat_id_hint else "")
                if not chat_key:
                    try:
                        resolved_chat = await get_lead_peer(int(lead_id), channel="avito")
                    except Exception:
                        resolved_chat = ""
                    chat_key = str(resolved_chat or "").strip()
                if chat_key:
                    try:
                        payload = {"text": echo_text, "extra": echo_variants, "ts": int(time.time())}
                        await r.set(
                            avito_bot_echo_key(tenant, chat_key),
                            json.dumps(payload, ensure_ascii=False),
                            ex=AVITO_BOT_ECHO_TTL_SECONDS,
                        )
                    except Exception as exc:
                        log(
                            "event=avito_echo_cache_failed_pre tenant=%s lead_id=%s error=%s"
                            % (tenant, lead_id, exc)
                        )
        st, body = await send_avito(
            tenant,
            lead_id,
            text,
            chat_id=chat_hint,
            account_id=avito_account_id,
            attachments=attachments or None,
        )
        if 200 <= st < 300 and not manager_message:
            echo_text = normalize_echo_text(text or "")
            echo_variants: list[str] = []
            if echo_text:
                echo_variants.append(echo_text)
            if attachments:
                echo_variants.append("__image__")
            if not echo_text and attachments:
                echo_text = "__image__"
            if echo_text:
                chat_key = chat_hint or (str(avito_chat_id_hint).strip() if avito_chat_id_hint else "")
                if not chat_key:
                    try:
                        resolved_chat = await get_lead_peer(int(lead_id), channel="avito")
                    except Exception:
                        resolved_chat = ""
                    chat_key = str(resolved_chat or "").strip()
                if chat_key:
                    try:
                        payload = {"text": echo_text, "extra": echo_variants, "ts": int(time.time())}
                        await r.set(
                            avito_bot_echo_key(tenant, chat_key),
                            json.dumps(payload, ensure_ascii=False),
                            ex=AVITO_BOT_ECHO_TTL_SECONDS,
                        )
                    except Exception as exc:
                        log(
                            "event=avito_echo_cache_failed tenant=%s lead_id=%s error=%s"
                            % (tenant, lead_id, exc)
                )
    elif channel == "telegram":
        peer_id = None
        if peer_value:
            try:
                peer_id = int(peer_value)
            except Exception:
                peer_id = None
        elif peer_raw is not None:
            try:
                peer_id = int(peer_raw)
            except Exception:
                peer_id = None
        st, body = await send_telegram(
            tenant,
            tg_slot=_normalize_tg_slot(item.get("tg_slot")),
            chat_id=int(chat_id),
            peer_id=peer_id,
            peer=peer_value,
            telegram_user_id=telegram_user_id,
            username=username,
            text=text or None,
            attachments=attachments or None,
            reply_to=reply_to,
            lead_id=actual_lead_id,
        )
    elif channel == "max":
        chat_hint = max_chat_id_hint
        if chat_hint is not None:
            chat_hint = str(chat_hint).strip() or None
        st, body = await send_max(
            tenant,
            lead_id,
            text,
            chat_id=chat_hint,
            user_id=max_user_id,
            attachments=attachments or None,
        )
    else:
        recipient_value = raw_to if isinstance(raw_to, str) and raw_to.strip() else phone
        st, body = await send_whatsapp(
            tenant,
            recipient_value or "",
            text or None,
            attachment,
            attachments or None,
        )

    if 200 <= st < 300:
        status = "sent"
        reason = "ok"
    elif st in {401, 403}:
        status = "unauthorized"
        reason = f"status_{st}"
    elif st == 422:
        status = "skipped"
        reason = "validation"
    elif st == 0:
        status = "skipped"
        reason = "network"
    else:
        status = "skipped"
        reason = f"status_{st}"
    if message_db_id:
        new_status = "sent" if 200 <= st < 300 else "failed"
        try:
            await update_message_status(message_db_id, new_status)
        except Exception as exc:
            log(
                "event=send_result status=warning reason=update_message_status_failed "
                f"channel={channel} message_id={message_db_id} error={exc}"
            )

    if 200 <= st < 300 and actual_lead_id and actual_lead_id > 0:
        outbound_attachments: list[dict[str, Any]] = []
        if isinstance(attachments, list):
            for att in attachments:
                if isinstance(att, Mapping):
                    outbound_attachments.append(dict(att))
        if isinstance(attachment, Mapping):
            outbound_attachments.append(dict(attachment))
        try:
            await amocrm_service.amocrm_on_outbound_message(
                int(tenant),
                int(actual_lead_id),
                text=text or "",
                channel=str(channel),
                attachments=outbound_attachments or None,
                source_role=("manager" if manager_message else "bot"),
            )
        except Exception as exc:
            log(
                "event=amocrm_outbound_note_failed "
                f"channel={channel} tenant={tenant} lead_id={actual_lead_id} error={exc}"
            )

    status_str = str(status)
    reason_str = str(reason)
    log(
        f"event=send_result status={status_str} reason={reason_str} channel={channel} lead_id={actual_lead_id} code={st}"
    )
    return (status_str, reason_str, body, st)

# ==== Writer ====
async def write_result(item: dict, status: str, status_code: int, reason: str):
    lead_id = int(item.get("lead_id") or 0)
    tenant_raw = item.get("tenant_id") or item.get("tenant") or os.getenv("TENANT_ID", "1")
    try:
        tenant_id = int(tenant_raw)
    except Exception:
        tenant_id = int(os.getenv("TENANT_ID", "1"))
    attachment = item.get("attachment") if isinstance(item.get("attachment"), dict) else None
    text = (item.get("text") or "").strip()
    if not text and attachment:
        fname = attachment.get("filename") or ""
        text = f"[attachment] {fname}".strip()
    manager_message = _is_manager_message(item)
    sent_status = "sent"

    telegram_user_id = None
    peer_value: Optional[str] = None
    for candidate in (
        item.get("to_peer"),
        item.get("peer"),
        item.get("telegram_user_id"),
        item.get("peer_id"),
    ):
        if candidate is not None and peer_value is None:
            peer_value = str(candidate).strip() or None
    raw_peer = item.get("telegram_user_id") or item.get("peer_id")
    if raw_peer is not None:
        try:
            telegram_user_id = int(raw_peer)
        except Exception:
            telegram_user_id = None
    if telegram_user_id is None and peer_value is not None:
        try:
            telegram_user_id = int(peer_value)
        except Exception:
            telegram_user_id = None
    username = item.get("username") if isinstance(item.get("username"), str) else None

    channel_name = _resolve_channel(item)
    stored_message_id_raw = item.get("_message_db_id")
    try:
        stored_message_id = int(stored_message_id_raw)
    except Exception:
        stored_message_id = None
    if stored_message_id is not None and stored_message_id <= 0:
        stored_message_id = None
    resolved_lead_override = item.get("_resolved_lead_id")
    if isinstance(resolved_lead_override, int) and resolved_lead_override > 0:
        lead_id = resolved_lead_override

    if channel_name == "telegram" and stored_message_id:
        lead_ref = lead_id
    else:
        resolved_lead_id: Optional[int] = None
        try:
            upsert_kwargs = {
                "channel": channel_name,
                "source_real_id": None,
                "tenant_id": tenant_id,
                "telegram_username": username,
                "peer_id": telegram_user_id,
                "peer": peer_value,
                "contact": username,
            }
            if telegram_user_id is not None:
                upsert_kwargs["telegram_user_id"] = int(telegram_user_id)
            resolved_lead_id = await upsert_lead(
                lead_id,
                **upsert_kwargs,
            )
        except Exception as exc:
            log(
                "event=send_result status=skipped reason=lead_upsert_error "
                f"channel={channel_name} lead_id={lead_id} tenant={tenant_id} error={exc}"
            )
            return

        lead_ref = resolved_lead_id or lead_id
        lead_available = bool(stored_message_id)
        if not lead_available and resolved_lead_id:
            try:
                lead_available = await lead_exists(resolved_lead_id, tenant_id=tenant_id)
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("lead_exists").inc()
                log(
                    "event=send_result status=skipped reason=lead_check_error "
                    f"channel={channel_name} lead_id={resolved_lead_id} tenant={tenant_id} error={exc}"
                )
        elif not lead_available:
            log(
                "event=send_result status=skipped reason=lead_upsert_missing "
                f"channel={channel_name} lead_id={lead_id} tenant={tenant_id}"
            )

        if not lead_available:
            log(
                "event=send_result status=skipped reason=lead_missing_for_message "
                f"channel={channel_name} lead_id={lead_ref} tenant={tenant_id}"
            )
            return

        if not stored_message_id:
            try:
                attachments = _collect_outgoing_attachments(item, tenant_id)
                await insert_message_out(
                    lead_ref,
                    text,
                    None,
                    status=sent_status,
                    tenant_id=tenant_id,
                    channel=channel_name,
                    telegram_user_id=telegram_user_id,
                    telegram_username=username,
                    is_bot=not (manager_message or _is_followup_message(item)),
                    attachments=attachments or None,
                    source=(
                        (
                            f"followup:tg_slot:{_normalize_tg_slot(item.get('tg_slot'))}"
                            if _is_followup_message(item)
                            else (f"manager:tg_slot:{_normalize_tg_slot(item.get('tg_slot'))}" if manager_message else f"bot:tg_slot:{_normalize_tg_slot(item.get('tg_slot'))}")
                        )
                        if channel_name == "telegram"
                        else ("followup" if _is_followup_message(item) else ("manager" if manager_message else "bot"))
                    ),
                )
            except Exception as exc:
                log(f"[worker] insert_message_out err: {exc}")

    out = {
        "lead_id": lead_id,
        "reply": text,
        "status": sent_status,
        "version": APP_VERSION,
        "ch": item.get("ch") or item.get("provider") or "whatsapp",
    }
    await r.rpush(OUTBOX_QUEUE_KEY, json.dumps(out, ensure_ascii=False))
    log(
        f"event=enqueue_outbox queue={OUTBOX_QUEUE_KEY} lead_id={lead_id} channel={out['ch']} status={sent_status}"
    )
    log(f"[worker] reply -> lead {lead_id}: {text[:160]} ({sent_status})")


# Notification dispatcher
async def _process_notification(item: Mapping[str, Any]) -> None:
    event_name = str(item.get("event") or "notify").strip() or "notify"
    tenant_raw = item.get("tenant_id") or item.get("tenant") or os.getenv("TENANT_ID", "1")
    try:
        tenant_id = int(tenant_raw)
    except Exception:
        tenant_id = int(os.getenv("TENANT_ID", "1"))
    lead_raw = item.get("lead_id")
    try:
        lead_id = int(lead_raw) if lead_raw is not None else 0
    except Exception:
        lead_id = 0
    chat_ids = _coerce_chat_ids(item.get("chat_ids"))
    if not chat_ids:
        chat_ids = notification_chat_ids(tenant_id, event_name)
    text = (item.get("text") or "").strip()
    if not text:
        log(
            f"event=notify_skip reason=empty_text tenant={tenant_id} lead_id={lead_id} event={event_name}"
        )
        return
    if not chat_ids:
        log(
            f"event=notify_skip reason=missing_chat_ids tenant={tenant_id} lead_id={lead_id} event={event_name}"
        )
        return

    log(
        f"event=notify_dispatch tenant={tenant_id} lead_id={lead_id} event={event_name} chat_ids={chat_ids}"
    )
    for chat_id in chat_ids:
        try:
            target = int(chat_id)
        except Exception:
            continue
        log(
            f"event=notify_send_attempt tenant={tenant_id} lead_id={lead_id} event={event_name} chat_id={target}"
        )
        send_ok = False
        send_status = 0
        send_error = ""
        # Prefer dedicated notify bot if token is present. If есть токен, не падаем в fallback на tgworker.
        if NOTIFY_BOT_TOKEN:
            send_ok, send_status, send_error = await _send_notify_bot(target, text)
            if not send_ok:
                log(
                    "event=notify_send_failed tenant=%s lead_id=%s event=%s chat_id=%s status=%s error=%s"
                    % (tenant_id, lead_id, event_name, target, send_status, send_error or "-")
                )
                continue
        if not send_ok:
            headers = {}
            if ADMIN_TOKEN:
                headers["X-Admin-Token"] = ADMIN_TOKEN
            status_code, body_text = await telegram_transport.send(
                tenant=tenant_id,
                peer=str(target),
                text=text,
                headers=headers or None,
            )
            send_status = status_code
            if status_code == 200:
                send_ok = True
            else:
                send_error = body_text
        if send_ok:
            log(
                f"event=notify_send_success tenant={tenant_id} lead_id={lead_id} event={event_name} chat_id={target} status={send_status}"
            )
            continue
        log(
            "event=notify_send_failed tenant=%s lead_id=%s event=%s chat_id=%s status=%s error=%s"
            % (tenant_id, lead_id, event_name, target, send_status, send_error or "-")
        )

# Debug helper: log when notify type payload is seen in queue.

# ==== Loop ====
async def process_incoming_queue() -> None:
    log(
        f"[worker] inbox loop start enabled={int(INBOX_ENABLED)} queue={INCOMING_QUEUE_KEY}"
    )
    if not INBOX_ENABLED:
        return
    while True:
        try:
            try:
                popped = await r.brpop(INCOMING_QUEUE_KEY, timeout=INBOX_BLOCK_TIMEOUT)
            except redis_ex.ConnectionError:
                await asyncio.sleep(1.0)
                continue

            if not popped:
                continue

            _, raw_item = popped
            try:
                event = json.loads(raw_item)
            except json.JSONDecodeError:
                preview = raw_item[:160] if isinstance(raw_item, str) else str(raw_item)[:160]
                log(
                    f"event=incoming_parse_error queue={INCOMING_QUEUE_KEY} preview={preview}"
                )
                continue

            if not isinstance(event, dict):
                log(
                    f"event=incoming_skip reason=invalid_payload queue={INCOMING_QUEUE_KEY}"
                )
                continue

            try:
                await _handle_incoming_event(event)
            except Exception as exc:
                channel_hint = event.get("channel") or event.get("ch") or event.get("provider") or "-"
                log(
                    "event=incoming_unhandled channel=%s error=%s"
                    % (channel_hint, exc)
                )
                await asyncio.sleep(0)

        except Exception as exc:
            log(f"event=incoming_loop_error error={exc}")
            await asyncio.sleep(0.5)


async def process_queue():
    log(f"[worker] loop start, queues={QUEUES}")
    while True:
        item: Dict[str, Any] | None = None
        try:
            item = _pop_ready_deferred_outbox()
            if item is None:
                timeout_seconds = 5
                next_wait = _next_deferred_outbox_wait()
                if next_wait is not None:
                    timeout_seconds = max(1, min(5, int(next_wait) if next_wait > 0 else 1))
                try:
                    popped = await r.brpop(QUEUES, timeout=timeout_seconds)
                except redis_ex.ConnectionError:
                    await asyncio.sleep(1.0)
                    continue

                if not popped:
                    continue

                _, raw_item = popped
                try:
                    item = json.loads(raw_item)
                except json.JSONDecodeError:
                    log(f"[worker] json decode err: {raw_item[:200]}")
                    continue
            if isinstance(item, Mapping) and item.get("type") == "notify":
                log(
                    f"event=notify_queue_item tenant={item.get('tenant') or item.get('tenant_id') or '-'} "
                    f"lead_id={item.get('lead_id') or '-'} event={item.get('event') or 'notify'}"
                )
                try:
                    await _process_notification(item)
                except Exception:
                    log(
                        "event=notify_unhandled tenant=%s lead_id=%s event=%s"
                        % (
                            item.get("tenant_id") or item.get("tenant") or "-",
                            item.get("lead_id") or "-",
                            item.get("event") or "notify",
                        )
                    )
                continue

            if _is_status_echo(item):
                channel_hint = _resolve_channel(item)
                tenant_raw = item.get("tenant_id") or item.get("tenant") or os.getenv("TENANT_ID", "1")
                try:
                    tenant_id = int(tenant_raw)
                except Exception:
                    tenant_id = int(os.getenv("TENANT_ID", "1"))
                status = str(item.get("status") or "").strip() or "-"
                log(
                    f"event=outbox_status_echo_skip channel={channel_hint or '-'} tenant={tenant_id} status={status}"
                )
                continue

            raw_channel = item.get("provider") or item.get("ch") or item.get("channel")
            channel = ""
            if isinstance(raw_channel, str):
                channel = raw_channel.strip().lower()
            elif raw_channel is not None:
                channel = str(raw_channel).strip().lower()
            if not channel:
                channel = _resolve_channel(item)
            tenant_raw = item.get("tenant_id") or item.get("tenant") or os.getenv("TENANT_ID", "1")
            try:
                tenant_id = int(tenant_raw)
            except Exception:
                tenant_id = int(os.getenv("TENANT_ID", "1"))
            lead_candidate = _coerce_int(item.get("lead_id"))
            lead_for_log = lead_candidate if lead_candidate is not None else 0
            log(
                f"event=send_attempt channel={channel or '-'} tenant={tenant_id} lead_id={lead_for_log}"
            )

            not_before_ts = _parse_send_not_before_ts(item)
            if not_before_ts > 0:
                wait_seconds = max(0.0, not_before_ts - time.time())
                if wait_seconds > 0:
                    split_idx = _coerce_int(item.get("split_part_index")) or 0
                    split_total = _coerce_int(item.get("split_part_total")) or 0
                    log(
                        "event=send_wait_deferred channel=%s tenant=%s lead_id=%s wait=%.2fs part=%s/%s"
                        % (
                            channel or "-",
                            tenant_id,
                            lead_for_log,
                            wait_seconds,
                            split_idx or "-",
                            split_total or "-",
                        )
                    )
                    _defer_outbox_item(item, not_before_ts)
                    continue

            status, reason, body, code = await do_send(item)
            status_str = str(status)
            reason_str = str(reason)
            log(
                f"[worker] send ch={channel or '-'} status={status_str} reason={reason_str} code={code} body={body[:200]}"
            )
            resolved_lead_for_log = item.get("_resolved_lead_id")
            if isinstance(resolved_lead_for_log, int) and resolved_lead_for_log > 0:
                lead_for_status = resolved_lead_for_log
            else:
                lead_for_status = lead_for_log
            if status_str == "sent":
                log(
                    f"event=send_success channel={channel or '-'} tenant={tenant_id} lead_id={lead_for_status} reason={reason_str} code={code}"
                )
            else:
                log(
                    "event=send_failed "
                    f"channel={channel or '-'} tenant={tenant_id} lead_id={lead_for_status} reason={reason_str or status_str} code={code}"
                )
            if channel == "telegram":
                try:
                    await r.incrby("metrics:telegram:outgoing", 1)
                except Exception:
                    pass
            if status_str == "sent":
                await write_result(item, status_str, code, reason_str)

        except Exception as e:
            try:
                await r.lpush(OUTBOX_DLQ_KEY, json.dumps(item or {}, ensure_ascii=False))
            except Exception:
                pass
            log(f"[worker] err: {e}")
            await asyncio.sleep(0.5)


def _amocrm_backoff_seconds(attempts: int) -> int:
    if attempts <= 1:
        return 5
    delay = 5 * (2 ** min(attempts - 1, 6))
    return int(min(delay, 300))


def _parse_amocrm_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except Exception:
            raw = ""
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception:
            data = {}
        if isinstance(data, dict):
            return dict(data)
    return {}


def _amocrm_stage_id_from_cfg(amocrm_cfg: Mapping[str, Any] | None, stage_index: int) -> int | None:
    stages = amocrm_cfg.get("stages") if isinstance(amocrm_cfg, Mapping) else None
    if not isinstance(stages, list) or not stages:
        return None
    try:
        idx = int(stage_index)
    except Exception:
        idx = 0
    if idx < 0 or idx >= len(stages):
        idx = 0
    stage = stages[idx] if isinstance(stages[idx], Mapping) else None
    stage_id_raw = stage.get("amo_stage_id") if isinstance(stage, Mapping) else None
    try:
        stage_id = int(stage_id_raw)
    except Exception:
        stage_id = 0
    if stage_id > 0:
        return stage_id
    for item in stages:
        if not isinstance(item, Mapping):
            continue
        try:
            fallback_id = int(item.get("amo_stage_id") or 0)
        except Exception:
            fallback_id = 0
        if fallback_id > 0:
            return fallback_id
    return None


def _is_amocrm_lead_not_found_error(exc: Exception) -> bool:
    text = str(exc or "")
    return "amocrm_http_error:400" in text and "Lead not found" in text


async def _amocrm_entity_exists_in_worker(client: Any, *, entity_type: str, entity_id: int | None) -> bool | None:
    if not entity_id:
        return False
    kind = str(entity_type or "").strip().lower()
    try:
        if kind == "lead":
            payload = await client.get_lead(int(entity_id))
        elif kind == "contact":
            payload = await client.get_contact(int(entity_id))
        else:
            return None
        if not isinstance(payload, Mapping):
            return False
        remote_id = payload.get("id")
        try:
            return int(remote_id) == int(entity_id)
        except Exception:
            return False
    except Exception as exc:
        text = str(exc or "")
        if "amocrm_http_error:404" in text:
            return False
        return None


async def _recover_amocrm_missing_lead(
    *,
    tenant_id: int,
    lead_id: int,
    payload: Mapping[str, Any],
    amocrm_cfg: Mapping[str, Any],
    client: Any,
    link: Mapping[str, Any] | None,
) -> int | None:
    pipeline_id_raw = payload.get("pipeline_id") or amocrm_cfg.get("pipeline_id") or (link or {}).get("pipeline_id")
    try:
        pipeline_id = int(pipeline_id_raw)
    except Exception:
        pipeline_id = 0
    if pipeline_id <= 0:
        return None
    stage_id = _amocrm_stage_id_from_cfg(amocrm_cfg, int((link or {}).get("stage_index") or 0))
    if not stage_id:
        return None
    contact_id_raw = payload.get("amo_contact_id") or (link or {}).get("provider_contact_id")
    try:
        contact_id = int(contact_id_raw) if contact_id_raw is not None else None
    except Exception:
        contact_id = None
    lead_name = str(payload.get("lead_name") or f"Avio lead {lead_id}").strip() or f"Avio lead {lead_id}"
    new_lead_id = await client.create_lead(
        pipeline_id=int(pipeline_id),
        status_id=int(stage_id),
        name=lead_name,
        contact_id=contact_id,
        custom_fields=None,
    )
    if not new_lead_id:
        return None
    await crm_links.update_provider_lead_id(
        int(tenant_id),
        int(lead_id),
        amocrm_service.AMOCRM_PROVIDER,
        int(new_lead_id),
    )
    chat_link = await crm_chat_links.get_link(
        int(tenant_id),
        int(lead_id),
        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
    )
    await crm_chat_links.upsert_link(
        int(tenant_id),
        int(lead_id),
        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
        external_chat_id=str((chat_link or {}).get("external_chat_id") or ""),
        external_conversation_id=str((chat_link or {}).get("external_conversation_id") or ""),
        external_contact_id=int(contact_id) if contact_id is not None else None,
        external_lead_id=int(new_lead_id),
        chat_scope_id=str((chat_link or {}).get("chat_scope_id") or ""),
        source_id=str((chat_link or {}).get("source_id") or ""),
    )
    return int(new_lead_id)


async def _handle_amocrm_event(event: Mapping[str, Any]) -> None:
    tenant_id = int(event.get("tenant_id") or 0)
    lead_id = int(event.get("lead_id") or 0)
    payload = _parse_amocrm_payload(event.get("payload"))
    event_type = str(event.get("event_type") or payload.get("event_type") or "")
    cfg = read_tenant_config(int(tenant_id))
    amocrm_cfg = amocrm_service.get_amocrm_cfg(cfg)
    if not amocrm_cfg or not bool(amocrm_cfg.get("enabled")):
        return
    base_url = await amocrm_service.resolve_api_base_url(amocrm_cfg, int(tenant_id))
    oauth_cfg = amocrm_service.resolve_oauth_cfg(amocrm_cfg, int(tenant_id))
    client = amocrm_integration.AmoCRMClient(
        tenant_id=int(tenant_id),
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )
    if event_type == "create_lead":
        link = await crm_links.get_link(int(tenant_id), int(lead_id), amocrm_service.AMOCRM_PROVIDER)
        if link and link.get("provider_lead_id") is not None:
            try:
                provider_lead_id_value = int(link.get("provider_lead_id"))
            except Exception:
                provider_lead_id_value = None
            lead_exists = await _amocrm_entity_exists_in_worker(
                client,
                entity_type="lead",
                entity_id=provider_lead_id_value,
            )
            if lead_exists is False:
                await crm_links.update_provider_lead_id(
                    int(tenant_id),
                    int(lead_id),
                    amocrm_service.AMOCRM_PROVIDER,
                    None,
                )
                link = await crm_links.get_link(int(tenant_id), int(lead_id), amocrm_service.AMOCRM_PROVIDER)
            elif lead_exists is True:
                return
        stage_id = payload.get("stage_id")
        pipeline_id = payload.get("pipeline_id") or amocrm_cfg.get("pipeline_id")
        lead_name = str(payload.get("lead_name") or f"Avio lead {lead_id}")
        contact_phone = payload.get("contact_phone")
        contact_name = payload.get("contact_name")
        custom_fields = payload.get("custom_fields")
        source_channel = str(payload.get("channel") or "").strip().lower()
        if not stage_id or not pipeline_id:
            raise amocrm_integration.AmoCRMError("amocrm_stage_missing")

        async def _contact_exists(contact_value: int | None) -> bool:
            if not contact_value:
                return False
            try:
                payload = await client.get_contact(int(contact_value))
            except Exception:
                return False
            if not isinstance(payload, Mapping) or not payload:
                return False
            remote_id = payload.get("id")
            try:
                return int(remote_id) == int(contact_value)
            except Exception:
                return False

        existing_contact_id = None
        if isinstance(link, Mapping) and link.get("provider_contact_id") is not None:
            try:
                existing_contact_id = int(link.get("provider_contact_id"))
            except Exception:
                existing_contact_id = None
        if existing_contact_id is None and source_channel in {"telegram", "avito"}:
            try:
                chat_link = await crm_chat_links.get_link(
                    int(tenant_id),
                    int(lead_id),
                    amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
                )
            except Exception:
                chat_link = None
            if isinstance(chat_link, Mapping) and chat_link.get("external_contact_id") is not None:
                try:
                    existing_contact_id = int(chat_link.get("external_contact_id"))
                except Exception:
                    existing_contact_id = None
        if existing_contact_id and not await _contact_exists(existing_contact_id):
            existing_contact_id = None
        phone_value = str(contact_phone or "").strip() or None
        name_value = str(contact_name or "").strip() or None
        contact_id = existing_contact_id or await client.upsert_contact(
            phone=phone_value,
            name=name_value,
        )
        if contact_id and not await _contact_exists(int(contact_id)):
            contact_id = await client.upsert_contact(
                phone=phone_value,
                name=name_value,
            )
            if contact_id and not await _contact_exists(int(contact_id)):
                contact_id = None
        if contact_id:
            await crm_links.update_provider_contact_id(
                int(tenant_id),
                int(lead_id),
                amocrm_service.AMOCRM_PROVIDER,
                int(contact_id),
            )
        amo_lead_id = await client.create_lead(
            pipeline_id=int(pipeline_id),
            status_id=int(stage_id),
            name=lead_name,
            contact_id=contact_id,
            custom_fields=custom_fields if isinstance(custom_fields, list) else None,
        )
        if amo_lead_id:
            await crm_links.update_provider_lead_id(
                int(tenant_id), int(lead_id), amocrm_service.AMOCRM_PROVIDER, int(amo_lead_id)
            )
            chat_link = await crm_chat_links.get_link(
                int(tenant_id),
                int(lead_id),
                amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
            )
            # Always normalize to canonical amo conversation id once amo_lead_id is known.
            # This prevents parallel temporary/fallback conversation ids in Inbox.
            fallback_chat_id = (
                str((chat_link or {}).get("external_chat_id") or "").strip()
                or f"avio:{int(tenant_id)}:amo:{int(amo_lead_id)}"
            )
            fallback_conversation_id = (
                str((chat_link or {}).get("external_conversation_id") or "").strip()
                or fallback_chat_id
            )
            canonical_chat_id = fallback_chat_id
            canonical_conversation_id = fallback_conversation_id
            try:
                canonical_chat_id, canonical_conversation_id = await amocrm_chat_service._canonical_chat_identity(
                    int(tenant_id),
                    provider_lead_id=int(amo_lead_id),
                    fallback_chat_id=fallback_chat_id,
                    fallback_conversation_id=fallback_conversation_id,
                )
            except Exception:
                pass
            chat_scope_id = str((chat_link or {}).get("chat_scope_id") or "")
            source_id = str((chat_link or {}).get("source_id") or "")
            chat_link = await crm_chat_links.upsert_link(
                int(tenant_id),
                int(lead_id),
                amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
                external_chat_id=str(canonical_chat_id or ""),
                external_conversation_id=str(canonical_conversation_id or ""),
                external_contact_id=int(contact_id) if contact_id is not None else None,
                external_lead_id=int(amo_lead_id),
                chat_scope_id=chat_scope_id,
                source_id=source_id,
            )
            if isinstance(chat_link, Mapping):
                try:
                    await amocrm_chat_service.sync_chat_profile(
                        int(tenant_id),
                        int(lead_id),
                        cfg=cfg,
                    )
                except Exception:
                    log(
                        "event=amocrm_chat_profile_sync_failed tenant=%s lead_id=%s"
                        % (tenant_id, lead_id)
                    )
                try:
                    fetchrow = getattr(db_module, "_fetchrow", None)
                    bootstrap_text = str(payload.get("bootstrap_text") or "").strip()
                    bootstrap_direction = str(payload.get("bootstrap_direction") or "").strip().lower()
                    if bootstrap_direction not in {"in", "out"}:
                        bootstrap_direction = "out"
                    bootstrap_attachments_raw = payload.get("bootstrap_attachments")
                    bootstrap_attachments = (
                        list(bootstrap_attachments_raw)
                        if isinstance(bootstrap_attachments_raw, list)
                        else None
                    )
                    if fetchrow:
                        if not bootstrap_text:
                            row = await fetchrow(
                                """
                                SELECT text
                                FROM messages
                                WHERE tenant_id = $1
                                  AND lead_id = $2
                                  AND is_bot = TRUE
                                  AND text IS NOT NULL
                                  AND btrim(text) <> ''
                                ORDER BY id DESC
                                LIMIT 1
                                """,
                                int(tenant_id),
                                int(lead_id),
                            )
                            if isinstance(row, Mapping):
                                bootstrap_text = str(row.get("text") or "").strip()
                            elif row:
                                try:
                                    bootstrap_text = str(dict(row).get("text") or "").strip()
                                except Exception:
                                    bootstrap_text = ""
                    if bootstrap_text:
                        await amocrm_chat_service.enqueue_message(
                            int(tenant_id),
                            int(lead_id),
                            direction=bootstrap_direction,
                            text=bootstrap_text,
                            channel=source_channel or "telegram",
                            attachments=bootstrap_attachments,
                        )
                    else:
                        log(
                            "event=amocrm_chat_bootstrap_skipped tenant=%s lead_id=%s reason=no_message_text"
                            % (tenant_id, lead_id)
                        )
                except Exception as exc:
                    log(
                        "event=amocrm_chat_bootstrap_failed tenant=%s lead_id=%s error=%s"
                        % (tenant_id, lead_id, exc)
                    )
            stage_index = payload.get("stage_index")
            if stage_index is not None:
                try:
                    stage_index_val = int(stage_index)
                except Exception:
                    stage_index_val = None
                if stage_index_val is not None:
                    await crm_links.update_stage_index(
                        int(tenant_id),
                        int(lead_id),
                        amocrm_service.AMOCRM_PROVIDER,
                        stage_index_val,
                        pipeline_id=int(pipeline_id) if pipeline_id else None,
                    )
        return
    if event_type == "delete_lead":
        amo_lead_id_raw = payload.get("amo_lead_id") or payload.get("provider_lead_id")
        try:
            amo_lead_id = int(amo_lead_id_raw) if amo_lead_id_raw is not None else 0
        except Exception:
            amo_lead_id = 0
        if amo_lead_id <= 0:
            return
        try:
            await client.delete_lead(int(amo_lead_id))
        except amocrm_integration.AmoCRMError as exc:
            if "amocrm_http_error:404" not in str(exc):
                raise
        return
    if event_type == "delete_contact":
        amo_contact_id_raw = payload.get("amo_contact_id") or payload.get("provider_contact_id")
        try:
            amo_contact_id = int(amo_contact_id_raw) if amo_contact_id_raw is not None else 0
        except Exception:
            amo_contact_id = 0
        if amo_contact_id <= 0:
            return
        try:
            await client.delete_contact(int(amo_contact_id))
        except amocrm_integration.AmoCRMError as exc:
            if "amocrm_http_error:404" not in str(exc):
                raise
        return
    link = await crm_links.get_link(int(tenant_id), int(lead_id), amocrm_service.AMOCRM_PROVIDER)
    provider_lead_id = link.get("provider_lead_id") if isinstance(link, Mapping) else None
    if event_type == "chat_sync_message":
        provider_contact_id = link.get("provider_contact_id") if isinstance(link, Mapping) else None
        direction = str(payload.get("direction") or "in").strip().lower()
        channel = str(payload.get("channel") or "").strip().lower()
        lead_exists = await _amocrm_entity_exists_in_worker(
            client,
            entity_type="lead",
            entity_id=int(provider_lead_id) if provider_lead_id is not None else None,
        )
        if lead_exists is False and provider_lead_id is not None:
            await crm_links.update_provider_lead_id(
                int(tenant_id),
                int(lead_id),
                amocrm_service.AMOCRM_PROVIDER,
                None,
            )
            provider_lead_id = None
        if provider_lead_id is not None and provider_contact_id is None:
            try:
                resolved_contact = await client.get_lead_contact_id(int(provider_lead_id))
            except Exception:
                resolved_contact = None
            if resolved_contact:
                provider_contact_id = int(resolved_contact)
                await crm_links.update_provider_contact_id(
                    int(tenant_id),
                    int(lead_id),
                    amocrm_service.AMOCRM_PROVIDER,
                    int(provider_contact_id),
                )
        contact_exists = await _amocrm_entity_exists_in_worker(
            client,
            entity_type="contact",
            entity_id=int(provider_contact_id) if provider_contact_id is not None else None,
        )
        if contact_exists is False and provider_contact_id is not None:
            await crm_links.update_provider_contact_id(
                int(tenant_id),
                int(lead_id),
                amocrm_service.AMOCRM_PROVIDER,
                None,
            )
            provider_contact_id = None
        # Do not let Chat API bootstrap a separate entity when CRM link isn't ready yet.
        if provider_lead_id is None or provider_contact_id is None:
            if channel in {"avito", "telegram"} and direction in {"in", "out"}:
                raise amocrm_integration.AmoCRMError("amocrm_chat_link_missing")
        external_chat_id = str(payload.get("external_chat_id") or "").strip()
        external_conversation_id = str(payload.get("external_conversation_id") or external_chat_id).strip()
        try:
            external_chat_id, external_conversation_id = await amocrm_chat_service._canonical_chat_identity(
                int(tenant_id),
                provider_lead_id=int(provider_lead_id) if provider_lead_id is not None else None,
                fallback_chat_id=external_chat_id,
                fallback_conversation_id=external_conversation_id,
            )
        except Exception:
            pass
        payload = {
            **dict(payload),
            "external_chat_id": external_chat_id,
            "external_conversation_id": external_conversation_id,
        }
        await crm_chat_links.upsert_link(
            int(tenant_id),
            int(lead_id),
            amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
            external_chat_id=str(external_chat_id or ""),
            external_conversation_id=str(external_conversation_id or ""),
            external_contact_id=int(provider_contact_id) if provider_contact_id is not None else None,
            external_lead_id=int(provider_lead_id) if provider_lead_id is not None else None,
            chat_scope_id=str(payload.get("scope_id") or ""),
            source_id=str(payload.get("source_id") or ""),
        )
        await amocrm_chat_service.push_message(
            int(tenant_id),
            payload={
                **dict(payload),
                "tenant_id": int(tenant_id),
                "lead_id": int(lead_id),
                "amo_lead_id": int(provider_lead_id) if provider_lead_id is not None else None,
                "amo_contact_id": int(provider_contact_id) if provider_contact_id is not None else None,
            },
            cfg=cfg,
        )
        return
    if not provider_lead_id:
        raise amocrm_integration.AmoCRMError("amocrm_lead_missing")
    if event_type == "update_fields":
        custom_fields = payload.get("custom_fields")
        lead_name = str(payload.get("lead_name") or "").strip() or None
        if isinstance(custom_fields, list) or lead_name:
            try:
                await client.update_lead_fields(
                    int(provider_lead_id),
                    name=lead_name,
                    custom_fields=custom_fields if isinstance(custom_fields, list) else [],
                )
            except amocrm_integration.AmoCRMError as exc:
                if (
                    lead_name
                    and not (isinstance(custom_fields, list) and custom_fields)
                    and _is_amocrm_lead_not_found_error(exc)
                ):
                    recovered_lead_id = await _recover_amocrm_missing_lead(
                        tenant_id=int(tenant_id),
                        lead_id=int(lead_id),
                        payload=payload,
                        amocrm_cfg=amocrm_cfg,
                        client=client,
                        link=link if isinstance(link, Mapping) else None,
                    )
                    if recovered_lead_id:
                        await client.update_lead_fields(
                            int(recovered_lead_id),
                            name=lead_name,
                            custom_fields=[],
                        )
                        return
                raise
        return
    if event_type == "update_contact_fields":
        custom_fields = payload.get("custom_fields")
        contact_name = str(payload.get("contact_name") or "").strip() or None
        provider_contact_id = link.get("provider_contact_id") if isinstance(link, Mapping) else None
        if not provider_contact_id and provider_lead_id:
            provider_contact_id = await client.get_lead_contact_id(int(provider_lead_id))
            if provider_contact_id:
                await crm_links.update_provider_contact_id(
                    int(tenant_id),
                    int(lead_id),
                    amocrm_service.AMOCRM_PROVIDER,
                    int(provider_contact_id),
                )
        if provider_contact_id and (isinstance(custom_fields, list) or contact_name):
            await client.update_contact_fields(
                int(provider_contact_id),
                name=contact_name,
                custom_fields=custom_fields if isinstance(custom_fields, list) else [],
            )
        return
    if event_type == "add_files":
        attachments = payload.get("attachments")
        if not provider_lead_id or not isinstance(attachments, list):
            return
        for item in attachments:
            if not isinstance(item, Mapping):
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            content, name, detected_mime = _download_file(url)
            if not content:
                log(
                    f"amocrm_file_skip tenant={tenant_id} lead_id={lead_id} "
                    f"reason=download_failed url={url}"
                )
                continue
            filename = str(item.get("filename") or name or "attachment")
            content_type = (
                str(item.get("mime") or item.get("mime_type") or "").strip()
                or (detected_mime.strip() if detected_mime else "")
                or None
            )
            file_uuid = await client.upload_file(
                filename=filename,
                content=content,
                content_type=content_type,
            )
            if file_uuid:
                await client.attach_file_to_lead(int(provider_lead_id), file_uuid)
            else:
                log(
                    f"amocrm_file_skip tenant={tenant_id} lead_id={lead_id} "
                    f"reason=upload_failed url={url}"
                )
        return
    if event_type == "move_stage":
        stage_id = payload.get("stage_id")
        pipeline_id = payload.get("pipeline_id") or amocrm_cfg.get("pipeline_id")
        if not stage_id:
            return
        await client.move_lead_stage(
            int(provider_lead_id),
            status_id=int(stage_id),
            pipeline_id=int(pipeline_id) if pipeline_id else None,
        )
        stage_index = payload.get("stage_index")
        if stage_index is not None:
            try:
                stage_index_val = int(stage_index)
            except Exception:
                stage_index_val = None
            if stage_index_val is not None:
                await crm_links.update_stage_index(
                    int(tenant_id),
                    int(lead_id),
                    amocrm_service.AMOCRM_PROVIDER,
                    stage_index_val,
                    pipeline_id=int(pipeline_id) if pipeline_id else None,
                )
        return
    if event_type == "add_note":
        text = str(payload.get("text") or "").strip()
        if text:
            await client.add_lead_note(int(provider_lead_id), text)
        return


async def process_amocrm_outbox() -> None:
    if not AMOCRM_OUTBOX_ENABLED:
        log("event=amocrm_outbox_disabled")
        return
    log("event=amocrm_outbox_loop_start")
    while True:
        try:
            events = await crm_outbox.take_pending(limit=AMOCRM_OUTBOX_LIMIT)
            if not events:
                await asyncio.sleep(2.0)
                continue
            events = sorted(
                events,
                key=lambda item: int(item.get("id") or 0),
            )
            for event in events:
                event_id = event.get("id")
                if not event_id:
                    continue
                try:
                    await _handle_amocrm_event(event)
                    await crm_outbox.mark_done(int(event_id))
                    log(
                        f"amocrm_event_done tenant={event.get('tenant_id')} "
                        f"lead_id={event.get('lead_id')} event={event.get('event_type')}"
                    )
                except Exception as exc:
                    attempts = int(event.get("attempts") or 0) + 1
                    if attempts >= AMOCRM_OUTBOX_MAX_ATTEMPTS:
                        await crm_outbox.mark_dead(int(event_id), str(exc))
                        log(
                            f"amocrm_event_dead tenant={event.get('tenant_id')} "
                            f"lead_id={event.get('lead_id')} event={event.get('event_type')} error={exc}"
                        )
                    else:
                        delay = _amocrm_backoff_seconds(attempts)
                        next_retry = datetime.now(tz=timezone.utc) + timedelta(seconds=delay)
                        await crm_outbox.mark_retry(int(event_id), attempts, next_retry, str(exc))
                        log(
                            f"amocrm_event_retry tenant={event.get('tenant_id')} "
                            f"lead_id={event.get('lead_id')} event={event.get('event_type')} attempts={attempts}"
                        )
            await asyncio.sleep(0)
        except Exception as exc:
            log(f"event=amocrm_outbox_loop_error err={exc}")
            await asyncio.sleep(2.0)


async def process_training_embeddings() -> None:
    if not LEARNING_EMBEDDINGS_ENABLED:
        log("event=training_embeddings_disabled")
        return
    log(f"event=training_embeddings_loop_start model={EMBEDDING_MODEL}")
    while True:
        try:
            pending = await fetch_pending_training_examples(limit=5)
            if not pending:
                await asyncio.sleep(5.0)
                continue
            texts = [str(item.get("q_text") or "") for item in pending]
            try:
                vectors = await training_embeddings.embed_texts(texts)
            except Exception as exc:
                log(f"event=training_embeddings_failed reason={exc} count={len(pending)}")
                for item in pending:
                    try:
                        await set_training_embedding(
                            item.get("id"),
                            None,
                            embedding_model=EMBEDDING_MODEL,
                            status="failed",
                            error=str(exc),
                        )
                    except Exception:
                        pass
                await asyncio.sleep(5.0)
                continue

            for item, vec in zip(pending, vectors):
                try:
                    await set_training_embedding(
                        item.get("id"),
                        list(vec) if isinstance(vec, (list, tuple)) else vec,
                        embedding_model=EMBEDDING_MODEL,
                        status="ready",
                        error=None,
                    )
                    log(
                        f"event=training_embedding_saved id={item.get('id')} tenant={item.get('tenant_id')}"
                    )
                except Exception as exc:
                    log(
                        "event=training_embedding_save_failed id=%s tenant=%s err=%s"
                        % (item.get("id"), item.get("tenant_id"), exc)
                    )
            await asyncio.sleep(0.2)
        except Exception as exc:
            log(f"event=training_embeddings_loop_error err={exc}")
            await asyncio.sleep(2.0)

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
        tasks.append(
            asyncio.create_task(process_incoming_queue(), name="inbox-loop")
        )
    if LEARNING_EMBEDDINGS_ENABLED:
        tasks.append(
            asyncio.create_task(process_training_embeddings(), name="training-embeddings-loop")
        )
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())

AVITO_CHAT_CACHE: Dict[int, str] = {}
