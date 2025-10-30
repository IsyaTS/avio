from __future__ import annotations
import os
import base64
import re
import json
import time
import asyncio
import urllib.request
import urllib.error
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, Optional
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, unquote, quote

import httpx

import redis.asyncio as redis
from redis import exceptions as redis_ex

try:
    from app.core import settings as core_settings, tenant_waweb_url  # type: ignore
except Exception:  # pragma: no cover - fallback for bootstrap edge cases
    from types import SimpleNamespace

    core_settings = SimpleNamespace(APP_VERSION="v21.0", WA_WEB_URL="http://waweb:9001")  # type: ignore[assignment]
    def tenant_waweb_url(tenant: int | None) -> str:  # type: ignore
        if tenant is None:
            return "http://waweb:9001"
        return f"http://waweb-{tenant}:9001"

from app.db import (
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
    resolve_or_create_contact,
    link_lead_contact,
)
from app.dao import get_or_create_by_peer
from app.metrics import MESSAGE_OUT_COUNTER, DB_ERRORS_COUNTER
from app.common import (
    OUTBOX_QUEUE_KEY,
    OUTBOX_DLQ_KEY,
    get_outbox_whitelist,
    normalize_username,
    smart_reply_enabled,
    whitelist_contains_number,
)
from app.core import build_llm_messages, ask_llm
from app.integrations import avito as avito_integration
from app.transport import (
    WhatsAppAddressError,
    normalize_e164_digits,
    normalize_whatsapp_recipient,
)
from app.transport import telegram as telegram_transport
from app.web.common import WA_INTERNAL_TOKEN as COMMON_WA_INTERNAL_TOKEN

# Guard against attribute absence when the worker boots before settings load
_default_version = getattr(core_settings, "APP_VERSION", "v21.0")

APP_VERSION = os.getenv("APP_VERSION", _default_version)

# ==== ENV ====
REDIS_URL  = os.getenv("REDIS_URL", "redis://redis:6379/0")
# Match waweb INTERNAL_SYNC_TOKEN resolution (shared with the web layer)
WA_INTERNAL_TOKEN = COMMON_WA_INTERNAL_TOKEN
TGWORKER_BASE_URL = (
    os.getenv("TGWORKER_URL")
    or os.getenv("TGWORKER_BASE_URL")
    or os.getenv("TG_WORKER_URL")
    or ""
).strip()
if not TGWORKER_BASE_URL:
    TGWORKER_BASE_URL = "http://tgworker:9000"
TGWORKER_BASE_URL = TGWORKER_BASE_URL.rstrip("/") or "http://tgworker:9000"
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
AVITO_TIMEOUT = getattr(core_settings, "AVITO_TIMEOUT", 10.0)
_INBOX_ENABLED_RAW = (os.getenv("INBOX_ENABLED") or "").strip().lower()
INBOX_ENABLED = _INBOX_ENABLED_RAW not in {"", "0", "false", "no", "off"}
INCOMING_QUEUE_KEY = (
    os.getenv("INCOMING_QUEUE_KEY")
    or os.getenv("INBOX_QUEUE_KEY")
    or "inbox:message_in"
)
try:
    INBOX_BLOCK_TIMEOUT = max(1, int(os.getenv("INBOX_BLOCK_TIMEOUT", "5")))
except Exception:
    INBOX_BLOCK_TIMEOUT = 5
TENANT_ID  = int(os.getenv("TENANT_ID","1"))
QUEUES = [OUTBOX_QUEUE_KEY]

r = redis.from_url(REDIS_URL, decode_responses=True)

# ==== Utils ====
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


def log(*parts: object):
    if len(parts) == 1:
        print(parts[0], flush=True)
    else:
        print(" ".join(str(p) for p in parts), flush=True)

AVITO_CHAT_CACHE: Dict[int, str] = {}

def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _normalize_whatsapp_peer(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raw = str(raw)
    peer = raw.strip()
    if not peer:
        return None
    return peer.lower()


def _coerce_int(value: Any) -> Optional[int]:
    try:
        result = int(str(value).strip())
    except Exception:
        return None
    return result


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
    if item.get("telegram_user_id") is not None or item.get("peer_id") is not None:
        return "telegram"
    return "whatsapp"


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
    url = _normalize_url(str(blob.get("url") or ""))
    if not url:
        return None
    attachment_type = str(blob.get("type") or blob.get("kind") or "file").strip() or "file"
    name = blob.get("name") or blob.get("filename") or blob.get("title")
    if isinstance(name, str):
        name = name.strip() or None
    mime = blob.get("mime") or blob.get("mime_type") or blob.get("content_type")
    if isinstance(mime, str):
        mime = mime.strip() or None
    size_value: Optional[int] = None
    for key in ("size", "filesize", "length"):
        raw = blob.get(key)
        candidate = _coerce_int(raw)
        if candidate is not None:
            size_value = candidate
            break
    normalized = {
        "type": attachment_type,
        "url": url,
    }
    if name:
        normalized["name"] = name
    if mime:
        normalized["mime"] = mime
    if size_value is not None and size_value >= 0:
        normalized["size"] = size_value
    return normalized


def _normalize_attachments(blobs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for blob in blobs:
        item = _normalize_attachment(blob)
        if item:
            normalized.append(item)
    return normalized


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
        return relative, absolute

    path = parsed.path or ""
    if not path.startswith("/"):
        path = f"/{path}"
    relative = urlunsplit(("", "", path, query, fragment))
    absolute = f"{_internal_base_url()}{relative}"
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
    prepared["b64"] = base64.b64encode(data).decode("ascii")
    prepared["sendMediaAsDocument"] = True
    prepared.setdefault("size", len(data))
    return _tokenize_attachment_mapping(prepared)


def _build_wa_document_payload(
    attachment: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not isinstance(attachment, Mapping):
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
    tenant_raw = event.get("tenant") or event.get("tenant_id") or os.getenv("TENANT_ID", "1")
    try:
        tenant_id = int(tenant_raw)
    except Exception:
        tenant_id = int(os.getenv("TENANT_ID", "1"))

    text_raw = event.get("text")
    text = "" if text_raw is None else str(text_raw)
    text = text.strip()

    message_id_raw = event.get("message_id")
    message_id = str(message_id_raw) if message_id_raw is not None else ""

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

    lead_candidate = _coerce_int(event.get("lead_id"))
    lead_id = lead_candidate if lead_candidate and lead_candidate > 0 else 0

    title_hint: Optional[str] = None
    normalized_username = normalize_username(username)
    if normalized_username:
        title_hint = f"tg:{normalized_username}"
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

    contact_hint = normalized_username or username

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

    peer_log_hint = peer_value or (str(peer_id) if peer_id is not None else None)
    if peer_log_hint is None and telegram_user_id is not None:
        peer_log_hint = str(telegram_user_id)
    log(
        f"event=inbox_lead_resolved channel=telegram tenant={tenant_id} lead_id={lead_id} peer={peer_log_hint or '-'}"
    )

    if lead_id <= 0:
        log(
            f"event=skip_missing_lead channel=telegram tenant={tenant_id} message_id={message_id}"
        )
        return

    if not text:
        log(
            f"event=skip_no_text channel=telegram tenant={tenant_id} lead_id={lead_id}"
        )
        return

    if not smart_reply_enabled(tenant_id):
        log(
            f"event=smart_reply_disabled channel=telegram tenant={tenant_id} lead_id={lead_id}"
        )
        return

    contact_id = _coerce_int(event.get("contact_id"))
    refer_id = contact_id if contact_id and contact_id > 0 else lead_id

    try:
        messages = await build_llm_messages(refer_id, text, "telegram", tenant=tenant_id)
    except Exception as exc:
        log(
            "event=smart_reply_failed channel=telegram tenant=%s lead_id=%s stage=build_messages error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    try:
        reply = await ask_llm(
            messages,
            tenant=tenant_id,
            contact_id=refer_id if refer_id > 0 else None,
            channel="telegram",
        )
    except Exception as exc:
        log(
            "event=smart_reply_failed channel=telegram tenant=%s lead_id=%s stage=ask_llm error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    reply_text = (reply or "").strip()
    if not reply_text:
        log(
            f"event=smart_reply_empty channel=telegram tenant={tenant_id} lead_id={lead_id}"
        )
        return

    log(
        f"event=smart_reply_generated channel=telegram tenant={tenant_id} lead_id={lead_id}"
    )

    out_payload: Dict[str, Any] = {
        "lead_id": int(lead_id),
        "tenant": int(tenant_id),
        "tenant_id": int(tenant_id),
        "provider": "telegram",
        "ch": "telegram",
        "channel": "telegram",
        "text": reply_text,
        "attachments": [],
    }
    if message_id:
        out_payload["message_id"] = message_id
    if telegram_user_id is not None:
        out_payload["telegram_user_id"] = str(telegram_user_id)
    if peer_id is not None:
        out_payload["peer_id"] = int(peer_id)
    if username:
        out_payload["username"] = username

    try:
        await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(out_payload, ensure_ascii=False))
    except Exception as exc:
        log(
            "event=smart_reply_enqueue_failed channel=telegram tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    log(
        f"event=smart_reply_enqueued channel=telegram tenant={tenant_id} lead_id={lead_id}"
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
        event.get("from")
        or event.get("from_jid")
        or event.get("from_raw")
        or event.get("sender")
    )
    sender_peer = _normalize_whatsapp_peer(sender_raw)
    if not sender_peer:
        log(
            f"event=skip_invalid_sender channel=whatsapp tenant={tenant_id} message_id={message_id}"
        )
        return

    peer_local = sender_peer.split("@", 1)[0]
    sender_digits = _digits(peer_local)

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
        if text:
            try:
                await insert_message_in(
                    lead_id,
                    text,
                    status="received",
                    tenant_id=tenant_id,
                )
                stored_incoming = True
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("insert_message_in").inc()
                log(
                    "event=store_incoming_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
                    % (tenant_id, lead_id, exc)
                )

    if text and not stored_incoming and db_available:
        try:
            await insert_message_in(
                lead_id,
                text,
                status="received",
                tenant_id=tenant_id,
            )
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("insert_message_in").inc()
            log(
                "event=store_incoming_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
                % (tenant_id, lead_id, exc)
            )

    refer_id = contact_id if contact_id and contact_id > 0 else lead_id

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

    try:
        messages = await build_llm_messages(
            refer_id,
            text,
            "whatsapp",
            tenant=tenant_id,
        )
    except Exception as exc:
        log(
            "event=smart_reply_failed channel=whatsapp tenant=%s lead_id=%s stage=build_messages error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    try:
        reply = await ask_llm(
            messages,
            tenant=tenant_id,
            contact_id=refer_id if refer_id > 0 else None,
            channel="whatsapp",
        )
    except Exception as exc:
        log(
            "event=smart_reply_failed channel=whatsapp tenant=%s lead_id=%s stage=ask_llm error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    reply_text = (reply or "").strip()
    if not reply_text:
        log(
            f"event=smart_reply_empty channel=whatsapp tenant={tenant_id} lead_id={lead_id}"
        )
        return

    log(
        f"event=smart_reply_generated channel=whatsapp tenant={tenant_id} lead_id={lead_id}"
    )

    out_payload: Dict[str, Any] = {
        "lead_id": int(lead_id),
        "tenant": int(tenant_id),
        "tenant_id": int(tenant_id),
        "provider": "whatsapp",
        "ch": "whatsapp",
        "channel": "whatsapp",
        "text": reply_text,
        "attachments": [],
        "to": sender_digits,
    }
    if message_id:
        out_payload["message_id"] = message_id

    try:
        await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(out_payload, ensure_ascii=False))
    except Exception as exc:
        log(
            "event=smart_reply_enqueue_failed channel=whatsapp tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    log(
        f"event=smart_reply_enqueued channel=whatsapp tenant={tenant_id} lead_id={lead_id}"
    )


async def _handle_avito_incoming(event: Mapping[str, Any]) -> None:
    tenant_raw = event.get("tenant") or event.get("tenant_id") or os.getenv("TENANT_ID", "1")
    try:
        tenant_id = int(tenant_raw)
    except Exception:
        tenant_id = int(os.getenv("TENANT_ID", "1"))

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

    attachments = event.get("attachments") if isinstance(event.get("attachments"), list) else []
    if not text and not attachments:
        log(
            f"event=skip_empty_message channel=avito tenant={tenant_id} chat_id={chat_id}"
        )
        return

    account_id = _coerce_int(event.get("account_id") or (event.get("avito") or {}).get("account_id"))
    user_id = _coerce_int(event.get("avito_user_id") or (event.get("avito") or {}).get("user_id"))
    login_value = event.get("avito_login") or (event.get("avito") or {}).get("login")
    login = login_value.strip() if isinstance(login_value, str) else None

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

    lead_id = _coerce_int(event.get("lead_id"))
    if not lead_id or lead_id <= 0:
        lead_row = None
        try:
            lead_row = await find_lead_by_peer(tenant_id, "avito", chat_id)
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("find_lead_by_peer").inc()
            log(
                "event=warning reason=db_error operation=find_lead_by_peer channel=avito tenant=%s chat_id=%s error=%s"
                % (tenant_id, chat_id, exc)
            )
        if lead_row and lead_row.get("id"):
            lead_id = int(lead_row["id"])
        else:
            account_hint = account_id if account_id is not None else tenant_id
            lead_id = avito_integration.stable_lead_id(account_hint, chat_id)

    contact_id = 0
    try:
        contact_id = await resolve_or_create_contact(
            avito_user_id=user_id,
            avito_login=login,
        )
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

    try:
        await insert_message_in(
            lead_id,
            text,
            status="received",
            tenant_id=tenant_id,
        )
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("insert_message_in").inc()
        log(
            "event=store_incoming_failed channel=avito tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )

    if not text:
        return

    if not smart_reply_enabled(tenant_id):
        log(
            f"event=smart_reply_disabled channel=avito tenant={tenant_id} lead_id={lead_id}"
        )
        return

    refer_id = contact_id if contact_id and contact_id > 0 else lead_id

    try:
        messages = await build_llm_messages(
            refer_id,
            text,
            "avito",
            tenant=tenant_id,
        )
    except Exception as exc:
        log(
            "event=smart_reply_failed channel=avito tenant=%s lead_id=%s stage=build_messages error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    try:
        reply = await ask_llm(
            messages,
            tenant=tenant_id,
            contact_id=refer_id if refer_id > 0 else None,
            channel="avito",
        )
    except Exception as exc:
        log(
            "event=smart_reply_failed channel=avito tenant=%s lead_id=%s stage=ask_llm error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    reply_text = (reply or "").strip()
    if not reply_text:
        log(
            f"event=smart_reply_empty channel=avito tenant={tenant_id} lead_id={lead_id}"
        )
        return

    out_payload: Dict[str, Any] = {
        "lead_id": int(lead_id),
        "tenant": int(tenant_id),
        "tenant_id": int(tenant_id),
        "provider": "avito",
        "ch": "avito",
        "channel": "avito",
        "text": reply_text,
        "attachments": [],
        "chat_id": chat_id,
        "peer": chat_id,
        "peer_id": chat_id,
    }
    if account_id is not None:
        out_payload["account_id"] = account_id
    if message_id:
        out_payload["message_id"] = message_id
    if user_id is not None:
        out_payload["avito_user_id"] = user_id
    if login:
        out_payload["avito_login"] = login

    try:
        await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(out_payload, ensure_ascii=False))
    except Exception as exc:
        log(
            "event=smart_reply_enqueue_failed channel=avito tenant=%s lead_id=%s error=%s"
            % (tenant_id, lead_id, exc)
        )
        return

    log(
        f"event=smart_reply_enqueued channel=avito tenant={tenant_id} lead_id={lead_id}"
    )


_INCOMING_EVENT_HANDLERS: dict[
    str, Callable[[Mapping[str, Any]], Awaitable[None]]
] = {
    "telegram": _handle_telegram_incoming,
    "whatsapp": _handle_whatsapp_incoming,
    "avito": _handle_avito_incoming,
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
        payload["attachment"] = attachment_copy

    if attachments:
        for blob in attachments:
            if not isinstance(blob, Mapping):
                continue
            if attachment is not None and blob is attachment:
                continue
            _append_attachment(blob)

    if attachments_payload:
        payload["attachments"] = attachments_payload
        if document_block:
            payload["document"] = document_block

    headers: Dict[str, str] = {}
    admin_token = (
        str(getattr(core_settings, "ADMIN_TOKEN", "") or "")
        or ADMIN_TOKEN
        or ""
    ).strip()
    headers["X-Auth-Token"] = admin_token
    if WA_INTERNAL_TOKEN:
        headers.setdefault("X-Internal-Token", WA_INTERNAL_TOKEN)

    last_status, last_body = 0, ""
    retry_delays = (0.5, 1.0, 2.0)
    for attempt in range(len(retry_delays)):
        last_status, last_body = await asyncio.to_thread(
            _http_json, "POST", url, payload, 12.0, headers
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

async def send_avito(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: Optional[str] = None,
    account_id: Optional[int] = None,
) -> tuple[int, str]:
    text_value = (text or "").strip()
    if not text_value:
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

    url = f"https://api.avito.ru/messenger/v1/accounts/{account_value}/chats/{chat_text}/messages"
    payload = {"type": "text", "message": {"text": text_value}}
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    async def _post_message(current_token: str) -> httpx.Response:
        headers["Authorization"] = f"Bearer {current_token}"
        async with httpx.AsyncClient(timeout=AVITO_TIMEOUT) as client:
            return await client.post(url, json=payload, headers=headers)

    response = await _post_message(token)

    if response.status_code == 401 and integration.get("refresh_token"):
        try:
            refreshed = await avito_integration.refresh_access_token(int(tenant_id))
            new_token = str(refreshed.get("access_token") or "").strip()
        except avito_integration.AvitoOAuthError as exc:
            log(
                "event=send_result status=error reason=token_refresh_failed channel=avito tenant=%s error=%s"
                % (tenant_id, exc)
            )
            return (response.status_code, response.text)

        if new_token:
            response = await _post_message(new_token)

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

    for attempt in range(3):
        last_status, last_body = await telegram_transport.send(
            tenant=tenant_id,
            text=text_value,
            peer=peer_hint,
            attachments=normalized_attachments or None,
            meta=meta or None,
            headers=headers,
            lead_id=lead_id,
            timeout=15.0,
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
            authorized = await _wait_until_authorized(int(tenant_id))
            unauthorized_checked = True
            if authorized:
                continue
            last_error = parsed_error or "not_authorized"
            break

        if last_status == 422:
            last_error = parsed_error or "validation_error"
            break

        if last_status == 429 or last_status == 0 or last_status >= 500:
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

    if not text and not attachment:
        log(
            f"event=send_result status=skipped reason=empty channel={channel} lead_id={lead_id}"
        )
        return ("skipped", "empty", "", 0)

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

    message_db_id: Optional[int] = None
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

        normalized_username = normalize_username(username)
        title_hint: Optional[str] = None
        if normalized_username:
            title_hint = f"tg:{normalized_username}"
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
        log(
            f"event=send_attempt channel=telegram tenant={tenant} lead_id={actual_lead_id} send_target={chat_id}"
        )
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
        st, body = await send_whatsapp(
            tenant,
            recipient_value or "",
            text or None,
            prepared_attachment,
            prepared_attachments or None,
        )
        if st == 401:
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
        st, body = await send_avito(
            tenant,
            lead_id,
            text,
            chat_id=chat_hint,
            account_id=avito_account_id,
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
    stored_message_id = item.get("_message_db_id")
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
        lead_available = False
        if resolved_lead_id:
            try:
                lead_available = await lead_exists(resolved_lead_id, tenant_id=tenant_id)
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("lead_exists").inc()
                log(
                    "event=send_result status=skipped reason=lead_check_error "
                    f"channel={channel_name} lead_id={resolved_lead_id} tenant={tenant_id} error={exc}"
                )
        else:
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

        sent_status = "sent"
        try:
            await insert_message_out(
                lead_ref,
                text,
                None,
                status=sent_status,
                tenant_id=tenant_id,
                channel=channel_name,
                telegram_user_id=telegram_user_id,
                telegram_username=username,
            )
        except Exception as exc:
            log(f"[worker] insert_message_out err: {exc}")
    sent_status = "sent"

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
            try:
                popped = await r.brpop(QUEUES, timeout=5)
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

async def main():
    log(f"[worker] boot {APP_VERSION}")
    await init_db()
    tasks = [
        asyncio.create_task(process_queue(), name="outbox-loop"),
    ]
    if INBOX_ENABLED:
        tasks.append(
            asyncio.create_task(process_incoming_queue(), name="inbox-loop")
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
