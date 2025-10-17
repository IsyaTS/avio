from __future__ import annotations
import os
import re
import json
import asyncio
import urllib.request
import urllib.error
from typing import Any, Dict, Iterable, Mapping, Optional
from urllib.parse import urljoin, urlparse

import redis.asyncio as redis
from redis import exceptions as redis_ex

try:
    from app.core import settings as core_settings  # type: ignore
except Exception:  # pragma: no cover - fallback for bootstrap edge cases
    from types import SimpleNamespace

    core_settings = SimpleNamespace(APP_VERSION="v21.0")  # type: ignore[assignment]

from app.db import (
    init_db,
    insert_message_out,
    upsert_lead,
    lead_exists,
    find_lead_by_telegram,
    get_telegram_user_id_by_lead,
    update_message_status,
)
from app.metrics import MESSAGE_OUT_COUNTER, DB_ERRORS_COUNTER
from app.common import (
    OUTBOX_QUEUE_KEY,
    OUTBOX_DLQ_KEY,
    get_outbox_whitelist,
    normalize_username,
    smart_reply_enabled,
)
from app.core import build_llm_messages, ask_llm

# Guard against attribute absence when the worker boots before settings load
_default_version = getattr(core_settings, "APP_VERSION", "v21.0")

APP_VERSION = os.getenv("APP_VERSION", _default_version)

# ==== ENV ====
REDIS_URL  = os.getenv("REDIS_URL", "redis://redis:6379/0")
WA_WEB_URL = (os.getenv("WA_WEB_URL", "http://waweb:9001") or "http://waweb:9001").rstrip("/")
# Match waweb INTERNAL_SYNC_TOKEN resolution (WA_WEB_TOKEN or WEBHOOK_SECRET)
WA_INTERNAL_TOKEN = (os.getenv("WA_WEB_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
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
TGWORKER_SEND_URL = f"{TGWORKER_BASE_URL}/send"
TGWORKER_STATUS_URL = f"{TGWORKER_BASE_URL}/status"
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()
_OUTBOX_ENABLED_RAW = (os.getenv("OUTBOX_ENABLED") or "").strip().lower()
OUTBOX_ENABLED = _OUTBOX_ENABLED_RAW not in {"0", "false"}
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
def log(msg: str):
    print(msg, flush=True)

def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _coerce_int(value: Any) -> Optional[int]:
    try:
        result = int(str(value).strip())
    except Exception:
        return None
    return result


OUTBOX_WHITELIST = get_outbox_whitelist()


def _whitelist_allows(
    *,
    telegram_user_id: Optional[int],
    username: Optional[str],
    raw_to: Any,
) -> bool:
    if OUTBOX_WHITELIST.allow_all:
        return True

    candidate_ids: set[int] = set()
    if telegram_user_id is not None:
        candidate_ids.add(int(telegram_user_id))
    raw_id = _coerce_int(raw_to)
    if raw_id is not None:
        candidate_ids.add(raw_id)
    for candidate in candidate_ids:
        if candidate in OUTBOX_WHITELIST.ids:
            return True

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
    return any(name in OUTBOX_WHITELIST.usernames for name in candidate_names)


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


async def _handle_incoming_event(event: Mapping[str, Any]) -> None:
    channel_raw = event.get("channel") or event.get("ch") or event.get("provider")
    channel = ""
    if isinstance(channel_raw, str):
        channel = channel_raw.strip().lower()
    elif channel_raw is not None:
        channel = str(channel_raw).strip().lower()
    if channel != "telegram":
        return

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

    upsert_kwargs: Dict[str, Any] = {
        "channel": "telegram",
        "tenant_id": tenant_id,
        "peer_id": peer_id,
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

    log(
        f"event=inbox_lead_resolved channel=telegram tenant={tenant_id} lead_id={lead_id}"
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
    attachment: dict | None = None,
) -> tuple[int, str]:
    url = f"{WA_WEB_URL}/session/{tenant_id}/send"
    payload: Dict[str, Any] = {"to": phone}
    if text:
        payload["text"] = text
    if attachment:
        payload["attachment"] = attachment
    headers: Dict[str, str] = {}
    if WA_INTERNAL_TOKEN:
        headers["X-Auth-Token"] = WA_INTERNAL_TOKEN
    last_status, last_body = 0, ""
    for attempt in range(3):
        last_status, last_body = await asyncio.to_thread(
            _http_json, "POST", url, payload, 12.0, headers
        )
        if 200 <= last_status < 500:
            break
        await asyncio.sleep(0.5 * (attempt + 1))
    return last_status, last_body

async def send_avito(tenant_id: int, lead_id: int, text: str) -> tuple[int,str]:
    # заглушка, если есть WA — шлём туда; при необходимости заменить на Avito API
    phone = ""
    return await send_whatsapp(tenant_id, phone, text)


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
    telegram_user_id: int | None,
    username: str | None,
    text: str | None,
    attachments: list[dict[str, Any]] | None = None,
    reply_to: str | None = None,
) -> tuple[int, str]:
    target = int(chat_id)
    normalized_attachments = _normalize_attachments(attachments or [])
    payload: Dict[str, Any] = {
        "tenant": int(tenant_id),
        "channel": "telegram",
        "to": target,
    }
    meta: Dict[str, Any] = {}
    if reply_to:
        meta["reply_to"] = reply_to
    if peer_id is not None:
        meta["peer_id"] = peer_id
    if meta:
        payload["meta"] = meta
    text_value = str(text) if text is not None else ""
    text_value = text_value.strip()
    if text_value:
        payload["text"] = text_value
    if normalized_attachments:
        payload["attachments"] = normalized_attachments

    headers: Dict[str, str] = {}
    if TG_WORKER_TOKEN:
        headers["X-Auth-Token"] = TG_WORKER_TOKEN
    headers["X-Admin-Token"] = ADMIN_TOKEN

    payload_log = json.dumps(payload, ensure_ascii=False)
    log(f"[worker] telegram send target send_target={target}")
    log(f"[worker] telegram send payload={payload_log}")

    last_status, last_body = 0, ""
    last_error: Optional[str] = None
    unauthorized_checked = False

    for attempt in range(3):
        last_status, last_body = await asyncio.to_thread(
            _http_json, "POST", TGWORKER_SEND_URL, payload, 15.0, headers
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
    peer_raw = item.get("peer_id")
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
        if not _whitelist_allows(
            telegram_user_id=telegram_user_id,
            username=username,
            raw_to=raw_to,
        ):
            log(
                "event=send_result status=skipped reason=whitelist_miss "
                f"channel={channel} lead_id={lead_id} telegram_user_id={telegram_user_id} "
                f"username={username} raw_to={raw_to}"
            )
            return ("skipped", "whitelist", "", 0)

    if channel != "telegram":
        try:
            lead_known = await lead_exists(lead_id, tenant_id=tenant)
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("lead_exists").inc()
            log(
                "event=send_result status=skipped reason=db_error operation=lead_exists "
                f"channel={channel} lead_id={lead_id} error={exc}"
            )
            return ("skipped", "db_error", "", 0)

        if not lead_known:
            log(
                f"event=send_result status=skipped reason=err:no_lead channel={channel} lead_id={lead_id}"
            )
            return ("skipped", "err:no_lead", "", 0)

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
        st, body = await send_whatsapp(tenant, phone, text or None, attachment)
    elif channel == "avito":
        st, body = await send_avito(tenant, lead_id, text)
    elif channel == "telegram":
        peer_id = None
        if peer_raw is not None:
            try:
                peer_id = int(peer_raw)
            except Exception:
                peer_id = None
        st, body = await send_telegram(
            tenant,
            chat_id=int(chat_id),
            peer_id=peer_id,
            telegram_user_id=telegram_user_id,
            username=username,
            text=text or None,
            attachments=attachments or None,
            reply_to=reply_to,
        )
    else:
        st, body = await send_whatsapp(tenant, phone, text or None, attachment)

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
    raw_peer = item.get("telegram_user_id") or item.get("peer_id")
    if raw_peer is not None:
        try:
            telegram_user_id = int(raw_peer)
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
