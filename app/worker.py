from __future__ import annotations
import os
import re
import json
import asyncio
import urllib.request
import urllib.error
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urljoin, urlparse

import redis.asyncio as redis
from redis import exceptions as redis_ex

try:
    from app.core import settings as core_settings  # type: ignore
except Exception:  # pragma: no cover - fallback for bootstrap edge cases
    from types import SimpleNamespace

    core_settings = SimpleNamespace(APP_VERSION="v21.0")  # type: ignore[assignment]

from app.db import init_db, insert_message_out, upsert_lead
from app.metrics import MESSAGE_OUT_COUNTER

# Guard against attribute absence when the worker boots before settings load
_default_version = getattr(core_settings, "APP_VERSION", "v21.0")

APP_VERSION = os.getenv("APP_VERSION", _default_version)

# ==== ENV ====
REDIS_URL  = os.getenv("REDIS_URL", "redis://redis:6379/0")
WA_WEB_URL = (os.getenv("WA_WEB_URL", "http://waweb:8088") or "http://waweb:8088").rstrip("/")
# Match waweb INTERNAL_SYNC_TOKEN resolution (WA_WEB_TOKEN or WEBHOOK_SECRET)
WA_INTERNAL_TOKEN = (os.getenv("WA_WEB_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
TGWORKER_BASE_URL = (
    os.getenv("TGWORKER_BASE_URL")
    or os.getenv("TG_WORKER_URL")
    or os.getenv("TGWORKER_URL")
    or ""
).strip()
if not TGWORKER_BASE_URL:
    raise RuntimeError("TGWORKER_BASE_URL is not configured")
TGWORKER_BASE_URL = TGWORKER_BASE_URL.rstrip("/")
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

TENANT_ID  = int(os.getenv("TENANT_ID","1"))
OUTBOX     = "outbox:send"
DLQ        = "outbox:dlq"
QUEUES = [OUTBOX]

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


def _normalize_username(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if not cleaned.startswith("@"):
        cleaned = f"@{cleaned.lstrip('@')}"
    return cleaned


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


def _resolve_telegram_to(
    raw_to: Any,
    *,
    peer_id: Optional[int],
    telegram_user_id: Optional[int],
    username: Optional[str],
) -> Optional[int | str]:
    if isinstance(raw_to, str):
        candidate = raw_to.strip()
        if candidate:
            lowered = candidate.lower()
            if lowered in {"me", "self"}:
                return "me"
            if candidate.startswith("@"):
                return candidate
            coerced = _coerce_int(candidate)
            if coerced is not None:
                return coerced
            return _normalize_username(candidate)
    elif raw_to is not None:
        coerced = _coerce_int(raw_to)
        if coerced is not None:
            return coerced

    normalized_username = _normalize_username(username)
    if normalized_username:
        return normalized_username

    for candidate in (telegram_user_id, peer_id):
        if candidate is None:
            continue
        coerced = _coerce_int(candidate)
        if coerced is not None:
            return coerced

    return None


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
    peer_id: int | None,
    telegram_user_id: int | None,
    username: str | None,
    text: str | None,
    raw_to: Any,
    attachments: list[dict[str, Any]] | None = None,
    reply_to: str | None = None,
) -> tuple[int, str]:
    target = _resolve_telegram_to(
        raw_to,
        peer_id=peer_id,
        telegram_user_id=telegram_user_id,
        username=username,
    )
    if target is None:
        body = json.dumps({"error": "recipient_unresolved"}, ensure_ascii=False)
        return 422, body

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
    text_value = (text or "").strip()
    if text_value:
        payload["text"] = text_value
    if normalized_attachments:
        payload["attachments"] = normalized_attachments

    headers: Dict[str, str] = {}
    if TG_WORKER_TOKEN:
        headers["X-Auth-Token"] = TG_WORKER_TOKEN

    payload_log = json.dumps(payload, ensure_ascii=False)
    log(f"[worker] telegram send payload={payload_log}")

    last_status, last_body = 0, ""
    last_error: Optional[str] = None
    unauthorized_checked = False

    for attempt in range(3):
        last_status, last_body = await asyncio.to_thread(
            _http_json, "POST", TGWORKER_SEND_URL, payload, 15.0, headers
        )
        if 200 <= last_status < 300:
            MESSAGE_OUT_COUNTER.labels("telegram").inc()
            break

        parsed_error: Optional[str] = None
        try:
            parsed = json.loads(last_body) if last_body else {}
        except json.JSONDecodeError:
            parsed = {}
        if isinstance(parsed, dict):
            raw_error = parsed.get("error")
            if raw_error:
                parsed_error = str(raw_error)
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

        if last_status == 0 or last_status >= 500:
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
async def do_send(item: dict) -> tuple[str, str]:
    channel = (item.get("ch") or item.get("provider") or "").lower()
    text     = (item.get("text") or "").strip()
    lead_id  = int(item.get("lead_id") or 0)
    phone    = _digits(item.get("to") or "")
    raw_to   = item.get("to")
    peer_raw = item.get("peer_id")
    username = item.get("username")
    telegram_user_id = None
    if item.get("telegram_user_id") is not None:
        try:
            telegram_user_id = int(item.get("telegram_user_id") or 0)
        except Exception:
            telegram_user_id = None
    tenant   = int(item.get("tenant_id") or os.getenv("TENANT_ID","1"))
    attachment = item.get("attachment") if isinstance(item.get("attachment"), dict) else None
    raw_attachments = item.get("attachments") if isinstance(item.get("attachments"), list) else []
    attachments: list[dict[str, Any]] = []
    for blob in raw_attachments:
        if isinstance(blob, dict):
            attachments.append(blob)
    if attachment:
        attachments.append(attachment)
    reply_to = item.get("reply_to") if isinstance(item.get("reply_to"), str) else None

    if (not text and not attachment) or not lead_id:
        return ("skipped", "empty")

    if not SEND:
        return ("dry-run", f"provider={channel}")

    if channel == "whatsapp":
        st, body = await send_whatsapp(tenant, phone, text or None, attachment)
    elif channel == "avito":
        st, body = await send_avito(tenant, lead_id, text)
    elif channel == "telegram":
        peer_id = None
        if peer_raw:
            try:
                peer_id = int(peer_raw)
            except Exception:
                peer_id = None
        target_user = telegram_user_id if telegram_user_id is not None else None
        st, body = await send_telegram(
            tenant,
            peer_id=peer_id,
            telegram_user_id=target_user,
            username=username,
            text=text or None,
            raw_to=raw_to,
            attachments=attachments or None,
            reply_to=reply_to,
        )
    else:
        st, body = await send_whatsapp(tenant, phone, text or None, attachment)

    if 200 <= st < 300:
        status = "sent"
    elif st == 422:
        status = "err:validation"
    elif st in {401, 403}:
        status = "err:unauthorized"
    elif st == 0:
        status = "err:network"
    else:
        status = f"err:{st}"
    return (status, body)

# ==== Writer ====
async def write_result(item: dict, status: str):
    lead_id = int(item.get("lead_id") or 0)
    tenant_id = int(item.get("tenant_id") or os.getenv("TENANT_ID", "1"))
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

    try:
        channel_name = (item.get("ch") or item.get("provider") or "whatsapp")
        await upsert_lead(
            lead_id,
            channel=channel_name,
            source_real_id=None,
            tenant_id=tenant_id,
            telegram_user_id=telegram_user_id,
            telegram_username=username,
        )
    except Exception as e:
        log(f"[worker] upsert_lead err: {e}")

    sent_status = "sent" if status.startswith("sent") else status
    try:
        await insert_message_out(lead_id, text, None, status=sent_status, tenant_id=tenant_id)
    except Exception as e:
        log(f"[worker] insert_message_out err: {e}")

    out = {
        "lead_id": lead_id,
        "reply": text,
        "status": sent_status,
        "version": APP_VERSION,
        "ch": item.get("ch") or item.get("provider") or "whatsapp",
    }
    await r.rpush("outbox", json.dumps(out, ensure_ascii=False))
    log(f"[worker] reply -> lead {lead_id}: {text[:160]} ({sent_status})")


# ==== Loop ====
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

            status, body = await do_send(item)
            channel = (item.get("ch") or item.get("provider") or "").lower()
            log(
                f"[worker] send ch={channel or '-'} status={status} body={body[:200]}"
            )
            if channel == "telegram":
                try:
                    await r.incrby("metrics:telegram:outgoing", 1)
                except Exception:
                    pass
            await write_result(item, status)

        except Exception as e:
            try:
                await r.lpush(DLQ, json.dumps(item or {}, ensure_ascii=False))
            except Exception:
                pass
            log(f"[worker] err: {e}")
            await asyncio.sleep(0.5)

async def main():
    log(f"[worker] boot {APP_VERSION}")
    await init_db()
    try:
        await process_queue()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    asyncio.run(main())
