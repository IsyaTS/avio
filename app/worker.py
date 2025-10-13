from __future__ import annotations
import os
import re
import json
import asyncio
import urllib.request
import urllib.error
from typing import Any, Dict, Tuple

import redis.asyncio as redis
from redis import exceptions as redis_ex

from db import init_db, upsert_lead, insert_message_out

try:
    from core import settings as core_settings  # type: ignore
    _default_version = getattr(core_settings, "APP_VERSION", "v21.0")
except Exception:
    _default_version = "v21.0"

APP_VERSION = os.getenv("APP_VERSION", _default_version)

# ==== ENV ====
REDIS_URL  = os.getenv("REDIS_URL", "redis://redis:6379/0")
WA_WEB_URL = (os.getenv("WA_WEB_URL", "http://waweb:8088") or "http://waweb:8088").rstrip("/")
# Match waweb INTERNAL_SYNC_TOKEN resolution (WA_WEB_TOKEN or WEBHOOK_SECRET)
WA_INTERNAL_TOKEN = (os.getenv("WA_WEB_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
TG_WORKER_URL = (os.getenv("TG_WORKER_URL", "http://tgworker:9000") or "http://tgworker:9000").rstrip("/")
TG_WORKER_TOKEN = (os.getenv("TG_WORKER_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
SEND       = (os.getenv("SEND_ENABLED","true").lower() == "true")

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

def _http_json(
    method: str,
    url: str,
    data: dict | None = None,
    timeout: float = 10.0,
    headers: Dict[str, str] | None = None,
) -> Tuple[int, str]:
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


async def send_telegram(
    tenant_id: int,
    peer_id: int | None,
    telegram_user_id: int | None,
    text: str | None,
    attachments: list[dict[str, Any]] | None = None,
    reply_to: str | None = None,
    username: str | None = None,
) -> tuple[int, str]:
    url = f"{TG_WORKER_URL}/send"
    payload: Dict[str, Any] = {"tenant": tenant_id}
    if peer_id:
        payload["peer_id"] = int(peer_id)
    if telegram_user_id:
        payload["telegram_user_id"] = int(telegram_user_id)
    if text:
        payload["text"] = text
    if attachments:
        payload["attachments"] = attachments
    if reply_to:
        payload["reply_to"] = reply_to
    if username:
        payload["username"] = username
    headers: Dict[str, str] = {}
    if TG_WORKER_TOKEN:
        headers["X-Auth-Token"] = TG_WORKER_TOKEN
    last_status, last_body = 0, ""
    for attempt in range(3):
        last_status, last_body = await asyncio.to_thread(
            _http_json, "POST", url, payload, 12.0, headers
        )
        if 200 <= last_status < 300:
            break
        log(
            f"[worker] telegram send retry attempt={attempt + 1} status={last_status} body={last_body[:160]}"
        )
        if 200 <= last_status < 500:
            break
        await asyncio.sleep(0.5 * (attempt + 1))
    return last_status, last_body

# ==== Core send ====
async def do_send(item: dict) -> tuple[str, str]:
    channel = (item.get("ch") or item.get("provider") or "").lower()
    text     = (item.get("text") or "").strip()
    lead_id  = int(item.get("lead_id") or 0)
    phone    = _digits(item.get("to") or "")
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
            peer_id,
            target_user,
            text or None,
            attachments or None,
            reply_to,
            username,
        )
    else:
        st, body = await send_whatsapp(tenant, phone, text or None, attachment)

    status = "sent" if 200 <= st < 300 else f"err:{st}"
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
