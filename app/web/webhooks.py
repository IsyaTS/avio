from __future__ import annotations

import json
import os
import time
import pathlib
import logging
import random
from typing import Any, Dict, Tuple, Mapping, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

try:
    import core  # type: ignore
except ImportError:  # pragma: no cover
    from app import core  # type: ignore

try:
    from db import (  # type: ignore
        resolve_or_create_contact,
        link_lead_contact,
        insert_message_in,
        upsert_lead,
        insert_webhook_event,
    )
except ImportError:  # pragma: no cover
    from app.db import (  # type: ignore
        resolve_or_create_contact,
        link_lead_contact,
        insert_message_in,
        upsert_lead,
        insert_webhook_event,
    )

try:
    from . import common as C  # type: ignore
except ImportError:  # pragma: no cover
    from app.web import common as C  # type: ignore

from app.integrations import avito

from .ui import templates  # noqa: F401 - ensure templates loaded for compatibility
from app.common import OUTBOX_QUEUE_KEY, smart_reply_enabled
from app.metrics import DB_ERRORS_COUNTER, WEBHOOK_PROVIDER_COUNTER
from app.repo import provider_tokens as provider_tokens_repo


logger = logging.getLogger("app.web.webhooks")

INCOMING_QUEUE_KEY = (
    os.getenv("INBOX_QUEUE")
    or os.getenv("INBOX_QUEUE_KEY")
    or os.getenv("INCOMING_QUEUE_KEY")
    or "inbox:message_in"
)
INCOMING_DEDUP_TTL = 60 * 60 * 24  # 24 hours

router = APIRouter()


ask_llm = core.ask_llm  # type: ignore[attr-defined]
build_llm_messages = core.build_llm_messages  # type: ignore[attr-defined]
settings = core.settings  # type: ignore[attr-defined]


_redis_queue = settings.r
_catalog_sent_cache: dict[Tuple[int, str], float] = {}

WA_QR_CACHE_TTL_MIN = 180  # seconds
WA_QR_CACHE_TTL_MAX = 300  # seconds

_CATALOG_KEYWORDS = (
    "каталог",
    "прайс",
    "прайс-лист",
    "catalog",
    "price",
    "pdf",
)


def _user_requested_catalog(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(token in lowered for token in _CATALOG_KEYWORDS)


def _digits(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _ok(data: dict | None = None, status: int = 200) -> JSONResponse:
    payload = {"ok": True}
    if data:
        payload.update(data)
    return JSONResponse(payload, status_code=status)


def _resolve_catalog_attachment(
    cfg: dict | None,
    tenant: int,
    request: Request | None = None,
) -> tuple[dict | None, str]:
    meta: dict | None = None
    if isinstance(cfg, dict):
        integrations = cfg.get("integrations", {}) if isinstance(cfg.get("integrations"), dict) else {}
        raw_meta = integrations.get("uploaded_catalog")
        if isinstance(raw_meta, dict) and raw_meta:
            meta = raw_meta
    if not meta:
        persona_meta = core.persona_catalog_pdf(int(tenant))
        if persona_meta:
            meta = persona_meta
    if not isinstance(meta, dict):
        return None, ""
    if (meta.get("type") or "").lower() != "pdf":
        return None, ""
    raw_path = (meta.get("path") or "").replace("\\", "/")
    if not raw_path:
        return None, ""
    try:
        safe = pathlib.PurePosixPath(raw_path)
    except Exception:
        return None, ""
    if safe.is_absolute() or ".." in safe.parts:
        return None, ""

    try:
        tenant_root = core.tenant_dir(tenant)
        target = tenant_root / str(safe)
    except Exception:
        return None, ""

    if not target.exists() or not target.is_file():
        return None, ""

    if request is not None:
        base = str(request.url_for("internal_catalog_file", tenant=str(tenant)))
    else:
        base_root = settings.APP_INTERNAL_URL or settings.APP_PUBLIC_URL or ""
        if not base_root:
            base_root = "http://app:8000"
        base = f"{base_root.rstrip('/')}/internal/tenant/{tenant}/catalog-file"

    from urllib.parse import quote

    url = f"{base}?path={quote(str(safe), safe='/')}"
    token = settings.WEBHOOK_SECRET or ""
    if token:
        url += f"&token={quote(token)}"

    filename = meta.get("original") or safe.name
    mime = meta.get("mime") or "application/pdf"
    caption = f"Каталог в PDF: {filename}"

    attachment = {
        "url": url,
        "filename": filename,
        "mime_type": mime,
    }
    return attachment, caption


async def process_incoming(body: dict, request: Request | None = None) -> JSONResponse:
    src = body.get("source") or {}
    provider = (
        src.get("type")
        or body.get("provider")
        or body.get("channel")
        or body.get("ch")
        or "whatsapp"
    ).lower()
    raw_tenant = src.get("tenant") or body.get("tenant_id") or os.getenv("TENANT_ID", "1")
    tenant_candidate = _coerce_int(raw_tenant)
    if tenant_candidate is None:
        logger.warning(
            "lead_upsert_err:invalid_tenant message_in_lead_upsert_fail tenant_raw=%s",
            raw_tenant,
        )
        raise HTTPException(status_code=400, detail="invalid_tenant")
    tenant = tenant_candidate

    msg = body.get("message") or {}
    raw_message_id = (
        msg.get("message_id")
        or msg.get("id")
        or (msg.get("key") or {}).get("id")
        or body.get("message_id")
        or body.get("id")
    )
    message_id = str(raw_message_id) if raw_message_id is not None else ""
    text = (msg.get("text") or msg.get("body") or body.get("text") or "").strip()
    whatsapp_phone = ""
    telegram_user_id: int | None = None
    telegram_username: str | None = None
    peer_id: int | None = None
    peer_value: str | None = None
    contact_value: str | None = None
    avito_user_id: int | None = None
    avito_login: str | None = None
    avito_chat_id: str | None = None
    avito_account_id = _coerce_int(body.get("account_id") or src.get("account_id"))
    attachments: list[dict[str, Any]] = []

    raw_attachments = msg.get("attachments") or body.get("attachments")
    if isinstance(raw_attachments, list):
        attachments = [item for item in raw_attachments if isinstance(item, dict)]

    if provider == "telegram":
        raw_id = (
            msg.get("telegram_user_id")
            or body.get("telegram_user_id")
            or body.get("user_id")
        )
        if raw_id is not None:
            try:
                telegram_user_id = int(raw_id)
            except Exception:
                telegram_user_id = None
        raw_username = msg.get("telegram_username") or body.get("username")
        if isinstance(raw_username, str):
            telegram_username = raw_username.strip() or None
        else:
            telegram_username = None
        contact_value = telegram_username
        peer_candidate = (
            msg.get("peer")
            or body.get("peer")
            or msg.get("peer_id")
            or body.get("peer_id")
            or msg.get("chat_id")
            or body.get("chat_id")
        )
        if peer_candidate is not None:
            peer_value = str(peer_candidate).strip() or None
            if peer_value is not None:
                try:
                    peer_id = int(peer_value)
                except Exception:
                    peer_id = None
    elif provider == "avito":
        chat_candidate = (
            msg.get("chat_id")
            or body.get("chat_id")
            or msg.get("conversation_id")
            or payload.get("chat_id")
            or payload.get("conversation_id")
        )
        if isinstance(chat_candidate, dict):
            chat_candidate = chat_candidate.get("id")
        if chat_candidate is not None:
            chat_text = str(chat_candidate).strip()
            avito_chat_id = chat_text or None
            if avito_chat_id:
                peer_value = avito_chat_id

        author_info = msg.get("author") or msg.get("sender") or body.get("author") or {}
        if not isinstance(author_info, Mapping):
            author_info = {}
        avito_user_id = _coerce_int(
            author_info.get("id")
            or author_info.get("user_id")
            or msg.get("author_id")
            or body.get("avito_user_id")
        )
        login_candidate = (
            author_info.get("login")
            or author_info.get("username")
            or author_info.get("name")
            or msg.get("author_login")
            or body.get("avito_login")
        )
        if isinstance(login_candidate, str):
            login_candidate = login_candidate.strip()
        avito_login = login_candidate or None
        contact_value = avito_login or (str(avito_user_id) if avito_user_id else None)
    else:
        from_id = msg.get("from") or msg.get("author") or body.get("from") or ""
        whatsapp_phone = _digits(from_id.split("@", 1)[0] if from_id else "")
        contact_value = whatsapp_phone or None

    lead_hint = _coerce_int(body.get("leadId") or body.get("lead_id"))
    ts_fallback = int(time.time() * 1000)
    lead_id_value = lead_hint
    if provider == "telegram":
        if telegram_user_id is not None:
            lead_id_value = telegram_user_id
        elif peer_id is not None:
            lead_id_value = peer_id
    elif provider == "avito":
        account_hint = avito_account_id if avito_account_id is not None else tenant
        if avito_chat_id:
            lead_id_value = avito.stable_lead_id(account_hint, avito_chat_id)
    if lead_id_value in (None, 0):
        lead_id_value = ts_fallback
    lead_id = int(lead_id_value)

    channel = provider or "whatsapp"
    peer_for_log = ""
    if provider == "telegram":
        if peer_value is not None:
            peer_for_log = peer_value
        elif peer_id is not None:
            peer_for_log = str(peer_id)
        elif telegram_user_id is not None:
            peer_for_log = str(telegram_user_id)
    elif whatsapp_phone:
        peer_for_log = whatsapp_phone
    logger.info(
        "webhook_received channel=%s tenant=%s lead_id=%s message_id=%s peer=%s",
        channel,
        tenant,
        lead_id,
        message_id or "",
        peer_for_log or "-",
    )

    if not text and provider != "telegram":
        return _ok({"skipped": True, "reason": "no_text"})

    if provider == "telegram" and await _is_duplicate("telegram", tenant, message_id or None):
        logger.info(
            "stage=incoming_duplicate ch=telegram tenant=%s message_id=%s", tenant, message_id
        )
        return _ok({"skipped": True, "reason": "duplicate"})

    stored_incoming = False
    ts_ms = int(time.time() * 1000)
    from_addr = ""
    to_addr = ""

    if provider == "telegram":
        from_addr = str(telegram_user_id or "")
        if telegram_user_id is not None:
            to_addr = str(telegram_user_id)
        elif peer_id is not None:
            to_addr = str(peer_id)
    elif provider == "avito":
        from_addr = avito_login or (str(avito_user_id) if avito_user_id else "")
        to_addr = ""
    else:
        from_addr = whatsapp_phone
        to_candidate = (
            msg.get("to")
            or body.get("to")
            or (body.get("destination") if isinstance(body.get("destination"), str) else "")
        )
        to_addr = _digits(to_candidate)

    normalized_event: Dict[str, Any] = {
        "event": "messages.incoming",
        "ch": channel,
        "tenant": tenant,
        "lead_id": lead_id,
        "message_id": message_id or str(lead_id),
        "from": from_addr,
        "to": to_addr,
        "text": text,
        "attachments": attachments,
        "ts": ts_ms,
    }
    if telegram_user_id is not None:
        normalized_event["telegram_user_id"] = telegram_user_id
    if telegram_username:
        normalized_event["username"] = telegram_username
    if peer_id is not None:
        normalized_event["peer_id"] = peer_id
    if provider == "telegram":
        if peer_value is None and telegram_user_id is not None:
            peer_value = str(telegram_user_id)
        if peer_value is not None:
            normalized_event["peer"] = peer_value
            lead_contacts = normalized_event.setdefault("lead_contacts", {})
            telegram_contact: dict[str, Any] = {"peer": peer_value}
            if contact_value:
                telegram_contact["contact"] = contact_value
            lead_contacts["telegram"] = telegram_contact
    if provider == "avito":
        if avito_chat_id:
            normalized_event["peer"] = avito_chat_id
            lead_contacts = normalized_event.setdefault("lead_contacts", {})
            avito_contact: dict[str, Any] = {"peer": avito_chat_id}
            if contact_value:
                avito_contact["contact"] = contact_value
            lead_contacts["avito"] = avito_contact
        if avito_account_id is not None:
            normalized_event["account_id"] = avito_account_id
        normalized_event["avito"] = {
            "account_id": avito_account_id,
            "chat_id": avito_chat_id,
            "user_id": avito_user_id,
            "login": avito_login,
        }
    if provider not in {"telegram", "avito"}:
        normalized_event["auto_reply_handled"] = True

    try:
        await _redis_queue.lpush(
            INCOMING_QUEUE_KEY, json.dumps(normalized_event, ensure_ascii=False)
        )
        if channel == "telegram":
            await _redis_queue.incrby("metrics:telegram:incoming", 1)
        elif channel == "whatsapp":
            await _redis_queue.incrby("metrics:whatsapp:incoming", 1)
        elif channel == "avito":
            await _redis_queue.incrby("metrics:avito:incoming", 1)
        logger.info(
            "stage=incoming_enqueued ch=%s tenant=%s message_id=%s", channel, tenant, normalized_event["message_id"]
        )
    except Exception:
        logger.exception(
            "stage=incoming_enqueue_failed ch=%s tenant=%s", channel, tenant
        )

    contact_id = 0
    try:
        upsert_kwargs = {
            "channel": provider or "whatsapp",
            "tenant_id": tenant,
            "telegram_username": telegram_username,
            "peer_id": peer_id,
            "peer": peer_value,
            "contact": contact_value,
        }
        if telegram_user_id is not None:
            upsert_kwargs["telegram_user_id"] = int(telegram_user_id)
        if provider == "avito":
            if avito_chat_id:
                upsert_kwargs["peer"] = avito_chat_id
            if avito_account_id is not None:
                upsert_kwargs["source_real_id"] = avito_account_id
            if avito_login and not upsert_kwargs.get("title"):
                upsert_kwargs["title"] = f"Avito · {avito_login}"
        resolved_lead = await upsert_lead(
            lead_id,
            **upsert_kwargs,
        )
    except Exception as exc:
        logger.exception(
            "lead_upsert_err:db_error tenant=%s lead_id=%s message_in_lead_upsert_fail",
            tenant,
            lead_id,
        )
        raise HTTPException(status_code=500, detail="lead_upsert_failed") from exc

    if resolved_lead:
        try:
            lead_id = int(resolved_lead)
        except Exception:
            pass
        else:
            normalized_event["lead_id"] = lead_id
    logger.info(
        "lead_upsert_ok tenant=%s lead_id=%s resolved=%s",
        tenant,
        lead_id,
        resolved_lead,
    )

    try:
        contact_id = await resolve_or_create_contact(
            whatsapp_phone=whatsapp_phone or None,
            avito_user_id=avito_user_id,
            avito_login=avito_login,
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
        )
        if contact_id:
            await link_lead_contact(
                lead_id,
                contact_id,
                channel=provider,
                peer=peer_value if provider in {"telegram", "avito"} else None,
            )
            if text:
                await insert_message_in(
                    lead_id,
                    text,
                    status="received",
                    tenant_id=tenant,
                    telegram_user_id=telegram_user_id,
                )
                stored_incoming = True
    except Exception:
        pass

    if text and not stored_incoming:
        try:
            await insert_message_in(
                lead_id,
                text,
                status="received",
                tenant_id=tenant,
                telegram_user_id=telegram_user_id,
            )
        except Exception:
            pass

    refer_id = contact_id or lead_id

    cache_key: tuple[int, str] | None = None
    now_ts = time.time()
    if provider == "telegram" and telegram_user_id:
        cache_key = (tenant, f"tg:{telegram_user_id}")
    elif whatsapp_phone:
        cache_key = (tenant, whatsapp_phone)

    catalog_already_sent = False
    if cache_key:
        cached_ts = _catalog_sent_cache.get(cache_key)
        if cached_ts and now_ts - cached_ts < core.STATE_TTL_SECONDS:
            catalog_already_sent = True
        elif cached_ts:
            _catalog_sent_cache.pop(cache_key, None)

    cfg = None
    behavior: dict[str, object] = {}
    attachment, caption = None, ""
    try:
        cfg = core.load_tenant(tenant)
        if isinstance(cfg, dict):
            raw_behavior = cfg.get("behavior")
            if isinstance(raw_behavior, dict):
                behavior = raw_behavior
        attachment, caption = _resolve_catalog_attachment(cfg, tenant, request)
    except Exception:
        cfg = None
        behavior = {}
        attachment, caption = None, ""

    forced_catalog = bool(text and _user_requested_catalog(text))
    should_send_catalog = bool(attachment) and (forced_catalog or not catalog_already_sent)

    if should_send_catalog:
        if forced_catalog and cache_key:
            _catalog_sent_cache.pop(cache_key, None)
        catalog_text = (caption or "Каталог во вложении (PDF).").strip()
        resolved_provider = provider or "whatsapp"
        catalog_out: Dict[str, Any] = {
            "lead_id": lead_id,
            "text": catalog_text,
            "provider": resolved_provider,
            "ch": resolved_provider,
            "tenant_id": int(tenant),
            "tenant": int(tenant),
            "message_id": message_id or str(lead_id),
            "attachments": [attachment] if attachment else [],
        }
        catalog_out["attachment"] = attachment
        if resolved_provider == "telegram":
            if telegram_user_id:
                catalog_out["telegram_user_id"] = int(telegram_user_id)
            if peer_value:
                catalog_out["peer"] = peer_value
            if peer_id is not None:
                catalog_out["peer_id"] = int(peer_id)
            if not catalog_out.get("telegram_user_id") and not catalog_out.get("peer"):
                should_send_catalog = False
        else:
            catalog_out["to"] = whatsapp_phone

        if should_send_catalog:
            await _redis_queue.lpush(OUTBOX_QUEUE_KEY, json.dumps(catalog_out, ensure_ascii=False))
            if cache_key:
                _catalog_sent_cache[cache_key] = time.time()
            try:
                core.record_bot_reply(refer_id, tenant, provider, catalog_text, tenant_cfg=cfg)
            except Exception:
                pass
            return _ok({"queued": True, "leadId": lead_id})

    fallback_reply = (
        "Принял запрос. Скидываю весь каталог. Если нужно PDF — напишите «каталог pdf»."
    )
    reply: str | None = None
    if smart_reply_enabled(tenant):
        try:
            msgs = await build_llm_messages(refer_id, text or "", provider, tenant=tenant)
            reply = await ask_llm(msgs, tenant=tenant, contact_id=refer_id, channel=provider)
        except Exception:
            reply = fallback_reply
    else:
        logger.info(
            "event=smart_reply_disabled tenant=%s channel=%s lead_id=%s",
            tenant,
            provider,
            lead_id,
        )

    reply_text = str(reply or "").strip()
    if not reply_text:
        return _ok({"queued": False, "leadId": lead_id, "smartReply": False})

    if provider == "telegram":
        logger.info(
            "event=smart_reply_deferred tenant=%s channel=%s lead_id=%s",
            tenant,
            provider,
            lead_id,
        )
        return _ok({"queued": False, "leadId": lead_id, "smartReply": True})

    resolved_provider = provider or "whatsapp"
    out: Dict[str, Any] = {
        "lead_id": lead_id,
        "text": reply_text,
        "provider": resolved_provider,
        "ch": resolved_provider,
        "tenant_id": int(tenant),
        "tenant": int(tenant),
        "message_id": message_id or str(lead_id),
        "attachments": [],
    }
    out["to"] = whatsapp_phone

    await _redis_queue.lpush(OUTBOX_QUEUE_KEY, json.dumps(out, ensure_ascii=False))

    behavior = behavior or {}
    always_full = bool(behavior.get("always_full_catalog")) if behavior else False
    send_pages_pref = bool(behavior.get("send_catalog_as_pages")) if behavior else False
    should_send_catalog_pages = (always_full or send_pages_pref) and not catalog_already_sent

    if should_send_catalog_pages:
        try:
            items = core.read_all_catalog(cfg)
            pages = core.paginate_catalog_text(items, cfg, int(os.getenv("CATALOG_PAGE_SIZE", "10")))
        except Exception:
            pages = []
        if pages:
            for page in pages:
                page_text = str(page or "").strip()
                if not page_text:
                    continue
                page_out = {
                    "lead_id": lead_id,
                    "text": page_text,
                    "provider": resolved_provider,
                    "ch": resolved_provider,
                    "tenant_id": int(tenant),
                    "tenant": int(tenant),
                    "message_id": message_id or str(lead_id),
                    "attachments": [],
                    "to": whatsapp_phone,
                }
                await _redis_queue.lpush(OUTBOX_QUEUE_KEY, json.dumps(page_out, ensure_ascii=False))
            if cache_key:
                _catalog_sent_cache[cache_key] = time.time()

    return _ok({"queued": True, "leadId": lead_id})


def _extract_token(request: Request) -> str:
    query_token = (request.query_params.get("token") or "").strip()
    headers = getattr(request, "headers", {}) or {}
    header_token = headers.get("X-Webhook-Token") or ""
    auth_header = headers.get("Authorization") or ""
    if auth_header.lower().startswith("bearer "):
        auth_header = auth_header[7:]
    header_token = (header_token or auth_header).strip()
    return query_token or header_token


def _extract_provider_token(request: Request) -> str:
    query_token = (request.query_params.get("token") or "").strip()
    if query_token:
        return query_token
    headers = getattr(request, "headers", {}) or {}
    header_token = headers.get("X-Provider-Token")
    if header_token:
        return str(header_token).strip()
    auth_header = headers.get("Authorization") or ""
    if isinstance(auth_header, str) and auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return ""


def _sanitize_media_item(blob: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, raw_value in blob.items():
        if isinstance(raw_value, (str, int, float, bool)) or raw_value is None:
            sanitized[str(key)] = raw_value
        else:
            sanitized[str(key)] = str(raw_value)
    return sanitized


def _normalize_whatsapp_incoming(
    payload: dict[str, Any], tenant: int, lead_hint: int | None = None
) -> dict[str, Any]:
    channel_value = str(payload.get("channel") or payload.get("provider") or "whatsapp").strip().lower()
    if channel_value and channel_value not in {"whatsapp", "wa"}:
        raise ValueError("invalid_channel")

    message_id_raw = payload.get("message_id") or payload.get("id")
    message_id = str(message_id_raw).strip() if message_id_raw is not None else ""
    if not message_id:
        raise ValueError("missing_message_id")

    sender_raw = (
        payload.get("from")
        or payload.get("from_id")
        or payload.get("from_jid")
        or payload.get("fromAddress")
        or ""
    )
    sender_str = str(sender_raw).strip()
    if not sender_str:
        raise ValueError("missing_from")
    sender_digits = _digits(sender_str)
    if not sender_digits:
        raise ValueError("invalid_from")
    sender_jid = sender_str.lower()
    if not sender_jid.endswith("@c.us"):
        sender_jid = f"{sender_digits}@c.us"

    text_raw = payload.get("text") or payload.get("body")
    text = str(text_raw).strip() if isinstance(text_raw, str) else ""

    raw_media = payload.get("media") or payload.get("attachments") or []
    media: list[dict[str, Any]] = []
    if isinstance(raw_media, list):
        for item in raw_media:
            if isinstance(item, dict):
                media.append(_sanitize_media_item(item))

    normalized: dict[str, Any] = {
        "event": "messages.incoming",
        "tenant": int(tenant),
        "channel": "whatsapp",
        "provider": "whatsapp",
        "message_id": message_id,
        "from": sender_digits,
        "from_jid": sender_jid,
        "from_raw": sender_str,
    }

    if text:
        normalized["text"] = text
    if media:
        normalized["media"] = media

    ts_value = payload.get("ts") or payload.get("timestamp")
    if ts_value is not None:
        normalized["ts"] = ts_value

    for optional_key in ("to", "wa_id", "conversation_id"):
        if optional_key in payload:
            normalized[optional_key] = payload[optional_key]

    lead_value = lead_hint if isinstance(lead_hint, int) and lead_hint > 0 else None
    if lead_value is None:
        conversation_hint = _coerce_int(payload.get("conversation_id"))
        if conversation_hint and conversation_hint > 0:
            lead_value = conversation_hint
    if lead_value is None and sender_digits:
        try:
            lead_value = int(sender_digits)
        except Exception:
            lead_value = None
    if lead_value is None:
        ts_hint = _coerce_int(ts_value)
        if ts_hint and ts_hint > 0:
            lead_value = ts_hint
    if lead_value is None:
        lead_value = int(time.time() * 1000)

    normalized["lead_id"] = int(lead_value)

    return normalized


async def _queue_incoming_event(event_payload: dict[str, Any]) -> None:
    try:
        serialized = json.dumps(event_payload, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="invalid_payload") from exc

    try:
        await _redis_queue.lpush(INCOMING_QUEUE_KEY, serialized)
        channel_hint = str(
            (
                event_payload.get("channel")
                or event_payload.get("ch")
                or event_payload.get("provider")
                or ""
            )
        ).strip()
        tenant_hint = event_payload.get("tenant") or event_payload.get("tenant_id") or ""
        message_id = event_payload.get("message_id") or ""
        logger.info(
            "incoming_enqueued ch=%s tenant=%s message_id=%s",
            channel_hint or "-",
            tenant_hint,
            message_id or "-",
        )
    except Exception as exc:  # pragma: no cover - Redis connectivity issues
        logger.exception(
            "webhook_provider_queue_failed tenant=%s", event_payload.get("tenant")
        )
        raise HTTPException(status_code=500, detail="queue_error") from exc


async def _cache_whatsapp_qr(
    payload: dict[str, Any], tenant: int, provider: str, event_name: str
) -> dict[str, Any]:
    qr_id_raw = (
        payload.get("qr_id")
        or payload.get("qrId")
        or payload.get("id")
        or payload.get("qr")
    )
    svg_raw = (
        payload.get("svg")
        or payload.get("qr")
        or payload.get("data")
    )
    if svg_raw is None:
        nested_payload = payload.get("payload")
        if isinstance(nested_payload, dict):
            svg_raw = nested_payload.get("svg")

    try:
        qr_id = str(qr_id_raw).strip() if qr_id_raw is not None else ""
    except Exception:
        qr_id = ""
    if not qr_id:
        raise HTTPException(status_code=422, detail="invalid_qr")

    if not isinstance(svg_raw, str):
        svg_value = ""
    else:
        svg_value = svg_raw.strip()
    if not svg_value or not svg_value.lstrip().startswith("<svg"):
        raise HTTPException(status_code=422, detail="invalid_qr")

    ttl = random.randint(WA_QR_CACHE_TTL_MIN, WA_QR_CACHE_TTL_MAX)
    cache_key = f"wa:qr:{tenant}:{qr_id}"
    svg_key = f"{cache_key}:svg"
    last_key = f"wa:qr:last:{tenant}"

    entry = {
        "tenant": int(tenant),
        "qr_id": qr_id,
        "qr_svg": svg_value,
        "provider": provider,
        "event": event_name,
        "updated_at": int(time.time()),
    }

    try:
        serialized_entry = json.dumps(entry, ensure_ascii=False)
    except Exception:
        serialized_entry = None

    try:
        await _redis_queue.set(svg_key, svg_value, ex=ttl)
        await _redis_queue.set(last_key, qr_id, ex=ttl)
        if serialized_entry is not None:
            await _redis_queue.set(cache_key, serialized_entry, ex=ttl)
    except Exception as exc:  # pragma: no cover - Redis failures
        logger.exception("wa_qr_cache_write_failed tenant=%s qr_id=%s", tenant, qr_id)
        raise HTTPException(status_code=500, detail="cache_error") from exc

    logger.info("wa_qr_cached tenant=%s qr_id=%s ttl=%s", tenant, qr_id, ttl)
    return {"qr_id": qr_id}


@router.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    token = _extract_token(request)
    secret = settings.WEBHOOK_SECRET or ""
    if secret and token != secret:
        raise HTTPException(status_code=401, detail="unauthorized")

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid_json")
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_payload")

    if not isinstance(payload, dict):
        payload = {}

    tenant = int(payload.get("tenant_id") or os.getenv("TENANT_ID", "1"))
    raw_peer_value = (
        payload.get("peer")
        or payload.get("peer_id")
        or payload.get("chat_id")
        or payload.get("to_peer")
    )
    if raw_peer_value is not None:
        peer_value = str(raw_peer_value).strip() or None
    else:
        peer_value = None
    body = {
        "source": {"type": "telegram", "tenant": tenant},
        "message": {
            "text": (payload.get("text") or "").strip(),
            "telegram_user_id": payload.get("user_id"),
            "telegram_username": payload.get("username"),
            "media": payload.get("media"),
        },
        "telegram": payload,
    }
    if peer_value is not None:
        body["peer"] = peer_value
        body["message"]["peer"] = peer_value
        body["message"]["peer_id"] = raw_peer_value

    return await process_incoming(body, request)


async def provider_webhook(request: Request) -> JSONResponse:
    channel_label = "whatsapp"
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        WEBHOOK_PROVIDER_COUNTER.labels("invalid_json", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_json")
    except Exception:
        WEBHOOK_PROVIDER_COUNTER.labels("invalid_payload", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_payload")

    if not isinstance(payload, dict):
        WEBHOOK_PROVIDER_COUNTER.labels("invalid_payload", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_payload")

    raw_tenant = (
        payload.get("tenant")
        or request.query_params.get("tenant")
        or request.query_params.get("t")
    )
    tenant_candidate = _coerce_int(raw_tenant)
    if tenant_candidate is None:
        WEBHOOK_PROVIDER_COUNTER.labels("invalid_tenant", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_tenant")
    tenant = int(tenant_candidate)

    provider_value = str(payload.get("provider") or payload.get("channel") or channel_label).strip().lower()
    if provider_value and provider_value not in {"whatsapp", "wa"}:
        WEBHOOK_PROVIDER_COUNTER.labels("ignored", channel_label).inc()
        return JSONResponse({"ok": True, "queued": False, "event": provider_value or "ignored"})

    token = _extract_provider_token(request)
    if not token:
        WEBHOOK_PROVIDER_COUNTER.labels("unauthorized", channel_label).inc()
        raise HTTPException(status_code=401, detail="unauthorized")

    try:
        stored = await provider_tokens_repo.get_by_tenant(tenant)
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("provider_token_get").inc()
        WEBHOOK_PROVIDER_COUNTER.labels("error", channel_label).inc()
        logger.exception(
            "provider_token_lookup_failed channel=%s tenant=%s",
            channel_label,
            tenant,
        )
        raise HTTPException(status_code=500, detail="db_error") from exc

    if not stored or stored.token != token:
        WEBHOOK_PROVIDER_COUNTER.labels("unauthorized", channel_label).inc()
        raise HTTPException(status_code=401, detail="unauthorized")

    raw_event = str(payload.get("event") or "").strip().lower()
    event = "qr" if raw_event == "wa_qr" else raw_event
    if not event:
        WEBHOOK_PROVIDER_COUNTER.labels("invalid_payload", channel_label).inc()
        raise HTTPException(status_code=422, detail="invalid_event")

    if event == "messages.incoming":
        lead_hint = _coerce_int(payload.get("lead_id") or payload.get("leadId"))
        try:
            normalized_event = _normalize_whatsapp_incoming(payload, tenant, lead_hint)
        except ValueError as exc:
            WEBHOOK_PROVIDER_COUNTER.labels("invalid_payload", channel_label).inc()
            raise HTTPException(status_code=422, detail=str(exc) or "invalid_payload") from exc

        text_value = ""
        if "text" in normalized_event and isinstance(normalized_event.get("text"), str):
            text_value = normalized_event["text"].strip()
        media_items = normalized_event.get("media") if isinstance(normalized_event.get("media"), list) else []
        if not text_value and not media_items:
            WEBHOOK_PROVIDER_COUNTER.labels("invalid_payload", channel_label).inc()
            raise HTTPException(status_code=422, detail="empty_message")

        try:
            await insert_webhook_event(
                "whatsapp",
                "messages.incoming",
                lead_hint,
                payload,
            )
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("webhook_event_insert").inc()
            WEBHOOK_PROVIDER_COUNTER.labels("error", channel_label).inc()
            logger.exception(
                "webhook_event_store_failed channel=%s tenant=%s",
                channel_label,
                tenant,
            )
            raise HTTPException(status_code=500, detail="db_error") from exc

        try:
            await _queue_incoming_event(normalized_event)
        except HTTPException as exc:
            status_label = "invalid_payload" if exc.status_code < 500 else "queue_error"
            WEBHOOK_PROVIDER_COUNTER.labels(status_label, channel_label).inc()
            raise

        WEBHOOK_PROVIDER_COUNTER.labels("ok", channel_label).inc()
        sender_for_log = normalized_event.get("from_jid") or normalized_event.get("from") or "-"
        message_id = normalized_event.get("message_id") or "-"
        logger.info(
            "event=webhook_received channel=%s tenant=%s from=%s msg=%s",
            channel_label,
            tenant,
            sender_for_log,
            message_id,
        )
        return JSONResponse({"ok": True, "queued": True})

    if event == "ready":
        ready_event = {
            "event": "ready",
            "tenant": tenant,
            "channel": channel_label,
            "provider": channel_label,
        }
        state_value = str(payload.get("state") or payload.get("status") or "ready")
        ready_event["state"] = state_value
        ts_value = payload.get("ts") or payload.get("timestamp")
        if ts_value is not None:
            ready_event["ts"] = ts_value
        try:
            await _queue_incoming_event(ready_event)
        except HTTPException as exc:
            status_label = "invalid_payload" if exc.status_code < 500 else "queue_error"
            WEBHOOK_PROVIDER_COUNTER.labels(status_label, channel_label).inc()
            raise
        WEBHOOK_PROVIDER_COUNTER.labels("ok", channel_label).inc()
        logger.info(
            "event=webhook_received channel=%s tenant=%s state=%s",
            channel_label,
            tenant,
            state_value,
        )
        return JSONResponse({"ok": True, "queued": True, "event": "ready"})

    if event == "qr":
        try:
            qr_meta = await _cache_whatsapp_qr(payload, tenant, channel_label, "qr")
        except HTTPException as exc:
            status_label = "invalid_payload" if exc.status_code < 500 else "error"
            WEBHOOK_PROVIDER_COUNTER.labels(status_label, channel_label).inc()
            raise
        WEBHOOK_PROVIDER_COUNTER.labels("ok", channel_label).inc()
        response_payload: dict[str, Any] = {"ok": True, "queued": False, "event": "qr"}
        response_payload.update(qr_meta)
        return JSONResponse(response_payload)

    WEBHOOK_PROVIDER_COUNTER.labels("ignored", channel_label).inc()
    return JSONResponse({"ok": True, "queued": False, "event": event})


@router.post("/webhook")
async def webhook_entry(request: Request) -> JSONResponse:
    return await provider_webhook(request)


@router.post("/webhook/provider")
async def webhook_provider_compat(request: Request) -> JSONResponse:
    logger.warning("deprecated_webhook_provider_path")
    return await provider_webhook(request)


__all__ = ["router", "process_incoming", "provider_webhook"]


async def _is_duplicate(provider: str, tenant: int, message_id: str | None) -> bool:
    if not message_id:
        return False
    key = f"incoming:{provider}:{tenant}:{message_id}"
    try:
        created = await _redis_queue.setnx(key, int(time.time()))
        if not created:
            return True
        await _redis_queue.expire(key, INCOMING_DEDUP_TTL)
    except Exception:
        logger.exception("stage=dedup provider=%s tenant=%s", provider, tenant)
    return False
