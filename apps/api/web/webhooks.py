from __future__ import annotations

import json
import os
import time
import pathlib
import logging
import random
import re
from typing import Any, Dict, Tuple, Mapping, Optional, Iterable
from urllib.parse import urlsplit, urlunsplit

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from libs.core import sales_core as core  # type: ignore

from libs.core.db import (
    resolve_or_create_contact,
    link_lead_contact,
    insert_message_in,
    insert_message_out,
    upsert_lead,
    insert_webhook_event,
)

from . import common as C  # type: ignore

from libs.core.integrations import avito

from .ui import templates  # noqa: F401 - ensure templates loaded for compatibility
from libs.core.common import (
    OUTBOX_QUEUE_KEY,
    HANDOFF_SILENCE_TTL_SECONDS,
    default_fallback_reply,
    handoff_silence_key,
    is_manager_telegram,
    is_manager_whatsapp,
    smart_reply_enabled,
)
from libs.core.metrics import DB_ERRORS_COUNTER, WEBHOOK_PROVIDER_COUNTER
from libs.core.repo import provider_tokens as provider_tokens_repo


def _json_safe(value: Any) -> Any:
    """Make payload JSON-safe by stringifying unknown types."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_safe(v) for v in value]
    try:
        return str(value)
    except Exception:
        return repr(value)


logger = logging.getLogger("app.web.webhooks")

INCOMING_QUEUE_KEY = (
    os.getenv("INBOX_QUEUE")
    or os.getenv("INBOX_QUEUE_KEY")
    or os.getenv("INCOMING_QUEUE_KEY")
    or "inbox:message_in"
)
INCOMING_DEDUP_TTL = 60 * 60 * 24  # 24 hours

router = APIRouter()


settings = core.settings  # type: ignore[attr-defined]


_redis_queue = settings.r
_catalog_sent_cache: dict[Tuple[int, str], float] = {}
_WA_JID_CACHE_TTL = 60 * 60 * 24 * 30  # 30 days


def _catalog_cache_redis_key(cache_key: tuple[int, str]) -> str:
    tenant, identifier = cache_key
    return f"catalog:sent:{tenant}:{identifier}"


async def _catalog_was_recently_sent(cache_key: tuple[int, str] | None) -> bool:
    if not cache_key:
        return False
    ttl = getattr(core, "STATE_TTL_SECONDS", 600)
    now_ts = time.time()
    cached_ts = _catalog_sent_cache.get(cache_key)
    if cached_ts and now_ts - cached_ts < ttl:
        return True
    if cached_ts:
        _catalog_sent_cache.pop(cache_key, None)
    redis_key = _catalog_cache_redis_key(cache_key)
    try:
        stored = await _redis_queue.get(redis_key)
    except Exception:
        stored = None
    if stored:
        _catalog_sent_cache[cache_key] = now_ts
        return True
    return False


async def _mark_catalog_sent(cache_key: tuple[int, str] | None) -> None:
    if not cache_key:
        return
    ttl = getattr(core, "STATE_TTL_SECONDS", 600)
    _catalog_sent_cache[cache_key] = time.time()
    redis_key = _catalog_cache_redis_key(cache_key)
    try:
        await _redis_queue.set(redis_key, str(int(time.time())), ex=ttl)
    except Exception:
        logger.debug("catalog_sent_cache_set_failed key=%s", redis_key, exc_info=True)


async def _reset_catalog_cache(cache_key: tuple[int, str] | None) -> None:
    if not cache_key:
        return
    _catalog_sent_cache.pop(cache_key, None)
    redis_key = _catalog_cache_redis_key(cache_key)
    try:
        await _redis_queue.delete(redis_key)
    except Exception:
        logger.debug("catalog_sent_cache_delete_failed key=%s", redis_key, exc_info=True)

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

try:
    _catalog_inline_limit_mb = float(os.getenv("WA_CATALOG_INLINE_LIMIT_MB", "5") or "0")
except ValueError:
    _catalog_inline_limit_mb = 5.0
if _catalog_inline_limit_mb < 0:
    _catalog_inline_limit_mb = 0.0
CATALOG_INLINE_LIMIT_BYTES = (
    int(_catalog_inline_limit_mb * 1024 * 1024) if _catalog_inline_limit_mb > 0 else 0
)


def _user_requested_catalog(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(token in lowered for token in _CATALOG_KEYWORDS)


def _digits(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())


def _has_photo_attachment(blobs: Iterable[Mapping[str, Any]] | None) -> bool:
    """
    Lightweight detector for photo/image attachments across providers.

    Checks type/kind/mime and Telegram-specific markers (MessageMediaPhoto/Photo).
    """
    if not blobs:
        return False
    for blob in blobs:
        if not isinstance(blob, Mapping):
            continue
        # Raw markers without requiring URL presence
        marker = str(blob.get("_") or "").strip().lower()
        raw_type = str(blob.get("type") or blob.get("kind") or "").strip().lower()
        raw_mime = str(
            blob.get("mime") or blob.get("mime_type") or blob.get("mimetype") or ""
        ).strip().lower()
        if marker and ("photo" in marker or "image" in marker):
            return True
        if raw_type in {"photo", "image", "picture"}:
            return True
        if raw_mime.startswith("image/"):
            return True
        type_raw = str(blob.get("type") or blob.get("kind") or "").strip().lower()
        mime_raw = str(
            blob.get("mime")
            or blob.get("mime_type")
            or blob.get("mimetype")
            or ""
        ).strip().lower()
        marker = str(blob.get("_") or "").strip().lower()
        nested_media = blob.get("media") if isinstance(blob.get("media"), Mapping) else None
        nested_photo_raw = blob.get("photo")
        nested_photo = nested_photo_raw if isinstance(nested_photo_raw, Mapping) else None
        if type_raw in {"photo", "image", "picture"}:
            return True
        if mime_raw.startswith("image/"):
            return True
        if marker and ("photo" in marker or "image" in marker):
            return True
        if nested_media and _has_photo_attachment([nested_media]):
            return True
        if nested_photo and _has_photo_attachment([nested_photo]):
            return True
        if isinstance(nested_photo_raw, list) and nested_photo_raw:
            if _has_photo_attachment([item for item in nested_photo_raw if isinstance(item, Mapping)]):
                return True
    return False


def _as_mapping(candidate: Any) -> Mapping[str, Any] | None:
    if isinstance(candidate, Mapping):
        return candidate
    if isinstance(candidate, (bytes, bytearray)):
        try:
            parsed = json.loads(candidate.decode("utf-8"))
        except Exception:
            return None
        return parsed if isinstance(parsed, Mapping) else None
    if isinstance(candidate, str):
        try:
            parsed = json.loads(candidate)
        except Exception:
            return None
        return parsed if isinstance(parsed, Mapping) else None
    return None


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
    try:
        resolved_meta = core.resolve_catalog_pdf_meta(int(tenant), cfg)
    except Exception:
        resolved_meta = None
    if not resolved_meta:
        return None, ""

    relative_path = resolved_meta.get("relative_path") or ""
    absolute_path = resolved_meta.get("absolute_path") or ""
    filename = resolved_meta.get("filename") or pathlib.Path(relative_path or "catalog.pdf").name
    mime = resolved_meta.get("mime") or "application/pdf"

    target = pathlib.Path(absolute_path or "")
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

    url = f"{base}?path={quote(str(relative_path), safe='/')}"
    token = getattr(C, "WA_INTERNAL_TOKEN", "") or ""
    if token:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}token={quote(token)}"

    caption = f"Каталог в PDF: {filename}"

    attachment = {
        "type": "document",
        "url": url,
        "path": str(target),
        "filename": filename,
        "mime_type": mime,
    }
    attachment["mime"] = mime
    attachment["mimetype"] = mime
    attachment["sendMediaAsDocument"] = True
    if caption:
        attachment["caption"] = caption
    return attachment, caption


async def _remember_whatsapp_jid(tenant: int, lead_id: int, jid: str) -> None:
    if tenant <= 0 or lead_id <= 0:
        return
    cleaned = str(jid or "").strip()
    if not cleaned:
        return
    cache_key = f"wa:jid:{tenant}"
    try:
        await _redis_queue.hset(cache_key, str(lead_id), cleaned)
        await _redis_queue.expire(cache_key, _WA_JID_CACHE_TTL)
        logger.info(
            "wa_jid_cache_update tenant=%s lead_id=%s jid=%s",
            tenant,
            lead_id,
            cleaned,
        )
    except Exception:
        logger.warning(
            "wa_jid_cache_failed tenant=%s lead_id=%s", tenant, lead_id, exc_info=True
        )


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
    manager_flag = False
    out_flag = False
    # Respect top-level manager/out flags coming from transports (e.g., tgworker).
    for candidate in (body.get("manager"), body.get("out")):
        if isinstance(candidate, str):
            candidate = candidate.strip().lower() in {"1", "true", "yes", "on"}
        if candidate:
            manager_flag = True
            if body.get("out") is not None:
                out_flag = bool(candidate)
            break
    # Diagnostics to track manager flag inputs for Telegram.
    if provider == "telegram":
        logger.info(
            "manager_diag provider=telegram tenant=%s body_keys=%s msg_keys=%s manager=%s out=%s origin=%s",
            tenant,
            ",".join(sorted(body.keys())),
            ",".join(sorted(msg.keys())) if isinstance(msg, Mapping) else "",
            body.get("manager") or msg.get("manager"),
            body.get("out") or msg.get("out"),
            body.get("origin") or msg.get("origin"),
        )
    for candidate in (
        body.get("manager"),
        msg.get("manager"),
        src.get("manager"),
    ):
        if isinstance(candidate, str):
            candidate = candidate.strip().lower() in {"1", "true", "yes", "on"}
        if candidate:
            manager_flag = True
            break
    origin_raw = body.get("origin") or msg.get("origin") or src.get("origin")
    if isinstance(origin_raw, str) and origin_raw.startswith("telegram:manager"):
        manager_flag = True
    provider_raw = _as_mapping(msg.get("provider_raw")) or _as_mapping(body.get("provider_raw")) or {}
    if isinstance(provider_raw, Mapping):
        key_obj = provider_raw.get("key") if isinstance(provider_raw.get("key"), Mapping) else {}
        if key_obj.get("fromMe"):
            manager_flag = True
        if provider_raw.get("fromMe"):
            manager_flag = True
        if provider_raw.get("out") or provider_raw.get("outgoing"):
            manager_flag = True
            out_flag = True
    message_obj = msg.get("message") if isinstance(msg.get("message"), Mapping) else {}
    if isinstance(message_obj, Mapping):
        if message_obj.get("out") or message_obj.get("outgoing"):
            manager_flag = True
            out_flag = True
        if message_obj.get("fromMe"):
            manager_flag = True
        meta_obj = message_obj.get("meta") if isinstance(message_obj.get("meta"), Mapping) else {}
        if isinstance(meta_obj, Mapping):
            if meta_obj.get("manager"):
                manager_flag = True
    if not manager_flag:
        for candidate in (
            body.get("out"),
            msg.get("out"),
            src.get("out"),
            src.get("outgoing"),
        ):
            if isinstance(candidate, str):
                candidate = candidate.strip().lower() in {"1", "true", "yes", "on"}
            if candidate:
                manager_flag = True
                break
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
    lead_id_value: int | None = None
    raw_attachments = msg.get("attachments") or body.get("attachments")
    if isinstance(raw_attachments, list):
        attachments = [item for item in raw_attachments if isinstance(item, dict)]

    # Collect possible media blobs across providers
    extra_media: list[Mapping[str, Any]] = []
    for candidate in (
        msg.get("media"),
        body.get("media"),
        (msg.get("message") or {}).get("media") if isinstance(msg.get("message"), Mapping) else None,
        (msg.get("provider_raw") or {}).get("media") if isinstance(msg.get("provider_raw"), Mapping) else None,
        (body.get("provider_raw") or {}).get("media") if isinstance(body.get("provider_raw"), Mapping) else None,
    ):
        if isinstance(candidate, list):
            extra_media.extend(item for item in candidate if isinstance(item, Mapping))
        elif isinstance(candidate, Mapping):
            extra_media.append(candidate)
    for candidate in (
        msg.get("photo"),
        body.get("photo"),
        (msg.get("message") or {}).get("photo") if isinstance(msg.get("message"), Mapping) else None,
        (msg.get("provider_raw") or {}).get("photo") if isinstance(msg.get("provider_raw"), Mapping) else None,
        (body.get("provider_raw") or {}).get("photo") if isinstance(body.get("provider_raw"), Mapping) else None,
    ):
        if isinstance(candidate, list):
            extra_media.extend(item for item in candidate if isinstance(item, Mapping))
        elif isinstance(candidate, Mapping):
            extra_media.append(candidate)
    if extra_media:
        attachments.extend([dict(item) for item in extra_media if isinstance(item, Mapping)])

    if provider == "telegram":
        # Force treat media/photo from provider_raw as attachment to avoid loss in downstream logic.
        if not attachments and isinstance(provider_raw, Mapping):
            media_obj = _as_mapping(provider_raw.get("media"))
            photo_obj = _as_mapping(provider_raw.get("photo"))
            forced = []
            for obj in (media_obj, photo_obj):
                if not obj:
                    continue
                kind = str(obj.get("_") or obj.get("type") or "photo")
                photo_id = obj.get("id") or (obj.get("photo") or {}).get("id") if isinstance(obj, Mapping) else None
                attachment = {"type": kind}
                if photo_id:
                    attachment["photo_id"] = photo_id
                    attachment["url"] = f"telegram://{tenant}/{photo_id}"
                forced.append(attachment)
            if forced:
                attachments = forced
                msg["attachments"] = attachments
                body["attachments"] = attachments
    has_photo = _has_photo_attachment(attachments)
    logger.debug(
        "webhook_photo_probe provider=%s tenant=%s lead_hint=%s has_photo_initial=%s attachments_len=%s provider_raw_keys=%s",
        provider,
        tenant,
        lead_id_value,
        int(has_photo),
        len(attachments),
        list(provider_raw.keys()) if isinstance(provider_raw, Mapping) else None,
    )
    if not has_photo and isinstance(provider_raw, Mapping):
        media_obj = _as_mapping(provider_raw.get("media"))
        photo_obj = _as_mapping(provider_raw.get("photo"))
        if media_obj or photo_obj:
            if _has_photo_attachment([obj for obj in (media_obj, photo_obj) if obj]):
                has_photo = True
        if not has_photo:
            try:
                raw_dump = json.dumps(provider_raw, ensure_ascii=False)
            except Exception:
                raw_dump = str(provider_raw)
            lowered_dump = raw_dump.lower()
            if "messagemediaphoto" in lowered_dump or '"photo"' in lowered_dump:
                has_photo = True
    if provider == "telegram" and not has_photo:
        telegram_media_candidates = [
            _as_mapping(msg.get("media")),
            _as_mapping(body.get("media")),
            _as_mapping((msg.get("message") or {}).get("media")) if isinstance(msg.get("message"), Mapping) else None,
            _as_mapping(msg.get("photo")),
            _as_mapping(body.get("photo")),
            _as_mapping((msg.get("message") or {}).get("photo")) if isinstance(msg.get("message"), Mapping) else None,
        ]
        if any(candidate for candidate in telegram_media_candidates if candidate is not None):
            has_photo = True
    if not has_photo:
        provider_raw_alt = _as_mapping(msg.get("provider_raw")) or _as_mapping(body.get("provider_raw"))
        if isinstance(provider_raw_alt, Mapping):
            marker = str(provider_raw_alt.get("_") or "").lower()
            if marker and ("photo" in marker or "image" in marker):
                has_photo = True
            elif any(key.lower().startswith("photo") for key in provider_raw_alt.keys() if isinstance(key, str)):
                has_photo = True

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
        if is_manager_telegram(telegram_user_id):
            manager_flag = True
        # If Telegram did not send sender_id but we have a peer_id, use it for manager detection.
        if telegram_user_id is None and peer_id is not None:
            telegram_user_id = peer_id
            if is_manager_telegram(telegram_user_id):
                manager_flag = True
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
        if is_manager_whatsapp(whatsapp_phone):
            manager_flag = True

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
    resolved_provider = channel
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

    if manager_flag:
        try:
            await _redis_queue.set(
                handoff_silence_key(int(tenant), int(lead_id)),
                str(int(time.time())),
                ex=HANDOFF_SILENCE_TTL_SECONDS,
            )
        except Exception:
            logger.debug("handoff_flag_set_failed tenant=%s lead_id=%s", tenant, lead_id, exc_info=True)
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
            if resolved_lead:
                try:
                    lead_id = int(resolved_lead)
                except Exception:
                    pass
        except Exception:
            logger.exception("lead_upsert_err:db_error tenant=%s lead_id=%s manager_message_upsert_fail", tenant, lead_id)

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
                    channel=provider or "whatsapp",
                    peer=peer_value or "",
                )
        except Exception:
            logger.debug("manager_contact_link_failed tenant=%s lead_id=%s", tenant, lead_id, exc_info=True)

        if text:
            try:
                await insert_message_out(
                    lead_id,
                    text,
                    provider_msg_id=message_id,
                    status="sent",
                    tenant_id=tenant,
                    channel=provider,
                    telegram_user_id=telegram_user_id,
                    telegram_username=telegram_username,
                    title=contact_value,
                    is_bot=False,
                )
            except Exception:
                logger.exception("manager_message_store_failed tenant=%s lead_id=%s", tenant, lead_id)

        return _ok({"queued": False, "smartReply": False, "handoff": True})

    if not text and not has_photo and provider != "telegram":
        return _ok({"skipped": True, "reason": "no_text"})

    if provider == "telegram" and await _is_duplicate("telegram", tenant, message_id or None):
        logger.info(
            "stage=incoming_duplicate ch=telegram tenant=%s message_id=%s", tenant, message_id
        )
        return _ok({"skipped": True, "reason": "duplicate"})
    logger.info(
        "stage=pre_reply_checks ch=%s tenant=%s lead_id=%s msg=%s has_photo=%s attachments=%s text_len=%s",
        channel,
        tenant,
        lead_id,
        message_id or "",
        int(bool(has_photo)),
        len(attachments),
        len(text or ""),
    )

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
    # Preserve manager/out flags for downstream workers.
    if manager_flag:
        normalized_event["manager"] = True
    if out_flag:
        normalized_event["out"] = True
    if provider_raw:
        normalized_event["provider_raw"] = provider_raw
    if isinstance(msg, Mapping):
        normalized_event["message"] = msg
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
    auto_reply_handled = False
    event_enqueued = False

    async def _enqueue_incoming_event() -> None:
        nonlocal event_enqueued
        if event_enqueued:
            return
        payload = dict(normalized_event)
        if auto_reply_handled:
            payload["auto_reply_handled"] = True
        else:
            payload.pop("auto_reply_handled", None)
        try:
            payload = _json_safe(payload)
            logger.info(
                "stage=incoming_enqueue_attempt ch=%s tenant=%s message_id=%s attachments=%s",
                channel,
                tenant,
                payload.get("message_id") or "",
                len(payload.get("attachments") or []),
            )
            await _redis_queue.lpush(
                INCOMING_QUEUE_KEY, json.dumps(payload, ensure_ascii=False)
            )
            if channel == "telegram":
                await _redis_queue.incrby("metrics:telegram:incoming", 1)
            elif channel == "whatsapp":
                await _redis_queue.incrby("metrics:whatsapp:incoming", 1)
            elif channel == "avito":
                await _redis_queue.incrby("metrics:avito:incoming", 1)
            logger.info(
                "stage=incoming_enqueued ch=%s tenant=%s message_id=%s",
                channel,
                tenant,
                payload.get("message_id") or "",
            )
            event_enqueued = True
        except Exception:
            logger.exception(
                "stage=incoming_enqueue_failed ch=%s tenant=%s", channel, tenant
            )

    sender_jid_value = normalized_event.get("from_jid")

    async def _queue_text_reply(
        text: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
    ) -> bool:
        cleaned = str(text or "").strip()
        if not cleaned:
            return False
        out: Dict[str, Any] = {
            "lead_id": lead_id,
            "text": cleaned,
            "provider": resolved_provider,
            "ch": resolved_provider,
            "tenant_id": int(tenant),
            "tenant": int(tenant),
            "message_id": message_id or str(lead_id),
            "attachments": attachments or [],
        }
        if resolved_provider == "telegram":
            if telegram_user_id:
                out["telegram_user_id"] = int(telegram_user_id)
            if peer_value:
                out["peer"] = peer_value
            if peer_id is not None:
                out["peer_id"] = int(peer_id)
            if not out.get("telegram_user_id") and not out.get("peer"):
                return False
        else:
            if not whatsapp_phone:
                return False
            out["to"] = whatsapp_phone
            _assign_whatsapp_to_jid(out, resolved_provider, sender_jid_value)
        await _redis_queue.lpush(OUTBOX_QUEUE_KEY, json.dumps(out, ensure_ascii=False))
        try:
            core.record_bot_reply(refer_id, tenant, provider, cleaned, tenant_cfg=cfg)
        except Exception:
            pass
        return True

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

    if (
        provider == "whatsapp"
        and normalized_event.get("from_jid")
        and tenant
        and lead_id
    ):
        await _remember_whatsapp_jid(
            int(tenant),
            int(lead_id),
            str(normalized_event["from_jid"]),
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
        logger.info(
            "stage=contact_resolved tenant=%s lead_id=%s contact_id=%s has_photo=%s text_len=%s attachments=%s",
            tenant,
            lead_id,
            contact_id,
            int(bool(has_photo)),
            len(text or ""),
            len(attachments),
        )
    except Exception:
        logger.exception("contact_upsert_err tenant=%s lead_id=%s", tenant, lead_id)

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

    if has_photo:
        try:
            await _redis_queue.set(
                handoff_silence_key(int(tenant), int(lead_id)),
                str(int(time.time())),
                ex=HANDOFF_SILENCE_TTL_SECONDS,
            )
        except Exception:
            logger.debug("handoff_flag_set_failed tenant=%s lead_id=%s", tenant, lead_id, exc_info=True)
        try:
            logger.info(
                "event=handoff_enqueue_has_photo ch=%s tenant=%s lead_id=%s attachments=%s keys=%s",
                channel,
                tenant,
                lead_id,
                len(attachments),
                list(normalized_event.keys()),
            )
            payload = dict(normalized_event)
            payload["handoff"] = True
            serialized = json.dumps(_json_safe(payload), ensure_ascii=False)
            await _redis_queue.lpush(INCOMING_QUEUE_KEY, serialized)
            if channel == "telegram":
                await _redis_queue.incrby("metrics:telegram:incoming", 1)
            logger.info(
                "stage=incoming_enqueued_photo ch=%s tenant=%s message_id=%s",
                channel,
                tenant,
                payload.get("message_id") or "",
            )
        except Exception:
            logger.exception(
                "stage=incoming_enqueue_photo_failed ch=%s tenant=%s", channel, tenant
            )
            raise HTTPException(status_code=500, detail="queue_error")
        return _ok({"queued": True, "leadId": lead_id, "smartReply": False, "handoff": True, "reason": "photo_received"})

    refer_id = contact_id or lead_id

    cache_key: tuple[int, str] | None = None
    now_ts = time.time()
    if provider == "telegram":
        if telegram_user_id:
            cache_key = (tenant, f"tg:{telegram_user_id}")
        elif peer_value:
            cache_key = (tenant, f"tg:peer:{peer_value}")
        elif telegram_username:
            cache_key = (tenant, f"tg:user:{telegram_username.lower()}")
    elif whatsapp_phone:
        cache_key = (tenant, whatsapp_phone)
    if cache_key is None and lead_id:
        cache_key = (tenant, f"lead:{lead_id}")

    catalog_already_sent = await _catalog_was_recently_sent(cache_key)

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

    attachment_path: pathlib.Path | None = None
    attachment_size = 0
    attachment_mtime = 0
    if isinstance(attachment, Mapping):
        path_value = attachment.get("path")
        if isinstance(path_value, str) and path_value.strip():
            try:
                candidate = pathlib.Path(path_value)
                stat = candidate.stat()
                attachment_path = candidate
                attachment_size = stat.st_size
                attachment_mtime = int(stat.st_mtime)
            except Exception:
                attachment_path = None
                attachment_size = 0
                attachment_mtime = 0
    file_url = _build_public_catalog_url(tenant, attachment_mtime, request)
    if resolved_provider == "telegram":
        file_url = ""
    use_file_link = False
    if resolved_provider in {"whatsapp"} and file_url:
        use_file_link = True
    elif file_url and attachment_size and CATALOG_INLINE_LIMIT_BYTES and attachment_size > CATALOG_INLINE_LIMIT_BYTES:
        use_file_link = True

    lowered_text = text.lower() if isinstance(text, str) else ""
    effective_cache_state = int(catalog_already_sent)
    forced_catalog = bool(text and _user_requested_catalog(text))
    price_question = any(
        token in lowered_text
        for token in (
            "сколько стоит",
            "цена",
            "стоимость",
            "ценник",
            "почем",
            "почём",
            "прайс на",
        )
    )
    has_attachment = bool(attachment)
    has_file_link = bool(file_url)
    should_send_catalog = (has_attachment or has_file_link) and (forced_catalog or not catalog_already_sent)
    logger.warning(
        "catalog_flow tenant=%s text=%r has_attachment=%s has_link=%s forced=%s already_sent=%s cache_key=%s",
        tenant,
        text,
        int(has_attachment),
        int(has_file_link),
        int(forced_catalog),
        effective_cache_state,
        cache_key,
    )
    if price_question and not forced_catalog:
        should_send_catalog = False
    logger.warning(
        "catalog_flow tenant=%s text=%r forced=%s already_sent=%s attachment=%s cache_hit=%s",
        tenant,
        text,
        int(forced_catalog),
        int(catalog_already_sent),
        isinstance(attachment, dict),
        bool(cache_key and cache_key in _catalog_sent_cache),
    )

    catalog_sent_now = False
    if should_send_catalog and (provider or "").lower() != "avito":
        if forced_catalog and cache_key:
            await _reset_catalog_cache(cache_key)
        catalog_text_override = None
        if use_file_link:
            catalog_text_override = f"Каталог: {file_url}"
            attachment = None
            caption = ""
            logger.info(
                "catalog_file_link tenant=%s size_bytes=%s url=%s",
                tenant,
                attachment_size,
                file_url,
            )
        catalog_text = (catalog_text_override or caption or "Каталог во вложении (PDF).").strip()
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
        if attachment:
            catalog_out["attachment"] = attachment
        if resolved_provider == "telegram":
            send_catalog_first = True
            raw_send_catalog_flag = behavior.get("send_catalog_on_first_message") if behavior else None
            if raw_send_catalog_flag is not None:
                try:
                    send_catalog_first = bool(raw_send_catalog_flag)
                except Exception:
                    send_catalog_first = True
            if not send_catalog_first:
                should_send_catalog = False
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
            _assign_whatsapp_to_jid(catalog_out, resolved_provider, sender_jid_value)

        if should_send_catalog:
            await _redis_queue.lpush(OUTBOX_QUEUE_KEY, json.dumps(catalog_out, ensure_ascii=False))
            await _mark_catalog_sent(cache_key)
            try:
                core.record_bot_reply(refer_id, tenant, provider, catalog_text, tenant_cfg=cfg)
            except Exception:
                pass
            catalog_sent_now = True

    if catalog_sent_now and not text:
        await _enqueue_incoming_event()
        return _ok({"queued": True, "leadId": lead_id})

    if price_question and not catalog_sent_now:
        # Do not send price replies for Avito to avoid overriding auto-replies/templates.
        if resolved_provider == "avito":
            await _enqueue_incoming_event()
            return _ok({"queued": True, "leadId": lead_id})

        def _tokenize(value: str) -> tuple[set[str], set[str]]:
            tokens = re.findall(r"[a-zA-Zа-яА-Я0-9]+", value.lower())
            letters = {tok for tok in tokens if len(tok) >= 3 and not tok.isdigit()}
            numbers = {tok for tok in tokens if tok.isdigit()}
            return letters, numbers

        def _has_relevance(query_text: str, item_payload: Mapping[str, Any]) -> bool:
            letters_q, numbers_q = _tokenize(query_text)
            price_words = {"цена", "стоимость", "прайс", "сколько", "почем", "почём"}
            letters_q = {t for t in letters_q if t not in price_words}
            if not letters_q and not numbers_q:
                return False

            fields = [
                item_payload.get("title"),
                item_payload.get("name"),
                item_payload.get("sku"),
                item_payload.get("id"),
                item_payload.get("category"),
                item_payload.get("color"),
                item_payload.get("material"),
                item_payload.get("size"),
            ]
            item_text = " ".join([str(f) for f in fields if f])
            letters_i, numbers_i = _tokenize(item_text)
            return bool(letters_q & letters_i or numbers_q & numbers_i)

        def _format_price(raw: Any) -> str | None:
            raw_text = str(raw or "").strip()
            if not raw_text:
                return None
            match = re.search(r"\d[\d\s.,]*", raw_text)
            digits = re.sub(r"\D", "", match.group(0)) if match else ""
            if not digits:
                return None
            if len(digits) > 9:
                return None
            try:
                value = int(digits)
            except Exception:
                return None
            if value <= 0:
                return None
            return f"{value:,}".replace(",", " ")

        if smart_reply_enabled(tenant):
            try:
                catalog_matches = core.search_catalog({}, limit=5, tenant=tenant, query=text or "")
            except Exception:
                catalog_matches = []
            if catalog_matches:
                best = catalog_matches[0]
                if _has_relevance(text or "", best):
                    formatted_price = _format_price(best.get("price"))
                    if formatted_price:
                        title_hint = str(best.get("title") or best.get("name") or "").strip()
                        reply_price = title_hint or "Эта модель"
                        reply_text = f"{reply_price} стоит {formatted_price} ₽."
                        stock_value = best.get("stock")
                        if stock_value not in (None, "", "0"):
                            try:
                                stock_int = int(str(stock_value).strip())
                            except Exception:
                                stock_int = None
                            if stock_int is not None and stock_int > 0:
                                reply_text += " В наличии."
                        price_out: Dict[str, Any] = {
                            "lead_id": lead_id,
                            "text": reply_text,
                            "provider": resolved_provider,
                            "ch": resolved_provider,
                            "tenant_id": int(tenant),
                            "tenant": int(tenant),
                            "message_id": message_id or str(lead_id),
                            "attachments": [],
                        }
                        if resolved_provider == "telegram":
                            if telegram_user_id:
                                price_out["telegram_user_id"] = int(telegram_user_id)
                            if peer_value:
                                price_out["peer"] = peer_value
                            if peer_id is not None:
                                price_out["peer_id"] = int(peer_id)
                        else:
                            chat_target = avito_chat_id or peer_value or (str(peer_id) if peer_id is not None else "")
                            if chat_target:
                                price_out["peer"] = chat_target
                                price_out["peer_id"] = chat_target
                                price_out["chat_id"] = chat_target
                            if avito_account_id is not None:
                                price_out["account_id"] = avito_account_id
                            if whatsapp_phone:
                                price_out["to"] = whatsapp_phone
                            _assign_whatsapp_to_jid(price_out, resolved_provider, sender_jid_value)
                        await _redis_queue.lpush(OUTBOX_QUEUE_KEY, json.dumps(price_out, ensure_ascii=False))
                        try:
                            core.record_bot_reply(refer_id, tenant, provider, reply_text, tenant_cfg=cfg)
                        except Exception:
                            pass
                        auto_reply_handled = True
                        await _enqueue_incoming_event()
                        return _ok({"queued": True, "leadId": lead_id})

    behavior = behavior or {}
    always_full = bool(behavior.get("always_full_catalog")) if behavior else False
    send_pages_pref = bool(behavior.get("send_catalog_as_pages")) if behavior else False
    should_send_catalog_pages = False  # отключено для всех каналов

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
                }
                if resolved_provider == "telegram":
                    if telegram_user_id:
                        page_out["telegram_user_id"] = int(telegram_user_id)
                    if peer_value:
                        page_out["peer"] = peer_value
                    if peer_id is not None:
                        page_out["peer_id"] = int(peer_id)
                    if not page_out.get("telegram_user_id") and not page_out.get("peer"):
                        continue
                elif resolved_provider == "whatsapp":
                    if not whatsapp_phone:
                        continue
                    page_out["to"] = whatsapp_phone
                    _assign_whatsapp_to_jid(page_out, resolved_provider, sender_jid_value)
                else:
                    chat_target = avito_chat_id or peer_value or (str(peer_id) if peer_id is not None else "")
                    if not chat_target:
                        continue
                    page_out["peer"] = chat_target
                    page_out["peer_id"] = chat_target
                    page_out["chat_id"] = chat_target
                    if avito_account_id is not None:
                        page_out["account_id"] = avito_account_id
                await _redis_queue.lpush(OUTBOX_QUEUE_KEY, json.dumps(page_out, ensure_ascii=False))
            await _mark_catalog_sent(cache_key)

    fallback_text = default_fallback_reply()
    if not smart_reply_enabled(tenant):
        logger.info(
            "event=smart_reply_disabled tenant=%s channel=%s lead_id=%s",
            tenant,
            provider,
            lead_id,
        )
        fallback_sent = await _queue_text_reply(fallback_text)
        auto_reply_handled = True
        await _enqueue_incoming_event()
        return _ok({"queued": bool(fallback_sent), "leadId": lead_id, "smartReply": False})

    if provider == "telegram":
        logger.info(
            "event=smart_reply_deferred tenant=%s channel=%s lead_id=%s",
            tenant,
            provider,
            lead_id,
        )

    if has_photo:
        logger.info(
            "event=handoff_enqueue_has_photo ch=%s tenant=%s lead_id=%s attachments=%s keys=%s",
            channel,
            tenant,
            lead_id,
            len(attachments),
            list(normalized_event.keys()),
        )
        try:
            serialized = json.dumps(_json_safe(normalized_event), ensure_ascii=False)
            await _redis_queue.lpush(INCOMING_QUEUE_KEY, serialized)
            await _redis_queue.incrby("metrics:telegram:incoming", 1)
            logger.info(
                "stage=incoming_enqueued_photo ch=%s tenant=%s message_id=%s",
                channel,
                tenant,
                normalized_event.get("message_id") or "",
            )
        except Exception:
            logger.exception(
                "stage=incoming_enqueue_photo_failed ch=%s tenant=%s", channel, tenant
            )
            raise HTTPException(status_code=500, detail="queue_error")
        return _ok({"queued": True, "leadId": lead_id, "smartReply": False, "handoff": True})

    await _enqueue_incoming_event()
    return _ok({"queued": False, "leadId": lead_id, "smartReply": True})


def _build_public_catalog_url(
    tenant: int,
    attachment_mtime: int,
    request: Request | None,
) -> str:
    base_override = (getattr(settings, "APP_PUBLIC_URL", "") or "").strip()
    raw_url = ""
    if request is not None:
        try:
            raw_url = str(request.url_for("public_catalog_file", tenant=str(tenant)))
        except Exception:
            raw_url = ""
    if not raw_url:
        fallback_base = (
            base_override
            or getattr(settings, "APP_INTERNAL_URL", "") 
            or getattr(settings, "APP_PUBLIC_URL", "")
            or "http://app:8000"
        )
        raw_url = f"{fallback_base.rstrip('/')}/pub/catalog/file/{tenant}"
    if base_override:
        try:
            current = urlsplit(raw_url)
            target = urlsplit(base_override)
            path = current.path or f"/pub/catalog/file/{tenant}"
            query = current.query
            raw_url = urlunsplit(
                (
                    target.scheme or current.scheme or "https",
                    target.netloc or current.netloc,
                    path,
                    query,
                    current.fragment,
                )
            )
        except Exception:
            raw_url = f"{base_override.rstrip('/')}/pub/catalog/file/{tenant}"
    if attachment_mtime:
        separator = "&" if "?" in raw_url else "?"
        raw_url = f"{raw_url}{separator}v={attachment_mtime}"
    return raw_url


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


def _assign_whatsapp_to_jid(outbound: dict[str, Any], provider_label: str | None, sender_jid: str | None) -> None:
    if not sender_jid:
        return
    provider_value = (provider_label or "").strip().lower()
    if provider_value != "whatsapp" and provider_value != "wa":
        return
    outbound["to_jid"] = sender_jid.strip()


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

    message_node = payload.get("message")
    if not isinstance(message_node, Mapping):
        message_node = {}
    sender_candidates = (
        payload.get("from_jid"),
        payload.get("from_raw"),
        message_node.get("from_jid"),
        message_node.get("from_raw"),
        payload.get("from"),
        payload.get("from_id"),
        message_node.get("from"),
        message_node.get("from_id"),
        payload.get("fromAddress"),
    )
    sender_str = ""
    for candidate in sender_candidates:
        if candidate is None:
            continue
        candidate_text = str(candidate).strip()
        if candidate_text:
            sender_str = candidate_text
            break
    if not sender_str:
        raise ValueError("missing_from")
    sender_digits = _digits(sender_str)
    if not sender_digits:
        raise ValueError("invalid_from")
    formatted_jid = sender_str.strip()
    formatted_lower = formatted_jid.lower()
    if "@" not in formatted_lower:
        sender_jid = f"{sender_digits}@c.us"
    elif formatted_lower.endswith(("@c.us", "@s.whatsapp.net", "@lid", "@g.us")):
        sender_jid = formatted_lower
    else:
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
        "from": sender_jid,
        "from_jid": sender_jid,
        "from_raw": sender_str,
    }
    if sender_digits:
        normalized["from_digits"] = sender_digits

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
        raw_body = await request.body()
        payload = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid_json")
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_payload")

    if not isinstance(payload, dict):
        payload = {}

    try:
        logger.info(
            "telegram_webhook_raw keys=%s",
            list(payload.keys()),
        )
    except Exception:
        logger.exception("telegram_webhook_raw_log_failed")

    # Diagnostic: log raw webhook body to inspect missing manager/out flags.
    try:
        logger.info(
            "manager_diag_raw webhook=telegram len=%s body=%s",
            len(raw_body) if raw_body is not None else 0,
            raw_body.decode("utf-8", errors="ignore") if raw_body else "",
        )
    except Exception:
        logger.exception("manager_diag_raw_failed webhook=telegram")

    tenant_raw = payload.get("tenant_id") or payload.get("tenant")
    try:
        tenant = int(tenant_raw) if tenant_raw is not None else 0
    except Exception:
        tenant = 0
    if tenant <= 0:
        raise HTTPException(status_code=400, detail="invalid_tenant")
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
    raw_msg = payload.get("message")
    message: dict[str, Any] = dict(raw_msg) if isinstance(raw_msg, Mapping) else {}
    # ensure text and common fields are present even if tgworker put them on the root level
    message.setdefault("text", (payload.get("text") or "").strip())
    if "telegram_user_id" not in message:
        message["telegram_user_id"] = payload.get("user_id")
    if "telegram_username" not in message:
        message["telegram_username"] = payload.get("username")
    if "media" not in message:
        message["media"] = payload.get("media")
    if "attachments" not in message and isinstance(payload.get("attachments"), list):
        message["attachments"] = payload.get("attachments")
    # Enrich attachments from provider_raw.media (e.g., MessageMediaPhoto) so worker sees photos.
    try:
        provider_raw = payload.get("provider_raw") if isinstance(payload.get("provider_raw"), dict) else {}
        media_obj = message.get("media") if isinstance(message.get("media"), dict) else provider_raw.get("media") if isinstance(provider_raw, dict) else None
        attachments = message.get("attachments") if isinstance(message.get("attachments"), list) else []
        if media_obj and isinstance(media_obj, Mapping):
            media_type = str(media_obj.get("_") or media_obj.get("type") or "").strip()
            attachment: dict[str, Any] = {"type": media_type or "photo"}
            photo_obj = media_obj.get("photo") if isinstance(media_obj.get("photo"), Mapping) else None
            photo_id = photo_obj.get("id") if isinstance(photo_obj, Mapping) else None
            if photo_id:
                attachment["photo_id"] = photo_id
                attachment["url"] = f"telegram://{tenant}/{photo_id}"
            if attachment not in attachments:
                attachments = list(attachments)
                attachments.append(attachment)
                message["attachments"] = attachments
                payload["attachments"] = attachments
        elif isinstance(message.get("attachments"), list):
            payload["attachments"] = message["attachments"]
    except Exception:
        logger.exception("telegram_webhook_attach_enrich_failed tenant=%s", tenant)
    # preserve raw transport payload to inspect media (e.g., MessageMediaPhoto) and flags
    if "provider_raw" not in message and payload.get("provider_raw") is not None:
        message["provider_raw"] = payload.get("provider_raw")
    body = {
        "source": {"type": "telegram", "tenant": tenant},
        "message": message,
        "telegram": payload,
    }
    if payload.get("provider_raw") is not None:
        body["provider_raw"] = payload.get("provider_raw")
    # Preserve manager/out flags from tgworker payload.
    if payload.get("manager") is not None:
        body["manager"] = payload.get("manager")
        body["message"]["manager"] = payload.get("manager")
    if payload.get("out") is not None:
        body["out"] = payload.get("out")
        body["message"]["out"] = payload.get("out")
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
