from __future__ import annotations

import json
import os
import time
import pathlib
import logging
from typing import Any, Dict, Tuple

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response

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
    )
except ImportError:  # pragma: no cover
    from app.db import (  # type: ignore
        resolve_or_create_contact,
        link_lead_contact,
        insert_message_in,
        upsert_lead,
    )

try:
    from . import common as C  # type: ignore
except ImportError:  # pragma: no cover
    from app.web import common as C  # type: ignore

from .public import templates  # noqa: F401 - ensure templates loaded for compatibility
from app.common import OUTBOX_QUEUE_KEY, smart_reply_enabled


logger = logging.getLogger("app.web.webhooks")

INCOMING_QUEUE_KEY = "inbox:message_in"
INCOMING_DEDUP_TTL = 60 * 60 * 24  # 24 hours

router = APIRouter()


ask_llm = core.ask_llm  # type: ignore[attr-defined]
build_llm_messages = core.build_llm_messages  # type: ignore[attr-defined]
settings = core.settings  # type: ignore[attr-defined]


_redis_queue = settings.r
_catalog_sent_cache: dict[Tuple[int, str], float] = {}

WA_QR_CACHE_TTL = 600  # seconds, >= 120 per webhook spec


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
    if not isinstance(cfg, dict):
        return None, ""
    integrations = cfg.get("integrations", {}) if isinstance(cfg.get("integrations"), dict) else {}
    meta = integrations.get("uploaded_catalog")
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
    telegram_username = None
    peer_id: int | None = None
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
        telegram_username = msg.get("telegram_username") or body.get("username")
        peer_candidate = (
            msg.get("peer_id")
            or body.get("peer_id")
            or msg.get("chat_id")
            or body.get("chat_id")
        )
        if peer_candidate is not None:
            try:
                peer_id = int(peer_candidate)
            except Exception:
                peer_id = None
    else:
        from_id = msg.get("from") or msg.get("author") or body.get("from") or ""
        whatsapp_phone = _digits(from_id.split("@", 1)[0] if from_id else "")

    lead_hint = _coerce_int(body.get("leadId") or body.get("lead_id"))
    ts_fallback = int(time.time() * 1000)
    lead_id_value = lead_hint
    if provider == "telegram":
        if telegram_user_id is not None:
            lead_id_value = telegram_user_id
        elif peer_id is not None:
            lead_id_value = peer_id
    if lead_id_value in (None, 0):
        lead_id_value = ts_fallback
    lead_id = int(lead_id_value)

    channel = provider or "whatsapp"
    logger.info(
        "webhook_received channel=%s tenant=%s lead_id=%s message_id=%s",
        channel,
        tenant,
        lead_id,
        message_id or "",
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

    try:
        await _redis_queue.lpush(
            INCOMING_QUEUE_KEY, json.dumps(normalized_event, ensure_ascii=False)
        )
        if channel == "telegram":
            await _redis_queue.incrby("metrics:telegram:incoming", 1)
        elif channel == "whatsapp":
            await _redis_queue.incrby("metrics:whatsapp:incoming", 1)
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
        }
        if telegram_user_id is not None:
            upsert_kwargs["telegram_user_id"] = int(telegram_user_id)
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
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
        )
        if contact_id:
            await link_lead_contact(lead_id, contact_id)
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

    if attachment and not catalog_already_sent and provider != "telegram":
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
        catalog_out["to"] = whatsapp_phone
        catalog_out["attachment"] = attachment
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

    if not reply:
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
        "text": reply,
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
                page_out = {
                    "lead_id": lead_id,
                    "text": page,
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

    return await process_incoming(body, request)


@router.post("/webhook/provider")
async def provider_webhook(request: Request) -> Response:
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

    provider = str(payload.get("provider") or "").strip().lower()
    event = str(payload.get("event") or "").strip().lower()
    if provider != "whatsapp" or event != "wa_qr":
        return Response(status_code=204)

    tenant_candidate = _coerce_int(payload.get("tenant"))
    qr_id_raw = payload.get("qr_id")
    svg_raw = payload.get("svg")
    if svg_raw is None:
        svg_raw = payload.get("qr")
    if svg_raw is None:
        svg_raw = payload.get("data")
    if svg_raw is None:
        nested_payload = payload.get("payload")
        if isinstance(nested_payload, dict):
            svg_raw = nested_payload.get("svg")
    if tenant_candidate is None:
        raise HTTPException(status_code=400, detail="invalid_tenant")
    if qr_id_raw is None:
        raise HTTPException(status_code=400, detail="invalid_payload")

    tenant = int(tenant_candidate)
    qr_id = str(qr_id_raw).strip()
    svg = str(svg_raw or "").strip()
    svg_lower = svg.lower()
    if not qr_id or not svg or "<svg" not in svg_lower or "</svg" not in svg_lower:
        logger.warning("wa_qr_invalid tenant=%s qr_id=%s reason=no_svg", tenant, qr_id or "-")
        raise HTTPException(status_code=400, detail="invalid_svg")

    png_raw = payload.get("png_base64") or payload.get("png")
    png_value = str(png_raw or "").strip() or None
    txt_raw = payload.get("txt") or payload.get("text") or payload.get("qr_text")
    txt_value = str(txt_raw or "").strip() or None

    cache_key = f"wa:qr:{tenant}:{qr_id}"
    svg_key = f"{cache_key}:svg"
    png_key = f"{cache_key}:png"
    txt_key = f"{cache_key}:txt"
    last_key = f"wa:qr:last:{tenant}"
    now_ts = int(time.time())
    entry = {
        "tenant": tenant,
        "qr_id": qr_id,
        "qr_svg": svg,
        "qr_png": png_value,
        "qr_text": txt_value,
        "provider": provider,
        "event": event,
        "updated_at": now_ts,
    }

    try:
        await _redis_queue.set(svg_key, svg, ex=WA_QR_CACHE_TTL)
        if png_value:
            await _redis_queue.set(png_key, png_value, ex=WA_QR_CACHE_TTL)
        if txt_value:
            await _redis_queue.set(txt_key, txt_value, ex=WA_QR_CACHE_TTL)
        await _redis_queue.set(last_key, qr_id, ex=WA_QR_CACHE_TTL)
        await _redis_queue.set(cache_key, json.dumps(entry, ensure_ascii=False), ex=WA_QR_CACHE_TTL)
    except Exception:
        logger.exception("wa_qr_cache_write_failed tenant=%s qr_id=%s", tenant, qr_id)
        raise HTTPException(status_code=500, detail="cache_error")

    logger.info("wa_qr_cached tenant=%s qr_id=%s", tenant, qr_id)
    return Response(status_code=204)


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
