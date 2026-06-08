from __future__ import annotations

import json
import os
import time
import logging
import random
from typing import Any, Tuple, Mapping, Iterable

from fastapi import APIRouter, HTTPException, Request  # noqa: F401 - re-exported for legacy tests
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
from libs.core.integrations import max as max_integration
from libs.core.services import amocrm as amocrm_service
from libs.core.services import max_personal_service
from libs.core.services.queue_contract import push_json_left, push_json_right
from libs.core.learning.service import capture_intervention_episode

from .ui import templates  # noqa: F401 - ensure templates loaded for compatibility
from libs.core.common import (
    OUTBOX_QUEUE_KEY,
    HANDOFF_SILENCE_TTL_SECONDS,
    default_fallback_reply,
    handoff_silence_key,
    handoff_silence_meta_key,
    is_manager_telegram,
    is_manager_whatsapp,
    smart_reply_enabled,
)
from libs.core.metrics import DB_ERRORS_COUNTER, WEBHOOK_PROVIDER_COUNTER
from libs.core.message_envelope import (
    build_envelope,
    content_fingerprint,
    detect_message_kind,
    normalize_attachments,
    sanitize_display_name,
    text_or_placeholder,
)
from libs.core.repo import provider_tokens as provider_tokens_repo
from libs.core.lib.numbers import coerce_int as _coerce_int_shared
from libs.core.lib.tg_slots import (
    TG_SLOT_MAX as _TG_SLOT_MAX,
    TG_SLOT_MIN as _TG_SLOT_MIN,
    decode_virtual_tenant as _decode_virtual_tenant,
)
from .services import webhook_provider_runtime
from .services import webhook_max_runtime
from .services import webhook_telegram_runtime
from .services import webhook_entry_runtime
from .services import webhook_incoming_runtime
from .services import webhook_incoming_storage_runtime
from .services import webhook_catalog_runtime
from .services import webhook_process_runtime
from .services import webhook_manager_outgoing_runtime
from .services import webhook_payload_helpers_runtime


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


def _provider_webhook_runtime_deps() -> webhook_provider_runtime.ProviderWebhookDeps:
    return webhook_provider_runtime.ProviderWebhookDeps(
        json_module=json,
        redis_queue=_redis_queue,
        incoming_queue_key=INCOMING_QUEUE_KEY,
        provider_tokens_repo=provider_tokens_repo,
        webhook_provider_counter=WEBHOOK_PROVIDER_COUNTER,
        db_errors_counter=DB_ERRORS_COUNTER,
        extract_provider_token_fn=_extract_provider_token,
        coerce_int_fn=_coerce_int,
        digits_fn=_digits,
        sanitize_media_item_fn=_sanitize_media_item,
        insert_webhook_event_fn=insert_webhook_event,
        logger=logger,
        wa_qr_cache_ttl_min=WA_QR_CACHE_TTL_MIN,
        wa_qr_cache_ttl_max=WA_QR_CACHE_TTL_MAX,
        random_module=random,
        time_module=time,
    )


def _max_webhook_runtime_deps() -> webhook_max_runtime.MaxWebhookDeps:
    return webhook_max_runtime.MaxWebhookDeps(
        json_module=json,
        extract_token_fn=_extract_token,
        max_integration_module=max_integration,
        max_personal_service_module=max_personal_service,
        process_incoming_fn=process_incoming,
        getenv_fn=os.getenv,
        logger=logger,
    )


def _telegram_webhook_runtime_deps() -> webhook_telegram_runtime.TelegramWebhookDeps:
    return webhook_telegram_runtime.TelegramWebhookDeps(
        json_module=json,
        extract_token_fn=_extract_token,
        settings=settings,
        decode_tg_slot_tenant_fn=_decode_tg_slot_tenant,
        normalize_attachments_fn=normalize_attachments,
        detect_message_kind_fn=detect_message_kind,
        process_incoming_fn=process_incoming,
        logger=logger,
    )


def _incoming_runtime_deps() -> webhook_incoming_runtime.IncomingRuntimeDeps:
    return webhook_incoming_runtime.IncomingRuntimeDeps(
        coerce_int_fn=_coerce_int,
        getenv_fn=os.getenv,
        as_mapping_fn=_as_mapping,
        logger=logger,
    )


def _incoming_parse_deps() -> webhook_incoming_runtime.IncomingParseDeps:
    return webhook_incoming_runtime.IncomingParseDeps(
        json_module=json,
        normalize_attachments_fn=normalize_attachments,
        has_photo_attachment_fn=_has_photo_attachment,
        as_mapping_fn=_as_mapping,
        sanitize_display_name_fn=sanitize_display_name,
        is_human_readable_name_fn=_is_human_readable_name,
        is_manager_telegram_fn=is_manager_telegram,
        is_manager_whatsapp_fn=is_manager_whatsapp,
        is_avito_system_message_fn=_is_avito_system_message,
        coerce_int_fn=_coerce_int,
        digits_fn=_digits,
        avito_module=avito,
        logger=logger,
    )


def _incoming_envelope_deps() -> webhook_incoming_runtime.IncomingEnvelopeDeps:
    return webhook_incoming_runtime.IncomingEnvelopeDeps(
        build_envelope_fn=build_envelope,
        extract_tg_slot_fn=_extract_tg_slot,
        digits_fn=_digits,
    )


def _incoming_event_queue_deps() -> webhook_incoming_runtime.IncomingEventQueueDeps:
    return webhook_incoming_runtime.IncomingEventQueueDeps(
        redis_queue=_redis_queue,
        incoming_queue_key=INCOMING_QUEUE_KEY,
        push_json_left_fn=push_json_left,
        json_safe_fn=_json_safe,
        logger=logger,
    )


def _text_reply_deps() -> webhook_incoming_runtime.TextReplyDeps:
    return webhook_incoming_runtime.TextReplyDeps(
        redis_queue=_redis_queue,
        outbox_queue_key=OUTBOX_QUEUE_KEY,
        push_json_left_fn=push_json_left,
        extract_tg_slot_fn=_extract_tg_slot,
        assign_whatsapp_to_jid_fn=_assign_whatsapp_to_jid,
        core_module=core,
    )


def _photo_handoff_enqueue_deps() -> webhook_incoming_runtime.PhotoHandoffEnqueueDeps:
    return webhook_incoming_runtime.PhotoHandoffEnqueueDeps(
        redis_queue=_redis_queue,
        incoming_queue_key=INCOMING_QUEUE_KEY,
        push_json_left_fn=push_json_left,
        json_safe_fn=_json_safe,
        logger=logger,
    )


def _incoming_guard_deps() -> webhook_incoming_runtime.IncomingGuardDeps:
    return webhook_incoming_runtime.IncomingGuardDeps(
        content_fingerprint_fn=content_fingerprint,
        is_duplicate_fn=_is_duplicate,
        logger=logger,
    )


def _incoming_post_catalog_deps() -> webhook_incoming_runtime.IncomingPostCatalogDeps:
    return webhook_incoming_runtime.IncomingPostCatalogDeps(
        redis_queue=_redis_queue,
        handoff_silence_key_fn=handoff_silence_key,
        default_fallback_reply_fn=default_fallback_reply,
        smart_reply_enabled_fn=smart_reply_enabled,
        queue_text_reply_deps=_text_reply_deps(),
        photo_handoff_enqueue_deps=_photo_handoff_enqueue_deps(),
        logger=logger,
    )


def _process_prep_deps() -> webhook_process_runtime.ProcessPrepDeps:
    return webhook_process_runtime.ProcessPrepDeps(
        incoming_envelope_deps=_incoming_envelope_deps(),
        incoming_event_queue_deps=_incoming_event_queue_deps(),
        incoming_storage_deps=_incoming_storage_deps(),
    )


def _process_flow_deps() -> webhook_process_runtime.ProcessFlowDeps:
    return webhook_process_runtime.ProcessFlowDeps(
        catalog_flow_deps=_catalog_flow_deps(),
        incoming_post_catalog_deps=_incoming_post_catalog_deps(),
    )


def _incoming_storage_deps() -> webhook_incoming_storage_runtime.IncomingStorageDeps:
    return webhook_incoming_storage_runtime.IncomingStorageDeps(
        redis_queue=_redis_queue,
        json_module=json,
        upsert_lead_fn=upsert_lead,
        resolve_or_create_contact_fn=resolve_or_create_contact,
        link_lead_contact_fn=link_lead_contact,
        insert_message_in_fn=insert_message_in,
        remember_whatsapp_jid_fn=_remember_whatsapp_jid,
        text_or_placeholder_fn=text_or_placeholder,
        extract_tg_slot_fn=_extract_tg_slot,
        has_contact_identifiers_fn=_has_contact_identifiers,
        handoff_silence_key_fn=handoff_silence_key,
        handoff_silence_meta_key_fn=handoff_silence_meta_key,
        handoff_silence_ttl_seconds=HANDOFF_SILENCE_TTL_SECONDS,
        logger=logger,
    )


def _catalog_flow_deps() -> webhook_catalog_runtime.CatalogFlowDeps:
    return webhook_catalog_runtime.CatalogFlowDeps(
        redis_queue=_redis_queue,
        outbox_queue_key=OUTBOX_QUEUE_KEY,
        catalog_inline_limit_bytes=CATALOG_INLINE_LIMIT_BYTES,
        catalog_sent_cache=_catalog_sent_cache,
        core_module=core,
        push_json_left_fn=push_json_left,
        push_json_right_fn=push_json_right,
        catalog_was_recently_sent_fn=_catalog_was_recently_sent,
        mark_catalog_sent_fn=_mark_catalog_sent,
        reset_catalog_cache_fn=_reset_catalog_cache,
        catalog_message_mark_once_fn=_catalog_message_mark_once,
        resolve_catalog_attachment_fn=_resolve_catalog_attachment,
        build_public_catalog_url_fn=_build_public_catalog_url,
        assign_whatsapp_to_jid_fn=_assign_whatsapp_to_jid,
        user_requested_catalog_fn=_user_requested_catalog,
        smart_reply_enabled_fn=smart_reply_enabled,
        logger=logger,
    )


def _manager_outgoing_deps() -> webhook_manager_outgoing_runtime.ManagerOutgoingDeps:
    return webhook_manager_outgoing_runtime.ManagerOutgoingDeps(
        redis_queue=_redis_queue,
        json_module=json,
        handoff_silence_key_fn=handoff_silence_key,
        handoff_silence_meta_key_fn=handoff_silence_meta_key,
        handoff_silence_ttl_seconds=HANDOFF_SILENCE_TTL_SECONDS,
        upsert_lead_fn=upsert_lead,
        resolve_or_create_contact_fn=resolve_or_create_contact,
        link_lead_contact_fn=link_lead_contact,
        insert_message_out_fn=insert_message_out,
        capture_manager_intervention_fn=_capture_manager_intervention,
        amocrm_service_module=amocrm_service,
        content_fingerprint_fn=content_fingerprint,
        text_or_placeholder_fn=text_or_placeholder,
        has_contact_identifiers_fn=_has_contact_identifiers,
        ok_response_fn=_ok,
        logger=logger,
    )


async def _capture_manager_intervention(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    manager_message_id: int | None,
    source_event: str,
) -> None:
    if not manager_message_id:
        return
    try:
        await capture_intervention_episode(
            tenant_id=int(tenant_id),
            lead_id=int(lead_id),
            channel=str(channel or ""),
            source_event=source_event,
            manager_message_id=int(manager_message_id),
            log_fn=lambda msg: logger.info(msg),
        )
    except Exception:
        logger.exception(
            "learning_v2_capture_failed tenant=%s lead_id=%s channel=%s source_event=%s",
            tenant_id,
            lead_id,
            channel,
            source_event,
        )


def _decode_tg_slot_tenant(raw_tenant: int) -> tuple[int, int]:
    return _decode_virtual_tenant(raw_tenant)


def _extract_tg_slot(message: Mapping[str, Any], payload: Mapping[str, Any]) -> int:
    for source in (message.get("tg_slot"), payload.get("tg_slot"), payload.get("slot")):
        try:
            slot = int(source)
        except Exception:
            continue
        if _TG_SLOT_MIN <= slot <= _TG_SLOT_MAX:
            return slot
    return _TG_SLOT_MIN


def _catalog_cache_redis_key(cache_key: tuple[int, str]) -> str:
    tenant, identifier = cache_key
    return f"catalog:sent:{tenant}:{identifier}"


def _catalog_message_dedup_redis_key(
    *,
    tenant: int,
    provider: str,
    lead_id: int,
    message_id: str,
) -> str:
    provider_norm = (provider or "").strip().lower() or "unknown"
    mid_norm = (message_id or "").strip() or str(lead_id)
    return f"catalog:msgdedup:{tenant}:{provider_norm}:{lead_id}:{mid_norm}"


async def _catalog_message_mark_once(
    *,
    tenant: int,
    provider: str,
    lead_id: int,
    message_id: str,
    ttl_seconds: int = 1800,
) -> bool:
    key = _catalog_message_dedup_redis_key(
        tenant=int(tenant),
        provider=str(provider or ""),
        lead_id=int(lead_id),
        message_id=str(message_id or ""),
    )
    try:
        created = await _redis_queue.set(key, "1", ex=max(60, int(ttl_seconds)), nx=True)
    except Exception:
        logger.debug("catalog_dedup_set_failed key=%s", key, exc_info=True)
        # Do not block catalog flow if Redis transiently fails.
        return True
    return bool(created)


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
    return webhook_payload_helpers_runtime.digits(s)


def _is_human_readable_name(value: Any) -> bool:
    return webhook_payload_helpers_runtime.is_human_readable_name(value)


def _has_photo_attachment(blobs: Iterable[Mapping[str, Any]] | None) -> bool:
    return webhook_payload_helpers_runtime.has_photo_attachment(blobs)


def _as_mapping(candidate: Any) -> Mapping[str, Any] | None:
    return webhook_payload_helpers_runtime.as_mapping(candidate)


def _coerce_int(value: Any) -> int | None:
    return _coerce_int_shared(value)


def _is_avito_system_message(
    text: str,
    message: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> bool:
    return webhook_payload_helpers_runtime.is_avito_system_message(text, message, payload)


def _has_contact_identifiers(
    *,
    phone: str | None = None,
    whatsapp_phone: str | None = None,
    avito_user_id: int | None = None,
    avito_login: str | None = None,
    telegram_user_id: int | None = None,
    telegram_username: str | None = None,
    max_user_id: int | None = None,
    max_username: str | None = None,
) -> bool:
    return webhook_payload_helpers_runtime.has_contact_identifiers(
        phone=phone,
        whatsapp_phone=whatsapp_phone,
        avito_user_id=avito_user_id,
        avito_login=avito_login,
        telegram_user_id=telegram_user_id,
        telegram_username=telegram_username,
        max_user_id=max_user_id,
        max_username=max_username,
    )


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
    return webhook_payload_helpers_runtime.resolve_catalog_attachment(
        cfg,
        tenant,
        request,
        deps=webhook_payload_helpers_runtime.CatalogAttachmentDeps(
            core_module=core,
            settings_module=settings,
            client_config_module=C,
        ),
    )


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
        logger.warning("wa_jid_cache_failed tenant=%s lead_id=%s", tenant, lead_id, exc_info=True)


async def process_incoming(body: dict, request: Request | None = None) -> JSONResponse:
    return await webhook_entry_runtime.process_incoming_entry(
        body,
        request,
        deps=webhook_entry_runtime.ProcessIncomingEntryDeps(
            incoming_runtime_module=webhook_incoming_runtime,
            manager_outgoing_runtime_module=webhook_manager_outgoing_runtime,
            process_runtime_module=webhook_process_runtime,
            incoming_runtime_deps_fn=_incoming_runtime_deps,
            incoming_parse_deps_fn=_incoming_parse_deps,
            manager_outgoing_deps_fn=_manager_outgoing_deps,
            incoming_guard_deps_fn=_incoming_guard_deps,
            process_prep_deps_fn=_process_prep_deps,
            process_flow_deps_fn=_process_flow_deps,
            ok_response_fn=_ok,
            logger=logger,
        ),
    )


def _build_public_catalog_url(
    tenant: int,
    attachment_mtime: int,
    request: Request | None,
) -> str:
    return webhook_payload_helpers_runtime.build_public_catalog_url(
        tenant,
        attachment_mtime,
        request,
        settings_module=settings,
    )


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


def _assign_whatsapp_to_jid(
    outbound: dict[str, Any], provider_label: str | None, sender_jid: str | None
) -> None:
    if not sender_jid:
        return
    provider_value = (provider_label or "").strip().lower()
    if provider_value != "whatsapp" and provider_value != "wa":
        return
    outbound["to_jid"] = sender_jid.strip()


def _normalize_whatsapp_incoming(
    payload: dict[str, Any], tenant: int, lead_hint: int | None = None
) -> dict[str, Any]:
    return webhook_provider_runtime.normalize_whatsapp_incoming(
        payload,
        tenant,
        lead_hint,
        deps=_provider_webhook_runtime_deps(),
    )


async def _queue_incoming_event(event_payload: dict[str, Any]) -> None:
    return await webhook_provider_runtime.queue_incoming_event(
        event_payload,
        deps=_provider_webhook_runtime_deps(),
    )


async def _cache_whatsapp_qr(
    payload: dict[str, Any], tenant: int, provider: str, event_name: str
) -> dict[str, Any]:
    return await webhook_provider_runtime.cache_whatsapp_qr(
        payload,
        tenant,
        provider,
        event_name,
        deps=_provider_webhook_runtime_deps(),
    )


@router.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    return await webhook_telegram_runtime.telegram_webhook(
        request,
        deps=_telegram_webhook_runtime_deps(),
    )


@router.post("/webhook/max")
async def max_webhook(request: Request):
    return await webhook_max_runtime.max_webhook(
        request,
        deps=_max_webhook_runtime_deps(),
    )


@router.post("/webhook/max_personal")
async def max_personal_webhook(request: Request):
    return await webhook_max_runtime.max_personal_webhook(
        request,
        deps=_max_webhook_runtime_deps(),
    )


async def provider_webhook(request: Request) -> JSONResponse:
    return await webhook_provider_runtime.provider_webhook(
        request,
        deps=_provider_webhook_runtime_deps(),
    )


@router.post("/webhook")
async def webhook_entry(request: Request) -> JSONResponse:
    return await provider_webhook(request)


@router.post("/webhook/provider")
async def webhook_provider_compat(request: Request) -> JSONResponse:
    logger.warning("deprecated_webhook_provider_path")
    return await provider_webhook(request)


__all__ = ["router", "process_incoming", "provider_webhook"]


async def _is_duplicate(
    provider: str,
    tenant: int,
    message_id: str | None,
    *,
    fingerprint: str | None = None,
) -> bool:
    msg_suffix = str(message_id or "").strip()
    fp_suffix = str(fingerprint or "").strip()
    key_suffix = f"msg:{msg_suffix}" if msg_suffix else f"fp:{fp_suffix}" if fp_suffix else ""
    if not key_suffix:
        return False
    key = f"incoming:{provider}:{tenant}:{key_suffix}"
    try:
        created = await _redis_queue.setnx(key, int(time.time()))
        if not created:
            return True
        await _redis_queue.expire(key, INCOMING_DEDUP_TTL)
    except Exception:
        logger.exception("stage=dedup provider=%s tenant=%s", provider, tenant)
    return False
