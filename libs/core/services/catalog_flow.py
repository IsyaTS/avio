from __future__ import annotations

import json
import logging
import os
import pathlib
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Tuple

try:  # FastAPI is available in the app context, but keep import defensive
    from fastapi import Request  # type: ignore
except Exception:  # pragma: no cover - fallback for worker-only usage
    Request = None  # type: ignore[assignment]

from libs.core import sales_core as core
from libs.core.common import OUTBOX_QUEUE_KEY, smart_reply_enabled

try:  # pragma: no cover - optional when running outside web context
    from libs.core.web import common as web_common  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    web_common = None  # type: ignore[assignment]

logger = logging.getLogger("app.services.catalog_flow")

settings = core.settings  # type: ignore[attr-defined]
_redis_queue = settings.r

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

_catalog_sent_cache: dict[Tuple[int, str], float] = {}


def _catalog_cache_redis_key(cache_key: tuple[int, str]) -> str:
    tenant, identifier = cache_key
    return f"catalog:sent:{tenant}:{identifier}"


async def _catalog_was_recently_sent(
    cache_key: tuple[int, str] | None,
    redis_conn,
) -> bool:
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
        stored = await redis_conn.get(redis_key)
    except Exception:
        stored = None
    if stored:
        _catalog_sent_cache[cache_key] = now_ts
        return True
    return False


async def _mark_catalog_sent(cache_key: tuple[int, str] | None, redis_conn) -> None:
    if not cache_key:
        return
    ttl = getattr(core, "STATE_TTL_SECONDS", 600)
    _catalog_sent_cache[cache_key] = time.time()
    redis_key = _catalog_cache_redis_key(cache_key)
    try:
        await redis_conn.set(redis_key, str(int(time.time())), ex=ttl)
    except Exception:
        logger.debug("catalog_sent_cache_set_failed key=%s", redis_key, exc_info=True)


async def _reset_catalog_cache(cache_key: tuple[int, str] | None, redis_conn) -> None:
    if not cache_key:
        return
    _catalog_sent_cache.pop(cache_key, None)
    redis_key = _catalog_cache_redis_key(cache_key)
    try:
        await redis_conn.delete(redis_key)
    except Exception:
        logger.debug("catalog_sent_cache_delete_failed key=%s", redis_key, exc_info=True)


@dataclass
class CatalogFlowResult:
    tenant_cfg: dict | None = None
    behavior: dict[str, object] = field(default_factory=dict)
    catalog_already_sent: bool = False
    catalog_sent: bool = False
    price_reply_sent: bool = False
    stop_processing: bool = False
    stop_reason: str | None = None
    auto_reply_handled: bool = False


def _user_requested_catalog(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(token in lowered for token in _CATALOG_KEYWORDS)


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
        try:
            base = str(request.url_for("internal_catalog_file", tenant=str(tenant)))
        except Exception:
            base = ""
    else:
        base = str(getattr(settings, "APP_INTERNAL_URL", "") or getattr(settings, "APP_PUBLIC_URL", "") or "")
        if not base:
            base = "http://app:8000"
        base = f"{base.rstrip('/')}/internal/tenant/{tenant}/catalog-file"

    from urllib.parse import quote

    url = f"{base}?path={quote(str(relative_path), safe='/')}"
    token = ""
    if web_common is not None:
        token = getattr(web_common, "WA_INTERNAL_TOKEN", "") or ""
    if not token:
        token = (
            getattr(core, "WA_INTERNAL_TOKEN", "")
            or getattr(settings, "WA_INTERNAL_TOKEN", "")
            or getattr(core, "INTERNAL_SYNC_TOKEN", "")
            or getattr(settings, "INTERNAL_SYNC_TOKEN", "")
            or ""
        )
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
        "mime": mime,
        "mimetype": mime,
        "sendMediaAsDocument": True,
    }
    if caption:
        attachment["caption"] = caption
    return attachment, caption


def _build_public_catalog_url(
    tenant: int,
    attachment_mtime: int,
    request: Request | None,
    cfg: Mapping[str, Any] | None = None,
) -> str:
    viewer_link = ""
    if isinstance(cfg, Mapping):
        integrations = cfg.get("integrations")
        if isinstance(integrations, Mapping):
            viewer_link = str(
                integrations.get("catalog_url")
                or integrations.get("pdf_catalog_url")
                or ""
            ).strip()
        if not viewer_link:
            passport = cfg.get("passport")
            if isinstance(passport, Mapping):
                viewer_link = str(passport.get("catalog_url") or "").strip()
    if not viewer_link:
        persona_meta = core.persona_meta_config(tenant)
        if isinstance(persona_meta, Mapping):
            viewer_link = str(persona_meta.get("catalog_url") or "").strip()
    if viewer_link:
        separator = "&" if "?" in viewer_link else "?"
        version = attachment_mtime or int(time.time())
        return f"{viewer_link}{separator}v={version}"

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
        raw_url = f"{fallback_base.rstrip('/')}/public/catalog/{tenant}"

    separator = "&" if "?" in raw_url else "?"
    version = attachment_mtime or int(time.time())
    return f"{raw_url}{separator}v={version}"


async def handle_catalog_flow(
    *,
    tenant: int,
    lead_id: int,
    refer_id: int,
    text: str,
    provider: str,
    resolved_provider: str,
    message_id: str,
    cache_key: tuple[int, str] | None,
    whatsapp_phone: str | None = None,
    whatsapp_jid: str | None = None,
    telegram_user_id: int | None = None,
    peer_value: str | None = None,
    peer_id: int | None = None,
    request: Request | None = None,
    redis_conn=None,
    tenant_cfg: dict | None = None,
) -> CatalogFlowResult:
    """Shared catalog logic for both webhooks and worker."""

    result = CatalogFlowResult()
    redis_conn = redis_conn or _redis_queue

    text_value = (text or "").strip()
    now_ts = time.time()

    key = cache_key
    if key is None:
        if resolved_provider == "telegram":
            if telegram_user_id:
                key = (tenant, f"tg:{telegram_user_id}")
            elif peer_value:
                key = (tenant, f"tg:peer:{peer_value}")
        elif whatsapp_phone:
            key = (tenant, whatsapp_phone)
    if key is None and lead_id:
        key = (tenant, f"lead:{lead_id}")

    catalog_already_sent = await _catalog_was_recently_sent(key, redis_conn)
    result.catalog_already_sent = catalog_already_sent

    cfg = tenant_cfg
    behavior: dict[str, object] = {}
    attachment: dict | None = None
    caption = ""
    try:
        if cfg is None:
            cfg = core.load_tenant(tenant)
        if isinstance(cfg, Mapping):
            raw_behavior = cfg.get("behavior")
            if isinstance(raw_behavior, Mapping):
                behavior = dict(raw_behavior)
        attachment, caption = _resolve_catalog_attachment(cfg, tenant, request)
    except Exception:
        cfg = cfg if isinstance(cfg, dict) else None
        behavior = {}
        attachment, caption = None, ""

    result.tenant_cfg = cfg if isinstance(cfg, dict) else None
    result.behavior = behavior

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

    file_url = _build_public_catalog_url(tenant, attachment_mtime, request, cfg)
    prefer_link = resolved_provider == "whatsapp"
    use_file_link = False
    if file_url and (
        prefer_link
        or (
            attachment_size
            and CATALOG_INLINE_LIMIT_BYTES
            and attachment_size > CATALOG_INLINE_LIMIT_BYTES
        )
    ):
        use_file_link = True

    lowered_text = text_value.lower()
    forced_catalog = bool(text_value and _user_requested_catalog(text_value))
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
        text_value,
        int(has_attachment),
        int(has_file_link),
        int(forced_catalog),
        int(catalog_already_sent),
        key,
    )
    if price_question and not forced_catalog:
        should_send_catalog = False
    logger.warning(
        "catalog_flow tenant=%s text=%r forced=%s already_sent=%s attachment=%s cache_hit=%s",
        tenant,
        text_value,
        int(forced_catalog),
        int(catalog_already_sent),
        isinstance(attachment, dict),
        bool(key and key in _catalog_sent_cache),
    )

    catalog_sent_now = False
    if should_send_catalog and (provider or "").lower() != "avito":
        if forced_catalog and key:
            await _reset_catalog_cache(key, redis_conn)
        catalog_text_override = None
        attachment_payload = attachment
        caption_value = caption
        if use_file_link:
            catalog_text_override = f"Каталог: {file_url}"
            attachment_payload = None
            caption_value = ""
            logger.info(
                "catalog_file_link tenant=%s size_bytes=%s url=%s",
                tenant,
                attachment_size,
                file_url,
            )
        catalog_text = (catalog_text_override or caption_value or "Каталог во вложении (PDF).").strip()
        catalog_out: Dict[str, Any] = {
            "lead_id": lead_id,
            "text": catalog_text,
            "provider": resolved_provider,
            "ch": resolved_provider,
            "tenant_id": int(tenant),
            "tenant": int(tenant),
            "message_id": message_id or str(lead_id),
            "attachments": [attachment_payload] if attachment_payload else [],
        }
        if attachment_payload:
            catalog_out["attachment"] = attachment_payload
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
            if whatsapp_jid:
                catalog_out["to_jid"] = whatsapp_jid

        if should_send_catalog:
            await redis_conn.lpush(OUTBOX_QUEUE_KEY, json.dumps(catalog_out, ensure_ascii=False))
            await _mark_catalog_sent(key, redis_conn)
            try:
                core.record_bot_reply(refer_id, tenant, provider, catalog_text, tenant_cfg=cfg)
            except Exception:
                pass
            catalog_sent_now = True

    result.catalog_sent = catalog_sent_now

    if catalog_sent_now and not text_value:
        result.stop_processing = True
        result.stop_reason = "catalog_only"
        result.auto_reply_handled = False
        return result

    if price_question and not catalog_sent_now:
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

        auto_reply_flag = False
        if isinstance(behavior, Mapping):
            try:
                auto_reply_flag = bool(behavior.get("auto_reply"))
            except Exception:
                auto_reply_flag = False

        avito_price_blocked = resolved_provider == "avito" and auto_reply_flag
        # Do not send price-replies in Avito channel at all to avoid overriding auto-reply/templates.
        if resolved_provider == "avito":
            avito_price_blocked = True

        if smart_reply_enabled(tenant) and not avito_price_blocked:
            try:
                catalog_matches = core.search_catalog({}, limit=5, tenant=tenant, query=text_value or "")
            except Exception:
                catalog_matches = []
            if catalog_matches:
                best = catalog_matches[0]
                if _has_relevance(text_value or "", best):
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
                            price_out["to"] = whatsapp_phone
                            if whatsapp_jid:
                                price_out["to_jid"] = whatsapp_jid
                        await redis_conn.lpush(OUTBOX_QUEUE_KEY, json.dumps(price_out, ensure_ascii=False))
                        try:
                            core.record_bot_reply(refer_id, tenant, provider, reply_text, tenant_cfg=cfg)
                        except Exception:
                            pass
                        result.price_reply_sent = True
                        result.stop_processing = True
                        result.stop_reason = "price_reply"
                        result.auto_reply_handled = True
                        return result

    behavior = behavior or {}
    always_full = bool(behavior.get("always_full_catalog")) if behavior else False
    send_pages_pref = bool(behavior.get("send_catalog_as_pages")) if behavior else False
    should_send_catalog_pages = False  # отключено для всех каналов

    if should_send_catalog_pages:
        try:
            items = core.read_all_catalog(cfg)
            page_size = int(os.getenv("CATALOG_PAGE_SIZE", "10"))
            pages = core.paginate_catalog_text(items, cfg, page_size)
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
                if whatsapp_jid:
                    page_out["to_jid"] = whatsapp_jid
                await redis_conn.lpush(OUTBOX_QUEUE_KEY, json.dumps(page_out, ensure_ascii=False))
            await _mark_catalog_sent(key, redis_conn)

    return result


__all__ = [
    "CatalogFlowResult",
    "handle_catalog_flow",
    "_catalog_sent_cache",
    "_resolve_catalog_attachment",
]
