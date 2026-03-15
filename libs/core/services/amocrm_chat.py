from __future__ import annotations

import email.utils
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import pathlib
import re
import secrets
import time
import uuid
from typing import Any, Mapping
from urllib.parse import urlencode, urlparse

import httpx

from libs.core import sales_core as core_module
from libs.core import db as db_module
from libs.core.integrations import avito as avito_integration
from libs.core.message_envelope import (
    content_fingerprint,
    detect_message_kind,
    normalize_attachments,
    sanitize_display_name,
)
from libs.core.repo import crm_chat_links, crm_links, crm_outbox

logger = logging.getLogger(__name__)

AMOCRM_CHAT_PROVIDER = "amocrm"

_ENV_CHAT_ENABLED = "AMOCRM_CHAT_ENABLED"
_ENV_CHAT_SCOPE_ID = "AMOCRM_CHAT_SCOPE_ID"
_ENV_CHAT_CHANNEL_ID = "AMOCRM_CHAT_CHANNEL_ID"
_ENV_CHAT_SOURCE_ID = "AMOCRM_CHAT_SOURCE_ID"
_ENV_CHAT_PUSH_URL = "AMOCRM_CHAT_PUSH_URL"
_ENV_CHAT_WEBHOOK_TOKEN = "AMOCRM_CHAT_WEBHOOK_TOKEN"
_ENV_CHAT_SECRET = "AMOCRM_CHAT_SECRET"
_ENV_CHAT_TITLE = "AMOCRM_CHAT_TITLE"
_ENV_CHAT_BOT_ID = "AMOCRM_CHAT_BOT_ID"
_ENV_CHAT_BASE_URL = "AMOCRM_CHAT_BASE_URL"

_DEFAULT_CHAT_SOURCE_ID = "telegram"
_DEFAULT_CHAT_TITLE = "Avio Telegram"
_DEFAULT_CHAT_BASE_URL = "https://amojo.amocrm.ru"


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _env_value(name: str, tenant_id: int | None = None) -> str:
    if tenant_id is not None:
        scoped = os.getenv(f"{name}_TENANT_{int(tenant_id)}")
        if scoped:
            return scoped.strip()
    return (os.getenv(name) or "").strip()


def _chat_cfg(raw_cfg: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(raw_cfg, Mapping):
        return {}
    integrations = raw_cfg.get("integrations")
    if not isinstance(integrations, Mapping):
        return {}
    amocrm_cfg = integrations.get("amocrm")
    if not isinstance(amocrm_cfg, Mapping):
        return {}
    chat_cfg = amocrm_cfg.get("chat")
    if not isinstance(chat_cfg, Mapping):
        return {}
    return dict(chat_cfg)


def resolve_chat_cfg(cfg: Mapping[str, Any] | None, tenant_id: int | None = None) -> dict[str, Any]:
    chat_cfg = _chat_cfg(cfg)
    enabled_raw = _env_value(_ENV_CHAT_ENABLED, tenant_id)
    if enabled_raw:
        enabled = enabled_raw.lower() in {"1", "true", "yes", "on"}
    else:
        enabled = bool(chat_cfg.get("enabled"))
    scope_id = _env_value(_ENV_CHAT_SCOPE_ID, tenant_id) or str(chat_cfg.get("scope_id") or "").strip()
    channel_id = _env_value(_ENV_CHAT_CHANNEL_ID, tenant_id) or str(chat_cfg.get("channel_id") or "").strip()
    source_id = (
        _env_value(_ENV_CHAT_SOURCE_ID, tenant_id)
        or str(chat_cfg.get("source_id") or "").strip()
        or _DEFAULT_CHAT_SOURCE_ID
    )
    push_url = _env_value(_ENV_CHAT_PUSH_URL, tenant_id) or str(chat_cfg.get("push_url") or "").strip()
    webhook_token = _env_value(_ENV_CHAT_WEBHOOK_TOKEN, tenant_id) or str(chat_cfg.get("webhook_token") or "").strip()
    secret = _env_value(_ENV_CHAT_SECRET, tenant_id) or str(chat_cfg.get("secret") or "").strip()
    title = _env_value(_ENV_CHAT_TITLE, tenant_id) or str(chat_cfg.get("title") or "").strip() or _DEFAULT_CHAT_TITLE
    bot_id = str(_env_value(_ENV_CHAT_BOT_ID, tenant_id) or chat_cfg.get("bot_id") or "").strip()
    amojo_base_url = (
        _env_value(_ENV_CHAT_BASE_URL, tenant_id)
        or str(chat_cfg.get("base_url") or "").strip()
        or _DEFAULT_CHAT_BASE_URL
    ).rstrip("/")
    return {
        "enabled": enabled,
        "scope_id": scope_id,
        "channel_id": channel_id,
        "source_id": source_id,
        "push_url": push_url.rstrip("/"),
        "webhook_token": webhook_token,
        "secret": secret,
        "title": title,
        "bot_id": bot_id,
        "base_url": amojo_base_url,
    }


def env_configured(tenant_id: int | None = None) -> bool:
    cfg = resolve_chat_cfg({}, tenant_id)
    return bool((cfg.get("scope_id") or cfg.get("channel_id")) and cfg.get("secret"))


def is_enabled(cfg: Mapping[str, Any] | None, tenant_id: int | None = None) -> bool:
    resolved = resolve_chat_cfg(cfg, tenant_id)
    return bool(
        resolved.get("enabled")
        and resolved.get("source_id")
        and (resolved.get("scope_id") or (resolved.get("channel_id") and resolved.get("secret")))
    )


def mask_chat_cfg(cfg: Mapping[str, Any] | None, tenant_id: int | None = None) -> dict[str, Any]:
    resolved = resolve_chat_cfg(cfg, tenant_id)
    return {
        "enabled": bool(resolved.get("enabled")),
        "scope_id": str(resolved.get("scope_id") or ""),
        "channel_id": str(resolved.get("channel_id") or ""),
        "source_id": str(resolved.get("source_id") or ""),
        "push_url": str(resolved.get("push_url") or ""),
        "webhook_token_set": bool(resolved.get("webhook_token")),
        "secret_set": bool(resolved.get("secret")),
        "title": str(resolved.get("title") or ""),
        "bot_id_set": bool(resolved.get("bot_id")),
        "env_configured": env_configured(tenant_id),
    }


def ensure_chat_cfg_in_tenant(cfg: Mapping[str, Any] | None, tenant_id: int | None = None) -> dict[str, Any] | None:
    if not isinstance(cfg, Mapping):
        return None
    resolved = resolve_chat_cfg(cfg, tenant_id)
    if not any(resolved.values()):
        return None
    updated = dict(cfg)
    integrations = updated.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg = integrations.get("amocrm")
    if not isinstance(amocrm_cfg, dict):
        amocrm_cfg = {}
    chat_cfg = dict(amocrm_cfg.get("chat") or {})
    chat_cfg["enabled"] = bool(resolved.get("enabled"))
    if resolved.get("scope_id"):
        chat_cfg["scope_id"] = resolved["scope_id"]
    if resolved.get("channel_id"):
        chat_cfg["channel_id"] = resolved["channel_id"]
    if resolved.get("source_id"):
        chat_cfg["source_id"] = resolved["source_id"]
    if resolved.get("title"):
        chat_cfg["title"] = resolved["title"]
    if resolved.get("bot_id"):
        chat_cfg["bot_id"] = resolved["bot_id"]
    if resolved.get("push_url"):
        chat_cfg["push_url"] = resolved["push_url"]
    if not chat_cfg.get("webhook_token"):
        if resolved.get("webhook_token"):
            chat_cfg["webhook_token"] = resolved["webhook_token"]
        else:
            chat_cfg["webhook_token"] = secrets.token_urlsafe(24)
    amocrm_cfg["chat"] = chat_cfg
    integrations["amocrm"] = amocrm_cfg
    updated["integrations"] = integrations
    return updated


def build_webhook_path_token(cfg: Mapping[str, Any] | None, tenant_id: int | None = None) -> str:
    resolved = resolve_chat_cfg(cfg, tenant_id)
    token = str(resolved.get("webhook_token") or "").strip()
    if token:
        return token
    seed = f"{tenant_id or 0}:{resolved.get('scope_id') or ''}:{resolved.get('source_id') or ''}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]


def build_webhook_url(base_url: str, cfg: Mapping[str, Any] | None, tenant_id: int | None = None) -> str:
    token = build_webhook_path_token(cfg, tenant_id)
    return f"{str(base_url or '').rstrip('/')}/pub/integrations/amocrm/chat/webhook?token={token}"


def build_avatar_path_token(cfg: Mapping[str, Any] | None, tenant_id: int, peer_id: str | int) -> str:
    secret = build_webhook_path_token(cfg, int(tenant_id))
    payload = f"{int(tenant_id)}:{str(peer_id).strip()}"
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()[:24]


def build_avatar_proxy_url(
    base_url: str,
    cfg: Mapping[str, Any] | None,
    tenant_id: int,
    peer_id: str | int,
) -> str:
    peer_value = str(peer_id).strip()
    if not peer_value:
        return ""
    token = build_avatar_path_token(cfg, int(tenant_id), peer_value)
    return (
        f"{str(base_url or '').rstrip('/')}/pub/integrations/amocrm/chat/avatar/"
        f"{int(tenant_id)}/{peer_value}/{token}"
    )


def build_lead_avatar_path_token(cfg: Mapping[str, Any] | None, tenant_id: int, lead_id: int) -> str:
    secret = build_webhook_path_token(cfg, int(tenant_id))
    payload = f"lead:{int(tenant_id)}:{int(lead_id)}"
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()[:24]


def build_lead_avatar_proxy_url(
    base_url: str,
    cfg: Mapping[str, Any] | None,
    tenant_id: int,
    lead_id: int,
) -> str:
    token = build_lead_avatar_path_token(cfg, int(tenant_id), int(lead_id))
    return (
        f"{str(base_url or '').rstrip('/')}/pub/integrations/amocrm/chat/lead-avatar/"
        f"{int(tenant_id)}/{int(lead_id)}/{token}"
    )


def find_tenant_by_webhook_token(token: str | None) -> int | None:
    token_value = str(token or "").strip()
    if not token_value:
        return None
    tenants_root = getattr(core_module, "TENANTS_DIR", None)
    if tenants_root is None:
        return None
    try:
        entries = list(pathlib.Path(tenants_root).iterdir())
    except Exception:
        entries = []
    for entry in entries:
        if not entry.is_dir():
            continue
        try:
            tenant_id = int(entry.name)
        except Exception:
            continue
        cfg = core_module.read_tenant_config(tenant_id)
        if build_webhook_path_token(cfg, tenant_id) == token_value:
            return tenant_id
    return None


def find_tenant_by_scope_id(scope_id: str | None) -> int | None:
    scope_value = str(scope_id or "").strip()
    if not scope_value:
        return None
    tenants_root = getattr(core_module, "TENANTS_DIR", None)
    if tenants_root is None:
        return None
    try:
        entries = list(pathlib.Path(tenants_root).iterdir())
    except Exception:
        entries = []
    for entry in entries:
        if not entry.is_dir():
            continue
        try:
            tenant_id = int(entry.name)
        except Exception:
            continue
        cfg = core_module.read_tenant_config(tenant_id)
        resolved = resolve_chat_cfg(cfg, tenant_id)
        if str(resolved.get("scope_id") or "").strip() == scope_value:
            return tenant_id
    return None


def _stable_external_chat_id(tenant_id: int, lead_id: int, channel: str) -> str:
    return f"avio:{int(tenant_id)}:{str(channel or 'telegram').strip().lower()}:{int(lead_id)}"


def _canonical_external_user_id(
    *,
    amo_lead_id: int | None,
    amo_contact_id: int | None,
    fallback_external_user_id: str,
) -> str:
    """Keep one sender identity per amo contact across bridged channels.

    Without this, the same conversation_id may branch into multiple threads
    when Avito and Telegram use different sender IDs.
    """
    if amo_lead_id is not None:
        try:
            lead_value = int(amo_lead_id)
        except Exception:
            lead_value = None
        if lead_value and lead_value > 0:
            return f"amo-lead:{lead_value}"
    if amo_contact_id is not None:
        try:
            contact_value = int(amo_contact_id)
        except Exception:
            contact_value = None
        if contact_value and contact_value > 0:
            return f"amo-contact:{contact_value}"
    return str(fallback_external_user_id or "").strip() or "lead"


async def _canonical_chat_identity(
    tenant_id: int,
    *,
    provider_lead_id: int | None,
    fallback_chat_id: str,
    fallback_conversation_id: str,
) -> tuple[str, str]:
    chat_id = str(fallback_chat_id or "").strip()
    conversation_id = str(fallback_conversation_id or "").strip() or chat_id
    if not provider_lead_id:
        return chat_id, conversation_id
    deterministic = f"avio:{int(tenant_id)}:amo:{int(provider_lead_id)}"
    return deterministic, deterministic


def _public_base_url() -> str:
    for key in ("PUBLIC_BASE_URL", "APP_PUBLIC_URL", "APP_BASE_URL"):
        value = (os.getenv(key) or "").strip().rstrip("/")
        if value:
            return value
    try:
        settings_obj = getattr(core_module, "settings", None)
        value = str(getattr(settings_obj, "APP_PUBLIC_URL", "") or "").strip().rstrip("/")
        if value:
            return value
    except Exception:
        pass
    return ""


def _fallback_avatar_url(tenant_id: int, lead_id: int) -> str | None:
    base_url = _public_base_url()
    if not base_url or tenant_id <= 0 or lead_id <= 0:
        return None
    cfg = core_module.read_tenant_config(int(tenant_id))
    return build_lead_avatar_proxy_url(base_url, cfg, int(tenant_id), int(lead_id))


def _amojo_headers(method: str, path: str, body: bytes, secret: str) -> dict[str, str]:
    content_type = "application/json"
    content_md5 = hashlib.md5(body).hexdigest()
    date_value = email.utils.formatdate(usegmt=True)
    canonical = "\n".join(
        [
            str(method or "").upper(),
            content_md5,
            content_type,
            date_value,
            path,
        ]
    )
    digest = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha1).hexdigest()
    return {
        "Content-Type": content_type,
        "Content-MD5": content_md5,
        "Date": date_value,
        "X-Signature": digest,
    }


async def _amojo_request(
    tenant_id: int,
    *,
    cfg: Mapping[str, Any],
    method: str,
    path: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    base_url = str(cfg.get("base_url") or _DEFAULT_CHAT_BASE_URL).rstrip("/")
    secret = str(cfg.get("secret") or "").strip()
    if not secret:
        raise RuntimeError("amocrm_chat_secret_missing")
    body = json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    headers = _amojo_headers(method, path, body, secret)
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.request(
            method.upper(),
            f"{base_url}{path}",
            content=body,
            headers=headers,
        )
    if response.status_code >= 400:
        detail = response.text[:500]
        raise RuntimeError(f"amocrm_chat_http_error:{response.status_code}:{detail}")
    if response.status_code == 204 or not response.content:
        return {}
    try:
        data = response.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


async def _resolve_amocrm_account(
    tenant_id: int,
    cfg: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    from libs.core.integrations import amocrm as amocrm_integration
    from libs.core.repo import amocrm_tokens
    from libs.core.services import amocrm as amocrm_service

    tenant_cfg = dict(cfg) if isinstance(cfg, Mapping) else core_module.read_tenant_config(int(tenant_id))
    amocrm_cfg = amocrm_service.get_amocrm_cfg(tenant_cfg) or {}
    token_entry = await amocrm_tokens.get(int(tenant_id))
    base_url = await amocrm_service.resolve_api_base_url(amocrm_cfg, int(tenant_id), token_entry)
    if not base_url:
        raise RuntimeError("amocrm_base_url_missing")
    oauth_cfg = amocrm_service.resolve_oauth_cfg(amocrm_cfg, int(tenant_id))
    client = amocrm_integration.AmoCRMClient(
        tenant_id=int(tenant_id),
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )
    account_payload = await client.get_account(with_amojo_id=True)
    return account_payload if isinstance(account_payload, dict) else {}, tenant_cfg


async def ensure_connected(
    tenant_id: int,
    *,
    cfg: Mapping[str, Any] | None = None,
    webhook_base_url: str | None = None,
) -> dict[str, Any]:
    tenant_cfg = dict(cfg) if isinstance(cfg, Mapping) else core_module.read_tenant_config(int(tenant_id))
    resolved = resolve_chat_cfg(tenant_cfg, int(tenant_id))
    if not resolved.get("enabled"):
        return resolved
    if resolved.get("scope_id"):
        return resolved
    if not resolved.get("channel_id") or not resolved.get("secret"):
        logger.info("amocrm_chat_connect_skipped tenant=%s reason=channel_or_secret_missing", tenant_id)
        return resolved
    try:
        account_payload, current_cfg = await _resolve_amocrm_account(int(tenant_id), tenant_cfg)
    except Exception:
        logger.exception("amocrm_chat_account_resolve_failed tenant=%s", tenant_id)
        return resolved
    amojo_id = str(
        account_payload.get("amojo_id")
        or account_payload.get("account_id")
        or (current_cfg.get("integrations", {}) if isinstance(current_cfg, Mapping) else {}).get("amocrm", {}).get("amojo_id")
        or ""
    ).strip()
    if not amojo_id:
        logger.info("amocrm_chat_connect_skipped tenant=%s reason=amojo_id_missing", tenant_id)
        return resolved
    base_url = str(webhook_base_url or _public_base_url()).rstrip("/")
    webhook_url = build_webhook_url(base_url, current_cfg, int(tenant_id)) if base_url else ""
    if not webhook_url:
        logger.info("amocrm_chat_connect_skipped tenant=%s reason=webhook_base_missing", tenant_id)
        return resolved
    payload = {
        "account_id": amojo_id,
        "hook_api_version": "v2",
        "title": str(resolved.get("title") or _DEFAULT_CHAT_TITLE),
        "user": {
            "id": str(resolved.get("bot_id") or f"avio:{int(tenant_id)}"),
            "name": "Avio",
        },
        "webhook_url": webhook_url,
        "is_time_window_disabled": True,
    }
    try:
        response = await _amojo_request(
            int(tenant_id),
            cfg=resolved,
            method="POST",
            path=f"/v2/origin/custom/{resolved['channel_id']}/connect",
            payload=payload,
        )
    except Exception:
        logger.exception("amocrm_chat_connect_failed tenant=%s", tenant_id)
        return resolved
    scope_id = ""
    for key in ("scope_id", "scope"):
        value = response.get(key)
        if value:
            scope_id = str(value).strip()
            break
    if not scope_id:
        for key in ("account", "payload", "result"):
            nested = response.get(key)
            if not isinstance(nested, Mapping):
                continue
            value = nested.get("scope_id") or nested.get("scope")
            if value:
                scope_id = str(value).strip()
                break
    if not scope_id:
        logger.warning("amocrm_chat_connect_no_scope tenant=%s response=%s", tenant_id, response)
        return resolved
    updated = dict(current_cfg) if isinstance(current_cfg, Mapping) else {}
    integrations = updated.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg = integrations.get("amocrm")
    if not isinstance(amocrm_cfg, dict):
        amocrm_cfg = {}
    chat_cfg = dict(amocrm_cfg.get("chat") or {})
    chat_cfg["enabled"] = True
    chat_cfg["scope_id"] = scope_id
    chat_cfg["source_id"] = str(resolved.get("source_id") or _DEFAULT_CHAT_SOURCE_ID)
    chat_cfg["channel_id"] = str(resolved.get("channel_id") or "")
    chat_cfg["title"] = str(resolved.get("title") or _DEFAULT_CHAT_TITLE)
    if not chat_cfg.get("webhook_token"):
        chat_cfg["webhook_token"] = build_webhook_path_token(current_cfg, int(tenant_id))
    amocrm_cfg["amojo_id"] = amojo_id
    amocrm_cfg["chat"] = chat_cfg
    integrations["amocrm"] = amocrm_cfg
    updated["integrations"] = integrations
    core_module.write_tenant_config(int(tenant_id), updated)
    return resolve_chat_cfg(updated, int(tenant_id))


async def _lead_sender_profile(lead_id: int) -> tuple[str, str, dict[str, Any]]:
    meta = await db_module.get_lead_dialog_metadata(int(lead_id))
    data = dict(meta or {})
    tenant_id = int(data.get("tenant_id") or 0)
    channel = str(data.get("channel") or "").strip().lower()
    username = str(data.get("telegram_username") or data.get("contact_telegram_username") or "").strip()
    contact = sanitize_display_name(data.get("contact"))
    title = sanitize_display_name(data.get("title"))
    if title and re.fullmatch(r"(?i)tg:id\s+\d+", title):
        title = None
    phone = str(data.get("phone") or data.get("whatsapp_phone") or "").strip()
    avito_login = sanitize_display_name(data.get("avito_login"))
    avito_user_id = str(data.get("avito_user_id") or "").strip()
    source_real_id = str(data.get("source_real_id") or "").strip()
    peer = str(data.get("peer") or "").strip()
    nickname = f"@{username}" if username and not username.startswith("@") else username
    avatar_url: str | None = None

    if channel == "avito":
        display_name = contact or avito_login or title or f"Avito {lead_id}"
        external_user_id = avito_user_id or source_real_id or peer or f"lead:{int(lead_id)}"
        profile: dict[str, Any] = {"name": display_name}
        linked_tg_user_id: str = ""
        linked_tg_username: str = ""
        linked_tg_title: str = ""
        if tenant_id > 0:
            try:
                crm_link = await crm_links.get_link(int(tenant_id), int(lead_id), AMOCRM_CHAT_PROVIDER)
            except Exception:
                crm_link = None
            provider_lead_id = _coerce_int((crm_link or {}).get("provider_lead_id")) if isinstance(crm_link, Mapping) else None
            if provider_lead_id is not None:
                fetchrow = getattr(db_module, "_fetchrow", None)
                if fetchrow:
                    try:
                        row = await fetchrow(
                            """
                            SELECT l.id,
                                   l.telegram_user_id,
                                   l.peer,
                                   l.telegram_username,
                                   l.title
                            FROM crm_links cl
                            JOIN leads l ON l.id = cl.lead_id
                            WHERE cl.tenant_id = $1
                              AND cl.provider = $2
                              AND cl.provider_lead_id = $3
                              AND l.tenant_id = $1
                              AND l.channel = 'telegram'
                            ORDER BY cl.updated_at DESC, l.updated_at DESC
                            LIMIT 1
                            """,
                            int(tenant_id),
                            AMOCRM_CHAT_PROVIDER,
                            int(provider_lead_id),
                        )
                    except Exception:
                        row = None
                    if row:
                        row_map = dict(row)
                        tg_user = str(row_map.get("telegram_user_id") or row_map.get("peer") or "").strip()
                        if tg_user and tg_user.lstrip("-").isdigit():
                            linked_tg_user_id = tg_user
                        linked_tg_username = str(row_map.get("telegram_username") or "").strip()
                        linked_tg_title = sanitize_display_name(row_map.get("title")) or ""
        try:
            live_profile = await avito_integration.resolve_chat_participant_profile(
                int(tenant_id),
                account_id=_coerce_int(source_real_id),
                chat_id=peer,
                author_id=_coerce_int(avito_user_id),
            )
        except Exception:
            live_profile = {}
        live_name = sanitize_display_name((live_profile or {}).get("name"))
        if live_name:
            display_name = live_name
            profile["name"] = live_name
        live_user_id = str((live_profile or {}).get("user_id") or "").strip()
        if live_user_id:
            external_user_id = live_user_id
        if phone:
            profile["phone"] = phone
        if linked_tg_username:
            profile["nickname"] = (
                linked_tg_username
                if linked_tg_username.startswith("@")
                else f"@{linked_tg_username}"
            )
        elif avito_login:
            profile["nickname"] = avito_login
        live_profile_url = str((live_profile or {}).get("profile_url") or "").strip()
        if live_profile_url:
            profile["profile_link"] = live_profile_url
        if linked_tg_user_id and tenant_id > 0:
            base_url = _public_base_url()
            if base_url:
                avatar_url = build_avatar_proxy_url(
                    base_url,
                    core_module.read_tenant_config(int(tenant_id)),
                    int(tenant_id),
                    linked_tg_user_id,
                )
        if not avatar_url:
            avatar_url = _fallback_avatar_url(int(tenant_id), int(lead_id))
        if avatar_url:
            profile["avatar"] = avatar_url
        if not live_name and linked_tg_title:
            profile["name"] = linked_tg_title
        return external_user_id, display_name, profile

    display_name = title or sanitize_display_name(nickname) or contact or f"Lead {lead_id}"
    external_user_id = str(data.get("telegram_user_id") or "").strip() or f"lead:{int(lead_id)}"
    profile: dict[str, Any] = {"name": display_name}
    if phone:
        profile["phone"] = phone
    if username:
        profile["nickname"] = username if username.startswith("@") else f"@{username}"
        profile["profile_link"] = f"https://t.me/{username.lstrip('@')}"
    if tenant_id > 0 and external_user_id and external_user_id.isdigit():
        base_url = _public_base_url()
        if base_url:
            avatar_url = build_avatar_proxy_url(
                base_url,
                core_module.read_tenant_config(int(tenant_id)),
                int(tenant_id),
                external_user_id,
            )
    if not avatar_url:
        avatar_url = _fallback_avatar_url(int(tenant_id), int(lead_id))
    if avatar_url:
        profile["avatar"] = avatar_url
    return external_user_id, display_name, profile


def _message_payload_text(text: str, attachments: list[dict[str, Any]] | None = None) -> str:
    message_text = str(text or "").strip()
    if message_text:
        return message_text
    if attachments:
        return "Вложение"
    return ""


def _guess_file_name(url: str, mime: str, fallback: str) -> str:
    parsed = urlparse(str(url or "").strip())
    candidate = pathlib.PurePosixPath(parsed.path or "").name
    if candidate:
        return candidate
    ext = mimetypes.guess_extension((mime or "").split(";")[0].strip()) if mime else None
    if ext and not str(fallback or "").endswith(ext):
        return f"{fallback}{ext}"
    return fallback


def _public_attachment_url(tenant_id: int, attachment: Mapping[str, Any]) -> str:
    raw_url = str(attachment.get("url") or "").strip()
    if not raw_url:
        return ""
    if raw_url.startswith("http://") or raw_url.startswith("https://"):
        return raw_url
    base_url = _public_base_url()
    if raw_url.startswith("telegram://"):
        parsed = urlparse(raw_url)
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 2 and base_url:
            try:
                peer_id = int(parts[-2])
                message_id = int(parts[-1])
            except Exception:
                return ""
            access_key = (core_module.get_tenant_pubkey(int(tenant_id)) or "").strip()
            if not access_key:
                return ""
            query = urlencode({"tenant": int(tenant_id), "k": access_key})
            return f"{base_url}/pub/tg/media/{peer_id}/{message_id}?{query}"
        return ""
    if raw_url.startswith("/") and base_url:
        return f"{base_url}{raw_url}"
    return raw_url


def _attachment_message_type(attachment: Mapping[str, Any]) -> str:
    raw_type = str(attachment.get("type") or attachment.get("_") or "").strip().lower()
    mime = str(
        attachment.get("mime")
        or attachment.get("mime_type")
        or attachment.get("content_type")
        or ""
    ).strip().lower()
    if mime.startswith("image/") or "photo" in raw_type or "image" in raw_type or "picture" in raw_type:
        return "picture"
    if "voice" in raw_type or mime.startswith("audio/ogg") or mime.startswith("audio/opus"):
        return "voice"
    if mime.startswith("audio/"):
        return "audio"
    if mime.startswith("video/") or "video" in raw_type:
        return "video"
    return "file"


def _attachment_file_size(attachment: Mapping[str, Any]) -> int | None:
    for key in ("size", "file_size"):
        raw = attachment.get(key)
        try:
            value = int(raw)
        except Exception:
            continue
        if value > 0:
            return value
    return None


def _normalize_media_attachment(
    tenant_id: int,
    attachment: Mapping[str, Any],
) -> dict[str, Any] | None:
    media_url = _public_attachment_url(int(tenant_id), attachment)
    if not media_url:
        return None
    mime = str(
        attachment.get("mime")
        or attachment.get("mime_type")
        or attachment.get("content_type")
        or ""
    ).strip()
    message_type = _attachment_message_type(attachment)
    default_name = {
        "picture": "photo",
        "voice": "voice",
        "audio": "audio",
        "video": "video",
        "file": "file",
    }.get(message_type, "file")
    file_name = str(
        attachment.get("filename")
        or attachment.get("name")
        or attachment.get("title")
        or ""
    ).strip() or _guess_file_name(media_url, mime, default_name)
    payload = {
        "message_type": message_type,
        "media": media_url,
        "file_name": file_name,
    }
    file_size = _attachment_file_size(attachment)
    if file_size is not None:
        payload["file_size"] = file_size
    return payload


def _build_message_payload(
    *,
    tenant_id: int,
    resolved: Mapping[str, Any],
    direction: str,
    source_id: str,
    conversation_id: str,
    external_user_id: str,
    display_name: str,
    profile: Mapping[str, Any],
    text: str,
    media: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    now = time.time()
    message_type = str((media or {}).get("message_type") or "text").strip() or "text"
    message: dict[str, Any] = {
        "type": message_type,
        "text": str(text or "").strip(),
    }
    if media:
        message["media"] = str(media.get("media") or "").strip()
        file_name = str(media.get("file_name") or "").strip()
        if file_name:
            message["file_name"] = file_name
        file_size = media.get("file_size")
        if file_size:
            message["file_size"] = int(file_size)
    payload: dict[str, Any] = {
        "event_type": "new_message",
        "payload": {
            "timestamp": int(now),
            "msec_timestamp": int(now * 1000),
            "msgid": str(uuid.uuid4()),
            "conversation_id": conversation_id,
            "silent": False,
            "source": {"external_id": source_id},
            "message": message,
        },
    }
    if direction == "in":
        payload["payload"]["sender"] = {
            "id": external_user_id,
            "name": display_name,
            "profile": dict(profile or {}),
        }
        avatar = str((profile or {}).get("avatar") or "").strip()
        if avatar:
            payload["payload"]["sender"]["avatar"] = avatar
    else:
        payload["payload"]["sender"] = {
            "id": str(resolved.get("bot_id") or f"avio:{int(tenant_id)}:bot"),
            "ref_id": str(resolved.get("bot_id") or ""),
            "name": "Avio",
        }
        avatar = str((profile or {}).get("avatar") or "").strip()
        if avatar:
            # Amo inbox list may render avatar from the latest sender.
            # Keep client avatar on outbound events too to preserve dialog identity.
            payload["payload"]["sender"]["avatar"] = avatar
        payload["payload"]["receiver"] = {
            "id": external_user_id,
            "name": display_name,
            "profile": dict(profile or {}),
        }
        if avatar:
            payload["payload"]["receiver"]["avatar"] = avatar
        profile_link = str((profile or {}).get("profile_link") or "").strip()
        if profile_link:
            payload["payload"]["receiver"]["profile_link"] = profile_link
    return payload


async def _ensure_remote_chat(
    tenant_id: int,
    *,
    resolved: Mapping[str, Any],
    conversation_id: str,
    external_user_id: str,
    display_name: str,
    profile: Mapping[str, Any],
    source_id: str,
) -> dict[str, Any]:
    scope_id = str(resolved.get("scope_id") or "").strip()
    if not scope_id or not conversation_id or not external_user_id or not display_name:
        return {}
    user_payload: dict[str, Any] = {
        "id": external_user_id,
        "name": display_name,
    }
    avatar = str((profile or {}).get("avatar") or "").strip()
    if avatar:
        user_payload["avatar"] = avatar
    profile_payload: dict[str, Any] = {}
    phone = str((profile or {}).get("phone") or "").strip()
    if phone:
        profile_payload["phone"] = phone
    email = str((profile or {}).get("email") or "").strip()
    if email:
        profile_payload["email"] = email
    if avatar:
        profile_payload["avatar"] = avatar
    if profile_payload:
        user_payload["profile"] = profile_payload
    profile_link = str((profile or {}).get("profile_link") or "").strip()
    if profile_link:
        user_payload["profile_link"] = profile_link
    payload: dict[str, Any] = {
        "account_id": str(resolved.get("amojo_id") or "").strip(),
        "conversation_id": conversation_id,
        "user": user_payload,
        "users": [user_payload],
    }
    if source_id:
        payload["source"] = {"external_id": source_id}
    return await _amojo_request(
        int(tenant_id),
        cfg=resolved,
        method="POST",
        path=f"/v2/origin/custom/{scope_id}/chats",
        payload=payload,
    )


async def _refresh_remote_chat_profile(
    tenant_id: int,
    *,
    resolved: Mapping[str, Any],
    conversation_id: str,
    remote_chat_id: str | None,
    external_user_id: str,
    display_name: str,
    profile: Mapping[str, Any],
    source_id: str,
) -> None:
    scope_id = str(resolved.get("scope_id") or "").strip()
    if not scope_id:
        return
    user_payload: dict[str, Any] = {
        "id": external_user_id,
        "name": display_name,
    }
    avatar = str((profile or {}).get("avatar") or "").strip()
    if avatar:
        user_payload["avatar"] = avatar
    profile_payload: dict[str, Any] = {}
    phone = str((profile or {}).get("phone") or "").strip()
    if phone:
        profile_payload["phone"] = phone
    email = str((profile or {}).get("email") or "").strip()
    if email:
        profile_payload["email"] = email
    if avatar:
        profile_payload["avatar"] = avatar
    if profile_payload:
        user_payload["profile"] = profile_payload
    profile_link = str((profile or {}).get("profile_link") or "").strip()
    if profile_link:
        user_payload["profile_link"] = profile_link

    payload: dict[str, Any] = {
        "conversation_id": conversation_id,
        "user": user_payload,
        "users": [user_payload],
    }
    if source_id:
        payload["source"] = {"external_id": source_id}

    targets: list[str] = []
    chat_id_value = str(remote_chat_id or "").strip()
    conv_value = str(conversation_id or "").strip()
    if chat_id_value:
        targets.append(chat_id_value)
    if conv_value and conv_value not in targets:
        targets.append(conv_value)
    if not targets:
        return
    for target in targets:
        try:
            await _amojo_request(
                int(tenant_id),
                cfg=resolved,
                method="PATCH",
                path=f"/v2/origin/custom/{scope_id}/chats/{target}",
                payload=payload,
            )
            return
        except Exception:
            logger.debug(
                "amocrm_chat_profile_refresh_failed tenant=%s conversation_id=%s target=%s",
                tenant_id,
                conversation_id,
                target,
            )


def _extract_remote_chat_id(payload: Mapping[str, Any] | None) -> str:
    if not isinstance(payload, Mapping):
        return ""
    direct = str(payload.get("id") or payload.get("chat_id") or "").strip()
    if direct:
        return direct
    for key in ("chat", "payload", "result", "_embedded"):
        nested = payload.get(key)
        if not isinstance(nested, Mapping):
            continue
        value = str(nested.get("id") or nested.get("chat_id") or "").strip()
        if value:
            return value
    return ""


async def _bind_contact_chat(
    tenant_id: int,
    *,
    contact_id: int | None,
    remote_chat_id: str | None,
    cfg: Mapping[str, Any] | None,
    lead_id: int | None = None,
) -> bool:
    chat_id = str(remote_chat_id or "").strip()
    if not chat_id:
        return False
    from libs.core.integrations import amocrm as amocrm_integration
    from libs.core.services import amocrm as amocrm_service

    tenant_cfg = dict(cfg) if isinstance(cfg, Mapping) else core_module.read_tenant_config(int(tenant_id))
    amocrm_cfg = amocrm_service.get_amocrm_cfg(tenant_cfg) or {}
    base_url = await amocrm_service.resolve_api_base_url(amocrm_cfg, int(tenant_id))
    oauth_cfg = amocrm_service.resolve_oauth_cfg(amocrm_cfg, int(tenant_id))
    client = amocrm_integration.AmoCRMClient(
        tenant_id=int(tenant_id),
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )

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

    async def _recover_contact_id() -> int | None:
        if not lead_id:
            return None
        existing_link = await crm_links.get_link(
            int(tenant_id),
            int(lead_id),
            AMOCRM_CHAT_PROVIDER,
        )
        if isinstance(existing_link, Mapping) and existing_link.get("provider_contact_id") is not None:
            try:
                existing_contact_id = int(existing_link.get("provider_contact_id"))
            except Exception:
                existing_contact_id = None
            if existing_contact_id and await _contact_exists(existing_contact_id):
                return existing_contact_id
        meta = await db_module.get_lead_dialog_metadata(int(lead_id))
        data = dict(meta or {})
        phone = str(data.get("phone") or data.get("whatsapp_phone") or "").strip() or None
        display_name = (
            sanitize_display_name(data.get("contact"))
            or sanitize_display_name(data.get("title"))
            or sanitize_display_name(data.get("avito_login"))
            or sanitize_display_name(data.get("telegram_username") or data.get("contact_telegram_username"))
        )
        new_contact_id = await client.upsert_contact(
            phone=phone,
            name=str(display_name or "").strip() or None,
        )
        if not new_contact_id:
            return None
        await crm_links.update_provider_contact_id(
            int(tenant_id),
            int(lead_id),
            AMOCRM_CHAT_PROVIDER,
            int(new_contact_id),
        )
        existing = await crm_chat_links.get_link(int(tenant_id), int(lead_id), AMOCRM_CHAT_PROVIDER)
        if isinstance(existing, Mapping):
            await crm_chat_links.upsert_link(
                int(tenant_id),
                int(lead_id),
                AMOCRM_CHAT_PROVIDER,
                external_chat_id=str(existing.get("external_chat_id") or ""),
                external_conversation_id=str(existing.get("external_conversation_id") or ""),
                external_contact_id=int(new_contact_id),
                external_lead_id=int(existing.get("external_lead_id")) if existing.get("external_lead_id") is not None else None,
                chat_scope_id=str(existing.get("chat_scope_id") or ""),
                source_id=str(existing.get("source_id") or ""),
            )
        return int(new_contact_id)

    resolved_contact_id = int(contact_id) if contact_id else None
    if resolved_contact_id and not await _contact_exists(resolved_contact_id):
        resolved_contact_id = None
    if not resolved_contact_id and lead_id:
        try:
            resolved_contact_id = await _recover_contact_id()
        except Exception:
            logger.exception(
                "amocrm_chat_contact_recover_failed tenant=%s lead_id=%s chat_id=%s",
                tenant_id,
                lead_id,
                chat_id,
            )
        if not resolved_contact_id:
            return False

    try:
        await client.link_contact_chat(int(resolved_contact_id), chat_id)
        return True
    except Exception as exc:
        err_text = str(exc or "")
        if lead_id and "AlreadyExists" in err_text and "entity_id" in err_text:
            try:
                current_link = await crm_links.get_link(
                    int(tenant_id),
                    int(lead_id),
                    AMOCRM_CHAT_PROVIDER,
                )
            except Exception:
                current_link = None
            if (
                isinstance(current_link, Mapping)
                and current_link.get("provider_lead_id") is not None
                and current_link.get("provider_contact_id") is not None
            ):
                # Chat is already bound in amoCRM. Do not overwrite a ready local mapping
                # with an arbitrary entity from AlreadyExists payload.
                return True
            match = re.search(r'"entity_id"\s*:\s*(\d+)', err_text)
            if match:
                try:
                    existing_entity_id = int(match.group(1))
                except Exception:
                    existing_entity_id = None
                if existing_entity_id:
                    try:
                        await crm_links.update_provider_contact_id(
                            int(tenant_id),
                            int(lead_id),
                            AMOCRM_CHAT_PROVIDER,
                            int(existing_entity_id),
                        )
                        existing = await crm_chat_links.get_link(int(tenant_id), int(lead_id), AMOCRM_CHAT_PROVIDER)
                        if isinstance(existing, Mapping):
                            existing_external_lead_id = _coerce_int(existing.get("external_lead_id"))
                            await crm_chat_links.upsert_link(
                                int(tenant_id),
                                int(lead_id),
                                AMOCRM_CHAT_PROVIDER,
                                external_chat_id=str(existing.get("external_chat_id") or ""),
                                external_conversation_id=str(existing.get("external_conversation_id") or ""),
                                external_contact_id=int(existing_entity_id),
                                external_lead_id=existing_external_lead_id,
                                chat_scope_id=str(existing.get("chat_scope_id") or ""),
                                source_id=str(existing.get("source_id") or ""),
                            )
                    except Exception:
                        logger.exception(
                            "amocrm_chat_contact_bind_already_exists_recover_failed tenant=%s lead_id=%s chat_id=%s",
                            tenant_id,
                            lead_id,
                            chat_id,
                        )
                    return True
        if lead_id and "EntityNotFound" in str(exc) and "contact_id" in str(exc):
            try:
                recovered = await _recover_contact_id()
            except Exception:
                recovered = None
            if recovered:
                try:
                    await client.link_contact_chat(int(recovered), chat_id)
                    return True
                except Exception:
                    logger.exception(
                        "amocrm_chat_contact_rebind_failed tenant=%s lead_id=%s chat_id=%s",
                        tenant_id,
                        lead_id,
                        chat_id,
                    )
        logger.exception(
            "amocrm_chat_contact_bind_failed tenant=%s contact_id=%s chat_id=%s",
            tenant_id,
            resolved_contact_id,
            chat_id,
        )
        return False


async def enqueue_message(
    tenant_id: int,
    lead_id: int,
    *,
    direction: str,
    text: str,
    channel: str,
    attachments: list[dict[str, Any]] | None = None,
) -> None:
    async def _resolve_target_lead_id(current_lead_id: int) -> int:
        meta = await db_module.get_lead_dialog_metadata(int(current_lead_id))
        current_channel = str((meta or {}).get("channel") or "").strip().lower()
        if current_channel != "avito":
            return int(current_lead_id)
        current_link = await crm_links.get_link(int(tenant_id), int(current_lead_id), AMOCRM_CHAT_PROVIDER)
        provider_lead = (
            int(current_link.get("provider_lead_id"))
            if isinstance(current_link, Mapping) and current_link.get("provider_lead_id") is not None
            else None
        )
        if provider_lead is None:
            return int(current_lead_id)
        fetchrow = getattr(db_module, "_fetchrow", None)
        if not fetchrow:
            return int(current_lead_id)
        row = await fetchrow(
            """
            SELECT l.id
            FROM crm_links cl
            JOIN leads l ON l.id = cl.lead_id
            WHERE cl.tenant_id = $1
              AND cl.provider = $2
              AND cl.provider_lead_id = $3
              AND l.tenant_id = $1
              AND l.channel = 'telegram'
            ORDER BY cl.updated_at DESC
            LIMIT 1
            """,
            int(tenant_id),
            AMOCRM_CHAT_PROVIDER,
            int(provider_lead),
        )
        try:
            target = int((row or {}).get("id") or 0)
        except Exception:
            target = 0
        return int(target) if target > 0 else int(current_lead_id)

    cfg = core_module.read_tenant_config(int(tenant_id))
    if not is_enabled(cfg, int(tenant_id)):
        return
    source_lead_id = int(lead_id)
    target_lead_id = await _resolve_target_lead_id(source_lead_id)
    crm_link = await crm_links.get_link(int(tenant_id), int(target_lead_id), AMOCRM_CHAT_PROVIDER)
    external_lead_id = crm_link.get("provider_lead_id") if isinstance(crm_link, Mapping) else None
    external_contact_id = crm_link.get("provider_contact_id") if isinstance(crm_link, Mapping) else None
    chat_cfg = resolve_chat_cfg(cfg, int(tenant_id))
    existing = await crm_chat_links.get_link(int(tenant_id), int(target_lead_id), AMOCRM_CHAT_PROVIDER)
    external_chat_id = (
        str((existing or {}).get("external_chat_id") or "").strip()
        or _stable_external_chat_id(int(tenant_id), int(target_lead_id), channel)
    )
    external_conversation_id = str((existing or {}).get("external_conversation_id") or external_chat_id)
    # For Avito->Telegram bridge, keep conversation identity bound to the source Avito lead,
    # otherwise repeated bridges may reuse an old Telegram conversation id.
    if channel == "avito" and source_lead_id != target_lead_id:
        source_chat = await crm_chat_links.get_link(int(tenant_id), int(source_lead_id), AMOCRM_CHAT_PROVIDER)
        preferred_chat_id = str((existing or {}).get("external_chat_id") or "").strip()
        preferred_conversation_id = str((existing or {}).get("external_conversation_id") or "").strip()
        if not preferred_chat_id:
            preferred_chat_id = str((source_chat or {}).get("external_chat_id") or "").strip()
            preferred_conversation_id = str((source_chat or {}).get("external_conversation_id") or "").strip()
        if not preferred_chat_id:
            preferred_chat_id = _stable_external_chat_id(int(tenant_id), int(source_lead_id), "avito")
            preferred_conversation_id = preferred_chat_id
        external_chat_id = preferred_chat_id
        external_conversation_id = preferred_conversation_id

    external_chat_id, external_conversation_id = await _canonical_chat_identity(
        int(tenant_id),
        provider_lead_id=int(external_lead_id) if external_lead_id is not None else None,
        fallback_chat_id=external_chat_id,
        fallback_conversation_id=external_conversation_id,
    )
    source_id = str(chat_cfg.get("source_id") or "").strip()
    scope_id = str(chat_cfg.get("scope_id") or "").strip()
    await crm_chat_links.upsert_link(
        int(tenant_id),
        int(target_lead_id),
        AMOCRM_CHAT_PROVIDER,
        external_chat_id=external_chat_id,
        external_conversation_id=external_conversation_id,
        external_contact_id=int(external_contact_id) if external_contact_id is not None else None,
        external_lead_id=int(external_lead_id) if external_lead_id is not None else None,
        chat_scope_id=scope_id,
        source_id=source_id,
    )
    if source_lead_id != target_lead_id:
        await crm_chat_links.upsert_link(
            int(tenant_id),
            int(source_lead_id),
            AMOCRM_CHAT_PROVIDER,
            external_chat_id=external_chat_id,
            external_conversation_id=external_conversation_id,
            external_contact_id=int(external_contact_id) if external_contact_id is not None else None,
            external_lead_id=int(external_lead_id) if external_lead_id is not None else None,
            chat_scope_id=scope_id,
            source_id=source_id,
        )
    if external_lead_id is None or external_contact_id is None:
        logger.info(
            "amocrm_chat_enqueue_deferred tenant=%s lead_id=%s channel=%s reason=crm_link_missing keep_event=1",
            tenant_id,
            target_lead_id,
            channel,
        )
    channel_norm = str(channel or "").strip().lower()
    payload_text = str(text or "").strip()
    if (
        channel_norm == "avito"
        and source_lead_id != target_lead_id
        and payload_text
        and not payload_text.lower().startswith("[avito]")
    ):
        payload_text = f"[Avito] {payload_text}"

    payload = {
        "direction": direction,
        "channel": channel,
        "text": payload_text,
        "attachments": normalize_attachments(attachments or []),
        "external_chat_id": external_chat_id,
        "external_conversation_id": external_conversation_id,
        "external_contact_id": external_contact_id,
        "external_lead_id": external_lead_id,
        "scope_id": scope_id,
        "source_id": source_id,
    }
    try:
        is_dup = await crm_outbox.has_recent_event(
            int(tenant_id),
            AMOCRM_CHAT_PROVIDER,
            int(target_lead_id),
            "chat_sync_message",
            payload,
            window_seconds=45,
        )
    except Exception:
        is_dup = False
    if is_dup:
        logger.info(
            "amocrm_chat_enqueue_skip_duplicate tenant=%s lead_id=%s channel=%s direction=%s",
            tenant_id,
            target_lead_id,
            channel,
            direction,
        )
        return
    await crm_outbox.enqueue(
        int(tenant_id),
        AMOCRM_CHAT_PROVIDER,
        int(target_lead_id),
        "chat_sync_message",
        payload,
    )


async def sync_chat_profile(
    tenant_id: int,
    lead_id: int,
    *,
    cfg: Mapping[str, Any] | None = None,
) -> None:
    existing = await crm_chat_links.get_link(int(tenant_id), int(lead_id), AMOCRM_CHAT_PROVIDER)
    if not isinstance(existing, Mapping):
        return
    external_chat_id = str(existing.get("external_chat_id") or "").strip()
    conversation_id = str(existing.get("external_conversation_id") or external_chat_id or "").strip()
    if not conversation_id:
        return
    resolved = await ensure_connected(int(tenant_id), cfg=cfg)
    chat_cfg = resolve_chat_cfg(cfg, int(tenant_id))
    source_id = str(existing.get("source_id") or chat_cfg.get("source_id") or _DEFAULT_CHAT_SOURCE_ID).strip()
    external_user_id, display_name, profile = await _lead_sender_profile(int(lead_id))
    external_user_id = _canonical_external_user_id(
        amo_lead_id=_coerce_int(existing.get("external_lead_id")),
        amo_contact_id=_coerce_int(existing.get("external_contact_id")),
        fallback_external_user_id=external_user_id,
    )
    remote_chat = await _ensure_remote_chat(
        int(tenant_id),
        resolved=resolved,
        conversation_id=conversation_id,
        external_user_id=external_user_id,
        display_name=display_name,
        profile=profile,
        source_id=source_id,
    )
    remote_chat_id = _extract_remote_chat_id(remote_chat)
    await _refresh_remote_chat_profile(
        int(tenant_id),
        resolved=resolved,
        conversation_id=conversation_id,
        remote_chat_id=remote_chat_id,
        external_user_id=external_user_id,
        display_name=display_name,
        profile=profile,
        source_id=source_id,
    )
    await _bind_contact_chat(
        int(tenant_id),
        contact_id=int(existing.get("external_contact_id")) if existing.get("external_contact_id") is not None else None,
        remote_chat_id=remote_chat_id,
        cfg=cfg,
        lead_id=int(lead_id),
    )


async def push_message(
    tenant_id: int,
    *,
    payload: Mapping[str, Any],
    cfg: Mapping[str, Any] | None = None,
) -> None:
    resolved = await ensure_connected(int(tenant_id), cfg=cfg)
    push_url = str(resolved.get("push_url") or "").strip()
    if push_url:
        headers = {"Content-Type": "application/json"}
        token = str(resolved.get("webhook_token") or "").strip()
        if token:
            headers["X-Avio-Chat-Token"] = token
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(push_url, json=dict(payload), headers=headers)
        response.raise_for_status()
        return
    scope_id = str(resolved.get("scope_id") or "").strip()
    source_id = str(resolved.get("source_id") or _DEFAULT_CHAT_SOURCE_ID).strip()
    if not scope_id:
        logger.info(
            "amocrm_chat_push_skipped tenant=%s reason=scope_id_missing external_chat_id=%s",
            tenant_id,
            payload.get("external_chat_id"),
        )
        return
    direction = str(payload.get("direction") or "in").strip().lower()
    lead_id = int(payload.get("lead_id") or 0)
    raw_attachments = normalize_attachments(payload.get("attachments") or [])
    media_items: list[dict[str, Any]] = []
    seen_media_urls: set[str] = set()
    for item in raw_attachments:
        if not isinstance(item, Mapping):
            continue
        normalized = _normalize_media_attachment(int(tenant_id), item)
        if not normalized:
            continue
        media_url = str(normalized.get("media") or "").strip()
        if not media_url or media_url in seen_media_urls:
            continue
        seen_media_urls.add(media_url)
        media_items.append(normalized)
    explicit_text = str(payload.get("text") or "").strip()
    text = explicit_text if media_items else _message_payload_text(explicit_text, raw_attachments)
    if not text and not media_items:
        logger.info("amocrm_chat_push_skipped tenant=%s reason=empty_text lead_id=%s", tenant_id, lead_id)
        return
    if direction == "out" and not resolved.get("bot_id"):
        logger.info("amocrm_chat_push_skipped tenant=%s reason=bot_id_missing lead_id=%s", tenant_id, lead_id)
        return
    external_chat_id = str(payload.get("external_chat_id") or "").strip()
    conversation_id = str(payload.get("external_conversation_id") or external_chat_id or "").strip()
    if not conversation_id:
        logger.info("amocrm_chat_push_skipped tenant=%s reason=conversation_id_missing lead_id=%s", tenant_id, lead_id)
        return
    external_user_id, display_name, profile = (
        await _lead_sender_profile(lead_id)
        if lead_id > 0
        else ("lead", "Lead", {"name": "Lead"})
    )
    external_user_id = _canonical_external_user_id(
        amo_lead_id=(
            _coerce_int(payload.get("amo_lead_id"))
            or _coerce_int(payload.get("external_lead_id"))
        ),
        amo_contact_id=(
            _coerce_int(payload.get("amo_contact_id"))
            or _coerce_int(payload.get("external_contact_id"))
        ),
        fallback_external_user_id=external_user_id,
    )
    logger.info(
        "amocrm_chat_push_prepare tenant=%s lead_id=%s direction=%s conversation_id=%s sender_id=%s contact_id=%s",
        tenant_id,
        lead_id,
        direction,
        conversation_id,
        external_user_id,
        _coerce_int(payload.get("amo_contact_id")) or _coerce_int(payload.get("external_contact_id")),
    )
    remote_chat: dict[str, Any] = {}
    try:
        remote_chat = await _ensure_remote_chat(
            int(tenant_id),
            resolved=resolved,
            conversation_id=conversation_id,
            external_user_id=external_user_id,
            display_name=display_name,
            profile=profile,
            source_id=source_id,
        )
    except Exception:
        logger.exception(
            "amocrm_chat_create_failed tenant=%s lead_id=%s conversation_id=%s",
            tenant_id,
            lead_id,
            conversation_id,
        )
    contact_bind_ok = False
    remote_chat_id = _extract_remote_chat_id(remote_chat)
    await _refresh_remote_chat_profile(
        int(tenant_id),
        resolved=resolved,
        conversation_id=conversation_id,
        remote_chat_id=remote_chat_id,
        external_user_id=external_user_id,
        display_name=display_name,
        profile=profile,
        source_id=source_id,
    )
    try:
        contact_bind_ok = await _bind_contact_chat(
            int(tenant_id),
            contact_id=int(payload.get("amo_contact_id")) if payload.get("amo_contact_id") is not None else None,
            remote_chat_id=remote_chat_id,
            cfg=cfg,
            lead_id=int(lead_id) if lead_id > 0 else None,
        )
    except Exception:
        contact_bind_ok = False
        logger.exception(
            "amocrm_chat_contact_bind_failed tenant=%s lead_id=%s conversation_id=%s",
            tenant_id,
            lead_id,
            conversation_id,
        )
    required_contact_id = _coerce_int(payload.get("amo_contact_id")) or _coerce_int(payload.get("external_contact_id"))
    if required_contact_id and not contact_bind_ok:
        logger.warning(
            "amocrm_chat_push_skipped tenant=%s lead_id=%s reason=contact_bind_failed contact_id=%s conversation_id=%s",
            tenant_id,
            lead_id,
            required_contact_id,
            conversation_id,
        )
        return
    last_msgid: str | None = None
    payload_specs: list[tuple[str, Mapping[str, Any] | None]]
    if media_items:
        payload_specs = [(text if idx == 0 else "", media) for idx, media in enumerate(media_items)]
    else:
        payload_specs = [(text, None)]
    for item_text, item_media in payload_specs:
        message_payload = _build_message_payload(
            tenant_id=int(tenant_id),
            resolved=resolved,
            direction=direction,
            source_id=source_id,
            conversation_id=conversation_id,
            external_user_id=external_user_id,
            display_name=display_name,
            profile=profile,
            text=item_text,
            media=item_media,
        )
        await _amojo_request(
            int(tenant_id),
            cfg=resolved,
            method="POST",
            path=f"/v2/origin/custom/{scope_id}",
            payload=message_payload,
        )
        last_msgid = str((message_payload.get("payload") or {}).get("msgid") or "").strip() or last_msgid
    await crm_chat_links.touch_message_ids(
        int(tenant_id),
        int(lead_id),
        AMOCRM_CHAT_PROVIDER,
        inbound_message_id=last_msgid if direction == "in" else None,
        outbound_message_id=last_msgid if direction == "out" else None,
    )


def _extract_str(data: Any, *path: str) -> str:
    current = data
    for key in path:
        if not isinstance(current, Mapping):
            return ""
        current = current.get(key)
    return str(current or "").strip()


def _extract_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        if isinstance(parsed, Mapping):
            return parsed
    return None


def _coerce_http_url(value: Any) -> str:
    text = str(value or "").strip()
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return ""


def _extract_media_url(value: Any) -> str:
    direct = _coerce_http_url(value)
    if direct:
        return direct
    if isinstance(value, Mapping):
        for key in (
            "url",
            "media",
            "src",
            "href",
            "link",
            "download_url",
            "preview_url",
            "file_url",
            "original",
            "full",
            "content",
        ):
            direct = _extract_media_url(value.get(key))
            if direct:
                return direct
    if isinstance(value, (list, tuple)):
        for item in value:
            direct = _extract_media_url(item)
            if direct:
                return direct
    return ""


def _extract_webhook_attachments(
    payload: Mapping[str, Any],
    message_obj: Mapping[str, Any] | None,
    content_obj: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    candidates = [content_obj, message_obj, _extract_mapping(payload.get("payload"))]
    attachments: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in candidates:
        if not isinstance(item, Mapping):
            continue
        msg_type = str(item.get("type") or "").strip().lower()
        media_value = (
            item.get("media")
            or item.get("file")
            or item.get("attachment")
            or item.get("preview")
            or item.get("content")
        )
        media_url = _extract_media_url(media_value)
        if not media_url:
            continue
        attachment_type = {
            "picture": "image",
            "image": "image",
            "photo": "image",
            "video": "video",
            "voice": "voice",
            "audio": "audio",
            "file": "file",
            "document": "file",
        }.get(msg_type, "file")
        filename = str(
            item.get("file_name")
            or item.get("filename")
            or item.get("name")
            or item.get("title")
            or ""
        ).strip()
        if not filename and media_url:
            try:
                filename = pathlib.Path(urlparse(media_url).path).name
            except Exception:
                filename = ""
        if not filename:
            filename = {
                "image": "image.jpg",
                "video": "video.mp4",
                "voice": "voice.ogg",
                "audio": "audio.ogg",
                "file": "file",
            }.get(attachment_type, attachment_type)
        mime = str(
            item.get("mime")
            or item.get("mime_type")
            or item.get("content_type")
            or ""
        ).strip()
        if not mime:
            mime = {
                "image": "image/jpeg",
                "video": "video/mp4",
                "voice": "audio/ogg",
                "audio": "audio/ogg",
                "file": "application/octet-stream",
            }.get(attachment_type, "application/octet-stream")
        if attachment_type == "voice" and "." not in filename:
            filename = f"{filename}.ogg"
        size_value = item.get("file_size") or item.get("size")
        try:
            size = int(size_value) if size_value is not None else None
        except Exception:
            size = None
        key = (attachment_type, media_url)
        if key in seen:
            continue
        seen.add(key)
        attachment: dict[str, Any] = {
            "type": attachment_type,
            "url": media_url,
            "name": filename,
            "mime": mime,
        }
        if size is not None and size >= 0:
            attachment["size"] = size
        attachments.append(attachment)
    return attachments


def extract_webhook_message(payload: Mapping[str, Any]) -> dict[str, Any]:
    message_obj = _extract_mapping(payload.get("message"))
    payload_obj = _extract_mapping(payload.get("payload"))
    conversation_obj = _extract_mapping(payload.get("conversation"))
    payload_message_obj = _extract_mapping((payload_obj or {}).get("message"))
    content_obj = _extract_mapping((message_obj or {}).get("message")) or payload_message_obj
    event_type = _extract_str(payload, "event_type") or _extract_str(payload, "type")
    message_text = (
        _extract_str(payload, "message", "text")
        or _extract_str(message_obj, "text")
        or _extract_str(content_obj, "text")
        or _extract_str(message_obj, "message", "text")
        or _extract_str(payload, "text")
        or _extract_str(payload, "payload", "message", "text")
        or _extract_str(payload_obj, "message", "text")
    )
    external_chat_id = (
        _extract_str(payload, "chat_id")
        or _extract_str(payload, "conversation_id")
        or _extract_str(payload, "message", "conversation_id")
        or _extract_str(message_obj, "conversation_id")
        or _extract_str(message_obj, "conversation", "client_id")
        or _extract_str(message_obj, "conversation", "id")
        or _extract_str(payload, "conversation", "id")
        or _extract_str(conversation_obj, "id")
        or _extract_str(payload_obj, "conversation_id")
    )
    external_conversation_id = (
        _extract_str(payload, "conversation_id")
        or _extract_str(payload, "message", "conversation_id")
        or _extract_str(message_obj, "conversation_id")
        or _extract_str(message_obj, "conversation", "client_id")
        or _extract_str(message_obj, "conversation", "id")
        or _extract_str(payload_obj, "conversation_id")
        or external_chat_id
    )
    external_message_id = (
        _extract_str(payload, "message_id")
        or _extract_str(payload, "message", "id")
        or _extract_str(message_obj, "id")
        or _extract_str(message_obj, "msgid")
        or _extract_str(payload, "payload", "message", "id")
        or _extract_str(payload, "payload", "msgid")
        or _extract_str(payload_obj, "message", "id")
        or _extract_str(payload_obj, "msgid")
    )
    attachments = normalize_attachments(
        _extract_webhook_attachments(
            payload,
            message_obj or payload_message_obj,
            content_obj or message_obj or payload_message_obj,
        )
    )
    return {
        "event_type": event_type,
        "text": message_text,
        "external_chat_id": external_chat_id,
        "external_conversation_id": external_conversation_id,
        "external_message_id": external_message_id,
        "attachments": attachments,
        "message_kind": detect_message_kind(message_text, attachments),
        "content_fingerprint": content_fingerprint(message_text, attachments),
        "raw": dict(payload),
    }
