"""Avito OAuth helpers and utilities."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Mapping, Optional, Tuple, Sequence
from urllib.parse import urlencode

import httpx

import app.core as core_module
from app.core import (
    settings,
    read_tenant_config,
    write_tenant_config,
)

logger = logging.getLogger(__name__)

AUTH_URL = getattr(settings, "AVITO_AUTH_URL", "https://www.avito.ru/oauth")
TOKEN_URL = getattr(settings, "AVITO_TOKEN_URL", "https://api.avito.ru/token/")
DEFAULT_SCOPE = getattr(settings, "AVITO_SCOPE", "") or ""
DEFAULT_REDIRECT_URL = getattr(settings, "AVITO_REDIRECT_URL", "").strip()
OAUTH_TIMEOUT = getattr(settings, "AVITO_TIMEOUT", 10.0) or 10.0


class AvitoOAuthError(RuntimeError):
    """Raised when Avito OAuth flow fails."""


def build_authorize_url(
    state: str,
    *,
    redirect_uri: str | None = None,
    scope: str | None = None,
) -> str:
    redirect = (redirect_uri or DEFAULT_REDIRECT_URL or "").strip()
    params: dict[str, str] = {
        "response_type": "code",
        "client_id": getattr(settings, "AVITO_CLIENT_ID", "").strip(),
        "state": state,
    }
    if redirect:
        params["redirect_uri"] = redirect
    scope_value = (scope or DEFAULT_SCOPE or "").strip()
    if scope_value:
        params["scope"] = scope_value
    return f"{AUTH_URL}?{urlencode(params)}"


async def exchange_code(
    code: str,
    *,
    redirect_uri: str | None = None,
) -> Mapping[str, Any]:
    client_id = getattr(settings, "AVITO_CLIENT_ID", "").strip()
    client_secret = getattr(settings, "AVITO_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise AvitoOAuthError("Avito client credentials are not configured")

    redirect = (redirect_uri or DEFAULT_REDIRECT_URL or "").strip()
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if redirect:
        data["redirect_uri"] = redirect

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
        response = await client.post(TOKEN_URL, data=data, headers=headers)

    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        message = f"Avito token exchange failed: HTTP {response.status_code}"
        logger.warning("%s detail=%s", message, detail)
        raise AvitoOAuthError(message)

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        logger.warning("Avito token response decode failed: %s", exc)
        raise AvitoOAuthError("Failed to decode Avito token response") from exc

    return payload


async def exchange_code_for_token(tenant: int, code: str) -> dict[str, Any]:
    """Exchange an authorization code for access and refresh tokens."""

    payload = await exchange_code(code)
    if not isinstance(payload, Mapping):
        raise AvitoOAuthError("Invalid Avito token response")
    return dict(payload)


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


_ACCOUNT_TENANT_CACHE: dict[int, int] = {}


def _cache_account_mapping(tenant: int, account_id: Any) -> None:
    account_value = _coerce_int(account_id)
    if account_value is None:
        return
    _ACCOUNT_TENANT_CACHE[account_value] = int(tenant)


def get_integration(tenant: int) -> Optional[dict[str, Any]]:
    cfg = read_tenant_config(int(tenant))
    if not isinstance(cfg, Mapping):
        return None
    integrations = cfg.get("integrations")
    if not isinstance(integrations, Mapping):
        return None
    avito_cfg = integrations.get("avito")
    if isinstance(avito_cfg, Mapping):
        result = dict(avito_cfg)
        account_value = result.get("account_id")
        if account_value is not None:
            _cache_account_mapping(int(tenant), account_value)
        return result
    return None


def update_integration(tenant: int, data: Mapping[str, Any]) -> dict[str, Any]:
    cfg = read_tenant_config(int(tenant))
    if not isinstance(cfg, dict):
        cfg = {}
    integrations = cfg.setdefault("integrations", {})
    existing = integrations.get("avito") if isinstance(integrations.get("avito"), Mapping) else {}
    avito_cfg = dict(existing)
    avito_cfg.update(data)
    integrations["avito"] = avito_cfg
    write_tenant_config(int(tenant), cfg)
    account_value = _coerce_int(avito_cfg.get("account_id"))
    if account_value is not None:
        _cache_account_mapping(int(tenant), account_value)
    return avito_cfg


def find_tenant_by_account(account_id: Any) -> Optional[int]:
    account_val = _coerce_int(account_id)
    if account_val is None:
        return None
    cached = _ACCOUNT_TENANT_CACHE.get(account_val)
    if cached is not None:
        return cached

    tenants_root = getattr(core_module, "TENANTS_DIR", None)
    if tenants_root is None:
        return None
    try:
        entries = list(tenants_root.iterdir())
    except Exception:
        entries = []

    for entry in entries:
        if not entry.is_dir():
            continue
        try:
            tenant_id = int(entry.name)
        except Exception:
            continue
        integration = get_integration(tenant_id)
        if not integration:
            continue
        cached_account = _coerce_int(integration.get("account_id"))
        if cached_account == account_val:
            _cache_account_mapping(tenant_id, account_val)
            return tenant_id
    return None


def stable_lead_id(account_id: Any, chat_id: Any) -> int:
    base = f"{account_id}:{chat_id}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()
    # Use upper 60 bits to stay within signed BIGINT range
    return int(digest[:15], 16) or int(digest[15:30], 16) or 1


async def _refresh_access_token(tenant: int, integration: Mapping[str, Any]) -> dict[str, Any]:
    refresh_token = integration.get("refresh_token")
    if not refresh_token:
        raise AvitoOAuthError("Avito refresh token is missing")

    client_id = getattr(settings, "AVITO_CLIENT_ID", "").strip()
    client_secret = getattr(settings, "AVITO_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise AvitoOAuthError("Avito client credentials are not configured")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
        response = await client.post(TOKEN_URL, data=data, headers=headers)

    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        message = f"Avito token refresh failed: HTTP {response.status_code}"
        logger.warning("%s detail=%s tenant=%s", message, detail, tenant)
        raise AvitoOAuthError(message)

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        logger.warning("Avito token refresh decode failed: %s", exc)
        raise AvitoOAuthError("Failed to decode Avito token response") from exc

    merged = dict(integration)
    merged.update(payload)
    expires_in = _coerce_int(payload.get("expires_in"))
    now = int(time.time())
    if expires_in and expires_in > 0:
        merged["expires_at"] = now + int(expires_in)
    merged["obtained_at"] = now
    stored = update_integration(tenant, merged)
    return stored


async def refresh_access_token(tenant: int) -> dict[str, Any]:
    integration = get_integration(int(tenant))
    if not integration:
        raise AvitoOAuthError("Avito integration is not configured for tenant")
    refreshed = await _refresh_access_token(int(tenant), integration)
    token_value = str(refreshed.get("access_token") or "").strip()
    if token_value:
        try:
            refreshed = await ensure_account_info(int(tenant), refreshed, token=token_value)
        except AvitoOAuthError:
            raise
        except Exception:
            logger.exception("avito_account_info_refresh_failed tenant=%s", tenant)
    return refreshed


async def ensure_access_token(tenant: int) -> Tuple[str, dict[str, Any]]:
    integration = get_integration(int(tenant))
    if not integration:
        raise AvitoOAuthError("Avito integration is not configured for tenant")

    token = str(integration.get("access_token") or "").strip()
    expires_at = integration.get("expires_at")
    now = int(time.time())
    needs_refresh = False
    if not token:
        needs_refresh = True
    elif expires_at is not None:
        try:
            exp = int(expires_at)
        except Exception:
            needs_refresh = True
        else:
            if exp - 30 <= now:
                needs_refresh = True

    if needs_refresh:
        integration = await _refresh_access_token(int(tenant), integration)
        token = str(integration.get("access_token") or "").strip()
        if not token:
            raise AvitoOAuthError("Failed to obtain Avito access token")

    try:
        integration = await ensure_account_info(int(tenant), integration, token=token)
    except AvitoOAuthError:
        raise
    except Exception:
        logger.exception("avito_account_info_sync_failed tenant=%s", tenant)

    return token, integration


async def ensure_account_info(
    tenant: int,
    integration: Mapping[str, Any],
    *,
    token: Optional[str] = None,
) -> dict[str, Any]:
    if integration.get("account_id"):
        return dict(integration)
    token_value = token or str(integration.get("access_token") or "").strip()
    if not token_value:
        return dict(integration)

    info = await _fetch_account_info(token_value)
    if not info:
        return dict(integration)

    merged = dict(integration)
    merged.update(info)
    return update_integration(int(tenant), merged)


async def sync_account_info(tenant: int) -> dict[str, Any]:
    """Synchronize Avito account metadata for the tenant."""

    integration = get_integration(int(tenant)) or {}
    token = str(integration.get("access_token") or "").strip()
    if not token:
        raise AvitoOAuthError("Avito access token is not configured for tenant")
    updated = await ensure_account_info(int(tenant), integration, token=token)
    return dict(updated)


async def _fetch_account_info(token: str) -> Optional[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    candidate_urls = [
        "https://api.avito.ru/messenger/v1/accounts",
        "https://api.avito.ru/api/v1/accounts/self",
        "https://api.avito.ru/core/v1/accounts/self",
    ]
    async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
        for url in candidate_urls:
            try:
                response = await client.get(url, headers=headers)
            except httpx.HTTPError as exc:  # pragma: no cover - network exception
                logger.warning("avito_account_info_request_failed url=%s error=%s", url, exc)
                continue

            if response.status_code == 401:
                raise AvitoOAuthError("Avito token unauthorized while fetching account info")
            if response.status_code >= 500:
                logger.warning(
                    "avito_account_info_server_error status=%s url=%s", response.status_code, url
                )
                continue
            if response.status_code != 200:
                logger.info(
                    "avito_account_info_unexpected status=%s url=%s", response.status_code, url
                )
                continue

            try:
                payload = response.json()
            except json.JSONDecodeError:
                logger.warning("avito_account_info_decode_failed url=%s", url)
                continue

            info = _extract_account_info(payload)
            if info:
                return info

    return None


def _extract_account_info(payload: Any) -> Optional[dict[str, Any]]:
    def normalize(item: Mapping[str, Any]) -> Optional[dict[str, Any]]:
        candidate = (
            item.get("id")
            or item.get("account_id")
            or item.get("accountId")
            or item.get("account")
        )
        account = _coerce_int(candidate)
        if account is None:
            return None
        info: dict[str, Any] = {"account_id": account}
        name_candidate = (
            item.get("login")
            or item.get("name")
            or item.get("title")
            or item.get("username")
        )
        if isinstance(name_candidate, str) and name_candidate.strip():
            info["account_login"] = name_candidate.strip()
        return info

    if isinstance(payload, Mapping):
        direct = normalize(payload)
        if direct:
            return direct
        for key in ("accounts", "result", "data"):
            arr = payload.get(key)
            if isinstance(arr, list):
                for entry in arr:
                    if isinstance(entry, Mapping):
                        info = normalize(entry)
                        if info:
                            return info
    return None


async def ensure_webhook(
    tenant: int,
    url: str,
    *,
    event_types: Sequence[str] | None = None,
) -> bool:
    token, _ = await ensure_access_token(int(tenant))
    types = list(event_types) if event_types else ["messages"]
    existing = await _list_webhooks(token)
    normalized_url = url.rstrip("/")
    for entry in existing:
        try:
            entry_url = str(entry.get("url") or "").rstrip("/")
        except Exception:
            entry_url = ""
        if entry_url == normalized_url:
            return True

    payload = {"url": url, "types": types}
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    target = "https://api.avito.ru/messenger/v3/webhook"
    async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
        response = await client.post(target, json=payload, headers=headers)

    if response.status_code == 401:
        raise AvitoOAuthError("Avito token unauthorized while creating webhook")
    if response.status_code >= 500:
        logger.warning(
            "avito_webhook_register_failed status=%s body=%s", response.status_code, response.text
        )
        return False
    if response.status_code >= 400:
        logger.info(
            "avito_webhook_register_unexpected status=%s body=%s",
            response.status_code,
            response.text,
        )
        return False
    return True


async def _list_webhooks(token: str) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    target = "https://api.avito.ru/messenger/v1/subscriptions"
    async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
        response = await client.post(target, headers=headers, json={})

    if response.status_code == 401:
        raise AvitoOAuthError("Avito token unauthorized while listing webhooks")
    if response.status_code >= 500:
        logger.warning(
            "avito_webhook_list_server_error status=%s", response.status_code
        )
        return []
    if response.status_code >= 400:
        logger.info(
            "avito_webhook_list_unexpected status=%s body=%s",
            response.status_code,
            response.text,
        )
        return []

    try:
        payload = response.json()
    except json.JSONDecodeError:
        return []

    result = []
    if isinstance(payload, Mapping):
        if isinstance(payload.get("subscriptions"), list):
            result = [
                dict(item)
                for item in payload["subscriptions"]
                if isinstance(item, Mapping)
            ]
    elif isinstance(payload, list):
        result = [dict(item) for item in payload if isinstance(item, Mapping)]
    return result
