"""Avito OAuth helpers and utilities."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Mapping, Optional, Tuple, Sequence
from urllib.parse import quote, urlencode

import httpx

from libs.core import sales_core as core_module
from libs.core.lib.numbers import coerce_int as _coerce_int_shared

settings = core_module.settings  # type: ignore[attr-defined]
read_tenant_config = core_module.read_tenant_config  # type: ignore[attr-defined]
write_tenant_config = core_module.write_tenant_config  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)

AUTH_URL = getattr(settings, "AVITO_AUTH_URL", "https://www.avito.ru/oauth")
TOKEN_URL = getattr(settings, "AVITO_TOKEN_URL", "https://api.avito.ru/token/")
DEFAULT_SCOPE = getattr(settings, "AVITO_SCOPE", "") or ""
DEFAULT_REDIRECT_URL = getattr(settings, "AVITO_REDIRECT_URL", "").strip()
OAUTH_TIMEOUT = getattr(settings, "AVITO_TIMEOUT", 10.0) or 10.0


class AvitoOAuthError(RuntimeError):
    """Raised when Avito OAuth flow fails."""


def build_authorize_url(
    state: str | None,
    *,
    redirect_uri: str | None = None,
    scope: str | None = None,
) -> str:
    redirect = (redirect_uri or DEFAULT_REDIRECT_URL or "").strip()
    params: dict[str, str] = {
        "response_type": "code",
        "client_id": getattr(settings, "AVITO_CLIENT_ID", "").strip(),
    }
    if state:
        params["state"] = state
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
    return _coerce_int_shared(value)


_ACCOUNT_TENANT_CACHE: dict[int, int] = {}
_CHAT_ACCOUNT_CACHE: dict[str, tuple[float, int, int]] = {}
_CHAT_ACCOUNT_TTL_SECONDS = 300
_CHAT_PROFILE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CHAT_PROFILE_TTL_SECONDS = 3600


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
    try:
        from libs.core.repo import avito_accounts
        import asyncio

        async def _find() -> Optional[int]:
            account = await avito_accounts.find_active_by_account_id(int(account_val))
            if account and account.get("tenant_id") is not None:
                return int(account["tenant_id"])
            return None

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:
            found = asyncio.run(_find())
            if found is not None:
                _cache_account_mapping(found, account_val)
                return found
    except Exception:
        pass

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


def _coerce_chat_profile_avatar(user: Mapping[str, Any]) -> str:
    profile = user.get("public_user_profile")
    if not isinstance(profile, Mapping):
        return ""
    avatar = profile.get("avatar")
    if not isinstance(avatar, Mapping):
        return ""
    images = avatar.get("images")
    if isinstance(images, Mapping):
        for key in ("128x128", "96x96", "96x64", "72x72", "64x64", "48x48", "36x36", "24x24"):
            value = images.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in images.values():
            if isinstance(value, str) and value.strip():
                return value.strip()
    value = avatar.get("default")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return ""


def _extract_chat_participant_profile(
    payload: Mapping[str, Any],
    *,
    author_id: int | None,
    account_id: int | None,
) -> dict[str, Any]:
    users = payload.get("users")
    if not isinstance(users, list):
        return {}

    def user_id_value(user: Mapping[str, Any]) -> int | None:
        return _coerce_int(
            user.get("id")
            or user.get("user_id")
            or (user.get("public_user_profile") or {}).get("user_id")
        )

    def choose_user() -> Mapping[str, Any] | None:
        if author_id is not None:
            for user in users:
                if isinstance(user, Mapping) and user_id_value(user) == author_id:
                    return user
        for user in users:
            if not isinstance(user, Mapping):
                continue
            uid = user_id_value(user)
            if account_id is not None and uid == account_id:
                continue
            return user
        return None

    user = choose_user()
    if not isinstance(user, Mapping):
        return {}
    profile = user.get("public_user_profile")
    if not isinstance(profile, Mapping):
        profile = {}
    name = str(user.get("name") or user.get("username") or user.get("login") or "").strip()
    avatar = _coerce_chat_profile_avatar(user)
    profile_url = str(profile.get("url") or "").strip()
    return {
        "user_id": user_id_value(user),
        "name": name,
        "avatar": avatar,
        "profile_url": profile_url,
    }


async def resolve_chat_participant_profile(
    tenant: int,
    *,
    account_id: int | None,
    chat_id: str,
    author_id: int | None = None,
) -> dict[str, Any]:
    chat_text = str(chat_id or "").strip()
    if not chat_text:
        return {}
    account_val = _coerce_int(account_id)
    cache_key = f"{int(tenant)}:{account_val or 0}:{chat_text}:{author_id or 0}"
    now = time.time()
    cached = _CHAT_PROFILE_CACHE.get(cache_key)
    if cached and now - cached[0] <= _CHAT_PROFILE_TTL_SECONDS:
        return dict(cached[1])

    try:
        if account_val is not None:
            token, integration = await ensure_access_token_for_account(int(tenant), int(account_val))
        else:
            token, integration = await ensure_access_token(int(tenant))
    except Exception:
        logger.exception("avito_chat_profile_token_failed tenant=%s account_id=%s", tenant, account_val)
        return {}

    if account_val is None:
        account_val = _coerce_int(integration.get("account_id"))
    if account_val is None:
        return {}

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
        direct_url = f"https://api.avito.ru/messenger/v2/accounts/{account_val}/chats/{quote(chat_text, safe='')}"
        try:
            response = await client.get(direct_url, headers=headers)
        except httpx.HTTPError:
            response = None
        if response is not None and response.status_code == 200:
            try:
                payload = response.json()
            except Exception:
                payload = {}
            info = _extract_chat_participant_profile(
                payload, author_id=author_id, account_id=account_val
            )
            if info:
                _CHAT_PROFILE_CACHE[cache_key] = (now, dict(info))
                return dict(info)

        for offset in (0, 100, 200, 300, 400):
            list_url = f"https://api.avito.ru/messenger/v2/accounts/{account_val}/chats?limit=100&offset={offset}"
            try:
                response = await client.get(list_url, headers=headers)
            except httpx.HTTPError:
                continue
            if response.status_code != 200:
                continue
            try:
                payload = response.json()
            except Exception:
                continue
            chats = payload.get("chats")
            if not isinstance(chats, list):
                continue
            found = False
            for item in chats:
                if not isinstance(item, Mapping):
                    continue
                if str(item.get("id") or "").strip() != chat_text:
                    continue
                found = True
                info = _extract_chat_participant_profile(
                    item, author_id=author_id, account_id=account_val
                )
                if info:
                    _CHAT_PROFILE_CACHE[cache_key] = (now, dict(info))
                    return dict(info)
            if len(chats) < 100 and not found:
                break
    return {}


async def _chat_exists(token: str, account_id: int, chat_id: str) -> bool:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    urls = [
        f"https://api.avito.ru/messenger/v1/accounts/{account_id}/chats/{chat_id}",
        f"https://api.avito.ru/messenger/v3/accounts/{account_id}/chats/{chat_id}",
    ]
    async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
        for url in urls:
            try:
                response = await client.get(url, headers=headers)
            except httpx.HTTPError as exc:
                logger.warning(
                    "avito_chat_lookup_failed account_id=%s chat_id=%s url=%s error=%s",
                    account_id,
                    chat_id,
                    url,
                    exc,
                )
                continue
            if response.status_code == 200:
                return True
            if response.status_code in (403, 404):
                continue
            if response.status_code == 401:
                raise AvitoOAuthError("Avito token unauthorized while resolving chat")
            logger.info(
                "avito_chat_lookup_unexpected account_id=%s chat_id=%s url=%s status=%s",
                account_id,
                chat_id,
                url,
                response.status_code,
            )
    return False


async def resolve_tenant_by_chat(chat_id: str) -> tuple[Optional[int], Optional[int]]:
    chat_key = str(chat_id or "").strip()
    if not chat_key:
        return None, None
    now = time.time()
    cached = _CHAT_ACCOUNT_CACHE.get(chat_key)
    if cached and now - cached[0] <= _CHAT_ACCOUNT_TTL_SECONDS:
        return cached[1], cached[2]

    try:
        from libs.core.repo import avito_accounts

        tenants_seen: set[tuple[int, int]] = set()
        tenants_root = getattr(core_module, "TENANTS_DIR", None)
        rows: list[dict[str, Any]] = []
        if tenants_root is not None:
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
                try:
                    rows.extend(await avito_accounts.list_accounts(tenant_id))
                except Exception:
                    continue
        for row in rows:
            tenant_id = _coerce_int(row.get("tenant_id"))
            account_id = _coerce_int(row.get("account_id"))
            if tenant_id is None or account_id is None or (tenant_id, account_id) in tenants_seen:
                continue
            tenants_seen.add((tenant_id, account_id))
            try:
                token, _ = await ensure_access_token_for_account(tenant_id, account_id)
                if await _chat_exists(token, account_id, chat_key):
                    _CHAT_ACCOUNT_CACHE[chat_key] = (now, int(tenant_id), int(account_id))
                    _cache_account_mapping(int(tenant_id), int(account_id))
                    return int(tenant_id), int(account_id)
            except Exception:
                continue
    except Exception:
        pass

    tenants_root = getattr(core_module, "TENANTS_DIR", None)
    if tenants_root is None:
        return None, None
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
        account_id = _coerce_int(integration.get("account_id"))
        if account_id is None:
            continue
        try:
            token, _ = await ensure_access_token(tenant_id)
        except AvitoOAuthError:
            continue
        except Exception:
            logger.exception("avito_chat_lookup_token_failed tenant=%s", tenant_id)
            continue
        try:
            if await _chat_exists(token, account_id, chat_key):
                _CHAT_ACCOUNT_CACHE[chat_key] = (now, int(tenant_id), int(account_id))
                _cache_account_mapping(int(tenant_id), int(account_id))
                return int(tenant_id), int(account_id)
        except AvitoOAuthError:
            continue
        except Exception:
            logger.exception(
                "avito_chat_lookup_failed tenant=%s account_id=%s chat_id=%s",
                tenant_id,
                account_id,
                chat_key,
            )
            continue

    return None, None


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

    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        logger.warning("Avito token refresh returned no access_token tenant=%s", tenant)
        raise AvitoOAuthError("Avito token refresh returned no access token")

    merged = dict(integration)
    merged.update(payload)
    expires_in = _coerce_int(payload.get("expires_in"))
    now = int(time.time())
    if expires_in and expires_in > 0:
        merged["expires_at"] = now + int(expires_in)
    merged["obtained_at"] = now
    stored = update_integration(tenant, merged)
    return stored


async def _refresh_access_token_legacy(tenant: int) -> dict[str, Any]:
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


async def _ensure_access_token_legacy(tenant: int) -> Tuple[str, dict[str, Any]]:
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


async def refresh_access_token(tenant: int) -> dict[str, Any]:
    from libs.core.services import avito_account_tokens
    from libs.core.repo import avito_accounts

    account = await avito_accounts.get_primary_account(int(tenant))
    if account and account.get("account_id") is not None:
        return await avito_account_tokens.refresh_access_token_for_account(
            int(tenant),
            int(account["account_id"]),
        )
    return await _refresh_access_token_legacy(int(tenant))


async def ensure_access_token(tenant: int) -> Tuple[str, dict[str, Any]]:
    from libs.core.services import avito_account_tokens

    return await avito_account_tokens.ensure_primary_access_token(int(tenant))


async def ensure_access_token_for_account(
    tenant: int,
    account_id: int,
) -> Tuple[str, dict[str, Any]]:
    from libs.core.services import avito_account_tokens

    return await avito_account_tokens.ensure_access_token_for_account(int(tenant), int(account_id))


async def refresh_access_token_for_account(tenant: int, account_id: int) -> dict[str, Any]:
    from libs.core.services import avito_account_tokens

    return await avito_account_tokens.refresh_access_token_for_account(int(tenant), int(account_id))


async def get_account_integration(tenant: int, account_id: int) -> Optional[dict[str, Any]]:
    from libs.core.repo import avito_accounts

    return await avito_accounts.get_account(int(tenant), int(account_id))


async def list_accounts(tenant: int, *, include_disconnected: bool = False) -> list[dict[str, Any]]:
    from libs.core.repo import avito_accounts

    accounts = await avito_accounts.list_accounts(
        int(tenant),
        include_disconnected=include_disconnected,
    )
    if accounts:
        return accounts
    legacy = get_integration(int(tenant)) or {}
    account_id = _coerce_int(legacy.get("account_id"))
    if account_id is None or not (legacy.get("access_token") or legacy.get("refresh_token")):
        return []
    account = await avito_accounts.upsert_account_tokens(
        int(tenant),
        int(account_id),
        legacy,
        account_login=str(legacy.get("account_login") or "") or None,
        is_primary=True,
    )
    return [dict(account)] if account else []


async def get_primary_account(tenant: int) -> Optional[dict[str, Any]]:
    from libs.core.repo import avito_accounts

    account = await avito_accounts.get_primary_account(int(tenant))
    if account:
        return account
    accounts = await list_accounts(int(tenant))
    return accounts[0] if accounts else None


async def upsert_oauth_account_from_payload(
    tenant: int,
    token_payload: Mapping[str, Any],
) -> dict[str, Any]:
    from libs.core.services import avito_account_tokens

    return await avito_account_tokens.upsert_oauth_account_from_payload(int(tenant), token_payload)


async def set_primary_account(tenant: int, account_id: int) -> Optional[dict[str, Any]]:
    from libs.core.repo import avito_accounts

    account = await avito_accounts.set_primary_account(int(tenant), int(account_id))
    if account:
        avito_accounts.sync_primary_mirror_to_tenant_config(int(tenant), account)
    return account


async def update_account_display_name(
    tenant: int,
    account_id: int,
    display_name: str | None,
) -> Optional[dict[str, Any]]:
    from libs.core.repo import avito_accounts

    return await avito_accounts.update_account_display_name(
        int(tenant),
        int(account_id),
        display_name,
    )


async def disconnect_account(tenant: int, account_id: int) -> Optional[dict[str, Any]]:
    from libs.core.repo import avito_accounts

    account = await avito_accounts.disconnect_account(int(tenant), int(account_id))
    primary = await avito_accounts.get_primary_account(int(tenant))
    avito_accounts.sync_primary_mirror_to_tenant_config(int(tenant), primary)
    return account


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
    account_id = _coerce_int(updated.get("account_id"))
    if account_id is not None:
        try:
            from libs.core.repo import avito_accounts

            account = await avito_accounts.upsert_account_tokens(
                int(tenant),
                int(account_id),
                updated,
                account_login=str(updated.get("account_login") or "") or None,
                is_primary=True,
            )
            if account:
                return dict(account)
        except Exception:
            logger.debug("avito_account_row_sync_failed tenant=%s", tenant, exc_info=True)
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
            item.get("id") or item.get("account_id") or item.get("accountId") or item.get("account")
        )
        account = _coerce_int(candidate)
        if account is None:
            return None
        info: dict[str, Any] = {"account_id": account}
        name_candidate = (
            item.get("login") or item.get("name") or item.get("title") or item.get("username")
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
    account_id: int | None = None,
    event_types: Sequence[str] | None = None,
) -> bool:
    if account_id is not None:
        token, _ = await ensure_access_token_for_account(int(tenant), int(account_id))
    else:
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
        logger.warning("avito_webhook_list_server_error status=%s", response.status_code)
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
            result = [dict(item) for item in payload["subscriptions"] if isinstance(item, Mapping)]
    elif isinstance(payload, list):
        result = [dict(item) for item in payload if isinstance(item, Mapping)]
    return result


async def delete_webhook(tenant: int, url: str, *, account_id: int | None = None) -> bool:
    url_value = str(url or "").strip()
    if not url_value:
        return False
    if account_id is not None:
        token, _ = await ensure_access_token_for_account(int(tenant), int(account_id))
    else:
        token, _ = await ensure_access_token(int(tenant))
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    normalized = url_value.rstrip("/")
    try:
        existing = await _list_webhooks(token)
    except AvitoOAuthError:
        raise
    except Exception:
        logger.exception("avito_webhook_list_failed tenant=%s", tenant)
        existing = []
    if existing:
        found = False
        for entry in existing:
            try:
                entry_url = str(entry.get("url") or "").rstrip("/")
            except Exception:
                entry_url = ""
            if entry_url == normalized:
                found = True
                break
        if not found:
            return True

    target = "https://api.avito.ru/messenger/v1/webhook/unsubscribe"
    async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
        response = await client.post(target, json={"url": url_value}, headers=headers)

    if response.status_code in (200, 204):
        return True
    if response.status_code == 401:
        raise AvitoOAuthError("Avito token unauthorized while deleting webhook")
    if response.status_code >= 500:
        logger.warning(
            "avito_webhook_delete_failed status=%s body=%s", response.status_code, response.text
        )
        return False
    logger.info(
        "avito_webhook_delete_unexpected status=%s body=%s",
        response.status_code,
        response.text,
    )
    return False


async def get_voice_files(
    tenant: int,
    voice_ids: Sequence[str],
    *,
    account_id: int | None = None,
) -> dict[str, str]:
    """Resolve temporary download URLs for Avito voice messages."""

    ids = [str(item or "").strip() for item in voice_ids if str(item or "").strip()]
    if not ids:
        return {}

    if account_id is not None:
        token, integration = await ensure_access_token_for_account(int(tenant), int(account_id))
    else:
        token, integration = await ensure_access_token(int(tenant))
    account_val = _coerce_int(account_id if account_id is not None else integration.get("account_id"))
    if account_val is None:
        raise AvitoOAuthError("Avito account id is missing for getVoiceFiles")

    async def _request(current_token: str) -> httpx.Response:
        headers = {
            "Authorization": f"Bearer {current_token}",
            "Accept": "application/json",
        }
        params = [("voice_ids", item) for item in ids]
        url = f"https://api.avito.ru/messenger/v1/accounts/{account_val}/getVoiceFiles"
        async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT) as client:
            return await client.get(url, headers=headers, params=params)

    response = await _request(token)
    if response.status_code == 401 and integration.get("refresh_token"):
        if account_val is not None:
            refreshed = await refresh_access_token_for_account(int(tenant), int(account_val))
        else:
            refreshed = await refresh_access_token(int(tenant))
        refreshed_token = str(refreshed.get("access_token") or "").strip()
        if refreshed_token:
            response = await _request(refreshed_token)

    if response.status_code >= 400:
        logger.info(
            "avito_get_voice_files_failed tenant=%s account_id=%s status=%s body=%s",
            tenant,
            account_val,
            response.status_code,
            response.text,
        )
        return {}

    try:
        payload = response.json()
    except Exception:
        return {}

    raw_urls = payload.get("voices_urls") if isinstance(payload, Mapping) else None
    if not isinstance(raw_urls, Mapping):
        return {}

    result: dict[str, str] = {}
    for key, value in raw_urls.items():
        voice_key = str(key or "").strip()
        voice_url = str(value or "").strip()
        if voice_key and voice_url:
            result[voice_key] = voice_url
    return result


async def resolve_voice_url(
    tenant: int,
    voice_id: str,
    *,
    account_id: int | None = None,
) -> str:
    """Return a single temporary URL for Avito voice_id."""

    voice_key = str(voice_id or "").strip()
    if not voice_key:
        return ""
    mapping = await get_voice_files(int(tenant), [voice_key], account_id=account_id)
    if not mapping:
        return ""
    direct = str(mapping.get(voice_key) or "").strip()
    if direct:
        return direct
    for candidate in mapping.values():
        value = str(candidate or "").strip()
        if value:
            return value
    return ""
