from __future__ import annotations

import json
import logging
import time
from typing import Any, Mapping

import httpx

from libs.core.integrations import avito
from libs.core.repo import avito_accounts

logger = logging.getLogger(__name__)


def _coerce_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return int(text)
    except Exception:
        return None


def _access_token_valid(account: Mapping[str, Any]) -> bool:
    token = str(account.get("access_token") or "").strip()
    if not token:
        return False
    expires_at = _coerce_int(account.get("expires_at"))
    if expires_at is None:
        return True
    return int(expires_at) - 30 > int(time.time())


async def sync_account_info_for_token(token: str) -> dict[str, Any]:
    info = await avito._fetch_account_info(str(token or "").strip())  # type: ignore[attr-defined]
    return dict(info or {})


async def upsert_oauth_account_from_payload(
    tenant_id: int,
    token_payload: Mapping[str, Any],
) -> dict[str, Any]:
    token_value = str(token_payload.get("access_token") or "").strip()
    if not token_value:
        raise avito.AvitoOAuthError("Avito access token is missing")
    info = await sync_account_info_for_token(token_value)
    account_id = _coerce_int(info.get("account_id"))
    if account_id is None:
        raise avito.AvitoOAuthError("Avito account id is missing")
    account_login = str(info.get("account_login") or "").strip() or None
    account = await avito_accounts.upsert_account_tokens(
        int(tenant_id),
        int(account_id),
        token_payload,
        account_login=account_login,
        is_primary=None,
    )
    if not account:
        raise avito.AvitoOAuthError("Avito account token store failed")
    if bool(account.get("is_primary")):
        avito_accounts.sync_primary_mirror_to_tenant_config(int(tenant_id), account)
    return account


async def ensure_primary_access_token(tenant_id: int) -> tuple[str, dict[str, Any]]:
    account = await avito_accounts.get_primary_account(int(tenant_id))
    if account:
        return await ensure_access_token_for_account(int(tenant_id), int(account["account_id"]))
    legacy = avito.get_integration(int(tenant_id)) or {}
    account_id = _coerce_int(legacy.get("account_id"))
    if account_id is not None and (legacy.get("access_token") or legacy.get("refresh_token")):
        account = await avito_accounts.upsert_account_tokens(
            int(tenant_id),
            int(account_id),
            legacy,
            account_login=str(legacy.get("account_login") or "") or None,
            is_primary=True,
        )
        if account:
            return await ensure_access_token_for_account(int(tenant_id), int(account_id))
    token, integration = await avito._ensure_access_token_legacy(int(tenant_id))  # type: ignore[attr-defined]
    account_id = _coerce_int(integration.get("account_id"))
    if account_id is not None:
        account = await avito_accounts.upsert_account_tokens(
            int(tenant_id),
            int(account_id),
            integration,
            account_login=str(integration.get("account_login") or "") or None,
            is_primary=True,
        )
        if account:
            avito_accounts.sync_primary_mirror_to_tenant_config(int(tenant_id), account)
            integration = dict(account)
    return str(token), dict(integration)


async def ensure_access_token_for_account(
    tenant_id: int,
    account_id: int,
) -> tuple[str, dict[str, Any]]:
    account = await avito_accounts.get_account(int(tenant_id), int(account_id))
    if not account or str(account.get("status") or "") != "active":
        raise avito.AvitoOAuthError("Avito account is not connected for tenant")
    if _access_token_valid(account):
        return str(account["access_token"]), dict(account)
    account = await refresh_access_token_for_account(int(tenant_id), int(account_id))
    token = str(account.get("access_token") or "").strip()
    if not token:
        raise avito.AvitoOAuthError("Failed to obtain Avito access token")
    return token, dict(account)


async def refresh_access_token_for_account(tenant_id: int, account_id: int) -> dict[str, Any]:
    account = await avito_accounts.get_account(int(tenant_id), int(account_id))
    if not account:
        raise avito.AvitoOAuthError("Avito account is not connected for tenant")
    refresh_token = str(account.get("refresh_token") or "").strip()
    if not refresh_token:
        raise avito.AvitoOAuthError("Avito refresh token is missing")
    client_id = getattr(avito.settings, "AVITO_CLIENT_ID", "").strip()
    client_secret = getattr(avito.settings, "AVITO_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise avito.AvitoOAuthError("Avito client credentials are not configured")
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    async with httpx.AsyncClient(timeout=avito.OAUTH_TIMEOUT) as client:
        response = await client.post(avito.TOKEN_URL, data=data, headers=headers)
    if response.status_code >= 400:
        raise avito.AvitoOAuthError(f"Avito token refresh failed: HTTP {response.status_code}")
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise avito.AvitoOAuthError("Failed to decode Avito token response") from exc
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise avito.AvitoOAuthError("Avito token refresh returned no access token")
    merged = dict(account)
    merged.update(payload)
    expires_in = _coerce_int(payload.get("expires_in"))
    now = int(time.time())
    if expires_in and expires_in > 0:
        merged["expires_at"] = now + int(expires_in)
    merged["obtained_at"] = now
    updated = await avito_accounts.refresh_account_tokens(int(tenant_id), int(account_id), merged)
    if not updated:
        raise avito.AvitoOAuthError("Avito account token refresh store failed")
    if bool(updated.get("is_primary")):
        avito_accounts.sync_primary_mirror_to_tenant_config(int(tenant_id), updated)
    return dict(updated)


__all__ = [
    "ensure_access_token_for_account",
    "ensure_primary_access_token",
    "refresh_access_token_for_account",
    "sync_account_info_for_token",
    "upsert_oauth_account_from_payload",
]
