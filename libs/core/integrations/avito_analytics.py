from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Mapping, Sequence
from urllib.parse import urlencode

import httpx

from libs.core import sales_core as core_module
from libs.core.models.avito_analytics import AvitoAnalyticsToken
from libs.core.repo import avito_analytics_tokens as tokens_repo

settings = core_module.settings  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)

AUTH_URL = getattr(settings, "AVITO_AUTH_URL", "https://www.avito.ru/oauth")
TOKEN_URL = getattr(settings, "AVITO_TOKEN_URL", "https://api.avito.ru/token/")
API_BASE = getattr(settings, "AVITO_API_BASE", "https://api.avito.ru").rstrip("/")
DEFAULT_TIMEOUT = getattr(settings, "AVITO_TIMEOUT", 10.0) or 10.0

ANALYTICS_REDIRECT = (
    os.getenv("AVITO_ANALYTICS_REDIRECT_URI")
    or getattr(settings, "AVITO_REDIRECT_URL", "")
).strip()
DEFAULT_SCOPES = (
    os.getenv(
        "AVITO_ANALYTICS_SCOPES",
        "autoload:reports,items:apply_vas,items:info,job:applications,job:cv,"
        "messenger:read,messenger:write,ratings:read,short_term_rent:read,stats:read,"
        "trx:commission,user:read,user_balance:read,user_operations:read,cpxpromo:read",
    )
    .strip()
)


class AvitoOAuthError(RuntimeError):
    """Raised when Avito OAuth flow fails."""


class AvitoAPIError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, payload: Any = None, retryable: bool = False):
        super().__init__(message)
        self.status = status
        self.payload = payload
        self.retryable = retryable


def build_authorize_url(state: str, *, scope: str | None = None, redirect_uri: str | None = None) -> str:
    redirect = (redirect_uri or ANALYTICS_REDIRECT or "").strip()
    params: dict[str, str] = {
        "response_type": "code",
        "client_id": getattr(settings, "AVITO_CLIENT_ID", "").strip(),
        "state": state,
    }
    if redirect:
        params["redirect_uri"] = redirect
    scope_value = (scope or DEFAULT_SCOPES).strip()
    if scope_value:
        params["scope"] = scope_value
    return f"{AUTH_URL}?{urlencode(params)}"


async def exchange_code_for_token(code: str, *, redirect_uri: str | None = None) -> Mapping[str, Any]:
    client_id = getattr(settings, "AVITO_CLIENT_ID", "").strip()
    client_secret = getattr(settings, "AVITO_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise AvitoOAuthError("Avito client credentials are not configured")

    redirect = (redirect_uri or ANALYTICS_REDIRECT or "").strip()
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if redirect:
        data["redirect_uri"] = redirect

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        response = await client.post(TOKEN_URL, data=data, headers=headers)

    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        logger.warning("avito_analytics_token_exchange_failed status=%s detail=%s", response.status_code, detail)
        raise AvitoOAuthError(f"Avito token exchange failed: HTTP {response.status_code}")

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:  # pragma: no cover
        logger.warning("avito_analytics_token_decode_failed error=%s", exc)
        raise AvitoOAuthError("Failed to decode Avito token response") from exc
    return payload


async def refresh_access_token(refresh_token: str) -> Mapping[str, Any]:
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
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        response = await client.post(TOKEN_URL, data=data, headers=headers)
    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        logger.warning("avito_analytics_token_refresh_failed status=%s detail=%s", response.status_code, detail)
        raise AvitoOAuthError(f"Avito token refresh failed: HTTP {response.status_code}")
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:  # pragma: no cover
        logger.warning("avito_analytics_token_refresh_decode_failed error=%s", exc)
        raise AvitoOAuthError("Failed to decode Avito refresh response") from exc
    return payload


async def ensure_access_token(account_id: int) -> tuple[str, AvitoAnalyticsToken]:
    token_entry = await tokens_repo.get(int(account_id))
    if not token_entry or not token_entry.refresh_token:
        raise AvitoOAuthError("Avito account is not authorized")
    token_value = (token_entry.access_token or "").strip()
    now_ts = time.time()
    expires_ts = token_entry.expires_at.timestamp() if token_entry.expires_at else None
    needs_refresh = not token_value or (expires_ts is not None and expires_ts - 30 <= now_ts)
    if needs_refresh:
        refreshed = await refresh_access_token(token_entry.refresh_token)
        access_token = str(refreshed.get("access_token") or "").strip()
        refresh_token = str(refreshed.get("refresh_token") or token_entry.refresh_token or "").strip()
        expires_in = refreshed.get("expires_in")
        expires_at = None
        obtained_at = datetime.now(tz=timezone.utc)
        if expires_in:
            try:
                expires_at = obtained_at + timedelta(seconds=int(expires_in))
            except Exception:
                expires_at = None
        token_type = refreshed.get("token_type") or token_entry.token_type
        try:
            token_entry = await tokens_repo.update_tokens(
                int(account_id),
                access_token=access_token or None,
                refresh_token=refresh_token or None,
                expires_at=expires_at,
                obtained_at=obtained_at,
                token_type=token_type,
            ) or token_entry
        except Exception as exc:
            raise AvitoOAuthError("Failed to persist refreshed token") from exc
        token_value = access_token
    if not token_value:
        raise AvitoOAuthError("Avito access token unavailable")
    return token_value, token_entry


async def avito_request(
    method: str,
    path: str,
    token: str,
    *,
    params: Mapping[str, Any] | None = None,
    json: Any = None,
    headers: Mapping[str, str] | None = None,
    timeout: float | None = None,
) -> Mapping[str, Any] | list[Any]:
    target = path
    if not path.startswith("http"):
        target = f"{API_BASE}/{path.lstrip('/')}"
    merged_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if headers:
        merged_headers.update(headers)
    attempt = 0
    last_error: Exception | None = None
    while attempt < 3:
        attempt += 1
        try:
            async with httpx.AsyncClient(timeout=timeout or DEFAULT_TIMEOUT) as client:
                response = await client.request(method.upper(), target, params=params, json=json, headers=merged_headers)
        except httpx.TimeoutException as exc:
            last_error = exc
            if attempt >= 3:
                raise AvitoAPIError("Avito request timeout", payload={"path": path}) from exc
            await asyncio.sleep(0.3 * attempt)
            continue
        except httpx.HTTPError as exc:
            last_error = exc
            if attempt >= 2:
                raise AvitoAPIError("Avito network error", payload={"path": path}) from exc
            await asyncio.sleep(0.2)
            continue

        if response.status_code == 401:
            raise AvitoAPIError("Unauthorized", status=response.status_code, payload=response.text)
        if response.status_code == 403:
            raise AvitoAPIError("Forbidden or scope missing", status=response.status_code, payload=response.text)
        if response.status_code == 429:
            if attempt >= 3:
                raise AvitoAPIError("Rate limited", status=response.status_code, payload=response.text, retryable=True)
            await asyncio.sleep(0.5 * attempt)
            continue
        if response.status_code >= 500:
            if attempt >= 3:
                raise AvitoAPIError("Avito server error", status=response.status_code, payload=response.text, retryable=True)
            await asyncio.sleep(0.4 * attempt)
            continue
        if response.status_code >= 400:
            raise AvitoAPIError(
                f"HTTP {response.status_code}",
                status=response.status_code,
                payload=response.text,
            )
        try:
            return response.json()
        except json.JSONDecodeError:
            return {"raw": response.text}

    if last_error:
        raise AvitoAPIError("Avito request failed", payload=str(last_error))
    raise AvitoAPIError("Avito request failed for unknown reasons")


# High-level API helpers (best-effort: if endpoint is missing, caller should handle exceptions)
async def get_user_me(access_token: str) -> Mapping[str, Any] | list[Any]:
    # Try multiple candidate endpoints
    candidates = [
        "/core/v1/accounts/self",
        "/api/v1/accounts/self",
        "/messenger/v1/accounts",
    ]
    last_exc: Exception | None = None
    for path in candidates:
        try:
            payload = await avito_request("GET", path, access_token)
            return payload
        except AvitoAPIError as exc:
            last_exc = exc
            continue
    if last_exc:
        raise last_exc
    return {}


async def list_items(access_token: str, *, page: int = 1, limit: int = 100) -> Mapping[str, Any] | list[Any]:
    params = {"page": page, "limit": limit}
    return await avito_request("GET", "/core/v1/items", access_token, params=params)


async def get_items_stats(
    access_token: str,
    user_id: int | None,
    item_ids: Sequence[int] | Sequence[str] | None,
    date_from: str,
    date_to: str,
    fields: Sequence[str] | None = None,
) -> Mapping[str, Any] | list[Any]:
    payload: dict[str, Any] = {
        "dateFrom": date_from,
        "dateTo": date_to,
    }
    if item_ids:
        payload["itemIds"] = list(item_ids)
    if fields:
        payload["fields"] = list(fields)
    target = f"/stats/v1/accounts/{user_id}/items" if user_id else "/stats/v1/items"
    try:
        return await avito_request("POST", target, access_token, json=payload)
    except AvitoAPIError:
        # Try GET fallback
        params = dict(payload)
        return await avito_request("GET", target, access_token, params=params)


async def get_calls_stats(
    access_token: str,
    user_id: int | None,
    date_from: str,
    date_to: str,
) -> Mapping[str, Any] | list[Any]:
    target = f"/stats/v1/accounts/{user_id}/calls" if user_id else "/stats/v1/calls"
    params = {"dateFrom": date_from, "dateTo": date_to}
    return await avito_request("GET", target, access_token, params=params)


async def get_balance(access_token: str, user_id: int | None) -> Mapping[str, Any] | list[Any]:
    target = f"/core/v1/accounts/{user_id}/balance" if user_id else "/core/v1/accounts/self/balance"
    return await avito_request("GET", target, access_token)


async def get_operations(
    access_token: str,
    user_id: int | None,
    date_from: str,
    date_to: str,
    *,
    limit: int = 200,
    offset: int = 0,
) -> Mapping[str, Any] | list[Any]:
    target = f"/core/v1/accounts/{user_id}/operations" if user_id else "/core/v1/accounts/self/operations"
    params = {"dateFrom": date_from, "dateTo": date_to, "limit": limit, "offset": offset}
    return await avito_request("GET", target, access_token, params=params)


async def messenger_list_chats(
    access_token: str,
    user_id: int | None,
    *,
    limit: int = 50,
    offset: int = 0,
) -> Mapping[str, Any] | list[Any]:
    target = f"/messenger/v3/accounts/{user_id}/chats" if user_id else "/messenger/v3/chats"
    params = {"limit": limit, "offset": offset}
    return await avito_request("GET", target, access_token, params=params)


async def messenger_get_messages(
    access_token: str,
    user_id: int | None,
    chat_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> Mapping[str, Any] | list[Any]:
    target = f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/messages" if user_id else f"/messenger/v3/chats/{chat_id}/messages"
    params = {"limit": limit, "offset": offset}
    return await avito_request("GET", target, access_token, params=params)


# Job applications
async def job_get_applications_by_ids(access_token: str, ids: Sequence[str]) -> Mapping[str, Any] | list[Any]:
    payload = {"ids": list(ids)}
    return await avito_request("POST", "/job/v1/applications/get_by_ids", access_token, json=payload)


async def job_try_list_applications(access_token: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any] | list[Any] | None:
    params = params or {}
    candidates = [
        ("GET", "/job/v1/applications"),
        ("POST", "/job/v1/applications/search"),
        ("GET", "/job/v2/applications"),
        ("POST", "/job/v2/applications/search"),
    ]
    for method, path in candidates:
        try:
            return await avito_request(method, path, access_token, params=params if method == "GET" else None, json=params if method == "POST" else None)
        except AvitoAPIError as exc:
            if exc.status == 401:
                raise
            if exc.status in {403, 404}:
                return None
            if exc.status == 429 and exc.retryable:
                await asyncio.sleep(0.4)
                continue
            continue
        except Exception:
            continue
    return None


async def job_get_resume_v2(access_token: str, resume_id: str) -> Mapping[str, Any] | list[Any] | None:
    try:
        return await avito_request("GET", f"/job/v2/resumes/{resume_id}", access_token)
    except AvitoAPIError as exc:
        if exc.status in {401, 403, 404}:
            return None
        raise


async def job_get_vacancy_v2(access_token: str, vacancy_id: str) -> Mapping[str, Any] | list[Any] | None:
    try:
        return await avito_request("GET", f"/job/v2/vacancies/{vacancy_id}", access_token)
    except AvitoAPIError as exc:
        if exc.status in {401, 403, 404}:
            return None
        raise


# VAS pricing
async def get_vas_prices(access_token: str, user_id: int | None, payload: Mapping[str, Any] | None = None) -> Mapping[str, Any] | list[Any]:
    target = f"/core/v1/accounts/{user_id}/price/vas" if user_id else "/core/v1/accounts/self/price/vas"
    return await avito_request("POST", target, access_token, json=payload or {})


async def get_vas_packages_prices(access_token: str, user_id: int | None, payload: Mapping[str, Any] | None = None) -> Mapping[str, Any] | list[Any]:
    target = f"/core/v1/accounts/{user_id}/price/vas_packages" if user_id else "/core/v1/accounts/self/price/vas_packages"
    return await avito_request("POST", target, access_token, json=payload or {})


__all__ = [
    "AvitoOAuthError",
    "AvitoAPIError",
    "build_authorize_url",
    "exchange_code_for_token",
    "refresh_access_token",
    "ensure_access_token",
    "avito_request",
    "get_user_me",
    "list_items",
    "get_items_stats",
    "get_calls_stats",
    "get_balance",
    "get_operations",
    "messenger_list_chats",
    "messenger_get_messages",
    "job_get_applications_by_ids",
    "job_try_list_applications",
    "job_get_resume_v2",
    "job_get_vacancy_v2",
    "get_vas_prices",
    "get_vas_packages_prices",
    "DEFAULT_SCOPES",
    "ANALYTICS_REDIRECT",
]
