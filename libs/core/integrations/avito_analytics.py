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
ALL_ITEM_STATUSES = "active,old,removed,blocked,rejected"

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

        error_payload = {"url": target, "params": params, "json": json}
        if response.status_code == 401:
            raise AvitoAPIError("Unauthorized", status=response.status_code, payload=error_payload)
        if response.status_code == 403:
            raise AvitoAPIError("Forbidden or scope missing", status=response.status_code, payload=error_payload)
        if response.status_code == 429:
            if attempt >= 3:
                raise AvitoAPIError("Rate limited", status=response.status_code, payload=error_payload, retryable=True)
            await asyncio.sleep(0.5 * attempt)
            continue
        if response.status_code >= 500:
            if attempt >= 3:
                raise AvitoAPIError("Avito server error", status=response.status_code, payload=error_payload, retryable=True)
            await asyncio.sleep(0.4 * attempt)
            continue
        if response.status_code >= 400:
            raise AvitoAPIError(
                f"HTTP {response.status_code}",
                status=response.status_code,
                payload=error_payload,
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


async def list_items(
    access_token: str,
    *,
    page: int = 1,
    per_page: int = 100,
    statuses: str | None = ALL_ITEM_STATUSES,
) -> Mapping[str, Any] | list[Any]:
    params: dict[str, Any] = {"page": page, "per_page": per_page}
    if statuses:
        params["status"] = statuses
    return await avito_request("GET", "/core/v1/items", access_token, params=params)


async def get_items_stats(
    access_token: str,
    user_id: int | None,
    date_from: str,
    date_to: str,
    *,
    metrics: Sequence[str] | None = None,
    grouping: str = "item",
    limit: int = 1000,
    offset: int = 0,
    filters: Mapping[str, Any] | None = None,
    item_ids: Sequence[int] | Sequence[str] | None = None,
) -> Mapping[str, Any] | list[Any]:
    if not user_id:
        raise AvitoAPIError("User id required for stats request", status=400)
    payload: dict[str, Any] = {
        "dateFrom": date_from,
        "dateTo": date_to,
        "metrics": list(metrics) if metrics else ["views", "contacts", "favorites"],
        "grouping": grouping,
        "limit": limit,
        "offset": offset,
    }
    if filters:
        payload["filter"] = dict(filters)
    target = f"/stats/v2/accounts/{user_id}/items"
    try:
        return await avito_request("POST", target, access_token, json=payload)
    except AvitoAPIError as exc:
        # fallback to v1 shallow stats for partial coverage
        if exc.status not in (400, 401, 403, 404):
            raise
        if not item_ids:
            raise
        fallback_payload = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "itemIds": list(item_ids)[:200],
        }
        return await avito_request("POST", f"/stats/v1/accounts/{user_id}/items", access_token, json=fallback_payload)


async def get_items_stats_v1(
    access_token: str,
    user_id: int,
    item_ids: Sequence[int] | Sequence[str],
    date_from: str,
    date_to: str,
) -> Mapping[str, Any] | list[Any]:
    payload: dict[str, Any] = {
        "dateFrom": date_from,
        "dateTo": date_to,
        "itemIds": list(item_ids),
    }
    target = f"/stats/v1/accounts/{user_id}/items"
    return await avito_request("POST", target, access_token, json=payload)


async def get_calls_stats(
    access_token: str,
    user_id: int | None,
    date_from: str,
    date_to: str,
    item_ids: Sequence[int] | Sequence[str] | None = None,
) -> Mapping[str, Any] | list[Any]:
    if not user_id:
        raise AvitoAPIError("User id required for calls stats", status=400)
    payload: dict[str, Any] = {"dateFrom": date_from, "dateTo": date_to}
    if item_ids:
        payload["itemIds"] = list(item_ids)
    target = f"/core/v1/accounts/{user_id}/calls/stats/"
    return await avito_request("POST", target, access_token, json=payload)


async def get_balance(access_token: str, user_id: int | None) -> Mapping[str, Any] | list[Any]:
    if not user_id:
        raise AvitoAPIError("User id required for balance", status=400)
    target = f"/core/v1/accounts/{user_id}/balance/"
    return await avito_request("GET", target, access_token)


async def get_item_info(access_token: str, account_id: int, item_id: int) -> Mapping[str, Any] | list[Any]:
    target = f"/core/v1/accounts/{int(account_id)}/items/{int(item_id)}/"
    return await avito_request("GET", target, access_token)


async def get_operations(
    access_token: str,
    date_from: str,
    date_to: str,
) -> Mapping[str, Any] | list[Any]:
    payload = {
        "dateTimeFrom": f"{date_from}T00:00:00",
        "dateTimeTo": f"{date_to}T23:59:59",
    }
    return await avito_request("POST", "/core/v1/accounts/operations_history/", access_token, json=payload)


async def messenger_list_chats(
    access_token: str,
    user_id: int | None,
    *,
    limit: int = 50,
    offset: int = 0,
    item_ids: Sequence[int] | Sequence[str] | None = None,
) -> Mapping[str, Any] | list[Any]:
    params = {"limit": limit, "offset": offset}
    if item_ids:
        params["item_ids"] = ",".join(str(item_id) for item_id in item_ids)
    targets = []
    if user_id:
        targets.append(f"/messenger/v2/accounts/{user_id}/chats")
        targets.append(f"/messenger/v3/accounts/{user_id}/chats")
    targets.append("/messenger/v3/chats")
    last_exc: AvitoAPIError | None = None
    for target in targets:
        try:
            return await avito_request("GET", target, access_token, params=params)
        except AvitoAPIError as exc:
            last_exc = exc
            if exc.status == 404:
                continue
            if exc.status not in (401, 403, 404, 429):
                raise
            if exc.status == 429:
                await asyncio.sleep(5.0)
                continue
            continue
    if last_exc:
        if last_exc.status in (404, 429):
            return []
        raise last_exc
    raise AvitoAPIError("Avito chats endpoint unavailable")


async def messenger_get_messages(
    access_token: str,
    user_id: int | None,
    chat_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> Mapping[str, Any] | list[Any]:
    params = {"limit": limit, "offset": offset}
    targets: list[str] = []
    if user_id:
        targets.append(f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/messages/")
        targets.append(f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/messages")
    else:
        targets.append(f"/messenger/v3/chats/{chat_id}/messages/")
        targets.append(f"/messenger/v3/chats/{chat_id}/messages")
    last_exc: AvitoAPIError | None = None
    for target in targets:
        try:
            return await avito_request("GET", target, access_token, params=params)
        except AvitoAPIError as exc:
            last_exc = exc
            if exc.status in {403, 404, 405}:
                continue
            raise
    if last_exc:
        raise last_exc
    raise AvitoAPIError("Avito messages endpoint unavailable")


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
    if not user_id:
        raise AvitoAPIError("User id required for VAS prices", status=400)
    target = f"/core/v1/accounts/{user_id}/vas/prices"
    return await avito_request("POST", target, access_token, json=payload or {})


async def get_vas_packages_prices(access_token: str, user_id: int | None, payload: Mapping[str, Any] | None = None) -> Mapping[str, Any] | list[Any]:
    if not user_id:
        raise AvitoAPIError("User id required for VAS packages", status=400)
    # API does not expose dedicated pricing endpoint for packages; reuse common prices catalog.
    target = f"/core/v1/accounts/{user_id}/vas/prices"
    return await avito_request("POST", target, access_token, json=payload or {})


# Optional/feature-detection endpoints
async def try_get_ratings(access_token: str, user_id: int | None) -> Mapping[str, Any] | list[Any] | None:
    candidates = []
    if user_id:
        candidates.append(f"/ratings/v1/accounts/{user_id}")
        candidates.append(f"/core/v1/accounts/{user_id}/ratings")
    candidates.append("/ratings/v1/accounts/self")
    last_exc: AvitoAPIError | None = None
    for path in candidates:
        try:
            return await avito_request("GET", path, access_token)
        except AvitoAPIError as exc:
            last_exc = exc
            if exc.status in {403, 404}:
                return None
            if exc.status == 429 and exc.retryable:
                await asyncio.sleep(0.5)
                continue
            continue
    if last_exc and last_exc.status in {403, 404}:
        return None
    return None


async def try_get_autoload_reports(access_token: str) -> Mapping[str, Any] | list[Any] | None:
    candidates = ["/autoload/v1/reports", "/autoload/v2/reports", "/core/v1/autoload/reports"]
    last_exc: AvitoAPIError | None = None
    for path in candidates:
        try:
            return await avito_request("GET", path, access_token)
        except AvitoAPIError as exc:
            last_exc = exc
            if exc.status in {403, 404}:
                return None
            if exc.status == 429 and exc.retryable:
                await asyncio.sleep(0.5)
                continue
            continue
    if last_exc and last_exc.status in {403, 404}:
        return None
    return None


async def try_get_cpx_campaigns(access_token: str, user_id: int | None) -> Mapping[str, Any] | list[Any] | None:
    candidates = []
    if user_id:
        candidates.append(f"/cpxpromo/v1/accounts/{user_id}/campaigns")
    candidates.append("/cpxpromo/v1/campaigns")
    last_exc: AvitoAPIError | None = None
    for path in candidates:
        try:
            return await avito_request("GET", path, access_token)
        except AvitoAPIError as exc:
            last_exc = exc
            if exc.status in {403, 404}:
                return None
            if exc.status == 429 and exc.retryable:
                await asyncio.sleep(0.5)
                continue
            continue
    if last_exc and last_exc.status in {403, 404}:
        return None
    return None


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
    "get_items_stats_v1",
    "get_calls_stats",
    "get_balance",
    "get_item_info",
    "get_operations",
    "messenger_list_chats",
    "messenger_get_messages",
    "job_get_applications_by_ids",
    "job_try_list_applications",
    "job_get_resume_v2",
    "job_get_vacancy_v2",
    "get_vas_prices",
    "get_vas_packages_prices",
    "try_get_ratings",
    "try_get_autoload_reports",
    "try_get_cpx_campaigns",
    "DEFAULT_SCOPES",
    "ANALYTICS_REDIRECT",
]
