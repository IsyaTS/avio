from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from libs.core.integrations import avito_analytics as avito_api
from libs.core.models.avito_analytics import AvitoAnalyticsToken
from libs.core.repo import avito_analytics_tokens as tokens_repo
from libs.core.sales_core import settings

logger = logging.getLogger(__name__)

_CACHE_TTL = int(os.getenv("AVITO_ANALYTICS_CACHE_TTL", "900") or "900")


class AvitoAnalyticsError(RuntimeError):
    pass


def _cache_key(account_id: int, period_days: int) -> str:
    return f"avito:analytics:report:{int(account_id)}:{int(period_days)}"


async def _get_cache(account_id: int, period_days: int) -> dict[str, Any] | None:
    redis_conn = getattr(settings, "r", None)
    if not redis_conn:
        return None
    try:
        cached = await redis_conn.get(_cache_key(account_id, period_days))
    except Exception:
        logger.debug("avito_analytics_cache_get_failed", exc_info=True)
        return None
    if not cached:
        return None
    try:
        return json.loads(cached)
    except Exception:
        return None


async def _set_cache(account_id: int, period_days: int, payload: Mapping[str, Any]) -> None:
    redis_conn = getattr(settings, "r", None)
    if not redis_conn:
        return
    try:
        await redis_conn.set(_cache_key(account_id, period_days), json.dumps(payload, ensure_ascii=False), ex=_CACHE_TTL)
    except Exception:
        logger.debug("avito_analytics_cache_set_failed", exc_info=True)


async def drop_cache(account_id: int, period_days: int | None = None) -> None:
    redis_conn = getattr(settings, "r", None)
    if not redis_conn:
        return
    keys = []
    if period_days is not None:
        keys.append(_cache_key(account_id, period_days))
    else:
        for days in (7, 30, 90):
            keys.append(_cache_key(account_id, days))
    try:
        await redis_conn.delete(*keys)
    except Exception:
        logger.debug("avito_analytics_cache_delete_failed", exc_info=True)


def _dt(days: int) -> tuple[str, str]:
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=int(days))
    return start.date().isoformat(), now.date().isoformat()


def _extract_items(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        for key in ("items", "result", "data"):
            arr = payload.get(key)
            if isinstance(arr, list):
                return [item for item in arr if isinstance(item, Mapping)]
    return []


def _extract_stats(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        for key in ("items", "result", "data", "stats"):
            arr = payload.get(key)
            if isinstance(arr, list):
                return [item for item in arr if isinstance(item, Mapping)]
    return []


def _extract_operations(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        arr = payload.get("operations") or payload.get("items") or payload.get("result") or payload.get("data")
        if isinstance(arr, list):
            return [item for item in arr if isinstance(item, Mapping)]
    return []


def _sum_fields(entries: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> float:
    total = 0.0
    for item in entries:
        for field in fields:
            try:
                val = item.get(field)
            except Exception:
                val = None
            if val is None:
                continue
            try:
                total += float(val)
            except Exception:
                continue
    return total


def _item_id(entry: Mapping[str, Any]) -> str | None:
    for key in ("id", "item_id", "itemId"):
        if key in entry and entry.get(key) is not None:
            return str(entry.get(key))
    return None


async def _call(func, *args, warnings: list[str], label: str, **kwargs) -> Any:
    try:
        return await func(*args, **kwargs)
    except avito_api.AvitoAPIError as exc:
        warn = f"{label}: {exc} ({exc.status or 'n/a'})"
        warnings.append(warn)
        logger.info("avito_analytics_call_failed label=%s status=%s", label, exc.status, exc_info=True)
        return None
    except Exception as exc:  # pragma: no cover - network
        warnings.append(f"{label}: unexpected error")
        logger.exception("avito_analytics_call_failed_unexpected label=%s", label)
        return None


def _build_items_table(items: list[Mapping[str, Any]], stats: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    stats_map: dict[str, Mapping[str, Any]] = {}
    for entry in stats:
        item_id = _item_id(entry)
        if not item_id:
            continue
        stats_map[item_id] = entry

    result: list[dict[str, Any]] = []
    for item in items:
        item_id = _item_id(item)
        row = {
            "id": item_id,
            "title": item.get("title") or item.get("name") or "",
            "status": item.get("status") or item.get("state"),
            "price": item.get("price"),
            "created": item.get("created") or item.get("date") or item.get("created_at"),
            "url": item.get("url") or item.get("link"),
        }
        if item_id and item_id in stats_map:
            stat = stats_map[item_id]
            row.update(
                {
                    "views": stat.get("uniqViews") or stat.get("views"),
                    "contacts": stat.get("uniqContacts") or stat.get("contacts"),
                    "calls": stat.get("calls"),
                    "favorites": stat.get("favorites"),
                }
            )
        result.append(row)
    return result


async def build_report(account_id: int, period_days: int = 30, *, force_refresh: bool = False) -> dict[str, Any]:
    cached = None if force_refresh else await _get_cache(account_id, period_days)
    if cached:
        return cached

    access_token, token_entry = await avito_api.ensure_access_token(int(account_id))
    date_from, date_to = _dt(period_days)

    warnings: list[str] = []
    account_meta = token_entry.raw_payload if isinstance(token_entry.raw_payload, Mapping) else {}
    user_id_hint = None
    if account_meta:
        user_id_hint = (
            account_meta.get("id")
            or account_meta.get("account_id")
            or account_meta.get("accountId")
            or account_meta.get("account")
        )
    user_info, items_payload, stats_payload, calls_payload, balance_payload, operations_payload, chats_payload = await asyncio.gather(
        _call(avito_api.get_user_me, access_token, warnings=warnings, label="user"),
        _call(avito_api.list_items, access_token, warnings=warnings, label="items"),
        _call(
            avito_api.get_items_stats,
            access_token,
            user_id_hint,
            None,
            date_from,
            date_to,
            fields=["uniqViews", "uniqContacts", "calls", "favorites"],
            warnings=warnings,
            label="stats",
        ),
        _call(avito_api.get_calls_stats, access_token, user_id_hint, date_from, date_to, warnings=warnings, label="calls"),
        _call(avito_api.get_balance, access_token, user_id_hint, warnings=warnings, label="balance"),
        _call(avito_api.get_operations, access_token, user_id_hint, date_from, date_to, warnings=warnings, label="operations"),
        _call(avito_api.messenger_list_chats, access_token, user_id_hint, warnings=warnings, label="chats"),
    )

    items = _extract_items(items_payload)
    stats_items = _extract_stats(stats_payload)
    operations = _extract_operations(operations_payload)
    chats = _extract_items(chats_payload)

    total_items = len(items)
    active_items = len([it for it in items if str(it.get("status") or "").lower() in {"active", "published"}])
    inactive_items = total_items - active_items
    total_views = _sum_fields(stats_items, ("uniqViews", "views"))
    total_contacts = _sum_fields(stats_items, ("uniqContacts", "contacts"))
    total_calls = _sum_fields(stats_items, ("calls",))
    spend_total = _sum_fields(operations, ("amount", "sum"))

    items_table = _build_items_table(items, stats_items)

    raw_block = {
        "user": user_info,
        "items": items_payload,
        "stats": stats_payload,
        "calls": calls_payload,
        "balance": balance_payload,
        "operations": operations_payload,
        "chats": chats_payload,
    }

    report = {
        "meta": {
            "account_id": account_id,
            "period_days": period_days,
            "date_from": date_from,
            "date_to": date_to,
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "scopes": token_entry.scopes,
            "warnings": warnings,
        },
        "summary_cards": {
            "total_items": total_items,
            "active_items": active_items,
            "inactive_items": inactive_items,
            "views": int(total_views),
            "contacts": int(total_contacts),
            "calls": int(total_calls),
            "spend": spend_total,
            "chats": len(chats),
        },
        "items_table": items_table,
        "operations": operations,
        "raw": raw_block,
    }
    await _set_cache(account_id, period_days, report)
    return report


async def accounts_summary() -> list[dict[str, Any]]:
    tokens = await tokens_repo.list_tokens()
    return tokens_repo.summary_from_tokens(tokens)


__all__ = [
    "AvitoAnalyticsError",
    "build_report",
    "drop_cache",
    "accounts_summary",
]
