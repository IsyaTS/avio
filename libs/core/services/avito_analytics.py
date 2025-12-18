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
from libs.core.repo import avito_job_applications as job_repo
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
        for key in ("items", "result", "data", "resources"):
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


def _normalize_job_applications(entries: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in entries:
        try:
            app_id = item.get("id") or item.get("application_id") or item.get("applyId")
        except Exception:
            app_id = None
        if not app_id:
            continue
        normalized.append(
            {
                "id": str(app_id),
                "status": item.get("status") or item.get("state"),
                "created_at": item.get("created_at") or item.get("createdAt") or item.get("created"),
                "vacancy_id": item.get("vacancy_id") or item.get("vacancyId") or item.get("vacancy") or None,
                "resume_id": item.get("resume_id") or item.get("resumeId") or item.get("resume") or None,
                "applicant_id": item.get("applicant_id") or item.get("applicantId") or item.get("user_id"),
                "applicant": item.get("applicant") or item.get("applicant_name"),
            }
        )
    return normalized


def _vas_error_payload(exc: avito_api.AvitoAPIError) -> dict[str, Any]:
    return {"status": exc.status, "payload": exc.payload}


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
    token_holder = {"token": access_token}

    async def _refresh_token() -> None:
        new_token, new_entry = await avito_api.ensure_access_token(int(account_id))
        token_holder["token"] = new_token
        nonlocal token_entry
        token_entry = new_entry

    user_info, items_payload, stats_payload, calls_payload, balance_payload, operations_payload, chats_payload = await asyncio.gather(
        _call(avito_api.get_user_me, token_holder["token"], warnings=warnings, label="user"),
        _call(avito_api.list_items, token_holder["token"], warnings=warnings, label="items"),
        _call(
            avito_api.get_items_stats,
            token_holder["token"],
            user_id_hint,
            None,
            date_from,
            date_to,
            fields=["uniqViews", "uniqContacts", "calls", "favorites"],
            warnings=warnings,
            label="stats",
        ),
        _call(avito_api.get_calls_stats, token_holder["token"], user_id_hint, date_from, date_to, warnings=warnings, label="calls"),
        _call(avito_api.get_balance, token_holder["token"], user_id_hint, warnings=warnings, label="balance"),
        _call(avito_api.get_operations, token_holder["token"], user_id_hint, date_from, date_to, warnings=warnings, label="operations"),
        _call(avito_api.messenger_list_chats, token_holder["token"], user_id_hint, warnings=warnings, label="chats"),
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

    # Job applications (job:applications)
    job_meta = {"available": False, "source": "none", "warnings": []}
    job_table: list[dict[str, Any]] = []
    job_raw: dict[str, Any] = {}
    job_ids_source: list[str] = []

    params = {"dateFrom": date_from, "dateTo": date_to, "limit": 200}
    applications_list = None
    try:
        applications_list = await avito_api.job_try_list_applications(token_holder["token"], params=params)
        if applications_list is not None:
            job_meta["source"] = "poll"
    except avito_api.AvitoAPIError as exc:
        if exc.status == 401:
            try:
                await _refresh_token()
                applications_list = await avito_api.job_try_list_applications(token_holder["token"], params=params)
            except Exception:
                job_meta["warnings"].append("job_applications_auth_failed")
        else:
            job_meta["warnings"].append(f"job_list_failed:{exc.status or 'err'}")

    if applications_list is None:
        try:
            job_ids_source = await job_repo.list_recent_ids(int(account_id), period_days=period_days, limit=500)
            if job_ids_source:
                job_meta["source"] = "stored"
        except Exception:
            job_meta["warnings"].append("job_ids_store_unavailable")

    applications_by_ids = None
    normalized_apps: list[dict[str, Any]] = []
    if applications_list:
        job_raw["list"] = applications_list
        items_array = _extract_items(applications_list)
        normalized_apps = _normalize_job_applications(items_array)
    elif job_ids_source:
        try:
            applications_by_ids = await avito_api.job_get_applications_by_ids(token_holder["token"], job_ids_source[:200])
        except avito_api.AvitoAPIError as exc:
            if exc.status == 401:
                try:
                    await _refresh_token()
                    applications_by_ids = await avito_api.job_get_applications_by_ids(token_holder["token"], job_ids_source[:200])
                except Exception:
                    job_meta["warnings"].append("job_get_by_ids_auth_failed")
            else:
                job_meta["warnings"].append(f"job_get_by_ids_failed:{exc.status or 'err'}")
        if applications_by_ids:
            job_raw["by_ids"] = applications_by_ids
            items_array = _extract_items(applications_by_ids)
            normalized_apps = _normalize_job_applications(items_array)

    if normalized_apps:
        job_table = normalized_apps
        job_meta["available"] = True

    # Enrich resumes/vacancies (best-effort)
    resumes_raw = []
    vacancies_raw = []
    if job_table:
        resume_ids = []
        vacancy_ids = []
        for entry in job_table:
            if entry.get("resume_id"):
                resume_ids.append(str(entry["resume_id"]))
            if entry.get("vacancy_id"):
                vacancy_ids.append(str(entry["vacancy_id"]))
        resume_ids = list(dict.fromkeys(resume_ids))[:10]
        vacancy_ids = list(dict.fromkeys(vacancy_ids))[:10]
        for rid in resume_ids:
            try:
                resp = await avito_api.job_get_resume_v2(token_holder["token"], rid)
                if resp:
                    resumes_raw.append(resp)
            except avito_api.AvitoAPIError:
                continue
        for vid in vacancy_ids:
            try:
                resp = await avito_api.job_get_vacancy_v2(token_holder["token"], vid)
                if resp:
                    vacancies_raw.append(resp)
            except avito_api.AvitoAPIError:
                continue

    job_raw["resumes"] = resumes_raw
    job_raw["vacancies"] = vacancies_raw
    job_raw["stored_ids"] = job_ids_source

    if job_table:
        report["job_applications"] = {
            "meta": job_meta,
            "kpi": {
                "total_applications": len(job_table),
                "unique_applicants": len({row.get("applicant_id") for row in job_table if row.get("applicant_id")}),
                "by_status": {status: len([1 for row in job_table if row.get("status") == status]) for status in {row.get("status") for row in job_table}},
            },
            "table": job_table,
            "raw": job_raw,
        }
    else:
        report["job_applications"] = {"meta": job_meta, "table": [], "raw": job_raw}

    # VAS pricing (items:apply_vas)
    vas_meta = {"available": False, "warnings": []}
    vas_raw: dict[str, Any] = {}
    vas_prices = None
    vas_packages = None
    try:
        vas_prices = await avito_api.get_vas_prices(token_holder["token"], user_id_hint, payload={})
        vas_raw["prices"] = vas_prices
    except avito_api.AvitoAPIError as exc:
        if exc.status == 401:
            try:
                await _refresh_token()
                vas_prices = await avito_api.get_vas_prices(token_holder["token"], user_id_hint, payload={})
                vas_raw["prices"] = vas_prices
            except Exception:
                vas_meta["warnings"].append("vas_prices_auth_failed")
        elif exc.status in {400, 403}:
            vas_meta["warnings"].append(f"vas_prices_unavailable:{exc.status}")
            vas_raw["prices_error"] = _vas_error_payload(exc)
        else:
            vas_meta["warnings"].append("vas_prices_error")
            vas_raw["prices_error"] = _vas_error_payload(exc)
    try:
        vas_packages = await avito_api.get_vas_packages_prices(token_holder["token"], user_id_hint, payload={})
        vas_raw["packages"] = vas_packages
    except avito_api.AvitoAPIError as exc:
        if exc.status == 401:
            try:
                await _refresh_token()
                vas_packages = await avito_api.get_vas_packages_prices(token_holder["token"], user_id_hint, payload={})
                vas_raw["packages"] = vas_packages
            except Exception:
                vas_meta["warnings"].append("vas_packages_auth_failed")
        elif exc.status in {400, 403}:
            vas_meta["warnings"].append(f"vas_packages_unavailable:{exc.status}")
            vas_raw["packages_error"] = _vas_error_payload(exc)
        else:
            vas_meta["warnings"].append("vas_packages_error")
            vas_raw["packages_error"] = _vas_error_payload(exc)

    cheapest_promos: list[dict[str, Any]] = []
    if isinstance(vas_prices, Mapping):
        services = vas_prices.get("services") or vas_prices.get("result") or []
        if isinstance(services, list):
            for entry in services:
                if not isinstance(entry, Mapping):
                    continue
                try:
                    price = float(entry.get("price") or entry.get("amount") or 0)
                except Exception:
                    price = None
                cheapest_promos.append(
                    {
                        "name": entry.get("name") or entry.get("service"),
                        "price": price,
                        "duration": entry.get("duration") or entry.get("period"),
                    }
                )
        cheapest_promos = sorted([c for c in cheapest_promos if c.get("price") is not None], key=lambda x: x["price"])[:10]

    if vas_prices or vas_packages:
        vas_meta["available"] = True
    report["vas"] = {
        "meta": vas_meta,
        "cheapest_promos": cheapest_promos,
        "raw": vas_raw,
    }
    report["meta"]["warnings"].extend(job_meta.get("warnings", []))
    report["meta"]["warnings"].extend(vas_meta.get("warnings", []))

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
    "_normalize_job_applications",
    "_vas_error_payload",
]
