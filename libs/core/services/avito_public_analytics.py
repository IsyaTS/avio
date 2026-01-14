from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import asyncio
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from libs.core.integrations import avito_analytics as avito_api
from libs.core.sales_core import settings
from libs.core.services import avito_analytics_report as report_utils

logger = logging.getLogger(__name__)

_CACHE_TTL = int(os.getenv("AVITO_ANALYTICS_CACHE_TTL", "600") or "600")
_MSK_TZ = timezone(timedelta(hours=3))
_WORKDAY_START = 9
_WORKDAY_END = 21
_DEFAULT_WEEKEND_DAYS = {5, 6}
_SESSION_GAP_MINUTES = 30


def _cache_key(account_id: int, period: int, sla: int, fast: bool, params: Mapping[str, Any] | None) -> str:
    params_json = json.dumps(params or {}, sort_keys=True, ensure_ascii=False)
    params_hash = hashlib.sha1(params_json.encode("utf-8")).hexdigest()[:10]
    return f"avito:analytics:pub:{account_id}:{period}:{sla}:{int(fast)}:{params_hash}"


async def _get_cache(account_id: int, period: int, sla: int, fast: bool, params: Mapping[str, Any] | None) -> dict[str, Any] | None:
    redis_conn = getattr(settings, "r", None)
    if not redis_conn:
        return None
    try:
        raw = await redis_conn.get(_cache_key(account_id, period, sla, fast, params))
    except Exception:
        logger.debug("avito_public_cache_get_failed", exc_info=True)
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def _set_cache(
    account_id: int, period: int, sla: int, fast: bool, params: Mapping[str, Any] | None, payload: Mapping[str, Any]
) -> None:
    redis_conn = getattr(settings, "r", None)
    if not redis_conn:
        return
    try:
        await redis_conn.set(
            _cache_key(account_id, period, sla, fast, params),
            json.dumps(payload, ensure_ascii=False),
            ex=_CACHE_TTL,
        )
    except Exception:
        logger.debug("avito_public_cache_set_failed", exc_info=True)


def _dt_range(days: int) -> tuple[str, str]:
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=int(days))
    return start.date().isoformat(), now.date().isoformat()


async def _fetch_stats_series(
    token: str,
    user_id: int,
    date_from: str,
    date_to: str,
    metrics: list[str],
) -> list[dict[str, Any]]:
    try:
        payload = await avito_api.get_items_stats(
            token,
            user_id,
            date_from,
            date_to,
            metrics=metrics,
            grouping="day",
            limit=1000,
            offset=0,
        )
    except avito_api.AvitoAPIError:
        return []
    rows = report_utils._extract_stats(payload)
    series: list[dict[str, Any]] = []
    for row in rows:
        ts = row.get("id")
        if ts is None:
            continue
        try:
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
        except Exception:
            dt = str(ts)
        series.append(
            {
                "date": dt,
                "views": row.get("views"),
                "contacts": row.get("contacts"),
                "calls": row.get("calls"),
                "spending": row.get("spending"),
            }
        )
    return series


def _build_heatmap(chats: list[Mapping[str, Any]]) -> list[list[int]]:
    grid = [[0 for _ in range(24)] for _ in range(7)]
    for chat in chats:
        messages = chat.get("_messages") or []
        if not isinstance(messages, list):
            continue
        for msg in messages:
            if not isinstance(msg, Mapping):
                continue
            if report_utils._classify_direction(msg, None) != "in":
                continue
            ts = report_utils._parse_ts(msg.get("created_at") or msg.get("sent_at") or msg.get("timestamp") or msg.get("ts"))
            if not ts:
                continue
            grid[ts.weekday()][ts.hour] += 1
    return grid


def _chat_active_after(chat: Mapping[str, Any], threshold: datetime) -> bool:
    updated = chat.get("updated") or chat.get("last_message", {}).get("created")
    if updated is None:
        return True
    ts = report_utils._parse_ts(updated)
    if not ts:
        return True
    return ts >= threshold


def _parse_time_minutes(value: Any, default_minutes: int) -> int:
    if value is None:
        return default_minutes
    if isinstance(value, (int, float)):
        hours = int(value)
        minutes = 0
    else:
        text = str(value).strip()
        if not text:
            return 0
        if ":" in text:
            parts = text.split(":", 1)
            try:
                hours = int(parts[0])
            except Exception:
                return default_minutes
            try:
                minutes = int(parts[1])
            except Exception:
                minutes = 0
        else:
            try:
                hours = int(text)
            except Exception:
                return default_minutes
            minutes = 0
    hours = max(0, min(23, hours))
    minutes = max(0, min(59, minutes))
    return hours * 60 + minutes


def _parse_weekend_days(value: Any) -> set[int]:
    if value is None:
        return set(_DEFAULT_WEEKEND_DAYS)
    if isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        text = str(value).strip()
        if not text:
            return set()
        candidates = [part.strip() for part in text.replace(";", ",").split(",")]
    result: set[int] = set()
    for candidate in candidates:
        try:
            day = int(candidate)
        except Exception:
            continue
        if 0 <= day <= 6:
            result.add(day)
    return result


def _format_minutes(minutes: int) -> str:
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def _calc_night_weekend(
    chats: list[Mapping[str, Any]],
    workday_start_min: int,
    workday_end_min: int,
    weekend_days: set[int],
    *,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> dict[str, Any]:
    total = 0
    night_weekend = 0
    for chat in chats:
        messages = chat.get("_messages") or []
        if not isinstance(messages, list):
            continue
        for msg in messages:
            if not isinstance(msg, Mapping):
                continue
            if report_utils._classify_direction(msg, None) != "in":
                continue
            ts = report_utils._parse_ts(
                msg.get("created_at")
                or msg.get("created")
                or msg.get("sent_at")
                or msg.get("timestamp")
                or msg.get("ts")
            )
            if not ts:
                continue
            if period_start and ts < period_start:
                continue
            if period_end and ts > period_end:
                continue
            total += 1
            local_ts = ts.astimezone(_MSK_TZ) if ts.tzinfo else ts.replace(tzinfo=timezone.utc).astimezone(_MSK_TZ)
            minutes = local_ts.hour * 60 + local_ts.minute
            if workday_start_min == workday_end_min:
                is_worktime = True
            elif workday_start_min < workday_end_min:
                is_worktime = workday_start_min <= minutes < workday_end_min
            else:
                is_worktime = minutes >= workday_start_min or minutes < workday_end_min
            is_weekend = local_ts.weekday() in weekend_days
            is_night = not is_worktime
            if is_weekend or is_night:
                night_weekend += 1
    share = (night_weekend / total) if total else None
    return {
        "incoming_total": total,
        "night_weekend_incoming": night_weekend,
        "night_weekend_share": share,
        "workday_start": _format_minutes(workday_start_min),
        "workday_end": _format_minutes(workday_end_min),
        "weekend_days": sorted(weekend_days),
        "timezone": "Europe/Moscow",
    }


def _calc_message_stats(
    chats: list[Mapping[str, Any]],
    seller_id: int | None,
    period_start: datetime,
    period_end: datetime,
    workday_start_min: int,
    workday_end_min: int,
) -> dict[str, Any]:
    incoming_by_hour = [0 for _ in range(24)]
    incoming_total = 0
    messages_total = 0
    messages_per_chat_total = 0
    chats_with_messages = 0
    response_times: list[float] = []
    active_time_sec = 0.0
    work_hours_incoming = 0
    for chat in chats:
        messages = chat.get("_messages") or []
        if not isinstance(messages, list):
            continue
        msg_rows: list[tuple[datetime, str]] = []
        for msg in messages:
            if not isinstance(msg, Mapping):
                continue
            ts = report_utils._parse_ts(
                msg.get("created_at")
                or msg.get("created")
                or msg.get("sent_at")
                or msg.get("timestamp")
                or msg.get("ts")
            )
            if not ts:
                continue
            if ts < period_start or ts > period_end:
                continue
            msg_rows.append((ts, report_utils._classify_direction(msg, seller_id)))
        if not msg_rows:
            continue
        msg_rows.sort(key=lambda x: x[0])
        chats_with_messages += 1
        messages_total += len(msg_rows)
        messages_per_chat_total += len(msg_rows)

        pending_in = None
        session_start = msg_rows[0][0]
        last_ts = msg_rows[0][0]
        for ts, direction in msg_rows:
            local_ts = ts.astimezone(_MSK_TZ) if ts.tzinfo else ts.replace(tzinfo=timezone.utc).astimezone(_MSK_TZ)
            if direction == "in":
                incoming_total += 1
                incoming_by_hour[local_ts.hour] += 1
                minutes = local_ts.hour * 60 + local_ts.minute
                if workday_start_min == workday_end_min:
                    in_work = True
                elif workday_start_min < workday_end_min:
                    in_work = workday_start_min <= minutes < workday_end_min
                else:
                    in_work = minutes >= workday_start_min or minutes < workday_end_min
                if in_work:
                    work_hours_incoming += 1
                if pending_in is None:
                    pending_in = ts
            elif direction == "out" and pending_in:
                response_times.append((ts - pending_in).total_seconds())
                pending_in = None
            gap = (ts - last_ts).total_seconds()
            if gap > _SESSION_GAP_MINUTES * 60:
                active_time_sec += max(0.0, (last_ts - session_start).total_seconds())
                session_start = ts
            last_ts = ts
        active_time_sec += max(0.0, (last_ts - session_start).total_seconds())

    avg_messages_per_chat = (messages_per_chat_total / chats_with_messages) if chats_with_messages else None
    avg_response_sec = (sum(response_times) / len(response_times)) if response_times else None
    work_hours_share = (work_hours_incoming / incoming_total) if incoming_total else None
    return {
        "incoming_total": incoming_total,
        "messages_total": messages_total,
        "incoming_by_hour": incoming_by_hour,
        "incoming_in_work_hours": work_hours_incoming,
        "incoming_in_work_hours_share": work_hours_share,
        "avg_messages_per_chat": avg_messages_per_chat,
        "avg_response_sec": avg_response_sec,
        "responses_measured": len(response_times),
        "active_time_sec": active_time_sec,
        "chats_with_messages": chats_with_messages,
    }


async def build_report(
    account_id: int,
    *,
    tenant_id: int | None = None,
    period_days: int = 7,
    sla_minutes: int = 15,
    fast: bool = True,
    calc_params: Mapping[str, Any] | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    cached = None if force_refresh else await _get_cache(account_id, period_days, sla_minutes, fast, calc_params)
    if cached:
        return cached
    token, token_entry = await avito_api.ensure_access_token(int(account_id))
    user_id = int(account_id)

    date_from, date_to = _dt_range(period_days)
    now = datetime.now(tz=timezone.utc)
    period_start = now - timedelta(days=int(period_days))
    period_end = now
    warnings: list[str] = []
    started = time.monotonic()

    items, items_raw = await report_utils._list_all_items(token, warnings, started, fast=fast)
    item_ids: list[int | str] = []
    for it in items:
        iid = it.get("id")
        if iid is None:
            continue
        try:
            item_ids.append(int(iid))
        except Exception:
            item_ids.append(str(iid))

    stats_items, stats_raw = await report_utils._fetch_stats_v1(
        token, user_id, item_ids, date_from, date_to, warnings, started
    )
    stats_series = await _fetch_stats_series(token, user_id, date_from, date_to, ["views", "contacts", "spending"])

    calls_payload, balance_payload, operations_payload, chats_payload = await asyncio.gather(
        avito_api.get_calls_stats(token, user_id, date_from, date_to, item_ids=item_ids[:200]),
        avito_api.get_balance(token, user_id),
        avito_api.get_operations(token, date_from, date_to),
        avito_api.messenger_list_chats(token, user_id, limit=50, offset=0),
    )

    operations = report_utils._extract_operations(operations_payload)
    chats_initial = report_utils._extract_items(chats_payload)
    if fast:
        extra_chats, chats_pages = await report_utils._fetch_chats_sample(token, user_id, warnings, fast=fast)
    else:
        extra_chats, chats_pages = await report_utils._fetch_chats_all(token, user_id, warnings, started=started)
    if not chats_initial and extra_chats:
        chats_initial = extra_chats
    elif extra_chats:
        chats_initial = chats_initial + [c for c in extra_chats if c not in chats_initial]

    chats_active = [chat for chat in chats_initial if _chat_active_after(chat, period_start)]
    await report_utils._attach_messages(token, user_id, chats_active, warnings, fast=fast)

    calls_totals, calls_has_days = report_utils._sum_calls_stats(calls_payload)
    if not calls_has_days:
        try:
            calls_payload = await avito_api.get_calls_stats(token, user_id, date_from, date_to)
            calls_totals, calls_has_days = report_utils._sum_calls_stats(calls_payload)
        except avito_api.AvitoAPIError:
            calls_has_days = False
    if not calls_has_days:
        warnings.append("calls_unavailable")

    sla_stats = report_utils._calc_sla(chats_active, user_id, sla_minutes)
    items_table = report_utils._build_items_table(items, stats_items)
    summary = report_utils._calc_summary(items_table, operations, sla_stats)
    losses = report_utils._calc_losses(sla_stats, calc_params)
    workday_start_min = _WORKDAY_START * 60
    workday_end_min = _WORKDAY_END * 60
    weekend_days = set(_DEFAULT_WEEKEND_DAYS)
    if calc_params:
        workday_start_min = _parse_time_minutes(calc_params.get("workday_start"), workday_start_min)
        workday_end_min = _parse_time_minutes(calc_params.get("workday_end"), workday_end_min)
        weekend_days = _parse_weekend_days(calc_params.get("weekend_days"))
    heatmap = _build_heatmap(chats_active)
    night_weekend = _calc_night_weekend(
        chats_active,
        workday_start_min,
        workday_end_min,
        weekend_days,
        period_start=period_start,
        period_end=period_end,
    )
    message_stats = _calc_message_stats(
        chats_active,
        user_id,
        period_start,
        period_end,
        workday_start_min,
        workday_end_min,
    )
    if calls_totals.get("calls"):
        summary["calls"] = int(calls_totals["calls"])
    if stats_series:
        views_total = sum(float(row.get("views") or 0) for row in stats_series)
        contacts_total = sum(float(row.get("contacts") or 0) for row in stats_series)
        if views_total:
            summary["views"] = int(views_total)
        if contacts_total:
            summary["chats"] = int(contacts_total)
        spending_total = sum(float(row.get("spending") or 0) for row in stats_series)
        if spending_total:
            summary["spend"] = round(spending_total / 100, 2)
    if night_weekend.get("night_weekend_share") is not None:
        summary["night_weekend_share"] = round(float(night_weekend["night_weekend_share"]) * 100, 1)
    leads_total = float(summary.get("chats") or 0) + float(summary.get("calls") or 0)
    if leads_total > 0:
        summary["chat_share_percent"] = round(float(summary.get("chats") or 0) / leads_total * 100, 1)
        summary["call_share_percent"] = round(float(summary.get("calls") or 0) / leads_total * 100, 1)
    if leads_total > 0 and summary.get("spend"):
        summary["client_cost"] = round(float(summary["spend"]) / leads_total, 2)
    if message_stats.get("incoming_in_work_hours_share") is not None:
        message_stats["incoming_in_work_hours_share"] = round(
            float(message_stats["incoming_in_work_hours_share"]) * 100, 1
        )
    if message_stats.get("avg_messages_per_chat") is not None:
        message_stats["avg_messages_per_chat"] = round(float(message_stats["avg_messages_per_chat"]), 2)
    if message_stats.get("avg_response_sec") is not None:
        message_stats["avg_response_sec"] = round(float(message_stats["avg_response_sec"]), 1)
    if message_stats.get("active_time_sec") is not None:
        message_stats["active_time_hours"] = round(float(message_stats["active_time_sec"]) / 3600, 2)

    report = {
        "meta": {
            "tenant_id": tenant_id,
            "account_id": user_id,
            "period_days": period_days,
            "date_from": date_from,
            "date_to": date_to,
            "sla_minutes": sla_minutes,
            "fast": fast,
            "warnings": warnings,
            "chat_sampled": len(chats_active),
        },
        "summary": summary,
        "losses": losses,
        "funnel": {
            "views": summary.get("views"),
            "chats": summary.get("chats"),
            "calls": summary.get("calls"),
        },
        "listings": {"items": items_table},
        "stats": {"series": stats_series, "items": stats_items},
        "messaging": {
            "sla": {
                "median_first_response_sec": statistics.median(sla_stats.first_response_seconds)
                if sla_stats.first_response_seconds
                else None,
                "avg_first_response_sec": statistics.mean(sla_stats.first_response_seconds)
                if sla_stats.first_response_seconds
                else None,
                "p90_first_response_sec": statistics.quantiles(sla_stats.first_response_seconds, n=10)[-1]
                if len(sla_stats.first_response_seconds) >= 10
                else None,
                "lt_5m": sla_stats.slow_buckets.get("lt_5m", 0),
                "lt_15m": sla_stats.slow_buckets.get("lt_15m", 0),
                "lt_60m": sla_stats.slow_buckets.get("lt_60m", 0),
                "breach": sla_stats.slow_buckets.get("breach", 0),
                "unanswered": sla_stats.unanswered,
                "total_chats_sampled": sla_stats.chats_total,
            },
            "stats": message_stats,
            "chats": chats_active[: report_utils._CHAT_SAMPLE_LIMIT],
            "heatmap": heatmap,
            "activity": night_weekend,
        },
        "spend": {"balance": balance_payload, "operations": operations},
        "calls": calls_payload,
        "raw": {
            "items": items_raw,
            "stats": stats_raw,
            "operations": operations_payload,
            "balance": balance_payload,
            "calls": calls_payload,
            "chats": chats_payload,
            "chats_pages": chats_pages if 'chats_pages' in locals() else None,
        },
    }

    await _set_cache(account_id, period_days, sla_minutes, fast, calc_params, report)
    return report


__all__ = ["build_report"]
