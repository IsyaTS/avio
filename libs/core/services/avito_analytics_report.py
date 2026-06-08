from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import statistics
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from libs.core.integrations import avito_analytics as avito_api
from libs.core.sales_core import settings

logger = logging.getLogger(__name__)

_CACHE_TTL = int(os.getenv("AVITO_ANALYTICS_CACHE_TTL", "900") or "900")
_FAST_MODE = bool(int(os.getenv("AVITO_ANALYTICS_FAST_MODE", "0") or "0"))
_ITEMS_DELAY = float(os.getenv("AVITO_ANALYTICS_ITEMS_DELAY", "1.5") or "1.5")
_STATS_DELAY = float(os.getenv("AVITO_ANALYTICS_STATS_DELAY", "2.0") or "2.0")
_TIME_BUDGET = int(os.getenv("AVITO_ANALYTICS_TIME_BUDGET", "900") or "900")
_STATS_CHUNK = int(os.getenv("AVITO_ANALYTICS_STATS_CHUNK", "120") or "120")
_CHAT_SAMPLE_LIMIT = int(os.getenv("AVITO_ANALYTICS_CHAT_SAMPLE", "200") or "200")
_MESSAGES_PER_CHAT = int(os.getenv("AVITO_ANALYTICS_MESSAGES_PER_CHAT", "120") or "120")
_CHAT_MESSAGES_CONCURRENCY = int(os.getenv("AVITO_ANALYTICS_MESSAGES_CONCURRENCY", "6") or "6")


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _dt_range(days: int) -> tuple[str, str]:
    now = _now()
    start = now - timedelta(days=int(days))
    return start.date().isoformat(), now.date().isoformat()


def _monotonic() -> float:
    return time.monotonic()


def _budget_left(start_ts: float) -> float:
    return max(_TIME_BUDGET - (_monotonic() - start_ts), 0)


def _cache_key(account_id: int, period: int, sla: int, params: Mapping[str, Any] | None) -> str:
    params_json = json.dumps(params or {}, sort_keys=True, ensure_ascii=False)
    params_hash = hashlib.sha1(params_json.encode("utf-8")).hexdigest()[:10]
    return f"avito:analytics:v2:{account_id}:{period}:{sla}:{params_hash}"


async def _get_cache(
    account_id: int, period: int, sla: int, params: Mapping[str, Any] | None
) -> dict[str, Any] | None:
    redis_conn = getattr(settings, "r", None)
    if not redis_conn:
        return None
    try:
        raw = await redis_conn.get(_cache_key(account_id, period, sla, params))
    except Exception:
        logger.debug("avito_analytics_v2_cache_get_failed", exc_info=True)
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def _set_cache(
    account_id: int,
    period: int,
    sla: int,
    params: Mapping[str, Any] | None,
    payload: Mapping[str, Any],
) -> None:
    redis_conn = getattr(settings, "r", None)
    if not redis_conn:
        return
    try:
        await redis_conn.set(
            _cache_key(account_id, period, sla, params),
            json.dumps(payload, ensure_ascii=False),
            ex=_CACHE_TTL,
        )
    except Exception:
        logger.debug("avito_analytics_v2_cache_set_failed", exc_info=True)


def _extract_items(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, Mapping)]
    if isinstance(payload, Mapping):
        for key in ("items", "result", "data", "resources", "chats", "messages"):
            arr = payload.get(key)
            if isinstance(arr, list):
                return [x for x in arr if isinstance(x, Mapping)]
    return []


def _extract_stats(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, Mapping)]
    if isinstance(payload, Mapping):
        res = payload.get("result")
        if isinstance(res, Mapping):
            if isinstance(res.get("groupings"), list):
                normalized: list[dict[str, Any]] = []
                for group in res["groupings"]:
                    if not isinstance(group, Mapping):
                        continue
                    entry: dict[str, Any] = {"id": group.get("id")}
                    metrics = group.get("metrics")
                    if isinstance(metrics, list):
                        for metric in metrics:
                            if not isinstance(metric, Mapping):
                                continue
                            slug = metric.get("slug")
                            entry[str(slug)] = metric.get("value")
                    normalized.append(entry)
                return normalized
            items = res.get("items")
            if isinstance(items, list):
                normalized = []
                for row in items:
                    if not isinstance(row, Mapping):
                        continue
                    item_id = row.get("itemId") or row.get("item_id") or row.get("id")
                    stats = row.get("stats")
                    views = contacts = favorites = calls = 0.0
                    if isinstance(stats, list):
                        for stat in stats:
                            if not isinstance(stat, Mapping):
                                continue
                            views += float(stat.get("uniqViews") or stat.get("views") or 0)
                            contacts += float(stat.get("uniqContacts") or stat.get("contacts") or 0)
                            favorites += float(
                                stat.get("uniqFavorites") or stat.get("favorites") or 0
                            )
                            calls += float(stat.get("calls") or 0)
                    normalized.append(
                        {
                            "id": item_id,
                            "views": views,
                            "contacts": contacts,
                            "favorites": favorites,
                            "calls": calls or None,
                        }
                    )
                return normalized
        for key in ("items", "result", "data", "stats"):
            arr = payload.get(key)
            if isinstance(arr, list):
                return [x for x in arr if isinstance(x, Mapping)]
    return []


def _extract_operations(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, Mapping)]
    if isinstance(payload, Mapping):
        arr = payload.get("operations") or payload.get("items") or payload.get("data")
        if arr is None:
            result = payload.get("result")
            if isinstance(result, Mapping):
                arr = result.get("operations") or result.get("items")
        if isinstance(arr, list):
            return [x for x in arr if isinstance(x, Mapping)]
    return []


def _operation_amount(op: Mapping[str, Any]) -> float:
    amount = float(
        op.get("amountTotal") or op.get("amountRub") or op.get("amount") or op.get("sum") or 0
    )
    name = str(op.get("operationName") or "").lower()
    op_type = str(op.get("operationType") or "").lower()
    service_type = str(op.get("serviceType") or "").lower()
    is_cpa = "cpa" in op_type or "cpa" in name or service_type == "cpa"
    if "пополнение" in name:
        return 0.0
    if "аванс" in op_type and not is_cpa:
        return 0.0
    if "внесение" in op_type and not is_cpa:
        return 0.0
    if any(word in op_type or word in name for word in ("сторно", "возврат")):
        return -amount
    return amount


def _sum_calls_stats(payload: Any) -> tuple[dict[str, float], bool]:
    totals = {"calls": 0.0, "answered": 0.0, "new": 0.0, "new_answered": 0.0}
    has_days = False
    items: list[Mapping[str, Any]] = []
    if isinstance(payload, list):
        items = [x for x in payload if isinstance(x, Mapping)]
    elif isinstance(payload, Mapping):
        res = payload.get("result")
        if isinstance(res, Mapping) and isinstance(res.get("items"), list):
            items = [x for x in res["items"] if isinstance(x, Mapping)]
        elif isinstance(payload.get("items"), list):
            items = [x for x in payload["items"] if isinstance(x, Mapping)]
    for item in items:
        days = item.get("days")
        if not isinstance(days, list):
            continue
        has_days = True
        for day in days:
            if not isinstance(day, Mapping):
                continue
            totals["calls"] += float(day.get("calls") or 0)
            totals["answered"] += float(day.get("answered") or 0)
            totals["new"] += float(day.get("new") or 0)
            totals["new_answered"] += float(day.get("newAnswered") or 0)
    return totals, has_days


def _parse_ts(val: Any) -> datetime | None:
    if isinstance(val, datetime):
        return val
    if val is None or val == "":
        return None
    if isinstance(val, (int, float)):
        ts = float(val)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return datetime.strptime(str(val), fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(str(val))
    except Exception:
        return None


def _classify_direction(msg: Mapping[str, Any], seller_id: int | None) -> str:
    direction = str(
        msg.get("direction")
        or msg.get("type")
        or msg.get("author_type")
        or msg.get("authorType")
        or ""
    ).lower()
    sender = (
        msg.get("senderId")
        or msg.get("authorId")
        or msg.get("author_id")
        or msg.get("user_id")
        or msg.get("userId")
    )
    if direction in {"incoming", "from_client", "buyer", "in"}:
        return "in"
    if direction in {"outgoing", "from_seller", "seller", "out"}:
        return "out"
    if seller_id is not None and sender is not None:
        try:
            if int(sender) == int(seller_id):
                return "out"
        except Exception:
            pass
    return "in"


@dataclass
class SLAStats:
    first_response_seconds: list[float]
    unanswered: int
    chats_total: int
    slow_buckets: Counter


def _calc_sla(
    chats: Sequence[Mapping[str, Any]], seller_id: int | None, sla_minutes: int
) -> SLAStats:
    frt: list[float] = []
    unanswered = 0
    total = 0
    slow = Counter()
    sla_sec = sla_minutes * 60
    for chat in chats:
        messages = chat.get("_messages") or []
        if not isinstance(messages, list):
            continue
        total += 1
        msgs_sorted: list[tuple[datetime, str]] = []
        for msg in messages:
            if not isinstance(msg, Mapping):
                continue
            ts = _parse_ts(
                msg.get("created_at")
                or msg.get("created")
                or msg.get("sent_at")
                or msg.get("timestamp")
                or msg.get("ts")
            )
            if not ts:
                continue
            msgs_sorted.append((ts, _classify_direction(msg, seller_id)))
        msgs_sorted.sort(key=lambda x: x[0])
        first_in = next((ts for ts, d in msgs_sorted if d == "in"), None)
        first_out_after_in = None
        last_in = None
        last_out = None
        for ts, d in msgs_sorted:
            if d == "in":
                last_in = ts
            if d == "out":
                last_out = ts
                if first_in and ts >= first_in and first_out_after_in is None:
                    first_out_after_in = ts
        if first_in and first_out_after_in:
            delta = (first_out_after_in - first_in).total_seconds()
            frt.append(delta)
            if delta > sla_sec:
                slow["breach"] += 1
            if delta <= 300:
                slow["lt_5m"] += 1
            if delta <= 900:
                slow["lt_15m"] += 1
            if delta <= 3600:
                slow["lt_60m"] += 1
        if last_in and (last_out is None or last_out < last_in):
            unanswered += 1
    return SLAStats(
        first_response_seconds=frt, unanswered=unanswered, chats_total=total, slow_buckets=slow
    )


async def _list_all_items(
    token: str, warnings: list[str], started: float, *, fast: bool = False
) -> tuple[list[Mapping[str, Any]], Any]:
    all_items: list[Mapping[str, Any]] = []
    pages_raw: list[Any] = []
    page = 1
    per_page = 100 if not fast else 50
    max_pages = 200 if not fast else 3
    while page <= max_pages:
        if _budget_left(started) <= 0:
            warnings.append("items_time_budget_exceeded")
            break
        attempts = 0
        payload = None
        while attempts < 4:
            attempts += 1
            try:
                payload = await avito_api.list_items(
                    token, page=page, per_page=per_page, statuses=avito_api.ALL_ITEM_STATUSES
                )
                break
            except avito_api.AvitoAPIError as exc:
                if exc.status == 429:
                    warnings.append(f"items_rate_limited_p{page}")
                    await asyncio.sleep(_ITEMS_DELAY * (attempts + 1))
                    per_page = max(50, int(per_page / 2))  # сузить запрос
                    continue
                warnings.append(f"items_p{page}_failed:{exc.status or 'err'}")
                payload = None
                break
        if payload is None:
            break
        pages_raw.append(payload)
        items = _extract_items(payload)
        if items:
            all_items.extend(items)
        meta = payload.get("meta") if isinstance(payload, Mapping) else {}
        per_page = meta.get("per_page") or per_page
        if not items or len(items) < per_page:
            break
        page += 1
        await asyncio.sleep(_ITEMS_DELAY)
    return all_items, {"pages": pages_raw}


async def _fetch_stats_v1(
    token: str,
    user_id: int,
    item_ids: Sequence[int | str],
    date_from: str,
    date_to: str,
    warnings: list[str],
    started: float,
) -> tuple[list[Mapping[str, Any]], Any]:
    stats_all: list[Mapping[str, Any]] = []
    raw_pages: list[Any] = []
    chunks: list[list[int | str]] = []
    buf: list[int | str] = []
    for iid in item_ids:
        buf.append(iid)
        if len(buf) >= _STATS_CHUNK:
            chunks.append(buf)
            buf = []
    if buf:
        chunks.append(buf)
    for idx, chunk in enumerate(chunks):
        if _budget_left(started) <= 0:
            warnings.append("stats_time_budget_exceeded")
            break
        try:
            payload = await avito_api.get_items_stats_v1(token, user_id, chunk, date_from, date_to)
        except avito_api.AvitoAPIError as exc:
            if exc.status == 429:
                warnings.append("stats_truncated_rate_limit_offset_%s" % idx)
                await asyncio.sleep(_STATS_DELAY * 2)
                continue
            warnings.append(f"stats_page_failed:{idx}")
            continue
        raw_pages.append(payload)
        stats_all.extend(_extract_stats(payload))
        await asyncio.sleep(_STATS_DELAY)
    return stats_all, {"pages": raw_pages}


async def _fetch_chats_sample(
    token: str, user_id: int | None, warnings: list[str], *, fast: bool = False
) -> tuple[list[Mapping[str, Any]], Any]:
    chats: list[Mapping[str, Any]] = []
    raw_pages: list[Any] = []
    offset = 0
    limit = 50 if not fast else 30
    sample_limit = _CHAT_SAMPLE_LIMIT if not fast else min(_CHAT_SAMPLE_LIMIT, 50)
    while len(chats) < sample_limit:
        try:
            payload = await avito_api.messenger_list_chats(
                token, user_id, limit=limit, offset=offset
            )
        except avito_api.AvitoAPIError as exc:
            if exc.status == 429:
                warnings.append("chats_rate_limited")
                await asyncio.sleep(1.0)
                break
            if exc.status in {403, 404}:
                warnings.append("chats_unavailable")
                break
            warnings.append("chats_failed")
            break
        raw_pages.append(payload)
        rows = _extract_items(payload)
        if rows:
            chats.extend(rows)
        if not rows or len(rows) < limit:
            break
        offset += limit
        await asyncio.sleep(0.3)
    return chats, {"pages": raw_pages}


async def _fetch_chats_all(
    token: str,
    user_id: int | None,
    warnings: list[str],
    *,
    started: float | None = None,
    limit: int = 50,
) -> tuple[list[Mapping[str, Any]], Any]:
    chats: list[Mapping[str, Any]] = []
    raw_pages: list[Any] = []
    offset = 0
    while True:
        if started is not None and _budget_left(started) <= 0:
            warnings.append("chats_time_budget_exceeded")
            break
        try:
            payload = await avito_api.messenger_list_chats(
                token, user_id, limit=limit, offset=offset
            )
        except avito_api.AvitoAPIError as exc:
            if exc.status == 429:
                warnings.append("chats_rate_limited")
                await asyncio.sleep(1.0)
                break
            if exc.status in {403, 404}:
                warnings.append("chats_unavailable")
                break
            warnings.append("chats_failed")
            break
        raw_pages.append(payload)
        rows = _extract_items(payload)
        if rows:
            chats.extend(rows)
        if not rows or len(rows) < limit:
            break
        offset += limit
        await asyncio.sleep(0.3)
    return chats, {"pages": raw_pages}


async def _attach_messages(
    token: str,
    user_id: int | None,
    chats: list[Mapping[str, Any]],
    warnings: list[str],
    *,
    fast: bool = False,
) -> None:
    tasks = []
    limited = chats if not fast else chats[: min(_CHAT_SAMPLE_LIMIT, 50)]
    sem = asyncio.Semaphore(max(1, _CHAT_MESSAGES_CONCURRENCY))

    async def runner(chat: Mapping[str, Any]) -> None:
        chat_id = chat.get("id") or chat.get("chat_id") or chat.get("chatId")
        if not chat_id:
            return
        async with sem:
            await _load_messages_for_chat(token, user_id, chat, warnings, fast=fast)

    for chat in limited:
        tasks.append(runner(chat))
    await asyncio.gather(*tasks)


async def _load_messages_for_chat(
    token: str,
    user_id: int | None,
    chat: Mapping[str, Any],
    warnings: list[str],
    *,
    fast: bool = False,
) -> None:
    chat_id = chat.get("id") or chat.get("chat_id") or chat.get("chatId")
    messages: list[Any] = []
    offset = 0
    limit = 50 if not fast else 20
    msg_cap = None if not fast else min(_MESSAGES_PER_CHAT, 50)
    while True:
        if msg_cap and len(messages) >= msg_cap:
            warnings.append("messages_truncated")
            break
        try:
            payload = await avito_api.messenger_get_messages(
                token, user_id, str(chat_id), limit=limit, offset=offset
            )
        except avito_api.AvitoAPIError as exc:
            if exc.status in {403, 404}:
                break
            if exc.status == 429:
                warnings.append("messages_rate_limited")
                await asyncio.sleep(0.5)
                break
            break
        rows = _extract_items(payload)
        if rows:
            messages.extend(rows)
        if not rows or len(rows) < limit:
            break
        offset += limit
    chat["_messages"] = messages


def _build_items_table(
    items: Sequence[Mapping[str, Any]], stats: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    stats_map: dict[str, Mapping[str, Any]] = {}
    for st in stats:
        iid = st.get("id") or st.get("itemId") or st.get("item_id")
        if iid is None:
            continue
        stats_map[str(iid)] = st
    table: list[dict[str, Any]] = []
    for item in items:
        iid = item.get("id")
        iid_str = str(iid) if iid is not None else None
        st = stats_map.get(iid_str or "")
        table.append(
            {
                "id": iid,
                "title": item.get("title"),
                "status": item.get("status"),
                "price": item.get("price"),
                "created_at": item.get("created"),
                "views": st.get("views") if st else None,
                "contacts": st.get("contacts") if st else None,
                "calls": st.get("calls") if st else None,
                "url": item.get("url") or item.get("link"),
            }
        )
    return table


def _calc_summary(
    items_table: Sequence[Mapping[str, Any]],
    operations: Sequence[Mapping[str, Any]],
    sla_stats: SLAStats,
) -> dict[str, Any]:
    views = sum(float(row.get("views") or 0) for row in items_table)
    contacts = sum(float(row.get("contacts") or 0) for row in items_table)
    calls = sum(float(row.get("calls") or 0) for row in items_table)
    spend = sum(_operation_amount(op) for op in operations)
    if spend < 0:
        spend = 0
    summary = {
        "views": int(views),
        "chats": int(contacts),
        "calls": int(calls),
        "messages": None,
        "unanswered": sla_stats.unanswered,
        "sla_breach": sla_stats.slow_buckets.get("breach", 0),
        "spend": spend,
    }
    return summary


def _calc_losses(sla_stats: SLAStats, params: Mapping[str, Any] | None) -> dict[str, Any]:
    p = params or {}
    avg_check = float(p.get("avg_check") or 0)
    close_rate_chat = float(p.get("close_rate_chat") or 0)
    margin = (
        float(p.get("gross_margin") or 0) / 100 if p.get("gross_margin") not in (None, "") else 0
    )
    loss_factor = float(p.get("loss_factor_slow_response") or 0)
    unanswered = sla_stats.unanswered
    slow = sla_stats.slow_buckets.get("breach", 0)
    revenue_unanswered = (
        unanswered * avg_check * close_rate_chat if avg_check and close_rate_chat else None
    )
    revenue_slow = (
        slow * avg_check * close_rate_chat * loss_factor
        if avg_check and close_rate_chat and loss_factor
        else None
    )
    return {
        "unanswered_leads": unanswered,
        "slow_response_leads": slow,
        "revenue_at_risk_unanswered": revenue_unanswered,
        "revenue_at_risk_slow": revenue_slow,
        "profit_at_risk_unanswered": revenue_unanswered * margin
        if revenue_unanswered is not None and margin
        else None,
    }


async def build_report(
    account_id: int,
    period_days: int = 30,
    sla_minutes: int = 15,
    calc_params: Mapping[str, Any] | None = None,
    fast: bool | None = None,
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    if fast is None:
        fast = _FAST_MODE
    cached = (
        None
        if force_refresh
        else await _get_cache(account_id, period_days, sla_minutes, calc_params)
    )
    if cached:
        return cached

    token, token_entry = await avito_api.ensure_access_token(int(account_id))
    date_from, date_to = _dt_range(period_days)
    warnings: list[str] = []
    started = _monotonic()

    user_info = await avito_api.get_user_me(token)
    user_id = None
    if isinstance(user_info, Mapping):
        for key in ("id", "account_id", "accountId", "account", "user_id"):
            if key in user_info and user_info.get(key) is not None:
                try:
                    user_id = int(user_info.get(key))
                    break
                except Exception:
                    continue
    if user_id is None:
        user_id = token_entry.account_id

    items, items_raw = await _list_all_items(token, warnings, started, fast=fast)
    item_ids: list[int | str] = []
    for it in items:
        iid = it.get("id")
        if iid is None:
            continue
        try:
            item_ids.append(int(iid))
        except Exception:
            item_ids.append(str(iid))

    stats_items, stats_raw = await _fetch_stats_v1(
        token, int(user_id), item_ids, date_from, date_to, warnings, started
    )

    (
        calls_payload,
        balance_payload,
        operations_payload,
        chats_payload,
        ratings_payload,
        autoload_payload,
        cpx_payload,
        job_payload,
    ) = await asyncio.gather(
        avito_api.get_calls_stats(token, user_id, date_from, date_to, item_ids=item_ids[:200])
        if user_id
        else asyncio.sleep(0, result=None),
        avito_api.get_balance(token, user_id) if user_id else asyncio.sleep(0, result=None),
        avito_api.get_operations(token, date_from, date_to),
        avito_api.messenger_list_chats(token, user_id, limit=50, offset=0),
        avito_api.try_get_ratings(token, user_id),
        avito_api.try_get_autoload_reports(token),
        avito_api.try_get_cpx_campaigns(token, user_id),
        avito_api.job_try_list_applications(token, {"dateFrom": date_from, "dateTo": date_to}),
    )

    calls_totals, calls_has_days = _sum_calls_stats(calls_payload)
    if not calls_has_days:
        try:
            calls_payload = await avito_api.get_calls_stats(token, user_id, date_from, date_to)
            calls_totals, calls_has_days = _sum_calls_stats(calls_payload)
        except avito_api.AvitoAPIError:
            calls_has_days = False
    if not calls_has_days:
        warnings.append("calls_unavailable")

    operations = _extract_operations(operations_payload)
    chats_initial = _extract_items(chats_payload)
    # Extend chats with pagination + messages
    extra_chats, chats_pages = await _fetch_chats_sample(token, user_id, warnings, fast=fast)
    if not chats_initial and extra_chats:
        chats_initial = extra_chats
    elif extra_chats:
        chats_initial = chats_initial + [c for c in extra_chats if c not in chats_initial]

    await _attach_messages(token, user_id, chats_initial, warnings, fast=fast)

    sla_stats = _calc_sla(chats_initial, user_id, sla_minutes)

    items_table = _build_items_table(items, stats_items)
    summary = _calc_summary(items_table, operations, sla_stats)
    if calls_totals.get("calls"):
        summary["calls"] = int(calls_totals["calls"])
    leads_total = float(summary.get("chats") or 0) + float(summary.get("calls") or 0)
    if leads_total > 0 and summary.get("spend"):
        summary["client_cost"] = round(float(summary["spend"]) / leads_total, 2)
    losses = _calc_losses(sla_stats, calc_params)

    report = {
        "meta": {
            "account_id": account_id,
            "period_days": period_days,
            "date_from": date_from,
            "date_to": date_to,
            "sla_minutes": sla_minutes,
            "warnings": warnings,
            "chat_sampled": len(chats_initial),
            "chat_sample_limit": _CHAT_SAMPLE_LIMIT,
        },
        "user": user_info,
        "summary": summary,
        "losses": losses,
        "listings": {
            "items": items_table,
        },
        "messaging": {
            "sla": {
                "median_first_response_sec": statistics.median(sla_stats.first_response_seconds)
                if sla_stats.first_response_seconds
                else None,
                "p90_first_response_sec": statistics.quantiles(
                    sla_stats.first_response_seconds, n=10
                )[-1]
                if len(sla_stats.first_response_seconds) >= 10
                else None,
                "lt_5m": sla_stats.slow_buckets.get("lt_5m", 0),
                "lt_15m": sla_stats.slow_buckets.get("lt_15m", 0),
                "lt_60m": sla_stats.slow_buckets.get("lt_60m", 0),
                "breach": sla_stats.slow_buckets.get("breach", 0),
                "unanswered": sla_stats.unanswered,
                "total_chats_sampled": sla_stats.chats_total,
            },
            "chats": chats_initial[:_CHAT_SAMPLE_LIMIT],
        },
        "spend": {
            "balance": balance_payload,
            "operations": operations,
        },
        "ratings": ratings_payload,
        "autoload": autoload_payload,
        "cpx": cpx_payload,
        "job_applications": job_payload,
        "calls": calls_payload,
        "raw": {
            "items": items_raw,
            "stats": stats_raw,
            "operations": operations_payload,
            "balance": balance_payload,
            "calls": calls_payload,
            "chats": chats_payload,
            "chats_pages": chats_pages if "chats_pages" in locals() else None,
            "ratings": ratings_payload,
            "autoload": autoload_payload,
            "cpx": cpx_payload,
            "job_applications": job_payload,
        },
    }

    await _set_cache(account_id, period_days, sla_minutes, calc_params, report)
    return report


__all__ = ["build_report"]
