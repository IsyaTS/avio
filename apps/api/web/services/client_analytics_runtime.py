from __future__ import annotations

import re
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

from fastapi import Request
from fastapi.responses import Response


SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ClientAnalyticsDeps:
    resolve_tenant_and_key_fn: SyncFn
    db_module: Any


async def analytics_summary_api(
    request: Request,
    *,
    tenant: int | str | None,
    days: int,
    deps: ClientAnalyticsDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    data = await deps.db_module.get_tenant_message_stats(
        int(tenant_id), days=int(days), limit=30000
    )
    rows = data.get("rows") if isinstance(data, dict) else []
    if not rows:
        return _empty_summary(days)

    by_day: dict[str, dict[str, int]] = {}
    by_channel: dict[str, dict[str, int]] = {}
    incoming = 0
    outgoing = 0
    outgoing_mix = {"bot": 0, "manager": 0, "followup": 0}
    question_counts: dict[str, int] = {}
    per_lead: dict[int, list[dict[str, Any]]] = {}

    for row in rows:
        day_bucket, channel_bucket = _buckets(row, by_day, by_channel)
        direction = int(row.get("direction") or 0)
        if direction == 0:
            incoming += 1
            day_bucket["incoming"] += 1
            channel_bucket["incoming"] += 1
            norm = _normalize_question_text(str(row.get("text") or ""))
            if len(norm) >= 3:
                question_counts[norm] = question_counts.get(norm, 0) + 1
        else:
            outgoing += 1
            day_bucket["outgoing"] += 1
            channel_bucket["outgoing"] += 1
            _count_outgoing_mix(row, outgoing_mix)

        lead_id = _coerce_int(row.get("lead_id"))
        if lead_id > 0:
            per_lead.setdefault(lead_id, []).append(row)

    response_times = _response_times(per_lead)
    avg_seconds = float(sum(response_times) / len(response_times)) if response_times else 0.0
    median_seconds = float(statistics.median(response_times)) if response_times else 0.0
    top_questions = [
        {"text": text, "count": count}
        for text, count in sorted(question_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
    ]
    by_day_list = [
        {"date": day, "incoming": vals["incoming"], "outgoing": vals["outgoing"]}
        for day, vals in sorted(by_day.items(), key=lambda kv: kv[0])
        if day != "unknown"
    ]
    return {
        "ok": True,
        "period_days": int(days),
        "messages": {
            "incoming": incoming,
            "outgoing": outgoing,
            "by_day": by_day_list,
            "by_channel": by_channel,
        },
        "response_time": {
            "avg_seconds": round(avg_seconds, 1),
            "median_seconds": round(median_seconds, 1),
            "samples": len(response_times),
        },
        "outgoing_mix": outgoing_mix,
        "top_questions": top_questions,
    }


def _empty_summary(days: int) -> dict[str, Any]:
    return {
        "ok": True,
        "period_days": int(days),
        "messages": {"incoming": 0, "outgoing": 0, "by_day": [], "by_channel": {}},
        "response_time": {"avg_seconds": 0, "median_seconds": 0, "samples": 0},
        "outgoing_mix": {"bot": 0, "manager": 0, "followup": 0},
        "top_questions": [],
    }


def _normalize_question_text(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Zа-яА-Я0-9\\s]", " ", text or "")
    cleaned = re.sub(r"\\s+", " ", cleaned).strip().lower()
    return cleaned


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _day_key(created: Any) -> str:
    if isinstance(created, datetime):
        return created.date().isoformat()
    if isinstance(created, str):
        try:
            return datetime.fromisoformat(created).date().isoformat()
        except Exception:
            return ""
    return ""


def _buckets(
    row: dict[str, Any],
    by_day: dict[str, dict[str, int]],
    by_channel: dict[str, dict[str, int]],
) -> tuple[dict[str, int], dict[str, int]]:
    day_key = _day_key(row.get("created_at")) or "unknown"
    day_bucket = by_day.setdefault(day_key, {"incoming": 0, "outgoing": 0})
    channel = (row.get("channel") or "unknown").lower()
    channel_bucket = by_channel.setdefault(channel, {"incoming": 0, "outgoing": 0})
    return day_bucket, channel_bucket


def _count_outgoing_mix(row: dict[str, Any], outgoing_mix: dict[str, int]) -> None:
    src = str(row.get("source") or "").lower()
    is_bot = bool(row.get("is_bot"))
    if src == "followup":
        outgoing_mix["followup"] += 1
    elif src == "manager" or not is_bot:
        outgoing_mix["manager"] += 1
    else:
        outgoing_mix["bot"] += 1


def _as_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _response_times(per_lead: dict[int, list[dict[str, Any]]]) -> list[float]:
    response_times: list[float] = []
    for lead_rows in per_lead.values():
        lead_rows_sorted = sorted(
            lead_rows,
            key=lambda row: (row.get("created_at") or datetime.min, row.get("id") or 0),
        )
        for idx, item in enumerate(lead_rows_sorted):
            if int(item.get("direction") or 0) != 0:
                continue
            base_time = _as_datetime(item.get("created_at"))
            if not base_time:
                continue
            next_out = _next_outgoing_time(lead_rows_sorted[idx + 1:], base_time)
            if next_out:
                delta = (next_out - base_time).total_seconds()
                if 0 <= delta <= 24 * 3600:
                    response_times.append(delta)
    return response_times


def _next_outgoing_time(rows: list[dict[str, Any]], base_time: datetime) -> datetime | None:
    for row in rows:
        if int(row.get("direction") or 0) != 1:
            continue
        out_time = _as_datetime(row.get("created_at"))
        if out_time and out_time >= base_time:
            return out_time
    return None
