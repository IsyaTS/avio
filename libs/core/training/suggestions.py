from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

from libs.core import db
from libs.core.training import utils as training_utils

_log = logging.getLogger("training")


@dataclass
class Suggestion:
    q_text: str
    a_text: str
    count: int


def _normalize(text: str) -> str:
    return training_utils.sanitize_text(text or "").lower()


def _similar(a: str, b: str, *, threshold: float = 0.86) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True
    if abs(len(a) - len(b)) > max(len(a), len(b)) * 0.45:
        return False
    return SequenceMatcher(None, a, b).ratio() >= threshold


def _pair_messages(rows: Iterable[dict[str, Any]], *, reply_window_minutes: int = 60) -> List[Tuple[str, str]]:
    by_lead_last_inbound: Dict[int, Tuple[str, datetime]] = {}
    pairs: List[Tuple[str, str]] = []
    window = timedelta(minutes=reply_window_minutes)

    for row in rows:
        try:
            lead_id = int(row.get("lead_id") or 0)
        except Exception:
            continue
        if lead_id <= 0:
            continue
        text = str(row.get("text") or "").strip()
        if not text:
            continue
        created_at = row.get("created_at")
        if not isinstance(created_at, datetime):
            try:
                created_at = datetime.fromisoformat(str(created_at))
            except Exception:
                created_at = None
        if not isinstance(created_at, datetime):
            continue
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        direction = int(row.get("direction") or 0)
        is_bot = bool(row.get("is_bot"))

        if direction == 0:
            by_lead_last_inbound[lead_id] = (text, created_at)
            continue

        if direction == 1 and not is_bot:
            inbound = by_lead_last_inbound.get(lead_id)
            if not inbound:
                continue
            q_text, q_ts = inbound
            if created_at - q_ts > window:
                continue
            pairs.append((q_text, text))
            by_lead_last_inbound.pop(lead_id, None)
    return pairs


def _cluster_questions(pairs: List[Tuple[str, str]], *, min_len: int = 6) -> List[Suggestion]:
    clusters: List[Dict[str, Any]] = []

    for q_text, a_text in pairs:
        q_norm = _normalize(q_text)
        if len(q_norm) < min_len:
            continue
        matched = None
        for cluster in clusters:
            if _similar(q_norm, cluster["norm"]):
                matched = cluster
                break
        if matched is None:
            matched = {
                "norm": q_norm,
                "questions": [],
                "answers": [],
            }
            clusters.append(matched)
        matched["questions"].append(q_text.strip())
        matched["answers"].append(a_text.strip())

    suggestions: List[Suggestion] = []
    for cluster in clusters:
        answers = [a for a in cluster["answers"] if len(a) >= 6]
        if not answers:
            continue
        answer_counts = Counter(answers)
        best_answer, _ = answer_counts.most_common(1)[0]
        q_counts = Counter(cluster["questions"])
        best_question, count = q_counts.most_common(1)[0]
        suggestions.append(Suggestion(q_text=best_question, a_text=best_answer, count=int(count)))

    return suggestions


async def build_suggestions(
    tenant_id: int,
    *,
    days: int = 7,
    limit: int = 2000,
    min_count: int = 5,
    reply_window_minutes: int = 60,
    max_items: int = 20,
) -> List[Suggestion]:
    since = datetime.now(tz=timezone.utc) - timedelta(days=max(1, days))
    rows = await db.list_recent_messages(tenant_id, since=since, limit=limit)
    if not rows:
        return []
    pairs = _pair_messages(rows, reply_window_minutes=reply_window_minutes)
    if not pairs:
        return []
    suggestions = _cluster_questions(pairs)
    filtered = [s for s in suggestions if s.count >= min_count]
    filtered.sort(key=lambda s: s.count, reverse=True)
    return filtered[: max_items or 20]


async def refresh_training_suggestions(
    tenant_id: int,
    *,
    days: int = 7,
    limit: int = 2000,
    min_count: int = 5,
    reply_window_minutes: int = 60,
    max_items: int = 20,
) -> List[Suggestion]:
    suggestions = await build_suggestions(
        tenant_id,
        days=days,
        limit=limit,
        min_count=min_count,
        reply_window_minutes=reply_window_minutes,
        max_items=max_items,
    )
    try:
        await db.delete_training_suggestions(tenant_id)
    except Exception:
        _log.warning("training_suggest_cleanup_failed tenant=%s", tenant_id)

    for suggestion in suggestions:
        source = f"auto_suggest:{suggestion.count}"
        try:
            await db.record_training_example(
                tenant_id,
                lead_id=None,
                message_id=None,
                source=source,
                source_feedback_id=None,
                q_text=suggestion.q_text,
                a_text=suggestion.a_text,
                is_bad=False,
                is_active=False,
                embedding_status="pending",
            )
        except Exception:
            _log.exception("training_suggest_save_failed tenant=%s", tenant_id)
    return suggestions

