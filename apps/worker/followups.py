"""Follow-up scheduler and dispatcher using Redis."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Tuple

import redis.asyncio as redis

from libs.core.sales_core import read_tenant_config
from libs.core.db import (
    get_contact_phone_by_lead,
    get_lead_peer,
    get_telegram_user_id_by_lead,
)
from libs.core.common import (
    HANDOFF_SILENCE_TTL_SECONDS,
    OUTBOX_QUEUE_KEY,
    handoff_silence_key,
    handoff_silence_meta_key,
)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

FOLLOWUP_ZSET = "followup:schedule"
FOLLOWUP_JOB_PREFIX = "followup:job"
FOLLOWUP_SENT_PREFIX = "followup:sent"
FOLLOWUP_SCHEDULED_PREFIX = "followup:scheduled"
FOLLOWUP_FACT_PREFIX = "followup:fact"
FOLLOWUP_PENDING_PREFIX = "followup:pending"
FOLLOWUP_OPTOUT_PREFIX = "followup:optout"
FOLLOWUP_STOP_NOTICE_PREFIX = "followup:stop_notice"

# Loop tuning
POLL_INTERVAL = max(0.5, float(os.getenv("FOLLOWUP_POLL_INTERVAL", "2.0")))
BATCH_LIMIT = max(1, int(os.getenv("FOLLOWUP_BATCH_LIMIT", "20")))
SCHEDULE_DEDUP_TTL = max(300, int(os.getenv("FOLLOWUP_SCHEDULE_DEDUP_TTL", "86400")))
SENT_DEDUP_TTL = max(900, int(os.getenv("FOLLOWUP_SENT_DEDUP_TTL", "86400")))
RETRY_DELAY_SECONDS = max(60, int(os.getenv("FOLLOWUP_RETRY_DELAY", "300")))
FACT_TTL_SECONDS = max(3600, int(os.getenv("FOLLOWUP_FACT_TTL_SECONDS", str(90 * 86400))))
CAPTURE_TTL_SECONDS = max(300, int(os.getenv("FOLLOWUP_CAPTURE_TTL_SECONDS", str(14 * 86400))))
FUZZY_MAX_DISTANCE = max(0, int(os.getenv("FOLLOWUP_FUZZY_MAX_DISTANCE", "1")))
FACT_KEY_MAX_LEN = 64
OPTOUT_TTL_SECONDS = max(3600, int(os.getenv("FOLLOWUP_OPTOUT_TTL_SECONDS", str(365 * 86400))))

STOP_TOKENS = {
    "stop",
    "стоп",
    "отписка",
    "отписаться",
    "unsubscribe",
    "стоп.",
    "стоп!",
}

r = redis.from_url(REDIS_URL, decode_responses=True)
log = logging.getLogger("followups")

_rules_cache: Dict[int, Tuple[float, List[dict]]] = {}
_RULES_CACHE_TTL = 30.0
_ANY_CHANNELS = {"any", "*"}
_MAX_CHANNELS = {"max", "max_personal"}


def _now() -> float:
    return time.time()


def _normalize_fact_key(raw: Any) -> str:
    key = str(raw or "").strip().lower()
    if not key:
        return ""
    key = re.sub(r"\s+", "_", key)
    if FACT_KEY_MAX_LEN and len(key) > FACT_KEY_MAX_LEN:
        key = key[:FACT_KEY_MAX_LEN]
    return key


def _normalize_channel(raw: Any) -> str:
    channel = str(raw or "").strip().lower()
    if not channel:
        return ""
    channel = channel.replace("-", "_").replace(" ", "_")
    aliases = {
        "tg": "telegram",
        "wa": "whatsapp",
        "maxpersonal": "max_personal",
        "max_personal_qr": "max_personal",
        "max_qr": "max_personal",
    }
    return aliases.get(channel, channel)


def _channel_matches_rule(rule_channel: Any, actual_channel: Any) -> bool:
    rule_norm = _normalize_channel(rule_channel) or "any"
    actual_norm = _normalize_channel(actual_channel)
    if rule_norm in _ANY_CHANNELS:
        return True
    if not actual_norm:
        return False
    if rule_norm == actual_norm:
        return True
    # UI and tenant configs use "MAX", while QR transport is "max_personal".
    # Treat them as one product channel for follow-up applicability only.
    return rule_norm in _MAX_CHANNELS and actual_norm in _MAX_CHANNELS


def _job_channel_for_rule(rule_channel: Any, actual_channel: Any) -> str:
    rule_norm = _normalize_channel(rule_channel) or "any"
    actual_norm = _normalize_channel(actual_channel)
    if rule_norm in _ANY_CHANNELS:
        return actual_norm or "whatsapp"
    if rule_norm in _MAX_CHANNELS and actual_norm in _MAX_CHANNELS:
        return actual_norm
    return rule_norm


def _normalize_phrase_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    items: List[str] = []
    if isinstance(raw, str):
        items = re.split(r"[\n,]+", raw)
    elif isinstance(raw, (list, tuple, set)):
        for entry in raw:
            if entry is None:
                continue
            items.append(str(entry))
    else:
        items = [str(raw)]
    cleaned: List[str] = []
    for item in items:
        token = str(item or "").strip().lower()
        if token:
            cleaned.append(token)
    return cleaned


def _normalize_condition(raw: Any) -> List[dict]:
    if not raw:
        return []
    if isinstance(raw, Mapping):
        raw_list = [raw]
    elif isinstance(raw, list):
        raw_list = raw
    else:
        return []
    normalized: List[dict] = []
    op_aliases = {"=": "eq", "==": "eq", "!=": "neq", "<>": "neq"}
    allowed_ops = {"eq", "neq", "exists", "not_exists", "in", "not_in"}
    for cond in raw_list:
        if not isinstance(cond, Mapping):
            continue
        key = _normalize_fact_key(cond.get("key") or cond.get("fact"))
        if not key:
            continue
        op_raw = str(cond.get("op") or cond.get("operator") or "eq").strip().lower()
        op = op_aliases.get(op_raw, op_raw)
        if op not in allowed_ops:
            op = "eq"
        if op in {"exists", "not_exists"}:
            normalized.append({"key": key, "op": op})
            continue
        value = cond.get("value")
        if op in {"in", "not_in"}:
            values = _normalize_phrase_list(value)
            if not values:
                continue
            normalized.append({"key": key, "op": op, "value": values})
            continue
        if value is None:
            continue
        value_str = str(value).strip().lower()
        if not value_str:
            continue
        normalized.append({"key": key, "op": op, "value": value_str})
    return normalized


def _normalize_capture(raw: Any) -> Optional[dict]:
    if not isinstance(raw, Mapping):
        return None
    key = _normalize_fact_key(raw.get("key") or raw.get("fact"))
    if not key:
        return None
    yes_tokens = _normalize_phrase_list(raw.get("yes") or raw.get("yes_tokens"))
    no_tokens = _normalize_phrase_list(raw.get("no") or raw.get("no_tokens"))
    if not yes_tokens and not no_tokens:
        yes_tokens = ["да"]
        no_tokens = ["нет"]
    value_yes = str(raw.get("value_yes") or raw.get("valueYes") or "yes").strip().lower()
    value_no = str(raw.get("value_no") or raw.get("valueNo") or "no").strip().lower()
    return {
        "key": key,
        "yes": yes_tokens,
        "no": no_tokens,
        "value_yes": value_yes,
        "value_no": value_no,
    }


def _normalize_match_text(text: str) -> str:
    cleaned = re.sub(r"[^\w\s]+", " ", text.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return f" {cleaned} "


def _normalize_match_token(token: str) -> str:
    cleaned = re.sub(r"[^\w\s]+", " ", token.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _levenshtein_limit(a: str, b: str, max_dist: int) -> int:
    if a == b:
        return 0
    if max_dist <= 0:
        return max_dist + 1
    if abs(len(a) - len(b)) > max_dist:
        return max_dist + 1
    if len(a) > len(b):
        a, b = b, a
    previous = list(range(len(a) + 1))
    for i, ch_b in enumerate(b, 1):
        current = [i]
        min_row = current[0]
        for j, ch_a in enumerate(a, 1):
            insert_cost = current[j - 1] + 1
            delete_cost = previous[j] + 1
            replace_cost = previous[j - 1] + (ch_a != ch_b)
            cost = insert_cost
            if delete_cost < cost:
                cost = delete_cost
            if replace_cost < cost:
                cost = replace_cost
            current.append(cost)
            if cost < min_row:
                min_row = cost
        if min_row > max_dist:
            return max_dist + 1
        previous = current
    return previous[-1]


def _token_matches(text: str, token: str) -> bool:
    cleaned = _normalize_match_token(token)
    if not cleaned:
        return False
    if " " in cleaned:
        return cleaned in text
    if len(cleaned) <= 3:
        return f" {cleaned} " in text
    if cleaned in text:
        return True
    if FUZZY_MAX_DISTANCE <= 0:
        return False
    words = text.strip().split()
    for word in words:
        if abs(len(word) - len(cleaned)) > FUZZY_MAX_DISTANCE:
            continue
        if _levenshtein_limit(word, cleaned, FUZZY_MAX_DISTANCE) <= FUZZY_MAX_DISTANCE:
            return True
    return False


def _is_stop_text(text: str) -> bool:
    cleaned = _normalize_match_text(text or "")
    if not cleaned.strip():
        return False
    for token in STOP_TOKENS:
        if _token_matches(cleaned, token):
            return True
    return False


def _fact_key(tenant_id: int, lead_id: int, fact_key: str) -> str:
    return f"{FOLLOWUP_FACT_PREFIX}:{tenant_id}:{lead_id}:{fact_key}"


def _optout_key(tenant_id: int, lead_id: int) -> str:
    return f"{FOLLOWUP_OPTOUT_PREFIX}:{tenant_id}:{lead_id}"


def _stop_notice_key(tenant_id: int, lead_id: int) -> str:
    return f"{FOLLOWUP_STOP_NOTICE_PREFIX}:{tenant_id}:{lead_id}"


async def is_opted_out(tenant_id: int, lead_id: int) -> bool:
    if tenant_id <= 0 or lead_id <= 0:
        return False
    try:
        return bool(await r.get(_optout_key(tenant_id, lead_id)))
    except Exception:
        return False


async def handle_opt_out(tenant_id: int, lead_id: int, text: str) -> bool:
    if tenant_id <= 0 or lead_id <= 0:
        return False
    if not _is_stop_text(text):
        return False
    try:
        await r.set(_optout_key(tenant_id, lead_id), "1", ex=OPTOUT_TTL_SECONDS)
    except Exception:
        pass
    try:
        timestamp = int(time.time())
        await r.set(
            handoff_silence_key(int(tenant_id), int(lead_id)),
            str(timestamp),
            ex=OPTOUT_TTL_SECONDS,
        )
        meta_key = handoff_silence_meta_key(int(tenant_id), int(lead_id))
        if meta_key:
            payload = {"reason": "opt_out", "ts": timestamp}
            await r.set(meta_key, json.dumps(payload, ensure_ascii=False), ex=OPTOUT_TTL_SECONDS)
    except Exception:
        pass
    return True


def _pending_key(tenant_id: int, lead_id: int) -> str:
    return f"{FOLLOWUP_PENDING_PREFIX}:{tenant_id}:{lead_id}"


async def _get_fact(tenant_id: int, lead_id: int, fact_key: str) -> Optional[str]:
    try:
        raw = await r.get(_fact_key(tenant_id, lead_id, fact_key))
    except Exception:
        raw = None
    if raw is None:
        return None
    text = str(raw).strip().lower()
    return text if text else None


async def _set_fact(tenant_id: int, lead_id: int, fact_key: str, value: str) -> None:
    if tenant_id <= 0 or lead_id <= 0 or not fact_key:
        return
    try:
        await r.set(_fact_key(tenant_id, lead_id, fact_key), value, ex=FACT_TTL_SECONDS)
    except Exception:
        pass


async def _set_pending_capture(
    tenant_id: int,
    lead_id: int,
    capture: Mapping[str, Any],
    *,
    capture_id: Optional[str] = None,
) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    key = _pending_key(tenant_id, lead_id)
    field = capture_id or uuid.uuid4().hex
    payload = json.dumps(dict(capture), ensure_ascii=False)
    try:
        pipe = r.pipeline()
        pipe.hset(key, mapping={field: payload})
        pipe.expire(key, CAPTURE_TTL_SECONDS)
        await pipe.execute()
    except Exception:
        pass


async def capture_followup_answer(tenant_id: int, lead_id: int, text: str, channel: str) -> bool:
    if tenant_id <= 0 or lead_id <= 0 or not text:
        return False
    pending_key = _pending_key(tenant_id, lead_id)
    try:
        pending = await r.hgetall(pending_key)
    except Exception:
        pending = {}
    if not pending:
        return False
    normalized_text = _normalize_match_text(text)
    for capture_id, payload in pending.items():
        try:
            raw_spec = json.loads(payload) if payload else {}
        except Exception:
            raw_spec = {}
        capture = _normalize_capture(raw_spec)
        if not capture:
            continue
        yes_tokens = capture.get("yes") or []
        no_tokens = capture.get("no") or []
        yes_match = any(_token_matches(normalized_text, token) for token in yes_tokens)
        no_match = any(_token_matches(normalized_text, token) for token in no_tokens)
        if yes_match and not no_match:
            value = str(capture.get("value_yes") or "yes").strip().lower()
        elif no_match and not yes_match:
            value = str(capture.get("value_no") or "no").strip().lower()
        else:
            continue
        fact_key = capture.get("key") or ""
        await _set_fact(tenant_id, lead_id, str(fact_key), value)
        try:
            await r.hdel(pending_key, capture_id)
            remaining = await r.hlen(pending_key)
            if remaining <= 0:
                await r.delete(pending_key)
        except Exception:
            pass
        log.info(
            "event=followup_fact_set tenant=%s lead_id=%s key=%s value=%s",
            tenant_id,
            lead_id,
            fact_key,
            value,
        )
        try:
            await _trigger_followups_on_answer(
                tenant_id,
                lead_id,
                channel=channel,
                fact_key=str(fact_key),
            )
        except Exception:
            pass
        return True
    return False

def _valid_rule(rule: Mapping[str, Any]) -> Optional[dict]:
    if not isinstance(rule, Mapping):
        return None
    channel = _normalize_channel(rule.get("channel")) or "any"
    if channel not in {"any", "*", "whatsapp", "telegram", "avito", "max", "max_personal"}:
        channel = "any"
    try:
        delay_minutes = int(rule.get("delay_minutes") or 0)
    except Exception:
        delay_minutes = 0
    text_value = str(rule.get("text") or "").strip()
    if not text_value:
        return None
    try:
        max_attempts = int(rule.get("max_attempts") or 1)
    except Exception:
        max_attempts = 1
    if max_attempts < 1:
        max_attempts = 1
    active = bool(rule.get("active", True))
    if not active:
        return None
    trigger_on_answer = bool(rule.get("trigger_on_answer"))
    if delay_minutes <= 0 and not trigger_on_answer:
        return None
    condition_list = _normalize_condition(rule.get("condition"))
    condition: Optional[dict | list] = None
    if condition_list:
        condition = condition_list[0] if len(condition_list) == 1 else condition_list
    capture = _normalize_capture(rule.get("capture"))
    stop_notice_after = bool(rule.get("stop_notice_after"))
    normalized = {
        "channel": channel,
        "delay_minutes": delay_minutes if delay_minutes > 0 else 0,
        "text": text_value,
        "max_attempts": max_attempts,
    }
    if trigger_on_answer:
        normalized["trigger_on_answer"] = True
    if condition:
        normalized["condition"] = condition
    if capture:
        normalized["capture"] = capture
    if stop_notice_after:
        normalized["stop_notice_after"] = True
    return normalized


def _load_rules(tenant_id: int) -> List[dict]:
    now_ts = _now()
    cached = _rules_cache.get(tenant_id)
    if cached and now_ts - cached[0] <= _RULES_CACHE_TTL:
        return cached[1]
    cfg = read_tenant_config(tenant_id)
    rules_raw = cfg.get("follow_up") if isinstance(cfg, Mapping) else []
    valid: List[dict] = []
    if isinstance(rules_raw, list):
        for rule in rules_raw:
            normalized = _valid_rule(rule)
            if normalized:
                valid.append(normalized)
    _rules_cache[tenant_id] = (now_ts, valid)
    return valid


async def schedule_followups(tenant_id: int, lead_id: int, incoming_channel: str) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    if await is_opted_out(tenant_id, lead_id):
        return
    rules = _load_rules(tenant_id)
    if not rules:
        return
    channel_norm = _normalize_channel(incoming_channel)
    pipe = r.pipeline()
    now_ts = _now()
    for idx, rule in enumerate(rules):
        if rule.get("trigger_on_answer"):
            continue
        rule_channel = rule.get("channel") or "any"
        if not _channel_matches_rule(rule_channel, channel_norm):
            continue
        dedup_key = f"{FOLLOWUP_SCHEDULED_PREFIX}:{tenant_id}:{lead_id}:{idx}"
        already = await r.get(dedup_key)
        if already:
            continue
        schedule_at = now_ts + rule["delay_minutes"] * 60
        job_id = uuid.uuid4().hex
        job_key = f"{FOLLOWUP_JOB_PREFIX}:{job_id}"
        job_payload = {
            "id": job_id,
            "tenant_id": str(int(tenant_id)),
            "lead_id": str(int(lead_id)),
            "channel": _job_channel_for_rule(rule_channel, channel_norm),
            "text": rule["text"],
            "schedule_at": str(int(schedule_at)),
            "rule_id": str(idx),
            "attempts": "0",
            "max_attempts": str(int(rule["max_attempts"])),
            "source_channel": channel_norm or "",
        }
        if rule.get("stop_notice_after"):
            job_payload["stop_notice_after"] = "1"
        condition = rule.get("condition")
        if condition:
            job_payload["condition"] = json.dumps(condition, ensure_ascii=False)
        capture = rule.get("capture")
        if capture:
            job_payload["capture"] = json.dumps(capture, ensure_ascii=False)
        pipe.hset(job_key, mapping=job_payload)
        pipe.zadd(FOLLOWUP_ZSET, {job_id: schedule_at})
        ttl_seconds = int(rule["delay_minutes"] * 60 + SCHEDULE_DEDUP_TTL)
        pipe.set(dedup_key, "1", ex=ttl_seconds)
    try:
        await pipe.execute()
    except Exception as exc:
        log.warning(
            "event=followup_schedule_failed tenant=%s lead_id=%s error=%s", tenant_id, lead_id, exc
        )


async def _take_due_jobs(limit: int) -> List[Tuple[str, float]]:
    now_ts = _now()
    try:
        ids = await r.zrangebyscore(FOLLOWUP_ZSET, "-inf", now_ts, 0, limit)
    except Exception:
        return []
    if not ids:
        return []
    pipe = r.pipeline()
    for job_id in ids:
        pipe.zrem(FOLLOWUP_ZSET, job_id)
    try:
        await pipe.execute()
    except Exception:
        # Best-effort: even if removal failed, return nothing to avoid double-processing.
        return []
    return [(str(job_id), now_ts) for job_id in ids]


async def _fetch_job(job_id: str) -> Optional[Dict[str, Any]]:
    job_key = f"{FOLLOWUP_JOB_PREFIX}:{job_id}"
    try:
        data = await r.hgetall(job_key)
    except Exception:
        data = {}
    if not data:
        return None
    try:
        data["tenant_id"] = int(data.get("tenant_id") or 0)
    except Exception:
        data["tenant_id"] = 0
    try:
        data["lead_id"] = int(data.get("lead_id") or 0)
    except Exception:
        data["lead_id"] = 0
    try:
        data["attempts"] = int(data.get("attempts") or 0)
    except Exception:
        data["attempts"] = 0
    try:
        data["max_attempts"] = int(data.get("max_attempts") or 1)
    except Exception:
        data["max_attempts"] = 1
    try:
        data["rule_id"] = int(data.get("rule_id") or 0)
    except Exception:
        data["rule_id"] = 0
    raw_stop_notice = data.get("stop_notice_after")
    if isinstance(raw_stop_notice, str):
        data["stop_notice_after"] = raw_stop_notice.strip().lower() in {"1", "true", "yes", "on"}
    for field in ("condition", "capture"):
        raw = data.get(field)
        if isinstance(raw, str) and raw.strip():
            try:
                data[field] = json.loads(raw)
            except Exception:
                data[field] = None
    return data


async def _resolve_target(job: Mapping[str, Any]) -> Tuple[Optional[dict], Optional[str]]:
    channel = (job.get("channel") or "").strip().lower() or "whatsapp"
    tenant_id = int(job.get("tenant_id") or 0)
    lead_id = int(job.get("lead_id") or 0)
    if channel == "telegram":
        chat_id = await get_telegram_user_id_by_lead(lead_id)
        if not chat_id:
            return None, "missing_telegram_user"
        payload = {
            "lead_id": lead_id,
            "tenant": tenant_id,
            "tenant_id": tenant_id,
            "channel": "telegram",
            "ch": "telegram",
            "text": job.get("text") or "",
            "peer": chat_id,
            "peer_id": chat_id,
            "telegram_user_id": chat_id,
            "origin": "followup",
        }
        return payload, None
    if channel == "avito":
        chat_id = await get_lead_peer(lead_id, channel="avito")
        if not chat_id:
            return None, "missing_chat"
        payload = {
            "lead_id": lead_id,
            "tenant": tenant_id,
            "tenant_id": tenant_id,
            "channel": "avito",
            "ch": "avito",
            "text": job.get("text") or "",
            "peer": chat_id,
            "peer_id": chat_id,
            "chat_id": chat_id,
            "origin": "followup",
        }
        return payload, None
    if channel == "max":
        chat_id = await get_lead_peer(lead_id, channel="max")
        if not chat_id:
            return None, "missing_chat"
        payload = {
            "lead_id": lead_id,
            "tenant": tenant_id,
            "tenant_id": tenant_id,
            "channel": "max",
            "ch": "max",
            "text": job.get("text") or "",
            "peer": chat_id,
            "peer_id": chat_id,
            "chat_id": chat_id,
            "origin": "followup",
        }
        return payload, None
    if channel == "max_personal":
        chat_id = await get_lead_peer(lead_id, channel="max_personal")
        if not chat_id:
            return None, "missing_chat"
        payload = {
            "lead_id": lead_id,
            "tenant": tenant_id,
            "tenant_id": tenant_id,
            "channel": "max_personal",
            "ch": "max_personal",
            "text": job.get("text") or "",
            "peer": chat_id,
            "peer_id": chat_id,
            "chat_id": chat_id,
            "origin": "followup",
        }
        return payload, None
    # Default to WhatsApp
    phone = await get_contact_phone_by_lead(lead_id)
    if not phone:
        return None, "missing_phone"
    payload = {
        "lead_id": lead_id,
        "tenant": tenant_id,
        "tenant_id": tenant_id,
        "channel": "whatsapp",
        "ch": "whatsapp",
        "text": job.get("text") or "",
        "to": phone,
        "origin": "followup",
    }
    return payload, None


async def _mark_sent(job: Mapping[str, Any]) -> None:
    tenant_id = int(job.get("tenant_id") or 0)
    lead_id = int(job.get("lead_id") or 0)
    rule_id = job.get("rule_id") or 0
    try:
        key = f"{FOLLOWUP_SENT_PREFIX}:{tenant_id}:{lead_id}:{rule_id}"
        await r.set(key, "1", ex=SENT_DEDUP_TTL)
    except Exception:
        pass
    try:
        await r.delete(f"{FOLLOWUP_JOB_PREFIX}:{job.get('id')}")
    except Exception:
        pass


async def _maybe_send_stop_notice(job: Mapping[str, Any], payload: Mapping[str, Any]) -> None:
    if not job.get("stop_notice_after"):
        tenant_id = int(job.get("tenant_id") or 0)
        if tenant_id <= 0:
            return
        rules = _load_rules(tenant_id)
        if any(rule.get("stop_notice_after") for rule in rules):
            return
    tenant_id = int(job.get("tenant_id") or 0)
    lead_id = int(job.get("lead_id") or 0)
    if tenant_id <= 0 or lead_id <= 0:
        return
    try:
        notice_key = _stop_notice_key(tenant_id, lead_id)
        if await r.get(notice_key):
            return
        await r.set(notice_key, "1", ex=SENT_DEDUP_TTL)
    except Exception:
        return
    notice_text = 'Напишите "стоп", чтобы отписаться от рассылки.'
    followup_payload = dict(payload)
    followup_payload["text"] = notice_text
    followup_payload["origin"] = "followup"
    try:
        await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(followup_payload, ensure_ascii=False))
    except Exception:
        pass


async def _mute_smart_reply(tenant_id: int, lead_id: int) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    try:
        timestamp = int(time.time())
        await r.set(
            handoff_silence_key(int(tenant_id), int(lead_id)),
            str(timestamp),
            ex=HANDOFF_SILENCE_TTL_SECONDS,
        )
        meta_key = handoff_silence_meta_key(int(tenant_id), int(lead_id))
        if meta_key:
            payload = {"reason": "followup_sent", "ts": timestamp}
            await r.set(
                meta_key,
                json.dumps(payload, ensure_ascii=False),
                ex=HANDOFF_SILENCE_TTL_SECONDS,
            )
    except Exception:
        pass


async def _retry_later(job: Mapping[str, Any], reason: str) -> None:
    job_id = job.get("id")
    if not job_id:
        return
    job_key = f"{FOLLOWUP_JOB_PREFIX}:{job_id}"
    attempts = int(job.get("attempts") or 0) + 1
    try:
        await r.hset(job_key, mapping={"attempts": attempts, "last_error": reason})
        await r.zadd(FOLLOWUP_ZSET, {job_id: _now() + RETRY_DELAY_SECONDS})
    except Exception as exc:
        log.warning("event=followup_retry_failed job_id=%s error=%s", job_id, exc)


async def _condition_allows(job: Mapping[str, Any]) -> bool:
    condition = job.get("condition")
    conditions = _normalize_condition(condition)
    if not conditions:
        return True
    tenant_id = int(job.get("tenant_id") or 0)
    lead_id = int(job.get("lead_id") or 0)
    for cond in conditions:
        key = str(cond.get("key") or "").strip()
        if not key:
            continue
        fact_value = await _get_fact(tenant_id, lead_id, key)
        fact_norm = (fact_value or "").strip().lower()
        op = str(cond.get("op") or "eq").strip().lower()
        if op == "exists":
            if not fact_norm:
                return False
            continue
        if op == "not_exists":
            if fact_norm:
                return False
            continue
        if op == "eq":
            expected = str(cond.get("value") or "").strip().lower()
            if not expected or not fact_norm or fact_norm != expected:
                return False
            continue
        if op == "neq":
            expected = str(cond.get("value") or "").strip().lower()
            if expected and fact_norm == expected:
                return False
            continue
        if op == "in":
            raw_values = cond.get("value") or []
            values = {str(v).strip().lower() for v in raw_values if str(v).strip()}
            if not values or not fact_norm or fact_norm not in values:
                return False
            continue
        if op == "not_in":
            raw_values = cond.get("value") or []
            values = {str(v).strip().lower() for v in raw_values if str(v).strip()}
            if not values:
                return False
            if fact_norm and fact_norm in values:
                return False
            continue
    return True


async def _trigger_followups_on_answer(
    tenant_id: int,
    lead_id: int,
    *,
    channel: str,
    fact_key: str,
) -> None:
    if tenant_id <= 0 or lead_id <= 0:
        return
    if await is_opted_out(tenant_id, lead_id):
        return
    rules = _load_rules(tenant_id)
    if not rules:
        return
    channel_norm = _normalize_channel(channel)
    candidates: list[tuple[int, dict]] = []
    fact_key_norm = str(fact_key or "").strip().lower()
    for idx, rule in enumerate(rules):
        if not rule.get("trigger_on_answer"):
            continue
        condition = rule.get("condition")
        conditions = _normalize_condition(condition)
        if not conditions:
            continue
        if not any(str(cond.get("key") or "").strip().lower() == fact_key_norm for cond in conditions):
            continue
        rule_channel = _normalize_channel(rule.get("channel")) or "any"
        if not _channel_matches_rule(rule_channel, channel_norm):
            continue
        candidates.append((idx, rule))
    if not candidates:
        return
    candidates.sort(key=lambda item: item[0])
    for idx, rule in candidates:
        rule_channel = _normalize_channel(rule.get("channel")) or "any"
        condition = rule.get("condition")
        job_payload = {
            "tenant_id": tenant_id,
            "lead_id": lead_id,
            "channel": _job_channel_for_rule(rule_channel, channel_norm),
            "text": rule.get("text") or "",
            "rule_id": idx,
            "max_attempts": int(rule.get("max_attempts") or 1),
            "attempts": 0,
            "condition": condition,
        }
        if rule.get("stop_notice_after"):
            job_payload["stop_notice_after"] = True
        allowed = await _condition_allows(job_payload)
        if not allowed:
            continue
        sent_key = f"{FOLLOWUP_SENT_PREFIX}:{tenant_id}:{lead_id}:{idx}"
        try:
            if await r.get(sent_key):
                continue
        except Exception:
            pass
        payload, err = await _resolve_target(job_payload)
        if err or not payload:
            continue
        try:
            await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
        except Exception:
            continue
        await _mute_smart_reply(tenant_id, lead_id)
        await _maybe_send_stop_notice(job_payload, payload)
        capture = rule.get("capture")
        if isinstance(capture, Mapping):
            try:
                await _set_pending_capture(tenant_id, lead_id, capture)
            except Exception:
                pass
        try:
            await r.set(sent_key, "1", ex=SENT_DEDUP_TTL)
        except Exception:
            pass
        log.info(
            "event=followup_triggered tenant=%s lead_id=%s channel=%s rule_id=%s fact=%s",
            tenant_id,
            lead_id,
            payload.get("channel"),
            idx,
            fact_key,
        )
        return


async def _process_job(job_id: str) -> None:
    job = await _fetch_job(job_id)
    if not job:
        return
    if await is_opted_out(int(job.get("tenant_id") or 0), int(job.get("lead_id") or 0)):
        try:
            await r.delete(f"{FOLLOWUP_JOB_PREFIX}:{job_id}")
        except Exception:
            pass
        return
    if job.get("attempts", 0) >= job.get("max_attempts", 1):
        log.info(
            "event=followup_drop reason=max_attempts job_id=%s tenant=%s lead_id=%s",
            job_id,
            job.get("tenant_id"),
            job.get("lead_id"),
        )
        try:
            await r.delete(f"{FOLLOWUP_JOB_PREFIX}:{job_id}")
        except Exception:
            pass
        return
    allowed = await _condition_allows(job)
    if not allowed:
        log.info(
            "event=followup_skip_condition tenant=%s lead_id=%s rule_id=%s",
            job.get("tenant_id"),
            job.get("lead_id"),
            job.get("rule_id"),
        )
        try:
            await r.delete(f"{FOLLOWUP_JOB_PREFIX}:{job_id}")
        except Exception:
            pass
        return
    payload, err = await _resolve_target(job)
    if err or not payload:
        await _retry_later(job, err or "resolve_failed")
        return
    try:
        await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        await _retry_later(job, f"enqueue_error:{exc}")
        return
    await _mute_smart_reply(int(job.get("tenant_id") or 0), int(job.get("lead_id") or 0))
    await _maybe_send_stop_notice(job, payload)
    capture = job.get("capture")
    if isinstance(capture, Mapping):
        try:
            await _set_pending_capture(
                int(job.get("tenant_id") or 0),
                int(job.get("lead_id") or 0),
                capture,
                capture_id=str(job.get("id") or ""),
            )
        except Exception:
            pass
    await _mark_sent(job)
    log.info(
        "event=followup_enqueued tenant=%s lead_id=%s channel=%s rule_id=%s",
        job.get("tenant_id"),
        job.get("lead_id"),
        payload.get("channel"),
        job.get("rule_id"),
    )


async def run_loop() -> None:
    log.info("event=followup_loop_start interval=%s batch=%s", POLL_INTERVAL, BATCH_LIMIT)
    try:
        while True:
            jobs = await _take_due_jobs(BATCH_LIMIT)
            if not jobs:
                await asyncio.sleep(POLL_INTERVAL)
                continue
            for job_id, _ in jobs:
                try:
                    await _process_job(job_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("event=followup_job_error job_id=%s error=%s", job_id, exc)
            await asyncio.sleep(0)
    except asyncio.CancelledError:
        log.info("event=followup_loop_stop status=cancelled")
        raise
    except Exception:
        log.exception("event=followup_loop_crashed")
        raise
