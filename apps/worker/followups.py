"""Follow-up scheduler and dispatcher using Redis."""

from __future__ import annotations

import asyncio
import json
import logging
import os
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
from libs.core.common import OUTBOX_QUEUE_KEY

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

FOLLOWUP_ZSET = "followup:schedule"
FOLLOWUP_JOB_PREFIX = "followup:job"
FOLLOWUP_SENT_PREFIX = "followup:sent"
FOLLOWUP_SCHEDULED_PREFIX = "followup:scheduled"

# Loop tuning
POLL_INTERVAL = max(0.5, float(os.getenv("FOLLOWUP_POLL_INTERVAL", "2.0")))
BATCH_LIMIT = max(1, int(os.getenv("FOLLOWUP_BATCH_LIMIT", "20")))
SCHEDULE_DEDUP_TTL = max(300, int(os.getenv("FOLLOWUP_SCHEDULE_DEDUP_TTL", "86400")))
SENT_DEDUP_TTL = max(900, int(os.getenv("FOLLOWUP_SENT_DEDUP_TTL", "86400")))
RETRY_DELAY_SECONDS = max(60, int(os.getenv("FOLLOWUP_RETRY_DELAY", "300")))

r = redis.from_url(REDIS_URL, decode_responses=True)
log = logging.getLogger("followups")

_rules_cache: Dict[int, Tuple[float, List[dict]]] = {}
_RULES_CACHE_TTL = 30.0


def _now() -> float:
    return time.time()


def _valid_rule(rule: Mapping[str, Any]) -> Optional[dict]:
    if not isinstance(rule, Mapping):
        return None
    channel = str(rule.get("channel") or "").strip().lower() or "any"
    if channel not in {"any", "*", "whatsapp", "telegram", "avito"}:
        channel = "any"
    try:
        delay_minutes = int(rule.get("delay_minutes") or 0)
    except Exception:
        delay_minutes = 0
    if delay_minutes <= 0:
        return None
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
    return {
        "channel": channel,
        "delay_minutes": delay_minutes,
        "text": text_value,
        "max_attempts": max_attempts,
    }


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
    rules = _load_rules(tenant_id)
    if not rules:
        return
    channel_norm = (incoming_channel or "").strip().lower()
    pipe = r.pipeline()
    now_ts = _now()
    for idx, rule in enumerate(rules):
        rule_channel = rule.get("channel") or "any"
        if rule_channel not in {channel_norm, "any", "*"}:
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
            "channel": rule_channel if rule_channel not in {"any", "*"} else channel_norm or "whatsapp",
            "text": rule["text"],
            "schedule_at": str(int(schedule_at)),
            "rule_id": str(idx),
            "attempts": "0",
            "max_attempts": str(int(rule["max_attempts"])),
            "source_channel": channel_norm or "",
        }
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


async def _process_job(job_id: str) -> None:
    job = await _fetch_job(job_id)
    if not job:
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
    payload, err = await _resolve_target(job)
    if err or not payload:
        await _retry_later(job, err or "resolve_failed")
        return
    try:
        await r.lpush(OUTBOX_QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        await _retry_later(job, f"enqueue_error:{exc}")
        return
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
