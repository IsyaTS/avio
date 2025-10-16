"""Background outbox dispatcher."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict

from app.db import (
    bump_attempt,
    mark_failed,
    mark_sent,
    take_outbox_batch,
)
from app.providers import telegram_bot

logger = logging.getLogger("app.outbox_worker")

_POLL_INTERVAL = max(0.5, float(os.getenv("OUTBOX_POLL_INTERVAL", "1.5")))
_BATCH_LIMIT = max(1, int(os.getenv("OUTBOX_BATCH_LIMIT", "10")))
_MAX_RETRY_ATTEMPTS = max(1, int(os.getenv("OUTBOX_MAX_RETRY_ATTEMPTS", "3")))

_task: asyncio.Task | None = None


async def _process_entry(entry: Dict[str, Any]) -> None:
    lead_raw = entry.get("lead_id")
    tenant_raw = entry.get("tenant_id")
    try:
        lead_id = int(lead_raw)
    except Exception:
        lead_id = 0
    try:
        tenant_id = int(tenant_raw)
    except Exception:
        tenant_id = 0
    dedup = str(entry.get("dedup_hash") or "")
    text = str(entry.get("text") or "").strip()
    telegram_user_id = entry.get("telegram_user_id")
    try:
        attempts = int(entry.get("attempts") or 0)
    except Exception:
        attempts = 0

    if not dedup:
        logger.warning(
            "event=outbox_skip reason=missing_dedup tenant=%s lead_id=%s", tenant_id, lead_id
        )
        return

    if not text:
        logger.warning(
            "event=outbox_skip reason=empty_text tenant=%s lead_id=%s", tenant_id, lead_id
        )
        await mark_failed(lead_id, dedup, "empty_text")
        return

    if tenant_id <= 0:
        logger.warning(
            "event=outbox_skip reason=missing_tenant lead_id=%s tenant_raw=%s",
            lead_id,
            tenant_raw,
        )
        await mark_failed(lead_id, dedup, "missing_tenant")
        return

    if telegram_user_id in (None, ""):
        logger.warning(
            "event=outbox_skip reason=missing_telegram_user tenant=%s lead_id=%s", tenant_id, lead_id
        )
        await mark_failed(lead_id, dedup, "missing_telegram_user")
        return

    try:
        chat_id = int(telegram_user_id)
    except Exception:
        logger.warning(
            "event=outbox_skip reason=invalid_telegram_user tenant=%s lead_id=%s value=%s",
            tenant_id,
            lead_id,
            telegram_user_id,
        )
        await mark_failed(lead_id, dedup, "invalid_telegram_user")
        return

    logger.info(
        "event=outbox_send_attempt tenant=%s lead_id=%s chat_id=%s attempts=%s",
        tenant_id,
        lead_id,
        chat_id,
        attempts,
    )

    ok, status, error_text = await telegram_bot.send_message(
        tenant_id=tenant_id,
        telegram_user_id=chat_id,
        text=text,
    )
    if ok:
        await mark_sent(lead_id, dedup)
        logger.info(
            "event=outbox_send_success tenant=%s lead_id=%s status=%s",
            tenant_id,
            lead_id,
            status,
        )
        return

    error_text = error_text or "send_failed"
    retryable = status in {0, 429} or status >= 500
    if attempts + 1 < _MAX_RETRY_ATTEMPTS and retryable:
        await bump_attempt(lead_id, dedup, error_text)
        logger.warning(
            "event=outbox_send_retry tenant=%s lead_id=%s status=%s error=%s attempts=%s",
            tenant_id,
            lead_id,
            status,
            error_text,
            attempts + 1,
        )
        return

    await mark_failed(lead_id, dedup, error_text)
    logger.error(
        "event=outbox_send_failed tenant=%s lead_id=%s status=%s error=%s attempts=%s",
        tenant_id,
        lead_id,
        status,
        error_text,
        attempts,
    )


async def _run_loop() -> None:
    logger.info(
        "event=outbox_worker_start poll_interval=%s batch_limit=%s max_retry=%s",
        _POLL_INTERVAL,
        _BATCH_LIMIT,
        _MAX_RETRY_ATTEMPTS,
    )
    try:
        while True:
            try:
                batch = await take_outbox_batch(_BATCH_LIMIT)
            except Exception:
                logger.exception("event=outbox_fetch_failed")
                await asyncio.sleep(_POLL_INTERVAL)
                continue

            if not batch:
                await asyncio.sleep(_POLL_INTERVAL)
                continue

            for entry in batch:
                try:
                    await _process_entry(entry)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "event=outbox_entry_unhandled tenant=%s lead_id=%s",
                        entry.get("tenant_id"),
                        entry.get("lead_id"),
                    )
                    dedup = str(entry.get("dedup_hash") or "")
                    try:
                        lead_ref = int(entry.get("lead_id") or 0)
                    except Exception:
                        lead_ref = 0
                    if lead_ref and dedup:
                        try:
                            await mark_failed(lead_ref, dedup, "unhandled_exception")
                        except Exception:
                            logger.exception(
                                "event=outbox_entry_mark_failed_error lead_id=%s", lead_ref
                            )
            await asyncio.sleep(0)
    except asyncio.CancelledError:
        logger.info("event=outbox_worker_stop status=cancelled")
        raise
    except Exception:
        logger.exception("event=outbox_worker_crashed")
        raise


async def start() -> None:
    global _task
    if _task and not _task.done():
        return
    loop = asyncio.get_running_loop()
    _task = loop.create_task(_run_loop(), name="outbox-worker")


async def stop() -> None:
    global _task
    task = _task
    _task = None
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    await telegram_bot.aclose()


__all__ = ["start", "stop"]
