from __future__ import annotations

import json
import logging
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)

_SCHEMA_READY = False


async def ensure_schema() -> None:
    """Ensure the crm_outbox table exists (idempotent)."""

    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("crm_outbox_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS crm_outbox (
          id           BIGSERIAL PRIMARY KEY,
          tenant_id    INTEGER NOT NULL,
          provider     TEXT NOT NULL,
          lead_id      BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
          event_type   TEXT NOT NULL,
          payload      JSONB NOT NULL,
          attempts     INTEGER NOT NULL DEFAULT 0,
          next_retry_at TIMESTAMPTZ,
          last_error   TEXT,
          status       TEXT NOT NULL DEFAULT 'pending',
          created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_crm_outbox_status_retry ON crm_outbox(status, next_retry_at)",
        "CREATE INDEX IF NOT EXISTS idx_crm_outbox_tenant_created ON crm_outbox(tenant_id, created_at DESC)",
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception("crm_outbox_ensure_failed statement=%s", stmt.strip().split("\n", 1)[0])
            raise
    _SCHEMA_READY = True

def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    if isinstance(row, dict):
        return dict(row)
    if isinstance(row, Mapping):
        return dict(row.items())
    try:
        return dict(row)
    except Exception:
        return None


async def enqueue(
    tenant_id: int,
    provider: str,
    lead_id: int,
    event_type: str,
    payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    payload_json = json.dumps(payload, ensure_ascii=False)
    try:
        row = await fetchrow(
            """
            INSERT INTO crm_outbox (
              tenant_id,
              provider,
              lead_id,
              event_type,
              payload,
              created_at,
              updated_at
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, now(), now())
            RETURNING id,
                      tenant_id,
                      provider,
                      lead_id,
                      event_type,
                      payload,
                      attempts,
                      next_retry_at,
                      last_error,
                      status,
                      created_at,
                      updated_at
            """,
            int(tenant_id),
            str(provider),
            int(lead_id),
            str(event_type),
            payload_json,
        )
    except Exception:
        logger.exception(
            "crm_outbox_enqueue_failed tenant_id=%s lead_id=%s provider=%s event=%s",
            tenant_id,
            lead_id,
            provider,
            event_type,
        )
        raise
    return _row_to_dict(row)


async def has_recent_event(
    tenant_id: int,
    provider: str,
    lead_id: int,
    event_type: str,
    payload: Mapping[str, Any],
    *,
    window_seconds: int = 120,
) -> bool:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return False
    payload_json = json.dumps(payload, ensure_ascii=False)
    try:
        row = await fetchrow(
            """
            SELECT 1
            FROM crm_outbox
            WHERE tenant_id = $1
              AND provider = $2
              AND lead_id = $3
              AND event_type = $4
              AND payload = $5::jsonb
              AND created_at >= now() - make_interval(secs => $6)
            LIMIT 1
            """,
            int(tenant_id),
            str(provider),
            int(lead_id),
            str(event_type),
            payload_json,
            int(window_seconds),
        )
    except Exception:
        logger.exception(
            "crm_outbox_has_recent_failed tenant_id=%s lead_id=%s provider=%s event=%s",
            tenant_id,
            lead_id,
            provider,
            event_type,
        )
        return False
    return bool(row)


async def take_pending(limit: int = 10) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch_fn = getattr(db_module, "_fetch", None)
    if not fetch_fn:
        return []
    try:
        rows = await fetch_fn(
            """
            WITH next AS (
                SELECT id
                FROM crm_outbox
                WHERE status = 'pending'
                  AND (next_retry_at IS NULL OR next_retry_at <= now())
                ORDER BY created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE crm_outbox
                SET status = 'processing',
                    updated_at = now()
                WHERE id IN (SELECT id FROM next)
                RETURNING id,
                          tenant_id,
                          provider,
                          lead_id,
                          event_type,
                          payload,
                          attempts,
                          next_retry_at,
                          last_error,
                          status,
                          created_at,
                          updated_at
            )
            SELECT * FROM updated
            """,
            limit,
        )
    except Exception:
        logger.exception("crm_outbox_take_pending_failed")
        raise
    result: list[dict[str, Any]] = []
    for row in rows or []:
        parsed = _row_to_dict(row)
        if parsed:
            result.append(parsed)
    return result


async def mark_done(event_id: int) -> None:
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    try:
        await exec_fn(
            """
            UPDATE crm_outbox
            SET status = 'done',
                updated_at = now(),
                last_error = NULL
            WHERE id = $1
            """,
            int(event_id),
        )
    except Exception:
        logger.exception("crm_outbox_mark_done_failed event_id=%s", event_id)


async def mark_retry(
    event_id: int,
    attempts: int,
    next_retry_at: Any,
    error: str | None,
) -> None:
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    try:
        await exec_fn(
            """
            UPDATE crm_outbox
            SET status = 'pending',
                attempts = $2,
                next_retry_at = $3,
                last_error = left($4, 2000),
                updated_at = now()
            WHERE id = $1
            """,
            int(event_id),
            int(attempts),
            next_retry_at,
            error or "",
        )
    except Exception:
        logger.exception("crm_outbox_mark_retry_failed event_id=%s", event_id)


async def mark_dead(event_id: int, error: str | None) -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    try:
        await exec_fn(
            """
            UPDATE crm_outbox
            SET status = 'dead',
                last_error = left($2, 2000),
                updated_at = now()
            WHERE id = $1
            """,
            int(event_id),
            error or "",
        )
    except Exception:
        logger.exception("crm_outbox_mark_dead_failed event_id=%s", event_id)


__all__ = ["enqueue", "take_pending", "mark_done", "mark_retry", "mark_dead", "ensure_schema"]
