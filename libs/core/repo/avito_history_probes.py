from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


async def ensure_schema() -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("avito_history_probes_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS avito_history_probe_jobs (
          job_id              TEXT PRIMARY KEY,
          tenant_id           INTEGER NOT NULL,
          status              TEXT NOT NULL,
          period_from         TIMESTAMPTZ NOT NULL,
          period_to           TIMESTAMPTZ NOT NULL,
          chat_limit          INTEGER NOT NULL DEFAULT 0,
          chats_seen          INTEGER NOT NULL DEFAULT 0,
          chats_with_messages INTEGER NOT NULL DEFAULT 0,
          messages_seen       INTEGER NOT NULL DEFAULT 0,
          messages_in_period  INTEGER NOT NULL DEFAULT 0,
          oldest_message_at   TIMESTAMPTZ,
          newest_message_at   TIMESTAMPTZ,
          api_errors_summary  JSONB,
          error_code          TEXT,
          created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
          finished_at         TIMESTAMPTZ,
          updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_avito_history_probe_jobs_tenant_created
          ON avito_history_probe_jobs(tenant_id, created_at DESC)
        """,
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception(
                "avito_history_probes_ensure_failed statement=%s",
                stmt.strip().split("\n", 1)[0],
            )
            raise


def _json(value: Mapping[str, Any] | None) -> str | None:
    if not value:
        return None
    return json.dumps(dict(value), ensure_ascii=False)


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    try:
        data = dict(row)
    except Exception:
        return None
    summary = data.get("api_errors_summary")
    if isinstance(summary, str):
        try:
            data["api_errors_summary"] = json.loads(summary)
        except Exception:
            data["api_errors_summary"] = {}
    return data


async def create_job(
    *,
    job_id: str,
    tenant_id: int,
    period_from: datetime,
    period_to: datetime,
    chat_limit: int,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        logger.debug("avito_history_probes_create_skip reason=no_db")
        return {
            "job_id": job_id,
            "tenant_id": int(tenant_id),
            "status": "running",
            "period_from": period_from,
            "period_to": period_to,
            "chat_limit": int(chat_limit),
            "created_at": _now(),
        }
    row = await fetchrow(
        """
        INSERT INTO avito_history_probe_jobs (
          job_id, tenant_id, status, period_from, period_to,
          chat_limit, created_at, updated_at
        )
        VALUES ($1, $2, 'running', $3, $4, $5, $6, $6)
        RETURNING *
        """,
        str(job_id),
        int(tenant_id),
        period_from,
        period_to,
        int(chat_limit),
        _now(),
    )
    return _row_to_dict(row)


async def finish_job(
    *,
    job_id: str,
    status: str,
    chats_seen: int = 0,
    chats_with_messages: int = 0,
    messages_seen: int = 0,
    messages_in_period: int = 0,
    oldest_message_at: datetime | None = None,
    newest_message_at: datetime | None = None,
    api_errors_summary: Mapping[str, Any] | None = None,
    error_code: str | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    now = _now()
    if not fetchrow:
        logger.debug("avito_history_probes_finish_skip reason=no_db")
        return {
            "job_id": job_id,
            "status": status,
            "chats_seen": int(chats_seen),
            "chats_with_messages": int(chats_with_messages),
            "messages_seen": int(messages_seen),
            "messages_in_period": int(messages_in_period),
            "oldest_message_at": oldest_message_at,
            "newest_message_at": newest_message_at,
            "api_errors_summary": dict(api_errors_summary or {}),
            "error_code": error_code,
            "finished_at": now,
            "updated_at": now,
        }
    row = await fetchrow(
        """
        UPDATE avito_history_probe_jobs SET
          status = $2,
          chats_seen = $3,
          chats_with_messages = $4,
          messages_seen = $5,
          messages_in_period = $6,
          oldest_message_at = $7,
          newest_message_at = $8,
          api_errors_summary = $9::jsonb,
          error_code = $10,
          finished_at = $11,
          updated_at = $11
        WHERE job_id = $1
        RETURNING *
        """,
        str(job_id),
        str(status),
        int(chats_seen),
        int(chats_with_messages),
        int(messages_seen),
        int(messages_in_period),
        oldest_message_at,
        newest_message_at,
        _json(api_errors_summary),
        error_code,
        now,
    )
    return _row_to_dict(row)


async def update_progress(
    *,
    job_id: str,
    chats_seen: int = 0,
    chats_with_messages: int = 0,
    messages_seen: int = 0,
    messages_in_period: int = 0,
    oldest_message_at: datetime | None = None,
    newest_message_at: datetime | None = None,
    api_errors_summary: Mapping[str, Any] | None = None,
    error_code: str | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    now = _now()
    if not fetchrow:
        logger.debug("avito_history_probes_progress_skip reason=no_db")
        return {
            "job_id": job_id,
            "status": "running",
            "chats_seen": int(chats_seen),
            "chats_with_messages": int(chats_with_messages),
            "messages_seen": int(messages_seen),
            "messages_in_period": int(messages_in_period),
            "oldest_message_at": oldest_message_at,
            "newest_message_at": newest_message_at,
            "api_errors_summary": dict(api_errors_summary or {}),
            "error_code": error_code,
            "updated_at": now,
        }
    row = await fetchrow(
        """
        UPDATE avito_history_probe_jobs SET
          status = CASE WHEN status = 'running' THEN 'running' ELSE status END,
          chats_seen = $2,
          chats_with_messages = $3,
          messages_seen = $4,
          messages_in_period = $5,
          oldest_message_at = $6,
          newest_message_at = $7,
          api_errors_summary = $8::jsonb,
          error_code = $9,
          updated_at = $10
        WHERE job_id = $1
        RETURNING *
        """,
        str(job_id),
        int(chats_seen),
        int(chats_with_messages),
        int(messages_seen),
        int(messages_in_period),
        oldest_message_at,
        newest_message_at,
        _json(api_errors_summary),
        error_code,
        now,
    )
    return _row_to_dict(row)


async def get_job(tenant_id: int, job_id: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT *
        FROM avito_history_probe_jobs
        WHERE tenant_id = $1 AND job_id = $2
        LIMIT 1
        """,
        int(tenant_id),
        str(job_id),
    )
    return _row_to_dict(row)


__all__ = ["create_job", "finish_job", "get_job", "update_progress", "ensure_schema"]
