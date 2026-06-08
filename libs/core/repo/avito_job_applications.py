from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)


async def ensure_schema() -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("avito_job_applications_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS avito_job_application_events (
          id                BIGSERIAL PRIMARY KEY,
          avito_account_id  BIGINT NOT NULL,
          application_id    TEXT NOT NULL,
          source            TEXT,
          payload_json      JSONB,
          created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_avito_job_application_events_account_app
          ON avito_job_application_events(avito_account_id, application_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_avito_job_application_events_account_created
          ON avito_job_application_events(avito_account_id, created_at DESC)
        """,
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception(
                "avito_job_applications_ensure_failed statement=%s", stmt.strip().split("\n", 1)[0]
            )
            raise


async def store_event(
    avito_account_id: int,
    application_id: str,
    *,
    source: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> None:
    if not application_id:
        return
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("avito_job_applications_store_skip reason=no_db")
        return
    payload_json = json.dumps(payload, ensure_ascii=False) if payload else None
    try:
        await exec_fn(
            """
            INSERT INTO avito_job_application_events (avito_account_id, application_id, source, payload_json)
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT (avito_account_id, application_id) DO NOTHING
            """,
            int(avito_account_id),
            str(application_id),
            source,
            payload_json,
        )
    except Exception:
        logger.exception(
            "avito_job_applications_store_failed account=%s app=%s",
            avito_account_id,
            application_id,
        )
        raise


async def list_recent_ids(
    avito_account_id: int, *, period_days: int = 30, limit: int = 500
) -> list[str]:
    fetch_fn = getattr(db_module, "_fetch", None)
    if not fetch_fn:
        return []
    await ensure_schema()
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=int(period_days))
    try:
        rows = await fetch_fn(
            """
            SELECT application_id
            FROM avito_job_application_events
            WHERE avito_account_id = $1
              AND created_at >= $2
            ORDER BY created_at DESC
            LIMIT $3
            """,
            int(avito_account_id),
            cutoff,
            int(limit),
        )
    except Exception:
        logger.exception("avito_job_applications_list_failed account=%s", avito_account_id)
        return []
    ids: list[str] = []
    for row in rows:
        try:
            ids.append(str(row["application_id"]))
        except Exception:
            continue
    return ids


__all__ = ["store_event", "list_recent_ids", "ensure_schema"]
