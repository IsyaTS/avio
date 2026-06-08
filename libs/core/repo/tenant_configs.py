from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Mapping

try:
    import psycopg
except Exception:  # pragma: no cover - optional in minimal test envs
    psycopg = None  # type: ignore[assignment]

from libs.core import db as db_module


logger = logging.getLogger(__name__)
_UNAVAILABLE_UNTIL = 0.0
_LAST_FAILURE_LOG_AT = 0.0
_UNAVAILABLE_TTL_SECONDS = 30.0
_FAILURE_LOG_INTERVAL_SECONDS = 300.0


def enabled() -> bool:
    raw = (os.getenv("TENANT_CONFIG_DB_ENABLED") or "").strip().lower()
    if raw:
        return raw not in {"0", "false", "no", "off", "disabled"}
    return (os.getenv("TESTING") or "").strip() != "1"


def _database_url() -> str:
    value = str(getattr(db_module, "DATABASE_URL", "") or os.getenv("DATABASE_URL") or "")
    return value.replace("postgresql+asyncpg://", "postgresql://").strip()


def _connect():
    if time.monotonic() < _UNAVAILABLE_UNTIL:
        return None
    if psycopg is None:
        return None
    dsn = _database_url()
    if not dsn:
        return None
    return psycopg.connect(dsn, connect_timeout=2)


def _mark_unavailable(event: str, tenant_id: int | None = None) -> None:
    global _UNAVAILABLE_UNTIL, _LAST_FAILURE_LOG_AT
    now = time.monotonic()
    _UNAVAILABLE_UNTIL = now + _UNAVAILABLE_TTL_SECONDS
    if now - _LAST_FAILURE_LOG_AT >= _FAILURE_LOG_INTERVAL_SECONDS:
        _LAST_FAILURE_LOG_AT = now
        if tenant_id is None:
            logger.warning("%s", event, exc_info=True)
        else:
            logger.warning("%s tenant=%s", event, tenant_id, exc_info=True)
    else:
        if tenant_id is None:
            logger.debug("%s", event, exc_info=True)
        else:
            logger.debug("%s tenant=%s", event, tenant_id, exc_info=True)


def ensure_schema() -> bool:
    if not enabled():
        return False
    try:
        with _connect() as con:
            if con is None:
                return False
            with con.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tenant_configs (
                        tenant_id BIGINT PRIMARY KEY,
                        config JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_tenant_configs_updated_at
                    ON tenant_configs(updated_at)
                    """
                )
        return True
    except Exception:
        _mark_unavailable("tenant_configs_schema_unavailable")
        return False


def _row_to_payload(row: Any) -> tuple[dict[str, Any], float] | None:
    if not row:
        return None
    cfg = row[0]
    updated = row[1] if len(row) > 1 else None
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:
            cfg = {}
    if not isinstance(cfg, Mapping):
        cfg = {}
    marker = 0.0
    if isinstance(updated, datetime):
        marker = updated.timestamp()
    elif updated is not None:
        try:
            marker = float(updated)
        except Exception:
            marker = 0.0
    return dict(cfg), marker


def get(tenant_id: int) -> tuple[dict[str, Any], float] | None:
    if not enabled():
        return None
    try:
        with _connect() as con:
            if con is None:
                return None
            with con.cursor() as cur:
                cur.execute(
                    "SELECT config, updated_at FROM tenant_configs WHERE tenant_id = %s",
                    (int(tenant_id),),
                )
                return _row_to_payload(cur.fetchone())
    except Exception:
        _mark_unavailable("tenant_config_db_read_failed", int(tenant_id))
        return None


def upsert(tenant_id: int, cfg: Mapping[str, Any]) -> bool:
    if not enabled():
        return False
    payload = json.dumps(dict(cfg or {}), ensure_ascii=False)
    try:
        with _connect() as con:
            if con is None:
                return False
            with con.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tenant_configs(tenant_id, config)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (tenant_id)
                    DO UPDATE SET config = EXCLUDED.config, updated_at = now()
                    """,
                    (int(tenant_id), payload),
                )
        return True
    except Exception:
        _mark_unavailable("tenant_config_db_write_failed", int(tenant_id))
        return False


__all__ = ["enabled", "ensure_schema", "get", "upsert"]
