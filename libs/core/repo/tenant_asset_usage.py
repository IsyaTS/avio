from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from libs.core import db as db_module

_ENSURING_SCHEMA = False


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    try:
        return dict(row)
    except Exception:
        return None


async def ensure_schema() -> None:
    global _ENSURING_SCHEMA
    if _ENSURING_SCHEMA:
        return
    _ENSURING_SCHEMA = True
    try:
        exec_fn = getattr(db_module, "_exec", None)
        if not exec_fn:
            return
        for statement in (
            """
            CREATE TABLE IF NOT EXISTS tenant_asset_usage_events (
                id BIGSERIAL PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                lead_id BIGINT NOT NULL,
                channel TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                rule_id TEXT,
                event_type TEXT NOT NULL DEFAULT 'sent',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
            "ALTER TABLE tenant_asset_usage_events ALTER COLUMN lead_id TYPE BIGINT USING lead_id::bigint",
            "CREATE INDEX IF NOT EXISTS idx_tenant_asset_usage_lead_asset ON tenant_asset_usage_events(tenant_id, lead_id, asset_id)",
            "CREATE INDEX IF NOT EXISTS idx_tenant_asset_usage_created_at ON tenant_asset_usage_events(tenant_id, created_at)",
        ):
            await exec_fn(statement)
    finally:
        _ENSURING_SCHEMA = False


async def record_usage(
    tenant_id: int,
    lead_id: int,
    channel: str,
    asset_id: str,
    rule_id: str | None,
    *,
    event_type: str = "sent",
) -> None:
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    await exec_fn(
        """
        INSERT INTO tenant_asset_usage_events (tenant_id, lead_id, channel, asset_id, rule_id, event_type)
        VALUES ($1,$2,$3,$4,$5,$6)
        """,
        int(tenant_id),
        int(lead_id),
        str(channel),
        str(asset_id),
        rule_id,
        str(event_type),
    )


async def was_used_recently(
    tenant_id: int,
    lead_id: int,
    asset_id: str,
    *,
    ttl_seconds: int | None = None,
) -> bool:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return False
    since = None
    if ttl_seconds and ttl_seconds > 0:
        since = datetime.now(timezone.utc) - timedelta(seconds=int(ttl_seconds))
    if since is not None:
        row = await fetchrow(
            """
            SELECT id FROM tenant_asset_usage_events
            WHERE tenant_id=$1 AND lead_id=$2 AND asset_id=$3 AND created_at >= $4
            LIMIT 1
            """,
            int(tenant_id),
            int(lead_id),
            str(asset_id),
            since,
        )
    else:
        row = await fetchrow(
            """
            SELECT id FROM tenant_asset_usage_events
            WHERE tenant_id=$1 AND lead_id=$2 AND asset_id=$3
            LIMIT 1
            """,
            int(tenant_id),
            int(lead_id),
            str(asset_id),
        )
    return bool(row)


async def list_usage_for_lead(tenant_id: int, lead_id: int) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    rows = await fetch(
        """
        SELECT * FROM tenant_asset_usage_events
        WHERE tenant_id=$1 AND lead_id=$2
        ORDER BY created_at DESC
        """,
        int(tenant_id),
        int(lead_id),
    )
    return [item for row in rows if (item := _row_to_dict(row))]
