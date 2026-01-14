from __future__ import annotations

import logging
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)

_SCHEMA_READY = False


async def ensure_schema() -> None:
    """Ensure the crm_links table exists (idempotent)."""

    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("crm_links_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS crm_links (
          id               BIGSERIAL PRIMARY KEY,
          tenant_id        INTEGER NOT NULL,
          lead_id          BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
          provider         TEXT NOT NULL,
          provider_lead_id BIGINT,
          provider_contact_id BIGINT,
          pipeline_id      BIGINT,
          stage_index      INTEGER NOT NULL DEFAULT 0,
          inbound_count    INTEGER NOT NULL DEFAULT 0,
          created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        "ALTER TABLE crm_links ADD COLUMN IF NOT EXISTS provider_contact_id BIGINT",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_crm_links_tenant_lead_provider ON crm_links(tenant_id, lead_id, provider)",
        "CREATE INDEX IF NOT EXISTS idx_crm_links_provider_lead ON crm_links(provider, provider_lead_id)",
        "CREATE INDEX IF NOT EXISTS idx_crm_links_tenant_updated ON crm_links(tenant_id, updated_at DESC)",
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception("crm_links_ensure_failed statement=%s", stmt.strip().split("\n", 1)[0])
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


async def get_link(
    tenant_id: int,
    lead_id: int,
    provider: str,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            SELECT id,
                   tenant_id,
                   lead_id,
                   provider,
                   provider_lead_id,
                   provider_contact_id,
                   pipeline_id,
                   stage_index,
                   inbound_count,
                   created_at,
                   updated_at
            FROM crm_links
            WHERE tenant_id = $1 AND lead_id = $2 AND provider = $3
            """,
            int(tenant_id),
            int(lead_id),
            str(provider),
        )
    except Exception:
        logger.exception(
            "crm_links_get_failed tenant_id=%s lead_id=%s provider=%s",
            tenant_id,
            lead_id,
            provider,
        )
        raise
    return _row_to_dict(row)


async def create_link(
    tenant_id: int,
    lead_id: int,
    provider: str,
    *,
    pipeline_id: int | None,
    stage_index: int = 0,
    inbound_count: int = 0,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            INSERT INTO crm_links (
              tenant_id,
              lead_id,
              provider,
              pipeline_id,
              stage_index,
              inbound_count
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tenant_id, lead_id, provider) DO NOTHING
            RETURNING id,
                      tenant_id,
                      lead_id,
                      provider,
                      provider_lead_id,
                      provider_contact_id,
                      pipeline_id,
                      stage_index,
                      inbound_count,
                      created_at,
                      updated_at
            """,
            int(tenant_id),
            int(lead_id),
            str(provider),
            pipeline_id,
            int(stage_index),
            int(inbound_count),
        )
    except Exception:
        logger.exception(
            "crm_links_create_failed tenant_id=%s lead_id=%s provider=%s",
            tenant_id,
            lead_id,
            provider,
        )
        raise
    return _row_to_dict(row)


async def update_provider_lead_id(
    tenant_id: int,
    lead_id: int,
    provider: str,
    provider_lead_id: int | None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            UPDATE crm_links
            SET provider_lead_id = $4,
                updated_at = now()
            WHERE tenant_id = $1 AND lead_id = $2 AND provider = $3
            RETURNING id,
                      tenant_id,
                      lead_id,
                      provider,
                      provider_lead_id,
                      provider_contact_id,
                      pipeline_id,
                      stage_index,
                      inbound_count,
                      created_at,
                      updated_at
            """,
            int(tenant_id),
            int(lead_id),
            str(provider),
            provider_lead_id,
        )
    except Exception:
        logger.exception(
            "crm_links_update_provider_failed tenant_id=%s lead_id=%s provider=%s",
            tenant_id,
            lead_id,
            provider,
        )
        raise
    return _row_to_dict(row)


async def update_stage_index(
    tenant_id: int,
    lead_id: int,
    provider: str,
    stage_index: int,
    *,
    pipeline_id: int | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            UPDATE crm_links
            SET stage_index = $4,
                pipeline_id = COALESCE($5, pipeline_id),
                updated_at = now()
            WHERE tenant_id = $1 AND lead_id = $2 AND provider = $3
            RETURNING id,
                      tenant_id,
                      lead_id,
                      provider,
                      provider_lead_id,
                      provider_contact_id,
                      pipeline_id,
                      stage_index,
                      inbound_count,
                      created_at,
                      updated_at
            """,
            int(tenant_id),
            int(lead_id),
            str(provider),
            int(stage_index),
            pipeline_id,
        )
    except Exception:
        logger.exception(
            "crm_links_update_stage_failed tenant_id=%s lead_id=%s provider=%s",
            tenant_id,
            lead_id,
            provider,
        )
        raise
    return _row_to_dict(row)


async def increment_inbound_count(
    tenant_id: int,
    lead_id: int,
    provider: str,
    *,
    pipeline_id: int | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            UPDATE crm_links
            SET inbound_count = inbound_count + 1,
                pipeline_id = COALESCE($4, pipeline_id),
                updated_at = now()
            WHERE tenant_id = $1 AND lead_id = $2 AND provider = $3
            RETURNING id,
                      tenant_id,
                      lead_id,
                      provider,
                      provider_lead_id,
                      provider_contact_id,
                      pipeline_id,
                      stage_index,
                      inbound_count,
                      created_at,
                      updated_at
            """,
            int(tenant_id),
            int(lead_id),
            str(provider),
            pipeline_id,
        )
    except Exception:
        logger.exception(
            "crm_links_increment_failed tenant_id=%s lead_id=%s provider=%s",
            tenant_id,
            lead_id,
            provider,
        )
        raise
    return _row_to_dict(row)


async def update_provider_contact_id(
    tenant_id: int,
    lead_id: int,
    provider: str,
    provider_contact_id: int | None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            UPDATE crm_links
            SET provider_contact_id = $4,
                updated_at = now()
            WHERE tenant_id = $1 AND lead_id = $2 AND provider = $3
            RETURNING id,
                      tenant_id,
                      lead_id,
                      provider,
                      provider_lead_id,
                      provider_contact_id,
                      pipeline_id,
                      stage_index,
                      inbound_count,
                      created_at,
                      updated_at
            """,
            int(tenant_id),
            int(lead_id),
            str(provider),
            provider_contact_id,
        )
    except Exception:
        logger.exception(
            "crm_links_update_provider_contact_failed tenant_id=%s lead_id=%s provider=%s",
            tenant_id,
            lead_id,
            provider,
        )
        raise
    return _row_to_dict(row)


__all__ = [
    "get_link",
    "create_link",
    "update_provider_lead_id",
    "update_provider_contact_id",
    "update_stage_index",
    "increment_inbound_count",
    "ensure_schema",
]
