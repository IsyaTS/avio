from __future__ import annotations

import logging
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)

_SCHEMA_READY = False


async def ensure_schema() -> None:
    """Ensure the crm_extracted_fields table exists (idempotent)."""

    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("crm_fields_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS crm_extracted_fields (
          id          BIGSERIAL PRIMARY KEY,
          tenant_id   INTEGER NOT NULL,
          lead_id     BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
          provider    TEXT NOT NULL,
          field_key   TEXT NOT NULL,
          field_value TEXT NOT NULL,
          amo_field_id BIGINT,
          updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_crm_extracted_fields ON crm_extracted_fields(tenant_id, lead_id, provider, field_key)",
        "CREATE INDEX IF NOT EXISTS idx_crm_extracted_fields_tenant ON crm_extracted_fields(tenant_id, provider)",
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception("crm_fields_ensure_failed statement=%s", stmt.strip().split("\n", 1)[0])
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


async def list_fields(
    tenant_id: int,
    lead_id: int,
    provider: str,
) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch_fn = getattr(db_module, "_fetch", None)
    if not fetch_fn:
        return []
    try:
        rows = await fetch_fn(
            """
            SELECT id,
                   tenant_id,
                   lead_id,
                   provider,
                   field_key,
                   field_value,
                   amo_field_id,
                   updated_at
            FROM crm_extracted_fields
            WHERE tenant_id = $1 AND lead_id = $2 AND provider = $3
            """,
            int(tenant_id),
            int(lead_id),
            str(provider),
        )
    except Exception:
        logger.exception(
            "crm_fields_list_failed tenant_id=%s lead_id=%s provider=%s",
            tenant_id,
            lead_id,
            provider,
        )
        raise
    result: list[dict[str, Any]] = []
    for row in rows or []:
        parsed = _row_to_dict(row)
        if parsed:
            result.append(parsed)
    return result


async def upsert_field(
    tenant_id: int,
    lead_id: int,
    provider: str,
    *,
    field_key: str,
    field_value: str,
    amo_field_id: int | None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            INSERT INTO crm_extracted_fields (
              tenant_id,
              lead_id,
              provider,
              field_key,
              field_value,
              amo_field_id,
              updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, now())
            ON CONFLICT (tenant_id, lead_id, provider, field_key) DO UPDATE SET
              field_value = EXCLUDED.field_value,
              amo_field_id = COALESCE(EXCLUDED.amo_field_id, crm_extracted_fields.amo_field_id),
              updated_at = now()
            RETURNING id,
                      tenant_id,
                      lead_id,
                      provider,
                      field_key,
                      field_value,
                      amo_field_id,
                      updated_at
            """,
            int(tenant_id),
            int(lead_id),
            str(provider),
            str(field_key),
            str(field_value),
            amo_field_id,
        )
    except Exception:
        logger.exception(
            "crm_fields_upsert_failed tenant_id=%s lead_id=%s provider=%s key=%s",
            tenant_id,
            lead_id,
            provider,
            field_key,
        )
        raise
    return _row_to_dict(row)


__all__ = ["list_fields", "upsert_field", "ensure_schema"]
