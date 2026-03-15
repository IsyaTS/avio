from __future__ import annotations

import logging
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)

_SCHEMA_READY = False


async def ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("crm_chat_links_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS crm_chat_links (
          id BIGSERIAL PRIMARY KEY,
          tenant_id INTEGER NOT NULL,
          lead_id BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
          provider TEXT NOT NULL,
          external_chat_id TEXT,
          external_conversation_id TEXT,
          external_contact_id BIGINT,
          external_lead_id BIGINT,
          chat_scope_id TEXT,
          source_id TEXT,
          last_inbound_message_id TEXT,
          last_outbound_message_id TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_crm_chat_links_tenant_lead_provider ON crm_chat_links(tenant_id, lead_id, provider)",
        "CREATE INDEX IF NOT EXISTS idx_crm_chat_links_external_chat ON crm_chat_links(provider, external_chat_id)",
        "CREATE INDEX IF NOT EXISTS idx_crm_chat_links_external_conversation ON crm_chat_links(provider, external_conversation_id)",
        "CREATE INDEX IF NOT EXISTS idx_crm_chat_links_tenant_updated ON crm_chat_links(tenant_id, updated_at DESC)",
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception("crm_chat_links_ensure_failed statement=%s", stmt.strip().split("\n", 1)[0])
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


async def get_link(tenant_id: int, lead_id: int, provider: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT id,
               tenant_id,
               lead_id,
               provider,
               external_chat_id,
               external_conversation_id,
               external_contact_id,
               external_lead_id,
               chat_scope_id,
               source_id,
               last_inbound_message_id,
               last_outbound_message_id,
               created_at,
               updated_at
        FROM crm_chat_links
        WHERE tenant_id = $1 AND lead_id = $2 AND provider = $3
        """,
        int(tenant_id),
        int(lead_id),
        str(provider),
    )
    return _row_to_dict(row)


async def find_by_external_chat(
    provider: str,
    *,
    external_chat_id: str | None = None,
    external_conversation_id: str | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    chat_id = str(external_chat_id or "").strip()
    conversation_id = str(external_conversation_id or "").strip()
    if not chat_id and not conversation_id:
        return None
    row = await fetchrow(
        """
        SELECT id,
               tenant_id,
               lead_id,
               provider,
               external_chat_id,
               external_conversation_id,
               external_contact_id,
               external_lead_id,
               chat_scope_id,
               source_id,
               last_inbound_message_id,
               last_outbound_message_id,
               created_at,
               updated_at
        FROM crm_chat_links
        WHERE provider = $1
          AND (
            ($2 <> '' AND external_chat_id = $2)
            OR ($3 <> '' AND external_conversation_id = $3)
          )
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        str(provider),
        chat_id,
        conversation_id,
    )
    return _row_to_dict(row)


async def find_by_scope_id(provider: str, scope_id: str | None) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    scope_value = str(scope_id or "").strip()
    if not scope_value:
        return None
    row = await fetchrow(
        """
        SELECT id,
               tenant_id,
               lead_id,
               provider,
               external_chat_id,
               external_conversation_id,
               external_contact_id,
               external_lead_id,
               chat_scope_id,
               source_id,
               last_inbound_message_id,
               last_outbound_message_id,
               created_at,
               updated_at
        FROM crm_chat_links
        WHERE provider = $1
          AND chat_scope_id = $2
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        str(provider),
        scope_value,
    )
    return _row_to_dict(row)


async def upsert_link(
    tenant_id: int,
    lead_id: int,
    provider: str,
    *,
    external_chat_id: str | None = None,
    external_conversation_id: str | None = None,
    external_contact_id: int | None = None,
    external_lead_id: int | None = None,
    chat_scope_id: str | None = None,
    source_id: str | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        INSERT INTO crm_chat_links (
          tenant_id,
          lead_id,
          provider,
          external_chat_id,
          external_conversation_id,
          external_contact_id,
          external_lead_id,
          chat_scope_id,
          source_id
        )
        VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7, NULLIF($8, ''), NULLIF($9, ''))
        ON CONFLICT (tenant_id, lead_id, provider)
        DO UPDATE SET
          external_chat_id = COALESCE(NULLIF(EXCLUDED.external_chat_id, ''), crm_chat_links.external_chat_id),
          external_conversation_id = COALESCE(NULLIF(EXCLUDED.external_conversation_id, ''), crm_chat_links.external_conversation_id),
          external_contact_id = COALESCE(EXCLUDED.external_contact_id, crm_chat_links.external_contact_id),
          external_lead_id = COALESCE(EXCLUDED.external_lead_id, crm_chat_links.external_lead_id),
          chat_scope_id = COALESCE(NULLIF(EXCLUDED.chat_scope_id, ''), crm_chat_links.chat_scope_id),
          source_id = COALESCE(NULLIF(EXCLUDED.source_id, ''), crm_chat_links.source_id),
          updated_at = now()
        RETURNING id,
                  tenant_id,
                  lead_id,
                  provider,
                  external_chat_id,
                  external_conversation_id,
                  external_contact_id,
                  external_lead_id,
                  chat_scope_id,
                  source_id,
                  last_inbound_message_id,
                  last_outbound_message_id,
                  created_at,
                  updated_at
        """,
        int(tenant_id),
        int(lead_id),
        str(provider),
        str(external_chat_id or ""),
        str(external_conversation_id or ""),
        int(external_contact_id) if external_contact_id is not None else None,
        int(external_lead_id) if external_lead_id is not None else None,
        str(chat_scope_id or ""),
        str(source_id or ""),
    )
    result = _row_to_dict(row)
    if not result:
        return None

    # Keep one canonical chat mapping per tenant/provider/chat identity.
    # Without this cleanup, stale rows may remain after lead merge/relink and
    # produce duplicated chat events in amoCRM Inbox.
    exec_fn = getattr(db_module, "_exec", None)
    if exec_fn:
        chat_value = str(result.get("external_chat_id") or "").strip()
        conv_value = str(result.get("external_conversation_id") or "").strip()
        if chat_value:
            await exec_fn(
                """
                DELETE FROM crm_chat_links
                WHERE tenant_id = $1
                  AND provider = $2
                  AND lead_id <> $3
                  AND external_chat_id = $4
                """,
                int(tenant_id),
                str(provider),
                int(lead_id),
                chat_value,
            )
        if conv_value:
            await exec_fn(
                """
                DELETE FROM crm_chat_links
                WHERE tenant_id = $1
                  AND provider = $2
                  AND lead_id <> $3
                  AND external_conversation_id = $4
                """,
                int(tenant_id),
                str(provider),
                int(lead_id),
                conv_value,
            )

    return result


async def touch_message_ids(
    tenant_id: int,
    lead_id: int,
    provider: str,
    *,
    inbound_message_id: str | None = None,
    outbound_message_id: str | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        UPDATE crm_chat_links
        SET last_inbound_message_id = COALESCE(NULLIF($4, ''), last_inbound_message_id),
            last_outbound_message_id = COALESCE(NULLIF($5, ''), last_outbound_message_id),
            updated_at = now()
        WHERE tenant_id = $1 AND lead_id = $2 AND provider = $3
        RETURNING id,
                  tenant_id,
                  lead_id,
                  provider,
                  external_chat_id,
                  external_conversation_id,
                  external_contact_id,
                  external_lead_id,
                  chat_scope_id,
                  source_id,
                  last_inbound_message_id,
                  last_outbound_message_id,
                  created_at,
                  updated_at
        """,
        int(tenant_id),
        int(lead_id),
        str(provider),
        str(inbound_message_id or ""),
        str(outbound_message_id or ""),
    )
    return _row_to_dict(row)
