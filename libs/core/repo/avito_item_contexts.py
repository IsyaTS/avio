from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from libs.core import db as db_module


_ENSURING_SCHEMA = False
_SCHEMA_READY = False
_VALID_STATUSES = {"resolved", "unknown", "error"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    try:
        return dict(row)
    except Exception:
        return None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_status(value: str | None) -> str:
    status = (value or "unknown").strip().lower()
    return status if status in _VALID_STATUSES else "unknown"


async def ensure_schema() -> None:
    global _ENSURING_SCHEMA, _SCHEMA_READY
    if _SCHEMA_READY or _ENSURING_SCHEMA:
        return
    _ENSURING_SCHEMA = True
    try:
        exec_fn = getattr(db_module, "_exec", None)
        if not exec_fn:
            return
        statements = (
            """
            CREATE TABLE IF NOT EXISTS avito_item_contexts (
                id BIGSERIAL PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                account_id BIGINT NOT NULL,
                item_id BIGINT NOT NULL,
                city TEXT,
                address TEXT,
                url TEXT,
                source TEXT NOT NULL DEFAULT 'unknown',
                status TEXT NOT NULL DEFAULT 'unknown',
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT avito_item_contexts_unique_item
                    UNIQUE (tenant_id, account_id, item_id),
                CONSTRAINT avito_item_contexts_status_check
                    CHECK (status IN ('resolved', 'unknown', 'error'))
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_avito_item_contexts_tenant_account_item
                ON avito_item_contexts(tenant_id, account_id, item_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_avito_item_contexts_tenant_city
                ON avito_item_contexts(tenant_id, city)
            """,
            """
            CREATE TABLE IF NOT EXISTS avito_lead_item_contexts (
                id BIGSERIAL PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                lead_id BIGINT NOT NULL,
                account_id BIGINT NOT NULL,
                item_id BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT avito_lead_item_contexts_unique_lead
                    UNIQUE (tenant_id, lead_id)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_avito_lead_item_contexts_tenant_lead
                ON avito_lead_item_contexts(tenant_id, lead_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_avito_lead_item_contexts_tenant_account_item
                ON avito_lead_item_contexts(tenant_id, account_id, item_id)
            """,
        )
        for stmt in statements:
            await exec_fn(stmt)
        _SCHEMA_READY = True
    finally:
        _ENSURING_SCHEMA = False


async def get_context(tenant_id: int, account_id: int, item_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT * FROM avito_item_contexts
        WHERE tenant_id = $1 AND account_id = $2 AND item_id = $3
        LIMIT 1
        """,
        int(tenant_id),
        int(account_id),
        int(item_id),
    )
    return _row_to_dict(row)


async def upsert_context(
    tenant_id: int,
    account_id: int,
    item_id: int,
    *,
    city: str | None = None,
    address: str | None = None,
    url: str | None = None,
    source: str = "unknown",
    status: str = "unknown",
    last_error: str | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    cleaned_source = _clean_text(source) or "unknown"
    cleaned_status = _clean_status(status)
    if not fetchrow:
        return {
            "tenant_id": int(tenant_id),
            "account_id": int(account_id),
            "item_id": int(item_id),
            "city": _clean_text(city),
            "address": _clean_text(address),
            "url": _clean_text(url),
            "source": cleaned_source,
            "status": cleaned_status,
            "last_error": _clean_text(last_error),
        }
    row = await fetchrow(
        """
        INSERT INTO avito_item_contexts (
            tenant_id, account_id, item_id, city, address, url,
            source, status, last_error, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (tenant_id, account_id, item_id)
        DO UPDATE SET
            city = COALESCE($4::text, avito_item_contexts.city),
            address = COALESCE($5::text, avito_item_contexts.address),
            url = COALESCE($6::text, avito_item_contexts.url),
            source = COALESCE(NULLIF($7::text, ''), avito_item_contexts.source),
            status = $8,
            last_error = $9,
            updated_at = $10
        RETURNING *
        """,
        int(tenant_id),
        int(account_id),
        int(item_id),
        _clean_text(city),
        _clean_text(address),
        _clean_text(url),
        cleaned_source,
        cleaned_status,
        _clean_text(last_error),
        _now(),
    )
    return _row_to_dict(row)


async def mark_error(
    tenant_id: int,
    account_id: int,
    item_id: int,
    error_code_or_message: str,
) -> dict[str, Any] | None:
    return await upsert_context(
        tenant_id,
        account_id,
        item_id,
        source="api",
        status="error",
        last_error=(error_code_or_message or "error")[:200],
    )


async def upsert_lead_item_context(
    tenant_id: int,
    lead_id: int,
    account_id: int,
    item_id: int,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return {
            "tenant_id": int(tenant_id),
            "lead_id": int(lead_id),
            "account_id": int(account_id),
            "item_id": int(item_id),
        }
    row = await fetchrow(
        """
        INSERT INTO avito_lead_item_contexts (
            tenant_id, lead_id, account_id, item_id, updated_at
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (tenant_id, lead_id)
        DO UPDATE SET
            account_id = $3,
            item_id = $4,
            updated_at = $5
        RETURNING *
        """,
        int(tenant_id),
        int(lead_id),
        int(account_id),
        int(item_id),
        _now(),
    )
    return _row_to_dict(row)


async def get_context_for_lead(tenant_id: int, lead_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT lic.tenant_id,
               lic.lead_id,
               lic.account_id,
               lic.item_id,
               ctx.city,
               ctx.address,
               ctx.url,
               ctx.source,
               ctx.status,
               ctx.last_error
        FROM avito_lead_item_contexts lic
        LEFT JOIN avito_item_contexts ctx
          ON ctx.tenant_id = lic.tenant_id
         AND ctx.account_id = lic.account_id
         AND ctx.item_id = lic.item_id
        WHERE lic.tenant_id = $1 AND lic.lead_id = $2
        LIMIT 1
        """,
        int(tenant_id),
        int(lead_id),
    )
    return _row_to_dict(row)


async def list_contexts_for_leads(tenant_id: int, lead_ids: list[int]) -> dict[int, dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    safe_ids: list[int] = []
    for item in lead_ids:
        try:
            lead_ref = int(item)
        except Exception:
            continue
        if lead_ref > 0:
            safe_ids.append(lead_ref)
    if not fetch or not safe_ids:
        return {}
    rows = await fetch(
        """
        SELECT lic.tenant_id,
               lic.lead_id,
               lic.account_id,
               lic.item_id,
               ctx.city,
               ctx.address,
               ctx.url,
               ctx.source,
               ctx.status,
               ctx.last_error
        FROM avito_lead_item_contexts lic
        LEFT JOIN avito_item_contexts ctx
          ON ctx.tenant_id = lic.tenant_id
         AND ctx.account_id = lic.account_id
         AND ctx.item_id = lic.item_id
        WHERE lic.tenant_id = $1 AND lic.lead_id = ANY($2::bigint[])
        """,
        int(tenant_id),
        safe_ids,
    )
    result: dict[int, dict[str, Any]] = {}
    for row in rows or []:
        data = _row_to_dict(row)
        if data and data.get("lead_id") is not None:
            result[int(data["lead_id"])] = data
    return result


async def list_contexts_for_tenant(tenant_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    safe_limit = max(1, min(int(limit or 100), 1000))
    rows = await fetch(
        """
        SELECT * FROM avito_item_contexts
        WHERE tenant_id = $1
        ORDER BY updated_at DESC
        LIMIT $2
        """,
        int(tenant_id),
        safe_limit,
    )
    return [dict(row) for row in rows or []]


__all__ = [
    "ensure_schema",
    "get_context",
    "upsert_context",
    "mark_error",
    "upsert_lead_item_context",
    "get_context_for_lead",
    "list_contexts_for_leads",
    "list_contexts_for_tenant",
]
