from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)
_ENSURING_SCHEMA = False
_SCHEMA_READY = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    try:
        return dict(row)
    except Exception:
        return None


def _token_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    data = dict(payload or {})
    return {
        "access_token": data.get("access_token"),
        "refresh_token": data.get("refresh_token"),
        "expires_at": data.get("expires_at"),
        "obtained_at": data.get("obtained_at"),
        "scope": data.get("scope"),
    }


async def ensure_schema() -> None:
    global _ENSURING_SCHEMA, _SCHEMA_READY
    if _SCHEMA_READY or _ENSURING_SCHEMA:
        return
    _ENSURING_SCHEMA = True
    try:
        await _ensure_schema_inner()
        _SCHEMA_READY = True
    finally:
        _ENSURING_SCHEMA = False


async def _ensure_schema_inner() -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS tenant_avito_accounts (
            id BIGSERIAL PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            account_id BIGINT NOT NULL,
            account_login TEXT,
            display_name TEXT,
            access_token TEXT,
            refresh_token TEXT,
            expires_at BIGINT,
            obtained_at BIGINT,
            scope TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            is_primary BOOLEAN NOT NULL DEFAULT FALSE,
            last_webhook_at TIMESTAMPTZ,
            last_error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT tenant_avito_accounts_status_check
                CHECK (status IN ('active', 'disconnected', 'error')),
            CONSTRAINT tenant_avito_accounts_tenant_account_key
                UNIQUE (tenant_id, account_id)
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_avito_accounts_active_account
            ON tenant_avito_accounts(account_id)
            WHERE status = 'active'
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_tenant_avito_accounts_tenant_status
            ON tenant_avito_accounts(tenant_id, status)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_tenant_avito_accounts_account_id
            ON tenant_avito_accounts(account_id)
        """,
    )
    for stmt in statements:
        await exec_fn(stmt)
    await exec_fn("ALTER TABLE tenant_avito_accounts ADD COLUMN IF NOT EXISTS display_name TEXT")
    await _backfill_legacy_primary_accounts()


async def _backfill_legacy_primary_accounts() -> None:
    from libs.core import sales_core as core_module

    tenants_root = getattr(core_module, "TENANTS_DIR", None)
    if tenants_root is None:
        return
    try:
        entries = list(tenants_root.iterdir())
    except Exception:
        return
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return
    for entry in entries:
        if not entry.is_dir() or not entry.name.isdigit():
            continue
        tenant_id = int(entry.name)
        try:
            cfg = core_module.read_tenant_config(tenant_id)
        except Exception:
            continue
        integrations = cfg.get("integrations") if isinstance(cfg, Mapping) else {}
        avito_cfg = integrations.get("avito") if isinstance(integrations, Mapping) else {}
        if not isinstance(avito_cfg, Mapping):
            continue
        try:
            account_id = int(avito_cfg.get("account_id") or 0)
        except Exception:
            account_id = 0
        if account_id <= 0 or not (avito_cfg.get("access_token") or avito_cfg.get("refresh_token")):
            continue
        try:
            existing = await fetchrow(
                "SELECT id FROM tenant_avito_accounts WHERE tenant_id = $1 AND account_id = $2 LIMIT 1",
                tenant_id,
                account_id,
            )
            if existing:
                continue
            await upsert_account_tokens(
                tenant_id,
                account_id,
                avito_cfg,
                account_login=str(avito_cfg.get("account_login") or "") or None,
                is_primary=True,
            )
        except Exception:
            logger.debug(
                "tenant_avito_accounts_backfill_skip tenant=%s account_id=%s",
                tenant_id,
                account_id,
                exc_info=True,
            )


async def upsert_account_tokens(
    tenant_id: int,
    account_id: int,
    payload: Mapping[str, Any] | None,
    *,
    account_login: str | None = None,
    is_primary: bool | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return {
            "tenant_id": int(tenant_id),
            "account_id": int(account_id),
            "account_login": account_login,
            "status": "active",
            "is_primary": bool(is_primary),
            **_token_payload(payload),
        }
    active_owner = await find_active_by_account_id(account_id)
    if active_owner and int(active_owner.get("tenant_id") or 0) != int(tenant_id):
        raise ValueError("account_already_connected")
    if is_primary is None:
        is_primary = (await get_primary_account(tenant_id)) is None
    if is_primary:
        await _unset_primary(int(tenant_id))
    data = _token_payload(payload)
    row = await fetchrow(
        """
        INSERT INTO tenant_avito_accounts (
            tenant_id, account_id, account_login, access_token, refresh_token,
            expires_at, obtained_at, scope, status, is_primary, last_error, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9, NULL, $10)
        ON CONFLICT (tenant_id, account_id)
        DO UPDATE SET
            account_login = COALESCE(NULLIF($3::text, ''), tenant_avito_accounts.account_login),
            access_token = COALESCE($4::text, tenant_avito_accounts.access_token),
            refresh_token = COALESCE($5::text, tenant_avito_accounts.refresh_token),
            expires_at = COALESCE($6::bigint, tenant_avito_accounts.expires_at),
            obtained_at = COALESCE($7::bigint, tenant_avito_accounts.obtained_at),
            scope = COALESCE(NULLIF($8::text, ''), tenant_avito_accounts.scope),
            status = 'active',
            is_primary = $9,
            last_error = NULL,
            updated_at = $10
        RETURNING *
        """,
        int(tenant_id),
        int(account_id),
        (account_login or "").strip() or None,
        data.get("access_token"),
        data.get("refresh_token"),
        data.get("expires_at"),
        data.get("obtained_at"),
        data.get("scope"),
        bool(is_primary),
        _now(),
    )
    return _row_to_dict(row)


async def _unset_primary(tenant_id: int) -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if exec_fn:
        await exec_fn(
            "UPDATE tenant_avito_accounts SET is_primary = false, updated_at = $2 WHERE tenant_id = $1",
            int(tenant_id),
            _now(),
        )


async def list_accounts(
    tenant_id: int,
    *,
    include_disconnected: bool = False,
) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    if include_disconnected:
        rows = await fetch(
            """
            SELECT * FROM tenant_avito_accounts
            WHERE tenant_id = $1
            ORDER BY is_primary DESC, created_at ASC
            """,
            int(tenant_id),
        )
    else:
        rows = await fetch(
            """
            SELECT * FROM tenant_avito_accounts
            WHERE tenant_id = $1 AND status = 'active'
            ORDER BY is_primary DESC, created_at ASC
            """,
            int(tenant_id),
        )
    return [dict(row) for row in rows or []]


async def get_account(tenant_id: int, account_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        "SELECT * FROM tenant_avito_accounts WHERE tenant_id = $1 AND account_id = $2 LIMIT 1",
        int(tenant_id),
        int(account_id),
    )
    return _row_to_dict(row)


async def get_primary_account(tenant_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT * FROM tenant_avito_accounts
        WHERE tenant_id = $1 AND status = 'active'
        ORDER BY is_primary DESC, created_at ASC
        LIMIT 1
        """,
        int(tenant_id),
    )
    return _row_to_dict(row)


async def find_active_by_account_id(account_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT * FROM tenant_avito_accounts
        WHERE account_id = $1 AND status = 'active'
        LIMIT 1
        """,
        int(account_id),
    )
    return _row_to_dict(row)


async def set_primary_account(tenant_id: int, account_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    account = await get_account(int(tenant_id), int(account_id))
    if not account or str(account.get("status") or "") != "active":
        return None
    await _unset_primary(int(tenant_id))
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return account
    row = await fetchrow(
        """
        UPDATE tenant_avito_accounts
        SET is_primary = true, updated_at = $3
        WHERE tenant_id = $1 AND account_id = $2 AND status = 'active'
        RETURNING *
        """,
        int(tenant_id),
        int(account_id),
        _now(),
    )
    return _row_to_dict(row)


async def update_account_display_name(
    tenant_id: int,
    account_id: int,
    display_name: str | None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    cleaned = (display_name or "").strip()[:120] or None
    if not fetchrow:
        account = await get_account(int(tenant_id), int(account_id))
        if account:
            account["display_name"] = cleaned
        return account
    row = await fetchrow(
        """
        UPDATE tenant_avito_accounts
        SET display_name = $3, updated_at = $4
        WHERE tenant_id = $1 AND account_id = $2
        RETURNING *
        """,
        int(tenant_id),
        int(account_id),
        cleaned,
        _now(),
    )
    return _row_to_dict(row)


async def disconnect_account(tenant_id: int, account_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    account = await get_account(int(tenant_id), int(account_id))
    if not account:
        return None
    was_primary = bool(account.get("is_primary"))
    fetchrow = getattr(db_module, "_fetchrow", None)
    row = None
    if fetchrow:
        row = await fetchrow(
            """
            UPDATE tenant_avito_accounts SET
                access_token = NULL,
                refresh_token = NULL,
                expires_at = NULL,
                obtained_at = NULL,
                status = 'disconnected',
                is_primary = false,
                updated_at = $3
            WHERE tenant_id = $1 AND account_id = $2
            RETURNING *
            """,
            int(tenant_id),
            int(account_id),
            _now(),
        )
    if was_primary:
        next_account = await get_primary_account(int(tenant_id))
        if next_account:
            await set_primary_account(int(tenant_id), int(next_account["account_id"]))
    return _row_to_dict(row) or account


async def update_account_error(tenant_id: int, account_id: int, error: str | None) -> None:
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if exec_fn:
        await exec_fn(
            """
            UPDATE tenant_avito_accounts
            SET status = CASE WHEN $3::text IS NULL OR $3::text = '' THEN status ELSE 'error' END,
                last_error = NULLIF($3::text, ''),
                updated_at = $4
            WHERE tenant_id = $1 AND account_id = $2
            """,
            int(tenant_id),
            int(account_id),
            (error or "").strip() or None,
            _now(),
        )


async def mark_webhook_seen(account_id: int) -> None:
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if exec_fn:
        await exec_fn(
            """
            UPDATE tenant_avito_accounts
            SET last_webhook_at = $2, updated_at = $2
            WHERE account_id = $1 AND status = 'active'
            """,
            int(account_id),
            _now(),
        )


async def refresh_account_tokens(
    tenant_id: int,
    account_id: int,
    payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    account = await get_account(int(tenant_id), int(account_id))
    return await upsert_account_tokens(
        int(tenant_id),
        int(account_id),
        payload,
        account_login=str((account or {}).get("account_login") or "") or None,
        is_primary=bool((account or {}).get("is_primary")),
    )


def sync_primary_mirror_to_tenant_config(tenant_id: int, account: Mapping[str, Any] | None) -> dict[str, Any]:
    from libs.core.integrations import avito as avito_integration

    if not account:
        return avito_integration.update_integration(
            int(tenant_id),
            {
                "access_token": None,
                "refresh_token": None,
                "expires_at": None,
                "obtained_at": None,
                "account_id": None,
                "account_login": None,
            },
        )
    payload = {
        "access_token": account.get("access_token"),
        "refresh_token": account.get("refresh_token"),
        "expires_at": account.get("expires_at"),
        "obtained_at": account.get("obtained_at"),
        "scope": account.get("scope"),
        "account_id": account.get("account_id"),
        "account_login": account.get("account_login"),
    }
    return avito_integration.update_integration(int(tenant_id), payload)


__all__ = [
    "disconnect_account",
    "ensure_schema",
    "find_active_by_account_id",
    "get_account",
    "get_primary_account",
    "list_accounts",
    "mark_webhook_seen",
    "refresh_account_tokens",
    "set_primary_account",
    "sync_primary_mirror_to_tenant_config",
    "update_account_error",
    "update_account_display_name",
    "upsert_account_tokens",
]
