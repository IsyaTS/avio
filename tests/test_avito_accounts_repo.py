from __future__ import annotations

from typing import Any

import pytest

from libs.core.repo import avito_accounts


pytestmark = pytest.mark.unit


class _MemoryDb:
    def __init__(self) -> None:
        self.rows: dict[tuple[int, int], dict[str, Any]] = {}
        self.execs: list[str] = []

    async def exec(self, sql: str, *args: Any) -> str:
        self.execs.append(sql)
        if sql.strip().startswith("UPDATE tenant_avito_accounts SET is_primary = false"):
            tenant_id = int(args[0])
            for row in self.rows.values():
                if row["tenant_id"] == tenant_id:
                    row["is_primary"] = False
        return "OK"

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        tenant_id = int(args[0])
        include_disconnected = "status = 'active'" not in sql
        rows = [
            dict(row)
            for row in self.rows.values()
            if row["tenant_id"] == tenant_id and (include_disconnected or row["status"] == "active")
        ]
        return sorted(rows, key=lambda item: (not item["is_primary"], item["account_id"]))

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        if "WHERE account_id = $1 AND status = 'active'" in sql:
            account_id = int(args[0])
            for row in self.rows.values():
                if row["account_id"] == account_id and row["status"] == "active":
                    return dict(row)
            return None
        tenant_id = int(args[0])
        account_id = int(args[1]) if len(args) > 1 else None
        if sql.strip().startswith("SELECT *") and account_id is not None:
            row = self.rows.get((tenant_id, account_id))
            return dict(row) if row else None
        if "ORDER BY is_primary DESC" in sql:
            rows = await self.fetch(sql, tenant_id)
            return rows[0] if rows else None
        if sql.strip().startswith("INSERT INTO tenant_avito_accounts"):
            row = {
                "tenant_id": tenant_id,
                "account_id": account_id,
                "account_login": args[2],
                "display_name": None,
                "access_token": args[3],
                "refresh_token": args[4],
                "expires_at": args[5],
                "obtained_at": args[6],
                "scope": args[7],
                "status": "active",
                "is_primary": bool(args[8]),
            }
            self.rows[(tenant_id, account_id)] = row
            return dict(row)
        if sql.strip().startswith("UPDATE tenant_avito_accounts") and "is_primary = true" in sql:
            row = self.rows.get((tenant_id, account_id))
            if row:
                row["is_primary"] = True
                return dict(row)
        if sql.strip().startswith("UPDATE tenant_avito_accounts") and "display_name = $3" in sql:
            row = self.rows.get((tenant_id, account_id))
            if row:
                row["display_name"] = args[2]
                return dict(row)
        if sql.strip().startswith("UPDATE tenant_avito_accounts") and "status = 'disconnected'" in sql:
            row = self.rows.get((tenant_id, account_id))
            if row:
                row.update({"status": "disconnected", "access_token": None, "refresh_token": None, "is_primary": False})
                return dict(row)
        return None


@pytest.mark.asyncio
async def test_upsert_second_account_does_not_overwrite_primary(monkeypatch):
    db = _MemoryDb()

    async def _skip_backfill() -> None:
        return None

    monkeypatch.setattr(avito_accounts.db_module, "_exec", db.exec)
    monkeypatch.setattr(avito_accounts.db_module, "_fetchrow", db.fetchrow)
    monkeypatch.setattr(avito_accounts.db_module, "_fetch", db.fetch)
    monkeypatch.setattr(avito_accounts, "_backfill_legacy_primary_accounts", _skip_backfill)

    first = await avito_accounts.upsert_account_tokens(
        101, 111, {"access_token": "a1", "refresh_token": "r1"}, account_login="one"
    )
    second = await avito_accounts.upsert_account_tokens(
        101, 222, {"access_token": "a2", "refresh_token": "r2"}, account_login="two"
    )

    assert first and first["is_primary"] is True
    assert second and second["is_primary"] is False
    rows = await avito_accounts.list_accounts(101)
    assert [row["account_id"] for row in rows] == [111, 222]


@pytest.mark.asyncio
async def test_set_primary_and_disconnect_promotes_next(monkeypatch):
    db = _MemoryDb()

    async def _skip_backfill() -> None:
        return None

    monkeypatch.setattr(avito_accounts.db_module, "_exec", db.exec)
    monkeypatch.setattr(avito_accounts.db_module, "_fetchrow", db.fetchrow)
    monkeypatch.setattr(avito_accounts.db_module, "_fetch", db.fetch)
    monkeypatch.setattr(avito_accounts, "_backfill_legacy_primary_accounts", _skip_backfill)
    await avito_accounts.upsert_account_tokens(101, 111, {"access_token": "a1"}, is_primary=True)
    await avito_accounts.upsert_account_tokens(101, 222, {"access_token": "a2"}, is_primary=False)

    switched = await avito_accounts.set_primary_account(101, 222)
    assert switched and switched["is_primary"] is True
    await avito_accounts.disconnect_account(101, 222)

    primary = await avito_accounts.get_primary_account(101)
    assert primary and primary["account_id"] == 111


@pytest.mark.asyncio
async def test_update_account_display_name(monkeypatch):
    db = _MemoryDb()

    async def _skip_backfill() -> None:
        return None

    monkeypatch.setattr(avito_accounts.db_module, "_exec", db.exec)
    monkeypatch.setattr(avito_accounts.db_module, "_fetchrow", db.fetchrow)
    monkeypatch.setattr(avito_accounts.db_module, "_fetch", db.fetch)
    monkeypatch.setattr(avito_accounts, "_backfill_legacy_primary_accounts", _skip_backfill)
    await avito_accounts.upsert_account_tokens(101, 222, {"access_token": "a2"}, account_login="Сергей")

    renamed = await avito_accounts.update_account_display_name(101, 222, " Двери Гермес ")

    assert renamed and renamed["display_name"] == "Двери Гермес"
    loaded = await avito_accounts.get_account(101, 222)
    assert loaded and loaded["account_login"] == "Сергей"
