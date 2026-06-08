from __future__ import annotations

from typing import Any

import pytest

from libs.core.repo import avito_item_contexts


pytestmark = pytest.mark.unit


class _MemoryDb:
    def __init__(self) -> None:
        self.rows: dict[tuple[int, int, int], dict[str, Any]] = {}
        self.lead_links: dict[tuple[int, int], dict[str, Any]] = {}
        self.execs: list[str] = []

    async def exec(self, sql: str, *args: Any) -> str:
        self.execs.append(sql)
        return "OK"

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        tenant_id = int(args[0])
        if sql.strip().startswith("SELECT lic.tenant_id"):
            lead_id = int(args[1])
            link = self.lead_links.get((tenant_id, lead_id))
            if not link:
                return None
            ctx = self.rows.get((tenant_id, link["account_id"], link["item_id"]), {})
            return {**link, **ctx}
        if sql.strip().startswith("INSERT INTO avito_lead_item_contexts"):
            lead_id = int(args[1])
            row = {"tenant_id": tenant_id, "lead_id": lead_id, "account_id": int(args[2]), "item_id": int(args[3])}
            self.lead_links[(tenant_id, lead_id)] = row
            return dict(row)
        account_id = int(args[1])
        item_id = int(args[2])
        key = (tenant_id, account_id, item_id)
        if sql.strip().startswith("SELECT *"):
            row = self.rows.get(key)
            return dict(row) if row else None
        if sql.strip().startswith("INSERT INTO avito_item_contexts"):
            row = dict(self.rows.get(key) or {})
            row.update(
                {
                    "tenant_id": tenant_id,
                    "account_id": account_id,
                    "item_id": item_id,
                    "city": args[3] if args[3] is not None else row.get("city"),
                    "address": args[4] if args[4] is not None else row.get("address"),
                    "url": args[5] if args[5] is not None else row.get("url"),
                    "source": args[6] or row.get("source") or "unknown",
                    "status": args[7],
                    "last_error": args[8],
                }
            )
            self.rows[key] = row
            return dict(row)
        return None

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        tenant_id = int(args[0])
        if "FROM avito_lead_item_contexts" in sql:
            lead_ids = {int(item) for item in args[1]}
            result = []
            for (row_tenant, lead_id), link in self.lead_links.items():
                if row_tenant == tenant_id and lead_id in lead_ids:
                    ctx = self.rows.get((tenant_id, link["account_id"], link["item_id"]), {})
                    result.append({**link, **ctx})
            return result
        limit = int(args[1])
        rows = [dict(row) for key, row in self.rows.items() if key[0] == tenant_id]
        return rows[:limit]


@pytest.fixture()
def memory_db(monkeypatch) -> _MemoryDb:
    db = _MemoryDb()
    monkeypatch.setattr(avito_item_contexts.db_module, "_exec", db.exec)
    monkeypatch.setattr(avito_item_contexts.db_module, "_fetchrow", db.fetchrow)
    monkeypatch.setattr(avito_item_contexts.db_module, "_fetch", db.fetch)
    return db


@pytest.mark.asyncio
async def test_upsert_get_and_repeat_update(memory_db: _MemoryDb) -> None:
    await avito_item_contexts.ensure_schema()
    assert memory_db.execs

    first = await avito_item_contexts.upsert_context(
        101,
        123,
        749,
        city="Уфа",
        address="Уфа, Менделеева 80",
        source="address",
        status="resolved",
    )
    second = await avito_item_contexts.upsert_context(
        101,
        123,
        749,
        city="Стерлитамак",
        source="url",
        status="resolved",
    )
    loaded = await avito_item_contexts.get_context(101, 123, 749)

    assert first and first["city"] == "Уфа"
    assert second and second["city"] == "Стерлитамак"
    assert loaded and loaded["source"] == "url"
    assert loaded["address"] == "Уфа, Менделеева 80"


@pytest.mark.asyncio
async def test_unique_key_includes_tenant_account_item(memory_db: _MemoryDb) -> None:
    await avito_item_contexts.upsert_context(101, 1, 10, city="Уфа", status="resolved")
    await avito_item_contexts.upsert_context(101, 2, 10, city="Казань", status="resolved")
    await avito_item_contexts.upsert_context(102, 1, 10, city="Оренбург", status="resolved")

    assert (await avito_item_contexts.get_context(101, 1, 10))["city"] == "Уфа"
    assert (await avito_item_contexts.get_context(101, 2, 10))["city"] == "Казань"
    assert (await avito_item_contexts.get_context(102, 1, 10))["city"] == "Оренбург"


@pytest.mark.asyncio
async def test_mark_error_sets_error_status(memory_db: _MemoryDb) -> None:
    row = await avito_item_contexts.mark_error(101, 123, 749, "AvitoAPIError:403")

    assert row and row["status"] == "error"
    assert row["last_error"] == "AvitoAPIError:403"


@pytest.mark.asyncio
async def test_lead_item_context_maps_lead_to_resolved_city(memory_db: _MemoryDb) -> None:
    await avito_item_contexts.upsert_context(101, 222, 333, city="Уфа", status="resolved", source="url")
    await avito_item_contexts.upsert_lead_item_context(101, 501, 222, 333)

    loaded = await avito_item_contexts.get_context_for_lead(101, 501)
    listed = await avito_item_contexts.list_contexts_for_leads(101, [501])

    assert loaded and loaded["city"] == "Уфа"
    assert listed[501]["item_id"] == 333
