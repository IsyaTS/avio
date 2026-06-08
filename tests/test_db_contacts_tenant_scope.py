import asyncio

import pytest


pytestmark = pytest.mark.unit


def test_resolve_or_create_contact_scopes_max_user_by_tenant(monkeypatch):
    from libs.core import db

    contacts: list[dict] = []
    seq = {"value": 1000}

    async def fake_fetchrow(sql: str, *args):
        compact = " ".join(sql.split())
        if "SELECT id FROM contacts WHERE tenant_id=$1::int AND max_user_id=$2::bigint" in compact:
            tenant_id, max_user_id = int(args[0]), int(args[1])
            for row in contacts:
                if row["tenant_id"] == tenant_id and row["max_user_id"] == max_user_id:
                    return {"id": row["id"]}
            return None

        if compact.startswith("INSERT INTO contacts("):
            seq["value"] += 1
            row = {
                "id": seq["value"],
                "tenant_id": int(args[0]),
                "max_user_id": int(args[7]) if args[7] is not None else None,
                "max_username": args[8],
            }
            contacts.append(row)
            return {"id": row["id"]}

        return None

    async def fake_exec(sql: str, *args):
        return "UPDATE 1"

    monkeypatch.setattr(db, "_fetchrow", fake_fetchrow)
    monkeypatch.setattr(db, "_exec", fake_exec)

    async def run():
        c101 = await db.resolve_or_create_contact(
            tenant_id=101,
            max_user_id=5550001,
            max_username="tenant_101_user",
        )
        c202 = await db.resolve_or_create_contact(
            tenant_id=202,
            max_user_id=5550001,
            max_username="tenant_202_user",
        )
        c101_repeat = await db.resolve_or_create_contact(
            tenant_id=101,
            max_user_id=5550001,
            max_username="tenant_101_user_new_name",
        )
        return c101, c202, c101_repeat

    c101, c202, c101_repeat = asyncio.run(run())

    assert c101 != c202
    assert c101 == c101_repeat
    assert len(contacts) == 2
    assert {row["tenant_id"] for row in contacts} == {101, 202}
