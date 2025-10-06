import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


def test_fetch_threads_filters_by_tenant(monkeypatch, tmp_path):
    # Point offline storage to a temp directory and force offline mode
    monkeypatch.setenv("TESTING", "1")
    monkeypatch.setenv("OFFLINE_DIALOGS_DIR", str(tmp_path))
    monkeypatch.setenv("OFFLINE_DIALOGS_MAX_RECORDS", "100")

    import app.db as db

    # Ensure offline mode
    monkeypatch.setattr(db, "asyncpg", None)
    monkeypatch.setattr(db, "_pool", None)
    monkeypatch.setattr(db, "_OFFLINE_DIR", Path(tmp_path))
    monkeypatch.setattr(db, "_OFFLINE_THREADS_FILE", Path(tmp_path) / "threads.jsonl")

    async def scenario():
        await db.insert_message_in(lead_id=101, text="Привет", tenant_id=1)
        await db.insert_message_out(lead_id=101, text="Здравствуйте", provider_msg_id=None, tenant_id=1)
        await db.insert_message_in(lead_id=202, text="Hello", tenant_id=2)

        dialogs_t1 = await db.fetch_threads(tenant=1, limit=10)
        dialogs_t2 = await db.fetch_threads(tenant=2, limit=10)

        # Lead without explicit tenant_id should be visible for both
        await db.insert_message_in(lead_id=303, text="Generic", tenant_id=None)
        dialogs_generic = await db.fetch_threads(tenant=2, limit=10)

        assert dialogs_t1 and dialogs_t1[0]["lead_id"] == 101
        assert not any(d["lead_id"] == 202 for d in dialogs_t1)
        assert dialogs_t2 and dialogs_t2[0]["lead_id"] == 202
        assert any(d["lead_id"] == 303 for d in dialogs_generic)

    asyncio.run(scenario())


def test_export_dialogs_limits_per_lead(monkeypatch):
    import app.db as db

    captured = {}

    async def fake_load(
        tenant_val,
        since_ts,
        until_ts,
        limit_dialogs,
        channel,
        per_message_limit,
        allow_offline,
    ):
        captured["per_limit"] = per_message_limit
        captured["allow_offline"] = allow_offline

        heavy_messages = [
            {"ts": float(idx), "direction": idx % 2, "text": f"msg-{idx}"}
            for idx in range(10)
        ]
        if per_message_limit is not None:
            heavy_messages = heavy_messages[-per_message_limit:]

        dialogs = [
            {
                "lead_id": 111,
                "contact_id": None,
                "whatsapp_phone": "",
                "title": "Heavy",
                "messages": list(heavy_messages),
                "chat_id": "lead:111",
            },
            {
                "lead_id": 222,
                "contact_id": 10,
                "whatsapp_phone": "+79991234567@s.whatsapp.net",
                "title": "Ack",
                "messages": [
                    {"ts": 1.0, "direction": 0, "text": "hello"},
                    {"ts": 2.0, "direction": 1, "text": "ack"},
                ],
                "chat_id": "contact:10",
            },
            {
                "lead_id": 333,
                "contact_id": None,
                "whatsapp_phone": None,
                "title": "Hola",
                "messages": [{"ts": 5.0, "direction": 0, "text": "hola"}],
                "chat_id": "lead:333",
            },
        ]
        meta = {
            "distinct_chat_ids": [d["chat_id"] for d in dialogs],
            "filtered_groups": 0,
        }
        return dialogs, meta

    monkeypatch.setattr(db, "_load_whatsapp_dialogs", fake_load)

    async def scenario():
        dialogs = await db.export_dialogs(
            tenant_id=1,
            since_ts=None,
            max_conversations=3,
            per_conversation_limit=5,
        )

        lead_ids = {d.get("lead_id") for d in dialogs}
        assert lead_ids == {111, 222, 333}

        heavy = next(d for d in dialogs if d.get("lead_id") == 111)
        assert len(heavy.get("messages") or []) == 5
        assert all(m.get("role") in {"user", "assistant"} for m in heavy.get("messages") or [])

    asyncio.run(scenario())

    assert captured["per_limit"] == 5
    assert captured["allow_offline"] is False
