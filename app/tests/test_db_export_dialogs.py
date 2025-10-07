import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


def test_export_dialogs_transforms_messages(monkeypatch):
    import app.db as db

    captured = {}

    async def fake_load(tenant_val, since_ts, until_ts, limit_dialogs, channel, per_message_limit, allow_offline):
        captured["args"] = (tenant_val, since_ts, until_ts, limit_dialogs, channel, per_message_limit, allow_offline)
        dialogs = [
            {
                "lead_id": 501,
                "contact_id": 7001,
                "whatsapp_phone": "+79001234567@s.whatsapp.net",
                "title": "Lead 501",
                "messages": [
                    {"ts": since_ts + 10, "direction": 0, "text": "hello"},
                    {"ts": since_ts + 20, "direction": 1, "text": "hi"},
                ],
                "chat_id": "+79001234567@s.whatsapp.net",
            },
            {
                "lead_id": 502,
                "contact_id": 7002,
                "whatsapp_phone": None,
                "title": "Lead 502",
                "messages": [
                    {"ts": since_ts + 5, "direction": 0, "text": "ping"},
                ],
                "chat_id": "contact:7002",
            },
        ]
        meta = {"distinct_chat_ids": [d["chat_id"] for d in dialogs], "filtered_groups": 0}
        return dialogs, meta

    monkeypatch.setattr(db, "_load_whatsapp_dialogs", fake_load)

    async def run_export():
        return await db.export_dialogs(
            tenant_id=42,
            channel="whatsapp",
            exclude_groups=True,
            since_ts=12345.0,
            max_conversations=50,
            per_conversation_limit=0,
        )

    dialogs = asyncio.run(run_export())

    assert captured["args"][0] == 42
    assert captured["args"][4] == "whatsapp"
    # per_conversation_limit=0 should translate to no hard cap
    assert captured["args"][5] is None
    assert captured["args"][6] is False

    assert len(dialogs) == 2
    first = dialogs[0]
    assert first["lead_id"] == 501
    assert first["whatsapp_phone"] == "+79001234567@s.whatsapp.net"
    assert first["messages"][0]["role"] == "user"
    assert first["messages"][1]["role"] == "assistant"
    assert first["messages"][1]["direction"] == 1
    assert first["last_message_ts"] == first["messages"][1]["ts"]

    second = dialogs[1]
    assert second["lead_id"] == 502
    assert second["messages"][0]["role"] == "user"
    assert second["messages"][0]["text"] == "ping"


def test_load_whatsapp_dialogs_allows_missing_contacts(monkeypatch):
    import app.db as db

    tenant = 77
    since_ts = 1_700_000_000.0
    until_ts = 1_700_086_400.0

    dialogs_data = [
        {
            "lead_id": 101,
            "contact_id": None,
            "whatsapp_phone": "",
            "title": "No Contact",
            "chat_id": "chat:101",
            "message_limit": 1,
            "message_total": 1,
        },
        {
            "lead_id": 102,
            "contact_id": 202,
            "whatsapp_phone": "+79991234567@s.whatsapp.net",
            "title": "With Contact",
            "chat_id": "+79991234567@s.whatsapp.net",
            "message_limit": 1,
            "message_total": 1,
        },
    ]
    messages = {
        101: [{"ts": since_ts + 10, "direction": 0, "text": "hi"}],
        102: [{"ts": since_ts + 20, "direction": 1, "text": "hello"}],
    }

    async def fake_stream(**_kwargs):
        async def generator():
            for dialog in dialogs_data:
                lead_id = dialog["lead_id"]

                async def message_batches():
                    yield list(messages[lead_id])

                yield dialog, message_batches()

        meta = {
            "dialog_count": len(dialogs_data),
            "messages_exported": sum(len(v) for v in messages.values()),
            "distinct_chat_ids": [dialog["chat_id"] for dialog in dialogs_data],
            "filtered_groups": 0,
        }
        return generator(), meta

    monkeypatch.setattr(db, "stream_whatsapp_dialogs", fake_stream)

    async def run_load():
        return await db._load_whatsapp_dialogs(
            tenant_val=tenant,
            since_ts=since_ts,
            until_ts=until_ts,
            limit_dialogs=None,
            channel="whatsapp",
            per_message_limit=None,
            allow_offline=True,
        )

    dialogs, meta = asyncio.run(run_load())

    assert len(dialogs) == 2
    by_lead = {dialog["lead_id"]: dialog for dialog in dialogs}
    assert set(by_lead) == {101, 102}

    no_contact = by_lead[101]
    assert no_contact["contact_id"] is None
    assert not no_contact["whatsapp_phone"]
    assert no_contact["chat_id"].startswith("chat:")

    with_contact = by_lead[102]
    assert with_contact["contact_id"] == 202
    assert with_contact["whatsapp_phone"].endswith("@s.whatsapp.net")

    assert meta["filtered_groups"] == 0
