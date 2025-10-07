import asyncio
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from app.export import whatsapp as whatsapp_export


def test_whatsapp_export_allows_formerly_blocklisted_lead(monkeypatch):
    async def fake_fetch(tenant, since, until, limit, per_message_limit=None):
        dialogs = [
            {
                "lead_id": 2001,
                "messages": [{"ts": 1.0, "direction": 0, "text": "hello"}],
                "chat_id": "lead_2001",
            }
        ]
        meta = {
            "dialog_count": 1,
            "messages_exported": 1,
            "distinct_chat_ids": ["lead_2001"],
            "filtered_groups": 0,
        }
        return dialogs, meta

    monkeypatch.setattr(whatsapp_export.db_module, "fetch_whatsapp_dialogs", fake_fetch)

    async def attempt():
        return await whatsapp_export.build_whatsapp_zip(
            tenant=1,
            since=datetime.now(timezone.utc),
            until=datetime.now(timezone.utc),
            limit_dialogs=None,
            agent_name="Agent",
        )

    buffer, stats = asyncio.run(attempt())
    assert buffer is not None
    assert stats["dialog_count"] == 1
    assert stats["message_count"] == 1
    assert stats["meta"].get("dialog_count") == 1


def test_whatsapp_export_uses_contact_filename(monkeypatch):
    async def fake_fetch(tenant, since, until, limit, per_message_limit=None):
        dialogs = [
            {
                "lead_id": 42,
                "contact_id": 987,
                "whatsapp_phone": None,
                "title": "",
                "messages": [
                    {"ts": 1.0, "direction": 0, "text": "hello"},
                    {"ts": 2.0, "direction": 1, "text": "hi"},
                ],
                "chat_id": "contact:987",
            }
        ]
        meta = {
            "dialog_count": 1,
            "messages_exported": 2,
            "distinct_chat_ids": ["contact:987"],
            "filtered_groups": 0,
        }
        return dialogs, meta

    monkeypatch.setattr(whatsapp_export.db_module, "fetch_whatsapp_dialogs", fake_fetch)

    async def attempt():
        return await whatsapp_export.build_whatsapp_zip(
            tenant=1,
            since=datetime.now(timezone.utc),
            until=datetime.now(timezone.utc),
            limit_dialogs=None,
            agent_name="Agent",
        )

    buffer, stats = asyncio.run(attempt())
    assert buffer is not None
    buffer.seek(0)
    with zipfile.ZipFile(buffer) as archive:
        names = archive.namelist()
        assert names == ["contact_987.txt"]
        content = archive.read(names[0]).decode("utf-8")
        assert "hello" in content and "Agent" in content


def test_whatsapp_export_uses_title_for_group(monkeypatch):
    async def fake_fetch(tenant, since, until, limit, per_message_limit=None):
        dialogs = [
            {
                "lead_id": 101,
                "contact_id": None,
                "whatsapp_phone": None,
                "title": "Team Rocket",
                "messages": [
                    {"ts": 1.0, "direction": 0, "text": "prepare for trouble"},
                    {"ts": 2.0, "direction": 1, "text": "make it double"},
                ],
                "chat_id": "lead:101",
            }
        ]
        meta = {
            "dialog_count": 1,
            "messages_exported": 2,
            "distinct_chat_ids": ["lead:101"],
            "filtered_groups": 0,
        }
        return dialogs, meta

    monkeypatch.setattr(whatsapp_export.db_module, "fetch_whatsapp_dialogs", fake_fetch)

    async def attempt():
        return await whatsapp_export.build_whatsapp_zip(
            tenant=1,
            since=datetime.now(timezone.utc),
            until=datetime.now(timezone.utc),
            limit_dialogs=None,
            agent_name="Agent",
        )

    buffer, stats = asyncio.run(attempt())
    assert buffer is not None
    buffer.seek(0)
    with zipfile.ZipFile(buffer) as archive:
        names = archive.namelist()
        assert names == ["Team Rocket.txt"]
