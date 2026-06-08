from __future__ import annotations

import pytest
from starlette.requests import Request

from apps.api.web import client as client_module


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _request(path: str = "/api/dialogs") -> Request:
    async def _receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(b"host", b"dev.avio.website")],
            "query_string": b"",
            "server": ("dev.avio.website", 443),
            "scheme": "https",
            "client": ("testclient", 1),
        },
        _receive,
    )


@pytest.mark.anyio
async def test_list_dialogs_includes_avito_account_and_item_city(monkeypatch):
    monkeypatch.setattr(client_module, "_resolve_tenant_and_key", lambda request, tenant: (1, "k"))

    async def _fake_fetch_dialogs(_tenant_id: int, *, limit: int = 200):
        return [
            {
                "id": 501,
                "channel": "avito",
                "title": "Avito · buyer",
                "contact": "",
                "peer": "chat-1",
                "source_real_id": 222,
                "last_message": "привет",
                "last_ts": None,
            }
        ]

    class _Accounts:
        async def list_accounts(self, tenant_id: int, *, include_disconnected: bool = False):
            return [{"account_id": 222, "account_login": "seller-ufa", "display_name": "Двери Гермес"}]

    class _Items:
        async def list_contexts_for_leads(self, tenant_id: int, lead_ids: list[int]):
            return {501: {"lead_id": 501, "account_id": 222, "item_id": 333, "city": "Уфа", "status": "resolved"}}

    monkeypatch.setattr(client_module.db, "fetch_dialogs_for_tenant", _fake_fetch_dialogs)
    monkeypatch.setattr(client_module.avito_accounts, "list_accounts", _Accounts().list_accounts)
    monkeypatch.setattr(client_module.avito_item_contexts, "list_contexts_for_leads", _Items().list_contexts_for_leads)

    body = await client_module.list_dialogs_api(_request(), tenant=1, limit=200)

    item = body["dialogs"][0]
    assert item["avito_account_id"] == 222
    assert item["avito_account_display_name"] == "Двери Гермес"
    assert item["avito_account_login"] == "seller-ufa"
    assert item["avito_item_id"] == 333
    assert item["avito_item_city"] == "Уфа"


@pytest.mark.anyio
async def test_get_dialog_messages_includes_avito_account_and_item_city(monkeypatch):
    monkeypatch.setattr(client_module, "_resolve_tenant_and_key", lambda request, tenant: (1, "k"))

    async def _fake_meta(_lead_id: int):
        return {
            "id": 501,
            "tenant_id": 1,
            "channel": "avito",
            "title": "Avito · buyer",
            "source_real_id": 222,
        }

    async def _fake_messages(_tenant_id: int, _lead_id: int, *, limit: int = 50, before=None):
        return []

    async def _fake_feedback(_tenant_id: int, _message_ids):
        return set()

    async def _fake_get_account(_tenant_id: int, _account_id: int):
        return {"account_id": 222, "account_login": "seller-ufa", "display_name": "Двери Гермес"}

    async def _fake_context_for_lead(_tenant_id: int, _lead_id: int):
        return {"lead_id": 501, "account_id": 222, "item_id": 333, "city": "Уфа", "status": "resolved"}

    monkeypatch.setattr(client_module.db, "get_lead_dialog_metadata", _fake_meta)
    monkeypatch.setattr(client_module.db, "list_messages_for_lead", _fake_messages)
    monkeypatch.setattr(client_module.db, "list_feedback_message_ids", _fake_feedback)
    monkeypatch.setattr(client_module.avito_accounts, "get_account", _fake_get_account)
    monkeypatch.setattr(client_module.avito_item_contexts, "get_context_for_lead", _fake_context_for_lead)
    monkeypatch.setattr(client_module, "_load_silence_status", lambda tenant_id, lead_id, channel: {})

    body = await client_module.get_dialog_messages_api(501, _request("/api/dialogs/501"), tenant=1)

    assert body["avito_account_id"] == 222
    assert body["avito_account_display_name"] == "Двери Гермес"
    assert body["avito_account_login"] == "seller-ufa"
    assert body["avito_item_id"] == 333
    assert body["avito_item_city"] == "Уфа"


@pytest.mark.anyio
async def test_list_dialogs_maps_max_personal_channel_and_title(monkeypatch):
    monkeypatch.setattr(client_module, "_resolve_tenant_and_key", lambda request, tenant: (1, "k"))

    async def _fake_fetch_dialogs(_tenant_id: int, *, limit: int = 200):
        return [
            {
                "id": 42,
                "channel": "max_personal",
                "title": "93267442",
                "contact": "",
                "peer": "93267442",
                "max_username": "nikita",
                "max_user_id": 93267442,
                "last_message": "привет",
                "last_ts": None,
            }
        ]

    monkeypatch.setattr(client_module.db, "fetch_dialogs_for_tenant", _fake_fetch_dialogs)

    body = await client_module.list_dialogs_api(_request(), tenant=1, limit=200)
    assert body["ok"] is True
    assert len(body["dialogs"]) == 1
    item = body["dialogs"][0]
    assert item["channel"] == "max"
    assert item["title"] == "nikita"


@pytest.mark.anyio
async def test_list_dialogs_prefers_human_name_over_technical_max_title(monkeypatch):
    monkeypatch.setattr(client_module, "_resolve_tenant_and_key", lambda request, tenant: (1, "k"))

    async def _fake_fetch_dialogs(_tenant_id: int, *, limit: int = 200):
        return [
            {
                "id": 77,
                "channel": "max_personal",
                "title": "max_personal:id 93267442",
                "contact": "Айдар",
                "peer": "93267442",
                "max_username": "nikita",
                "max_user_id": 93267442,
                "last_message": "привет",
                "last_ts": None,
            }
        ]

    monkeypatch.setattr(client_module.db, "fetch_dialogs_for_tenant", _fake_fetch_dialogs)

    body = await client_module.list_dialogs_api(_request(), tenant=1, limit=200)
    assert body["ok"] is True
    assert len(body["dialogs"]) == 1
    item = body["dialogs"][0]
    assert item["channel"] == "max"
    assert item["title"] == "Айдар"


@pytest.mark.anyio
async def test_get_dialog_messages_maps_max_personal_channel_and_title(monkeypatch):
    monkeypatch.setattr(client_module, "_resolve_tenant_and_key", lambda request, tenant: (1, "k"))

    async def _fake_meta(_lead_id: int):
        return {
            "id": 10,
            "tenant_id": 1,
            "channel": "max_personal",
            "title": "93267442",
            "contact": "",
            "peer": "93267442",
            "max_username": "nikita",
            "max_user_id": 93267442,
        }

    async def _fake_messages(_tenant_id: int, _lead_id: int, *, limit: int = 50, before=None):
        return []

    async def _fake_feedback(_tenant_id: int, _message_ids):
        return set()

    monkeypatch.setattr(client_module.db, "get_lead_dialog_metadata", _fake_meta)
    monkeypatch.setattr(client_module.db, "list_messages_for_lead", _fake_messages)
    monkeypatch.setattr(client_module.db, "list_feedback_message_ids", _fake_feedback)
    monkeypatch.setattr(client_module, "_load_silence_status", lambda tenant_id, lead_id, channel: {})

    body = await client_module.get_dialog_messages_api(10, _request("/api/dialogs/10"), tenant=1)
    assert body["ok"] is True
    assert body["channel"] == "max"
    assert body["title"] == "nikita"


@pytest.mark.anyio
async def test_get_dialog_messages_prefers_human_name_for_technical_max_title(monkeypatch):
    monkeypatch.setattr(client_module, "_resolve_tenant_and_key", lambda request, tenant: (1, "k"))

    async def _fake_meta(_lead_id: int):
        return {
            "id": 10,
            "tenant_id": 1,
            "channel": "max_personal",
            "title": "max_personal:93267442",
            "contact": "Айдар",
            "peer": "93267442",
            "max_username": "nikita",
            "max_user_id": 93267442,
        }

    async def _fake_messages(_tenant_id: int, _lead_id: int, *, limit: int = 50, before=None):
        return []

    async def _fake_feedback(_tenant_id: int, _message_ids):
        return set()

    monkeypatch.setattr(client_module.db, "get_lead_dialog_metadata", _fake_meta)
    monkeypatch.setattr(client_module.db, "list_messages_for_lead", _fake_messages)
    monkeypatch.setattr(client_module.db, "list_feedback_message_ids", _fake_feedback)
    monkeypatch.setattr(client_module, "_load_silence_status", lambda tenant_id, lead_id, channel: {})

    body = await client_module.get_dialog_messages_api(10, _request("/api/dialogs/10"), tenant=1)
    assert body["ok"] is True
    assert body["channel"] == "max"
    assert body["title"] == "Айдар"


@pytest.mark.anyio
async def test_send_dialog_message_enqueues_outbox_payload(monkeypatch):
    monkeypatch.setattr(client_module, "_resolve_tenant_and_key", lambda request, tenant: (1, "k"))
    pushed: list[tuple[str, str]] = []
    inserted: list[dict] = []

    async def _fake_meta(_lead_id: int):
        return {
            "id": 10,
            "tenant_id": 1,
            "channel": "telegram",
            "peer": "12345",
            "telegram_user_id": "12345",
            "telegram_username": "client",
            "title": "Client",
        }

    async def _fake_insert_message_out(*args, **kwargs):
        inserted.append({"args": args, "kwargs": kwargs})
        return 501

    class _Redis:
        def lpush(self, key: str, value: str):
            pushed.append((key, value))

    class _Request:
        async def json(self):
            return {"text": "Привет", "tg_slot": 2}

    monkeypatch.setattr(client_module.db, "get_lead_dialog_metadata", _fake_meta)
    monkeypatch.setattr(client_module.db, "insert_message_out", _fake_insert_message_out)
    monkeypatch.setattr(client_module.C, "redis_client", lambda: _Redis())

    body = await client_module.send_dialog_message_api(10, _Request(), tenant=1)

    assert body["ok"] is True
    assert inserted[0]["kwargs"]["source"] == "manager:tg_slot:2"
    assert pushed
    assert pushed[0][0] == client_module.OUTBOX_QUEUE_KEY
    assert '"telegram_user_id": 12345' in pushed[0][1]
    assert '"_message_db_id": 501' in pushed[0][1]
