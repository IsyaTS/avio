from __future__ import annotations

import pytest

from libs.core.integrations import avito_analytics


pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_messenger_get_messages_skips_legacy_v1_endpoint(monkeypatch) -> None:
    paths: list[str] = []

    async def fake_request(method, path, token, *, params=None):
        paths.append(path)
        return {"messages": []}

    monkeypatch.setattr(avito_analytics, "avito_request", fake_request)

    result = await avito_analytics.messenger_get_messages(
        "token",
        123,
        "chat-1",
    )

    assert result == {"messages": []}
    assert paths == ["/messenger/v3/accounts/123/chats/chat-1/messages/"]


@pytest.mark.asyncio
async def test_messenger_list_chats_passes_item_ids(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_request(method, path, token, *, params=None):
        calls.append({"path": path, "params": dict(params or {})})
        return {"chats": []}

    monkeypatch.setattr(avito_analytics, "avito_request", fake_request)

    result = await avito_analytics.messenger_list_chats(
        "token",
        123,
        limit=100,
        offset=0,
        item_ids=["1", "2"],
    )

    assert result == {"chats": []}
    assert calls == [
        {
            "path": "/messenger/v2/accounts/123/chats",
            "params": {"limit": 100, "offset": 0, "item_ids": "1,2"},
        }
    ]
