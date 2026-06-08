from __future__ import annotations

from typing import Any

import pytest

from libs.core.integrations import avito
from libs.core.integrations import avito as avito_integration
from libs.core.services import avito_account_tokens


pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_ensure_access_token_for_account_returns_requested_account(monkeypatch):
    async def fake_get_account(tenant_id: int, account_id: int) -> dict[str, Any]:
        return {
            "tenant_id": tenant_id,
            "account_id": account_id,
            "status": "active",
            "access_token": "token-account",
            "refresh_token": "refresh-account",
            "expires_at": 4_000_000_000,
        }

    monkeypatch.setattr(avito_account_tokens.avito_accounts, "get_account", fake_get_account)

    token, account = await avito_account_tokens.ensure_access_token_for_account(101, 222)

    assert token == "token-account"
    assert account["account_id"] == 222


@pytest.mark.asyncio
async def test_ensure_access_token_for_account_rejects_missing_account(monkeypatch):
    async def fake_get_account(_tenant_id: int, _account_id: int) -> None:
        return None

    monkeypatch.setattr(avito_account_tokens.avito_accounts, "get_account", fake_get_account)

    with pytest.raises(avito.AvitoOAuthError, match="not connected"):
        await avito_account_tokens.ensure_access_token_for_account(101, 333)


@pytest.mark.asyncio
async def test_upsert_oauth_account_from_payload_requires_account_info(monkeypatch):
    async def fake_sync(_token: str) -> dict[str, Any]:
        return {}

    monkeypatch.setattr(avito_account_tokens, "sync_account_info_for_token", fake_sync)

    with pytest.raises(avito.AvitoOAuthError, match="account id"):
        await avito_account_tokens.upsert_oauth_account_from_payload(
            101,
            {"access_token": "new-token", "refresh_token": "new-refresh"},
        )


@pytest.mark.asyncio
async def test_resolve_chat_profile_uses_requested_account_token(monkeypatch):
    calls: list[tuple[int, int]] = []

    async def fake_ensure_for_account(tenant_id: int, account_id: int):
        calls.append((tenant_id, account_id))
        return "token-222", {"account_id": account_id}

    async def fake_ensure_primary(_tenant_id: int):
        raise AssertionError("primary token must not be used")

    class Response:
        status_code = 200

        def json(self):
            return {
                "users": [
                    {"id": 222, "name": "Seller"},
                    {"id": 333, "name": "Buyer"},
                ]
            }

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

        async def get(self, url: str, headers: dict[str, str] | None = None):
            assert headers and headers["Authorization"] == "Bearer token-222"
            assert "/accounts/222/chats/chat-1" in url
            return Response()

    avito_integration._CHAT_PROFILE_CACHE.clear()
    monkeypatch.setattr(avito_integration, "ensure_access_token_for_account", fake_ensure_for_account)
    monkeypatch.setattr(avito_integration, "ensure_access_token", fake_ensure_primary)
    monkeypatch.setattr(avito_integration.httpx, "AsyncClient", Client)

    result = await avito_integration.resolve_chat_participant_profile(
        101,
        account_id=222,
        chat_id="chat-1",
        author_id=333,
    )

    assert calls == [(101, 222)]
    assert result["name"] == "Buyer"


@pytest.mark.asyncio
async def test_resolve_chat_profile_primary_fallback_without_account(monkeypatch):
    async def fake_ensure_primary(tenant_id: int):
        assert tenant_id == 101
        return "primary-token", {"account_id": 222}

    class Response:
        status_code = 200

        def json(self):
            return {"users": [{"id": 333, "name": "Buyer"}]}

    class Client:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

        async def get(self, url: str, headers: dict[str, str] | None = None):
            assert headers and headers["Authorization"] == "Bearer primary-token"
            assert "/accounts/222/chats/chat-2" in url
            return Response()

    avito_integration._CHAT_PROFILE_CACHE.clear()
    monkeypatch.setattr(avito_integration, "ensure_access_token", fake_ensure_primary)
    monkeypatch.setattr(avito_integration.httpx, "AsyncClient", Client)

    result = await avito_integration.resolve_chat_participant_profile(
        101,
        account_id=None,
        chat_id="chat-2",
        author_id=333,
    )

    assert result["name"] == "Buyer"
