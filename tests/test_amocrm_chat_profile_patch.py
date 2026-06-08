from __future__ import annotations

import asyncio

import pytest

from libs.core.services import amocrm_chat


def test_refresh_remote_chat_profile_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def fake_amojo_request(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal called
        called = True
        return {}

    monkeypatch.delenv("AMOCRM_CHAT_PROFILE_PATCH", raising=False)
    monkeypatch.setattr(amocrm_chat, "_amojo_request", fake_amojo_request)

    asyncio.run(
        amocrm_chat._refresh_remote_chat_profile(
            101,
            resolved={"scope_id": "scope-1"},
            conversation_id="conv-1",
            remote_chat_id="chat-1",
            external_user_id="lead-1",
            display_name="Lead",
            profile={},
            source_id="telegram",
        )
    )

    assert called is False


def test_refresh_remote_chat_profile_patches_remote_chat_id_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, str]] = []

    async def fake_amojo_request(*_args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append({"path": str(kwargs.get("path") or "")})
        return {}

    monkeypatch.setenv("AMOCRM_CHAT_PROFILE_PATCH", "1")
    monkeypatch.setattr(amocrm_chat, "_amojo_request", fake_amojo_request)

    asyncio.run(
        amocrm_chat._refresh_remote_chat_profile(
            101,
            resolved={"scope_id": "scope-1"},
            conversation_id="conv-1",
            remote_chat_id="chat-1",
            external_user_id="lead-1",
            display_name="Lead",
            profile={},
            source_id="telegram",
        )
    )

    assert calls == [{"path": "/v2/origin/custom/scope-1/chats/chat-1"}]
