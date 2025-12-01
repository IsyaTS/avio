from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest

from apps.tgworker.manager import TelegramSessionManager


class _Peer:
    def __init__(self, user_id: int) -> None:
        self.user_id = user_id


class _Message:
    def __init__(self, peer_id: int, text: str, msg_id: int) -> None:
        self.out = False
        self.message = text
        self.id = msg_id
        self.peer_id = _Peer(peer_id)
        self.sender_id = None
        self.date = datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        return {"id": self.id, "out": self.out, "message": self.message}


class _Event:
    def __init__(self, message: _Message) -> None:
        self.is_private = True
        self.message = message
        self.chat = None
        self.chat_id = None

    async def get_sender(self):  # type: ignore[no-untyped-def]
        return None


class _StubChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class _GroupEvent:
    def __init__(self, chat_id: int = 555) -> None:
        self.out = False
        self.is_private = False
        self.is_group = True
        self.is_channel = False
        self.chat = _StubChat(chat_id)
        self.chat_id = chat_id


def test_handle_new_message_skips_group_updates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    manager: TelegramSessionManager | None = None
    set_status_called = False
    webhook_called = False

    def _fake_set_status(*args, **kwargs) -> None:
        nonlocal set_status_called
        set_status_called = True

    async def _fake_send_webhook(*args, **kwargs) -> bool:
        nonlocal webhook_called
        webhook_called = True
        return True

    try:
        manager = TelegramSessionManager(
            api_id=1,
            api_hash="hash",
            sessions_dir=tmp_path,
            webhook_url="http://example.com",
            device_model="TestDevice",
            system_version="1.0",
            app_version="1.0",
            lang_code="en",
            system_lang_code="en",
            webhook_token=None,
            qr_ttl=120.0,
            qr_poll_interval=1.0,
        )

        monkeypatch.setattr(manager, "_set_status", _fake_set_status)
        monkeypatch.setattr(manager, "_send_webhook", _fake_send_webhook)
        monkeypatch.setattr(manager, "_update_metrics", lambda *args, **kwargs: None)

        event = _GroupEvent()
        loop.run_until_complete(
            manager._handle_new_message(tenant=42, client=object(), event=event)  # type: ignore[arg-type]
        )
    finally:
        if manager is not None:
            loop.run_until_complete(manager._http.aclose())
        asyncio.set_event_loop(None)
        loop.close()

    assert set_status_called is False
    assert webhook_called is False
    assert manager is not None
    assert manager._states == {}
    assert manager._incoming_dedup == {}


def test_handle_new_message_marks_manager_when_sender_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    manager: TelegramSessionManager | None = None
    webhook_called = False
    webhook_extra: dict[str, object] | None = None

    async def _fake_send_webhook(message, extra=None):  # type: ignore[no-untyped-def]
        nonlocal webhook_called, webhook_extra
        webhook_called = True
        webhook_extra = extra or {}
        return True

    try:
        manager = TelegramSessionManager(
            api_id=1,
            api_hash="hash",
            sessions_dir=tmp_path,
            webhook_url="http://example.com",
            device_model="TestDevice",
            system_version="1.0",
            app_version="1.0",
            lang_code="en",
            system_lang_code="en",
            webhook_token=None,
            qr_ttl=120.0,
            qr_poll_interval=1.0,
        )
        monkeypatch.setattr(manager, "_send_webhook", _fake_send_webhook)
        monkeypatch.setattr(manager, "_update_metrics", lambda *args, **kwargs: None)
        manager._self_ids[42] = 999

        message = _Message(peer_id=123, text="hello", msg_id=1)
        event = _Event(message)

        loop.run_until_complete(
            manager._handle_new_message(tenant=42, client=object(), event=event)  # type: ignore[arg-type]
        )
    finally:
        if manager is not None:
            loop.run_until_complete(manager._http.aclose())
        asyncio.set_event_loop(None)
        loop.close()

    assert webhook_called is True
    assert webhook_extra == {"manager": True, "out": True, "origin": "telegram:manager"}


def test_handle_new_message_marks_manager_when_raw_out_true(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    manager: TelegramSessionManager | None = None
    webhook_called = False

    async def _fake_send_webhook(message, extra=None):  # type: ignore[no-untyped-def]
        nonlocal webhook_called
        webhook_called = True
        return True

    try:
        manager = TelegramSessionManager(
            api_id=1,
            api_hash="hash",
            sessions_dir=tmp_path,
            webhook_url="http://example.com",
            device_model="TestDevice",
            system_version="1.0",
            app_version="1.0",
            lang_code="en",
            system_lang_code="en",
            webhook_token=None,
            qr_ttl=120.0,
            qr_poll_interval=1.0,
        )
        monkeypatch.setattr(manager, "_send_webhook", _fake_send_webhook)
        monkeypatch.setattr(manager, "_update_metrics", lambda *args, **kwargs: None)
        manager._self_ids[42] = 999

        message = _Message(peer_id=123, text="hello", msg_id=2)
        message.out = False
        message.to_dict = lambda: {"id": message.id, "out": True, "message": message.message}  # type: ignore[assignment]
        event = _Event(message)

        loop.run_until_complete(
            manager._handle_new_message(tenant=42, client=object(), event=event)  # type: ignore[arg-type]
        )
    finally:
        if manager is not None:
            loop.run_until_complete(manager._http.aclose())
        asyncio.set_event_loop(None)
        loop.close()

    assert webhook_called is True
