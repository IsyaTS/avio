import asyncio
import asyncio
import tempfile
from pathlib import Path
import asyncio
import tempfile
from pathlib import Path

import pytest
from telethon.client.telegramclient import TelegramClient
from telethon.tl.types import InputPeerSelf

from tgworker.manager import NotAuthorizedError, TelegramSessionManager


class _DummyTelethonClient(TelegramClient):
    def __init__(self) -> None:  # pragma: no cover - super init bypassed
        pass


@pytest.mark.anyio
async def test_resolve_self_peer_returns_input_peer(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        manager = TelegramSessionManager(
            api_id=1,
            api_hash="hash",
            sessions_dir=Path(tmpdir),
            webhook_url="http://example.com",
            device_model="Test",
            system_version="1.0",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru",
            webhook_token=None,
            qr_ttl=120.0,
            qr_poll_interval=1.0,
        )

        async def _noop() -> None:
            return None

        dummy_client = _DummyTelethonClient()

        async def _ensure_authorized(_: int) -> _DummyTelethonClient:
            return dummy_client

        monkeypatch.setattr(manager, "wait_until_ready", _noop)
        monkeypatch.setattr(manager, "_ensure_authorized_client", _ensure_authorized)

        try:
            result = await manager.resolve_self_peer(tenant=1)
            assert isinstance(result, InputPeerSelf)
        finally:
            await manager.shutdown()
            loop.close()
            asyncio.set_event_loop(None)


@pytest.mark.anyio
async def test_resolve_self_peer_not_authorized(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        manager = TelegramSessionManager(
            api_id=1,
            api_hash="hash",
            sessions_dir=Path(tmpdir),
            webhook_url="http://example.com",
            device_model="Test",
            system_version="1.0",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru",
            webhook_token=None,
            qr_ttl=120.0,
            qr_poll_interval=1.0,
        )

        async def _noop() -> None:
            return None

        async def _ensure_authorized(_: int):
            return None

        monkeypatch.setattr(manager, "wait_until_ready", _noop)
        monkeypatch.setattr(manager, "_ensure_authorized_client", _ensure_authorized)

        try:
            with pytest.raises(NotAuthorizedError):
                await manager.resolve_self_peer(tenant=42)
        finally:
            await manager.shutdown()
            loop.close()
            asyncio.set_event_loop(None)
