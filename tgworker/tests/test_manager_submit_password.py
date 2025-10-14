from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tgworker.manager import SessionState, TelegramSessionManager


class _DummySession:
    def __init__(self) -> None:
        self.saved = False

    def save(self) -> None:  # pragma: no cover - simple flag setter
        self.saved = True


class _FakeClient:
    def __init__(self) -> None:
        self._connected = True
        self.sign_in_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.session = _DummySession()
        self.last_request: object | None = None

    async def connect(self) -> None:  # pragma: no cover - compatibility hook
        self._connected = True

    def is_connected(self) -> bool:
        return self._connected

    async def disconnect(self) -> None:  # pragma: no cover - compatibility hook
        self._connected = False

    async def sign_in(self, *args: object, **kwargs: object) -> None:
        self.sign_in_calls.append((args, kwargs))
        if len(self.sign_in_calls) == 1:
            raise TypeError("logout_other_sessions unexpected keyword")

    async def __call__(self, request: object) -> None:
        self.last_request = request


@pytest.mark.anyio
async def test_submit_password_retries_without_logout_other_sessions(
    monkeypatch, tmp_path: Path, anyio_backend: str
):
    if anyio_backend != "asyncio":
        pytest.skip("requires asyncio backend")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    manager: TelegramSessionManager | None = None
    try:
        manager = TelegramSessionManager(
            api_id=12345,
            api_hash="hash",
            sessions_dir=tmp_path,
            webhook_url="http://example.test/hook",
            device_model="test-device",
            system_version="1.0",
            app_version="1.0",
            lang_code="en",
            system_lang_code="en",
            qr_ttl=120.0,
            qr_poll_interval=1.0,
        )

        tenant_id = 101
        state = SessionState(tenant_id=tenant_id)
        state.status = "needs_2fa"
        state.needs_2fa = True
        state.awaiting_password = True
        state.needs_2fa_expires_at = time.time() + 30
        state.twofa_pending = True

        fake_client = _FakeClient()

        manager._states[tenant_id] = state
        manager._clients[tenant_id] = fake_client
        monkeypatch.setattr(manager, "_register_handlers", lambda *args, **kwargs: None)
        monkeypatch.setattr(manager, "_ensure_session_permissions", lambda *args, **kwargs: None)
        monkeypatch.setattr(manager, "_update_metrics", lambda *args, **kwargs: None)

        result = await manager.submit_password(tenant_id, "secret")
    finally:
        if manager is not None:
            await manager._http.aclose()
        asyncio.set_event_loop(None)
        loop.close()

    assert result.status_code == 200
    assert len(fake_client.sign_in_calls) == 2
    first_args, first_kwargs = fake_client.sign_in_calls[0]
    second_args, second_kwargs = fake_client.sign_in_calls[1]
    assert first_kwargs == {"password": "secret"}
    assert first_args == ()
    assert second_args == ("secret",)
    assert second_kwargs == {}
    assert manager._states[tenant_id].status == "authorized"
