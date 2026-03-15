from __future__ import annotations

from typing import Any

import pytest

from apps.worker import main as worker_module


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        text: str = "",
        json_data: dict[str, Any] | None = None,
        content: bytes | None = None,
    ) -> None:
        self.status_code = int(status_code)
        self.text = text
        self._json_data = json_data or {}
        self.content = content or b""

    def json(self) -> dict[str, Any]:
        return dict(self._json_data)


@pytest.mark.anyio
async def test_send_avito_voice_attachment_without_text(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    async def fake_ensure_access_token(_tenant: int) -> tuple[str, dict[str, Any]]:
        return "token-1", {"account_id": 374186368, "refresh_token": "r1"}

    async def fake_get_lead_peer(_lead_id: int, *, channel: str | None = None) -> str:
        assert channel == "avito"
        return "u2i-test-chat"

    class _FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> "_FakeAsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def get(self, url: str, *args: Any, **kwargs: Any) -> _FakeResponse:
            calls.append(f"GET {url}")
            return _FakeResponse(200, content=b"voice-data")

        async def post(self, url: str, *args: Any, **kwargs: Any) -> _FakeResponse:
            calls.append(f"POST {url}")
            if url.endswith("/uploadFiles"):
                return _FakeResponse(200, json_data={"file-123": {"ok": True}})
            if url.endswith("/messages/voice"):
                return _FakeResponse(404, text="not_found")
            if url.endswith("/messages/file"):
                return _FakeResponse(200, text='{"ok":true}')
            return _FakeResponse(500, text="unexpected")

    monkeypatch.setattr(worker_module.avito_integration, "ensure_access_token", fake_ensure_access_token)
    monkeypatch.setattr(worker_module, "get_lead_peer", fake_get_lead_peer)
    monkeypatch.setattr(worker_module.httpx, "AsyncClient", _FakeAsyncClient)

    status, body = await worker_module.send_avito(
        tenant_id=101,
        lead_id=892595514860754996,
        text="",
        attachments=[
            {
                "type": "voice",
                "url": "https://example.com/voice.ogg",
                "name": "voice.ogg",
                "mime": "audio/ogg",
            }
        ],
    )

    assert status == 200
    assert "uploadFiles" in " ".join(calls)
    assert any("/messages/voice" in item for item in calls)
    assert any("/messages/file" in item for item in calls)
    assert body == '{"ok":true}'


@pytest.mark.anyio
async def test_send_avito_voice_attachment_with_avito_voice_id(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    async def fake_ensure_access_token(_tenant: int) -> tuple[str, dict[str, Any]]:
        return "token-1", {"account_id": 374186368, "refresh_token": "r1"}

    async def fake_get_lead_peer(_lead_id: int, *, channel: str | None = None) -> str:
        assert channel == "avito"
        return "u2i-test-chat"

    class _FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> "_FakeAsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def get(self, url: str, *args: Any, **kwargs: Any) -> _FakeResponse:
            calls.append(f"GET {url}")
            return _FakeResponse(500, text="unexpected")

        async def post(self, url: str, *args: Any, **kwargs: Any) -> _FakeResponse:
            calls.append(f"POST {url}")
            if url.endswith("/messages/voice"):
                return _FakeResponse(200, text='{"ok":true}')
            return _FakeResponse(500, text="unexpected")

    monkeypatch.setattr(worker_module.avito_integration, "ensure_access_token", fake_ensure_access_token)
    monkeypatch.setattr(worker_module, "get_lead_peer", fake_get_lead_peer)
    monkeypatch.setattr(worker_module.httpx, "AsyncClient", _FakeAsyncClient)

    status, body = await worker_module.send_avito(
        tenant_id=101,
        lead_id=892595514860754996,
        text="",
        attachments=[
            {
                "type": "voice",
                "voice_id": "8543e2a2-a7fb-4803-a123-3d6d3c9b4f71",
                "mime": "audio/mp4",
            }
        ],
    )

    assert status == 200
    assert any("/messages/voice" in item for item in calls)
    assert "uploadFiles" not in " ".join(calls)
    assert body == '{"ok":true}'


@pytest.mark.anyio
async def test_handle_avito_incoming_photo_calls_amocrm_once(monkeypatch: pytest.MonkeyPatch) -> None:
    amocrm_calls: list[tuple[int, int, str, str, list[dict[str, Any]]]] = []

    class _FakeRedis:
        async def set(self, *args: Any, **kwargs: Any) -> None:
            return None

        async def get(self, *args: Any, **kwargs: Any) -> None:
            return None

        async def lpush(self, *args: Any, **kwargs: Any) -> None:
            return None

    async def _noop_async(*args: Any, **kwargs: Any) -> None:
        return None

    async def fake_get_or_create_by_peer(*args: Any, **kwargs: Any) -> int:
        return 892595514860754996

    async def fake_lead_exists(*args: Any, **kwargs: Any) -> bool:
        return True

    async def fake_maybe_amocrm_inbound(
        tenant_id: int,
        lead_id: int,
        text: str,
        channel: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
    ) -> None:
        amocrm_calls.append((tenant_id, lead_id, text, channel, list(attachments or [])))

    monkeypatch.setattr(worker_module, "r", _FakeRedis(), raising=False)
    monkeypatch.setattr(
        worker_module.avito_integration,
        "get_integration",
        lambda _tenant: {"access_token": "a", "refresh_token": "r", "account_id": 374186368},
    )
    monkeypatch.setattr(worker_module.avito_integration, "update_integration", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker_module, "get_or_create_by_peer", fake_get_or_create_by_peer)
    monkeypatch.setattr(worker_module, "lead_exists", fake_lead_exists)
    monkeypatch.setattr(worker_module.followups, "handle_opt_out", _noop_async)
    monkeypatch.setattr(worker_module.followups, "capture_followup_answer", _noop_async)
    monkeypatch.setattr(worker_module.followups, "schedule_followups", _noop_async)
    monkeypatch.setattr(worker_module, "resolve_or_create_contact", _noop_async)
    monkeypatch.setattr(worker_module, "link_lead_contact", _noop_async)
    monkeypatch.setattr(worker_module, "insert_message_in", _noop_async)
    monkeypatch.setattr(worker_module, "_maybe_amocrm_inbound", fake_maybe_amocrm_inbound)
    monkeypatch.setattr(worker_module, "_match_behavior_trigger", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker_module, "_mark_handoff_silence", _noop_async)
    monkeypatch.setattr(worker_module, "_cancel_pending_smart_reply", _noop_async)
    monkeypatch.setattr(worker_module, "_resolve_avito_user_name", _noop_async)

    event = {
        "tenant": 101,
        "chat_id": "u2i-photo-case",
        "message_id": "msg-photo-1",
        "text": "",
        "attachments": [
            {
                "type": "image",
                "url": "https://example.com/photo.jpg",
                "name": "photo.jpg",
            }
        ],
        "avito_login": "user_test",
        "_incoming_stored": True,
    }

    await worker_module._handle_avito_incoming(event)

    assert len(amocrm_calls) == 1
    assert amocrm_calls[0][3] == "avito"
    assert amocrm_calls[0][4][0]["type"] == "image"


def test_extract_tg_username_from_text_variants() -> None:
    assert worker_module._extract_tg_username("мой tg: @Isyyaa") == "@Isyyaa"
    assert worker_module._extract_tg_username("пишите сюда https://t.me/Isyyaa") == "@Isyyaa"
    assert worker_module._extract_tg_username("email user@example.com") == ""
    assert worker_module._extract_tg_username("тут нет username") == ""


@pytest.mark.anyio
async def test_handle_avito_incoming_username_bridges_to_telegram(monkeypatch: pytest.MonkeyPatch) -> None:
    sent_payloads: list[tuple[int, str, str, int | None, int | None]] = []

    class _FakeRedis:
        async def set(self, *args: Any, **kwargs: Any) -> None:
            return None

        async def get(self, *args: Any, **kwargs: Any) -> None:
            return None

        async def lpush(self, *args: Any, **kwargs: Any) -> None:
            return None

    async def _noop_async(*args: Any, **kwargs: Any) -> None:
        return None

    async def fake_get_or_create_by_peer(*args: Any, **kwargs: Any) -> int:
        return 892595514860754996

    async def fake_lead_exists(*args: Any, **kwargs: Any) -> bool:
        return True

    async def fake_send_to_username(
        tenant_id: int,
        username: str,
        text: str,
        *,
        lead_id: int | None = None,
        contact_id: int | None = None,
    ) -> tuple[int, str]:
        sent_payloads.append((tenant_id, username, text, lead_id, contact_id))
        return 200, '{"ok":true}'

    async def fake_send_to_phone(*args: Any, **kwargs: Any) -> tuple[int, str]:
        raise AssertionError("phone flow must not be called for username-only message")

    monkeypatch.setattr(worker_module, "r", _FakeRedis(), raising=False)
    monkeypatch.setattr(
        worker_module.avito_integration,
        "get_integration",
        lambda _tenant: {"access_token": "a", "refresh_token": "r", "account_id": 374186368},
    )
    monkeypatch.setattr(worker_module.avito_integration, "update_integration", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker_module, "get_or_create_by_peer", fake_get_or_create_by_peer)
    monkeypatch.setattr(worker_module, "lead_exists", fake_lead_exists)
    monkeypatch.setattr(worker_module.followups, "handle_opt_out", _noop_async)
    monkeypatch.setattr(worker_module.followups, "capture_followup_answer", _noop_async)
    monkeypatch.setattr(worker_module.followups, "schedule_followups", _noop_async)
    monkeypatch.setattr(worker_module, "resolve_or_create_contact", _noop_async)
    monkeypatch.setattr(worker_module, "link_lead_contact", _noop_async)
    monkeypatch.setattr(worker_module, "insert_message_in", _noop_async)
    monkeypatch.setattr(worker_module, "_maybe_amocrm_inbound", _noop_async)
    monkeypatch.setattr(worker_module, "_match_behavior_trigger", lambda *_a, **_kw: None)
    monkeypatch.setattr(worker_module, "_mark_handoff_silence", _noop_async)
    monkeypatch.setattr(worker_module, "_cancel_pending_smart_reply", _noop_async)
    monkeypatch.setattr(worker_module, "_resolve_avito_user_name", _noop_async)
    monkeypatch.setattr(worker_module, "_send_telegram_to_username", fake_send_to_username)
    monkeypatch.setattr(worker_module, "_send_telegram_to_phone", fake_send_to_phone)

    event = {
        "tenant": 101,
        "chat_id": "u2i-username-case",
        "message_id": "msg-user-1",
        "text": "пишите в телеграм @Isyyaa",
        "avito_login": "user_test",
        "_incoming_stored": True,
    }

    await worker_module._handle_avito_incoming(event)

    assert len(sent_payloads) == 1
    tenant_id, username, text, lead_id, contact_id = sent_payloads[0]
    assert tenant_id == 101
    assert username == "@Isyyaa"
    assert text.strip() != ""
    assert lead_id == 892595514860754996
    assert contact_id is None
