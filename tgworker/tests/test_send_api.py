import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from tgworker.api import create_app


@pytest.fixture
def tgworker_app(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TELEGRAM_API_ID", "1")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.setattr(
        "tgworker.api.telegram_config",
        lambda: SimpleNamespace(
            api_id=1,
            api_hash="hash",
            sessions_dir=Path(tempfile.mkdtemp()),
            device_model="Test",
            system_version="1.0",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru",
            qr_ttl=120.0,
            qr_poll_interval=1.0,
        ),
    )

    class DummyManager:
        def __init__(self) -> None:
            self.sent_payloads: list[dict[str, object]] = []
            self.logout_calls: list[tuple[int, bool]] = []
            self.self_peer: int = 555

        async def start(self) -> None:  # pragma: no cover - startup hook
            return None

        async def shutdown(self) -> None:  # pragma: no cover - shutdown hook
            return None

        async def send_message(
            self,
            *,
            tenant: int,
            text: str | None = None,
            peer_id: int | None = None,
            telegram_user_id: int | None = None,
            username: str | None = None,
            attachments: list[dict[str, object]] | None = None,
            reply_to: str | None = None,
        ) -> dict[str, int | None]:
            self.sent_payloads.append(
                {
                    "tenant": tenant,
                    "text": text,
                    "peer_id": peer_id,
                    "attachments": attachments or [],
                    "reply_to": reply_to,
                }
            )
            resolved_peer = peer_id or self.self_peer
            return {"peer_id": resolved_peer, "message_id": 777}

        async def resolve_self_peer(self, tenant: int) -> int | None:
            return self.self_peer if tenant == 1 else None

        async def logout(self, tenant: int, *, force: bool = False) -> None:
            self.logout_calls.append((tenant, force))

        async def get_status(self, tenant: int):  # pragma: no cover - unused in tests
            return SimpleNamespace(
                to_payload=lambda: {"status": "authorized"},
                twofa_pending=False,
                needs_2fa=False,
            )

        async def login_flow_state(self, tenant: int):  # pragma: no cover - unused in tests
            return SimpleNamespace(
                status="authorized",
                qr_id=None,
                qr_login_obj=None,
                qr_png=None,
                qr_expires_at=None,
                last_error=None,
                needs_2fa=False,
                twofa_pending=False,
            )

        def stats_snapshot(self) -> dict[str, int]:
            return {"authorized": 0, "waiting": 0, "needs_2fa": 0}

    dummy = DummyManager()
    monkeypatch.setattr("tgworker.api.SessionManager", lambda *args, **kwargs: dummy)
    app = create_app()
    with TestClient(app) as client:
        yield client, dummy


def test_send_defaults_channel(tgworker_app):
    client, manager = tgworker_app
    response = client.post(
        "/send",
        json={"tenant": 1, "to": 123456, "text": "ping"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["peer_id"] == 123456
    assert payload["message_id"] == 777
    assert manager.sent_payloads


def test_send_to_me_resolves_self_peer(tgworker_app):
    client, manager = tgworker_app
    manager.self_peer = 987654
    response = client.post(
        "/send",
        json={"tenant": 1, "channel": "telegram", "to": "me", "text": "hello"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["peer_id"] == 987654
    assert manager.sent_payloads[-1]["peer_id"] == 987654


def test_session_logout_aliases(tgworker_app):
    client, manager = tgworker_app
    first = client.post("/session/logout", json={"tenant": 1})
    second = client.post("/session/logout", json={"tenant_id": 2, "force": True})
    assert first.status_code == 200
    assert second.status_code == 200
    assert manager.logout_calls == [(1, False), (2, True)]
