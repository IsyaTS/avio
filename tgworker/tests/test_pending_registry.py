from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from tgworker.api import create_app
from tgworker.session_manager import LoginFlowStateSnapshot, SessionSnapshot, TwoFASubmitResult


class StubSessionManager:
    def __init__(self, now: float) -> None:
        self.now = now
        self.stage = "idle"
        self.qr_png = b"stub-png"
        self.current_snapshot = SessionSnapshot(
            tenant_id=0,
            status="disconnected",
            qr_id=None,
            qr_valid_until=None,
            twofa_pending=False,
            twofa_since=None,
            last_error=None,
        )
        self.current_flow = LoginFlowStateSnapshot(
            tenant_id=0,
            status="disconnected",
            qr_id=None,
            qr_login_obj=None,
            qr_png=None,
            qr_expires_at=None,
            last_error=None,
            needs_2fa=False,
            twofa_pending=False,
        )

    async def start(self) -> None:  # pragma: no cover - wiring
        return None

    async def shutdown(self) -> None:  # pragma: no cover - wiring
        return None

    async def start_session(self, tenant: int, force: bool = True) -> SessionSnapshot:
        self.stage = "waiting"
        self.current_snapshot = SessionSnapshot(
            tenant_id=tenant,
            status="waiting_qr",
            qr_id="qr-%d" % tenant,
            qr_valid_until=int((self.now + 60) * 1000),
            twofa_pending=False,
            twofa_since=None,
            last_error=None,
        )
        self.current_flow = LoginFlowStateSnapshot(
            tenant_id=tenant,
            status="waiting_qr",
            qr_id="qr-%d" % tenant,
            qr_login_obj=None,
            qr_png=self.qr_png,
            qr_expires_at=self.now + 60,
            last_error=None,
            needs_2fa=False,
            twofa_pending=False,
        )
        return self.current_snapshot

    async def get_status(self, tenant: int) -> SessionSnapshot:
        return self.current_snapshot

    async def login_flow_state(self, tenant: int) -> LoginFlowStateSnapshot:
        return self.current_flow

    def get_qr_png(self, qr_id: str, tenant: int | None = None) -> bytes:
        return self.qr_png

    def stats_snapshot(self) -> dict[str, int]:
        return {
            "authorized": int(self.stage == "authorized"),
            "waiting": int(self.stage == "waiting"),
            "needs_2fa": int(self.stage == "need_2fa"),
        }

    async def submit_password(self, tenant: int, password: str) -> TwoFASubmitResult:
        self.stage = "authorized"
        self.current_snapshot = SessionSnapshot(
            tenant_id=tenant,
            status="authorized",
            qr_id=None,
            qr_valid_until=None,
            twofa_pending=False,
            twofa_since=None,
            last_error=None,
        )
        self.current_flow = LoginFlowStateSnapshot(
            tenant_id=tenant,
            status="authorized",
            qr_id=None,
            qr_login_obj=None,
            qr_png=None,
            qr_expires_at=None,
            last_error=None,
            needs_2fa=False,
            twofa_pending=False,
        )
        return TwoFASubmitResult(status_code=200, body={"ok": True}, headers=None)

    def require_twofa(self, tenant: int) -> None:
        self.stage = "need_2fa"
        self.current_snapshot = SessionSnapshot(
            tenant_id=tenant,
            status="needs_2fa",
            qr_id=None,
            qr_valid_until=None,
            twofa_pending=True,
            twofa_since=int(self.now * 1000),
            last_error=None,
        )
        self.current_flow = LoginFlowStateSnapshot(
            tenant_id=tenant,
            status="need_2fa",
            qr_id=None,
            qr_login_obj=None,
            qr_png=None,
            qr_expires_at=None,
            last_error=None,
            needs_2fa=True,
            twofa_pending=True,
        )

    def expire_qr(self, tenant: int) -> None:
        self.stage = "waiting"
        self.current_snapshot = SessionSnapshot(
            tenant_id=tenant,
            status="waiting_qr",
            qr_id="qr-%d" % tenant,
            qr_valid_until=int((self.now - 1) * 1000),
            twofa_pending=False,
            twofa_since=None,
            last_error=None,
        )
        self.current_flow = LoginFlowStateSnapshot(
            tenant_id=tenant,
            status="waiting_qr",
            qr_id="qr-%d" % tenant,
            qr_login_obj=None,
            qr_png=self.qr_png,
            qr_expires_at=self.now - 1,
            last_error=None,
            needs_2fa=False,
            twofa_pending=False,
        )


@pytest.fixture
def stub_manager(monkeypatch, tmp_path):
    manager = StubSessionManager(now=1_700_000_000.0)

    def _fake_config():
        return SimpleNamespace(
            api_id=1,
            api_hash="hash",
            sessions_dir=tmp_path,
            device_model="Test",
            system_version="Linux",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru",
            qr_ttl=120.0,
            qr_poll_interval=1.0,
        )

    monkeypatch.setattr("tgworker.api.SessionManager", lambda *args, **kwargs: manager)
    monkeypatch.setattr("tgworker.api.telegram_config", _fake_config)

    monkeypatch.setattr("tgworker.api.time.time", lambda: manager.now)

    app = create_app()
    return manager, app


def test_pending_flow_transitions(monkeypatch, stub_manager):
    manager, app = stub_manager
    client = TestClient(app)

    start_response = client.post("/qr/start", json={"tenant": 1})
    assert start_response.status_code == 200
    entry = app.state.pending_registry[1]
    assert entry.state == "waiting_qr"
    assert entry.authorized is False

    manager.require_twofa(1)
    manager.now += 5
    status_response = client.get("/status", params={"tenant": 1})
    assert status_response.status_code == 200
    entry = app.state.pending_registry[1]
    assert entry.state == "need_2fa"
    assert entry.last_error is None

    submit_response = client.post("/2fa", json={"tenant": 1, "password": "secret"})
    assert submit_response.status_code == 200
    entry = app.state.pending_registry[1]
    assert entry.state == "authorized"
    assert entry.authorized is True
    assert entry.last_error is None


def test_pending_qr_timeout_marks_failed(monkeypatch, stub_manager):
    manager, app = stub_manager
    client = TestClient(app)

    client.post("/qr/start", json={"tenant": 2})
    entry = app.state.pending_registry[2]
    manager.expire_qr(2)
    manager.now += 200

    qr_id = entry.qr_id or ""
    qr_response = client.get("/qr/png", params={"tenant": 2, "qr_id": qr_id})
    assert qr_response.status_code in (404, 410)
    entry = app.state.pending_registry[2]
    assert entry.state == "failed"
    assert entry.last_error == "qr_expired"
