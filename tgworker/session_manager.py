from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple

from .manager import (
    QRExpiredError,
    QRNotFoundError,
    SessionState,
    TelegramSessionManager,
)


@dataclass(slots=True)
class SessionSnapshot:
    """Lightweight snapshot of the Telegram session state."""

    tenant_id: int
    status: str
    qr_id: Optional[str]
    qr_valid_until: Optional[int]
    needs_2fa: bool
    twofa_pending: bool
    twofa_since: Optional[int]
    last_error: Optional[str]

    @classmethod
    def from_state(cls, state: SessionState) -> "SessionSnapshot":
        qr_valid_until = None
        if state.qr_expires_at is not None:
            try:
                qr_valid_until = int(state.qr_expires_at * 1000)
            except Exception:
                qr_valid_until = None
        twofa_since = None
        if state.twofa_since is not None:
            try:
                twofa_since = int(state.twofa_since * 1000)
            except Exception:
                twofa_since = None
        return cls(
            tenant_id=state.tenant_id,
            status=state.status,
            qr_id=state.qr_id,
            qr_valid_until=qr_valid_until,
            needs_2fa=bool(state.needs_2fa),
            twofa_pending=bool(state.twofa_pending),
            twofa_since=twofa_since,
            last_error=state.last_error,
        )


class SessionManager:
    """Thin wrapper above ``TelegramSessionManager`` that exposes snapshots."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        sessions_dir,
        webhook_url: str,
        webhook_token: Optional[str] = None,
    ) -> None:
        self._manager = TelegramSessionManager(
            api_id=api_id,
            api_hash=api_hash,
            sessions_dir=sessions_dir,
            webhook_url=webhook_url,
            webhook_token=webhook_token,
        )

    async def start(self) -> None:
        await self._manager.start()

    async def shutdown(self) -> None:
        await self._manager.shutdown()

    def stats_snapshot(self) -> dict[str, Any]:
        return self._manager.stats_snapshot()

    async def start_session(
        self, tenant_id: int, *, force: bool = False
    ) -> Tuple[SessionSnapshot, Any]:
        state = await self._manager.start_session(tenant_id, force=force)
        snapshot = SessionSnapshot.from_state(state)
        return snapshot, state.qr_login

    async def poll_login(self, tenant_id: int, qr_login: Any | None = None) -> None:
        if qr_login is not None:
            state = self._manager._states.get(tenant_id)
            if state is not None:
                state.qr_login = qr_login
        await self._manager.poll_login(tenant_id)

    async def get_status(self, tenant_id: int) -> SessionSnapshot:
        state = await self._manager.get_status(tenant_id)
        return SessionSnapshot.from_state(state)

    async def hard_reset(self, tenant_id: int) -> SessionSnapshot:
        state = await self._manager.hard_reset(tenant_id)
        return SessionSnapshot.from_state(state)

    async def logout(self, tenant_id: int) -> None:
        await self._manager.logout(tenant_id)

    async def submit_password(self, tenant_id: int, password: str) -> bool:
        return await self._manager.submit_password(tenant_id, password)

    def get_qr_png(self, qr_id: str) -> bytes:
        return self._manager.get_qr_png(qr_id)

    def get_qr_url(self, qr_id: str) -> str:
        return self._manager.get_qr_url(qr_id)

    async def send_message(
        self,
        tenant: int,
        *,
        text: str | None = None,
        peer_id: int | None = None,
        username: str | None = None,
        media_url: str | None = None,
    ) -> None:
        await self._manager.send_message(
            tenant=tenant,
            text=text,
            peer_id=peer_id,
            username=username,
            media_url=media_url,
        )


__all__ = [
    "SessionManager",
    "SessionSnapshot",
    "QRExpiredError",
    "QRNotFoundError",
]
