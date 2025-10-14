from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from .manager import (
    QRExpiredError,
    QRNotFoundError,
    SessionState,
    TelegramSessionManager,
    TwoFASubmitResult,
    LoginFlowStateSnapshot,
    NotAuthorizedError,
    SESSION_DIR,
)


@dataclass(slots=True)
class SessionSnapshot:
    """Lightweight snapshot of the Telegram session state."""

    tenant_id: int
    status: str
    qr_id: Optional[str]
    qr_valid_until: Optional[int]
    twofa_pending: bool
    twofa_since: Optional[int]
    last_error: Optional[str]
    twofa_backoff_until: Optional[int] = None
    needs_2fa: bool = False
    can_restart: bool = False

    @classmethod
    def from_state(cls, state: SessionState) -> "SessionSnapshot":
        qr_valid_until = None
        if state.qr_expires_at is not None:
            try:
                value = float(state.qr_expires_at)
                if value < 10_000_000_000:
                    value *= 1000.0
                qr_valid_until = int(value)
            except Exception:
                qr_valid_until = None
        qr_id = state.qr_id
        needs_twofa = bool(getattr(state, "needs_2fa", False))
        if state.twofa_pending or needs_twofa or state.status == "needs_2fa":
            qr_id = None
            qr_valid_until = None
        twofa_since = None
        if state.twofa_since is not None:
            try:
                value = float(state.twofa_since)
                if value < 10_000_000_000:
                    value *= 1000.0
                twofa_since = int(value)
            except Exception:
                twofa_since = None
        twofa_pending = bool(state.twofa_pending or state.status == "needs_2fa" or needs_twofa)
        backoff_ms = None
        if state.twofa_backoff_until is not None:
            try:
                backoff_ms = int(float(state.twofa_backoff_until) * 1000)
            except Exception:
                backoff_ms = None

        return cls(
            tenant_id=state.tenant_id,
            status=state.status,
            qr_id=qr_id,
            qr_valid_until=qr_valid_until,
            twofa_pending=twofa_pending,
            twofa_since=twofa_since,
            last_error=state.last_error,
            can_restart=bool(getattr(state, "can_restart", False)),
            needs_2fa=bool(needs_twofa or twofa_pending),
            twofa_backoff_until=backoff_ms,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "status": self.status,
            "qr_id": self.qr_id,
            "qr_valid_until": self.qr_valid_until,
            "needs_2fa": bool(self.needs_2fa),
            "twofa_pending": bool(self.twofa_pending),
            "twofa_since": self.twofa_since,
            "last_error": self.last_error,
            "can_restart": bool(self.can_restart),
            "twofa_backoff_until": self.twofa_backoff_until,
        }


class SessionManager:
    """Thin wrapper above ``TelegramSessionManager`` that exposes snapshots."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        webhook_url: str,
        *,
        sessions_dir: Path = SESSION_DIR,
        device_model: str,
        system_version: str,
        app_version: str,
        lang_code: str,
        system_lang_code: str,
        webhook_token: Optional[str] = None,
        qr_ttl: float,
        qr_poll_interval: float,
    ) -> None:
        self._manager = TelegramSessionManager(
            api_id=api_id,
            api_hash=api_hash,
            sessions_dir=sessions_dir,
            webhook_url=webhook_url,
            device_model=device_model,
            system_version=system_version,
            app_version=app_version,
            lang_code=lang_code,
            system_lang_code=system_lang_code,
            webhook_token=webhook_token,
            qr_ttl=qr_ttl,
            qr_poll_interval=qr_poll_interval,
        )

    async def start(self) -> None:
        await self._manager.start(background=True)

    async def shutdown(self) -> None:
        await self._manager.shutdown()

    def stats_snapshot(self) -> dict[str, Any]:
        return self._manager.stats_snapshot()

    async def start_session(
        self, tenant_id: int, *, force: bool = False
    ) -> SessionSnapshot:
        state = await self._manager.start_session(tenant_id, force=force)
        if not state.twofa_pending and state.status == "waiting_qr":
            await self._manager.poll_login(tenant_id)
        return SessionSnapshot.from_state(state)

    async def poll_login(self, tenant_id: int) -> None:
        await self._manager.poll_login(tenant_id)

    async def get_status(self, tenant_id: int) -> SessionSnapshot:
        state = await self._manager.get_status(tenant_id)
        return SessionSnapshot.from_state(state)

    async def hard_reset(self, tenant_id: int) -> SessionSnapshot:
        state = await self._manager.hard_reset(tenant_id)
        return SessionSnapshot.from_state(state)

    async def logout(self, tenant_id: int, *, force: bool = False) -> None:
        await self._manager.logout(tenant_id, force=force)

    async def submit_password(self, tenant_id: int, password: str) -> TwoFASubmitResult:
        return await self._manager.submit_password(tenant_id, password)

    def get_qr_png(self, qr_id: str, tenant: int | None = None) -> bytes:
        return self._manager.get_qr_png(qr_id, tenant=tenant)

    def get_qr_url(self, qr_id: str) -> str:
        return self._manager.get_qr_url(qr_id)

    async def login_flow_state(self, tenant_id: int):
        return await self._manager.login_flow_state(tenant_id)

    async def send_message(
        self,
        tenant: int,
        *,
        text: str | None = None,
        peer_id: Any | None = None,
        telegram_user_id: int | None = None,
        username: str | None = None,
        attachments: list[Dict[str, Any]] | None = None,
        reply_to: str | None = None,
    ) -> dict[str, Any]:
        return await self._manager.send_message(
            tenant=tenant,
            text=text,
            peer_id=peer_id,
            telegram_user_id=telegram_user_id,
            username=username,
            attachments=attachments,
            reply_to=reply_to,
        )

    async def resolve_self_peer(self, tenant_id: int) -> Any:
        client = await self._manager.get_client(tenant_id)
        if client is None:
            raise NotAuthorizedError("session_not_authorized")
        try:
            from telethon import TelegramClient  # type: ignore
            from telethon.tl.types import InputPeerSelf  # type: ignore
        except Exception:
            return await self._manager.resolve_self_peer(tenant_id)
        if isinstance(client, TelegramClient):
            return InputPeerSelf()
        return await self._manager.resolve_self_peer(tenant_id)


__all__ = [
    "SessionManager",
    "SessionSnapshot",
    "QRExpiredError",
    "QRNotFoundError",
    "TwoFASubmitResult",
    "LoginFlowStateSnapshot",
    "NotAuthorizedError",
]
