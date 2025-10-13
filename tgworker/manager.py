from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import math
import os
import secrets
import time
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
import qrcode
from prometheus_client import Counter, Gauge
from app.lib.transport_utils import message_in_asdict
from app.schemas import Attachment, MessageIn
from telethon import TelegramClient, events, functions
from telethon.errors import BadRequestError, RPCError, SessionPasswordNeededError
from telethon.errors.rpcerrorlist import (
    AuthKeyUnregisteredError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhonePasswordFloodError,
)


LOGGER = logging.getLogger("tgworker")


QR_LOGIN_TIMEOUT = 120.0
NEEDS_2FA_TTL = 90.0
PASSWORD_FLOOD_BACKOFF = 60.0


SESSIONS_AUTHORIZED = Gauge(
    "tgworker_sessions_authorized",
    "Number of Telegram sessions that are currently authorized",
)
SESSIONS_WAITING = Gauge(
    "tgworker_sessions_waiting",
    "Number of Telegram sessions waiting for QR authorization",
)
SESSIONS_NEEDS_2FA = Gauge(
    "tgworker_sessions_needs_2fa",
    "Number of Telegram sessions requiring manual 2FA confirmation",
)
EVENT_ERRORS = Counter(
    "tgworker_events_errors_total",
    "Telegram session errors grouped by category",
    labelnames=("type",),
)
AUTHORIZED_DISCONNECTS = Counter(
    "tgworker_authorized_disconnect_total",
    "Authorized Telegram sessions transitioning to disconnected without manual logout",
    labelnames=("reason",),
)
MESSAGE_IN_COUNTER = Counter(
    "message_in_total",
    "Normalized incoming messages",
    labelnames=("channel",),
)
MESSAGE_OUT_COUNTER = Counter(
    "message_out_total",
    "Normalized outgoing messages",
    labelnames=("channel",),
)
SEND_FAIL_COUNTER = Counter(
    "send_fail_total",
    "Failed send attempts",
    labelnames=("channel", "reason"),
)


class QRNotFoundError(Exception):
    """Raised when a QR identifier is unknown or no longer tracked."""


class QRExpiredError(Exception):
    """Raised when a QR identifier has expired and should not be reused."""

    def __init__(self, valid_until: Optional[float] = None) -> None:
        super().__init__("qr_expired")
        self.valid_until = valid_until


@dataclass(slots=True)
class CachedQR:
    tenant_id: int
    png: bytes
    expires_at: Optional[float] = None


@dataclass(slots=True)
class SessionState:
    tenant_id: int
    status: str = "disconnected"
    qr_id: Optional[str] = None
    qr_png: Optional[bytes] = None
    qr_url: Optional[str] = None
    qr_expires_at: Optional[float] = None
    qr_login: Optional[Any] = None
    waiting_task: Optional[asyncio.Task[Any]] = None
    last_error: Optional[str] = None
    needs_2fa: bool = False
    awaiting_password: bool = False
    needs_2fa_expires_at: Optional[float] = None
    last_seen: Optional[float] = None
    can_restart: bool = False
    last_needs_2fa_at: Optional[float] = None
    restart_pending: bool = False
    twofa_pending: bool = False
    twofa_since: Optional[float] = None
    twofa_backoff_until: Optional[float] = None


@dataclass(slots=True)
class TwoFASubmitResult:
    status_code: int
    body: Dict[str, Any]
    headers: Optional[Dict[str, str]] = None

    def is_ok(self) -> bool:
        return 200 <= int(self.status_code) < 300


class TelegramSessionManager:
    """Manage tenant-scoped TelegramClient instances and QR flows."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        sessions_dir: Path,
        webhook_url: str,
        *,
        device_model: str,
        system_version: str,
        app_version: str,
        lang_code: str,
        system_lang_code: str,
        webhook_token: str | None = None,
        http_timeout: float = 10.0,
    ) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._sessions_dir = sessions_dir
        self._sessions_dir.mkdir(parents=True, exist_ok=True)
        self._webhook_url = webhook_url.rstrip("/")
        self._webhook_token = (webhook_token or "").strip() or None
        self._device_model = device_model
        self._system_version = system_version
        self._app_version = app_version
        self._lang_code = lang_code
        self._system_lang_code = system_lang_code
        self._http = httpx.AsyncClient(timeout=http_timeout)
        self._clients: Dict[int, TelegramClient] = {}
        self._states: Dict[int, SessionState] = {}
        self._qr_lookup: Dict[str, int] = {}
        self._qr_cache: Dict[str, CachedQR] = {}
        self._expired_qr: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._loop = asyncio.get_event_loop()
        self._started = False
        self._delivered_incoming: int = 0
        self._self_ids: Dict[int, Any] = {}
        self._bootstrap_task: Optional[asyncio.Task[None]] = None
        self._bootstrap_ready = asyncio.Event()
        self._bootstrap_ready.set()

    async def start(self, *, background: bool = False) -> None:
        if self._started:
            return
        self._started = True
        self._bootstrap_ready.clear()
        if background:
            self._bootstrap_task = self._loop.create_task(self._run_bootstrap())
        else:
            await self._run_bootstrap()

    async def wait_until_ready(self) -> None:
        await self._bootstrap_ready.wait()
        task = self._bootstrap_task
        if task and not task.done():
            with contextlib.suppress(Exception):
                await task

    async def _run_bootstrap(self) -> None:
        try:
            await self._bootstrap_existing_sessions()
        except Exception:
            LOGGER.exception("stage=bootstrap_failed_unhandled")
        finally:
            self._bootstrap_task = None
            self._bootstrap_ready.set()

    @staticmethod
    def _qr_valid_until_ms(expires_at: Optional[float]) -> Optional[int]:
        if not expires_at:
            return None
        try:
            return int(expires_at * 1000)
        except Exception:
            return None

    def _cleanup_expired_qr_cache(self) -> None:
        cutoff = time.time() - 900
        stale_keys = [
            qr_id for qr_id, ts in list(self._expired_qr.items()) if ts and ts < cutoff
        ]
        for qr_id in stale_keys:
            self._expired_qr.pop(qr_id, None)
        now = time.time()
        expired_cached = [
            qr_id
            for qr_id, cached in list(self._qr_cache.items())
            if cached.expires_at and cached.expires_at <= now
        ]
        for qr_id in expired_cached:
            self._qr_cache.pop(qr_id, None)
            self._qr_lookup.pop(qr_id, None)

    def _ensure_session_permissions(self, path: Path) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
        except OSError as exc:
            LOGGER.warning(
                "event=session_file_prepare_failed path=%s error=%s",
                path,
                exc,
            )
            return
        try:
            os.chmod(path, 0o600)
        except OSError as exc:
            LOGGER.warning(
                "event=session_file_chmod_failed path=%s error=%s",
                path,
                exc,
            )

    def _set_status(
        self,
        tenant: int,
        state: SessionState,
        status: str,
        *,
        reason: str | None = None,
    ) -> None:
        previous = state.status or "unknown"
        if previous != status:
            if reason:
                LOGGER.info(
                    "stage=state_transition tenant_id=%s from=%s to=%s reason=%s",
                    tenant,
                    previous,
                    status,
                    reason,
                )
            else:
                LOGGER.info(
                    "stage=state_transition tenant_id=%s from=%s to=%s",
                    tenant,
                    previous,
                    status,
                )
        if previous == "authorized" and status == "disconnected" and reason != "manual_logout":
            AUTHORIZED_DISCONNECTS.labels(reason or "unknown").inc()
        state.status = status

    def _record_qr_expired(
        self,
        tenant: int,
        qr_id: str,
        valid_until: Optional[float],
        *,
        reason: str,
    ) -> None:
        if not qr_id:
            return
        self._cleanup_expired_qr_cache()
        timestamp = valid_until if valid_until is not None else time.time()
        self._expired_qr[qr_id] = timestamp
        LOGGER.info(
            "event=qr_expired tenant_id=%s qr_id=%s qr_valid_until=%s reason=%s",
            tenant,
            qr_id,
            self._qr_valid_until_ms(valid_until),
            reason,
        )
        if reason in {"needs_2fa", "timeout", "expired"}:
            self._qr_cache.pop(qr_id, None)
            self._qr_lookup.pop(qr_id, None)

    @staticmethod
    def _clear_qr_state_locked(state: SessionState) -> None:
        state.qr_id = None
        state.qr_png = None
        state.qr_url = None
        state.qr_expires_at = None
        state.qr_login = None

    def _expire_qr_locked(
        self,
        tenant: int,
        state: SessionState,
        *,
        reason: str,
        set_error: bool = True,
    ) -> None:
        if not state.qr_id:
            return
        qr_id = state.qr_id
        self._qr_lookup.pop(qr_id, None)
        valid_until = state.qr_expires_at
        self._record_qr_expired(tenant, qr_id, valid_until, reason=reason)
        self._clear_qr_state_locked(state)
        if set_error:
            state.last_error = "qr_expired"
        state.can_restart = True

    def _extend_needs_2fa_ttl(self, state: SessionState) -> None:
        state.needs_2fa_expires_at = time.time() + NEEDS_2FA_TTL

    def _expire_needs_2fa_locked(
        self, tenant: int, state: SessionState
    ) -> tuple[Optional[TelegramClient], bool]:
        client: Optional[TelegramClient] = None
        expired = False
        if (
            state.status == "needs_2fa"
            and state.needs_2fa_expires_at is not None
            and state.needs_2fa_expires_at <= time.time()
        ):
            client = self._clients.pop(tenant, None)
            if state.qr_id:
                self._qr_cache.pop(state.qr_id, None)
                self._qr_lookup.pop(state.qr_id, None)
            self._set_status(tenant, state, "disconnected", reason="twofa_timeout")
            state.needs_2fa = False
            state.awaiting_password = False
            state.needs_2fa_expires_at = None
            state.qr_id = None
            state.qr_png = None
            state.qr_expires_at = None
            state.waiting_task = None
            state.qr_login = None
            state.last_error = "twofa_timeout"
            state.last_seen = time.time()
            state.restart_pending = False
            state.last_needs_2fa_at = None
            state.twofa_pending = False
            state.twofa_since = None
            state.twofa_backoff_until = None
            state.can_restart = True
            expired = True
            LOGGER.warning(
                "stage=needs_2fa_timeout event=twofa_timeout tenant_id=%s", tenant
            )
            self._update_metrics()
        return client, expired

    async def shutdown(self) -> None:
        await self.wait_until_ready()
        task = self._bootstrap_task
        if task and not task.done():
            with contextlib.suppress(Exception):
                await task
        for state in list(self._states.values()):
            if state.waiting_task and not state.waiting_task.done():
                state.waiting_task.cancel()
        for client in list(self._clients.values()):
            with contextlib.suppress(Exception):
                await client.disconnect()
        self._clients.clear()
        await self._http.aclose()
        self._update_metrics()

    async def _bootstrap_existing_sessions(self) -> None:
        for path in sorted(self._sessions_dir.glob("*.session")):
            tenant = self._tenant_from_path(path)
            if tenant is None:
                continue
            try:
                client = self._build_client(tenant)
                await client.connect()
                if await client.is_user_authorized():
                    self._clients[tenant] = client
                    self._states[tenant] = SessionState(
                        tenant_id=tenant, status="authorized", last_seen=time.time()
                    )
                    self._register_handlers(tenant, client)
                    LOGGER.info("stage=authorized tenant_id=%s event=bootstrap", tenant)
                else:
                    await client.disconnect()
                    self._states[tenant] = SessionState(tenant_id=tenant, status="disconnected")
            except Exception as exc:
                LOGGER.exception("stage=bootstrap_failed tenant_id=%s error=%s", tenant, exc)
                self._states[tenant] = SessionState(
                    tenant_id=tenant,
                    status="disconnected",
                    last_error=str(exc),
                )
        self._update_metrics()

    def _build_client(self, tenant: int) -> TelegramClient:
        session_path = self._sessions_dir / f"{tenant}.session"
        self._ensure_session_permissions(session_path)
        return TelegramClient(
            str(session_path),
            self._api_id,
            self._api_hash,
            device_model=self._device_model,
            system_version=self._system_version,
            app_version=self._app_version,
            lang_code=self._lang_code,
            system_lang_code=self._system_lang_code,
        )

    def _tenant_from_path(self, path: Path) -> Optional[int]:
        try:
            return int(path.stem)
        except ValueError:
            return None

    def _mark_authkey_unregistered_locked(
        self, tenant: int, state: SessionState
    ) -> Optional[asyncio.Task[Any]]:
        task: Optional[asyncio.Task[Any]] = None
        if state.waiting_task and not state.waiting_task.done():
            task = state.waiting_task
        state.waiting_task = None
        if state.qr_id:
            self._qr_lookup.pop(state.qr_id, None)
        self._clear_qr_state_locked(state)
        self._set_status(tenant, state, "disconnected", reason="authkey_unregistered")
        state.last_error = "authkey_unregistered"
        state.needs_2fa = False
        state.awaiting_password = False
        state.needs_2fa_expires_at = None
        state.restart_pending = False
        state.last_needs_2fa_at = None
        state.last_seen = time.time()
        state.twofa_pending = False
        state.twofa_since = None
        state.qr_login = None
        state.can_restart = True
        self._states[tenant] = state
        self._update_metrics()
        return task

    def _hard_reset_state_locked(
        self,
        tenant: int,
        state: Optional[SessionState] = None,
        *,
        reason: str = "reset",
        remove_session_file: bool = False,
    ) -> tuple[SessionState, Optional[TelegramClient], Optional[asyncio.Task[Any]], bool]:
        state = state or self._states.setdefault(tenant, SessionState(tenant_id=tenant))
        client = self._clients.pop(tenant, None)
        task: Optional[asyncio.Task[Any]] = None
        if state.waiting_task and not state.waiting_task.done():
            task = state.waiting_task
        state.waiting_task = None
        if state.qr_id:
            self._expire_qr_locked(tenant, state, reason=reason, set_error=False)
        removed_session_file = False
        if remove_session_file:
            path = self._sessions_dir / f"{tenant}.session"
            try:
                path.unlink()
                removed_session_file = True
            except FileNotFoundError:
                removed_session_file = False
        self._set_status(tenant, state, "disconnected", reason=reason)
        self._clear_qr_state_locked(state)
        state.last_error = None
        state.needs_2fa = False
        if state.needs_2fa_expires_at and state.needs_2fa_expires_at <= time.time():
            state.awaiting_password = False
            state.needs_2fa_expires_at = None
        state.can_restart = False
        state.restart_pending = False
        state.last_needs_2fa_at = None
        state.last_seen = time.time()
        state.twofa_pending = False
        state.twofa_since = None
        state.twofa_backoff_until = None
        state.qr_login = None
        self._states[tenant] = state
        self._update_metrics()
        return state, client, task, removed_session_file

    async def hard_reset(self, tenant: int) -> SessionState:
        await self.wait_until_ready()
        client_to_disconnect: Optional[TelegramClient] = None
        task_to_cancel: Optional[asyncio.Task[Any]] = None
        removed_file = False
        async with self._lock:
            state = self._states.get(tenant)
            state, client_to_disconnect, task_to_cancel, removed_file = self._hard_reset_state_locked(
                tenant, state, reason="hard_reset", remove_session_file=True
            )
        if task_to_cancel:
            task_to_cancel.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task_to_cancel
        if client_to_disconnect:
            with contextlib.suppress(Exception):
                await client_to_disconnect.disconnect()
        LOGGER.info(
            "stage=hard_reset tenant_id=%s removed_session_file=%s", tenant, removed_file
        )
        return self._states[tenant]

    async def start_session(self, tenant: int, *, force: bool = False) -> SessionState:
        await self.wait_until_ready()
        clients_to_disconnect: list[TelegramClient] = []
        tasks_to_cancel: list[asyncio.Task[Any]] = []
        result_state: Optional[SessionState] = None
        resume_client: Optional[TelegramClient] = None
        should_resume = False
        need_new_qr = force

        async with self._lock:
            state = self._states.get(tenant)
            if state:
                client, _ = self._expire_needs_2fa_locked(tenant, state)
                if client:
                    clients_to_disconnect.append(client)
            state = self._states.get(tenant) or state or SessionState(tenant_id=tenant)
            self._states[tenant] = state
            state.restart_pending = False

            if state.twofa_pending:
                state.can_restart = False
                result_state = state
                need_new_qr = False
            else:
                session_path = self._sessions_dir / f"{tenant}.session"
                if not force and session_path.exists():
                    client = self._clients.get(tenant)
                    if client is None:
                        client = self._build_client(tenant)
                        self._clients[tenant] = client
                    resume_client = client
                    should_resume = True

                if not need_new_qr:
                    if state.status == "authorized":
                        result_state = state
                    elif state.status == "needs_2fa":
                        if state.twofa_pending:
                            state.can_restart = False
                            result_state = state
                            should_resume = False
                        else:
                            need_new_qr = True
                    else:
                        stuck_statuses = {"disconnected", "error", "twofa_timeout"}
                        if state.status in stuck_statuses:
                            if state.last_error == "twofa_timeout" and not force:
                                result_state = state
                            else:
                                need_new_qr = True
                        elif state.last_error == "twofa_timeout" and not force:
                            result_state = state
                        elif state.last_error == "qr_login_timeout":
                            need_new_qr = True
                        elif state.status == "waiting_qr":
                            if not state.qr_id or (
                                state.waiting_task and state.waiting_task.done()
                            ):
                                need_new_qr = True
                            else:
                                should_resume = False
                        else:
                            need_new_qr = True

                if result_state is None and not need_new_qr:
                    result_state = state

                if (
                    not force
                    and result_state is state
                    and state.last_error == "twofa_timeout"
                ):
                    should_resume = False

        need_new_qr_after_resume = need_new_qr
        if (
            should_resume
            and resume_client is not None
            and (result_state is None or result_state.status != "authorized")
        ):
            try:
                if not resume_client.is_connected():
                    await resume_client.connect()
                if await resume_client.is_user_authorized():
                    async with self._lock:
                        state = self._states.setdefault(
                            tenant, SessionState(tenant_id=tenant)
                        )
                        self._set_status(tenant, state, "authorized", reason="session_resume")
                        state.last_error = None
                        state.needs_2fa = False
                        state.awaiting_password = False
                        state.needs_2fa_expires_at = None
                        state.last_seen = time.time()
                        state.waiting_task = None
                        state.qr_login = None
                        state.can_restart = False
                        state.twofa_pending = False
                        state.twofa_since = None
                        self._states[tenant] = state
                        self._update_metrics()
                        result_state = state
                    self._register_handlers(tenant, resume_client)
                    LOGGER.info("stage=authorized tenant_id=%s event=session_resume", tenant)
                    need_new_qr_after_resume = False
                else:
                    need_new_qr_after_resume = True
            except AuthKeyUnregisteredError:
                EVENT_ERRORS.labels("authkey_unregistered").inc()
                async with self._lock:
                    state = self._states.setdefault(
                        tenant, SessionState(tenant_id=tenant)
                    )
                    task = self._mark_authkey_unregistered_locked(tenant, state)
                    if task:
                        tasks_to_cancel.append(task)
                    stored = self._clients.pop(tenant, None)
                    if stored and stored is not resume_client:
                        clients_to_disconnect.append(stored)
                    result_state = state
                clients_to_disconnect.append(resume_client)
                LOGGER.warning(
                    "stage=authkey_unregistered tenant_id=%s source=start_session_resume",
                    tenant,
                )
                need_new_qr_after_resume = False
            except Exception as exc:
                LOGGER.exception(
                    "stage=session_resume_failed tenant_id=%s error=%s", tenant, exc
                )
                async with self._lock:
                    state = self._states.setdefault(
                        tenant, SessionState(tenant_id=tenant)
                    )
                    state.last_error = str(exc)
                    state.last_seen = time.time()
                    self._states[tenant] = state
                stored = self._clients.pop(tenant, None)
                if stored and stored is not resume_client:
                    clients_to_disconnect.append(stored)
                clients_to_disconnect.append(resume_client)
                need_new_qr_after_resume = True

        async with self._lock:
            state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
            if state.twofa_pending:
                state.can_restart = False
                result_state = state
                need_new_qr_after_resume = False

            if need_new_qr_after_resume:
                state, client, task, _ = self._hard_reset_state_locked(
                    tenant, state, reason="regen", remove_session_file=False
                )
                if client:
                    clients_to_disconnect.append(client)
                if task:
                    tasks_to_cancel.append(task)

                client = self._build_client(tenant)
                phase = "connect"
                try:
                    await client.connect()
                    self._clients[tenant] = client
                    phase = "qr_login"
                    qr_login = await client.qr_login()
                except AuthKeyUnregisteredError:
                    EVENT_ERRORS.labels("authkey_unregistered").inc()
                    state = self._states.setdefault(
                        tenant, SessionState(tenant_id=tenant)
                    )
                    task = self._mark_authkey_unregistered_locked(tenant, state)
                    if task:
                        tasks_to_cancel.append(task)
                    self._clients.pop(tenant, None)
                    clients_to_disconnect.append(client)
                    LOGGER.warning(
                        "stage=authkey_unregistered tenant_id=%s source=start_session_%s",
                        tenant,
                        phase,
                    )
                    result_state = state
                else:
                    png = self._build_qr_png(qr_login.url)
                    qr_id = secrets.token_urlsafe(16)

                    qr_expires_at = time.time() + 180.0
                    expires_raw = getattr(qr_login, "expires", None)
                    if isinstance(expires_raw, (int, float)):
                        qr_expires_at = float(expires_raw)
                    else:
                        try:
                            qr_expires_at = float(expires_raw.timestamp())  # type: ignore[arg-type]
                        except Exception:
                            pass

                    self._set_status(tenant, state, "waiting_qr", reason="qr_login")
                    state.qr_id = qr_id
                    state.qr_png = png
                    state.qr_url = qr_login.url
                    state.qr_expires_at = qr_expires_at
                    state.qr_login = qr_login
                    state.last_error = None
                    state.needs_2fa = False
                    state.awaiting_password = False
                    state.needs_2fa_expires_at = None
                    state.can_restart = False
                    state.restart_pending = False
                    state.last_needs_2fa_at = None
                    state.last_seen = time.time()
                    state.twofa_pending = False
                    state.twofa_since = None
                    state.twofa_backoff_until = None
                    self._qr_lookup[qr_id] = tenant
                    LOGGER.info("stage=qr_start tenant_id=%s qr_id=%s", tenant, qr_id)
                    LOGGER.info(
                        "event=qr_new tenant_id=%s qr_id=%s qr_valid_until=%s",
                        tenant,
                        qr_id,
                        self._qr_valid_until_ms(state.qr_expires_at),
                    )

                    state.waiting_task = None
                    self._update_metrics()
                    self._qr_cache[qr_id] = CachedQR(
                        tenant_id=tenant,
                        png=png,
                        expires_at=state.qr_expires_at,
                    )
                    result_state = state
            else:
                self._update_metrics()
                if result_state is None:
                    result_state = state

        for task in tasks_to_cancel:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for client in clients_to_disconnect:
            with contextlib.suppress(Exception):
                await client.disconnect()

        if result_state is None:
            result_state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))

        return result_state

    async def poll_login(self, tenant: int) -> None:
        await self.wait_until_ready()
        async with self._lock:
            state = self._states.get(tenant)
            if not state:
                return
            if state.status != "waiting_qr" or state.qr_login is None:
                return
            if state.waiting_task and not state.waiting_task.done():
                return
            client = self._clients.get(tenant)
            if client is None:
                LOGGER.warning("stage=poll_login_skip tenant_id=%s reason=no_client", tenant)
                return
            qr_login = state.qr_login
            task = self._loop.create_task(
                self._poll_login_loop(tenant, client, state, qr_login)
            )
            state.waiting_task = task

    async def _poll_login_loop(self, tenant: int, client: TelegramClient, state: SessionState, qr_login) -> None:
        qr_id = state.qr_id
        valid_until = state.qr_expires_at or (time.time() + QR_LOGIN_TIMEOUT)
        deadline = valid_until if valid_until else time.time() + QR_LOGIN_TIMEOUT
        poll_step = 5.0
        try:
            while True:
                now = time.time()
                remaining = deadline - now
                if remaining <= 0:
                    raise asyncio.TimeoutError
                timeout = poll_step if poll_step < remaining else remaining
                if timeout <= 0:
                    raise asyncio.TimeoutError
                timeout = min(max(timeout, 0.1), remaining)
                try:
                    result = await qr_login.wait(timeout=timeout)
                except asyncio.TimeoutError:
                    continue
                if result is None:
                    await asyncio.sleep(0.1)
                    continue
                LOGGER.info("stage=qr_ready tenant_id=%s", tenant)
                self._set_status(tenant, state, "authorized", reason="qr_ready")
                state.last_error = None
                if qr_id:
                    LOGGER.info(
                        "event=qr_scanned tenant_id=%s qr_id=%s qr_valid_until=%s",
                        tenant,
                        qr_id,
                        self._qr_valid_until_ms(valid_until),
                    )
                    self._record_qr_expired(tenant, qr_id, valid_until, reason="scanned")
                self._clear_qr_state_locked(state)
                state.needs_2fa = False
                state.last_seen = time.time()
                state.twofa_pending = False
                state.twofa_since = None
                state.twofa_backoff_until = None
                self._register_handlers(tenant, client)
                LOGGER.info("stage=authorized tenant_id=%s", tenant)
                break
        except SessionPasswordNeededError:
            self._set_status(tenant, state, "needs_2fa", reason="qr_password_required")
            state.needs_2fa = True
            state.awaiting_password = True
            if qr_id:
                self._record_qr_expired(tenant, qr_id, valid_until, reason="needs_2fa")
            self._clear_qr_state_locked(state)
            state.last_error = "two_factor_required"
            state.last_seen = time.time()
            self._extend_needs_2fa_ttl(state)
            timestamp_sec = time.time()
            state.last_needs_2fa_at = timestamp_sec
            state.twofa_pending = True
            state.twofa_since = int(timestamp_sec * 1000.0)
            state.qr_id = None
            state.qr_expires_at = None
            state.qr_login = None
            state.can_restart = False
            state.twofa_backoff_until = None
            EVENT_ERRORS.labels("needs_2fa").inc()
            LOGGER.warning(
                "stage=needs_2fa state=needs_2fa ttl=%ss tenant_id=%s",
                int(NEEDS_2FA_TTL),
                tenant,
            )
        except AuthKeyUnregisteredError:
            await self._handle_authkey_unregistered(
                tenant, client, source="poll_login_loop"
            )
        except asyncio.TimeoutError:
            self._set_status(tenant, state, "disconnected", reason="qr_login_timeout")
            state.last_error = "qr_login_timeout"
            state.needs_2fa = False
            state.awaiting_password = False
            state.twofa_pending = False
            state.twofa_since = None
            if qr_id:
                self._record_qr_expired(tenant, qr_id, valid_until, reason="timeout")
            self._clear_qr_state_locked(state)
            EVENT_ERRORS.labels("timeout").inc()
            LOGGER.warning("stage=qr_timeout event=qr_timeout tenant_id=%s", tenant)
        except asyncio.CancelledError:
            LOGGER.info("stage=qr_cancel tenant_id=%s", tenant)
            raise
        except RPCError as exc:
            self._set_status(tenant, state, "disconnected", reason="rpc_error")
            state.last_error = str(exc)
            state.needs_2fa = False
            state.awaiting_password = False
            state.twofa_pending = False
            state.twofa_since = None
            if qr_id:
                self._record_qr_expired(tenant, qr_id, valid_until, reason="rpc_error")
            self._clear_qr_state_locked(state)
            EVENT_ERRORS.labels("rpc_error").inc()
            LOGGER.error("stage=send_fail tenant_id=%s error=%s", tenant, exc)
        except Exception as exc:
            self._set_status(tenant, state, "disconnected", reason="exception")
            state.last_error = str(exc)
            state.needs_2fa = False
            state.awaiting_password = False
            state.twofa_pending = False
            state.twofa_since = None
            if qr_id:
                self._record_qr_expired(tenant, qr_id, valid_until, reason="exception")
            self._clear_qr_state_locked(state)
            EVENT_ERRORS.labels("exception").inc()
            LOGGER.exception("stage=qr_fail tenant_id=%s", tenant)
        finally:
            if qr_id:
                self._qr_lookup.pop(qr_id, None)
            state.waiting_task = None
            state.qr_login = None
            self._update_metrics()



    async def submit_password(self, tenant: int, password: str) -> TwoFASubmitResult:
        await self.wait_until_ready()
        secret = password or ""
        if not secret.strip():
            return TwoFASubmitResult(
                status_code=400,
                body={"error": "password_required"},
            )

        client_to_disconnect: Optional[TelegramClient] = None
        expired_twofa = False
        client: Optional[TelegramClient] = None
        early_result: Optional[TwoFASubmitResult] = None

        async with self._lock:
            state = self._states.get(tenant)
            if not state:
                return TwoFASubmitResult(
                    status_code=409,
                    body={"error": "twofa_not_pending"},
                )

            client_to_disconnect, expired_twofa = self._expire_needs_2fa_locked(tenant, state)
            if not expired_twofa:
                awaiting_valid = (
                    state.awaiting_password
                    and (
                        state.needs_2fa_expires_at is None
                        or state.needs_2fa_expires_at > time.time()
                    )
                )
                accepts_password = (
                    state.status == "needs_2fa" or state.twofa_pending or awaiting_valid
                )
                if not accepts_password:
                    if state.status == "authorized":
                        early_result = TwoFASubmitResult(
                            status_code=200,
                            body={"ok": True},
                        )
                    else:
                        early_result = TwoFASubmitResult(
                            status_code=409,
                            body={"error": "twofa_not_pending"},
                        )
                else:
                    now = time.time()
                    if state.twofa_backoff_until and state.twofa_backoff_until > now:
                        retry_after = max(
                            1, int(math.ceil(state.twofa_backoff_until - now))
                        )
                        detail = f"flood_wait {retry_after}"
                        LOGGER.warning(
                            "stage=password_failed event=backoff_active tenant_id=%s detail=%s",
                            tenant,
                            detail,
                        )
                        early_result = TwoFASubmitResult(
                            status_code=429,
                            body={
                                "error": "flood_wait",
                                "retry_after": retry_after,
                                "detail": detail,
                            },
                            headers={"Retry-After": str(retry_after)},
                        )
                    else:
                        client = self._clients.get(tenant)
                        if client is None:
                            client = self._build_client(tenant)
                            await client.connect()
                            self._clients[tenant] = client
                        elif not client.is_connected():
                            await client.connect()

                        self._set_status(tenant, state, "needs_2fa", reason="password_submit")
                        state.needs_2fa = True
                        state.awaiting_password = True
                        state.twofa_pending = True
                        if state.twofa_since is None:
                            state.twofa_since = int(time.time() * 1000.0)
                        state.qr_id = None
                        state.qr_png = None
                        state.qr_expires_at = None
                        state.last_seen = time.time()
                        state.last_needs_2fa_at = time.time()
                        state.twofa_backoff_until = None
                        self._extend_needs_2fa_ttl(state)

        if client_to_disconnect:
            with contextlib.suppress(Exception):
                await client_to_disconnect.disconnect()

        if expired_twofa:
            return TwoFASubmitResult(
                status_code=409,
                body={"error": "twofa_expired"},
            )

        if early_result is not None:
            return early_result

        if client is None:
            return TwoFASubmitResult(
                status_code=400,
                body={"error": "password_exception", "detail": "client_unavailable"},
            )

        async def _mark_failure(
            reason: str,
            error_code: str,
            *,
            backoff: Optional[float] = None,
            last_error: Optional[str] = None,
        ) -> None:
            async with self._lock:
                state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
                self._set_status(tenant, state, "needs_2fa", reason=reason)
                state.needs_2fa = True
                state.awaiting_password = True
                state.last_error = last_error if last_error is not None else error_code
                state.last_seen = time.time()
                self._extend_needs_2fa_ttl(state)
                state.twofa_pending = True
                state.twofa_since = int(time.time() * 1000.0)
                if backoff and backoff > 0:
                    state.twofa_backoff_until = time.time() + backoff
                else:
                    state.twofa_backoff_until = None
                self._update_metrics()

        async def _failure_response(
            *,
            status: int,
            error: str,
            event: str,
            detail: Optional[str] = None,
            retry_after: Optional[int] = None,
            backoff: Optional[float] = None,
            exc: Optional[Exception] = None,
            extra: Optional[Dict[str, Any]] = None,
            last_error: Optional[str] = None,
        ) -> TwoFASubmitResult:
            EVENT_ERRORS.labels("password_failed").inc()
            await _mark_failure(event, error, backoff=backoff, last_error=last_error)
            parts = [f"stage=password_failed event={event} tenant_id={tenant}"]
            detail_value = detail or error
            if retry_after is not None:
                parts.append(f"retry_after={retry_after}")
            if detail_value:
                parts.append(f"detail={detail_value}")
            message = " ".join(parts)
            if event == "password_exception":
                if exc is not None:
                    LOGGER.exception(message, exc_info=exc)
                else:
                    LOGGER.error(message)
            else:
                LOGGER.warning(message)
            headers = None
            body: Dict[str, Any] = {"error": error}
            if detail_value:
                body["detail"] = detail_value
            if retry_after is not None:
                body["retry_after"] = int(retry_after)
                if status == 429:
                    headers = {"Retry-After": str(int(retry_after))}
            if extra:
                body.update(extra)
            return TwoFASubmitResult(status_code=status, body=body, headers=headers)

        async def _sign_in_with_compat() -> None:
            try:
                await client.sign_in(password=secret)
            except TypeError as exc_type:
                message = str(exc_type) if exc_type else ""
                if "unexpected keyword" in message or "logout_other_sessions" in message:
                    LOGGER.warning(
                        "stage=password_warn event=sign_in_retry tenant_id=%s detail=%s",
                        tenant,
                        message or "unexpected_keyword",
                    )
                    await client.sign_in(secret)
                    return
                raise

        def _is_network_or_mtproto_error(exc: Exception) -> bool:
            if isinstance(exc, RPCError):
                return True
            if isinstance(exc, (asyncio.TimeoutError, ConnectionError, OSError)):
                return True
            return False

        max_attempts = 2
        signed_in = False
        for attempt in range(max_attempts):
            try:
                await client(functions.account.GetPasswordRequest())
            except BadRequestError as exc_bad_request:
                detail = str(exc_bad_request) or "get_password_bad_request"
                return await _failure_response(
                    status=400,
                    error="password_exception",
                    event="password_exception",
                    detail=detail,
                    exc=exc_bad_request,
                    last_error="password_exception",
                )
            except Exception as exc:
                status = 502 if _is_network_or_mtproto_error(exc) else 400
                detail = str(exc) or "get_password_failed"
                return await _failure_response(
                    status=status,
                    error="password_exception",
                    event="password_exception",
                    detail=detail,
                    exc=exc,
                    last_error="password_exception",
                )

            try:
                await _sign_in_with_compat()
                signed_in = True
                break
            except PasswordHashInvalidError:
                return await _failure_response(
                    status=400,
                    error="password_invalid",
                    event="password_invalid",
                    detail="password_invalid",
                    last_error="password_exception",
                )
            except PhonePasswordFloodError as exc_flood:
                wait_seconds = getattr(exc_flood, "seconds", None)
                if wait_seconds is None or int(wait_seconds) <= 0:
                    wait_seconds = int(PASSWORD_FLOOD_BACKOFF)
                wait_seconds = max(1, int(wait_seconds))
                return await _failure_response(
                    status=429,
                    error="flood_wait",
                    event="phone_password_flood",
                    retry_after=wait_seconds,
                    backoff=float(wait_seconds),
                    detail=f"phone_password_flood {wait_seconds}",
                    extra={"ttl": int(wait_seconds)},
                )
            except FloodWaitError as exc_wait:
                wait_seconds = max(
                    1, int(getattr(exc_wait, "seconds", PASSWORD_FLOOD_BACKOFF))
                )
                return await _failure_response(
                    status=429,
                    error="flood_wait",
                    event="flood_wait",
                    retry_after=wait_seconds,
                    backoff=float(wait_seconds),
                    detail=f"flood_wait {wait_seconds}",
                    extra={"ttl": int(wait_seconds)},
                )
            except BadRequestError as exc_bad:
                message = (
                    getattr(exc_bad, "message", None)
                    or getattr(exc_bad, "rpc_error", None)
                    or str(exc_bad)
                    or ""
                )
                normalized = message.upper()
                if "SRP_ID_INVALID" in normalized:
                    if attempt + 1 >= max_attempts:
                        return await _failure_response(
                            status=409,
                            error="srp_invalid",
                            event="srp_invalid",
                            detail="srp_invalid",
                        )
                    LOGGER.warning(
                        "stage=password_failed event=srp_invalid tenant_id=%s attempt=%s detail=srp_invalid",
                        tenant,
                        attempt + 1,
                    )
                    await asyncio.sleep(0)
                    continue
                return await _failure_response(
                    status=400,
                    error="password_exception",
                    event="password_exception",
                    detail=message or "bad_request_error",
                    exc=exc_bad,
                    last_error="password_exception",
                )
            except RPCError as exc_rpc:
                detail = str(exc_rpc) or "telegram_error"
                return await _failure_response(
                    status=502,
                    error="password_exception",
                    event="password_exception",
                    detail=detail,
                    exc=exc_rpc,
                    last_error="password_exception",
                )
            except SessionPasswordNeededError:
                return TwoFASubmitResult(
                    status_code=409,
                    body={"error": "twofa_pending"},
                )
            except TypeError as exc_type:
                detail = str(exc_type) or "type_error"
                return await _failure_response(
                    status=400,
                    error="password_exception",
                    event="password_exception",
                    detail=detail,
                    exc=exc_type,
                    last_error="password_exception",
                )
            except Exception as exc_generic:
                detail = str(exc_generic) or "exception"
                status = 502 if _is_network_or_mtproto_error(exc_generic) else 400
                return await _failure_response(
                    status=status,
                    error="password_exception",
                    event="password_exception",
                    detail=detail,
                    exc=exc_generic,
                    last_error="password_exception",
                )

        if not signed_in:
            return await _failure_response(
                status=400,
                error="password_exception",
                event="password_exception",
                detail="password_loop_exhausted",
                last_error="password_exception",
            )

        async with self._lock:
            state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
            self._set_status(tenant, state, "authorized", reason="password_ok")
            state.qr_id = None
            state.qr_png = None
            state.qr_expires_at = None
            state.needs_2fa = False
            state.awaiting_password = False
            state.needs_2fa_expires_at = None
            state.last_error = None
            state.last_seen = time.time()
            state.waiting_task = None
            state.last_needs_2fa_at = None
            state.twofa_pending = False
            state.twofa_since = None
            state.can_restart = False
            state.twofa_backoff_until = None
            self._register_handlers(tenant, client)
            self._update_metrics()

        with contextlib.suppress(Exception):
            session_obj = getattr(client, "session", None)
            if session_obj is not None:
                session_obj.save()

        self._ensure_session_permissions(self._sessions_dir / f"{tenant}.session")
        LOGGER.info(
            "stage=password_ok event=password_ok tenant_id=%s detail=password_ok",
            tenant,
        )
        return TwoFASubmitResult(status_code=200, body={"ok": True})
    def _register_handlers(self, tenant: int, client: TelegramClient) -> None:
        if getattr(client, "_avio_handlers_registered", False):
            return

        @client.on(events.NewMessage)
        async def _on_message(event):
            try:
                await self._handle_new_message(tenant, client, event)
            except asyncio.CancelledError:
                raise
            except AuthKeyUnregisteredError:
                await self._handle_authkey_unregistered(
                    tenant, client, source="on_message_wrapper"
                )
            except RPCError as exc:
                error = str(exc) or "telegram_error"
                EVENT_ERRORS.labels("event_rpc_error").inc()
                LOGGER.error(
                    "stage=event_handler_error tenant_id=%s source=on_message_wrapper error=%s",
                    tenant,
                    error,
                )
                await self._handle_event_disconnect(
                    tenant,
                    client,
                    error=error,
                    source="on_message_wrapper",
                )
            except Exception as exc:
                EVENT_ERRORS.labels("event_exception").inc()
                LOGGER.exception(
                    "stage=event_handler_error tenant_id=%s source=on_message_wrapper",
                    tenant,
                )
                await self._handle_event_disconnect(
                    tenant,
                    client,
                    error=str(exc) or "event_handler_error",
                    source="on_message_wrapper",
                )

        client._avio_handlers_registered = True  # type: ignore[attr-defined]

    async def _handle_new_message(self, tenant: int, client: TelegramClient, event: events.NewMessage.Event) -> None:
        if getattr(event, "out", False):
            return
        state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
        self._set_status(tenant, state, "authorized", reason="incoming_message")
        state.last_seen = time.time()
        self._update_metrics()

        try:
            message = event.message
            sender = await event.get_sender()
            username = getattr(sender, "username", None) or getattr(event.chat, "username", None)

            sender_id = getattr(message, "sender_id", None)
            if sender_id is None:
                from_peer = getattr(message, "from_id", None)
                sender_id = (
                    getattr(from_peer, "user_id", None)
                    or getattr(from_peer, "channel_id", None)
                    or getattr(from_peer, "chat_id", None)
                )

            peer_id = None
            try:
                if message.peer_id is not None:
                    peer_id = (
                        getattr(message.peer_id, "user_id", None)
                        or getattr(message.peer_id, "channel_id", None)
                        or getattr(message.peer_id, "chat_id", None)
                    )
            except AttributeError:
                peer_id = getattr(message, "chat_id", None)

            me = self._self_ids.get(tenant)
            if me is None:
                with contextlib.suppress(Exception):
                    me_info = await client.get_me()
                    me = getattr(me_info, "id", None)
                    if me is not None:
                        self._self_ids[tenant] = me

            attachments: list[Attachment] = []
            if message.media:
                att_type = message.media.__class__.__name__
                file_obj = getattr(message, "file", None)
                name = getattr(file_obj, "name", None)
                mime = getattr(file_obj, "mime_type", None)
                size = getattr(file_obj, "size", None)
                fallback_id = getattr(message, "id", None)
                url = f"telegram://{tenant}/{fallback_id or 0}"
                try:
                    attachments.append(
                        Attachment(type=att_type, url=url, name=name, mime=mime, size=size)
                    )
                except Exception:
                    LOGGER.debug(
                        "stage=attachment_normalize_fail tenant_id=%s attachment_type=%s",
                        tenant,
                        att_type,
                    )

            ts = getattr(message, "date", None)
            if isinstance(ts, datetime):
                ts_unix = int(ts.timestamp())
            else:
                ts_unix = int(time.time())

            normalized = MessageIn(
                tenant=tenant,
                channel="telegram",
                from_id=sender_id or username or "unknown",
                to=me or peer_id or tenant,
                text=message.message or "",
                attachments=attachments,
                ts=ts_unix,
                provider_raw=message.to_dict(),
            )
            delivered = await self._send_webhook(normalized)
            if delivered:
                self._delivered_incoming += 1
            MESSAGE_IN_COUNTER.labels("telegram").inc()
            LOGGER.info(
                "event=message_in stage=incoming tenant_id=%s from_id=%s to_id=%s",  # noqa: G004
                tenant,
                sender_id or username,
                me or peer_id or tenant,
            )
        except asyncio.CancelledError:
            raise
        except AuthKeyUnregisteredError:
            await self._handle_authkey_unregistered(
                tenant, client, source="handle_new_message"
            )
        except RPCError as exc:
            error = str(exc) or "telegram_error"
            EVENT_ERRORS.labels("event_rpc_error").inc()
            LOGGER.error(
                "stage=event_handler_error tenant_id=%s source=handle_new_message error=%s",
                tenant,
                error,
            )
            await self._handle_event_disconnect(
                tenant,
                client,
                error=error,
                source="handle_new_message",
            )
        except Exception as exc:
            EVENT_ERRORS.labels("event_exception").inc()
            LOGGER.exception(
                "stage=event_handler_error tenant_id=%s source=handle_new_message",
                tenant,
            )
            await self._handle_event_disconnect(
                tenant,
                client,
                error=str(exc) or "event_handler_error",
                source="handle_new_message",
            )

    async def _send_webhook(self, message: MessageIn) -> bool:
        headers = {"Content-Type": "application/json"}
        if self._webhook_token:
            headers["X-Webhook-Token"] = self._webhook_token
        try:
            response = await self._http.post(
                self._webhook_url,
                json=message_in_asdict(message),
                headers=headers,
            )
            if 200 <= response.status_code < 300:
                return True
            reason = f"status_{response.status_code}"
            SEND_FAIL_COUNTER.labels("telegram", reason).inc()
            LOGGER.warning(
                "event=message_in_forward_fail stage=webhook_status tenant_id=%s status=%s",
                message.tenant,
                response.status_code,
            )
        except httpx.HTTPError as exc:
            EVENT_ERRORS.labels("webhook").inc()
            SEND_FAIL_COUNTER.labels("telegram", "http_error").inc()
            LOGGER.error(
                "event=message_in_forward_fail stage=webhook_error tenant_id=%s error=%s",
                message.tenant,
                exc,
            )
        return False

    async def get_status(self, tenant: int) -> SessionState:
        await self.wait_until_ready()
        client_to_disconnect: Optional[TelegramClient] = None
        async with self._lock:
            state = self._states.get(tenant)
            if not state:
                state = SessionState(tenant_id=tenant, status="disconnected")
                self._states[tenant] = state
            else:
                client_to_disconnect, _ = self._expire_needs_2fa_locked(tenant, state)
                if state.status == "needs_2fa" or state.needs_2fa:
                    state.needs_2fa = True
                    if state.status != "needs_2fa":
                        self._set_status(tenant, state, "needs_2fa", reason="status_poll")
                    if not state.twofa_pending:
                        state.twofa_pending = True
                        if state.twofa_since is None:
                            state.twofa_since = int(time.time() * 1000.0)
                    state.awaiting_password = True
                    state.last_seen = time.time()
                    self._extend_needs_2fa_ttl(state)
                    if state.qr_id:
                        self._qr_lookup.pop(state.qr_id, None)
                        self._clear_qr_state_locked(state)
            client = self._clients.get(tenant)
            is_active = bool(client and client.is_connected())
            if state.twofa_pending:
                state.can_restart = False
            elif state.status in {"waiting_qr", "needs_2fa"} and not is_active:
                state.can_restart = True
            else:
                state.can_restart = False
            now = time.time()
            if state.status == "waiting_qr" and state.qr_id:
                if state.qr_expires_at and state.qr_expires_at <= now:
                    self._expire_qr_locked(tenant, state, reason="expired")
                    if not state.restart_pending:
                        state.restart_pending = True
                    state.can_restart = True
                    LOGGER.info(
                        "stage=qr_expired tenant_id=%s reason=status_poll", tenant
                    )
                elif (
                    state.qr_expires_at
                    and state.qr_expires_at - now <= 15.0
                    and not state.restart_pending
                ):
                    state.restart_pending = True
                    state.can_restart = True
                    LOGGER.info(
                        "stage=qr_expiring tenant_id=%s seconds_left=%.2f",
                        tenant,
                        max(state.qr_expires_at - now, 0.0),
                    )
            result = state

        if client_to_disconnect:
            with contextlib.suppress(Exception):
                await client_to_disconnect.disconnect()

        return result

    async def _soft_disconnect(
        self,
        tenant: int,
        *,
        client: Optional[TelegramClient] = None,
        error: Optional[str] = None,
        allow_restart: bool = True,
        remove_session: bool = False,
    ) -> bool:
        task_to_cancel: Optional[asyncio.Task[Any]] = None
        client_to_disconnect = client
        extra_client: Optional[TelegramClient] = None
        session_path: Optional[Path] = None
        async with self._lock:
            state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
            if state.waiting_task and not state.waiting_task.done():
                task_to_cancel = state.waiting_task
            state.waiting_task = None
            if state.qr_id:
                self._qr_lookup.pop(state.qr_id, None)
            self._clear_qr_state_locked(state)
            base_reason = "manual_logout" if error is None else (error or "unknown")
            metric_reason = base_reason.strip().lower().replace(" ", "_") or "unknown"
            if len(metric_reason) > 64:
                metric_reason = metric_reason[:64]
            self._set_status(tenant, state, "disconnected", reason=metric_reason)
            state.last_error = error
            state.needs_2fa = False
            state.awaiting_password = False
            state.needs_2fa_expires_at = None
            state.restart_pending = False
            state.last_needs_2fa_at = None
            state.last_seen = time.time()
            state.twofa_pending = False
            state.twofa_since = None
            state.qr_login = None
            state.can_restart = allow_restart
            stored_client = self._clients.pop(tenant, None)
            if client_to_disconnect is None:
                client_to_disconnect = stored_client
            elif stored_client and stored_client is not client_to_disconnect:
                extra_client = stored_client
            self._states[tenant] = state
            self._update_metrics()
            if remove_session:
                session_path = self._sessions_dir / f"{tenant}.session"

        if task_to_cancel:
            task_to_cancel.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task_to_cancel

        for instance in (client_to_disconnect, extra_client):
            if instance is None:
                continue
            with contextlib.suppress(Exception):
                await instance.disconnect()

        removed_file = False
        if session_path is not None:
            with contextlib.suppress(FileNotFoundError):
                session_path.unlink()
                removed_file = True

        return removed_file

    async def _handle_event_disconnect(
        self,
        tenant: int,
        client: TelegramClient,
        *,
        error: str,
        source: str,
        remove_session: bool = False,
    ) -> None:
        removed = await self._soft_disconnect(
            tenant,
            client=client,
            error=error,
            allow_restart=True,
            remove_session=remove_session,
        )
        LOGGER.warning(
            "stage=event_disconnect tenant_id=%s source=%s error=%s removed_session_file=%s",
            tenant,
            source,
            error,
            removed,
        )

    async def _handle_authkey_unregistered(
        self, tenant: int, client: TelegramClient, *, source: str
    ) -> None:
        EVENT_ERRORS.labels("authkey_unregistered").inc()
        removed = await self._soft_disconnect(
            tenant,
            client=client,
            error="authkey_unregistered",
            allow_restart=True,
            remove_session=False,
        )
        LOGGER.warning(
            "stage=authkey_unregistered tenant_id=%s source=%s removed_session_file=%s",
            tenant,
            source,
            removed,
        )

    async def logout(self, tenant: int) -> None:
        await self.wait_until_ready()
        removed = await self._soft_disconnect(
            tenant,
            error=None,
            allow_restart=True,
            remove_session=False,
        )
        LOGGER.info(
            "stage=logout tenant_id=%s removed_session_file=%s",
            tenant,
            removed,
        )

    async def send_message(
        self,
        tenant: int,
        text: str | None = None,
        peer_id: int | None = None,
        telegram_user_id: int | None = None,
        username: str | None = None,
        attachments: Optional[list[Dict[str, Any]]] = None,
        reply_to: str | None = None,
    ) -> None:
        await self.wait_until_ready()
        client = await self._ensure_authorized_client(tenant)
        if client is None:
            raise RuntimeError("session_not_authorized")

        entity = None
        if peer_id:
            entity = peer_id
        elif telegram_user_id:
            entity = telegram_user_id
        elif username:
            entity = username
        if entity is None:
            raise ValueError("missing_target")

        reply_to_id: Optional[int | str] = None
        if reply_to:
            try:
                reply_to_id = int(reply_to)
            except (TypeError, ValueError):
                reply_to_id = reply_to

        attachments_list = attachments or []
        sent_text = False
        try:
            if attachments_list:
                async with httpx.AsyncClient(timeout=15.0) as session:
                    for attachment in attachments_list:
                        url = attachment.get("url") if isinstance(attachment, dict) else None
                        if not url:
                            continue
                        try:
                            resp = await session.get(url)
                            resp.raise_for_status()
                            data = resp.content
                        except httpx.HTTPError as exc:
                            EVENT_ERRORS.labels("attachment_fetch").inc()
                            LOGGER.error(
                                "stage=send_fetch_fail tenant_id=%s url=%s error=%s",
                                tenant,
                                url,
                                exc,
                            )
                            continue
                        caption = None
                        if isinstance(attachment, dict):
                            caption = attachment.get("caption") or attachment.get("text")
                        caption_text = caption if isinstance(caption, str) else None
                        if text and not sent_text:
                            caption_text = text
                            sent_text = True
                        await client.send_file(
                            entity,
                            file=io.BytesIO(data),
                            caption=caption_text or "",
                            reply_to=reply_to_id,
                        )
            if text and not sent_text:
                await client.send_message(entity, text or "", reply_to=reply_to_id)
            elif not attachments_list:
                await client.send_message(entity, text or "", reply_to=reply_to_id)
            MESSAGE_OUT_COUNTER.labels("telegram").inc()
            LOGGER.info(
                "event=message_out stage=send_ok tenant_id=%s peer_id=%s",
                tenant,
                peer_id or username,
            )
        except RPCError as exc:
            EVENT_ERRORS.labels("rpc_error").inc()
            SEND_FAIL_COUNTER.labels("telegram", "rpc_error").inc()
            LOGGER.error(
                "event=message_out stage=send_fail tenant_id=%s peer_id=%s error=%s",
                tenant,
                peer_id or username,
                exc,
            )
            raise
        except Exception as exc:
            EVENT_ERRORS.labels("exception").inc()
            SEND_FAIL_COUNTER.labels("telegram", "exception").inc()
            LOGGER.exception(
                "event=message_out stage=send_fail tenant_id=%s peer_id=%s",
                tenant,
                peer_id or username,
            )
            raise

    async def resolve_self_peer(self, tenant: int) -> int | None:
        await self.wait_until_ready()
        client = await self._ensure_authorized_client(tenant)
        if client is None:
            return None
        me = self._self_ids.get(tenant)
        if me is None:
            with contextlib.suppress(Exception):
                info = await client.get_me()
                me = getattr(info, "id", None)
                if me is not None:
                    self._self_ids[tenant] = me
        return int(me) if me is not None else None

    def delivered_incoming_total(self) -> int:
        return self._delivered_incoming

    async def _ensure_authorized_client(self, tenant: int) -> Optional[TelegramClient]:
        clients_to_disconnect: list[TelegramClient] = []
        task_to_cancel: Optional[asyncio.Task[Any]] = None
        async with self._lock:
            state = self._states.get(tenant)
            if state:
                client_to_disconnect, _ = self._expire_needs_2fa_locked(tenant, state)
                if client_to_disconnect:
                    clients_to_disconnect.append(client_to_disconnect)
            client = self._clients.get(tenant)
            if client is None:
                client = self._build_client(tenant)
                await client.connect()
                self._clients[tenant] = client
            elif not client.is_connected():
                await client.connect()

            try:
                authorized = await client.is_user_authorized()
            except AuthKeyUnregisteredError:
                EVENT_ERRORS.labels("authkey_unregistered").inc()
                state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
                task = self._mark_authkey_unregistered_locked(tenant, state)
                if task:
                    task_to_cancel = task
                self._clients.pop(tenant, None)
                clients_to_disconnect.append(client)
                LOGGER.warning(
                    "stage=authkey_unregistered tenant_id=%s source=ensure_authorized_client",
                    tenant,
                )
                result = None
            else:
                if authorized:
                    state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
                    self._set_status(tenant, state, "authorized", reason="ensure_authorized")
                    state.last_seen = time.time()
                    self._register_handlers(tenant, client)
                    self._update_metrics()
                    result = client
                else:
                    result = None

        if task_to_cancel:
            task_to_cancel.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task_to_cancel

        for instance in clients_to_disconnect:
            with contextlib.suppress(Exception):
                await instance.disconnect()

        return result

    def _resolve_qr_state(self, qr_id: str) -> tuple[int, SessionState]:
        tenant = self._qr_lookup.get(qr_id)
        if tenant is None:
            expired_ts = self._expired_qr.get(qr_id)
            if expired_ts is not None:
                raise QRExpiredError(expired_ts)
            raise QRNotFoundError(qr_id)
        state = self._states.get(tenant)
        if not state or state.qr_id != qr_id:
            expired_ts = self._expired_qr.get(qr_id)
            if expired_ts is not None:
                raise QRExpiredError(expired_ts)
            raise QRNotFoundError(qr_id)
        return tenant, state

    def get_qr_png(self, qr_id: str, tenant: int | None = None) -> bytes:
        cached = self._qr_cache.get(qr_id)
        if cached is not None:
            if tenant is not None and cached.tenant_id != tenant:
                raise QRNotFoundError(qr_id)
            expires_at = cached.expires_at
            if expires_at and expires_at <= time.time():
                self._qr_cache.pop(qr_id, None)
                state = self._states.get(cached.tenant_id)
                if state is not None and state.qr_id == qr_id:
                    self._expire_qr_locked(cached.tenant_id, state, reason="timeout")
                else:
                    self._record_qr_expired(
                        cached.tenant_id, qr_id, expires_at, reason="expired"
                    )
                raise QRExpiredError(expires_at)
            return cached.png

        tenant_id, state = self._resolve_qr_state(qr_id)
        if tenant is not None and tenant_id != tenant:
            raise QRNotFoundError(qr_id)
        if state.qr_expires_at and state.qr_expires_at <= time.time():
            valid_until = state.qr_expires_at
            self._expire_qr_locked(tenant_id, state, reason="timeout")
            if not state.restart_pending:
                state.restart_pending = True
            state.can_restart = True
            LOGGER.info("stage=qr_expired tenant_id=%s reason=qr_fetch", tenant_id)
            raise QRExpiredError(valid_until)
        if not state.qr_png:
            raise QRNotFoundError(qr_id)
        self._qr_cache[qr_id] = CachedQR(
            tenant_id=tenant_id,
            png=state.qr_png,
            expires_at=state.qr_expires_at,
        )
        return state.qr_png

    def get_qr_url(self, qr_id: str) -> str:
        tenant, state = self._resolve_qr_state(qr_id)
        if state.qr_expires_at and state.qr_expires_at <= time.time():
            valid_until = state.qr_expires_at
            self._expire_qr_locked(tenant, state, reason="timeout")
            if not state.restart_pending:
                state.restart_pending = True
            state.can_restart = True
            LOGGER.info("stage=qr_expired tenant_id=%s reason=qr_url", tenant)
            raise QRExpiredError(valid_until)
        if not state.qr_url:
            raise QRNotFoundError(qr_id)
        return state.qr_url

    def _build_qr_png(self, url: str) -> bytes:
        qr = qrcode.QRCode(
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=14,
            border=4,
        )
        qr.add_data(url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="#000000", back_color="#FFFFFF").convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    def stats_snapshot(self) -> Dict[str, Any]:
        authorized = sum(1 for state in self._states.values() if state.status == "authorized")
        waiting = sum(1 for state in self._states.values() if state.status == "waiting_qr")
        needs_2fa = sum(1 for state in self._states.values() if state.status == "needs_2fa")
        return {
            "authorized": authorized,
            "waiting": waiting,
            "needs_2fa": needs_2fa,
        }

    def _update_metrics(self) -> None:
        snapshot = self.stats_snapshot()
        SESSIONS_AUTHORIZED.set(snapshot["authorized"])
        SESSIONS_WAITING.set(snapshot["waiting"])
        SESSIONS_NEEDS_2FA.set(snapshot["needs_2fa"])

