from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
import io
import logging
from pathlib import Path
import secrets
import time
from typing import Any, Dict, Optional

import httpx
import qrcode
from prometheus_client import Counter, Gauge
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, RPCError
from telethon.errors.rpcerrorlist import PasswordHashInvalidError


LOGGER = logging.getLogger("tgworker")


QR_LOGIN_TIMEOUT = 120.0


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


@dataclass(slots=True)
class SessionState:
    tenant_id: int
    status: str = "disconnected"
    qr_id: Optional[str] = None
    qr_png: Optional[bytes] = None
    qr_expires_at: Optional[float] = None
    waiting_task: Optional[asyncio.Task[Any]] = None
    last_error: Optional[str] = None
    needs_2fa: bool = False
    last_seen: Optional[float] = None


class TelegramSessionManager:
    """Manage tenant-scoped TelegramClient instances and QR flows."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        sessions_dir: Path,
        webhook_url: str,
        webhook_token: str | None = None,
        http_timeout: float = 10.0,
    ) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._sessions_dir = sessions_dir
        self._sessions_dir.mkdir(parents=True, exist_ok=True)
        self._webhook_url = webhook_url.rstrip("/")
        self._webhook_token = (webhook_token or "").strip() or None
        self._http = httpx.AsyncClient(timeout=http_timeout)
        self._clients: Dict[int, TelegramClient] = {}
        self._states: Dict[int, SessionState] = {}
        self._qr_lookup: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._loop = asyncio.get_event_loop()
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._bootstrap_existing_sessions()

    async def shutdown(self) -> None:
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
        return TelegramClient(str(session_path), self._api_id, self._api_hash)

    def _tenant_from_path(self, path: Path) -> Optional[int]:
        try:
            return int(path.stem)
        except ValueError:
            return None

    async def start_session(self, tenant: int) -> SessionState:
        async with self._lock:
            state = self._states.get(tenant)
            if state and state.status == "authorized":
                return state

            client = self._clients.get(tenant)
            if client is None:
                client = self._build_client(tenant)
                await client.connect()
                self._clients[tenant] = client
            elif not client.is_connected():
                await client.connect()

            if await client.is_user_authorized():
                state = SessionState(tenant_id=tenant, status="authorized", last_seen=time.time())
                self._states[tenant] = state
                self._register_handlers(tenant, client)
                self._update_metrics()
                return state

            if state and state.waiting_task and not state.waiting_task.done():
                # Reuse existing QR if still valid.
                return state

            qr_login = await client.qr_login()
            png = self._build_qr_png(qr_login.url)
            qr_id = secrets.token_urlsafe(16)

            state = SessionState(
                tenant_id=tenant,
                status="waiting_qr",
                qr_id=qr_id,
                qr_png=png,
                qr_expires_at=time.time() + 180,
            )
            self._states[tenant] = state
            self._qr_lookup[qr_id] = tenant
            LOGGER.info("stage=qr_start tenant_id=%s", tenant)

            state.waiting_task = self._loop.create_task(
                self._wait_for_authorization(tenant, client, state, qr_login)
            )
            self._update_metrics()
            return state

    async def _wait_for_authorization(self, tenant: int, client: TelegramClient, state: SessionState, qr_login) -> None:
        qr_id = state.qr_id
        try:
            result = await qr_login.wait(timeout=QR_LOGIN_TIMEOUT)
            LOGGER.info("stage=qr_ready tenant_id=%s", tenant)
            if result is None:
                await asyncio.sleep(0.1)
            state.status = "authorized"
            state.qr_id = None
            state.qr_png = None
            state.qr_expires_at = None
            state.needs_2fa = False
            state.last_seen = time.time()
            self._register_handlers(tenant, client)
            LOGGER.info("stage=authorized tenant_id=%s", tenant)
        except SessionPasswordNeededError:
            state.status = "needs_2fa"
            state.needs_2fa = True
            state.last_error = "Two-factor authentication required"
            EVENT_ERRORS.labels("needs_2fa").inc()
            LOGGER.warning("stage=needs_2fa event=needs_2fa tenant_id=%s", tenant)
        except asyncio.TimeoutError:
            state.status = "disconnected"
            state.last_error = "qr_login_timeout"
            EVENT_ERRORS.labels("timeout").inc()
            LOGGER.warning("stage=qr_timeout event=qr_timeout tenant_id=%s", tenant)
        except asyncio.CancelledError:
            LOGGER.info("stage=qr_cancel tenant_id=%s", tenant)
            raise
        except RPCError as exc:
            state.status = "disconnected"
            state.last_error = str(exc)
            EVENT_ERRORS.labels("rpc_error").inc()
            LOGGER.error("stage=send_fail tenant_id=%s error=%s", tenant, exc)
        except Exception as exc:
            state.status = "disconnected"
            state.last_error = str(exc)
            EVENT_ERRORS.labels("exception").inc()
            LOGGER.exception("stage=qr_fail tenant_id=%s", tenant)
        finally:
            if qr_id:
                self._qr_lookup.pop(qr_id, None)
            state.waiting_task = None
            self._update_metrics()

    async def submit_password(self, tenant: int, password: str) -> SessionState:
        secret = password or ""
        if not secret:
            raise ValueError("password_required")

        async with self._lock:
            state = self._states.get(tenant)
            if not state:
                raise ValueError("session_not_found")
            if state.status != "needs_2fa":
                raise ValueError("password_not_required")
            client = self._clients.get(tenant)
            if client is None:
                client = self._build_client(tenant)
                await client.connect()
                self._clients[tenant] = client
            elif not client.is_connected():
                await client.connect()

        try:
            await client.sign_in(password=secret)
            authorized = await client.is_user_authorized()
        except PasswordHashInvalidError as exc:
            async with self._lock:
                state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
                state.status = "needs_2fa"
                state.needs_2fa = True
                state.last_error = "invalid_password"
                self._update_metrics()
            EVENT_ERRORS.labels("password_failed").inc()
            LOGGER.warning("stage=password_failed event=password_failed tenant_id=%s error=invalid_password", tenant)
            raise ValueError("invalid_password") from exc
        except RPCError as exc:
            message = str(exc) or "telegram_error"
            async with self._lock:
                state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
                state.status = "needs_2fa"
                state.needs_2fa = True
                state.last_error = message
                self._update_metrics()
            EVENT_ERRORS.labels("password_failed").inc()
            LOGGER.error("stage=password_failed event=password_failed tenant_id=%s error=%s", tenant, message)
            raise RuntimeError("telegram_error") from exc
        except Exception as exc:
            async with self._lock:
                state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
                state.status = "needs_2fa"
                state.needs_2fa = True
                state.last_error = str(exc)
                self._update_metrics()
            EVENT_ERRORS.labels("password_failed").inc()
            LOGGER.exception("stage=password_failed event=password_failed tenant_id=%s", tenant)
            raise RuntimeError("telegram_error") from exc

        if not authorized:
            async with self._lock:
                state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
                state.status = "needs_2fa"
                state.needs_2fa = True
                state.last_error = "authorization_pending"
                self._update_metrics()
            EVENT_ERRORS.labels("password_failed").inc()
            LOGGER.warning("stage=password_failed event=password_failed tenant_id=%s error=authorization_pending", tenant)
            raise RuntimeError("authorization_pending")

        async with self._lock:
            state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
            state.status = "authorized"
            state.qr_id = None
            state.qr_png = None
            state.qr_expires_at = None
            state.needs_2fa = False
            state.last_error = None
            state.last_seen = time.time()
            state.waiting_task = None
            self._register_handlers(tenant, client)
            self._update_metrics()
        LOGGER.info("stage=password_ok event=password_ok tenant_id=%s", tenant)
        return state

    def _register_handlers(self, tenant: int, client: TelegramClient) -> None:
        if getattr(client, "_avio_handlers_registered", False):
            return

        @client.on(events.NewMessage)
        async def _on_message(event):
            await self._handle_new_message(tenant, client, event)

        client._avio_handlers_registered = True  # type: ignore[attr-defined]

    async def _handle_new_message(self, tenant: int, client: TelegramClient, event: events.NewMessage.Event) -> None:
        if getattr(event, "out", False):
            return
        state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
        state.status = "authorized"
        state.last_seen = time.time()
        self._update_metrics()

        message = event.message
        sender = await event.get_sender()
        username = getattr(sender, "username", None) or getattr(event.chat, "username", None)
        peer_id = None
        try:
            if message.peer_id is not None:
                peer_id = getattr(message.peer_id, "user_id", None) or getattr(message.peer_id, "channel_id", None)
        except AttributeError:
            peer_id = getattr(message, "sender_id", None)

        media_payload: Any = None
        if message.media:
            media_payload = {
                "class": message.media.__class__.__name__,
            }

        payload = {
            "tenant_id": tenant,
            "user_id": peer_id,
            "username": username,
            "text": message.message or "",
            "media": media_payload,
        }
        await self._send_webhook(payload)
        LOGGER.info("stage=incoming tenant_id=%s peer_id=%s", tenant, peer_id)

    async def _send_webhook(self, payload: Dict[str, Any]) -> None:
        headers = {"Content-Type": "application/json"}
        if self._webhook_token:
            headers["X-Webhook-Token"] = self._webhook_token
        try:
            await self._http.post(self._webhook_url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            EVENT_ERRORS.labels("webhook").inc()
            LOGGER.error("stage=send_fail tenant_id=%s error=%s", payload.get("tenant_id"), exc)

    async def get_status(self, tenant: int) -> SessionState:
        async with self._lock:
            state = self._states.get(tenant)
            if not state:
                state = SessionState(tenant_id=tenant, status="disconnected")
                self._states[tenant] = state
            return state

    async def logout(self, tenant: int) -> None:
        async with self._lock:
            state = self._states.get(tenant)
            if state and state.waiting_task and not state.waiting_task.done():
                state.waiting_task.cancel()
            client = self._clients.pop(tenant, None)
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.log_out()
                with contextlib.suppress(Exception):
                    await client.disconnect()
            state = SessionState(tenant_id=tenant, status="disconnected")
            self._states[tenant] = state
            path = self._sessions_dir / f"{tenant}.session"
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
            LOGGER.info("stage=logout tenant_id=%s", tenant)
            self._update_metrics()

    async def send_message(
        self,
        tenant: int,
        text: str | None = None,
        peer_id: int | None = None,
        username: str | None = None,
        media_url: str | None = None,
    ) -> None:
        client = await self._ensure_authorized_client(tenant)
        if client is None:
            raise RuntimeError("session_not_authorized")

        entity = None
        if peer_id:
            entity = peer_id
        elif username:
            entity = username
        if entity is None:
            raise ValueError("missing_target")

        try:
            if media_url:
                async with httpx.AsyncClient(timeout=15.0) as session:
                    resp = await session.get(media_url)
                    resp.raise_for_status()
                    data = resp.content
                await client.send_file(entity, file=io.BytesIO(data), caption=text or "")
            else:
                await client.send_message(entity, text or "")
            LOGGER.info("stage=send_ok tenant_id=%s peer_id=%s", tenant, peer_id or username)
        except RPCError as exc:
            EVENT_ERRORS.labels("rpc_error").inc()
            LOGGER.error("stage=send_fail tenant_id=%s peer_id=%s error=%s", tenant, peer_id or username, exc)
            raise
        except Exception as exc:
            EVENT_ERRORS.labels("exception").inc()
            LOGGER.exception("stage=send_fail tenant_id=%s peer_id=%s", tenant, peer_id or username)
            raise

    async def _ensure_authorized_client(self, tenant: int) -> Optional[TelegramClient]:
        async with self._lock:
            client = self._clients.get(tenant)
            if client is None:
                client = self._build_client(tenant)
                await client.connect()
                self._clients[tenant] = client
            elif not client.is_connected():
                await client.connect()

            if await client.is_user_authorized():
                state = self._states.setdefault(tenant, SessionState(tenant_id=tenant))
                state.status = "authorized"
                state.last_seen = time.time()
                self._register_handlers(tenant, client)
                self._update_metrics()
                return client

            return None

    def get_qr_png(self, qr_id: str) -> Optional[bytes]:
        tenant = self._qr_lookup.get(qr_id)
        if tenant is None:
            return None
        state = self._states.get(tenant)
        if not state or state.qr_id != qr_id:
            return None
        if state.qr_expires_at and state.qr_expires_at < time.time():
            return None
        return state.qr_png

    def _build_qr_png(self, url: str) -> bytes:
        qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=6, border=2)
        qr.add_data(url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
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

