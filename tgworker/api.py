from __future__ import annotations

import asyncio
import base64
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, SecretStr
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

from config import telegram_config
from app.schemas import TransportMessage

from .session_manager import (
    QRExpiredError,
    QRNotFoundError,
    SessionManager,
    TwoFASubmitResult,
    LoginFlowStateSnapshot,
    SessionSnapshot,
)


logger = logging.getLogger("tgworker.api")


TG_QR_START_TOTAL = Counter(
    "tg_qr_start_total", "Total number of QR login sessions initiated"
)
TG_QR_EXPIRED_TOTAL = Counter(
    "tg_qr_expired_total",
    "Total number of Telegram QR codes that expired before authorization",
)
TG_2FA_REQUIRED_TOTAL = Counter(
    "tg_2fa_required_total", "Total number of login flows that required 2FA"
)
TG_LOGIN_SUCCESS_TOTAL = Counter(
    "tg_login_success_total", "Total number of successful Telegram authorizations"
)
TG_LOGIN_FAIL_TOTAL = Counter(
    "tg_login_fail_total",
    "Total number of failed Telegram authorizations",
    ["reason"],
)


@dataclass(slots=True)
class PendingEntry:
    tenant: int
    qr_login_obj: Any | None = None
    png_bytes: bytes | None = None
    expires_at: float | None = None
    expires_at_ms: int | None = None
    state: str = "idle"
    last_error: str | None = None
    qr_id: str | None = None
    authorized: bool = False
    _expired_recorded: bool = False

    def is_expired(self) -> bool:
        return self.expires_at is not None and self.expires_at <= time.time()


class QRStartRequest(BaseModel):
    tenant: int = Field(..., ge=1)
    force: bool = False


class StartRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)
    force: bool = False


class RestartRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)


class LogoutRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)


class PasswordRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)
    password: SecretStr


class TwoFARequest(BaseModel):
    tenant: int = Field(..., ge=1)
    password: SecretStr


def _resolve_webhook_url() -> tuple[str, Optional[str]]:
    explicit = os.getenv("APP_WEBHOOK")
    if explicit:
        url = explicit.rstrip("/")
    else:
        base = (
            os.getenv("TG_WEBHOOK_URL")
            or os.getenv("APP_BASE_URL")
            or os.getenv("APP_INTERNAL_URL")
            or "http://app:8000"
        )
        url = f"{base.rstrip('/')}/webhook/provider"
    token = os.getenv("WEBHOOK_SECRET") or None
    if token:
        from urllib.parse import quote_plus

        url = f"{url}?token={quote_plus(token)}"
    return url, None


def create_app() -> FastAPI:
    cfg = telegram_config()
    webhook_url, webhook_token = _resolve_webhook_url()
    manager = SessionManager(
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        sessions_dir=cfg.sessions_dir,
        webhook_url=webhook_url,
        device_model=cfg.device_model,
        system_version=cfg.system_version,
        app_version=cfg.app_version,
        lang_code=cfg.lang_code,
        system_lang_code=cfg.system_lang_code,
        webhook_token=webhook_token,
        qr_ttl=cfg.qr_ttl,
        qr_poll_interval=cfg.qr_poll_interval,
    )

    app = FastAPI(title="tgworker")
    app.state.session_manager = manager
    pending_registry: dict[int, PendingEntry] = {}
    tenant_locks: dict[int, asyncio.Lock] = {}
    app.state.pending_registry = pending_registry
    app.state.pending_locks = tenant_locks

    NO_STORE_HEADERS = {
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    }

    def _tenant_lock(tenant: int) -> asyncio.Lock:
        lock = tenant_locks.get(tenant)
        if lock is None:
            lock = asyncio.Lock()
            tenant_locks[tenant] = lock
        return lock

    def _ms_to_seconds(value: Optional[int]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value) / 1000.0
        except Exception:
            return None

    def _derive_state(snapshot: SessionSnapshot, flow: LoginFlowStateSnapshot) -> str:
        statuses = {snapshot.status or "disconnected", flow.status or "disconnected"}
        if "authorized" in statuses:
            return "authorized"
        if (
            snapshot.needs_2fa
            or snapshot.twofa_pending
            or flow.needs_2fa
            or flow.twofa_pending
            or "needs_2fa" in statuses
        ):
            return "need_2fa"
        if "waiting_qr" in statuses and (snapshot.qr_id or flow.qr_id):
            return "waiting_qr"
        if snapshot.last_error or flow.last_error:
            return "failed"
        return "idle"

    def _apply_state(entry: PendingEntry, new_state: str, *, reason: Optional[str]) -> None:
        previous = entry.state
        if previous != new_state:
            if reason:
                logger.info(
                    "event=login_flow tenant=%s state=%s reason=%s",
                    entry.tenant,
                    new_state,
                    reason,
                )
            else:
                logger.info(
                    "event=login_flow tenant=%s state=%s",
                    entry.tenant,
                    new_state,
                )
            if new_state == "waiting_qr":
                TG_QR_START_TOTAL.inc()
            elif new_state == "need_2fa":
                TG_2FA_REQUIRED_TOTAL.inc()
            elif new_state == "authorized":
                TG_LOGIN_SUCCESS_TOTAL.inc()
            elif new_state == "failed":
                TG_LOGIN_FAIL_TOTAL.labels(reason or entry.last_error or "unknown").inc()
        entry.state = new_state
        entry.authorized = new_state == "authorized"

    def _state_name(entry: PendingEntry) -> str:
        if entry.authorized:
            return "authorized"
        if entry.state == "need_2fa":
            return "need_2fa"
        if entry.state == "waiting_qr":
            return "need_qr"
        if entry.state == "waiting":
            return "waiting"
        return "need_qr"

    def _entry_payload(
        entry: PendingEntry,
        *,
        stats: Optional[dict[str, Any]] = None,
        include_png: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": entry.state != "failed",
            "tenant": entry.tenant,
            "authorized": entry.authorized,
            "state": _state_name(entry),
            "raw_state": entry.state,
            "needs_2fa": entry.state == "need_2fa",
            "qr_id": entry.qr_id,
            "expires_at": entry.expires_at_ms,
            "last_error": entry.last_error,
        }
        if stats:
            payload.update(
                {
                    "authorized_count": int(stats.get("authorized", 0) or 0),
                    "waiting_count": int(stats.get("waiting", 0) or 0),
                    "needs_2fa_count": int(stats.get("needs_2fa", 0) or 0),
                }
            )
        if include_png and entry.png_bytes:
            payload["qr_png"] = base64.b64encode(entry.png_bytes).decode("ascii")
        return payload

    async def _refresh_pending(
        tenant: int,
        *,
        snapshot: Optional[SessionSnapshot] = None,
        flow: Optional[LoginFlowStateSnapshot] = None,
    ) -> PendingEntry:
        entry = pending_registry.get(tenant)
        if entry is None:
            entry = PendingEntry(tenant=tenant)
            pending_registry[tenant] = entry

        if snapshot is None:
            snapshot = await manager.get_status(tenant)
        if flow is None:
            flow = await manager.login_flow_state(tenant)

        new_qr_id = snapshot.qr_id or flow.qr_id
        if new_qr_id != entry.qr_id:
            entry._expired_recorded = False
            entry.png_bytes = None
        entry.qr_id = new_qr_id
        entry.qr_login_obj = flow.qr_login_obj

        expires_ms = snapshot.qr_valid_until
        if expires_ms is not None:
            try:
                entry.expires_at_ms = int(float(expires_ms))
            except Exception:
                entry.expires_at_ms = None
        elif flow.qr_expires_at is not None:
            try:
                value = float(flow.qr_expires_at)
                if value < 10_000_000_000:
                    value *= 1000.0
                entry.expires_at_ms = int(value)
            except Exception:
                entry.expires_at_ms = None
        else:
            entry.expires_at_ms = None

        if entry.expires_at_ms is not None:
            entry.expires_at = _ms_to_seconds(entry.expires_at_ms)
        else:
            entry.expires_at = None

        entry.last_error = snapshot.last_error or flow.last_error

        derived_state = _derive_state(snapshot, flow)

        if derived_state == "waiting_qr":
            entry.last_error = None
            if entry.qr_id:
                png_blob = flow.qr_png or entry.png_bytes
                if png_blob is None:
                    try:
                        png_blob = manager.get_qr_png(entry.qr_id, tenant=tenant)
                    except QRExpiredError:
                        if not entry._expired_recorded:
                            TG_QR_EXPIRED_TOTAL.inc()
                            entry._expired_recorded = True
                        entry.last_error = "qr_expired"
                        entry.png_bytes = None
                        _apply_state(entry, "failed", reason="qr_expired")
                        pending_registry[tenant] = entry
                        return entry
                    except QRNotFoundError:
                        png_blob = None
                entry.png_bytes = png_blob
        else:
            if derived_state == "authorized":
                entry.last_error = None
            entry.png_bytes = None

        _apply_state(entry, derived_state, reason=entry.last_error if derived_state == "failed" else None)

        if entry.state == "waiting_qr" and entry.is_expired():
            entry.last_error = "qr_expired"
            if not entry._expired_recorded:
                TG_QR_EXPIRED_TOTAL.inc()
                entry._expired_recorded = True
            entry.png_bytes = None
            entry.qr_login_obj = None
            _apply_state(entry, "failed", reason="qr_expired")

        pending_registry[tenant] = entry
        return entry

    @app.on_event("startup")
    async def _startup() -> None:  # pragma: no cover - wiring
        await manager.start()
        if cfg.api_id <= 0 or not cfg.api_hash:
            logger.warning("telegram api credentials are not configured")

    @app.on_event("shutdown")
    async def _shutdown() -> None:  # pragma: no cover - wiring
        await manager.shutdown()

    def require_credentials() -> None:
        if cfg.api_id <= 0 or not cfg.api_hash:
            raise HTTPException(status_code=503, detail="telegram_credentials_missing")

    def _safe_stats_snapshot() -> dict[str, int]:
        try:
            snapshot = manager.stats_snapshot()
            if isinstance(snapshot, dict):
                return snapshot
        except Exception:
            logger.warning("event=stats_snapshot_failed", exc_info=True)
        return {"authorized": 0, "waiting": 0, "needs_2fa": 0}

    @app.post("/qr/start")
    async def qr_start(payload: QRStartRequest):
        tenant = payload.tenant
        force_login = bool(getattr(payload, "force", False))
        lock = _tenant_lock(tenant)
        async with lock:
            entry = await _refresh_pending(tenant)
            if entry.authorized:
                body = _entry_payload(entry, stats=_safe_stats_snapshot())
                body.update({"error": "already_authorized"})
                return JSONResponse(body, status_code=409, headers=dict(NO_STORE_HEADERS))

            if entry.state == "need_2fa":
                body = _entry_payload(entry, stats=_safe_stats_snapshot())
                return JSONResponse(body, headers=dict(NO_STORE_HEADERS))

            should_start = force_login
            if not should_start:
                if entry.state != "waiting_qr" or entry.qr_id is None:
                    should_start = True
                elif entry.is_expired():
                    should_start = True

            if should_start:
                snapshot = await manager.start_session(tenant, force=force_login)
                entry = await _refresh_pending(tenant, snapshot=snapshot)

            if entry.state == "waiting_qr" and entry.qr_id and entry.png_bytes is None:
                try:
                    entry.png_bytes = manager.get_qr_png(entry.qr_id, tenant=tenant)
                except QRExpiredError:
                    if not entry._expired_recorded:
                        TG_QR_EXPIRED_TOTAL.inc()
                        entry._expired_recorded = True
                    entry.last_error = "qr_expired"
                    entry.png_bytes = None
                    _apply_state(entry, "failed", reason="qr_expired")
                except QRNotFoundError:
                    entry.png_bytes = None

            pending_registry[tenant] = entry

            if entry.state == "failed" and entry.last_error == "qr_expired":
                body = _entry_payload(entry, stats=_safe_stats_snapshot())
                body.update({"error": "qr_expired"})
                return JSONResponse(body, status_code=410, headers=dict(NO_STORE_HEADERS))

            body = _entry_payload(entry, stats=_safe_stats_snapshot(), include_png=False)
            return JSONResponse(body, headers=dict(NO_STORE_HEADERS))

    @app.get("/qr/png")
    async def qr_png(
        tenant: int = Query(..., ge=1), qr_id: str = Query(..., min_length=1)
    ):
        lock = _tenant_lock(tenant)
        async with lock:
            entry = await _refresh_pending(tenant)
            if entry.qr_id and entry.qr_id != qr_id:
                entry.png_bytes = None
            if entry.expires_at and entry.is_expired():
                if not entry._expired_recorded:
                    TG_QR_EXPIRED_TOTAL.inc()
                    entry._expired_recorded = True
                entry.last_error = "qr_expired"
                entry.png_bytes = None
                pending_registry[tenant] = entry
                _apply_state(entry, "failed", reason="qr_expired")
                return JSONResponse(
                    {"error": "qr_expired"},
                    status_code=410,
                    headers=dict(NO_STORE_HEADERS),
                )
            try:
                blob = manager.get_qr_png(qr_id, tenant=tenant)
            except QRExpiredError:
                if not entry._expired_recorded:
                    TG_QR_EXPIRED_TOTAL.inc()
                    entry._expired_recorded = True
                entry.last_error = "qr_expired"
                entry.png_bytes = None
                pending_registry[tenant] = entry
                _apply_state(entry, "failed", reason="qr_expired")
                return JSONResponse(
                    {"error": "qr_expired"},
                    status_code=410,
                    headers=dict(NO_STORE_HEADERS),
                )
            except QRNotFoundError:
                return JSONResponse(
                    {"error": "qr_not_found"},
                    status_code=404,
                    headers=dict(NO_STORE_HEADERS),
                )

            entry.qr_id = qr_id
            entry.png_bytes = blob
            pending_registry[tenant] = entry
            return Response(content=blob, media_type="image/png", headers=dict(NO_STORE_HEADERS))

    @app.post("/2fa")
    async def submit_twofa(payload: TwoFARequest):
        tenant = payload.tenant
        lock = _tenant_lock(tenant)
        async with lock:
            entry = await _refresh_pending(tenant)
            if entry.state != "need_2fa":
                body = {"ok": False, "error": "not_waiting_2fa", "state": entry.state}
                return JSONResponse(body, status_code=409, headers=dict(NO_STORE_HEADERS))

            result = await manager.submit_password(
                tenant, payload.password.get_secret_value()
            )
            headers = dict(NO_STORE_HEADERS)
            if result.headers:
                headers.update(result.headers)
            status_code = int(result.status_code)
            body = dict(result.body or {})
            error = str(body.get("error") or "").strip()

            if status_code == 200 and not error:
                entry = await _refresh_pending(tenant)
                return JSONResponse(_entry_payload(entry), headers=headers)

            if error in {"password_invalid", "bad_password"} or status_code == 401:
                TG_LOGIN_FAIL_TOTAL.labels("bad_password").inc()
                response = {"ok": False, "error": "bad_password"}
                detail = body.get("detail")
                if detail:
                    response["detail"] = detail
                return JSONResponse(response, status_code=401, headers=headers)

            if error == "srp_invalid":
                TG_LOGIN_FAIL_TOTAL.labels("srp_invalid").inc()
                response = {"ok": False, "error": error}
                detail = body.get("detail")
                if detail:
                    response["detail"] = detail
                return JSONResponse(response, status_code=409, headers=headers)

            if error in {"phone_password_flood", "flood_wait"}:
                TG_LOGIN_FAIL_TOTAL.labels(error).inc()
                response = {"ok": False, "error": error}
                if "retry_after" in body:
                    response["retry_after"] = body["retry_after"]
                detail = body.get("detail")
                if detail:
                    response["detail"] = detail
                snapshot = await manager.get_status(tenant)
                if snapshot.twofa_backoff_until is not None:
                    response["backoff_until"] = int(snapshot.twofa_backoff_until)
                return JSONResponse(response, status_code=429, headers=headers)

            if error:
                TG_LOGIN_FAIL_TOTAL.labels(error).inc()
                return JSONResponse(
                    {"ok": False, "error": error},
                    status_code=status_code or 500,
                    headers=headers,
                )

            logger.error(
                "event=login_flow tenant=%s state=need_2fa reason=password_exception",
                tenant,
            )
            return JSONResponse(
                {"ok": False, "error": "password_exception"},
                status_code=status_code or 500,
                headers=headers,
            )

    @app.get("/status")
    async def status(tenant: int = Query(..., ge=1)):
        lock = _tenant_lock(tenant)
        async with lock:
            entry = await _refresh_pending(tenant)
            body = _entry_payload(entry, stats=_safe_stats_snapshot())
            return JSONResponse(body, headers=dict(NO_STORE_HEADERS))

    @app.post("/session/start")
    async def start_session(payload: StartRequest, _: None = Depends(require_credentials)):
        if not payload.force:
            current = await manager.get_status(payload.tenant_id)
            if current.twofa_pending or current.needs_2fa:
                return JSONResponse(current.to_payload(), headers=dict(NO_STORE_HEADERS))

        snapshot = await manager.start_session(payload.tenant_id, force=payload.force)
        return JSONResponse(snapshot.to_payload(), headers=dict(NO_STORE_HEADERS))

    @app.post("/rpc/start")
    async def rpc_start(payload: StartRequest):
        snapshot = await manager.start_session(payload.tenant_id, force=payload.force)
        body = {
            "status": snapshot.status,
            "qr_id": snapshot.qr_id,
            "qr_valid_until": snapshot.qr_valid_until,
        }
        return JSONResponse(body, headers=dict(NO_STORE_HEADERS))

    @app.post("/session/restart")
    async def restart_session(payload: RestartRequest, _: None = Depends(require_credentials)):
        snapshot = await manager.start_session(payload.tenant_id, force=True)
        return JSONResponse(snapshot.to_payload(), headers=dict(NO_STORE_HEADERS))

    @app.get("/session/status")
    async def session_status(tenant: int = Query(..., ge=1)):
        session_snapshot = await manager.get_status(tenant)
        stats = manager.stats_snapshot()
        payload = session_snapshot.to_payload()
        payload["stats"] = stats
        return JSONResponse(payload, headers=dict(NO_STORE_HEADERS))

    @app.get("/rpc/status")
    async def rpc_status(tenant_id: int = Query(..., ge=1)):
        session_snapshot = await manager.get_status(tenant_id)
        stats = manager.stats_snapshot()
        payload = session_snapshot.to_payload()
        payload["stats"] = stats
        return JSONResponse(payload, headers=dict(NO_STORE_HEADERS))

    @app.get("/rpc/qr.png")
    async def rpc_qr_png(
        tenant: int = Query(..., ge=1), qr_id: str = Query(..., min_length=1)
    ):
        try:
            blob = manager.get_qr_png(qr_id, tenant=tenant)
        except QRExpiredError:
            return JSONResponse(
                {"error": "qr_expired"},
                status_code=410,
                headers=dict(NO_STORE_HEADERS),
            )
        except QRNotFoundError:
            return JSONResponse(
                {"error": "qr_not_found"},
                status_code=404,
                headers=dict(NO_STORE_HEADERS),
            )
        headers = dict(NO_STORE_HEADERS)
        return Response(content=blob, media_type="image/png", headers=headers)

    @app.get("/session/qr/{qr_id}.png")
    async def session_qr(qr_id: str):
        try:
            blob = manager.get_qr_png(qr_id)
        except QRExpiredError as exc:
            raise HTTPException(status_code=404, detail="qr_expired") from exc
        except QRNotFoundError as exc:
            raise HTTPException(status_code=404, detail="qr_not_found") from exc
        headers = dict(NO_STORE_HEADERS)
        return Response(content=blob, media_type="image/png", headers=headers)

    @app.get("/session/qr/{qr_id}.txt")
    async def session_qr_txt(qr_id: str):
        try:
            login_url = manager.get_qr_url(qr_id)
        except QRExpiredError as exc:
            raise HTTPException(status_code=404, detail="qr_expired") from exc
        except QRNotFoundError as exc:
            raise HTTPException(status_code=404, detail="qr_not_found") from exc
        headers = dict(NO_STORE_HEADERS)
        return PlainTextResponse(login_url, headers=headers)

    @app.post("/session/logout")
    async def session_logout(payload: LogoutRequest):
        await manager.logout(payload.tenant_id)
        return JSONResponse({"ok": True}, headers=dict(NO_STORE_HEADERS))

    def _password_response(result: TwoFASubmitResult) -> JSONResponse:
        headers = dict(NO_STORE_HEADERS)
        if result.headers:
            headers.update(result.headers)
        status_code = int(result.status_code)
        body = dict(result.body)
        return JSONResponse(body, status_code=status_code, headers=headers)

    @app.post("/session/password")
    async def session_password(payload: PasswordRequest):
        try:
            result = await manager.submit_password(
                payload.tenant_id, payload.password.get_secret_value()
            )
        except Exception:
            logger.exception(
                "stage=password_failed event=password_exception endpoint=session_password"
            )
            return JSONResponse(
                {"error": "password_exception"},
                status_code=500,
                headers=dict(NO_STORE_HEADERS),
            )
        return _password_response(result)

    @app.post("/rpc/twofa.submit")
    async def rpc_twofa_submit(payload: PasswordRequest):
        try:
            result = await manager.submit_password(
                payload.tenant_id, payload.password.get_secret_value()
            )
        except Exception:
            logger.exception(
                "stage=password_failed event=password_exception endpoint=rpc_twofa_submit"
            )
            return JSONResponse(
                {"error": "password_exception"},
                status_code=500,
                headers=dict(NO_STORE_HEADERS),
            )
        headers = dict(NO_STORE_HEADERS)
        if result.headers:
            headers.update(result.headers)

        body = dict(result.body)
        status_code = int(result.status_code)
        error = str(body.get("error") or "").strip()

        if status_code == 200 and not error:
            snapshot = await manager.get_status(payload.tenant_id)
            return JSONResponse(snapshot.to_payload(), status_code=200, headers=headers)

        if error == "password_invalid":
            detail = body.get("detail") if isinstance(body, dict) else None
            response_body = {"error": error}
            if detail:
                response_body["detail"] = detail
            return JSONResponse(response_body, status_code=400, headers=headers)

        if error == "srp_invalid":
            detail = body.get("detail") if isinstance(body, dict) else None
            response_body = {"error": error}
            if detail:
                response_body["detail"] = detail
            return JSONResponse(response_body, status_code=409, headers=headers)

        if error in {"phone_password_flood", "flood_wait"}:
            snapshot = await manager.get_status(payload.tenant_id)
            response_body = {"error": error}
            retry_after = body.get("retry_after")
            if retry_after is not None:
                response_body["retry_after"] = retry_after
            backoff_until = snapshot.twofa_backoff_until
            if backoff_until is not None:
                response_body["backoff_until"] = int(backoff_until)
            detail = body.get("detail") if isinstance(body, dict) else None
            if detail:
                response_body["detail"] = detail
            return JSONResponse(response_body, status_code=429, headers=headers)

        if error:
            logger.error(
                "stage=rpc_twofa_submit_error event=%s tenant_id=%s", error, payload.tenant_id
            )
            return JSONResponse({"error": error}, status_code=500, headers=headers)

        return JSONResponse(body or {"error": "password_exception"}, status_code=500, headers=headers)

    @app.post("/send")
    async def send_message(payload: TransportMessage, _: None = Depends(require_credentials)):
        headers = dict(NO_STORE_HEADERS)

        def _error(status: int, message: str) -> JSONResponse:
            return JSONResponse({"ok": False, "error": message}, status_code=status, headers=headers)

        if payload.channel and payload.channel not in {"telegram"}:
            return _error(400, "channel_mismatch")
        if not payload.has_content:
            return _error(400, "empty_message")

        target = payload.to
        peer_id: Optional[int] = None
        if isinstance(target, str) and target.strip().lower() == "me":
            self_peer = await manager.resolve_self_peer(payload.tenant)
            if self_peer is None:
                return _error(409, "self_unavailable")
            peer_id = self_peer
        else:
            try:
                peer_id = int(target)
            except (TypeError, ValueError):
                return _error(400, "invalid_to")
            if peer_id <= 0:
                return _error(400, "invalid_to")

        attachments = [
            {
                "url": att.url,
                "name": att.name,
                "filename": att.name,
                "mime": att.mime,
                "mime_type": att.mime,
                "size": att.size,
                "type": att.type,
            }
            for att in payload.attachments
        ]

        try:
            await manager.send_message(
                tenant=payload.tenant,
                text=payload.text,
                peer_id=peer_id,
                telegram_user_id=None,
                username=None,
                attachments=attachments,
                reply_to=payload.meta.get("reply_to") if isinstance(payload.meta, dict) else None,
            )
        except ValueError as exc:
            return _error(400, str(exc))
        except RuntimeError as exc:
            return _error(409, str(exc))

        return JSONResponse({"ok": True}, headers=headers)

    @app.get("/health")
    async def health():
        stats = _safe_stats_snapshot()
        return {
            "ok": True,
            "authorized_count": int(stats.get("authorized", 0) or 0),
            "waiting_count": int(stats.get("waiting", 0) or 0),
            "needs_2fa": int(stats.get("needs_2fa", 0) or 0),
        }

    @app.get("/metrics")
    async def metrics():
        data = generate_latest()
        return PlainTextResponse(data.decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

    return app


__all__ = ["create_app"]

