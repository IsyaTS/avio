from __future__ import annotations

import asyncio
import base64
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, SecretStr, ValidationError
from telethon.errors import RPCError
from telethon.errors.rpcerrorlist import ChatAdminRequiredError
try:  # pragma: no cover - pydantic v1/v2 compatibility
    from pydantic import ConfigDict
except ImportError:  # pragma: no cover - pydantic v1
    ConfigDict = None  # type: ignore[misc]
try:  # pragma: no cover - optional validator APIs
    from pydantic import model_validator
except ImportError:  # pragma: no cover - pydantic v1
    model_validator = None  # type: ignore[assignment]
try:  # pragma: no cover - optional validator APIs
    from pydantic import root_validator
except ImportError:  # pragma: no cover - pydantic v2
    root_validator = None  # type: ignore[assignment]
try:  # pragma: no cover - field validator compatibility
    from pydantic import field_validator
except ImportError:  # pragma: no cover - pydantic v1
    field_validator = None  # type: ignore[assignment]
try:  # pragma: no cover - fallback for pydantic v1
    from pydantic import validator
except ImportError:  # pragma: no cover - pydantic v2
    validator = None  # type: ignore[assignment]
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from config import telegram_config
from app.schemas import Attachment

from .session_manager import (
    QRExpiredError,
    QRNotFoundError,
    SessionManager,
    TwoFASubmitResult,
    LoginFlowStateSnapshot,
    SessionSnapshot,
    NotAuthorizedError,
)
from .metrics import (
    TG_2FA_REQUIRED_TOTAL,
    TG_LOGIN_FAIL_TOTAL,
    TG_LOGIN_SUCCESS_TOTAL,
    TG_QR_EXPIRED_TOTAL,
    TG_QR_START_TOTAL,
)


logger = logging.getLogger("tgworker.api")
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()
_TELEGRAM_WEBHOOK_PATH = "/webhook/telegram"


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


if ConfigDict is None:

    class _TenantModel(BaseModel):
        class Config:
            allow_population_by_field_name = True
            allow_population_by_alias = True

        @classmethod
        def _ensure_tenant(cls, values: dict[str, Any]) -> dict[str, Any]:
            if "tenant" not in values and "tenant_id" in values:
                values = dict(values)
                values["tenant"] = values.pop("tenant_id")
            return values

        if model_validator is not None:  # pragma: no branch - prefer v2 API

            @model_validator(mode="before")
            def _alias_tenant(cls, values: Any) -> Any:
                if isinstance(values, dict):
                    return cls._ensure_tenant(values)
                return values
        elif root_validator is not None:  # pragma: no branch - fallback for v1

            @root_validator(pre=True)
            def _alias_tenant(cls, values: dict[str, Any]) -> dict[str, Any]:
                return cls._ensure_tenant(values)

else:  # pragma: no cover - executed only on pydantic v2

    class _TenantModel(BaseModel):
        model_config = ConfigDict(populate_by_name=True)

        @classmethod
        def _ensure_tenant(cls, values: dict[str, Any]) -> dict[str, Any]:
            if isinstance(values, dict) and "tenant" not in values and "tenant_id" in values:
                data = dict(values)
                data["tenant"] = data.pop("tenant_id")
                return data
            return values

        if model_validator is not None:  # pragma: no branch - optional

            @model_validator(mode="before")
            def _alias_tenant(cls, values: Any) -> Any:
                if isinstance(values, dict):
                    return cls._ensure_tenant(values)
                return values
        elif root_validator is not None:  # pragma: no branch - fallback

            @root_validator(pre=True)
            def _alias_tenant(cls, values: dict[str, Any]) -> dict[str, Any]:
                return cls._ensure_tenant(values)


class TelegramSendRequest(_TenantModel):
    tenant: int = Field(..., ge=1)
    channel: str = Field(..., min_length=1)
    to: int | str
    text: str | None = None
    attachments: list[Attachment] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)

    if field_validator is not None:  # pragma: no branch - prefer v2 API

        @field_validator("channel")
        @classmethod
        def _normalize_channel(cls, value: str) -> str:
            cleaned = (value or "").strip().lower()
            if cleaned != "telegram":
                raise ValueError("channel_must_be_telegram")
            return "telegram"

    elif validator is not None:  # pragma: no branch - fallback for v1

        @validator("channel")
        def _normalize_channel(cls, value: str) -> str:
            cleaned = (value or "").strip().lower()
            if cleaned != "telegram":
                raise ValueError("channel_must_be_telegram")
            return "telegram"


class TenantBody(_TenantModel):
    tenant: int = Field(..., ge=1)


class TenantForceBody(TenantBody):
    force: bool = False


class TenantQuery(TenantBody):
    pass


class QRStartRequest(TenantForceBody):
    pass


class StartRequest(TenantForceBody):
    pass


class RestartRequest(TenantBody):
    pass


class LogoutRequest(TenantBody):
    force: bool = False


class PasswordRequest(TenantBody):
    password: SecretStr


class TwoFARequest(TenantBody):
    password: SecretStr


def _resolve_webhook_url() -> tuple[str, Optional[str]]:
    explicit = (os.getenv("APP_WEBHOOK") or "").strip()
    if explicit:
        url = explicit.rstrip("/")
    else:
        tg_specific = (os.getenv("TG_WEBHOOK_URL") or "").strip()
        if tg_specific:
            url = tg_specific.rstrip("/")
        else:
            base = (os.getenv("APP_INTERNAL_URL") or "").strip()
            if not base:
                base = (os.getenv("APP_BASE_URL") or "").strip()
            if not base:
                base = "http://app:8000"
            url = f"{base.rstrip('/')}{_TELEGRAM_WEBHOOK_PATH}"
    token = (os.getenv("WEBHOOK_SECRET") or "").strip() or None
    return url, token


def create_app() -> FastAPI:
    cfg = telegram_config()
    webhook_url, webhook_token = _resolve_webhook_url()
    logger.info(
        "stage=webhook_url_resolved url=%s token_present=%s",
        webhook_url,
        "true" if webhook_token else "false",
    )
    manager = SessionManager(
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        webhook_url=webhook_url,
        sessions_dir=cfg.sessions_dir,
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

    def _unauthorized_response(route: str, tenant: int | None = None) -> JSONResponse:
        logger.warning("event=admin_token_invalid route=%s tenant=%s", route, tenant)
        return JSONResponse(
            {"error": "not_authorized"},
            status_code=401,
            headers=dict(NO_STORE_HEADERS),
        )

    def _enforce_admin(
        request: Request,
        route: str,
        *,
        tenant: int | None = None,
    ) -> JSONResponse | None:
        if not ADMIN_TOKEN:
            return None
        header = request.headers.get("X-Admin-Token", "").strip()
        if not header or header != ADMIN_TOKEN:
            return _unauthorized_response(route, tenant)
        return None

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
                if not force_login:
                    body = _entry_payload(entry, stats=_safe_stats_snapshot())
                    body.update({"error": "already_authorized"})
                    return JSONResponse(
                        body, status_code=409, headers=dict(NO_STORE_HEADERS)
                    )
                await manager.logout(tenant, force=force_login)
                entry = await _refresh_pending(tenant)

            if entry.state == "need_2fa":
                body = _entry_payload(entry, stats=_safe_stats_snapshot())
                return JSONResponse(body, headers=dict(NO_STORE_HEADERS))

            should_start = force_login or entry.authorized
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
        tenant_params: TenantQuery = Depends(), qr_id: str = Query(..., min_length=1)
    ):
        tenant = tenant_params.tenant
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
    async def status(request: Request, tenant_params: TenantQuery = Depends()):
        tenant = tenant_params.tenant
        unauthorized = _enforce_admin(request, "/status", tenant=tenant)
        if unauthorized is not None:
            return unauthorized
        lock = _tenant_lock(tenant)
        async with lock:
            entry = await _refresh_pending(tenant)
            body = _entry_payload(entry, stats=_safe_stats_snapshot())
            return JSONResponse(body, headers=dict(NO_STORE_HEADERS))

    @app.post("/session/start")
    async def start_session(
        request: Request,
        payload: StartRequest,
        _: None = Depends(require_credentials),
    ):
        unauthorized = _enforce_admin(request, "/session/start", tenant=payload.tenant)
        if unauthorized is not None:
            return unauthorized
        if not payload.force:
            current = await manager.get_status(payload.tenant)
            if current.twofa_pending or current.needs_2fa:
                return JSONResponse(current.to_payload(), headers=dict(NO_STORE_HEADERS))

        snapshot = await manager.start_session(payload.tenant, force=payload.force)
        return JSONResponse(snapshot.to_payload(), headers=dict(NO_STORE_HEADERS))

    @app.post("/rpc/start")
    async def rpc_start(request: Request, payload: StartRequest):
        unauthorized = _enforce_admin(request, "/rpc/start", tenant=payload.tenant)
        if unauthorized is not None:
            return unauthorized
        snapshot = await manager.start_session(payload.tenant, force=payload.force)
        body = {
            "status": snapshot.status,
            "qr_id": snapshot.qr_id,
            "qr_valid_until": snapshot.qr_valid_until,
        }
        return JSONResponse(body, headers=dict(NO_STORE_HEADERS))

    @app.post("/session/restart")
    async def restart_session(
        request: Request,
        payload: RestartRequest,
        _: None = Depends(require_credentials),
    ):
        unauthorized = _enforce_admin(request, "/session/restart", tenant=payload.tenant)
        if unauthorized is not None:
            return unauthorized
        snapshot = await manager.start_session(payload.tenant, force=True)
        return JSONResponse(snapshot.to_payload(), headers=dict(NO_STORE_HEADERS))

    @app.get("/session/status")
    async def session_status(request: Request, tenant_params: TenantQuery = Depends()):
        tenant = tenant_params.tenant
        unauthorized = _enforce_admin(request, "/session/status", tenant=tenant)
        if unauthorized is not None:
            return unauthorized
        session_snapshot = await manager.get_status(tenant)
        stats = manager.stats_snapshot()
        payload = session_snapshot.to_payload()
        payload["stats"] = stats
        return JSONResponse(payload, headers=dict(NO_STORE_HEADERS))

    @app.get("/rpc/status")
    async def rpc_status(request: Request, tenant_params: TenantQuery = Depends()):
        tenant = tenant_params.tenant
        unauthorized = _enforce_admin(request, "/rpc/status", tenant=tenant)
        if unauthorized is not None:
            return unauthorized
        session_snapshot = await manager.get_status(tenant)
        stats = manager.stats_snapshot()
        payload = session_snapshot.to_payload()
        payload["stats"] = stats
        return JSONResponse(payload, headers=dict(NO_STORE_HEADERS))

    @app.get("/rpc/qr.png")
    async def rpc_qr_png(
        request: Request,
        tenant_params: TenantQuery = Depends(),
        qr_id: str = Query(..., min_length=1),
    ):
        tenant = tenant_params.tenant
        unauthorized = _enforce_admin(request, "/rpc/qr.png", tenant=tenant)
        if unauthorized is not None:
            return unauthorized
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
    async def session_qr(request: Request, qr_id: str):
        unauthorized = _enforce_admin(request, "/session/qr.png")
        if unauthorized is not None:
            return unauthorized
        try:
            blob = manager.get_qr_png(qr_id)
        except QRExpiredError as exc:
            raise HTTPException(status_code=404, detail="qr_expired") from exc
        except QRNotFoundError as exc:
            raise HTTPException(status_code=404, detail="qr_not_found") from exc
        headers = dict(NO_STORE_HEADERS)
        return Response(content=blob, media_type="image/png", headers=headers)

    @app.get("/session/qr/{qr_id}.txt")
    async def session_qr_txt(request: Request, qr_id: str):
        unauthorized = _enforce_admin(request, "/session/qr.txt")
        if unauthorized is not None:
            return unauthorized
        try:
            login_url = manager.get_qr_url(qr_id)
        except QRExpiredError as exc:
            raise HTTPException(status_code=404, detail="qr_expired") from exc
        except QRNotFoundError as exc:
            raise HTTPException(status_code=404, detail="qr_not_found") from exc
        headers = dict(NO_STORE_HEADERS)
        return PlainTextResponse(login_url, headers=headers)

    @app.post("/session/logout")
    async def session_logout(request: Request, payload: LogoutRequest = Body(...)):
        unauthorized = _enforce_admin(request, "/session/logout", tenant=payload.tenant)
        if unauthorized is not None:
            return unauthorized
        await manager.logout(payload.tenant, force=payload.force)
        return JSONResponse({"ok": True}, headers=dict(NO_STORE_HEADERS))

    def _password_response(result: TwoFASubmitResult) -> JSONResponse:
        headers = dict(NO_STORE_HEADERS)
        if result.headers:
            headers.update(result.headers)
        status_code = int(result.status_code)
        body = dict(result.body)
        return JSONResponse(body, status_code=status_code, headers=headers)

    @app.post("/session/password")
    async def session_password(request: Request, payload: PasswordRequest):
        unauthorized = _enforce_admin(request, "/session/password", tenant=payload.tenant)
        if unauthorized is not None:
            return unauthorized
        try:
            result = await manager.submit_password(
                payload.tenant, payload.password.get_secret_value()
            )
        except Exception:
            logger.exception(
                "stage=password_failed event=password_exception route=/session/password tenant=%s",
                payload.tenant,
            )
            return JSONResponse(
                {"error": "password_exception"},
                status_code=500,
                headers=dict(NO_STORE_HEADERS),
            )
        return _password_response(result)

    @app.post("/rpc/twofa.submit")
    async def rpc_twofa_submit(request: Request, payload: PasswordRequest):
        unauthorized = _enforce_admin(request, "/rpc/twofa.submit", tenant=payload.tenant)
        if unauthorized is not None:
            return unauthorized
        try:
            result = await manager.submit_password(
                payload.tenant, payload.password.get_secret_value()
            )
        except Exception:
            logger.exception(
                "stage=password_failed event=password_exception route=/rpc/twofa.submit tenant=%s",
                payload.tenant,
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
            snapshot = await manager.get_status(payload.tenant)
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
            snapshot = await manager.get_status(payload.tenant)
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
                "stage=rpc_twofa_submit_error event=%s tenant_id=%s",
                error,
                payload.tenant,
            )
            return JSONResponse({"error": error}, status_code=500, headers=headers)

        return JSONResponse(body or {"error": "password_exception"}, status_code=500, headers=headers)

    @app.post("/send")
    async def send_message(
        request: Request,
        raw_payload: dict[str, Any] = Body(...),
        _: None = Depends(require_credentials),
    ):
        headers = dict(NO_STORE_HEADERS)
        tenant_hint: int | None = None
        try:
            tenant_hint = int(raw_payload.get("tenant"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            tenant_hint = None
        unauthorized = _enforce_admin(request, "/send", tenant=tenant_hint)
        if unauthorized is not None:
            return unauthorized

        def _error(status: int, message: str) -> JSONResponse:
            return JSONResponse({"error": message}, status_code=status, headers=headers)

        try:
            payload = TelegramSendRequest(**raw_payload)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors()) from exc

        has_text = isinstance(payload.text, str) and payload.text.strip()
        if not has_text and not payload.attachments:
            raise HTTPException(
                status_code=422,
                detail=[
                    {
                        "loc": ["body", "text"],
                        "msg": "message_content_required",
                        "type": "value_error",
                    }
                ],
            )

        peer_entity: Any | None = None
        username: str | None = None
        telegram_user_id: int | None = None

        target = payload.to
        if isinstance(target, str):
            normalized = target.strip()
            lowered = normalized.lower()
            if lowered in {"me", "self"}:
                try:
                    resolver = getattr(manager, "resolve_self_peer")
                except AttributeError:
                    return _error(400, "unsupported_version")
                try:
                    peer_entity = await resolver(payload.tenant)
                except AttributeError:
                    return _error(400, "unsupported_version")
                except NotAuthorizedError:
                    return _error(409, "not_authorized")
                except Exception:
                    logger.exception(
                        "event=resolve_self_peer_failed route=/send tenant=%s",
                        payload.tenant,
                    )
                    return _error(500, "send_failed")
                if peer_entity is None:
                    return _error(409, "not_authorized")
            else:
                try:
                    candidate = int(normalized)
                except (TypeError, ValueError):
                    if not normalized:
                        raise HTTPException(
                            status_code=422,
                            detail=[
                                {
                                    "loc": ["body", "to"],
                                    "msg": "recipient_required",
                                    "type": "value_error",
                                }
                            ],
                        )
                    username = normalized
                else:
                    if candidate <= 0:
                        raise HTTPException(
                            status_code=422,
                            detail=[
                                {
                                    "loc": ["body", "to"],
                                    "msg": "recipient_invalid",
                                    "type": "value_error",
                                }
                            ],
                        )
                    peer_entity = candidate
        else:
            if target <= 0:
                raise HTTPException(
                    status_code=422,
                    detail=[
                        {
                            "loc": ["body", "to"],
                            "msg": "recipient_invalid",
                            "type": "value_error",
                        }
                    ],
                )
            peer_entity = int(target)

        if peer_entity is None and username is None and telegram_user_id is None:
            raise HTTPException(
                status_code=422,
                detail=[
                    {
                        "loc": ["body", "to"],
                        "msg": "recipient_unresolved",
                        "type": "value_error",
                    }
                ],
            )

        meta_peer_value: Any = None

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

        reply_to_value = None
        if isinstance(payload.meta, dict):
            reply_to_value = payload.meta.get("reply_to")
            meta_peer_value = payload.meta.get("peer_id")
            raw_meta_user = payload.meta.get("telegram_user_id")
            if telegram_user_id is None and raw_meta_user is not None:
                try:
                    telegram_user_id = int(raw_meta_user)
                except (TypeError, ValueError):
                    telegram_user_id = telegram_user_id

        forbidden_peer = False
        candidate_ids: list[int] = []
        for candidate in (
            peer_entity if isinstance(peer_entity, int) else None,
            telegram_user_id,
        ):
            if candidate is None:
                continue
            try:
                candidate_ids.append(int(candidate))
            except (TypeError, ValueError):
                continue
        if meta_peer_value is not None:
            try:
                candidate_ids.append(int(meta_peer_value))
            except (TypeError, ValueError):
                pass
        for candidate in candidate_ids:
            if candidate < 0 or str(candidate).startswith("-100"):
                forbidden_peer = True
                break
        if not forbidden_peer and isinstance(payload.to, str):
            lowered_target = payload.to.strip().lower()
            if lowered_target.startswith("-100"):
                forbidden_peer = True
        if forbidden_peer:
            detail = {"type": "forbidden_peer_type", "peer_id": payload.to}
            return JSONResponse(
                {"error": "forbidden_peer_type", "details": detail},
                status_code=403,
                headers=headers,
            )

        async def _send_once() -> dict[str, Any]:
            return await manager.send_message(
                tenant=payload.tenant,
                text=payload.text,
                peer_id=peer_entity,
                telegram_user_id=telegram_user_id,
                username=username,
                attachments=attachments,
                reply_to=reply_to_value,
            )

        def _extract_error(result: dict[str, Any] | None) -> str:
            if isinstance(result, dict) and "error" in result:
                raw_error = result.get("error")
                if raw_error is not None:
                    return str(raw_error).strip()
            return ""

        def _rpc_error_response(exc: RPCError) -> JSONResponse:
            error_type = exc.__class__.__name__
            peer_hint: Any = payload.to
            detail: dict[str, Any] = {
                "type": error_type,
                "peer_id": peer_hint,
            }
            message = str(exc).strip()
            if message:
                detail["message"] = message
            if isinstance(exc, ChatAdminRequiredError):
                logger.warning(
                    "event=send_message_forbidden_peer route=/send tenant=%s peer=%s",
                    payload.tenant,
                    peer_hint,
                )
                return JSONResponse(
                    {"error": "forbidden_peer_type", "details": detail},
                    status_code=403,
                    headers=headers,
                )
            logger.error(
                "event=send_message_rpc_error route=/send tenant=%s type=%s peer=%s",
                payload.tenant,
                error_type,
                peer_hint,
            )
            return JSONResponse(
                {"error": "send_failed", "details": detail},
                status_code=500,
                headers=headers,
            )

        try:
            result = await _send_once()
        except ValueError as exc:
            return _error(400, str(exc))
        except RPCError as exc:
            return _rpc_error_response(exc)
        except Exception:
            logger.exception(
                "event=send_message_failed route=/send tenant=%s", payload.tenant
            )
            return _error(500, "send_failed")

        error_value = _extract_error(result)

        if error_value == "authkey_unregistered":
            logger.warning(
                "event=authkey_unregistered_autoretry route=/send tenant=%s", payload.tenant
            )
            try:
                await manager.start_session(payload.tenant, force=True)
            except Exception:
                logger.exception(
                    "event=authkey_restart_failed route=/send tenant=%s", payload.tenant
                )
                headers["X-Reauth"] = "1"
                headers["Cache-Control"] = NO_STORE_HEADERS.get("Cache-Control", "no-store")
                headers["Pragma"] = NO_STORE_HEADERS.get("Pragma", "no-cache")
                headers["Expires"] = NO_STORE_HEADERS.get("Expires", "0")
                return JSONResponse(
                    {"error": "relogin_required"},
                    status_code=409,
                    headers=headers,
                )
            try:
                result = await _send_once()
            except RPCError as exc:
                return _rpc_error_response(exc)
            except Exception:
                logger.exception(
                    "event=send_message_failed route=/send tenant=%s", payload.tenant
                )
                return _error(500, "send_failed")
            error_value = _extract_error(result)

        if error_value:
            if error_value == "authkey_unregistered":
                headers["X-Reauth"] = "1"
                headers["Cache-Control"] = NO_STORE_HEADERS.get("Cache-Control", "no-store")
                headers["Pragma"] = NO_STORE_HEADERS.get("Pragma", "no-cache")
                headers["Expires"] = NO_STORE_HEADERS.get("Expires", "0")
                return JSONResponse(
                    {"error": "relogin_required"},
                    status_code=409,
                    headers=headers,
                )
            if error_value == "not_authorized":
                logger.warning(
                    "event=send_message_not_authorized route=/send tenant=%s peer=%s",
                    payload.tenant,
                    payload.to,
                )
                return JSONResponse({"error": "not_authorized"}, status_code=401, headers=headers)
            if error_value == "send_failed":
                detail = {
                    "type": "unknown",
                    "peer_id": payload.to,
                }
                return JSONResponse(
                    {"error": "send_failed", "details": detail},
                    status_code=500,
                    headers=headers,
                )
            logger.error(
                "event=send_message_unhandled_error route=/send tenant=%s error=%s",
                payload.tenant,
                error_value,
            )
            return JSONResponse({"error": "send_failed"}, status_code=500, headers=headers)

        response_payload: dict[str, Any] = {"ok": True}
        if isinstance(result, dict):
            peer_value = result.get("peer_id")
            if isinstance(peer_value, int):
                response_payload["peer_id"] = peer_value
            message_id = result.get("message_id")
            if message_id is not None:
                try:
                    response_payload["message_id"] = int(message_id)
                except (TypeError, ValueError):
                    pass

        return JSONResponse(response_payload, headers=headers)

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

