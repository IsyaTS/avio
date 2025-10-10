from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, SecretStr, model_validator
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from config import telegram_config

from .session_manager import SessionManager, QRExpiredError, QRNotFoundError


logger = logging.getLogger("tgworker.api")


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


class SendRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)
    text: Optional[str] = None
    peer_id: Optional[int] = Field(None, ge=1)
    username: Optional[str] = None
    media_url: Optional[str] = Field(None, max_length=2048)

    @model_validator(mode="after")
    def _validate_target(self) -> "SendRequest":
        peer_id = self.peer_id
        username = self.username
        if not peer_id and not username:
            raise ValueError("peer_id_or_username_required")
        if not self.text and not self.media_url:
            raise ValueError("text_or_media_required")
        if username:
            normalized = username.strip()
            if normalized and not normalized.startswith("@"):
                normalized = f"@{normalized}"
            self.username = normalized or None
        return self


def _resolve_webhook_url() -> tuple[str, Optional[str]]:
    base = os.getenv("TG_WEBHOOK_URL") or os.getenv("APP_INTERNAL_URL") or "http://app:8000"
    token = os.getenv("WEBHOOK_SECRET") or None
    return f"{base.rstrip('/')}/webhook/telegram", token


def create_app() -> FastAPI:
    cfg = telegram_config()
    webhook_url, webhook_token = _resolve_webhook_url()
    manager = SessionManager(
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        sessions_dir=cfg.sessions_dir,
        webhook_url=webhook_url,
        webhook_token=webhook_token,
    )

    app = FastAPI(title="tgworker")

    NO_STORE_HEADERS = {
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    }

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

    @app.post("/session/start")
    async def start_session(payload: StartRequest, _: None = Depends(require_credentials)):
        snapshot = await manager.start_session(payload.tenant_id, force=payload.force)
        return {
            "tenant_id": payload.tenant_id,
            "status": snapshot.status,
            "qr_id": snapshot.qr_id,
            "qr_valid_until": snapshot.qr_valid_until,
            "needs_2fa": snapshot.status == "needs_2fa",
            "twofa_pending": snapshot.twofa_pending,
            "twofa_since": snapshot.twofa_since,
        }

    @app.post("/session/restart")
    async def restart_session(payload: RestartRequest, _: None = Depends(require_credentials)):
        snapshot = await manager.start_session(payload.tenant_id, force=True)
        return {
            "tenant_id": payload.tenant_id,
            "status": snapshot.status,
            "qr_id": snapshot.qr_id,
            "qr_valid_until": snapshot.qr_valid_until,
            "needs_2fa": snapshot.status == "needs_2fa",
            "twofa_pending": snapshot.twofa_pending,
            "twofa_since": snapshot.twofa_since,
        }

    @app.get("/session/status")
    async def session_status(tenant: int = Query(..., ge=1)):
        session_snapshot = await manager.get_status(tenant)
        stats = manager.stats_snapshot()
        return {
            "tenant_id": tenant,
            "status": session_snapshot.status,
            "qr_id": session_snapshot.qr_id,
            "qr_valid_until": session_snapshot.qr_valid_until,
            "needs_2fa": session_snapshot.status == "needs_2fa",
            "twofa_pending": session_snapshot.twofa_pending,
            "twofa_since": session_snapshot.twofa_since,
            "last_error": session_snapshot.last_error,
            "can_restart": session_snapshot.can_restart,
            "stats": stats,
        }

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
        return {"ok": True}

    @app.post("/session/password")
    async def session_password(payload: PasswordRequest):
        tenant = payload.tenant_id
        password = payload.password.get_secret_value()
        cache_headers = dict(NO_STORE_HEADERS)
        if not password:
            return JSONResponse({"error": "password_required"}, status_code=400, headers=cache_headers)

        try:
            ok = await manager.submit_password(tenant, password)
        except ValueError as exc:
            message = str(exc)
            if message == "invalid_password":
                return JSONResponse({"error": "invalid_password"}, status_code=400, headers=cache_headers)
            if message == "password_required":
                return JSONResponse({"error": "password_required"}, status_code=400, headers=cache_headers)
            if message == "twofa_timeout":
                return JSONResponse({"error": "twofa_timeout"}, status_code=409, headers=cache_headers)
            if message == "session_not_found":
                raise HTTPException(status_code=404, detail="session_not_found") from exc
            if message == "password_not_required":
                logger.warning(
                    "stage=password event=password_not_required tenant_id=%s", tenant
                )
                return JSONResponse({"error": "password_not_required"}, status_code=400, headers=cache_headers)
            raise HTTPException(status_code=400, detail=message) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        if not ok:
            return JSONResponse({"error": "invalid_password"}, status_code=400, headers=cache_headers)

        logger.info("stage=password_ok event=password_forward tenant_id=%s", tenant)
        return JSONResponse({"ok": True}, headers=cache_headers)

    @app.post("/send")
    async def send_message(payload: SendRequest, _: None = Depends(require_credentials)):
        try:
            await manager.send_message(
                tenant=payload.tenant_id,
                text=payload.text,
                peer_id=payload.peer_id,
                username=payload.username,
                media_url=payload.media_url,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"ok": True}

    @app.get("/health")
    async def health():
        snapshot = manager.stats_snapshot()
        return {"authorized_count": snapshot["authorized"], "waiting_count": snapshot["waiting"], "needs_2fa": snapshot["needs_2fa"]}

    @app.get("/metrics")
    async def metrics():
        data = generate_latest()
        return PlainTextResponse(data.decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

    return app


__all__ = ["create_app"]

