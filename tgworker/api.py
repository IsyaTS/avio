from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field, model_validator
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from config import telegram_config

from .manager import TelegramSessionManager


logger = logging.getLogger("tgworker.api")


class StartRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)


class LogoutRequest(BaseModel):
    tenant_id: int = Field(..., ge=1)


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
    manager = TelegramSessionManager(
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        sessions_dir=cfg.sessions_dir,
        webhook_url=webhook_url,
        webhook_token=webhook_token,
    )

    app = FastAPI(title="tgworker")

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
        state = await manager.start_session(payload.tenant_id)
        return {
            "tenant_id": payload.tenant_id,
            "status": state.status,
            "qr_id": state.qr_id,
            "needs_2fa": state.needs_2fa,
        }

    @app.get("/session/status")
    async def session_status(tenant: int = Query(..., ge=1)):
        state = await manager.get_status(tenant)
        snapshot = manager.stats_snapshot()
        return {
            "tenant_id": tenant,
            "status": state.status,
            "qr_id": state.qr_id,
            "needs_2fa": state.needs_2fa,
            "last_error": state.last_error,
            "stats": snapshot,
        }

    @app.get("/session/qr/{qr_id}.png")
    async def session_qr(qr_id: str):
        blob = manager.get_qr_png(qr_id)
        if not blob:
            raise HTTPException(status_code=404, detail="qr_not_found")
        return Response(content=blob, media_type="image/png", headers={"Cache-Control": "no-store"})

    @app.post("/session/logout")
    async def session_logout(payload: LogoutRequest):
        await manager.logout(payload.tenant_id)
        return {"ok": True}

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
