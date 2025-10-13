from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, SecretStr
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from config import telegram_config
from app.schemas import TransportMessage

from .session_manager import (
    QRExpiredError,
    QRNotFoundError,
    SessionManager,
    TwoFASubmitResult,
)


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


def _resolve_webhook_url() -> tuple[str, Optional[str]]:
    explicit = os.getenv("APP_WEBHOOK")
    if explicit:
        url = explicit.rstrip("/")
    else:
        base = os.getenv("TG_WEBHOOK_URL") or os.getenv("APP_INTERNAL_URL") or "http://app:8000"
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
    )

    app = FastAPI(title="tgworker")
    app.state.session_manager = manager

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
        if payload.channel and payload.channel not in {"telegram"}:
            raise HTTPException(status_code=400, detail="channel_mismatch")
        if not payload.has_content:
            raise HTTPException(status_code=400, detail="empty_message")

        target = payload.to
        peer_id: Optional[int] = None
        if isinstance(target, str) and target.strip().lower() == "me":
            self_peer = await manager.resolve_self_peer(payload.tenant)
            if self_peer is None:
                raise HTTPException(status_code=409, detail="self_unavailable")
            peer_id = self_peer
        else:
            try:
                peer_id = int(target)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="invalid_to") from None
            if peer_id <= 0:
                raise HTTPException(status_code=400, detail="invalid_to")

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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"ok": True}

    @app.get("/health")
    async def health():
        snapshot = manager.stats_snapshot()
        return {
            "authorized_count": snapshot["authorized"],
            "waiting_count": snapshot["waiting"],
            "needs_2fa": snapshot["needs_2fa"],
            "message_in_total": manager.delivered_incoming_total(),
        }

    @app.get("/metrics")
    async def metrics():
        data = generate_latest()
        return PlainTextResponse(data.decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

    return app


__all__ = ["create_app"]

