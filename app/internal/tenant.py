from __future__ import annotations

import logging
import os
import secrets

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

try:  # pragma: no cover - runtime import order varies
    import core  # type: ignore
except ImportError:  # pragma: no cover - tests/scripts fallback
    from app import core  # type: ignore

try:  # pragma: no cover - optional package alias
    from app.web import common as common_module  # type: ignore
except ImportError:  # pragma: no cover - fallback for tests
    import importlib

    common_module = importlib.import_module("app.web.common")

from app.repo import provider_tokens


logger = logging.getLogger(__name__)
settings = core.settings  # type: ignore[attr-defined]
router = APIRouter(prefix="/internal/tenant", tags=["internal-tenant"])


def _success(payload: dict[str, object]) -> JSONResponse:
    return JSONResponse({"ok": True, **payload})


@router.post("/{tenant}/ensure")
async def ensure_tenant(tenant: int, request: Request) -> JSONResponse:
    """Ensure tenant filesystem scaffolding and provider token."""

    token_header = (request.headers.get("X-Auth-Token") or "").strip()
    token_query = (request.query_params.get("token") or "").strip()
    allowed = (
        (settings.WEBHOOK_SECRET or "").strip()
        or (os.getenv("WA_WEB_TOKEN") or "").strip()
    )
    if allowed and token_header != allowed and token_query != allowed:
        raise HTTPException(status_code=401, detail="unauthorized")

    try:
        common_module.ensure_tenant_files(int(tenant))
    except Exception as exc:
        logger.warning("tenant_files_ensure_failed tenant=%s error=%s", tenant, exc)
        raise HTTPException(status_code=500, detail="ensure_failed") from exc

    existing = await provider_tokens.get_by_tenant(int(tenant))
    if existing and existing.token:
        token_value = existing.token
    else:
        generated = secrets.token_urlsafe(32)
        created = await provider_tokens.create_for_tenant(int(tenant), generated)
        if created is None:
            logger.error("provider_token_create_failed tenant=%s", tenant)
            raise HTTPException(status_code=503, detail="token_unavailable")
        token_value = created.token

    logger.info("tenant_ensure_ok tenant=%s", tenant)
    return _success({"tenant": int(tenant), "provider_token": token_value})


__all__ = ["router", "ensure_tenant"]
