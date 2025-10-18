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

from app.metrics import DB_ERRORS_COUNTER, INTERNAL_TENANT_COUNTER
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
        INTERNAL_TENANT_COUNTER.labels("unauthorized").inc()
        raise HTTPException(status_code=401, detail="unauthorized")

    try:
        await provider_tokens.ensure_schema()
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("provider_token_ensure").inc()
        INTERNAL_TENANT_COUNTER.labels("error").inc()
        logger.exception("provider_token_schema_ensure_failed tenant=%s", tenant)
        raise HTTPException(status_code=500, detail="db_error") from exc

    try:
        common_module.ensure_tenant_files(int(tenant))
    except Exception as exc:
        logger.warning("tenant_files_ensure_failed tenant=%s error=%s", tenant, exc)
        INTERNAL_TENANT_COUNTER.labels("error").inc()
        raise HTTPException(status_code=500, detail="ensure_failed") from exc

    try:
        existing = await provider_tokens.get_by_tenant(int(tenant))
    except Exception as exc:
        DB_ERRORS_COUNTER.labels("provider_token_get").inc()
        INTERNAL_TENANT_COUNTER.labels("error").inc()
        logger.exception("provider_token_fetch_failed tenant=%s", tenant)
        raise HTTPException(status_code=500, detail="db_error") from exc

    if existing and existing.token:
        token_value = existing.token
    else:
        generated = secrets.token_urlsafe(32)
        try:
            created = await provider_tokens.create_for_tenant(int(tenant), generated)
        except Exception as exc:
            DB_ERRORS_COUNTER.labels("provider_token_create").inc()
            INTERNAL_TENANT_COUNTER.labels("error").inc()
            logger.exception("provider_token_create_failed tenant=%s", tenant)
            raise HTTPException(status_code=500, detail="db_error") from exc
        if created is None:
            try:
                existing_after = await provider_tokens.get_by_tenant(int(tenant))
            except Exception as exc:
                DB_ERRORS_COUNTER.labels("provider_token_get").inc()
                INTERNAL_TENANT_COUNTER.labels("error").inc()
                logger.exception("provider_token_fetch_failed tenant=%s", tenant)
                raise HTTPException(status_code=500, detail="db_error") from exc
            if not existing_after or not existing_after.token:
                INTERNAL_TENANT_COUNTER.labels("error").inc()
                logger.error("provider_token_unavailable tenant=%s", tenant)
                raise HTTPException(status_code=503, detail="token_unavailable")
            token_value = existing_after.token
        else:
            token_value = created.token

    logger.info("tenant_ensure_ok tenant=%s", tenant)
    INTERNAL_TENANT_COUNTER.labels("ok").inc()
    return _success({"tenant": int(tenant), "provider_token": token_value})


__all__ = ["router", "ensure_tenant"]
