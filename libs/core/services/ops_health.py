from __future__ import annotations

import time
from typing import Any, Iterable, Mapping

from libs.core import db
from libs.core.learning.config import intervention_learning_settings
from libs.core.sales_core import read_tenant_config


def _coerce_tenants(raw: str | Iterable[int] | None) -> list[int]:
    if raw is None:
        return []
    if isinstance(raw, str):
        values = raw.replace(";", ",").split(",")
    else:
        values = list(raw)
    tenants: list[int] = []
    for item in values:
        try:
            tenant = int(str(item).strip())
        except Exception:
            continue
        if tenant > 0 and tenant not in tenants:
            tenants.append(tenant)
    return tenants


def _status(ok: bool, **extra: Any) -> dict[str, Any]:
    return {"ok": bool(ok), **extra}


async def _db_status() -> dict[str, Any]:
    try:
        fetchrow = getattr(db, "_fetchrow", None)
        if fetchrow is None:
            return _status(False, error="db_fetchrow_unavailable")
        row = await fetchrow("SELECT 1 AS ok")
        value = None
        if row is not None:
            try:
                value = row["ok"]
            except Exception:
                value = getattr(row, "ok", None)
        return _status(value == 1)
    except Exception as exc:
        return _status(False, error=type(exc).__name__)


def _redis_status(redis_client: Any | None) -> dict[str, Any]:
    if redis_client is None:
        return _status(False, error="redis_client_unavailable")
    try:
        pong = redis_client.ping()
        return _status(bool(pong))
    except Exception as exc:
        return _status(False, error=type(exc).__name__)


async def _training_counts(tenant: int) -> dict[str, Any]:
    try:
        fetchrow = getattr(db, "_fetchrow", None)
        if fetchrow is None:
            return {"available": False, "error": "db_fetchrow_unavailable"}
        row = await fetchrow(
            """
            SELECT
                count(*) FILTER (WHERE is_active = TRUE AND is_bad = FALSE) AS active,
                count(*) FILTER (WHERE embedding_status = 'ready' AND is_active = TRUE AND is_bad = FALSE) AS ready
            FROM training_examples
            WHERE tenant_id = $1
            """,
            int(tenant),
        )
        if not row:
            return {"available": True, "active": 0, "ready": 0}
        return {
            "available": True,
            "active": int(row["active"] or 0),
            "ready": int(row["ready"] or 0),
        }
    except Exception as exc:
        return {"available": False, "error": type(exc).__name__}


def _avito_status(cfg: Mapping[str, Any]) -> dict[str, Any]:
    integrations = cfg.get("integrations") if isinstance(cfg, Mapping) else {}
    avito = integrations.get("avito") if isinstance(integrations, Mapping) else {}
    if not isinstance(avito, Mapping):
        avito = {}
    access_token = str(avito.get("access_token") or "").strip()
    refresh_token = str(avito.get("refresh_token") or "").strip()
    expires_at = None
    try:
        expires_at = int(avito.get("expires_at")) if avito.get("expires_at") is not None else None
    except Exception:
        expires_at = None
    now = int(time.time())
    return {
        "configured": bool(access_token or refresh_token or avito.get("account_id")),
        "has_access_token": bool(access_token),
        "has_refresh_token": bool(refresh_token),
        "expires_at": expires_at,
        "expired": bool(expires_at is not None and expires_at <= now),
        "account_id_present": avito.get("account_id") is not None,
        "account_login_present": bool(str(avito.get("account_login") or "").strip()),
    }


async def _tenant_status(tenant: int) -> dict[str, Any]:
    try:
        cfg = read_tenant_config(int(tenant))
    except Exception as exc:
        return {"tenant": int(tenant), "ok": False, "error": type(exc).__name__}
    if not isinstance(cfg, Mapping):
        return {"tenant": int(tenant), "ok": False, "error": "config_not_mapping"}

    behavior = cfg.get("behavior") if isinstance(cfg.get("behavior"), Mapping) else {}
    learning = intervention_learning_settings(cfg)
    training = await _training_counts(int(tenant))
    return {
        "tenant": int(tenant),
        "ok": True,
        "settings": {
            "avito_smart_reply_enabled": bool(
                behavior.get("avito_smart_reply_enabled")
                or behavior.get("auto_reply")
                or behavior.get("auto_reply_enabled")
            ),
            "has_persona_ref": bool(cfg.get("persona") or cfg.get("personas")),
            "follow_up_count": len(cfg.get("follow_up") or []) if isinstance(cfg.get("follow_up"), list) else 0,
        },
        "avito": _avito_status(cfg),
        "learning": {
            "enabled": bool(learning.get("enabled")),
            "capture_enabled": bool(learning.get("capture_enabled")),
            "runtime_enabled": bool(learning.get("runtime_enabled")),
            "apply_mode": bool(learning.get("apply_mode")),
            "shadow_mode": bool(learning.get("shadow_mode")),
            "training_examples": training,
        },
    }


async def build_deep_health(
    *,
    redis_client: Any | None = None,
    tenants: str | Iterable[int] | None = None,
) -> dict[str, Any]:
    tenant_ids = _coerce_tenants(tenants)
    tenant_reports = [await _tenant_status(tenant) for tenant in tenant_ids]
    db_report = await _db_status()
    redis_report = _redis_status(redis_client)
    ok = bool(db_report.get("ok")) and bool(redis_report.get("ok")) and all(
        bool(item.get("ok")) for item in tenant_reports
    )
    return {
        "ok": ok,
        "status": "healthy" if ok else "degraded",
        "db": db_report,
        "redis": redis_report,
        "tenants": tenant_reports,
    }
