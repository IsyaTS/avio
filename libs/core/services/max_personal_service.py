from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import secrets
import time
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

from libs.core import sales_core as core

settings = core.settings  # type: ignore[attr-defined]
read_tenant_config = core.read_tenant_config  # type: ignore[attr-defined]
write_tenant_config = core.write_tenant_config  # type: ignore[attr-defined]

_TRUE_VALUES = {"1", "true", "yes", "on"}
_RUNTIME_INTEGRATION_OVERRIDES: dict[int, dict[str, Any]] = {}
_LOG = logging.getLogger("max.personal.service")


def _overlay_config_dir() -> Path:
    from_settings = os.getenv("TENANT_CONFIG_DIR") or os.getenv("TENANTS_CONFIG_DIR") or ""
    if from_settings.strip():
        return Path(from_settings.strip())
    configured = getattr(core, "TENANT_CONFIG_DIR", None)
    if configured:
        try:
            return Path(str(configured))
        except Exception:
            pass
    return Path("/app/config/tenants")


def _overlay_config_path(tenant_id: int) -> Path:
    return _overlay_config_dir() / f"{int(tenant_id)}.json"


def _load_overlay_config(tenant_id: int) -> dict[str, Any]:
    path = _overlay_config_path(int(tenant_id))
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _write_overlay_config(tenant_id: int, cfg: Mapping[str, Any]) -> bool:
    path = _overlay_config_path(int(tenant_id))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(dict(cfg), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return True
    except Exception as exc:
        _LOG.warning(
            "max_personal_overlay_write_failed tenant=%s path=%s error=%s",
            tenant_id,
            path,
            exc,
        )
        return False


def _session_metadata_paths(tenant_id: int) -> list[Path]:
    tenant_key = int(tenant_id)
    bases: list[Path] = []
    env_sessions = os.getenv("MAX_PERSONAL_SESSIONS_DIR") or ""
    if env_sessions.strip():
        bases.append(Path(env_sessions.strip()))
    # Runtime default inside containers.
    bases.append(Path("/data/max-personal-sessions"))
    # Dev/staging host paths.
    bases.append(Path("data/max-personal-sessions"))
    bases.append(Path("apps/maxworker/data/max-personal-sessions"))
    seen: set[str] = set()
    result: list[Path] = []
    for base in bases:
        key = str(base)
        if key in seen:
            continue
        seen.add(key)
        result.append(base / f"tenant-{tenant_key}" / "avio-session.json")
    return result


def _session_metadata_payload(tenant_id: int) -> dict[str, Any]:
    for path in _session_metadata_paths(int(tenant_id)):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(raw, dict):
            return raw
    return {}


def _session_metadata_secret(tenant_id: int) -> str:
    payload = _session_metadata_payload(int(tenant_id))
    token = str(payload.get("webhook_token") or "").strip()
    if token:
        return token
    callback_url = str(payload.get("callback_url") or "").strip()
    if callback_url:
        try:
            query = parse_qs(urlparse(callback_url).query)
            query_token = str((query.get("token") or [""])[0] or "").strip()
            if query_token:
                return query_token
        except Exception:
            pass
    return ""


def _session_metadata_status(tenant_id: int) -> str:
    payload = _session_metadata_payload(int(tenant_id))
    return str(payload.get("last_status") or "").strip()


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _settings_attr(name: str, default: str) -> str:
    value = getattr(settings, name, "") if settings is not None else ""
    text = str(value or "").strip()
    if text:
        return text
    return default


def max_personal_worker_url() -> str:
    env_value = (
        os.getenv("MAX_PERSONAL_WORKER_URL")
        or os.getenv("MAXWORKER_URL")
        or _settings_attr("MAX_PERSONAL_WORKER_URL", "")
    ).strip()
    return (env_value or "http://maxworker:9010").rstrip("/")


def max_personal_worker_token() -> str:
    return (
        os.getenv("MAX_PERSONAL_WORKER_TOKEN")
        or os.getenv("MAXWORKER_TOKEN")
        or os.getenv("WEBHOOK_SECRET")
        or _settings_attr("WEBHOOK_SECRET", "")
    ).strip()


def global_kill_switch() -> bool:
    raw = (os.getenv("MAX_PERSONAL_KILL_SWITCH") or "").strip().lower()
    return raw in _TRUE_VALUES


def global_outbound_disabled() -> bool:
    raw = (os.getenv("MAX_PERSONAL_OUTBOUND_DISABLED") or "").strip().lower()
    return raw in _TRUE_VALUES


def get_integration(tenant_id: int) -> dict[str, Any]:
    tenant_key = int(tenant_id)
    cfg = read_tenant_config(tenant_key)
    integration: dict[str, Any] = {}
    if not isinstance(cfg, Mapping):
        cfg = {}
    integrations = cfg.get("integrations") if isinstance(cfg, Mapping) else {}
    if isinstance(integrations, Mapping):
        max_cfg = integrations.get("max_personal")
        if isinstance(max_cfg, Mapping):
            integration = dict(max_cfg)
    # Recover critical fields from persisted maxworker session metadata when config drift happened.
    if not str(integration.get("event_secret") or "").strip():
        restored_secret = _session_metadata_secret(tenant_key)
        if restored_secret:
            integration["event_secret"] = restored_secret
    if not str(integration.get("session_status") or "").strip():
        restored_status = _session_metadata_status(tenant_key)
        if restored_status:
            integration["session_status"] = restored_status
    override = _RUNTIME_INTEGRATION_OVERRIDES.get(tenant_key)
    if isinstance(override, Mapping):
        integration.update(dict(override))
    return integration


def update_integration(tenant_id: int, updates: Mapping[str, Any]) -> dict[str, Any]:
    tenant_key = int(tenant_id)
    cfg = read_tenant_config(tenant_key)
    if not isinstance(cfg, dict):
        cfg = {}
    integrations = cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    existing = integrations.get("max_personal")
    merged: dict[str, Any]
    if isinstance(existing, Mapping):
        merged = dict(existing)
    else:
        merged = {}
    merged.update(dict(updates))
    merged["updated_at"] = int(time.time())
    _RUNTIME_INTEGRATION_OVERRIDES[tenant_key] = dict(merged)
    integrations["max_personal"] = merged
    cfg["integrations"] = integrations
    try:
        write_tenant_config(tenant_key, cfg)
    except Exception as exc:
        _LOG.warning(
            "max_personal_config_write_failed tenant=%s error=%s",
            tenant_key,
            exc,
        )
        # Fallback for read-only primary tenant.json: persist into tenant overlay config.
        overlay_cfg = _load_overlay_config(tenant_key)
        overlay_integrations = overlay_cfg.get("integrations")
        if not isinstance(overlay_integrations, dict):
            overlay_integrations = {}
        overlay_mp = overlay_integrations.get("max_personal")
        merged_overlay: dict[str, Any]
        if isinstance(overlay_mp, Mapping):
            merged_overlay = dict(overlay_mp)
        else:
            merged_overlay = {}
        merged_overlay.update(dict(merged))
        overlay_integrations["max_personal"] = merged_overlay
        overlay_cfg["integrations"] = overlay_integrations
        _write_overlay_config(tenant_key, overlay_cfg)
    return merged


def ensure_event_secret(tenant_id: int) -> str:
    current = get_integration(int(tenant_id))
    existing = str(current.get("event_secret") or "").strip()
    if existing:
        update_integration(int(tenant_id), {"event_secret": existing})
        return existing
    # Prefer current session-bound secret to avoid callback/token desync.
    from_session = _session_metadata_secret(int(tenant_id))
    if from_session:
        update_integration(int(tenant_id), {"event_secret": from_session})
        return from_session
    generated = secrets.token_urlsafe(20)
    update_integration(int(tenant_id), {"event_secret": generated})
    return generated


def integration_enabled(tenant_id: int) -> bool:
    if global_kill_switch():
        return False
    cfg = get_integration(int(tenant_id))
    if "enabled" in cfg and cfg.get("enabled") is not None:
        return _as_bool(cfg.get("enabled"), False)
    # Safe fallback: authorized session should not be implicitly disabled by config drift.
    session_status = str(cfg.get("session_status") or "").strip().lower()
    if session_status == "authorized":
        return True
    return False


def outbound_enabled(tenant_id: int) -> bool:
    if global_outbound_disabled() or global_kill_switch():
        return False
    cfg = get_integration(int(tenant_id))
    return _as_bool(cfg.get("outbound_enabled"), True)


def build_state_payload(tenant_id: int, session_payload: Mapping[str, Any] | None) -> dict[str, Any]:
    cfg = get_integration(int(tenant_id))
    state = str((session_payload or {}).get("status") or cfg.get("session_status") or "idle")
    account = (session_payload or {}).get("account") or cfg.get("account") or {}
    if not isinstance(account, Mapping):
        account = {}
    return {
        "tenant": int(tenant_id),
        "enabled": integration_enabled(int(tenant_id)),
        "outbound_enabled": outbound_enabled(int(tenant_id)),
        "kill_switch": global_kill_switch(),
        "status": state,
        "connected": state == "authorized",
        "last_error": (session_payload or {}).get("last_error") or cfg.get("session_last_error"),
        "account": dict(account),
    }


__all__ = [
    "build_state_payload",
    "ensure_event_secret",
    "get_integration",
    "global_kill_switch",
    "global_outbound_disabled",
    "integration_enabled",
    "max_personal_worker_token",
    "max_personal_worker_url",
    "outbound_enabled",
    "update_integration",
]
