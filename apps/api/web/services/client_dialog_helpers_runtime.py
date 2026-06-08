from __future__ import annotations

import json
import pathlib
import re
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import quote_plus, urlparse

from fastapi import Request


_MAX_TECHNICAL_TITLE_RE = re.compile(r"^(max|max_personal)\s*:\s*(id\s*)?\d+$", re.IGNORECASE)


def parse_tg_slot_from_source(source: Any, *, slot_min: int, slot_max: int) -> int | None:
    text = str(source or "").strip().lower()
    if not text:
        return None
    patterns = [
        r"tg_slot[:=](\d+)",
        r"telegram[:_](\d+)",
        r"slot[:=](\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            slot = int(match.group(1))
        except Exception:
            continue
        if slot_min <= slot <= slot_max:
            return slot
    return None


def is_technical_max_title(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.strip()
    if not normalized:
        return False
    if normalized.isdigit():
        return True
    return bool(_MAX_TECHNICAL_TITLE_RE.match(normalized))


def tg_slot_tenant(tenant_id: int, slot: int, *, virtual_tenant_id_fn) -> int:
    return virtual_tenant_id_fn(tenant_id, slot)


def load_telegram_slot_profiles(
    tenant_id: int,
    *,
    common_module: Any,
    slot_min: int,
    slot_max: int,
    virtual_tenant_id_fn,
) -> list[dict[str, Any]]:
    cfg = common_module.read_tenant_config(int(tenant_id)) or {}
    telegram_cfg = cfg.get("telegram") if isinstance(cfg, Mapping) else {}
    if not isinstance(telegram_cfg, Mapping):
        telegram_cfg = {}
    slot_count_raw = telegram_cfg.get("slot_count")
    try:
        slot_count = int(slot_count_raw)
    except Exception:
        slot_count = 1
    slot_count = max(slot_min, min(slot_max, slot_count))
    slot_enabled_cfg = telegram_cfg.get("slot_enabled")
    enabled_map: dict[int, bool] = {}
    if isinstance(slot_enabled_cfg, Mapping):
        for slot in range(slot_min, slot_max + 1):
            raw = slot_enabled_cfg.get(str(slot), slot_enabled_cfg.get(slot))
            enabled_map[slot] = bool(raw is not False)
    else:
        for slot in range(slot_min, slot_max + 1):
            enabled_map[slot] = True

    profiles: list[dict[str, Any]] = []
    for slot in range(slot_min, slot_count + 1):
        if not enabled_map.get(slot, True):
            continue
        virtual_tenant = tg_slot_tenant(
            int(tenant_id),
            slot,
            virtual_tenant_id_fn=virtual_tenant_id_fn,
        )
        path = f"/status?{urllib.parse.urlencode({'tenant': virtual_tenant})}"
        code, body, _ = common_module.tg_http("GET", path, timeout=5.0)
        if code < 200 or code >= 300:
            continue
        try:
            payload = json.loads(body.decode("utf-8", errors="ignore"))
        except Exception:
            payload = {}
        if not isinstance(payload, Mapping) or not bool(payload.get("authorized")):
            continue
        account_title = str(payload.get("account_title") or "").strip()
        account_username = str(payload.get("account_username") or "").strip()
        account_phone = str(payload.get("account_phone") or "").strip()
        label = (
            account_title
            or (f"@{account_username}" if account_username else "")
            or (f"+{account_phone}" if account_phone else "")
        )
        if not label:
            label = f"Telegram #{slot}"
        profiles.append(
            {
                "slot": slot,
                "label": label,
                "username": account_username or None,
                "phone": account_phone or None,
            }
        )
    return profiles


def ts_iso(ts: int | None) -> str | None:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return None


def channel_reply_enabled(cfg: Mapping[str, Any], channel: str) -> bool:
    behavior = cfg.get("behavior") if isinstance(cfg, Mapping) else None
    behavior_map = behavior if isinstance(behavior, Mapping) else {}
    channel_norm = (channel or "").strip().lower()
    if channel_norm == "telegram":
        for key in (
            "telegram_reply_enabled",
            "telegram_smart_reply_enabled",
            "telegram_ai_enabled",
        ):
            if key in behavior_map:
                return bool(behavior_map.get(key))
        root_flag = behavior_map.get("telegram_reply_enabled")
        if root_flag is not None:
            return bool(root_flag)
        return True
    if channel_norm == "max":
        for key in ("max_reply_enabled", "max_smart_reply_enabled", "max_ai_enabled"):
            if key in behavior_map:
                return bool(behavior_map.get(key))
        root_flag = behavior_map.get("max_reply_enabled")
        if root_flag is not None:
            return bool(root_flag)
        return True
    if channel_norm == "avito":
        return True
    return True


def load_silence_status(
    tenant_id: int,
    lead_id: int,
    channel: str,
    *,
    common_module: Any,
    silence_key_fn,
    silence_meta_key_fn,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "active": False,
        "reason": None,
        "since": None,
        "ttl_seconds": None,
        "auto_reply_enabled": True,
    }
    try:
        cfg = common_module.read_tenant_config(tenant_id)
    except Exception:
        cfg = {}
    result["auto_reply_enabled"] = channel_reply_enabled(cfg, channel)
    redis_client = common_module.redis_client()
    silence_key = silence_key_fn(int(tenant_id), int(lead_id))
    if not silence_key:
        return result
    try:
        raw_ts = redis_client.get(silence_key)
    except Exception:
        raw_ts = None
    if not raw_ts:
        return result
    result["active"] = True
    try:
        ts_val = int(raw_ts)
    except Exception:
        ts_val = None
    result["since"] = ts_iso(ts_val)
    try:
        ttl = redis_client.ttl(silence_key)
    except Exception:
        ttl = None
    if isinstance(ttl, int) and ttl >= 0:
        result["ttl_seconds"] = ttl
    meta_key = silence_meta_key_fn(int(tenant_id), int(lead_id))
    try:
        meta_raw = redis_client.get(meta_key) if meta_key else None
    except Exception:
        meta_raw = None
    if isinstance(meta_raw, str) and meta_raw.strip():
        try:
            payload = json.loads(meta_raw)
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            reason = payload.get("reason")
            if isinstance(reason, str) and reason.strip():
                result["reason"] = reason.strip()
    if not result.get("reason"):
        result["reason"] = "silence_active"
    return result


def tenant_root(tenant: int, *, common_module: Any) -> pathlib.Path:
    return pathlib.Path(common_module.tenant_dir(tenant))


def photo_manifest_path(tenant: int, *, common_module: Any) -> pathlib.Path:
    return tenant_root(tenant, common_module=common_module) / "uploads" / "photos" / "manifest.json"


def read_photo_manifest(tenant: int, *, common_module: Any) -> list[dict[str, Any]]:
    path = photo_manifest_path(tenant, common_module=common_module)
    if not path.exists() or not path.is_file():
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception:
        return []
    if isinstance(raw, list):
        return [entry for entry in raw if isinstance(entry, dict)]
    return []


def photo_public_url(
    request: Request,
    tenant_id: int,
    key: str,
    photo_id: str,
    *,
    common_module: Any,
) -> str:
    base = common_module.public_url(request, f"/pub/files/photos/{photo_id}")
    if not base:
        return ""
    joiner = "&" if "?" in base else "?"
    return f"{base}{joiner}tenant={tenant_id}&k={quote_plus(key)}"


def normalize_message_attachments(
    request: Request,
    tenant_id: int,
    key: str,
    attachments: Any,
    *,
    common_module: Any,
) -> list[dict[str, Any]]:
    raw = attachments
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = None
    if isinstance(raw, Mapping):
        raw = [dict(raw)]
    if not isinstance(raw, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        entry = dict(item)
        url = entry.get("url")
        photo_id = str(entry.get("photo_id") or "").strip()
        if not url and photo_id:
            entry["url"] = photo_public_url(
                request,
                tenant_id,
                key,
                photo_id,
                common_module=common_module,
            )
        elif isinstance(url, str) and url.startswith("telegram://"):
            parts = url.replace("telegram://", "").split("/")
            if len(parts) >= 3:
                peer_id = parts[1]
                message_id = parts[2]
                entry["url"] = common_module.public_url(
                    request,
                    f"/pub/tg/media/{peer_id}/{message_id}?tenant={tenant_id}&k={quote_plus(key)}",
                )
        elif isinstance(url, str):
            parsed = urlparse(url)
            if parsed.netloc in {"app:8000", "app"}:
                rebuilt = parsed.path or ""
                if parsed.query:
                    rebuilt = f"{rebuilt}?{parsed.query}"
                entry["url"] = common_module.public_url(request, rebuilt)
        if not entry.get("url"):
            peer_id = entry.get("peer_id")
            message_id = entry.get("message_id")
            if peer_id and message_id:
                entry["url"] = common_module.public_url(
                    request,
                    f"/pub/tg/media/{peer_id}/{message_id}?tenant={tenant_id}&k={quote_plus(key)}",
                )
        normalized.append(entry)
    return normalized
