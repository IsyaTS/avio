from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


def _as_bool(value: Any) -> bool:
    return bool(value)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _sanitize_string_list(value: Any) -> list[str]:
    result: list[str] = []
    if isinstance(value, (list, tuple, set)):
        for item in value:
            if isinstance(item, str) and item.strip():
                result.append(item.strip())
    elif isinstance(value, str) and value.strip():
        result.extend(item.strip() for item in value.split(",") if item.strip())
    return result


def sanitize_behavior_triggers(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    result: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        phrases_raw = item.get("phrases") or item.get("keywords") or []
        phrases = _sanitize_string_list(phrases_raw)
        if not phrases:
            continue
        channels_raw = item.get("channels") or [
            "telegram",
            "avito",
            "whatsapp",
            "max",
            "max_personal",
        ]
        channels = _sanitize_string_list(channels_raw)
        if not channels:
            channels = ["telegram", "avito", "whatsapp", "max", "max_personal"]
        result.append(
            {
                "phrases": phrases,
                "channels": [channel.lower() for channel in channels],
                "silence": bool(item.get("silence", True)),
                "notify": bool(item.get("notify", False)),
            }
        )
    return result


def merge_behavior_settings(
    existing_behavior: Mapping[str, Any] | None,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    behavior = deepcopy(dict(existing_behavior or {}))
    incoming = payload or {}

    if "auto_reply" in incoming:
        behavior["auto_reply"] = _as_bool(incoming.get("auto_reply"))
        behavior["auto_reply_enabled"] = behavior["auto_reply"]
    elif "auto_reply_enabled" in incoming:
        behavior["auto_reply_enabled"] = _as_bool(incoming.get("auto_reply_enabled"))
        behavior["auto_reply"] = behavior["auto_reply_enabled"]

    if "auto_reply_text" in incoming:
        behavior["auto_reply_text"] = _as_text(incoming.get("auto_reply_text"))
    if "avito_phone_tg_template" in incoming:
        behavior["avito_phone_tg_template"] = _as_text(incoming.get("avito_phone_tg_template"))
    if "avito_smart_reply_enabled" in incoming:
        behavior["avito_smart_reply_enabled"] = _as_bool(
            incoming.get("avito_smart_reply_enabled")
        )

    if "brain_mode" in incoming or "brain_mode" not in behavior:
        requested_brain_mode = str(incoming.get("brain_mode") or "").strip().lower()
        if requested_brain_mode not in {"smart", "classic"}:
            requested_brain_mode = str(behavior.get("brain_mode") or "").strip().lower()
        if requested_brain_mode not in {"smart", "classic"}:
            requested_brain_mode = "classic"
        behavior["brain_mode"] = requested_brain_mode
        behavior["human_reply_mode"] = requested_brain_mode == "classic"

    for key in (
        "max_reply_enabled",
        "telegram_reply_enabled",
        "send_catalog_on_first_message",
        "send_catalog_on_first_message_max",
        "auto_photo_enabled",
        "asset_actions_enabled",
    ):
        if key in incoming:
            behavior[key] = _as_bool(incoming.get(key))

    if "auto_photo_max" in incoming:
        try:
            auto_photo_max = int(incoming.get("auto_photo_max") or 0)
        except Exception:
            auto_photo_max = 0
        behavior["auto_photo_max"] = auto_photo_max if auto_photo_max >= 0 else 0

    if "asset_actions_max_per_reply" in incoming:
        try:
            max_assets = int(incoming.get("asset_actions_max_per_reply") or 0)
        except Exception:
            max_assets = 0
        behavior["asset_actions_max_per_reply"] = max_assets if max_assets >= 0 else 0

    if "triggers" in incoming:
        raw_triggers = incoming.get("triggers")
        behavior["triggers"] = sanitize_behavior_triggers(raw_triggers)

    if "photo_expected_markers" in incoming:
        behavior["photo_expected_markers"] = _sanitize_string_list(
            incoming.get("photo_expected_markers")
        )
    if "photo_expected_reply" in incoming:
        behavior["photo_expected_reply"] = _as_text(incoming.get("photo_expected_reply"))
    if "photo_expected_ttl" in incoming:
        try:
            ttl_val = int(incoming.get("photo_expected_ttl") or 0)
        except Exception:
            ttl_val = 0
        behavior["photo_expected_ttl"] = ttl_val if ttl_val > 0 else 0

    return behavior


__all__ = ["merge_behavior_settings", "sanitize_behavior_triggers"]
