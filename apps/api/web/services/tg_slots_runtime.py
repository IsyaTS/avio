from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class TgSlotsDeps:
    slot_min: int
    slot_max: int
    normalize_slot_fn: Any


def tg_slots_config(cfg: Mapping[str, Any] | None, deps: TgSlotsDeps) -> dict[str, Any]:
    telegram_cfg = cfg.get("telegram") if isinstance(cfg, Mapping) else None
    if not isinstance(telegram_cfg, Mapping):
        telegram_cfg = {}
    multi_mode = _bool_value(telegram_cfg.get("multi_slot_enabled"), default=True)
    slot_enabled = _slot_enabled_config(telegram_cfg.get("slot_enabled"), deps)
    slot_count = deps.normalize_slot_fn(telegram_cfg.get("slot_count"))
    return {
        "multi_mode": bool(multi_mode),
        "slot_enabled": slot_enabled,
        "slot_count": int(slot_count),
        "slot_min": deps.slot_min,
        "slot_max": deps.slot_max,
    }


def apply_tg_slots_payload(
    cfg: dict[str, Any],
    payload: Any,
    deps: TgSlotsDeps,
) -> dict[str, Any]:
    telegram_cfg = cfg.get("telegram")
    if not isinstance(telegram_cfg, dict):
        telegram_cfg = {}
        cfg["telegram"] = telegram_cfg
    if not isinstance(payload, Mapping):
        return cfg
    raw_multi = payload.get("multi_mode")
    if isinstance(raw_multi, (bool, str)):
        telegram_cfg["multi_slot_enabled"] = _bool_value(raw_multi, default=True)
    telegram_cfg["slot_count"] = deps.normalize_slot_fn(payload.get("slot_count"))
    telegram_cfg["slot_enabled"] = _slot_enabled_config(payload.get("slot_enabled"), deps)
    return cfg


def _slot_enabled_config(raw: Any, deps: TgSlotsDeps) -> dict[str, bool]:
    slot_enabled: dict[str, bool] = {}
    if isinstance(raw, Mapping):
        for slot in range(deps.slot_min, deps.slot_max + 1):
            raw_flag = raw.get(str(slot), raw.get(slot))
            slot_enabled[str(slot)] = _bool_value(raw_flag, default=True)
        return slot_enabled
    for slot in range(deps.slot_min, deps.slot_max + 1):
        slot_enabled[str(slot)] = True
    return slot_enabled


def _bool_value(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default
