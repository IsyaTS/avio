from __future__ import annotations

import os
from typing import Any, Mapping


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _coerce_int(value: Any, default: int, *, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    parsed = max(min_value, parsed)
    parsed = min(max_value, parsed)
    return parsed


def _coerce_float(value: Any, default: float, *, min_value: float, max_value: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    parsed = max(min_value, parsed)
    parsed = min(max_value, parsed)
    return parsed


def intervention_learning_settings(cfg: Mapping[str, Any] | None) -> dict[str, Any]:
    learning = cfg.get("learning") if isinstance(cfg, Mapping) else {}
    if not isinstance(learning, Mapping):
        learning = {}
    policy = learning.get("intervention_policy") if isinstance(learning, Mapping) else {}
    if not isinstance(policy, Mapping):
        policy = {}

    env_disable = _coerce_bool(os.getenv("LEARNING_POLICY_V2_DISABLE"), False)
    env_force_shadow = _coerce_bool(os.getenv("LEARNING_POLICY_V2_FORCE_SHADOW"), False)

    enabled = _coerce_bool(policy.get("enabled"), _coerce_bool(learning.get("enabled"), False)) and not env_disable
    shadow_mode = _coerce_bool(policy.get("shadow_mode"), True)
    apply_mode = _coerce_bool(policy.get("apply_mode"), False) and not env_force_shadow
    if apply_mode:
        shadow_mode = True

    return {
        "enabled": enabled,
        "capture_enabled": _coerce_bool(policy.get("capture_enabled"), enabled),
        "runtime_enabled": _coerce_bool(policy.get("runtime_enabled"), enabled),
        "shadow_mode": shadow_mode,
        "apply_mode": apply_mode,
        "kill_switch": _coerce_bool(policy.get("kill_switch"), False) or env_disable,
        "stitch_window_seconds": _coerce_int(policy.get("stitch_window_seconds"), 45, min_value=5, max_value=300),
        "episode_history_limit": _coerce_int(policy.get("episode_history_limit"), 24, min_value=8, max_value=80),
        "runtime_history_limit": _coerce_int(policy.get("runtime_history_limit"), 12, min_value=4, max_value=40),
        "outcome_horizon_minutes": _coerce_int(policy.get("outcome_horizon_minutes"), 180, min_value=5, max_value=2880),
        "decision_window_minutes": _coerce_int(policy.get("decision_window_minutes"), 180, min_value=5, max_value=1440),
        "max_rules": _coerce_int(policy.get("max_rules"), 12, min_value=1, max_value=100),
        "min_similarity": _coerce_float(policy.get("min_similarity"), 0.64, min_value=0.1, max_value=1.0),
        "min_confidence": _coerce_float(policy.get("min_confidence"), 0.72, min_value=0.1, max_value=1.0),
        "min_evidence": _coerce_int(policy.get("min_evidence"), 3, min_value=1, max_value=1000),
        "min_distinct_leads": _coerce_int(policy.get("min_distinct_leads"), 2, min_value=1, max_value=1000),
        "min_reward_delta": _coerce_float(policy.get("min_reward_delta"), 0.15, min_value=-5.0, max_value=5.0),
        "max_negative_evidence": _coerce_int(policy.get("max_negative_evidence"), 2, min_value=0, max_value=1000),
        "max_question_repeat_penalty": _coerce_float(policy.get("max_question_repeat_penalty"), 0.4, min_value=0.0, max_value=5.0),
    }
