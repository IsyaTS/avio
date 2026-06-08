from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class AssetRuleMatchResult:
    matched_actions: list[dict[str, Any]] = field(default_factory=list)
    blocked_actions: list[dict[str, Any]] = field(default_factory=list)
    missing_slots: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


def match_asset_rules(
    rules: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any],
    *,
    max_matches: int = 1,
) -> AssetRuleMatchResult:
    slots = context.get("slots") if isinstance(context.get("slots"), Mapping) else {}
    channel = str(context.get("channel") or "").strip().lower()
    matched: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    missing: list[str] = []
    checked = 0
    for rule in rules:
        checked += 1
        decision = _match_single_rule(rule, slots, channel)
        if decision["status"] == "matched":
            matched.append({"rule": dict(rule), "action": decision["action"]})
            if len(matched) >= max_matches:
                break
        else:
            blocked.append({"rule_id": rule.get("rule_id"), "reason": decision["reason"]})
            for slot in decision.get("missing_slots", []):
                if slot not in missing:
                    missing.append(slot)
    return AssetRuleMatchResult(
        matched_actions=matched,
        blocked_actions=blocked,
        missing_slots=missing,
        stats={"rules_checked": checked, "rules_matched": len(matched), "rules_blocked": len(blocked)},
    )


def _match_single_rule(rule: Mapping[str, Any], slots: Mapping[str, Any], channel: str) -> dict[str, Any]:
    if str(rule.get("status") or "").lower() != "active" or bool(rule.get("needs_review")):
        return {"status": "blocked", "reason": "inactive_or_needs_review"}
    guards = rule.get("guards") if isinstance(rule.get("guards"), Mapping) else {}
    allowed_channels = guards.get("allowed_channels") if isinstance(guards.get("allowed_channels"), list) else []
    if allowed_channels and channel not in {str(item).strip().lower() for item in allowed_channels}:
        return {"status": "blocked", "reason": "channel_not_allowed"}
    required = [str(item).strip() for item in (guards.get("requires_known_slots") or []) if str(item).strip()]
    missing = [slot for slot in required if not _slot_value(slots, slot)]
    if missing:
        return {"status": "blocked", "reason": "missing_required_slots", "missing_slots": missing}
    conditions = rule.get("conditions") if isinstance(rule.get("conditions"), Mapping) else {}
    for item in conditions.get("all") or []:
        if not isinstance(item, Mapping):
            continue
        if not _condition_matches(item, slots):
            return {"status": "blocked", "reason": "condition_not_matched"}
    action = rule.get("action") if isinstance(rule.get("action"), Mapping) else {}
    return {"status": "matched", "action": dict(action)}


def _condition_matches(condition: Mapping[str, Any], slots: Mapping[str, Any]) -> bool:
    slot = str(condition.get("slot") or "").strip()
    operator = str(condition.get("operator") or "equals").strip().lower()
    expected_raw = condition.get("value")
    actual = _slot_value(slots, slot).lower().replace("ё", "е")
    if not slot or not actual:
        return False
    if operator in {"in", "not_in"}:
        expected_values = {_normalize_expected(item) for item in _as_list(expected_raw)}
        if not expected_values:
            return False
        matched = actual in expected_values
        return matched if operator == "in" else not matched
    expected = _normalize_expected(expected_raw)
    if not expected:
        return False
    if operator == "contains":
        return expected in actual or actual in expected
    return expected == actual


def _slot_value(slots: Mapping[str, Any], slot: str) -> str:
    cleaned = slot.replace("slots.", "").strip()
    return str(slots.get(cleaned) or "").strip()


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _normalize_expected(value: Any) -> str:
    return str(value or "").strip().lower().replace("ё", "е")
