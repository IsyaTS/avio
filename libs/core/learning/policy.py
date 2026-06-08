from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .state_snapshot import DialogueStateSnapshot


@dataclass(frozen=True)
class PolicyDecision:
    status: str
    mode: str
    reason: str
    similarity: float
    confidence: float
    recommended_action: str
    avoid_action: str
    style_hints: dict[str, Any]
    rule: Mapping[str, Any] | None

    @property
    def would_apply(self) -> bool:
        return self.status == "eligible"


def should_activate_candidate(
    candidate: Mapping[str, Any],
    *,
    min_evidence: int,
    min_distinct_leads: int,
    min_reward_delta: float,
    max_negative_evidence: int,
) -> bool:
    return (
        int(candidate.get("evidence_count") or 0) >= int(min_evidence)
        and int(candidate.get("distinct_leads_count") or 0) >= int(min_distinct_leads)
        and float(candidate.get("reward_delta") or 0.0) >= float(min_reward_delta)
        and int(candidate.get("negative_evidence") or 0) <= int(max_negative_evidence)
    )


def _coerce_payload(rule: Mapping[str, Any]) -> dict[str, Any]:
    payload = rule.get("fingerprint_payload") if isinstance(rule, Mapping) else {}
    if isinstance(payload, Mapping):
        return dict(payload)
    if isinstance(payload, str):
        raw = payload.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _similarity(snapshot: DialogueStateSnapshot, payload: Mapping[str, Any]) -> float:
    weights = {
        "intent": 0.24,
        "pending_fact_key": 0.16,
        "last_plan_action": 0.14,
        "known_fact_keys": 0.12,
        "has_price_context": 0.06,
        "has_shortlist": 0.06,
        "has_cta": 0.04,
        "has_handoff": 0.04,
        "frustration_signal": 0.05,
        "repair_signal": 0.03,
        "complaint_signal": 0.03,
        "after_catalog": 0.03,
    }
    snap = snapshot.fingerprint_payload
    score = 0.0
    total = 0.0
    for key, weight in weights.items():
        total += weight
        left = snap.get(key)
        right = payload.get(key)
        if key == "known_fact_keys":
            left_set = set(left or [])
            right_set = set(right or [])
            if not left_set and not right_set:
                score += weight
            elif left_set and right_set:
                score += weight * (len(left_set & right_set) / max(len(left_set | right_set), 1))
        elif left == right:
            score += weight
    return round(score / total, 4) if total else 0.0


def _has_hard_guard(snapshot: DialogueStateSnapshot, recommended_action: str) -> str:
    if snapshot.direct_question and snapshot.user_intent in {"price", "attributes", "variants", "selection"}:
        if recommended_action in {"ask_missing_fact", "ask_clarifying_question", "handoff"} and snapshot.has_price_context:
            return "direct_question_guard"
    if snapshot.pending_fact_key and snapshot.user_intent in {"price", "variants", "attributes"}:
        if recommended_action == "ask_missing_fact":
            return "qualification_loop_guard"
    if recommended_action == "handoff" and not (snapshot.has_handoff or snapshot.complaint_signal):
        return "handoff_guard"
    return ""


def select_runtime_policy(
    *,
    snapshot: DialogueStateSnapshot,
    rules: Sequence[Mapping[str, Any]],
    settings: Mapping[str, Any],
) -> PolicyDecision:
    if not rules:
        return PolicyDecision(
            status="no_rule",
            mode="shadow" if settings.get("shadow_mode") else "apply",
            reason="no_active_rules",
            similarity=0.0,
            confidence=0.0,
            recommended_action="",
            avoid_action="",
            style_hints={},
            rule=None,
        )

    best_rule: Mapping[str, Any] | None = None
    best_similarity = 0.0
    best_confidence = 0.0
    for rule in rules:
        payload = _coerce_payload(rule)
        similarity = _similarity(snapshot, payload)
        try:
            confidence = float(rule.get("confidence") or 0.0)
        except Exception:
            confidence = 0.0
        if similarity > best_similarity or (similarity == best_similarity and confidence > best_confidence):
            best_rule = rule
            best_similarity = similarity
            best_confidence = confidence

    if not best_rule:
        return PolicyDecision(
            status="no_rule",
            mode="shadow" if settings.get("shadow_mode") else "apply",
            reason="no_match",
            similarity=0.0,
            confidence=0.0,
            recommended_action="",
            avoid_action="",
            style_hints={},
            rule=None,
        )

    recommended_action = str(best_rule.get("recommended_action") or "").strip().lower()
    avoid_action = str(best_rule.get("avoid_action") or "").strip().lower()
    style_hints = dict(best_rule.get("style_hints") or {}) if isinstance(best_rule.get("style_hints"), Mapping) else {}
    mode = "apply" if settings.get("apply_mode") else "shadow"

    if best_similarity < float(settings.get("min_similarity") or 0.0):
        return PolicyDecision("skipped", mode, "low_similarity", best_similarity, best_confidence, recommended_action, avoid_action, style_hints, best_rule)
    if best_confidence < float(settings.get("min_confidence") or 0.0):
        return PolicyDecision("skipped", mode, "low_confidence", best_similarity, best_confidence, recommended_action, avoid_action, style_hints, best_rule)
    guard_reason = _has_hard_guard(snapshot, recommended_action)
    if guard_reason:
        return PolicyDecision("skipped", mode, guard_reason, best_similarity, best_confidence, recommended_action, avoid_action, style_hints, best_rule)
    return PolicyDecision("eligible", mode, "matched", best_similarity, best_confidence, recommended_action, avoid_action, style_hints, best_rule)


def format_policy_hint(decision: PolicyDecision) -> str:
    if not decision.would_apply:
        return ""
    style = decision.style_hints or {}
    style_blob = ", ".join(f"{key}={value}" for key, value in sorted(style.items()) if value not in (None, "", False))
    lines = [
        "Intervention policy hint for this tenant:",
        f"- recommended_action: {decision.recommended_action}",
    ]
    if decision.avoid_action:
        lines.append(f"- avoid_action: {decision.avoid_action}")
    if style_blob:
        lines.append(f"- style_hints: {style_blob}")
    lines.append(
        "- This hint is advisory only. Answer the user's current question first, do not invent facts, and do not repeat qualification if the question is already answerable."
    )
    return "\n".join(lines)
