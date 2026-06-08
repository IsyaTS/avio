from __future__ import annotations

from typing import Any, Mapping

from libs.core.repo import tenant_asset_rules

ALLOWED_ACTION_TYPES = {
    "send_asset",
    "send_catalog",
    "send_link",
    "ask_clarification",
    "handoff_to_manager",
    "noop",
}


def normalize_asset_rule(
    tenant_id: int,
    asset_id: str,
    metadata: Mapping[str, Any],
    *,
    source: str = "asset_title",
    priority: int = 0,
) -> dict[str, Any]:
    conditions = _mapping(metadata.get("conditions"))
    action = _mapping(metadata.get("action"))
    guards = _mapping(metadata.get("guards"))
    action_type = str(action.get("type") or "send_asset").strip()
    if action_type not in ALLOWED_ACTION_TYPES:
        action_type = "noop"
        guards["blocked_reason"] = "unsupported_action"
    action["type"] = action_type
    action.setdefault("asset_id", asset_id)
    confidence = _confidence(metadata.get("confidence"))
    needs_review = bool(metadata.get("needs_review")) or confidence < 0.75 or action_type == "noop"
    status = "needs_review" if needs_review else "active"
    rule_id = tenant_asset_rules.stable_rule_id(int(tenant_id), asset_id, source, conditions)
    return {
        "tenant_id": int(tenant_id),
        "rule_id": rule_id,
        "asset_id": asset_id,
        "source": source,
        "status": status,
        "priority": int(priority),
        "trigger": {
            "asset_intent": str(metadata.get("asset_intent") or "").strip(),
            "domain": str(metadata.get("domain") or "").strip(),
            "human_summary": str(metadata.get("human_summary") or "").strip(),
        },
        "conditions": conditions,
        "action": action,
        "guards": guards,
        "confidence": confidence,
        "needs_review": needs_review,
        "compiler_version": str(metadata.get("compiler_version") or "asset_rule_compiler_v1"),
    }


async def compile_and_store_asset_rule(
    tenant_id: int,
    asset_id: str,
    metadata: Mapping[str, Any],
    *,
    source: str = "asset_title",
    priority: int = 0,
) -> dict[str, Any] | None:
    rule = normalize_asset_rule(
        tenant_id,
        asset_id,
        metadata,
        source=source,
        priority=priority,
    )
    return await tenant_asset_rules.upsert_rule(
        int(tenant_id),
        str(rule["rule_id"]),
        asset_id=str(asset_id),
        source=str(rule["source"]),
        status=str(rule["status"]),
        priority=int(rule["priority"]),
        trigger=_mapping(rule.get("trigger")),
        conditions=_mapping(rule.get("conditions")),
        action=_mapping(rule.get("action")),
        guards=_mapping(rule.get("guards")),
        confidence=float(rule["confidence"]),
        needs_review=bool(rule["needs_review"]),
        compiler_version=str(rule["compiler_version"]),
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _confidence(value: Any) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = 0.0
    return max(0.0, min(1.0, parsed))
