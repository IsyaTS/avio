from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class ContextualApplicabilityDecision:
    status: str
    missing_requires: list[str] = field(default_factory=list)
    safe_prompt_case: dict[str, Any] | None = None
    clarification_hint: str | None = None
    reason_code: str = ""


CONDITIONAL_FACTS = {"mentions_address", "mentions_price", "city_specific", "price_specific"}


def evaluate_case_applicability(
    case: Mapping[str, Any],
    current_context: Mapping[str, Any],
) -> ContextualApplicabilityDecision:
    mode = str(case.get("mode") or _nested(case, "applicability", "mode") or "").strip()
    if mode in {"review", "reject"}:
        return ContextualApplicabilityDecision(status="blocked", reason_code="review_or_reject")
    applicability = case.get("applicability") if isinstance(case.get("applicability"), Mapping) else {}
    reply_facts = case.get("reply_facts") if isinstance(case.get("reply_facts"), Mapping) else {}
    requires = _string_list(applicability.get("requires"))
    if mode == "clarify_first":
        missing = [item for item in requires if not _has_requirement(current_context, item)]
        return ContextualApplicabilityDecision(
            status="clarification_needed",
            missing_requires=missing,
            safe_prompt_case=_safe_case(case, include_reply=True),
            clarification_hint=_manager_reply_text(case) or _clarification_hint(missing),
            reason_code="clarify_first",
        )
    if not requires and any(bool(reply_facts.get(key)) for key in CONDITIONAL_FACTS):
        return ContextualApplicabilityDecision(status="blocked", reason_code="conditional_without_requires")
    missing = [item for item in requires if not _has_requirement(current_context, item)]
    if mode == "context_bound" and missing:
        return ContextualApplicabilityDecision(
            status="clarification_needed",
            missing_requires=missing,
            clarification_hint=_clarification_hint(missing),
            reason_code="missing_requires",
        )
    if mode == "style_only":
        return ContextualApplicabilityDecision(
            status="style_only",
            safe_prompt_case=_safe_case(case, include_reply=True, style_only=True),
            reason_code="style_only",
        )
    if missing:
        return ContextualApplicabilityDecision(
            status="clarification_needed",
            missing_requires=missing,
            clarification_hint=_clarification_hint(missing),
            reason_code="missing_requires",
        )
    return ContextualApplicabilityDecision(
        status="applicable",
        safe_prompt_case=_safe_case(case, include_reply=True),
        reason_code="applicable",
    )


def filter_applicable_cases(
    cases: Sequence[Mapping[str, Any]],
    current_context: Mapping[str, Any],
) -> tuple[list[ContextualApplicabilityDecision], list[ContextualApplicabilityDecision], int]:
    applicable: list[ContextualApplicabilityDecision] = []
    clarification: list[ContextualApplicabilityDecision] = []
    blocked = 0
    for case in cases:
        decision = evaluate_case_applicability(case, current_context)
        if decision.status in {"applicable", "style_only"}:
            applicable.append(decision)
        elif decision.status == "clarification_needed":
            clarification.append(decision)
        else:
            blocked += 1
    return applicable, clarification, blocked


def _has_requirement(context: Mapping[str, Any], requirement: str) -> bool:
    req = str(requirement or "").strip()
    if not req:
        return True
    slots = context.get("slots") if isinstance(context.get("slots"), Mapping) else {}
    known_slots = set(str(item) for item in context.get("known_slots") or [])
    if req.startswith("slots."):
        key = req.split(".", 1)[1]
        return bool(slots.get(key)) or key in known_slots
    legacy = context.get(req)
    return bool(legacy) or req in known_slots or bool(slots.get(req))


def _safe_case(case: Mapping[str, Any], *, include_reply: bool, style_only: bool = False) -> dict[str, Any]:
    dialog = case.get("dialog") if isinstance(case.get("dialog"), Mapping) else {}
    history = dialog.get("history") if isinstance(dialog.get("history"), list) else []
    safe_history = [
        {"role": str(row.get("role") or ""), "text": str(row.get("text") or "")}
        for row in history[-4:]
        if isinstance(row, Mapping) and str(row.get("role") or "") in {"client", "manager"} and str(row.get("text") or "").strip()
    ]
    item = {
        "mode": str(case.get("mode") or _nested(case, "applicability", "mode") or ""),
        "requires": _string_list(_nested(case, "applicability", "requires")),
        "history": safe_history,
        "style_only": bool(style_only),
    }
    if include_reply:
        item["manager_reply"] = _manager_reply_text(case)
    return item


def _manager_reply_text(case: Mapping[str, Any]) -> str:
    dialog = case.get("dialog") if isinstance(case.get("dialog"), Mapping) else {}
    reply = dialog.get("manager_reply") if isinstance(dialog.get("manager_reply"), Mapping) else {}
    return str(reply.get("text") or "").strip()


def _clarification_hint(missing: Sequence[str]) -> str:
    clean = [item.replace("slots.", "") for item in missing if str(item).strip()]
    if not clean:
        return "Уточни недостающие условия перед ответом."
    return "Уточни: " + ", ".join(clean[:4])


def _nested(mapping: Mapping[str, Any], *keys: str) -> Any:
    value: Any = mapping
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []
