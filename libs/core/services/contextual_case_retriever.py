from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Sequence

from libs.core.repo import contextual_cases as contextual_repo
from libs.core.services.contextual_case_applicability import filter_applicable_cases
from libs.core.services.contextual_runtime_extractor import build_current_context


TokenizeFn = Callable[[str], set[str]]


@dataclass(frozen=True)
class ContextualRetrievalResult:
    current_context: dict[str, Any]
    applicable_cases: list[dict[str, Any]] = field(default_factory=list)
    clarification_cases: list[dict[str, Any]] = field(default_factory=list)
    blocked_count: int = 0
    retrieval_stats: dict[str, Any] = field(default_factory=dict)


async def retrieve_contextual_cases(
    *,
    tenant_id: int,
    user_text: str,
    history: Sequence[Mapping[str, Any]] | None = None,
    sales_state: Any = None,
    k: int = 3,
    min_score: float = 0.62,
    repo_module: Any = contextual_repo,
) -> ContextualRetrievalResult:
    domain_schema = await repo_module.get_active_domain_schema(int(tenant_id))
    current_context = build_current_context(
        tenant_id=int(tenant_id),
        user_text=user_text,
        history=history,
        channel="avito",
        sales_state=sales_state,
        domain_schema=domain_schema,
    )
    cases = await repo_module.list_active_cases_for_retrieval(int(tenant_id), limit=500, require_embedding=False)
    scored = _score_cases(cases, user_text=user_text, current_context=current_context, min_score=float(min_score))
    top_cases = [case for _score, case in scored[: max(1, int(k or 3)) * 4]]
    applicable, clarification, blocked = filter_applicable_cases(top_cases, current_context)
    applicable_cases = [item.safe_prompt_case for item in applicable if item.safe_prompt_case][: max(1, int(k or 3))]
    clarification_cases = [
        {"missing_requires": item.missing_requires, "clarification_hint": item.clarification_hint}
        for item in clarification[:3]
    ]
    ids = [int(case.get("id")) for _score, case in scored[: max(1, int(k or 3))] if str(case.get("id") or "").isdigit()]
    if ids:
        try:
            await repo_module.increment_contextual_case_usage(ids)
        except Exception:
            pass
    return ContextualRetrievalResult(
        current_context=current_context,
        applicable_cases=applicable_cases,
        clarification_cases=clarification_cases,
        blocked_count=blocked,
        retrieval_stats={
            "candidate_count": len(cases),
            "scored_count": len(scored),
            "applicable_count": len(applicable_cases),
            "clarification_count": len(clarification_cases),
            "blocked_count": blocked,
        },
    )


def _score_cases(
    cases: Sequence[Mapping[str, Any]],
    *,
    user_text: str,
    current_context: Mapping[str, Any],
    min_score: float,
) -> list[tuple[float, dict[str, Any]]]:
    query_tokens = _tokens(user_text)
    current_domain = str(current_context.get("domain") or "")
    current_intent = str(current_context.get("intent") or "")
    current_slots = current_context.get("slots") if isinstance(current_context.get("slots"), Mapping) else {}
    scored: list[tuple[float, dict[str, Any]]] = []
    for raw in cases:
        case = dict(raw)
        if not case.get("is_active", True):
            continue
        mode = str(case.get("mode") or "")
        if mode in {"review", "reject"}:
            continue
        search_text = str(case.get("search_text") or "")
        score = _jaccard(query_tokens, _tokens(search_text))
        if current_domain and str(case.get("domain") or "") == current_domain:
            score += 0.18
        if current_intent and str(case.get("intent") or "") == current_intent:
            score += 0.18
        case_context = case.get("context") if isinstance(case.get("context"), Mapping) else {}
        case_slots = case_context.get("slots") if isinstance(case_context.get("slots"), Mapping) else {}
        overlap = set(current_slots.keys()) & set(case_slots.keys())
        score += min(0.18, 0.06 * len(overlap))
        requires = case.get("applicability") if isinstance(case.get("applicability"), Mapping) else {}
        missing = [
            item
            for item in requires.get("requires") or []
            if isinstance(item, str) and item.startswith("slots.") and item.split(".", 1)[1] not in current_slots
        ]
        if missing:
            score -= min(0.2, 0.05 * len(missing))
        if score >= float(min_score):
            scored.append((score, case))
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored


def _tokens(text: str) -> set[str]:
    return {item for item in re.findall(r"[A-Za-zА-Яа-я0-9]{3,}", str(text or "").lower()) if item}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, len(left | right))
