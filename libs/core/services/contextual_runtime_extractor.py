from __future__ import annotations

from typing import Any, Mapping, Sequence

from libs.core.services.avito_contextual_case_builder import AvitoContextualCaseCandidate, AvitoContextualMessage
from libs.core.services.avito_domain_context_extractor import extract_context


def build_current_context(
    *,
    tenant_id: int,
    user_text: str,
    history: Sequence[Mapping[str, Any]] | None = None,
    channel: str = "avito",
    sales_state: Any = None,
    domain_schema: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    messages: list[AvitoContextualMessage] = []
    for row in history or []:
        role = str(row.get("role") or "").strip().lower()
        if role == "user":
            mapped = "client"
        elif role == "assistant":
            mapped = "manager"
        elif role in {"client", "manager"}:
            mapped = role
        else:
            continue
        text = str(row.get("content") or row.get("text") or "").strip()
        if text:
            messages.append(AvitoContextualMessage(role=mapped, text=text))
    messages.append(AvitoContextualMessage(role="client", text=str(user_text or "")))
    candidate = AvitoContextualCaseCandidate(
        source="runtime",
        tenant_id=int(tenant_id),
        dialog_id="runtime",
        case_id="runtime",
        turn_index=len(messages),
        channel=str(channel or "avito"),
        history=messages,
        manager_reply=AvitoContextualMessage(role="manager", text=""),
        created_at="",
    )
    extracted = extract_context(candidate, domain_schema=domain_schema or {})
    context = dict(extracted.get("context") or {})
    slots = dict(context.get("slots") or {})
    facts = getattr(sales_state, "facts", None)
    if isinstance(facts, Mapping):
        for key, value in facts.items():
            if value and str(key) not in slots:
                slots[str(key)] = str(value)
    known = set(str(item) for item in context.get("known_slots") or [])
    known.update(slots.keys())
    context["slots"] = slots
    context["known_slots"] = sorted(known)
    return context
