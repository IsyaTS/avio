from __future__ import annotations

from typing import Any, Mapping

from libs.core.sales_core import read_tenant_config
from libs.core.services.contextual_case_retriever import ContextualRetrievalResult, retrieve_contextual_cases


DEFAULT_SETTINGS = {
    "enabled": False,
    "shadow_mode": True,
    "apply_mode": False,
    "top_k": 3,
    "min_score": 0.62,
    "max_prompt_chars": 3500,
}


async def build_contextual_cases_block_for_runtime(
    *,
    tenant_id: int,
    user_text: str,
    history: list[dict[str, str]] | None = None,
    contact_id: int = 0,
    channel: str = "avito",
    log_fn: Any = None,
) -> dict[str, Any]:
    settings = contextual_settings_for_tenant(int(tenant_id))
    if not settings.get("enabled"):
        return {"enabled": False, "applied": False, "block": "", "stats": {}}
    try:
        result = await retrieve_contextual_cases(
            tenant_id=int(tenant_id),
            user_text=user_text,
            history=history,
            k=int(settings.get("top_k") or 3),
            min_score=float(settings.get("min_score") or 0.62),
        )
    except Exception as exc:
        if log_fn:
            log_fn("event=contextual_cases_retrieval_failed tenant=%s channel=%s error=%s" % (tenant_id, channel, exc))
        return {"enabled": True, "applied": False, "block": "", "stats": {"error": "retrieval_failed"}}
    if settings.get("shadow_mode") and not settings.get("apply_mode"):
        if log_fn:
            stats = result.retrieval_stats
            log_fn(
                "event=contextual_cases_shadow tenant=%s channel=%s applicable=%s clarification=%s blocked=%s"
                % (
                    tenant_id,
                    channel,
                    stats.get("applicable_count", 0),
                    stats.get("clarification_count", 0),
                    stats.get("blocked_count", 0),
                )
            )
        return {"enabled": True, "applied": False, "block": "", "stats": result.retrieval_stats}
    block = build_contextual_cases_block(
        result,
        max_cases=int(settings.get("top_k") or 3),
        max_chars=int(settings.get("max_prompt_chars") or 3500),
    )
    return {"enabled": True, "applied": bool(block), "block": block, "stats": result.retrieval_stats}


def contextual_settings_for_tenant(tenant_id: int) -> dict[str, Any]:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = {}
    learning = cfg.get("learning") if isinstance(cfg, Mapping) else {}
    contextual = learning.get("contextual_cases") if isinstance(learning, Mapping) else {}
    result = dict(DEFAULT_SETTINGS)
    if isinstance(contextual, Mapping):
        result.update({key: contextual.get(key) for key in result if key in contextual})
    result["enabled"] = bool(result.get("enabled"))
    result["shadow_mode"] = bool(result.get("shadow_mode"))
    result["apply_mode"] = bool(result.get("apply_mode"))
    try:
        result["top_k"] = max(1, min(5, int(result.get("top_k") or 3)))
    except Exception:
        result["top_k"] = 3
    try:
        result["min_score"] = max(0.0, min(1.0, float(result.get("min_score") or 0.62)))
    except Exception:
        result["min_score"] = 0.62
    try:
        result["max_prompt_chars"] = max(800, min(6000, int(result.get("max_prompt_chars") or 3500)))
    except Exception:
        result["max_prompt_chars"] = 3500
    return result


def build_contextual_cases_block(
    retrieval_result: ContextualRetrievalResult,
    *,
    max_cases: int = 3,
    max_chars: int = 3500,
) -> str:
    lines: list[str] = [
        "Контекстные примеры менеджера.",
        "Используй только если условия совпадают. Не копируй факты из примера, если они не подтверждены текущим диалогом.",
        "Каталог, текущая персона, бизнес-правила клиента и текущие факты клиента важнее этих примеров.",
    ]
    ctx = retrieval_result.current_context
    known_slots = ctx.get("known_slots") or []
    missing_slots = ctx.get("missing_slots") or []
    lines.extend(
        [
            "",
            "Текущий контекст:",
            f"- intent: {ctx.get('intent') or 'unknown'}",
            f"- known slots: {', '.join(map(str, known_slots)) if known_slots else 'нет'}",
            f"- missing slots: {', '.join(map(str, missing_slots)) if missing_slots else 'нет'}",
        ]
    )
    cases = retrieval_result.applicable_cases[: max(1, int(max_cases))]
    if cases:
        lines.append("")
        lines.append("Подходящие примеры:")
    for idx, case in enumerate(cases, start=1):
        requires = ", ".join(case.get("requires") or []) or "нет"
        lines.append(f"{idx}. mode: {case.get('mode') or 'direct_example'}")
        lines.append(f"   requires satisfied: {requires}")
        for row in case.get("history") or []:
            role = "Клиент" if row.get("role") == "client" else "Менеджер"
            text = _compact(row.get("text"), 260)
            if text:
                lines.append(f"   {role}: {text}")
        reply = _compact(case.get("manager_reply"), 360)
        if reply:
            prefix = "   Стиль менеджера" if case.get("style_only") else "   Менеджер"
            lines.append(f"{prefix}: {reply}")
        lines.append("   Применение: адаптируй формулировку, не добавляй неподтверждённые факты.")
    if retrieval_result.clarification_cases:
        lines.append("")
        lines.append("Если данных не хватает:")
        for item in retrieval_result.clarification_cases[:2]:
            hint = _compact(item.get("clarification_hint"), 220)
            if hint:
                lines.append(f"- Сначала уточни: {hint}")
    block = "\n".join(lines).strip()
    if len(block) > int(max_chars):
        block = block[: int(max_chars)].rsplit("\n", 1)[0].strip()
    return block


def _compact(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"
