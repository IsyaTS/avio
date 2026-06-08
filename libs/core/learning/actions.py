from __future__ import annotations

import re
from typing import Any, Mapping

_PRICE_RE = re.compile(r"(?:\d+[\d\s]{2,})(?:\s*(?:₽|руб|руб\.|k|тыс))?", re.IGNORECASE)
_RANGE_RE = re.compile(r"(?:от\s+\d|до\s+\d|\d+\s*[-–]\s*\d+)", re.IGNORECASE)
_COMPARE_RE = re.compile(r"(?:чем отличается|сравн|разница|или)", re.IGNORECASE)
_ATTRIBUTE_RE = re.compile(r"(?:ширин|высот|цвет|материал|толщин|размер|характеристик|параметр)", re.IGNORECASE)
_OBJECTION_RE = re.compile(r"(?:дорог|долго|неудоб|сомнева|не уверен|не подходит|слишком)", re.IGNORECASE)
_REPAIR_RE = re.compile(r"(?:не так|ошиб|имел в виду|не это|не тот|исправ)", re.IGNORECASE)
_HANDOFF_RE = re.compile(r"(?:менеджер|специалист|оператор|передам коллеге|подключу человека|переключу на человека)", re.IGNORECASE)
_CTA_RE = re.compile(r"(?:запиш|созвон|перезвон|оставьте|пришлите номер|оформим|удобно сегодня|замер|встреч)", re.IGNORECASE)
_SHORTLIST_RE = re.compile(r"(?:вариант|подбор|модел|несколько|подобрал|могу показать|shortlist)", re.IGNORECASE)
_DIRECT_QUESTION_RE = re.compile(r"\?")
_CLARIFY_RE = re.compile(r"(?:уточн|подскажите|какой|какая|какие|какого|куда|когда|сколько)", re.IGNORECASE)
_FACT_PROMPTS = {
    "city": re.compile(r"(?:город|куда|в каком городе)", re.IGNORECASE),
    "budget": re.compile(r"(?:бюджет|до какой суммы|сколько планируете)", re.IGNORECASE),
    "model": re.compile(r"(?:какая модель|какой вариант|какую модель)", re.IGNORECASE),
    "timeline": re.compile(r"(?:когда|срок|сроки)", re.IGNORECASE),
    "contact": re.compile(r"(?:номер|телефон|контакт)", re.IGNORECASE),
}


def _sentence_count(text: str) -> int:
    parts = [chunk.strip() for chunk in re.split(r"[.!?]+", text) if chunk.strip()]
    return len(parts)


def _style_hints(text: str) -> dict[str, Any]:
    chars = len(text)
    question_count = text.count("?")
    sentence_count = max(1, _sentence_count(text)) if text else 0
    if chars <= 120:
        answer_length = "short"
    elif chars <= 320:
        answer_length = "medium"
    else:
        answer_length = "long"
    directness = "direct" if question_count <= 1 and sentence_count <= 3 else "guided"
    cta_density = "high" if _CTA_RE.search(text) else "low"
    empathy = bool(re.search(r"(?:понима|сожале|неприятно|постараюсь|давайте разберемся)", text, re.IGNORECASE))
    return {
        "answer_length": answer_length,
        "directness": directness,
        "cta_density": cta_density,
        "empathy": empathy,
        "one_question_max": question_count <= 1,
        "question_count": question_count,
    }


def classify_action(
    text: str,
    *,
    last_plan: Mapping[str, Any] | None = None,
    pending_fact_key: str = "",
    source_role: str = "assistant",
) -> dict[str, Any]:
    raw = str(text or "").strip()
    low = raw.lower()
    style_hints = _style_hints(raw)
    plan_action = str((last_plan or {}).get("action") or "").strip().lower()
    plan_intent = str((last_plan or {}).get("intent") or "").strip().lower()

    action = "answer_direct"
    confidence = 0.55

    if not raw:
        return {"action": "answer_direct", "confidence": 0.0, "style_hints": style_hints}
    if "handoff" in plan_action or "handoff" in plan_intent or _HANDOFF_RE.search(raw):
        action = "handoff"
        confidence = 0.9
    elif _REPAIR_RE.search(raw):
        action = "repair_context"
        confidence = 0.84
    elif _OBJECTION_RE.search(low) and style_hints.get("empathy"):
        action = "handle_objection"
        confidence = 0.82
    elif _CTA_RE.search(raw):
        action = "schedule_cta"
        confidence = 0.8
    elif _COMPARE_RE.search(raw):
        action = "compare_options"
        confidence = 0.8
    elif _RANGE_RE.search(raw) and _PRICE_RE.search(raw):
        action = "give_price_range"
        confidence = 0.86
    elif _PRICE_RE.search(raw):
        action = "give_price"
        confidence = 0.84
    elif _SHORTLIST_RE.search(raw):
        action = "offer_shortlist"
        confidence = 0.76
    elif _ATTRIBUTE_RE.search(raw):
        action = "answer_attributes"
        confidence = 0.75
    elif pending_fact_key and _DIRECT_QUESTION_RE.search(raw):
        action = "ask_missing_fact"
        confidence = 0.72
    else:
        for fact_key, pattern in _FACT_PROMPTS.items():
            if fact_key == pending_fact_key and pattern.search(raw):
                action = "ask_missing_fact"
                confidence = 0.78
                break
        else:
            if _DIRECT_QUESTION_RE.search(raw) or _CLARIFY_RE.search(raw):
                action = "ask_clarifying_question"
                confidence = 0.68

    if source_role == "manager" and action == "ask_missing_fact" and not pending_fact_key:
        action = "ask_clarifying_question"
        confidence = min(confidence, 0.7)

    return {
        "action": action,
        "confidence": confidence,
        "style_hints": style_hints,
    }
