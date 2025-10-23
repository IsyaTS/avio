from __future__ import annotations

import re
from typing import Optional, Protocol

from .planner import GeneratedPlan
class PersonaHintsProtocol(Protocol):
    cta: str

    def wants_friendly(self) -> bool:
        ...


def _normalize(text: str) -> str:
    return (text or "").strip()


def enforce_plan_alignment(
    reply: str,
    plan: GeneratedPlan,
    persona_hints: Optional[PersonaHintsProtocol] = None,
) -> str:
    """Ensure CTA and planned questions make it into the final reply."""

    text = _normalize(reply)
    if not text:
        return text

    existing_lower = text.lower()

    plan.cta = ""

    appended_parts: list[str] = []

    question_to_add: Optional[str] = None
    for question in plan.next_questions:
        q_clean = question.strip()
        if not q_clean:
            continue
        if _question_present(q_clean, text):
            question_to_add = None
            break
        if question_to_add is None:
            question_to_add = q_clean

    if question_to_add:
        appended_parts.append(question_to_add)

    if appended_parts:
        text = text.rstrip() + "\n\n" + "\n".join(appended_parts)

    if persona_hints and persona_hints.wants_friendly():
        if not getattr(persona_hints, "no_emoji", False):
            if not re.search(r"[\)\]»☺😊😀😄😃😉😎❤️]", text):
                text = text + " \U0001F60A"

    return text


def _question_present(question: str, reply: str) -> bool:
    pattern = re.escape(question.strip())
    return bool(re.search(pattern, reply, flags=re.IGNORECASE))

