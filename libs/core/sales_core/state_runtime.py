from __future__ import annotations

import time
from typing import Any, Callable

from .models import SalesState


def remember_question_state(
    state: SalesState,
    question: str,
    *,
    question_fingerprint_fn: Callable[[str], str],
) -> None:
    clean = (question or "").strip()
    if not clean:
        return
    if clean not in state.asked_questions:
        state.asked_questions.append(clean)
        if len(state.asked_questions) > 24:
            state.asked_questions = state.asked_questions[-24:]
    fingerprint = question_fingerprint_fn(clean)
    if fingerprint:
        if fingerprint not in (state.asked_question_fingerprints or []):
            state.asked_question_fingerprints.append(fingerprint)
            if len(state.asked_question_fingerprints) > 32:
                state.asked_question_fingerprints = state.asked_question_fingerprints[-32:]
    state.last_question_text = clean


def remember_cta_state(state: SalesState, cta_text: str) -> None:
    clean = (cta_text or "").strip()
    if not clean:
        return
    state.cta_last_text = clean
    state.cta_last_sent_ts = time.time()


def cta_allowed(
    state: SalesState,
    channel_name: str | None,
    *,
    cta_cooldown_seconds: float,
) -> bool:
    if not isinstance(state, SalesState):
        return True
    if (state.user_message_count or 0) <= 1:
        return False
    if state.sentiment_score <= -1.2:
        return False
    now_ts = time.time()
    if state.cta_last_sent_ts and (now_ts - state.cta_last_sent_ts) < cta_cooldown_seconds:
        return False
    if channel_name and channel_name.lower() == "avito":
        return True
    return True


def max_questions_limit(persona_hints: Any, default: int = 1) -> int:
    if persona_hints and getattr(persona_hints, "max_questions", None) is not None:
        try:
            return max(0, int(getattr(persona_hints, "max_questions")))
        except Exception:
            return max(0, default)
    return max(0, default)


def apply_plan_alignment_to_state(
    state: SalesState,
    context: Any,
    previous_fingerprints: set[str],
    *,
    remember_question_fn: Callable[[SalesState, str], None],
    remember_cta_fn: Callable[[SalesState, str], None],
) -> None:
    if not isinstance(state, SalesState):
        return
    new_fingerprints = set(context.asked_fingerprints or []) - set(previous_fingerprints or set())
    for fingerprint in new_fingerprints:
        question = context.fingerprint_map.get(fingerprint)
        if question:
            remember_question_fn(state, question)
    for question in context.applied_questions or []:
        remember_question_fn(state, question)
    if context.applied_cta:
        remember_cta_fn(state, context.applied_cta)


def make_enforcement_context(
    state: SalesState,
    persona_hints: Any,
    channel_name: str,
    *,
    max_questions_fn: Callable[[Any], int],
    cta_allowed_fn: Callable[[SalesState, str], bool],
    enforcement_context_cls: type,
) -> Any:
    asked = set(state.asked_question_fingerprints or [])
    return enforcement_context_cls(
        channel=channel_name,
        max_questions=max_questions_fn(persona_hints),
        asked_fingerprints=set(asked),
        persona_cta=persona_hints.cta if persona_hints else "",
        allow_cta=cta_allowed_fn(state, channel_name),
        recent_cta=state.cta_last_text,
        recent_cta_ts=state.cta_last_sent_ts,
    )
