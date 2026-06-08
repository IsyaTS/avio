from __future__ import annotations

import re
from typing import Any, Callable, Mapping, MutableMapping


def install_quality_bindings(
    ctx: MutableMapping[str, Any],
    *,
    delegate_sync: Callable[[Callable[[], Any], str], Callable[..., Any]],
    llm_runtime_cls: type[Any],
    getenv: Callable[[str, str | None], str | None],
    openai_module: Any,
    stop_intent_re: Any,
    robotic_banned_patterns: Any,
    sentence_split_re: Any,
) -> None:
    humanize_runtime = lambda: ctx["_humanize_runtime"]()
    answer_quality_runtime = lambda: ctx["_answer_quality_runtime"]()
    instruction_runtime = lambda: ctx["_instruction_runtime"]()
    decision_runtime = lambda: ctx["_decision_runtime"]()
    fallback_runtime = lambda: ctx["_fallback_runtime"]()

    ctx["_apply_conversational_phrasing"] = delegate_sync(
        humanize_runtime,
        "apply_conversational_phrasing",
    )
    ctx["_recent_gratitude_count"] = delegate_sync(humanize_runtime, "recent_gratitude_count")
    ctx["_trim_redundant_gratitude_opening"] = delegate_sync(
        humanize_runtime,
        "trim_redundant_gratitude_opening",
    )
    ctx["_limit_questions"] = delegate_sync(answer_quality_runtime, "limit_questions")
    ctx["_enforce_exclamation_budget"] = delegate_sync(answer_quality_runtime, "enforce_exclamation_budget")
    ctx["_rotate_greeting"] = delegate_sync(humanize_runtime, "rotate_greeting")
    ctx["_has_address_fact"] = delegate_sync(humanize_runtime, "has_address_fact")
    ctx["_strip_unverified_local_claims"] = delegate_sync(humanize_runtime, "strip_unverified_local_claims")
    ctx["_normalize_entity_ack_opening"] = delegate_sync(humanize_runtime, "normalize_entity_ack_opening")
    ctx["_looks_like_contextual_short_followup"] = delegate_sync(
        humanize_runtime,
        "looks_like_contextual_short_followup",
    )
    ctx["_is_unsubscribe_intent"] = lambda text: bool(stop_intent_re.search(str(text or "")))

    def _is_quota_or_rate_limit_error(exc: Exception) -> bool:
        message = str(exc or "").lower()
        if "insufficient_quota" in message or "rate limit" in message:
            return True
        rate_limit_cls = getattr(openai_module, "RateLimitError", None)
        if rate_limit_cls and isinstance(exc, rate_limit_cls):
            return True
        return False

    ctx["_is_quota_or_rate_limit_error"] = _is_quota_or_rate_limit_error
    ctx["_unsubscribe_ack_text"] = lambda: ""
    ctx["_strip_instruction_leaks"] = delegate_sync(instruction_runtime, "strip_instruction_leaks")
    ctx["_safe_minimal_fallback_reply"] = delegate_sync(decision_runtime, "safe_minimal_fallback_reply")
    ctx["_llm_unavailable_reply"] = delegate_sync(fallback_runtime, "llm_unavailable_reply")

    def _apply_optional_lowercase_opening(
        text: str,
        state: Any,
        *,
        persona_hints: Any = None,
    ) -> str:
        return humanize_runtime().apply_optional_lowercase_opening(
            text,
            state,
            persona_hints=persona_hints,
            lowercase_opening_chance=float(getattr(ctx["settings"], "LOWERCASE_OPENING_CHANCE", 0.0) or 0.0),
        )

    ctx["_apply_optional_lowercase_opening"] = _apply_optional_lowercase_opening

    def _humanize_reply_text(
        reply: str,
        *,
        state: Any,
        persona_hints: Any = None,
    ) -> str:
        return humanize_runtime().humanize_reply_text(
            reply,
            state=state,
            persona_hints=persona_hints,
            lowercase_opening_chance=float(getattr(ctx["settings"], "LOWERCASE_OPENING_CHANCE", 0.0) or 0.0),
        )

    ctx["_humanize_reply_text"] = _humanize_reply_text
    ctx["_persona_requires_first_greeting"] = lambda persona_context: humanize_runtime().persona_requires_first_greeting(
        persona_context
    )

    def _ensure_dialog_greeting_on_first_reply(
        text: str,
        state: Any,
        persona_context: str = "",
    ) -> str:
        force_greeting = str(getattr(ctx["settings"], "FORCE_FIRST_GREETING", "0") or "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        return humanize_runtime().ensure_dialog_greeting_on_first_reply(
            text,
            state,
            persona_context=persona_context,
            force_first_greeting=force_greeting,
        )

    ctx["_ensure_dialog_greeting_on_first_reply"] = _ensure_dialog_greeting_on_first_reply

    def _answer_is_too_robotic(text: str) -> bool:
        candidate = (text or "").strip()
        if not candidate:
            return True
        for pattern in robotic_banned_patterns:
            if pattern.search(candidate):
                return True
        lowered = candidate.lower()
        if re.match(r"^\s*(понял|принял|здравствуйте)[,.! ]*$", lowered):
            return True
        return False

    ctx["_answer_is_too_robotic"] = _answer_is_too_robotic
    ctx["_count_sentences"] = delegate_sync(answer_quality_runtime, "count_sentences")
    ctx["_normalize_numbered_list_punctuation"] = delegate_sync(
        answer_quality_runtime,
        "normalize_numbered_list_punctuation",
    )

    def _enforce_sentence_budget(
        text: str,
        max_sentences: int = 4,
        max_chars: int = 420,
    ) -> str:
        return answer_quality_runtime().enforce_sentence_budget(
            text,
            max_sentences=max_sentences,
            max_chars=max_chars,
        )

    ctx["_enforce_sentence_budget"] = _enforce_sentence_budget
    ctx["_question_token_set"] = delegate_sync(answer_quality_runtime, "question_token_set")
    ctx["_extract_questions_from_text"] = delegate_sync(answer_quality_runtime, "extract_questions_from_text")
    ctx["_is_repeated_question_against_state"] = delegate_sync(
        answer_quality_runtime,
        "is_repeated_question_against_state",
    )
    ctx["_reply_has_repeated_question"] = delegate_sync(answer_quality_runtime, "reply_has_repeated_question")
    ctx["_drop_repeated_questions_from_reply"] = delegate_sync(
        answer_quality_runtime,
        "drop_repeated_questions_from_reply",
    )

    def _remember_questions_from_reply(state: Any, text: str) -> None:
        for question in ctx["_extract_questions_from_text"](text):
            ctx["_remember_question_state"](state, question)

    ctx["_remember_questions_from_reply"] = _remember_questions_from_reply

    def _render_passes_rubric(text: str, state: Any) -> bool:
        return answer_quality_runtime().render_passes_rubric(
            text,
            state,
            sentence_split_re=sentence_split_re,
        )

    ctx["_render_passes_rubric"] = _render_passes_rubric

    def _apply_base_answer_quality_floor(
        answer: str,
        *,
        state: Any,
        persona_hints: Any,
        grounding: Mapping[str, Any] | None,
        user_text: str,
    ) -> str:
        return answer_quality_runtime().apply_base_answer_quality_floor(
            answer,
            state=state,
            persona_hints=persona_hints,
            grounding=grounding,
            user_text=user_text,
        )

    ctx["_apply_base_answer_quality_floor"] = _apply_base_answer_quality_floor

    def _prefer_refined_answer(
        *,
        answer: str,
        refined: str,
        state: Any,
        persona_hints: Any,
        grounding: Mapping[str, Any] | None,
        user_text: str,
    ) -> str:
        return answer_quality_runtime().prefer_refined_answer(
            answer=answer,
            refined=refined,
            state=state,
            persona_hints=persona_hints,
            grounding=grounding,
            user_text=user_text,
            sentence_split_re=sentence_split_re,
        )

    ctx["_prefer_refined_answer"] = _prefer_refined_answer
    ctx["_safe_json_load"] = delegate_sync(fallback_runtime, "safe_json_load")
    ctx["_LLM_MIN_CALL_GAP_SECONDS"] = max(
        0.0,
        float(getenv("LLM_MIN_CALL_GAP_SECONDS", "0.25") or 0.25),
    )
    ctx["_LLM_RUNTIME"] = llm_runtime_cls(min_call_gap_seconds=ctx["_LLM_MIN_CALL_GAP_SECONDS"])

    async def _llm_rate_limit_gate() -> None:
        await ctx["_LLM_RUNTIME"].rate_limit_gate()

    ctx["_llm_rate_limit_gate"] = _llm_rate_limit_gate

    async def _llm_call_with_deadline(
        create_fn: Any,
        *,
        timeout_seconds: float,
        **kwargs: Any,
    ) -> Any:
        return await ctx["_LLM_RUNTIME"].call_with_deadline(
            create_fn,
            timeout_seconds=timeout_seconds,
            is_quota_or_rate_limit_error=ctx["_is_quota_or_rate_limit_error"],
            **kwargs,
        )

    ctx["_llm_call_with_deadline"] = _llm_call_with_deadline
