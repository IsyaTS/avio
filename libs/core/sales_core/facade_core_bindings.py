from __future__ import annotations

import re
from typing import Any, Callable, Mapping, MutableMapping, Sequence


def install_core_delegate_bindings(
    ctx: MutableMapping[str, Any],
    *,
    bind_private: Callable[..., None],
    delegate_sync: Callable[[Callable[[], Any], str], Callable[..., Any]],
    delegate_async: Callable[[Callable[[], Any], str], Callable[..., Any]],
    load_persona_fn: Callable[[int, str], str],
    state_cls: type[Any],
    quality_module: Any,
    persona_hints_cache_key: Callable[..., Any],
    persona_hints_clear: Callable[..., None],
    persona_hints_load: Callable[..., Any],
    facade_apply_plan_alignment_to_state: Callable[..., None],
    facade_make_enforcement_context: Callable[..., Any],
    state_runtime_apply_plan_alignment: Callable[..., None],
    state_runtime_make_enforcement_context: Callable[..., Any],
    needs_global_color_aliases: Mapping[str, Any],
) -> None:
    fact_key_runtime = lambda: ctx["_fact_key_runtime"]()
    fact_question_runtime = lambda: ctx["_fact_question_runtime"]()
    fallback_runtime = lambda: ctx["_fallback_runtime"]()
    decision_runtime = lambda: ctx["_decision_runtime"]()
    persona_script_runtime = lambda: ctx["_persona_script_runtime"]()
    persona_runtime = lambda: ctx["_persona_runtime"]()
    persona_turn_runtime = lambda: ctx["_persona_turn_runtime"]()
    instruction_runtime = lambda: ctx["_instruction_runtime"]()
    dialog_state_runtime = lambda: ctx["_dialog_state_runtime"]()
    location_runtime = lambda: ctx["_location_runtime"]()
    facts_runtime = lambda: ctx["_facts_runtime"]()
    policy_plan_runtime = lambda: ctx["_policy_plan_runtime"]()
    catalog_match_runtime = lambda: ctx["_catalog_match_runtime"]()
    grounding_runtime = lambda: ctx["_grounding_runtime"]()
    catalog_price_runtime = lambda: ctx["_catalog_price_runtime"]()
    catalog_guard_runtime = lambda: ctx["_catalog_guard_runtime"]()
    attribute_runtime = lambda: ctx["_attribute_runtime"]()
    catalog_metrics_runtime = lambda: ctx["_catalog_metrics_runtime"]()
    intent_runtime = lambda: ctx["_intent_runtime"]()
    context_guard_runtime = lambda: ctx["_context_guard_runtime"]()
    semantic_runtime = lambda: ctx["_semantic_runtime"]()
    state_store_runtime = lambda: ctx["_state_store_runtime"]()

    bind_private(
        fact_key_runtime,
        "is_plausible_contact_phone",
        "safe_short_text",
        "normalize_fact_key",
        "canonical_fact_key",
    )
    bind_private(
        fact_question_runtime,
        "normalize_required_facts",
        "missing_required_facts",
        "prioritize_missing_facts",
        "question_covers_fact",
        "question_lists_catalog_options",
        "replace_reply_question",
        "generic_question_for_fact",
    )
    bind_private(fact_question_runtime, "persona_driven_question_for_fact")
    bind_private(fallback_runtime, "has_substantive_non_question_payload")
    bind_private(decision_runtime, "enforce_next_required_fact_question")
    bind_private(persona_script_runtime, "fact_keys_from_line", "persona_rules_cache_key")
    bind_private(
        persona_runtime,
        "required_facts_from_persona_text",
        "line_to_question",
        "infer_persona_template_condition_and_action",
    )
    ctx["_persona_safe_uncertain_reply"] = lambda _persona_context="": ""
    bind_private(persona_turn_runtime, "persona_direct_reply_for_user_turn")
    bind_private(
        instruction_runtime,
        "is_operator_like_question",
        "is_operator_instruction_sentence",
        "is_response_format_instruction_sentence",
        "is_sequence_process_instruction_sentence",
        "strip_embedded_operator_tail",
    )
    bind_private(
        persona_script_runtime,
        "extract_primary_script_lines",
        "persona_script_questions",
        "persona_primary_script_question",
        "persona_catalog_unavailable_reply",
    )
    ctx["_explain_missing_fact_need"] = (
        lambda fact_key, *, persona_context="": persona_script_runtime().explain_missing_fact_need(
            fact_key,
            canonical_fact_key=ctx["_canonical_fact_key"],
            persona_context=persona_context,
        )
    )
    bind_private(persona_turn_runtime, "fallback_contextual_question")
    bind_private(
        persona_runtime,
        "extract_expected_tokens_from_condition",
        "extract_contact_artifacts",
        "detect_persona_line_channels",
        "is_delivery_directive_line",
        "delivery_rule_from_line",
        "infer_delivery_condition_from_line",
        "compile_persona_rules",
    )

    def _persona_question_for_fact(persona_context: str, fact_key: str) -> str:
        compiled = ctx["_compile_persona_rules"](persona_context)
        canonical = ctx["_canonical_fact_key"](fact_key)
        if not canonical:
            return ""
        for step in compiled.steps:
            if step.fact_key == canonical and step.question:
                return step.question
        return ""

    ctx["_persona_question_for_fact"] = _persona_question_for_fact

    def _resolve_persona_rules_context(
        *,
        tenant: int | None,
        channel_name: str,
        fallback_context: str = "",
    ) -> str:
        if tenant is not None:
            try:
                raw = str(load_persona_fn(int(tenant), channel_name) or "").strip()
                if raw:
                    return raw
            except Exception:
                pass
        return str(fallback_context or "").strip()

    ctx["_resolve_persona_rules_context"] = _resolve_persona_rules_context

    bind_private(persona_runtime, "conditional_rule_matches")
    bind_private(decision_runtime, "apply_persona_sequence_obligations")
    bind_private(
        persona_runtime,
        "is_contact_artifact_token",
        "reply_has_contact_artifact",
        "select_contact_artifacts_for_rule",
        "is_contact_request_text",
        "assistant_messages_since_contact",
        "delivery_rule_matches",
        "delivery_intro_text",
        "strip_unsolicited_links",
        "apply_persona_delivery_obligations",
    )
    bind_private(dialog_state_runtime, "state_facts_snapshot")
    bind_private(
        location_runtime,
        "looks_like_address_value",
        "is_plausible_city_text",
        "extract_city_hint",
        "extract_standalone_city_hint",
    )
    bind_private(dialog_state_runtime, "reply_contains_unconfirmed_required_claim")
    bind_private(facts_runtime, "capture_pending_fact_answer", "merge_fact_updates")

    def _all_required_facts_present(required: Sequence[str], facts: Mapping[str, str]) -> bool:
        normalized: list[str] = []
        for raw_key in required:
            key = ctx["_normalize_fact_key"](str(raw_key))
            if key:
                normalized.append(key)
        return policy_plan_runtime().all_required_facts_present(normalized, facts)

    ctx["_all_required_facts_present"] = _all_required_facts_present

    ctx["_normalize_slot_name"] = delegate_sync(policy_plan_runtime, "normalize_slot_name")
    ctx["_question_topic"] = delegate_sync(policy_plan_runtime, "question_topic")

    def _topic_has_confirmed_fact(topic: str, state: Any) -> bool:
        if not isinstance(state, state_cls):
            return False
        return policy_plan_runtime().topic_has_confirmed_fact(topic, state)

    ctx["_topic_has_confirmed_fact"] = _topic_has_confirmed_fact

    ctx["_fact_fingerprint"] = delegate_sync(dialog_state_runtime, "fact_fingerprint")
    ctx["_dedupe_repeated_fact_sentences"] = delegate_sync(dialog_state_runtime, "dedupe_repeated_fact_sentences")
    ctx["_update_fact_memory"] = delegate_sync(dialog_state_runtime, "update_fact_memory")
    ctx["_capture_pending_slot_answer"] = delegate_sync(facts_runtime, "capture_pending_slot_answer")
    ctx["_item_price_int"] = delegate_sync(catalog_match_runtime, "item_price_int")
    ctx["_item_label"] = delegate_sync(catalog_match_runtime, "item_label")
    ctx["_display_item_label"] = delegate_sync(catalog_match_runtime, "display_item_label")
    ctx["_shortlist_preview_text"] = delegate_sync(catalog_match_runtime, "shortlist_preview_text")
    ctx["_render_shortlist_preview_reply"] = delegate_sync(catalog_match_runtime, "render_shortlist_preview_reply")
    ctx["_best_numeric_attribute_delta_line"] = delegate_sync(catalog_match_runtime, "best_numeric_attribute_delta_line")
    ctx["_shortlist_attribute_answer"] = delegate_sync(catalog_match_runtime, "shortlist_attribute_answer")
    ctx["_item_mm_value"] = delegate_sync(catalog_match_runtime, "item_mm_value")
    ctx["_first_number_value"] = delegate_sync(catalog_match_runtime, "first_number_value")
    ctx["_item_number_value"] = delegate_sync(catalog_match_runtime, "item_number_value")
    ctx["_shortlist_comparison_followup_plan"] = delegate_sync(decision_runtime, "shortlist_comparison_followup_plan")
    ctx["_item_aliases"] = delegate_sync(catalog_match_runtime, "item_aliases")
    ctx["_token_overlap_score"] = delegate_sync(catalog_match_runtime, "token_overlap_score")
    ctx["_best_catalog_item_match"] = delegate_sync(catalog_match_runtime, "best_catalog_item_match")
    ctx["_strict_catalog_item_match"] = delegate_sync(catalog_match_runtime, "strict_catalog_item_match")
    ctx["_collect_grounding_items"] = delegate_sync(grounding_runtime, "collect_grounding_items")
    ctx["_model_root_tokens"] = delegate_sync(grounding_runtime, "model_root_tokens")
    ctx["_has_single_color_variant"] = delegate_sync(grounding_runtime, "has_single_color_variant")
    ctx["_build_reply_grounding"] = delegate_sync(grounding_runtime, "build_reply_grounding")
    ctx["_maybe_store_model_slot"] = delegate_sync(policy_plan_runtime, "maybe_store_model_slot")
    ctx["_enforce_semantic_plan_guards"] = delegate_sync(policy_plan_runtime, "enforce_semantic_plan_guards")

    def _compose_reply_from_policy_blocks(
        plan: Mapping[str, Any],
        *,
        state: Any,
        persona_context: str = "",
        known_facts: Mapping[str, str] | None = None,
        required_facts: Sequence[str] | None = None,
        block_requires_override: Mapping[int, Sequence[str]] | None = None,
        block_allowance_override: Mapping[int, bool] | None = None,
    ) -> tuple[str, str]:
        _ = persona_context
        return policy_plan_runtime().compose_reply_from_policy_blocks(
            plan,
            state=state,
            known_facts=known_facts,
            required_facts=required_facts,
            block_requires_override=block_requires_override,
            block_allowance_override=block_allowance_override,
        )

    ctx["_compose_reply_from_policy_blocks"] = _compose_reply_from_policy_blocks
    ctx["_extract_prices"] = delegate_sync(catalog_price_runtime, "extract_prices")
    ctx["_extract_price_spans"] = delegate_sync(catalog_price_runtime, "extract_price_spans")
    ctx["_format_rub_price"] = delegate_sync(catalog_price_runtime, "format_rub_price")
    ctx["_mentioned_catalog_items_in_order"] = delegate_sync(catalog_price_runtime, "mentioned_catalog_items_in_order")
    ctx["_catalog_item_is_two_panel"] = lambda _item: False
    ctx["_normalize_model_alias"] = delegate_sync(catalog_guard_runtime, "normalize_model_alias")
    ctx["_grounding_catalog_items"] = delegate_sync(catalog_guard_runtime, "grounding_catalog_items")
    ctx["_enforce_catalog_model_grounding"] = delegate_sync(catalog_guard_runtime, "enforce_catalog_model_grounding")
    ctx["_enforce_catalog_price_grounding"] = delegate_sync(grounding_runtime, "enforce_catalog_price_grounding")
    ctx["_VARIANTS_USER_HINT_RE"] = re.compile(
        r"(?iu)\b(вариант|варианты|подбер|покажи|покажите|скинь|скинуть|где варианты|что есть)\b"
    )
    ctx["_PRICE_INTENT_RE"] = re.compile(
        r"(?iu)\b(сколько\s+стоит|сколько\s+стоят|сколько\s+цена|цена|ценник|по\s*ч[её]м|поч[её]м|чо\s+по\s+чем|"
        r"от\s+скольк[аи]|от\s+какой\s+цены|по\s+какой\s+цене|что\s+за\s+дверь\s+за\s*\d+|за\s*\d+\s*$)\b"
    )
    ctx["_MIN_PRICE_INTENT_RE"] = re.compile(
        r"(?iu)\b(от\s+скольк|начина(?:ется|ются)|минимальн\w+|сам\w*\s+дешев\w*)\b"
    )
    ctx["_MAX_PRICE_INTENT_RE"] = re.compile(
        r"(?iu)\b(сам\w*\s+дорог\w*|наибол\w*\s+дорог\w*|максимальн\w*\s+цен\w*|подороже)\b"
    )
    ctx["_MODEL_NAME_INTENT_RE"] = re.compile(
        r"(?iu)\b(назван\w*|название\s+модел\w*|какая\s+модел\w*|что\s+за\s+модел\w*|как\s+называет\w*)\b"
    )
    bind_private(catalog_guard_runtime, "reply_mentions_catalog_item")
    bind_private(
        catalog_guard_runtime,
        "quote_likely_model_reference",
        "reply_mentions_unknown_model",
        "looks_like_model_reference_fragment",
        "neutralize_unknown_model_mentions",
        "neutralize_unverified_priced_labels",
        "neutralize_catalog_model_mentions",
        "normalize_catalog_name_case",
        "normalize_shouting_case",
        "stabilize_followup_price_reference",
        "selected_item_from_grounding",
    )
    bind_private(catalog_price_runtime, "format_short_catalog_variants")
    bind_private(
        attribute_runtime,
        "normalize_probe_token",
        "extract_attribute_probe",
        "is_noisy_attribute_value",
        "is_dimension_like_value",
        "iter_item_attribute_pairs",
        "format_attribute_pairs",
        "selected_item_brief_answer",
        "items_with_attribute",
        "items_with_attribute_direct",
        "narrow_catalog_items_by_user_text",
        "negative_attribute_probes",
        "exclude_items_with_negative_probes",
    )
    bind_private(grounding_runtime, "selected_item_attribute_answer")
    bind_private(
        location_runtime,
        "canonical_object_type_hint",
        "object_type_from_turn_text",
        "extract_store_addresses_from_persona",
    )
    bind_private(
        catalog_metrics_runtime,
        "item_object_type_hint",
        "filter_items_by_object_type_need",
        "extract_budget_cap_from_needs",
        "catalog_min_price",
        "catalog_max_price",
        "catalog_extreme_item_by_price",
        "extract_price_target_hint",
        "closest_catalog_item_by_price",
        "is_likely_price_value",
        "is_catalog_price_candidate",
        "is_specific_catalog_keyword",
    )
    bind_private(
        intent_runtime,
        "is_price_intent",
        "is_payment_intent",
        "is_store_address_intent",
        "is_channel_handoff_intent",
        "is_catalog_request_intent",
        "is_offtopic_message",
        "classify_turn_intent",
        "is_shortlist_feedback_turn",
        "is_deferral_message",
    )

    ctx["_GENERIC_PRICE_LABEL_TOKENS"] = {
        "самый",
        "самая",
        "самое",
        "дорогой",
        "дорогая",
        "дешевый",
        "дешевая",
        "доступный",
        "доступная",
        "вариант",
        "варианты",
        "каталог",
        "каталогу",
        "модель",
        "модели",
        "дверь",
        "цена",
        "стоит",
        "за",
        "от",
        "до",
    }
    bind_private(catalog_metrics_runtime, "catalog_has_object_type_evidence")
    bind_private(catalog_guard_runtime, "extract_explicit_model_probe", "has_unverified_priced_labels")
    bind_private(decision_runtime, "enforce_catalog_truth_guard")
    ctx["_ensure_concrete_variants_in_reply"] = lambda text, **_: (text or "").strip()
    ctx["_rewrite_loses_context_anchors"] = delegate_sync(context_guard_runtime, "rewrite_loses_context_anchors")
    ctx["_fallback_semantic_plan"] = delegate_sync(semantic_runtime, "fallback_semantic_plan")
    ctx["_semantic_plan"] = delegate_async(semantic_runtime, "semantic_plan")
    ctx["_render_from_semantic_plan"] = delegate_async(semantic_runtime, "render_from_semantic_plan")
    ctx["_render_direct_reply"] = delegate_async(semantic_runtime, "render_direct_reply")
    ctx["_mentioned_catalog_item_ids"] = delegate_sync(catalog_guard_runtime, "mentioned_catalog_item_ids")
    ctx["_catalog_claim_coverage_issues"] = delegate_sync(decision_runtime, "catalog_claim_coverage_issues")
    ctx["_audit_and_rewrite_persona_reply"] = delegate_async(decision_runtime, "audit_and_rewrite_persona_reply")
    ctx["_apply_plan_alignment_to_state"] = lambda state, context, previous_fingerprints: facade_apply_plan_alignment_to_state(
        state,
        context,
        previous_fingerprints,
        apply_fn=state_runtime_apply_plan_alignment,
        remember_question_fn=ctx["_remember_question_state"],
        remember_cta_fn=ctx["_remember_cta_state"],
    )
    ctx["_make_enforcement_context"] = lambda state, persona_hints, channel_name: facade_make_enforcement_context(
        state,
        persona_hints,
        channel_name,
        make_fn=state_runtime_make_enforcement_context,
        max_questions_fn=ctx["_max_questions_limit"],
        cta_allowed_fn=ctx["_cta_allowed"],
        enforcement_context_cls=quality_module.EnforcementContext,
    )
    ctx["_clean_persona_line"] = delegate_sync(persona_runtime, "clean_persona_line")
    ctx["extract_persona_hints"] = delegate_sync(persona_runtime, "extract_persona_hints")
    ctx["_PERSONA_HINTS_CACHE"] = {}
    ctx["_persona_hints_cache_key"] = persona_hints_cache_key
    ctx["_clear_persona_hints_cache"] = lambda tenant=None: persona_hints_clear(ctx["_PERSONA_HINTS_CACHE"], tenant)
    ctx["load_persona_hints"] = lambda tenant=None, channel=None: persona_hints_load(
        ctx["_PERSONA_HINTS_CACHE"],
        tenant=tenant,
        channel=channel,
        load_persona_fn=load_persona_fn,
        extract_persona_hints_fn=ctx["extract_persona_hints"],
    )
    ctx["load_sales_state"] = delegate_sync(state_store_runtime, "load_sales_state")
    ctx["save_sales_state"] = delegate_sync(state_store_runtime, "save_sales_state")
    ctx["reset_sales_state"] = delegate_sync(state_store_runtime, "reset_sales_state")
    ctx["_GLOBAL_COLOR_ALIASES"] = needs_global_color_aliases
