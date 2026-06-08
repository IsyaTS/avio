from __future__ import annotations

from typing import Any, Mapping

from .answer_quality_runtime import AnswerQualityRuntime
from .answer_quality_runtime import AnswerQualityRuntimeDeps
from .attribute_runtime import AttributeRuntime
from .attribute_runtime import AttributeRuntimeDeps
from .conversation_runtime import ConversationRuntime
from .conversation_runtime import ConversationRuntimeDeps
from .catalog_guard_runtime import CatalogGuardRuntime
from .catalog_guard_runtime import CatalogGuardRuntimeDeps
from .catalog_loader_runtime import CatalogLoaderDeps
from .catalog_loader_runtime import CatalogLoaderRuntime
from .catalog_metrics_runtime import CatalogMetricsRuntime
from .catalog_metrics_runtime import CatalogMetricsRuntimeDeps
from .catalog_price_runtime import CatalogPriceRuntime
from .catalog_price_runtime import CatalogPriceRuntimeDeps
from .catalog_search_runtime import CatalogSearchDeps
from .catalog_search_runtime import CatalogSearchRuntime
from .context_guard_runtime import ContextGuardRuntime
from .context_guard_runtime import ContextGuardRuntimeDeps
from .decision_runtime import DecisionRuntime
from .decision_runtime import DecisionRuntimeDeps
from .dialog_state_runtime import DialogStateRuntime
from .dialog_state_runtime import DialogStateRuntimeDeps
from .facts_runtime import FactsRuntime
from .facts_runtime import FactsRuntimeDeps
from .fact_key_runtime import FactKeyRuntime
from .fact_key_runtime import FactKeyRuntimeDeps
from .fact_question_runtime import FactQuestionRuntime
from .fact_question_runtime import FactQuestionRuntimeDeps
from .fallback_runtime import FallbackRuntime
from .fallback_runtime import FallbackRuntimeDeps
from .grounding_runtime import GroundingRuntime
from .grounding_runtime import GroundingRuntimeDeps
from .humanize_runtime import HumanizeRuntime
from .humanize_runtime import HumanizeRuntimeDeps
from .instruction_runtime import InstructionRuntime
from .instruction_runtime import InstructionRuntimeDeps
from .intent_runtime import IntentRuntime
from .intent_runtime import IntentRuntimeDeps
from .io_runtime import IoRuntime
from .io_runtime import IoRuntimeDeps
from .location_runtime import LocationRuntime
from .location_runtime import LocationRuntimeDeps
from .llm_entry_runtime import LlmEntryRuntime
from .llm_entry_runtime import LlmEntryRuntimeDeps
from .message_runtime import MessageRuntime
from .message_runtime import MessageRuntimeDeps
from .needs_runtime import NeedsRuntime
from .needs_runtime import NeedsRuntimeDeps
from .persona_script_runtime import PersonaScriptRuntime
from .persona_script_runtime import PersonaScriptRuntimeDeps
from .persona_turn_runtime import PersonaTurnRuntime
from .persona_turn_runtime import PersonaTurnRuntimeDeps
from .persona_runtime import PersonaRuntime
from .persona_runtime import PersonaRuntimeDeps
from .policy_plan_runtime import PolicyPlanRuntime
from .policy_plan_runtime import PolicyPlanRuntimeDeps
from .semantic_runtime import SemanticRuntime
from .semantic_runtime import SemanticRuntimeDeps
from .state_io_runtime import StateIoRuntime
from .state_io_runtime import StateIoRuntimeDeps
from .state_store_runtime import StateStoreRuntime
from .state_store_runtime import StateStoreRuntimeDeps
from .tenant_runtime import TenantRuntimeDeps
from .catalog_match_runtime import CatalogMatchRuntime
from .catalog_match_runtime import CatalogMatchRuntimeDeps
from .catalog_csv_runtime import CatalogCsvRuntime
from .catalog_csv_runtime import CatalogCsvRuntimeDeps
from .catalog_rules import CatalogRulesDeps
from .catalog_rules import CatalogRulesRuntime

try:
    from libs.core.repo import tenant_configs
except Exception:  # pragma: no cover - optional during partial imports
    tenant_configs = None  # type: ignore[assignment]


def _tenant_configs_module() -> Any:
    global tenant_configs
    if tenant_configs is not None:
        return tenant_configs
    try:
        from libs.core.repo import tenant_configs as tenant_configs_module
    except Exception:
        return None
    tenant_configs = tenant_configs_module  # type: ignore[assignment]
    return tenant_configs_module


def _v(ctx: Mapping[str, Any], name: str) -> Any:
    return ctx[name]


def build_answer_quality_runtime(ctx: Mapping[str, Any]) -> AnswerQualityRuntime:
    return AnswerQualityRuntime(
        AnswerQualityRuntimeDeps(
            normalize_text=_v(ctx, "_normalize_text"),
            sentence_split_re=_v(ctx, "_SENTENCE_SPLIT_RE"),
            question_cue_re=_v(ctx, "_QUESTION_CUE_RE"),
            eta_intent_re=_v(ctx, "_ETA_INTENT_RE"),
            urgent_today_re=_v(ctx, "_URGENT_TODAY_RE"),
            variants_user_hint_re=_v(ctx, "_VARIANTS_USER_HINT_RE"),
            model_name_intent_re=_v(ctx, "_MODEL_NAME_INTENT_RE"),
            question_fingerprint_fn=_v(ctx, "quality").question_fingerprint,
            is_operator_instruction_sentence=_v(ctx, "_is_operator_instruction_sentence"),
            is_response_format_instruction_sentence=_v(ctx, "_is_response_format_instruction_sentence"),
            is_sequence_process_instruction_sentence=_v(ctx, "_is_sequence_process_instruction_sentence"),
            question_covers_fact=_v(ctx, "_question_covers_fact"),
            state_facts_snapshot=_v(ctx, "_state_facts_snapshot"),
            max_questions_limit=_v(ctx, "_max_questions_limit"),
            normalize_catalog_name_case=_v(ctx, "_normalize_catalog_name_case"),
            normalize_shouting_case=_v(ctx, "_normalize_shouting_case"),
            dedupe_repeated_fact_sentences=_v(ctx, "_dedupe_repeated_fact_sentences"),
            strip_instruction_leaks=_v(ctx, "_strip_instruction_leaks"),
            grounding_catalog_items=_v(ctx, "_grounding_catalog_items"),
            is_price_intent=_v(ctx, "_is_price_intent"),
            reply_mentions_catalog_item=_v(ctx, "_reply_mentions_catalog_item"),
            answer_is_too_robotic=_v(ctx, "_answer_is_too_robotic"),
        )
    )


def build_humanize_runtime(ctx: Mapping[str, Any]) -> HumanizeRuntime:
    return HumanizeRuntime(
        HumanizeRuntimeDeps(
            normalize_text=_v(ctx, "_normalize_text"),
            looks_like_address_value=_v(ctx, "_looks_like_address_value"),
            strip_instruction_leaks=_v(ctx, "_strip_instruction_leaks"),
            limit_questions=_v(ctx, "_limit_questions"),
            max_questions_limit=_v(ctx, "_max_questions_limit"),
            sentence_split_re=_v(ctx, "_SENTENCE_SPLIT_RE"),
            opening_hey_re=_v(ctx, "_OPENING_HEY_RE"),
            greeting_prefix_re=_v(ctx, "_GREETING_PREFIX_RE"),
            gratitude_re=_v(ctx, "_GRATITUDE_RE"),
            gratitude_phrase_re=_v(ctx, "_GRATITUDE_PHRASE_RE"),
            neighbor_claim_re=_v(ctx, "_NEIGHBOR_CLAIM_RE"),
            entity_ack_prefix_re=_v(ctx, "_ENTITY_ACK_PREFIX_RE"),
            object_type_hint_re=_v(ctx, "_OBJECT_TYPE_HINT_RE"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            model_name_intent_re=_v(ctx, "_MODEL_NAME_INTENT_RE"),
            variants_user_hint_re=_v(ctx, "_VARIANTS_USER_HINT_RE"),
            opening_word_re=_v(ctx, "_OPENING_WORD_RE"),
            lowercase_opening_blocked=_v(ctx, "_LOWERCASE_OPENING_BLOCKED"),
        )
    )


def build_catalog_loader_runtime(ctx: Mapping[str, Any], *, logger_obj: Any) -> CatalogLoaderRuntime:
    return CatalogLoaderRuntime(
        CatalogLoaderDeps(
            catalog_csv_path=_v(ctx, "CATALOG_CSV"),
            logger=logger_obj,
            load_workbook=_v(ctx, "load_workbook"),
            catalog_cache=_v(ctx, "_CATALOG_CACHE"),
            tenant_dir=_v(ctx, "tenant_dir"),
            load_tenant=_v(ctx, "load_tenant"),
            persona_meta_config=_v(ctx, "persona_meta_config"),
            persona_catalog_csv=_v(ctx, "persona_catalog_csv"),
            merge_csv_mapping_meta=_v(ctx, "_merge_csv_mapping_meta"),
            normalize_catalog_items=_v(ctx, "_normalize_catalog_items"),
            read_csv_rows_best=_v(ctx, "_read_csv_rows_best"),
            apply_catalog_attribute_rules=_v(ctx, "_apply_catalog_attribute_rules"),
            enrich_catalog_color_aliases=_v(ctx, "_enrich_catalog_color_aliases"),
            normalize_text=_v(ctx, "_normalize_text"),
            collect_item_text=_v(ctx, "_collect_item_text"),
            persist_pdf_index_metadata=_v(ctx, "_persist_pdf_index_metadata"),
            format_items_for_prompt=_v(ctx, "format_items_for_prompt"),
        )
    )


def build_catalog_search_runtime(ctx: Mapping[str, Any], *, logger_obj: Any) -> CatalogSearchRuntime:
    return CatalogSearchRuntime(
        CatalogSearchDeps(
            normalize_text=_v(ctx, "_normalize_text"),
            collect_item_text=_v(ctx, "_collect_item_text"),
            item_price_int=_v(ctx, "_item_price_int"),
            text_match_score=_v(ctx, "_text_match_score"),
            tokenize_query=_v(ctx, "_tokenize_query"),
            read_catalog=_v(ctx, "_read_catalog"),
            persona_meta_config=_v(ctx, "persona_meta_config"),
            augment_color_needs=_v(ctx, "_augment_color_needs"),
            filter_catalog_items_by_rules=_v(ctx, "_filter_catalog_items_by_rules"),
            extract_price_order_intent=_v(ctx, "_extract_price_order_intent"),
            catalog_retriever=_v(ctx, "catalog_retriever"),
            logger=logger_obj,
            noise_need_re=_v(ctx, "_NOISE_NEED_RE"),
        )
    )


def build_needs_runtime(ctx: Mapping[str, Any]) -> NeedsRuntime:
    return NeedsRuntime(
        NeedsRuntimeDeps(
            tokenize_query=_v(ctx, "_tokenize_query"),
            looks_like_address_value=_v(ctx, "_looks_like_address_value"),
            object_type_from_turn_text=_v(ctx, "_object_type_from_turn_text"),
            normalize_text=_v(ctx, "_normalize_text"),
            max_price_intent_re=_v(ctx, "_MAX_PRICE_INTENT_RE"),
            min_price_intent_re=_v(ctx, "_MIN_PRICE_INTENT_RE"),
            noise_need_re=_v(ctx, "_NOISE_NEED_RE"),
            insulation_need_re=_v(ctx, "_INSULATION_NEED_RE"),
            needs_stopwords=tuple(_v(ctx, "NEEDS_STOPWORDS")),
        )
    )


def build_location_runtime(ctx: Mapping[str, Any]) -> LocationRuntime:
    return LocationRuntime(
        LocationRuntimeDeps(
            object_type_hint_re=_v(ctx, "_OBJECT_TYPE_HINT_RE"),
            greeting_prefix_re=_v(ctx, "_GREETING_PREFIX_RE"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            needs_stopwords=tuple(_v(ctx, "NEEDS_STOPWORDS")),
            normalize_text=_v(ctx, "_normalize_text"),
            is_price_intent=_v(ctx, "_is_price_intent"),
            is_store_address_intent=_v(ctx, "_is_store_address_intent"),
        )
    )


def build_catalog_guard_runtime(ctx: Mapping[str, Any]) -> CatalogGuardRuntime:
    return CatalogGuardRuntime(
        CatalogGuardRuntimeDeps(
            catalog_item_identity=_v(ctx, "_catalog_item_identity"),
            strict_catalog_item_match=_v(ctx, "_strict_catalog_item_match"),
            item_aliases=_v(ctx, "_item_aliases"),
            item_label=_v(ctx, "_item_label"),
            normalize_text=_v(ctx, "_normalize_text"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            needs_stopwords=tuple(_v(ctx, "NEEDS_STOPWORDS")),
            generic_model_words=tuple(_v(ctx, "_GENERIC_MODEL_WORDS")),
            model_quoted_mention_re=_v(ctx, "_MODEL_QUOTED_MENTION_RE"),
            generic_price_label_tokens=tuple(_v(ctx, "_GENERIC_PRICE_LABEL_TOKENS")),
            merge_catalog_results=_v(ctx, "_merge_catalog_results"),
            is_price_intent=_v(ctx, "_is_price_intent"),
            extract_price_order_intent=_v(ctx, "_extract_price_order_intent"),
            variants_user_hint_re=_v(ctx, "_VARIANTS_USER_HINT_RE"),
            model_name_intent_re=_v(ctx, "_MODEL_NAME_INTENT_RE"),
            extract_price_spans=_v(ctx, "_extract_price_spans"),
            is_likely_price_value=_v(ctx, "_is_likely_price_value"),
            format_rub_price=_v(ctx, "_format_rub_price"),
            item_price_int=_v(ctx, "_item_price_int"),
        )
    )


def build_catalog_price_runtime(ctx: Mapping[str, Any]) -> CatalogPriceRuntime:
    return CatalogPriceRuntime(
        CatalogPriceRuntimeDeps(
            price_inline_re=_v(ctx, "_PRICE_INLINE_RE"),
            price_thousands_re=_v(ctx, "_PRICE_THOUSANDS_RE"),
            normalize_model_alias=_v(ctx, "_normalize_model_alias"),
            item_aliases=_v(ctx, "_item_aliases"),
            catalog_item_identity=_v(ctx, "_catalog_item_identity"),
            item_label=_v(ctx, "_item_label"),
            item_price_int=_v(ctx, "_item_price_int"),
        )
    )


def build_catalog_metrics_runtime(ctx: Mapping[str, Any]) -> CatalogMetricsRuntime:
    return CatalogMetricsRuntime(
        CatalogMetricsRuntimeDeps(
            canonical_object_type_hint=_v(ctx, "_canonical_object_type_hint"),
            normalize_text=_v(ctx, "_normalize_text"),
            collect_item_text=_v(ctx, "_collect_item_text"),
            item_price_int=_v(ctx, "_item_price_int"),
            extract_price_spans=_v(ctx, "_extract_price_spans"),
            normalize_probe_token=_v(ctx, "_normalize_probe_token"),
        )
    )


def build_attribute_runtime(ctx: Mapping[str, Any]) -> AttributeRuntime:
    return AttributeRuntime(
        AttributeRuntimeDeps(
            normalize_text=_v(ctx, "_normalize_text"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            generic_fact_stopwords=tuple(_v(ctx, "_GENERIC_FACT_STOPWORDS")),
            collect_item_text=_v(ctx, "_collect_item_text"),
            tokenize_query=_v(ctx, "_tokenize_query"),
            text_match_score=_v(ctx, "_text_match_score"),
            needs_stopwords=tuple(_v(ctx, "NEEDS_STOPWORDS")),
            item_price_int=_v(ctx, "_item_price_int"),
            display_item_label=_v(ctx, "_display_item_label"),
            format_rub_price=_v(ctx, "_format_rub_price"),
        )
    )


def build_context_guard_runtime(ctx: Mapping[str, Any]) -> ContextGuardRuntime:
    return ContextGuardRuntime(
        ContextGuardRuntimeDeps(
            normalize_model_alias=_v(ctx, "_normalize_model_alias"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            generic_fact_stopwords=tuple(_v(ctx, "_GENERIC_FACT_STOPWORDS")),
        )
    )


def build_decision_runtime(
    ctx: Mapping[str, Any],
    *,
    settings_obj: Any,
    logger_obj: Any,
    api_timeout_error_cls: type[Exception],
) -> DecisionRuntime:
    return DecisionRuntime(
        DecisionRuntimeDeps(
            normalize_text=_v(ctx, "_normalize_text"),
            classify_turn_intent=_v(ctx, "_classify_turn_intent"),
            fallback_contextual_question=_v(ctx, "_fallback_contextual_question"),
            generic_question_for_fact=_v(ctx, "_generic_question_for_fact"),
            read_all_catalog=_v(ctx, "read_all_catalog"),
            catalog_item_identity=_v(ctx, "_catalog_item_identity"),
            item_mm_value=_v(ctx, "_item_mm_value"),
            item_number_value=_v(ctx, "_item_number_value"),
            extract_attribute_probe=_v(ctx, "_extract_attribute_probe"),
            is_shortlist_feedback_turn=_v(ctx, "_is_shortlist_feedback_turn"),
            looks_like_contextual_short_followup=_v(ctx, "_looks_like_contextual_short_followup"),
            shortlist_preview_text=_v(ctx, "_shortlist_preview_text"),
            render_shortlist_preview_reply=_v(ctx, "_render_shortlist_preview_reply"),
            is_price_intent=_v(ctx, "_is_price_intent"),
            looks_like_price_objection=_v(ctx, "_looks_like_price_objection"),
            extract_price_order_intent=_v(ctx, "_extract_price_order_intent"),
            item_price_int=_v(ctx, "_item_price_int"),
            persona_driven_question_for_fact=_v(ctx, "_persona_driven_question_for_fact"),
            question_covers_fact=_v(ctx, "_question_covers_fact"),
            is_repeated_question_against_state=_v(ctx, "_is_repeated_question_against_state"),
            items_with_attribute=_v(ctx, "_items_with_attribute"),
            shortlist_attribute_answer=_v(ctx, "_shortlist_attribute_answer"),
            best_numeric_attribute_delta_line=_v(ctx, "_best_numeric_attribute_delta_line"),
            format_rub_price=_v(ctx, "_format_rub_price"),
            grounding_catalog_items=_v(ctx, "_grounding_catalog_items"),
            enforce_catalog_model_grounding=_v(ctx, "_enforce_catalog_model_grounding"),
            enforce_catalog_price_grounding=_v(ctx, "_enforce_catalog_price_grounding"),
            reply_mentions_catalog_item=_v(ctx, "_reply_mentions_catalog_item"),
            extract_price_spans=_v(ctx, "_extract_price_spans"),
            is_catalog_price_candidate=_v(ctx, "_is_catalog_price_candidate"),
            catalog_has_object_type_evidence=_v(ctx, "_catalog_has_object_type_evidence"),
            object_type_from_turn_text=_v(ctx, "_object_type_from_turn_text"),
            filter_items_by_object_type_need=_v(ctx, "_filter_items_by_object_type_need"),
            selected_item_from_grounding=_v(ctx, "_selected_item_from_grounding"),
            item_object_type_hint=_v(ctx, "_item_object_type_hint"),
            catalog_item_is_two_panel=_v(ctx, "_catalog_item_is_two_panel"),
            infer_user_needs=_v(ctx, "infer_user_needs"),
            is_specific_catalog_keyword=_v(ctx, "_is_specific_catalog_keyword"),
            normalize_probe_token=_v(ctx, "_normalize_probe_token"),
            items_with_attribute_direct=_v(ctx, "_items_with_attribute_direct"),
            extract_budget_cap_from_needs=_v(ctx, "_extract_budget_cap_from_needs"),
            closest_catalog_item_by_price=_v(ctx, "_closest_catalog_item_by_price"),
            item_label=_v(ctx, "_item_label"),
            reply_mentions_unknown_model=_v(ctx, "_reply_mentions_unknown_model"),
            neutralize_unknown_model_mentions=_v(ctx, "_neutralize_unknown_model_mentions"),
            extract_explicit_model_probe=_v(ctx, "_extract_explicit_model_probe"),
            strict_catalog_item_match=_v(ctx, "_strict_catalog_item_match"),
            has_unverified_priced_labels=_v(ctx, "_has_unverified_priced_labels"),
            neutralize_unverified_priced_labels=_v(ctx, "_neutralize_unverified_priced_labels"),
            selected_item_attribute_answer=_v(ctx, "_selected_item_attribute_answer"),
            narrow_catalog_items_by_user_text=_v(ctx, "_narrow_catalog_items_by_user_text"),
            negative_attribute_probes=_v(ctx, "_negative_attribute_probes"),
            exclude_items_with_negative_probes=_v(ctx, "_exclude_items_with_negative_probes"),
            catalog_extreme_item_by_price=_v(ctx, "_catalog_extreme_item_by_price"),
            extract_price_target_hint=_v(ctx, "_extract_price_target_hint"),
            catalog_min_price=_v(ctx, "_catalog_min_price"),
            neutralize_catalog_model_mentions=_v(ctx, "_neutralize_catalog_model_mentions"),
            is_likely_price_value=_v(ctx, "_is_likely_price_value"),
            model_name_intent_re=_v(ctx, "_MODEL_NAME_INTENT_RE"),
            variants_user_hint_re=_v(ctx, "_VARIANTS_USER_HINT_RE"),
            min_price_intent_re=_v(ctx, "_MIN_PRICE_INTENT_RE"),
            max_price_intent_re=_v(ctx, "_MAX_PRICE_INTENT_RE"),
            sentence_split_re=_v(ctx, "_SENTENCE_SPLIT_RE"),
            mentioned_catalog_item_ids=_v(ctx, "_mentioned_catalog_item_ids"),
            best_catalog_item_match=_v(ctx, "_best_catalog_item_match"),
            normalize_model_alias=_v(ctx, "_normalize_model_alias"),
            item_aliases=_v(ctx, "_item_aliases"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            generic_fact_stopwords=_v(ctx, "_GENERIC_FACT_STOPWORDS"),
            load_sales_state=_v(ctx, "load_sales_state"),
            load_persona_hints=_v(ctx, "load_persona_hints"),
            load_persona=_v(ctx, "load_persona"),
            branding_for_tenant=_v(ctx, "_branding_for_tenant"),
            state_facts_snapshot=_v(ctx, "_state_facts_snapshot"),
            persona_direct_reply_for_user_turn=_v(ctx, "_persona_direct_reply_for_user_turn"),
            build_reply_grounding=_v(ctx, "_build_reply_grounding"),
            search_catalog=_v(ctx, "search_catalog"),
            apply_base_answer_quality_floor=_v(ctx, "_apply_base_answer_quality_floor"),
            humanize_reply_text=_v(ctx, "_humanize_reply_text"),
            ensure_dialog_greeting_on_first_reply=_v(ctx, "_ensure_dialog_greeting_on_first_reply"),
            save_sales_state=_v(ctx, "save_sales_state"),
            canonical_fact_key=_v(ctx, "_canonical_fact_key"),
            required_facts_from_persona_text=_v(ctx, "_required_facts_from_persona_text"),
            missing_required_facts=_v(ctx, "_missing_required_facts"),
            prioritize_missing_facts=_v(ctx, "_prioritize_missing_facts"),
            persona_script_questions=_v(ctx, "_persona_script_questions"),
            persona_primary_script_question=_v(ctx, "_persona_primary_script_question"),
            selected_item_brief_answer=_v(ctx, "_selected_item_brief_answer"),
            explain_missing_fact_need=_v(ctx, "_explain_missing_fact_need"),
            normalize_slot_name=_v(ctx, "_normalize_slot_name"),
            extract_questions_from_text=_v(ctx, "_extract_questions_from_text"),
            fact_keys_from_line=_v(ctx, "_fact_keys_from_line"),
            apply_persona_sequence_obligations=_v(ctx, "_apply_persona_sequence_obligations"),
            apply_persona_delivery_obligations=_v(ctx, "_apply_persona_delivery_obligations"),
            update_fact_memory=_v(ctx, "_update_fact_memory"),
            remember_questions_from_reply=_v(ctx, "_remember_questions_from_reply"),
            persona_catalog_unavailable_reply=_v(ctx, "_persona_catalog_unavailable_reply"),
            order_intent_re=_v(ctx, "_ORDER_INTENT_RE"),
            catalog_unavailable_re=_v(ctx, "_CATALOG_UNAVAILABLE_RE"),
            low_signal_context_re=_v(ctx, "_LOW_SIGNAL_CONTEXT_RE"),
            reply_contains_unconfirmed_required_claim=_v(ctx, "_reply_contains_unconfirmed_required_claim"),
            has_substantive_non_question_payload=_v(ctx, "_has_substantive_non_question_payload"),
            is_payment_intent=_v(ctx, "_is_payment_intent"),
            is_store_address_intent=_v(ctx, "_is_store_address_intent"),
            is_channel_handoff_intent=_v(ctx, "_is_channel_handoff_intent"),
            is_deferral_message=_v(ctx, "_is_deferral_message"),
            extract_store_addresses_from_persona=_v(ctx, "_extract_store_addresses_from_persona"),
            extract_city_hint=_v(ctx, "_extract_city_hint"),
            question_lists_catalog_options=_v(ctx, "_question_lists_catalog_options"),
            replace_reply_question=_v(ctx, "_replace_reply_question"),
            strip_instruction_leaks=_v(ctx, "_strip_instruction_leaks"),
            compile_persona_rules=_v(ctx, "_compile_persona_rules"),
            strip_embedded_operator_tail=_v(ctx, "_strip_embedded_operator_tail"),
            count_sentences=_v(ctx, "_count_sentences"),
            fact_fingerprint=_v(ctx, "_fact_fingerprint"),
            sales_state_cls=_v(ctx, "SalesState"),
            conditional_rule_matches=_v(ctx, "_conditional_rule_matches"),
            llm_call_with_deadline=_v(ctx, "_llm_call_with_deadline"),
            safe_json_load=_v(ctx, "_safe_json_load"),
            enforce_sentence_budget=_v(ctx, "_enforce_sentence_budget"),
            rewrite_loses_context_anchors=_v(ctx, "_rewrite_loses_context_anchors"),
            reply_has_repeated_question=_v(ctx, "_reply_has_repeated_question"),
            format_items_for_prompt=_v(ctx, "format_items_for_prompt"),
            quality_dedupe_repeated_blocks=_v(ctx, "quality")._dedupe_repeated_blocks,
            contact_url_re=_v(ctx, "_CONTACT_URL_RE"),
            contact_handle_re=_v(ctx, "_CONTACT_HANDLE_RE"),
            contact_phone_re=_v(ctx, "_CONTACT_PHONE_RE"),
            greeting_prefix_re=_v(ctx, "_GREETING_PREFIX_RE"),
            question_cue_re=_v(ctx, "_QUESTION_CUE_RE"),
            repair_turn_re=_v(ctx, "_REPAIR_TURN_RE"),
            price_inline_re=_v(ctx, "_PRICE_INLINE_RE"),
            env_bool=_v(ctx, "_env_bool"),
            safe_short_text=_v(ctx, "_safe_short_text"),
            normalize_fact_key=_v(ctx, "_normalize_fact_key"),
            display_item_label=_v(ctx, "_display_item_label"),
            resolve_persona_rules_context=_v(ctx, "_resolve_persona_rules_context"),
            resolve_chat_completion_callable=_v(ctx, "_resolve_chat_completion_callable"),
            build_human_mode_messages=_v(ctx, "_build_human_mode_messages"),
            render_direct_reply=_v(ctx, "_render_direct_reply"),
            stabilize_followup_price_reference=_v(ctx, "_stabilize_followup_price_reference"),
            wrap_llm_reply=_v(ctx, "_wrap_llm_reply"),
            record_bot_reply=_v(ctx, "record_bot_reply"),
            llm_unavailable_reply=_v(ctx, "_llm_unavailable_reply"),
            is_quota_or_rate_limit_error=_v(ctx, "_is_quota_or_rate_limit_error"),
            settings_obj=settings_obj,
            logger_obj=logger_obj,
            api_timeout_error_cls=api_timeout_error_cls,
        )
    )


def build_persona_runtime(ctx: Mapping[str, Any]) -> PersonaRuntime:
    return PersonaRuntime(
        PersonaRuntimeDeps(
            normalize_text=_v(ctx, "_normalize_text"),
            normalize_probe_token=_v(ctx, "_normalize_probe_token"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            contact_url_re=_v(ctx, "_CONTACT_URL_RE"),
            contact_handle_re=_v(ctx, "_CONTACT_HANDLE_RE"),
            contact_phone_re=_v(ctx, "_CONTACT_PHONE_RE"),
            is_plausible_contact_phone=_v(ctx, "_is_plausible_contact_phone"),
            persona_rules_cache_key=_v(ctx, "_persona_rules_cache_key"),
            persona_rules_cache=_v(ctx, "_PERSONA_RULES_CACHE"),
            persona_compiled_rules_cls=_v(ctx, "PersonaCompiledRules"),
            persona_conditional_rule_cls=_v(ctx, "PersonaConditionalRule"),
            persona_delivery_rule_cls=_v(ctx, "PersonaDeliveryRule"),
            persona_step_rule_cls=_v(ctx, "PersonaStepRule"),
            sales_state_cls=_v(ctx, "SalesState"),
            extract_primary_script_lines=_v(ctx, "_extract_primary_script_lines"),
            fact_keys_from_line=_v(ctx, "_fact_keys_from_line"),
            line_to_question=_v(ctx, "_line_to_question"),
            is_operator_like_question=_v(ctx, "_is_operator_like_question"),
            canonical_fact_key=_v(ctx, "_canonical_fact_key"),
            question_covers_fact=_v(ctx, "_question_covers_fact"),
            infer_persona_template_condition_and_action=_v(ctx, "_infer_persona_template_condition_and_action"),
            extract_questions_from_text=_v(ctx, "_extract_questions_from_text"),
            generic_question_for_fact=_v(ctx, "_generic_question_for_fact"),
            persona_hints_cls=_v(ctx, "PersonaHints"),
            persona_hints_key_re=_v(ctx, "_PERSONA_HINTS_KEY_RE"),
        )
    )


def build_message_runtime(
    ctx: Mapping[str, Any],
    *,
    settings_obj: Any,
    logger_obj: Any,
    openai_module: Any,
    api_timeout_error_cls: type[Exception],
) -> MessageRuntime:
    return MessageRuntime(
        MessageRuntimeDeps(
            load_persona=_v(ctx, "load_persona"),
            extract_persona_hints=_v(ctx, "extract_persona_hints"),
            persona_hints_cache_key=_v(ctx, "_persona_hints_cache_key"),
            persona_hints_cache=_v(ctx, "_PERSONA_HINTS_CACHE"),
            branding_for_tenant=_v(ctx, "_branding_for_tenant"),
            load_sales_state=_v(ctx, "load_sales_state"),
            capture_pending_fact_answer=_v(ctx, "_capture_pending_fact_answer"),
            merge_fact_updates=_v(ctx, "_merge_fact_updates"),
            extract_city_hint=_v(ctx, "_extract_city_hint"),
            canonical_fact_key=_v(ctx, "_canonical_fact_key"),
            extract_questions_from_text=_v(ctx, "_extract_questions_from_text"),
            question_covers_fact=_v(ctx, "_question_covers_fact"),
            extract_standalone_city_hint=_v(ctx, "_extract_standalone_city_hint"),
            is_plausible_city_text=_v(ctx, "_is_plausible_city_text"),
            safe_short_text=_v(ctx, "_safe_short_text"),
            save_sales_state=_v(ctx, "save_sales_state"),
            state_facts_snapshot=_v(ctx, "_state_facts_snapshot"),
            summarize_sales_state=_v(ctx, "summarize_sales_state"),
            normalize_slot_name=_v(ctx, "_normalize_slot_name"),
            infer_user_needs=_v(ctx, "infer_user_needs"),
            object_type_from_turn_text=_v(ctx, "_object_type_from_turn_text"),
            search_catalog=_v(ctx, "search_catalog"),
            format_items_for_prompt=_v(ctx, "format_items_for_prompt"),
            read_all_catalog=_v(ctx, "read_all_catalog"),
            resolve_chat_completion_callable=_v(ctx, "_resolve_chat_completion_callable"),
            build_reply_grounding=_v(ctx, "_build_reply_grounding"),
            llm_call_with_deadline=_v(ctx, "_llm_call_with_deadline"),
            settings_obj=settings_obj,
            enforce_catalog_truth_guard=_v(ctx, "_enforce_catalog_truth_guard"),
            planner_generated_plan_cls=_v(ctx, "planner").GeneratedPlan,
            make_enforcement_context=_v(ctx, "_make_enforcement_context"),
            apply_plan_alignment_to_state=_v(ctx, "_apply_plan_alignment_to_state"),
            update_fact_memory=_v(ctx, "_update_fact_memory"),
            remember_questions_from_reply=_v(ctx, "_remember_questions_from_reply"),
            wrap_llm_reply=_v(ctx, "_wrap_llm_reply"),
            record_bot_reply=_v(ctx, "record_bot_reply"),
            api_timeout_error_cls=api_timeout_error_cls,
            logger_obj=logger_obj,
            is_quota_or_rate_limit_error=_v(ctx, "_is_quota_or_rate_limit_error"),
            llm_unavailable_reply=_v(ctx, "_llm_unavailable_reply"),
            build_human_mode_messages=_v(ctx, "_build_human_mode_messages"),
            planner_generate_sales_reply=_v(ctx, "planner").generate_sales_reply,
            enforce_catalog_price_grounding=_v(ctx, "_enforce_catalog_price_grounding"),
            is_unsubscribe_intent=_v(ctx, "_is_unsubscribe_intent"),
            unsubscribe_ack_text=_v(ctx, "_unsubscribe_ack_text"),
            get_openai_client=_v(ctx, "_get_openai_client"),
            openai_module=openai_module,
            openai_api_key=settings_obj.OPENAI_API_KEY,
            load_persona_hints=_v(ctx, "load_persona_hints"),
            load_tenant=_v(ctx, "load_tenant"),
            resolve_brain_mode=_v(ctx, "_resolve_brain_mode"),
            single_llm_reply=_v(ctx, "_single_llm_reply"),
            low_signal_user_reply_re=_v(ctx, "_LOW_SIGNAL_USER_REPLY_RE"),
            low_signal_context_re=_v(ctx, "_LOW_SIGNAL_CONTEXT_RE"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
        )
    )


def build_semantic_runtime(ctx: Mapping[str, Any], *, settings_obj: Any) -> SemanticRuntime:
    return SemanticRuntime(
        SemanticRuntimeDeps(
            llm_call_with_deadline=_v(ctx, "_llm_call_with_deadline"),
            safe_json_load=_v(ctx, "_safe_json_load"),
            format_items_for_prompt=_v(ctx, "format_items_for_prompt"),
            settings_obj=settings_obj,
        )
    )


def build_facts_runtime(ctx: Mapping[str, Any]) -> FactsRuntime:
    return FactsRuntime(
        FactsRuntimeDeps(
            canonical_fact_key=_v(ctx, "_canonical_fact_key"),
            normalize_fact_key=_v(ctx, "_normalize_fact_key"),
            normalize_text=_v(ctx, "_normalize_text"),
            safe_short_text=_v(ctx, "_safe_short_text"),
            classify_turn_intent=_v(ctx, "_classify_turn_intent"),
            state_facts_snapshot=_v(ctx, "_state_facts_snapshot"),
            extract_city_hint=_v(ctx, "_extract_city_hint"),
            extract_standalone_city_hint=_v(ctx, "_extract_standalone_city_hint"),
            looks_like_address_value=_v(ctx, "_looks_like_address_value"),
            extract_budget=_v(ctx, "_extract_budget"),
            extract_price_spans=_v(ctx, "_extract_price_spans"),
            read_catalog=_v(ctx, "_read_catalog"),
            best_catalog_item_match=_v(ctx, "_best_catalog_item_match"),
            item_label=_v(ctx, "_item_label"),
            object_type_from_turn_text=_v(ctx, "_object_type_from_turn_text"),
            canonical_object_type_hint=_v(ctx, "_canonical_object_type_hint"),
            infer_user_needs=_v(ctx, "infer_user_needs"),
            is_plausible_city_text=_v(ctx, "_is_plausible_city_text"),
            normalize_slot_name=lambda slot: _v(ctx, "_normalize_slot_name")(slot, ""),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            low_signal_context_re=_v(ctx, "_LOW_SIGNAL_CONTEXT_RE"),
            catalog_unavailable_re=_v(ctx, "_CATALOG_UNAVAILABLE_RE"),
            object_type_hint_re=_v(ctx, "_OBJECT_TYPE_HINT_RE"),
            generic_model_words=_v(ctx, "_GENERIC_MODEL_WORDS"),
            needs_stopwords=_v(ctx, "NEEDS_STOPWORDS"),
        )
    )


def build_intent_runtime(ctx: Mapping[str, Any]) -> IntentRuntime:
    return IntentRuntime(
        IntentRuntimeDeps(
            catalog_request_re=_v(ctx, "_CATALOG_REQUEST_RE"),
            order_intent_re=_v(ctx, "_ORDER_INTENT_RE"),
            model_name_intent_re=_v(ctx, "_MODEL_NAME_INTENT_RE"),
            offtopic_smalltalk_re=_v(ctx, "_OFFTOPIC_SMALLTALK_RE"),
            why_question_re=_v(ctx, "_WHY_QUESTION_RE"),
            catalog_unavailable_re=_v(ctx, "_CATALOG_UNAVAILABLE_RE"),
            low_signal_context_re=_v(ctx, "_LOW_SIGNAL_CONTEXT_RE"),
            repair_turn_re=_v(ctx, "_REPAIR_TURN_RE"),
            is_unsubscribe_intent=_v(ctx, "_is_unsubscribe_intent"),
            extract_city_hint=_v(ctx, "_extract_city_hint"),
            looks_like_address_value=_v(ctx, "_looks_like_address_value"),
            object_type_from_turn_text=_v(ctx, "_object_type_from_turn_text"),
            extract_attribute_probe=_v(ctx, "_extract_attribute_probe"),
        )
    )


def build_grounding_runtime(ctx: Mapping[str, Any]) -> GroundingRuntime:
    return GroundingRuntime(
        GroundingRuntimeDeps(
            catalog_item_identity=_v(ctx, "_catalog_item_identity"),
            low_signal_user_reply_re=_v(ctx, "_LOW_SIGNAL_USER_REPLY_RE"),
            low_signal_context_re=_v(ctx, "_LOW_SIGNAL_CONTEXT_RE"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            noise_need_re=_v(ctx, "_NOISE_NEED_RE"),
            infer_user_needs=_v(ctx, "infer_user_needs"),
            search_catalog=_v(ctx, "search_catalog"),
            read_catalog=_v(ctx, "_read_catalog"),
            normalize_text=_v(ctx, "_normalize_text"),
            item_aliases=_v(ctx, "_item_aliases"),
            normalize_model_alias=_v(ctx, "_normalize_model_alias"),
            merge_catalog_results=_v(ctx, "_merge_catalog_results"),
            item_label=_v(ctx, "_item_label"),
            global_color_aliases=_v(ctx, "_GLOBAL_COLOR_ALIASES"),
            normalize_color_token=_v(ctx, "_normalize_color_token"),
            best_catalog_item_match=_v(ctx, "_best_catalog_item_match"),
            enforce_catalog_model_grounding=_v(ctx, "_enforce_catalog_model_grounding"),
            grounding_catalog_items=_v(ctx, "_grounding_catalog_items"),
            sentence_split_re=_v(ctx, "_SENTENCE_SPLIT_RE"),
            selected_item_from_grounding=_v(ctx, "_selected_item_from_grounding"),
            item_price_int=_v(ctx, "_item_price_int"),
            extract_price_spans=_v(ctx, "_extract_price_spans"),
            is_likely_price_value=_v(ctx, "_is_likely_price_value"),
            mentioned_catalog_items_in_order=_v(ctx, "_mentioned_catalog_items_in_order"),
            format_rub_price=_v(ctx, "_format_rub_price"),
            extract_attribute_probe=_v(ctx, "_extract_attribute_probe"),
            tokenize_query=_v(ctx, "_tokenize_query"),
            generic_fact_stopwords=_v(ctx, "_GENERIC_FACT_STOPWORDS"),
            normalize_probe_token=_v(ctx, "_normalize_probe_token"),
            iter_item_attribute_pairs=_v(ctx, "_iter_item_attribute_pairs"),
            normalize_text_fn=_v(ctx, "_normalize_text"),
            format_attribute_pairs=_v(ctx, "_format_attribute_pairs"),
            is_dimension_like_value=_v(ctx, "_is_dimension_like_value"),
        )
    )


def build_fact_key_runtime(ctx: Mapping[str, Any]) -> FactKeyRuntime:
    return FactKeyRuntime(
        FactKeyRuntimeDeps(
            fact_canonical_aliases=_v(ctx, "_FACT_CANONICAL_ALIASES"),
        )
    )


def build_fact_question_runtime(ctx: Mapping[str, Any]) -> FactQuestionRuntime:
    return FactQuestionRuntime(
        FactQuestionRuntimeDeps(
            canonical_fact_key=_v(ctx, "_canonical_fact_key"),
            normalize_text=_v(ctx, "_normalize_text"),
            item_label=_v(ctx, "_item_label"),
            normalize_model_alias=_v(ctx, "_normalize_model_alias"),
            sentence_split_re=_v(ctx, "_SENTENCE_SPLIT_RE"),
            persona_question_for_fact=_v(ctx, "_persona_question_for_fact"),
            persona_primary_script_question=lambda persona_text, state: _v(ctx, "_persona_primary_script_question")(
                persona_text,
                state=state,
            ),
        )
    )


def build_persona_script_runtime(ctx: Mapping[str, Any]) -> PersonaScriptRuntime:
    return PersonaScriptRuntime(
        PersonaScriptRuntimeDeps(
            normalize_text=_v(ctx, "_normalize_text"),
            line_to_question=_v(ctx, "_line_to_question"),
            is_operator_like_question=_v(ctx, "_is_operator_like_question"),
            is_repeated_question_against_state=_v(ctx, "_is_repeated_question_against_state"),
        )
    )


def build_state_io_runtime(ctx: Mapping[str, Any]) -> StateIoRuntime:
    return StateIoRuntime(
        StateIoRuntimeDeps(
            state_key_prefix=_v(ctx, "STATE_KEY_PREFIX"),
            state_ttl_seconds=_v(ctx, "STATE_TTL_SECONDS"),
            state_store_unavailable_sentinel=_v(ctx, "_STATE_STORE_UNAVAILABLE"),
            state_cache=_v(ctx, "_STATE_CACHE"),
            with_sync_redis_fn=_v(ctx, "_with_sync_redis"),
        )
    )


def build_state_store_runtime(ctx: Mapping[str, Any]) -> StateStoreRuntime:
    return StateStoreRuntime(
        StateStoreRuntimeDeps(
            state_key_fn=_v(ctx, "_state_key"),
            state_store_read_fn=_v(ctx, "_state_store_read"),
            state_store_write_fn=_v(ctx, "_state_store_write"),
            state_cache=_v(ctx, "_STATE_CACHE"),
            with_sync_redis_fn=_v(ctx, "_with_sync_redis"),
            sales_state_cls=_v(ctx, "SalesState"),
        )
    )


def build_io_runtime(ctx: Mapping[str, Any]) -> IoRuntime:
    return IoRuntime(
        IoRuntimeDeps(
            with_sync_redis_fn=_v(ctx, "_with_sync_redis"),
            tenant_pubkeys_hash=_v(ctx, "TENANT_PUBKEYS_HASH"),
        )
    )


def build_instruction_runtime(ctx: Mapping[str, Any]) -> InstructionRuntime:
    return InstructionRuntime(
        InstructionRuntimeDeps(
            normalize_text=_v(ctx, "_normalize_text"),
            sentence_split_re=_v(ctx, "_SENTENCE_SPLIT_RE"),
            instruction_leak_line_re=_v(ctx, "_INSTRUCTION_LEAK_LINE_RE"),
            instruction_list_line_re=_v(ctx, "_INSTRUCTION_LIST_LINE_RE"),
            shortlist_leak_re=_v(ctx, "_SHORTLIST_LEAK_RE"),
        )
    )


def build_catalog_rules_runtime(ctx: Mapping[str, Any]) -> CatalogRulesRuntime:
    return CatalogRulesRuntime(CatalogRulesDeps(match_key=_v(ctx, "_match_key")))


def build_catalog_csv_runtime(ctx: Mapping[str, Any]) -> CatalogCsvRuntime:
    return CatalogCsvRuntime(
        CatalogCsvRuntimeDeps(
            field_clean_re=_v(ctx, "_FIELD_CLEAN_RE"),
            normalize_text=_v(ctx, "_normalize_text"),
        )
    )


def build_dialog_state_runtime(ctx: Mapping[str, Any]) -> DialogStateRuntime:
    return DialogStateRuntime(
        DialogStateRuntimeDeps(
            canonical_fact_key=_v(ctx, "_canonical_fact_key"),
            safe_short_text=_v(ctx, "_safe_short_text"),
            normalize_slot_name=lambda slot: _v(ctx, "_normalize_slot_name")(slot, ""),
            is_plausible_city_text=_v(ctx, "_is_plausible_city_text"),
            normalize_text=_v(ctx, "_normalize_text"),
            extract_city_hint=_v(ctx, "_extract_city_hint"),
            extract_standalone_city_hint=_v(ctx, "_extract_standalone_city_hint"),
            extract_store_addresses_from_persona=_v(ctx, "_extract_store_addresses_from_persona"),
            canonical_object_type_hint=_v(ctx, "_canonical_object_type_hint"),
            infer_user_needs=_v(ctx, "infer_user_needs"),
            object_type_from_turn_text=_v(ctx, "_object_type_from_turn_text"),
            extract_questions_from_text=_v(ctx, "_extract_questions_from_text"),
            question_covers_fact=_v(ctx, "_question_covers_fact"),
            sentence_split_re=_v(ctx, "_SENTENCE_SPLIT_RE"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            generic_fact_stopwords=_v(ctx, "_GENERIC_FACT_STOPWORDS"),
        )
    )


def build_fallback_runtime(ctx: Mapping[str, Any]) -> FallbackRuntime:
    return FallbackRuntime(
        FallbackRuntimeDeps(
            grounding_catalog_items=_v(ctx, "_grounding_catalog_items"),
            classify_turn_intent=_v(ctx, "_classify_turn_intent"),
            normalize_text=_v(ctx, "_normalize_text"),
            shortlist_preview_text=_v(ctx, "_shortlist_preview_text"),
            extract_attribute_probe=_v(ctx, "_extract_attribute_probe"),
            display_item_label=_v(ctx, "_display_item_label"),
            item_label=_v(ctx, "_item_label"),
            catalog_min_price=_v(ctx, "_catalog_min_price"),
            catalog_max_price=_v(ctx, "_catalog_max_price"),
            format_rub_price=_v(ctx, "_format_rub_price"),
            is_price_intent=_v(ctx, "_is_price_intent"),
            looks_like_price_objection=_v(ctx, "_looks_like_price_objection"),
            variants_user_hint_re=_v(ctx, "_VARIANTS_USER_HINT_RE"),
            price_inline_re=_v(ctx, "_PRICE_INLINE_RE"),
            price_thousands_re=_v(ctx, "_PRICE_THOUSANDS_RE"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            generic_fact_stopwords=_v(ctx, "_GENERIC_FACT_STOPWORDS"),
        )
    )


def build_persona_turn_runtime(ctx: Mapping[str, Any]) -> PersonaTurnRuntime:
    return PersonaTurnRuntime(
        PersonaTurnRuntimeDeps(
            compile_persona_rules=_v(ctx, "_compile_persona_rules"),
            conditional_rule_matches=_v(ctx, "_conditional_rule_matches"),
            extract_attribute_probe=_v(ctx, "_extract_attribute_probe"),
            model_name_intent_re=_v(ctx, "_MODEL_NAME_INTENT_RE"),
            variants_user_hint_re=_v(ctx, "_VARIANTS_USER_HINT_RE"),
            state_facts_snapshot=_v(ctx, "_state_facts_snapshot"),
            canonical_fact_key=_v(ctx, "_canonical_fact_key"),
            required_facts_from_persona_text=_v(ctx, "_required_facts_from_persona_text"),
            missing_required_facts=_v(ctx, "_missing_required_facts"),
            persona_driven_question_for_fact=_v(ctx, "_persona_driven_question_for_fact"),
            is_repeated_question_against_state=_v(ctx, "_is_repeated_question_against_state"),
            question_covers_fact=_v(ctx, "_question_covers_fact"),
            extract_price_spans=_v(ctx, "_extract_price_spans"),
            normalize_text=_v(ctx, "_normalize_text"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            needs_stopwords=_v(ctx, "NEEDS_STOPWORDS"),
            generic_fact_stopwords=_v(ctx, "_GENERIC_FACT_STOPWORDS"),
            order_intent_re=_v(ctx, "_ORDER_INTENT_RE"),
            is_price_intent=_v(ctx, "_is_price_intent"),
            eta_intent_re=_v(ctx, "_ETA_INTENT_RE"),
        )
    )


def build_catalog_match_runtime(ctx: Mapping[str, Any]) -> CatalogMatchRuntime:
    return CatalogMatchRuntime(
        CatalogMatchRuntimeDeps(
            format_rub_price=_v(ctx, "_format_rub_price"),
            fallback_contextual_question=_v(ctx, "_fallback_contextual_question"),
            persona_driven_question_for_fact=_v(ctx, "_persona_driven_question_for_fact"),
            question_covers_fact=_v(ctx, "_question_covers_fact"),
            is_repeated_question_against_state=_v(ctx, "_is_repeated_question_against_state"),
            generic_question_for_fact=_v(ctx, "_generic_question_for_fact"),
            normalize_text=_v(ctx, "_normalize_text"),
            is_dimension_like_value=_v(ctx, "_is_dimension_like_value"),
            selected_item_attribute_answer=_v(ctx, "_selected_item_attribute_answer"),
            normalize_model_alias=_v(ctx, "_normalize_model_alias"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            needs_stopwords=_v(ctx, "NEEDS_STOPWORDS"),
            generic_model_words=_v(ctx, "_GENERIC_MODEL_WORDS"),
        )
    )


def build_tenant_runtime_deps(
    ctx: Mapping[str, Any],
    *,
    settings_obj: Any,
    logger_obj: Any,
) -> TenantRuntimeDeps:
    tenant_config_repo = _tenant_configs_module()
    return TenantRuntimeDeps(
        settings=settings_obj,
        logger=logger_obj,
        yaml_module=_v(ctx, "yaml"),
        root_dir=_v(ctx, "ROOT_DIR"),
        data_dir=_v(ctx, "DATA_DIR"),
        tenants_dir=_v(ctx, "TENANTS_DIR"),
        tenant_config_dir=_v(ctx, "TENANT_CONFIG_DIR"),
        default_tenant_json=_v(ctx, "DEFAULT_TENANT_JSON"),
        default_persona_md=_v(ctx, "DEFAULT_PERSONA_MD"),
        persona_md_fallback=ctx.get("PERSONA_MD", _v(ctx, "DEFAULT_PERSONA_MD")),
        tenant_config_cache=_v(ctx, "_TENANT_CONFIG_CACHE"),
        tenant_persona_cache=_v(ctx, "_TENANT_PERSONA_CACHE"),
        persona_hints_cache=_v(ctx, "_PERSONA_HINTS_CACHE"),
        clear_persona_hints_cache=_v(ctx, "_clear_persona_hints_cache"),
        coerce_bool=_v(ctx, "_coerce_bool"),
        tenant_config_db_get=(
            tenant_config_repo.get if tenant_config_repo is not None else None
        ),
        tenant_config_db_upsert=(
            tenant_config_repo.upsert if tenant_config_repo is not None else None
        ),
    )


def build_policy_plan_runtime(ctx: Mapping[str, Any]) -> PolicyPlanRuntime:
    return PolicyPlanRuntime(
        PolicyPlanRuntimeDeps(
            classify_turn_intent=_v(ctx, "_classify_turn_intent"),
            fact_token_re=_v(ctx, "_FACT_TOKEN_RE"),
            model_name_intent_re=_v(ctx, "_MODEL_NAME_INTENT_RE"),
            generic_model_words=_v(ctx, "_GENERIC_MODEL_WORDS"),
            read_catalog=_v(ctx, "_read_catalog"),
            best_catalog_item_match=_v(ctx, "_best_catalog_item_match"),
            item_label=_v(ctx, "_item_label"),
            safe_short_text=lambda text: _v(ctx, "_safe_short_text")(text, limit=120),
            slot_aliases=_v(ctx, "_SLOT_ALIASES"),
            question_topic_to_slot=_v(ctx, "_QUESTION_TOPIC_TO_SLOT"),
            question_fingerprint_fn=_v(ctx, "quality").question_fingerprint,
            state_facts_snapshot=_v(ctx, "_state_facts_snapshot"),
            normalize_required_facts=_v(ctx, "_normalize_required_facts"),
            missing_required_facts=_v(ctx, "_missing_required_facts"),
            canonical_fact_key=_v(ctx, "_canonical_fact_key"),
            question_covers_fact=_v(ctx, "_question_covers_fact"),
        )
    )


def build_conversation_runtime(ctx: Mapping[str, Any]) -> ConversationRuntime:
    return ConversationRuntime(
        ConversationRuntimeDeps(
            infer_user_needs=_v(ctx, "infer_user_needs"),
            coerce_bool=_v(ctx, "_coerce_bool"),
            env_bool=_v(ctx, "_env_bool"),
            remember_question_state=_v(ctx, "_remember_question_state"),
            remember_cta_state=_v(ctx, "_remember_cta_state"),
            cta_allowed=_v(ctx, "_cta_allowed"),
            format_needs_for_prompt=_v(ctx, "format_needs_for_prompt"),
            persona_meta_config=_v(ctx, "persona_meta_config"),
            load_tenant=_v(ctx, "load_tenant"),
            branding_for_tenant=_v(ctx, "_branding_for_tenant"),
            load_sales_state=_v(ctx, "load_sales_state"),
            load_persona_hints=_v(ctx, "load_persona_hints"),
            save_sales_state=_v(ctx, "save_sales_state"),
            default_tenant_json=_v(ctx, "DEFAULT_TENANT_JSON"),
            search_catalog=_v(ctx, "search_catalog"),
            pick_cta=_v(ctx, "pick_cta"),
            entry_apply_persona_need_mappings=_v(ctx, "_entry_apply_persona_need_mappings"),
            entry_observe_user_message=_v(ctx, "_entry_observe_user_message"),
            entry_summarize_sales_state=_v(ctx, "_entry_summarize_sales_state"),
            entry_record_bot_reply=_v(ctx, "_entry_record_bot_reply"),
            entry_make_rule_based_reply=_v(ctx, "_entry_make_rule_based_reply"),
        )
    )


def build_llm_entry_runtime(ctx: Mapping[str, Any]) -> LlmEntryRuntime:
    return LlmEntryRuntime(
        LlmEntryRuntimeDeps(
            message_runtime_getter=_v(ctx, "_message_runtime"),
            decision_runtime_getter=_v(ctx, "_decision_runtime"),
            reply_runtime_getter=_v(ctx, "_reply_runtime"),
            brain_mode_human_reply_enabled=_v(ctx, "_brain_mode_human_reply_enabled"),
            brain_mode_resolve=_v(ctx, "_brain_mode_resolve"),
            env_bool=_v(ctx, "_env_bool"),
            coerce_bool=_v(ctx, "_coerce_bool"),
            load_tenant=_v(ctx, "load_tenant"),
        )
    )
