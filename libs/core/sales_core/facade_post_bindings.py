from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping


def install_post_runtime_bindings(
    ctx: MutableMapping[str, Any],
    *,
    data_dir: Path,
    root_dir: Path,
    build_default_tenant_json: Callable[[Path], Mapping[str, Any]],
    load_default_persona_md: Callable[[Path], str],
    bind_private: Callable[..., None],
    bind_named: Callable[..., None],
    delegate_sync: Callable[..., Any],
    delegate_async: Callable[..., Any],
    format_items_for_prompt: Callable[..., str],
    format_needs_for_prompt: Callable[..., str],
    pick_cta: Callable[..., Mapping[str, str]],
    analyze_sentiment_delta: Callable[[str], float],
    normalize_text: Callable[[str], str],
    match_key: Callable[..., Any],
    collect_item_text: Callable[..., str],
    tokenize_query: Callable[..., list[str]],
    text_match_score: Callable[..., float],
    global_color_aliases: Mapping[str, tuple[str, ...]] | Mapping[str, Any],
) -> None:
    tenant_runtime = lambda: ctx["_tenant_runtime"]()
    catalog_loader_runtime = ctx["_catalog_loader_runtime"]
    needs_runtime = ctx["_needs_runtime"]
    catalog_search_runtime = ctx["_catalog_search_runtime"]
    conversation_runtime = ctx["_conversation_runtime"]
    llm_entry_runtime = ctx["_llm_entry_runtime"]
    reply_runtime = ctx["_reply_runtime"]

    ctx["DEFAULT_TENANT_JSON"] = build_default_tenant_json(data_dir)
    ctx["DEFAULT_PERSONA_MD"] = load_default_persona_md(root_dir)

    bind_private(
        tenant_runtime,
        "ensure_passport_public_key",
        "merge_dicts",
        "load_external_tenant_config",
        "normalize_tenant_config",
        "persist_pdf_index_metadata",
        "persona_cache_key",
        "persona_path",
        "branding_for_tenant",
    )
    bind_named(
        tenant_runtime,
        {
            "tenant_dir": "tenant_dir",
            "ensure_tenant_files": "ensure_tenant_files",
            "read_tenant_config": "read_tenant_config",
            "write_tenant_config": "write_tenant_config",
            "read_persona": "read_persona",
            "write_persona": "write_persona",
            "load_tenant": "load_tenant",
        },
    )

    bind_private(tenant_runtime, "resolve_persona_relative_path", "normalize_catalog_pdf_candidate")
    bind_named(
        tenant_runtime,
        {
            "load_persona": "load_persona",
            "load_persona_structured": "load_persona_structured",
            "persona_meta_config": "persona_meta_config",
            "persona_catalog_pdf": "persona_catalog_pdf",
            "persona_catalog_csv": "persona_catalog_csv",
            "resolve_catalog_pdf_meta": "resolve_catalog_pdf_meta",
        },
    )

    ctx["CATALOG_CSV"] = data_dir / "catalog_sample.csv"

    bind_private(
        ctx["_catalog_csv_runtime"],
        "canonicalize_field_name",
        "merge_csv_mapping_meta",
        "prepare_field_mapping",
        "has_price_digits",
        "normalize_csv_delimiter",
        "csv_delimiter_candidates",
        "read_csv_rows_with_delimiter",
        "score_csv_rows",
        "read_csv_rows_best",
        "normalize_catalog_item",
        "normalize_catalog_items",
    )
    bind_private(
        ctx["_catalog_rules_runtime"],
        "apply_catalog_attribute_rules",
        "catalog_rule_matches",
        "catalog_condition_matches",
        "filter_catalog_items_by_rules",
        "ensure_list",
        "needs_block_matches",
        "item_fields_match",
    )

    bind_private(catalog_loader_runtime, "read_catalog")
    bind_named(
        catalog_loader_runtime,
        {
            "read_all_catalog": "read_all_catalog",
            "paginate_catalog_text": "paginate_catalog_text",
        },
    )
    bind_private(needs_runtime, "normalize_color_token", "normalize_alias_map")

    ctx["_GLOBAL_COLOR_ALIASES"] = global_color_aliases

    bind_private(
        needs_runtime,
        "persona_color_alias_map",
        "augment_color_needs",
        "build_color_lookup_map",
        "collect_color_text",
        "enrich_catalog_color_aliases",
        "extract_budget",
        "extract_price_order_intent",
        "looks_like_price_objection",
    )
    bind_named(needs_runtime, {"infer_user_needs": "infer_user_needs"})
    bind_named(
        catalog_search_runtime,
        {
            "_value_matches": "_value_matches",
            "_score": "_score",
        },
    )
    ctx["_normalize_text"] = normalize_text
    ctx["_match_key"] = match_key
    ctx["_collect_item_text"] = collect_item_text
    ctx["_tokenize_query"] = tokenize_query
    ctx["_tag_boost"] = lambda _item: 0.0
    ctx["_text_match_score"] = text_match_score
    bind_named(
        catalog_search_runtime,
        {
            "_legacy_rank_catalog": "_legacy_rank_catalog",
            "_catalog_item_identity": "_catalog_item_identity",
            "_merge_catalog_results": "_merge_catalog_results",
            "_sort_catalog_by_price_order": "_sort_catalog_by_price_order",
            "search_catalog": "search_catalog",
        },
    )

    ctx["format_items_for_prompt"] = format_items_for_prompt
    ctx["format_needs_for_prompt"] = format_needs_for_prompt
    ctx["pick_cta"] = pick_cta
    ctx["analyze_sentiment_delta"] = analyze_sentiment_delta

    bind_private(
        conversation_runtime,
        "build_sales_conversation_engine",
        "conversation_entrypoint_deps",
        "apply_persona_need_mappings",
    )
    bind_named(
        conversation_runtime,
        {
            "observe_user_message": "observe_user_message",
            "summarize_sales_state": "summarize_sales_state",
            "record_bot_reply": "record_bot_reply",
            "make_rule_based_reply": "make_rule_based_reply",
        },
    )

    ctx["build_llm_messages"] = delegate_async(llm_entry_runtime, "build_llm_messages")
    ctx["_wrap_llm_reply"] = delegate_sync(reply_runtime, "wrap_llm_reply")
    ctx["_direct_llm_reply"] = delegate_async(llm_entry_runtime, "direct_llm_reply")
    ctx["_human_reply_mode_enabled"] = delegate_sync(llm_entry_runtime, "human_reply_mode_enabled")
    ctx["_resolve_brain_mode"] = delegate_sync(llm_entry_runtime, "resolve_brain_mode")
    ctx["_build_human_mode_messages"] = delegate_sync(llm_entry_runtime, "build_human_mode_messages")
    ctx["_human_llm_reply"] = delegate_async(llm_entry_runtime, "human_llm_reply")
    ctx["_single_llm_reply"] = delegate_async(llm_entry_runtime, "single_llm_reply")
    ctx["ask_llm"] = delegate_async(llm_entry_runtime, "ask_llm")
