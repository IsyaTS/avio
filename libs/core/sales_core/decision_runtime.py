from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Sequence

from .llm_response_text import extract_llm_response_text


@dataclass(frozen=True)
class DecisionRuntimeDeps:
    # shortlist followup
    normalize_text: Callable[[Any], str]
    classify_turn_intent: Callable[[str], str]
    fallback_contextual_question: Callable[..., str]
    generic_question_for_fact: Callable[[str], str]
    read_all_catalog: Callable[..., List[Dict[str, Any]]]
    catalog_item_identity: Callable[[Dict[str, Any]], str]
    item_mm_value: Callable[..., float | None]
    item_number_value: Callable[..., float]
    extract_attribute_probe: Callable[[str], str]
    is_shortlist_feedback_turn: Callable[..., bool]
    looks_like_contextual_short_followup: Callable[[str], bool]
    shortlist_preview_text: Callable[..., str]
    render_shortlist_preview_reply: Callable[..., str]
    is_price_intent: Callable[[str], bool]
    looks_like_price_objection: Callable[[str], bool]
    extract_price_order_intent: Callable[[str], str | None]
    item_price_int: Callable[[Dict[str, Any]], int | None]
    persona_driven_question_for_fact: Callable[..., str]
    question_covers_fact: Callable[[str, str], bool]
    is_repeated_question_against_state: Callable[[str, Any], bool]
    items_with_attribute: Callable[[Sequence[Mapping[str, Any]], str], List[Mapping[str, Any]]]
    shortlist_attribute_answer: Callable[[str, Sequence[Mapping[str, Any]]], str]
    best_numeric_attribute_delta_line: Callable[[Sequence[Mapping[str, Any]], Sequence[Mapping[str, Any]]], str]
    format_rub_price: Callable[[int], str]

    # catalog truth guard
    grounding_catalog_items: Callable[[Mapping[str, Any] | None], List[Dict[str, Any]]]
    enforce_catalog_model_grounding: Callable[..., str]
    enforce_catalog_price_grounding: Callable[..., str]
    reply_mentions_catalog_item: Callable[[str, Sequence[Mapping[str, Any]]], bool]
    extract_price_spans: Callable[[str], List[tuple[int, int, int]]]
    is_catalog_price_candidate: Callable[[int, set[int] | None], bool]
    catalog_has_object_type_evidence: Callable[[Sequence[Mapping[str, Any]], str], bool]
    object_type_from_turn_text: Callable[[str], str]
    filter_items_by_object_type_need: Callable[[Sequence[Mapping[str, Any]], Mapping[str, Any]], List[Dict[str, Any]]]
    selected_item_from_grounding: Callable[..., Mapping[str, Any] | None]
    item_object_type_hint: Callable[[Mapping[str, Any]], str]
    catalog_item_is_two_panel: Callable[[Mapping[str, Any]], bool]
    infer_user_needs: Callable[[str], Dict[str, Any]]
    is_specific_catalog_keyword: Callable[[str], bool]
    normalize_probe_token: Callable[[str], str]
    items_with_attribute_direct: Callable[[Sequence[Mapping[str, Any]], str], List[Mapping[str, Any]]]
    extract_budget_cap_from_needs: Callable[[Mapping[str, Any]], int | None]
    closest_catalog_item_by_price: Callable[[Sequence[Mapping[str, Any]], int], Mapping[str, Any] | None]
    item_label: Callable[[Mapping[str, Any]], str]
    reply_mentions_unknown_model: Callable[[str, Sequence[Mapping[str, Any]]], bool]
    neutralize_unknown_model_mentions: Callable[[str, Sequence[Mapping[str, Any]]], str]
    extract_explicit_model_probe: Callable[[str], str]
    strict_catalog_item_match: Callable[[str, Sequence[Mapping[str, Any]]], Mapping[str, Any] | None]
    has_unverified_priced_labels: Callable[[str, Sequence[Mapping[str, Any]]], bool]
    neutralize_unverified_priced_labels: Callable[[str, Sequence[Mapping[str, Any]]], str]
    selected_item_attribute_answer: Callable[[str, Mapping[str, Any]], str]
    narrow_catalog_items_by_user_text: Callable[[Sequence[Mapping[str, Any]], str], List[Dict[str, Any]]]
    negative_attribute_probes: Callable[[str], List[str]]
    exclude_items_with_negative_probes: Callable[[Sequence[Mapping[str, Any]], Sequence[str]], List[Dict[str, Any]]]
    catalog_extreme_item_by_price: Callable[[Sequence[Mapping[str, Any]], bool], Mapping[str, Any] | None]
    extract_price_target_hint: Callable[[str], int | None]
    catalog_min_price: Callable[[Sequence[Mapping[str, Any]]], int]
    neutralize_catalog_model_mentions: Callable[[str, Sequence[Mapping[str, Any]]], str]
    is_likely_price_value: Callable[[int], bool]

    model_name_intent_re: Any
    variants_user_hint_re: Any
    min_price_intent_re: Any
    max_price_intent_re: Any
    sentence_split_re: Any
    mentioned_catalog_item_ids: Callable[[str, Sequence[Mapping[str, Any]]], set[str]]
    best_catalog_item_match: Callable[[str, Sequence[Mapping[str, Any]]], Mapping[str, Any] | None]
    normalize_model_alias: Callable[[str], str]
    item_aliases: Callable[[Mapping[str, Any]], list[str]]
    fact_token_re: Any
    generic_fact_stopwords: set[str]

    # fallback reply runtime
    load_sales_state: Callable[[int | None, int], Any]
    load_persona_hints: Callable[[int | None, str], Any]
    load_persona: Callable[[int | None, str], str]
    branding_for_tenant: Callable[[int | None, str], Dict[str, str]]
    state_facts_snapshot: Callable[[Any], Dict[str, str]]
    persona_direct_reply_for_user_turn: Callable[..., str]
    build_reply_grounding: Callable[..., Mapping[str, Any]]
    search_catalog: Callable[..., List[Dict[str, Any]]]
    apply_base_answer_quality_floor: Callable[..., str]
    humanize_reply_text: Callable[..., str]
    ensure_dialog_greeting_on_first_reply: Callable[..., str]
    save_sales_state: Callable[[Any], None]
    canonical_fact_key: Callable[[str], str]
    required_facts_from_persona_text: Callable[[str], List[str]]
    missing_required_facts: Callable[[Sequence[str], Mapping[str, str]], List[str]]
    prioritize_missing_facts: Callable[..., List[str]]
    persona_script_questions: Callable[[str], List[str]]
    persona_primary_script_question: Callable[..., str]
    selected_item_brief_answer: Callable[[Mapping[str, Any]], str]
    explain_missing_fact_need: Callable[..., str]
    normalize_slot_name: Callable[[str, str], str]
    extract_questions_from_text: Callable[[str], List[str]]
    fact_keys_from_line: Callable[[str], List[str]]
    apply_persona_sequence_obligations: Callable[..., str]
    apply_persona_delivery_obligations: Callable[..., str]
    update_fact_memory: Callable[[Any, str], None]
    remember_questions_from_reply: Callable[[Any, str], None]
    persona_catalog_unavailable_reply: Callable[[str], str]
    order_intent_re: Any
    catalog_unavailable_re: Any
    low_signal_context_re: Any

    # required-fact question enforcement
    reply_contains_unconfirmed_required_claim: Callable[..., bool]
    has_substantive_non_question_payload: Callable[[str], bool]
    is_payment_intent: Callable[[str], bool]
    is_store_address_intent: Callable[[str], bool]
    is_channel_handoff_intent: Callable[[str], bool]
    is_deferral_message: Callable[[str], bool]
    extract_store_addresses_from_persona: Callable[[str], Dict[str, str]]
    extract_city_hint: Callable[..., str]
    question_lists_catalog_options: Callable[[str, Sequence[Mapping[str, Any]]], bool]
    replace_reply_question: Callable[[str, str], str]

    # persona sequence obligations
    strip_instruction_leaks: Callable[[str], str]
    compile_persona_rules: Callable[[str], Any]
    strip_embedded_operator_tail: Callable[[str], str]
    count_sentences: Callable[[str], int]
    fact_fingerprint: Callable[[str], str]
    sales_state_cls: type
    conditional_rule_matches: Callable[..., bool]

    # audit & rewrite
    llm_call_with_deadline: Callable[..., Any]
    safe_json_load: Callable[[str], dict[str, Any] | None]
    enforce_sentence_budget: Callable[[str, int], str]
    rewrite_loses_context_anchors: Callable[[str, str, Sequence[Mapping[str, Any]]], bool]
    reply_has_repeated_question: Callable[[str, Any], bool]
    format_items_for_prompt: Callable[[Sequence[Mapping[str, Any]], str], str]
    quality_dedupe_repeated_blocks: Callable[[str], str]
    contact_url_re: Any
    contact_handle_re: Any
    contact_phone_re: Any
    greeting_prefix_re: Any
    question_cue_re: Any
    repair_turn_re: Any
    price_inline_re: Any

    # single llm reply orchestration
    env_bool: Callable[[str, bool], bool]
    safe_short_text: Callable[[str, int], str]
    normalize_fact_key: Callable[[str], str]
    display_item_label: Callable[[Mapping[str, Any]], str]
    resolve_persona_rules_context: Callable[..., str]
    resolve_chat_completion_callable: Callable[[Any], Any]
    build_human_mode_messages: Callable[[List[Dict[str, str]]], List[Dict[str, str]]]
    render_direct_reply: Callable[..., Any]
    stabilize_followup_price_reference: Callable[..., str]
    wrap_llm_reply: Callable[..., str]
    record_bot_reply: Callable[[int, int | None, str, str], None]
    llm_unavailable_reply: Callable[..., str]
    is_quota_or_rate_limit_error: Callable[[Exception], bool]
    ensure_dialog_greeting_on_first_reply: Callable[..., str]
    settings_obj: Any
    logger_obj: Any
    api_timeout_error_cls: type


class DecisionRuntime:
    def __init__(self, deps: DecisionRuntimeDeps) -> None:
        self.deps = deps

    def shortlist_comparison_followup_plan(
        self,
        user_text: str,
        items: Sequence[Mapping[str, Any]],
        *,
        tenant: int | None,
        persona_context: str = "",
        state: Any = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        _normalize_text = self.deps.normalize_text
        _classify_turn_intent = self.deps.classify_turn_intent
        _fallback_contextual_question = self.deps.fallback_contextual_question
        _generic_question_for_fact = self.deps.generic_question_for_fact
        read_all_catalog = self.deps.read_all_catalog
        _catalog_item_identity = self.deps.catalog_item_identity
        _item_mm_value = self.deps.item_mm_value
        _item_number_value = self.deps.item_number_value
        _extract_attribute_probe = self.deps.extract_attribute_probe
        _is_shortlist_feedback_turn = self.deps.is_shortlist_feedback_turn
        _looks_like_contextual_short_followup = self.deps.looks_like_contextual_short_followup
        _shortlist_preview_text = self.deps.shortlist_preview_text
        _render_shortlist_preview_reply = self.deps.render_shortlist_preview_reply
        _is_price_intent = self.deps.is_price_intent
        _looks_like_price_objection = self.deps.looks_like_price_objection
        _extract_price_order_intent = self.deps.extract_price_order_intent
        _item_price_int = self.deps.item_price_int
        _persona_driven_question_for_fact = self.deps.persona_driven_question_for_fact
        _question_covers_fact = self.deps.question_covers_fact
        _is_repeated_question_against_state = self.deps.is_repeated_question_against_state
        _items_with_attribute = self.deps.items_with_attribute
        _shortlist_attribute_answer = self.deps.shortlist_attribute_answer
        _best_numeric_attribute_delta_line = self.deps.best_numeric_attribute_delta_line
        _format_rub_price = self.deps.format_rub_price

        shortlist = [dict(item) for item in list(items or [])[:4] if isinstance(item, Mapping)]
        if not shortlist:
            return "", []
        low = _normalize_text(user_text)
        turn_intent = _classify_turn_intent(user_text)
        if turn_intent == "repair":
            repair = _fallback_contextual_question(
                user_text,
                state=state,
                persona_context=persona_context,
            ) or _generic_question_for_fact("model")
            return (repair, shortlist[:2])
        current_thicknesses = [
            val
            for val in (
                _item_mm_value(item, "Толщина полотна", "Толщина короба")
                for item in shortlist
            )
            if val is not None
        ]
        if not current_thicknesses:
            return "", shortlist[:2]
        current_max = max(current_thicknesses)
        current_min = min(current_thicknesses)
        pending_model_followup = str(getattr(state, "pending_fact_key", "") or "").strip().lower() == "model"

        def _build_non_repeating_alternatives(
            *,
            prefer_thicker: bool,
            min_thickness_floor: float | None = None,
        ) -> list[dict[str, Any]]:
            if tenant is None:
                return []
            try:
                catalog_items = read_all_catalog(tenant=tenant)
            except Exception:
                catalog_items = []
            if not catalog_items:
                return []
            shortlist_ids = {_catalog_item_identity(dict(item)) for item in shortlist}
            alternatives: list[Mapping[str, Any]] = []
            for item in catalog_items:
                identity = _catalog_item_identity(dict(item))
                if identity in shortlist_ids:
                    continue
                thickness = _item_mm_value(item, "Толщина полотна", "Толщина короба")
                if prefer_thicker:
                    if thickness is None or thickness <= current_max:
                        continue
                if min_thickness_floor is not None:
                    if thickness is None or thickness < min_thickness_floor:
                        continue
                alternatives.append(item)
            if not alternatives:
                return []
            alternatives = sorted(
                alternatives,
                key=lambda item: (
                    _item_number_value(item, "price", "Цена"),
                    _item_mm_value(item, "Толщина полотна", "Толщина короба") or 0,
                ),
            )
            return [dict(item) for item in alternatives[:2]]

        attr_probe = _extract_attribute_probe(user_text)
        asks_attribute = bool(attr_probe)
        asks_variants = _is_shortlist_feedback_turn(user_text)
        if _looks_like_contextual_short_followup(user_text):
            if not pending_model_followup:
                prefer_thicker = current_max <= 50
                min_floor = current_min if current_min >= 60 else None
                alternatives = _build_non_repeating_alternatives(
                    prefer_thicker=prefer_thicker,
                    min_thickness_floor=min_floor,
                )
                if alternatives:
                    preview = _shortlist_preview_text(alternatives, limit=2)
                    if preview:
                        delta = _best_numeric_attribute_delta_line(shortlist[:2], alternatives[:2])
                        preview_text = f"{preview} {delta}".strip()
                        return (
                            _render_shortlist_preview_reply(
                                preview_text,
                                ask_detail=True,
                                persona_context=persona_context,
                                state=state,
                                user_text=user_text,
                            ),
                            alternatives,
                        )
            preview = _shortlist_preview_text(shortlist[:2], limit=2)
            if preview:
                return (
                    _render_shortlist_preview_reply(
                        preview,
                        ask_detail=True,
                        persona_context=persona_context,
                        state=state,
                        user_text=user_text,
                    ),
                    shortlist[:2],
                )
            return "", shortlist[:2]
        if _is_price_intent(user_text) or _looks_like_price_objection(user_text):
            asks_cheaper = _extract_price_order_intent(user_text) == "asc"
            prices = [_item_price_int(dict(item)) for item in shortlist]
            valid_prices = [int(price) for price in prices if isinstance(price, int) and price > 0]
            price_order = _extract_price_order_intent(user_text)
            if (price_order == "asc" or asks_cheaper) and valid_prices and tenant is not None:
                try:
                    catalog_items = read_all_catalog(tenant=tenant)
                except Exception:
                    catalog_items = []
                if catalog_items:
                    shortlist_ids = {_catalog_item_identity(dict(item)) for item in shortlist}
                    cheaper: list[Mapping[str, Any]] = []
                    price_floor = min(valid_prices)
                    for item in catalog_items:
                        identity = _catalog_item_identity(dict(item))
                        if identity in shortlist_ids:
                            continue
                        item_price = _item_price_int(dict(item))
                        if not isinstance(item_price, int) or item_price <= 0:
                            continue
                        if item_price < price_floor:
                            cheaper.append(item)
                    if cheaper:
                        cheaper = sorted(
                            cheaper,
                            key=lambda item: _item_number_value(item, "price", "Цена"),
                        )
                        cheaper_items = [dict(item) for item in cheaper[:2]]
                        preview = _shortlist_preview_text(cheaper_items, limit=2)
                        if preview:
                            return (
                                _render_shortlist_preview_reply(
                                    preview,
                                    ask_detail=True,
                                    persona_context=persona_context,
                                    state=state,
                                    user_text=user_text,
                                ),
                                cheaper_items,
                            )
            if valid_prices:
                min_price = min(valid_prices)
                max_price = max(valid_prices)
                budget_question = _persona_driven_question_for_fact(
                    persona_context,
                    "budget",
                    state=state,
                )
                budget_question = str(budget_question or "").strip()
                if asks_cheaper:
                    price_text = f"Сейчас самый доступный вариант — {_format_rub_price(min_price)}."
                    if budget_question:
                        return (f"{price_text} {budget_question}".strip(), shortlist[:2])
                    return (price_text, shortlist[:2])
                if min_price == max_price:
                    price_text = f"Цена: {_format_rub_price(min_price)}."
                else:
                    price_text = f"Цена: {_format_rub_price(min_price)} - {_format_rub_price(max_price)}."
                if budget_question:
                    return (f"{price_text} {budget_question}".strip(), shortlist[:2])
                return (price_text, shortlist[:2])
            budget_question = _persona_driven_question_for_fact(persona_context, "budget", state=state)
            if budget_question:
                return (budget_question, shortlist[:2])
            return (_generic_question_for_fact("budget"), shortlist[:2])
        if asks_attribute:
            probe = _extract_attribute_probe(user_text)
            if probe:
                matched_items = _items_with_attribute(shortlist[:2], probe)
                if matched_items:
                    matched_preview = _shortlist_preview_text(matched_items[:2], limit=2)
                    if matched_preview:
                        return (
                            _render_shortlist_preview_reply(
                                matched_preview,
                                ask_detail=True,
                                persona_context=persona_context,
                                state=state,
                                user_text=user_text,
                            ),
                            [dict(item) for item in matched_items[:2]],
                        )
            attr_answer = _shortlist_attribute_answer(user_text, shortlist[:2])
            attr_answer_norm = _normalize_text(attr_answer)
            last_reply_norm = _normalize_text(str(getattr(state, "last_bot_reply", "") or "")) if state is not None else ""
            if not pending_model_followup:
                alternatives = _build_non_repeating_alternatives(
                    prefer_thicker=(current_max <= 50),
                    min_thickness_floor=current_min if current_min >= 50 else None,
                )
                if alternatives:
                    preview = _shortlist_preview_text(alternatives, limit=2)
                    if preview:
                        delta = _best_numeric_attribute_delta_line(shortlist[:2], alternatives[:2])
                        preview_text = f"{preview} {delta}".strip()
                        return (
                            _render_shortlist_preview_reply(
                                preview_text,
                                ask_detail=True,
                                persona_context=persona_context,
                                state=state,
                                user_text=user_text,
                            ),
                            alternatives,
                        )
            if attr_answer and attr_answer_norm and attr_answer_norm != last_reply_norm:
                return (attr_answer, shortlist[:2])
            if attr_answer:
                return (attr_answer, shortlist[:2])
            preview = _shortlist_preview_text(shortlist[:2], limit=2)
            if preview:
                return (
                    _render_shortlist_preview_reply(
                        preview,
                        ask_detail=True,
                        persona_context=persona_context,
                        state=state,
                        user_text=user_text,
                    ),
                    shortlist[:2],
                )
            return "", shortlist[:2]

        if not asks_variants:
            return "", shortlist[:2]
        alternatives = _build_non_repeating_alternatives(prefer_thicker=False)
        if alternatives:
            preview = _shortlist_preview_text(alternatives, limit=2)
            if preview:
                delta = _best_numeric_attribute_delta_line(shortlist[:2], alternatives[:2])
                preview_text = f"{preview} {delta}".strip()
                return (
                    _render_shortlist_preview_reply(
                        preview_text,
                        ask_detail=True,
                        persona_context=persona_context,
                        state=state,
                        user_text=user_text,
                    ),
                    alternatives,
                )
        preview = _shortlist_preview_text(shortlist[:2], limit=2)
        if preview:
            return (
                _render_shortlist_preview_reply(
                    preview,
                    ask_detail=True,
                    persona_context=persona_context,
                    state=state,
                    user_text=user_text,
                ),
                shortlist[:2],
            )
        return "", shortlist[:2]

    def apply_persona_sequence_obligations(
        self,
        reply: str,
        *,
        persona_context: str,
        last_user_message: str,
        known_facts: Mapping[str, str] | None = None,
        state: Any = None,
    ) -> str:
        _classify_turn_intent = self.deps.classify_turn_intent
        _strip_instruction_leaks = self.deps.strip_instruction_leaks
        _canonical_fact_key = self.deps.canonical_fact_key
        _extract_attribute_probe = self.deps.extract_attribute_probe
        _MODEL_NAME_INTENT_RE = self.deps.model_name_intent_re
        _VARIANTS_USER_HINT_RE = self.deps.variants_user_hint_re
        _compile_persona_rules = self.deps.compile_persona_rules
        _normalize_text = self.deps.normalize_text
        _FACT_TOKEN_RE = self.deps.fact_token_re
        _GENERIC_FACT_STOPWORDS = self.deps.generic_fact_stopwords
        _strip_embedded_operator_tail = self.deps.strip_embedded_operator_tail
        _count_sentences = self.deps.count_sentences
        _SENTENCE_SPLIT_RE = self.deps.sentence_split_re
        _fact_fingerprint = self.deps.fact_fingerprint
        _conditional_rule_matches = self.deps.conditional_rule_matches
        SalesState = self.deps.sales_state_cls

        candidate = (reply or "").strip()
        persona_text = str(persona_context or "").strip()
        if not candidate or not persona_text:
            return candidate
        facts = dict(known_facts or {})
        turn_intent = _classify_turn_intent(last_user_message, known_facts=facts)
        if turn_intent in {"why_question", "repair", "catalog_problem"}:
            return _strip_instruction_leaks(candidate)
        if isinstance(state, SalesState) and _canonical_fact_key(str(state.pending_fact_key or "")) == "model":
            low_candidate = _normalize_text(candidate)
            if (
                "модел" in low_candidate
                or "каталог" in low_candidate
                or "вариант" in low_candidate
            ):
                return _strip_instruction_leaks(candidate)
        if str(facts.get("model") or "").strip():
            if (
                bool(_extract_attribute_probe(last_user_message))
                or bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or "")))
                or bool(_VARIANTS_USER_HINT_RE.search(str(last_user_message or "")))
            ):
                return _strip_instruction_leaks(candidate)
        compiled = _compile_persona_rules(persona_text)
        if not compiled.conditionals:
            return candidate

        def _normalize_action_for_reply(action_text: str) -> str:
            raw = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(action_text or "")).strip()
            raw = raw.replace("`", " ")
            if not raw:
                return ""
            low = raw.lower().replace("ё", "е")
            if low.startswith("не "):
                return ""
            # Persona sometimes stores meta-instructions like:
            # "в этом же ответе добавьте: "..." и "...""
            # Extract quoted user-facing fragments first.
            quoted_parts = [
                str(part or "").strip()
                for part in re.findall(r"[\"«]([^\"»]{3,220})[\"»]", raw)
                if str(part or "").strip()
            ]
            if quoted_parts:
                normalized_chunks: list[str] = []
                for chunk in quoted_parts:
                    raw_chunk = str(chunk or "")
                    if re.search(r"<\s*[^>\n]{1,40}\s*>", raw_chunk):
                        continue
                    chunk_clean = re.sub(r"<\s*[^>\n]{1,40}\s*>", "", raw_chunk).strip()
                    if not chunk_clean:
                        continue
                    if chunk_clean[-1] not in ".!?":
                        chunk_clean += "."
                    normalized_chunks.append(chunk_clean)
                merged = " ".join(normalized_chunks).strip()
                if merged:
                    return merged[:220].strip()
            # Question-like directives should stay in planning, not in final text.
            if re.match(
                r"(?iu)^\s*(?:сначала|затем|потом|далее|обязательно|просто)?\s*"
                r"(спросите|уточните|попросите|напишите|задайте)\b",
                low,
            ):
                return ""
            # Internal infinitive instructions should not leak into user text.
            if re.match(
                r"(?iu)^\s*(?:сначала|затем|потом|далее|обязательно|просто)?\s*"
                r"(предупредить|уточнить|добавить|сообщить|указать|"
                r"предложить|спросить|попросить|написать|отправить|"
                r"перевести|соотнести|проверить|зафиксировать)\b",
                low,
            ):
                return ""
            # Purely internal delivery instructions should not leak to user text.
            if re.search(
                r"(?iu)\b(в\s+этом\s+же\s+ответе|в\s+текущем\s+сообщении|"
                r"только\s+потом|переходить\s+к\s+уточнениям|"
                r"добавьте\s*:\s*$|дать\s+адрес\s+магазина\s+и\s+условие\s+скидки)\b",
                low,
            ):
                return ""
            # Pure operator directives ("how to ask") must not leak into user text.
            if re.search(
                r"(?iu)\b(при\s+известном\s+городе|без\s+повтора|"
                r"сразу\s+давайте\s+адрес|затем\s+один\s+уточняющ)\b",
                low,
            ):
                return ""
            # Convert imperative instruction stems to neutral phrasing.
            stripped = re.sub(
                r"(?iu)^\s*(?:честно\s+|сразу\s+|коротко\s+|обязательно\s+)*"
                r"(предлагайте|сообщайте|добавьте|укажите|пишите|отправляйте)\s+",
                "",
                raw,
            ).strip(" ,")
            if not stripped:
                stripped = raw
            stripped = _strip_embedded_operator_tail(stripped).strip()
            if not stripped:
                return ""
            low_stripped = stripped.lower().replace("ё", "е")
            # Root-cause guard: never allow operator imperatives to be appended as
            # user-visible text from persona conditional actions.
            if ("?" not in stripped) and re.search(
                r"(?iu)\b(поздоровайт\w*|поприветствуйт\w*|скажите|спросите|"
                r"уточните|напишите|дайте|предложите|предлагайте|попросите|задайте)\b",
                low_stripped,
            ):
                return ""
            # Minor normalization for common promo wording.
            if low_stripped.startswith("скидку "):
                stripped = "Действует скидка " + stripped[len("скидку ") :]
            elif low_stripped.startswith("скидка "):
                stripped = "Действует " + stripped
            if stripped and stripped[-1] not in ".!?":
                stripped += "."
            return stripped[:220].strip()

        def _reply_covers_action(reply_text: str, action_text: str) -> bool:
            rep = _normalize_text(reply_text)
            act = _normalize_text(action_text)
            if not rep or not act:
                return False
            action_numbers = set(re.findall(r"\d{2,}", action_text))
            if action_numbers and not action_numbers.issubset(set(re.findall(r"\d{2,}", reply_text))):
                return False
            action_tokens = [
                tok
                for tok in _FACT_TOKEN_RE.findall(act)
                if len(tok) >= 4 and tok not in _GENERIC_FACT_STOPWORDS
            ]
            if not action_tokens:
                return False
            hits = sum(1 for tok in action_tokens if tok in rep)
            return hits >= max(1, int(len(action_tokens) * 0.45))

        def _append_action_sentence(reply_text: str, sentence: str) -> str:
            base = str(reply_text or "").strip()
            addon = str(sentence or "").strip()
            if not addon:
                return base
            if not base:
                return addon
            if _count_sentences(base) >= 3:
                parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(base) if part.strip()]
                if parts:
                    parts[-1] = f"{parts[-1]} {addon}".strip()
                    return " ".join(parts).strip()
                return f"{base} {addon}".strip()
            if base.endswith((".", "!", "?")):
                return f"{base} {addon}".strip()
            return f"{base}. {addon}".strip()

        def _assistant_recently_covers_action(action_text: str, window: int = 6) -> bool:
            if not isinstance(state, SalesState):
                return False
            history = state.history if isinstance(state.history, list) else []
            if not history:
                return False
            checked = 0
            for item in reversed(history):
                role = str(item.get("role") or "").strip().lower()
                if role != "assistant":
                    continue
                content = str(item.get("content") or "").strip()
                if not content:
                    continue
                checked += 1
                if _reply_covers_action(content, action_text):
                    return True
                if checked >= max(1, int(window)):
                    break
            return False

        def _user_explicitly_requests_action(action_text: str) -> bool:
            user_msg = str(last_user_message or "").strip()
            if not user_msg:
                return False
            if _reply_covers_action(user_msg, action_text):
                return True
            action_tokens = [
                tok
                for tok in _FACT_TOKEN_RE.findall(_normalize_text(action_text))
                if len(tok) >= 4 and tok not in _GENERIC_FACT_STOPWORDS
            ]
            if not action_tokens:
                return False
            user_norm = _normalize_text(user_msg)
            hits = sum(1 for tok in action_tokens if tok in user_norm)
            user_tokens = [tok for tok in _FACT_TOKEN_RE.findall(user_norm) if len(tok) >= 3]
            if user_tokens and len(user_tokens) <= 5 and hits >= 1:
                return True
            return hits >= max(1, int(len(action_tokens) * 0.4))

        def _action_recently_in_memory(action_text: str) -> bool:
            if not isinstance(state, SalesState):
                return False
            fp = _fact_fingerprint(action_text)
            if not fp:
                return False
            recent = list(state.recent_fact_fingerprints or [])
            if fp in recent:
                return True
            cur_tokens = set(fp.split())
            if not cur_tokens:
                return False
            for prev in recent:
                prev_tokens = set(str(prev or "").split())
                if not prev_tokens:
                    continue
                overlap = len(cur_tokens & prev_tokens)
                if overlap < 3:
                    continue
                ratio = overlap / max(1, min(len(cur_tokens), len(prev_tokens)))
                if ratio >= 0.72:
                    return True
            return False

        appended_fingerprints: set[str] = set()
        out = candidate
        for rule in compiled.conditionals:
            if not _conditional_rule_matches(
                rule,
                last_user_message=last_user_message,
                known_facts=known_facts,
                state=state,
            ):
                continue
            action = str(rule.action_text or "").strip()
            if not action:
                continue
            action_for_reply = _normalize_action_for_reply(action)
            if not action_for_reply:
                continue
            if _assistant_recently_covers_action(action_for_reply) and (
                not _user_explicitly_requests_action(action_for_reply)
            ):
                continue
            action_parts = [
                part.strip()
                for part in _SENTENCE_SPLIT_RE.split(action_for_reply)
                if part and part.strip()
            ]
            if not action_parts:
                action_parts = [action_for_reply]
            for part in action_parts:
                part_fp = _fact_fingerprint(part)
                if part_fp and part_fp in appended_fingerprints:
                    continue
                if _reply_covers_action(out, part):
                    continue
                if _action_recently_in_memory(part) and (not _user_explicitly_requests_action(part)):
                    continue
                out = _append_action_sentence(out, part)
                if part_fp:
                    appended_fingerprints.add(part_fp)
        candidate = _strip_instruction_leaks(out)
        return candidate

    def enforce_next_required_fact_question(
        self,
        reply: str,
        *,
        state: Any,
        persona_context: str,
        known_facts: Mapping[str, str] | None = None,
        user_text: str = "",
        grounding: Mapping[str, Any] | None = None,
    ) -> tuple[str, str]:
        _required_facts_from_persona_text = self.deps.required_facts_from_persona_text
        _missing_required_facts = self.deps.missing_required_facts
        _state_facts_snapshot = self.deps.state_facts_snapshot
        _canonical_fact_key = self.deps.canonical_fact_key
        _classify_turn_intent = self.deps.classify_turn_intent
        _reply_contains_unconfirmed_required_claim = self.deps.reply_contains_unconfirmed_required_claim
        _has_substantive_non_question_payload = self.deps.has_substantive_non_question_payload
        _is_payment_intent = self.deps.is_payment_intent
        _is_store_address_intent = self.deps.is_store_address_intent
        _is_channel_handoff_intent = self.deps.is_channel_handoff_intent
        _is_deferral_message = self.deps.is_deferral_message
        _grounding_catalog_items = self.deps.grounding_catalog_items
        _selected_item_from_grounding = self.deps.selected_item_from_grounding
        _is_price_intent = self.deps.is_price_intent
        _MODEL_NAME_INTENT_RE = self.deps.model_name_intent_re
        _VARIANTS_USER_HINT_RE = self.deps.variants_user_hint_re
        _extract_attribute_probe = self.deps.extract_attribute_probe
        _extract_store_addresses_from_persona = self.deps.extract_store_addresses_from_persona
        _extract_city_hint = self.deps.extract_city_hint
        _persona_driven_question_for_fact = self.deps.persona_driven_question_for_fact
        _question_covers_fact = self.deps.question_covers_fact
        _extract_questions_from_text = self.deps.extract_questions_from_text
        _question_lists_catalog_options = self.deps.question_lists_catalog_options
        _replace_reply_question = self.deps.replace_reply_question
        _selected_item_attribute_answer = self.deps.selected_item_attribute_answer
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _CATALOG_UNAVAILABLE_RE = self.deps.catalog_unavailable_re
        _LOW_SIGNAL_CONTEXT_RE = self.deps.low_signal_context_re
        _catalog_min_price = self.deps.catalog_min_price
        _format_rub_price = self.deps.format_rub_price
        _generic_question_for_fact = self.deps.generic_question_for_fact
        _normalize_text = self.deps.normalize_text

        required = _required_facts_from_persona_text(persona_context)
        if not required:
            return (reply or "").strip(), ""
        facts = dict(known_facts or _state_facts_snapshot(state))
        missing = _missing_required_facts(required, facts)
        if not missing:
            return (reply or "").strip(), ""

        next_key = _canonical_fact_key(missing[0]) or ""
        if not next_key:
            return (reply or "").strip(), ""

        user_raw = str(user_text or "").strip()
        turn_intent = _classify_turn_intent(user_raw, known_facts=facts)
        candidate = (reply or "").strip()
        if _reply_contains_unconfirmed_required_claim(
            candidate,
            missing_required=missing,
            known_facts=facts,
            user_text=user_raw,
            persona_context=persona_context,
        ):
            candidate = ""
        if turn_intent == "offtopic":
            if candidate:
                return candidate, ""
            return "Готов помочь по вашему запросу.", ""
        user_low = user_raw.lower()
        candidate_substantive = _has_substantive_non_question_payload(candidate) or (
            len(candidate) >= 72 and candidate.count("?") <= 1
        ) or (len(candidate) >= 46 and bool(_extract_questions_from_text(candidate)))
        payment_intent = _is_payment_intent(user_raw)
        store_address_intent = _is_store_address_intent(user_raw)
        handoff_intent = _is_channel_handoff_intent(user_raw)
        deferral_intent = _is_deferral_message(user_raw)
        grounding_items = _grounding_catalog_items(grounding)
        selected_item = _selected_item_from_grounding(grounding, grounding_items)
        direct_followup_intent = (
            _is_price_intent(user_raw)
            or bool(_MODEL_NAME_INTENT_RE.search(user_raw))
            or bool(_extract_attribute_probe(user_raw))
        )
        # Guard conflict resolver: do not overwrite a direct grounded answer
        # with a mandatory next-step question.
        if candidate and selected_item is not None and direct_followup_intent:
            return candidate, ""

        # Payment/requisites dialogues should not be derailed by mandatory qualification questions.
        if payment_intent and candidate:
            payment_questions: List[str] = []
            for q in _extract_questions_from_text(candidate):
                if (
                    _question_covers_fact(q, "city")
                    or _question_covers_fact(q, "address")
                    or _question_covers_fact(q, "object_type")
                ):
                    continue
                payment_questions.append(q.strip())
            if payment_questions:
                return " ".join(payment_questions), ""
            return candidate, ""
        if handoff_intent and candidate:
            return candidate, ""

        if store_address_intent:
            city_map = _extract_store_addresses_from_persona(persona_context)
            known_city = (
                str(
                    facts.get("city")
                    or (state.known_slots.get("city") if isinstance(state.known_slots, dict) else "")
                    or ""
                )
                .strip()
                .lower()
                .replace("ё", "е")
            )
            if (not known_city) and city_map and isinstance(state.history, list):
                for item in reversed(state.history):
                    if str(item.get("role") or "").strip().lower() != "user":
                        continue
                    txt = str(item.get("content") or "").lower().replace("ё", "е")
                    for city_key in city_map.keys():
                        if city_key and city_key in txt:
                            known_city = city_key
                            break
                    if known_city:
                        break
            if (not known_city) and isinstance(state.history, list):
                for item in reversed(state.history):
                    if str(item.get("role") or "").strip().lower() != "user":
                        continue
                    txt_raw = str(item.get("content") or "").strip()
                    hint = _extract_city_hint(txt_raw, allow_standalone=True)
                    if hint:
                        known_city = hint.lower().replace("ё", "е")
                        break
            if known_city:
                for city_key, address in city_map.items():
                    if city_key and (city_key in known_city or known_city in city_key):
                        return address, ""
            # Address/store request without city: ask city directly, but keep persona wording.
            return _persona_driven_question_for_fact(persona_context, "city", state=state), "city"

        if next_key == "model":
            items = grounding_items
            asks_price_or_name = _is_price_intent(user_raw) or bool(
                _MODEL_NAME_INTENT_RE.search(user_raw)
            )
            asks_model_options = bool(_VARIANTS_USER_HINT_RE.search(user_raw)) or asks_price_or_name
            asks_selected_attribute = bool(selected_item) and bool(
                _selected_item_attribute_answer(user_raw, selected_item)
            )
            user_explicit_model = bool(_best_catalog_item_match(user_raw, items)) if items else False
            candidate_has_substance = candidate_substantive
            persona_model_question = _persona_driven_question_for_fact(
                persona_context,
                "model",
                state=state,
            )
            existing_questions = _extract_questions_from_text(candidate)
            if (
                existing_questions
                and (not asks_model_options)
                and (not user_explicit_model)
                and any(_question_lists_catalog_options(q, items) for q in existing_questions)
            ):
                return _replace_reply_question(candidate, persona_model_question), "model"

            # Guard conflict resolver:
            # if the user asked a direct factual follow-up and reply already contains a substantive answer,
            # do not override with generic "what model did you like?" question.
            if (
                candidate
                and candidate_has_substance
                and (asks_price_or_name or asks_selected_attribute or user_explicit_model)
            ):
                return candidate, ""

            cannot_open_catalog = bool(_CATALOG_UNAVAILABLE_RE.search(user_low))
            if (not cannot_open_catalog) and "каталог" in user_low:
                if _LOW_SIGNAL_CONTEXT_RE.search(user_low):
                    cannot_open_catalog = True
                elif ("не могу" in user_low or "не получается" in user_low) and "посмотр" in user_low:
                    cannot_open_catalog = True
            if cannot_open_catalog and candidate_substantive:
                return candidate, ""
            if candidate_substantive:
                # LLM produced substantive guidance; do not overwrite with rigid next-step prompt.
                if any(_question_covers_fact(item, "model") for item in _extract_questions_from_text(candidate)):
                    return candidate, "model"
                if asks_model_options or asks_price_or_name or asks_selected_attribute or user_explicit_model:
                    return candidate, ""
                if persona_model_question:
                    if _extract_questions_from_text(candidate):
                        return _replace_reply_question(candidate, persona_model_question), "model"
                    return f"{candidate} {persona_model_question}".strip(), "model"
                return candidate, ""

        # Keep strict progression only for core qualification facts.
        # For domain-specific flows (education, legal, etc.) avoid overriding natural reply logic.
        if next_key not in {"city", "address", "object_type", "model"}:
            return (reply or "").strip(), ""

        if next_key == "city" and _is_price_intent(user_raw):
            cand_low = _normalize_text(candidate)
            if candidate and ("город" in cand_low or "населен" in cand_low):
                return candidate, ""
            if candidate and not _extract_questions_from_text(candidate):
                city_question = _persona_driven_question_for_fact(persona_context, "city", state=state)
                if city_question:
                    return f"{candidate} {city_question}".strip(), "city"
                return candidate, ""

        if deferral_intent and candidate:
            return candidate, ""

        if next_key == "city" and _is_price_intent(user_raw):
            if candidate_substantive:
                return candidate, ""
            price_floor = _catalog_min_price(grounding_items)
            if price_floor:
                lead = f"Цена зависит от модели, старт от {_format_rub_price(price_floor)}."
            else:
                lead = "Цена зависит от модели."
            question = _persona_driven_question_for_fact(persona_context, "city", state=state)
            if question:
                return f"{lead} {question}".strip(), "city"
            return lead, ""

        question = _persona_driven_question_for_fact(persona_context, next_key, state=state)
        if not question:
            return candidate, ""
        existing_questions = _extract_questions_from_text(candidate)
        if existing_questions:
            # If reply is already meaningful and asks a coherent question,
            # do not hard-replace it with a scripted mandatory slot question.
            if candidate_substantive:
                if any(_question_covers_fact(item, next_key) for item in existing_questions):
                    return candidate, next_key
                return candidate, ""
            if any(_question_covers_fact(item, next_key) for item in existing_questions):
                if candidate_substantive:
                    return candidate, next_key
                for item in existing_questions:
                    if _question_covers_fact(item, next_key):
                        # Keep the actual qualification question and drop service preambles.
                        return item.strip(), next_key
                return candidate, next_key
            # For core qualification chain we enforce the missing step question to keep
            # deterministic progression from persona script.
            if next_key in {"city", "address", "object_type", "model"}:
                if candidate_substantive:
                    return candidate, ""
                return question, next_key
            # For non-core keys keep substantive guidance.
            if candidate_substantive:
                return candidate, ""
            return question, next_key
        if not candidate:
            return question, next_key
        if len(candidate) <= 120 and not candidate_substantive:
            # Keep short informative text and append one required question instead of replacing it.
            if _extract_questions_from_text(candidate):
                return candidate, next_key
            return f"{candidate} {question}".strip(), next_key
        if candidate_substantive:
            return candidate, ""
        return question, next_key

    def safe_minimal_fallback_reply(
        self,
        *,
        tenant: int | None,
        channel_name: str,
        contact_ref: int,
        last_user_message: str,
    ) -> str:
        load_sales_state = self.deps.load_sales_state
        load_persona_hints = self.deps.load_persona_hints
        load_persona = self.deps.load_persona
        _branding_for_tenant = self.deps.branding_for_tenant
        _state_facts_snapshot = self.deps.state_facts_snapshot
        _classify_turn_intent = self.deps.classify_turn_intent
        _persona_direct_reply_for_user_turn = self.deps.persona_direct_reply_for_user_turn
        _build_reply_grounding = self.deps.build_reply_grounding
        _grounding_catalog_items = self.deps.grounding_catalog_items
        read_all_catalog = self.deps.read_all_catalog
        _selected_item_from_grounding = self.deps.selected_item_from_grounding
        _normalize_text = self.deps.normalize_text
        _persona_catalog_unavailable_reply = self.deps.persona_catalog_unavailable_reply
        _shortlist_preview_text = self.deps.shortlist_preview_text
        infer_user_needs = self.deps.infer_user_needs
        search_catalog = self.deps.search_catalog
        _extract_attribute_probe = self.deps.extract_attribute_probe
        _items_with_attribute = self.deps.items_with_attribute
        _extract_budget_cap_from_needs = self.deps.extract_budget_cap_from_needs
        _item_price_int = self.deps.item_price_int
        _extract_price_order_intent = self.deps.extract_price_order_intent
        _render_shortlist_preview_reply = self.deps.render_shortlist_preview_reply
        _apply_base_answer_quality_floor = self.deps.apply_base_answer_quality_floor
        _ensure_dialog_greeting_on_first_reply = self.deps.ensure_dialog_greeting_on_first_reply
        save_sales_state = self.deps.save_sales_state
        _canonical_fact_key = self.deps.canonical_fact_key
        _looks_like_contextual_short_followup = self.deps.looks_like_contextual_short_followup
        _is_shortlist_feedback_turn = self.deps.is_shortlist_feedback_turn
        _shortlist_comparison_followup_plan = self.shortlist_comparison_followup_plan
        _shortlist_attribute_answer = self.deps.shortlist_attribute_answer
        _required_facts_from_persona_text = self.deps.required_facts_from_persona_text
        _missing_required_facts = self.deps.missing_required_facts
        _prioritize_missing_facts = self.deps.prioritize_missing_facts
        _persona_script_questions = self.deps.persona_script_questions
        _persona_primary_script_question = self.deps.persona_primary_script_question
        _is_price_intent = self.deps.is_price_intent
        _MODEL_NAME_INTENT_RE = self.deps.model_name_intent_re
        _VARIANTS_USER_HINT_RE = self.deps.variants_user_hint_re
        _selected_item_attribute_answer = self.deps.selected_item_attribute_answer
        _selected_item_brief_answer = self.deps.selected_item_brief_answer
        _fallback_contextual_question = self.deps.fallback_contextual_question
        _persona_driven_question_for_fact = self.deps.persona_driven_question_for_fact
        _explain_missing_fact_need = self.deps.explain_missing_fact_need
        _question_covers_fact = self.deps.question_covers_fact
        _generic_question_for_fact = self.deps.generic_question_for_fact
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _item_label = self.deps.item_label
        _format_rub_price = self.deps.format_rub_price
        _normalize_slot_name = self.deps.normalize_slot_name
        _extract_questions_from_text = self.deps.extract_questions_from_text
        _fact_keys_from_line = self.deps.fact_keys_from_line
        _apply_persona_sequence_obligations = self.deps.apply_persona_sequence_obligations
        _apply_persona_delivery_obligations = self.deps.apply_persona_delivery_obligations
        _is_repeated_question_against_state = self.deps.is_repeated_question_against_state
        _ORDER_INTENT_RE = self.deps.order_intent_re
        _catalog_min_price = self.deps.catalog_min_price
        _update_fact_memory = self.deps.update_fact_memory
        _remember_questions_from_reply = self.deps.remember_questions_from_reply
        _CATALOG_UNAVAILABLE_RE = self.deps.catalog_unavailable_re
        _LOW_SIGNAL_CONTEXT_RE = self.deps.low_signal_context_re

        state = load_sales_state(tenant, contact_ref)
        persona_hints = load_persona_hints(tenant, channel_name)
        persona_text = load_persona(tenant, channel_name)
        branding = _branding_for_tenant(tenant, channel_name)
        known_facts = _state_facts_snapshot(state)
        turn_intent = _classify_turn_intent(last_user_message, known_facts=known_facts)
        direct_persona_reply = _persona_direct_reply_for_user_turn(
            persona_text,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        grounding = _build_reply_grounding(
            tenant=tenant,
            state=state,
            user_text=last_user_message,
        )
        catalog_items = _grounding_catalog_items(grounding)
        if (not catalog_items) and (tenant is not None):
            try:
                catalog_items = read_all_catalog(tenant=tenant)
            except Exception:
                catalog_items = []
        effective_grounding: dict[str, Any] = dict(grounding or {})
        if catalog_items and not _grounding_catalog_items(grounding):
            effective_grounding["catalog_items"] = [dict(item) for item in list(catalog_items or [])[:40]]
        selected = _selected_item_from_grounding(grounding, catalog_items)
        last_user_low = _normalize_text(str(last_user_message or ""))
        model_repair_turn = bool(
            _LOW_SIGNAL_CONTEXT_RE.search(str(last_user_message or ""))
            or _CATALOG_UNAVAILABLE_RE.search(str(last_user_message or ""))
            or ("груз" in last_user_low)
            or ("открыва" in last_user_low)
            or ("открыл" in last_user_low and "не" in last_user_low)
        )
        if state.pending_fact_key == "model" and model_repair_turn:
            intro = _persona_catalog_unavailable_reply(persona_text)
            preview = ""
            preview_items: list[Mapping[str, Any]] = []
            grounding_items = _grounding_catalog_items(grounding)
            if grounding_items:
                preview_items = [dict(item) for item in list(grounding_items or [])[:2]]
                preview = _shortlist_preview_text(preview_items, limit=2)
            if (not preview_items) and tenant is not None:
                try:
                    # Build fresh retrieval needs from confirmed facts and current user turn.
                    # Avoid stale free-form state.needs tokens degrading catalog match quality.
                    needs: Dict[str, Any] = {}
                    for key in ("city", "object_type", "address"):
                        fact_val = str((state.facts or {}).get(key) or "").strip()
                        if fact_val:
                            needs[key] = fact_val
                    query_needs = infer_user_needs(last_user_message)
                    safe_need_keys = {"keywords", "budget_max", "price_order", "color", "object_type", "dimensions", "model"}
                    for key, value in dict(query_needs or {}).items():
                        if value in (None, "", [], {}, ()):
                            continue
                        if key not in safe_need_keys:
                            continue
                        needs[key] = value
                    preview_items = list(
                        search_catalog(
                            needs,
                            limit=2,
                            tenant=tenant,
                            query=last_user_message,
                        )
                        or []
                    )
                    preview = _shortlist_preview_text(preview_items, limit=2)
                    if not preview_items:
                        catalog_items = list(read_all_catalog(tenant=tenant) or [])
                        probe = _extract_attribute_probe(last_user_message)
                        if probe:
                            probe_items = _items_with_attribute(catalog_items, probe)
                            if probe_items:
                                catalog_items = [dict(item) for item in probe_items]
                        budget_cap = _extract_budget_cap_from_needs(query_needs)
                        if budget_cap:
                            filtered = [
                                dict(item)
                                for item in catalog_items
                                if isinstance(_item_price_int(dict(item)), int)
                                and int(_item_price_int(dict(item)) or 0) > 0
                                and int(_item_price_int(dict(item)) or 0) <= int(budget_cap)
                            ]
                            if filtered:
                                catalog_items = filtered
                        order = _extract_price_order_intent(last_user_message)
                        if order in {"asc", "desc"}:
                            reverse = order == "desc"
                            priced = [
                                dict(item)
                                for item in catalog_items
                                if isinstance(_item_price_int(dict(item)), int)
                                and int(_item_price_int(dict(item)) or 0) > 0
                            ]
                            if priced:
                                catalog_items = sorted(
                                    priced,
                                    key=lambda item: int(_item_price_int(dict(item)) or 0),
                                    reverse=reverse,
                                )
                        preview_items = [dict(item) for item in list(catalog_items or [])[:2]]
                        preview = _shortlist_preview_text(preview_items, limit=2)
                except Exception:
                    preview = ""
                    preview_items = []
            if preview and intro:
                reply = f"{intro.rstrip('.!?')}: {preview}."
            elif preview:
                reply = f"{preview}."
            else:
                reply = intro or _persona_driven_question_for_fact(persona_text, "model", state=state)
            if preview_items:
                effective_grounding["catalog_items"] = [dict(item) for item in preview_items[:8]]
                state.last_items = [dict(item) for item in preview_items[:2]]
            state.pending_fact_key = "model"
            reply = _apply_base_answer_quality_floor(
                reply,
                state=state,
                persona_hints=persona_hints,
                grounding=effective_grounding,
                user_text=last_user_message,
            )
            reply = _ensure_dialog_greeting_on_first_reply(reply, state, persona_context=persona_text)
            state.last_bot_reply = reply
            state.append_history("assistant", reply)
            state.last_updated_ts = time.time()
            save_sales_state(state)
            return reply
        current_pending = _canonical_fact_key(str(state.pending_fact_key or ""))
        unresolved_model_followup = bool(
            current_pending == "model"
            and selected is None
            and bool(state.last_items)
            and (
                bool(_extract_attribute_probe(last_user_message))
                or _looks_like_contextual_short_followup(last_user_message)
                or bool(
                    state.last_items
                    and _is_shortlist_feedback_turn(last_user_message, known_facts=known_facts)
                )
            )
        )
        if unresolved_model_followup:
            shortlist_answer, shortlist_items = _shortlist_comparison_followup_plan(
                last_user_message,
                state.last_items or [],
                tenant=tenant,
                persona_context=persona_text,
                state=state,
            )
            if not shortlist_answer:
                shortlist_answer = _shortlist_attribute_answer(last_user_message, state.last_items or [])
            if shortlist_answer:
                reply = shortlist_answer
                if shortlist_items:
                    state.last_items = [dict(item) for item in shortlist_items[:2]]
            else:
                reply = _fallback_contextual_question(
                    last_user_message,
                    state=state,
                    persona_context=persona_text,
                ) or _persona_driven_question_for_fact(persona_text, "model", state=state)
            state.pending_fact_key = "model"
            reply = _apply_base_answer_quality_floor(
                reply,
                state=state,
                persona_hints=persona_hints,
                grounding=effective_grounding,
                user_text=last_user_message,
            )
            state.last_bot_reply = reply
            state.append_history("assistant", reply)
            state.last_updated_ts = time.time()
            save_sales_state(state)
            return reply
        if current_pending == "model" and selected is None:
            model_probe = _extract_attribute_probe(last_user_message)
            if model_probe:
                probe_items: list[Mapping[str, Any]] = []
                source_items = _grounding_catalog_items(grounding)
                if not source_items and tenant is not None:
                    try:
                        source_items = read_all_catalog(tenant=tenant)
                    except Exception:
                        source_items = []
                if source_items:
                    probe_items = _items_with_attribute(source_items, model_probe)
                if not probe_items and source_items:
                    probe_items = [dict(item) for item in list(source_items or [])[:2]]
                if probe_items:
                    shortlist = [dict(item) for item in list(probe_items or [])[:2]]
                    preview = _shortlist_preview_text(shortlist, limit=2)
                    if preview:
                        reply = _render_shortlist_preview_reply(
                            preview,
                            ask_detail=True,
                            persona_context=persona_text,
                            state=state,
                            user_text=last_user_message,
                        )
                        state.last_items = shortlist[:2]
                        state.pending_fact_key = "model"
                        effective_grounding["catalog_items"] = [dict(item) for item in shortlist[:8]]
                        reply = _apply_base_answer_quality_floor(
                            reply,
                            state=state,
                            persona_hints=persona_hints,
                            grounding=effective_grounding,
                            user_text=last_user_message,
                        )
                        reply = _ensure_dialog_greeting_on_first_reply(reply, state, persona_context=persona_text)
                        state.last_bot_reply = reply
                        state.append_history("assistant", reply)
                        state.last_updated_ts = time.time()
                        save_sales_state(state)
                        return reply

        required = _required_facts_from_persona_text(persona_text)
        missing = _missing_required_facts(required, known_facts)
        missing = _prioritize_missing_facts(missing, turn_intent=turn_intent)
        script_questions = _persona_script_questions(persona_text)
        script_question = _persona_primary_script_question(persona_text, state=state)
        suppress_followup_questions = False
        catalog_intent = bool(
            _is_price_intent(last_user_message)
            or _MODEL_NAME_INTENT_RE.search(str(last_user_message or ""))
            or _VARIANTS_USER_HINT_RE.search(str(last_user_message or ""))
        )
        selected_followup_intent = bool(
            selected is not None
            and (
                _is_price_intent(last_user_message)
                or bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or "")))
                or bool(_VARIANTS_USER_HINT_RE.search(str(last_user_message or "")))
                or bool(_extract_attribute_probe(last_user_message))
            )
        )
        if direct_persona_reply and turn_intent not in {"repair", "catalog_problem"}:
            reply = direct_persona_reply
        elif selected_followup_intent:
            state.pending_fact_key = ""
            reply = _selected_item_attribute_answer(last_user_message, selected)
            if not reply and bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or ""))):
                reply = _selected_item_brief_answer(selected)
            if not reply and _is_price_intent(last_user_message):
                selected_name = _item_label(dict(selected))
                selected_price = _item_price_int(dict(selected))
                if selected_name and selected_price:
                    reply = f"{selected_name} {_format_rub_price(selected_price)}".strip()
            if not reply:
                reply = _selected_item_brief_answer(selected)
        elif turn_intent == "why_question":
            target_key = current_pending or (missing[0] if missing else "")
            reply = _explain_missing_fact_need(target_key, persona_context=persona_text)
            if not reply:
                reply = _persona_driven_question_for_fact(persona_text, target_key or "model", state=state)
            followup_question = ""
            if target_key:
                followup_question = _persona_driven_question_for_fact(
                    persona_text,
                    target_key,
                    state=state,
                )
            if reply and followup_question and not _is_repeated_question_against_state(followup_question, state):
                reply = f"{reply.rstrip()} {followup_question}".strip()
            elif not reply and followup_question:
                reply = followup_question
            if target_key:
                state.pending_fact_key = _canonical_fact_key(target_key)
        elif turn_intent == "offtopic":
            reply = "Готов помочь по вашему запросу."
            suppress_followup_questions = True
            missing = []
            state.pending_fact_key = ""
        elif turn_intent == "repair":
            target_key = current_pending or (missing[0] if missing else "")
            if target_key:
                if _canonical_fact_key(target_key) == "model" and state.last_items:
                    shortlist_answer, shortlist_items = _shortlist_comparison_followup_plan(
                        last_user_message,
                        state.last_items or [],
                        tenant=tenant,
                        persona_context=persona_text,
                        state=state,
                    )
                    if not shortlist_answer:
                        shortlist_answer = _shortlist_attribute_answer(
                            last_user_message, state.last_items or []
                        )
                    if shortlist_answer:
                        reply = shortlist_answer
                        if shortlist_items:
                            state.last_items = [dict(item) for item in shortlist_items[:2]]
                    else:
                        reply = _fallback_contextual_question(
                            last_user_message,
                            state=state,
                            persona_context=persona_text,
                        )
                else:
                    reply = _persona_driven_question_for_fact(persona_text, target_key, state=state)
                state.pending_fact_key = _canonical_fact_key(target_key)
            else:
                reply = _fallback_contextual_question(
                    last_user_message,
                    state=state,
                    persona_context=persona_text,
                ) or _persona_primary_script_question(persona_text, state=state)
        elif selected is not None and not missing:
            state.pending_fact_key = ""
            reply = _selected_item_attribute_answer(last_user_message, selected)
            if not reply:
                explicit_match = _best_catalog_item_match(last_user_message, catalog_items)
                if explicit_match is not None:
                    reply = _selected_item_brief_answer(explicit_match)
            if not reply:
                reply = _selected_item_brief_answer(selected)
        elif catalog_intent:
            reply = ""
            if selected is not None and _is_price_intent(last_user_message):
                selected_name = _item_label(dict(selected))
                selected_price = _item_price_int(dict(selected))
                if selected_name and selected_price:
                    reply = f"По каталогу {selected_name} стоит {_format_rub_price(selected_price)}."
            if not reply and _is_price_intent(last_user_message):
                min_price = _catalog_min_price(catalog_items)
                if min_price:
                    reply = f"По каталогу есть варианты от {_format_rub_price(min_price)}."
            if not reply:
                preferred_question = script_question if missing else ""
                if preferred_question and (
                    _is_price_intent(last_user_message)
                    or bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or "")))
                    or bool(_VARIANTS_USER_HINT_RE.search(str(last_user_message or "")))
                ):
                    if not (
                        _question_covers_fact(preferred_question, "model")
                        or _question_covers_fact(preferred_question, "budget")
                    ):
                        preferred_question = ""
                reply = preferred_question or _fallback_contextual_question(
                    last_user_message,
                    state=state,
                    persona_context=persona_text,
                )
            if missing:
                selected_key = missing[0]
                selected_question = _persona_driven_question_for_fact(
                    persona_text,
                    selected_key,
                    state=state,
                )
                if selected_question and ("?" not in reply):
                    reply = f"{reply} {selected_question}".strip()
                    state.pending_fact_key = _canonical_fact_key(selected_key)
        elif missing and not suppress_followup_questions:
            selected_key = missing[0]
            selected_question = _persona_driven_question_for_fact(
                persona_text,
                selected_key,
                state=state,
            )
            for key in missing:
                q = _persona_driven_question_for_fact(persona_text, key, state=state)
                if not _is_repeated_question_against_state(q, state):
                    selected_key = key
                    selected_question = q
                    break
            if _is_repeated_question_against_state(selected_question, state):
                non_address = [k for k in missing if _canonical_fact_key(k) != "address"]
                for key in non_address:
                    q = _persona_driven_question_for_fact(persona_text, key, state=state)
                    if not _is_repeated_question_against_state(q, state):
                        selected_key = key
                        selected_question = q
                        break
            state.pending_fact_key = _canonical_fact_key(selected_key)
            reply = selected_question
        elif turn_intent == "catalog_request" and str(branding.get("CATALOG_URL") or "").strip():
            reply = f"Вот каталог: {str(branding.get('CATALOG_URL') or '').strip()}"
        elif _ORDER_INTENT_RE.search(str(last_user_message or "")):
            if str(known_facts.get("contact") or "").strip():
                reply = "Продолжаем оформление. Подтверждение отправлю по вашему контакту."
            else:
                state.pending_fact_key = "contact"
                reply = "Чтобы оформить, оставьте, пожалуйста, телефон или удобный мессенджер."
        elif known_facts.get("model") and not missing:
            state.pending_fact_key = ""
            reply = ""
            if selected is not None:
                attr_answer = _selected_item_attribute_answer(last_user_message, selected)
                if attr_answer:
                    reply = attr_answer
                elif bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or ""))):
                    reply = _selected_item_brief_answer(selected)
            if not reply:
                reply = _fallback_contextual_question(
                    last_user_message,
                    state=state,
                    persona_context=persona_text,
                ) or _selected_item_brief_answer(selected if isinstance(selected, Mapping) else {})
        else:
            if script_question and missing:
                reply = script_question
                for key in _fact_keys_from_line(script_question):
                    canonical = _canonical_fact_key(key)
                    if canonical:
                        state.pending_fact_key = canonical
                        break
            else:
                reply = _fallback_contextual_question(
                    last_user_message,
                    state=state,
                    persona_context=persona_text,
                )
        reply = _apply_base_answer_quality_floor(
            reply,
            state=state,
            persona_hints=persona_hints,
            grounding=effective_grounding,
            user_text=last_user_message,
        )
        reply = _apply_persona_sequence_obligations(
            reply,
            persona_context=persona_text,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        reply = _apply_persona_delivery_obligations(
            reply,
            persona_context=persona_text,
            channel_name=channel_name,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        if len((reply or "").strip()) <= 8 and not suppress_followup_questions:
            preferred_short_question = str(script_question or "").strip()
            if preferred_short_question and _is_repeated_question_against_state(preferred_short_question, state):
                preferred_short_question = ""
            if (not preferred_short_question) and missing:
                inferred = _persona_driven_question_for_fact(persona_text, missing[0], state=state)
                if inferred and not _is_repeated_question_against_state(inferred, state):
                    preferred_short_question = inferred
            reply = preferred_short_question or _fallback_contextual_question(
                last_user_message,
                state=state,
                persona_context=persona_text,
            )
        reply = _apply_base_answer_quality_floor(
            reply,
            state=state,
            persona_hints=persona_hints,
            grounding=effective_grounding,
            user_text=last_user_message,
        )
        if direct_persona_reply:
            direct_questions = _extract_questions_from_text(reply)
            if direct_questions:
                pending_key = ""
                for question in direct_questions:
                    inferred = _normalize_slot_name("", question=question)
                    canonical = _canonical_fact_key(inferred)
                    if canonical:
                        pending_key = canonical
                        break
                if not pending_key:
                    for miss_key in missing:
                        if any(_question_covers_fact(question, miss_key) for question in direct_questions):
                            pending_key = _canonical_fact_key(miss_key)
                            break
                state.pending_fact_key = pending_key
        if state is not None and script_questions and _is_repeated_question_against_state(reply, state):
            preferred_pending = _canonical_fact_key(str(state.pending_fact_key or ""))
            if preferred_pending:
                pending_question = _persona_driven_question_for_fact(
                    persona_text,
                    preferred_pending,
                    state=state,
                )
                if pending_question and not _is_repeated_question_against_state(pending_question, state):
                    reply = pending_question
            if _is_repeated_question_against_state(reply, state) and not preferred_pending:
                for question in script_questions:
                    if not _is_repeated_question_against_state(question, state):
                        reply = question
                        break
        if not str(reply or "").strip():
            preferred_script = str(script_question or "").strip()
            if preferred_script and not _is_repeated_question_against_state(preferred_script, state):
                reply = preferred_script
            preferred_key = (
                _canonical_fact_key(str(state.pending_fact_key or ""))
                or _canonical_fact_key(current_pending)
                or (_canonical_fact_key(missing[0]) if missing else "")
                or ""
            )
            if preferred_key == "model" and not catalog_items:
                non_model_missing = [
                    _canonical_fact_key(key)
                    for key in missing
                    if _canonical_fact_key(key) and _canonical_fact_key(key) != "model"
                ]
                preferred_key = (non_model_missing[0] if non_model_missing else "") or "budget"
            if (not preferred_key) and catalog_items:
                preferred_key = "model"
            if not preferred_key:
                preferred_key = "budget"
            if not str(reply or "").strip():
                reply = _persona_driven_question_for_fact(persona_text, preferred_key, state=state)
            if not str(reply or "").strip():
                reply = _generic_question_for_fact(preferred_key)
        reply = _ensure_dialog_greeting_on_first_reply(reply, state, persona_context=persona_text)
        state.last_bot_reply = reply
        state.append_history("assistant", reply)
        state.last_updated_ts = time.time()
        _update_fact_memory(state, reply)
        _remember_questions_from_reply(state, reply)
        save_sales_state(state)
        return reply

    def enforce_catalog_truth_guard(
        self,
        text: str,
        *,
        grounding: Mapping[str, Any] | None = None,
        user_text: str = "",
    ) -> str:
        _grounding_catalog_items = self.deps.grounding_catalog_items
        _is_price_intent = self.deps.is_price_intent
        _MODEL_NAME_INTENT_RE = self.deps.model_name_intent_re
        _VARIANTS_USER_HINT_RE = self.deps.variants_user_hint_re
        _MIN_PRICE_INTENT_RE = self.deps.min_price_intent_re
        _MAX_PRICE_INTENT_RE = self.deps.max_price_intent_re
        _SENTENCE_SPLIT_RE = self.deps.sentence_split_re
        _extract_price_spans = self.deps.extract_price_spans
        _is_likely_price_value = self.deps.is_likely_price_value
        _enforce_catalog_model_grounding = self.deps.enforce_catalog_model_grounding
        _enforce_catalog_price_grounding = self.deps.enforce_catalog_price_grounding
        _reply_mentions_catalog_item = self.deps.reply_mentions_catalog_item
        _item_price_int = self.deps.item_price_int
        _is_catalog_price_candidate = self.deps.is_catalog_price_candidate
        _catalog_has_object_type_evidence = self.deps.catalog_has_object_type_evidence
        _normalize_text = self.deps.normalize_text
        _object_type_from_turn_text = self.deps.object_type_from_turn_text
        _filter_items_by_object_type_need = self.deps.filter_items_by_object_type_need
        _selected_item_from_grounding = self.deps.selected_item_from_grounding
        _item_object_type_hint = self.deps.item_object_type_hint
        _catalog_item_is_two_panel = self.deps.catalog_item_is_two_panel
        infer_user_needs = self.deps.infer_user_needs
        _is_specific_catalog_keyword = self.deps.is_specific_catalog_keyword
        _normalize_probe_token = self.deps.normalize_probe_token
        _items_with_attribute_direct = self.deps.items_with_attribute_direct
        _items_with_attribute = self.deps.items_with_attribute
        _extract_budget_cap_from_needs = self.deps.extract_budget_cap_from_needs
        _closest_catalog_item_by_price = self.deps.closest_catalog_item_by_price
        _extract_attribute_probe = self.deps.extract_attribute_probe
        _item_label = self.deps.item_label
        _format_rub_price = self.deps.format_rub_price
        _reply_mentions_unknown_model = self.deps.reply_mentions_unknown_model
        _neutralize_unknown_model_mentions = self.deps.neutralize_unknown_model_mentions
        _extract_explicit_model_probe = self.deps.extract_explicit_model_probe
        _strict_catalog_item_match = self.deps.strict_catalog_item_match
        _has_unverified_priced_labels = self.deps.has_unverified_priced_labels
        _neutralize_unverified_priced_labels = self.deps.neutralize_unverified_priced_labels
        _selected_item_attribute_answer = self.deps.selected_item_attribute_answer
        _narrow_catalog_items_by_user_text = self.deps.narrow_catalog_items_by_user_text
        _negative_attribute_probes = self.deps.negative_attribute_probes
        _exclude_items_with_negative_probes = self.deps.exclude_items_with_negative_probes
        _catalog_extreme_item_by_price = self.deps.catalog_extreme_item_by_price
        _extract_price_target_hint = self.deps.extract_price_target_hint
        _catalog_min_price = self.deps.catalog_min_price
        _neutralize_catalog_model_mentions = self.deps.neutralize_catalog_model_mentions

        base = (text or "").strip()
        if not base:
            return base
        items = _grounding_catalog_items(grounding)
        user_raw = str(user_text or "")
        if not items:
            if (
                _is_price_intent(user_raw)
                or bool(_MODEL_NAME_INTENT_RE.search(user_raw))
                or bool(_VARIANTS_USER_HINT_RE.search(user_raw))
            ):
                patched = base
                spans = [span for span in _extract_price_spans(patched) if _is_likely_price_value(span[2])]
                if re.search(r"(?iu)\bскидк\w*\b", patched):
                    spans = [
                        span
                        for span in spans
                        if not re.search(r"(?iu)\bскидк\w*\b", patched[max(0, span[0] - 32): span[1]])
                    ]
                for start, end, _ in sorted(spans, key=lambda item: item[0], reverse=True):
                    patched = patched[:start] + "цена по каталогу" + patched[end:]
                if patched.strip():
                    return patched
            return base

        normalized = _enforce_catalog_model_grounding(base, grounding=grounding)
        normalized = _enforce_catalog_price_grounding(normalized, grounding=grounding)

        low = normalized.lower()
        mentions_known = _reply_mentions_catalog_item(normalized, items)
        has_unknown_model_marker = "модель из каталога" in low
        asks_price = _is_price_intent(user_raw)
        asks_min_price_global = bool(_MIN_PRICE_INTENT_RE.search(user_raw))
        asks_max_price_global = bool(_MAX_PRICE_INTENT_RE.search(user_raw))
        asks_variants = bool(_VARIANTS_USER_HINT_RE.search(user_raw))
        asks_model_name = bool(_MODEL_NAME_INTENT_RE.search(user_raw))
        catalog_prices = {
            int(price)
            for price in (_item_price_int(dict(item)) for item in items)
            if isinstance(price, int) and price > 0
        }
        price_spans = [
            span
            for span in _extract_price_spans(normalized)
            if _is_catalog_price_candidate(span[2], catalog_prices)
        ]
        has_price_tokens = bool(price_spans)
        needs_ctx = dict((grounding or {}).get("needs") or {})
        prefers_insulation = bool(
            needs_ctx.get("insulation_priority") or needs_ctx.get("noise_priority")
        )
        object_type_need = _normalize_text(needs_ctx.get("object_type") or "")
        turn_object_type = _object_type_from_turn_text(user_raw)
        if turn_object_type in {"apartment", "house"}:
            object_type_need = turn_object_type
            needs_ctx["object_type"] = turn_object_type
        candidate_items = list(items)
        filtered_by_object = _filter_items_by_object_type_need(candidate_items, needs_ctx)
        if filtered_by_object:
            candidate_items = filtered_by_object
        object_filter_restricted = len(candidate_items) < len(items)
        selected_item = _selected_item_from_grounding(grounding, items)
        if selected_item is not None and object_type_need in {"apartment", "house"}:
            selected_hint = _item_object_type_hint(selected_item)
            if selected_hint and selected_hint != object_type_need:
                selected_item = None
        object_type_has_evidence = _catalog_has_object_type_evidence(items, object_type_need)
        if prefers_insulation and object_type_need == "apartment" and not (
            asks_min_price_global or asks_max_price_global
        ):
            two_panel = [item for item in candidate_items if _catalog_item_is_two_panel(item)]
            if two_panel:
                candidate_items = two_panel

        kw_values = needs_ctx.get("keywords")
        has_specific_kw = False
        if asks_price:
            query_needs = infer_user_needs(user_text)
            q_keywords = query_needs.get("keywords") if isinstance(query_needs, Mapping) else None
            if isinstance(q_keywords, Sequence):
                q_specific = [kw for kw in q_keywords if _is_specific_catalog_keyword(str(kw or ""))]
                if q_specific:
                    kw_values = q_specific
        if isinstance(kw_values, Sequence):
            has_specific_kw = any(_is_specific_catalog_keyword(str(kw or "")) for kw in kw_values)
        if isinstance(kw_values, Sequence):
            best_kw_match: List[Mapping[str, Any]] | None = None
            for raw_kw in kw_values:
                kw = str(raw_kw or "").strip()
                if len(kw) < 4:
                    continue
                kw_norm = _normalize_probe_token(kw)
                allow_semantic_kw = not any(
                    kw_norm.startswith(stem)
                    for stem in ("квартир", "двер", "дом", "частн", "дорог", "дешев", "сам")
                )
                matched_by_kw = _items_with_attribute_direct(candidate_items, kw)
                if not matched_by_kw and allow_semantic_kw:
                    matched_by_kw = _items_with_attribute(candidate_items, kw)
                if not matched_by_kw and (not object_filter_restricted):
                    matched_by_kw = _items_with_attribute_direct(items, kw)
                if not matched_by_kw and allow_semantic_kw and (not object_filter_restricted):
                    matched_by_kw = _items_with_attribute(items, kw)
                if matched_by_kw:
                    if best_kw_match is None or len(matched_by_kw) < len(best_kw_match):
                        best_kw_match = list(matched_by_kw)
            if best_kw_match:
                candidate_items = list(best_kw_match)

        invalid_prices = [
            span[2]
            for span in price_spans
            if catalog_prices
            and _is_catalog_price_candidate(span[2], catalog_prices)
            and span[2] not in catalog_prices
        ]
        if invalid_prices:
            patched = normalized
            replacements: list[tuple[int, int, str]] = []
            for start, end, value in price_spans:
                if catalog_prices and int(value) not in catalog_prices:
                    replacements.append((start, end, "цена по каталогу"))
            if replacements:
                for start, end, value in sorted(replacements, key=lambda item: item[0], reverse=True):
                    patched = patched[:start] + value + patched[end:]
                normalized = patched
            if asks_price or asks_variants or asks_model_name:
                pass

        if _reply_mentions_unknown_model(normalized, candidate_items or items):
            normalized = _neutralize_unknown_model_mentions(normalized, candidate_items or items)
            parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(normalized) if part.strip()]
            kept = [
                part
                for part in parts
                if not _reply_mentions_unknown_model(part, candidate_items or items)
            ]
            if kept:
                normalized = " ".join(kept).strip()
            else:
                normalized = _neutralize_unknown_model_mentions(base, candidate_items or items).strip()
                normalized = re.sub(r"\s{2,}", " ", normalized).strip()
            if _reply_mentions_unknown_model(normalized, candidate_items or items):
                if not (asks_variants or asks_price or asks_model_name):
                    return normalized

        if asks_variants and (not mentions_known):
            return normalized

        explicit_probe = _extract_explicit_model_probe(user_raw)
        if explicit_probe and _strict_catalog_item_match(explicit_probe, candidate_items or items) is None:
            return normalized

        if asks_price and has_price_tokens:
            if _has_unverified_priced_labels(normalized, candidate_items or items):
                normalized = _neutralize_unverified_priced_labels(
                    normalized,
                    candidate_items or items,
                )
                normalized = _neutralize_unknown_model_mentions(normalized, candidate_items or items)
                return normalized

        if has_unknown_model_marker:
            return normalized

        selected_attr_answer = ""
        asked_probe = _extract_attribute_probe(user_text)
        if selected_item is not None and (not asks_price) and (not asks_model_name):
            selected_attr_answer = _selected_item_attribute_answer(user_text, selected_item)
            if selected_attr_answer:
                return selected_attr_answer
        if asked_probe and (not asks_price) and (not asks_model_name):
            probe_norm = _normalize_probe_token(asked_probe)
            source_for_attr = list((grounding or {}).get("catalog_items") or items)
            if probe_norm.startswith("двухпанел"):
                source_for_attr = list(candidate_items)
            attr_items = _items_with_attribute(source_for_attr, asked_probe)
            if not attr_items:
                attr_items = _items_with_attribute(items, asked_probe)
            budget_cap = _extract_budget_cap_from_needs(dict((grounding or {}).get("needs") or {}))
            if budget_cap and attr_items:
                limited = [
                    it for it in attr_items if (_item_price_int(dict(it)) or 10**9) <= budget_cap
                ]
                if limited:
                    attr_items = limited
                else:
                    nearest_attr = _closest_catalog_item_by_price(attr_items, budget_cap)
                    if nearest_attr is not None:
                        nm = str(_item_label(dict(nearest_attr)) or "").lower()
                        pr = _item_price_int(dict(nearest_attr))
                        if nm and pr:
                            return f"{nm} {_format_rub_price(pr)}".strip()
            if selected_item is not None:
                selected_attr_answer = _selected_item_attribute_answer(user_text, selected_item)
                if selected_attr_answer:
                    return selected_attr_answer
            if probe_norm:
                return normalized

        if asks_price and (
            (not mentions_known)
            or has_price_tokens
            or asks_min_price_global
            or asks_max_price_global
        ):
            if (
                object_type_need in {"apartment", "house"}
                and (not object_filter_restricted)
                and (not object_type_has_evidence)
                and (not has_specific_kw)
            ):
                guarded = _neutralize_catalog_model_mentions(normalized, items)
                for start, end, value in sorted(
                    _extract_price_spans(guarded), key=lambda item: item[0], reverse=True
                ):
                    if _is_catalog_price_candidate(value, None):
                        guarded = guarded[:start] + "цена по каталогу" + guarded[end:]
                return re.sub(r"\s{2,}", " ", guarded).strip()
            if (
                object_type_need in {"apartment", "house"}
                and (asks_min_price_global or asks_max_price_global)
                and len(candidate_items) == len(items)
                and (not has_specific_kw)
                and not _catalog_has_object_type_evidence(candidate_items, object_type_need)
            ):
                return normalized
            narrowed_by_text = _narrow_catalog_items_by_user_text(candidate_items or items, user_text)
            if narrowed_by_text:
                candidate_items = list(narrowed_by_text)
            neg_probes = _negative_attribute_probes(user_text)
            if neg_probes:
                neg_filtered = _exclude_items_with_negative_probes(candidate_items or items, neg_probes)
                if not neg_filtered:
                    neg_filtered = _exclude_items_with_negative_probes(items, neg_probes)
                if neg_filtered:
                    candidate_items = list(neg_filtered)
            asks_min_price = asks_min_price_global
            asks_max_price = asks_max_price_global
            normalized_words = len(re.findall(r"(?u)\b\w+\b", normalized))
            normalized_sentences = len([s for s in re.split(r"[.!?]+", normalized) if s.strip()])
            if (not asks_min_price) and (not asks_max_price):
                if normalized_words >= 10 and (normalized_sentences >= 2 or "?" in normalized):
                    spans = _extract_price_spans(normalized)
                    if not spans:
                        return normalized
                    if mentions_known and all(int(span[2]) in {int(p) for p in [_item_price_int(dict(it)) for it in items] if p} for span in spans):
                        return normalized
            if asks_max_price:
                max_item = _catalog_extreme_item_by_price(candidate_items or items, highest=True)
                if max_item is not None:
                    max_name = _item_label(dict(max_item))
                    max_price = _item_price_int(dict(max_item))
                    if max_name and max_price:
                        return f"{max_name} {_format_rub_price(max_price)}".strip()
            if asks_min_price:
                min_item = _catalog_extreme_item_by_price(candidate_items or items, highest=False)
                if min_item is not None:
                    min_name = _item_label(dict(min_item))
                    min_price = _item_price_int(dict(min_item))
                    if min_name and min_price:
                        return f"{min_name} {_format_rub_price(min_price)}".strip()
            if selected_item is not None and (not asks_min_price):
                selected_name = _item_label(dict(selected_item))
                selected_price = _item_price_int(dict(selected_item))
                if selected_name and selected_price:
                    return f"{selected_name} {_format_rub_price(selected_price)}".strip()
            target_price = _extract_price_target_hint(user_text)
            if target_price:
                nearest = _closest_catalog_item_by_price(candidate_items, target_price)
                if nearest is not None:
                    name = _item_label(dict(nearest))
                    price = _item_price_int(dict(nearest))
                    if name and price:
                        return f"{name} {_format_rub_price(price)}".strip()
            min_price = _catalog_min_price(candidate_items)
            if min_price:
                return _format_rub_price(min_price)

        if asks_model_name:
            if (
                object_type_need in {"apartment", "house"}
                and (not object_filter_restricted)
                and (not object_type_has_evidence)
                and (not has_specific_kw)
            ):
                return _neutralize_catalog_model_mentions(normalized, items)
            if selected_item is not None:
                selected_name = _item_label(dict(selected_item))
                selected_price = _item_price_int(dict(selected_item))
                if selected_name and selected_price:
                    return f"{selected_name} {_format_rub_price(selected_price)}".strip()
            target_price = _extract_price_target_hint(user_text)
            ref_item: Mapping[str, Any] | None = None
            if target_price:
                ref_item = _closest_catalog_item_by_price(candidate_items, target_price)
            if ref_item is None and candidate_items:
                ref_item = candidate_items[0]
            if ref_item is not None:
                name = _item_label(dict(ref_item))
                price = _item_price_int(dict(ref_item))
                if name and price:
                    return f"{name} {_format_rub_price(price)}".strip()
                if name:
                    return str(name or "").strip()

        return normalized

    async def single_llm_reply(
        self,
        client: Any,
        messages: List[Dict[str, str]],
        persona_hints: Any,
        state: Any,
        channel_name: str,
        contact_ref: int,
        tenant: int | None,
        last_user_message: str,
    ) -> str:
        settings = self.deps.settings_obj
        logger = self.deps.logger_obj
        APITimeoutError = self.deps.api_timeout_error_cls
        _env_bool = self.deps.env_bool
        _safe_short_text = self.deps.safe_short_text
        _normalize_fact_key = self.deps.normalize_fact_key
        _display_item_label = self.deps.display_item_label
        infer_user_needs = self.deps.infer_user_needs
        search_catalog = self.deps.search_catalog
        read_all_catalog = self.deps.read_all_catalog
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _canonical_fact_key = self.deps.canonical_fact_key
        _normalize_model_alias = self.deps.normalize_model_alias
        _strict_catalog_item_match = self.deps.strict_catalog_item_match
        _catalog_item_identity = self.deps.catalog_item_identity
        _item_aliases = self.deps.item_aliases
        _item_label = self.deps.item_label
        _item_price_int = self.deps.item_price_int
        _extract_price_spans = self.deps.extract_price_spans
        _format_rub_price = self.deps.format_rub_price
        _grounding_catalog_items = self.deps.grounding_catalog_items
        _selected_item_from_grounding = self.deps.selected_item_from_grounding
        _normalize_text = self.deps.normalize_text
        _extract_attribute_probe = self.deps.extract_attribute_probe
        _is_price_intent = self.deps.is_price_intent
        _looks_like_price_objection = self.deps.looks_like_price_objection
        _classify_turn_intent = self.deps.classify_turn_intent
        _state_facts_snapshot = self.deps.state_facts_snapshot
        _strip_instruction_leaks = self.deps.strip_instruction_leaks
        _resolve_persona_rules_context = self.deps.resolve_persona_rules_context
        _resolve_chat_completion_callable = self.deps.resolve_chat_completion_callable
        _build_human_mode_messages = self.deps.build_human_mode_messages
        _render_direct_reply = self.deps.render_direct_reply
        _ensure_dialog_greeting_on_first_reply = self.deps.ensure_dialog_greeting_on_first_reply
        _apply_base_answer_quality_floor = self.deps.apply_base_answer_quality_floor
        _humanize_reply_text = self.deps.humanize_reply_text
        _apply_persona_sequence_obligations = self.apply_persona_sequence_obligations
        _apply_persona_delivery_obligations = self.deps.apply_persona_delivery_obligations
        _enforce_next_required_fact_question = self.enforce_next_required_fact_question
        _stabilize_followup_price_reference = self.deps.stabilize_followup_price_reference
        _update_fact_memory = self.deps.update_fact_memory
        _remember_questions_from_reply = self.deps.remember_questions_from_reply
        _extract_questions_from_text = self.deps.extract_questions_from_text
        _normalize_slot_name = self.deps.normalize_slot_name
        save_sales_state = self.deps.save_sales_state
        _wrap_llm_reply = self.deps.wrap_llm_reply
        record_bot_reply = self.deps.record_bot_reply
        _llm_unavailable_reply = self.deps.llm_unavailable_reply
        _is_quota_or_rate_limit_error = self.deps.is_quota_or_rate_limit_error
        _audit_and_rewrite_persona_reply = self.audit_and_rewrite_persona_reply
        _VARIANTS_USER_HINT_RE = self.deps.variants_user_hint_re
        _PRICE_INLINE_RE = self.deps.price_inline_re
        _llm_call_with_deadline = self.deps.llm_call_with_deadline
        format_items_for_prompt = self.deps.format_items_for_prompt
        _safe_json_load = self.deps.safe_json_load
        _reply_mentions_catalog_item = self.deps.reply_mentions_catalog_item

        eval_lite_mode = _env_bool("SALES_EVAL_LITE", False)

        def _default_policy() -> Dict[str, Any]:
            return {
                "action": "respond",
                "intent": "general",
                "intent_tags": [],
                "respond_to_user_question_first": True,
                "continue_flow": True,
                "question_strategy": {
                    "should_ask": False,
                    "question_goal": "",
                    "question_fact_key": "",
                },
                "claims": [],
                "fact_updates": [],
                "selected_item_ref": "",
                "reply_plan": {
                    "tone": "persona",
                    "brief": True,
                    "ack": True,
                },
            }

        def _build_policy_grounding() -> Dict[str, Any]:
            merged: List[Dict[str, Any]] = []
            seen: set[str] = set()
            search_needs: Dict[str, Any] = {}

            def _append(items: Sequence[Mapping[str, Any]]) -> None:
                for raw in items or []:
                    item = dict(raw)
                    identity = _catalog_item_identity(item)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    merged.append(item)

            if state.last_items:
                _append(state.last_items)
            if isinstance(state.facts, Mapping):
                for key in ("city", "address", "object_type", "model", "budget", "timeline", "dimensions", "color"):
                    value = _safe_short_text(str(state.facts.get(key) or ""), 120)
                    if value:
                        search_needs[key] = value
            if isinstance(state.needs, Mapping):
                for key in ("keywords", "budget_max", "price_order", "object_type", "color", "dimensions"):
                    value = state.needs.get(key)
                    if value in (None, "", [], {}, ()):
                        continue
                    search_needs[key] = value
            try:
                turn_needs = infer_user_needs(last_user_message)
            except Exception:
                turn_needs = {}
            if isinstance(turn_needs, Mapping):
                for key in ("keywords", "budget_max", "price_order", "object_type", "color", "dimensions"):
                    value = turn_needs.get(key)
                    if value in (None, "", [], {}, ()):
                        continue
                    search_needs[key] = value
            if tenant is not None:
                try:
                    _append(search_catalog(search_needs, limit=8, tenant=tenant, query=last_user_message))
                except Exception:
                    pass
                if not merged:
                    try:
                        _append(read_all_catalog(tenant=tenant)[:8])
                    except Exception:
                        pass

            selected_item: Dict[str, Any] | None = None
            selected_hint = str((state.facts or {}).get("model") or "").strip()
            if selected_hint and merged:
                matched = _best_catalog_item_match(selected_hint, merged)
                if isinstance(matched, Mapping):
                    selected_item = dict(matched)
            return {
                "items": [dict(item) for item in merged[:8]],
                "selected_item": dict(selected_item) if isinstance(selected_item, Mapping) else None,
            }

        def _coerce_policy(raw: Mapping[str, Any] | None) -> Dict[str, Any]:
            plan = _default_policy()
            if not isinstance(raw, Mapping):
                return plan
            plan["action"] = str(raw.get("action") or "respond").strip().lower() or "respond"
            plan["intent"] = str(raw.get("intent") or "general").strip().lower() or "general"
            raw_tags = raw.get("intent_tags")
            tags: List[str] = []
            if isinstance(raw_tags, Sequence):
                for tag in raw_tags[:8]:
                    normalized = str(tag or "").strip().lower()
                    if normalized and normalized not in tags:
                        tags.append(normalized)
            heuristic_tags: list[str] = []
            if _is_price_intent(last_user_message) or _looks_like_price_objection(last_user_message):
                heuristic_tags.append("price")
            if _VARIANTS_USER_HINT_RE.search(last_user_message):
                heuristic_tags.append("variants")
            if _extract_attribute_probe(last_user_message):
                heuristic_tags.append("attributes")
            turn_intent_hint = _classify_turn_intent(last_user_message, known_facts=state.facts)
            if turn_intent_hint in {"repair", "catalog_problem"}:
                heuristic_tags.append("repair")
            if re.search(r"(?iu)\b(тоже\s+сам|одно\s+и\s+то\s+же|опять\s+то\s+же|повтор)\b", last_user_message):
                heuristic_tags.append("complaint")
            for tag in heuristic_tags:
                if tag not in tags:
                    tags.append(tag)
            plan["intent_tags"] = tags
            plan["respond_to_user_question_first"] = bool(raw.get("respond_to_user_question_first", True))
            plan["continue_flow"] = bool(raw.get("continue_flow", True))
            qs_raw = raw.get("question_strategy")
            if isinstance(qs_raw, Mapping):
                plan["question_strategy"] = {
                    "should_ask": bool(qs_raw.get("should_ask", False)),
                    "question_goal": str(qs_raw.get("question_goal") or "").strip(),
                    "question_fact_key": _canonical_fact_key(str(qs_raw.get("question_fact_key") or "")) or "",
                }
            rp_raw = raw.get("reply_plan")
            if isinstance(rp_raw, Mapping):
                plan["reply_plan"] = {
                    "tone": str(rp_raw.get("tone") or "persona").strip() or "persona",
                    "brief": bool(rp_raw.get("brief", True)),
                    "ack": bool(rp_raw.get("ack", True)),
                }
            claims: List[Dict[str, Any]] = []
            raw_claims = raw.get("claims")
            if isinstance(raw_claims, Sequence):
                for item in raw_claims[:16]:
                    if not isinstance(item, Mapping):
                        continue
                    claims.append(
                        {
                            "type": str(item.get("type") or "").strip().lower(),
                            "subject": str(item.get("subject") or "").strip(),
                            "attribute": str(item.get("attribute") or "").strip(),
                            "value": str(item.get("value") or "").strip(),
                            "confidence": float(item.get("confidence") or 0.0),
                        }
                    )
            plan["claims"] = claims
            updates: List[Dict[str, str]] = []
            raw_updates = raw.get("fact_updates")
            if isinstance(raw_updates, Sequence):
                for item in raw_updates[:16]:
                    if not isinstance(item, Mapping):
                        continue
                    fact_key = _canonical_fact_key(str(item.get("fact_key") or "")) or _normalize_fact_key(
                        str(item.get("fact_key") or "")
                    )
                    value = _safe_short_text(str(item.get("value") or ""), 160)
                    if not fact_key or not value:
                        continue
                    updates.append(
                        {
                            "fact_key": fact_key,
                            "value": value,
                            "source": str(item.get("source") or "model_inferred").strip() or "model_inferred",
                        }
                    )
            plan["fact_updates"] = updates
            plan["selected_item_ref"] = str(raw.get("selected_item_ref") or "").strip()
            return plan

        def _match_grounded_item(
            ref: str,
            items: Sequence[Mapping[str, Any]],
            selected_item: Mapping[str, Any] | None,
        ) -> Dict[str, Any] | None:
            value = str(ref or "").strip()
            if not value and isinstance(selected_item, Mapping):
                return dict(selected_item)
            if not value:
                return None
            value_norm = _normalize_model_alias(value)
            if not value_norm:
                return None
            for item in items:
                item_map = dict(item)
                identity = _normalize_model_alias(_catalog_item_identity(item_map))
                if identity and identity == value_norm:
                    return item_map
                for alias in _item_aliases(item_map):
                    alias_norm = _normalize_model_alias(alias)
                    if not alias_norm:
                        continue
                    if alias_norm == value_norm or alias_norm in value_norm or value_norm in alias_norm:
                        return item_map
            strict = _strict_catalog_item_match(value, items)
            if isinstance(strict, Mapping):
                return dict(strict)
            best = _best_catalog_item_match(value, items)
            if isinstance(best, Mapping):
                return dict(best)
            return None

        def _validate_policy_claims(
            policy: Dict[str, Any],
            grounding_map: Mapping[str, Any],
        ) -> tuple[List[Dict[str, Any]], List[str]]:
            allowed_types = {
                "catalog_attribute",
                "catalog_price",
                "catalog_item_identity",
                "catalog_shortlist_offer",
                "state_ack",
                "business_rule",
                "channel_action",
                "handoff_action",
                "soft_sales_claim",
            }
            items = [dict(item) for item in _grounding_catalog_items(grounding_map)]
            selected_item = _selected_item_from_grounding(grounding_map, items)
            validated: List[Dict[str, Any]] = []
            dropped: List[str] = []

            for raw in list(policy.get("claims") or []):
                if not isinstance(raw, Mapping):
                    continue
                claim_type = str(raw.get("type") or "").strip().lower()
                if claim_type not in allowed_types:
                    dropped.append("unsupported_claim_type")
                    continue
                subject = str(raw.get("subject") or "").strip()
                attribute = str(raw.get("attribute") or "").strip()
                value = str(raw.get("value") or "").strip()
                target = _match_grounded_item(subject, items, selected_item)

                if claim_type == "catalog_shortlist_offer":
                    if not items:
                        dropped.append("shortlist_without_grounding")
                        continue
                    validated.append(
                        {
                            "type": claim_type,
                            "subject": subject,
                            "attribute": "",
                            "value": value,
                            "confidence": float(raw.get("confidence") or 0.0),
                        }
                    )
                    continue

                if claim_type == "catalog_item_identity":
                    if not target:
                        dropped.append("identity_without_match")
                        continue
                    item_id = _catalog_item_identity(target)
                    validated.append(
                        {
                            "type": claim_type,
                            "subject": _display_item_label(target) or _item_label(target),
                            "item_id": item_id,
                            "attribute": "",
                            "value": value,
                            "confidence": float(raw.get("confidence") or 0.0),
                        }
                    )
                    continue

                if claim_type == "catalog_price":
                    if not target:
                        dropped.append("price_without_match")
                        continue
                    item_id = _catalog_item_identity(target)
                    price_value = _item_price_int(target)
                    if not isinstance(price_value, int) or price_value <= 0:
                        dropped.append("price_without_numeric_value")
                        continue
                    if value:
                        spans = _extract_price_spans(value)
                        claim_price = int(spans[0][2]) if spans else None
                        if claim_price is not None and claim_price != int(price_value):
                            dropped.append("price_value_mismatch")
                            continue
                    validated.append(
                        {
                            "type": claim_type,
                            "subject": _display_item_label(target) or _item_label(target),
                            "item_id": item_id,
                            "attribute": "price",
                            "value": _format_rub_price(int(price_value)),
                            "confidence": float(raw.get("confidence") or 0.0),
                        }
                    )
                    continue

                if claim_type == "catalog_attribute":
                    if not target:
                        dropped.append("attribute_without_match")
                        continue
                    item_id = _catalog_item_identity(target)
                    attr_norm = _normalize_text(attribute)
                    val_norm = _normalize_text(value)
                    resolved_attr = ""
                    resolved_val = ""
                    for k, v in dict(target).items():
                        key = str(k or "").strip()
                        val = str(v or "").strip()
                        if not key or not val:
                            continue
                        key_norm = _normalize_text(key)
                        if key_norm in {"title", "name", "id", "sku", "url", "price", "_search_text"}:
                            continue
                        attr_match = bool(attr_norm and (attr_norm in key_norm or key_norm in attr_norm))
                        val_match = bool(val_norm and (val_norm in _normalize_text(val) or _normalize_text(val) in val_norm))
                        if attr_norm and val_norm and not (attr_match and val_match):
                            continue
                        if attr_norm and not attr_match:
                            continue
                        if val_norm and not val_match:
                            continue
                        resolved_attr = key
                        resolved_val = val
                        break
                    if not resolved_attr:
                        dropped.append("attribute_not_grounded")
                        continue
                    validated.append(
                        {
                            "type": claim_type,
                            "subject": _display_item_label(target) or _item_label(target),
                            "item_id": item_id,
                            "attribute": resolved_attr,
                            "value": resolved_val,
                            "confidence": float(raw.get("confidence") or 0.0),
                        }
                    )
                    continue

                validated.append(
                    {
                        "type": claim_type,
                        "subject": subject,
                        "attribute": attribute,
                        "value": value,
                        "confidence": float(raw.get("confidence") or 0.0),
                    }
                )
            return validated, dropped

        def _seed_policy_catalog_claims(
            policy: Dict[str, Any],
            grounding_map: Mapping[str, Any],
        ) -> None:
            tags = {
                str(tag or "").strip().lower()
                for tag in (policy.get("intent_tags") or [])
                if str(tag or "").strip()
            }
            requires_catalog = bool({"price", "variants", "attributes", "selection"} & tags)
            if not requires_catalog:
                return
            claims = [dict(item) for item in list(policy.get("claims") or []) if isinstance(item, Mapping)]
            has_catalog_claim = any(
                str(item.get("type") or "").strip().lower().startswith("catalog_")
                for item in claims
            )
            if has_catalog_claim:
                return
            items = [dict(item) for item in _grounding_catalog_items(grounding_map)]
            if not items:
                return
            limit = 2 if bool({"variants", "selection"} & tags) else 1
            for item in items[:limit]:
                label = _display_item_label(item) or _item_label(item)
                if not label:
                    continue
                claims.append(
                    {
                        "type": "catalog_item_identity",
                        "subject": label,
                        "attribute": "",
                        "value": "",
                        "confidence": 0.6,
                    }
                )
                price_value = _item_price_int(item)
                if isinstance(price_value, int) and price_value > 0:
                    claims.append(
                        {
                            "type": "catalog_price",
                            "subject": label,
                            "attribute": "price",
                            "value": _format_rub_price(price_value),
                            "confidence": 0.6,
                        }
                    )
            policy["claims"] = claims

        def _constrain_claims_by_turn_intent(
            policy: Dict[str, Any],
            validated_claims: Sequence[Mapping[str, Any]],
        ) -> List[Dict[str, Any]]:
            tags = {
                str(tag or "").strip().lower()
                for tag in (policy.get("intent_tags") or [])
                if str(tag or "").strip()
            }
            # Catalog claims are allowed only when user intent explicitly requires
            # factual catalog grounding (price/variants/attributes/repair/selection).
            allow_catalog_claims = bool(
                {"price", "variants", "attributes", "repair", "complaint", "selection"} & tags
            )
            allowed_without_catalog = {"state_ack", "business_rule", "channel_action", "handoff_action"}
            constrained: List[Dict[str, Any]] = []
            for raw in list(validated_claims or []):
                claim = dict(raw)
                claim_type = str(claim.get("type") or "").strip().lower()
                if allow_catalog_claims:
                    constrained.append(claim)
                    continue
                if claim_type in allowed_without_catalog:
                    constrained.append(claim)
            return constrained

        def _apply_policy_state_updates(policy: Dict[str, Any], grounding_map: Mapping[str, Any]) -> None:
            if not isinstance(state.facts, dict):
                state.facts = {}
            if not isinstance(state.known_slots, dict):
                state.known_slots = {}

            for item in list(policy.get("fact_updates") or []):
                if not isinstance(item, Mapping):
                    continue
                key = _canonical_fact_key(str(item.get("fact_key") or "")) or _normalize_fact_key(
                    str(item.get("fact_key") or "")
                )
                value = _safe_short_text(str(item.get("value") or ""), 160)
                if not key or not value:
                    continue
                state.facts[key] = value
                if key in {"city", "address", "object_type", "model", "budget", "timeline", "contact", "quantity", "color"}:
                    state.known_slots[key] = value

            selected_ref = str(policy.get("selected_item_ref") or "").strip()
            if selected_ref:
                items = [dict(item) for item in _grounding_catalog_items(grounding_map)]
                selected = _selected_item_from_grounding(grounding_map, items)
                matched = _match_grounded_item(selected_ref, items, selected)
                if isinstance(matched, Mapping):
                    model_label = _display_item_label(matched) or _item_label(matched)
                    if model_label:
                        state.facts["model"] = _safe_short_text(model_label, 180)
                        state.known_slots["model"] = _safe_short_text(model_label, 120)
                    reorder: List[Dict[str, Any]] = [dict(matched)]
                    matched_identity = _catalog_item_identity(dict(matched))
                    for candidate in items:
                        identity = _catalog_item_identity(dict(candidate))
                        if identity == matched_identity:
                            continue
                        reorder.append(dict(candidate))
                    state.last_items = reorder[:8]

        async def _llm_policy_decision(
            create_fn: Any,
            prepared_messages: List[Dict[str, str]],
            known_facts: Mapping[str, str],
            grounding_map: Mapping[str, Any],
            persona_context: str,
        ) -> Dict[str, Any]:
            dialogue_tail = [
                {"role": str(item.get("role") or ""), "content": str(item.get("content") or "")}
                for item in prepared_messages
                if str(item.get("role") or "").strip().lower() in {"user", "assistant"}
            ][-10:]
            persona_excerpt = str(persona_context or "").strip()
            if len(persona_excerpt) > 7000:
                persona_excerpt = persona_excerpt[:7000]
            pending_fact_key = _canonical_fact_key(str(state.pending_fact_key or "")) or ""
            grounding_preview = format_items_for_prompt(
                [dict(item) for item in _grounding_catalog_items(grounding_map)[:5]],
                "₽",
            )
            policy_system = (
                "You are a policy planner for a sales chatbot. "
                "You MUST follow persona instructions from context as hard contract. "
                "Prioritize current user intent first and never repeat already answered question. "
                "Return JSON only with schema keys: "
                "action,intent,intent_tags,respond_to_user_question_first,continue_flow,"
                "question_strategy,claims,fact_updates,selected_item_ref,reply_plan. "
                "intent_tags: array with zero or more tags from "
                "[price,variants,attributes,repair,complaint,handoff,selection]. "
                "question_strategy keys: should_ask,question_goal,question_fact_key. "
                "claims item keys: type,subject,attribute,value,confidence. "
                "fact_updates item keys: fact_key,value,source. "
                "Allowed claim types: catalog_attribute,catalog_price,catalog_item_identity,"
                "catalog_shortlist_offer,state_ack,business_rule,channel_action,handoff_action,soft_sales_claim. "
                "Keep claims empty unless they are needed to answer the current user turn. "
                "For greeting/rapport turns without product question, do not output catalog claims. "
                "Set question_strategy.should_ask=false when the user asked a direct factual question and it is answerable from grounding. "
                "Always prioritize current user turn over scripted next step."
            )
            policy_user = (
                f"Persona contract:\n{persona_excerpt or 'none'}\n\n"
                f"Known facts: {json.dumps(dict(known_facts or {}), ensure_ascii=False)}\n"
                f"Current pending_fact_key: {pending_fact_key or 'none'}\n"
                f"Last assistant reply: {str(state.last_bot_reply or '').strip() or 'none'}\n"
                f"Grounded catalog preview:\n{grounding_preview or 'none'}\n"
                f"Last user message: {last_user_message}\n"
                f"Recent dialogue: {json.dumps(dialogue_tail, ensure_ascii=False)}"
            )
            try:
                response = await _llm_call_with_deadline(
                    create_fn,
                    timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                    model=settings.OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": policy_system},
                        {"role": "user", "content": policy_user},
                    ],
                    temperature=0.0,
                    max_tokens=480,
                    response_format={"type": "json_object"},
                    timeout=settings.OPENAI_TIMEOUT_SECONDS,
                )
                content = extract_llm_response_text(response)
                if content:
                    parsed = _safe_json_load(content)
                    return _coerce_policy(parsed)
            except Exception:
                pass
            return _default_policy()

        async def _render_policy_reply(
            create_fn: Any,
            prepared_messages: List[Dict[str, str]],
            policy: Mapping[str, Any],
            grounding_map: Mapping[str, Any],
        ) -> str:
            grounding_preview = format_items_for_prompt(
                [dict(item) for item in _grounding_catalog_items(grounding_map)[:6]],
                "₽",
            )
            validated_catalog_claims = [
                {
                    "type": str(item.get("type") or "").strip(),
                    "subject": str(item.get("subject") or "").strip(),
                    "item_id": str(item.get("item_id") or "").strip(),
                    "attribute": str(item.get("attribute") or "").strip(),
                    "value": str(item.get("value") or "").strip(),
                }
                for item in list(policy.get("claims") or [])
                if isinstance(item, Mapping)
                and str(item.get("type") or "").strip().lower().startswith("catalog_")
            ]
            render_system = (
                "Сформируй финальный ответ клиенту. "
                "Сначала ответь на текущий смысл последней реплики, затем один уместный следующий шаг. "
                "Следуй персоне и не используй служебные фразы. "
                "Не начинай с «Понял/Поняла/Спасибо, что уточнили». "
                "Не выдумывай товары/характеристики вне grounded catalog context. "
                "Для factual-утверждений используй только validated_catalog_claims. "
                "Если validated_catalog_claims пустые или в них нет нужного факта, не добавляй этот факт в ответ. "
                "Если intent_tags содержит variants или selection — покажи минимум один конкретный альтернативный вариант из каталога и назови его как вариант/модель. "
                "Если intent_tags содержит price — обязательно закрой вопрос цены явным ценовым объяснением. "
                "Если intent_tags содержит attributes — обязательно дай конкретный параметр/характеристику. "
                "Если intent_tags содержит repair или complaint — сначала обработай это и затем верни к подбору с конкретными моделями/вариантами; явно покажи, что можешь сразу предложить подходящие модели. "
                "Если в policy есть missing_coverage — исправь именно эти пробелы. "
                "1-3 коротких предложения, максимум 1 вопрос."
            )
            render_user = (
                f"Политика (JSON): {json.dumps(dict(policy or {}), ensure_ascii=False)}\n\n"
                f"Validated catalog claims: {json.dumps(validated_catalog_claims, ensure_ascii=False)}\n\n"
                f"Grounded catalog context:\n{grounding_preview or 'none'}\n\n"
                f"Последняя реплика клиента: {last_user_message}"
            )
            render_messages = [
                prepared_messages[0] if prepared_messages else {"role": "system", "content": ""},
                *prepared_messages[1:],
                {"role": "system", "content": render_system},
                {"role": "user", "content": render_user},
            ]
            for token_limit in (260, 520):
                response = await _llm_call_with_deadline(
                    create_fn,
                    timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                    model=settings.OPENAI_MODEL,
                    messages=render_messages,
                    temperature=0.1,
                    top_p=0.9,
                    max_tokens=token_limit,
                    timeout=settings.OPENAI_TIMEOUT_SECONDS,
                )
                answer_text = extract_llm_response_text(response)
                if answer_text:
                    return answer_text
            return ""

        def _mentioned_catalog_ids(text: str, items: Sequence[Mapping[str, Any]]) -> set[str]:
            candidate = _normalize_text(text)
            if not candidate:
                return set()
            hits: set[str] = set()
            for raw_item in items:
                item = dict(raw_item)
                identity = _catalog_item_identity(item)
                if not identity:
                    continue
                for alias in _item_aliases(item):
                    alias_norm = _normalize_model_alias(alias)
                    if len(alias_norm) < 4:
                        continue
                    if alias_norm in candidate:
                        hits.add(identity)
                        break
            return hits

        def _missing_intent_coverage(
            text: str,
            policy: Mapping[str, Any],
            grounding_map: Mapping[str, Any],
        ) -> List[str]:
            candidate = str(text or "").strip()
            if not candidate:
                return ["empty"]
            tags = [
                str(tag or "").strip().lower()
                for tag in (policy.get("intent_tags") or [])
                if str(tag or "").strip()
            ]
            items = [dict(item) for item in _grounding_catalog_items(grounding_map)]
            missing: List[str] = []
            variant_like = ("variants" in tags) or ("selection" in tags)
            if variant_like and items:
                mentions_item = _reply_mentions_catalog_item(candidate, items)
                has_price = bool(_PRICE_INLINE_RE.search(candidate))
                if not mentions_item and not has_price:
                    missing.append("variants")
                if not re.search(r"(?iu)\b(вариант|модель)\w*\b", candidate):
                    missing.append("variants_wording")
                prev_ids = _mentioned_catalog_ids(str(state.last_bot_reply or ""), items)
                current_ids = _mentioned_catalog_ids(candidate, items)
                all_ids = {_catalog_item_identity(dict(item)) for item in items}
                available_new = {item_id for item_id in all_ids if item_id and item_id not in prev_ids}
                if prev_ids and current_ids and current_ids.issubset(prev_ids):
                    if len(items) > len(current_ids):
                        missing.append("variants_alternative")
                if prev_ids and current_ids and available_new and (current_ids & prev_ids):
                    missing.append("variants_no_repeat")
            if "price" in tags:
                has_price_signal = bool(
                    _PRICE_INLINE_RE.search(candidate)
                    or re.search(r"(?iu)\b(цена|диапазон|дешевле|альтернатив\w*)\b", candidate)
                )
                if not has_price_signal:
                    missing.append("price")
            if "attributes" in tags:
                has_attribute_signal = bool(re.search(r"\d", candidate)) or ":" in candidate
                if not has_attribute_signal:
                    missing.append("attributes")
            if ("repair" in tags or "complaint" in tags) and items:
                has_catalog_recovery = _reply_mentions_catalog_item(candidate, items) or bool(
                    _PRICE_INLINE_RE.search(candidate)
                )
                if not has_catalog_recovery:
                    missing.append("repair_catalog_recovery")
                has_recovery_offer = bool(
                    re.search(
                        r"(?iu)\b(могу\s+сразу\s+предлож\w*|подходящ\w*\s+(модел\w*|вариант\w*))\b",
                        candidate,
                    )
                )
                if not has_recovery_offer:
                    missing.append("repair_offer")
            if "complaint" in tags and items:
                has_alternative_signal = bool(
                    re.search(r"(?iu)\b(друг\w*\s+вариант\w*|альтернатив\w*|что\s+важнее)\b", candidate)
                )
                if not has_alternative_signal:
                    missing.append("complaint_alternative")
            return missing

        try:
            create_fn = _resolve_chat_completion_callable(client)
            if not create_fn:
                raise RuntimeError("openai client missing chat.completions.create")

            prepared_messages = _build_human_mode_messages(messages)
            grounding = _build_policy_grounding()
            known_facts = _state_facts_snapshot(state)
            persona_context = ""
            if prepared_messages and str(prepared_messages[0].get("role") or "").strip().lower() == "system":
                persona_context = str(prepared_messages[0].get("content") or "")
            persona_rules_context = _resolve_persona_rules_context(
                tenant=tenant,
                channel_name=channel_name,
                fallback_context=persona_context,
            )

            policy = await _llm_policy_decision(
                create_fn,
                prepared_messages,
                known_facts,
                grounding,
                persona_rules_context,
            )
            _seed_policy_catalog_claims(policy, grounding)
            validated_claims, dropped_claims = _validate_policy_claims(policy, grounding)
            policy["claims"] = _constrain_claims_by_turn_intent(policy, validated_claims)
            if dropped_claims:
                policy["dropped_claims"] = dropped_claims

            _apply_policy_state_updates(policy, grounding)

            answer = await _render_policy_reply(create_fn, prepared_messages, policy, grounding)
            if not answer:
                answer = await _render_direct_reply(
                    create_fn,
                    model=settings.OPENAI_MODEL,
                    timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                    prepared_messages=prepared_messages,
                )
            if not answer:
                raise RuntimeError("empty llm render")
            if not eval_lite_mode:
                missing_coverage = _missing_intent_coverage(answer, policy, grounding)
                if missing_coverage:
                    retry_policy = dict(policy or {})
                    retry_policy["missing_coverage"] = list(missing_coverage)
                    retry_answer = await _render_policy_reply(
                        create_fn,
                        prepared_messages,
                        retry_policy,
                        grounding,
                    )
                    if retry_answer:
                        answer = retry_answer

            answer = _strip_instruction_leaks(answer)
            final_answer = str(answer or "").strip()
            if not eval_lite_mode:
                final_answer = await _audit_and_rewrite_persona_reply(
                    create_fn,
                    model=settings.OPENAI_MODEL,
                    timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                    prepared_messages=prepared_messages,
                    answer=final_answer,
                    last_user_message=last_user_message,
                    state=state,
                    grounding=grounding,
                    policy=policy,
                )
            final_answer = _ensure_dialog_greeting_on_first_reply(
                final_answer,
                state,
                persona_context=persona_rules_context,
            )
            final_answer = _apply_base_answer_quality_floor(
                final_answer,
                state=state,
                persona_hints=persona_hints,
                grounding=grounding,
                user_text=last_user_message,
            )
            known_facts_after = _state_facts_snapshot(state)
            final_answer = _apply_persona_sequence_obligations(
                final_answer,
                persona_context=persona_rules_context,
                last_user_message=last_user_message,
                known_facts=known_facts_after,
                state=state,
            )
            final_answer = _apply_persona_delivery_obligations(
                final_answer,
                persona_context=persona_rules_context,
                channel_name=channel_name,
                last_user_message=last_user_message,
                known_facts=known_facts_after,
                state=state,
            )
            final_answer, enforced_pending_key = _enforce_next_required_fact_question(
                final_answer,
                state=state,
                persona_context=persona_rules_context,
                known_facts=known_facts_after,
                user_text=last_user_message,
                grounding=grounding,
            )
            final_answer = _stabilize_followup_price_reference(
                final_answer,
                state=state,
                user_text=last_user_message,
                grounding=grounding,
            )
            final_answer = _apply_base_answer_quality_floor(
                final_answer,
                state=state,
                persona_hints=persona_hints,
                grounding=grounding,
                user_text=last_user_message,
            )
            final_answer = _humanize_reply_text(
                final_answer,
                state=state,
                persona_hints=persona_hints,
            )
            final_answer = _apply_base_answer_quality_floor(
                final_answer,
                state=state,
                persona_hints=persona_hints,
                grounding=grounding,
                user_text=last_user_message,
            )
            if not final_answer:
                final_answer = str(answer or "").strip()

            actual_questions = _extract_questions_from_text(final_answer)
            question_strategy = policy.get("question_strategy")
            pending_key = _canonical_fact_key(str(enforced_pending_key or "")) or ""
            if (
                (not pending_key)
                and isinstance(question_strategy, Mapping)
                and bool(question_strategy.get("should_ask"))
                and actual_questions
            ):
                pending_key = _canonical_fact_key(str(question_strategy.get("question_fact_key") or "")) or ""
            if not pending_key and actual_questions:
                inferred = _normalize_slot_name("", question=actual_questions[0])
                pending_key = _canonical_fact_key(inferred) or ""
            state.pending_fact_key = pending_key
            state.pending_slot = ""
            state.last_plan = dict(policy or {})
            _update_fact_memory(state, final_answer)
            _remember_questions_from_reply(state, final_answer)
            save_sales_state(state)
            result = _wrap_llm_reply(final_answer, plan=dict(policy or {}), raw_answer=answer)
            record_bot_reply(contact_ref, tenant, channel_name, str(result))
            return result
        except APITimeoutError as exc:
            logger.warning("single llm timeout: %s", exc)
            raise
        except Exception as exc:
            if _is_quota_or_rate_limit_error(exc):
                logger.warning("single llm quota/rate limited, fallback enabled")
                fallback = _llm_unavailable_reply(
                    user_text=last_user_message,
                )
                return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)
            logger.exception("single llm failed", exc_info=exc)
            raise
        fallback = _llm_unavailable_reply(
            user_text=last_user_message,
        )
        return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)

    async def audit_and_rewrite_persona_reply(
        self,
        create_fn: Any,
        *,
        model: str,
        timeout_seconds: float,
        prepared_messages: List[Dict[str, str]],
        answer: str,
        last_user_message: str,
        state: Any = None,
        grounding: Mapping[str, Any] | None = None,
        policy: Mapping[str, Any] | None = None,
    ) -> str:
        _strip_instruction_leaks = self.deps.strip_instruction_leaks
        _llm_call_with_deadline = self.deps.llm_call_with_deadline
        _safe_json_load = self.deps.safe_json_load
        _enforce_sentence_budget = self.deps.enforce_sentence_budget
        _rewrite_loses_context_anchors = self.deps.rewrite_loses_context_anchors
        _reply_has_repeated_question = self.deps.reply_has_repeated_question
        _grounding_catalog_items = self.deps.grounding_catalog_items
        format_items_for_prompt = self.deps.format_items_for_prompt
        _selected_item_from_grounding = self.deps.selected_item_from_grounding
        _item_label = self.deps.item_label
        _normalize_text = self.deps.normalize_text
        _extract_attribute_probe = self.deps.extract_attribute_probe
        _is_price_intent = self.deps.is_price_intent
        _reply_mentions_catalog_item = self.deps.reply_mentions_catalog_item
        _catalog_claim_coverage_issues = self.catalog_claim_coverage_issues
        _MODEL_NAME_INTENT_RE = self.deps.model_name_intent_re
        _VARIANTS_USER_HINT_RE = self.deps.variants_user_hint_re
        _CONTACT_URL_RE = self.deps.contact_url_re
        _CONTACT_HANDLE_RE = self.deps.contact_handle_re
        _CONTACT_PHONE_RE = self.deps.contact_phone_re
        _GREETING_PREFIX_RE = self.deps.greeting_prefix_re
        _QUESTION_CUE_RE = self.deps.question_cue_re
        _REPAIR_TURN_RE = self.deps.repair_turn_re
        _CATALOG_UNAVAILABLE_RE = self.deps.catalog_unavailable_re
        _PRICE_INLINE_RE = self.deps.price_inline_re
        _FACT_TOKEN_RE = self.deps.fact_token_re
        _GENERIC_FACT_STOPWORDS = self.deps.generic_fact_stopwords
        quality_dedupe = self.deps.quality_dedupe_repeated_blocks
        SalesState = self.deps.sales_state_cls

        # Two-stage quality gate:
        # 1) deterministic cleanup
        # 2) LLM judge + optional bounded rewrite
        candidate = (answer or "").strip()
        if not candidate:
            return candidate

        dialogue_tail = [
            {"role": str(m.get("role") or ""), "content": str(m.get("content") or "")}
            for m in (prepared_messages or [])
            if str(m.get("role") or "").strip().lower() in {"user", "assistant"}
        ][-6:]

        # Deterministic cleanup only.
        rewrite = _strip_instruction_leaks(candidate)
        rewrite = quality_dedupe(rewrite)
        rewrite = re.sub(r"[ \t]+", " ", rewrite)
        rewrite = re.sub(r"\n{3,}", "\n\n", rewrite).strip()
        if not rewrite:
            return candidate

        # Do not lose factual anchors from user context.
        if _rewrite_loses_context_anchors(candidate, rewrite, dialogue_tail):
            return candidate

        # Do not drop contact artifacts if they already existed in answer.
        original_artifacts: set[str] = set()
        for token in _CONTACT_URL_RE.findall(candidate):
            original_artifacts.add(token)
        for token in _CONTACT_HANDLE_RE.findall(candidate):
            original_artifacts.add(token)
        for token in _CONTACT_PHONE_RE.findall(candidate):
            original_artifacts.add(token.strip())
        if original_artifacts and not all(artifact in rewrite for artifact in original_artifacts):
            return candidate

        # Keep substantive answer if cleanup accidentally over-shrunk text.
        if len(rewrite) < 8 and len(candidate) > 24:
            return candidate
        if isinstance(state, SalesState) and _reply_has_repeated_question(rewrite, state):
            # Rewriting should not reintroduce repeated question loops.
            return candidate

        candidate = rewrite

        persona_context = ""
        for message in prepared_messages or []:
            if str(message.get("role") or "").strip().lower() != "system":
                continue
            chunk = str(message.get("content") or "").strip()
            if chunk:
                persona_context = chunk
                break
        if len(persona_context) > 5000:
            persona_context = persona_context[:5000]
        grounding_items = _grounding_catalog_items(grounding)
        grounding_preview = format_items_for_prompt(
            [dict(item) for item in list(grounding_items or [])[:6]], "₽"
        )
        validated_catalog_claims = [
            {
                "type": str(item.get("type") or "").strip(),
                "subject": str(item.get("subject") or "").strip(),
                "item_id": str(item.get("item_id") or "").strip(),
                "attribute": str(item.get("attribute") or "").strip(),
                "value": str(item.get("value") or "").strip(),
            }
            for item in list((policy or {}).get("claims") or [])
            if isinstance(item, Mapping)
            and str(item.get("type") or "").strip().lower().startswith("catalog_")
        ]
        selected_item = _selected_item_from_grounding(grounding, grounding_items)
        selected_label = _item_label(dict(selected_item)) if isinstance(selected_item, Mapping) else ""
        policy_tags = {
            str(tag or "").strip().lower()
            for tag in (policy or {}).get("intent_tags", [])
            if str(tag or "").strip()
        }
        user_raw = str(last_user_message or "")
        user_norm = _normalize_text(user_raw)
        user_tokens = [
            tok
            for tok in _FACT_TOKEN_RE.findall(user_norm)
            if len(tok) >= 3 and not tok.isdigit() and tok not in _GENERIC_FACT_STOPWORDS
        ]
        greeting_like = bool(_GREETING_PREFIX_RE.match(user_raw))
        attr_probe = _extract_attribute_probe(user_raw)
        has_attr_intent_cue = bool(_QUESTION_CUE_RE.search(user_raw) or "?" in user_raw)
        lexical_attribute_intent = bool(attr_probe) and not greeting_like and (
            has_attr_intent_cue
            or bool(_MODEL_NAME_INTENT_RE.search(user_raw))
            or bool(_VARIANTS_USER_HINT_RE.search(user_raw))
            or bool(_is_price_intent(user_raw))
            or len(user_tokens) >= 2
        )
        intent_flags = {
            "price_intent": bool("price" in policy_tags) or bool(_is_price_intent(last_user_message)),
            "variants_intent": bool({"variants", "selection"} & policy_tags)
            or bool(_VARIANTS_USER_HINT_RE.search(str(last_user_message or ""))),
            "repair_intent": bool(
                {"repair", "complaint"} & policy_tags
                or
                _REPAIR_TURN_RE.match(str(last_user_message or ""))
                or _CATALOG_UNAVAILABLE_RE.search(str(last_user_message or ""))
            ),
            "attribute_intent": bool("attributes" in policy_tags) or lexical_attribute_intent,
        }

        judge_prompt = (
            "Ты quality-judge ответа менеджера. Верни только JSON: "
            '{"ok":true|false,"rewrite_needed":true|false,"issues":["..."],'
            '"needs":{"price":true|false,"variants":true|false,"attributes":true|false,"catalog_recovery":true|false}}. '
            "Критерии: "
            "1) ответ уместен последней реплике клиента, "
            "2) нет повтора уже заданного вопроса, "
            "3) нет служебной/инструктивной речи, "
            "4) соблюдены ограничения персоны по тону и шагам, "
            "5) нет неподтверждённых factual-утверждений, которых нет в grounded catalog context, "
            "6) нет роботизированных стартов вроде 'Понял', 'Поняла', 'Спасибо, что уточнили', "
            "7) если клиент просит варианты/цену/характеристики, ответ должен содержать конкретику из grounded catalog context, "
            "8) максимум один вопрос в ответе. "
            "9) если intent_flags.price_intent=true, ответ должен явно закрывать вопрос цены "
            "(цена/диапазон/дешевле/альтернатива) без ухода в общие фразы. "
            "10) если intent_flags.variants_intent=true, ответ должен показать конкретные варианты/модели "
            "или сразу перейти к показу, без абстрактного ответа. "
            "11) если intent_flags.repair_intent=true, ответ должен вернуть диалог к каталогу/вариантам, "
            "а не зависать в пустом уточнении. "
            "12) factual-утверждения о каталоге разрешены только если они есть в validated_catalog_claims. "
            "Если в ответе есть факт про модель/цену/характеристику вне validated_catalog_claims — rewrite_needed=true. "
            "12) выстави needs.* на основе последней реплики клиента и хвоста диалога: "
            "price=true если ждут ответ по цене/дешевле/диапазону; "
            "variants=true если ждут показать/предложить варианты; "
            "attributes=true если ждут характеристики/качество/параметры; "
            "catalog_recovery=true если диалог нужно вернуть к каталогу/подбору после сбоя/непонимания. "
            "Если ответ уже хороший — ok=true."
        )
        judge_user = (
            f"Персона:\n{persona_context or 'нет'}\n\n"
            f"Grounded catalog context:\n{grounding_preview or 'нет'}\n"
            f"Selected item: {selected_label or 'нет'}\n\n"
            f"Validated catalog claims: {json.dumps(validated_catalog_claims, ensure_ascii=False)}\n\n"
            f"Intent flags: {json.dumps(intent_flags, ensure_ascii=False)}\n\n"
            f"Последняя реплика клиента: {last_user_message}\n"
            f"Текущий ответ: {candidate}\n"
            f"Хвост диалога: {json.dumps(dialogue_tail, ensure_ascii=False)}"
        )
        try:
            judge_resp = await _llm_call_with_deadline(
                create_fn,
                timeout_seconds=timeout_seconds,
                model=model,
                messages=[
                    {"role": "system", "content": judge_prompt},
                    {"role": "user", "content": judge_user},
                ],
                temperature=0.0,
                max_tokens=120,
                response_format={"type": "json_object"},
                timeout=timeout_seconds,
            )
            judge_choices = getattr(judge_resp, "choices", None)
            judge_payload: dict[str, Any] = {}
            if isinstance(judge_choices, list) and judge_choices:
                judge_msg = getattr(judge_choices[0], "message", None)
                judge_payload = _safe_json_load(str(getattr(judge_msg, "content", "") or "")) or {}
            judge_ok = bool(judge_payload.get("ok"))
            rewrite_needed = bool(judge_payload.get("rewrite_needed"))
            issues_raw = judge_payload.get("issues")
            if isinstance(issues_raw, list):
                issues = [str(item).strip() for item in issues_raw if str(item).strip()]
            else:
                issues = []
            needs_payload = judge_payload.get("needs")
            needs_map = needs_payload if isinstance(needs_payload, Mapping) else {}
            needs_price = bool(needs_map.get("price")) or bool(intent_flags.get("price_intent"))
            needs_variants = bool(needs_map.get("variants")) or bool(intent_flags.get("variants_intent"))
            needs_recovery = bool(needs_map.get("catalog_recovery")) or bool(intent_flags.get("repair_intent"))
            needs_attributes = bool(needs_map.get("attributes")) or bool(intent_flags.get("attribute_intent"))
            claim_coverage_issues = _catalog_claim_coverage_issues(
                candidate,
                policy=policy,
                grounding_items=grounding_items,
            )
            if claim_coverage_issues:
                rewrite_needed = True
                judge_ok = False
                for issue in claim_coverage_issues:
                    if issue not in issues:
                        issues.append(issue)
            if _reply_mentions_catalog_item(candidate, grounding_items):
                claim_guard_prompt = (
                    "Ты строгий факт-чекер. Проверяй ответ менеджера только на предмет factual-утверждений по каталогу. "
                    "Допускаются только факты, которые непосредственно следуют из validated_catalog_claims. "
                    "Если в ответе есть хотя бы один факт о модели/цене/характеристиках вне validated_catalog_claims, "
                    "верни ok=false. Верни только JSON: "
                    '{"ok":true|false,"unsupported_facts":["..."]}.'
                )
                claim_guard_user = (
                    f"Validated catalog claims: {json.dumps(validated_catalog_claims, ensure_ascii=False)}\n\n"
                    f"Grounded catalog context:\n{grounding_preview or 'нет'}\n\n"
                    f"Последняя реплика клиента: {last_user_message}\n"
                    f"Ответ менеджера: {candidate}"
                )
                try:
                    claim_guard_resp = await _llm_call_with_deadline(
                        create_fn,
                        timeout_seconds=timeout_seconds,
                        model=model,
                        messages=[
                            {"role": "system", "content": claim_guard_prompt},
                            {"role": "user", "content": claim_guard_user},
                        ],
                        temperature=0.0,
                        max_tokens=160,
                        response_format={"type": "json_object"},
                        timeout=timeout_seconds,
                    )
                    claim_guard_choices = getattr(claim_guard_resp, "choices", None)
                    claim_guard_payload: dict[str, Any] = {}
                    if isinstance(claim_guard_choices, list) and claim_guard_choices:
                        claim_guard_msg = getattr(claim_guard_choices[0], "message", None)
                        claim_guard_payload = _safe_json_load(
                            str(getattr(claim_guard_msg, "content", "") or "")
                        ) or {}
                    if not bool(claim_guard_payload.get("ok", True)):
                        rewrite_needed = True
                        judge_ok = False
                        unsupported_raw = claim_guard_payload.get("unsupported_facts")
                        if isinstance(unsupported_raw, list):
                            for item in unsupported_raw:
                                marker = str(item or "").strip()
                                if marker and marker not in issues:
                                    issues.append(marker)
                        if "factual_not_entailed_by_validated_claims" not in issues:
                            issues.append("factual_not_entailed_by_validated_claims")
                except Exception:
                    pass

            candidate_low = _normalize_text(candidate)
            has_price_specific = bool(_PRICE_INLINE_RE.search(candidate)) or bool(
                re.search(r"(?iu)\b(цен|диапаз|дешев|альтернатив)\w*\b", candidate_low)
            )
            has_catalog_specific = has_price_specific or _reply_mentions_catalog_item(
                candidate, grounding_items
            )
            if needs_variants and not has_catalog_specific:
                rewrite_needed = True
                judge_ok = False
                issues.append("variants_without_catalog_specifics")
            if needs_price and not has_price_specific:
                rewrite_needed = True
                judge_ok = False
                issues.append("price_intent_without_price_specifics")
            if needs_recovery:
                has_repair_recovery = bool(
                    re.search(r"(?iu)\b(модел|вариант|каталог|цен|характерист)\w*\b", candidate_low)
                ) or has_catalog_specific
                if not has_repair_recovery:
                    rewrite_needed = True
                    judge_ok = False
                    issues.append("repair_intent_without_catalog_recovery")
            if needs_attributes:
                probe = attr_probe
                probe_norm = _normalize_text(probe)
                attribute_terms: set[str] = set()
                for item in list(grounding_items or [])[:4]:
                    for key, value in dict(item).items():
                        key_norm = _normalize_text(key)
                        if not key_norm or key_norm.startswith("_"):
                            continue
                        if key_norm in {"title", "name", "sku", "id", "url", "price"}:
                            continue
                        for token in _FACT_TOKEN_RE.findall(key_norm):
                            if len(token) >= 4 and token not in _GENERIC_FACT_STOPWORDS:
                                attribute_terms.add(token)
                        val_norm = _normalize_text(value)
                        for token in _FACT_TOKEN_RE.findall(val_norm):
                            if len(token) >= 4 and token not in _GENERIC_FACT_STOPWORDS:
                                attribute_terms.add(token)
                has_attribute_terms = any(token in candidate_low for token in list(attribute_terms)[:30])
                has_attribute_specific = (
                    bool(re.search(r"\d", candidate))
                    or (bool(probe_norm) and probe_norm in candidate_low)
                    or has_attribute_terms
                )
                if not has_attribute_specific:
                    rewrite_needed = True
                    judge_ok = False
                    issues.append("attribute_intent_without_specifics")
            if judge_ok or not rewrite_needed:
                return candidate

            rewrite_system = (
                "Перепиши ответ менеджера строго в рамках смысла текущего ответа. "
                "Не добавляй новые факты, цены или обещания. "
                "Не используй стартовые шаблоны вроде 'Понял', 'Поняла', 'Спасибо, что уточнили'. "
                "Если факт не подтверждён grounded catalog context, не утверждай его как факт. "
                "Любые factual-утверждения о каталоге можно брать только из validated_catalog_claims. "
                "Нельзя добавлять свойства/характеристики модели, которых нет в validated_catalog_claims. "
                "Если клиент просит варианты/цену/характеристики, добавь конкретику из grounded catalog context. "
                "Если intent_flags.price_intent=true, обязательно закрой вопрос цены (цена/диапазон/дешевле/альтернатива). "
                "Если intent_flags.variants_intent=true, покажи конкретные варианты/модели или явно предложи показать варианты сейчас. "
                "Если intent_flags.repair_intent=true, верни диалог к подбору из каталога с конкретикой. "
                "Если needs.attributes=true, обязательно дай конкретные параметры из grounded context "
                "(например толщина/материал/наполнение или другой подтверждённый атрибут). "
                "Ответь на последнюю реплику клиента в живом тоне. "
                "1-2 коротких предложения, максимум 1 вопрос."
            )
            rewrite_user = (
                f"Персона:\n{persona_context or 'нет'}\n\n"
                f"Grounded catalog context:\n{grounding_preview or 'нет'}\n"
                f"Selected item: {selected_label or 'нет'}\n\n"
                f"Validated catalog claims: {json.dumps(validated_catalog_claims, ensure_ascii=False)}\n\n"
                f"Intent flags: {json.dumps(intent_flags, ensure_ascii=False)}\n\n"
                f"Needs: {json.dumps(needs_map, ensure_ascii=False)}\n\n"
                f"Последняя реплика клиента: {last_user_message}\n"
                f"Проблемы текущего ответа: {json.dumps(issues, ensure_ascii=False)}\n"
                f"Текущий ответ: {candidate}\n"
                "Сделай улучшенный ответ."
            )
            rewrite_resp = await _llm_call_with_deadline(
                create_fn,
                timeout_seconds=timeout_seconds,
                model=model,
                messages=[
                    {"role": "system", "content": rewrite_system},
                    {"role": "user", "content": rewrite_user},
                ],
                temperature=0.2,
                max_tokens=180,
                timeout=timeout_seconds,
            )
            rewrite_choices = getattr(rewrite_resp, "choices", None)
            if isinstance(rewrite_choices, list) and rewrite_choices:
                rewrite_msg = getattr(rewrite_choices[0], "message", None)
                improved = str(getattr(rewrite_msg, "content", "") or "").strip()
                if improved:
                    improved = _strip_instruction_leaks(improved)
                    improved = quality_dedupe(improved)
                    improved = re.sub(r"[ \t]+", " ", improved)
                    improved = re.sub(r"\n{3,}", "\n\n", improved).strip()
                    improved = _enforce_sentence_budget(improved, max_sentences=2)
                    improved_claim_issues = _catalog_claim_coverage_issues(
                        improved,
                        policy=policy,
                        grounding_items=grounding_items,
                    )
                    if improved_claim_issues:
                        return candidate
                    if improved and not _rewrite_loses_context_anchors(candidate, improved, dialogue_tail):
                        if not (isinstance(state, SalesState) and _reply_has_repeated_question(improved, state)):
                            return improved
        except Exception:
            return candidate
        return candidate

    def catalog_claim_coverage_issues(
        self,
        answer: str,
        *,
        policy: Mapping[str, Any] | None,
        grounding_items: Sequence[Mapping[str, Any]],
    ) -> list[str]:
        _mentioned_catalog_item_ids = self.deps.mentioned_catalog_item_ids
        _strict_catalog_item_match = self.deps.strict_catalog_item_match
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _catalog_item_identity = self.deps.catalog_item_identity
        _extract_price_spans = self.deps.extract_price_spans
        _SENTENCE_SPLIT_RE = self.deps.sentence_split_re
        _normalize_model_alias = self.deps.normalize_model_alias
        _item_aliases = self.deps.item_aliases
        _FACT_TOKEN_RE = self.deps.fact_token_re
        _GENERIC_FACT_STOPWORDS = self.deps.generic_fact_stopwords

        candidate = str(answer or "").strip()
        if not candidate:
            return []
        items = [dict(item) for item in list(grounding_items or [])]
        if not items:
            return []

        claim_by_item: dict[str, dict[str, Any]] = {}
        for raw in list((policy or {}).get("claims") or []):
            if not isinstance(raw, Mapping):
                continue
            claim_type = str(raw.get("type") or "").strip().lower()
            if not claim_type.startswith("catalog_"):
                continue
            target_item: Mapping[str, Any] | None = None
            item_ref = str(raw.get("item_id") or "").strip()
            if item_ref:
                for item in items:
                    if _catalog_item_identity(item) == item_ref:
                        target_item = item
                        break
            if target_item is None:
                subject = str(raw.get("subject") or "").strip()
                strict = _strict_catalog_item_match(subject, items) if subject else None
                if isinstance(strict, Mapping):
                    target_item = dict(strict)
                elif subject:
                    best = _best_catalog_item_match(subject, items)
                    if isinstance(best, Mapping):
                        target_item = dict(best)
            if not isinstance(target_item, Mapping):
                continue
            item_id = _catalog_item_identity(dict(target_item))
            if not item_id:
                continue
            slot = claim_by_item.setdefault(
                item_id,
                {
                    "price_values": set(),
                    "has_attribute": False,
                },
            )
            if claim_type == "catalog_price":
                for _, _, value in _extract_price_spans(str(raw.get("value") or "")):
                    slot["price_values"].add(int(value))
            elif claim_type == "catalog_attribute":
                slot["has_attribute"] = True

        mentioned_ids = _mentioned_catalog_item_ids(candidate, items)
        if mentioned_ids and not claim_by_item:
            return ["catalog_item_mentioned_without_validated_claims"]
        if mentioned_ids - set(claim_by_item.keys()):
            return ["catalog_item_mentioned_without_item_claim"]

        issues: list[str] = []
        all_claim_prices: set[int] = set()
        for payload in claim_by_item.values():
            all_claim_prices.update(set(payload.get("price_values") or set()))

        sentence_parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(candidate) if part.strip()]
        for sentence in sentence_parts:
            sentence_ids = _mentioned_catalog_item_ids(sentence, items)
            if not sentence_ids:
                continue
            price_spans = _extract_price_spans(sentence)
            for _, _, value in price_spans:
                if all_claim_prices and int(value) not in all_claim_prices:
                    issues.append("catalog_price_not_in_validated_claims")
                    break
            if issues:
                continue
            all_have_attribute_claim = all(
                bool((claim_by_item.get(item_id) or {}).get("has_attribute"))
                for item_id in sentence_ids
            )
            if all_have_attribute_claim:
                continue
            normalized_sentence = _normalize_model_alias(sentence)
            for item_id in sentence_ids:
                item = next((dict(it) for it in items if _catalog_item_identity(dict(it)) == item_id), None)
                if not item:
                    continue
                for alias in _item_aliases(item):
                    alias_norm = _normalize_model_alias(alias)
                    if len(alias_norm) < 4:
                        continue
                    normalized_sentence = normalized_sentence.replace(alias_norm, " ")
            normalized_sentence = re.sub(r"\b\d+\b", " ", normalized_sentence)
            residual_tokens = [
                token
                for token in _FACT_TOKEN_RE.findall(normalized_sentence)
                if len(token) >= 4 and token not in _GENERIC_FACT_STOPWORDS
            ]
            if len(residual_tokens) >= 4:
                issues.append("attribute_like_details_without_validated_claim")
                break

        return issues
