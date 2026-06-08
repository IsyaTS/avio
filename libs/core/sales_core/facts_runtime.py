from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class FactsRuntimeDeps:
    canonical_fact_key: Callable[[str], str]
    normalize_fact_key: Callable[[str], str]
    normalize_text: Callable[[Any], str]
    safe_short_text: Callable[[str, int], str]
    classify_turn_intent: Callable[..., str]
    state_facts_snapshot: Callable[[Any], dict[str, str]]
    extract_city_hint: Callable[..., str]
    extract_standalone_city_hint: Callable[[str], str]
    looks_like_address_value: Callable[[str], bool]
    extract_budget: Callable[[str], int | None]
    extract_price_spans: Callable[[str], list[tuple[int, int, int]]]
    read_catalog: Callable[[int | None], list[dict[str, Any]]]
    best_catalog_item_match: Callable[[str, list[dict[str, Any]]], Mapping[str, Any] | None]
    item_label: Callable[[Mapping[str, Any]], str]
    object_type_from_turn_text: Callable[[str], str]
    canonical_object_type_hint: Callable[[Any], str]
    infer_user_needs: Callable[[str], dict[str, Any]]
    is_plausible_city_text: Callable[[str], bool]
    normalize_slot_name: Callable[[str], str]

    fact_token_re: Any
    low_signal_context_re: Any
    catalog_unavailable_re: Any
    object_type_hint_re: Any
    generic_model_words: set[str]
    needs_stopwords: set[str]


class FactsRuntime:
    def __init__(self, deps: FactsRuntimeDeps) -> None:
        self.deps = deps

    def capture_pending_fact_answer(self, state: Any, user_text: str) -> None:
        _canonical_fact_key = self.deps.canonical_fact_key
        _safe_short_text = self.deps.safe_short_text
        _classify_turn_intent = self.deps.classify_turn_intent
        _state_facts_snapshot = self.deps.state_facts_snapshot
        _extract_city_hint = self.deps.extract_city_hint
        _normalize_text = self.deps.normalize_text
        _looks_like_address_value = self.deps.looks_like_address_value
        _extract_budget = self.deps.extract_budget
        _extract_price_spans = self.deps.extract_price_spans
        _read_catalog = self.deps.read_catalog
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _item_label = self.deps.item_label
        _object_type_from_turn_text = self.deps.object_type_from_turn_text
        _is_plausible_city_text = self.deps.is_plausible_city_text

        _FACT_TOKEN_RE = self.deps.fact_token_re
        _LOW_SIGNAL_CONTEXT_RE = self.deps.low_signal_context_re
        _CATALOG_UNAVAILABLE_RE = self.deps.catalog_unavailable_re
        _OBJECT_TYPE_HINT_RE = self.deps.object_type_hint_re

        key = _canonical_fact_key(getattr(state, "pending_fact_key", ""))
        if not key:
            return
        text = str(user_text or "").strip()
        if not text:
            return
        if not isinstance(getattr(state, "facts", None), dict):
            state.facts = {}
        turn_intent = _classify_turn_intent(text, known_facts=_state_facts_snapshot(state))
        if turn_intent in {
            "unsubscribe",
            "payment",
            "store_address",
            "handoff",
            "catalog_request",
            "offtopic",
            "why_question",
            "catalog_problem",
            "repair",
        }:
            return
        city_hint = _extract_city_hint(text, allow_standalone=(key == "city"))
        if city_hint:
            state.facts["city"] = _safe_short_text(city_hint, 180)
        tokens = [tok for tok in _FACT_TOKEN_RE.findall(text) if tok]
        if "?" in text and len(tokens) > 8:
            return
        low = _normalize_text(text)
        if key in {"city", "object_type", "model"} and "?" in text:
            return
        if key in {"city", "object_type", "model"} and len(tokens) > 6:
            return
        if key == "city" and len(tokens) > 3:
            return
        if key == "address" and not _looks_like_address_value(text):
            return
        if key in {"city", "object_type"} and _looks_like_address_value(text):
            return
        if key == "object_type" and not _OBJECT_TYPE_HINT_RE.search(low):
            return
        if key == "budget":
            if _extract_budget(low) is None and not _extract_price_spans(text):
                return
        if key == "dimensions":
            has_size_pattern = bool(
                re.search(
                    r"(?iu)\b\d{2,4}\s*[xх*]\s*\d{2,4}(?:\s*[xх*]\s*\d{2,4})?\b",
                    text,
                )
            )
            has_dimension_words = bool(
                re.search(r"(?iu)\b(размер|проем|проём|ширин|высот|фото)\w*\b", low)
            )
            if not (has_size_pattern or has_dimension_words):
                return
        if key == "model":
            if _LOW_SIGNAL_CONTEXT_RE.search(low) or _CATALOG_UNAVAILABLE_RE.search(low):
                return
            noisy = {"каталог", "грузится", "не", "могу", "пока", "позже"}
            if tokens and sum(1 for tok in tokens if _normalize_text(tok) in noisy) >= max(
                1, len(tokens) // 2
            ):
                return
            tenant = getattr(state, "tenant", None)
            if not isinstance(tenant, int):
                return
            try:
                catalog_items = _read_catalog(int(tenant))
            except Exception:
                return
            match = _best_catalog_item_match(text, catalog_items or [])
            if not match:
                return
            label = _item_label(match)
            if not label:
                return
            state.facts[key] = _safe_short_text(label, 180)
            if not isinstance(getattr(state, "known_slots", None), dict):
                state.known_slots = {}
            state.known_slots["model"] = _safe_short_text(label, 120)
            state.pending_fact_key = ""
            return
        if key == "object_type":
            normalized_obj = _object_type_from_turn_text(text)
            if not normalized_obj:
                return
            state.facts[key] = normalized_obj
        elif key == "city":
            if not _is_plausible_city_text(text):
                return
            state.facts[key] = _safe_short_text(text, 180)
        else:
            state.facts[key] = _safe_short_text(text, 180)
        state.pending_fact_key = ""

    def merge_fact_updates(
        self,
        state: Any,
        updates: Mapping[str, Any] | None,
        *,
        user_text: str = "",
    ) -> None:
        _canonical_fact_key = self.deps.canonical_fact_key
        _normalize_fact_key = self.deps.normalize_fact_key
        _safe_short_text = self.deps.safe_short_text
        _extract_city_hint = self.deps.extract_city_hint
        _extract_standalone_city_hint = self.deps.extract_standalone_city_hint
        _normalize_text = self.deps.normalize_text
        _canonical_object_type_hint = self.deps.canonical_object_type_hint
        infer_user_needs = self.deps.infer_user_needs
        _object_type_from_turn_text = self.deps.object_type_from_turn_text
        _is_plausible_city_text = self.deps.is_plausible_city_text
        _looks_like_address_value = self.deps.looks_like_address_value
        _extract_budget = self.deps.extract_budget
        _read_catalog = self.deps.read_catalog
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _item_label = self.deps.item_label
        _OBJECT_TYPE_HINT_RE = self.deps.object_type_hint_re
        _FACT_TOKEN_RE = self.deps.fact_token_re
        _GENERIC_MODEL_WORDS = self.deps.generic_model_words
        NEEDS_STOPWORDS = self.deps.needs_stopwords

        if not updates:
            return
        if not isinstance(getattr(state, "facts", None), dict):
            state.facts = {}
        user_raw = str(user_text or "").strip()
        user_city = _extract_city_hint(user_raw, allow_standalone=True) or _extract_standalone_city_hint(
            user_raw
        )
        user_city_norm = _normalize_text(user_city)
        user_obj = _canonical_object_type_hint(
            (infer_user_needs(user_raw) or {}).get("object_type") or _object_type_from_turn_text(user_raw)
        )

        def _fact_value_is_valid(key: str, value: str) -> bool:
            raw = str(value or "").strip()
            if not raw:
                return False
            if key == "city":
                return _is_plausible_city_text(raw)
            if key == "address":
                return _looks_like_address_value(raw)
            if key == "object_type":
                return bool(_OBJECT_TYPE_HINT_RE.search(_normalize_text(raw)))
            if key == "model":
                low = _normalize_text(raw)
                tokens = [tok for tok in _FACT_TOKEN_RE.findall(low) if tok]
                if not tokens:
                    return False
                if all(tok in _GENERIC_MODEL_WORDS or tok in NEEDS_STOPWORDS for tok in tokens):
                    return False
                tenant = getattr(state, "tenant", None)
                if not isinstance(tenant, int):
                    return False
                try:
                    catalog_items = _read_catalog(int(tenant))
                except Exception:
                    return False
                return _best_catalog_item_match(raw, catalog_items or []) is not None
            if key == "budget":
                return _extract_budget(_normalize_text(raw)) is not None
            return True

        def _fact_update_supported_by_user_turn(key: str, value: str) -> bool:
            if key == "city":
                if not user_city_norm:
                    return False
                value_norm = _normalize_text(value)
                if not value_norm:
                    return False
                return bool(
                    value_norm == user_city_norm
                    or value_norm in user_city_norm
                    or user_city_norm in value_norm
                )
            if key == "object_type":
                value_obj = _canonical_object_type_hint(value)
                return bool(value_obj and user_obj and value_obj == user_obj)
            if key == "address":
                return _looks_like_address_value(user_raw)
            if key == "model":
                tenant = getattr(state, "tenant", None)
                if not isinstance(tenant, int):
                    return False
                try:
                    catalog_items = _read_catalog(int(tenant))
                except Exception:
                    return False
                return _best_catalog_item_match(user_raw, catalog_items or []) is not None
            return True

        for raw_key, raw_value in dict(updates or {}).items():
            key = _canonical_fact_key(str(raw_key))
            if not key:
                continue
            value = _safe_short_text(str(raw_value or ""), 180)
            if not value:
                continue
            if not _fact_value_is_valid(key, value):
                continue
            if not _fact_update_supported_by_user_turn(key, value):
                continue
            if key == "object_type":
                normalized_obj = _object_type_from_turn_text(value)
                if not normalized_obj:
                    continue
                value = normalized_obj
            elif key == "model":
                tenant = getattr(state, "tenant", None)
                if not isinstance(tenant, int):
                    continue
                try:
                    catalog_items = _read_catalog(int(tenant))
                except Exception:
                    continue
                match = _best_catalog_item_match(user_raw or value, catalog_items or [])
                if not match:
                    continue
                label = _item_label(match)
                if not label:
                    continue
                value = _safe_short_text(label, 180)
            state.facts[key] = value

    def capture_pending_slot_answer(self, state: Any, user_text: str) -> None:
        _normalize_slot_name = self.deps.normalize_slot_name
        _classify_turn_intent = self.deps.classify_turn_intent
        _state_facts_snapshot = self.deps.state_facts_snapshot
        _normalize_text = self.deps.normalize_text
        _looks_like_address_value = self.deps.looks_like_address_value
        _is_plausible_city_text = self.deps.is_plausible_city_text
        _read_catalog = self.deps.read_catalog
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _item_label = self.deps.item_label
        _canonical_fact_key = self.deps.canonical_fact_key
        _normalize_fact_key = self.deps.normalize_fact_key
        _safe_short_text = self.deps.safe_short_text

        _FACT_TOKEN_RE = self.deps.fact_token_re
        _LOW_SIGNAL_CONTEXT_RE = self.deps.low_signal_context_re
        _CATALOG_UNAVAILABLE_RE = self.deps.catalog_unavailable_re
        _OBJECT_TYPE_HINT_RE = self.deps.object_type_hint_re

        slot = _normalize_slot_name(getattr(state, "pending_slot", ""))
        if not slot or slot == "other":
            state.pending_slot = ""
            return
        text = str(user_text or "").strip()
        if not text:
            return
        turn_intent = _classify_turn_intent(text, known_facts=_state_facts_snapshot(state))
        if turn_intent in {
            "unsubscribe",
            "payment",
            "store_address",
            "handoff",
            "catalog_request",
            "offtopic",
            "why_question",
            "catalog_problem",
            "repair",
        }:
            return
        token_count = len(_FACT_TOKEN_RE.findall(text))
        if "?" in text and token_count > 8:
            return
        if slot in {"object", "location", "model"} and "?" in text:
            return
        low = _normalize_text(text)
        if slot in {"object", "location"} and _looks_like_address_value(text):
            return
        if slot == "location" and not _is_plausible_city_text(text):
            return
        if slot == "object" and not _OBJECT_TYPE_HINT_RE.search(low):
            return
        if slot == "model":
            if _LOW_SIGNAL_CONTEXT_RE.search(low) or _CATALOG_UNAVAILABLE_RE.search(low):
                return
            generic_model_noise = {"не", "могу", "пока", "грузится", "каталог", "потом"}
            tokens = [tok for tok in _FACT_TOKEN_RE.findall(low) if tok]
            if tokens and sum(1 for tok in tokens if tok in generic_model_noise) >= max(
                1, len(tokens) // 2
            ):
                return
            tenant = getattr(state, "tenant", None)
            if not isinstance(tenant, int):
                return
            try:
                catalog_items = _read_catalog(int(tenant))
            except Exception:
                return
            match = _best_catalog_item_match(text, catalog_items or [])
            if not match:
                return
            label = _item_label(match)
            if not label:
                return
            if not isinstance(getattr(state, "known_slots", None), dict):
                state.known_slots = {}
            state.known_slots[slot] = _safe_short_text(label, limit=140)
            if not isinstance(getattr(state, "facts", None), dict):
                state.facts = {}
            state.facts["model"] = _safe_short_text(label, limit=180)
            state.pending_slot = ""
            return
        if not isinstance(getattr(state, "known_slots", None), dict):
            state.known_slots = {}
        state.known_slots[slot] = _safe_short_text(text, limit=140)
        if not isinstance(getattr(state, "facts", None), dict):
            state.facts = {}
        canonical = _canonical_fact_key(slot)
        if canonical:
            state.facts[canonical] = _safe_short_text(text, limit=180)
        state.facts[_normalize_fact_key(slot)] = _safe_short_text(text, limit=180)
        state.pending_slot = ""
