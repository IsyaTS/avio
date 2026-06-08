from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Sequence


@dataclass(frozen=True)
class GroundingRuntimeDeps:
    catalog_item_identity: Callable[[Dict[str, Any]], str]
    low_signal_user_reply_re: Any
    low_signal_context_re: Any
    fact_token_re: Any
    noise_need_re: Any
    infer_user_needs: Callable[[str], Dict[str, Any]]
    search_catalog: Callable[..., List[Dict[str, Any]]]
    read_catalog: Callable[[int], List[Dict[str, Any]]]
    normalize_text: Callable[[Any], str]
    item_aliases: Callable[[Mapping[str, Any]], List[str]]
    normalize_model_alias: Callable[[str], str]
    merge_catalog_results: Callable[[List[Dict[str, Any]], List[Dict[str, Any]], int], List[Dict[str, Any]]]

    item_label: Callable[[Mapping[str, Any]], str]
    global_color_aliases: Mapping[str, Sequence[str]]
    normalize_color_token: Callable[[str], str]
    best_catalog_item_match: Callable[[str, Sequence[Mapping[str, Any]]], Mapping[str, Any] | None]

    enforce_catalog_model_grounding: Callable[..., str]
    grounding_catalog_items: Callable[[Mapping[str, Any] | None], List[Dict[str, Any]]]
    sentence_split_re: Any
    selected_item_from_grounding: Callable[..., Mapping[str, Any] | None]
    item_price_int: Callable[[Dict[str, Any]], int | None]
    extract_price_spans: Callable[[str], List[tuple[int, int, int]]]
    is_likely_price_value: Callable[[int], bool]
    mentioned_catalog_items_in_order: Callable[[str, Sequence[Mapping[str, Any]]], List[Mapping[str, Any]]]
    format_rub_price: Callable[[int], str]

    extract_attribute_probe: Callable[[str], str]
    tokenize_query: Callable[[str], List[str]]
    generic_fact_stopwords: set[str]
    normalize_probe_token: Callable[[str], str]
    iter_item_attribute_pairs: Callable[..., List[tuple[str, str, str, str]]]
    normalize_text_fn: Callable[[Any], str]
    format_attribute_pairs: Callable[[Sequence[tuple[str, str]], int], str]
    is_dimension_like_value: Callable[[str], bool]


class GroundingRuntime:
    def __init__(self, deps: GroundingRuntimeDeps) -> None:
        self.deps = deps

    def collect_grounding_items(
        self,
        tenant: int | None,
        state: Any,
        user_text: str,
    ) -> List[Dict[str, Any]]:
        _catalog_item_identity = self.deps.catalog_item_identity
        _LOW_SIGNAL_USER_REPLY_RE = self.deps.low_signal_user_reply_re
        _LOW_SIGNAL_CONTEXT_RE = self.deps.low_signal_context_re
        _FACT_TOKEN_RE = self.deps.fact_token_re
        _NOISE_NEED_RE = self.deps.noise_need_re
        infer_user_needs = self.deps.infer_user_needs
        search_catalog = self.deps.search_catalog
        _read_catalog = self.deps.read_catalog
        _normalize_text = self.deps.normalize_text
        _item_aliases = self.deps.item_aliases
        _normalize_model_alias = self.deps.normalize_model_alias
        _merge_catalog_results = self.deps.merge_catalog_results

        merged: List[Dict[str, Any]] = []
        seen: set[str] = set()

        def _append(items: Sequence[Mapping[str, Any]]) -> None:
            for item in items:
                identity = _catalog_item_identity(dict(item))
                if identity in seen:
                    continue
                merged.append(dict(item))
                seen.add(identity)

        if getattr(state, "last_items", None):
            _append(state.last_items)

        effective_query = str(user_text or "").strip()

        def _is_low_signal_reply(text: str) -> bool:
            value = str(text or "").strip()
            if not value:
                return True
            if _LOW_SIGNAL_USER_REPLY_RE.match(value):
                return True
            if _LOW_SIGNAL_CONTEXT_RE.search(value):
                return True
            tokens = [tok for tok in _FACT_TOKEN_RE.findall(value.lower().replace("ё", "е")) if tok]
            if len(tokens) <= 1 and len(value) <= 16:
                return True
            return False

        if _is_low_signal_reply(effective_query):
            for entry in reversed(getattr(state, "history", []) or []):
                if str(entry.get("role") or "").strip().lower() != "user":
                    continue
                content = str(entry.get("content") or "").strip()
                if not content:
                    continue
                if _NOISE_NEED_RE.search(content):
                    effective_query = content
                    break
            if effective_query == str(user_text or "").strip():
                for entry in reversed(getattr(state, "history", []) or []):
                    if str(entry.get("role") or "").strip().lower() != "user":
                        continue
                    content = str(entry.get("content") or "").strip()
                    if not content or content == effective_query:
                        continue
                    if _is_low_signal_reply(content):
                        continue
                    if len(_FACT_TOKEN_RE.findall(content.lower().replace("ё", "е"))) < 2:
                        continue
                    effective_query = content
                    break

        if tenant is not None:
            try:
                needs: Dict[str, Any] = dict(getattr(state, "needs", {}) or {})
                state_facts = dict(getattr(state, "facts", {}) or {})
                for key in ("object_type", "city", "address", "model"):
                    fact_val = str(state_facts.get(key) or "").strip()
                    if fact_val and key not in needs:
                        needs[key] = fact_val
                query_needs = infer_user_needs(effective_query or user_text)
                for key, value in query_needs.items():
                    if value in (None, "", [], {}, ()):
                        continue
                    if key == "keywords":
                        merged_tokens: List[str] = [
                            str(x) for x in (needs.get("keywords") or []) if str(x).strip()
                        ]
                        for token in value if isinstance(value, list) else [value]:
                            token_str = str(token).strip()
                            if token_str and token_str not in merged_tokens:
                                merged_tokens.append(token_str)
                        if merged_tokens:
                            needs["keywords"] = merged_tokens[:8]
                        continue
                    needs[key] = value
                extra = search_catalog(needs, limit=8, tenant=tenant, query=effective_query or user_text)
                _append(extra)
            except Exception:
                pass
            if not merged:
                try:
                    _append(_read_catalog(int(tenant))[:8])
                except Exception:
                    pass

            try:
                catalog_items = _read_catalog(int(tenant))
            except Exception:
                catalog_items = []
            if catalog_items and getattr(state, "history", None):
                hay = _normalize_text(
                    " ".join(str(entry.get("content") or "") for entry in (state.history or [])[-8:])
                )
                if hay:
                    hinted: List[Dict[str, Any]] = []
                    hinted_seen: set[str] = set()
                    for item in catalog_items:
                        aliases = _item_aliases(item)
                        if not aliases:
                            continue
                        matched = False
                        for alias in aliases:
                            alias_norm = _normalize_model_alias(alias)
                            if len(alias_norm) < 4:
                                continue
                            if alias_norm in hay:
                                matched = True
                                break
                        if not matched:
                            continue
                        identity = _catalog_item_identity(dict(item))
                        if identity in hinted_seen:
                            continue
                        hinted_seen.add(identity)
                        hinted.append(dict(item))
                        if len(hinted) >= 6:
                            break
                    if hinted:
                        merged = _merge_catalog_results(hinted[:], merged, 12)
                        return merged[:12]
        return merged[:12]

    def model_root_tokens(self, item: Mapping[str, Any]) -> set[str]:
        _item_label = self.deps.item_label
        _GLOBAL_COLOR_ALIASES = self.deps.global_color_aliases
        _normalize_color_token = self.deps.normalize_color_token
        _FACT_TOKEN_RE = self.deps.fact_token_re

        label = (_item_label(item) or "").lower().replace("ё", "е")
        if not label:
            return set()
        color_tokens: set[str] = set()
        for key, aliases in _GLOBAL_COLOR_ALIASES.items():
            color_tokens.add(_normalize_color_token(key))
            color_tokens.update(_normalize_color_token(alias) for alias in aliases)
        tokens = []
        for token in _FACT_TOKEN_RE.findall(label):
            if len(token) < 2:
                continue
            if token in color_tokens:
                continue
            tokens.append(token)
        return set(tokens)

    def has_single_color_variant(
        self,
        selected_item: Mapping[str, Any],
        catalog_items: Sequence[Mapping[str, Any]],
    ) -> bool:
        _normalize_color_token = self.deps.normalize_color_token

        root = self.model_root_tokens(selected_item)
        if not root:
            return False
        color_values: set[str] = set()
        for item in catalog_items:
            tokens = self.model_root_tokens(item)
            if not tokens:
                continue
            overlap = len(root & tokens)
            if overlap == 0:
                continue
            if overlap < max(1, min(len(root), len(tokens)) // 2):
                continue
            color = _normalize_color_token(str(item.get("color") or ""))
            if color:
                color_values.add(color)
        if not color_values:
            return False
        return len(color_values) <= 1

    def build_reply_grounding(
        self,
        *,
        tenant: int | None,
        state: Any,
        user_text: str,
    ) -> Dict[str, Any]:
        _read_catalog = self.deps.read_catalog
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _item_aliases = self.deps.item_aliases
        infer_user_needs = self.deps.infer_user_needs

        items = self.collect_grounding_items(tenant, state, user_text)
        full_catalog: List[Dict[str, Any]] = []
        if tenant is not None:
            try:
                full_catalog = [dict(item) for item in _read_catalog(int(tenant))]
            except Exception:
                full_catalog = []
        selected_query = (
            (getattr(state, "known_slots", {}) or {}).get("model")
            or str((getattr(state, "facts", {}) or {}).get("model") or "").strip()
            or user_text
        )
        selected_item = _best_catalog_item_match(selected_query, full_catalog or items)
        forbidden_topics: set[str] = set()

        if selected_item is not None and full_catalog and self.has_single_color_variant(selected_item, full_catalog):
            forbidden_topics.add("color")

        model_aliases: set[str] = set()
        source_for_aliases = full_catalog or items
        for item in source_for_aliases:
            for alias in _item_aliases(item):
                normalized = re.sub(r"[^0-9a-zа-яё]+", " ", str(alias).lower().replace("ё", "е")).strip()
                if normalized and len(normalized) >= 3:
                    model_aliases.add(normalized)

        needs_payload: Dict[str, Any] = dict(getattr(state, "needs", {}) or {})
        if not needs_payload:
            inferred_current = infer_user_needs(user_text or "")
            if isinstance(inferred_current, dict):
                needs_payload.update(
                    {k: v for k, v in inferred_current.items() if v not in (None, "", [], {}, ())}
                )
        state_facts = dict(getattr(state, "facts", {}) or {})
        for key in ("object_type", "city", "address", "model"):
            fact_val = str(state_facts.get(key) or "").strip()
            if fact_val and key not in needs_payload:
                needs_payload[key] = fact_val
        if "object_type" not in needs_payload:
            for entry in reversed(getattr(state, "history", []) or []):
                if str(entry.get("role") or "").strip().lower() != "user":
                    continue
                content = str(entry.get("content") or "").strip()
                if not content:
                    continue
                probe = infer_user_needs(content)
                obj = str((probe or {}).get("object_type") or "").strip()
                if obj:
                    needs_payload["object_type"] = obj
                    break

        return {
            "items": items,
            "catalog_items": full_catalog[:500],
            "needs": needs_payload,
            "selected_item": dict(selected_item) if isinstance(selected_item, Mapping) else None,
            "forbid_question_topics": sorted(forbidden_topics),
            "model_aliases": sorted(model_aliases),
        }

    def enforce_catalog_price_grounding(
        self,
        text: str,
        *,
        grounding: Mapping[str, Any] | None = None,
    ) -> str:
        _enforce_catalog_model_grounding = self.deps.enforce_catalog_model_grounding
        _grounding_catalog_items = self.deps.grounding_catalog_items
        _SENTENCE_SPLIT_RE = self.deps.sentence_split_re
        _selected_item_from_grounding = self.deps.selected_item_from_grounding
        _item_price_int = self.deps.item_price_int
        _extract_price_spans = self.deps.extract_price_spans
        _is_likely_price_value = self.deps.is_likely_price_value
        _mentioned_catalog_items_in_order = self.deps.mentioned_catalog_items_in_order
        _best_catalog_item_match = self.deps.best_catalog_item_match
        _format_rub_price = self.deps.format_rub_price

        base = (text or "").strip()
        if not base:
            return base
        base = _enforce_catalog_model_grounding(base, grounding=grounding)
        items = _grounding_catalog_items(grounding)
        if not items:
            return base
        sentences = [part.strip() for part in _SENTENCE_SPLIT_RE.split(base) if part.strip()] or [base]
        out: list[str] = []
        selected_item = _selected_item_from_grounding(grounding, items)
        selected_price = _item_price_int(dict(selected_item)) if isinstance(selected_item, Mapping) else None
        catalog_prices = {
            int(price)
            for price in (_item_price_int(dict(item)) for item in items)
            if isinstance(price, int) and price > 0
        }
        for sentence in sentences:
            price_spans = [span for span in _extract_price_spans(sentence) if _is_likely_price_value(span[2])]
            if not price_spans:
                out.append(sentence)
                continue
            discount_spans: list[tuple[int, int, int]] = []
            for span in price_spans:
                window = sentence[max(0, span[0] - 32) : min(len(sentence), span[1] + 16)]
                if re.search(r"(?iu)\b(скидк|акци|промокод|купон)\w*\b", window):
                    discount_spans.append(span)
            price_spans = [span for span in price_spans if span not in discount_spans]
            if not price_spans:
                out.append(sentence)
                continue
            mentioned_items = _mentioned_catalog_items_in_order(sentence, items)
            if not mentioned_items:
                item = _best_catalog_item_match(sentence, items)
                if item is not None:
                    mentioned_items = [item]
            if (not mentioned_items) and isinstance(selected_price, int) and selected_price > 0:
                mentioned_items = [dict(selected_item or {})]
            expected_prices: list[int] = []
            for item in mentioned_items:
                price = _item_price_int(item)
                if price:
                    expected_prices.append(price)

            if not expected_prices:
                if catalog_prices and all(int(span[2]) in catalog_prices for span in price_spans):
                    out.append(sentence)
                    continue
                patched = sentence
                for start, end, value in sorted(price_spans, key=lambda item: item[0], reverse=True):
                    if catalog_prices and int(value) in catalog_prices:
                        continue
                    patched = patched[:start] + "цена по каталогу" + patched[end:]
                out.append(patched)
                continue

            replacements: list[tuple[int, int, str]] = []
            for idx, span in enumerate(price_spans):
                expected = expected_prices[min(idx, len(expected_prices) - 1)]
                if span[2] == int(expected):
                    continue
                replacements.append((span[0], span[1], _format_rub_price(int(expected))))
            if not replacements:
                out.append(sentence)
                continue
            patched = sentence
            for start, end, value in sorted(replacements, key=lambda item: item[0], reverse=True):
                patched = patched[:start] + value + patched[end:]
            out.append(patched)
        rebuilt = " ".join(out).strip()
        return rebuilt or base

    def selected_item_attribute_answer(
        self,
        user_text: str,
        selected_item: Mapping[str, Any],
    ) -> str:
        _extract_attribute_probe = self.deps.extract_attribute_probe
        _tokenize_query = self.deps.tokenize_query
        _GENERIC_FACT_STOPWORDS = self.deps.generic_fact_stopwords
        _normalize_probe_token = self.deps.normalize_probe_token
        _iter_item_attribute_pairs = self.deps.iter_item_attribute_pairs
        _normalize_text = self.deps.normalize_text_fn
        _format_attribute_pairs = self.deps.format_attribute_pairs
        _FACT_TOKEN_RE = self.deps.fact_token_re
        _is_dimension_like_value = self.deps.is_dimension_like_value

        raw_user = str(user_text or "").strip()
        if not raw_user:
            return ""
        question_like = bool(
            ("?" in raw_user)
            or re.match(r"(?iu)^\s*(какой|какая|какие|какое|сколько|чем|как|почему|зачем)\b", raw_user)
        )
        probe = _extract_attribute_probe(raw_user)
        user_tokens = [tok for tok in _tokenize_query(raw_user) if tok]
        if not probe and not question_like:
            return ""
        source_tokens = _tokenize_query(probe) if probe else user_tokens
        probe_tokens = [
            _normalize_probe_token(tok)
            for tok in source_tokens
            if tok and len(tok) >= 3 and tok not in _GENERIC_FACT_STOPWORDS
        ]
        probe_tokens = [tok for tok in probe_tokens if tok and tok not in _GENERIC_FACT_STOPWORDS]
        if not probe_tokens:
            return ""

        blocked = {
            "id",
            "sku",
            "article",
            "title",
            "name",
            "price",
            "cost",
            "url",
            "image",
            "photo",
            "link",
            "stock",
            "color",
            "tags",
        }
        item_map = dict(selected_item)
        entries = _iter_item_attribute_pairs(item_map, blocked=blocked)
        candidates: list[tuple[float, str, str]] = []
        for key, val, _, _ in entries:
            hay_tokens = {
                _normalize_probe_token(tok)
                for tok in _tokenize_query(f"{key} {val}")
                if tok and len(tok) >= 3
            }
            if not hay_tokens:
                continue
            score = 0.0
            for tok in probe_tokens:
                probe_tok = _normalize_probe_token(tok)
                if not probe_tok:
                    continue
                if probe_tok in hay_tokens:
                    score += 2.0
                    continue
                if any(probe_tok in h or h in probe_tok for h in hay_tokens):
                    score += 0.8
            if score > 0:
                candidates.append((score, key, val))
        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            selected_pairs: list[tuple[str, str]] = []
            for _, key, value in candidates:
                clean = str(value or "").strip()
                if not clean:
                    continue
                pair = (str(key or "").strip(), clean)
                if pair in selected_pairs:
                    continue
                selected_pairs.append(pair)
                if len(selected_pairs) >= 2:
                    break
            if len(selected_pairs) == 1 and "?" in raw_user:
                selected_values_norm = {_normalize_text(val) for _, val in selected_pairs}
                for key, val, key_norm, _ in entries:
                    if key_norm in blocked:
                        continue
                    if _normalize_text(val) in selected_values_norm:
                        continue
                    selected_pairs.append((key, val))
                    break
            rendered = _format_attribute_pairs(selected_pairs, max_pairs=2)
            if rendered:
                return rendered

        if question_like and entries:
            fallback_values: list[tuple[float, str, str, bool]] = []
            for key, val, _, val_norm in entries:
                score = float(min(len(val_norm), 20))
                has_digits = bool(re.search(r"\d", val))
                if has_digits:
                    score += 8.0
                if re.search(r"\b(?:мм|cm|kg|кг|см|шт|pcs|mm)\b", val, re.IGNORECASE):
                    score += 3.0
                if _is_dimension_like_value(val):
                    score -= 12.0
                token_count = len(_FACT_TOKEN_RE.findall(val_norm))
                if token_count >= 10:
                    score -= 8.0
                if "," in val:
                    score -= 10.0
                if len(val_norm) > 56:
                    score -= 10.0
                fallback_values.append((score, key, val, has_digits))
            if fallback_values:
                fallback_values.sort(key=lambda x: x[0], reverse=True)
                picked_pairs: list[tuple[str, str]] = []
                top_numeric: tuple[str, str] | None = None
                top_text: tuple[str, str] | None = None
                for _, key, val, has_digits in fallback_values:
                    pair = (str(key or "").strip(), str(val or "").strip())
                    if not pair[0] or not pair[1]:
                        continue
                    if has_digits and top_numeric is None:
                        top_numeric = pair
                    if (not has_digits) and top_text is None:
                        top_text = pair
                    if top_numeric is not None and top_text is not None:
                        break
                if top_numeric is not None:
                    picked_pairs.append(top_numeric)
                if top_text is not None and top_text not in picked_pairs:
                    picked_pairs.append(top_text)
                rendered = _format_attribute_pairs(picked_pairs, max_pairs=2)
                if rendered:
                    return rendered
        return ""
