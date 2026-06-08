from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple


@dataclass(frozen=True)
class CatalogSearchDeps:
    normalize_text: Callable[[Any], str]
    collect_item_text: Callable[[Dict[str, Any]], str]
    item_price_int: Callable[[Dict[str, Any]], int | None]
    text_match_score: Callable[[Dict[str, Any], List[str]], float]
    tokenize_query: Callable[[str | None], List[str]]
    read_catalog: Callable[[int | None], List[Dict[str, Any]]]
    persona_meta_config: Callable[[int | None], Dict[str, Any]]
    augment_color_needs: Callable[[Dict[str, Any], Mapping[str, Any] | None], None]
    filter_catalog_items_by_rules: Callable[[List[Dict[str, Any]], Mapping[str, Any], Mapping[str, Any] | None], List[Dict[str, Any]]]
    extract_price_order_intent: Callable[[str], str | None]
    catalog_retriever: Any
    logger: Any
    noise_need_re: Any


class CatalogSearchRuntime:
    def __init__(self, deps: CatalogSearchDeps) -> None:
        self.deps = deps

    def _value_matches(self, item: Dict[str, Any], fields: Tuple[str, ...], needle: str) -> bool:
        for field_name in fields:
            val = item.get(field_name)
            if not val:
                continue
            if isinstance(val, (list, tuple, set)):
                texts = [str(v) for v in val if v]
            else:
                texts = [str(val)]
            for text in texts:
                if needle in self.deps.normalize_text(text):
                    return True
        return False

    def _score(self, item: Dict[str, Any], needs: Dict[str, Any]) -> float:
        score = 0.0
        haystack_text = self.deps.normalize_text(self.deps.collect_item_text(item))

        primary = needs.get("type")
        if primary:
            needle = self.deps.normalize_text(primary)
            if needle and (
                self._value_matches(item, ("type", "category", "segment", "group"), needle)
                or needle in haystack_text
            ):
                score += 3.0

        keywords = needs.get("keywords") or []
        if keywords:
            for kw in keywords[:3]:
                needle = self.deps.normalize_text(kw)
                if needle and needle in haystack_text:
                    score += 1.0

        size = needs.get("size") or needs.get("width")
        if size:
            size_str = self.deps.normalize_text(str(size))
            if self._value_matches(
                item,
                ("size", "width", "dimensions", "length", "height", "depth"),
                size_str,
            ):
                score += 1.5

        color_tokens = needs.get("_color_tokens") or []
        if color_tokens:
            item_color_aliases = set(str(alias) for alias in (item.get("_color_aliases") or []) if alias)
            for token in color_tokens:
                if self._value_matches(item, ("color", "finish", "shade", "title", "name", "tags"), token):
                    score += 0.8
                    break
                if item_color_aliases and token in item_color_aliases:
                    score += 0.8
                    break

        budget = needs.get("budget_max")
        if budget:
            try:
                price = int("".join(ch for ch in str(item.get("price") or "0") if ch.isdigit()))
                if price and price <= int(budget):
                    score += 1.5
            except Exception:
                pass
        return score

    def _legacy_rank_catalog(
        self,
        items: List[Dict[str, Any]],
        needs: Dict[str, Any],
        limit: int,
        query: str | None,
    ) -> List[Dict[str, Any]]:
        query_tokens = self.deps.tokenize_query(query)
        price_order = str(needs.get("price_order") or "").strip().lower()
        if price_order not in {"asc", "desc"}:
            price_order = ""
        price_values = [self.deps.item_price_int(dict(item)) for item in items]
        clean_prices = [int(v) for v in price_values if isinstance(v, int) and v > 0]
        price_floor = min(clean_prices) if clean_prices else None
        price_ceil = max(clean_prices) if clean_prices else None

        def _total_score(item: Dict[str, Any]) -> float:
            base = self._score(item, needs)
            matched = self.deps.text_match_score(item, query_tokens)
            price_bias = 0.0
            if price_order and price_floor is not None and price_ceil is not None and price_ceil > price_floor:
                item_price = self.deps.item_price_int(dict(item))
                if isinstance(item_price, int) and item_price > 0:
                    ratio = (item_price - price_floor) / max(1, (price_ceil - price_floor))
                    if price_order == "desc":
                        price_bias = 3.0 * ratio
                    else:
                        price_bias = 3.0 * (1.0 - ratio)
            return base + matched + price_bias

        scored = sorted(items, key=_total_score, reverse=True)
        if limit <= 0:
            return scored
        return scored[:limit]

    @staticmethod
    def _catalog_item_identity(item: Dict[str, Any]) -> str:
        for key in ("id", "sku", "title", "name"):
            val = item.get(key)
            if val:
                return str(val)
        return json.dumps(item, ensure_ascii=False, sort_keys=True)

    def _merge_catalog_results(
        self,
        base: List[Dict[str, Any]],
        fallback: List[Dict[str, Any]],
        limit: int,
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return base
        seen = {self._catalog_item_identity(item) for item in base}
        for item in fallback:
            identity = self._catalog_item_identity(item)
            if identity in seen:
                continue
            base.append(item)
            seen.add(identity)
            if len(base) >= limit:
                break
        return base

    def _sort_catalog_by_price_order(
        self,
        items: Sequence[Mapping[str, Any]],
        order: str,
    ) -> List[Dict[str, Any]]:
        normalized_order = str(order or "").strip().lower()
        if normalized_order not in {"asc", "desc"}:
            return [dict(item) for item in items]
        indexed = list(enumerate(items))

        def _key(entry: Tuple[int, Mapping[str, Any]]) -> Tuple[int, int, int]:
            idx, item = entry
            price = self.deps.item_price_int(dict(item))
            if not isinstance(price, int) or price <= 0:
                return (1, 0, idx)
            if normalized_order == "desc":
                return (0, -price, idx)
            return (0, price, idx)

        indexed.sort(key=_key)
        return [dict(item) for _, item in indexed]

    def search_catalog(
        self,
        needs: Dict[str, Any],
        limit: int = 5,
        tenant: int | None = None,
        query: str | None = None,
    ) -> List[Dict[str, Any]]:
        needs = dict(needs or {})
        query_price_order = self.deps.extract_price_order_intent(str(query or ""))
        if query_price_order and "price_order" not in needs:
            needs["price_order"] = query_price_order
        items = self.deps.read_catalog(tenant)
        if not items:
            items = self.deps.read_catalog(None)
        persona_meta: Dict[str, Any] = {}
        if tenant is not None:
            try:
                persona_meta = self.deps.persona_meta_config(int(tenant))
            except Exception:
                persona_meta = {}
        self.deps.augment_color_needs(needs, persona_meta)
        filtered = self.deps.filter_catalog_items_by_rules(items, needs, persona_meta) if items else []
        if filtered:
            items = filtered

        explicit_price_order = str(needs.get("price_order") or "").strip().lower()
        if explicit_price_order in {"asc", "desc"} and items:
            ranked = self._legacy_rank_catalog(items, needs, 0, query)
            ordered = self._sort_catalog_by_price_order(ranked, explicit_price_order)
            if limit <= 0:
                return ordered
            return ordered[:limit]

        advanced: List[Dict[str, Any]] = []
        if self.deps.catalog_retriever and items:
            try:
                advanced = self.deps.catalog_retriever.retrieve_context(
                    items=items,
                    needs=needs,
                    query=query or "",
                    tenant=tenant,
                    limit=limit,
                )
            except Exception as exc:
                self.deps.logger.exception("catalog retriever failed", exc_info=exc)

        if advanced:
            wants_noise = bool(self.deps.noise_need_re.search(self.deps.normalize_text(query or "")))
            if wants_noise:
                fallback_noise = self._legacy_rank_catalog(items, needs, max(limit, 8), query)
                blended_noise = self._merge_catalog_results(list(advanced), fallback_noise, max(limit, 8))
                ranked_noise = self._legacy_rank_catalog(blended_noise, needs, limit, query)
                if ranked_noise:
                    return ranked_noise[:limit]
            if limit <= 0:
                return advanced
            if len(advanced) < limit:
                fallback = self._legacy_rank_catalog(items, needs, limit, query)
                if fallback:
                    advanced = self._merge_catalog_results(advanced, fallback, limit)
            return advanced[:limit]

        return self._legacy_rank_catalog(items, needs, limit, query)
