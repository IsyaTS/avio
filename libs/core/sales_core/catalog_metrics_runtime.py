from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence


_GENERIC_PRICE_KEYWORD_PREFIXES = (
    "цен",
    "стоим",
    "скольк",
    "дорог",
    "дешев",
    "сам",
    "покаж",
    "вариант",
    "модел",
    "двер",
    "квартир",
    "дом",
    "частн",
)


@dataclass(frozen=True)
class CatalogMetricsRuntimeDeps:
    canonical_object_type_hint: Callable[[Any], str]
    normalize_text: Callable[[Any], str]
    collect_item_text: Callable[[dict[str, Any]], str]
    item_price_int: Callable[[Mapping[str, Any]], int | None]
    extract_price_spans: Callable[[str], list[tuple[int, int, int]]]
    normalize_probe_token: Callable[[str], str]


class CatalogMetricsRuntime:
    def __init__(self, deps: CatalogMetricsRuntimeDeps) -> None:
        self.deps = deps

    def item_object_type_hint(self, item: Mapping[str, Any]) -> str:
        direct = self.deps.canonical_object_type_hint(item.get("object_type"))
        if direct:
            return direct

        tags = item.get("tags") or []
        tag_tokens = {
            self.deps.normalize_text(tag)
            for tag in (tags if isinstance(tags, Sequence) and not isinstance(tags, (str, bytes)) else [tags])
            if str(tag or "").strip()
        }
        if "house_ready" in tag_tokens:
            return "house"
        if "apartment_ready" in tag_tokens:
            return "apartment"

        for raw_key, raw_val in item.items():
            key = self.deps.normalize_text(raw_key)
            if not key:
                continue
            if any(token in key for token in ("object", "usage", "назнач", "помещ", "тип")):
                kind = self.deps.canonical_object_type_hint(raw_val)
                if kind:
                    return kind

        hay = self.deps.normalize_text(self.deps.collect_item_text(dict(item)))
        if hay:
            kind = self.deps.canonical_object_type_hint(hay)
            if kind:
                return kind
        return ""

    def filter_items_by_object_type_need(
        self,
        items: Sequence[Mapping[str, Any]],
        needs: Mapping[str, Any] | None,
    ) -> list[Mapping[str, Any]]:
        if not items or not isinstance(needs, Mapping):
            return list(items)
        target = self.deps.normalize_text(needs.get("object_type") or "")
        if target not in {"apartment", "house"}:
            return list(items)

        hints = [self.item_object_type_hint(item) for item in items]
        if target == "apartment":
            if any(hint == "house" for hint in hints):
                out = [item for item, hint in zip(items, hints) if hint != "house"]
                if out:
                    return out
            if any(hint == "apartment" for hint in hints):
                out = [item for item, hint in zip(items, hints) if hint == "apartment"]
                if out:
                    return out
            return list(items)

        if any(hint == "house" for hint in hints):
            out = [item for item, hint in zip(items, hints) if hint == "house"]
            if out:
                return out
        return list(items)

    def extract_budget_cap_from_needs(self, needs: Mapping[str, Any] | None) -> Optional[int]:
        if not isinstance(needs, Mapping):
            return None
        for key in ("budget_max", "budget", "budget_to", "max_budget"):
            raw = needs.get(key)
            if raw in (None, ""):
                continue
            try:
                if isinstance(raw, (int, float)):
                    val = int(raw)
                else:
                    match = re.search(r"\d[\d\s.,]*", str(raw))
                    if not match:
                        continue
                    val = int(re.sub(r"\D", "", match.group(0)))
                if val > 0:
                    return val
            except Exception:
                continue
        return None

    def catalog_min_price(self, items: Sequence[Mapping[str, Any]]) -> int | None:
        vals = [self.deps.item_price_int(dict(item)) for item in items]
        clean = [int(v) for v in vals if isinstance(v, int) and v > 0]
        if not clean:
            return None
        return min(clean)

    def catalog_max_price(self, items: Sequence[Mapping[str, Any]]) -> int | None:
        vals = [self.deps.item_price_int(dict(item)) for item in items]
        clean = [int(v) for v in vals if isinstance(v, int) and v > 0]
        if not clean:
            return None
        return max(clean)

    def catalog_extreme_item_by_price(
        self,
        items: Sequence[Mapping[str, Any]],
        *,
        highest: bool,
    ) -> Mapping[str, Any] | None:
        best: Mapping[str, Any] | None = None
        best_price: int | None = None
        for item in items:
            price = self.deps.item_price_int(dict(item))
            if not isinstance(price, int) or price <= 0:
                continue
            if best is None or best_price is None:
                best = item
                best_price = price
                continue
            if highest and price > best_price:
                best = item
                best_price = price
            if (not highest) and price < best_price:
                best = item
                best_price = price
        return best

    def extract_price_target_hint(self, text: str) -> int | None:
        raw = str(text or "")
        if not raw.strip():
            return None
        spans = self.deps.extract_price_spans(raw)
        if spans:
            return int(spans[0][2])
        match_k = re.search(r"(?iu)\b(\d{1,3})\s*(?:тыс(?:\.|яч)?|тысяч(?:а|и)?|к)\b", raw)
        if match_k:
            try:
                return int(match_k.group(1)) * 1000
            except Exception:
                return None
        match_plain = re.search(r"(?iu)\bза\s*(\d{1,3})\b", raw)
        if match_plain:
            try:
                return int(match_plain.group(1)) * 1000
            except Exception:
                return None
        return None

    def closest_catalog_item_by_price(
        self,
        items: Sequence[Mapping[str, Any]],
        target_price: int,
    ) -> Mapping[str, Any] | None:
        best: Mapping[str, Any] | None = None
        best_diff: int | None = None
        for item in items:
            price = self.deps.item_price_int(dict(item))
            if not price:
                continue
            diff = abs(int(price) - int(target_price))
            if best is None or best_diff is None or diff < best_diff:
                best = item
                best_diff = diff
        return best

    def is_likely_price_value(self, value: int) -> bool:
        try:
            number = int(value)
        except Exception:
            return False
        if number <= 0:
            return False
        if len(str(abs(number))) >= 9:
            return False
        return True

    def is_catalog_price_candidate(self, value: int, catalog_prices: set[int] | None) -> bool:
        if not self.is_likely_price_value(value):
            return False
        prices = {int(v) for v in (catalog_prices or set()) if isinstance(v, int) and v > 0}
        if not prices:
            return True
        min_catalog = min(prices)
        dynamic_floor = max(300, int(min_catalog * 0.33))
        try:
            return int(value) >= dynamic_floor
        except Exception:
            return False

    def is_specific_catalog_keyword(self, keyword: str) -> bool:
        token = self.deps.normalize_probe_token(keyword)
        if len(token) < 4:
            return False
        if any(token.startswith(prefix) for prefix in _GENERIC_PRICE_KEYWORD_PREFIXES):
            return False
        return True

    def catalog_has_object_type_evidence(
        self,
        items: Sequence[Mapping[str, Any]],
        object_type_need: str,
    ) -> bool:
        kind = self.deps.normalize_text(object_type_need)
        if kind not in {"apartment", "house"}:
            return True
        hints = ("квартир", "apartment", "flat") if kind == "apartment" else ("частн", "дом", "house")
        for item in items:
            text = self.deps.normalize_text(self.deps.collect_item_text(dict(item)))
            if any(hint in text for hint in hints):
                return True
        return False
