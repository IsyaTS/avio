from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class CatalogPriceRuntimeDeps:
    price_inline_re: Any
    price_thousands_re: Any
    normalize_model_alias: Callable[[str], str]
    item_aliases: Callable[[Mapping[str, Any]], list[str]]
    catalog_item_identity: Callable[[Mapping[str, Any]], str]
    item_label: Callable[[Mapping[str, Any]], str]
    item_price_int: Callable[[Mapping[str, Any]], int | None]


class CatalogPriceRuntime:
    def __init__(self, deps: CatalogPriceRuntimeDeps) -> None:
        self.deps = deps

    def extract_prices(self, sentence: str) -> list[int]:
        return [item[2] for item in self.extract_price_spans(sentence)]

    def extract_price_spans(self, sentence: str) -> list[tuple[int, int, int]]:
        raw = str(sentence or "")
        if not raw:
            return []
        spans: list[tuple[int, int, int]] = []
        for match in self.deps.price_inline_re.finditer(raw):
            digits = re.sub(r"\D", "", match.group(0))
            if len(digits) < 4:
                continue
            try:
                price = int(digits)
            except Exception:
                continue
            spans.append((match.start(), match.end(), price))
        for match in self.deps.price_thousands_re.finditer(raw):
            try:
                base = int(str(match.group(1) or "0"))
            except Exception:
                continue
            if base <= 0:
                continue
            spans.append((match.start(), match.end(), base * 1000))
        if not spans:
            return []
        spans.sort(key=lambda item: (item[0], -(item[1] - item[0])))
        merged: list[tuple[int, int, int]] = []
        for span in spans:
            if not merged:
                merged.append(span)
                continue
            prev = merged[-1]
            if span[0] < prev[1]:
                continue
            merged.append(span)
        return merged

    @staticmethod
    def format_rub_price(value: int) -> str:
        return f"{int(value):,}".replace(",", " ") + " ₽"

    def mentioned_catalog_items_in_order(
        self,
        sentence: str,
        items: Sequence[Mapping[str, Any]],
    ) -> list[Mapping[str, Any]]:
        normalized_sentence = self.deps.normalize_model_alias(sentence)
        if not normalized_sentence:
            return []
        hits: list[tuple[int, Mapping[str, Any]]] = []
        for item in items:
            aliases = self.deps.item_aliases(item)
            if not aliases:
                continue
            best_pos: int | None = None
            for alias in aliases:
                alias_norm = self.deps.normalize_model_alias(alias)
                if len(alias_norm) < 3:
                    continue
                pos = normalized_sentence.find(alias_norm)
                if pos < 0:
                    continue
                if best_pos is None or pos < best_pos:
                    best_pos = pos
            if best_pos is None:
                continue
            hits.append((best_pos, item))
        if not hits:
            return []
        hits.sort(key=lambda item: item[0])
        ordered: list[Mapping[str, Any]] = []
        seen: set[str] = set()
        for _, item in hits:
            identity = self.deps.catalog_item_identity(dict(item))
            if identity in seen:
                continue
            seen.add(identity)
            ordered.append(item)
        return ordered

    def format_short_catalog_variants(
        self,
        items: Sequence[Mapping[str, Any]],
        limit: int = 2,
    ) -> str:
        chunks: list[str] = []
        for item in list(items)[: max(1, int(limit))]:
            label = self.deps.item_label(item)
            if not label:
                continue
            price = self.deps.item_price_int(item)
            if price:
                price_text = f"{price:,}".replace(",", " ") + " ₽"
                chunks.append(f"{label} — {price_text}")
            else:
                chunks.append(label)
        return "; ".join(chunks)
