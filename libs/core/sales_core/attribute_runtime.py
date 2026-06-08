from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class AttributeRuntimeDeps:
    normalize_text: Callable[[Any], str]
    fact_token_re: Any
    generic_fact_stopwords: Sequence[str]
    collect_item_text: Callable[[dict[str, Any]], str]
    tokenize_query: Callable[[str], list[str]]
    text_match_score: Callable[[dict[str, Any], list[str]], float]
    needs_stopwords: Sequence[str]
    item_price_int: Callable[[dict[str, Any]], int | None]
    display_item_label: Callable[[dict[str, Any]], str]
    format_rub_price: Callable[[int], str]


class AttributeRuntime:
    def __init__(self, deps: AttributeRuntimeDeps) -> None:
        self.deps = deps

    def normalize_probe_token(self, token: str) -> str:
        value = self.deps.normalize_text(token)
        for suffix in (
            "ая",
            "яя",
            "ое",
            "ее",
            "ый",
            "ий",
            "ой",
            "ую",
            "юю",
            "ые",
            "ие",
            "ого",
            "его",
            "ому",
            "ему",
            "ыми",
            "ими",
            "ых",
            "их",
            "ость",
            "ности",
            "ом",
            "ем",
            "ам",
            "ям",
            "ах",
            "ях",
            "ами",
            "ями",
            "у",
            "ю",
            "а",
            "я",
            "е",
            "ы",
            "и",
        ):
            if len(value) > len(suffix) + 2 and value.endswith(suffix):
                return value[: -len(suffix)]
        return value

    def extract_attribute_probe(self, user_text: str) -> str:
        tokens = [
            tok
            for tok in self.deps.fact_token_re.findall(self.deps.normalize_text(str(user_text or "")))
            if len(tok) >= 3 and not tok.isdigit() and tok not in self.deps.generic_fact_stopwords
        ]
        candidates: list[tuple[int, int, str]] = []
        for idx, token in enumerate(tokens):
            normalized = self.normalize_probe_token(token)
            if not normalized or normalized in self.deps.generic_fact_stopwords:
                continue
            candidates.append((len(normalized), idx, normalized))
        if not candidates:
            return ""
        candidates.sort(key=lambda row: (row[0], row[1]))
        return str(candidates[-1][2] or "").strip()

    def is_noisy_attribute_value(self, value: str) -> bool:
        raw = str(value or "").strip()
        if not raw:
            return True
        token_count = len(self.deps.fact_token_re.findall(self.deps.normalize_text(raw)))
        if token_count >= 24:
            return True
        if raw.count(":") >= 3:
            return True
        if raw.count(";") >= 3:
            return True
        return False

    def is_dimension_like_value(self, value: str) -> bool:
        raw = str(value or "").strip()
        if not raw:
            return False
        if re.search(r"\d{2,5}\s*[xх*]\s*\d{2,5}", raw, re.IGNORECASE):
            return True
        if re.search(r"\d{2,5}\s*/\s*\d{2,5}", raw):
            return True
        return False

    def iter_item_attribute_pairs(
        self,
        item_map: Mapping[str, Any],
        *,
        blocked: set[str],
    ) -> list[tuple[str, str, str, str]]:
        pairs: list[tuple[str, str, str, str]] = []
        for raw_key, raw_val in item_map.items():
            key = str(raw_key or "").strip()
            val = str(raw_val or "").strip()
            if not key or not val:
                continue
            key_norm = self.deps.normalize_text(key)
            if not key_norm or key_norm in blocked or key_norm.startswith("_"):
                continue
            if self.is_noisy_attribute_value(val):
                continue
            val_norm = self.deps.normalize_text(val)
            if len(val_norm) < 2:
                continue
            pairs.append((key, val, key_norm, val_norm))
        return pairs

    def format_attribute_pairs(self, pairs: Sequence[tuple[str, str]], *, max_pairs: int = 2) -> str:
        out: list[str] = []
        seen: set[str] = set()
        for key, val in pairs:
            key_clean = str(key or "").strip()
            val_clean = str(val or "").strip()
            if not key_clean or not val_clean:
                continue
            dedupe = f"{self.deps.normalize_text(key_clean)}::{self.deps.normalize_text(val_clean)}"
            if dedupe in seen:
                continue
            seen.add(dedupe)
            out.append(f"{key_clean}: {val_clean}")
            if len(out) >= max(1, int(max_pairs or 1)):
                break
        return "; ".join(out).strip()

    def selected_item_brief_answer(self, selected_item: Mapping[str, Any]) -> str:
        item_map = dict(selected_item)
        name = self.deps.display_item_label(item_map)
        if not name:
            return ""
        parts = [name]
        price = self.deps.item_price_int(item_map)
        if price:
            parts.append(f"{self.deps.format_rub_price(price)}")
        inside = str(item_map.get("Цвет внутренней панели") or item_map.get("color") or "").strip()
        if inside:
            parts.append(f"внутри {inside}")
        lock_count = str(item_map.get("Количество замков") or "").strip()
        if lock_count:
            parts.append(f"{lock_count} замка")
        return ". ".join(parts[:3]) + "."

    def items_with_attribute(
        self,
        items: Sequence[Mapping[str, Any]],
        probe: str,
    ) -> list[Mapping[str, Any]]:
        needle = self.normalize_probe_token(probe)
        if not needle:
            return []
        direct: list[Mapping[str, Any]] = []
        for item in items:
            text = self.deps.normalize_text(self.deps.collect_item_text(dict(item)))
            if not text:
                continue
            if needle in text:
                direct.append(item)
        if direct:
            return direct

        probe_tokens = [tok for tok in self.deps.tokenize_query(probe) if tok]
        if not probe_tokens:
            return []
        semantic: list[Mapping[str, Any]] = []
        for item in items:
            try:
                score = self.deps.text_match_score(dict(item), probe_tokens)
            except Exception:
                score = 0.0
            if score > 0:
                semantic.append(item)
        return semantic

    def items_with_attribute_direct(
        self,
        items: Sequence[Mapping[str, Any]],
        probe: str,
    ) -> list[Mapping[str, Any]]:
        needle = self.normalize_probe_token(probe)
        if not needle:
            return []
        direct: list[Mapping[str, Any]] = []
        for item in items:
            text = self.deps.normalize_text(self.deps.collect_item_text(dict(item)))
            if text and needle in text:
                direct.append(item)
        return direct

    def negative_attribute_probes(self, text: str) -> set[str]:
        low = self.deps.normalize_text(text)
        return {
            self.normalize_probe_token(match.group(1))
            for match in re.finditer(r"(?iu)\bбез\s+([a-zа-яё0-9-]{3,})", low)
            if match.group(1)
        }

    def exclude_items_with_negative_probes(
        self,
        items: Sequence[Mapping[str, Any]],
        probes: set[str],
    ) -> list[Mapping[str, Any]]:
        if not items or not probes:
            return list(items)
        out: list[Mapping[str, Any]] = []
        for item in items:
            text = self.deps.normalize_text(self.deps.collect_item_text(dict(item)))
            if not text:
                continue
            if any(probe and probe in text for probe in probes):
                continue
            out.append(item)
        return out

    def narrow_catalog_items_by_user_text(
        self,
        items: Sequence[Mapping[str, Any]],
        user_text: str,
    ) -> list[Mapping[str, Any]]:
        if not items:
            return []
        tokens = [
            tok
            for tok in self.deps.tokenize_query(user_text)
            if tok and (tok not in self.deps.needs_stopwords) and (not tok.isdigit())
        ]
        if not tokens:
            return list(items)
        generic = {
            "товар",
            "товары",
            "услуга",
            "услуги",
            "модель",
            "модели",
            "вариант",
            "варианты",
            "дороже",
            "дешевле",
        }
        generic_stems = (
            "двер",
            "квартир",
            "дом",
            "частн",
            "дорог",
            "дешев",
            "сам",
            "покаж",
        )
        raw_low = self.deps.normalize_text(user_text)
        neg_probes = self.negative_attribute_probes(raw_low)
        source_items: list[Mapping[str, Any]] = list(items)
        if neg_probes:
            narrowed = []
            for item in source_items:
                text = self.deps.normalize_text(self.deps.collect_item_text(dict(item)))
                if not text:
                    continue
                if any(probe and probe in text for probe in neg_probes):
                    continue
                narrowed.append(item)
            if narrowed:
                source_items = narrowed
        best: list[Mapping[str, Any]] | None = None
        for token in tokens[:8]:
            token_norm = self.normalize_probe_token(token)
            if (
                token in generic
                or any(token_norm.startswith(stem) for stem in generic_stems)
                or (token_norm in neg_probes)
            ):
                continue
            matched = self.items_with_attribute_direct(source_items, token_norm)
            if not matched:
                continue
            if best is None or len(matched) < len(best):
                best = list(matched)
        return best if best is not None else source_items
