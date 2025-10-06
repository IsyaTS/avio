from __future__ import annotations

"""Catalog normalization pipeline shared across ingestion flows."""

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence, Set, Tuple

# NOTE: shared regex helpers align CSV and PDF sanitation behaviour
_INVISIBLE_RE = re.compile(r"[\u200b\u200c\u200d\u2028\u2029\u2060]")
_HYPHEN_LINEBREAK_RE = re.compile(r"-\s*\n\s*")
_LETTER_RUN_RE = re.compile(r"(?:(?<=\s)|^)([A-ZА-ЯЁ])(?:\s+([A-ZА-ЯЁ])){2,}(?=(?:\s|$))")
_TRAILING_LAST_LETTER_RE = re.compile(r"([A-Za-zА-Яа-яЁё])\s+([A-Za-zА-Яа-яЁё])\b")
_MULTI_SPACE_RE = re.compile(r"\s+")
_CURRENCY_REGEX = re.compile(r"(?:₽|руб\.?|\bр\.?\b|\$|€|usd|eur|byn|kzt|uah)", re.IGNORECASE)
_UNIT_REGEX = re.compile(r"\b(?:мм|см|cm|mm|kg|кг)\b", re.IGNORECASE)
_LONG_DIGITS_RE = re.compile(r"\d{6,}")
_ARTICLE_PATTERN = re.compile(r"[A-Za-zА-Яа-я0-9]{2,}[-_][A-Za-zА-Яа-я0-9]{2,}")
_PRICE_NUMBER_RE = re.compile(r"\d[\d\s.,]*")

BANNED_COLUMNS = {
    # Keep obvious technical/debug fields out of the final CSV header.
    # Do NOT ban descriptive fields like description/brand/category as they
    # are often needed for characteristics and search quality.
    "sku",
    "images",
    "source_page",
    "source_file",
    # Do not include page index in final CSV
    "page",
}

_TITLE_PRIORITY = (
    "title",
    "name",
    "product",
    "model",
    "item",
    "товар",
    "название",
    "наимен",  # наименование
    "пози",     # позиция
)
_PRICE_PRIORITY = (
    "price",
    "cost",
    "amount",
    "стоим",
    "цена",
)


@dataclass
class PipelineReport:
    """Structured summary of the normalization pipeline."""

    items: int
    columns: List[str]
    dropped_columns: List[str] = field(default_factory=list)
    duplicate_titles_fixed: List[Tuple[str, str]] = field(default_factory=list)
    coverage: Dict[str, int] = field(default_factory=dict)
    price_filled: int = 0
    merged_columns_map: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "items": self.items,
            "columns": list(self.columns),
            "dropped_columns": list(self.dropped_columns),
            "duplicate_titles_fixed": [list(pair) for pair in self.duplicate_titles_fixed],
            "coverage": dict(self.coverage),
            "price_filled": int(self.price_filled),
            "merged_columns_map": dict(self.merged_columns_map),
        }


def sanitize_value(value: Any) -> str:
    """Clean text the same way for CSV and PDF derived data."""

    text = "" if value is None else str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Join words broken by hyphen at line end (e.g., "назо-\nвание" -> "назование")
    text = _HYPHEN_LINEBREAK_RE.sub("", text)
    text = text.replace("_", " ")
    text = text.replace("\u00ad", "")
    text = _INVISIBLE_RE.sub("", text)
    text = _collapse_spaced_letters(text)
    text = _TRAILING_LAST_LETTER_RE.sub(r"\1\2", text)
    text = _MULTI_SPACE_RE.sub(" ", text)
    return text.strip()


def _collapse_spaced_letters(text: str) -> str:
    if not text:
        return ""

    def repl(match: re.Match[str]) -> str:
        chunk = match.group(0)
        return re.sub(r"\s+", "", chunk)

    return _LETTER_RUN_RE.sub(repl, text)


def clean_title(text: str) -> str:
    cleaned = sanitize_value(text)
    cleaned = _CURRENCY_REGEX.sub("", cleaned)
    cleaned = _UNIT_REGEX.sub("", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = cleaned.strip(" ,.;-·•")
    if cleaned:
        match = _ARTICLE_PATTERN.search(text)
        if match and match.group(0) not in cleaned:
            cleaned = f"{cleaned} {match.group(0)}".strip()
    return cleaned


def title_contains_forbidden(text: str) -> bool:
    if not text:
        return False
    if _CURRENCY_REGEX.search(text):
        return True
    if _UNIT_REGEX.search(text):
        return True
    if _LONG_DIGITS_RE.search(text):
        return True
    return False


def normalize_price_value(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    digits_only = re.sub(r"[^0-9.,\s]", "", text)
    if not digits_only:
        return ""
    digits_only = digits_only.replace("\u00a0", "").replace(" ", "")
    digits_only = digits_only.replace(",", ".")
    if digits_only.count(".") > 1:
        parts = digits_only.split(".")
        integer = parts[0] + "".join(parts[1:-1])
        fraction = parts[-1]
        digits_only = integer + (f".{fraction}" if fraction else "")
    elif digits_only.count(".") == 1:
        integer, fraction = digits_only.split(".")
        if not fraction:
            digits_only = integer
        elif len(fraction) == 3:
            digits_only = integer + fraction
    if not digits_only:
        return ""
    try:
        float(digits_only)
    except ValueError:
        return ""
    return digits_only


def _normalize_key_name(name: str) -> str:
    def _latin_to_cyrillic_lookalikes(s: str) -> str:
        # Map common Latin lookalikes to Cyrillic to unify keys like "BEC" -> "ВЕС"
        mapping = str.maketrans({
            "A": "А", "a": "а",
            "B": "В", "b": "в",
            "C": "С", "c": "с",
            "E": "Е", "e": "е",
            "H": "Н", "h": "н",
            "K": "К", "k": "к",
            "M": "М", "m": "м",
            "O": "О", "o": "о",
            "P": "Р", "p": "р",
            "T": "Т", "t": "т",
            "X": "Х", "x": "х",
            "Y": "У", "y": "у",
        })
        return s.translate(mapping)

    text = name.strip()
    # First normalize Unicode variants
    text = text.replace("ё", "е")
    # Unify Latin lookalikes to Cyrillic to help clustering
    text = _latin_to_cyrillic_lookalikes(text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9а-я\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # Heuristic suffix trimming for Russian noun forms
    for suffix in (
        "ыми", "ими", "ыми", "ями", "ами",
        "ого", "его", "ому", "ему",
        "ов", "ев", "ей", "ам", "ям", "ах", "ях",
        "ой", "ом", "ую", "ую", "ая", "яя",
        "а", "я", "у", "е", "ю", "ы", "и", "ь",
    ):
        if text.endswith(suffix) and len(text) > len(suffix) + 2:
            text = text[: -len(suffix)]
            break
    return text


def _value_type(values: Iterable[str]) -> str:
    has_numeric = False
    has_text = False
    for value in values:
        if not value:
            continue
        cleaned = value.strip()
        if not cleaned:
            continue
        normalized = re.sub(r"\s", "", cleaned)
        if re.fullmatch(r"\d+(?:[.,]\d+)?", normalized):
            has_numeric = True
            continue
        if re.search(r"[A-Za-zА-Яа-яЁё]", cleaned):
            has_text = True
        else:
            has_numeric = True
    if has_numeric and has_text:
        return "mixed"
    if has_numeric:
        return "numeric"
    if has_text:
        return "text"
    return "unknown"


def _cluster_columns(items: Sequence[MutableMapping[str, str]]) -> Dict[str, str]:
    value_lists: Dict[str, List[str]] = defaultdict(list)
    counts: Counter[str] = Counter()
    for item in items:
        for key, value in item.items():
            if key in {"id", "title", "price"}:
                continue
            if value:
                value_lists[key].append(value)
                counts[key] += 1
    keys = list(value_lists.keys())
    mapping: Dict[str, str] = {}
    used: Set[str] = set()
    for key in keys:
        if key in used:
            continue
        norm_key = _normalize_key_name(key)
        group = [key]
        group_types = {_value_type(value_lists[key])}
        used.add(key)
        for other in keys:
            if other in used:
                continue
            other_norm = _normalize_key_name(other)
            if not norm_key or not other_norm:
                continue
            similarity = SequenceMatcher(None, norm_key, other_norm).ratio()
            if similarity < 0.86:
                continue
            other_type = _value_type(value_lists[other])
            if ("numeric" in group_types and other_type in {"text", "mixed"}) or (
                other_type == "numeric" and group_types & {"text", "mixed"}
            ):
                continue
            group.append(other)
            group_types.add(other_type)
            used.add(other)
        if len(group) <= 1:
            continue
        # Prefer the most frequent and more descriptive (longer) header
        canonical = max(group, key=lambda name: (counts.get(name, 0), len(name)))
        for alias in group:
            if alias == canonical:
                continue
            mapping[alias] = canonical
    return mapping


def _apply_column_mapping(items: Sequence[MutableMapping[str, str]], mapping: Mapping[str, str]) -> None:
    if not mapping:
        return
    for item in items:
        for alias, canonical in mapping.items():
            if alias not in item:
                continue
            value = item.pop(alias)
            if not value:
                continue
            if canonical not in item or not item[canonical]:
                item[canonical] = value


def _drop_columns(items: Sequence[MutableMapping[str, str]]) -> Tuple[Set[str], Counter[str]]:
    if not items:
        return set(), Counter()
    row_count = len(items)
    keys = {key for item in items for key in item if key not in {"id", "title", "price"}}
    coverage: Counter[str] = Counter()
    for item in items:
        for key in keys:
            if item.get(key):
                coverage[key] += 1
    # Drop very sparse/noisy attributes; keep those seen at least twice or ~5%.
    min_coverage = max(2, math.ceil(row_count * 0.05)) if row_count else 0
    banned_lower = {name.lower() for name in BANNED_COLUMNS}
    drop: Set[str] = set()
    for key in keys:
        lower = key.lower()
        normalized = _normalize_key_name(key)
        # Only drop explicitly banned technical columns; do not drop
        # generic characteristic/description-like columns automatically.
        if lower in banned_lower:
            drop.add(key)
            continue
        if any(suffix in lower for suffix in ("_raw", "_trace", "_score")):
            drop.add(key)
            continue
        if coverage.get(key, 0) < min_coverage:
            drop.add(key)
    for item in items:
        for key in list(item.keys()):
            if key in {"id", "title", "price"}:
                continue
            if key in drop:
                item.pop(key, None)
    return drop, coverage


def _humanize_header_keys(items: Sequence[MutableMapping[str, str]]) -> None:
    """Rename terse numeric single-word headers into human-friendly phrases.

    For example: "замков" -> "количество замков" when values are numeric.
    Runs in-place on provided items.
    """
    if not items:
        return

    protected_prefixes = (
        "количество",
        "диаметр",
        "высота",
        "ширина",
        "длина",
        "толщина",
        "вес",
        "масса",
        "объем",
        "объём",
        "мощность",
        "напряжение",
        "ток",
        "частота",
        "скорость",
        "цвет",
        "материал",
        "бренд",
        "модель",
    )

    # Collect values per key to infer type
    values: Dict[str, List[str]] = defaultdict(list)
    for row in items:
        for key, value in row.items():
            if key in {"id", "title", "price"}:
                continue
            if value:
                values[key].append(value)

    def detect_unit(vals: List[str]) -> str | None:
        mm = kg = 0
        for v in vals:
            t = v.lower()
            if re.search(r"\b(?:мм|mm|см|cm)\b", t):
                mm += 1
            if re.search(r"\b(?:кг|kg)\b", t):
                kg += 1
        if mm and mm >= max(1, int(0.5 * len(vals))):
            return "mm"
        if kg and kg >= max(1, int(0.5 * len(vals))):
            return "kg"
        return None

    rename_map: Dict[str, str] = {}
    for key, vals in values.items():
        key_l = key.strip().lower()
        # Never rename reserved/banned technical keys
        if key_l in {name.lower() for name in BANNED_COLUMNS}:
            continue
        if any(key_l.startswith(pref) for pref in protected_prefixes):
            continue
        # Only consider single-token terse keys
        if len(key_l.split()) > 1:
            continue
        vtype = _value_type(vals)
        unit = detect_unit(vals)
        # Semantic renames
        if unit == "mm":
            rename_map[key] = f"толщина {key_l}"
            continue
        if unit == "kg":
            if key_l != "вес":
                rename_map[key] = "вес"
            continue
        if vtype == "numeric":
            rename_map[key] = f"количество {key_l}"
            continue
        # Domain-specific light heuristics
        if "покраск" in key_l:
            rename_map[key] = "цвет покраски"
            continue
        # Keep domain-agnostic; avoid renames tied to a specific product niche
        # Do not rename generic text columns like 'описание', 'цвет', etc.

    if not rename_map:
        return

    # Apply renames with merge semantics
    for row in items:
        for old, new in rename_map.items():
            if old in row:
                val = row.pop(old)
                if not val:
                    continue
                # Keep existing value if destination already set
                if new not in row or not row[new]:
                    row[new] = val


def _collect_column_frequencies(items: Sequence[Mapping[str, str]]) -> List[str]:
    freq: Counter[str] = Counter()
    for item in items:
        for key, value in item.items():
            if key in {"id", "title", "price"}:
                continue
            if value:
                freq[key] += 1
    return sorted(freq.keys(), key=lambda name: (-freq[name], name.lower()))


def _ensure_unique_titles(items: Sequence[MutableMapping[str, str]], attribute_columns: Sequence[str]) -> List[Tuple[str, str]]:
    """Ensure titles are unique by appending distinguishing details.

    If duplicates are found, try to append a short variant marker derived from
    the first non-empty attribute (e.g., color or material). If nothing usable
    is found, append an ordinal suffix.
    Returns a list of (original, updated) pairs for manifest reporting.
    """
    seen: Dict[str, List[int]] = defaultdict(list)
    for idx, row in enumerate(items):
        title = row.get("title") or ""
        seen[title].append(idx)

    fixes: List[Tuple[str, str]] = []
    for title, indices in seen.items():
        if len(indices) <= 1:
            continue
        for dup_order, idx in enumerate(indices, start=1):
            if dup_order == 1:
                continue
            row = items[idx]
            # Pick the first non-empty attribute to distinguish
            suffix = ""
            for key in attribute_columns:
                if key in {"id", "title", "price"}:
                    continue
                val = (row.get(key) or "").strip()
                if val:
                    # Keep suffix short
                    short = re.sub(r"\s+", " ", val)
                    if len(short) > 24:
                        short = short[:24].rstrip() + "…"
                    suffix = f" ({short})"
                    break
            if not suffix:
                suffix = f" (вариант {dup_order})"
            updated = f"{title}{suffix}".strip()
            if updated != title:
                row["title"] = updated
                fixes.append((title, updated))
    return fixes


def _assign_ids(items: Sequence[MutableMapping[str, str]]) -> None:
    for idx, item in enumerate(items, start=1):
        item["id"] = str(idx)


def _validate_items(items: Sequence[Mapping[str, str]], header: Sequence[str]) -> None:
    # Titles may repeat across variants; uniqueness not enforced.
    price_pattern = re.compile(r"^\d+(?:\.\d+)?$")
    for item in items:
        price = item.get("price", "")
        if price and not price_pattern.fullmatch(price):
            raise ValueError(f"invalid price value: {price}")
        title = item.get("title", "")
        if title_contains_forbidden(title):
            raise ValueError(f"forbidden tokens in title: {title}")
    if len(header) != len(set(header)):
        raise ValueError("duplicate columns in header")
    banned_lower = {name.lower() for name in BANNED_COLUMNS}
    for column in header:
        lower = column.lower()
        if lower in banned_lower or any(sfx in lower for sfx in ("_raw", "_trace", "_score")):
            raise ValueError(f"forbidden column in header: {column}")


def _is_preferred_title_key(key: str) -> bool:
    lowered = key.lower()
    return any(hint in lowered for hint in _TITLE_PRIORITY)


def _is_preferred_price_key(key: str) -> bool:
    lowered = key.lower()
    return any(hint in lowered for hint in _PRICE_PRIORITY)


def _choose_title(row: MutableMapping[str, str], fallback_index: int) -> str:
    candidates: List[Tuple[int, str]] = []
    priority = 0
    for key, value in row.items():
        if key in {"id", "title", "price"}:
            continue
        candidate_raw = value
        if not candidate_raw:
            continue
        cleaned = clean_title(candidate_raw)
        if not cleaned or title_contains_forbidden(cleaned):
            continue
        score = 0
        if _is_preferred_title_key(key):
            score += 2
        if len(cleaned) >= 3:
            score += 1
        candidates.append((score, cleaned))
    if row.get("title"):
        cleaned = clean_title(row["title"])
        if cleaned and not title_contains_forbidden(cleaned):
            # Prefer the pre-selected title from the parser over attribute-derived values
            candidates.append((priority + 5, cleaned))
    if candidates:
        best = max(candidates, key=lambda item: (item[0], len(item[1])))
        return best[1]
    return f"Позиция {fallback_index + 1}"


def _choose_price(row: Mapping[str, str]) -> str:
    # 1) Prefer explicit price-like keys or values with currency
    for key, value in row.items():
        if not value:
            continue
        normalized = normalize_price_value(value)
        if not normalized:
            continue
        if _is_preferred_price_key(key):
            return normalized
        if _CURRENCY_REGEX.search(value):
            return normalized
    # 2) Fallback: choose the largest plausible number (not a size/weight)
    best = ""
    best_val = 0.0
    for key, value in row.items():
        if not value:
            continue
        if _UNIT_REGEX.search(value):
            continue
        normalized = normalize_price_value(value)
        if not normalized:
            continue
        try:
            num = float(normalized)
        except Exception:
            continue
        # plausible retail price window
        if num < 999 or num > 5_000_000:
            continue
        if num > best_val:
            best_val = num
            best = normalized
    if best:
        return best
    return ""


def finalize_catalog_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    keep_existing_ids: bool = False,
) -> Tuple[List[Dict[str, str]], List[str], PipelineReport]:
    """Run the catalog pipeline and return normalized rows and header."""

    normalized: List[Dict[str, str]] = []
    price_filled = 0
    for idx, raw_row in enumerate(rows):
        if not isinstance(raw_row, Mapping):
            continue
        row: Dict[str, str] = {}
        for key, value in raw_row.items():
            key_text = str(key) if key is not None else ""
            if not key_text:
                continue
            row[key_text] = sanitize_value(value)
        title = _choose_title(row, idx)
        price = _choose_price(row)
        if price:
            price_filled += 1
        normalized_row: Dict[str, str] = {
            **{k: v for k, v in row.items() if k not in {"title", "price"}},
            "title": title,
            "price": price,
        }
        normalized.append(normalized_row)

    mapping = _cluster_columns(normalized)
    merged_map = dict(mapping)
    _apply_column_mapping(normalized, mapping)
    # Drop banned/empty columns before any renaming so reserved keys (e.g. page)
    # cannot leak back via humanization.
    dropped, coverage = _drop_columns(normalized)
    # Humanize terse numeric headers after we’ve removed banned columns.
    _humanize_header_keys(normalized)
    columns = _collect_column_frequencies(normalized)
    duplicate_fixes = _ensure_unique_titles(normalized, columns)
    if not keep_existing_ids:
        for row in normalized:
            row.pop("id", None)
        _assign_ids(normalized)
    header = ["id", "title", "price", *columns]
    _validate_items(normalized, header)

    report = PipelineReport(
        items=len(normalized),
        columns=header,
        dropped_columns=sorted(dropped),
        duplicate_titles_fixed=duplicate_fixes,
        coverage={key: coverage.get(key, 0) for key in columns},
        price_filled=price_filled,
        merged_columns_map=merged_map,
    )
    return normalized, header, report
