from __future__ import annotations

import csv
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Tuple


_FIELD_SYNONYMS: Dict[str, List[str]] = {
    "title": [
        "title",
        "name",
        "product",
        "productname",
        "item",
        "itemname",
        "goods",
        "model",
        "модель",
        "товар",
        "наименование",
        "название",
        "позиция",
        "описание",
        "характеристика",
    ],
    "price": [
        "price",
        "cost",
        "стоимость",
        "цена",
        "ценаактуальная",
        "ценапродажи",
        "ценаруб",
        "ценазасистему",
        "ценазасчет",
        "ценаскидкой",
        "ценабезскидки",
        "ценазам2",
        "ценазамкв",
        "ценазаметры",
        "ценарозничная",
        "ценазапозицию",
    ],
    "sku": [
        "sku",
        "код",
        "кодтовара",
        "артикул",
        "арт",
        "код1с",
        "идентификатор",
        "id",
        "article",
    ],
    "url": [
        "url",
        "link",
        "urlтовара",
        "ссылка",
        "hyperlink",
        "страница",
    ],
    "brand": [
        "brand",
        "бренд",
        "марка",
        "производитель",
        "manufacturer",
    ],
    "stock": [
        "stock",
        "наличие",
        "остаток",
        "остатки",
        "количество",
        "qty",
        "quantity",
        "available",
    ],
    "image": [
        "image",
        "photo",
        "img",
        "picture",
        "изображение",
        "картинка",
        "фото",
        "фотография",
    ],
    "description": [
        "description",
        "описание",
        "details",
        "характеристики",
        "features",
        "comment",
    ],
    "color": [
        "color",
        "colour",
        "цвет",
        "цветпанели",
        "цветвнутреннейпанели",
        "цветвнутренней",
        "цветснутри",
        "colorinside",
    ],
    "finish": [
        "finish",
        "coating",
        "цветнаружнойпанели",
        "цветнаружи",
        "coloroutside",
        "цветпокраски",
        "покраска",
        "наружнаяпокраска",
        "цветвнешнейпанели",
    ],
    "shade": [
        "shade",
        "оттенок",
        "расцветка",
        "цветподбор",
    ],
    "object_type": [
        "objecttype",
        "object",
        "target",
        "usage",
        "назначение",
        "типобъекта",
        "типпомещения",
        "типпомещ",
        "помещение",
        "длякого",
        "длячего",
        "application",
    ],
}


@dataclass(frozen=True)
class CatalogCsvRuntimeDeps:
    field_clean_re: Any
    normalize_text: Any


class CatalogCsvRuntime:
    def __init__(self, deps: CatalogCsvRuntimeDeps) -> None:
        self.deps = deps
        self.field_token_map: Dict[str, List[str]] = {
            key: sorted(
                {self.canonicalize_field_name(token) for token in tokens if token},
                key=len,
                reverse=True,
            )
            for key, tokens in _FIELD_SYNONYMS.items()
        }

    def canonicalize_field_name(self, name: str) -> str:
        return self.deps.field_clean_re.sub("", (name or "").lower())

    def merge_csv_mapping_meta(
        self,
        meta: Mapping[str, Any] | None,
        persona_meta: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = dict(meta or {})
        persona_csv = persona_meta.get("csv_mapping") if isinstance(persona_meta, Mapping) else None
        if not isinstance(persona_csv, Mapping):
            return result

        merged = dict(result.get("csv_mapping") or {})
        persona_columns = persona_csv.get("columns")
        existing_columns = dict(merged.get("columns") or {})

        if isinstance(persona_columns, Mapping):
            for canonical, aliases in persona_columns.items():
                key = str(canonical).strip()
                if not key:
                    continue
                alias_list: list[str] = []
                if isinstance(aliases, str):
                    alias_list = [aliases]
                elif isinstance(aliases, Mapping):
                    alias_list = [str(val) for val in aliases.values() if val]
                elif isinstance(aliases, Sequence):
                    alias_list = [str(val) for val in aliases if val]
                else:
                    alias_list = [str(aliases)]
                bucket = list(existing_columns.get(key, []))
                for alias in alias_list:
                    cleaned = str(alias).strip()
                    if cleaned and cleaned not in bucket:
                        bucket.append(cleaned)
                if bucket:
                    existing_columns[key] = bucket

        if existing_columns:
            merged["columns"] = existing_columns

        for extra_key, extra_value in persona_csv.items():
            if extra_key == "columns":
                continue
            merged.setdefault(extra_key, extra_value)

        if merged:
            result["csv_mapping"] = merged
        return result

    def prepare_field_mapping(self, meta: Dict[str, Any], items: List[Dict[str, Any]]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        if not items:
            return mapping

        sample_cols = list(items[0].keys())
        sample_canon = {col: self.canonicalize_field_name(col) for col in sample_cols}

        csv_mapping_meta = meta.get("csv_mapping") if isinstance(meta, Mapping) else {}
        csv_mapping_columns = (
            csv_mapping_meta.get("columns") if isinstance(csv_mapping_meta, Mapping) else {}
        )

        meta_fields = meta.get("fields") if isinstance(meta, dict) else None
        if isinstance(meta_fields, dict):
            for canonical, source in meta_fields.items():
                if not isinstance(canonical, str) or not isinstance(source, str):
                    continue
                cleaned_source = source.strip()
                if cleaned_source in sample_cols:
                    key_norm = canonical.strip()
                    key_lower = key_norm.lower()
                    mapping[key_lower] = cleaned_source
                    if key_norm != key_lower:
                        mapping.setdefault(key_norm, cleaned_source)

        used_sources = set(mapping.values())

        def _find_column(
            tokens: List[str],
            preferred: List[str] | None = None,
            raw_names: List[str] | None = None,
        ) -> str | None:
            preferred = preferred or []
            raw_lower = [name.lower() for name in raw_names] if raw_names else []
            for col in sample_cols:
                if col in used_sources:
                    continue
                canon = sample_canon.get(col) or ""
                if raw_lower and col.lower() in raw_lower:
                    used_sources.add(col)
                    return col
                for token in preferred:
                    if token and (canon == token or canon.startswith(token) or token in canon):
                        used_sources.add(col)
                        return col
                for token in tokens:
                    if not token:
                        continue
                    if canon == token or canon.startswith(token) or token in canon:
                        used_sources.add(col)
                        return col
            return None

        custom_aliases: Dict[str, List[str]] = {}
        if isinstance(csv_mapping_columns, Mapping):
            for canonical, aliases in csv_mapping_columns.items():
                key = str(canonical).strip()
                if not key:
                    continue
                if isinstance(aliases, str):
                    custom_aliases[key] = [aliases]
                elif isinstance(aliases, Mapping):
                    custom_aliases[key] = [str(val) for val in aliases.values() if val]
                elif isinstance(aliases, Sequence):
                    custom_aliases[key] = [str(val) for val in aliases if val]
                else:
                    custom_aliases[key] = [str(aliases)]

        for field_name, tokens in self.field_token_map.items():
            if field_name in mapping:
                continue
            preferred_aliases = custom_aliases.get(field_name, [])
            normalized_tokens = [
                self.canonicalize_field_name(alias) for alias in preferred_aliases if alias
            ] + list(tokens)
            column = _find_column(
                normalized_tokens,
                preferred=normalized_tokens,
                raw_names=preferred_aliases,
            )
            if column:
                mapping[field_name] = column

        for canonical, aliases in custom_aliases.items():
            key_norm = canonical.strip()
            key_lower = key_norm.lower()
            if key_lower in mapping or key_norm in mapping:
                continue
            normalized_tokens = [self.canonicalize_field_name(alias) for alias in aliases if alias]
            column = _find_column(
                normalized_tokens,
                preferred=normalized_tokens,
                raw_names=[alias.strip() for alias in aliases if isinstance(alias, str)],
            )
            if column:
                mapping[key_norm] = column
                mapping.setdefault(key_lower, column)

        if "price" not in mapping:
            numeric_candidates: List[str] = []
            for col in sample_cols:
                if col in used_sources:
                    continue
                canon = sample_canon.get(col) or ""
                if any(
                    token in canon
                    for token in ("цен", "price", "cost", "стоим", "руб", "uah", "usd", "eur")
                ):
                    mapping["price"] = col
                    used_sources.add(col)
                    break
                values = [str((row.get(col) or "")).strip() for row in items[:5]]
                digits = [re.sub(r"\D", "", val) for val in values if val]
                if any(len(d) >= 4 for d in digits):
                    numeric_candidates.append(col)
            if "price" not in mapping and numeric_candidates:
                mapping["price"] = numeric_candidates[0]
                used_sources.add(numeric_candidates[0])

        if "title" not in mapping:
            for col in sample_cols:
                if col in used_sources:
                    continue
                canon = sample_canon.get(col) or ""
                if any(
                    token in canon
                    for token in ("name", "товар", "пози", "model", "тип", "item", "наимен")
                ):
                    mapping["title"] = col
                    used_sources.add(col)
                    break
        return mapping

    @staticmethod
    def has_price_digits(value: Any) -> bool:
        text = str(value or "")
        digits = re.sub(r"\D", "", text)
        if len(digits) >= 4:
            return True
        lowered = text.lower()
        if len(digits) >= 3 and any(
            token in lowered for token in ("руб", "uah", "eur", "usd", "$", "€", "₽")
        ):
            return True
        try:
            normalized = text.replace(" ", "").replace(",", ".")
            float(normalized)
            return True
        except Exception:
            return False

    @staticmethod
    def normalize_csv_delimiter(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if text == "\\t":
            return "\t"
        return text[0]

    def csv_delimiter_candidates(self, sample: str, configured: Any) -> List[str]:
        candidates: List[str] = []

        def _append(delim: str | None) -> None:
            if not delim:
                return
            if delim not in candidates:
                candidates.append(delim)

        _append(self.normalize_csv_delimiter(configured))
        try:
            sniffed = csv.Sniffer().sniff(sample or "", delimiters=",;\t|")
            _append(self.normalize_csv_delimiter(getattr(sniffed, "delimiter", None)))
        except Exception:
            pass
        for delim in (",", ";", "\t", "|"):
            _append(delim)
        return candidates or [","]

    def read_csv_rows_with_delimiter(
        self,
        path: pathlib.Path,
        *,
        encoding: str,
        delimiter: str,
        row_limit: int = 500,
    ) -> Tuple[List[str], List[Dict[str, Any]]]:
        with open(path, "r", encoding=encoding, newline="") as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            header: List[str] = []
            for raw_header in reader:
                if not raw_header or not any((cell or "").strip() for cell in raw_header):
                    continue
                header = raw_header
                break
            if not header:
                return [], []

            normalized: List[str] = []
            seen_headers: Dict[str, int] = {}
            for idx_h, cell in enumerate(header):
                name = (cell or "").strip().lstrip("\ufeff")
                if not name:
                    name = f"column_{idx_h + 1}"
                if name in seen_headers:
                    seen_headers[name] += 1
                    name = f"{name}_{seen_headers[name]}"
                else:
                    seen_headers[name] = 0
                normalized.append(name)

            columns = normalized[:]
            local_items: List[Dict[str, Any]] = []
            for row in reader:
                if not row or not any(
                    (val.strip() if isinstance(val, str) else str(val or "").strip()) for val in row
                ):
                    continue
                while len(columns) < len(row):
                    columns.append(f"column_{len(columns) + 1}")
                record: Dict[str, Any] = {}
                for idx_col, value in enumerate(row):
                    key = columns[idx_col]
                    clean = value.strip() if isinstance(value, str) else str(value or "").strip()
                    record[key] = clean
                if any(record.values()):
                    local_items.append(record)
                if len(local_items) >= max(1, int(row_limit)):
                    break
        return columns, local_items

    def score_csv_rows(self, columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> float:
        if not columns or not rows:
            return -1.0
        col_count = max(1, len(columns))
        row_count = len(rows)
        non_empty_counts: List[int] = []
        price_like_rows = 0
        collapsed_blob_hits = 0
        for row in rows:
            values = [str(v or "").strip() for v in row.values()]
            non_empty = [v for v in values if v]
            non_empty_counts.append(len(non_empty))
            if any(self.has_price_digits(v) for v in non_empty):
                price_like_rows += 1
            if col_count == 1:
                first = values[0] if values else ""
                if any(token in first for token in (",", ";", "\t", "|")):
                    collapsed_blob_hits += 1

        avg_non_empty = (sum(non_empty_counts) / len(non_empty_counts)) if non_empty_counts else 0.0
        multi_field_ratio = (
            sum(1 for val in non_empty_counts if val >= 2) / len(non_empty_counts)
            if non_empty_counts
            else 0.0
        )
        price_ratio = price_like_rows / max(1, row_count)
        score = (
            (col_count * 6.0)
            + (avg_non_empty * 3.0)
            + (multi_field_ratio * 20.0)
            + (price_ratio * 15.0)
            + (min(row_count, 500) / 25.0)
        )
        if col_count == 1:
            blob_ratio = collapsed_blob_hits / max(1, row_count)
            score -= 50.0 + (blob_ratio * 40.0)
        return score

    def read_csv_rows_best(
        self,
        path: pathlib.Path,
        *,
        encoding: str,
        delimiter: Any,
        row_limit: int = 500,
    ) -> List[Dict[str, Any]]:
        try:
            with open(path, "r", encoding=encoding, newline="") as fh:
                sample = fh.read(4096)
        except Exception:
            sample = ""

        best_rows: List[Dict[str, Any]] = []
        best_score = float("-inf")
        for cand_delim in self.csv_delimiter_candidates(sample, delimiter):
            try:
                columns, rows = self.read_csv_rows_with_delimiter(
                    path,
                    encoding=encoding,
                    delimiter=cand_delim,
                    row_limit=row_limit,
                )
            except UnicodeDecodeError:
                raise
            except Exception:
                continue
            if not rows:
                continue
            score = self.score_csv_rows(columns, rows)
            if score > best_score:
                best_score = score
                best_rows = rows
        return best_rows

    def normalize_catalog_item(self, record: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
        normalized = dict(record)
        for target, source in mapping.items():
            if not source:
                continue
            if target in normalized and str(normalized[target]).strip():
                continue
            value = record.get(source)
            if value is None:
                continue
            normalized[target] = value

        def _ensure_title() -> None:
            title_candidates = [normalized.get("title"), normalized.get("name")]
            for candidate in title_candidates:
                if candidate and str(candidate).strip():
                    normalized.setdefault("name", candidate)
                    if not str(normalized.get("title") or "").strip():
                        normalized["title"] = candidate
                    return

            for key, value in record.items():
                if key in {"price", mapping.get("price", "")}:
                    continue
                text = str(value or "").strip()
                if len(text) >= 3 and not text.isdigit():
                    normalized.setdefault("title", text)
                    normalized.setdefault("name", text)
                    return

        def _ensure_price() -> None:
            current = normalized.get("price")
            if current and self.has_price_digits(current):
                return
            if current and isinstance(current, str) and current.strip():
                digits = re.sub(r"\D", "", current)
                if digits and len(digits) >= 4:
                    return

            preferred_columns = [mapping.get("price")]
            for key in record:
                if key in preferred_columns:
                    preferred_columns.append(key)
            seen = set(filter(None, preferred_columns))
            for key in preferred_columns:
                if not key:
                    continue
                text = str(record.get(key) or "").strip()
                if self.has_price_digits(text):
                    normalized["price"] = text
                    return

            for key, value in record.items():
                if key in seen:
                    continue
                text = str(value or "").strip()
                if self.has_price_digits(text):
                    normalized["price"] = text
                    return

        def _canonical_object_type_value(raw_value: Any) -> str:
            low = self.deps.normalize_text(raw_value)
            if not low:
                return ""
            is_apartment = bool(re.search(r"(?iu)\b(apartment|flat|квартир\w*|кв\.)\b", low))
            is_house = bool(re.search(r"(?iu)\b(house|home|частн\w*|коттедж\w*|дом\w*)\b", low))
            if is_apartment and not is_house:
                return "apartment"
            if is_house and not is_apartment:
                return "house"
            return ""

        def _ensure_object_type() -> None:
            direct = _canonical_object_type_value(normalized.get("object_type"))
            if direct:
                normalized["object_type"] = direct
                return

            probe_keys = (
                "object_type",
                "object",
                "target",
                "usage",
                "назначение",
                "тип помещения",
                "тип помещения/объекта",
                "тип объекта",
                "помещение",
            )
            for key in probe_keys:
                val = normalized.get(key)
                kind = _canonical_object_type_value(val)
                if kind:
                    normalized["object_type"] = kind
                    return
                val = record.get(key)
                kind = _canonical_object_type_value(val)
                if kind:
                    normalized["object_type"] = kind
                    return

        _ensure_title()
        _ensure_price()
        _ensure_object_type()
        return normalized

    def normalize_catalog_items(
        self,
        items: List[Dict[str, Any]],
        meta: Dict[str, Any] | Any,
    ) -> List[Dict[str, Any]]:
        if not items:
            return items
        meta_dict = meta if isinstance(meta, dict) else {}
        mapping = self.prepare_field_mapping(meta_dict, items)
        if not mapping:
            return [self.normalize_catalog_item(record, {}) for record in items]
        return [self.normalize_catalog_item(record, mapping) for record in items]
