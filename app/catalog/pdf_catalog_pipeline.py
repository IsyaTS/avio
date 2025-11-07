from __future__ import annotations

"""Unified PDF → CSV catalog pipeline with table, KV, and OCR strategies.

Общая схема:
    1. Таблицы (pdfplumber по умолчанию, Camelot опционально).
    2. Блочный текст + KV (на основе слов с координатами).
    3. OCR-фолбэк через PaddleOCR PP-Structure.

Публичный интерфейс — класс CatalogPipeline с методом `extract_items`.
Внутренне используется стратегия/цепочка Responsibility: каждая стадия
пытается выдать юниты, дальше включается следующая.
"""

from dataclasses import dataclass
import logging
import math
import os
import statistics
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
import re

try:  # pdfplumber & pdfminer (MIT)
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None  # type: ignore

try:  # Camelot (Apache-2.0)
    import camelot  # type: ignore
except Exception:  # pragma: no cover
    camelot = None  # type: ignore

try:  # PaddleOCR PP-Structure (Apache-2.0)
    from paddleocr import PPStructure  # type: ignore
except Exception:  # pragma: no cover
    PPStructure = None  # type: ignore

try:
    import numpy as np  # type: ignore
except Exception:  # pragma: no cover
    np = None  # type: ignore

try:
    import pypdfium2 as pdfium  # type: ignore
except Exception:  # pragma: no cover
    pdfium = None  # type: ignore

from PIL import Image

from .text_normalize import (
    collapse_spaces,
    normalize_unicode_nfkc,
    strip_confusables,
    unify_dashes_and_decimals,
)

logger = logging.getLogger(__name__)

# Разрешаем пробелы, в т.ч. тонкие (U+00A0/U+202F/U+2009), и опциональную дробь.
PRICE_REGEX = re.compile(
    r"(?:\d{1,3}(?:[\s\u00A0\u202F\u2009]\d{3})+|\d{4,})(?:[.,]\d{1,2})?"
)
KV_REGEX = re.compile(r"(?P<key>[^:–—•=]{2,})\s*[:=–—•]\s*(?P<value>.+)")


def _env_table_engine() -> str:
    candidate = (os.getenv("PDF_TABLES_ENGINE") or "plumber").strip().lower()
    return candidate if candidate in {"plumber", "camelot"} else "plumber"


def _env_render_dpi() -> int:
    raw = os.getenv("PDF_RENDER_DPI") or "220"
    try:
        value = int(raw)
    except ValueError:
        value = 220
    return max(72, min(600, value))


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = normalize_unicode_nfkc(str(value))
    text = strip_confusables(text)
    text = unify_dashes_and_decimals(text)
    text = collapse_spaces(text)
    return text


def _slug_key(value: str) -> str:
    normalized = _normalize_text(value).lower()
    normalized = re.sub(r"[^0-9a-zа-яё]+", "_", normalized)
    return normalized.strip("_")


@dataclass
class Line:
    text: str
    top: float
    bottom: float
    font_size: float


class CatalogPipeline:
    """Единый пайплайн для PDF с таблицами, KV и OCR фолбэком."""

    def __init__(
        self,
        *,
        table_engine: str | None = None,
        render_dpi: int | None = None,
    ) -> None:
        if pdfplumber is None:
            raise RuntimeError("pdfplumber is required for CatalogPipeline (pip install pdfplumber).")

        self.table_engine = (table_engine or _env_table_engine()).lower()
        if self.table_engine not in {"plumber", "camelot"}:
            self.table_engine = "plumber"
        self.render_dpi = render_dpi or _env_render_dpi()
        self.metrics: Dict[str, Any] = {}
        self._ocr_engine = None

    def extract_items(self, pdf_path: str) -> List[Dict[str, Any]]:
        self.metrics = {}
        items: List[Dict[str, Any]] = []
        pages_skipped = 0
        table_pages = 0
        price_values: List[int] = []
        low_price_candidates = 0

        plumber_doc = pdfplumber.open(pdf_path)
        pdf_doc = pdfium.PdfDocument(pdf_path) if pdfium is not None else None

        try:
            total_pages = len(plumber_doc.pages)
            for idx, page in enumerate(plumber_doc.pages, start=1):
                page_items: List[Dict[str, Any]] = []

                table_items = self._extract_tables(pdf_path, page, idx)
                if table_items:
                    table_pages += 1
                    page_items.extend(table_items)

                if not page_items:
                    kv_items = self._extract_kv(page, idx)
                    page_items.extend(kv_items)

                if not page_items and pdf_doc is not None:
                    image = self._render_page(pdf_doc, idx - 1)
                    ocr_items = self._extract_ocr(image, idx)
                    page_items.extend(ocr_items)

                if not page_items:
                    pages_skipped += 1
                    continue

                for candidate in page_items:
                    raw_price = candidate.get("price")
                    normalized_price, is_low = self._normalize_price(raw_price, idx)
                    if is_low:
                        low_price_candidates += 1
                    if not normalized_price:
                        continue
                    candidate["price"] = normalized_price
                    price_values.append(int(normalized_price))
                    items.append(candidate)

            self.metrics = {
                "items_found": len(items),
                "pages_total": total_pages,
                "pages_skipped_no_price": pages_skipped,
                "table_pages": table_pages,
                "median_price": statistics.median(price_values) if price_values else None,
                "low_price_rate": (
                    low_price_candidates / max(low_price_candidates + len(price_values), 1)
                ),
            }
            return items
        finally:
            plumber_doc.close()
            if pdf_doc is not None:
                pdf_doc.close()

    # ------------------------------------------------------------------
    # Таблицы
    # ------------------------------------------------------------------
    def _extract_tables(self, pdf_path: str, page, page_num: int) -> List[Dict[str, Any]]:
        rows: List[List[str]] = []
        if self.table_engine == "camelot" and camelot is not None:
            rows.extend(self._extract_tables_camelot(pdf_path, page_num))
        else:
            rows.extend(self._extract_tables_plumber(page))
        return self._rows_to_items(rows, page_num)

    def _extract_tables_plumber(self, page) -> List[List[str]]:
        tables = []
        try:
            for table in page.extract_tables() or []:
                normalized = [
                    [
                        _normalize_text(cell)
                        for cell in (row or [])
                        if _normalize_text(cell)
                    ]
                    for row in table or []
                ]
                tables.extend([row for row in normalized if row])
        except Exception:
            pass
        return tables

    def _extract_tables_camelot(self, pdf_path: str, page_num: int) -> List[List[str]]:
        tables: List[List[str]] = []
        if camelot is None:
            return tables
        for flavor in ("lattice", "stream"):
            try:
                result = camelot.read_pdf(
                    pdf_path,
                    pages=str(page_num),
                    flavor=flavor,
                )
            except Exception:
                continue
            for table in result:
                rows = [
                    [_normalize_text(cell) for cell in row if _normalize_text(cell)]
                    for row in table.data
                ]
                tables.extend([row for row in rows if row])
            if tables:
                break
        return tables

    # ------------------------------------------------------------------
    # KV-стратегия
    # ------------------------------------------------------------------
    def _extract_kv(self, page, page_num: int) -> List[Dict[str, Any]]:
        words = page.extract_words(
            use_text_flow=True,
            extra_attrs=["size", "fontname", "upright"],
        )
        lines = self._build_lines(words)
        if not lines:
            text = _normalize_text(page.extract_text())
            if not text:
                return []
            lines = [
                Line(text=line, top=0.0, bottom=0.0, font_size=12.0)
                for line in text.splitlines()
                if line
            ]
        items: List[Dict[str, Any]] = []
        for idx, line in enumerate(lines):
            for match in PRICE_REGEX.finditer(line.text):
                snippet = match.group(0)
                title = self._select_title(lines, idx) or snippet
                attrs = self._collect_attrs_near_line(lines, idx)
                item = {
                    "title": title,
                    "price": snippet,
                    "page": str(page_num),
                }
                item.update(attrs)
                items.append(item)
        return items

    def _build_lines(self, words: Sequence[Mapping[str, Any]]) -> List[Line]:
        normalized_words = []
        for word in words or []:
            text = _normalize_text(word.get("text") or "")
            if not text:
                continue
            normalized_words.append(
                {
                    "text": text,
                    "top": float(word.get("top", 0.0)),
                    "bottom": float(word.get("bottom", 0.0)),
                    "size": float(word.get("size") or 0.0),
                }
            )
        if not normalized_words:
            return []
        normalized_words.sort(key=lambda w: (w["top"], w["text"]))
        lines: List[Line] = []
        current: List[Dict[str, Any]] = []
        last_top = None
        for word in normalized_words:
            if last_top is None:
                current.append(word)
                last_top = word["top"]
                continue
            if abs(word["top"] - last_top) <= 3.0:
                current.append(word)
                last_top = (last_top + word["top"]) / 2
            else:
                lines.append(self._merge_line(current))
                current = [word]
                last_top = word["top"]
        if current:
            lines.append(self._merge_line(current))
        lines.sort(key=lambda ln: ln.top)
        return lines

    def _merge_line(self, words: Sequence[Mapping[str, Any]]) -> Line:
        text = " ".join(word["text"] for word in words)
        top = min(word["top"] for word in words)
        bottom = max(word["bottom"] for word in words)
        avg_size = (
            sum(word.get("size", 0.0) for word in words) / max(len(words), 1)
        )
        return Line(text=text, top=top, bottom=bottom, font_size=avg_size)

    def _select_title(self, lines: Sequence[Line], price_idx: int) -> str:
        best_line: Optional[Line] = None
        for offset in range(0, 4):
            candidate_idx = price_idx - offset
            if candidate_idx < 0:
                break
            candidate = lines[candidate_idx]
            if not candidate.text.strip():
                continue
            if best_line is None or candidate.font_size > best_line.font_size:
                best_line = candidate
        return best_line.text if best_line else ""

    def _collect_attrs_near_line(self, lines: Sequence[Line], price_idx: int) -> Dict[str, str]:
        attrs: Dict[str, str] = {}
        target_line = lines[price_idx]
        window = max(abs(target_line.bottom - target_line.top), 20.0) * 4
        for line in lines:
            if abs(line.top - target_line.top) > window:
                continue
            match = KV_REGEX.search(line.text)
            if not match:
                continue
            key = _slug_key(match.group("key"))
            value = _normalize_text(match.group("value"))
            if key and value and key not in attrs:
                attrs[key] = value
        return attrs

    # ------------------------------------------------------------------
    # OCR фолбэк
    # ------------------------------------------------------------------
    def _extract_ocr(self, image: Image.Image, page_num: int) -> List[Dict[str, Any]]:
        if PPStructure is None or np is None:
            return []
        if self._ocr_engine is None:
            try:
                self._ocr_engine = PPStructure(show_log=False, lang="en", layout=True, table=True)
            except Exception as exc:
                logger.warning("ppstructure_init_failed", exc_info=exc)
                return []
        np_image = np.array(image)[:, :, ::-1]
        try:
            result = self._ocr_engine(np_image)
        except Exception as exc:  # pragma: no cover
            logger.warning("ppstructure_inference_failed", exc_info=exc)
            return []

        rows: List[List[str]] = []
        text_items: List[Dict[str, Any]] = []
        for block in result:
            block_type = str(block.get("type") or "").lower()
            if block_type == "table":
                rows.extend(self._parse_ocr_table(block))
            else:
                text = self._ocr_block_text(block)
                if text:
                    text_items.append({"text": text})

        items = self._rows_to_items(rows, page_num)
        for entry in text_items:
            match = PRICE_REGEX.search(entry["text"])
            if not match:
                continue
            attrs = self._attrs_from_text(entry["text"])
            items.append(
                {
                    "title": entry["text"].splitlines()[0][:120],
                    "price": match.group(0),
                    "page": str(page_num),
                    **attrs,
                }
            )
        return items

    def _parse_ocr_table(self, block: Mapping[str, Any]) -> List[List[str]]:
        rows: List[List[str]] = []
        res = block.get("res")
        if isinstance(res, list):
            for entry in res:
                text = _normalize_text(entry.get("text") if isinstance(entry, dict) else str(entry))
                if text:
                    rows.append([text])
        elif isinstance(res, dict):
            html = res.get("html")
            if isinstance(html, str):
                for raw_row in re.split(r"</tr>", html, flags=re.IGNORECASE):
                    cells = re.findall(r">(.*?)<", raw_row)
                    normalized = [_normalize_text(cell) for cell in cells if _normalize_text(cell)]
                    if normalized:
                        rows.append(normalized)
        return rows

    def _ocr_block_text(self, block: Mapping[str, Any]) -> str:
        res = block.get("res")
        if isinstance(res, list):
            parts = [
                _normalize_text(entry.get("text"))
                for entry in res
                if isinstance(entry, dict) and entry.get("text")
            ]
            return "\n".join([part for part in parts if part])
        if isinstance(res, dict) and isinstance(res.get("text"), str):
            return _normalize_text(res["text"])
        return ""

    def _attrs_from_text(self, text: str) -> Dict[str, str]:
        attrs: Dict[str, str] = {}
        for line in text.splitlines():
            match = KV_REGEX.search(line)
            if not match:
                continue
            key = _slug_key(match.group("key"))
            value = _normalize_text(match.group("value"))
            if key and value:
                attrs[key] = value
        return attrs

    def _render_page(self, pdf_doc: "pdfium.PdfDocument", page_index: int) -> Image.Image:
        page = pdf_doc.get_page(page_index)
        scale = self.render_dpi / 72.0
        bitmap = page.render(scale=scale)
        image = bitmap.to_pil()
        page.close()
        return image.convert("RGB")

    # ------------------------------------------------------------------
    # Утилиты
    # ------------------------------------------------------------------
    def _rows_to_items(self, rows: Sequence[Sequence[str]], page_num: int) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for row in rows:
            cleaned = [cell for cell in row if cell]
            if not cleaned:
                continue
            price_match = None
            for cell in cleaned:
                match = PRICE_REGEX.search(cell)
                if match:
                    price_match = match.group(0)
                    break
            if not price_match:
                continue
            title = cleaned[0]
            attrs = {
                f"column_{idx+1}": value
                for idx, value in enumerate(cleaned)
                if value
            }
            attrs.pop("column_1", None)
            items.append(
                {
                    "title": title,
                    "price": price_match,
                    "page": str(page_num),
                    **attrs,
                }
            )
        return items

    def _normalize_price(self, raw: Any, page_num: int) -> Tuple[str, bool]:
        text = _normalize_text(str(raw or ""))
        if not text:
            return "", False
        digits = re.sub(r"[^\d]", "", text)
        if not digits:
            return "", False
        try:
            value = int(digits)
        except ValueError:
            return "", False
        if value <= 999:
            return "", True
        if page_num and value == page_num:
            return "", True
        return digits, False
