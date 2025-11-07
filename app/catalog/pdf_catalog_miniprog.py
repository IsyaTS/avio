from __future__ import annotations

"""Мини-пайплайн PDF → CSV.

Шаги:
    * pdfminer.six + LAParams для извлечения строк/слов (см. https://pdfminersix.readthedocs.io/)
    * pdfplumber для таблиц (https://github.com/jsvine/pdfplumber)
    * pypdfium2 для рендера при OCR-фолбэке (https://pypdfium2.readthedocs.io/)
    * opt: ocrmypdf + Tesseract + Ghostscript для внешнего OCR слоя.
"""

import logging
import os
import re
import shutil
import statistics
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

import pdfplumber
from pdfminer.high_level import extract_pages
from pdfminer.layout import LAParams, LTTextBoxHorizontal, LTTextContainer, LTTextLineHorizontal

try:
    import camelot  # type: ignore
except Exception:  # pragma: no cover - Camelot optional
    camelot = None

try:
    import pypdfium2 as pdfium  # type: ignore
except Exception:  # pragma: no cover
    pdfium = None  # type: ignore

from .text_normalize import (
    collapse_spaces,
    normalize_unicode_nfkc,
    strip_confusables,
    unify_dashes_and_decimals,
)

logger = logging.getLogger(__name__)
logging.getLogger("pypdf").setLevel(logging.ERROR)
logging.getLogger("pypdf.generic").setLevel(logging.ERROR)
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("pdfminer.pdfinterp").setLevel(logging.ERROR)

PRICE_REGEX = re.compile(r"(?:\d{1,3}(?:[\s\u00A0\u202F\u2009]\d{3})+|\d{4,})(?:[.,]\d{1,2})?")
KV_REGEX = re.compile(r"(?P<key>[^:–—•=]{2,})\s*[:=–—•]\s*(?P<value>.+)")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _normalize_text(value: str | None) -> str:
    text = normalize_unicode_nfkc(value or "")
    text = strip_confusables(text)
    text = unify_dashes_and_decimals(text)
    text = collapse_spaces(text)
    return text


@dataclass
class Line:
    text: str
    top: float
    bottom: float
    font_size: float


class CatalogMiniPipeline:
    """Chain-of-responsibility для Tables → KV → OCR."""

    def __init__(
        self,
        table_engine: str | None = None,
        render_dpi: int | None = None,
        ocr_fallback: bool | None = None,
    ) -> None:
        self.table_engine = (table_engine or os.getenv("PDF_TABLES_ENGINE") or "plumber").lower()
        if self.table_engine not in {"plumber", "camelot"}:
            self.table_engine = "plumber"
        self.render_dpi = render_dpi or _env_int("PDF_RENDER_DPI", 220)
        self.render_dpi = max(72, min(self.render_dpi, 600))
        self.ocr_fallback = bool(ocr_fallback if ocr_fallback is not None else _env_bool("PDF_OCR_FALLBACK", False))
        self.metrics: Dict[str, Any] = {}

    def extract_items(self, pdf_path: str) -> List[Dict[str, Any]]:
        base_items = self._run_single_pass(pdf_path)
        if base_items or not self.ocr_fallback:
            return base_items

        ocr_path = self._ocr_fallback_if_needed(pdf_path)
        if not ocr_path:
            return base_items
        try:
            return self._run_single_pass(ocr_path)
        finally:
            try:
                os.remove(ocr_path)
            except OSError:
                pass

    def _run_single_pass(self, pdf_path: str) -> List[Dict[str, Any]]:
        lines_by_page = self._load_words_laparams(pdf_path)
        table_items, table_pages = self._extract_from_tables(pdf_path)
        kv_items = self._build_blocks(lines_by_page)
        items = table_items + kv_items

        pages_total = len(lines_by_page)
        pages_with_price = {int(item.get("page", 0)) for item in items if item.get("price")}
        pages_skipped = pages_total - len(pages_with_price)
        digits = [int(item["price"]) for item in items if item.get("price", "").isdigit()]
        low_price_rate = 0.0
        if digits:
            low_hits = sum(1 for num in digits if num <= 999)
            low_price_rate = low_hits / len(digits)

        self.metrics = {
            "items_found": len(items),
            "pages_total": pages_total,
            "pages_skipped_no_price": max(pages_skipped, 0),
            "table_pages": len(table_pages),
            "median_price": statistics.median(digits) if digits else None,
            "low_price_rate": low_price_rate,
        }
        return items

    # ------------------------------------------------------------------
    # pdfminer (LAParams) → строки
    # ------------------------------------------------------------------
    def _load_words_laparams(self, pdf_path: str) -> List[Dict[str, Any]]:
        laparams = LAParams(
            char_margin=2.0,
            word_margin=0.1,
            line_margin=0.3,
        )
        pages: List[Dict[str, Any]] = []
        try:
            iterator = extract_pages(pdf_path, laparams=laparams)
        except Exception as exc:  # pragma: no cover
            logger.warning("pdfminer_extract_failed", exc_info=exc)
            return pages

        idx = 0
        for layout in iterator:
            idx += 1
            lines: List[Line] = []
            try:
                for element in layout:
                    if isinstance(element, LTTextLineHorizontal):
                        lines.extend(self._lines_from_text_container(element))
                    elif isinstance(element, (LTTextBoxHorizontal, LTTextContainer)):
                        for child in element:
                            if isinstance(child, LTTextLineHorizontal):
                                lines.extend(self._lines_from_text_container(child))
            except Exception as exc:
                logger.warning("laparams_page_failed", extra={"page": idx}, exc_info=exc)
                lines = []
            pages.append({"page": idx, "lines": lines})
        return pages

    def _lines_from_text_container(self, container: LTTextContainer | LTTextLineHorizontal) -> List[Line]:
        lines: List[Line] = []
        targets: List[LTTextLineHorizontal] = []
        if isinstance(container, LTTextLineHorizontal):
            targets.append(container)
        else:
            for child in container:
                if isinstance(child, LTTextLineHorizontal):
                    targets.append(child)
        for line in targets:
            if not hasattr(line, "get_text"):
                continue
            raw = line.get_text()
            cleaned = _normalize_text(raw)
            if not cleaned:
                continue
            font_sizes = [obj.size for obj in line if hasattr(obj, "size")]
            avg_size = sum(font_sizes) / len(font_sizes) if font_sizes else 10.0
            lines.append(Line(text=cleaned, top=line.y1, bottom=line.y0, font_size=avg_size))
        return lines

    # ------------------------------------------------------------------
    # KV-блоки и эвристики названия
    # ------------------------------------------------------------------
    def _build_blocks(self, pages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for entry in pages:
            page_num = int(entry.get("page") or 0)
            lines: List[Line] = entry.get("lines", [])
            if not lines:
                continue
            for idx, line in enumerate(lines):
                price = self._detect_price(line.text, page_num)
                if not price:
                    continue
                title = self._select_title(lines, idx)
                attrs = self._collect_attrs(lines, idx)
                item = {
                    "title": title or line.text[:120],
                    "price": price,
                    "page": str(page_num),
                }
                item.update(attrs)
                items.append(item)
        return items

    def _select_title(self, lines: Sequence[Line], price_idx: int) -> str:
        best: Line | None = None
        for offset in range(0, 4):
            idx = price_idx - offset
            if idx < 0:
                break
            candidate = lines[idx]
            if not candidate.text.strip():
                continue
            if best is None or candidate.font_size >= best.font_size:
                best = candidate
        return best.text if best else ""

    def _collect_attrs(self, lines: Sequence[Line], price_idx: int) -> Dict[str, str]:
        attrs: Dict[str, str] = {}
        target = lines[price_idx]
        window = max(abs(target.bottom - target.top), 16.0) * 5
        for line in lines:
            if abs(line.top - target.top) > window:
                continue
            match = KV_REGEX.search(line.text)
            if not match:
                continue
            key = self._slug(match.group("key"))
            value = _normalize_text(match.group("value"))
            if key and value and key not in attrs:
                attrs[key] = value
        return attrs

    def _detect_price(self, text: str, page_num: int) -> str:
        for match in PRICE_REGEX.finditer(text):
            digits = re.sub(r"[^\d]", "", match.group(0))
            if not digits:
                continue
            try:
                value = int(digits)
            except ValueError:
                continue
            if value <= 999:
                continue
            if page_num and value == page_num:
                continue
            return digits
        return ""

    # ------------------------------------------------------------------
    # Таблицы
    # ------------------------------------------------------------------
    def _extract_from_tables(self, pdf_path: str) -> Tuple[List[Dict[str, Any]], List[int]]:
        table_items: List[Dict[str, Any]] = []
        table_pages: List[int] = []

        if self.table_engine == "camelot" and camelot is not None:
            table_items, table_pages = self._extract_tables_camelot(pdf_path)
        else:
            table_items, table_pages = self._extract_tables_plumber(pdf_path)
        return table_items, table_pages

    def _extract_tables_plumber(self, pdf_path: str) -> Tuple[List[Dict[str, Any]], List[int]]:
        items: List[Dict[str, Any]] = []
        pages_with_tables: List[int] = []
        with pdfplumber.open(pdf_path) as doc:
            for idx, page in enumerate(doc.pages, start=1):
                try:
                    tables = page.extract_tables() or []
                except Exception as exc:
                    logger.warning("table_extract_plumber_failed", extra={"page": idx}, exc_info=exc)
                    continue
                normalized_rows = []
                for table in tables:
                    normalized_rows.extend(
                        [
                            [_normalize_text(cell) for cell in (row or []) if _normalize_text(cell)]
                            for row in table or []
                        ]
                    )
                for row in normalized_rows:
                    if not row:
                        continue
                    price = self._detect_price(" ".join(row), idx)
                    if not price:
                        continue
                    attrs = {f"column_{i+1}": cell for i, cell in enumerate(row)}
                    attrs.pop("column_1", None)
                    items.append({"title": row[0], "price": price, "page": str(idx), **attrs})
                    if idx not in pages_with_tables:
                        pages_with_tables.append(idx)
        return items, pages_with_tables

    def _extract_tables_camelot(self, pdf_path: str) -> Tuple[List[Dict[str, Any]], List[int]]:
        items: List[Dict[str, Any]] = []
        pages_with_tables: List[int] = []
        if camelot is None:  # pragma: no cover
            return items, pages_with_tables
        for flavor in ("lattice", "stream"):
            try:
                tables = camelot.read_pdf(pdf_path, pages="all", flavor=flavor)
            except Exception as exc:
                logger.warning("camelot_extract_failed", exc_info=exc)
                continue
            for table in tables:
                page_num = getattr(table, "page", None)
                for row in table.data:
                    normalized = [_normalize_text(cell) for cell in row if _normalize_text(cell)]
                    if not normalized:
                        continue
                    price = self._detect_price(" ".join(normalized), int(page_num or 0))
                    if not price:
                        continue
                    attrs = {f"column_{i+1}": cell for i, cell in enumerate(normalized)}
                    attrs.pop("column_1", None)
                    items.append(
                        {
                            "title": normalized[0],
                            "price": price,
                            "page": str(page_num or 0),
                            **attrs,
                        }
                    )
                    if page_num and page_num not in pages_with_tables:
                        pages_with_tables.append(int(page_num))
            if items:
                break
        return items, pages_with_tables

    # ------------------------------------------------------------------
    # OCR fallback (ocrmypdf + pypdfium2 rendering pre-check)
    # ------------------------------------------------------------------
    def _ocr_fallback_if_needed(self, pdf_path: str) -> str | None:
        if not self.ocr_fallback:
            return None
        if shutil.which("ocrmypdf") is None:
            logger.warning("ocrmypdf_missing")
            return None
        if pdfium is None:
            logger.warning("pypdfium_missing")
            return None

        tmp_dir = tempfile.mkdtemp(prefix="catalog_ocr_")
        target_pdf = os.path.join(tmp_dir, "ocr.pdf")

        # Быстрая проверка: попробуем отрендерить страницу, если pdfium падает — смысл OCR теряется.
        try:
            doc = pdfium.PdfDocument(pdf_path)
            page = doc.get_page(0)
            bitmap = page.render(scale=self.render_dpi / 72.0)
            _img = bitmap.to_pil()
            page.close()
            doc.close()
        except Exception as exc:
            logger.warning("pdfium_render_failed", exc_info=exc)
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return None

        cmd = [
            "ocrmypdf",
            "--skip-text",
            "--force-ocr",
            "--redo-ocr",
            "--language",
            "rus+eng",
            pdf_path,
            target_pdf,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        except FileNotFoundError:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return None
        if result.returncode != 0:
            logger.warning("ocrmypdf_failed", extra={"stdout": result.stdout[-400:], "stderr": result.stderr[-400:]})
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return None
        return target_pdf

    # ------------------------------------------------------------------
    def _slug(self, value: str) -> str:
        normalized = _normalize_text(value).lower()
        normalized = re.sub(r"[^0-9a-zа-яё]+", "_", normalized)
        return normalized.strip("_")
