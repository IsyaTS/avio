from __future__ import annotations

"""Machine-learning driven PDF catalog extractor using PaddleOCR PP-Structure.

The extractor renders PDF pages to PNG via `pypdfium2` and feeds them to
PaddleOCR's PP-Structure pipeline (Apache-2.0) to obtain layout blocks, tables,
and key-value candidates. Documentation references:

* PaddleOCR / PP-Structure: https://github.com/PaddlePaddle/PaddleOCR/tree/release/2.7/ppstructure
* pdfplumber (MIT) fallback extraction: https://github.com/jsvine/pdfplumber
"""

from dataclasses import dataclass, field
import io
import logging
import re
import statistics
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

try:  # optional heavy deps guarded for import-time friendliness
    import numpy as np  # type: ignore
except Exception:  # pragma: no cover - numpy shipped with PaddleOCR, but fallback to None
    np = None  # type: ignore

try:
    import pypdfium2 as pdfium  # type: ignore
except Exception:  # pragma: no cover - ensured via requirements but keep guard
    pdfium = None  # type: ignore

try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None  # type: ignore

try:
    from paddleocr import PPStructure  # type: ignore
except Exception:  # pragma: no cover - PaddleOCR optional at runtime
    PPStructure = None  # type: ignore

from PIL import Image
from rapidfuzz import fuzz  # type: ignore

from .text_normalize import (
    collapse_spaces,
    normalize_unicode_nfkc,
    strip_confusables,
    unify_dashes_and_decimals,
)

__all__ = ["MLCatalogExtractor", "MLPipelineResult", "normalize_price_candidate"]

logger = logging.getLogger(__name__)

PRICE_PATTERN = re.compile(r"(\d[\d\s\u00A0\u202F.,]{2,})")


def normalize_price_candidate(raw: str | None, page_num: int | None = None) -> str:
    """Normalize price to digits-only string if it looks like a valid amount.

    Prices must be >= 1000 and must not simply match the page number to avoid
    false positives from headings like "Страница 2".
    """

    if not raw:
        return ""
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return ""
    try:
        value = int(digits)
    except Exception:
        return ""
    if value <= 999:
        return ""
    if page_num and value == page_num:
        return ""
    return digits


@dataclass
class MLPipelineResult:
    """Structured result of ML-based extraction."""

    items: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    missing_pages: List[int] = field(default_factory=list)
    fallback_images: Dict[int, bytes] = field(default_factory=dict)


class MLCatalogExtractor:
    """Extract catalog-like items from PDF using PP-Structure + heuristic grouping."""

    def __init__(
        self,
        *,
        dpi: int = 220,
        lang: str = "en",
        use_gpu: bool = False,
    ) -> None:
        self.dpi = max(72, int(dpi))
        self.lang = lang
        self.use_gpu = use_gpu
        self._engine = None
        self._title_cache: List[str] = []

    def extract(self, pdf_path: str) -> MLPipelineResult:
        if pdfium is None:
            raise RuntimeError("pypdfium2 is required for MLCatalogExtractor (pip install pypdfium2).")

        doc = pdfium.PdfDocument(pdf_path)
        pages_total = len(doc)
        plumber_doc = pdfplumber.open(pdf_path) if pdfplumber is not None else None
        fallback_images: Dict[int, bytes] = {}
        missing_pages: List[int] = []
        collected: List[Dict[str, Any]] = []
        price_values: List[int] = []
        low_price_hits = 0
        table_pages = 0
        self._title_cache = []

        try:
            for page_index in range(pages_total):
                page_num = page_index + 1
                image = self._render_page(doc, page_index)
                layout_blocks = self._analyze_layout(
                    image=image,
                    plumber_doc=plumber_doc,
                    page_index=page_index,
                )
                page_items, page_has_table = self._items_from_blocks(layout_blocks, page_num)
                if page_has_table:
                    table_pages += 1
                if not page_items:
                    missing_pages.append(page_num)
                    fallback_images[page_num] = self._image_bytes(image)
                    continue
                for item in page_items:
                    price_norm = normalize_price_candidate(item.get("price"), page_num)
                    if not price_norm:
                        continue
                    item["price"] = price_norm
                    price_value = int(price_norm)
                    price_values.append(price_value)
                    if price_value <= 999:
                        low_price_hits += 1
                    collected.append(item)
        finally:
            if plumber_doc is not None:
                plumber_doc.close()
            doc.close()

        metrics = self._build_metrics(
            items=collected,
            pages_total=pages_total,
            pages_missing=missing_pages,
            table_pages=table_pages,
            price_values=price_values,
            low_price_hits=low_price_hits,
        )
        return MLPipelineResult(
            items=collected,
            metrics=metrics,
            missing_pages=missing_pages,
            fallback_images=fallback_images,
        )

    def _render_page(self, document: "pdfium.PdfDocument", page_index: int) -> Image.Image:
        page = document.get_page(page_index)
        scale = self.dpi / 72.0
        bitmap = page.render(scale=scale)
        image = bitmap.to_pil()
        page.close()
        return image.convert("RGB")

    def _analyze_layout(
        self,
        *,
        image: Image.Image,
        plumber_doc: "pdfplumber.PDF" | None,
        page_index: int,
    ) -> List[Dict[str, Any]]:
        if PPStructure is None or np is None:
            return self._fallback_layout(plumber_doc, page_index)

        if self._engine is None:
            try:
                self._engine = PPStructure(  # type: ignore[call-arg]
                    show_log=False,
                    lang=self.lang,
                    layout=True,
                    table=True,
                    use_gpu=self.use_gpu,
                )
            except Exception as exc:  # pragma: no cover - initialization failure
                logger.warning("ppstructure_init_failed", exc_info=exc)
                self._engine = None
                return self._fallback_layout(plumber_doc, page_index)

        np_image = np.array(image)[:, :, ::-1]  # RGB -> BGR
        try:
            return self._engine(np_image)  # type: ignore[misc]
        except Exception as exc:  # pragma: no cover - inference failure
            logger.warning("ppstructure_inference_failed", exc_info=exc)
            return self._fallback_layout(plumber_doc, page_index)

    def _fallback_layout(self, plumber_doc: "pdfplumber.PDF" | None, page_index: int) -> List[Dict[str, Any]]:
        if plumber_doc is None:
            return []
        try:
            raw_text = plumber_doc.pages[page_index].extract_text() or ""
        except Exception:
            raw_text = ""
        lines = [
            collapse_spaces(line)
            for line in raw_text.splitlines()
            if collapse_spaces(line)
        ]
        if not lines:
            return []
        return [{"type": "text", "lines": lines}]

    def _items_from_blocks(
        self,
        blocks: Sequence[Mapping[str, Any]],
        page_num: int,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        results: List[Dict[str, Any]] = []
        has_table = False

        for block in blocks or []:
            block_type = str(block.get("type") or "text").lower()
            if block_type == "table":
                has_table = True
                for row in self._rows_from_block(block):
                    row_text = " ".join(row)
                    price_candidate = normalize_price_candidate(row_text, page_num)
                    if not price_candidate:
                        continue
                    title = self._select_title(row) or f"Table row {len(results) + 1}"
                    attrs = {
                        f"column_{idx+1}": value
                        for idx, value in enumerate(row)
                        if value
                    }
                    attrs.pop("column_1", None)
                    item = self._compose_item(
                        title=title,
                        price=price_candidate,
                        page_num=page_num,
                        attrs=attrs,
                    )
                    results.append(item)
                continue

            lines = self._block_lines(block)
            text_blob = "\n".join(lines)
            price_candidate = normalize_price_candidate(text_blob, page_num)
            if not price_candidate:
                continue
            attrs = self._extract_attributes(lines)
            title = self._select_title(lines)
            item = self._compose_item(
                title=title,
                price=price_candidate,
                page_num=page_num,
                attrs=attrs,
            )
            results.append(item)

        return results, has_table

    def _rows_from_block(self, block: Mapping[str, Any]) -> List[List[str]]:
        res = block.get("res")
        rows: List[List[str]] = []
        if isinstance(res, list):
            for entry in res:
                if isinstance(entry, dict) and "text" in entry:
                    text = self._normalize_text(entry.get("text"))
                    if text:
                        rows.append([text])
                elif isinstance(entry, list):
                    normalized = [self._normalize_text(val) for val in entry if val]
                    if normalized:
                        rows.append(normalized)
        elif isinstance(res, dict):
            html = res.get("html")
            if isinstance(html, str):
                for raw_row in re.split(r"</tr>", html, flags=re.IGNORECASE):
                    cells = re.findall(r">(.*?)<", raw_row)
                    normalized = [self._normalize_text(cell) for cell in cells if self._normalize_text(cell)]
                    if normalized:
                        rows.append(normalized)
            elif "cells" in res and isinstance(res["cells"], list):
                for cell_row in res["cells"]:
                    normalized = [self._normalize_text(cell.get("text")) if isinstance(cell, dict) else "" for cell in cell_row]
                    normalized = [item for item in normalized if item]
                    if normalized:
                        rows.append(normalized)
        if not rows:
            text = self._block_text(block)
            for line in text.splitlines():
                columns = [segment for segment in re.split(r"\s{2,}", line) if segment]
                if columns:
                    rows.append([self._normalize_text(col) for col in columns if self._normalize_text(col)])
        return rows

    def _block_lines(self, block: Mapping[str, Any]) -> List[str]:
        if "lines" in block and isinstance(block["lines"], list):
            return [self._normalize_text(line) for line in block["lines"] if self._normalize_text(line)]
        if "text" in block and isinstance(block["text"], str):
            return [line for line in self._block_text(block).splitlines() if line]
        return [line for line in self._block_text(block).splitlines() if line]

    def _block_text(self, block: Mapping[str, Any]) -> str:
        res = block.get("res")
        parts: List[str] = []
        if isinstance(res, list):
            for entry in res:
                if isinstance(entry, dict) and "text" in entry:
                    normalized = self._normalize_text(entry.get("text"))
                    if normalized:
                        parts.append(normalized)
                elif isinstance(entry, str):
                    normalized = self._normalize_text(entry)
                    if normalized:
                        parts.append(normalized)
        elif isinstance(res, dict):
            if "text" in res and isinstance(res["text"], str):
                parts.append(self._normalize_text(res["text"]))
            if "html" in res and isinstance(res["html"], str):
                html_text = re.sub(r"<[^>]+>", " ", res["html"])
                parts.append(self._normalize_text(html_text))
        elif isinstance(res, str):
            parts.append(self._normalize_text(res))
        if "text" in block and isinstance(block["text"], str):
            parts.append(self._normalize_text(block["text"]))
        deduped = [item for item in parts if item]
        return "\n".join(deduped)

    def _normalize_text(self, text: str | None) -> str:
        if not text:
            return ""
        value = normalize_unicode_nfkc(text)
        value = strip_confusables(value)
        value = unify_dashes_and_decimals(value)
        value = collapse_spaces(value)
        return value

    def _select_title(self, lines: Sequence[str]) -> str:
        cleaned = [line for line in lines if len(line) >= 3]
        if not cleaned:
            return ""
        # Prefer multi-word lines
        cleaned.sort(key=lambda text: (text.isupper(), -len(text)))
        candidate = cleaned[0]
        for existing in self._title_cache:
            if fuzz.partial_ratio(existing.lower(), candidate.lower()) >= 92:
                candidate = f"{candidate} ({len(self._title_cache) + 1})"
                break
        self._title_cache.append(candidate)
        return candidate

    def _extract_attributes(self, lines: Sequence[str]) -> Dict[str, str]:
        attrs: Dict[str, str] = {}
        for line in lines:
            match = re.match(r"([^:–—-]+)\s*[:–—-]\s*(.+)", line)
            if not match:
                continue
            key = self._normalize_text(match.group(1))
            value = self._normalize_text(match.group(2))
            if not key or not value:
                continue
            key_slug = re.sub(r"\s+", "_", key.lower())
            attrs[key_slug] = value
        return attrs

    def _compose_item(self, *, title: str, price: str, page_num: int, attrs: Mapping[str, Any]) -> Dict[str, Any]:
        normalized_title = title or f"Item {page_num}"
        item: Dict[str, Any] = {
            "title": normalized_title,
            "price": price,
            "page": str(page_num),
        }
        for key, value in attrs.items():
            if not value:
                continue
            item[key] = value
        return item

    def _image_bytes(self, image: Image.Image) -> bytes:
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()

    def _build_metrics(
        self,
        *,
        items: Sequence[Mapping[str, Any]],
        pages_total: int,
        pages_missing: Sequence[int],
        table_pages: int,
        price_values: Sequence[int],
        low_price_hits: int,
    ) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {
            "items_found": len(items),
            "pages_total": pages_total,
            "pages_skipped_no_price": len(pages_missing),
            "table_pages": table_pages,
            "median_price": statistics.median(price_values) if price_values else None,
            "low_price_rate": (low_price_hits / len(price_values)) if price_values else 0.0,
        }
        return metrics
