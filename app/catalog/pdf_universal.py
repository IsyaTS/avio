from __future__ import annotations

"""Universal PDF extraction helpers.

The extractor relies on PyMuPDF (``page.get_text("dict")`` /
``get_text("words", sort=True)``) to obtain spans with coordinates and font
sizes so that we can rebuild text blocks geometrically. See
https://pymupdf.readthedocs.io/en/latest/textpage.html#get-text for the
structure of the returned dictionaries.

For more advanced layout-aware tuning we reference pdfminer.six LAParams
(`char_margin`, `word_margin`) under
https://pdfminersix.readthedocs.io/en/latest/reference/composable.html#laparams
as an alternative backend.
"""

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Literal, Sequence, Tuple
import collections
import logging
import re

from .text_normalize import (
    collapse_spaces,
    strip_confusables,
    unify_dashes_and_decimals,
)

try:  # pragma: no cover - optional dependency resolved at runtime
    import fitz  # type: ignore
except Exception:  # pragma: no cover
    fitz = None  # type: ignore

try:  # pragma: no cover - optional dependency resolved at runtime
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None  # type: ignore

try:  # pragma: no cover - optional dependency resolved at runtime
    import camelot  # type: ignore
except Exception:  # pragma: no cover
    camelot = None  # type: ignore


@dataclass
class Block:
    """Normalized text block produced from clustered PyMuPDF spans."""

    text: str
    bbox: Tuple[float, float, float, float]
    avg_font_size: float
    page_num: int


@dataclass
class TableBlock:
    """Lightweight representation of a detected table."""

    rows: List[List[str]]
    page_num: int
    bbox: Tuple[float, float, float, float] | None = None


@dataclass
class ExtractionStats:
    """Metrics captured during universal PDF extraction."""

    total_pages: int = 0
    low_confidence_pages: set[int] = field(default_factory=set)
    empty_pages: set[int] = field(default_factory=set)
    table_blocks: int = 0
    kv_blocks: int = 0
    price_hits: int = 0
    price_candidates: int = 0
    ocr_pages: int = 0

    @property
    def price_coverage(self) -> float:
        denom = max(self.price_candidates, 1)
        return self.price_hits / denom


class ExtractionResult(list):
    """List-like container returning items together with their stats."""

    def __init__(self, items: Sequence[dict], stats: ExtractionStats) -> None:
        super().__init__(items)
        self.stats = stats


class PdfUniversalExtractor:
    """Cluster PyMuPDF spans into Block objects keyed by page."""

    def __init__(self, *, min_chars_per_page: int = 35, gap_multiplier: float = 1.6) -> None:
        self.min_chars_per_page = min_chars_per_page
        self.gap_multiplier = gap_multiplier
        self.low_confidence_pages: set[int] = set()
        self.empty_pages: set[int] = set()
        self._page_char_counts: Dict[int, int] = {}
        self.total_pages: int = 0

    def extract(self, pdf_path: str) -> List[Block]:
        if fitz is None:  # pragma: no cover - guarded import
            raise RuntimeError("PyMuPDF is required for PdfUniversalExtractor (pip install PyMuPDF).")

        doc = fitz.open(pdf_path)
        self.total_pages = doc.page_count
        blocks: List[Block] = []
        try:
            for page_index in range(doc.page_count):
                page = doc.load_page(page_index)
                page_blocks = self._extract_page_blocks(page, page_index + 1)
                blocks.extend(page_blocks)
        finally:
            doc.close()
        return blocks

    def _extract_page_blocks(self, page: "fitz.Page", page_num: int) -> List[Block]:
        page_dict = page.get_text("dict")
        line_entries: List[Block] = []
        char_count = 0

        for block in page_dict.get("blocks", []):
            for line in block.get("lines", []):
                spans = line.get("spans", [])
                if not spans:
                    continue
                parts: List[str] = []
                font_sizes: List[float] = []
                for span in spans:
                    raw = span.get("text") or ""
                    normalized = strip_confusables(unify_dashes_and_decimals(raw))
                    normalized = collapse_spaces(normalized)
                    if not normalized:
                        continue
                    parts.append(normalized)
                    try:
                        font_sizes.append(float(span.get("size") or 0.0))
                    except Exception:
                        font_sizes.append(0.0)
                if not parts:
                    continue
                text = collapse_spaces(" ".join(parts))
                if not text:
                    continue
                bbox_tuple = tuple(line.get("bbox") or (0.0, 0.0, 0.0, 0.0))
                avg_font = sum(font_sizes) / max(len(font_sizes), 1)
                line_entries.append(Block(text=text, bbox=bbox_tuple, avg_font_size=avg_font, page_num=page_num))
                char_count += len(text)

        self._page_char_counts[page_num] = char_count
        if not line_entries:
            self.empty_pages.add(page_num)
            self.low_confidence_pages.add(page_num)
            return []
        if char_count < self.min_chars_per_page:
            self.low_confidence_pages.add(page_num)

        return self._cluster_lines(line_entries)

    def _cluster_lines(self, lines: Sequence[Block]) -> List[Block]:
        sorted_lines = sorted(lines, key=lambda item: item.bbox[1])
        result: List[Block] = []
        current: List[Block] = []

        def flush() -> None:
            if not current:
                return
            text = "\n".join(line.text for line in current)
            avg_font = sum(line.avg_font_size for line in current) / len(current)
            bbox = (
                min(line.bbox[0] for line in current),
                min(line.bbox[1] for line in current),
                max(line.bbox[2] for line in current),
                max(line.bbox[3] for line in current),
            )
            result.append(Block(text=text, bbox=bbox, avg_font_size=avg_font, page_num=current[0].page_num))
            current.clear()

        prev_bottom = None
        for line in sorted_lines:
            if not current:
                current.append(line)
                prev_bottom = line.bbox[3]
                continue
            assert prev_bottom is not None
            gap = line.bbox[1] - prev_bottom
            line_height = max(line.bbox[3] - line.bbox[1], 1.0)
            threshold = line_height * self.gap_multiplier
            if gap > threshold:
                flush()
                current.append(line)
            else:
                current.append(line)
            prev_bottom = line.bbox[3]

        flush()
        return result


class TableExtractor:
    """Locate tables via pdfplumber (default) or Camelot (text-based PDFs only).

    pdfplumber documentation: https://github.com/jsvine/pdfplumber
    Camelot limitations: https://camelot-py.readthedocs.io/en/master/user/faq.html#what-kind-of-pdfs-can-camelot-parse
    """

    def __init__(self, engine: Literal["plumber", "camelot"] = "plumber") -> None:
        self.engine = engine

    def extract(self, pdf_path: str) -> List[TableBlock]:
        if self.engine == "camelot":
            return self._extract_camelot(pdf_path)
        return self._extract_plumber(pdf_path)

    def _extract_plumber(self, pdf_path: str) -> List[TableBlock]:
        if pdfplumber is None:  # pragma: no cover - optional dependency
            raise RuntimeError("pdfplumber is required for table extraction (pip install pdfplumber).")

        tables: List[TableBlock] = []
        with pdfplumber.open(pdf_path) as doc:
            for page_idx, page in enumerate(doc.pages, start=1):
                for raw_table in page.extract_tables():
                    normalized_rows = [
                        [collapse_spaces(cell or "") for cell in (row or [])]
                        for row in raw_table or []
                    ]
                    if not any(cell.strip() for row in normalized_rows for cell in row):
                        continue
                    tables.append(TableBlock(rows=normalized_rows, page_num=page_idx, bbox=tuple(page.bbox)))
        return tables

    def _extract_camelot(self, pdf_path: str) -> List[TableBlock]:
        if camelot is None:  # pragma: no cover - optional dependency
            raise RuntimeError("Camelot is required for this table extraction mode (pip install camelot-py[cv]).")
        tables: List[TableBlock] = []
        for flavor in ("lattice", "stream"):
            try:
                result = camelot.read_pdf(pdf_path, pages="all", flavor=flavor)
            except Exception:
                continue
            for table in result:
                rows = [[collapse_spaces(cell or "") for cell in row] for row in table.data]
                if not any(cell.strip() for row in rows for cell in row):
                    continue
                try:
                    page_num = int(str(table.page))
                except Exception:
                    page_num = 1
                tables.append(TableBlock(rows=rows, page_num=page_num))
            if tables:
                break
        return tables


class KVExtractor:
    """Turn sentences that resemble 'key — value' into structured pairs."""

    KEY_PATTERN = re.compile(r"^\s*(?P<key>[^:–—•=]{2,64}?)\s*(?:[:=]|[-–—•])\s*(?P<value>.+)$")

    def extract(self, block_text: str) -> List[Tuple[str, str]]:
        lines = [collapse_spaces(part) for part in block_text.splitlines()]
        lines = [line for line in lines if line]
        pairs: List[Tuple[str, str]] = []
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            match = self.KEY_PATTERN.match(line)
            if not match:
                idx += 1
                continue
            key = match.group("key").strip(" .;,-")
            value = match.group("value").strip()
            idx += 1
            continuation: List[str] = []
            while idx < len(lines):
                lookahead = lines[idx]
                if self.KEY_PATTERN.match(lookahead):
                    break
                if lookahead.startswith(("•", "- ")):
                    break
                continuation.append(lookahead.rstrip(" .;,-"))
                idx += 1
            if continuation:
                value = f"{value} {' '.join(continuation)}".strip()
            if key and value:
                pairs.append((key, value))
        return pairs


_PRICE_PATTERN = re.compile(
    r"(?P<value>(?:\d{1,3}(?:[ \u00A0\u202F\u2009]\d{3})+|\d+)(?:[.,]\d{1,2})?)"
)


def _normalize_attr_key(text: str, seen: set[str]) -> str:
    slug = re.sub(r"[^0-9a-zA-Zа-яА-ЯёЁ]+", "_", text.lower()).strip("_")
    slug = slug or "attr"
    candidate = slug
    index = 2
    while candidate in seen:
        candidate = f"{slug}_{index}"
        index += 1
    seen.add(candidate)
    return candidate


def detect_title_and_price(blocks: Sequence[Block]) -> Tuple[str | None, int | None]:
    """Pick the top-most large block as title and parse thin-space aware price values.

    PyMuPDF keeps line ordering stable when using ``get_text("words", sort=True)``
    (see https://pymupdf.readthedocs.io/en/latest/textpage.html#get-text) so we
    rely on bounding-box Y coordinates to find the top rows. Thin-space and
    thousands separator nuances follow https://en.wikipedia.org/wiki/Thin_space .
    """

    if not blocks:
        return None, None

    sorted_blocks = sorted(blocks, key=lambda blk: (-blk.avg_font_size, blk.bbox[1]))
    title_candidate = None
    for block in sorted_blocks:
        if block.bbox[1] <= 200 or block.avg_font_size >= sorted_blocks[0].avg_font_size * 0.9:
            title_candidate = block.text.split("\n", 1)[0]
            break
    if not title_candidate:
        title_candidate = sorted_blocks[0].text.split("\n", 1)[0]

    price_value = None
    for block in blocks:
        for match in _PRICE_PATTERN.finditer(block.text):
            digits = re.sub(r"[^\d]", "", match.group("value"))
            if digits:
                try:
                    price_value = int(digits)
                    break
                except ValueError:
                    continue
        if price_value is not None:
            break

    return (title_candidate.strip() if title_candidate else None, price_value)


def extract_items(
    pdf_path: str,
    *,
    table_engine: Literal["plumber", "camelot"] = "plumber",
) -> ExtractionResult:
    """High-level routine turning PDF pages into product-like dictionaries.

    When no textual content is found on a page we mark it as low confidence so
    that the caller may trigger OCR (see _process_pdf for OCRmyPDF usage and
    requirements under https://ocrmypdf.readthedocs.io/en/latest/ which in
    turn depend on Tesseract, e.g. packages ``tesseract-ocr`` and
    ``tesseract-ocr-rus``).
    """

    text_extractor = PdfUniversalExtractor()
    kv_extractor = KVExtractor()
    table_extractor = TableExtractor(engine=table_engine)

    blocks = text_extractor.extract(pdf_path)
    stats = ExtractionStats(total_pages=text_extractor.total_pages or 0)
    stats.low_confidence_pages = set(text_extractor.low_confidence_pages)
    stats.empty_pages = set(text_extractor.empty_pages)

    try:
        tables = table_extractor.extract(pdf_path)
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.debug("table extraction failed", exc_info=exc)
        tables = []
    tables_by_page: Dict[int, List[TableBlock]] = collections.defaultdict(list)
    for table in tables:
        tables_by_page[table.page_num].append(table)

    blocks_by_page: Dict[int, List[Block]] = collections.defaultdict(list)
    for block in blocks:
        blocks_by_page[block.page_num].append(block)

    items: List[dict] = []
    processed_pages = set(blocks_by_page.keys()) | set(tables_by_page.keys())
    stats.total_pages = max(stats.total_pages, len(processed_pages))

    for page_num in sorted(processed_pages):
        page_tables = tables_by_page.get(page_num) or []
        page_blocks = blocks_by_page.get(page_num) or []
        if page_tables:
            table_items = _items_from_tables(page_tables, page_num)
            stats.table_blocks += len(table_items)
            _update_price_stats(stats, table_items)
            items.extend(table_items)
            continue
        kv_items = _items_from_blocks(page_blocks, kv_extractor, page_num)
        if kv_items:
            stats.kv_blocks += len(kv_items)
            _update_price_stats(stats, kv_items)
            items.extend(kv_items)
        else:
            stats.low_confidence_pages.add(page_num)

    return ExtractionResult(items, stats)


def _items_from_tables(tables: Sequence[TableBlock], page_num: int) -> List[dict]:
    items: List[dict] = []
    for table in tables:
        if not table.rows:
            continue
        headers_candidate = table.rows[0] or []
        if not headers_candidate:
            continue
        header_is_textual = any(re.search(r"[^\d]", cell or "") for cell in headers_candidate)
        headers: List[str] = []
        start_idx = 0
        if header_is_textual:
            headers = [
                re.sub(r"\s+", "_", (cell or "").strip().lower()) or f"col_{idx+1}"
                for idx, cell in enumerate(headers_candidate)
            ]
            start_idx = 1
        else:
            headers = [f"col_{idx+1}" for idx in range(len(headers_candidate))]
        for row in table.rows[start_idx:]:
            cleaned = [collapse_spaces(cell or "") for cell in row]
            if not any(cleaned):
                continue
            row_item: Dict[str, str] = {"page": str(page_num)}
            for idx, value in enumerate(cleaned):
                key = headers[idx] if idx < len(headers) else f"col_{idx+1}"
                row_item[key] = value
                if idx == 0 and value and not row_item.get("title"):
                    row_item["title"] = value
            if "title" not in row_item or not row_item["title"]:
                row_item["title"] = cleaned[0] if cleaned else f"Item {page_num}-{len(items)+1}"
            if "price" not in row_item:
                match_obj = None
                for candidate in cleaned:
                    match = _PRICE_PATTERN.search(candidate)
                    if match:
                        match_obj = match
                        break
                if match_obj:
                    digits = re.sub(r"[^\d]", "", match_obj.group("value"))
                    row_item["price"] = digits
            row_item.setdefault("price", "")
            items.append(row_item)
    return items


def _items_from_blocks(
    blocks: Sequence[Block],
    kv_extractor: KVExtractor,
    page_num: int,
) -> List[dict]:
    if not blocks:
        return []
    kv_pairs: List[Tuple[str, str]] = []
    for block in blocks:
        kv_pairs.extend(kv_extractor.extract(block.text))
    title, price_value = detect_title_and_price(blocks)
    if not kv_pairs and not title:
        return []
    attributes: Dict[str, str] = {}
    seen_keys: set[str] = set()
    for key, value in kv_pairs:
        normalized_key = _normalize_attr_key(key, seen_keys)
        attributes[normalized_key] = collapse_spaces(value)
    item = {
        "title": title or (kv_pairs[0][0] if kv_pairs else f"Page {page_num}"),
        "price": str(price_value) if price_value else "",
        "page": str(page_num),
    }
    item.update(attributes)
    return [item]


def _update_price_stats(stats: ExtractionStats, new_items: Sequence[dict]) -> None:
    stats.price_candidates += len(new_items)
    for row in new_items:
        price_text = str(row.get("price") or "").strip()
        if price_text:
            stats.price_hits += 1
logger = logging.getLogger(__name__)
