from __future__ import annotations

import csv
import io
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


SyncFn = Any


@dataclass(frozen=True)
class CatalogParseDeps:
    encoding_candidates: list[str]
    load_workbook_fn: SyncFn
    normalize_catalog_items_fn: SyncFn
    settings: Any
    pipeline_cls: Any
    catalog_index_module: Any
    catalog_index_error: type[Exception]
    logger: Any


DELIMITER_CANDIDATES = [";", ",", "\t"]


def normalize_headers(raw: Sequence[Any] | Any) -> list[str]:
    normalized: list[str] = []
    seen: dict[str, int] = {}
    for idx, cell in enumerate(raw):
        text = "" if cell is None else str(cell)
        clean = text.strip().lstrip("\ufeff")
        if not clean:
            clean = f"column_{idx + 1}"
        if clean in seen:
            seen[clean] += 1
            clean = f"{clean}_{seen[clean]}"
        else:
            seen[clean] = 0
        normalized.append(clean)
    if not normalized:
        normalized.append("title")
    return normalized


def relative_to(path: pathlib.Path, root: pathlib.Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def make_safe_filename(filename: str, ext: str, *, fallback: str) -> str:
    base = pathlib.Path(filename).stem or fallback
    base = re.sub(r"[^0-9A-Za-z._-]+", "_", base)
    base = base.strip("._") or fallback
    return f"{base}{ext}"


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value)


def strip_bom(text: str) -> str:
    if not text:
        return ""
    if text[0] == "\ufeff":
        return text.lstrip("\ufeff")
    return text


def detect_csv_delimiter(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ","

    first_line = ""
    for raw_line in io.StringIO(text):
        candidate = raw_line.strip("\r\n")
        if candidate:
            first_line = strip_bom(candidate)
            break

    if not first_line:
        return ","

    best = ","
    best_count = -1
    best_idx = len(DELIMITER_CANDIDATES)
    for idx, delimiter in enumerate(DELIMITER_CANDIDATES):
        count = first_line.count(delimiter)
        if count > best_count or (count == best_count and count > 0 and idx < best_idx):
            best = delimiter
            best_count = count
            best_idx = idx

    if best_count <= 0:
        return ","
    return best


def read_csv_bytes(raw: bytes, deps: CatalogParseDeps) -> tuple[list[dict[str, str]], dict[str, Any]]:
    encoding_used: str | None = None
    text: str | None = None
    for encoding in deps.encoding_candidates:
        try:
            text = raw.decode(encoding)
            encoding_used = encoding
            break
        except UnicodeDecodeError:
            continue
    if text is None or encoding_used is None:
        raise ValueError("encoding_detection_failed")

    delimiter = detect_csv_delimiter(text)

    stream = io.StringIO(text)
    reader = csv.reader(stream, delimiter=delimiter)
    header: list[str] | None = None
    for row in reader:
        if not row:
            continue
        meaningful = [stringify(cell) for cell in row if stringify(cell)]
        if not meaningful:
            continue
        header = normalize_headers(row)
        break
    records: list[dict[str, str]] = []
    if header is None:
        header = ["title"]
    for row in reader:
        if not row:
            continue
        cleaned = [stringify(value) for value in row]
        non_empty = [cell for cell in cleaned if cell]
        if not non_empty:
            continue
        if len(non_empty) == 1 and non_empty[0] == ".":
            continue
        while len(header) < len(row):
            header.append(f"column_{len(header) + 1}")
        record: dict[str, str] = {}
        for idx, value in enumerate(row):
            key = header[idx]
            record[key] = stringify(value)
        if any(record.values()):
            records.append(record)

    meta = {
        "type": "csv",
        "encoding": encoding_used,
        "delimiter": delimiter,
        "columns": header,
    }
    normalized = deps.normalize_catalog_items_fn(records, meta)
    return normalized, meta


def read_excel_bytes(raw: bytes, deps: CatalogParseDeps) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if deps.load_workbook_fn is None:
        raise RuntimeError("excel_support_unavailable")

    workbook = deps.load_workbook_fn(filename=io.BytesIO(raw), read_only=True, data_only=True)
    try:
        sheet = workbook.active
        header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True), None)
        if not header_row:
            header = ["title"]
        else:
            header = normalize_headers(header_row)
        records: list[dict[str, str]] = []
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if row is None:
                continue
            record: dict[str, str] = {}
            values = list(row)
            while len(header) < len(values):
                header.append(f"column_{len(header) + 1}")
            for idx, value in enumerate(values):
                key = header[idx]
                record[key] = stringify(value)
            if any(record.values()):
                records.append(record)
    finally:
        workbook.close()

    meta = {
        "type": "excel",
        "columns": header,
        "sheet": sheet.title if sheet is not None else "Sheet1",
        "encoding": "utf-8-sig",
        "delimiter": ";",
    }
    normalized = deps.normalize_catalog_items_fn(records, meta)
    return normalized, meta


def calc_price_coverage(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    filled = sum(1 for row in rows if str(row.get("price") or "").strip())
    return filled / len(rows)


def resolve_job_metrics(
    meta: Mapping[str, Any] | None,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "items_found": len(rows),
        "pages_total": 0,
        "pages_skipped_no_price": 0,
        "table_pages": 0,
        "median_price": None,
        "low_price_rate": 0.0,
        "price_coverage": calc_price_coverage(rows),
    }
    if isinstance(meta, Mapping):
        extraction = meta.get("extraction")
        if isinstance(extraction, Mapping):
            for key in ("pages_total", "pages_skipped_no_price", "table_pages"):
                value = extraction.get(key)
                if isinstance(value, (int, float)):
                    metrics[key] = int(value)
            if extraction.get("median_price") is not None:
                metrics["median_price"] = extraction.get("median_price")
            value = extraction.get("low_price_rate")
            if isinstance(value, (int, float)):
                metrics["low_price_rate"] = float(value)
            value = extraction.get("price_coverage")
            if isinstance(value, (int, float)):
                metrics["price_coverage"] = float(value)
            value = extraction.get("items_found")
            if isinstance(value, (int, float)):
                metrics["items_found"] = int(value)
    metrics["items_found"] = len(rows)
    return metrics


def process_pdf(
    *,
    tenant: int,
    saved_path: pathlib.Path,
    tenant_root: pathlib.Path,
    saved_rel_path: pathlib.Path,
    original_name: str,
    deps: CatalogParseDeps,
) -> tuple[list[dict[str, Any]], dict[str, Any], str | None]:
    table_engine = getattr(deps.settings, "PDF_TABLES_ENGINE", "plumber")
    render_dpi = int(getattr(deps.settings, "PDF_RENDER_DPI", 220) or 220)
    pipeline = deps.pipeline_cls(
        table_engine=table_engine,
        render_dpi=render_dpi,
        ocr_fallback=bool(getattr(deps.settings, "PDF_OCR_FALLBACK", False)),
    )

    try:
        items = pipeline.extract_items(str(saved_path))
        extraction_metrics = dict(getattr(pipeline, "metrics", {}))
    except Exception as exc:
        deps.logger.warning("catalog_miniprog_failed", exc_info=exc)
        items = []
        extraction_metrics = {}

    meta: dict[str, Any] = {
        "type": "pdf",
        "source_path": str(saved_rel_path),
        "original": original_name,
        "encoding": "utf-8-sig",
        "delimiter": ";",
        "pipeline_mode": "mini",
        "preserve_page_column": True,
        "extraction": extraction_metrics,
    }

    normalized = deps.normalize_catalog_items_fn(items, meta)
    meta["extraction"] = resolve_job_metrics(meta, normalized)

    indexes_dir = tenant_root / "indexes"
    indexes_dir.mkdir(parents=True, exist_ok=True)
    try:
        built_index = deps.catalog_index_module.build_pdf_index(
            saved_path,
            output_dir=indexes_dir,
            source_relpath=str(saved_rel_path),
            original_name=original_name,
        )
    except Exception as exc:
        deps.logger.exception(
            "catalog_pdf_index_failed tenant=%s file=%s",
            tenant,
            saved_path,
            exc_info=True,
        )
        raise deps.catalog_index_error("index_build_failed") from exc

    try:
        rel_index = str(built_index.index_path.relative_to(tenant_root))
    except Exception:
        rel_index = str(built_index.index_path)

    meta.update(
        {
            "index_path": rel_index,
            "indexed_at": built_index.generated_at,
            "chunk_count": built_index.chunk_count,
            "sha1": built_index.sha1,
            "page_count": built_index.page_count,
        }
    )
    return normalized, meta, rel_index
