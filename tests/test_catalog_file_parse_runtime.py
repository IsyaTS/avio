from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.api.web.services import catalog_file_parse_runtime


pytestmark = pytest.mark.unit


def _deps(**overrides):
    values = {
        "encoding_candidates": ["utf-8", "cp1251"],
        "load_workbook_fn": None,
        "normalize_catalog_items_fn": lambda rows, meta: rows,
        "settings": SimpleNamespace(PDF_TABLES_ENGINE="plumber", PDF_RENDER_DPI=180, PDF_OCR_FALLBACK=False),
        "pipeline_cls": None,
        "catalog_index_module": None,
        "catalog_index_error": RuntimeError,
        "logger": SimpleNamespace(warning=lambda *a, **k: None, exception=lambda *a, **k: None),
    }
    values.update(overrides)
    return catalog_file_parse_runtime.CatalogParseDeps(**values)


def test_detect_csv_delimiter_prefers_semicolon_for_catalog_exports() -> None:
    assert catalog_file_parse_runtime.detect_csv_delimiter("name;price\nA;1") == ";"
    assert catalog_file_parse_runtime.detect_csv_delimiter("name,price\nA,1") == ","
    assert catalog_file_parse_runtime.detect_csv_delimiter("name\tprice\nA\t1") == "\t"


def test_read_csv_bytes_normalizes_headers_and_skips_empty_rows() -> None:
    raw = "name;price;name\nChair;100;Main\n.\n\nTable;200;Alt\n".encode("utf-8")

    rows, meta = catalog_file_parse_runtime.read_csv_bytes(raw, _deps())

    assert rows == [
        {"name": "Chair", "price": "100", "name_1": "Main"},
        {"name": "Table", "price": "200", "name_1": "Alt"},
    ]
    assert meta["type"] == "csv"
    assert meta["encoding"] == "utf-8"
    assert meta["delimiter"] == ";"
    assert meta["columns"] == ["name", "price", "name_1"]


def test_resolve_job_metrics_keeps_extraction_metrics_but_recounts_items() -> None:
    metrics = catalog_file_parse_runtime.resolve_job_metrics(
        {
            "extraction": {
                "items_found": 99,
                "pages_total": 7,
                "pages_skipped_no_price": 2,
                "table_pages": 3,
                "median_price": 1500,
                "low_price_rate": 0.25,
                "price_coverage": 0.5,
            }
        },
        [{"price": "100"}, {"price": ""}],
    )

    assert metrics == {
        "items_found": 2,
        "pages_total": 7,
        "pages_skipped_no_price": 2,
        "table_pages": 3,
        "median_price": 1500,
        "low_price_rate": 0.25,
        "price_coverage": 0.5,
    }


def test_process_pdf_extracts_items_and_builds_index_once(tmp_path) -> None:
    calls: list[dict] = []
    saved_path = tmp_path / "catalog.pdf"
    saved_path.write_bytes(b"%PDF")

    class Pipeline:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.metrics = {"pages_total": 2, "items_found": 10}

        def extract_items(self, path: str):
            assert path == str(saved_path)
            return [{"title": "Door", "price": "100"}]

    class IndexModule:
        @staticmethod
        def build_pdf_index(path, *, output_dir, source_relpath, original_name):
            calls.append(
                {
                    "path": path,
                    "output_dir": output_dir,
                    "source_relpath": source_relpath,
                    "original_name": original_name,
                }
            )
            index_path = output_dir / "catalog.index.json"
            index_path.write_text("{}", encoding="utf-8")
            return SimpleNamespace(
                index_path=index_path,
                generated_at="2026-05-14T00:00:00Z",
                chunk_count=4,
                sha1="abc",
                page_count=2,
            )

    rows, meta, rel_index = catalog_file_parse_runtime.process_pdf(
        tenant=7,
        saved_path=saved_path,
        tenant_root=tmp_path,
        saved_rel_path=tmp_path / "uploads" / "catalog.pdf",
        original_name="source.pdf",
        deps=_deps(pipeline_cls=Pipeline, catalog_index_module=IndexModule),
    )

    assert rows == [{"title": "Door", "price": "100"}]
    assert len(calls) == 1
    assert calls[0]["source_relpath"].endswith("uploads/catalog.pdf")
    assert rel_index == "indexes/catalog.index.json"
    assert meta["index_path"] == rel_index
    assert meta["chunk_count"] == 4
    assert meta["extraction"]["items_found"] == 1
    assert meta["extraction"]["pages_total"] == 2
