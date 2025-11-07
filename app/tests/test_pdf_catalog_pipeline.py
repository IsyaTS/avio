from pathlib import Path

from app.catalog import text_normalize
from app.catalog.io import write_catalog_csv
from app.catalog.pdf_catalog_pipeline import CatalogPipeline


def test_normalization_nfkc_and_thin_spaces():
    value = "29\u00A0\u202F500"
    normalized = text_normalize.collapse_spaces(text_normalize.normalize_unicode_nfkc(value))
    assert normalized == "29 500"
    assert text_normalize.unify_dashes_and_decimals("7 . 5 — размер") == "7.5 - размер"


def test_price_regex_accepts_thousands_and_rejects_small_values():
    pipeline = CatalogPipeline(table_engine="plumber", render_dpi=100)
    assert pipeline._normalize_price("29 500", 1)[0] == "29500"
    assert pipeline._normalize_price("29500", 2)[0] == "29500"
    assert pipeline._normalize_price("999", 1)[0] == ""
    assert pipeline._normalize_price("2", 2)[0] == ""


def _write_pdf(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    def escape(text: str) -> str:
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    stream_parts = ["BT", "/F1 12 Tf", "36 770 Td"]
    for idx, raw_line in enumerate(lines):
        encoded = escape(raw_line)
        if idx == 0:
            stream_parts.append(f"({encoded}) Tj")
        else:
            stream_parts.append(f"T* ({encoded}) Tj")
    stream_parts.append("ET")
    content_stream = "\n".join(stream_parts).encode("utf-8")

    objects: list[bytes] = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content_stream), content_stream),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    output = bytearray()
    output.extend(b"%PDF-1.4\n")
    offsets: list[int] = []

    for idx, obj in enumerate(objects, start=1):
        offsets.append(len(output))
        output.extend(f"{idx} 0 obj\n".encode("ascii"))
        output.extend(obj)
        output.extend(b"\nendobj\n")

    xref_pos = len(output)
    output.extend(b"xref\n")
    output.extend(f"0 {len(objects)+1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets:
        output.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    output.extend(b"trailer\n")
    output.extend(f"<< /Size {len(objects)+1} /Root 1 0 R >>\n".encode("ascii"))
    output.extend(b"startxref\n")
    output.extend(f"{xref_pos}\n".encode("ascii"))
    output.extend(b"%%EOF")

    path.write_bytes(bytes(output))


def test_catalog_pipeline_integration(tmp_path, monkeypatch):
    pdf_path = tmp_path / "sample.pdf"
    _write_pdf(pdf_path, ["Model ALPHA-1", "Цена: 12 500 ₽", "Цвет: белый"])

    pipeline = CatalogPipeline(table_engine="plumber", render_dpi=120)
    items = pipeline.extract_items(str(pdf_path))
    assert items, "Pipeline should extract at least one item"
    metrics = pipeline.metrics
    assert metrics["items_found"] >= 1
    assert metrics["median_price"] is None or metrics["median_price"] >= 1000

    tenants_dir = tmp_path / "tenants"
    tenants_dir.mkdir(parents=True, exist_ok=True)

    from app.catalog import io as catalog_io

    def _tenant_dir(_: int) -> str:
        path = tenants_dir / "1"
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    monkeypatch.setattr(catalog_io.core_module, "tenant_dir", _tenant_dir)
    monkeypatch.setattr(catalog_io.core_module, "ensure_tenant_files", lambda tenant: Path(_tenant_dir(tenant)).mkdir(parents=True, exist_ok=True))

    meta = {"type": "pdf", "preserve_page_column": True}
    rel_path, header = write_catalog_csv(1, items, "catalog_pipeline_test", meta)
    assert header[:4] == ["id", "title", "price", "page"]
    assert (tenants_dir / "1" / rel_path).exists()
