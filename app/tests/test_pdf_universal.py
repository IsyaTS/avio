from pathlib import Path

import pytest

fitz = pytest.importorskip("fitz")

from app.catalog import pdf_universal
from app.catalog.text_normalize import (
    collapse_spaces,
    normalize_unicode_nfkc,
    strip_confusables,
    unify_dashes_and_decimals,
)


def test_text_normalization_helpers_handle_spaces_and_decimals():
    assert normalize_unicode_nfkc("ｔｅｓｔ") == "test"
    assert collapse_spaces("A\u00A0B\u202F C\u2009") == "A B C"
    assert unify_dashes_and_decimals("7 . 5 — размер") == "7.5 - размер"
    assert strip_confusables("ABECTOP") == "АВЕСТОР"


def test_detect_title_and_price_handles_thin_space_prices():
    blocks = [
        pdf_universal.Block(text="MODEL PRIMA\nЦена 29 500 ₽", bbox=(0.0, 0.0, 100.0, 40.0), avg_font_size=16.0, page_num=1),
        pdf_universal.Block(text="Стоимость: 29\u202f500", bbox=(0.0, 60.0, 100.0, 90.0), avg_font_size=13.0, page_num=1),
        pdf_universal.Block(text="Цена: 29500", bbox=(0.0, 110.0, 100.0, 140.0), avg_font_size=12.0, page_num=1),
    ]
    title, price = pdf_universal.detect_title_and_price(blocks)
    assert title.startswith("MODEL")
    assert price == 29500


def _build_pdf(path: Path, pages: list[list[str]]) -> None:
    doc = fitz.open()
    for lines in pages:
        page = doc.new_page()
        y = 72
        for line in lines:
            page.insert_text((60, y), line, fontsize=14)
            y += 20
    doc.save(path)


def test_universal_extract_items_on_sample_pdf(tmp_path):
    pdf_path = tmp_path / "catalog.pdf"
    _build_pdf(
        pdf_path,
        [
            ["Model ALPHA-100", "Цена: 29 500 ₽", "Ширина: 900 мм"],
            ["Model BETA-200", "Цена 31\u202f900 ₽", "Высота: 2050 мм"],
            ["Model GAMMA-300", "Price 29500", "Depth 70"],
        ],
    )
    result = pdf_universal.extract_items(str(pdf_path))
    assert isinstance(result, pdf_universal.ExtractionResult)
    assert len(result) >= 3
    assert result.stats.price_coverage >= 0.95


def test_write_catalog_csv_preserves_page_column(monkeypatch, tmp_path):
    from app.catalog import io as catalog_io

    tenants_root = tmp_path / "tenants"

    def _tenant_path(tenant: int) -> Path:
        path = tenants_root / str(int(tenant))
        path.mkdir(parents=True, exist_ok=True)
        return path

    monkeypatch.setattr(catalog_io.core_module, "tenant_dir", lambda tenant: str(_tenant_path(tenant)))
    monkeypatch.setattr(catalog_io.core_module, "ensure_tenant_files", lambda tenant: _tenant_path(tenant))

    rows = [
        {"title": "Item A", "price": "1200", "page": "1", "color": "white"},
        {"title": "Item B", "price": "2400", "page": "2", "width": "900"},
    ]
    meta = {"type": "pdf", "preserve_page_column": True}
    rel_path, header = catalog_io.write_catalog_csv(1, rows, "universal", meta)

    assert "page" in header
    csv_path = tenants_root / "1" / rel_path
    header_line = csv_path.read_text(encoding="utf-8-sig").splitlines()[0]
    assert header_line.startswith("id;title;price;page")
