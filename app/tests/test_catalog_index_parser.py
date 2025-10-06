import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INNER = ROOT / "app"
for candidate in (ROOT, INNER):
    value = str(candidate)
    if value not in sys.path:
        sys.path.append(value)

from app.catalog_index import CatalogChunk, CatalogIndex, index_to_catalog_items


def _build_index(tmp_path: Path, text: str) -> CatalogIndex:
    chunk = CatalogChunk(
        chunk_id="chunk-1",
        page=1,
        title="Супер каталог",
        text=text,
        identifiers=(),
    )
    index_path = tmp_path / "catalog.json"
    return CatalogIndex(
        catalog_id="cat-1",
        source_path="uploads/catalog.pdf",
        original_name="catalog.pdf",
        generated_at=1700000000,
        sha1="deadbeef",
        page_count=1,
        chunk_count=1,
        chunks=[chunk],
        index_path=index_path,
    )


def test_index_to_catalog_items_extracts_products(tmp_path):
    block_text = """
С У П Е Р И З Д Е Л И Е
Цена: 12 500 руб.
Цвет: Белый
Материал: Дерево
Описание: Лучшая модель
 Продолжение описания

С У П Е Р И З Д Е Л И Е
Цена — 13 200 руб.
Цвет — Серый
Материалы — МДФ
Описание: Серия Про
 Съёмная панель

Элитное изделие
Цена: 45 000 ₽
Цвет: Белый
Размер: 80 см
Материал: Массив
Описание: Премиальная коллекция
 продолжение

Контакты: +7 (800) 123-45-67
""".strip()

    index = _build_index(tmp_path, block_text)
    items = index_to_catalog_items(index)

    assert len(items) == 3
    titles = [item["title"] for item in items]
    assert len(set(titles)) == 3
    assert all("СУПЕР" in title for title in titles[:2])
    assert items[0]["price"].isdigit()
    assert items[1]["price"].isdigit()
    assert items[2]["price"] == "45000"

    for item in items:
        assert item["id"].isdigit()
        assert item.get("Материал")
        assert item.get("Цвет")
        assert item.get("Описание")
        assert item.get("page") == "1"

    # Материал/Материалы должны слиться в одну колонку
    assert all("Материалы" not in item for item in items)

    # Убедимся, что описание склеено
    assert "Продолжение" in items[0]["Описание"]

    csv_path = index.index_path.with_suffix(".csv")
    assert csv_path.exists()
    with csv_path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    assert rows[0][:3] == ["id", "title", "price"]
    assert len(rows) == 4  # header + 3 items

    manifest_path = index.index_path.with_suffix(".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["items_total"] == 4
    assert manifest["kept"] == 3
    assert manifest["dropped_non_product"] == 1
    assert manifest["columns"][0:3] == ["id", "title", "price"]
    assert manifest["merged_columns_map"].get("Материалы") == "Материал"
    assert manifest["logs"][0]["reason"] == "non_product"
    assert manifest["duplicate_titles_fixed"]
    assert "price_missing_examples" not in manifest
