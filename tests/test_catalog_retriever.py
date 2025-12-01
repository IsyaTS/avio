import pytest

from catalog import clear_catalog_cache, ensure_catalog_index, invalidate_catalog_index, retrieve_context


@pytest.fixture(autouse=True)
def reset_catalog_cache():
    clear_catalog_cache()
    yield
    clear_catalog_cache()


def _sample_items():
    return [
        {
            "title": "Смартфон Nova X",
            "description": "Камера 108 Мп, AMOLED экран, быстрая зарядка",
            "tags": ["хит", "смартфон"],
            "price": "49990",
        },
        {
            "title": "Умные часы FitGo",
            "description": "Отслеживание сна, уведомления, влагозащита",
            "tags": ["носима", "спорт"],
            "price": "12990",
        },
        {
            "title": "Смартфон Budget Mini",
            "description": "Двойная камера, большой аккумулятор",
            "tags": ["смартфон", "доступный"],
            "price": "19990",
        },
    ]


def test_retrieve_context_prioritises_relevant_items():
    items = _sample_items()

    results = retrieve_context(
        items=items,
        needs={"type": "смартфон"},
        query="нужен смартфон с хорошей камерой",
        tenant=42,
        limit=2,
    )

    assert len(results) == 2
    titles = [item["title"] for item in results]
    assert results[0]["title"].lower().startswith("смартфон")
    assert "Умные часы FitGo" not in titles[:2]
    assert all("_rag_score" in item for item in results)
    assert results[0]["_rag_score"] >= results[1]["_rag_score"]
    assert "камера" in results[0]["_match_excerpt"].lower()


def test_retrieve_context_returns_single_when_threshold_not_met():
    items = _sample_items()
    # Query unrelated to available products
    results = retrieve_context(
        items=items,
        needs={},
        query="игровой ноутбук",
        tenant=9,
        limit=3,
    )

    assert len(results) == 1
    assert results[0]["title"] in {item["title"] for item in items}
    assert "_rag_score" in results[0]


def test_indexer_invalidation_rebuilds_matrix():
    tenant = 5
    items = _sample_items()
    index1 = ensure_catalog_index(tenant, items)
    assert index1 is not None
    assert index1.matrix.shape[0] == len(items)

    extended = items + [
        {
            "title": "Планшет Vision",
            "description": "Яркий дисплей 11\" и защита зрения",
            "tags": ["планшет"],
        }
    ]
    index2 = ensure_catalog_index(tenant, extended)
    assert index2 is not None
    assert index2.signature != index1.signature
    assert index2.matrix.shape[0] == len(extended)

    invalidate_catalog_index(tenant)
    rebuilt = ensure_catalog_index(tenant, extended)
    assert rebuilt is not None
    assert rebuilt.signature == index2.signature
