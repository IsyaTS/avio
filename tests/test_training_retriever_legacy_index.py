from __future__ import annotations

from pathlib import Path

import pytest

from libs.core.training import indexer, retriever


pytestmark = pytest.mark.unit


def test_legacy_disk_index_retrieval_works_with_lightweight_tfidf(tmp_path: Path, monkeypatch) -> None:
    tenant_dir = tmp_path / "tenant"
    indexes = tenant_dir / "indexes"
    examples = [
        indexer.TrainingExample(q="где можно посмотреть двери", a="Посмотреть можно в магазине."),
        indexer.TrainingExample(q="нужна дверь с терморазрывом", a="Уточните размер проема."),
    ]
    idx = indexer.build_index(examples)
    assert idx is not None
    path = indexes / f"training_{idx.sha1}.pkl"
    idx.save(path)

    monkeypatch.setattr(retriever, "_tenant_dir", lambda _tenant: str(tenant_dir))
    monkeypatch.setattr(retriever, "_read_tenant_config", lambda _tenant: {"learning": {"top_k": 2, "min_score": 0.01}})
    retriever._CACHE.clear()

    results = retriever.retrieve_examples(101, "где посмотреть двери", k=2)

    assert results
    assert results[0].a == "Посмотреть можно в магазине."
