from __future__ import annotations

import asyncio
from typing import Any

import pytest

from apps.worker.services import training_embeddings_runtime


pytestmark = pytest.mark.unit


@pytest.mark.anyio
async def test_training_embeddings_loop_marks_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    logs: list[str] = []
    saved: list[dict[str, Any]] = []

    async def fake_fetch_pending_examples(*, limit: int) -> list[dict[str, Any]]:
        assert limit == 5
        return [
            {"id": 1, "tenant_id": 10, "q_text": "first"},
            {"id": 2, "tenant_id": 10, "q_text": "second"},
        ]

    async def fake_set_training_embedding(
        item_id: int,
        vector: Any,
        *,
        embedding_model: str,
        status: str,
        error: str | None,
    ) -> None:
        saved.append(
            {
                "id": item_id,
                "vector": vector,
                "embedding_model": embedding_model,
                "status": status,
                "error": error,
            }
        )

    async def fake_embed_texts(_texts: list[str]) -> list[list[float]]:
        raise RuntimeError("embed down")

    async def fake_sleep(seconds: float) -> None:
        assert seconds == 5.0
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await training_embeddings_runtime.process_training_embeddings_loop(
            training_embeddings_runtime.TrainingEmbeddingsDeps(
                enabled=True,
                embedding_model="text-embedding-3-small",
                fetch_pending_examples_fn=fake_fetch_pending_examples,
                set_training_embedding_fn=fake_set_training_embedding,
                embed_texts_fn=fake_embed_texts,
                sleep_fn=fake_sleep,
                log_fn=logs.append,
            )
        )

    assert saved == [
        {
            "id": 1,
            "vector": None,
            "embedding_model": "text-embedding-3-small",
            "status": "failed",
            "error": "embed down",
        },
        {
            "id": 2,
            "vector": None,
            "embedding_model": "text-embedding-3-small",
            "status": "failed",
            "error": "embed down",
        },
    ]
    assert any("event=training_embeddings_failed reason=embed down count=2" in item for item in logs)


@pytest.mark.anyio
async def test_training_embeddings_loop_saves_vectors() -> None:
    logs: list[str] = []
    saved: list[dict[str, Any]] = []

    async def fake_fetch_pending_examples(*, limit: int) -> list[dict[str, Any]]:
        assert limit == 5
        return [
            {"id": 7, "tenant_id": 3, "q_text": "hello"},
            {"id": 8, "tenant_id": 3, "q_text": "world"},
        ]

    async def fake_set_training_embedding(
        item_id: int,
        vector: Any,
        *,
        embedding_model: str,
        status: str,
        error: str | None,
    ) -> None:
        saved.append(
            {
                "id": item_id,
                "vector": vector,
                "embedding_model": embedding_model,
                "status": status,
                "error": error,
            }
        )

    async def fake_embed_texts(texts: list[str]) -> list[list[float]]:
        assert texts == ["hello", "world"]
        return [[0.1, 0.2], [0.3, 0.4]]

    async def fake_sleep(seconds: float) -> None:
        assert seconds == 0.2
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await training_embeddings_runtime.process_training_embeddings_loop(
            training_embeddings_runtime.TrainingEmbeddingsDeps(
                enabled=True,
                embedding_model="text-embedding-3-small",
                fetch_pending_examples_fn=fake_fetch_pending_examples,
                set_training_embedding_fn=fake_set_training_embedding,
                embed_texts_fn=fake_embed_texts,
                sleep_fn=fake_sleep,
                log_fn=logs.append,
            )
        )

    assert saved == [
        {
            "id": 7,
            "vector": [0.1, 0.2],
            "embedding_model": "text-embedding-3-small",
            "status": "ready",
            "error": None,
        },
        {
            "id": 8,
            "vector": [0.3, 0.4],
            "embedding_model": "text-embedding-3-small",
            "status": "ready",
            "error": None,
        },
    ]
    assert any("event=training_embedding_saved id=7 tenant=3" in item for item in logs)
