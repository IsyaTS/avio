from __future__ import annotations

import asyncio
from typing import Any

import pytest

from apps.worker.services import contextual_case_embeddings_runtime


pytestmark = pytest.mark.unit


@pytest.mark.anyio
async def test_contextual_case_embeddings_loop_saves_vectors() -> None:
    logs: list[str] = []
    saved: list[dict[str, Any]] = []

    async def fake_fetch_pending(*, limit: int) -> list[dict[str, Any]]:
        assert limit == 5
        return [{"id": 3, "tenant_id": 7, "search_text": "покос травы цена"}]

    async def fake_set_embedding(
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
        assert texts == ["покос травы цена"]
        return [[0.1, 0.2]]

    async def fake_sleep(seconds: float) -> None:
        assert seconds == 0.2
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await contextual_case_embeddings_runtime.process_contextual_case_embeddings_loop(
            contextual_case_embeddings_runtime.ContextualCaseEmbeddingsDeps(
                enabled=True,
                embedding_model="text-embedding-3-small",
                fetch_pending_contextual_case_embeddings_fn=fake_fetch_pending,
                set_contextual_case_embedding_fn=fake_set_embedding,
                embed_texts_fn=fake_embed_texts,
                sleep_fn=fake_sleep,
                log_fn=logs.append,
            )
        )

    assert saved == [
        {
            "id": 3,
            "vector": [0.1, 0.2],
            "embedding_model": "text-embedding-3-small",
            "status": "ready",
            "error": None,
        }
    ]
    assert any("event=contextual_case_embedding_saved id=3 tenant=7" in item for item in logs)
