from __future__ import annotations

import os
import logging
import asyncio
from typing import List

try:
    import openai  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    openai = None  # type: ignore

from libs.core.sales_core import settings as core_settings, _get_openai_client  # type: ignore

_log = logging.getLogger("training")
_LOG_PREFIX = "[training]"


def _resolve_model() -> str:
    return (os.getenv("EMBEDDING_MODEL") or getattr(core_settings, "EMBEDDING_MODEL", "") or "text-embedding-3-small")


async def embed_texts(texts: List[str]) -> List[List[float]]:
    client = _get_openai_client()
    if client is None:
        raise RuntimeError("openai_client_missing")
    model = _resolve_model()
    try:
        embeddings_iface = getattr(client, "embeddings", None)
        create_fn = getattr(embeddings_iface, "create", None)
        if create_fn:
            if asyncio.iscoroutinefunction(create_fn):
                resp = await create_fn(model=model, input=texts)
            else:
                resp = await asyncio.to_thread(create_fn, model=model, input=texts)
        elif openai and hasattr(openai, "Embedding"):
            resp = await asyncio.to_thread(openai.Embedding.create, model=model, input=texts)  # type: ignore[attr-defined]
        else:
            resp = await _embed_sync(client, model, texts)
    except AttributeError:
        resp = await _embed_sync(client, model, texts)
    data = getattr(resp, "data", None) or []
    embeddings: List[List[float]] = []
    for item in data:
        vec = getattr(item, "embedding", None)
        if vec:
            embeddings.append(list(vec))
    if not embeddings:
        raise RuntimeError("embedding_empty_result")
    _log.debug(f\"{_LOG_PREFIX} embeddings generated count=%s model=%s\", len(embeddings), model)
    return embeddings


async def _embed_sync(client: object, model: str, texts: List[str]):
    # Run in thread to avoid blocking event loop
    import asyncio

    def _call():
        if hasattr(client, "embeddings"):
            return client.embeddings.create(model=model, input=texts)  # type: ignore[attr-defined]
        if openai and hasattr(openai, "Embedding"):
            return openai.Embedding.create(model=model, input=texts)  # type: ignore[attr-defined]
        raise RuntimeError("embedding_api_missing")

    return await asyncio.to_thread(_call)


async def embed_query(text: str) -> List[float]:
    vectors = await embed_texts([text])
    return vectors[0] if vectors else []
