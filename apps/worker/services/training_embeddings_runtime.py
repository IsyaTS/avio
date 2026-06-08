from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Sequence


LogFn = Callable[[str], None]
FetchPendingFn = Callable[..., Awaitable[list[Mapping[str, Any]]]]
SetEmbeddingFn = Callable[..., Awaitable[None]]
EmbedTextsFn = Callable[[Sequence[str]], Awaitable[Sequence[Any]]]
SleepFn = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class TrainingEmbeddingsDeps:
    enabled: bool
    embedding_model: str
    fetch_pending_examples_fn: FetchPendingFn
    set_training_embedding_fn: SetEmbeddingFn
    embed_texts_fn: EmbedTextsFn
    sleep_fn: SleepFn
    log_fn: LogFn


async def process_training_embeddings_loop(deps: TrainingEmbeddingsDeps) -> None:
    if not deps.enabled:
        deps.log_fn("event=training_embeddings_disabled")
        return
    deps.log_fn(f"event=training_embeddings_loop_start model={deps.embedding_model}")
    while True:
        try:
            pending = await deps.fetch_pending_examples_fn(limit=5)
            if not pending:
                await deps.sleep_fn(5.0)
                continue
            texts = [str(item.get("q_text") or "") for item in pending]
            try:
                vectors = await deps.embed_texts_fn(texts)
            except Exception as exc:
                deps.log_fn(f"event=training_embeddings_failed reason={exc} count={len(pending)}")
                for item in pending:
                    try:
                        await deps.set_training_embedding_fn(
                            item.get("id"),
                            None,
                            embedding_model=deps.embedding_model,
                            status="failed",
                            error=str(exc),
                        )
                    except Exception:
                        pass
                await deps.sleep_fn(5.0)
                continue

            for item, vec in zip(pending, vectors):
                try:
                    await deps.set_training_embedding_fn(
                        item.get("id"),
                        list(vec) if isinstance(vec, (list, tuple)) else vec,
                        embedding_model=deps.embedding_model,
                        status="ready",
                        error=None,
                    )
                    deps.log_fn(
                        f"event=training_embedding_saved id={item.get('id')} tenant={item.get('tenant_id')}"
                    )
                except Exception as exc:
                    deps.log_fn(
                        "event=training_embedding_save_failed id=%s tenant=%s err=%s"
                        % (item.get("id"), item.get("tenant_id"), exc)
                    )
            await deps.sleep_fn(0.2)
        except Exception as exc:
            deps.log_fn(f"event=training_embeddings_loop_error err={exc}")
            await deps.sleep_fn(2.0)
