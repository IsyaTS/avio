from __future__ import annotations

import pathlib
from dataclasses import dataclass
import logging
import math
import os
from typing import Any, Dict, List, Optional, Tuple

from libs.core import db
from .indexer import TrainingIndex
from libs.core.training import utils as training_utils


_CACHE: Dict[int, Tuple[pathlib.Path, TrainingIndex]] = {}
_log = logging.getLogger("training")
_LOG_PREFIX = "[training]"


def _tenant_dir(tenant: int) -> str:
    # Lazy import to avoid circular dependency with sales_core.
    from libs.core.sales_core import tenant_dir as _tenant_dir_fn

    return _tenant_dir_fn(tenant)


def _read_tenant_config(tenant: int) -> Dict[str, Any]:
    # Lazy import to avoid circular dependency with sales_core.
    from libs.core.sales_core import read_tenant_config as _read_tenant_config_fn

    cfg = _read_tenant_config_fn(tenant)
    return cfg if isinstance(cfg, dict) else {}


def _learning_min_score(learn: Any, default: float = 0.18) -> float:
    if not isinstance(learn, dict):
        return default
    raw = learn.get("min_score", learn.get("min_similarity", default))
    try:
        value = float(raw)
    except Exception:
        value = default
    return max(0.0, min(1.0, value))


def _latest_index_path(tenant: int) -> Optional[pathlib.Path]:
    base = pathlib.Path(_tenant_dir(tenant))
    idx_dir = base / "indexes"
    if not idx_dir.exists():
        return None
    candidates = sorted(idx_dir.glob("training_*.pkl"))
    return candidates[-1] if candidates else None


def ensure_training_index(tenant: int) -> Optional[TrainingIndex]:
    path = _latest_index_path(tenant)
    if not path:
        return None
    cached = _CACHE.get(tenant)
    if cached and cached[0] == path:
        return cached[1]
    try:
        idx = TrainingIndex.load(path)
        _CACHE[tenant] = (path, idx)
        try:
            size = path.stat().st_size if path.exists() else 0
        except Exception:
            size = 0
        _log.info(
            f"{_LOG_PREFIX} index loaded tenant=%s path=%s size=%sB pairs=%s",
            tenant,
            str(path),
            size,
            len(idx.items),
        )
        return idx
    except Exception:
        _log.exception(f"{_LOG_PREFIX} index_load_failed tenant=%s", tenant, exc_info=True)
        return None


@dataclass
class RetrievedExample:
    q: str
    a: str
    score: float
    meta: Dict[str, Any]


def retrieve_examples(tenant: int, query: str, k: int = 3) -> List[RetrievedExample]:
    """Legacy TF-IDF retrieval from on-disk index (uploads)."""
    cfg = _read_tenant_config(tenant)
    learn = cfg.get("learning") if isinstance(cfg, dict) else {}
    try:
        min_chars = max(0, int((learn or {}).get("min_chars", 0)))
    except Exception:
        min_chars = 0
    if (os.getenv("TESTING") or "").strip() == "1":
        min_chars = 0
    try:
        top_k = max(1, int((learn or {}).get("top_k", k)))
    except Exception:
        top_k = k

    idx = ensure_training_index(tenant)
    if not idx or not (query or "").strip():
        return []
    try:
        q_vec = idx.vectorizer.transform([query])
        if hasattr(idx.matrix, "vectors") and hasattr(q_vec, "vectors"):
            doc_vectors = getattr(idx.matrix, "vectors", []) or []
            query_vectors = getattr(q_vec, "vectors", []) or []
            q_dict = query_vectors[0] if query_vectors else {}
            scored: list[tuple[int, float]] = []
            if q_dict:
                for idx_num, doc in enumerate(doc_vectors):
                    if not doc:
                        continue
                    score = 0.0
                    if len(q_dict) <= len(doc):
                        for term, weight in q_dict.items():
                            score += float(weight) * float(doc.get(term, 0.0))
                    else:
                        for term, weight in doc.items():
                            score += float(weight) * float(q_dict.get(term, 0.0))
                    if score > 0:
                        scored.append((idx_num, float(score)))
            scored.sort(key=lambda item: item[1], reverse=True)
            order = [item[0] for item in scored]
            score_lookup = {item[0]: item[1] for item in scored}
        else:
            import numpy as np  # type: ignore

            scores = (q_vec @ idx.matrix.T).toarray().ravel()
            order = np.argsort(-scores)
            score_lookup = {int(i): float(scores[int(i)]) for i in order if scores[int(i)] > 0}
        out: List[RetrievedExample] = []
        for i in order:
            ex = idx.items[int(i)]
            if len(ex.q.strip()) < min_chars or len(ex.a.strip()) < min_chars:
                continue
            score = float(score_lookup.get(int(i), 0.0))
            # lightweight floor: skip zero/negative matches
            if score <= 0:
                continue
            out.append(RetrievedExample(q=ex.q, a=ex.a, score=score, meta=ex.meta))
            if len(out) >= top_k:
                break
        _log.debug(
            f"{_LOG_PREFIX} retrieve tenant=%s query_len=%s returned=%s",
            tenant,
            len(query or ""),
            len(out),
        )
        return out
    except Exception:
        _log.exception(f"{_LOG_PREFIX} retrieve_failed tenant=%s", tenant, exc_info=True)
        return []


def build_examples_block(tenant: int, query: str) -> str:
    """Return a formatted block for the system prompt with 1–2 best examples."""
    cfg = _read_tenant_config(tenant)
    learn = cfg.get("learning") if isinstance(cfg, dict) else {}
    try:
        top_k = max(1, min(2, int((learn or {}).get("top_k", 2))))
    except Exception:
        top_k = 2
    results = retrieve_examples(tenant, query, k=top_k)
    if not results:
        return ""
    lines: List[str] = [
        "Примеры обучающих диалогов (если вопрос похож — отвечай максимально близко к примеру):"
    ]
    for ex in results[:top_k]:
        q = (ex.q or "").strip()
        a = (ex.a or "").strip()
        if not q or not a:
            continue
        lines.append(f"Клиент: {q}")
        lines.append(f"Менеджер: {a}")
    block = "\n".join(lines)
    return block.strip()


async def _retrieve_examples_from_db(tenant: int, query: str, k: int = 3) -> List[RetrievedExample]:
    cfg = _read_tenant_config(tenant)
    learn = cfg.get("learning") if isinstance(cfg, dict) else {}
    try:
        min_chars = max(0, int((learn or {}).get("min_chars", 0)))
    except Exception:
        min_chars = 0
    if (os.getenv("TESTING") or "").strip() == "1":
        min_chars = 0
    try:
        top_k = max(1, int((learn or {}).get("top_k", k)))
    except Exception:
        top_k = k
    min_score = _learning_min_score(learn)

    examples = await db.get_training_examples_for_retrieval(tenant, limit=max(top_k * 5, 20))
    examples = [ex for ex in examples if not ex.get("is_bad")]
    if not examples or not (query or "").strip():
        return []

    sanitized_query = training_utils.sanitize_text(query)
    texts = [training_utils.sanitize_text(ex.get("q_text")) for ex in examples]

    exact_matches: List[RetrievedExample] = []
    if sanitized_query:
        for idx, ex in enumerate(examples):
            q_text = (
                texts[idx] if idx < len(texts) else training_utils.sanitize_text(ex.get("q_text"))
            )
            if q_text and q_text.lower() == sanitized_query.lower():
                a_text = training_utils.sanitize_text(ex.get("a_text"))
                if len(q_text.strip()) < min_chars or len(a_text.strip()) < min_chars:
                    continue
                exact_matches.append(
                    RetrievedExample(
                        q=q_text,
                        a=a_text,
                        score=1.0,
                        meta={"id": ex.get("id"), "source": ex.get("source")},
                    )
                )
                if len(exact_matches) >= top_k:
                    break
    if exact_matches:
        try:
            await db.increment_training_examples_usage(
                [int(ex.meta.get("id")) for ex in exact_matches if ex.meta.get("id")]
            )
        except Exception:
            _log.debug(f"{_LOG_PREFIX} usage_increment_failed tenant=%s", tenant, exc_info=True)
        try:
            _log.info(
                f"{_LOG_PREFIX} retrieve_exact tenant=%s ids=%s",
                tenant,
                [ex.meta.get("id") for ex in exact_matches if ex.meta.get("id")],
            )
        except Exception:
            pass
        return exact_matches

    # Try embeddings first if present
    use_embeddings = any(ex.get("embedding") for ex in examples)
    scores: List[Tuple[int, float]] = []

    if use_embeddings:
        try:
            from libs.core.training import (
                embeddings as emb_mod,
            )  # local import to avoid hard dep at import time

            q_vec = await emb_mod.embed_query(sanitized_query)
            if q_vec:

                def _cosine(v1: List[float], v2: List[float]) -> float:
                    if not v1 or not v2:
                        return 0.0
                    if len(v1) != len(v2):
                        return 0.0
                    dot = sum(a * b for a, b in zip(v1, v2))
                    n1 = math.sqrt(sum(a * a for a in v1))
                    n2 = math.sqrt(sum(b * b for b in v2))
                    if n1 <= 0 or n2 <= 0:
                        return 0.0
                    return float(dot / (n1 * n2))

                for idx, ex in enumerate(examples):
                    vec = ex.get("embedding")
                    if not isinstance(vec, (list, tuple)):
                        continue
                    try:
                        vec_floats = [float(v) for v in vec]
                    except Exception:
                        continue
                    score = _cosine(q_vec, vec_floats)
                    if score > 0:
                        scores.append((idx, score))
        except Exception:
            _log.exception(f"{_LOG_PREFIX} embedding_retrieve_failed tenant=%s", tenant)
            scores = []

    if not scores:
        # TF-IDF fallback in-memory (works with lightweight shim).
        try:
            from libs.core.sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore

            vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 2), min_df=1)
            matrix = vectorizer.fit_transform(texts)
            q_vec = vectorizer.transform([sanitized_query])

            if hasattr(matrix, "vectors") and hasattr(q_vec, "vectors"):
                doc_vectors = getattr(matrix, "vectors", []) or []
                query_vectors = getattr(q_vec, "vectors", []) or []
                q_dict = query_vectors[0] if query_vectors else {}
                tfidf_scores: list[tuple[int, float]] = []
                if q_dict:
                    for idx, doc in enumerate(doc_vectors):
                        if not doc:
                            continue
                        score = 0.0
                        if len(q_dict) <= len(doc):
                            for term, weight in q_dict.items():
                                score += weight * float(doc.get(term, 0.0))
                        else:
                            for term, weight in doc.items():
                                score += float(weight) * float(q_dict.get(term, 0.0))
                        if score > 0:
                            tfidf_scores.append((idx, float(score)))
                tfidf_scores.sort(key=lambda item: item[1], reverse=True)
                scores = tfidf_scores
            else:
                import numpy as np  # type: ignore

                tfidf_scores = (q_vec @ matrix.T).toarray().ravel()
                order = np.argsort(-tfidf_scores)
                scores = [
                    (int(i), float(tfidf_scores[int(i)])) for i in order if tfidf_scores[int(i)] > 0
                ]
        except Exception:
            _log.exception(f"{_LOG_PREFIX} tfidf_retrieve_failed tenant=%s", tenant)
            return []

    out: List[RetrievedExample] = []
    seen_ids: set[int] = set()
    for idx, score in scores:
        if float(score) < min_score:
            continue
        if idx in seen_ids or idx >= len(examples):
            continue
        ex = examples[idx]
        q = training_utils.sanitize_text(ex.get("q_text"))
        a = training_utils.sanitize_text(ex.get("a_text"))
        if len(q.strip()) < min_chars or len(a.strip()) < min_chars:
            continue
        out.append(
            RetrievedExample(
                q=q,
                a=a,
                score=float(score),
                meta={"id": ex.get("id"), "source": ex.get("source")},
            )
        )
        seen_ids.add(idx)
        if len(out) >= top_k:
            break

    try:
        await db.increment_training_examples_usage(
            [int(ex.meta.get("id")) for ex in out if ex.meta.get("id")]
        )
    except Exception:
        _log.debug(f"{_LOG_PREFIX} usage_increment_failed tenant=%s", tenant, exc_info=True)
    try:
        _log.info(
            f"{_LOG_PREFIX} retrieve_db tenant=%s results=%s ids=%s",
            tenant,
            len(out),
            [ex.meta.get("id") for ex in out if ex.meta.get("id")],
        )
    except Exception:
        pass
    return out


async def retrieve_examples_async(tenant: int, query: str, k: int = 3) -> List[RetrievedExample]:
    # Prefer DB-backed examples; if none, fallback to legacy on-disk indexes.
    try:
        _log.info(
            f"{_LOG_PREFIX} retrieve_start tenant=%s query=%s",
            tenant,
            training_utils.sanitize_text(query),
        )
    except Exception:
        pass
    db_results = await _retrieve_examples_from_db(tenant, query, k=k)
    if db_results:
        return db_results
    return retrieve_examples(tenant, query, k=k)


async def build_examples_block_async(tenant: int, query: str) -> str:
    cfg = _read_tenant_config(tenant)
    learn = cfg.get("learning") if isinstance(cfg, dict) else {}
    if isinstance(learn, dict) and learn.get("enabled") is False:
        return ""
    try:
        top_k = max(1, min(2, int((learn or {}).get("top_k", 2))))
    except Exception:
        top_k = 2
    results = await retrieve_examples_async(tenant, query, k=top_k)
    if not results:
        return ""
    lines: List[str] = [
        "Проверенные примеры ответов менеджера для самообучения.",
        "Если текущий вопрос похож, используй пример как образец смысла и формулировки; адаптируй только под текущие факты диалога.",
        "Не выдумывай факты из примера, если они не подтверждены в текущем диалоге:",
    ]
    for ex in results[:top_k]:
        q = (ex.q or "").strip()
        a = (ex.a or "").strip()
        if not q or not a:
            continue
        lines.append(f"Клиент: {q}")
        lines.append(f"Менеджер: {a}")
    return "\n".join(lines).strip()
