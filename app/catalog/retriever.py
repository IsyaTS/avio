from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Optional, Sequence, Set

try:
    from sklearn.metrics.pairwise import linear_kernel  # type: ignore
except ImportError:  # pragma: no cover - optional dependency missing
    try:
        from app.sklearn.metrics.pairwise import linear_kernel  # type: ignore
    except ImportError:
        linear_kernel = None  # type: ignore[assignment]

from .indexer import ensure_catalog_index

_WORD_RE = re.compile(r"[\wёЁ]+", re.UNICODE)


def _compose_query_text(needs: Dict[str, Any], query: str | None) -> str:
    parts: List[str] = []
    if query:
        parts.append(query)

    normalized_needs = needs or {}
    for key in (
        "type",
        "category",
        "brand",
        "color",
        "budget",
        "budget_max",
        "audience",
        "problem",
    ):
        value = normalized_needs.get(key)
        if not value:
            continue
        if isinstance(value, (list, tuple, set)):
            parts.extend(str(v) for v in value if v)
        else:
            parts.append(str(value))
    return " ".join(parts)


def _highlight_excerpt(text: str, tokens: Sequence[str]) -> str:
    if not text:
        return ""
    lowered = text.lower()
    for token in tokens:
        pos = lowered.find(token)
        if pos == -1:
            continue
        start = max(0, pos - 40)
        end = min(len(text), pos + 60)
        excerpt = text[start:end].strip()
        if start > 0:
            excerpt = "…" + excerpt
        if end < len(text):
            excerpt = excerpt + "…"
        return excerpt
    return text[:120].strip()


def _extract_tokens(query: str) -> List[str]:
    raw = [match.group(0).lower() for match in _WORD_RE.finditer(query)]
    return [token for token in raw if len(token) >= 3]


def _score_threshold(count: int) -> float:
    if count <= 0:
        return 0.0
    # allow smaller catalogs to surface more items
    return 0.05 if count < 10 else 0.08


def _attach_metadata(item: Dict[str, Any], score: float, excerpt: str) -> Dict[str, Any]:
    enriched = dict(item)
    enriched.setdefault("_rag_score", float(score))
    if excerpt:
        enriched.setdefault("_match_excerpt", excerpt)
    return enriched


def _token_overlap_score(query_tokens: Set[str], item_tokens: Set[str]) -> float:
    if not query_tokens or not item_tokens:
        return 0.0
    common = query_tokens & item_tokens
    if not common:
        return 0.0
    precision = len(common) / len(query_tokens)
    recall = len(common) / len(item_tokens)
    return (precision * 0.7) + (recall * 0.3)


def retrieve_context(
    *,
    items: Sequence[Dict[str, Any]],
    needs: Optional[Dict[str, Any]] = None,
    query: str | None = None,
    tenant: Optional[int] = None,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    if not items:
        return []

    search_text = _compose_query_text(needs or {}, query)
    if not search_text.strip():
        return []

    index = ensure_catalog_index(tenant, items)
    if not index:
        return []

    matrix = getattr(index, "matrix", None)
    if matrix is not None:
        rows = getattr(matrix, "shape", (0,))
        if not rows or rows[0] == 0:
            return []

    tokens = _extract_tokens(search_text)
    query_token_set = set(tokens)

    if linear_kernel is not None and getattr(index, "vectorizer", None) is not None and matrix is not None:
        query_vec = index.vectorizer.transform([search_text])
        scores_row = linear_kernel(query_vec, matrix)[0]
        if hasattr(scores_row, "tolist"):
            scores_list = list(scores_row.tolist())
        elif isinstance(scores_row, list):
            scores_list = scores_row
        else:
            scores_list = list(scores_row)
    else:
        token_sets = getattr(index, "tokens", [])
        scores_list = [
            _token_overlap_score(query_token_set, token_sets[idx] if idx < len(token_sets) else set())
            for idx in range(len(index.items))
        ]

    if not scores_list:
        return []

    scored_indices = sorted(
        range(len(scores_list)), key=lambda idx: scores_list[idx], reverse=True
    )

    threshold = _score_threshold(len(index.items))
    results: List[Dict[str, Any]] = []
    for idx in scored_indices:
        score = float(scores_list[idx])
        if math.isclose(score, 0.0) or score < threshold:
            if results:
                break
            continue
        excerpt = _highlight_excerpt(index.texts[idx], tokens)
        results.append(_attach_metadata(index.items[idx], score, excerpt))
        if 0 < limit <= len(results):
            break

    if results:
        return results

    # If nothing crosses the threshold, surface top item to avoid empty context
    best_idx = scored_indices[0]
    best_excerpt = _highlight_excerpt(index.texts[best_idx], tokens)
    return [_attach_metadata(index.items[best_idx], float(scores_list[best_idx]), best_excerpt)]
