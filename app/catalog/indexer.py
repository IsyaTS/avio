from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    try:
        from app.sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
    except ImportError:
        TfidfVectorizer = None  # type: ignore[assignment]


@dataclass
class CatalogIndex:
    """In-memory representation of catalog data."""

    tenant: Optional[int]
    signature: str
    vectorizer: Any | None
    matrix: Any
    items: List[Dict[str, Any]]
    texts: List[str]
    tokens: List[set[str]]


_INDEX_CACHE: Dict[Tuple[Optional[int], str], CatalogIndex] = {}
_INDEX_LATEST: Dict[Optional[int], CatalogIndex] = {}
_LOCK = threading.Lock()


def _catalog_signature(items: Sequence[Dict[str, Any]]) -> str:
    digest = hashlib.sha1()
    digest.update(str(len(items)).encode("utf-8"))
    for item in items:
        try:
            payload = json.dumps(item, sort_keys=True, ensure_ascii=False)
        except Exception:
            payload = str(item)
        digest.update(payload.encode("utf-8"))
    return digest.hexdigest()


def _collect_text(item: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in (
        "title",
        "name",
        "description",
        "features",
        "tags",
        "brand",
        "material",
        "category",
        "color",
        "notes",
    ):
        value = item.get(key)
        if isinstance(value, (list, tuple, set)):
            parts.extend(str(v) for v in value if v)
        elif value not in (None, ""):
            parts.append(str(value))
    return " ".join(parts)


def _build_texts(items: Sequence[Dict[str, Any]]) -> List[str]:
    return [_collect_text(item) for item in items]


_WORD_RE = re.compile(r"[\wёЁ]+", re.UNICODE)


def _tokenize(text: str) -> set[str]:
    tokens = {match.group(0).lower() for match in _WORD_RE.finditer(text)}
    return {token for token in tokens if len(token) >= 3}


def ensure_catalog_index(tenant: Optional[int], items: Sequence[Dict[str, Any]]) -> Optional[CatalogIndex]:
    if not items:
        return None

    signature = _catalog_signature(items)
    cache_key = (tenant, signature)

    with _LOCK:
        cached = _INDEX_CACHE.get(cache_key)
        if cached:
            _INDEX_LATEST[tenant] = cached
            return cached

    texts = _build_texts(items)
    if not any(texts):
        return None

    tokens = [_tokenize(text) for text in texts]

    if TfidfVectorizer is not None:
        vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 2), min_df=1)
        matrix = vectorizer.fit_transform(texts)
    else:
        vectorizer = None
        matrix = None

    index = CatalogIndex(
        tenant=tenant,
        signature=signature,
        vectorizer=vectorizer,
        matrix=matrix,
        items=list(items),
        texts=texts,
        tokens=tokens,
    )

    with _LOCK:
        _INDEX_CACHE[cache_key] = index
        _INDEX_LATEST[tenant] = index

    return index


def invalidate_catalog_index(tenant: Optional[int]) -> None:
    with _LOCK:
        latest = _INDEX_LATEST.pop(tenant, None)
        if latest:
            _INDEX_CACHE.pop((tenant, latest.signature), None)


def clear_catalog_cache() -> None:
    with _LOCK:
        _INDEX_CACHE.clear()
        _INDEX_LATEST.clear()
