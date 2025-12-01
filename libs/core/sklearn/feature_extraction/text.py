"""Minimal TF-IDF implementation used to avoid heavy sklearn dependency."""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

_WORD_RE = re.compile(r"[\w\-]+", re.UNICODE)


@dataclass
class _Matrix:
    vectors: List[Dict[str, float]]
    vocab_size: int

    @property
    def shape(self) -> tuple[int, int]:
        return (len(self.vectors), self.vocab_size)


class TfidfVectorizer:
    """Small subset of sklearn's TF-IDF vectorizer.

    It implements just enough for the project's catalog retrieval tests and
    intentionally skips rarely used advanced options.
    """

    def __init__(
        self,
        *,
        analyzer: str = "word",
        ngram_range: tuple[int, int] = (1, 1),
        min_df: int = 1,
    ) -> None:
        if analyzer != "word":
            raise ValueError("Only analyzer='word' is supported in shim")
        if ngram_range[0] <= 0 or ngram_range[0] > ngram_range[1]:
            raise ValueError("Invalid ngram_range")
        self.ngram_range = ngram_range
        self.min_df = max(1, int(min_df))
        self.vocabulary_: Dict[str, int] = {}
        self._idf: Dict[str, float] = {}
        self._doc_count = 0

    # Public API ---------------------------------------------------------
    def fit_transform(self, raw_documents: Sequence[str]) -> _Matrix:
        tokens_per_doc = [self._tokenize(doc) for doc in raw_documents]
        self._build_vocabulary(tokens_per_doc)
        vectors = [self._vectorize(tokens) for tokens in tokens_per_doc]
        return _Matrix(vectors=vectors, vocab_size=len(self.vocabulary_))

    def transform(self, raw_documents: Sequence[str]) -> _Matrix:
        if not self.vocabulary_:
            raise ValueError("Vectorizer has not been fitted")
        vectors = [self._vectorize(self._tokenize(doc)) for doc in raw_documents]
        return _Matrix(vectors=vectors, vocab_size=len(self.vocabulary_))

    # Internal helpers ---------------------------------------------------
    def _tokenize(self, doc: str) -> List[str]:
        if not doc:
            return []
        words = _WORD_RE.findall(doc.lower())
        if not words:
            return []
        tokens: List[str] = []
        start, end = self.ngram_range
        for size in range(start, end + 1):
            if size == 1:
                tokens.extend(words)
                continue
            if len(words) < size:
                continue
            for idx in range(len(words) - size + 1):
                tokens.append(" ".join(words[idx : idx + size]))
        return tokens

    def _build_vocabulary(self, tokens_per_doc: Sequence[List[str]]) -> None:
        df_counter: Counter[str] = Counter()
        for tokens in tokens_per_doc:
            df_counter.update(set(tokens))
        self._doc_count = max(len(tokens_per_doc), 1)
        vocab_items = [term for term, df in df_counter.items() if df >= self.min_df]
        vocab_items.sort()
        self.vocabulary_ = {term: idx for idx, term in enumerate(vocab_items)}
        self._idf = {
            term: math.log((1 + self._doc_count) / (1 + df_counter[term])) + 1.0
            for term in self.vocabulary_
        }

    def _vectorize(self, tokens: Sequence[str]) -> Dict[str, float]:
        if not tokens or not self.vocabulary_:
            return {}
        counts = Counter(token for token in tokens if token in self.vocabulary_)
        if not counts:
            return {}
        total = sum(counts.values())
        vector: Dict[str, float] = {}
        norm_sq = 0.0
        for term, freq in counts.items():
            idf = self._idf.get(term)
            if idf is None:
                continue
            tf = freq / total if total else 0.0
            value = tf * idf
            vector[term] = value
            norm_sq += value * value
        if norm_sq > 0:
            norm = math.sqrt(norm_sq)
            for term in list(vector.keys()):
                vector[term] /= norm
        return vector


__all__ = ["TfidfVectorizer"]
