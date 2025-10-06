from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Optional, Tuple

from .indexer import TrainingIndex, TrainingExample
from core import tenant_dir, read_tenant_config


_CACHE: Dict[int, Tuple[pathlib.Path, TrainingIndex]] = {}
_log = logging.getLogger("training")
_LOG_PREFIX = "[training]"


def _latest_index_path(tenant: int) -> Optional[pathlib.Path]:
    base = pathlib.Path(tenant_dir(tenant))
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
        _log.info(f"{_LOG_PREFIX} index loaded tenant=%s path=%s size=%sB pairs=%s", tenant, str(path), size, len(idx.items))
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
    cfg = read_tenant_config(tenant)
    learn = cfg.get("learning") if isinstance(cfg, dict) else {}
    try:
        min_chars = max(0, int((learn or {}).get("min_chars", 15)))
    except Exception:
        min_chars = 15
    try:
        top_k = max(1, int((learn or {}).get("top_k", k)))
    except Exception:
        top_k = k

    idx = ensure_training_index(tenant)
    if not idx or not (query or "").strip():
        return []
    try:
        q_vec = idx.vectorizer.transform([query])
        import numpy as np  # type: ignore

        scores = (q_vec @ idx.matrix.T).toarray().ravel()
        order = np.argsort(-scores)
        out: List[RetrievedExample] = []
        for i in order:
            ex = idx.items[int(i)]
            if len(ex.q.strip()) < min_chars or len(ex.a.strip()) < min_chars:
                continue
            score = float(scores[int(i)])
            # lightweight floor: skip zero/negative matches
            if score <= 0:
                continue
            out.append(RetrievedExample(q=ex.q, a=ex.a, score=score, meta=ex.meta))
            if len(out) >= top_k:
                break
        _log.debug(f"{_LOG_PREFIX} retrieve tenant=%s query_len=%s returned=%s", tenant, len(query or ""), len(out))
        return out
    except Exception:
        _log.exception(f"{_LOG_PREFIX} retrieve_failed tenant=%s", tenant, exc_info=True)
        return []


def build_examples_block(tenant: int, query: str) -> str:
    """Return a formatted block for the system prompt with 1–2 best examples."""
    cfg = read_tenant_config(tenant)
    learn = cfg.get("learning") if isinstance(cfg, dict) else {}
    try:
        top_k = max(1, min(2, int((learn or {}).get("top_k", 2))))
    except Exception:
        top_k = 2
    results = retrieve_examples(tenant, query, k=top_k)
    if not results:
        return ""
    lines: List[str] = ["Примеры обучающих диалогов:"]
    for ex in results[:top_k]:
        q = (ex.q or "").strip()
        a = (ex.a or "").strip()
        if not q or not a:
            continue
        lines.append(f"Клиент: {q}")
        lines.append(f"Менеджер: {a}")
    block = "\n".join(lines)
    return block.strip()
