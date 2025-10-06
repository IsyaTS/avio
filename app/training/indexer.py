from __future__ import annotations

import csv
import dataclasses
import hashlib
import io
import json
import pathlib
import pickle
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import logging
from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore

_log = logging.getLogger("training")
_LOG_PREFIX = "[training]"


@dataclasses.dataclass
class TrainingExample:
    q: str
    a: str
    meta: Dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class TrainingIndex:
    vectorizer: TfidfVectorizer
    matrix: Any
    items: List[TrainingExample]
    created_at: int
    sha1: str

    def save(self, path: pathlib.Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as fh:
            pickle.dump(self, fh)

    @staticmethod
    def load(path: pathlib.Path) -> "TrainingIndex":
        with path.open("rb") as fh:
            return pickle.load(fh)


def _norm_text(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _anonymize(text: str) -> str:
    """Light PII scrubbing: emails, WhatsApp JIDs and long numbers."""
    if not text:
        return ""
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "<EMAIL>", text)
    text = re.sub(r"\b\d{5,}@s\.whatsapp\.net\b", "<WA_ID>", text)
    text = re.sub(r"(?<!\d)\d{5,}(?!\d)", "<NUMBER>", text)
    return text


def _postprocess_examples(examples: list["TrainingExample"], *, min_chars: int = 15) -> list["TrainingExample"]:
    """Filter by minimal length, anonymize, and deduplicate by normalized hash."""
    seen: set[str] = set()
    out: list[TrainingExample] = []
    for e in examples:
        q = _anonymize(_norm_text(e.q))
        a = _anonymize(_norm_text(e.a))
        if len(q) < min_chars or len(a) < min_chars:
            continue
        sig = hashlib.sha1(f"{q}\t{a}".encode("utf-8", errors="ignore")).hexdigest()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(TrainingExample(q=q, a=a, meta=dict(e.meta or {})))
    return out


def _extract_pairs_from_messages(messages: List[Dict[str, Any]]) -> Iterable[Tuple[str, str]]:
    last_user: Optional[str] = None
    for msg in messages:
        role = (msg.get("role") or "").strip().lower()
        content = _norm_text(msg.get("content") or msg.get("text") or "")
        if not content:
            continue
        if role in {"user", "client", "customer"}:
            last_user = content
        elif role in {"assistant", "agent", "manager", "bot"} and last_user:
            yield last_user, content
            last_user = None


def parse_jsonl(payload: bytes) -> List[TrainingExample]:
    examples: List[TrainingExample] = []
    fmt_counts = {"qna": 0, "messages": 0}
    for raw_line in io.StringIO(payload.decode("utf-8", errors="ignore")):
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        # Format A: {"q": "...", "a": "..."}
        q = _norm_text(obj.get("q") or obj.get("question")) if isinstance(obj, dict) else ""
        a = _norm_text(obj.get("a") or obj.get("answer")) if isinstance(obj, dict) else ""
        if q and a:
            examples.append(TrainingExample(q=q, a=a, meta={"source": "jsonl:qna"}))
            fmt_counts["qna"] += 1
            continue
        # Format B: {"messages": [{"role": "user", ...}, {"role": "assistant", ...}, ...]}
        if isinstance(obj, dict) and isinstance(obj.get("messages"), list):
            for qv, av in _extract_pairs_from_messages(obj["messages"]):
                examples.append(TrainingExample(q=qv, a=av, meta={"source": "jsonl:messages"}))
                fmt_counts["messages"] += 1
            continue
    cleaned = _postprocess_examples(examples)
    try:
        _log.info(f"{_LOG_PREFIX} parsed jsonl pairs=%s formats=%s", len(cleaned), fmt_counts)
    except Exception:
        pass
    return cleaned


def parse_json(payload: bytes) -> List[TrainingExample]:
    try:
        data = json.loads(payload.decode("utf-8", errors="ignore"))
    except Exception:
        return []
    examples: List[TrainingExample] = []
    if isinstance(data, list):
        # Either list of {q,a} or list of {messages: [...]}
        for item in data:
            if not isinstance(item, dict):
                continue
            q = _norm_text(item.get("q") or item.get("question"))
            a = _norm_text(item.get("a") or item.get("answer"))
            if q and a:
                examples.append(TrainingExample(q=q, a=a, meta={"source": "json:qna"}))
                continue
            messages = item.get("messages")
            if isinstance(messages, list):
                for qv, av in _extract_pairs_from_messages(messages):
                    examples.append(TrainingExample(q=qv, a=av, meta={"source": "json:messages"}))
    elif isinstance(data, dict) and isinstance(data.get("messages"), list):
        for qv, av in _extract_pairs_from_messages(data["messages"]):
            examples.append(TrainingExample(q=qv, a=av, meta={"source": "json:messages"}))
    cleaned = _postprocess_examples(examples)
    try:
        _log.info(f"{_LOG_PREFIX} parsed json pairs=%s", len(cleaned))
    except Exception:
        pass
    return cleaned


def parse_csv(payload: bytes) -> List[TrainingExample]:
    text = payload.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    candidates = ("q", "question", "user", "client")
    answers = ("a", "answer", "assistant", "agent", "manager", "bot")
    examples: List[TrainingExample] = []
    for row in reader:
        q = ""
        a = ""
        for k in candidates:
            if k in row and row[k]:
                q = _norm_text(row[k])
                break
        for k in answers:
            if k in row and row[k]:
                a = _norm_text(row[k])
                break
        if q and a:
            examples.append(TrainingExample(q=q, a=a, meta={"source": "csv"}))
    cleaned = _postprocess_examples(examples)
    try:
        _log.info(f"{_LOG_PREFIX} parsed csv pairs=%s", len(cleaned))
    except Exception:
        pass
    return cleaned


def build_index(examples: List[TrainingExample]) -> Optional[TrainingIndex]:
    clean = [e for e in examples if e.q and e.a]
    if not clean:
        return None
    texts = [e.q for e in clean]
    vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 2), min_df=1)
    matrix = vectorizer.fit_transform(texts)
    joined = "\n".join(f"{e.q}\t{e.a}" for e in clean)
    sha1 = hashlib.sha1(joined.encode("utf-8", errors="ignore")).hexdigest()
    idx = TrainingIndex(vectorizer=vectorizer, matrix=matrix, items=clean, created_at=int(time.time()), sha1=sha1)
    try:
        _log.info(f"{_LOG_PREFIX} built index pairs=%s sha1=%s", len(clean), sha1)
    except Exception:
        pass
    return idx


def save_manifest(index: TrainingIndex, index_path: pathlib.Path, source_relpath: str, original_name: str) -> Dict[str, Any]:
    manifest = {
        "type": "training",
        "sha1": index.sha1,
        "created_at": index.created_at,
        "pairs": len(index.items),
        "index_path": str(index_path),
        "source_path": source_relpath,
        "original": original_name,
    }
    manifest_path = index_path.with_suffix(".manifest.json")
    try:
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            size = index_path.stat().st_size if index_path.exists() else 0
        except Exception:
            size = 0
        _log.info(f"{_LOG_PREFIX} index saved path=%s size=%sB", str(index_path), size)
    except Exception:
        _log.exception(f"{_LOG_PREFIX} manifest_write_failed path=%s", str(manifest_path), exc_info=True)
    return manifest
