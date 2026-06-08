from __future__ import annotations

import dataclasses
import hashlib
import json
import pathlib
import pickle
import re
import time
from typing import Any, Mapping, Sequence

from libs.core.sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
from libs.core.training import exporter


@dataclasses.dataclass(frozen=True)
class DialogTurn:
    role: str
    text: str


@dataclasses.dataclass(frozen=True)
class DialogTrainingItem:
    dialog_id: str
    turns: list[DialogTurn]
    search_text: str
    meta: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class DialogTrainingIndex:
    vectorizer: TfidfVectorizer
    matrix: Any
    items: list[DialogTrainingItem]
    created_at: int
    sha1: str

    def save(self, path: pathlib.Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            pickle.dump(self, handle)

    @staticmethod
    def load(path: pathlib.Path) -> "DialogTrainingIndex":
        with path.open("rb") as handle:
            return pickle.load(handle)


@dataclasses.dataclass(frozen=True)
class RetrievedDialog:
    dialog_id: str
    score: float
    turns: list[DialogTurn]
    meta: dict[str, Any]


_CACHE: dict[int, tuple[pathlib.Path, DialogTrainingIndex]] = {}
_PRICE_RE = re.compile(r"\b(?:\d{4,6}\s*(?:₽|руб|р\b)?|\d{1,3}\s*(?:тыс|т\.р\.|₽|руб))\b", re.I)
_ADDRESS_RE = re.compile(r"\b(?:улица|ул\.|проспект|пр-т|коммунистическая|менделеева|адрес|магазин)\b", re.I)
_CONTACT_RE = re.compile(r"(?:\[PHONE\]|\[EMAIL\]|\[LINK\]|\[HANDLE\]|телеграм|telegram|ватсап|whatsapp|мах|@)", re.I)
_CITY_HINT_RE = re.compile(r"\b(?:уф[аеыу]?|стерлитамак[аеу]?|салават[аеу]?|ишимба[йеяю]|оренбург[аеу]?|казан[ьи]|в городе|мой город|наш город|район)\b", re.I)
_PRODUCT_HINT_RE = re.compile(r"\b(?:двер|термо|зеркал|дом|квартир|про[её]м|размер|фото|модель)\b", re.I)
_LOCATION_INTENT_RE = re.compile(r"\b(?:где|адрес|магазин|посмотреть|находитесь|находитесь|выбрать|шоурум)\b", re.I)


def build_index_from_dialogs(dialogs: Sequence[Sequence[Mapping[str, str]]]) -> DialogTrainingIndex | None:
    items: list[DialogTrainingItem] = []
    seen: set[str] = set()
    for dialog in dialogs:
        turns = _normalize_turns(dialog)
        if len(turns) < 2:
            continue
        if not any(turn.role == "client" for turn in turns):
            continue
        if not any(turn.role == "manager" for turn in turns):
            continue
        dialog_id = _dialog_id(turns)
        if dialog_id in seen:
            continue
        seen.add(dialog_id)
        search_text = _search_text(turns)
        if not search_text:
            continue
        items.append(DialogTrainingItem(dialog_id=dialog_id, turns=turns, search_text=search_text))
    if not items:
        return None
    vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 2), min_df=1)
    matrix = vectorizer.fit_transform([item.search_text for item in items])
    sha1 = hashlib.sha1("\n".join(f"{item.dialog_id}:{item.search_text}" for item in items).encode("utf-8")).hexdigest()
    return DialogTrainingIndex(vectorizer=vectorizer, matrix=matrix, items=items, created_at=int(time.time()), sha1=sha1)


def build_index_from_markdown(path: str | pathlib.Path) -> DialogTrainingIndex | None:
    dialogs = parse_markdown_dialogs(path)
    return build_index_from_dialogs(dialogs)


def build_index_from_dialog_dataset(path: str | pathlib.Path) -> DialogTrainingIndex | None:
    dialogs = parse_dialog_dataset_jsonl(path)
    return build_index_from_dialogs(dialogs)


def parse_markdown_dialogs(path: str | pathlib.Path) -> list[list[dict[str, str]]]:
    current: list[dict[str, str]] = []
    dialogs: list[list[dict[str, str]]] = []
    text = pathlib.Path(path).read_text(encoding="utf-8")
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            if current:
                dialogs.append(current)
            current = []
            continue
        if line.startswith("Клиент:"):
            current.append({"role": "client", "text": line.split(":", 1)[1].strip()})
        elif line.startswith("Менеджер:"):
            current.append({"role": "manager", "text": line.split(":", 1)[1].strip()})
    if current:
        dialogs.append(current)
    return dialogs


def parse_dialog_dataset_jsonl(path: str | pathlib.Path) -> list[list[dict[str, str]]]:
    dialogs: list[list[dict[str, str]]] = []
    for raw in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        turns = obj.get("dialog") if isinstance(obj, dict) else None
        if not isinstance(turns, list):
            continue
        dialog: list[dict[str, str]] = []
        for turn in turns:
            if not isinstance(turn, Mapping):
                continue
            role = str(turn.get("role") or "").strip().lower()
            text = str(turn.get("text") or "").strip()
            if role in {"client", "manager"} and text:
                dialog.append({"role": role, "text": text})
        if dialog:
            dialogs.append(dialog)
    return dialogs


def save_dialog_training_index(index: DialogTrainingIndex, *, tenant_dir: pathlib.Path) -> pathlib.Path:
    path = tenant_dir / "indexes" / f"dialog_training_{index.sha1}.pkl"
    index.save(path)
    manifest = {
        "type": "dialog_training",
        "sha1": index.sha1,
        "created_at": index.created_at,
        "dialogs": len(index.items),
        "index_path": str(path),
    }
    path.with_suffix(".manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    _CACHE.pop(_tenant_id_from_dir(tenant_dir), None)
    return path


def _tenant_id_from_dir(tenant_dir: pathlib.Path) -> int:
    try:
        return int(pathlib.Path(tenant_dir).name)
    except Exception:
        return 0


def _index_sort_key(path: pathlib.Path) -> tuple[int, float, str]:
    manifest_created_at = 0
    manifest = path.with_suffix(".manifest.json")
    if manifest.exists():
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            manifest_created_at = int(data.get("created_at") or 0)
        except Exception:
            manifest_created_at = 0
    try:
        mtime = float(path.stat().st_mtime)
    except Exception:
        mtime = 0.0
    return manifest_created_at, mtime, path.name


def ensure_dialog_index(tenant: int, *, tenant_dir_fn: Any) -> DialogTrainingIndex | None:
    try:
        base = pathlib.Path(tenant_dir_fn(int(tenant)))
    except Exception:
        return None
    idx_dir = base / "indexes"
    if not idx_dir.exists():
        return None
    candidates = sorted(idx_dir.glob("dialog_training_*.pkl"))
    if not candidates:
        return None
    path = max(candidates, key=_index_sort_key)
    cached = _CACHE.get(int(tenant))
    if cached and cached[0] == path:
        return cached[1]
    try:
        index = DialogTrainingIndex.load(path)
    except Exception:
        return None
    _CACHE[int(tenant)] = (path, index)
    return index


def retrieve_dialogs(
    tenant: int,
    query: str,
    *,
    tenant_dir_fn: Any,
    top_k: int = 2,
    min_score: float = 0.08,
) -> list[RetrievedDialog]:
    index = ensure_dialog_index(int(tenant), tenant_dir_fn=tenant_dir_fn)
    if index is None or not str(query or "").strip():
        return []
    q_vec = index.vectorizer.transform([exporter.scrub(query)])
    scores = _score_matrix(q_vec, index.matrix)
    results: list[RetrievedDialog] = []
    for idx, score in scores:
        if score < min_score or idx >= len(index.items):
            continue
        item = index.items[idx]
        results.append(RetrievedDialog(dialog_id=item.dialog_id, score=score, turns=item.turns, meta=dict(item.meta)))
        if len(results) >= top_k:
            break
    return results


def build_dialog_examples_block(
    tenant: int,
    query: str,
    *,
    tenant_dir_fn: Any,
    top_k: int = 2,
    min_score: float = 0.08,
    max_chars: int = 3500,
) -> str:
    results = retrieve_dialogs(
        int(tenant),
        query,
        tenant_dir_fn=tenant_dir_fn,
        top_k=top_k,
        min_score=min_score,
    )
    if not results:
        return ""
    query_facts = _query_facts(query)
    lines = [
        "Похожие реальные диалоги менеджера из обучающего набора.",
        "Используй их как стиль и сценарий общения, но не копируй цены, адреса, контакты и условия, если они не подтверждены текущим диалогом.",
        "Если в похожем диалоге ответ зависел от города, товара, размера, фото или других условий, а в текущем диалоге этих данных нет — сначала уточни недостающий параметр.",
    ]
    lines.extend(_guard_lines(query_facts))
    for number, result in enumerate(results, start=1):
        lines.append(f"Диалог {number}:")
        for turn in _safe_window(result.turns, query_facts=query_facts):
            label = "Клиент" if turn.role == "client" else "Менеджер"
            lines.append(f"{label}: {turn.text}")
        block = "\n".join(lines)
        if len(block) >= max_chars:
            break
    return "\n".join(lines)[:max_chars].strip()


def _normalize_turns(dialog: Sequence[Mapping[str, str]]) -> list[DialogTurn]:
    turns: list[DialogTurn] = []
    for item in dialog:
        role = str(item.get("role") or "").strip().lower()
        if role in {"user", "customer", "client"}:
            role = "client"
        elif role in {"assistant", "agent", "manager"}:
            role = "manager"
        else:
            continue
        text = _clean(exporter.scrub(str(item.get("text") or item.get("content") or "")))
        if not text:
            continue
        if turns and turns[-1].role == role:
            turns[-1] = DialogTurn(role=role, text=_clean(f"{turns[-1].text} {text}"))
        else:
            turns.append(DialogTurn(role=role, text=text))
    return turns


def _safe_window(turns: Sequence[DialogTurn], *, query_facts: Mapping[str, bool]) -> list[DialogTurn]:
    limited = list(turns[:8])
    safe: list[DialogTurn] = []
    for turn in limited:
        text = turn.text
        if turn.role == "manager" and _contains_conditional_fact(text) and not _facts_allow_fact(text, query_facts):
            text = _mask_conditional_fact_text(text)
        safe.append(DialogTurn(role=turn.role, text=text))
    return safe


def _search_text(turns: Sequence[DialogTurn]) -> str:
    client_text = " ".join(turn.text for turn in turns if turn.role == "client")
    manager_text = " ".join(turn.text for turn in turns if turn.role == "manager")
    return _clean(f"{client_text} {manager_text[:1000]}")


def _dialog_id(turns: Sequence[DialogTurn]) -> str:
    signature = "\n".join(f"{turn.role}:{turn.text}" for turn in turns)
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _score_matrix(q_vec: Any, matrix: Any) -> list[tuple[int, float]]:
    if hasattr(matrix, "vectors") and hasattr(q_vec, "vectors"):
        docs = getattr(matrix, "vectors", []) or []
        queries = getattr(q_vec, "vectors", []) or []
        query = queries[0] if queries else {}
        scored: list[tuple[int, float]] = []
        for idx, doc in enumerate(docs):
            score = 0.0
            if len(query) <= len(doc):
                for term, weight in query.items():
                    score += float(weight) * float(doc.get(term, 0.0))
            else:
                for term, weight in doc.items():
                    score += float(weight) * float(query.get(term, 0.0))
            if score > 0:
                scored.append((idx, score))
        scored.sort(key=lambda item: item[1], reverse=True)
        return scored
    import numpy as np  # type: ignore

    raw_scores = (q_vec @ matrix.T).toarray().ravel()
    return [(int(i), float(raw_scores[int(i)])) for i in np.argsort(-raw_scores) if raw_scores[int(i)] > 0]


def _query_facts(query: str) -> dict[str, bool]:
    text = str(query or "")
    return {
        "city": bool(_CITY_HINT_RE.search(text)),
        "product": bool(_PRODUCT_HINT_RE.search(text)),
        "price": bool(_PRICE_RE.search(text) or "цен" in text.lower() or "сто" in text.lower()),
        "location_intent": bool(_LOCATION_INTENT_RE.search(text)),
    }


def _guard_lines(query_facts: Mapping[str, bool]) -> list[str]:
    lines: list[str] = []
    if query_facts.get("location_intent") and not query_facts.get("city"):
        lines.append(
            "ВАЖНО: в текущем диалоге город клиента не определён. "
            "Если клиент спрашивает, где посмотреть или где находится магазин, сначала уточни город; "
            "не выбирай адрес, скидку, филиал или контакт из примеров, каталога или персоны без подтверждённого города."
        )
    return lines


def _contains_conditional_fact(text: str) -> bool:
    return bool(_PRICE_RE.search(text) or _ADDRESS_RE.search(text) or _CONTACT_RE.search(text))


def _facts_allow_fact(text: str, query_facts: Mapping[str, bool]) -> bool:
    if _PRICE_RE.search(text) and not query_facts.get("product"):
        return False
    if _ADDRESS_RE.search(text) and not query_facts.get("city"):
        return False
    if _CONTACT_RE.search(text) and not (query_facts.get("product") or query_facts.get("city")):
        return False
    return True


def _mask_conditional_fact_text(text: str) -> str:
    return (
        "В похожем диалоге менеджер дал ответ с условиями, которые зависят от контекста. "
        "В текущем диалоге сначала уточни недостающие данные, не копируй факт дословно."
    )


def _clean(text: str) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split()).strip()


__all__ = [
    "DialogTrainingIndex",
    "DialogTrainingItem",
    "DialogTurn",
    "RetrievedDialog",
    "build_dialog_examples_block",
    "build_index_from_dialog_dataset",
    "build_index_from_dialogs",
    "build_index_from_markdown",
    "parse_dialog_dataset_jsonl",
    "parse_markdown_dialogs",
    "retrieve_dialogs",
    "save_dialog_training_index",
]
