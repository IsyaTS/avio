from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Sequence


@dataclass(frozen=True)
class FactQuestionRuntimeDeps:
    canonical_fact_key: Callable[[str], str]
    normalize_text: Callable[[Any], str]
    item_label: Callable[[Mapping[str, Any]], str]
    normalize_model_alias: Callable[[str], str]
    sentence_split_re: Any
    persona_question_for_fact: Callable[[str, str], str]
    persona_primary_script_question: Callable[[str, Any], str]


class FactQuestionRuntime:
    def __init__(self, deps: FactQuestionRuntimeDeps) -> None:
        self.deps = deps

    def normalize_required_facts(self, raw: Any) -> List[str]:
        if isinstance(raw, str):
            items = [raw]
        elif isinstance(raw, Sequence):
            items = [str(x) for x in raw]
        else:
            items = []
        out: List[str] = []
        for item in items:
            key = self.deps.canonical_fact_key(item)
            if key and key not in out:
                out.append(key)
        return out[:12]

    def missing_required_facts(self, required: Sequence[str], facts: Mapping[str, str]) -> List[str]:
        if not required:
            return []
        missing: List[str] = []
        normalized_facts: Dict[str, str] = {}
        for raw_key, raw_val in dict(facts or {}).items():
            key = self.deps.canonical_fact_key(str(raw_key))
            val = str(raw_val or "").strip()
            if key and val:
                normalized_facts[key] = val
        for raw in required:
            key = self.deps.canonical_fact_key(raw)
            if not key:
                continue
            if not str(normalized_facts.get(key) or "").strip():
                missing.append(key)
        return missing

    def prioritize_missing_facts(self, missing: Sequence[str], *, turn_intent: str = "") -> List[str]:
        items = [self.deps.canonical_fact_key(item) for item in missing]
        clean = [item for item in items if item]
        if not clean:
            return []
        if str(turn_intent or "").strip().lower() == "order":
            order = (
                "contact",
                "city",
                "address",
                "object_type",
                "model",
                "budget",
                "timeline",
                "dimensions",
            )
        else:
            order = (
                "city",
                "object_type",
                "address",
                "model",
                "budget",
                "timeline",
                "dimensions",
                "contact",
            )
        weight = {key: idx for idx, key in enumerate(order)}
        return sorted(clean, key=lambda key: (weight.get(key, len(order)), clean.index(key)))

    def question_covers_fact(self, question: str, fact_key: str) -> bool:
        low = str(question or "").lower()
        key = self.deps.canonical_fact_key(fact_key)
        if not low or not key:
            return False
        token_map: Dict[str, tuple[str, ...]] = {
            "city": ("город", "населен", "локац"),
            "address": ("адрес", "улиц", "подъезд", "дом", "корп", "кв."),
            "object_type": ("квартир", "дом", "помещени"),
            "model": ("каталог", "модель", "вариант"),
            "dimensions": ("размер", "проем", "проём", "замер", "фото проема", "фото проёма"),
            "budget": ("бюдж", "цена", "стоим"),
            "timeline": ("срок", "когда", "дата", "сегодня", "завтра"),
            "contact": ("контакт", "телефон", "мессендж"),
        }
        for token in token_map.get(key, (key,)):
            if token in low:
                return True
        return False

    def question_lists_catalog_options(
        self,
        question: str,
        items: Sequence[Mapping[str, Any]],
    ) -> bool:
        low = self.deps.normalize_text(question)
        if not low or "или" not in low:
            return False
        if not items:
            return False

        hits: set[str] = set()
        for item in list(items)[:120]:
            probes: list[str] = []
            label = str(self.deps.item_label(item) or "").strip()
            if label:
                probes.append(self.deps.normalize_model_alias(label))
                label_tokens = [tok for tok in self.deps.normalize_model_alias(label).split() if tok]
                if len(label_tokens) >= 2:
                    probes.append(" ".join(label_tokens[:2]))
            color_val = str(item.get("color") or "").strip()
            if color_val:
                probes.append(self.deps.normalize_model_alias(color_val))
            for probe in probes:
                clean = str(probe or "").strip()
                if len(clean) < 3:
                    continue
                if clean in low:
                    hits.add(clean)
                    if len(hits) >= 2:
                        return True
        return False

    def replace_reply_question(self, reply: str, new_question: str) -> str:
        candidate = str(reply or "").strip()
        question = str(new_question or "").strip()
        if not question:
            return candidate
        if not candidate:
            return question
        parts = [part.strip() for part in self.deps.sentence_split_re.split(candidate) if part.strip()]
        if not parts:
            return question
        kept = [part for part in parts if "?" not in part]
        if kept:
            return " ".join(kept + [question]).strip()
        return question

    def generic_question_for_fact(self, fact_key: str) -> str:
        key = self.deps.canonical_fact_key(fact_key)
        prompts = {
            "city": "Подскажите, пожалуйста, в каком городе нужна установка?",
            "object_type": "Подскажите тип объекта: квартира или частный дом?",
            "address": "Подскажите, пожалуйста, адрес установки.",
            "model": "Подскажите, какая модель или тип из каталога интересует?",
            "budget": "Какой бюджет рассматриваете?",
            "timeline": "Когда планируете установку?",
            "dimensions": "Подскажите размеры проема, если уже есть замер.",
            "contact": "Оставьте, пожалуйста, удобный контакт для связи.",
        }
        return str(prompts.get(str(key or "").strip(), "") or "").strip()

    def persona_driven_question_for_fact(
        self,
        persona_context: str,
        fact_key: str,
        *,
        state: Any | None = None,
    ) -> str:
        question = self.deps.persona_question_for_fact(persona_context, fact_key)
        if question:
            return question
        script_question = self.deps.persona_primary_script_question(persona_context, state)
        if script_question and self.question_covers_fact(script_question, fact_key):
            return script_question
        return self.generic_question_for_fact(fact_key)
