from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List


@dataclass(frozen=True)
class PersonaScriptRuntimeDeps:
    normalize_text: Callable[[Any], str]
    line_to_question: Callable[[str], str]
    is_operator_like_question: Callable[[str], bool]
    is_repeated_question_against_state: Callable[[str, Any], bool]


class PersonaScriptRuntime:
    def __init__(self, deps: PersonaScriptRuntimeDeps) -> None:
        self.deps = deps

    def fact_keys_from_line(self, line: str) -> List[str]:
        low = str(line or "").lower().replace("ё", "е")
        if not low:
            return []
        mapping: Dict[str, tuple[str, ...]] = {
            "city": ("город", "населен", "локац"),
            "address": ("адрес", "улиц", "подъезд", "дом", "корп", "кв."),
            "object_type": ("квартир", "частный дом", "тип помещения", "тип объекта", "объект"),
            "model": ("что из каталога", "модель", "вариант"),
            "dimensions": ("размер", "проем", "проём", "фото проема", "фото проёма", "замер"),
            "budget": ("бюджет", "стоим", "цена"),
            "timeline": ("срок", "сегодня", "завтра", "дата"),
            "contact": ("контакт", "телефон", "мессендж"),
        }
        keys: List[str] = []
        for fact_key, tokens in mapping.items():
            if any(token in low for token in tokens):
                keys.append(fact_key)
        return keys

    def persona_rules_cache_key(self, persona_text: str) -> str:
        raw = str(persona_text or "")
        return hashlib.sha1(raw.encode("utf-8")).hexdigest() if raw else ""

    def extract_primary_script_lines(self, persona_text: str) -> List[str]:
        lines = [ln.strip() for ln in str(persona_text or "").splitlines()]
        primary_lines: List[str] = []
        in_primary_block = False
        for line in lines:
            if not line:
                continue
            low = line.lower().replace("ё", "е")
            is_heading = low.startswith("#")
            if is_heading and in_primary_block:
                in_primary_block = False
            if any(
                token in low
                for token in ("диалог-скрипт", "скрипт диалога", "последовательно уточни", "порядок диалога")
            ):
                in_primary_block = True
                continue
            if in_primary_block:
                if (
                    re.match(r"^\d+\)", low)
                    or re.match(r"^\d+\.", low)
                    or low.startswith("-")
                    or low.startswith("•")
                ):
                    primary_lines.append(line)
                    continue
                primary_lines.append(line)
        return primary_lines

    def persona_script_questions(self, persona_text: str) -> List[str]:
        _line_to_question = self.deps.line_to_question
        _is_operator_like_question = self.deps.is_operator_like_question
        _normalize_text = self.deps.normalize_text

        candidates = self.extract_primary_script_lines(persona_text)
        if not candidates:
            for line in str(persona_text or "").splitlines():
                clean = str(line or "").strip()
                if not clean:
                    continue
                low = clean.lower().replace("ё", "е")
                if any(
                    token in low
                    for token in (
                        "уточни",
                        "уточняй",
                        "спроси",
                        "спрашивай",
                        "узнай",
                        "узнавай",
                        "получи",
                        "получай",
                        "собери",
                        "собирай",
                        "попроси",
                        "выясни",
                        "выясняй",
                    )
                ):
                    candidates.append(clean)
        questions: List[str] = []
        seen: set[str] = set()
        for raw in candidates:
            question = _line_to_question(raw)
            if not question:
                continue
            if _is_operator_like_question(question):
                continue
            key = _normalize_text(question)
            if not key or key in seen:
                continue
            seen.add(key)
            questions.append(question)
        return questions

    def persona_primary_script_question(
        self,
        persona_text: str,
        *,
        state: Any | None = None,
    ) -> str:
        _is_repeated_question_against_state = self.deps.is_repeated_question_against_state

        questions = self.persona_script_questions(persona_text)
        for question in questions:
            if not question:
                continue
            if state is not None and _is_repeated_question_against_state(question, state):
                continue
            return question
        if questions and state is None:
            return questions[0]
        return ""

    def persona_catalog_unavailable_reply(self, persona_text: str) -> str:
        _normalize_text = self.deps.normalize_text

        lines = [str(line or "").strip() for line in str(persona_text or "").splitlines() if str(line or "").strip()]
        for line in lines:
            low = _normalize_text(line)
            if "каталог" not in low:
                continue
            if not (
                "не открыл" in low
                or "не открыва" in low
                or "пока не открыл" in low
                or "груз" in low
            ):
                continue
            quoted = [
                str(part or "").strip()
                for part in re.findall(r"[\"«]([^\"»]{8,260})[\"»]", line)
                if str(part or "").strip()
            ]
            if quoted:
                reply = quoted[0].strip()
                if reply and reply[-1] not in ".!?":
                    reply += "."
                return reply
            cleaned = re.sub(r"^[\-\s•\d\).:]+", "", line).strip()
            cleaned = re.sub(r"(?iu)^нормально:\s*", "", cleaned).strip()
            if cleaned and cleaned[-1] not in ".!?":
                cleaned += "."
            return cleaned
        return ""

    def explain_missing_fact_need(
        self,
        fact_key: str,
        *,
        canonical_fact_key: Callable[[str], str],
        persona_context: str = "",
    ) -> str:
        _ = str(persona_context or "")
        key = canonical_fact_key(fact_key)
        if not key:
            return ""
        overrides = {
            "city": "Город нужен, чтобы сразу подсказать адрес магазина, условия и ближайший вариант установки.",
            "object_type": "Уточняю тип объекта, потому что для квартиры и частного дома подходят разные варианты.",
            "address": "Адрес нужен, чтобы не ошибиться по выезду, установке и дальнейшему подбору.",
            "model": "Хочу понять, какой вариант Вам ближе, чтобы не промахнуться по цене и характеристикам.",
            "contact": "Контакт нужен, чтобы подтвердить заказ и отправить точные детали.",
        }
        reply = str(overrides.get(key) or "").strip()
        if not reply:
            return ""
        if reply[-1] not in ".!?":
            reply += "."
        return reply
