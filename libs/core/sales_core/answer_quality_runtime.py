from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class AnswerQualityRuntimeDeps:
    normalize_text: Callable[[str], str]
    sentence_split_re: Any
    question_cue_re: Any
    eta_intent_re: Any
    urgent_today_re: Any
    variants_user_hint_re: Any
    model_name_intent_re: Any
    question_fingerprint_fn: Callable[[str], str]
    is_operator_instruction_sentence: Callable[[str], bool]
    is_response_format_instruction_sentence: Callable[[str], bool]
    is_sequence_process_instruction_sentence: Callable[[str], bool]
    question_covers_fact: Callable[[str, str], bool]
    state_facts_snapshot: Callable[[Any], dict[str, str]]
    max_questions_limit: Callable[[Any], int]
    normalize_catalog_name_case: Callable[[str, Mapping[str, Any] | None], str]
    normalize_shouting_case: Callable[[str], str]
    dedupe_repeated_fact_sentences: Callable[[str, Any], str]
    strip_instruction_leaks: Callable[[str], str]
    grounding_catalog_items: Callable[[Mapping[str, Any] | None], list[dict[str, Any]]]
    is_price_intent: Callable[[str], bool]
    reply_mentions_catalog_item: Callable[[str, Sequence[Mapping[str, Any]]], bool]
    answer_is_too_robotic: Callable[[str], bool]


class AnswerQualityRuntime:
    def __init__(self, deps: AnswerQualityRuntimeDeps) -> None:
        self.deps = deps

    def count_sentences(self, text: str) -> int:
        chunks = [part.strip() for part in re.split(r"[.!?]+", text or "") if part.strip()]
        return len(chunks)

    def normalize_numbered_list_punctuation(self, text: str) -> str:
        candidate = str(text or "").strip()
        if not candidate:
            return candidate
        # Prevent sentence over-splitting on enumerations like "1. ... 2. ..."
        return re.sub(r"(?<![0-9A-Za-zА-Яа-яЁё])(\d{1,2})\.\s+", r"\1) ", candidate)

    def enforce_sentence_budget(
        self,
        text: str,
        max_sentences: int = 4,
        max_chars: int = 420,
    ) -> str:
        candidate = self.normalize_numbered_list_punctuation(text)
        if not candidate:
            return candidate
        parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
        limit_sent = max(1, int(max_sentences or 1))
        clipped = candidate
        if len(parts) > limit_sent:
            kept = parts[:limit_sent]
            clipped = " ".join(kept).strip()
        if clipped and clipped[-1] not in ".!?":
            clipped = clipped + "."
        limit_chars = max(120, int(max_chars or 120))
        if len(clipped) <= limit_chars:
            return clipped
        hard = clipped[:limit_chars].rstrip()
        tail_punct = max(hard.rfind("."), hard.rfind("?"), hard.rfind("!"))
        if tail_punct >= max(0, limit_chars - 120):
            hard = hard[: tail_punct + 1].rstrip()
        else:
            tail_space = hard.rfind(" ")
            if tail_space >= max(0, limit_chars - 80):
                hard = hard[:tail_space].rstrip()
            if hard and hard[-1] not in ".!?":
                hard = hard + "."
        return hard

    def question_token_set(self, question: str) -> set[str]:
        fp = self.deps.question_fingerprint_fn(str(question or ""))
        if not fp:
            return set()
        return {token for token in fp.split() if token}

    def extract_questions_from_text(self, text: str) -> list[str]:
        candidate = (text or "").strip()
        if not candidate:
            return []
        parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
        questions: list[str] = []
        for part in parts:
            if "?" in part:
                first_q = part.find("?")
                if first_q < 0:
                    continue
                question = part[: first_q + 1].strip()
                if len(question) < 4:
                    continue
                questions.append(question)
                continue
            # Some channels/users omit '?' — keep slot tracking robust for imperative questions.
            if self.deps.question_cue_re.search(part):
                clean = part.strip().rstrip(".!,:;")
                if len(clean) >= 4:
                    questions.append(clean + "?")
        return questions

    def is_repeated_question_against_state(self, question: str, state: Any) -> bool:
        q_tokens = self.question_token_set(question)
        if not q_tokens:
            return False
        core_fact_keys = ("city", "address", "object_type", "model", "budget", "timeline", "dimensions", "contact")

        def _question_fact_set(text: str) -> set[str]:
            out: set[str] = set()
            for key in core_fact_keys:
                if self.deps.question_covers_fact(text, key):
                    out.add(key)
            return out

        current_facts = _question_fact_set(question)
        previous_fps = [
            str(item or "").strip()
            for item in (getattr(state, "asked_question_fingerprints", []) or [])
            if str(item or "").strip()
        ]
        previous_questions = [
            str(item or "").strip()
            for item in (getattr(state, "asked_questions", []) or [])
            if str(item or "").strip()
        ]
        last_question = str(getattr(state, "last_question_text", "") or "").strip()
        if last_question:
            last_fp = self.deps.question_fingerprint_fn(last_question)
            if last_fp:
                previous_fps.append(last_fp)
                previous_questions.append(last_question)
        prev_fact_map: dict[str, set[str]] = {}
        for q_text in previous_questions[-24:]:
            fp = self.deps.question_fingerprint_fn(q_text)
            if not fp:
                continue
            prev_fact_map[fp] = _question_fact_set(q_text)
        for prev_fp in previous_fps[-24:]:
            prev_tokens = {token for token in prev_fp.split() if token}
            if not prev_tokens:
                continue
            overlap = len(q_tokens & prev_tokens)
            if overlap <= 0:
                continue
            coverage_cur = overlap / max(1, len(q_tokens))
            coverage_prev = overlap / max(1, len(prev_tokens))
            jaccard = overlap / max(1, len(q_tokens | prev_tokens))
            if coverage_cur >= 0.72 or coverage_prev >= 0.72 or jaccard >= 0.62:
                prev_facts = prev_fact_map.get(prev_fp, set())
                if current_facts and prev_facts and current_facts.isdisjoint(prev_facts):
                    continue
                return True
        return False

    def reply_has_repeated_question(self, text: str, state: Any) -> bool:
        for question in self.extract_questions_from_text(text):
            if self.is_repeated_question_against_state(question, state):
                return True
        return False

    def drop_repeated_questions_from_reply(self, text: str, state: Any) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return candidate
        parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
        kept: list[str] = []
        removed_count = 0
        for part in parts:
            if "?" not in part:
                kept.append(part)
                continue
            first_q = part.find("?")
            if first_q < 0:
                kept.append(part)
                continue
            question = part[: first_q + 1].strip()
            known_facts = self.deps.state_facts_snapshot(state)
            if any(
                self.deps.question_covers_fact(question, key) and str(known_facts.get(key) or "").strip()
                for key in ("city", "address", "object_type", "model")
            ):
                removed_count += 1
                continue
            if self.is_repeated_question_against_state(question, state):
                removed_count += 1
                continue
            kept.append(part)
        cleaned = " ".join(kept).strip()
        if cleaned:
            return cleaned
        if removed_count > 0:
            return candidate
        return candidate

    def render_passes_rubric(self, text: str, state: Any, *, sentence_split_re: Any) -> bool:
        candidate = (text or "").strip()
        if not candidate:
            return False
        if self.count_sentences(candidate) > 3:
            return False
        if candidate.count("?") > 1:
            return False
        low = candidate.lower()
        banned = (
            "спасибо, понял",
            "спасибо за подтверждение",
            "ваш запрос принят",
            "если что-то еще интересует",
            "рад вас видеть",
            "чем могу помочь",
            "после приветствия последовательно уточни",
            "собрал короткий шорт-лист",
        )
        if any(phrase in low for phrase in banned):
            return False
        for part in [part.strip() for part in sentence_split_re.split(candidate) if part.strip()]:
            if self.deps.is_operator_instruction_sentence(part):
                return False
        prev = (str(getattr(state, "last_bot_reply", "") or "")).strip().lower()
        if prev and len(prev) > 20 and candidate.lower() == prev:
            return False
        if self.reply_has_repeated_question(candidate, state):
            return False
        return True

    def apply_base_answer_quality_floor(
        self,
        answer: str,
        *,
        state: Any,
        persona_hints: Any,
        grounding: Mapping[str, Any] | None,
        user_text: str,
    ) -> str:
        _ = user_text
        candidate = str(answer or "").strip()
        if not candidate:
            return ""
        candidate = re.sub(r"(?<=\d)\.(?=\d)", ",", candidate)
        candidate = self.deps.normalize_catalog_name_case(candidate, grounding=grounding)
        candidate = self.deps.normalize_shouting_case(candidate)
        candidate = self.deps.dedupe_repeated_fact_sentences(candidate, state)
        candidate = self.deps.strip_instruction_leaks(candidate)
        candidate = self.drop_repeated_questions_from_reply(candidate, state)
        candidate = self.limit_questions(candidate, max_questions=min(1, self.deps.max_questions_limit(persona_hints)))
        candidate = self.enforce_exclamation_budget(candidate, max_exclamations=1)
        candidate = self.enforce_sentence_budget(candidate, max_sentences=2)
        return candidate.strip()

    def prefer_refined_answer(
        self,
        *,
        answer: str,
        refined: str,
        state: Any,
        persona_hints: Any,
        grounding: Mapping[str, Any] | None,
        user_text: str,
        sentence_split_re: Any,
    ) -> str:
        base_candidate = self.apply_base_answer_quality_floor(
            answer,
            state=state,
            persona_hints=persona_hints,
            grounding=grounding,
            user_text=user_text,
        )
        refined_candidate = self.apply_base_answer_quality_floor(
            refined,
            state=state,
            persona_hints=persona_hints,
            grounding=grounding,
            user_text=user_text,
        )
        if not refined_candidate:
            return base_candidate or refined_candidate
        if not base_candidate:
            return refined_candidate
        if refined_candidate == base_candidate:
            return refined_candidate

        base_ok = self.render_passes_rubric(base_candidate, state, sentence_split_re=sentence_split_re)
        refined_ok = self.render_passes_rubric(refined_candidate, state, sentence_split_re=sentence_split_re)
        if base_ok and (not refined_ok):
            return base_candidate

        if len(base_candidate) >= 40 and len(refined_candidate) < max(20, int(len(base_candidate) * 0.45)):
            return base_candidate
        if self.deps.answer_is_too_robotic(refined_candidate) and not self.deps.answer_is_too_robotic(base_candidate):
            return base_candidate

        refined_parts = [part.strip() for part in sentence_split_re.split(refined_candidate) if part.strip()]
        base_parts = [part.strip() for part in sentence_split_re.split(base_candidate) if part.strip()]
        refined_instructional = any(
            self.deps.is_operator_instruction_sentence(part)
            or self.deps.is_response_format_instruction_sentence(part)
            or self.deps.is_sequence_process_instruction_sentence(part)
            for part in refined_parts
        )
        base_instructional = any(
            self.deps.is_operator_instruction_sentence(part)
            or self.deps.is_response_format_instruction_sentence(part)
            or self.deps.is_sequence_process_instruction_sentence(part)
            for part in base_parts
        )
        if refined_instructional and not base_instructional:
            return base_candidate

        base_low = self.deps.normalize_text(base_candidate)
        refined_low = self.deps.normalize_text(refined_candidate)
        if (
            "ориентир по времени" in refined_low
            and "ориентир по времени" not in base_low
            and not self.deps.eta_intent_re.search(str(user_text or ""))
        ):
            return base_candidate
        if (
            "выезд сегодня возможен" in refined_low
            and "выезд сегодня возможен" not in base_low
            and not self.deps.urgent_today_re.search(str(user_text or ""))
        ):
            return base_candidate

        if refined_low.count("модель из каталога") > base_low.count("модель из каталога"):
            return base_candidate
        if refined_low.count("вариант") >= 3 and base_low.count("вариант") <= 1:
            return base_candidate
        if re.search(
            r"(?iu)\b(цвет|модель|тип|стиль|оттенок)\w*\s+вариант\b",
            refined_low,
        ) and not re.search(
            r"(?iu)\b(цвет|модель|тип|стиль|оттенок)\w*\s+вариант\b",
            base_low,
        ):
            return base_candidate

        grounding_items = self.deps.grounding_catalog_items(grounding)
        if grounding_items:
            direct_catalog_intent = bool(
                self.deps.is_price_intent(str(user_text or ""))
                or self.deps.variants_user_hint_re.search(str(user_text or ""))
                or self.deps.model_name_intent_re.search(str(user_text or ""))
            )
            if direct_catalog_intent:
                if self.deps.reply_mentions_catalog_item(base_candidate, grounding_items) and not self.deps.reply_mentions_catalog_item(
                    refined_candidate, grounding_items
                ):
                    return base_candidate

        return refined_candidate

    @staticmethod
    def limit_questions(text: str, max_questions: int = 1) -> str:
        if not text:
            return text
        if max_questions < 0:
            max_questions = 0
        question_cue = re.compile(
            r"(?iu)\b(как\s+удобнее|есть\s+ли|како[йеая]|какие|где|когда|сколько|что|"
            r"подскажите|уточните|нужен\s+ли|нужна\s+ли|можете\s+ли|хотите\s+ли)\b"
        )
        parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", str(text or "")) if part.strip()]
        if len(parts) > 1:
            kept: list[str] = []
            questions_left = max_questions
            for part in parts:
                if "?" not in part:
                    if questions_left <= 0 and question_cue.search(part):
                        continue
                    kept.append(part)
                    continue
                if questions_left <= 0:
                    statement = part.replace("?", ".").strip()
                    if question_cue.search(statement):
                        continue
                    kept.append(statement)
                    continue
                first_q = part.find("?")
                kept.append(part[: first_q + 1].strip())
                questions_left -= 1
            return " ".join(kept).strip()

        out: list[str] = []
        questions_left = max_questions
        for ch in text:
            if ch == "?":
                if questions_left <= 0:
                    out.append(".")
                    continue
                questions_left -= 1
            out.append(ch)
        return "".join(out)

    @staticmethod
    def enforce_exclamation_budget(text: str, max_exclamations: int = 1) -> str:
        candidate = str(text or "")
        if not candidate:
            return candidate
        if max_exclamations < 0:
            max_exclamations = 0
        out: list[str] = []
        left = max_exclamations
        for ch in candidate:
            if ch == "!":
                if left <= 0:
                    out.append(".")
                    continue
                left -= 1
            out.append(ch)
        return "".join(out)
