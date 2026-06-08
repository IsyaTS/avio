from __future__ import annotations

import hashlib
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class HumanizeRuntimeDeps:
    normalize_text: Callable[[str], str]
    looks_like_address_value: Callable[[str], bool]
    strip_instruction_leaks: Callable[[str], str]
    limit_questions: Callable[[str, int], str]
    max_questions_limit: Callable[[Any], int]

    sentence_split_re: Any
    opening_hey_re: Any
    greeting_prefix_re: Any
    gratitude_re: Any
    gratitude_phrase_re: Any
    neighbor_claim_re: Any
    entity_ack_prefix_re: Any
    object_type_hint_re: Any
    fact_token_re: Any
    model_name_intent_re: Any
    variants_user_hint_re: Any
    opening_word_re: Any
    lowercase_opening_blocked: set[str]


class HumanizeRuntime:
    def __init__(self, deps: HumanizeRuntimeDeps) -> None:
        self.deps = deps

    def recent_gratitude_count(self, state: Any, tail: int = 6) -> int:
        recent_assistant = [
            str(item.get("content") or "")
            for item in (getattr(state, "history", []) or [])
            if item.get("role") == "assistant"
        ]
        if tail > 0:
            recent_assistant = recent_assistant[-tail:]
        count = 0
        for text in recent_assistant:
            if self.deps.gratitude_re.search(text or ""):
                count += 1
        return count

    def apply_conversational_phrasing(
        self,
        text: str,
        *,
        persona_hints: Any | None = None,
    ) -> str:
        _ = persona_hints
        out = (text or "").strip()
        if not out:
            return out
        out = re.sub(
            r"\b([a-z]+_[a-z0-9_]+)\b",
            lambda m: m.group(1).replace("_", " "),
            out,
            flags=re.IGNORECASE,
        )
        lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
        out = "\n".join(lines).strip()
        if self.deps.opening_hey_re.match(out):
            out = self.deps.opening_hey_re.sub("Здравствуйте. ", out, count=1).strip()
        out = self.normalize_entity_ack_opening(out)
        return out

    def trim_redundant_gratitude_opening(self, text: str, state: Any) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return candidate
        match = self.deps.gratitude_phrase_re.match(candidate)
        if not match:
            return candidate
        opening = match.group(0).strip().lower()
        should_trim = "за обращение" in opening or self.recent_gratitude_count(state) >= 1
        if not should_trim:
            return candidate
        tail = candidate[match.end() :].strip()
        if not tail:
            return candidate
        if tail and tail[0].isalpha():
            tail = tail[0].upper() + tail[1:]
        return tail

    def rotate_greeting(self, text: str, state: Any) -> str:
        if not text:
            return text
        match = self.deps.greeting_prefix_re.match(text)
        if not match:
            return text
        last = (str(getattr(state, "last_bot_reply", "") or "")).strip()
        if not last:
            return text
        last_match = self.deps.greeting_prefix_re.match(last)
        if not last_match:
            return text
        current = match.group(1).lower()
        previous = last_match.group(1).lower()
        if current != previous:
            return text
        alternatives = ("Здравствуйте", "Добрый день", "Добрый вечер", "Приветствую")
        try:
            seed_src = (str(getattr(state, "last_user_text", "") or "")) + "|" + (str(getattr(state, "last_bot_reply", "") or ""))
            digest = hashlib.sha1(seed_src.encode("utf-8")).hexdigest()
            seed = int(digest[:8], 16)
        except Exception:
            seed = int(time.time())
        replacement = alternatives[seed % len(alternatives)]
        if replacement.lower() == current:
            replacement = alternatives[(seed + 1) % len(alternatives)]
        return text[: match.start()] + replacement + text[match.end() :]

    def has_address_fact(self, state: Any) -> bool:
        known_slots = dict(getattr(state, "known_slots", {}) or {})
        facts = dict(getattr(state, "facts", {}) or {})
        candidates = (
            str(known_slots.get("address") or "").strip(),
            str(facts.get("address") or "").strip(),
            str(facts.get("адрес") or "").strip(),
        )
        return any(bool(item) for item in candidates)

    def strip_unverified_local_claims(self, text: str, state: Any) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return candidate
        if self.has_address_fact(state):
            return candidate
        parts = [part.strip() for part in self.deps.sentence_split_re.split(candidate) if part.strip()]
        if not parts:
            return candidate
        kept: list[str] = []
        removed = False
        for part in parts:
            if self.deps.neighbor_claim_re.search(part):
                removed = True
                continue
            kept.append(part)
        if not removed:
            return candidate
        rebuilt = " ".join(kept).strip()
        return rebuilt or "Продолжаем подбор."

    def normalize_entity_ack_opening(self, text: str) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return candidate
        match = self.deps.entity_ack_prefix_re.match(candidate)
        if not match:
            return candidate
        entity = str(match.group(1) or "").strip()
        if len([tok for tok in entity.split() if tok]) > 3:
            return candidate
        tail = candidate[match.end() :].strip()
        if tail.lower().startswith("что "):
            return candidate
        if not tail:
            return candidate
        if tail and tail[0].isalpha():
            tail = tail[0].upper() + tail[1:]
        return tail.strip()

    def looks_like_contextual_short_followup(self, text: str) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        if "?" in raw:
            return False
        if re.search(r"\d", raw):
            return False
        if self.deps.looks_like_address_value(raw):
            return False
        low = self.deps.normalize_text(raw)
        if self.deps.object_type_hint_re.search(low):
            return False
        tokens = [tok for tok in self.deps.fact_token_re.findall(raw) if tok]
        if not tokens or len(tokens) > 2:
            return False
        if self.deps.model_name_intent_re.search(raw) or self.deps.variants_user_hint_re.search(raw):
            return False
        return True

    def apply_optional_lowercase_opening(
        self,
        text: str,
        state: Any,
        *,
        persona_hints: Any | None = None,
        lowercase_opening_chance: float = 0.0,
    ) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return candidate
        if int(getattr(state, "user_message_count", 0) or 0) <= 1:
            return candidate
        if self.deps.greeting_prefix_re.match(candidate):
            return candidate
        chance = max(0.0, min(1.0, float(lowercase_opening_chance or 0.0)))
        if chance <= 0:
            return candidate
        match = self.deps.opening_word_re.match(candidate)
        if not match:
            return candidate
        opening = match.group(1)
        lower_opening = opening.lower()
        if lower_opening in self.deps.lowercase_opening_blocked:
            return candidate
        if len(opening) >= 3 and opening.isupper():
            return candidate
        tone = (str(getattr(persona_hints, "tone", "") or "")).lower() if persona_hints else ""
        if any(token in tone for token in ("формал", "официал")):
            return candidate
        if random.random() > chance:
            return candidate
        lowered = opening[0].lower() + opening[1:]
        return candidate[: match.start(1)] + lowered + candidate[match.end(1) :]

    def humanize_reply_text(
        self,
        reply: str,
        *,
        state: Any,
        persona_hints: Any | None = None,
        lowercase_opening_chance: float = 0.0,
    ) -> str:
        text = (reply or "").strip()
        if not text:
            return text
        text = self.deps.strip_instruction_leaks(text)
        if not text:
            return ""
        text = re.sub(r"(?im)^\s*Вариант\s+\d+\s*:\s*", "", text).strip()
        text = re.sub(r"(?im)^\s*с уважением[,.! ]*$", "", text).strip()
        text = re.sub(r"(?im)^\s*обращайтесь в любое время[,.! ]*$", "", text).strip()
        text = re.sub(r"\s{2,}", " ", text)
        text = re.sub(r"\s+([,.;:!?])", r"\1", text)
        text = re.sub(r"\?\.+", "?", text)
        text = re.sub(r"!\.+", "!", text)
        text = re.sub(r"\.\?+", "?", text)
        text = re.sub(r"([,;:])\1+", r"\1", text)
        text = self.apply_conversational_phrasing(text, persona_hints=persona_hints)
        text = self.trim_redundant_gratitude_opening(text, state)
        text = self.strip_unverified_local_claims(text, state)
        text = self.normalize_entity_ack_opening(text)
        text = self.deps.limit_questions(text, max_questions=self.deps.max_questions_limit(persona_hints))
        text = self.apply_optional_lowercase_opening(
            text,
            state,
            persona_hints=persona_hints,
            lowercase_opening_chance=lowercase_opening_chance,
        )
        return text.strip()

    def persona_requires_first_greeting(self, persona_context: str) -> bool:
        low = str(persona_context or "").lower().replace("ё", "е")
        if not low:
            return False
        has_greeting_tokens = any(
            token in low for token in ("здорова", "приветств", "добрый день", "здравствуйте")
        )
        if not has_greeting_tokens:
            return False
        patterns = (
            r"перв\w+\s+сообщени\w+[^\n]{0,120}(здорова|приветств)",
            r"сначала[^\n]{0,120}(здорова|приветств)",
            r"обязатель\w+[^\n]{0,120}(здорова|приветств)",
            r"начина\w+[^\n]{0,120}с\s+приветств",
            r"на\s+старт[^\n]{0,120}(здравствуйте|здорова|приветств)",
        )
        return any(re.search(pattern, low) is not None for pattern in patterns)

    def ensure_dialog_greeting_on_first_reply(
        self,
        text: str,
        state: Any,
        *,
        persona_context: str = "",
        force_first_greeting: bool = False,
    ) -> str:
        candidate = str(text or "").strip()
        if not candidate:
            return candidate
        persona_force_greeting = self.persona_requires_first_greeting(persona_context)
        if not force_first_greeting and not persona_force_greeting:
            return candidate
        if int(getattr(state, "user_message_count", 0) or 0) > 1:
            return candidate
        if list(getattr(state, "history", []) or []):
            return candidate
        has_bot_history = bool(str(getattr(state, "last_bot_reply", "") or "").strip())
        if has_bot_history:
            return candidate
        if re.match(r"^\s*(https?://|@[\w\d_]+)", candidate):
            return candidate
        greeting = "Здравствуйте."
        persona_low = str(persona_context or "").lower().replace("ё", "е")
        if "добрый день" in persona_low:
            greeting = "Добрый день."
        elif "добрый вечер" in persona_low:
            greeting = "Добрый вечер."
        if self.deps.opening_hey_re.match(candidate):
            body = self.deps.opening_hey_re.sub("", candidate, count=1).strip()
            return f"{greeting} {body}".strip() if body else greeting
        if self.deps.greeting_prefix_re.match(candidate):
            return candidate
        body = str(candidate or "").strip()
        if not body:
            return greeting
        return f"{greeting} {body}".strip()
