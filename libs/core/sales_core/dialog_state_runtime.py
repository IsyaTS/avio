from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class DialogStateRuntimeDeps:
    canonical_fact_key: Callable[[str], str]
    safe_short_text: Callable[[str, int], str]
    normalize_slot_name: Callable[[str], str]
    is_plausible_city_text: Callable[[str], bool]
    normalize_text: Callable[[Any], str]
    extract_city_hint: Callable[..., str]
    extract_standalone_city_hint: Callable[[str], str]
    extract_store_addresses_from_persona: Callable[[str], dict[str, str]]
    canonical_object_type_hint: Callable[[Any], str]
    infer_user_needs: Callable[[str], dict[str, Any]]
    object_type_from_turn_text: Callable[[str], str]
    extract_questions_from_text: Callable[[str], list[str]]
    question_covers_fact: Callable[[str, str], bool]
    sentence_split_re: Any
    fact_token_re: Any
    generic_fact_stopwords: set[str]


class DialogStateRuntime:
    def __init__(self, deps: DialogStateRuntimeDeps) -> None:
        self.deps = deps

    def state_facts_snapshot(self, state: Any) -> dict[str, str]:
        facts: dict[str, str] = {}
        if isinstance(getattr(state, "facts", None), dict):
            for raw_k, raw_v in state.facts.items():
                key = self.deps.canonical_fact_key(str(raw_k))
                val = self.deps.safe_short_text(str(raw_v or ""), 180)
                if key and val:
                    facts[key] = val
        for raw_k, raw_v in (getattr(state, "known_slots", None) or {}).items():
            key = self.deps.canonical_fact_key(str(raw_k))
            val = self.deps.safe_short_text(str(raw_v or ""), 180)
            if key and val and key not in facts:
                facts[key] = val
            slot_norm = self.deps.normalize_slot_name(str(raw_k))
            if slot_norm == "location" and val:
                if self.deps.is_plausible_city_text(val):
                    facts.setdefault("city", val)
            elif slot_norm == "object" and val:
                facts.setdefault("object_type", val)
            elif slot_norm == "model" and val:
                facts.setdefault("model", val)
        return facts

    def reply_contains_unconfirmed_required_claim(
        self,
        reply: str,
        *,
        missing_required: Sequence[str],
        known_facts: Mapping[str, str] | None = None,
        user_text: str = "",
        persona_context: str = "",
    ) -> bool:
        candidate = str(reply or "").strip()
        if not candidate:
            return False
        missing = {
            self.deps.canonical_fact_key(item) for item in (missing_required or []) if str(item or "").strip()
        }
        missing.discard("")
        if not missing:
            return False

        facts = dict(known_facts or {})
        user_raw = str(user_text or "").strip()
        user_city = self.deps.extract_city_hint(
            user_raw,
            allow_standalone=True,
        ) or self.deps.extract_standalone_city_hint(user_raw)
        user_city_norm = self.deps.normalize_text(user_city)
        user_obj = self.deps.canonical_object_type_hint(
            (self.deps.infer_user_needs(user_raw) or {}).get("object_type")
            or self.deps.object_type_from_turn_text(user_raw)
        )
        reply_questions = self.deps.extract_questions_from_text(candidate)
        has_city_question = any(self.deps.question_covers_fact(q, "city") for q in reply_questions)
        has_object_question = any(self.deps.question_covers_fact(q, "object_type") for q in reply_questions)

        if "city" in missing and (not str(facts.get("city") or "").strip()) and not has_city_question:
            reply_city_norm = ""
            city_map = self.deps.extract_store_addresses_from_persona(persona_context)
            if city_map:
                low_reply = self.deps.normalize_text(candidate)
                for city_key in city_map.keys():
                    city_norm = self.deps.normalize_text(city_key)
                    if not city_norm:
                        continue
                    if re.search(rf"(?iu)(?<![\w-]){re.escape(city_norm)}(?![\w-])", low_reply):
                        reply_city_norm = city_norm
                        break
            if not reply_city_norm:
                extracted = self.deps.extract_city_hint(candidate, allow_standalone=True)
                if extracted:
                    reply_city_norm = self.deps.normalize_text(extracted)
            if reply_city_norm:
                if user_city_norm and reply_city_norm != user_city_norm:
                    return True
                if not user_city_norm:
                    return True

        if "object_type" in missing and (not str(facts.get("object_type") or "").strip()) and not has_object_question:
            reply_obj = self.deps.canonical_object_type_hint(
                (self.deps.infer_user_needs(candidate) or {}).get("object_type")
                or self.deps.object_type_from_turn_text(candidate)
            )
            if reply_obj:
                if user_obj and reply_obj != user_obj:
                    return True
                if not user_obj:
                    return True

        return False

    def fact_fingerprint(self, sentence: str) -> str:
        tokens = []
        for token in self.deps.fact_token_re.findall((sentence or "").lower().replace("ё", "е")):
            if len(token) < 3:
                continue
            if token in self.deps.generic_fact_stopwords:
                continue
            tokens.append(token)
        if not tokens:
            return ""
        return " ".join(sorted(set(tokens)))

    def dedupe_repeated_fact_sentences(self, text: str, state: Any) -> str:
        raw = (text or "").strip()
        if not raw:
            return raw
        recent = set(getattr(state, "recent_fact_fingerprints", None) or [])
        if not recent:
            return raw
        recent_token_sets = [set(fp.split()) for fp in recent if fp]
        parts = [part.strip() for part in self.deps.sentence_split_re.split(raw) if part.strip()]
        kept: list[str] = []
        for part in parts:
            if "?" in part:
                kept.append(part)
                continue
            fp = self.fact_fingerprint(part)
            if not fp:
                kept.append(part)
                continue
            if fp in recent:
                continue
            current_tokens = set(fp.split())
            is_near_duplicate = False
            if current_tokens:
                for prev_tokens in recent_token_sets:
                    if not prev_tokens:
                        continue
                    overlap = len(current_tokens & prev_tokens)
                    if overlap < 3:
                        continue
                    ratio = overlap / max(1, min(len(current_tokens), len(prev_tokens)))
                    if ratio >= 0.7:
                        is_near_duplicate = True
                        break
            if not is_near_duplicate:
                kept.append(part)
        if not kept:
            return raw
        rebuilt = " ".join(kept).strip()
        if raw.endswith("?") and not rebuilt.endswith("?"):
            rebuilt = rebuilt + "?"
        return rebuilt or raw

    def update_fact_memory(self, state: Any, text: str) -> None:
        parts = [part.strip() for part in self.deps.sentence_split_re.split(text or "") if part.strip()]
        for part in parts:
            if "?" in part:
                continue
            fp = self.fact_fingerprint(part)
            if not fp:
                continue
            if fp not in state.recent_fact_fingerprints:
                state.recent_fact_fingerprints.append(fp)
                if len(state.recent_fact_fingerprints) > 64:
                    state.recent_fact_fingerprints = state.recent_fact_fingerprints[-64:]
