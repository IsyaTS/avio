from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class PersonaTurnRuntimeDeps:
    compile_persona_rules: Callable[[str], Any]
    conditional_rule_matches: Callable[..., bool]
    extract_attribute_probe: Callable[[str], str]
    model_name_intent_re: Any
    variants_user_hint_re: Any
    state_facts_snapshot: Callable[[Any], dict[str, str]]
    canonical_fact_key: Callable[[str], str]
    required_facts_from_persona_text: Callable[[str], list[str]]
    missing_required_facts: Callable[[list[str], Mapping[str, str]], list[str]]
    persona_driven_question_for_fact: Callable[..., str]
    is_repeated_question_against_state: Callable[[str, Any], bool]
    question_covers_fact: Callable[[str, str], bool]
    extract_price_spans: Callable[[str], list[tuple[int, int, int]]]
    normalize_text: Callable[[Any], str]
    fact_token_re: Any
    needs_stopwords: set[str]
    generic_fact_stopwords: set[str]
    order_intent_re: Any
    is_price_intent: Callable[[str], bool]
    eta_intent_re: Any


class PersonaTurnRuntime:
    def __init__(self, deps: PersonaTurnRuntimeDeps) -> None:
        self.deps = deps

    def persona_direct_reply_for_user_turn(
        self,
        persona_context: str,
        *,
        last_user_message: str,
        known_facts: Mapping[str, str] | None = None,
        state: Any | None = None,
    ) -> str:
        persona_text = str(persona_context or "").strip()
        if not persona_text:
            return ""
        raw = str(last_user_message or "").strip()
        facts = dict(known_facts or {})
        if str(facts.get("model") or "").strip():
            if (
                "?" in raw
                or bool(self.deps.model_name_intent_re.search(raw))
                or bool(self.deps.variants_user_hint_re.search(raw))
                or bool(self.deps.extract_attribute_probe(raw))
            ):
                return ""
        compiled = self.deps.compile_persona_rules(persona_text)
        if not compiled.conditionals:
            return ""
        for rule in compiled.conditionals:
            if not self.deps.conditional_rule_matches(
                rule,
                last_user_message=last_user_message,
                known_facts={},
                state=None,
            ):
                continue
            quoted = [
                str(part or "").strip()
                for part in re.findall(r"[\"«]([^\"»]{8,260})[\"»]", str(rule.action_text or ""))
                if str(part or "").strip()
            ]
            normalized: list[str] = []
            for part in quoted:
                chunk = re.sub(r"<\s*[^>\n]{1,40}\s*>", "", part).strip()
                if not chunk:
                    continue
                if chunk[-1] not in ".!?":
                    chunk += "."
                normalized.append(chunk)
            if normalized:
                return " ".join(normalized).strip()
        return ""

    def fallback_contextual_question(
        self,
        user_text: str,
        *,
        state: Any | None = None,
        persona_context: str = "",
    ) -> str:
        raw = str(user_text or "").strip()
        known_facts = self.deps.state_facts_snapshot(state) if state is not None else {}
        current_pending = self.deps.canonical_fact_key(str(getattr(state, "pending_fact_key", "") or ""))
        if known_facts.get("model"):
            if (
                bool(self.deps.extract_attribute_probe(raw))
                or bool(self.deps.model_name_intent_re.search(raw))
                or bool(self.deps.variants_user_hint_re.search(raw))
            ):
                return ""
        if persona_context and state is not None:
            required = self.deps.required_facts_from_persona_text(persona_context)
            missing = self.deps.missing_required_facts(required, known_facts)
            if missing:
                persona_question = self.deps.persona_driven_question_for_fact(
                    persona_context,
                    missing[0],
                    state=state,
                )
                if persona_question and not self.deps.is_repeated_question_against_state(persona_question, state):
                    return persona_question
        if "?" in raw and not (state is not None and current_pending == "model" and bool(state.last_items)):
            if state is not None and current_pending:
                direct = self.deps.persona_driven_question_for_fact(
                    persona_context,
                    current_pending,
                    state=state,
                )
                if direct and not self.deps.is_repeated_question_against_state(direct, state):
                    return direct
        topic_tokens = [
            tok
            for tok in self.deps.fact_token_re.findall(self.deps.normalize_text(raw))
            if len(tok) >= 4 and tok not in self.deps.needs_stopwords and tok not in self.deps.generic_fact_stopwords
        ]
        _ = topic_tokens[0] if topic_tokens else ""
        if not raw:
            candidates = [""]
        elif self.deps.order_intent_re.search(raw):
            candidates = [""]
        elif self.deps.is_price_intent(raw):
            candidates = [""]
        elif self.deps.model_name_intent_re.search(raw) or self.deps.variants_user_hint_re.search(raw):
            candidates = [""]
        elif self.deps.eta_intent_re.search(raw):
            candidates = [""]
        else:
            candidates = [""]
        for candidate in candidates:
            if not candidate:
                continue
            if state is not None and self.deps.is_repeated_question_against_state(candidate, state):
                continue
            return candidate
        return ""
