from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class PolicyPlanRuntimeDeps:
    classify_turn_intent: Callable[[str], str]
    fact_token_re: Any
    model_name_intent_re: Any
    generic_model_words: Sequence[str]
    read_catalog: Callable[[int], Sequence[Mapping[str, Any]]]
    best_catalog_item_match: Callable[[str, Sequence[Mapping[str, Any]]], Mapping[str, Any] | None]
    item_label: Callable[[Mapping[str, Any]], str]
    safe_short_text: Callable[[str], str]
    slot_aliases: Mapping[str, Sequence[str]]
    question_topic_to_slot: Mapping[str, str]
    question_fingerprint_fn: Callable[[str], str]
    state_facts_snapshot: Callable[[Any], Mapping[str, str]]
    normalize_required_facts: Callable[[Any], list[str]]
    missing_required_facts: Callable[[Sequence[str], Mapping[str, str]], list[str]]
    canonical_fact_key: Callable[[str], str]
    question_covers_fact: Callable[[str, str], bool]


class PolicyPlanRuntime:
    def __init__(self, deps: PolicyPlanRuntimeDeps):
        self.deps = deps

    def all_required_facts_present(self, required: Sequence[str], facts: Mapping[str, str]) -> bool:
        for key in required:
            value = str(facts.get(key) or "").strip()
            if not value:
                return False
        return True

    def question_topic(self, question: str) -> str:
        low = str(question or "").lower()
        if not low:
            return "other"
        for topic, aliases in self.deps.slot_aliases.items():
            if any(token in low for token in aliases):
                return topic
        return "other"

    def normalize_slot_name(self, raw_slot: str, question: str = "") -> str:
        raw = str(raw_slot or "").strip().lower().replace("-", "_").replace(" ", "_")
        if raw in {"", "none", "null", "n/a", "na"}:
            raw = ""
        if raw in self.deps.slot_aliases:
            return raw
        for canonical, aliases in self.deps.slot_aliases.items():
            if raw and any(token in raw for token in aliases):
                return canonical
        topic = self.question_topic(question)
        return self.deps.question_topic_to_slot.get(topic, "other")

    def topic_has_confirmed_fact(self, topic: str, state: Any) -> bool:
        facts = self.deps.state_facts_snapshot(state)
        mapping: dict[str, tuple[str, ...]] = {
            "location": ("city", "address"),
            "object": ("object_type",),
            "model": ("model",),
            "budget": ("budget",),
            "timeline": ("timeline",),
            "dimensions": ("dimensions",),
            "contact": ("contact",),
        }
        keys = mapping.get(str(topic or "").strip().lower(), ())
        for key in keys:
            if str(facts.get(key) or "").strip():
                return True
        return False

    def maybe_store_model_slot(self, state: Any, tenant: int | None, user_text: str) -> None:
        if tenant is None:
            return
        text = str(user_text or "").strip()
        if not text:
            return
        if re.match(r"(?iu)^\s*(здрав\w*|привет\w*|добрый|салам|hello|hi)\b", text):
            return
        if self.deps.classify_turn_intent(text) == "offtopic":
            return
        low = text.lower().replace("ё", "е")
        if "?" in text and len(self.deps.fact_token_re.findall(text)) > 6:
            return
        tokens = [tok for tok in self.deps.fact_token_re.findall(low) if len(tok) >= 2]
        if not tokens:
            return
        if len(tokens) <= 2 and not self.deps.model_name_intent_re.search(text):
            return
        if all(tok in self.deps.generic_model_words for tok in tokens):
            return
        try:
            catalog_items = self.deps.read_catalog(int(tenant))
        except Exception:
            return
        if not catalog_items:
            return
        match = self.deps.best_catalog_item_match(text, catalog_items)
        if not match:
            return
        label = self.deps.item_label(match)
        if not label:
            return
        state.known_slots["model"] = self.deps.safe_short_text(label)

    def enforce_semantic_plan_guards(
        self,
        plan: dict[str, Any],
        *,
        state: Any,
        grounding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        guarded = dict(plan or {})
        question = str(guarded.get("question") or "").strip()
        slot = self.normalize_slot_name(str(guarded.get("question_slot") or ""), question)
        if slot == "other" and question:
            slot = self.deps.question_topic_to_slot.get(self.question_topic(question), "other")
        forbidden = set(str(topic) for topic in ((grounding or {}).get("forbid_question_topics") or []))

        if question:
            fp = self.deps.question_fingerprint_fn(question)
            already_asked = fp in set(state.asked_question_fingerprints or [])
            slot_filled = bool(slot and slot not in {"none", "other"} and state.known_slots.get(slot))
            topic = self.question_topic(question)
            recent_topics = [
                self.question_topic(str(item or ""))
                for item in (state.asked_questions or [])[-8:]
                if str(item or "").strip()
            ]
            asked_topic_with_fact = bool(
                topic not in {"", "none", "other"}
                and topic in recent_topics
                and self.topic_has_confirmed_fact(topic, state)
            )
            topic_forbidden = topic in forbidden
            if already_asked or slot_filled or topic_forbidden or asked_topic_with_fact:
                question = ""
                slot = "none"

        guarded["question"] = question
        guarded["question_slot"] = slot if slot else "none"
        guarded["required_facts"] = self.deps.normalize_required_facts(guarded.get("required_facts"))
        return guarded

    def compose_reply_from_policy_blocks(
        self,
        plan: Mapping[str, Any],
        *,
        state: Any,
        known_facts: Mapping[str, str] | None = None,
        required_facts: Sequence[str] | None = None,
        block_requires_override: Mapping[int, Sequence[str]] | None = None,
        block_allowance_override: Mapping[int, bool] | None = None,
    ) -> tuple[str, str]:
        blocks = plan.get("blocks")
        if not isinstance(blocks, list):
            return "", ""
        facts = dict(known_facts or {})
        missing_required = self.deps.missing_required_facts(required_facts or [], facts)
        out: list[str] = []
        question_used = False
        next_question_key = ""
        for idx, raw in enumerate(blocks[:8]):
            if (
                block_allowance_override
                and idx in block_allowance_override
                and not bool(block_allowance_override[idx])
            ):
                continue
            if not isinstance(raw, Mapping):
                continue
            text = str(raw.get("text") or "").strip()
            if not text:
                continue
            requires_raw = raw.get("requires")
            if isinstance(requires_raw, str):
                requires = [requires_raw]
            elif isinstance(requires_raw, Sequence):
                requires = [str(x) for x in requires_raw]
            else:
                requires = []
            if block_requires_override and idx in block_requires_override:
                requires.extend(
                    str(x) for x in (block_requires_override.get(idx) or []) if str(x).strip()
                )
            if not self.all_required_facts_present(requires, facts):
                continue
            block_type = str(raw.get("type") or "").strip().lower()
            if missing_required and block_type in {"offer", "cta"}:
                continue
            if block_type == "question" or "?" in text:
                if question_used:
                    continue
                q_fp = self.deps.question_fingerprint_fn(text)
                if q_fp and q_fp in set(state.asked_question_fingerprints or []):
                    continue
                q_key = self.deps.canonical_fact_key(str(raw.get("question_key") or ""))
                if missing_required:
                    if q_key and q_key not in missing_required:
                        continue
                    if not q_key:
                        matched = None
                        for miss_key in missing_required:
                            if self.deps.question_covers_fact(text, miss_key):
                                matched = miss_key
                                break
                        if not matched:
                            continue
                        q_key = matched
                question_used = True
                if q_key:
                    next_question_key = q_key
            out.append(text)
            if len(out) >= 3:
                break
        reply = " ".join(out).strip()
        return reply, next_question_key
