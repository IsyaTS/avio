from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class IntentRuntimeDeps:
    catalog_request_re: Any
    order_intent_re: Any
    model_name_intent_re: Any
    offtopic_smalltalk_re: Any
    why_question_re: Any
    catalog_unavailable_re: Any
    low_signal_context_re: Any
    repair_turn_re: Any

    is_unsubscribe_intent: Callable[[str], bool]
    extract_city_hint: Callable[..., str]
    looks_like_address_value: Callable[[str], bool]
    object_type_from_turn_text: Callable[[str], str]
    extract_attribute_probe: Callable[[str], str]


class IntentRuntime:
    def __init__(self, deps: IntentRuntimeDeps) -> None:
        self.deps = deps

    def is_price_intent(self, text: str) -> bool:
        low = str(text or "").lower().replace("ё", "е")
        if not low:
            return False
        patterns = (
            r"\bсколько\s+стоит\b",
            r"\bсколько\s+стоят\b",
            r"\bкакая\s+цена\b",
            r"\bкакую\s+цен[уы]\b",
            r"\bпо\s+какой\s+цене\b",
            r"\bпо\s+ч[её]м\b",
            r"\bпоч[её]м\b",
            r"\bот\s+сколь",
            r"\bцена\b",
            r"\bцены\b",
            r"\bценник\b",
            r"\bстоимость\b",
            r"\bстоит\b",
            r"\bстоят\b",
            r"\bсам\w*\s+дорог\w*\b",
            r"\bсам\w*\s+дешев\w*\b",
            r"\bподороже\b",
            r"\bподешевле\b",
            r"\bмаксимальн\w*\s+цен\w*\b",
            r"\bминимальн\w*\s+цен\w*\b",
        )
        return any(re.search(pattern, low) for pattern in patterns)

    def is_payment_intent(self, text: str) -> bool:
        low = str(text or "").lower().replace("ё", "е")
        if not low:
            return False
        markers = (
            "оплат",
            "оплата",
            "перевод",
            "реквиз",
            "карта",
            "номер карты",
            "остаток",
            "договор",
            "чек",
        )
        return any(token in low for token in markers)

    def is_store_address_intent(self, text: str) -> bool:
        low = str(text or "").lower().replace("ё", "е")
        if not low:
            return False
        return (
            ("адрес" in low and ("магаз" in low or "наход" in low or "где вы" in low))
            or ("часы работы" in low)
            or ("график" in low and ("работ" in low or "магаз" in low))
        )

    def is_channel_handoff_intent(self, text: str) -> bool:
        low = str(text or "").lower().replace("ё", "е")
        if not low:
            return False
        return any(
            token in low for token in ("telegram", "телег", "тг", "whatsapp", "ватсап", "вотсап")
        )

    def is_catalog_request_intent(self, text: str) -> bool:
        return bool(self.deps.catalog_request_re.search(str(text or "")))

    def is_offtopic_message(
        self,
        text: str,
        *,
        known_facts: Mapping[str, str] | None = None,
    ) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        if re.search(r"(?iu)\b(здравств|добрый|привет|салам|hi|hello)\b", raw):
            return False
        if self.deps.order_intent_re.search(raw):
            return False
        if self.is_price_intent(raw) or self.is_payment_intent(raw) or self.is_store_address_intent(raw):
            return False
        if self.is_channel_handoff_intent(raw) or self.is_catalog_request_intent(raw):
            return False
        if self.deps.model_name_intent_re.search(raw):
            return False
        if self.deps.extract_city_hint(raw):
            return False
        if self.deps.offtopic_smalltalk_re.search(raw):
            return True
        return False

    def classify_turn_intent(
        self,
        text: str,
        *,
        known_facts: Mapping[str, str] | None = None,
    ) -> str:
        raw = str(text or "").strip()
        if not raw:
            return "unknown"
        if self.deps.is_unsubscribe_intent(raw):
            return "unsubscribe"
        if self.deps.why_question_re.search(raw):
            return "why_question"
        if self.deps.catalog_unavailable_re.search(raw) or self.deps.low_signal_context_re.search(raw):
            return "catalog_problem"
        if self.deps.repair_turn_re.search(raw):
            return "repair"
        if self.is_payment_intent(raw):
            return "payment"
        if self.is_store_address_intent(raw):
            return "store_address"
        if self.is_channel_handoff_intent(raw):
            return "handoff"
        if self.is_catalog_request_intent(raw):
            return "catalog_request"
        if self.is_offtopic_message(raw, known_facts=known_facts):
            return "offtopic"
        return "product"

    def is_shortlist_feedback_turn(
        self,
        text: str,
        *,
        known_facts: Mapping[str, str] | None = None,
    ) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        if self.deps.model_name_intent_re.search(raw):
            return False
        if self.deps.looks_like_address_value(raw):
            return False
        if self.deps.extract_city_hint(raw, allow_standalone=True):
            return False
        if self.deps.object_type_from_turn_text(raw):
            return False
        if self.deps.order_intent_re.search(raw) or self.is_payment_intent(raw):
            return False
        if self.is_channel_handoff_intent(raw) or self.is_store_address_intent(raw):
            return False
        if self.is_catalog_request_intent(raw):
            return False
        if self.is_price_intent(raw) or self.deps.extract_attribute_probe(raw):
            return True
        turn_intent = self.classify_turn_intent(raw, known_facts=known_facts)
        return turn_intent in {"product", "repair", "catalog_problem", "why_question"}

    def is_deferral_message(self, text: str) -> bool:
        low = str(text or "").lower().replace("ё", "е")
        if not low:
            return False
        return any(
            token in low
            for token in ("позже", "потом", "вечером", "завтра", "позднее", "как буду", "как смогу")
        )
