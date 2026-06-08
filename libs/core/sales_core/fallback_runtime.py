from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class FallbackRuntimeDeps:
    grounding_catalog_items: Callable[[Mapping[str, Any] | None], list[dict[str, Any]]]
    classify_turn_intent: Callable[[str], str]
    normalize_text: Callable[[Any], str]
    shortlist_preview_text: Callable[..., str]
    extract_attribute_probe: Callable[[str], str]
    display_item_label: Callable[[Mapping[str, Any]], str]
    item_label: Callable[[Mapping[str, Any]], str]
    catalog_min_price: Callable[[list[dict[str, Any]]], int | None]
    catalog_max_price: Callable[[list[dict[str, Any]]], int | None]
    format_rub_price: Callable[[int], str]
    is_price_intent: Callable[[str], bool]
    looks_like_price_objection: Callable[[str], bool]
    variants_user_hint_re: Any
    price_inline_re: Any
    price_thousands_re: Any
    fact_token_re: Any
    generic_fact_stopwords: set[str]


class FallbackRuntime:
    def __init__(self, deps: FallbackRuntimeDeps) -> None:
        self.deps = deps

    @staticmethod
    def safe_json_load(raw: str) -> dict[str, Any]:
        text = (raw or "").strip()
        if not text:
            return {}
        try:
            data = json.loads(text)
            return data if isinstance(data, dict) else {}
        except Exception:
            match = re.search(r"\{.*\}", text, flags=re.DOTALL)
            if not match:
                return {}
            try:
                data = json.loads(match.group(0))
                return data if isinstance(data, dict) else {}
            except Exception:
                return {}

    def has_substantive_non_question_payload(self, text: str) -> bool:
        candidate = str(text or "").strip()
        if not candidate:
            return False
        candidate_low = self.deps.normalize_text(candidate)
        # Treat short acknowledgement stubs as non-substantive so the
        # required-fact follow-up question is not accidentally suppressed.
        if re.match(r"(?iu)^\s*(понял|поняла|принял|приняла|услышал|принято|ок|окей)\b[,.! ]*$", candidate_low):
            return False
        if re.match(
            r"(?iu)^\s*[а-яёa-z0-9][а-яёa-z0-9\-/\s]{1,64}[,—-]\s*(понял|поняла|принял|приняла|услышал|принято)\b[,.! ]*$",
            candidate_low,
        ):
            return False
        segments = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
        if not segments:
            segments = [candidate]
        for segment in segments:
            if "?" in segment:
                continue
            probe = str(segment or "").strip()
            if len(probe) < 16:
                continue
            if (
                self.deps.price_inline_re.search(probe)
                or self.deps.price_thousands_re.search(probe)
                or "%" in probe
                or "₽" in probe
            ):
                return True
            tokens = [
                tok
                for tok in self.deps.fact_token_re.findall(self.deps.normalize_text(probe))
                if len(tok) >= 3 and tok not in self.deps.generic_fact_stopwords
            ]
            if len(tokens) >= 3:
                return True
        return False

    def llm_unavailable_reply(
        self,
        *,
        user_text: str = "",
        grounding: Mapping[str, Any] | None = None,
    ) -> str:
        return "Секунду, сейчас позову менеджера"
