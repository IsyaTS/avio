from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence


@dataclass(frozen=True)
class CatalogMatchRuntimeDeps:
    format_rub_price: Callable[[int], str]
    fallback_contextual_question: Callable[..., str]
    persona_driven_question_for_fact: Callable[..., str]
    question_covers_fact: Callable[[str, str], bool]
    is_repeated_question_against_state: Callable[[str, Any], bool]
    generic_question_for_fact: Callable[[str], str]
    normalize_text: Callable[[Any], str]
    is_dimension_like_value: Callable[[str], bool]
    selected_item_attribute_answer: Callable[[str, Mapping[str, Any]], str]
    normalize_model_alias: Callable[[str], str]
    fact_token_re: Any
    needs_stopwords: set[str]
    generic_model_words: set[str]


class CatalogMatchRuntime:
    def __init__(self, deps: CatalogMatchRuntimeDeps) -> None:
        self.deps = deps

    def item_price_int(self, item: Mapping[str, Any]) -> Optional[int]:
        raw = str(item.get("price") or "").strip()
        if not raw:
            return None
        candidates: list[int] = []
        for match in re.finditer(r"\d[\d\s.,]*", raw):
            digits = re.sub(r"\D", "", str(match.group(0) or ""))
            if not digits:
                continue
            try:
                value = int(digits)
            except Exception:
                continue
            candidates.append(value)
        if not candidates:
            return None
        for value in candidates:
            if 1000 <= value <= 1_000_000:
                return value
        lowered = raw.lower()
        has_currency = any(token in lowered for token in ("₽", "руб", "rub", "$", "€", "usd", "eur"))
        for value in candidates:
            if 1 <= value < 1000 and has_currency:
                return value
        if len(candidates) == 1 and candidates[0] > 0:
            return candidates[0]
        return None

    @staticmethod
    def item_label(item: Mapping[str, Any]) -> str:
        for key in ("title", "name", "model", "sku", "id"):
            value = str(item.get(key) or "").strip()
            if value:
                return value
        return ""

    def display_item_label(self, item: Mapping[str, Any]) -> str:
        label = self.item_label(item)
        if not label:
            return ""
        if len(label) >= 3 and label.upper() == label:
            parts = []
            for token in label.split():
                if token.isupper() and any(ch.isalpha() for ch in token):
                    parts.append(token.capitalize())
                else:
                    parts.append(token)
            return " ".join(parts).strip()
        return label

    def shortlist_preview_text(
        self,
        items: Sequence[Mapping[str, Any]],
        *,
        limit: int = 2,
    ) -> str:
        parts: list[str] = []
        for item in list(items or [])[: max(1, int(limit))]:
            name = self.item_label(dict(item))
            price = self.item_price_int(dict(item))
            if not name:
                continue
            if price:
                parts.append(f"{name} — {self.deps.format_rub_price(price)}")
            else:
                parts.append(name)
        return "; ".join(parts).strip()

    def render_shortlist_preview_reply(
        self,
        preview: str,
        *,
        ask_detail: bool = True,
        persona_context: str = "",
        state: Any = None,
        fact_key: str = "model",
        user_text: str = "",
    ) -> str:
        text = str(preview or "").strip()
        if not text:
            return ""
        base = f"Варианты: {text}."
        followup = self.deps.fallback_contextual_question(
            user_text,
            state=state,
            persona_context=persona_context,
        ) or self.deps.persona_driven_question_for_fact(persona_context, fact_key, state=state)
        followup = str(followup or "").strip()
        if (
            followup
            and state is not None
            and fact_key
            and bool(getattr(state, "last_items", None))
            and self.deps.question_covers_fact(followup, fact_key)
        ):
            alt_followup = self.deps.fallback_contextual_question(
                user_text,
                state=state,
                persona_context="",
            )
            alt_followup = str(alt_followup or "").strip()
            if alt_followup and not self.deps.question_covers_fact(alt_followup, fact_key):
                followup = alt_followup
            else:
                followup = ""
        if followup and state is not None and self.deps.is_repeated_question_against_state(followup, state):
            generic_q = self.deps.generic_question_for_fact(fact_key)
            generic_q = str(generic_q or "").strip()
            if generic_q and not self.deps.is_repeated_question_against_state(generic_q, state):
                followup = generic_q
            else:
                followup = ""
        if ask_detail and followup:
            return f"{base} {followup}".strip()
        if followup:
            return f"{base} {followup}".strip()
        return base

    @staticmethod
    def item_mm_value(item: Mapping[str, Any], *keys: str) -> float | None:
        for key in keys:
            raw = str(item.get(key) or "").strip()
            if not raw:
                continue
            match = re.search(r"(\d+(?:[.,]\d+)?)", raw)
            if not match:
                continue
            try:
                return float(match.group(1).replace(",", "."))
            except Exception:
                continue
        return None

    @staticmethod
    def first_number_value(value: Any) -> float | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        match = re.search(r"(\d+(?:[.,]\d+)?)", raw.replace(" ", ""))
        if not match:
            return None
        try:
            return float(match.group(1).replace(",", "."))
        except Exception:
            return None

    @staticmethod
    def item_number_value(item: Mapping[str, Any], *keys: str) -> float:
        for key in keys:
            raw = str(item.get(key) or "").strip()
            if not raw:
                continue
            match = re.search(r"(\d+(?:[.,]\d+)?)", raw.replace(" ", ""))
            if not match:
                continue
            try:
                return float(match.group(1).replace(",", "."))
            except Exception:
                continue
        return 0.0

    def best_numeric_attribute_delta_line(
        self,
        current_items: Sequence[Mapping[str, Any]],
        alternative_items: Sequence[Mapping[str, Any]],
    ) -> str:
        cur = [dict(item) for item in list(current_items or [])[:2] if isinstance(item, Mapping)]
        alt = [dict(item) for item in list(alternative_items or [])[:2] if isinstance(item, Mapping)]
        if not cur or not alt:
            return ""
        numeric_by_key_cur: dict[str, list[float]] = {}
        numeric_by_key_alt: dict[str, list[float]] = {}
        value_samples: dict[str, str] = {}
        ignored_keys = {"price", "цена", "cost", "стоимость", "stock", "id", "sku", "_search_text"}
        for item in cur:
            for raw_key, raw_val in item.items():
                key = str(raw_key or "").strip()
                val = str(raw_val or "").strip()
                if not key or not val:
                    continue
                key_norm = self.deps.normalize_text(key)
                if key_norm in ignored_keys or key_norm.startswith("_"):
                    continue
                num = self.first_number_value(val)
                if num is None:
                    continue
                numeric_by_key_cur.setdefault(key, []).append(float(num))
                value_samples.setdefault(key, val)
        for item in alt:
            for raw_key, raw_val in item.items():
                key = str(raw_key or "").strip()
                val = str(raw_val or "").strip()
                if not key or not val:
                    continue
                key_norm = self.deps.normalize_text(key)
                if key_norm in ignored_keys or key_norm.startswith("_"):
                    continue
                num = self.first_number_value(val)
                if num is None:
                    continue
                numeric_by_key_alt.setdefault(key, []).append(float(num))
                value_samples.setdefault(key, val)
        best_key = ""
        best_score = 0.0
        best_cur_avg = 0.0
        best_alt_avg = 0.0
        for key, cur_vals in numeric_by_key_cur.items():
            alt_vals = numeric_by_key_alt.get(key) or []
            if not cur_vals or not alt_vals:
                continue
            cur_avg = sum(cur_vals) / max(1, len(cur_vals))
            alt_avg = sum(alt_vals) / max(1, len(alt_vals))
            gain = alt_avg - cur_avg
            if gain <= 0:
                continue
            sample = str(value_samples.get(key) or "")
            score = gain
            if re.search(r"\b(?:мм|cm|kg|кг|см|шт|pcs|mm)\b", sample, re.IGNORECASE):
                score += 4.0
            if self.deps.is_dimension_like_value(sample):
                score -= 6.0
            if score > best_score:
                best_key = key
                best_score = score
                best_cur_avg = cur_avg
                best_alt_avg = alt_avg
        if not best_key or best_score <= 0:
            return ""
        cur_str = str(int(round(best_cur_avg)))
        alt_str = str(int(round(best_alt_avg)))
        return f"{best_key}: {cur_str} -> {alt_str}."

    def shortlist_attribute_answer(
        self,
        user_text: str,
        items: Sequence[Mapping[str, Any]],
    ) -> str:
        shortlist = [dict(item) for item in list(items or [])[:2] if isinstance(item, Mapping)]
        if not shortlist:
            return ""
        per_item: list[tuple[str, str]] = []
        for item in shortlist:
            name = self.display_item_label(item) or self.item_label(item)
            answer = self.deps.selected_item_attribute_answer(user_text, item)
            if not answer:
                continue
            answer = answer.strip()
            if answer.endswith("."):
                answer = answer[:-1].strip()
            if not name or not answer:
                continue
            per_item.append((name, answer))
        if not per_item:
            return ""
        unique_answers = {ans.lower() for _, ans in per_item}
        if len(unique_answers) == 1:
            common = per_item[0][1].strip()
            if common and common[-1] not in ".!?":
                common += "."
            return common
        parts = [f"{name}: {answer}" for name, answer in per_item]
        merged = ". ".join(part.strip() for part in parts if part.strip()).strip()
        if merged and merged[-1] not in ".!?":
            merged += "."
        return merged

    def item_aliases(self, item: Mapping[str, Any]) -> list[str]:
        aliases: set[str] = set()
        for key in ("title", "name", "model", "sku", "id"):
            value = str(item.get(key) or "").strip()
            if not value:
                continue
            norm = value.lower().replace("ё", "е")
            aliases.add(norm)
            compact = re.sub(r"[^0-9a-zа-яё]+", " ", norm, flags=re.IGNORECASE).strip()
            if compact:
                aliases.add(compact)
        return sorted(alias for alias in aliases if len(alias) >= 3)

    def token_overlap_score(self, query: str, alias: str) -> float:
        q_tokens = {
            tok
            for tok in self.deps.fact_token_re.findall((query or "").lower().replace("ё", "е"))
            if (
                len(tok) >= 2
                and (not tok.isdigit())
                and tok not in self.deps.needs_stopwords
                and tok not in self.deps.generic_model_words
            )
        }
        a_tokens = {
            tok
            for tok in self.deps.fact_token_re.findall((alias or "").lower().replace("ё", "е"))
            if len(tok) >= 2 and (not tok.isdigit())
        }
        if not q_tokens or not a_tokens:
            return 0.0
        overlap = len(q_tokens & a_tokens)
        if overlap == 0:
            return 0.0
        return overlap / max(1, min(len(q_tokens), len(a_tokens)))

    def best_catalog_item_match(
        self,
        query: str,
        items: Sequence[Mapping[str, Any]],
    ) -> Optional[Mapping[str, Any]]:
        text = str(query or "").strip().lower().replace("ё", "е")
        if not text:
            return None
        best_item: Optional[Mapping[str, Any]] = None
        best_score = 0.0
        for item in items:
            aliases = self.item_aliases(item)
            score = 0.0
            for alias in aliases:
                if alias in text and len(alias) >= 4:
                    score = max(score, 2.5)
                    break
                if text in alias and len(text) >= 4:
                    score = max(score, 2.0)
                score = max(score, self.token_overlap_score(text, alias))
            if score > best_score:
                best_score = score
                best_item = item
        if best_score < 0.5:
            return None
        return best_item

    def strict_catalog_item_match(
        self,
        query: str,
        items: Sequence[Mapping[str, Any]],
    ) -> Optional[Mapping[str, Any]]:
        probe = self.deps.normalize_model_alias(query)
        if not probe:
            return None
        for item in items:
            for alias in self.item_aliases(item):
                if self.deps.normalize_model_alias(alias) == probe:
                    return item
        return None
