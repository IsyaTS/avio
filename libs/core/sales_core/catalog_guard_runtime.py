from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Pattern, Sequence


_SHOUTING_TOKEN_RE = re.compile(r"\b[A-ZА-ЯЁ][A-ZА-ЯЁ0-9./-]{3,}\b")


@dataclass(frozen=True)
class CatalogGuardRuntimeDeps:
    catalog_item_identity: Callable[[Dict[str, Any]], str]
    strict_catalog_item_match: Callable[[str, Sequence[Mapping[str, Any]]], Mapping[str, Any] | None]
    item_aliases: Callable[[Mapping[str, Any]], list[str]]
    item_label: Callable[[Mapping[str, Any]], str]
    normalize_text: Callable[[Any], str]
    fact_token_re: Pattern[str]
    needs_stopwords: Sequence[str]
    generic_model_words: Sequence[str]
    model_quoted_mention_re: Pattern[str]
    generic_price_label_tokens: Sequence[str]
    merge_catalog_results: Callable[
        [List[Dict[str, Any]], List[Dict[str, Any]], int],
        List[Dict[str, Any]],
    ]
    is_price_intent: Callable[[str], bool]
    extract_price_order_intent: Callable[[str], str | None]
    variants_user_hint_re: Pattern[str]
    model_name_intent_re: Pattern[str]
    extract_price_spans: Callable[[str], List[tuple[int, int, int]]]
    is_likely_price_value: Callable[[int], bool]
    format_rub_price: Callable[[int], str]
    item_price_int: Callable[[Mapping[str, Any]], int | None]


class CatalogGuardRuntime:
    def __init__(self, deps: CatalogGuardRuntimeDeps) -> None:
        self.deps = deps

    def normalize_model_alias(self, value: str) -> str:
        return re.sub(r"[^0-9a-zа-яё]+", " ", str(value or "").lower().replace("ё", "е")).strip()

    def grounding_catalog_items(self, grounding: Mapping[str, Any] | None) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[str] = set()
        for bucket_name in ("items", "catalog_items"):
            for raw_item in (grounding or {}).get(bucket_name) or []:
                if not isinstance(raw_item, Mapping):
                    continue
                item = dict(raw_item)
                identity = self.deps.catalog_item_identity(item)
                if identity in seen:
                    continue
                seen.add(identity)
                merged.append(item)
        return merged

    def enforce_catalog_model_grounding(
        self,
        text: str,
        *,
        grounding: Mapping[str, Any] | None = None,
    ) -> str:
        base = (text or "").strip()
        if not base:
            return base
        aliases = {
            self.normalize_model_alias(str(item))
            for item in ((grounding or {}).get("model_aliases") or [])
            if str(item or "").strip()
        }
        aliases = {item for item in aliases if len(item) >= 3}
        if not aliases:
            return base
        items = self.grounding_catalog_items(grounding)

        def _replace(match: re.Match[str]) -> str:
            noun = str(match.group(1) or "").strip()
            model_name = str(match.group(2) or "").strip()
            normalized = self.normalize_model_alias(model_name)
            if normalized and normalized in aliases:
                return match.group(0)
            if items and self.deps.strict_catalog_item_match(model_name, items) is not None:
                return match.group(0)
            if noun:
                return noun
            return ""

        return self.deps.model_quoted_mention_re.sub(_replace, base)

    def reply_mentions_catalog_item(self, text: str, items: Sequence[Mapping[str, Any]]) -> bool:
        hay = self.normalize_model_alias(text)
        if not hay:
            return False
        for item in items:
            label = self.normalize_model_alias(self.deps.item_label(item))
            if not label:
                continue
            if label in hay:
                return True
            tokens = [token for token in label.split() if token]
            if len(tokens) >= 2:
                short = f"{tokens[0]} {tokens[1]}"
                if len(short) >= 5 and short in hay:
                    return True
        return False

    def quote_likely_model_reference(self, source: str, quote_start: int) -> bool:
        raw = str(source or "")
        if not raw:
            return False
        left = raw[max(0, int(quote_start) - 56) : int(quote_start)]
        if not left:
            return False
        near_left = left[-28:]
        if re.search(
            r"(?iu)\b(цвет|оттен|стиль|фактур|дизайн|панел|внутри|снаружи|наружн|внутрен)\w*\b",
            near_left,
        ):
            return False
        return bool(
            re.search(
                r"(?iu)\b(модель|вариант|двер[ья]|позиц(?:ия|ии|ий)?|"
                r"предлож(?:у|ить|им|ите)?|подбер(?:у|ем|ите)?|"
                r"рекоменд(?:ую|уем|ует)|покаж(?:у|ем|ите)?)\b",
                left,
            )
        )

    def looks_like_model_reference_fragment(self, fragment: str) -> bool:
        probe = self.deps.normalize_text(fragment)
        if not probe:
            return False
        if re.search(r"(?iu)https?://|@[a-z0-9_]{2,}", probe):
            return False
        if re.search(
            r"(?iu)\b(telegram|телеграм|whatsapp|ватсап|вотсап|max|"
            r"контакт|телефон|номер|связ|детал|обсуд|удобн|перейд|"
            r"продолж|напис|позвон|каталог)\w*\b",
            probe,
        ):
            return False
        tokens = [tok for tok in self.deps.fact_token_re.findall(probe) if tok]
        if not tokens:
            return False
        if len(tokens) > 6:
            return False
        if tokens[0] in {"и", "или", "а", "но", "что", "чтобы", "как", "для", "в", "на", "с", "по"}:
            return False
        verb_like = sum(
            1 for tok in tokens if re.search(r"(?iu)(ть|ться|йте|ете|ешь|ем|ут|ют|им|ите)$", tok)
        )
        if verb_like >= 1 and len(tokens) >= 3:
            return False
        content_tokens = [
            tok
            for tok in tokens
            if tok not in self.deps.needs_stopwords and tok not in self.deps.generic_model_words
        ]
        return bool(content_tokens)

    def reply_mentions_unknown_model(self, text: str, items: Sequence[Mapping[str, Any]]) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        fragments = re.findall(
            r"(?iu)\b(?:модель|вариант)\s+[«\"]?([a-zа-яё0-9][^\"»\n,.!?;:]{1,80})",
            raw,
        )
        lower_raw = raw.lower().replace("ё", "е")
        if self.deps.extract_price_spans(raw) or re.search(r"(?iu)\b(предлож|двер|модель|вариант)\w*\b", lower_raw):
            for match in re.finditer(r"[«\"]([^\"»\n]{2,80})[»\"]", raw):
                token = str(match.group(1) or "").strip()
                if len(token) < 3:
                    continue
                if not self.quote_likely_model_reference(raw, match.start()):
                    continue
                fragments.append(token)
        for match in re.finditer(
            r"(?iu)\b(?:предлож(?:у|ить|им|ите)?|подбер(?:у|ем|ите)?|"
            r"рекоменд(?:ую|уем|ует)|покаж(?:у|ем|ите)?)\s+"
            r"([a-zа-яё0-9][a-zа-яё0-9\s./-]{2,80})",
            raw,
        ):
            probe = str(match.group(1) or "").strip()
            if probe:
                fragments.append(probe)
        if not fragments:
            return False
        for fragment in fragments:
            probe = str(fragment or "").strip(" -—\t")
            if len(probe) < 3:
                continue
            if not self.looks_like_model_reference_fragment(probe):
                continue
            probe_norm = self.deps.normalize_text(probe)
            if len(self.deps.fact_token_re.findall(probe_norm)) > 6:
                continue
            if any(marker in probe_norm for marker in ("по каталогу", "из каталога", "самый", "самая", "самое")):
                continue
            if self.deps.strict_catalog_item_match(probe, items) is None:
                stripped = re.sub(r"(?iu)\bс\s+зеркал\w*\b", " ", probe)
                stripped = re.sub(r"(?iu)\bзеркал\w*\b", " ", stripped)
                stripped = re.sub(r"\s{2,}", " ", stripped).strip()
                if stripped and self.deps.strict_catalog_item_match(stripped, items) is not None:
                    continue
                return True
        return False

    def neutralize_unknown_model_mentions(
        self,
        text: str,
        items: Sequence[Mapping[str, Any]],
    ) -> str:
        base = str(text or "").strip()
        if not base:
            return base

        def _known_or_close(fragment: str) -> bool:
            probe = str(fragment or "").strip(" -—\t")
            if len(probe) < 3:
                return True
            if self.deps.strict_catalog_item_match(probe, items) is not None:
                return True
            stripped = re.sub(r"(?iu)\bс\s+зеркал\w*\b", " ", probe)
            stripped = re.sub(r"(?iu)\bзеркал\w*\b", " ", stripped)
            stripped = re.sub(r"\s{2,}", " ", stripped).strip()
            if stripped and self.deps.strict_catalog_item_match(stripped, items) is not None:
                return True
            return False

        out = base
        prefixed_re = re.compile(r"(?iu)\b(модель|вариант)\s+[«\"]?([^\"»\n,.!?;:]{2,80})[»\"]?")

        def _replace_prefixed(match: re.Match[str]) -> str:
            fragment = str(match.group(2) or "").strip()
            if not self.looks_like_model_reference_fragment(fragment):
                return match.group(0)
            if _known_or_close(fragment):
                return match.group(0)
            return ""

        out = prefixed_re.sub(_replace_prefixed, out)

        quote_re = re.compile(r"[«\"]([^\"»\n]{2,80})[»\"]")
        src = out

        def _replace_quoted(match: re.Match[str]) -> str:
            fragment = str(match.group(1) or "").strip()
            if _known_or_close(fragment):
                return match.group(0)
            if self.quote_likely_model_reference(src, match.start()):
                return ""
            return match.group(0)

        out = quote_re.sub(_replace_quoted, out)
        verb_re = re.compile(
            r"(?iu)\b((?:могу\s+)?(?:предложить|предложу|подберу|рекомендую|покажу))\s+"
            r"([a-zа-яё0-9][a-zа-яё0-9\s./-]{2,80})"
        )

        def _replace_verb_phrase(match: re.Match[str]) -> str:
            lead = str(match.group(1) or "").strip()
            fragment = str(match.group(2) or "").strip()
            if not self.looks_like_model_reference_fragment(fragment):
                return match.group(0)
            if _known_or_close(fragment):
                return match.group(0)
            return lead

        out = verb_re.sub(_replace_verb_phrase, out)
        out = re.sub(r"(?iu)\bвариант\s+вариант\b", "вариант", out)
        return re.sub(r"\s{2,}", " ", out).strip()

    def neutralize_unverified_priced_labels(
        self,
        text: str,
        items: Sequence[Mapping[str, Any]],
    ) -> str:
        raw = str(text or "").strip()
        if not raw:
            return raw

        def _replace(match: re.Match[str]) -> str:
            label_raw = str(match.group(1) or "").strip()
            tokens = [
                tok
                for tok in self.deps.fact_token_re.findall(self.deps.normalize_text(label_raw))
                if len(tok) >= 3 and tok not in self.deps.generic_price_label_tokens
            ]
            if not tokens:
                return match.group(0)
            probe = " ".join(tokens[-5:])
            if self.deps.strict_catalog_item_match(probe, items) is not None:
                return match.group(0)
            return ""

        out = re.sub(
            r"(?iu)([a-zа-яё0-9][a-zа-яё0-9\s./-]{1,80})\s*(?:—|-|:)\s*(\d{1,3}(?:[ \u00A0\u202F]\d{3})+|\d{4,7})",
            _replace,
            raw,
        )
        return re.sub(r"\s{2,}", " ", out).strip()

    def neutralize_catalog_model_mentions(
        self,
        text: str,
        items: Sequence[Mapping[str, Any]],
    ) -> str:
        raw = str(text or "").strip()
        if not raw or not items:
            return raw
        out = raw
        seen: set[str] = set()
        for item in items:
            label = str(self.deps.item_label(item) or "").strip()
            if len(label) < 3:
                continue
            key = self.normalize_model_alias(label)
            if not key or key in seen:
                continue
            seen.add(key)
            out = re.sub(re.escape(label), "модель из каталога", out, flags=re.IGNORECASE)
            tokens = [token for token in label.split() if token]
            if len(tokens) >= 2:
                short = f"{tokens[0]} {tokens[1]}".strip()
                if len(short) >= 5:
                    out = re.sub(re.escape(short), "модель из каталога", out, flags=re.IGNORECASE)
        out = re.sub(r"(?iu)(модель из каталога[\s,;:]*){2,}", "модель из каталога ", out)
        return re.sub(r"\s{2,}", " ", out).strip()

    def normalize_catalog_name_case(
        self,
        text: str,
        *,
        grounding: Mapping[str, Any] | None = None,
    ) -> str:
        candidate = (text or "").strip()
        if not candidate:
            return candidate
        items = self.grounding_catalog_items(grounding)
        full_items = list((grounding or {}).get("catalog_items") or [])
        if full_items:
            items = self.deps.merge_catalog_results(full_items, items, 500)
        if not items:
            return candidate
        out = candidate
        seen: set[str] = set()
        for item in items:
            label = str(self.deps.item_label(item) or "").strip()
            if len(label) < 3:
                continue
            key = label.lower().replace("ё", "е")
            if key in seen:
                continue
            seen.add(key)
            lower_label = label.lower()
            out = re.sub(re.escape(label), lower_label, out, flags=re.IGNORECASE)
        return out

    def normalize_shouting_case(self, text: str) -> str:
        candidate = str(text or "").strip()
        if not candidate:
            return candidate

        def _replace(match: re.Match[str]) -> str:
            token = str(match.group(0) or "")
            letters = [ch for ch in token if ch.isalpha()]
            if len(letters) < 4:
                return token
            if any(ch.islower() for ch in letters):
                return token
            if len(letters) <= 4:
                return token
            return token.lower()

        return _SHOUTING_TOKEN_RE.sub(_replace, candidate)

    def stabilize_followup_price_reference(
        self,
        text: str,
        *,
        state: Any,
        user_text: str,
        grounding: Mapping[str, Any] | None = None,
    ) -> str:
        candidate = str(text or "").strip()
        if not candidate:
            return candidate
        user_raw = str(user_text or "").strip()
        if not user_raw or not self.deps.is_price_intent(user_raw):
            return candidate
        if self.deps.extract_price_order_intent(user_raw) in {"asc", "desc"}:
            return candidate
        if re.search(r"(?iu)\b(дорог|дешев|переплат)\w*", user_raw):
            return candidate
        if self.deps.variants_user_hint_re.search(user_raw) or self.deps.model_name_intent_re.search(user_raw):
            return candidate
        if re.search(r"\d", user_raw):
            return candidate

        previous_reply = str(getattr(state, "last_bot_reply", "") or "").strip()
        if not previous_reply:
            return candidate
        previous_prices = [
            span
            for span in self.deps.extract_price_spans(previous_reply)
            if self.deps.is_likely_price_value(int(span[2]))
        ]
        current_prices = [
            span
            for span in self.deps.extract_price_spans(candidate)
            if self.deps.is_likely_price_value(int(span[2]))
        ]
        if len(previous_prices) != 1 or not current_prices:
            return candidate

        previous_price = int(previous_prices[0][2])
        if any(int(span[2]) == previous_price for span in current_prices):
            return candidate

        grounding_items = self.grounding_catalog_items(grounding)
        if grounding_items and self.reply_mentions_catalog_item(candidate, grounding_items):
            return candidate

        start, end, _value = min(
            current_prices,
            key=lambda span: abs(int(span[2]) - previous_price),
        )
        replacement = self.deps.format_rub_price(previous_price)
        return f"{candidate[:start]}{replacement}{candidate[end:]}".strip()

    def selected_item_from_grounding(
        self,
        grounding: Mapping[str, Any] | None,
        items: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any] | None:
        raw = (grounding or {}).get("selected_item")
        if isinstance(raw, Mapping):
            selected = dict(raw)
            label = self.deps.item_label(selected)
            if label:
                for item in items:
                    if self.normalize_model_alias(self.deps.item_label(item)) == self.normalize_model_alias(label):
                        return item
            return selected
        return None

    def extract_explicit_model_probe(self, user_text: str) -> str:
        raw = str(user_text or "").strip()
        if not raw:
            return ""
        low = self.deps.normalize_text(raw)
        if not re.search(r"(?iu)\b(есть|имеется|в наличии|подойдет|подойд[её]т)\b", low):
            return ""
        if len(raw) > 55:
            return ""
        stop = {
            "есть",
            "имеется",
            "наличии",
            "в",
            "на",
            "для",
            "или",
            "и",
            "а",
            "подойдет",
            "подойдёт",
            "квартиры",
            "квартира",
            "дома",
            "дом",
            "частного",
        }
        tokens = [tok for tok in self.deps.fact_token_re.findall(low) if len(tok) >= 3 and tok not in stop]
        if len(tokens) < 2 or len(tokens) > 4:
            return ""
        return " ".join(tokens[:5])

    def has_unverified_priced_labels(
        self,
        text: str,
        items: Sequence[Mapping[str, Any]],
    ) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        for match in re.finditer(
            r"(?iu)([a-zа-яё0-9][a-zа-яё0-9\s./-]{1,80})\s*(?:—|-|:)\s*(\d{1,3}(?:[ \u00A0\u202F]\d{3})+|\d{4,7})",
            raw,
        ):
            label_raw = str(match.group(1) or "").strip()
            tokens = [
                tok
                for tok in self.deps.fact_token_re.findall(self.deps.normalize_text(label_raw))
                if len(tok) >= 3 and tok not in self.deps.generic_price_label_tokens
            ]
            if not tokens:
                continue
            probe = " ".join(tokens[-5:])
            if self.deps.strict_catalog_item_match(probe, items) is not None:
                continue
            return True
        return False

    def mentioned_catalog_item_ids(
        self,
        text: str,
        items: Sequence[Mapping[str, Any]],
    ) -> set[str]:
        candidate = self.normalize_model_alias(text)
        if not candidate:
            return set()
        hits: set[str] = set()
        for raw_item in items:
            item = dict(raw_item)
            item_id = self.deps.catalog_item_identity(item)
            if not item_id:
                continue
            for alias in self.deps.item_aliases(item):
                alias_norm = self.normalize_model_alias(alias)
                if len(alias_norm) < 4:
                    continue
                if alias_norm in candidate:
                    hits.add(item_id)
                    break
        return hits
