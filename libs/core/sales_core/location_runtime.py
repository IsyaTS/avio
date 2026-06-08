from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Optional, Pattern, Sequence


@dataclass(frozen=True)
class LocationRuntimeDeps:
    object_type_hint_re: Pattern[str]
    greeting_prefix_re: Pattern[str]
    fact_token_re: Pattern[str]
    needs_stopwords: Sequence[str]
    normalize_text: Callable[[Any], str]
    is_price_intent: Callable[[str], bool]
    is_store_address_intent: Callable[[str], bool]


class LocationRuntime:
    def __init__(self, deps: LocationRuntimeDeps) -> None:
        self.deps = deps

    def looks_like_address_value(self, text: str) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        low = raw.lower().replace("ё", "е")
        has_digit = bool(re.search(r"\d", low))
        if not has_digit:
            tokens = [tok for tok in re.split(r"[\s,.;:()]+", low) if tok]
            if len(tokens) < 1 or len(tokens) > 4:
                return False
            explicit_markers = (
                "ул",
                "улиц",
                "просп",
                "пр-",
                "переул",
                "пер",
                "шоссе",
                "бульвар",
                "наб",
            )
            if any(marker in low for marker in explicit_markers):
                return True
            if self.deps.object_type_hint_re.search(low):
                return False
            blocked_tokens = {
                "для",
                "частного",
                "частный",
                "дома",
                "дом",
                "квартиры",
                "квартира",
                "помещения",
                "помещение",
            }
            suffix_hits = 0
            for tok in tokens:
                if tok in blocked_tokens or tok in self.deps.needs_stopwords:
                    continue
                if re.search(r"(ского|ской|ская|ский|ина|ова|ева|овка|евка)$", tok):
                    suffix_hits += 1
            return suffix_hits >= 1 and 2 <= len(tokens) <= 3

        markers = (
            "ул",
            "улиц",
            "просп",
            "пр-",
            "дом",
            "д.",
            "корп",
            "к.",
            "стр",
            "с.",
            "переул",
            "пер",
            "шоссе",
            "бульвар",
            "наб",
            "/",
            "-",
        )
        if any(marker in low for marker in markers):
            return True
        tokens = [tok for tok in re.split(r"[\s,]+", low) if tok]
        if len(tokens) < 2 or len(tokens) > 5:
            return False
        has_letters = any(bool(re.search(r"[a-zа-я]", tok, re.IGNORECASE)) for tok in tokens)
        street_like = any(
            bool(re.search(r"(ского|ской|ская|ский|ина|ова|ева|овка|евка)$", tok))
            for tok in tokens
        )
        if self.deps.object_type_hint_re.search(low):
            return False
        return has_letters and has_digit and street_like

    def is_plausible_city_text(self, text: str) -> bool:
        raw = str(text or "").strip()
        if not raw:
            return False
        low_raw = raw.lower().replace("ё", "е")
        if self.deps.greeting_prefix_re.match(raw):
            return False
        if re.search(r"(?iu)\b(здравств\w*|привет\w*|добр\w+\s+д\w*|салам\w*|hello|hi)\b", low_raw):
            return False
        if low_raw in {"здравствуйте", "добрый день", "добрый вечер", "привет", "салам", "hello", "hi"}:
            return False
        if "?" in raw:
            return False
        if self.looks_like_address_value(raw):
            return False
        tokens = [tok for tok in re.split(r"[\s,.;:()]+", raw) if tok]
        if not tokens or len(tokens) > 3:
            return False
        normalized_tokens = [str(tok).lower().replace("ё", "е") for tok in tokens if str(tok).strip()]
        non_city_markers = {
            "зачем",
            "почему",
            "когда",
            "куда",
            "как",
            "что",
            "кто",
            "чего",
            "чем",
            "вам",
            "мне",
            "тебе",
            "адрес",
            "установка",
            "установки",
            "нужно",
            "надо",
        }
        if any(tok in non_city_markers for tok in normalized_tokens):
            return False
        if normalized_tokens and all(tok in self.deps.needs_stopwords for tok in normalized_tokens):
            return False
        if all(re.fullmatch(r"\d+", tok) for tok in tokens):
            return False
        if not any(re.search(r"[A-Za-zА-Яа-яЁё]", tok) for tok in tokens):
            return False
        for token in tokens:
            tok = str(token).strip()
            if not re.search(r"[А-Яа-яЁё]", tok):
                continue
            low = tok.lower()
            if low.endswith("ться") or low.endswith("ть") or low.endswith("чь"):
                return False
        return True

    def extract_city_hint(self, text: str, *, allow_standalone: bool = False) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        match = re.search(r"(?iu)\bгород\s+([A-Za-zА-Яа-яЁё\- ]{2,40})", raw)
        if match:
            candidate = match.group(1).strip(" ,.;:!?")
            if self.is_plausible_city_text(candidate):
                return candidate
        match = re.search(r"(?iu)\bиз\s+([A-Za-zА-Яа-яЁё\- ]{2,40})", raw)
        if match:
            candidate = match.group(1).strip(" ,.;:!?")
            if self.is_plausible_city_text(candidate):
                return candidate
        if allow_standalone and self.is_plausible_city_text(raw):
            return raw
        return ""

    def extract_standalone_city_hint(self, text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        if "?" in raw:
            return ""
        low = self.deps.normalize_text(raw)
        if self.deps.object_type_hint_re.search(low):
            return ""
        if self.deps.is_price_intent(raw) or self.deps.is_store_address_intent(raw):
            return ""
        tokens = [tok for tok in self.deps.fact_token_re.findall(raw) if tok]
        if not tokens or len(tokens) > 3:
            return ""
        return self.extract_city_hint(raw, allow_standalone=True)

    def canonical_object_type_hint(self, value: Any) -> str:
        low = self.deps.normalize_text(value)
        if not low:
            return ""
        is_apartment = bool(re.search(r"(?iu)\b(apartment|flat|квартир\w*|кв\.)\b", low))
        is_house = bool(re.search(r"(?iu)\b(house|home|частн\w*|коттедж\w*|дом\w*)\b", low))
        if is_apartment and not is_house:
            return "apartment"
        if is_house and not is_apartment:
            return "house"
        return ""

    def object_type_from_turn_text(self, text: str) -> str:
        low = self.deps.normalize_text(text)
        if not low:
            return ""
        apartment_markers = (
            "квартир",
            "кв.",
            "кв ",
            "апартамент",
            "flat",
            "apartment",
        )
        house_markers = (
            "частн",
            "дом",
            "коттедж",
            "таунхаус",
            "house",
            "home",
        )
        is_apartment = any(marker in low for marker in apartment_markers)
        is_house = any(marker in low for marker in house_markers)
        if is_apartment and not is_house:
            return "apartment"
        if is_house and not is_apartment:
            return "house"
        return ""

    def extract_store_addresses_from_persona(self, persona_context: str) -> dict[str, str]:
        mapping: dict[str, str] = {}
        text = str(persona_context or "")
        if not text:
            return mapping
        for raw in text.splitlines():
            line = raw.strip().strip("-").strip()
            if not line:
                continue
            m = re.match(r"(?u)^([A-Za-zА-Яа-яЁё/\s]{2,40})\s*[—-]\s*(.{3,120})$", line)
            if not m:
                continue
            city = str(m.group(1) or "").strip().lower().replace("ё", "е")
            addr = str(m.group(2) or "").strip()
            if not city or not addr:
                continue
            if any(bad in city for bad in ("магаз", "адрес", "telegram", "гарант", "бесплат")):
                continue
            mapping[city] = addr
        return mapping
