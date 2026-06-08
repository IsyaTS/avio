from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Pattern, Sequence, Tuple


def normalize_color_token(value: str | None) -> str:
    return str(value or "").strip().casefold().replace("ё", "е")


def normalize_alias_map(raw_map: Mapping[str, Sequence[str]]) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for key, aliases in raw_map.items():
        base = normalize_color_token(key)
        if not base:
            continue
        bucket: set[str] = set()
        for alias in aliases:
            alias_norm = normalize_color_token(alias)
            if alias_norm:
                bucket.add(alias_norm)
        normalized[base] = sorted(bucket)
    return normalized


SIZE_PATTERN = re.compile(
    r"(?P<value>\d{2,4})(?:\s|\-)?(?P<unit>см|mm|мм|cm|м|kg|кг|g|гр|ml|мл|l|л)",
    re.IGNORECASE,
)


COLOR_STEMS = {
    "бел": "белый",
    "черн": "чёрный",
    "чёр": "чёрный",
    "чер": "чёрный",
    "венг": "венге",
    "дуб": "дуб",
    "сер": "серый",
    "корич": "коричневый",
    "красн": "красный",
    "син": "синий",
    "голуб": "голубой",
    "зел": "зелёный",
    "зол": "золотой",
    "сталь": "стальной",
    "беж": "бежевый",
    "медн": "коричневый",
    "шокол": "коричневый",
    "орех": "коричневый",
    "мокк": "коричневый",
    "букл": "коричневый",
    "графит": "чёрный",
    "бетон": "серый",
    "айвор": "белый",
    "жемч": "белый",
    "перлам": "белый",
    "слон": "бежевый",
    "антрац": "чёрный",
}


GLOBAL_COLOR_ALIAS_RAW = {
    "белый": [
        "белый",
        "white",
        "snow",
        "снежный",
        "молочный",
        "молочн",
        "жемчужный",
        "перламутровый",
        "айвори",
        "ivory",
        "слоновая кость",
        "сливочный",
    ],
    "чёрный": [
        "чёрный",
        "черный",
        "черн",
        "black",
        "coal",
        "obsidian",
        "антрацит",
        "антрацитовый",
        "графит",
        "черный муар",
        "onyx",
        "графитовый",
        "каменный",
    ],
    "серый": [
        "серый",
        "серебристый",
        "серебро",
        "серебр",
        "metallic",
        "металлик",
        "grey",
        "gray",
        "бетон",
        "бетонный",
        "cement",
        "стальной",
        "steel",
        "никель",
        "chrome",
    ],
    "коричневый": [
        "коричневый",
        "корич",
        "brown",
        "шоколад",
        "шоколадный",
        "какао",
        "coffee",
        "кофейный",
        "капучино",
        "медный",
        "медный антик",
        "антик медь",
        "коньяк",
        "терракот",
        "терракотовый",
        "орех",
        "венге",
        "мокка",
        "мокко",
        "букле шоколад",
        "каштан",
        "махагон",
        "бурбон",
    ],
    "бежевый": [
        "бежевый",
        "беж",
        "beige",
        "кремовый",
        "крем",
        "песочный",
        "sand",
        "linen",
        "слоновая",
        "ваниль",
        "ретро",
        "сахарный",
        "карамель",
    ],
    "красный": [
        "красный",
        "красн",
        "red",
        "бордо",
        "бордовый",
        "марсала",
        "винный",
        "burgundy",
        "кирпичный",
        "терракота",
        "вишня",
        "carmin",
    ],
    "синий": [
        "синий",
        "син",
        "blue",
        "navy",
        "индиго",
        "ультрамарин",
        "кобальт",
        "лазурь",
    ],
    "голубой": [
        "голубой",
        "голуб",
        "teal",
        "бирюзовый",
        "аквамарин",
        "cyan",
        "лазурный",
        "небесный",
    ],
    "зелёный": [
        "зелёный",
        "зеленый",
        "green",
        "хаки",
        "оливковый",
        "olive",
        "мятный",
        "бирюзово-зелёный",
        "салатовый",
        "forest",
    ],
    "жёлтый": [
        "жёлтый",
        "желтый",
        "yellow",
        "янтарный",
        "amber",
        "горчичный",
        "охра",
        "солнечный",
    ],
    "серебристый": [
        "серебристый",
        "silver",
        "metallic",
        "алюминий",
        "стальной",
        "chrome",
    ],
    "золотой": [
        "gold",
        "golden",
        "золотой",
        "латунный",
        "бронзовый",
        "бронза",
    ],
    "венге": [
        "венге",
        "венге шоколад",
        "венге темный",
        "венге светлый",
    ],
    "орех": [
        "орех",
        "итальянский орех",
        "золотой орех",
        "темный орех",
        "светлый орех",
    ],
    "дуб": [
        "дуб",
        "дуб рустикальный",
        "дуб светлый",
        "дуб темный",
    ],
    "букле": [
        "букле",
        "букле антрацит",
        "букле шоколад",
        "букле серый",
    ],
}


GLOBAL_COLOR_ALIASES = normalize_alias_map(GLOBAL_COLOR_ALIAS_RAW)


@dataclass(frozen=True)
class NeedsRuntimeDeps:
    tokenize_query: Callable[[str | None], List[str]]
    looks_like_address_value: Callable[[str], bool]
    object_type_from_turn_text: Callable[[str], Optional[str]]
    normalize_text: Callable[[Any], str]
    max_price_intent_re: Pattern[str]
    min_price_intent_re: Pattern[str]
    noise_need_re: Pattern[str]
    insulation_need_re: Pattern[str]
    needs_stopwords: Sequence[str]


class NeedsRuntime:
    def __init__(self, deps: NeedsRuntimeDeps) -> None:
        self.deps = deps

    def normalize_color_token(self, value: str | None) -> str:
        return normalize_color_token(value)

    def normalize_alias_map(self, raw_map: Mapping[str, Sequence[str]]) -> dict[str, list[str]]:
        return normalize_alias_map(raw_map)

    def persona_color_alias_map(
        self,
        persona_meta: Mapping[str, Any] | None,
    ) -> dict[str, list[str]]:
        if not isinstance(persona_meta, Mapping):
            return {}
        raw = persona_meta.get("color_aliases")
        if not raw:
            return {}
        mapping: dict[str, list[str]] = {}
        if isinstance(raw, Mapping):
            iterator = raw.items()
        else:
            iterator = []
        for base, aliases in iterator:
            base_norm = normalize_color_token(base)
            if not base_norm:
                continue
            if isinstance(aliases, str):
                alias_list = [aliases]
            elif isinstance(aliases, Sequence):
                alias_list = [str(val) for val in aliases if val]
            elif isinstance(aliases, Mapping):
                alias_list = [str(val) for val in aliases.values() if val]
            else:
                alias_list = [str(aliases)]
            bucket: set[str] = set()
            for alias in alias_list:
                normalized = normalize_color_token(alias)
                if normalized:
                    bucket.add(normalized)
            if bucket:
                mapping[base_norm] = sorted(bucket)
        return mapping

    def augment_color_needs(self, needs: Dict[str, Any], persona_meta: Mapping[str, Any] | None) -> None:
        color_value = needs.get("color")
        if not color_value:
            needs.pop("_color_tokens", None)
            return
        canonical = normalize_color_token(color_value)
        if not canonical:
            needs.pop("_color_tokens", None)
            return
        tokens: set[str] = {canonical}
        tokens.update(GLOBAL_COLOR_ALIASES.get(canonical, []))
        persona_map = self.persona_color_alias_map(persona_meta)
        tokens.update(persona_map.get(canonical, []))
        needs["_color_tokens"] = sorted(tokens)

    def build_color_lookup_map(self, persona_meta: Mapping[str, Any] | None) -> dict[str, str]:
        lookup: dict[str, str] = {}

        def _register(canonical: str, aliases: Sequence[str]) -> None:
            canon = normalize_color_token(canonical)
            if not canon:
                return
            lookup.setdefault(canon, canon)
            for alias in aliases:
                alias_norm = normalize_color_token(alias)
                if alias_norm:
                    lookup.setdefault(alias_norm, canon)

        for base, synonyms in GLOBAL_COLOR_ALIASES.items():
            _register(base, synonyms)

        persona_map = self.persona_color_alias_map(persona_meta)
        for base, synonyms in persona_map.items():
            _register(base, synonyms)

        return lookup

    def collect_color_text(self, item: Mapping[str, Any]) -> str:
        values: list[str] = []
        for key in ("color", "finish", "shade", "title", "name", "tags", "description", "features"):
            value = item.get(key)
            if not value:
                continue
            if isinstance(value, str):
                values.append(value)
            elif isinstance(value, (list, tuple, set)):
                values.extend(str(val) for val in value if val)
        return " ".join(values)

    def enrich_catalog_color_aliases(
        self,
        items: List[Dict[str, Any]],
        persona_meta: Mapping[str, Any] | None,
    ) -> None:
        if not items:
            return
        lookup = self.build_color_lookup_map(persona_meta)
        if not lookup:
            return
        alias_items = sorted(lookup.items(), key=lambda kv: len(kv[0]), reverse=True)
        for item in items:
            text = self.collect_color_text(item)
            if not text:
                continue
            normalized = normalize_color_token(text)
            if not normalized:
                continue
            matched: set[str] = set()
            for alias, canonical in alias_items:
                if alias and alias in normalized:
                    matched.add(canonical)
            if not matched:
                continue
            current = item.get("_color_aliases")
            color_set: set[str] = set(current or [])
            color_set.update(matched)
            item["_color_aliases"] = sorted(color_set)
            tags = item.setdefault("tags", [])
            if isinstance(tags, list):
                for canonical in matched:
                    tag = f"color:{canonical}"
                    if tag not in tags:
                        tags.append(tag)

    def extract_budget(self, text: str) -> Optional[int]:
        if not text:
            return None
        lowered = text.lower()
        candidates: List[int] = []
        for match in re.finditer(r"\d+[\d\s]*", lowered):
            raw_number = match.group(0)
            digits = re.sub(r"\D", "", raw_number)
            if not digits:
                continue
            try:
                value = int(digits)
            except Exception:
                continue

            suffix = lowered[match.end() : match.end() + 4]
            prefix = lowered[max(0, match.start() - 12) : match.start()]

            def _has_token(container: str, tokens: Tuple[str, ...]) -> bool:
                return any(token in container for token in tokens)

            thousand_tokens = ("k", "к", "тыс", "т.", "т ", "тысяч")
            million_tokens = ("млн", "mln")
            currency_tokens = ("₽", "р", "rub", "руб", "eur", "€", "usd", "$")
            context_tokens = ("цен", "стоим", "бюдж", "до", "≈", "~", "max", "за ", "по ")

            if _has_token(suffix, thousand_tokens):
                value *= 1000
            elif _has_token(suffix, million_tokens):
                value *= 1_000_000

            has_currency = _has_token(suffix, currency_tokens) or _has_token(prefix, currency_tokens)
            has_context = _has_token(prefix, context_tokens)

            if value < 100:
                continue
            if not has_currency and not has_context and value < 1000:
                continue

            candidates.append(value)

        if not candidates:
            return None
        return max(candidates)

    def extract_price_order_intent(self, text: str) -> Optional[str]:
        low = str(text or "").lower().replace("ё", "е")
        if not low:
            return None
        if self.deps.max_price_intent_re.search(low):
            return "desc"
        if self.deps.min_price_intent_re.search(low) or ("подешев" in low) or ("дешев" in low):
            return "asc"
        return None

    def looks_like_price_objection(self, text: str) -> bool:
        low = self.deps.normalize_text(text)
        if not low:
            return False
        return any(stem in low for stem in ("дорог", "дешев", "переплат", "цен"))

    def infer_user_needs(self, text: str) -> Dict[str, Any]:
        raw = text or ""
        lowered = raw.lower()
        needs: Dict[str, Any] = {}
        is_address_like_turn = self.deps.looks_like_address_value(raw)

        tokens = [] if is_address_like_turn else self.deps.tokenize_query(raw)
        keywords = [
            tok
            for tok in tokens
            if tok and tok not in self.deps.needs_stopwords and not tok.isdigit()
        ]
        if keywords:
            needs["keywords"] = keywords[:6]
            needs["type"] = keywords[0]
            needs["focus"] = " ".join(keywords[:3])

        size_match = SIZE_PATTERN.search(lowered)
        if size_match:
            value = size_match.group("value")
            unit = size_match.group("unit").lower()
            normalized_unit = {
                "mm": "мм",
                "cm": "см",
                "m": "м",
                "kg": "кг",
                "g": "г",
                "gr": "г",
                "l": "л",
            }.get(unit, unit)
            needs["size"] = f"{value} {normalized_unit}"
            if normalized_unit in {"см", "mm", "мм"}:
                needs["width"] = value

        budget = self.extract_budget(lowered)
        if budget:
            needs["budget_max"] = budget
        price_order = self.extract_price_order_intent(lowered)
        if price_order:
            needs["price_order"] = price_order

        if not is_address_like_turn:
            for stem, title in COLOR_STEMS.items():
                if stem in lowered:
                    needs["color"] = title
                    break

        detected_object_type = self.deps.object_type_from_turn_text(lowered)
        if detected_object_type:
            needs["object_type"] = detected_object_type
        elif re.search(r"(?iu)\bэтаж\w*\b", lowered):
            needs["object_type"] = "apartment"

        if self.deps.noise_need_re.search(lowered):
            needs["noise_priority"] = True
        if self.deps.insulation_need_re.search(lowered):
            needs["insulation_priority"] = True

        return needs
