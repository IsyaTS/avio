from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

_CITY_WORDS: tuple[tuple[str, str], ...] = (
    ("казан", "Казань"),
    ("уф", "Уфа"),
    ("екатеринбург", "Екатеринбург"),
    ("стерлитамак", "Стерлитамак"),
    ("салават", "Салават"),
    ("ишимба", "Ишимбай"),
    ("оренбург", "Оренбург"),
    ("москв", "Москва"),
)


def build_asset_rule_context(
    *,
    tenant_id: int,
    lead_id: int = 0,
    channel: str,
    user_text: str,
    history: Sequence[Mapping[str, Any]] | None = None,
    known_facts: Mapping[str, Any] | None = None,
    avito_item_city: str | None = None,
) -> dict[str, Any]:
    text = _combined_text(user_text, history)
    facts = dict(known_facts or {})
    slots: dict[str, str] = {}
    city = _first_text(avito_item_city, facts.get("ad_city"), facts.get("city"), facts.get("client_city"), _extract_city(text))
    product = _first_text(facts.get("product"), facts.get("product_type"), _extract_product(text))
    account_brand = _first_text(facts.get("account_brand"), _extract_account_brand(facts))
    if city:
        slots["city"] = city
    if product:
        slots["product"] = product
    if account_brand:
        slots["account_brand"] = account_brand
    return {
        "tenant_id": int(tenant_id),
        "lead_id": int(lead_id or 0),
        "channel": str(channel or "").strip().lower(),
        "user_text": str(user_text or ""),
        "slots": slots,
        "known_slots": sorted(slots.keys()),
        "intent": _guess_intent(text),
    }


def _combined_text(user_text: str, history: Sequence[Mapping[str, Any]] | None) -> str:
    parts = [str(user_text or "")]
    for item in list(history or [])[-4:]:
        if isinstance(item, Mapping):
            parts.append(str(item.get("text") or item.get("content") or ""))
    return "\n".join(parts)


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _extract_city(text: str) -> str:
    low = str(text or "").lower().replace("ё", "е")
    for needle, city in _CITY_WORDS:
        if needle in low:
            return city
    return ""


def _extract_product(text: str) -> str:
    low = str(text or "").lower().replace("ё", "е")
    no_mirror = bool(re.search(r"\b(?:без|без\s+.*)\s+зеркал", low))
    if no_mirror and "двер" in low:
        if "квартир" in low:
            return "квартирная дверь без зеркала"
        return "дверь без зеркала"
    if "зеркал" in low and "двер" in low:
        if "квартир" in low:
            return "квартирная дверь с зеркалом"
        return "дверь с зеркалом"
    if "квартир" in low and "двер" in low:
        return "квартирная дверь"
    if "двер" in low:
        return "дверь"
    if "трав" in low or "покос" in low:
        return "покос травы"
    if "клининг" in low or "уборк" in low:
        return "клининг"
    match = re.search(r"(?:нужен|нужна|нужны|ищу|хочу)\s+([а-яa-z0-9 \-]{3,36})", low)
    return match.group(1).strip() if match else ""


def _extract_account_brand(facts: Mapping[str, Any]) -> str:
    text = " ".join(
        str(facts.get(key) or "")
        for key in ("account_label", "account_login", "avito_account_login", "display_name")
    ).lower().replace("ё", "е")
    if "гермес" in text:
        return "germes"
    if "айдар" in text:
        return "aidar"
    return ""


def _guess_intent(text: str) -> str:
    low = str(text or "").lower()
    if "каталог" in low or "вариант" in low or "фото" in low:
        return "catalog_request"
    if "сколько" in low or "цена" in low or "сто" in low:
        return "price_question"
    if "где" in low or "адрес" in low:
        return "location_question"
    return "general"
