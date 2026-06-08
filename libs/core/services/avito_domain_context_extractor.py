from __future__ import annotations

import re
from typing import Any, Mapping

from libs.core.services.avito_contextual_case_builder import AvitoContextualCaseCandidate
from libs.core.services.avito_domain_schema_discovery import generic_domain_schema


_PRICE_RE = re.compile(r"(цен[аы]?|стоимост|сколько стоит|руб|₽|тыс|оплат|бюджет)", re.I)
_LOCATION_RE = re.compile(r"(где|адрес|район|город|локац|находит|выезд|выезж|место|улица|ул\.?)", re.I)
_DELIVERY_RE = re.compile(r"(доставк|привез|выезд|выезж|регион|район)", re.I)
_INSTALL_RE = re.compile(r"(установк|монтаж|замер|поставить|сборк)", re.I)
_CONTACT_RE = re.compile(r"\[(PHONE|LINK|EMAIL|HANDLE)\]|(ватсап|whatsapp|телеграм|telegram|тг|мах|телефон|номер|каталог)", re.I)
_TIMING_RE = re.compile(r"(сегодня|завтра|срочно|когда|срок|время|дата|утром|вечером|выходн)", re.I)
_AVAILABILITY_RE = re.compile(r"(есть|налич|свобод|можно|доступн|работаете|занят)", re.I)
_ADDRESS_RE = re.compile(r"\b(адрес|магазин|салон|шоурум|офис|улица|ул\.?|проспект)\b|\b\d{1,4}\s*(?:[а-яa-z]|\b)", re.I)
_CITY_RE = re.compile(
    r"\b(уфа|уфе|уфы|стерлитамак|стерлитамаке|салават|салавате|ишимбай|ишимбае|оренбург|оренбурге|казань|казани)\b",
    re.I,
)
_AREA_RE = re.compile(r"\b\d+(?:[,.]\d+)?\s*(?:сот|га|м2|м²|кв\.?\s*м|квадрат)\w*", re.I)
_HEIGHT_RE = re.compile(r"\b(по\s+пояс|по\s+колено|высок[а-я]*|заросл[а-я]*|трав[а-я]*\s+\d+\s*см)\b", re.I)
_ROOMS_RE = re.compile(r"\b\d+\s*(?:комнат|комн\.?|к)\b", re.I)
_SIZE_RE = re.compile(r"\b\d{2,4}\s*[xх*]\s*\d{2,4}\b|\bразмер\b|\bпро[её]м\b", re.I)
_CITY_MAP = {
    "уфа": "Уфа",
    "уфе": "Уфа",
    "уфы": "Уфа",
    "стерлитамак": "Стерлитамак",
    "стерлитамаке": "Стерлитамак",
    "салават": "Салават",
    "салавате": "Салават",
    "ишимбай": "Ишимбай",
    "ишимбае": "Ишимбай",
    "оренбург": "Оренбург",
    "оренбурге": "Оренбург",
    "казань": "Казань",
    "казани": "Казань",
}


def extract_context(
    candidate: AvitoContextualCaseCandidate,
    *,
    domain_schema: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    schema = dict(domain_schema or generic_domain_schema(tenant_id=int(candidate.tenant_id)))
    history_text = "\n".join(message.text for message in candidate.history)
    client_text = "\n".join(message.text for message in candidate.history if message.role == "client")
    manager_text = "\n".join(message.text for message in candidate.history if message.role == "manager")
    reply_text = candidate.manager_reply.text
    all_text = f"{history_text}\n{reply_text}"
    slots = _extract_slots(all_text, schema)
    client_slots = _extract_slots(client_text, schema)
    for location_key in ("client_city", "location"):
        if location_key in client_slots:
            slots[location_key] = client_slots[location_key]
        else:
            slots.pop(location_key, None)
    client_city = _extract_city(client_text) or _slot_value(client_slots, "client_city") or _slot_value(client_slots, "location")
    business_city = _extract_city(reply_text) or _extract_city(manager_text)
    product_type = _legacy_product_type(all_text) or _slot_value(slots, "product_type") or _slot_value(slots, "door_type")
    premise_type = _legacy_premise_type(all_text) or _slot_value(slots, "premise_type")
    reply_facts = extract_reply_facts(reply_text, slots=slots, business_city=business_city, product_type=product_type)
    intent = _detect_intent(client_text, reply_text, reply_facts)
    missing_slots = _missing_slots(schema, slots, reply_facts)
    known_slots = sorted(slots.keys())
    known_facts = _legacy_known_facts(
        client_city=client_city,
        business_city=business_city,
        product_type=product_type,
        premise_type=premise_type,
    )
    missing_facts = []
    if (intent == "store_location" or reply_facts.get("mentions_address")) and not client_city:
        missing_facts.append("client_city")
    return {
        "context": {
            "intent": intent,
            "stage": _detect_stage(candidate, reply_text),
            "domain": str(schema.get("domain") or "generic_sales"),
            "domain_label": str(schema.get("domain_label") or "продажи"),
            "slots": slots,
            "known_slots": known_slots,
            "missing_slots": missing_slots,
            "client_city": client_city,
            "business_city": business_city,
            "product_type": product_type,
            "premise_type": premise_type,
            "known_facts": known_facts,
            "missing_facts": sorted(set(missing_facts)),
        },
        "reply_facts": reply_facts,
    }


def extract_reply_facts(
    text: str,
    *,
    slots: Mapping[str, Any] | None = None,
    business_city: str | None = None,
    product_type: str | None = None,
) -> dict[str, bool]:
    value = str(text or "")
    mentions_address = bool(_ADDRESS_RE.search(value))
    mentions_price = bool(_PRICE_RE.search(value))
    mentions_location = bool(_LOCATION_RE.search(value) or mentions_address or business_city)
    mentions_delivery = bool(_DELIVERY_RE.search(value))
    mentions_installation = bool(_INSTALL_RE.search(value))
    mentions_contact = bool(_CONTACT_RE.search(value))
    mentions_timing = bool(_TIMING_RE.search(value))
    mentions_availability = bool(_AVAILABILITY_RE.search(value))
    mentions_service_area = bool(mentions_location or mentions_delivery)
    slot_values = slots or {}
    return {
        "mentions_address": mentions_address,
        "mentions_price": mentions_price,
        "mentions_delivery": mentions_delivery,
        "mentions_installation": mentions_installation,
        "mentions_contact": mentions_contact,
        "mentions_location": mentions_location,
        "mentions_timing": mentions_timing,
        "mentions_availability": mentions_availability,
        "mentions_service_area": mentions_service_area,
        "city_specific": bool(mentions_address or business_city or slot_values.get("location") or slot_values.get("client_city")),
        "price_specific": bool(mentions_price),
        "product_specific": bool(product_type or slot_values.get("product_type") or slot_values.get("service_type")),
        "service_specific": bool(slot_values or product_type or mentions_installation or mentions_delivery),
    }


def _extract_slots(text: str, schema: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    slot_keys = set(_string_list(schema.get("required_slots"))) | set(_string_list(schema.get("optional_slots")))
    definitions = schema.get("slot_definitions") if isinstance(schema.get("slot_definitions"), Mapping) else {}
    for key in slot_keys:
        value = _extract_slot_value(key, str(definitions.get(key) or ""), text)
        if value:
            result[key] = value
    return result


def _extract_slot_value(key: str, definition: str, text: str) -> str | None:
    normalized = f"{key} {definition}".lower()
    value = str(text or "")
    if any(token in normalized for token in ("location", "city", "address", "район", "город", "адрес")):
        return _extract_city(value) or ("локация указана" if _LOCATION_RE.search(value) else None)
    if any(token in normalized for token in ("area", "площад", "сотк", "квадрат")):
        match = _AREA_RE.search(value)
        return match.group(0) if match else None
    if any(token in normalized for token in ("grass", "height", "трава", "высот", "зарос")):
        match = _HEIGHT_RE.search(value)
        return match.group(0) if match else None
    if any(token in normalized for token in ("room", "комнат")):
        match = _ROOMS_RE.search(value)
        return match.group(0) if match else None
    if any(token in normalized for token in ("size", "размер", "проем", "проём")):
        match = _SIZE_RE.search(value)
        return match.group(0) if match else None
    if any(token in normalized for token in ("waste", "вывоз", "убор")) and re.search(r"\b(вывоз|убрать|убор)\b", value, re.I):
        return "нужно уточнить"
    if any(token in normalized for token in ("urgency", "сроч")) and _TIMING_RE.search(value):
        return "срок указан"
    if any(token in normalized for token in ("access", "доступ")) and re.search(r"\b(доступ|заезд|проход|ключ)\b", value, re.I):
        return "доступ указан"
    if any(token in normalized for token in ("door", "product", "service", "тип", "товар", "услуг")):
        return _legacy_product_type(value)
    if any(token in normalized for token in ("premise", "помещ", "дом", "квартир")):
        return _legacy_premise_type(value)
    return None


def _detect_intent(client_text: str, reply_text: str, reply_facts: Mapping[str, Any]) -> str:
    latest = str(client_text or "")
    reply = str(reply_text or "")
    if reply_facts.get("mentions_price") or _PRICE_RE.search(latest):
        return "price_question"
    if reply_facts.get("mentions_delivery") or reply_facts.get("mentions_installation"):
        return "delivery_installation"
    if _LOCATION_RE.search(latest) or reply_facts.get("mentions_address"):
        return "store_location"
    if _CONTACT_RE.search(reply) or re.search(r"\b(каталог|вариант|ассортимент)\b", latest, re.I):
        return "catalog_request"
    if reply_facts.get("mentions_availability"):
        return "availability"
    return "other"


def _detect_stage(candidate: AvitoContextualCaseCandidate, reply_text: str) -> str:
    if len(candidate.history) <= 1:
        return "first_touch"
    if re.search(r"\?$", str(reply_text or "").strip()):
        return "clarification"
    if _CONTACT_RE.search(reply_text):
        return "handoff_to_messenger"
    return "offer"


def _missing_slots(schema: Mapping[str, Any], slots: Mapping[str, Any], reply_facts: Mapping[str, Any]) -> list[str]:
    required = set(_string_list(schema.get("required_slots")))
    if reply_facts.get("mentions_price"):
        required.update(_string_list(schema.get("price_depends_on")))
    if reply_facts.get("mentions_location") or reply_facts.get("mentions_service_area"):
        required.update(_string_list(schema.get("location_depends_on")))
    if reply_facts.get("service_specific"):
        required.update(_string_list(schema.get("service_depends_on")))
    if reply_facts.get("mentions_availability"):
        required.update(_string_list(schema.get("availability_depends_on")))
    return sorted(item for item in required if item not in slots)


def _legacy_product_type(text: str) -> str | None:
    normalized = str(text or "").lower()
    if "терморазрыв" in normalized:
        return "дверь с терморазрывом"
    if "межкомнат" in normalized:
        return "межкомнатная дверь"
    if "входн" in normalized:
        return "входная дверь"
    if "двер" in normalized:
        return "дверь"
    return None


def _legacy_premise_type(text: str) -> str | None:
    normalized = str(text or "").lower()
    if "частн" in normalized or "деревен" in normalized:
        return "частный дом"
    if "квартир" in normalized:
        return "квартира"
    if "дом" in normalized:
        return "дом"
    return None


def _legacy_known_facts(
    *,
    client_city: str | None,
    business_city: str | None,
    product_type: str | None,
    premise_type: str | None,
) -> list[str]:
    facts = []
    if client_city:
        facts.append("client_city")
    if business_city:
        facts.append("business_city")
    if product_type:
        facts.append("product_type")
    if premise_type:
        facts.append("premise_type")
    return facts


def _extract_city(text: str) -> str | None:
    match = _CITY_RE.search(str(text or ""))
    if not match:
        return None
    return _CITY_MAP.get(match.group(1).lower())


def _slot_value(slots: Mapping[str, Any], key: str) -> str | None:
    value = str(slots.get(key) or "").strip()
    return value or None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    return sorted({str(item).strip() for item in value if str(item or "").strip()})


__all__ = ["extract_context", "extract_reply_facts"]
