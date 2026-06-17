from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


SIZE_RE = re.compile(r"\b\d{2,4}\s*[xх]\s*\d{2,4}\b|\b\d{3,4}[-/]\d{3,4}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"\+?\d[\d\-\s()]{6,}\d")
EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PRICE_RE = re.compile(r"\b(сколько|цена|цен|стоим|прайс|подскажите стоимость)\b", re.IGNORECASE)
CITY_RE = re.compile(r"\b(город|в\s+[А-Яа-яё-]+|адрес|уф[аеыу]|стере|салават|бугуль|оренбург)\b", re.IGNORECASE)
CONTACT_REQUEST_RE = re.compile(r"\b(что.?подбираете|назовите|ваш номер|напишите номер|телефон|контакт)\b", re.IGNORECASE)
LOCATION_QUESTION_RE = re.compile(r"\b(где|адрес|шоурум|находитесь)\b", re.IGNORECASE)
SIZE_QUESTION_RE = re.compile(
    r"\b(какой\s+размер|размер\s+(?:подскажите|уточните|напишите)|подскажите\s+размер|уточните\s+размер|назовите[^\n?.!]{0,40}размер)\b",
    re.IGNORECASE,
)
CITY_QUESTION_RE = re.compile(r"\bкакой город|уточните город|в каком городе\b", re.IGNORECASE)
INSTRUCTION_RE = re.compile(r"\b(систем|инструкц|prompt|system prompt|system|playbook)\b", re.IGNORECASE)
MESSAGE_APP_RE = re.compile(r"\b(whatsapp|вot?сапп?|telegram|tg|макс|max)\b", re.IGNORECASE)
AVAILABILITY_RE = re.compile(r"\b(есть в наличии|в наличии)\b", re.IGNORECASE)
PRICE_VALUE_RE = re.compile(r"\d[\d\s.,]{3,}")
CITY_CLOSE_RE = re.compile(r"\b(спасибо|решили|не актуально|закрыть диалог|пока не нужно)\b", re.IGNORECASE)
HAS_HOUSE_RE = re.compile(r"\bхоз|хозпомещ|хозблок|гараж|кладовка\b", re.IGNORECASE)


@dataclass
class EvalCaseExpected:
    must_answer_about: list[str] | None = None
    should_answer_about: list[str] | None = None
    must_not_contain: list[str] | None = None
    should_contain_meaning: str | None = None
    forbidden_sources: list[str] | None = None
    notes: str | None = None
    detect_rules: list[str] | None = None
    should_not_push_messenger_early: bool = False


def _normalize_case_expected(raw: dict[str, Any] | None) -> EvalCaseExpected:
    raw = raw or {}
    return EvalCaseExpected(
        must_answer_about=list(raw.get("must_answer_about") or []),
        should_answer_about=list(raw.get("should_answer_about") or []),
        must_not_contain=list(raw.get("must_not_contain") or []),
        should_contain_meaning=raw.get("should_contain_meaning"),
        forbidden_sources=list(raw.get("forbidden_sources") or []),
        notes=raw.get("notes"),
        detect_rules=list(raw.get("detect_rules") or []),
        should_not_push_messenger_early=bool(raw.get("should_not_push_messenger_early") or False),
    )


def _contains_any(text: str, patterns: list[str]) -> bool:
    low = (text or "").lower()
    return any(pattern.lower() in low for pattern in patterns if pattern)


_TOPIC_MARKERS: dict[str, list[str]] = {
    "address": ["адрес", "шоурум", "магазин", "где", "самовывоз", "онлайн"],
    "apartment": ["квартир", "помещени", "вариант"],
    "availability": ["налич", "провер", "модель"],
    "b2b": ["юрлиц", "организац", "оптов", "счет"],
    "budget": ["сумм", "бюджет", "цен", "стоим"],
    "catalog": ["каталог", "вариант", "модель", "подборк", "фото"],
    "city": ["город", "уточн", "достав", "зон"],
    "contact": ["контакт", "телефон", "почт", "передам", "получил"],
    "delivery": ["достав", "привез", "адрес", "город", "расстояни", "самовывоз"],
    "discount": ["скидк", "цен", "бюджет", "вариант"],
    "discount_or_negotiation": ["скидк", "опт", "цен", "услов"],
    "installation": ["установ", "монтаж", "замер", "услуг"],
    "locks": ["замк", "замок", "замка"],
    "measurement": ["замер", "размер", "проем"],
    "opening": ["открыв", "лев", "прав"],
    "order": ["заказ", "самовывоз", "замер"],
    "order_status": ["статус", "заказ", "провер"],
    "payment": ["оплат", "налич", "безнал", "счет"],
    "photo": ["фото", "цвет", "каталог", "пример"],
    "pickup": ["самовывоз", "забрать", "адрес"],
    "policy": ["услов", "можно", "документ", "юрлиц"],
    "price": ["цен", "стоим", "сумм", "прайс", "расчет"],
    "product": ["модель", "вариант", "каталог", "товар"],
    "product_details": ["размер", "модель", "материал", "замк", "открыв", "характерист"],
    "scope_of_work": ["работ", "расшир", "срез", "проем", "замер"],
    "service": ["услуг", "сняти", "демонтаж", "установ"],
    "service_area": ["город", "зон", "адрес", "онлайн"],
    "size": ["размер", "проем", "ширин", "высот"],
    "warranty": ["гарант", "срок"],
}


_MEANING_MARKERS: dict[str, list[str]] = {
    "закрыть диалог": ["хорошо", "понял", "если понадобится", "обращайтесь", "будем на связи"],
    "проверим актуальность": ["провер", "актуальн", "объявлен"],
    "подтвердить": ["получил", "передам", "проверим", "ожида"],
}


def _topic_is_covered(topic: str, reply_low: str) -> bool:
    markers = _TOPIC_MARKERS.get(str(topic or "").strip().lower())
    if not markers:
        return True
    return any(marker in reply_low for marker in markers)


def _meaning_is_covered(meaning: str, reply_low: str) -> bool:
    markers = _MEANING_MARKERS.get(str(meaning or "").strip().lower())
    if not markers:
        return str(meaning or "").strip().lower() in reply_low
    return any(marker in reply_low for marker in markers)


def check_reply_quality(
    user_text: str,
    reply_text: str,
    expected: dict[str, Any] | None = None,
    source: str = "llm",
) -> list[str]:
    expected_cfg = _normalize_case_expected(expected)
    user = str(user_text or "").strip()
    reply = str(reply_text or "").strip()
    reply_low = reply.lower()
    violations: list[str] = []

    if not reply:
        violations.append("empty_reply")
        return violations

    if len(reply) > 500:
        violations.append("too_long")

    if source.startswith("fallback"):
        violations.append("generic_fallback")
    if INSTRUCTION_RE.search(reply):
        violations.append("contains_instruction_leak")
    if PHONE_RE.search(reply) or EMAIL_RE.search(reply) or "[PHONE]" in reply or "[EMAIL]" in reply:
        violations.append("contains_phone_or_private_data")

    if CITY_CLOSE_RE.search(user) and (
        "что подбираете" in reply_low or "какую дверь" in reply_low or "что вам" in reply_low
    ):
        violations.append("ignored_close_intent")

    if SIZE_RE.search(user) and SIZE_QUESTION_RE.search(reply_low):
        violations.append("ignored_size")

    if CITY_RE.search(user) and CITY_QUESTION_RE.search(reply_low):
        violations.append("ignored_city")

    if "есть в наличии" in reply_low:
        violations.append("contains_unconfirmed_availability_phrase")

    if PRICE_RE.search(reply_low) and not PRICE_RE.search(user) and not _contains_any(reply_low, ["примерно", "диапазон"]):
        # Если есть явный ценовой токен в ответе на неценовой вопрос.
        violations.append("price_without_price_intent")

    if (HAS_HOUSE_RE.search(user) is False) and (HAS_HOUSE_RE.search(reply_low) and "квартира" in user.lower()):
        violations.append("wrong_category_price")

    if CITY_RE.search(reply_low) is False and LOCATION_QUESTION_RE.search(user) and not _contains_any(reply_low, ["город", "адрес", "наш", "адреса"]):
        violations.append("address_without_location_intent")

    if LOCATION_QUESTION_RE.search(user) and "заказ" not in user.lower() and "каталог" not in user.lower():
        if "в каком" in reply_low or "уточните город" in reply_low:
            violations.append("repeated_question")

    if expected_cfg.should_not_push_messenger_early and MESSAGE_APP_RE.search(reply):
        violations.append("messenger_push_too_early")

    if PHONE_RE.search(user) and (
        "напишите номер" in reply_low or "отправьте номер" in reply_low or "номер" in reply_low and "уточните" in reply_low
    ):
        violations.append("ignored_contact")

    if LOCATION_QUESTION_RE.search(user) and "каталог" in user.lower():
        if "в наличии" in reply_low and "каталог" in reply_low:
            violations.append("catalog_push_without_answer")

    if CONTACT_REQUEST_RE.search(reply) and _contains_any(user.lower(), ["прислали", "после", "спасибо", "уже", "закры", "не нужно", "не актуально"]):
        violations.append("greeting_overrode_request")

    if expected_cfg.forbidden_sources and source in expected_cfg.forbidden_sources:
        violations.append("forbidden_source")

    for needle in expected_cfg.must_not_contain or []:
        if needle and needle.lower() in reply_low:
            violations.append(f"must_not_contain:{needle}")

    if expected_cfg.should_contain_meaning:
        if not _meaning_is_covered(expected_cfg.should_contain_meaning, reply_low):
            violations.append("missing_expected_meaning")

    if expected_cfg.detect_rules:
        for rule in expected_cfg.detect_rules:
            if not rule:
                continue
            if not _topic_is_covered(rule, reply_low):
                violations.append(f"missing_rule:{rule}")

    answer_topics = list(expected_cfg.must_answer_about or []) + list(expected_cfg.should_answer_about or [])
    if answer_topics:
        for topic in answer_topics:
            if not topic:
                continue
            topic_low = topic.lower()
            if topic_low == "location":
                if "город" not in user.lower() and "адрес" not in user.lower():
                    continue
            if not _topic_is_covered(topic_low, reply_low):
                if topic_low == "price":
                    violations.append("price_without_price_intent")
                elif topic_low == "catalog":
                    violations.append("asks_known_slot")
                else:
                    violations.append(f"missing_topic:{topic_low}")

    return sorted(set(violations))
