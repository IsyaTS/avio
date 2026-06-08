from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from libs.core.services.avito_contextual_case_builder import AvitoContextualCaseCandidate


CONTEXTUAL_MODES = {"direct_example", "context_bound", "clarify_first", "style_only"}
REVIEW_MODES = {"review"}
REJECT_MODES = {"reject"}
_ONLY_CONTACT_MASK_RE = re.compile(r"^(?:\[(?:PHONE|EMAIL|LINK|HANDLE)\]\s*)+$")
_CONTACT_TRANSFER_RE = re.compile(
    r"\b(наш номер|номер|телефон|ватсап|whatsapp|телеграм|telegram|тг|мах|отправим каталог|скинем каталог|"
    r"по ватсап|в ватсап|в тг|в мах)\b",
    re.I,
)
_CITY_CLARIFIER_RE = re.compile(r"\b(каком городе|какой город|городе находитесь|где проживаете|откуда вы|вы в каком городе)\b", re.I)
_KNOWN_CITY_RE = re.compile(
    r"\b(уфа|уфе|уфы|стерлитамак|стерлитамаке|салават|салавате|ишимбай|ишимбае|оренбург|оренбурге|казань|казани)\b",
    re.I,
)
_PRICE_SLOT_HINTS = {
    "area_size": ("площад", "сот", "квадрат", "м2", "м²"),
    "grass_height": ("трав", "высот", "зарос", "пояс", "колено"),
    "location": ("район", "адрес", "город", "локац", "место", "участ"),
    "client_city": ("город", "район", "адрес", "локац"),
    "cleaning_type": ("тип убор", "генеральн", "поддерживающ", "после ремонт"),
    "door_type": ("тип двер", "входн", "межкомнат", "термо"),
    "product_type": ("товар", "модель", "тип", "двер"),
    "premise_type": ("дом", "квартир", "помещ", "улиц", "тамбур"),
    "door_size_height_mm": ("размер", "высот", "проем", "проём"),
    "door_size_width_mm": ("размер", "ширин", "проем", "проём"),
    "size": ("размер", "проем", "проём", "ширин", "высот"),
    "installation_needed": ("установ", "монтаж"),
    "delivery_needed": ("достав", "привез", "выезд"),
    "waste_removal": ("вывоз", "убрать", "мусор"),
    "urgency": ("сроч", "сегодня", "завтра", "когда", "срок"),
    "availability_required_date": ("сегодня", "завтра", "дата", "срок", "когда"),
}
_SLOT_HINT_STOPWORDS = {
    "двер",
    "двери",
    "дверей",
    "нужн",
    "нужна",
    "нужно",
    "наличие",
    "клиент",
    "ответ",
    "товар",
    "услуг",
    "работ",
    "есть",
    "можно",
    "требуется",
    "требован",
}


@dataclass(frozen=True)
class AvitoContextualPolicyResult:
    contextual_cases: list[dict[str, Any]]
    review_cases: list[dict[str, Any]]
    reject_reasons: dict[str, int]
    stats: dict[str, int] = field(default_factory=dict)
    quality_summary: dict[str, Any] = field(default_factory=dict)


def classify_cases(
    candidates: Sequence[AvitoContextualCaseCandidate],
    *,
    rule_extractions: Mapping[str, Mapping[str, Any]],
    ai_extractions: Mapping[str, Mapping[str, Any]] | None = None,
    domain_schema: Mapping[str, Any] | None = None,
    ai_extracted_count: int = 0,
    ai_failed_count: int = 0,
    hard_reject_reasons: Mapping[str, int] | None = None,
    builder_stats: Mapping[str, int] | None = None,
) -> AvitoContextualPolicyResult:
    contextual: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    reject_reasons: dict[str, int] = {}
    mode_counts = {
        "direct_example": 0,
        "context_bound": 0,
        "clarify_first": 0,
        "style_only": 0,
        "review": 0,
        "reject": 0,
    }
    ai = ai_extractions or {}

    for candidate in candidates:
        rule_data = dict(rule_extractions.get(candidate.case_id) or {})
        ai_data = dict(ai.get(candidate.case_id) or {})
        case, mode, reason_code, confidence, extractor = _classify_one(
            candidate,
            rule_data=rule_data,
            ai_data=ai_data,
            domain_schema=domain_schema or {},
        )
        mode_counts[mode] = mode_counts.get(mode, 0) + 1
        if mode in CONTEXTUAL_MODES:
            case["quality"] = {
                "status": "usable",
                "confidence": confidence,
                "reason_code": reason_code,
                "extractor": extractor,
            }
            contextual.append(case)
        elif mode in REVIEW_MODES:
            case["quality"] = {
                "status": "review",
                "confidence": confidence,
                "reason_code": reason_code,
                "extractor": extractor,
            }
            review.append(case)
        else:
            _count(reject_reasons, reason_code or "rejected")

    hard_rejects = dict(hard_reject_reasons or {})
    summary = {
        "contextual_cases_count": len(contextual),
        "review_cases_count": len(review),
        "rejected_cases_count": sum(reject_reasons.values()) + sum(int(v) for v in hard_rejects.values()),
        "hard_reject_reasons": hard_rejects,
        "reject_reasons": dict(reject_reasons),
        "mode_counts": dict(mode_counts),
        "ai_extracted_count": int(ai_extracted_count),
        "ai_failed_count": int(ai_failed_count),
        "builder_stats": dict(builder_stats or {}),
    }
    return AvitoContextualPolicyResult(
        contextual_cases=contextual,
        review_cases=review,
        reject_reasons=reject_reasons,
        stats={
            "contextual_cases_count": len(contextual),
            "review_cases_count": len(review),
            "reject_count": int(mode_counts.get("reject", 0)),
            "review_count": int(mode_counts.get("review", 0)),
            "direct_example_count": int(mode_counts.get("direct_example", 0)),
            "context_bound_count": int(mode_counts.get("context_bound", 0)),
            "clarify_first_count": int(mode_counts.get("clarify_first", 0)),
            "style_only_count": int(mode_counts.get("style_only", 0)),
        },
        quality_summary=summary,
    )


def _classify_one(
    candidate: AvitoContextualCaseCandidate,
    *,
    rule_data: Mapping[str, Any],
    ai_data: Mapping[str, Any],
    domain_schema: Mapping[str, Any],
) -> tuple[dict[str, Any], str, str, float, str]:
    extractor = "ai_gpt_5_2" if ai_data else "rule_fallback"
    merged = _merge_extraction(rule_data, ai_data)
    context = _normalize_context(merged.get("context"))
    reply_facts = _normalize_reply_facts(merged.get("reply_facts"))
    applicability = _normalize_applicability(merged.get("applicability"))
    ai_quality = merged.get("quality") if isinstance(merged.get("quality"), Mapping) else {}
    mode = str(applicability.get("mode") or "direct_example")
    reason_code = str(ai_quality.get("reason_code") or "rule_default")
    confidence = _confidence(ai_quality.get("confidence"), default=0.72 if ai_data else 0.55)

    reply_text = candidate.manager_reply.text
    if _ONLY_CONTACT_MASK_RE.match(reply_text):
        mode, reason_code, confidence = "reject", "contact_only_reply", min(confidence, 0.2)
    elif _CITY_CLARIFIER_RE.search(reply_text):
        mode, reason_code = "clarify_first", "clarify_city"
        applicability = _with_mode(applicability, mode, requires=[], without=[])
    elif reply_facts.get("mentions_address") and context.get("business_city") and (
        not context.get("client_city") or not _client_history_mentions_city(candidate)
    ):
        mode, reason_code, confidence = "review", "address_without_client_city", min(confidence, 0.45)
        applicability = _with_mode(applicability, mode, requires=["slots.client_city"], without=["slots.client_city"])
    elif reply_facts.get("mentions_address") or context.get("business_city"):
        mode, reason_code = "context_bound", reason_code if reason_code != "rule_default" else "context_bound_address_answer"
        applicability = _with_mode(applicability, mode, requires=["slots.client_city"], without=["slots.client_city"])
        applicability["same_city_required"] = True
    elif reply_facts.get("mentions_price"):
        mode, reason_code = "context_bound", reason_code if reason_code != "rule_default" else "context_bound_price_answer"
        requirements = _context_bound_requirements(
            context,
            reply_facts,
            domain_schema=domain_schema,
            reply_text=reply_text,
            existing_requires=applicability.get("requires"),
        )
        applicability = _replace_requirements(applicability, mode, requirements=requirements)
    elif _contact_transfer_only(reply_text, reply_facts):
        mode, reason_code, confidence = "review", "messenger_transfer_only", min(confidence, 0.45)
        applicability = _with_mode(applicability, mode, requires=[], without=[])
    elif reply_facts.get("mentions_delivery") or reply_facts.get("mentions_installation"):
        if _schema_has_dependencies(domain_schema):
            mode, reason_code = "context_bound", reason_code if reason_code != "rule_default" else "context_bound_service_answer"
            requirements = _context_bound_requirements(
                context,
                reply_facts,
                domain_schema=domain_schema,
                reply_text=reply_text,
                existing_requires=applicability.get("requires"),
            )
            applicability = _replace_requirements(applicability, mode, requirements=requirements)
        else:
            mode = "direct_example" if mode not in {"context_bound", "review", "reject"} else mode
            reason_code = reason_code if reason_code != "rule_default" else "useful_conditions_answer"
    elif mode not in CONTEXTUAL_MODES | REVIEW_MODES | REJECT_MODES:
        mode = "direct_example"

    if mode in {"direct_example", "style_only"} and context.get("missing_facts"):
        mode, reason_code, confidence = "review", "missing_required_context", min(confidence, 0.5)
        applicability = _with_mode(applicability, mode, requires=list(context.get("missing_facts") or []), without=list(context.get("missing_facts") or []))

    if mode == "context_bound":
        requirements = _context_bound_requirements(
            context,
            reply_facts,
            domain_schema=domain_schema,
            reply_text=reply_text,
            existing_requires=applicability.get("requires"),
        )
        applicability = _replace_requirements(applicability, mode, requirements=requirements)
        if "slots.client_city" in requirements or "client_city" in requirements or "slots.location" in requirements:
            applicability["same_city_required"] = True
        if "slots.product_type" in requirements or "product_type" in requirements:
            applicability["same_product_required"] = True
        if not applicability.get("requires"):
            mode, reason_code, confidence = "review", "context_bound_missing_requirements", min(confidence, 0.5)
            applicability = _with_mode(applicability, mode, requires=[], without=[])

    applicability["mode"] = mode
    case = candidate.base_case()
    case.update(
        {
            "context": context,
            "reply_facts": reply_facts,
            "applicability": applicability,
        }
    )
    return case, mode, reason_code, confidence, extractor


def _merge_extraction(rule_data: Mapping[str, Any], ai_data: Mapping[str, Any]) -> dict[str, Any]:
    if not ai_data:
        return dict(rule_data or {})
    merged = dict(rule_data or {})
    for key in ("context", "reply_facts", "applicability", "quality"):
        value = ai_data.get(key)
        if isinstance(value, Mapping):
            base = dict(merged.get(key) or {}) if isinstance(merged.get(key), Mapping) else {}
            if key == "context":
                base.update(value)
            else:
                base.update({k: v for k, v in value.items() if v is not None})
            merged[key] = base
    return merged


def _client_history_mentions_city(candidate: AvitoContextualCaseCandidate) -> bool:
    client_text = "\n".join(
        str(message.text or "")
        for message in getattr(candidate, "history", []) or []
        if getattr(message, "role", "") == "client"
    )
    return bool(_KNOWN_CITY_RE.search(client_text))


def _normalize_context(value: Any) -> dict[str, Any]:
    raw = dict(value or {}) if isinstance(value, Mapping) else {}
    slots = _string_map(raw.get("slots"))
    return {
        "intent": str(raw.get("intent") or "other"),
        "stage": str(raw.get("stage") or "offer"),
        "domain": str(raw.get("domain") or "generic_sales"),
        "domain_label": _optional_str(raw.get("domain_label")) or "продажи",
        "slots": slots,
        "known_slots": _slot_list(raw.get("known_slots"), slots),
        "missing_slots": _slot_list(raw.get("missing_slots"), slots, allow_unknown=True),
        "client_city": _optional_str(raw.get("client_city")),
        "business_city": _optional_str(raw.get("business_city")),
        "product_type": _optional_str(raw.get("product_type")),
        "premise_type": _optional_str(raw.get("premise_type")),
        "known_facts": _string_list(raw.get("known_facts")),
        "missing_facts": _string_list(raw.get("missing_facts")),
    }


def _normalize_reply_facts(value: Any) -> dict[str, bool]:
    raw = dict(value or {}) if isinstance(value, Mapping) else {}
    return {
        "mentions_address": bool(raw.get("mentions_address")),
        "mentions_price": bool(raw.get("mentions_price")),
        "mentions_delivery": bool(raw.get("mentions_delivery")),
        "mentions_installation": bool(raw.get("mentions_installation")),
        "mentions_contact": bool(raw.get("mentions_contact")),
        "mentions_location": bool(raw.get("mentions_location")),
        "mentions_timing": bool(raw.get("mentions_timing")),
        "mentions_availability": bool(raw.get("mentions_availability")),
        "mentions_service_area": bool(raw.get("mentions_service_area")),
        "city_specific": bool(raw.get("city_specific")),
        "price_specific": bool(raw.get("price_specific")),
        "product_specific": bool(raw.get("product_specific")),
        "service_specific": bool(raw.get("service_specific")),
    }


def _normalize_applicability(value: Any) -> dict[str, Any]:
    raw = dict(value or {}) if isinstance(value, Mapping) else {}
    return {
        "mode": str(raw.get("mode") or "direct_example"),
        "requires": _string_list(raw.get("requires")),
        "same_city_required": bool(raw.get("same_city_required")),
        "same_product_required": bool(raw.get("same_product_required")),
        "safe_as_style_only": bool(raw.get("safe_as_style_only")),
        "do_not_use_directly_without": _string_list(raw.get("do_not_use_directly_without")),
    }


def _context_bound_requirements(
    context: Mapping[str, Any],
    reply_facts: Mapping[str, Any],
    *,
    domain_schema: Mapping[str, Any],
    reply_text: str = "",
    existing_requires: Any = None,
) -> list[str]:
    requirements: list[str] = []
    slots = _string_map(context.get("slots"))
    missing_slots = set(_string_list(context.get("missing_slots")))
    schema_slots = set(_schema_slot_keys(domain_schema))
    existing_slots = set(_unprefix_slots(existing_requires))
    text = " ".join(
        str(part or "")
        for part in (
            reply_text,
            context.get("intent"),
            " ".join(str(item) for item in slots.values()),
        )
    ).lower()

    if reply_facts.get("mentions_address") or reply_facts.get("city_specific") or context.get("business_city"):
        requirements.extend(_focused_slots(("client_city", "location"), domain_schema=domain_schema))
    if reply_facts.get("mentions_price") or reply_facts.get("price_specific") or reply_facts.get("product_specific"):
        requirements.extend(
            _focused_dependency_slots(
                domain_schema,
                "price_depends_on",
                text=text,
                missing_slots=missing_slots,
                existing_slots=existing_slots,
                fallback=("product_type",),
            )
        )
    if reply_facts.get("mentions_delivery") or reply_facts.get("mentions_location") or reply_facts.get("mentions_service_area"):
        requirements.extend(
            _focused_dependency_slots(
                domain_schema,
                "location_depends_on",
                text=text,
                missing_slots=missing_slots,
                existing_slots=existing_slots,
                fallback=("client_city", "location"),
            )
        )
    if reply_facts.get("mentions_installation") or reply_facts.get("service_specific"):
        requirements.extend(
            _focused_dependency_slots(
                domain_schema,
                "service_depends_on",
                text=text,
                missing_slots=missing_slots,
                existing_slots=existing_slots,
                fallback=("product_type",),
            )
        )
    if reply_facts.get("mentions_availability"):
        requirements.extend(
            _focused_dependency_slots(
                domain_schema,
                "availability_depends_on",
                text=text,
                missing_slots=missing_slots,
                existing_slots=existing_slots,
                fallback=(),
            )
        )
    for slot in missing_slots:
        if slot in schema_slots and _slot_is_mentioned(slot, text, domain_schema):
            requirements.append(slot)
    requirements.extend(_legacy_requires(context.get("missing_facts")))
    return _slot_requires(sorted(set(_unprefix_slots(requirements))))


def _domain_requires(domain_schema: Mapping[str, Any], key: str, *, fallback: Sequence[str]) -> list[str]:
    if key in domain_schema:
        return _slot_requires(_string_list(domain_schema.get(key)))
    values = _string_list(domain_schema.get(key))
    if not values:
        values = list(fallback)
    return _slot_requires(values)


def _schema_has_dependencies(domain_schema: Mapping[str, Any]) -> bool:
    return any(
        _string_list(domain_schema.get(key))
        for key in ("price_depends_on", "location_depends_on", "service_depends_on", "availability_depends_on")
    )


def _schema_slot_keys(domain_schema: Mapping[str, Any]) -> list[str]:
    definitions = domain_schema.get("slot_definitions") if isinstance(domain_schema.get("slot_definitions"), Mapping) else {}
    values = set(str(key) for key in definitions.keys())
    for key in ("required_slots", "optional_slots", "price_depends_on", "location_depends_on", "service_depends_on", "availability_depends_on"):
        values.update(_string_list(domain_schema.get(key)))
    return sorted(item for item in values if item)


def _focused_slots(slots: Sequence[str], *, domain_schema: Mapping[str, Any]) -> list[str]:
    schema_slots = set(_schema_slot_keys(domain_schema))
    selected = []
    for slot in slots:
        if not schema_slots or slot in schema_slots:
            selected.append(slot)
    return _slot_requires(selected)


def _focused_dependency_slots(
    domain_schema: Mapping[str, Any],
    key: str,
    *,
    text: str,
    missing_slots: set[str],
    existing_slots: set[str],
    fallback: Sequence[str],
) -> list[str]:
    candidates = _string_list(domain_schema.get(key))
    if key not in domain_schema and not candidates:
        candidates = list(fallback)
    selected = []
    for slot in candidates:
        if _slot_is_mentioned(slot, text, domain_schema):
            selected.append(slot)
    if not selected:
        for slot in candidates:
            if slot in missing_slots and _slot_is_core_dependency(slot, key):
                selected.append(slot)
    if not selected:
        selected = [slot for slot in fallback if slot in _schema_slot_keys(domain_schema) or not _schema_slot_keys(domain_schema)]
    return _slot_requires(selected)


def _slot_is_core_dependency(slot: str, key: str) -> bool:
    core_by_key = {
        "price_depends_on": {"area_size", "grass_height", "location", "cleaning_type", "product_type", "door_type", "premise_type", "size"},
        "location_depends_on": {"client_city", "location", "installation_location_area"},
        "service_depends_on": {"product_type", "door_type", "premise_type", "installation_needed", "delivery_needed", "grass_height", "access"},
        "availability_depends_on": {"client_city", "location", "availability_required_date", "urgency"},
    }
    return slot in core_by_key.get(key, set())


def _slot_is_mentioned(slot: str, text: str, domain_schema: Mapping[str, Any]) -> bool:
    normalized = str(text or "").lower()
    slot_text = str(slot or "").lower()
    hints = list(_PRICE_SLOT_HINTS.get(slot_text, ()))
    hints.extend(part for part in slot_text.split("_") if len(part) >= 5 and part not in _SLOT_HINT_STOPWORDS)
    definitions = domain_schema.get("slot_definitions") if isinstance(domain_schema.get("slot_definitions"), Mapping) else {}
    definition = str(definitions.get(slot) or "").lower()
    hints.extend(
        part
        for part in re.split(r"[^a-zа-яё0-9]+", definition)
        if len(part) >= 6 and part not in _SLOT_HINT_STOPWORDS
    )
    return any(hint and hint in normalized for hint in hints)


def _unprefix_slots(values: Any) -> list[str]:
    result = []
    for item in _string_list(values):
        result.append(item.removeprefix("slots."))
    return sorted(set(result))


def _slot_requires(values: Any) -> list[str]:
    result = []
    for item in _string_list(values):
        result.append(item if item.startswith("slots.") else f"slots.{item}")
    return result


def _legacy_requires(values: Any) -> list[str]:
    return [item for item in _string_list(values) if item]


def _with_mode(
    applicability: dict[str, Any],
    mode: str,
    *,
    requires: Sequence[str],
    without: Sequence[str],
) -> dict[str, Any]:
    updated = dict(applicability)
    updated["mode"] = mode
    updated["requires"] = sorted(set(_string_list(updated.get("requires")) + list(requires)))
    updated["do_not_use_directly_without"] = sorted(
        set(_string_list(updated.get("do_not_use_directly_without")) + list(without))
    )
    return updated


def _replace_requirements(
    applicability: dict[str, Any],
    mode: str,
    *,
    requirements: Sequence[str],
) -> dict[str, Any]:
    updated = dict(applicability)
    normalized = sorted(set(_string_list(requirements)))
    updated["mode"] = mode
    updated["requires"] = normalized
    updated["do_not_use_directly_without"] = normalized
    return updated


def _contact_transfer_only(reply_text: str, reply_facts: Mapping[str, Any]) -> bool:
    text = str(reply_text or "")
    if not _CONTACT_TRANSFER_RE.search(text):
        return False
    useful = any(
        bool(reply_facts.get(key))
        for key in ("mentions_address", "mentions_price", "mentions_delivery", "mentions_installation")
    )
    return not useful


def _optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _string_map(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, str] = {}
    for key, item in value.items():
        key_text = str(key or "").strip()
        item_text = str(item or "").strip()
        if key_text and item_text:
            result[key_text] = item_text
    return result


def _slot_list(value: Any, slots: Mapping[str, Any], *, allow_unknown: bool = False) -> list[str]:
    values = _string_list(value)
    if allow_unknown:
        return values
    allowed = set(slots.keys())
    return [item for item in values if item in allowed]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    result: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            result.append(text)
    return sorted(set(result))


def _confidence(value: Any, *, default: float) -> float:
    try:
        score = float(value)
    except Exception:
        return default
    if score > 1:
        score = score / 100.0
    return max(0.0, min(score, 1.0))


def _count(target: dict[str, int], key: str) -> None:
    target[key] = target.get(key, 0) + 1


__all__ = ["AvitoContextualPolicyResult", "classify_cases"]
