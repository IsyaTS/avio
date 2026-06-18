from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


DEFAULT_ALLOWED_CHANNELS = ("avito",)
FORBIDDEN_PHRASES = (
    "Понял",
    "Принято",
    "Уточните",
    "Чем могу помочь?",
    "Напишите подробнее",
    "Для уточнения информации",
    "Я могу помочь с этим",
)

_CATALOG_RE = re.compile(r"(?iu)\b(каталог|прайс|вариант|модел|фото|цвет|покаж|скин|пришл)\w*")
_PRICE_RE = re.compile(r"(?iu)\b(цен|стоим|сколько|дешев|дорого|бюджет|скидк|рассроч)\w*")
_AVAILABILITY_RE = re.compile(r"(?iu)\b(налич|есть|заказ|готов|можно\s+купить)\w*")
_LOCATION_RE = re.compile(r"(?iu)\b(где|адрес|магазин|шоурум|самовывоз|посмотреть|забрать)\w*")
_INSTALL_RE = re.compile(r"(?iu)\b(установ|монтаж|замер|достав|демонтаж|под\s+ключ|привез)\w*")
_MEASURE_RE = re.compile(r"(?iu)(\d{2,4}\s*[xх*/-]\s*\d{2,4}|\bразмер|\bпро[её]м|\bшир|\bвыс)")
_GREETING_RE = re.compile(r"(?iu)^\s*(здрав|добрый|привет|салам|hello|hi)\b")
_CITY_RE = re.compile(r"(?iu)\b(уфа|стерлитамак|салават|ишимбай|оренбург|казань)\b")
_OBJECT_RE = re.compile(r"(?iu)\b(квартир|дом|коттедж|дач|подъезд|офис|коммерц)\w*")


@dataclass(frozen=True)
class NicheBrainV2Context:
    tenant_id: int
    channel: str
    user_text: str
    history: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    contact_id: int = 0
    tenant_config: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class NicheBrainV2Result:
    enabled: bool = False
    applied: bool = False
    block: str = ""
    detected_client_intent: str = "unknown"
    implied_need: str = ""
    conversation_stage: str = ""
    what_client_likely_means: str = ""
    next_best_action: str = ""
    what_to_answer_first: str = ""
    one_allowed_followup_question: str = ""
    what_not_to_ask: tuple[str, ...] = field(default_factory=tuple)
    forbidden_phrases: tuple[str, ...] = FORBIDDEN_PHRASES
    tone_instruction: str = ""
    risk_flags: tuple[str, ...] = field(default_factory=tuple)


def build_niche_brain_v2_block(context: NicheBrainV2Context) -> NicheBrainV2Result:
    if not _is_enabled_for_context(context):
        return NicheBrainV2Result(enabled=False, applied=False)

    text = str(context.user_text or "").strip()
    intent = _detect_intent(text)
    history = _normalize_history(context.history)
    known_city = bool(_CITY_RE.search(_history_blob(history)) or _CITY_RE.search(text))
    known_object = bool(_OBJECT_RE.search(_history_blob(history)) or _OBJECT_RE.search(text))
    known_measure = bool(_MEASURE_RE.search(_history_blob(history)) or _MEASURE_RE.search(text))

    tactic = _build_tactic(
        text=text,
        intent=intent,
        known_city=known_city,
        known_object=known_object,
        known_measure=known_measure,
    )
    block = _render_block(tactic)
    return NicheBrainV2Result(enabled=True, applied=True, block=block, **tactic)


def _is_enabled_for_context(context: NicheBrainV2Context) -> bool:
    if _env_disabled():
        return False
    behavior = _behavior_config(context.tenant_config)
    cfg = behavior.get("niche_brain_v2")
    if not isinstance(cfg, Mapping):
        return False
    if not _coerce_bool(cfg.get("enabled"), False):
        return False
    if not _coerce_bool(cfg.get("apply_mode"), False):
        return False
    tenant_id = int(context.tenant_id)
    allowlist = _coerce_int_list(cfg.get("tenant_allowlist"))
    if tenant_id not in allowlist:
        return False
    channel = str(context.channel or "").strip().lower()
    if "allowed_channels" not in cfg or cfg.get("allowed_channels") is None:
        allowed_channels = list(DEFAULT_ALLOWED_CHANNELS)
    else:
        allowed_channels = _coerce_str_list(cfg.get("allowed_channels"))
    return channel in {item.lower() for item in allowed_channels}


def _build_tactic(
    *,
    text: str,
    intent: str,
    known_city: bool,
    known_object: bool,
    known_measure: bool,
) -> dict[str, Any]:
    if intent == "catalog_request":
        implied_need = "quickly see suitable door options inside Avito"
        likely = "client asks for catalog as a shortcut to options, not for a PDF ritual"
        action = "offer to pick options in the chat and ask one narrowing question only if needed"
        answer_first = "say that options can be selected here and move straight to category/budget or 2-3 directions"
        followup = "Какие двери смотрите: в квартиру или дом?"
        not_to_ask = ("do not ask a vague 'what exactly interests you'", "do not push messenger switch")
        risks = ("avoid_pdf_dump", "needs_fast_options")
    elif intent == "price":
        implied_need = "understand price level and whether there are suitable options"
        likely = "client wants a concrete price anchor, not a long qualification form"
        action = "answer with grounded price logic if catalog context supports it, otherwise explain what price depends on"
        answer_first = "close the price question before asking anything"
        followup = "На какой бюджет ориентироваться?"
        not_to_ask = ("do not ask several qualification questions before price context",)
        risks = ("catalog_grounding_required",)
    elif intent == "availability":
        implied_need = "know whether the option can be bought soon"
        likely = "client is checking availability and speed"
        action = "answer about availability carefully and ask for model/category if it is missing"
        answer_first = "say that availability is checked by model or suitable category"
        followup = "Какую модель или вариант смотрите?"
        not_to_ask = ("do not ask for address before model/category",)
        risks = ("availability_must_be_verified",)
    elif intent == "location":
        implied_need = "understand where to view or pick up the door"
        likely = "client wants a store/viewing path"
        action = "if city is known, use it; if not, ask city once"
        answer_first = "answer the location intent before selling extra options"
        followup = "В каком городе хотите посмотреть?"
        not_to_ask = ("do not ask budget before resolving city for location intent",)
        risks = ("address_must_be_grounded",)
    elif intent == "install_delivery":
        implied_need = "understand service cost and feasibility"
        likely = "client asks about full job conditions, not only the door"
        action = "separate door selection from service calculation and ask one missing operational fact"
        answer_first = "acknowledge installation/delivery as part of the order and avoid pretending exact cost without data"
        followup = "Город уже известен, нужны размеры проема?" if known_city else "В каком городе нужна установка или доставка?"
        not_to_ask = ("do not request phone first", "do not give exact service price without grounded data")
        risks = ("service_price_depends_on_context",)
    elif intent == "measurements":
        implied_need = "check whether dimensions fit"
        likely = "client gave sizing signal and expects it to be used"
        action = "reuse the size, do not ask it again, move to fitting/model next"
        answer_first = "state that the size/detail is visible and will be used for подбор"
        followup = "Дверь нужна в квартиру или дом?" if not known_object else "По бюджету какой ориентир?"
        not_to_ask = ("do not ask for dimensions again",)
        risks = ("respect_user_provided_dimensions",)
    elif intent == "greeting":
        implied_need = "start quickly without a robotic helpdesk opener"
        likely = "client expects a normal first sales turn"
        action = "open with a short useful offer and one primary qualifier"
        answer_first = "offer подбор дверей here in Avito"
        followup = "Дверь нужна в квартиру или дом?"
        not_to_ask = ("do not ask 'how can I help'", "do not ask budget and size in the same turn")
        risks = ("first_turn_quality",)
    else:
        implied_need = "get a useful next step without repeating obvious questions"
        likely = "client expects a direct answer from context"
        action = "answer the current meaning first, then ask one necessary next question"
        answer_first = "use any known city/object/size before asking"
        followup = _fallback_question(known_city=known_city, known_object=known_object, known_measure=known_measure)
        not_to_ask = ("do not ask what is already clear from the message or history",)
        risks = ("low_context" if not text else "needs_context_sensitive_reply",)

    return {
        "detected_client_intent": intent,
        "implied_need": implied_need,
        "conversation_stage": _conversation_stage(intent),
        "what_client_likely_means": likely,
        "next_best_action": action,
        "what_to_answer_first": answer_first,
        "one_allowed_followup_question": followup,
        "what_not_to_ask": not_to_ask,
        "forbidden_phrases": FORBIDDEN_PHRASES,
        "tone_instruction": (
            "short, concrete Avito seller tone: answer/educated guess first, then one question; "
            "avoid empty acknowledgements unless followed by useful information"
        ),
        "risk_flags": tuple(flag for flag in risks if flag),
    }


def _render_block(tactic: Mapping[str, Any]) -> str:
    lines = [
        "NICHE BRAIN V2 - CURRENT TACTIC",
        "This is the current reply tactic, not a catalog fact. Catalog, persona, and grounding rules still win.",
    ]
    for key in (
        "detected_client_intent",
        "implied_need",
        "conversation_stage",
        "what_client_likely_means",
        "next_best_action",
        "what_to_answer_first",
        "one_allowed_followup_question",
    ):
        lines.append(f"{key}: {_clean_value(tactic.get(key))}")
    lines.append("what_not_to_ask: " + "; ".join(_clean_list(tactic.get("what_not_to_ask"))))
    lines.append("forbidden_phrases: " + "; ".join(FORBIDDEN_PHRASES))
    lines.append(f"tone_instruction: {_clean_value(tactic.get('tone_instruction'))}")
    lines.append("risk_flags: " + "; ".join(_clean_list(tactic.get("risk_flags"))))
    return "\n".join(line for line in lines if line.strip())[:2200]


def _detect_intent(text: str) -> str:
    candidate = str(text or "").strip()
    if not candidate:
        return "unknown"
    if _CATALOG_RE.search(candidate):
        return "catalog_request"
    if _PRICE_RE.search(candidate):
        return "price"
    if _INSTALL_RE.search(candidate):
        return "install_delivery"
    if _AVAILABILITY_RE.search(candidate):
        return "availability"
    if _LOCATION_RE.search(candidate):
        return "location"
    if _MEASURE_RE.search(candidate):
        return "measurements"
    if _GREETING_RE.search(candidate):
        return "greeting"
    return "general"


def _conversation_stage(intent: str) -> str:
    if intent in {"greeting", "catalog_request", "general"}:
        return "primary_need_discovery"
    if intent in {"price", "availability", "location", "install_delivery", "measurements"}:
        return "specific_question_resolution"
    return "context_repair"


def _fallback_question(*, known_city: bool, known_object: bool, known_measure: bool) -> str:
    if not known_object:
        return "Дверь нужна в квартиру или дом?"
    if not known_city:
        return "В каком городе подбираете?"
    if not known_measure:
        return "Размер проема уже знаете?"
    return "По бюджету какой ориентир?"


def _behavior_config(cfg: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(cfg, Mapping):
        return {}
    behavior = cfg.get("behavior")
    return behavior if isinstance(behavior, Mapping) else {}


def _env_disabled() -> bool:
    raw = str(os.getenv("NICHE_BRAIN_V2_DISABLED") or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "disabled"}


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _coerce_int_list(value: Any) -> list[int]:
    raw_items: Sequence[Any]
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    elif isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",")]
    else:
        return []
    result: list[int] = []
    for item in raw_items:
        try:
            result.append(int(item))
        except Exception:
            continue
    return result


def _coerce_str_list(value: Any) -> list[str]:
    raw_items: Sequence[Any]
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    elif isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",")]
    else:
        return []
    return [str(item).strip().lower() for item in raw_items if str(item).strip()]


def _normalize_history(history: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for item in list(history or [])[-8:]:
        role = str(item.get("role") or "").strip().lower()
        content = str(item.get("content") or item.get("text") or "").strip()
        if role in {"user", "assistant"} and content:
            normalized.append({"role": role, "content": content})
    return normalized


def _history_blob(history: Sequence[Mapping[str, str]]) -> str:
    return "\n".join(str(item.get("content") or "") for item in history[-8:])


def _clean_value(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _clean_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [_clean_value(item) for item in value if _clean_value(item)]
    text = _clean_value(value)
    return [text] if text else []


__all__ = [
    "NicheBrainV2Context",
    "NicheBrainV2Result",
    "build_niche_brain_v2_block",
]
