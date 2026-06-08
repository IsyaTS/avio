from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence


@dataclass(frozen=True)
class AvitoDialogMessage:
    role: str
    text: str
    timestamp: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AvitoDialogFilterResult:
    accepted: bool
    reject_reason: str | None = None
    messages: list[AvitoDialogMessage] = field(default_factory=list)
    filter_stats: dict[str, int] = field(default_factory=dict)


_URL_RE = re.compile(r"https?://|www\.", re.I)
_PHONE_RE = re.compile(r"^\+?[\d\s().-]{7,}$")
_SERVICE_TEXT_PATTERNS = (
    "системное сообщение",
    "ссылка на объявление",
    "пользователь заблокирован",
    "сообщение удалено",
    "прикрепленное изображение",
    "вложение",
    "фото",
)
_AUTORESPONDER_PATTERNS = (
    "напишите стоп",
    "ранее вы интересовались",
    "получили наш каталог",
    "по каталогу и выездом",
    "отписаться от рассылки",
)
_REPEATED_HANDLE = "@dverigermes"
_CONTACT_CATALOG_PATTERNS = (
    "каталог",
    "телефон",
    "номер",
    "whatsapp",
    "ватсап",
    "telegram",
    "телеграм",
    "мах",
    "max",
    "звон",
    "напис",
    "отправили",
)
_BUSINESS_CONTEXT_PATTERNS = (
    "двер",
    "дом",
    "квартир",
    "офис",
    "помещен",
    "размер",
    "цена",
    "стоим",
    "монтаж",
    "установ",
    "налич",
    "материал",
    "цвет",
    "город",
    "адрес",
    "срок",
    "проем",
    "замер",
)
_CONTINUATION_START_RE = re.compile(
    r"^\s*(да|нет|тогда|конечно|хорошо|понял|поняла|ещ[её]|также|тоже|"
    r"в таком случае|поэтому|согласен|согласна|можно|нужно|а |и )\b",
    re.I,
)
_ACK_RE = re.compile(r"^\s*(да|нет|ок|окей|хорошо|спасибо|понял|поняла|стоп)[.!?\s]*$", re.I)


def evaluate_dialog(
    messages: Sequence[AvitoDialogMessage | Mapping[str, Any]],
    *,
    context_complete: bool = True,
) -> AvitoDialogFilterResult:
    normalized = [_coerce_message(message) for message in messages]
    sanitized = [message for message in normalized if _is_exportable_message(message)]
    has_manager_auto = any(
        message.role == "manager" and _is_autoresponder_text(message.text)
        for message in sanitized
    )
    repeated_handle_count = _count_repeated_autoresponder_handle(sanitized)
    stats = {
        "input_messages": len(normalized),
        "exportable_messages": len(sanitized),
        "system_removed": len(normalized) - len(sanitized),
        "autoresponder_messages": int(has_manager_auto),
        "repeated_autoresponder_handle": repeated_handle_count,
    }
    if not sanitized:
        return _reject("system_only", sanitized, stats)

    if has_manager_auto or repeated_handle_count >= 2:
        return _reject("autoresponder_present", sanitized, stats)

    if not context_complete or _looks_like_mid_context(sanitized[0]):
        return _reject("starts_mid_context", sanitized, stats)

    client_indexes = [
        index
        for index, message in enumerate(sanitized)
        if message.role == "client" and _is_meaningful_client_text(message.text)
    ]
    manager_indexes = [
        index
        for index, message in enumerate(sanitized)
        if message.role == "manager" and _is_meaningful_manager_text(message.text)
    ]
    if not client_indexes:
        return _reject("no_real_client", sanitized, stats)
    if not manager_indexes:
        return _reject("no_real_manager", sanitized, stats)

    first_client = client_indexes[0]
    if not any(index > first_client for index in manager_indexes):
        return _reject("no_manager_answer_after_client", sanitized, stats)

    if _is_manager_push_only(sanitized):
        return _reject("manager_push_only", sanitized, stats)

    manager_messages = [message for message in sanitized if message.role == "manager"]
    if manager_messages and all(_is_autoresponder_text(message.text) for message in manager_messages):
        return _reject("autoresponder_only", sanitized, stats)

    if _is_only_contacts_transfer(sanitized):
        return _reject("only_contacts_transfer", sanitized, stats)

    return AvitoDialogFilterResult(
        accepted=True,
        messages=sanitized,
        filter_stats=stats,
    )


def sanitize_messages(
    messages: Iterable[AvitoDialogMessage | Mapping[str, Any]],
) -> list[AvitoDialogMessage]:
    return [
        message
        for message in (_coerce_message(item) for item in messages)
        if _is_exportable_message(message)
    ]


def _reject(
    reason: str,
    messages: list[AvitoDialogMessage],
    stats: dict[str, int],
) -> AvitoDialogFilterResult:
    return AvitoDialogFilterResult(
        accepted=False,
        reject_reason=reason,
        messages=messages,
        filter_stats=stats,
    )


def _coerce_message(message: AvitoDialogMessage | Mapping[str, Any]) -> AvitoDialogMessage:
    if isinstance(message, AvitoDialogMessage):
        return message
    role = str(message.get("role") or "unknown").strip().lower()
    if role in {"user", "customer", "buyer", "in", "incoming"}:
        role = "client"
    elif role in {"assistant", "seller", "manager", "out", "outgoing"}:
        role = "manager"
    elif role not in {"client", "manager", "system"}:
        role = "unknown"
    text = str(message.get("text") or "").strip()
    timestamp = message.get("timestamp")
    return AvitoDialogMessage(
        role=role,
        text=text,
        timestamp=timestamp if isinstance(timestamp, datetime) else None,
        metadata=message.get("metadata") if isinstance(message.get("metadata"), Mapping) else {},
    )


def _is_exportable_message(message: AvitoDialogMessage) -> bool:
    text = _normalize_text(message.text)
    if not text or message.role not in {"client", "manager"}:
        return False
    if any(pattern in text for pattern in _SERVICE_TEXT_PATTERNS):
        return False
    if _URL_RE.search(text) and len(text.split()) <= 3:
        return False
    return True


def _is_meaningful_client_text(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if _PHONE_RE.match(normalized) or _URL_RE.search(normalized):
        return False
    if _ACK_RE.match(normalized):
        return False
    return True


def _is_meaningful_manager_text(text: str) -> bool:
    return bool(_normalize_text(text))


def _looks_like_mid_context(first_message: AvitoDialogMessage) -> bool:
    text = _normalize_text(first_message.text)
    if not text:
        return True
    if first_message.role == "manager" and _is_autoresponder_text(text):
        return False
    return bool(_CONTINUATION_START_RE.match(text))


def _is_manager_push_only(messages: Sequence[AvitoDialogMessage]) -> bool:
    first = messages[0]
    if first.role != "manager":
        return False
    client_texts = [message.text for message in messages if message.role == "client"]
    if not client_texts:
        return True
    if any(_has_business_context(text) for text in client_texts):
        return False
    return _is_autoresponder_text(first.text) or _is_contact_catalog_text(first.text)


def _is_only_contacts_transfer(messages: Sequence[AvitoDialogMessage]) -> bool:
    text_messages = [message.text for message in messages]
    if any(_has_business_context(text) for text in text_messages):
        return False
    return all(_is_contact_catalog_text(text) or _ACK_RE.match(_normalize_text(text)) for text in text_messages)


def _is_autoresponder_text(text: str) -> bool:
    normalized = _normalize_text(text)
    if _is_allowed_manager_catalog_template(normalized):
        return False
    return any(pattern in normalized for pattern in _AUTORESPONDER_PATTERNS)


def _count_repeated_autoresponder_handle(messages: Sequence[AvitoDialogMessage]) -> int:
    return sum(_normalize_text(message.text).count(_REPEATED_HANDLE) for message in messages)


def _is_allowed_manager_catalog_template(normalized: str) -> bool:
    # Allow the known manager catalog/contact template without storing its phone number.
    compact = normalized.replace(" ", "")
    return all(
        part in normalized
        for part in (
            "наш номер",
            "либо отправьте пожалуйста ваш номер телефона",
            "отправим каталог",
            "доставка и установка",
            "оплата только после установки",
            "без предоплат",
        )
    ) and all(part in compact for part in ("тг", "ватсап", "мах"))


def _is_contact_catalog_text(text: str) -> bool:
    normalized = _normalize_text(text)
    if _PHONE_RE.match(normalized):
        return True
    return any(pattern in normalized for pattern in _CONTACT_CATALOG_PATTERNS)


def _has_business_context(text: str) -> bool:
    normalized = _normalize_text(text)
    if "?" in text and not _is_contact_catalog_text(normalized):
        return True
    return any(pattern in normalized for pattern in _BUSINESS_CONTEXT_PATTERNS)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


__all__ = [
    "AvitoDialogFilterResult",
    "AvitoDialogMessage",
    "evaluate_dialog",
    "sanitize_messages",
]
