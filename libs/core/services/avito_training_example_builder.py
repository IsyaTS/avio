from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Sequence

from libs.core.services.avito_dialog_filter import AvitoDialogMessage


FILTER_VERSION = "avito_dialog_filter_v1"
_PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d(?:[\d\s\u00a0().,\-–—+]*\d){6,})(?!\d)")
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_LINK_RE = re.compile(r"\b(?:https?://|www\.)\S+", re.I)
_HANDLE_RE = re.compile(r"(?<!\w)@[A-Za-z0-9_]{3,32}\b")
_ACK_RE = re.compile(r"^(да|нет|ок|окей|хорошо|спасибо|понял|поняла|стоп)[.!?\s]*$", re.I)
_ONLY_MASK_RE = re.compile(r"^(?:\[(?:PHONE|EMAIL|LINK|HANDLE)\]\s*)+$")
_NUMBER_WORD_RE = re.compile(
    r"\b(ноль|нуль|один|одна|два|две|три|четыре|пять|шесть|семь|восемь|девять)\b",
    re.I,
)
_SPELLED_PHONE_RE = re.compile(
    r"(?ix)"
    r"(?<!\w)"
    r"(?=(?:[\d\s\u00a0().,\-–—+]|ноль|нуль|один|одна|два|две|три|четыре|пять|шесть|семь|восемь|девять){8,})"
    r"(?:\+?\d+|ноль|нуль|один|одна|два|две|три|четыре|пять|шесть|семь|восемь|девять)"
    r"(?:[\s\u00a0().,\-–—+]+(?:\d+|ноль|нуль|один|одна|два|две|три|четыре|пять|шесть|семь|восемь|девять))+"
    r"(?!\w)"
)
_SERVICE_REPLY_RE = re.compile(
    r"\b("
    r"отправил[аи]?|отправили|скинул[аи]?|скинули|написал[аи]?|написали|"
    r"передал[аи]?|передали|открыли|жд[её]м|ждем|посмотр(ите|ели)|"
    r"по ватсап|по whatsapp|в ватсап|в whatsapp|в тг|в telegram|в мах|"
    r"сейчас скинем|ссылк[ау]"
    r")\b",
    re.I,
)
_FOLLOWUP_REPLY_RE = re.compile(
    r"\b("
    r"открыли каталог|посмотрели каталог|получили каталог|актуально|"
    r"ещ[её] актуально|что решили|вам интересно|напишите стоп|отписаться"
    r")\b",
    re.I,
)


def build_training_examples(
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    *,
    tenant_id: int,
    created_at: datetime | None = None,
) -> list[dict[str, Any]]:
    created = _iso_utc(created_at)
    examples: list[dict[str, Any]] = []
    seen_examples: set[str] = set()
    for dialog in dialogs:
        prepared = _prepare_dialog(dialog)
        if not prepared:
            continue
        dialog_id = _dialog_id(prepared)
        example_index = 1
        context: list[dict[str, str]] = []
        index = 0
        while index < len(prepared):
            message = prepared[index]
            if message["role"] != "manager":
                context.append(message)
                index += 1
                continue

            manager_texts: list[str] = []
            while index < len(prepared) and prepared[index]["role"] == "manager":
                manager_texts.append(prepared[index]["text"])
                index += 1
            ideal_text = _clean_text(" ".join(manager_texts))
            if not ideal_text:
                continue
            if _is_weak_ideal_reply(ideal_text):
                context.append({"role": "manager", "text": ideal_text})
                continue
            if _has_client_context(context) and _last_meaningful_role(context) == "client":
                example_signature = _example_signature(context, ideal_text)
                if example_signature in seen_examples:
                    context.append({"role": "manager", "text": ideal_text})
                    continue
                seen_examples.add(example_signature)
                examples.append(
                    {
                        "source": "avito",
                        "tenant_id": int(tenant_id),
                        "dialog_id": dialog_id,
                        "example_id": f"{dialog_id}_{example_index:04d}",
                        "channel": "avito",
                        "context": [dict(item) for item in context],
                        "ideal_reply": {
                            "role": "manager",
                            "text": ideal_text,
                        },
                        "quality": {
                            "accepted": True,
                            "source": "manager_dialog",
                            "filter_version": FILTER_VERSION,
                        },
                        "created_at": created,
                    }
                )
                example_index += 1
            context.append({"role": "manager", "text": ideal_text})
    return _stable_shuffle_examples(examples)


def mask_contacts(text: str) -> str:
    value = _LINK_RE.sub("[LINK]", str(text or ""))
    value = _EMAIL_RE.sub("[EMAIL]", value)
    value = _mask_spelled_phone(value)
    value = _PHONE_RE.sub("[PHONE]", value)
    value = _HANDLE_RE.sub("[HANDLE]", value)
    return _clean_text(value)


def _prepare_dialog(dialog: Sequence[AvitoDialogMessage]) -> list[dict[str, str]]:
    prepared: list[dict[str, str]] = []
    for message in dialog:
        role = str(getattr(message, "role", "") or "").strip().lower()
        if role not in {"client", "manager"}:
            continue
        text = mask_contacts(getattr(message, "text", ""))
        if text:
            prepared.append({"role": role, "text": text})
    return prepared


def _dialog_id(messages: Sequence[dict[str, str]]) -> str:
    signature = "\n".join(f"{item['role']}:{item['text']}" for item in messages)
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _example_signature(context: Sequence[dict[str, str]], ideal_text: str) -> str:
    signature = "\n".join(f"{item.get('role')}:{item.get('text')}" for item in context)
    signature = f"{signature}\nmanager:{ideal_text}"
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _has_client_context(context: Sequence[dict[str, str]]) -> bool:
    return any(
        item.get("role") == "client" and _is_meaningful_client_text(item.get("text", ""))
        for item in context
    )


def _last_meaningful_role(context: Sequence[dict[str, str]]) -> str | None:
    for item in reversed(context):
        text = _clean_text(item.get("text", ""))
        if not text:
            continue
        if item.get("role") == "client" and not _is_meaningful_client_text(text):
            continue
        if item.get("role") == "manager":
            return str(item.get("role") or "")
        if item.get("role") == "client":
            return "client"
    return None


def _is_meaningful_client_text(text: str) -> bool:
    normalized = _clean_text(text)
    if not normalized:
        return False
    if normalized in {"[PHONE]", "[EMAIL]", "[LINK]", "[HANDLE]"}:
        return False
    return not _ACK_RE.match(normalized)


def _is_weak_ideal_reply(text: str) -> bool:
    normalized = _clean_text(text)
    if not normalized:
        return True
    if _ONLY_MASK_RE.match(normalized):
        return True
    if _is_service_or_followup_reply(normalized):
        return True
    return False


def _is_service_or_followup_reply(text: str) -> bool:
    normalized = _clean_text(text)
    if not normalized:
        return False
    return bool(_SERVICE_REPLY_RE.search(normalized) or _FOLLOWUP_REPLY_RE.search(normalized))


def _mask_spelled_phone(text: str) -> str:
    normalized = re.sub(
        r"(?<=\d)(?=(?:ноль|нуль|один|одна|два|две|три|четыре|пять|шесть|семь|восемь|девять)\b)",
        " ",
        text,
        flags=re.I,
    )

    def replace(match: re.Match[str]) -> str:
        value = match.group(0)
        digits = sum(ch.isdigit() for ch in value)
        word_count = len(_NUMBER_WORD_RE.findall(value))
        if digits >= 5 and word_count >= 1:
            return "[PHONE]"
        return value

    return _SPELLED_PHONE_RE.sub(replace, normalized)


def _stable_shuffle_examples(examples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        examples,
        key=lambda item: hashlib.sha256(
            f"{item.get('example_id')}:{item.get('dialog_id')}".encode("utf-8")
        ).hexdigest(),
    )


def _iso_utc(value: datetime | None) -> str:
    dt = value or datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_text(text: str) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split()).strip()


__all__ = [
    "FILTER_VERSION",
    "build_training_examples",
    "mask_contacts",
]
