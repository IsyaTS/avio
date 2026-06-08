from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

from libs.core.services.avito_dialog_filter import AvitoDialogMessage


CONTACT_MASKS = {"[PHONE]", "[EMAIL]", "[LINK]", "[HANDLE]"}
_PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d(?:[\d\s\u00a0().,\-–—+]*\d){6,})(?!\d)")
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_LINK_RE = re.compile(r"\b(?:https?://|www\.)\S+", re.I)
_HANDLE_RE = re.compile(r"(?<!\w)@[A-Za-z0-9_]{3,32}\b")
_ONLY_CONTACT_MASK_RE = re.compile(r"^(?:\[(?:PHONE|EMAIL|LINK|HANDLE)\]\s*)+$")
_ACK_RE = re.compile(r"^(да|нет|ок|окей|хорошо|спасибо|понял|поняла|стоп)[.!?\s]*$", re.I)
_AUTORESPONDER_RE = re.compile(
    r"(по каталогу и выездом|напишите\s+[\"'«»]?стоп[\"'«»]?,?\s*чтобы отписаться)",
    re.I,
)
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


@dataclass(frozen=True)
class AvitoContextualMessage:
    role: str
    text: str

    def to_dict(self) -> dict[str, str]:
        return {"role": self.role, "text": self.text}


@dataclass(frozen=True)
class AvitoContextualCaseCandidate:
    source: str
    tenant_id: int
    dialog_id: str
    case_id: str
    turn_index: int
    channel: str
    history: list[AvitoContextualMessage]
    manager_reply: AvitoContextualMessage
    created_at: str

    def base_case(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "tenant_id": int(self.tenant_id),
            "case_id": self.case_id,
            "dialog_id": self.dialog_id,
            "turn_index": int(self.turn_index),
            "channel": self.channel,
            "dialog": {
                "history": [message.to_dict() for message in self.history],
                "manager_reply": self.manager_reply.to_dict(),
            },
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class AvitoContextualCaseBuildResult:
    candidates: list[AvitoContextualCaseCandidate]
    hard_rejected_count: int
    hard_reject_reasons: dict[str, int]
    stats: dict[str, int] = field(default_factory=dict)


def build_contextual_case_candidates(
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    *,
    tenant_id: int,
    created_at: datetime | None = None,
) -> AvitoContextualCaseBuildResult:
    created = _iso_utc(created_at)
    candidates: list[AvitoContextualCaseCandidate] = []
    hard_reject_reasons: dict[str, int] = {}
    seen: set[str] = set()
    prepared_dialogs = 0

    for dialog in dialogs:
        prepared, broken = _prepare_dialog(dialog)
        if broken:
            _count(hard_reject_reasons, "system_or_unknown_role")
            continue
        if not prepared:
            _count(hard_reject_reasons, "empty_dialog")
            continue
        prepared_dialogs += 1
        dialog_id = _dialog_id(prepared)
        history: list[AvitoContextualMessage] = []
        turn_index = 1
        index = 0
        while index < len(prepared):
            message = prepared[index]
            if message.role != "manager":
                history.append(message)
                index += 1
                continue

            manager_texts: list[str] = []
            while index < len(prepared) and prepared[index].role == "manager":
                manager_texts.append(prepared[index].text)
                index += 1
            reply = AvitoContextualMessage(role="manager", text=_clean_text(" ".join(manager_texts)))
            reason = _hard_reject_reason(history, reply.text)
            if reason:
                _count(hard_reject_reasons, reason)
                if reply.text:
                    history.append(reply)
                continue

            case_id = _case_id(history, reply.text)
            if case_id in seen:
                _count(hard_reject_reasons, "exact_duplicate")
                history.append(reply)
                continue
            seen.add(case_id)
            candidates.append(
                AvitoContextualCaseCandidate(
                    source="avito",
                    tenant_id=int(tenant_id),
                    dialog_id=dialog_id,
                    case_id=case_id,
                    turn_index=turn_index,
                    channel="avito",
                    history=[AvitoContextualMessage(item.role, item.text) for item in history],
                    manager_reply=reply,
                    created_at=created,
                )
            )
            turn_index += 1
            history.append(reply)

    return AvitoContextualCaseBuildResult(
        candidates=candidates,
        hard_rejected_count=sum(hard_reject_reasons.values()),
        hard_reject_reasons=hard_reject_reasons,
        stats={
            "dialogs_seen": len(dialogs),
            "dialogs_prepared": prepared_dialogs,
            "candidates_built": len(candidates),
        },
    )


def mask_contacts(text: str) -> str:
    value = _LINK_RE.sub("[LINK]", str(text or ""))
    value = _EMAIL_RE.sub("[EMAIL]", value)
    value = _mask_spelled_phone(value)
    value = _PHONE_RE.sub("[PHONE]", value)
    value = _HANDLE_RE.sub("[HANDLE]", value)
    return _clean_text(value)


def _prepare_dialog(dialog: Sequence[AvitoDialogMessage]) -> tuple[list[AvitoContextualMessage], bool]:
    prepared: list[AvitoContextualMessage] = []
    broken = False
    for message in dialog:
        role = str(getattr(message, "role", "") or "").strip().lower()
        if role in {"system", ""}:
            continue
        if role not in {"client", "manager"}:
            broken = True
            continue
        text = mask_contacts(getattr(message, "text", ""))
        if text:
            prepared.append(AvitoContextualMessage(role=role, text=text))
    return prepared, broken


def _hard_reject_reason(history: Sequence[AvitoContextualMessage], manager_reply: str) -> str | None:
    if not history:
        return "empty_context"
    if not _clean_text(manager_reply):
        return "empty_manager_reply"
    if not any(item.role == "client" and _is_meaningful_client_text(item.text) for item in history):
        return "no_meaningful_client_context"
    if _last_meaningful_role(history) != "client":
        return "last_context_not_client"
    if _ONLY_CONTACT_MASK_RE.match(manager_reply):
        return "contact_only_reply"
    if _AUTORESPONDER_RE.search(manager_reply):
        return "clear_autoresponder_phrase"
    return None


def _last_meaningful_role(history: Sequence[AvitoContextualMessage]) -> str | None:
    for item in reversed(history):
        text = _clean_text(item.text)
        if not text:
            continue
        if item.role == "client" and not _is_meaningful_client_text(text):
            continue
        if item.role in {"client", "manager"}:
            return item.role
    return None


def _is_meaningful_client_text(text: str) -> bool:
    normalized = _clean_text(text)
    if not normalized or normalized in CONTACT_MASKS:
        return False
    return not _ACK_RE.match(normalized)


def _dialog_id(messages: Sequence[AvitoContextualMessage]) -> str:
    signature = "\n".join(f"{item.role}:{item.text}" for item in messages)
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _case_id(history: Sequence[AvitoContextualMessage], manager_reply: str) -> str:
    signature = "\n".join(f"{item.role}:{item.text}" for item in history)
    signature = f"{signature}\nmanager:{manager_reply}"
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


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


def _iso_utc(value: datetime | None) -> str:
    dt = value or datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_text(text: str) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _count(target: dict[str, int], key: str) -> None:
    target[key] = target.get(key, 0) + 1


__all__ = [
    "AvitoContextualCaseBuildResult",
    "AvitoContextualCaseCandidate",
    "AvitoContextualMessage",
    "build_contextual_case_candidates",
    "mask_contacts",
]
