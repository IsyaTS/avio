from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

from libs.core.services.avito_dialog_filter import AvitoDialogMessage
from libs.core.services.avito_training_example_builder import FILTER_VERSION, mask_contacts


CONTACT_MASKS = {"[PHONE]", "[EMAIL]", "[LINK]", "[HANDLE]"}
_ONLY_CONTACT_MASK_RE = re.compile(r"^(?:\[(?:PHONE|EMAIL|LINK|HANDLE)\]\s*)+$")
_ACK_RE = re.compile(r"^(да|нет|ок|окей|хорошо|спасибо|понял|поняла|стоп)[.!?\s]*$", re.I)
_AUTORESPONDER_RE = re.compile(
    r"(по каталогу и выездом|напишите\s+[\"'«»]?стоп[\"'«»]?,?\s*чтобы отписаться)",
    re.I,
)


@dataclass
class AvitoTrainingCandidate:
    source: str
    tenant_id: int
    dialog_id: str
    candidate_id: str
    example_id: str
    channel: str
    context: list[dict[str, str]]
    ideal_reply: dict[str, str]
    created_at: str
    soft_flags: list[str] = field(default_factory=list)
    rule_score: int = 100

    def to_training_example(self) -> dict[str, Any]:
        quality: dict[str, Any] = {
            "accepted": True,
            "source": "manager_dialog",
            "filter_version": FILTER_VERSION,
        }
        if self.soft_flags:
            quality["soft_flags"] = list(self.soft_flags)
        quality["rule_score"] = int(self.rule_score)
        return {
            "source": self.source,
            "tenant_id": int(self.tenant_id),
            "dialog_id": self.dialog_id,
            "example_id": self.example_id,
            "channel": self.channel,
            "context": [dict(item) for item in self.context],
            "ideal_reply": dict(self.ideal_reply),
            "quality": quality,
            "created_at": self.created_at,
        }

    def to_review_example(self, *, reason_code: str, score: int | None = None, tags: Sequence[str] | None = None) -> dict[str, Any]:
        return {
            "source": self.source,
            "tenant_id": int(self.tenant_id),
            "dialog_id": self.dialog_id,
            "candidate_id": self.candidate_id,
            "example_id": self.example_id,
            "channel": self.channel,
            "context": [dict(item) for item in self.context],
            "ideal_reply": dict(self.ideal_reply),
            "quality": {
                "accepted": False,
                "source": "manager_dialog",
                "filter_version": FILTER_VERSION,
                "needs_manual_review": True,
                "reason_code": reason_code,
                "score": int(score if score is not None else self.rule_score),
                "soft_flags": list(self.soft_flags),
                "tags": list(tags or []),
            },
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class AvitoTrainingCandidateBuildResult:
    candidates: list[AvitoTrainingCandidate]
    hard_rejected_count: int
    hard_reject_reasons: dict[str, int]
    stats: dict[str, int]


def build_training_candidates(
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    *,
    tenant_id: int,
    created_at: datetime | None = None,
) -> AvitoTrainingCandidateBuildResult:
    created = _iso_utc(created_at)
    candidates: list[AvitoTrainingCandidate] = []
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
        context: list[dict[str, str]] = []
        example_index = 1
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
            reason = _hard_reject_reason(context, ideal_text)
            if reason:
                _count(hard_reject_reasons, reason)
                context.append({"role": "manager", "text": ideal_text})
                continue

            candidate_id = _candidate_id(context, ideal_text)
            if candidate_id in seen:
                _count(hard_reject_reasons, "exact_duplicate")
                context.append({"role": "manager", "text": ideal_text})
                continue
            seen.add(candidate_id)
            candidates.append(
                AvitoTrainingCandidate(
                    source="avito",
                    tenant_id=int(tenant_id),
                    dialog_id=dialog_id,
                    candidate_id=candidate_id,
                    example_id=f"{dialog_id}_{example_index:04d}",
                    channel="avito",
                    context=[dict(item) for item in context],
                    ideal_reply={"role": "manager", "text": ideal_text},
                    created_at=created,
                )
            )
            example_index += 1
            context.append({"role": "manager", "text": ideal_text})

    return AvitoTrainingCandidateBuildResult(
        candidates=candidates,
        hard_rejected_count=sum(hard_reject_reasons.values()),
        hard_reject_reasons=hard_reject_reasons,
        stats={
            "dialogs_seen": len(dialogs),
            "dialogs_prepared": prepared_dialogs,
            "candidates_built": len(candidates),
        },
    )


def hard_reject_summary(result: AvitoTrainingCandidateBuildResult) -> dict[str, Any]:
    return {
        "hard_rejected_count": int(result.hard_rejected_count),
        "hard_reject_reasons": dict(result.hard_reject_reasons),
        "builder_stats": dict(result.stats),
    }


def _prepare_dialog(dialog: Sequence[AvitoDialogMessage]) -> tuple[list[dict[str, str]], bool]:
    prepared: list[dict[str, str]] = []
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
            prepared.append({"role": role, "text": text})
    return prepared, broken


def _hard_reject_reason(context: Sequence[dict[str, str]], ideal_text: str) -> str | None:
    if not context:
        return "empty_context"
    if not _clean_text(ideal_text):
        return "empty_ideal_reply"
    if not _has_client_context(context):
        return "no_meaningful_client_context"
    if _last_meaningful_role(context) != "client":
        return "last_context_not_client"
    if _ONLY_CONTACT_MASK_RE.match(ideal_text):
        return "contact_only_reply"
    if _AUTORESPONDER_RE.search(ideal_text):
        return "clear_autoresponder_phrase"
    return None


def _has_client_context(context: Sequence[dict[str, str]]) -> bool:
    return any(
        item.get("role") == "client" and _is_meaningful_client_text(item.get("text", ""))
        for item in context
    )


def _last_meaningful_role(context: Sequence[dict[str, str]]) -> str | None:
    for item in reversed(context):
        role = str(item.get("role") or "")
        text = _clean_text(item.get("text", ""))
        if not text:
            continue
        if role == "client" and not _is_meaningful_client_text(text):
            continue
        if role in {"client", "manager"}:
            return role
    return None


def _is_meaningful_client_text(text: str) -> bool:
    normalized = _clean_text(text)
    if not normalized:
        return False
    if normalized in CONTACT_MASKS:
        return False
    return not _ACK_RE.match(normalized)


def _dialog_id(messages: Sequence[dict[str, str]]) -> str:
    signature = "\n".join(f"{item['role']}:{item['text']}" for item in messages)
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _candidate_id(context: Sequence[dict[str, str]], ideal_text: str) -> str:
    signature = "\n".join(f"{item.get('role')}:{item.get('text')}" for item in context)
    signature = f"{signature}\nmanager:{ideal_text}"
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _count(target: dict[str, int], key: str) -> None:
    target[key] = target.get(key, 0) + 1


def _iso_utc(value: datetime | None) -> str:
    dt = value or datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_text(text: str) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split()).strip()


__all__ = [
    "AvitoTrainingCandidate",
    "AvitoTrainingCandidateBuildResult",
    "build_training_candidates",
    "hard_reject_summary",
]
