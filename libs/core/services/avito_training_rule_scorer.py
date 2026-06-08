from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Sequence

from libs.core.services.avito_training_candidate_builder import AvitoTrainingCandidate


_CONTACT_MASK_RE = re.compile(r"\[(PHONE|EMAIL|LINK|HANDLE)\]")
_SERVICE_STATUS_RE = re.compile(
    r"\b(отправил[аи]?|отправили|скинул[аи]?|скинули|передал[аи]?|передали|"
    r"отправим|отправляем|сейчас отправим|по ватсап|по whatsapp|в ватсап|"
    r"в whatsapp|открыли|жд[её]м|ждем)\b",
    re.I,
)
_FOLLOWUP_RE = re.compile(
    r"\b(открыли каталог|посмотрели каталог|актуально|что решили|вам интересно|"
    r"напишите\s+[\"'«»]?стоп|отписаться)\b",
    re.I,
)
_CATALOG_CONTACT_RE = re.compile(
    r"\b(каталог|номер|телефон|ватсап|whatsapp|telegram|телеграм|тг|мах|мессенджер)\b",
    re.I,
)
_CLARIFIER_RE = re.compile(r"\?$|^(сколько|какой|какая|какие|куда|где|когда|размер|адрес)\b", re.I)
_PHONE_LIKE_RE = re.compile(r"(?:\d[\s().,\-–—+]?){6,}")
_MID_DIALOG_RE = re.compile(r"\b(как говорил|как писали|тот вариант|этот вариант|по прошлому|ранее)\b", re.I)


@dataclass(frozen=True)
class AvitoTrainingRuleScore:
    candidate_id: str
    soft_flags: list[str]
    rule_score: int


def score_candidate(candidate: AvitoTrainingCandidate) -> AvitoTrainingRuleScore:
    flags: list[str] = []
    reply = str((candidate.ideal_reply or {}).get("text") or "").strip()
    context_text = " ".join(str(item.get("text") or "") for item in candidate.context)

    if _SERVICE_STATUS_RE.search(reply):
        flags.append("service_status")
    if _FOLLOWUP_RE.search(reply):
        flags.append("followup_ping")
    if len(reply.split()) <= 3:
        flags.append("short_reply")
        if _CLARIFIER_RE.search(reply):
            flags.append("short_clarifier_question")
    if _CATALOG_CONTACT_RE.search(reply):
        flags.append("catalog_or_contact_transfer")
    if _CONTACT_MASK_RE.search(reply) or _CONTACT_MASK_RE.search(context_text):
        flags.append("contains_contact_mask")
    if len(candidate.context) >= 12 or len(context_text) > 2200:
        flags.append("long_context")
    if _PHONE_LIKE_RE.search(reply) or _PHONE_LIKE_RE.search(context_text):
        flags.append("phone_like_after_mask")
    if candidate.context and _MID_DIALOG_RE.search(str(candidate.context[0].get("text") or "")):
        flags.append("context_maybe_mid_dialog")
    if _has_many_manager_templates(candidate.context):
        flags.append("too_many_manager_templates")

    score = max(0, 100 - sum(_FLAG_PENALTY.get(flag, 8) for flag in set(flags)))
    candidate.soft_flags = flags
    candidate.rule_score = score
    return AvitoTrainingRuleScore(candidate_id=candidate.candidate_id, soft_flags=flags, rule_score=score)


def score_candidates(candidates: Sequence[AvitoTrainingCandidate]) -> list[AvitoTrainingCandidate]:
    for candidate in candidates:
        score_candidate(candidate)
    return list(candidates)


def soft_flag_counts(candidates: Sequence[AvitoTrainingCandidate]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for candidate in candidates:
        counter.update(candidate.soft_flags)
    return dict(counter)


def needs_rule_fallback_review(candidate: AvitoTrainingCandidate) -> bool:
    flags = set(candidate.soft_flags or [])
    if flags.intersection({"service_status", "followup_ping", "context_maybe_mid_dialog"}):
        return True
    if flags == {"short_reply"}:
        return False
    return bool(flags.intersection({"catalog_or_contact_transfer", "too_many_manager_templates"}))


def _has_many_manager_templates(context: Sequence[dict[str, str]]) -> bool:
    manager_texts = [
        " ".join(str(item.get("text") or "").lower().split())
        for item in context
        if item.get("role") == "manager"
    ]
    if len(manager_texts) < 4:
        return False
    counts = Counter(manager_texts)
    return any(count >= 3 for count in counts.values())


_FLAG_PENALTY = {
    "service_status": 28,
    "followup_ping": 28,
    "catalog_or_contact_transfer": 14,
    "contains_contact_mask": 8,
    "long_context": 6,
    "too_many_manager_templates": 10,
    "phone_like_after_mask": 20,
    "context_maybe_mid_dialog": 18,
    "short_reply": 3,
    "short_clarifier_question": 0,
}


__all__ = [
    "AvitoTrainingRuleScore",
    "needs_rule_fallback_review",
    "score_candidate",
    "score_candidates",
    "soft_flag_counts",
]
