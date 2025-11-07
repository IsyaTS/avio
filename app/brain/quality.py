from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Iterable, Optional, Protocol

from .planner import GeneratedPlan

CTA_SIMILARITY_THRESHOLD = 0.82


class PersonaHintsProtocol(Protocol):
    cta: str

    def wants_friendly(self) -> bool:
        ...

    @property
    def no_emoji(self) -> bool:  # pragma: no cover - protocol shim
        return False


@dataclass
class EnforcementContext:
    """Context required to align reply with the generated plan."""

    channel: str = "whatsapp"
    max_questions: int = 1
    asked_fingerprints: set[str] = field(default_factory=set)
    fingerprint_map: dict[str, str] = field(default_factory=dict)
    persona_cta: str = ""
    allow_cta: bool = True
    recent_cta: str = ""
    recent_cta_ts: float = 0.0
    disable_channel_switch_prompts: bool = True
    applied_questions: list[str] = field(default_factory=list)
    applied_cta: str = ""

    def fingerprint_used(self, fingerprint: str) -> bool:
        return fingerprint in self.asked_fingerprints

    def register_fingerprint(self, fingerprint: str, question: str | None = None) -> None:
        self.asked_fingerprints.add(fingerprint)
        if question:
            self.fingerprint_map[fingerprint] = question


def _normalize(text: str) -> str:
    return (text or "").strip()


_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def _tokenize(text: str) -> list[str]:
    raw_tokens = _TOKEN_RE.findall((text or "").lower())
    normalized: list[str] = []
    for token in raw_tokens:
        cleaned = token.replace("ё", "е")
        stemmed = cleaned.rstrip("ауоияеыюьй")
        normalized.append(stemmed or cleaned)
    return [token for token in normalized if token]


def question_fingerprint(question: str) -> str:
    tokens = sorted(_tokenize(question))
    return " ".join(tokens)


def _question_present(question: str, reply: str) -> bool:
    q_tokens = set(_tokenize(question))
    if not q_tokens:
        return False
    for sentence in re.split(r"[\\n\\r]+|[.!?]", reply or ""):
        sentence_tokens = set(_tokenize(sentence))
        if sentence_tokens and q_tokens.issubset(sentence_tokens):
            return True
    return False


def _looks_like_channel_switch(question: str) -> bool:
    lowered = question.lower()
    return any(
        token in lowered
        for token in (
            "канал",
            "какой канал",
            "где удобнее",
            "перейд",
            "whatsapp",
            "телеграм",
            "telegram",
        )
    )


def _append_block(reply: str, block: str) -> str:
    if not block:
        return reply
    base = reply.rstrip()
    if not base:
        return block
    return f"{base}\n\n{block.strip()}"


def _cta_present(cta: str, reply: str) -> bool:
    norm_cta = _normalize(cta)
    if not norm_cta:
        return False
    pattern = re.escape(norm_cta)
    return bool(re.search(pattern, reply, flags=re.IGNORECASE))


def _cta_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    a_tokens = set(re.findall(r"\w+", a.lower()))
    b_tokens = set(re.findall(r"\w+", b.lower()))
    if not a_tokens or not b_tokens:
        return 0.0
    overlap = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return overlap / union if union else 0.0


def _select_cta(
    candidates: Iterable[str],
    reply: str,
    *,
    allow_cta: bool,
    recent_cta: str,
    recent_cta_ts: float,
) -> str:
    if not allow_cta:
        return ""

    now_ts = time.time()
    cooldown_active = recent_cta and (now_ts - recent_cta_ts) < 180.0
    for candidate in candidates:
        clean = _normalize(candidate)
        if not clean:
            continue
        if _cta_present(clean, reply):
            return clean
        if cooldown_active:
            if _cta_similarity(clean, recent_cta) >= CTA_SIMILARITY_THRESHOLD:
                continue
        return clean
    return ""


def enforce_plan_alignment(
    reply: str,
    plan: GeneratedPlan,
    persona_hints: Optional[PersonaHintsProtocol] = None,
    *,
    context: Optional[EnforcementContext] = None,
) -> str:
    """Ensure CTA and planned questions make it into the final reply."""

    text = _normalize(reply)
    if not text:
        return text

    ctx = context or EnforcementContext()
    channel = (ctx.channel or "whatsapp").lower()

    filtered_questions: list[str] = []
    for raw in plan.next_questions:
        question = _normalize(raw)
        if not question:
            continue
        fingerprint = question_fingerprint(question)
        if ctx.fingerprint_used(fingerprint):
            continue
        if ctx.disable_channel_switch_prompts and channel != "avito":
            if _looks_like_channel_switch(question):
                continue
        if len(filtered_questions) >= max(0, ctx.max_questions):
            continue
        filtered_questions.append(question)
        ctx.register_fingerprint(fingerprint, question)

    if filtered_questions != list(plan.next_questions):
        plan.next_questions = filtered_questions

    appended_questions: list[str] = []
    for question in filtered_questions[: max(1, ctx.max_questions)]:
        if _question_present(question, text):
            continue
        text = _append_block(text, question)
        appended_questions.append(question)
        break
    ctx.applied_questions = appended_questions

    plan_cta = _normalize(plan.cta)
    persona_cta = _normalize(ctx.persona_cta or getattr(persona_hints, "cta", ""))
    cta_candidates = [candidate for candidate in (plan_cta, persona_cta) if candidate]

    selected_cta = _select_cta(
        cta_candidates,
        text,
        allow_cta=ctx.allow_cta,
        recent_cta=ctx.recent_cta,
        recent_cta_ts=ctx.recent_cta_ts,
    )
    if selected_cta and not _cta_present(selected_cta, text):
        text = _append_block(text, selected_cta)
    ctx.applied_cta = selected_cta

    if persona_hints and persona_hints.wants_friendly():
        if not getattr(persona_hints, "no_emoji", False):
            if not re.search(r"[\)\]»☺😊😀😄😃😉😎❤️]", text):
                text = text + " \U0001F60A"

    if ctx.disable_channel_switch_prompts and (ctx.channel or "").lower() != "avito":
        text = _strip_channel_switch_blocks(text)
    text = _strip_forbidden_cta(text, ctx.allow_cta)
    text = _dedupe_repeated_blocks(text)
    return text.strip()


def _dedupe_repeated_blocks(text: str) -> str:
    """
    Collapse identical question/statement lines that the LLM may have duplicated.

    We fingerprint questions with question_fingerprint and fall back to a casefolded
    line comparison for neutral statements. Only the first occurrence is kept.
    """

    if not text:
        return text

    lines = [line.rstrip() for line in text.splitlines()]
    deduped: list[str] = []
    seen_questions: set[str] = set()
    seen_sentences: set[str] = set()

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if deduped and deduped[-1]:
                deduped.append("")
            continue

        if "?" in line:
            fingerprint = question_fingerprint(line)
            if fingerprint in seen_questions:
                continue
            seen_questions.add(fingerprint)
            deduped.append(line)
            continue

        normalized = re.sub(r"\s+", " ", line.casefold())
        if normalized in seen_sentences:
            continue
        seen_sentences.add(normalized)
        deduped.append(line)

    # keep intentional spacing: collapse multiple blanks while preserving paragraph breaks
    cleaned: list[str] = []
    blank_pending = False
    for line in deduped:
        if not line:
            blank_pending = True
            continue
        if blank_pending and cleaned:
            cleaned.append("")
        cleaned.append(line)
        blank_pending = False

    return "\n".join(cleaned)


def _strip_channel_switch_blocks(text: str) -> str:
    if not text:
        return text
    blocks = [block for block in text.split("\n\n") if block.strip()]
    kept: list[str] = []
    for block in blocks:
        paragraph = block.strip()
        if not paragraph:
            continue
        if _looks_like_channel_switch(paragraph):
            continue
        kept.append(block)
    return "\n\n".join(kept) or ""


_CTA_KEYWORDS = (
    "готов",
    "заброниру",
    "оформ",
    "подтверж",
    "закреп",
    "оставьте контакт",
    "перейд",
    "давайте",
    "зафиксир",
)


def _strip_forbidden_cta(text: str, allow_cta: bool) -> str:
    if allow_cta or not text:
        return text
    blocks = [block for block in text.split("\n\n") if block.strip()]
    while blocks:
        last = blocks[-1].strip()
        if not last:
            blocks.pop()
            continue
        lowered = last.lower()
        if any(keyword in lowered for keyword in _CTA_KEYWORDS):
            blocks.pop()
            continue
        break
    return "\n\n".join(blocks).strip()


__all__ = [
    "EnforcementContext",
    "enforce_plan_alignment",
    "question_fingerprint",
]
