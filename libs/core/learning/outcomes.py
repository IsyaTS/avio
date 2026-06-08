from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Sequence

from .stitching import StitchedTurn


_REPEAT_RE = re.compile(r"[^a-z0-9а-яё]+", re.IGNORECASE)


@dataclass(frozen=True)
class EpisodeOutcome:
    signals: dict[str, Any]
    reward: float
    finalized: bool


def _normalize(value: str) -> str:
    text = _REPEAT_RE.sub(" ", str(value or "").lower()).strip()
    return re.sub(r"\s+", " ", text)


def _similarity(a: str, b: str) -> float:
    left = set(_normalize(a).split())
    right = set(_normalize(b).split())
    if not left or not right:
        return 0.0
    overlap = len(left & right)
    union = len(left | right)
    return float(overlap / union) if union else 0.0


def repeated_question_hash(text: str) -> str:
    normalized = _normalize(text)
    if not normalized:
        return ""
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def compute_episode_outcome(
    *,
    trigger_user_text: str,
    subsequent_turns: Sequence[StitchedTurn],
    horizon_reached: bool,
) -> EpisodeOutcome:
    next_user = next((turn for turn in subsequent_turns if turn.role == "user"), None)
    manager_turns = [turn for turn in subsequent_turns if turn.role == "manager"]
    assistant_turns = [turn for turn in subsequent_turns if turn.role == "assistant"]

    user_continued = next_user is not None
    repeated_question_stopped = True
    if user_continued and next_user is not None:
        repeated_question_stopped = _similarity(trigger_user_text, next_user.text) < 0.82

    no_immediate_second_takeover = len(manager_turns) <= 1
    no_fallback_spiral = not any("fallback" in (" ".join(turn.sources)).lower() for turn in assistant_turns)
    dialogue_progressed_usefully = bool(user_continued or assistant_turns)

    reward = 0.0
    reward += 0.45 if user_continued else -0.15
    reward += 0.25 if repeated_question_stopped else -0.35
    reward += 0.15 if no_immediate_second_takeover else -0.25
    reward += 0.1 if no_fallback_spiral else -0.25
    reward += 0.2 if dialogue_progressed_usefully else -0.1

    finalized = horizon_reached or user_continued or len(subsequent_turns) >= 2
    return EpisodeOutcome(
        signals={
            "user_continued_dialogue": user_continued,
            "repeated_question_stopped": repeated_question_stopped,
            "no_immediate_second_takeover": no_immediate_second_takeover,
            "no_fallback_spiral": no_fallback_spiral,
            "dialogue_progressed_usefully": dialogue_progressed_usefully,
            "next_user_hash": repeated_question_hash(next_user.text if next_user else ""),
        },
        reward=round(reward, 4),
        finalized=bool(finalized),
    )
