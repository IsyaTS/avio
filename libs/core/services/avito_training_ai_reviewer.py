from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from libs.core.services.avito_training_candidate_builder import AvitoTrainingCandidate


ACCEPT_TRAINING = "accept_training"
REJECT_TRAINING = "reject_training"
NEEDS_MANUAL_REVIEW = "needs_manual_review"
VALID_DECISIONS = {ACCEPT_TRAINING, REJECT_TRAINING, NEEDS_MANUAL_REVIEW}


@dataclass(frozen=True)
class AvitoTrainingReviewDecision:
    candidate_id: str
    decision: str
    score: int
    reason_code: str
    reason: str = ""
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AvitoTrainingAIReviewResult:
    decisions: dict[str, AvitoTrainingReviewDecision]
    reviewed_count: int
    rejected_count: int
    manual_review_count: int
    failed_count: int
    disabled: bool
    mode: str


class InMemoryReviewCache:
    def __init__(self) -> None:
        self._values: dict[str, AvitoTrainingReviewDecision] = {}

    def get(self, key: str) -> AvitoTrainingReviewDecision | None:
        return self._values.get(key)

    def set(self, key: str, value: AvitoTrainingReviewDecision) -> None:
        self._values[key] = value


async def review_candidates(
    candidates: Sequence[AvitoTrainingCandidate],
    *,
    reviewer_client: Any | None = None,
    enabled: bool = True,
    concurrency: int = 4,
    timeout_seconds: float = 20.0,
    retries: int = 1,
    cache: InMemoryReviewCache | None = None,
) -> AvitoTrainingAIReviewResult:
    if not enabled or reviewer_client is None:
        return AvitoTrainingAIReviewResult(
            decisions={},
            reviewed_count=0,
            rejected_count=0,
            manual_review_count=0,
            failed_count=0,
            disabled=True,
            mode="disabled",
        )

    cache = cache or InMemoryReviewCache()
    semaphore = asyncio.Semaphore(max(1, min(int(concurrency or 4), 20)))
    decisions: dict[str, AvitoTrainingReviewDecision] = {}
    failed = 0

    async def run_one(candidate: AvitoTrainingCandidate) -> None:
        nonlocal failed
        cached = cache.get(candidate.candidate_id)
        if cached is not None:
            decisions[candidate.candidate_id] = cached
            return
        async with semaphore:
            decision = await _review_one(
                candidate,
                reviewer_client=reviewer_client,
                timeout_seconds=timeout_seconds,
                retries=retries,
            )
        if decision.reason_code == "ai_error":
            failed += 1
        cache.set(candidate.candidate_id, decision)
        decisions[candidate.candidate_id] = decision

    unique_candidates: dict[str, AvitoTrainingCandidate] = {}
    for candidate in candidates:
        unique_candidates.setdefault(candidate.candidate_id, candidate)
    await asyncio.gather(*(run_one(candidate) for candidate in unique_candidates.values()))
    rejected = sum(1 for decision in decisions.values() if decision.decision == REJECT_TRAINING)
    manual = sum(1 for decision in decisions.values() if decision.decision == NEEDS_MANUAL_REVIEW)
    return AvitoTrainingAIReviewResult(
        decisions=decisions,
        reviewed_count=len(decisions),
        rejected_count=rejected,
        manual_review_count=manual,
        failed_count=failed,
        disabled=False,
        mode="ai",
    )


async def _review_one(
    candidate: AvitoTrainingCandidate,
    *,
    reviewer_client: Any,
    timeout_seconds: float,
    retries: int,
) -> AvitoTrainingReviewDecision:
    attempts = max(1, int(retries or 0) + 1)
    last_error: Exception | None = None
    for _attempt in range(attempts):
        try:
            raw = await asyncio.wait_for(
                _call_reviewer(reviewer_client, candidate),
                timeout=max(0.001, float(timeout_seconds or 20.0)),
            )
            return _parse_decision(candidate.candidate_id, raw)
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0)
    return AvitoTrainingReviewDecision(
        candidate_id=candidate.candidate_id,
        decision=NEEDS_MANUAL_REVIEW,
        score=0,
        reason_code="ai_error",
        reason=type(last_error).__name__ if last_error else "ai_error",
        tags=["ai_error"],
    )


async def _call_reviewer(reviewer_client: Any, candidate: AvitoTrainingCandidate) -> Mapping[str, Any]:
    call = getattr(reviewer_client, "review_candidate", None)
    if not callable(call):
        raise TypeError("reviewer_client_missing_review_candidate")
    result = call(candidate)
    if hasattr(result, "__await__"):
        result = await result
    if not isinstance(result, Mapping):
        raise ValueError("invalid_ai_review_result")
    return result


def _parse_decision(candidate_id: str, raw: Mapping[str, Any]) -> AvitoTrainingReviewDecision:
    decision = str(raw.get("decision") or "").strip()
    if decision not in VALID_DECISIONS:
        raise ValueError("invalid_ai_decision")
    try:
        score = int(raw.get("score") or 0)
    except Exception:
        score = 0
    reason_code = str(raw.get("reason_code") or decision).strip()[:80] or decision
    reason = str(raw.get("reason") or "").strip()[:500]
    tags_raw = raw.get("tags")
    tags = [str(item).strip()[:80] for item in tags_raw if str(item).strip()] if isinstance(tags_raw, list) else []
    return AvitoTrainingReviewDecision(
        candidate_id=candidate_id,
        decision=decision,
        score=max(0, min(score, 100)),
        reason_code=reason_code,
        reason=reason,
        tags=tags,
    )


__all__ = [
    "ACCEPT_TRAINING",
    "NEEDS_MANUAL_REVIEW",
    "REJECT_TRAINING",
    "AvitoTrainingAIReviewResult",
    "AvitoTrainingReviewDecision",
    "InMemoryReviewCache",
    "review_candidates",
]
