from __future__ import annotations

import asyncio
from typing import Any

import pytest

from libs.core.services.avito_training_candidate_builder import AvitoTrainingCandidate
from libs.core.services import avito_training_ai_reviewer as reviewer


pytestmark = pytest.mark.unit


def candidate(candidate_id: str = "c1") -> AvitoTrainingCandidate:
    return AvitoTrainingCandidate(
        source="avito",
        tenant_id=1,
        dialog_id="d",
        candidate_id=candidate_id,
        example_id="d_0001",
        channel="avito",
        context=[{"role": "client", "text": "Нужна дверь"}],
        ideal_reply={"role": "manager", "text": "Подскажите размер?"},
        created_at="2026-05-22T00:00:00Z",
    )


class FakeReviewer:
    def __init__(self, payload: dict[str, Any] | Exception) -> None:
        self.payload = payload
        self.calls = 0

    async def review_candidate(self, _candidate: AvitoTrainingCandidate) -> dict[str, Any]:
        self.calls += 1
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


@pytest.mark.asyncio
async def test_accept_reject_and_review_decisions() -> None:
    for decision in ("accept_training", "reject_training", "needs_manual_review"):
        client = FakeReviewer({"decision": decision, "score": 87, "reason_code": "case", "tags": ["tag"]})
        result = await reviewer.review_candidates([candidate(decision)], reviewer_client=client)
        assert result.decisions[decision].decision == decision


@pytest.mark.asyncio
async def test_invalid_ai_json_routes_to_review_and_failed_count() -> None:
    client = FakeReviewer({"decision": "bad"})
    result = await reviewer.review_candidates([candidate()], reviewer_client=client, retries=0)

    decision = result.decisions["c1"]
    assert decision.decision == reviewer.NEEDS_MANUAL_REVIEW
    assert decision.reason_code == "ai_error"
    assert result.failed_count == 1


@pytest.mark.asyncio
async def test_timeout_routes_to_review() -> None:
    class SlowReviewer:
        async def review_candidate(self, _candidate: AvitoTrainingCandidate) -> dict[str, Any]:
            await asyncio.sleep(0.05)
            return {"decision": "accept_training"}

    result = await reviewer.review_candidates(
        [candidate()],
        reviewer_client=SlowReviewer(),
        timeout_seconds=0.01,
        retries=0,
    )

    assert result.decisions["c1"].decision == reviewer.NEEDS_MANUAL_REVIEW
    assert result.failed_count == 1


@pytest.mark.asyncio
async def test_cache_prevents_duplicate_calls() -> None:
    client = FakeReviewer({"decision": "accept_training", "score": 90, "reason_code": "ok"})
    cache = reviewer.InMemoryReviewCache()
    items = [candidate("same"), candidate("same")]
    result = await reviewer.review_candidates(items, reviewer_client=client, cache=cache)

    assert result.reviewed_count == 1
    assert client.calls == 1


@pytest.mark.asyncio
async def test_disabled_returns_no_decisions() -> None:
    result = await reviewer.review_candidates([candidate()], reviewer_client=None, enabled=False)
    assert result.disabled is True
    assert result.decisions == {}
