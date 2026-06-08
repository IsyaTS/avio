from __future__ import annotations

import pytest

from libs.core.services.contextual_case_applicability import evaluate_case_applicability


pytestmark = pytest.mark.unit


def _case(mode: str = "context_bound", requires: list[str] | None = None) -> dict:
    return {
        "mode": mode,
        "dialog": {
            "history": [{"role": "client", "text": "Сколько стоит покос?"}],
            "manager_reply": {"role": "manager", "text": "Цена зависит от площади и высоты травы."},
        },
        "reply_facts": {"mentions_price": True, "price_specific": True},
        "applicability": {"mode": mode, "requires": ["slots.area_size", "slots.grass_height"] if requires is None else requires},
    }


def test_context_bound_applicable_when_required_slots_present() -> None:
    decision = evaluate_case_applicability(
        _case(),
        {"slots": {"area_size": "10 соток", "grass_height": "по пояс"}, "known_slots": ["area_size", "grass_height"]},
    )
    assert decision.status == "applicable"
    assert decision.safe_prompt_case
    assert "Цена зависит" in decision.safe_prompt_case["manager_reply"]


def test_context_bound_missing_requires_does_not_include_manager_reply() -> None:
    decision = evaluate_case_applicability(_case(), {"slots": {"area_size": "10 соток"}, "known_slots": ["area_size"]})
    assert decision.status == "clarification_needed"
    assert decision.safe_prompt_case is None
    assert decision.missing_requires == ["slots.grass_height"]


def test_price_or_address_with_empty_requires_is_blocked() -> None:
    decision = evaluate_case_applicability(_case(requires=[]), {"slots": {}, "known_slots": []})
    assert decision.status == "blocked"
    assert decision.safe_prompt_case is None


def test_clarify_first_can_be_used_as_hint() -> None:
    decision = evaluate_case_applicability(
        _case(mode="clarify_first", requires=[]),
        {"slots": {}, "known_slots": []},
    )
    assert decision.status == "clarification_needed"
    assert decision.safe_prompt_case


def test_review_and_reject_are_ignored() -> None:
    assert evaluate_case_applicability(_case(mode="review"), {"slots": {}}).status == "blocked"
    assert evaluate_case_applicability(_case(mode="reject"), {"slots": {}}).status == "blocked"
