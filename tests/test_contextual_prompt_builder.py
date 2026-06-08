from __future__ import annotations

import pytest

from libs.core.services.contextual_case_retriever import ContextualRetrievalResult
from libs.core.services.contextual_prompt_builder import build_contextual_cases_block


pytestmark = pytest.mark.unit


def test_builds_prompt_with_priority_warning_and_applicable_case() -> None:
    block = build_contextual_cases_block(
        ContextualRetrievalResult(
            current_context={"intent": "price_question", "known_slots": ["area_size"], "missing_slots": []},
            applicable_cases=[
                {
                    "mode": "context_bound",
                    "requires": ["slots.area_size"],
                    "history": [{"role": "client", "text": "Сколько стоит?"}],
                    "manager_reply": "Цена зависит от площади.",
                }
            ],
        )
    )
    assert "Каталог, текущая персона, бизнес-правила клиента" in block
    assert "Цена зависит от площади" in block


def test_does_not_include_blocked_manager_reply() -> None:
    block = build_contextual_cases_block(
        ContextualRetrievalResult(
            current_context={"intent": "price_question", "known_slots": [], "missing_slots": []},
            applicable_cases=[],
            clarification_cases=[{"missing_requires": ["slots.area_size"], "clarification_hint": "Уточни: area_size"}],
        )
    )
    assert "Цена зависит" not in block
    assert "Сначала уточни" in block


def test_respects_max_chars() -> None:
    block = build_contextual_cases_block(
        ContextualRetrievalResult(
            current_context={"intent": "other"},
            applicable_cases=[
                {
                    "mode": "direct_example",
                    "requires": [],
                    "history": [{"role": "client", "text": "x" * 2000}],
                    "manager_reply": "y" * 2000,
                }
            ],
        ),
        max_chars=700,
    )
    assert len(block) <= 700
