from __future__ import annotations

import pytest

from libs.core.services import contextual_case_retriever


pytestmark = pytest.mark.unit


class FakeRepo:
    def __init__(self) -> None:
        self.used: list[int] = []

    async def get_active_domain_schema(self, tenant_id: int) -> dict:
        return {
            "domain": "lawn_mowing",
            "domain_label": "покос травы",
            "required_slots": ["area_size", "grass_height", "location"],
            "slot_definitions": {"area_size": "площадь", "grass_height": "высота травы", "location": "локация"},
        }

    async def list_active_cases_for_retrieval(self, tenant_id: int, *, limit: int = 500, require_embedding: bool = False) -> list[dict]:
        return [
            {
                "id": 1,
                "tenant_id": tenant_id,
                "is_active": True,
                "domain": "lawn_mowing",
                "intent": "price_question",
                "mode": "context_bound",
                "search_text": "покос травы цена площадь высота",
                "context": {"slots": {"area_size": "10 соток", "grass_height": "по пояс"}},
                "dialog": {
                    "history": [{"role": "client", "text": "Сколько стоит покос 10 соток?"}],
                    "manager_reply": {"role": "manager", "text": "Цена зависит от площади и высоты травы."},
                },
                "reply_facts": {"mentions_price": True},
                "applicability": {"mode": "context_bound", "requires": ["slots.area_size"]},
            },
            {
                "id": 2,
                "tenant_id": tenant_id,
                "is_active": False,
                "domain": "cleaning",
                "intent": "other",
                "mode": "direct_example",
                "search_text": "уборка",
                "context": {},
                "dialog": {},
                "applicability": {"mode": "direct_example", "requires": []},
            },
        ]

    async def increment_contextual_case_usage(self, ids):
        self.used.extend(ids)


@pytest.mark.asyncio
async def test_retrieves_same_domain_intent_case_with_fallback_scoring() -> None:
    repo = FakeRepo()
    result = await contextual_case_retriever.retrieve_contextual_cases(
        tenant_id=7,
        user_text="Сколько стоит покос травы 10 соток?",
        history=[],
        repo_module=repo,
        min_score=0.1,
    )
    assert result.applicable_cases
    assert "Цена зависит" in result.applicable_cases[0]["manager_reply"]
    assert repo.used == [1]


@pytest.mark.asyncio
async def test_missing_requirements_route_to_clarification() -> None:
    repo = FakeRepo()
    result = await contextual_case_retriever.retrieve_contextual_cases(
        tenant_id=7,
        user_text="Сколько стоит покос травы?",
        history=[],
        repo_module=repo,
        min_score=0.1,
    )
    assert not result.applicable_cases
    assert result.clarification_cases
