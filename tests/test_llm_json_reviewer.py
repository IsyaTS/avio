from __future__ import annotations

import pytest

from libs.core.services import llm_json_reviewer


pytestmark = pytest.mark.unit


def test_system_prompt_contains_quality_contract() -> None:
    prompt = llm_json_reviewer._system_prompt()

    assert "Return only a valid JSON object" in prompt
    assert "not to repeat regex filtering" in prompt
    assert "if copying it would improve sales dialog quality" in prompt
    assert "learning from this example would likely hurt future assistant quality" in prompt
    assert "comfortable letting a bot imitate directly" in prompt
    assert "mostly collecting a phone number" in prompt
    assert "long repeated contact/catalog transfer template" in prompt
    assert "accept_training" in prompt
    assert "reject_training" in prompt
    assert "needs_manual_review" in prompt
    assert "Short replies are allowed" not in prompt
    assert "is short but appropriate in context" in prompt
    assert "Do not reject only because" in prompt
    assert "service-only status" in prompt
    assert "contains a contact mask" in prompt
    assert "Soft flags and rule_score are hints only" in prompt


def test_gpt5_models_use_responses_api() -> None:
    assert llm_json_reviewer._use_responses_api("gpt-5.2-pro") is True
    assert llm_json_reviewer._use_responses_api("gpt-4.1") is False
