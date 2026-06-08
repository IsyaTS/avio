from __future__ import annotations

import pytest

from libs.core.sales_core.llm_runtime import LLMRuntime


pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_gpt5_chat_uses_max_completion_tokens() -> None:
    captured: dict[str, object] = {}

    def _create(**kwargs):
        captured.update(kwargs)
        return object()

    runtime = LLMRuntime(min_call_gap_seconds=0)

    await runtime.call_with_deadline(
        _create,
        timeout_seconds=2,
        is_quota_or_rate_limit_error=lambda _exc: False,
        model="gpt-5.2-chat-latest",
        messages=[{"role": "user", "content": "test"}],
        max_tokens=100,
        temperature=0.2,
        top_p=0.9,
        frequency_penalty=0.2,
        presence_penalty=0.05,
    )

    assert captured["model"] == "gpt-5.2-chat-latest"
    assert captured["max_completion_tokens"] == 100
    assert "max_tokens" not in captured
    assert "temperature" not in captured
    assert "top_p" not in captured
    assert "frequency_penalty" not in captured
    assert "presence_penalty" not in captured


@pytest.mark.asyncio
async def test_non_gpt5_chat_keeps_max_tokens() -> None:
    captured: dict[str, object] = {}

    def _create(**kwargs):
        captured.update(kwargs)
        return object()

    runtime = LLMRuntime(min_call_gap_seconds=0)

    await runtime.call_with_deadline(
        _create,
        timeout_seconds=2,
        is_quota_or_rate_limit_error=lambda _exc: False,
        model="gpt-4.1",
        messages=[{"role": "user", "content": "test"}],
        max_tokens=100,
        temperature=0.2,
    )

    assert captured["model"] == "gpt-4.1"
    assert captured["max_tokens"] == 100
    assert captured["temperature"] == 0.2
    assert "max_completion_tokens" not in captured
