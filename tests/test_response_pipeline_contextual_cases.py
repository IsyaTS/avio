from __future__ import annotations

import pytest

from libs.core import response_pipeline


pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_disabled_contextual_block_not_injected(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _contextual(**_kwargs):
        return {"enabled": False, "applied": False, "block": ""}

    async def _build(*_args, **_kwargs):
        return [{"role": "system", "content": "base"}]

    async def _ask(messages, **_kwargs):
        captured["messages"] = messages
        return "ok"

    monkeypatch.setattr(response_pipeline, "build_contextual_cases_block_for_runtime", _contextual)
    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", lambda *_a, **_k: "")
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", lambda **_k: {"policy_block": ""})
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask)
    result = await response_pipeline.run_response_pipeline(tenant_id=7, channel="avito", user_text="цена?")
    assert result.reply_text == "ok"
    assert "Контекстные примеры" not in captured["messages"][0]["content"]


@pytest.mark.asyncio
async def test_apply_contextual_block_injected(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _contextual(**_kwargs):
        return {"enabled": True, "applied": True, "block": "Контекстные примеры менеджера.\nМенеджер: безопасно"}

    async def _build(*_args, **_kwargs):
        return [{"role": "system", "content": "base"}]

    async def _ask(messages, **_kwargs):
        captured["messages"] = messages
        return "ok"

    monkeypatch.setattr(response_pipeline, "build_contextual_cases_block_for_runtime", _contextual)
    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build)
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", lambda *_a, **_k: "")
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", lambda **_k: {"policy_block": ""})
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask)
    await response_pipeline.run_response_pipeline(tenant_id=7, channel="avito", user_text="цена?")
    assert "Контекстные примеры менеджера" in captured["messages"][0]["content"]


@pytest.mark.asyncio
async def test_retrieval_failure_does_not_break_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _contextual(**_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(response_pipeline, "build_contextual_cases_block_for_runtime", _contextual)
    monkeypatch.setattr(response_pipeline, "build_llm_messages", lambda *_a, **_k: [{"role": "system", "content": "base"}])
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", lambda *_a, **_k: "")
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", lambda **_k: {"policy_block": ""})

    async def _ask(_messages, **_kwargs):
        return "ok"

    monkeypatch.setattr(response_pipeline, "ask_llm", _ask)
    result = await response_pipeline.run_response_pipeline(tenant_id=7, channel="avito", user_text="цена?")
    assert result.reply_text == "ok"


def test_dialog_dataset_requires_explicit_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def _build_block(*_args, **_kwargs):
        nonlocal called
        called = True
        return "Похожие реальные диалоги менеджера"

    monkeypatch.setattr(response_pipeline, "read_tenant_config", lambda _tenant: {"learning": {"dialog_dataset": {}}})
    monkeypatch.setattr(response_pipeline.dialog_retriever, "build_dialog_examples_block", _build_block)
    monkeypatch.setattr(response_pipeline.dialog_retriever, "ensure_dialog_index", lambda *_args, **_kwargs: object())

    assert response_pipeline._build_dialog_training_block(tenant_id=7, user_text="цена?") == ""
    assert response_pipeline._dialog_dataset_available(7) is False
    assert called is False


def test_dialog_dataset_enabled_uses_index(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        response_pipeline,
        "read_tenant_config",
        lambda _tenant: {"learning": {"dialog_dataset": {"enabled": True, "top_k": 1}}},
    )
    monkeypatch.setattr(
        response_pipeline.dialog_retriever,
        "build_dialog_examples_block",
        lambda *_args, **_kwargs: "Похожие реальные диалоги менеджера",
    )
    monkeypatch.setattr(response_pipeline.dialog_retriever, "ensure_dialog_index", lambda *_args, **_kwargs: object())

    assert "Похожие реальные диалоги менеджера" in response_pipeline._build_dialog_training_block(
        tenant_id=7,
        user_text="цена?",
    )
    assert response_pipeline._dialog_dataset_available(7) is True
