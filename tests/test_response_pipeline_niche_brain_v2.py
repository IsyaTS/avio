from __future__ import annotations

import pytest

from libs.core import response_pipeline
from libs.core.services import niche_brain_v2


pytestmark = pytest.mark.unit
_DEFAULT_ALLOWED_CHANNELS = object()


async def _build_llm_messages(*_args, **_kwargs):
    return [{"role": "system", "content": "base-system"}]


def _base_cfg(
    *,
    enabled: bool = True,
    apply_mode: bool = True,
    tenant_allowlist=None,
    allowed_channels=_DEFAULT_ALLOWED_CHANNELS,
):
    if tenant_allowlist is None:
        tenant_allowlist = [101]
    niche_cfg = {
        "enabled": enabled,
        "apply_mode": apply_mode,
        "tenant_allowlist": tenant_allowlist,
    }
    if allowed_channels is not _DEFAULT_ALLOWED_CHANNELS:
        niche_cfg["allowed_channels"] = allowed_channels
    return {
        "behavior": {
            "niche_brain_v2": niche_cfg
        }
    }


def _patch_pipeline(monkeypatch: pytest.MonkeyPatch, cfg: dict):
    captured: dict[str, object] = {}

    async def _ask_llm(messages, **_kwargs):
        captured["messages"] = messages
        return "ok"

    async def _contextual(**_kwargs):
        return {"enabled": False, "applied": False, "block": ""}

    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "build_contextual_cases_block_for_runtime", _contextual)
    monkeypatch.setattr(response_pipeline, "_build_dialog_training_block", lambda **_kwargs: "")
    monkeypatch.setattr(response_pipeline.training_retriever, "build_examples_block_async", lambda *_a, **_k: "")
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", lambda **_kwargs: {"policy_block": ""})
    monkeypatch.setattr(response_pipeline, "read_tenant_config", lambda _tenant: cfg)
    return captured


def _system_text(captured: dict[str, object]) -> str:
    messages = captured["messages"]
    assert isinstance(messages, list)
    return str(messages[0]["content"])


@pytest.mark.asyncio
async def test_disabled_does_not_change_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg(enabled=False))

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="avito",
        user_text="каталог есть?",
    )

    assert result.reply_text == "ok"
    assert _system_text(captured) == "base-system"


@pytest.mark.asyncio
async def test_enabled_without_apply_mode_does_not_add_block(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg(apply_mode=False))

    await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="avito",
        user_text="каталог есть?",
    )

    assert "NICHE BRAIN V2" not in _system_text(captured)


@pytest.mark.asyncio
async def test_apply_enabled_for_allowed_tenant_and_avito_adds_block(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg())

    await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="avito",
        user_text="каталог есть?",
    )

    system_text = _system_text(captured)
    assert "NICHE BRAIN V2 - CURRENT TACTIC" in system_text
    assert "detected_client_intent: catalog_request" in system_text


@pytest.mark.asyncio
async def test_tenant_not_allowed_does_not_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg(tenant_allowlist=[202]))

    await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="avito",
        user_text="каталог есть?",
    )

    assert "NICHE BRAIN V2" not in _system_text(captured)


@pytest.mark.asyncio
async def test_non_avito_channel_does_not_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg())

    await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="telegram",
        user_text="каталог есть?",
    )

    assert "NICHE BRAIN V2" not in _system_text(captured)


@pytest.mark.asyncio
async def test_empty_allowed_channels_disables_v2_for_allowed_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg(allowed_channels=[]))

    await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="avito",
        user_text="каталог есть?",
    )

    assert "NICHE BRAIN V2" not in _system_text(captured)


@pytest.mark.asyncio
async def test_missing_allowed_channels_defaults_to_avito(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg())

    await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="avito",
        user_text="каталог есть?",
    )

    assert "NICHE BRAIN V2 - CURRENT TACTIC" in _system_text(captured)


@pytest.mark.asyncio
async def test_kill_switch_disables_v2(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg())
    monkeypatch.setenv("NICHE_BRAIN_V2_DISABLED", "1")

    await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="avito",
        user_text="каталог есть?",
    )

    assert "NICHE BRAIN V2" not in _system_text(captured)


@pytest.mark.asyncio
async def test_niche_brain_exception_does_not_break_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _patch_pipeline(monkeypatch, _base_cfg())

    def _boom(_context):
        raise RuntimeError("boom")

    monkeypatch.setattr(response_pipeline, "build_niche_brain_v2_block", _boom)

    result = await response_pipeline.run_response_pipeline(
        tenant_id=101,
        channel="avito",
        user_text="каталог есть?",
    )

    assert result.reply_text == "ok"
    assert _system_text(captured) == "base-system"


def test_block_contains_required_content_fields() -> None:
    result = niche_brain_v2.build_niche_brain_v2_block(
        niche_brain_v2.NicheBrainV2Context(
            tenant_id=101,
            channel="avito",
            user_text="каталог есть?",
            tenant_config=_base_cfg(),
        )
    )

    assert result.applied is True
    assert "detected_client_intent" in result.block
    assert "implied_need" in result.block
    assert "next_best_action" in result.block
    assert "what_not_to_ask" in result.block
    assert "forbidden_phrases" in result.block
    assert "tone_instruction" in result.block


@pytest.mark.parametrize(
    ("user_text", "expected_intent"),
    [
        ("доставка есть?", "install_delivery"),
        ("есть доставка?", "install_delivery"),
        ("доставка есть по городу?", "install_delivery"),
        ("есть в наличии?", "availability"),
    ],
)
def test_delivery_intent_has_priority_over_generic_availability(
    user_text: str,
    expected_intent: str,
) -> None:
    result = niche_brain_v2.build_niche_brain_v2_block(
        niche_brain_v2.NicheBrainV2Context(
            tenant_id=101,
            channel="avito",
            user_text=user_text,
            tenant_config=_base_cfg(allowed_channels=["avito"]),
        )
    )

    assert result.detected_client_intent == expected_intent
