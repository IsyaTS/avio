from __future__ import annotations

import pytest

from libs.core.services import avito_behavior


pytestmark = pytest.mark.unit


def test_extract_avito_auto_reply_text_respects_disabled_flag() -> None:
    assert (
        avito_behavior.extract_avito_auto_reply_text(
            {"behavior": {"auto_reply": False, "auto_reply_text": "hello"}}
        )
        == ""
    )


def test_extract_avito_auto_reply_text_returns_trimmed_text() -> None:
    assert (
        avito_behavior.extract_avito_auto_reply_text(
            {"behavior": {"auto_reply": True, "auto_reply_text": " hello "}}
        )
        == "hello"
    )


def test_extract_avito_phone_tg_template_prefers_behavior_over_persona_meta() -> None:
    assert (
        avito_behavior.extract_avito_phone_tg_template(
            {"behavior": {"avito_phone_tg_template": " behavior "}},
            {"avito_phone_tg_template": "persona"},
        )
        == "behavior"
    )


def test_extract_avito_phone_tg_template_falls_back_to_legacy_persona_keys() -> None:
    assert (
        avito_behavior.extract_avito_phone_tg_template(
            {},
            {"persona.meta.avito_phone_tg_template": " legacy "},
        )
        == "legacy"
    )


def test_avito_smart_reply_enabled_uses_explicit_flags_only() -> None:
    assert avito_behavior.avito_smart_reply_enabled({"behavior": {}}) is False
    assert (
        avito_behavior.avito_smart_reply_enabled(
            {"behavior": {"avito_smart_reply_enabled": True}}
        )
        is True
    )
    assert (
        avito_behavior.avito_smart_reply_enabled({"behavior": {"avito_ai_enabled": True}})
        is True
    )
