from __future__ import annotations

import pytest

from libs.core.services import notifications


pytestmark = pytest.mark.unit


def test_coerce_chat_ids_accepts_sequences_and_skips_bad_values() -> None:
    assert notifications.coerce_chat_ids(["101", "bad", 0, 202]) == [101, 202]


def test_coerce_chat_ids_accepts_single_value() -> None:
    assert notifications.coerce_chat_ids("303") == [303]
    assert notifications.coerce_chat_ids("bad") == []


def test_build_notification_context_prefers_explicit_chat_ids() -> None:
    context = notifications.build_notification_context(
        {
            "event": " manager ",
            "tenant_id": "bad",
            "tenant": "12",
            "lead_id": "34",
            "chat_ids": ["101"],
            "text": " hello ",
        },
        default_tenant_id=9,
        configured_chat_ids=[202],
    )

    assert context.event_name == "manager"
    assert context.tenant_id == 9
    assert context.lead_id == 34
    assert context.chat_ids == [101]
    assert context.text == "hello"
    assert context.has_text
    assert context.has_targets


def test_build_notification_context_uses_configured_chat_ids_when_missing() -> None:
    context = notifications.build_notification_context(
        {"tenant": "12", "lead_id": "34", "text": "hello"},
        default_tenant_id=9,
        configured_chat_ids=[202, 0, 303],
    )

    assert context.event_name == "notify"
    assert context.tenant_id == 12
    assert context.lead_id == 34
    assert context.chat_ids == [202, 303]


def test_build_notification_context_reports_empty_text_and_targets() -> None:
    context = notifications.build_notification_context({"tenant": "12"}, default_tenant_id=9)

    assert context.tenant_id == 12
    assert context.lead_id == 0
    assert context.chat_ids == []
    assert context.text == ""
    assert not context.has_text
    assert not context.has_targets
