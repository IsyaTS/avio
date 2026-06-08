from __future__ import annotations

import pytest

from libs.core.services import incoming_events


pytestmark = pytest.mark.unit


def test_normalize_incoming_channel_prefers_channel() -> None:
    assert (
        incoming_events.normalize_incoming_channel(
            {"channel": " Telegram ", "ch": "whatsapp", "provider": "avito"}
        )
        == "telegram"
    )


def test_normalize_incoming_channel_falls_back_to_ch_and_provider() -> None:
    assert incoming_events.normalize_incoming_channel({"ch": " Avito "}) == "avito"
    assert incoming_events.normalize_incoming_channel({"provider": "Max_Personal"}) == "max_personal"


def test_normalize_incoming_channel_handles_non_string_values() -> None:
    assert incoming_events.normalize_incoming_channel({"channel": 123}) == "123"
    assert incoming_events.normalize_incoming_channel({}) == ""


def test_build_incoming_event_route_reports_handler_key() -> None:
    route = incoming_events.build_incoming_event_route({"provider": "whatsapp"})
    empty = incoming_events.build_incoming_event_route({})

    assert route.channel == "whatsapp"
    assert route.has_handler_key
    assert empty.channel == ""
    assert not empty.has_handler_key


def test_build_incoming_event_log_hints_normalizes_public_fields() -> None:
    hints = incoming_events.build_incoming_event_log_hints(
        {"provider": " Avito ", "tenant_id": 3, "message_id": "msg-1"}
    )

    assert hints.channel == "avito"
    assert hints.tenant == 3
    assert hints.message_id == "msg-1"


def test_build_incoming_event_log_hints_uses_safe_defaults() -> None:
    hints = incoming_events.build_incoming_event_log_hints({})

    assert hints.channel == "-"
    assert hints.tenant == ""
    assert hints.message_id == "-"


def test_normalize_event_text_strips_missing_and_non_string_values() -> None:
    assert incoming_events.normalize_event_text({}) == ""
    assert incoming_events.normalize_event_text({"text": " hello "}) == "hello"
    assert incoming_events.normalize_event_text({"text": 123}) == "123"


def test_looks_like_manager_outgoing_detects_origin_and_flags() -> None:
    assert incoming_events.looks_like_manager_outgoing({"origin": "telegram:manager:1"})
    assert incoming_events.looks_like_manager_outgoing({"origin": "max_personal:manager:1"})
    assert incoming_events.looks_like_manager_outgoing({"manager": True})
    assert incoming_events.looks_like_manager_outgoing({"provider_raw": {"outgoing": True}})
    assert incoming_events.looks_like_manager_outgoing({"message": {"key": {"fromMe": True}}})
    assert incoming_events.looks_like_manager_outgoing({"message": {"meta": {"out": True}}})
    assert not incoming_events.looks_like_manager_outgoing({"origin": "telegram:user"})


def test_collect_event_attachment_items_accepts_supported_fields_only() -> None:
    first = {"type": "image", "url": "1"}
    second = {"type": "file", "url": "2"}
    third = {"type": "image", "url": "3"}
    fourth = {"type": "image", "url": "4"}

    assert incoming_events.collect_event_attachment_items(
        {
            "attachments": [first, "bad"],
            "attachment": second,
            "media": [third, 123],
            "photo": fourth,
        }
    ) == [first, second, third, fourth]


def test_has_image_attachment_detects_normalized_image_type() -> None:
    assert incoming_events.has_image_attachment([{"type": " Image "}])
    assert not incoming_events.has_image_attachment([{"type": "document"}])


def test_fallback_lead_id_prefers_positive_hint_then_numeric_identity() -> None:
    assert (
        incoming_events.fallback_lead_id(
            lead_hint=101,
            numeric_identity="202",
            fallback_value=303,
        )
        == 101
    )
    assert (
        incoming_events.fallback_lead_id(
            lead_hint=None,
            numeric_identity="202",
            fallback_value=303,
        )
        == 202
    )
    assert (
        incoming_events.fallback_lead_id(
            lead_hint=0,
            numeric_identity="bad",
            fallback_value=303,
        )
        == 303
    )
