from __future__ import annotations

from libs.core.message_envelope import (
    MESSAGE_KIND_IMAGE,
    MESSAGE_KIND_MIXED,
    MESSAGE_KIND_TEXT,
    MESSAGE_KIND_VOICE,
    build_envelope,
    content_fingerprint,
    detect_message_kind,
    normalize_attachments,
    placeholder_for_kind,
    sanitize_display_name,
    text_or_placeholder,
)


def test_normalize_attachments_deduplicates_and_maps_voice() -> None:
    attachments = normalize_attachments(
        [
            {"type": "audio", "url": "https://example.com/voice.ogg", "mime": "audio/ogg"},
            {"type": "audio", "url": "https://example.com/voice.ogg", "mime": "audio/ogg"},
        ]
    )
    assert len(attachments) == 1
    assert attachments[0]["type"] == MESSAGE_KIND_VOICE
    assert attachments[0]["name"] == "voice.ogg"


def test_detect_message_kind_handles_text_image_and_mixed() -> None:
    image = [{"type": "image", "url": "https://example.com/a.jpg", "name": "a.jpg", "mime": "image/jpeg"}]
    assert detect_message_kind("привет", []) == MESSAGE_KIND_TEXT
    assert detect_message_kind("", image) == MESSAGE_KIND_IMAGE
    assert detect_message_kind("смотрите", image) == MESSAGE_KIND_MIXED


def test_placeholder_and_text_resolution_are_kind_aware() -> None:
    voice = [{"type": "voice", "url": "https://example.com/a.ogg", "name": "a.ogg", "mime": "audio/ogg"}]
    assert placeholder_for_kind(MESSAGE_KIND_VOICE) == "[Голосовое]"
    assert text_or_placeholder("", voice) == "[Голосовое]"
    assert text_or_placeholder("готово", voice) == "готово"


def test_content_fingerprint_depends_on_media() -> None:
    first = content_fingerprint("", [{"type": "image", "url": "https://example.com/1.jpg", "name": "1.jpg"}])
    second = content_fingerprint("", [{"type": "image", "url": "https://example.com/2.jpg", "name": "2.jpg"}])
    assert first != second


def test_build_envelope_sets_trace_and_trigger_fields() -> None:
    envelope = build_envelope(
        tenant_id=101,
        lead_id=555,
        source_channel="telegram",
        direction="incoming",
        author_kind="lead",
        provider_message_id="123",
        text="Здравствуйте",
        attachments=[],
        peer="944310340",
    )
    assert envelope["channel"] == "telegram"
    assert envelope["source_channel"] == "telegram"
    assert envelope["dialog_channel"] == "telegram"
    assert envelope["trigger_bot"] is True
    assert envelope["message_kind"] == MESSAGE_KIND_TEXT
    assert envelope["trace_id"].startswith("msg:")
    assert envelope["trace_id"].endswith(envelope["message_fingerprint"])


def test_sanitize_display_name_drops_punctuation_placeholders() -> None:
    assert sanitize_display_name(".") is None
    assert sanitize_display_name("  ...  ") is None
    assert sanitize_display_name("---@") is None
    assert sanitize_display_name("@Isyyaa") == "@Isyyaa"
    assert sanitize_display_name("Илья Петров") == "Илья Петров"
