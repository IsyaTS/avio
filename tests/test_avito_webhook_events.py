from __future__ import annotations

import pytest

from libs.core.services.avito_webhook_events import normalize_public_webhook_event


pytestmark = pytest.mark.unit


def test_normalize_public_webhook_event_extracts_text_and_identity() -> None:
    event = {
        "payload": {
            "account_id": "123456",
            "value": {
                "id": "msg-1",
                "item_id": "7492149515",
                "chat_id": {"id": "chat-1"},
                "type": "text",
                "author_id": "777",
                "author_login": " buyer ",
                "created": "2026-05-11T08:00:00Z",
                "content": {"text": "Есть Delta 100?"},
            },
        }
    }

    normalized = normalize_public_webhook_event(event)

    assert normalized.account_id == 123456
    assert normalized.item_id == 7492149515
    assert normalized.chat_id == "chat-1"
    assert normalized.message_id == "msg-1"
    assert normalized.avito_user_id == 777
    assert normalized.avito_login == "buyer"
    assert normalized.message_type == "text"
    assert normalized.text == "Есть Delta 100?"
    assert normalized.attachments == ()
    assert normalized.created_at == "2026-05-11T08:00:00Z"


def test_normalize_public_webhook_event_extracts_item_id_from_fallback_locations() -> None:
    normalized = normalize_public_webhook_event(
        {
            "payload": {
                "value": {
                    "chat_id": "chat-1",
                    "type": "text",
                    "content": {"text": "hi", "item_id": "123"},
                }
            }
        }
    )
    assert normalized.item_id == 123

    normalized_from_context = normalize_public_webhook_event(
        {
            "payload": {
                "value": {
                    "chat_id": "chat-1",
                    "type": "text",
                    "context": {"value": {"id": "456"}},
                    "content": {"text": "hi"},
                }
            }
        }
    )
    assert normalized_from_context.item_id == 456


def test_normalize_public_webhook_event_extracts_image_attachment() -> None:
    event = {
        "payload": {
            "value": {
                "chat_id": "chat-2",
                "type": "image",
                "content": {
                    "image": {
                        "name": "photo.jpg",
                        "sizes": [
                            {"url": "https://example.test/small.jpg"},
                            {"url": "https://example.test/large.jpg"},
                        ],
                    }
                },
            }
        }
    }

    normalized = normalize_public_webhook_event(event)

    assert normalized.chat_id == "chat-2"
    assert normalized.text == "__image__"
    assert normalized.attachments == (
        {"type": "image", "url": "https://example.test/large.jpg", "name": "photo.jpg"},
    )


def test_normalize_public_webhook_event_keeps_unresolved_voice_for_async_resolution() -> None:
    event = {
        "payload": {
            "value": {
                "conversation_id": "chat-3",
                "type": "voice",
                "content": {"voice": {"id": "voice-1", "name": "client.ogg", "mime": "audio/ogg"}},
            }
        }
    }

    normalized = normalize_public_webhook_event(event)

    assert normalized.chat_id == "chat-3"
    assert normalized.text == "__voice__"
    assert normalized.attachments == ()
    assert normalized.unresolved_voice == {
        "voice_id": "voice-1",
        "name": "client.ogg",
        "mime": "audio/ogg",
    }
