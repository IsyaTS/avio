from __future__ import annotations

from libs.core.services import amocrm_chat


def test_extract_webhook_message_parses_picture_payload() -> None:
    payload = {
        "event_type": "new_message",
        "payload": {
            "conversation_id": "chat-1",
            "msgid": "msg-1",
            "message": {
                "id": "msg-1",
                "type": "picture",
                "text": "",
                "media": {"url": "https://example.com/photo.jpg"},
                "file_name": "photo.jpg",
                "file_size": 42,
            },
        },
    }

    message = amocrm_chat.extract_webhook_message(payload)

    assert message["external_conversation_id"] == "chat-1"
    assert message["external_message_id"] == "msg-1"
    assert message["message_kind"] == "image"
    assert message["attachments"] == [
        {
            "type": "image",
            "url": "https://example.com/photo.jpg",
            "name": "photo.jpg",
            "mime": "image/jpeg",
            "size": 42,
        }
    ]


def test_extract_webhook_message_marks_voice() -> None:
    payload = {
        "message": {
            "conversation_id": "chat-2",
            "id": "msg-2",
            "message": {
                "type": "voice",
                "media": {"url": "https://example.com/voice"},
                "mime_type": "audio/ogg",
            },
        }
    }

    message = amocrm_chat.extract_webhook_message(payload)

    assert message["message_kind"] == "voice"
    assert message["attachments"][0]["type"] == "voice"
    assert message["attachments"][0]["name"].endswith(".ogg")
