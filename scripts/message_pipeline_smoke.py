from __future__ import annotations

from libs.core.message_envelope import build_envelope, text_or_placeholder
from libs.core.services import amocrm_chat


def main() -> None:
    cases: list[tuple[str, bool]] = []

    lead_tg = build_envelope(
        tenant_id=101,
        lead_id=1001,
        source_channel="telegram",
        text="Здравствуйте",
        attachments=[],
        peer="944310340",
    )
    cases.append(("telegram_lead_trigger", lead_tg["trigger_bot"] is True))
    cases.append(("telegram_lead_kind", lead_tg["message_kind"] == "text"))

    manager_tg = build_envelope(
        tenant_id=101,
        lead_id=1001,
        source_channel="telegram",
        direction="outgoing",
        author_kind="manager",
        text="ответ менеджера",
        attachments=[],
        trigger_bot=False,
        peer="944310340",
    )
    cases.append(("telegram_manager_no_trigger", manager_tg["trigger_bot"] is False))

    avito_media = build_envelope(
        tenant_id=101,
        lead_id=2002,
        source_channel="avito",
        text="",
        attachments=[
            {
                "type": "image",
                "url": "https://example.com/a.jpg",
                "name": "a.jpg",
                "mime": "image/jpeg",
            }
        ],
    )
    cases.append(("avito_image_kind", avito_media["message_kind"] == "image"))
    cases.append(("avito_image_placeholder", text_or_placeholder("", avito_media["attachments"]) == "[Фото]"))

    amo_photo = amocrm_chat.extract_webhook_message(
        {
            "payload": {
                "conversation_id": "chat-photo",
                "msgid": "msg-photo",
                "message": {
                    "id": "msg-photo",
                    "type": "picture",
                    "media": {"url": "https://example.com/photo.jpg"},
                    "file_name": "photo.jpg",
                },
            }
        }
    )
    cases.append(("amocrm_photo_kind", amo_photo["message_kind"] == "image"))
    cases.append(("amocrm_photo_attachment", bool(amo_photo["attachments"])))

    amo_voice = amocrm_chat.extract_webhook_message(
        {
            "payload": {
                "conversation_id": "chat-voice",
                "msgid": "msg-voice",
                "message": {
                    "id": "msg-voice",
                    "type": "voice",
                    "media": {"url": "https://example.com/voice"},
                    "mime_type": "audio/ogg",
                },
            }
        }
    )
    cases.append(("amocrm_voice_kind", amo_voice["message_kind"] == "voice"))
    cases.append(("amocrm_voice_placeholder", text_or_placeholder("", amo_voice["attachments"]) == "[Голосовое]"))

    failed = [name for name, ok in cases if not ok]
    if failed:
        raise SystemExit(f"message pipeline smoke failed: {', '.join(failed)}")
    print("message pipeline smoke ok")


if __name__ == "__main__":
    main()
