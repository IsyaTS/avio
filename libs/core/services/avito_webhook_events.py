from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from libs.core.lib.numbers import coerce_int


@dataclass(frozen=True)
class AvitoWebhookEvent:
    payload: Mapping[str, Any]
    value: Mapping[str, Any]
    content: Mapping[str, Any]
    account_id: int | None
    item_id: int | None
    chat_id: str
    message_type: str
    text: str
    attachments: tuple[dict[str, Any], ...]
    unresolved_voice: Mapping[str, Any] | None
    message_id: str | None
    avito_user_id: int | None
    avito_login: str | None
    created_at: Any | None
    published_at: Any | None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _extract_account_id(event: Mapping[str, Any], payload: Mapping[str, Any]) -> int | None:
    return coerce_int(
        payload.get("account_id")
        or event.get("account_id")
        or _mapping(payload.get("account")).get("id")
        or _mapping(event.get("account")).get("id")
    )


def _extract_chat_id(event: Mapping[str, Any], payload: Mapping[str, Any], value: Mapping[str, Any]) -> str:
    candidate = (
        value.get("chat_id")
        or value.get("conversation_id")
        or payload.get("chat_id")
        or payload.get("conversation_id")
        or event.get("chat_id")
    )
    if isinstance(candidate, Mapping):
        candidate = candidate.get("id")
    if not candidate:
        candidate = event.get("chat_id")
    return str(candidate).strip() if candidate else ""


def _extract_item_id(
    event: Mapping[str, Any],
    payload: Mapping[str, Any],
    value: Mapping[str, Any],
    content: Mapping[str, Any],
) -> int | None:
    context = _mapping(value.get("context") or payload.get("context") or event.get("context"))
    context_value = _mapping(context.get("value"))
    return coerce_int(
        value.get("item_id")
        or content.get("item_id")
        or payload.get("item_id")
        or event.get("item_id")
        or _mapping(payload.get("value")).get("item_id")
        or context_value.get("id")
    )


def _last_image_url(sizes_raw: Any) -> str:
    if isinstance(sizes_raw, list):
        url = ""
        for entry in sizes_raw:
            if isinstance(entry, Mapping) and entry.get("url"):
                url = str(entry["url"])
        return url
    if isinstance(sizes_raw, Mapping):
        for entry in sizes_raw.values():
            if isinstance(entry, str) and entry:
                return entry
    return ""


def _extract_attachments(
    *,
    message_type: str,
    content: Mapping[str, Any],
    text: str,
) -> tuple[str, list[dict[str, Any]], Mapping[str, Any] | None]:
    attachments: list[dict[str, Any]] = []
    unresolved_voice: Mapping[str, Any] | None = None

    if message_type == "image":
        image = _mapping(content.get("image"))
        url = _last_image_url(image.get("sizes"))
        if url:
            attachments.append({"type": "image", "url": url, "name": image.get("name") or "image"})
        if not text:
            text = "__image__"
    elif message_type == "voice":
        voice = _mapping(content.get("voice"))
        voice_url = (
            voice.get("url")
            or voice.get("download_url")
            or voice.get("file_url")
            or voice.get("media_url")
        )
        voice_id = voice.get("voice_id") or voice.get("id")
        if voice_url:
            attachments.append(
                {
                    "type": "voice",
                    "url": str(voice_url),
                    "name": str(voice.get("name") or "voice.ogg"),
                    "mime": str(voice.get("mime") or "audio/mp4"),
                    "voice_id": str(voice_id or "").strip() or None,
                }
            )
        elif voice_id:
            unresolved_voice = {
                "voice_id": str(voice_id),
                "name": str(voice.get("name") or "voice.mp4"),
                "mime": str(voice.get("mime") or "audio/mp4"),
            }
        if not text:
            text = "__voice__"
    elif message_type == "image" and not text:
        text = "__image__"
    return text, attachments, unresolved_voice


def normalize_public_webhook_event(event: Mapping[str, Any]) -> AvitoWebhookEvent:
    payload = _mapping(event.get("payload"))
    value = _mapping(payload.get("value") or event.get("value"))
    content = _mapping(value.get("content"))
    message_type = str(value.get("type") or "").strip().lower()

    text_candidate = content.get("text") if content else ""
    if not text_candidate:
        text_candidate = value.get("text") or payload.get("text") or ""
    text = str(text_candidate or "").strip()
    text, attachments, unresolved_voice = _extract_attachments(
        message_type=message_type,
        content=content,
        text=text,
    )

    message_id = value.get("id") or event.get("event_id") or event.get("id")
    login_candidate = value.get("author_login") or payload.get("user_login")
    avito_login = login_candidate.strip() if isinstance(login_candidate, str) and login_candidate.strip() else None

    return AvitoWebhookEvent(
        payload=payload,
        value=value,
        content=content,
        account_id=_extract_account_id(event, payload),
        item_id=_extract_item_id(event, payload, value, content),
        chat_id=_extract_chat_id(event, payload, value),
        message_type=message_type,
        text=text,
        attachments=tuple(attachments),
        unresolved_voice=unresolved_voice,
        message_id=str(message_id) if message_id is not None else None,
        avito_user_id=coerce_int(
            content.get("author_id")
            or value.get("author_id")
            or value.get("sender_id")
            or payload.get("user_id")
        ),
        avito_login=avito_login,
        created_at=value.get("created") or content.get("created") or payload.get("created"),
        published_at=value.get("published_at") or payload.get("published_at"),
    )


__all__ = ["AvitoWebhookEvent", "normalize_public_webhook_event"]
