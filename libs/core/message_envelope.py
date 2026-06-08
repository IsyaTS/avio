from __future__ import annotations

import hashlib
import mimetypes
import pathlib
import re
from typing import Any, Iterable, Mapping, MutableMapping
from urllib.parse import urlparse

from libs.core.lib.numbers import coerce_int as _coerce_int_shared


MESSAGE_KIND_TEXT = "text"
MESSAGE_KIND_IMAGE = "image"
MESSAGE_KIND_VIDEO = "video"
MESSAGE_KIND_VOICE = "voice"
MESSAGE_KIND_FILE = "file"
MESSAGE_KIND_MIXED = "mixed"

SUPPORTED_MESSAGE_KINDS = {
    MESSAGE_KIND_TEXT,
    MESSAGE_KIND_IMAGE,
    MESSAGE_KIND_VIDEO,
    MESSAGE_KIND_VOICE,
    MESSAGE_KIND_FILE,
    MESSAGE_KIND_MIXED,
}

ATTACHMENT_KIND_ALIASES = {
    "picture": MESSAGE_KIND_IMAGE,
    "photo": MESSAGE_KIND_IMAGE,
    "image": MESSAGE_KIND_IMAGE,
    "video": MESSAGE_KIND_VIDEO,
    "voice": MESSAGE_KIND_VOICE,
    "audio": MESSAGE_KIND_FILE,
    "document": MESSAGE_KIND_FILE,
    "file": MESSAGE_KIND_FILE,
}

PLACEHOLDERS = {
    MESSAGE_KIND_IMAGE: "[Фото]",
    MESSAGE_KIND_VIDEO: "[Видео]",
    MESSAGE_KIND_VOICE: "[Голосовое]",
    MESSAGE_KIND_FILE: "[Файл]",
    MESSAGE_KIND_MIXED: "[Вложение]",
}

_DISPLAY_NAME_SPACE_RE = re.compile(r"\s+")
_DISPLAY_NAME_HAS_WORD_RE = re.compile(r"[\w\d]+", re.UNICODE)


def normalize_channel(value: Any, *, default: str = "whatsapp") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "tg": "telegram",
        "telegram": "telegram",
        "wa": "whatsapp",
        "whatsapp": "whatsapp",
        "avito": "avito",
        "max": "max",
        "max_personal": "max_personal",
        "max-personal": "max_personal",
        "amocrm": "amocrm",
    }
    return aliases.get(raw, default)


def sanitize_display_name(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    collapsed = _DISPLAY_NAME_SPACE_RE.sub(" ", raw)
    if collapsed.strip().lower() in {"contact", "контакт"}:
        return None
    if not _DISPLAY_NAME_HAS_WORD_RE.search(collapsed):
        return None
    return collapsed


def normalize_direction(value: Any, *, outgoing: bool | None = None) -> str:
    if outgoing is not None:
        return "outgoing" if outgoing else "incoming"
    raw = str(value or "").strip().lower()
    if raw in {"out", "outgoing", "sent"}:
        return "outgoing"
    return "incoming"


def normalize_author_kind(value: Any, *, manager: bool = False, is_bot: bool = False) -> str:
    if manager:
        return "manager"
    if is_bot:
        return "bot"
    raw = str(value or "").strip().lower()
    if raw in {"manager", "bot", "system", "lead"}:
        return raw
    return "lead"


def _coerce_int(value: Any) -> int | None:
    return _coerce_int_shared(value, min_value=0)


def normalize_attachment_type(raw_type: Any, mime: Any = None, name: Any = None) -> str:
    type_value = str(raw_type or "").strip().lower()
    mime_value = str(mime or "").strip().lower()
    name_value = str(name or "").strip().lower()
    if (
        mime_value.startswith("image/")
        or "photo" in type_value
        or "image" in type_value
        or "picture" in type_value
    ):
        return MESSAGE_KIND_IMAGE
    if mime_value.startswith("video/") or "video" in type_value:
        return MESSAGE_KIND_VIDEO
    if "voice" in type_value or mime_value in {"audio/ogg", "audio/opus"}:
        return MESSAGE_KIND_VOICE
    if type_value in ATTACHMENT_KIND_ALIASES:
        return ATTACHMENT_KIND_ALIASES[type_value]
    guessed_mime, _ = mimetypes.guess_type(name_value)
    if guessed_mime:
        if guessed_mime.startswith("image/"):
            return MESSAGE_KIND_IMAGE
        if guessed_mime.startswith("video/"):
            return MESSAGE_KIND_VIDEO
        if guessed_mime in {"audio/ogg", "audio/opus"}:
            return MESSAGE_KIND_VOICE
    return MESSAGE_KIND_FILE


def normalize_attachment(blob: Mapping[str, Any]) -> dict[str, Any] | None:
    if not isinstance(blob, Mapping):
        return None
    url = str(blob.get("url") or "").strip()
    if not url:
        return None
    name = str(
        blob.get("name")
        or blob.get("filename")
        or blob.get("title")
        or pathlib.PurePosixPath(urlparse(url).path).name
        or ""
    ).strip()
    mime = str(
        blob.get("mime")
        or blob.get("mime_type")
        or blob.get("content_type")
        or blob.get("mimetype")
        or ""
    ).strip()
    attachment_type = normalize_attachment_type(
        blob.get("type") or blob.get("kind") or blob.get("_"), mime, name
    )
    if not name:
        defaults = {
            MESSAGE_KIND_IMAGE: "image.jpg",
            MESSAGE_KIND_VIDEO: "video.mp4",
            MESSAGE_KIND_VOICE: "voice.ogg",
            MESSAGE_KIND_FILE: "file",
        }
        name = defaults.get(attachment_type, "file")
    if attachment_type == MESSAGE_KIND_VOICE and "." not in name:
        name = f"{name}.ogg"
    caption = blob.get("caption") or blob.get("text") or blob.get("description")
    normalized: dict[str, Any] = {
        "type": attachment_type,
        "url": url,
        "name": name,
    }
    if mime:
        normalized["mime"] = mime
    size = _coerce_int(
        blob.get("size") or blob.get("file_size") or blob.get("filesize") or blob.get("length")
    )
    if size is not None:
        normalized["size"] = size
    if caption is not None:
        caption_value = str(caption).strip()
        if caption_value:
            normalized["caption"] = caption_value
    path_value = blob.get("path")
    if isinstance(path_value, str):
        cleaned_path = path_value.strip()
        if cleaned_path:
            normalized["path"] = cleaned_path
    return normalized


def normalize_attachments(blobs: Iterable[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for blob in blobs or []:
        item = normalize_attachment(blob)
        if not item:
            continue
        key = (
            str(item.get("type") or ""),
            str(item.get("url") or ""),
            str(item.get("name") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        normalized.append(item)
    return normalized


def detect_message_kind(text: Any, attachments: Iterable[Mapping[str, Any]] | None) -> str:
    normalized = normalize_attachments(attachments)
    has_text = bool(str(text or "").strip())
    if not normalized:
        return MESSAGE_KIND_TEXT if has_text else MESSAGE_KIND_FILE
    kinds = {str(item.get("type") or MESSAGE_KIND_FILE) for item in normalized}
    kinds.discard("")
    if has_text:
        return MESSAGE_KIND_MIXED
    if len(kinds) == 1:
        single = next(iter(kinds))
        if single in SUPPORTED_MESSAGE_KINDS:
            return single
    return MESSAGE_KIND_MIXED


def placeholder_for_kind(message_kind: str) -> str:
    normalized = str(message_kind or "").strip().lower()
    return PLACEHOLDERS.get(normalized, PLACEHOLDERS[MESSAGE_KIND_MIXED])


def text_or_placeholder(text: Any, attachments: Iterable[Mapping[str, Any]] | None) -> str:
    value = str(text or "").strip()
    if value:
        return value
    normalized = normalize_attachments(attachments)
    if not normalized:
        return ""
    return placeholder_for_kind(detect_message_kind("", normalized))


def content_fingerprint(text: Any, attachments: Iterable[Mapping[str, Any]] | None) -> str:
    normalized_text = " ".join(str(text or "").split())
    normalized_attachments = normalize_attachments(attachments)
    parts = [normalized_text]
    for item in normalized_attachments:
        parts.append(
            "|".join(
                [
                    str(item.get("type") or ""),
                    str(item.get("url") or ""),
                    str(item.get("name") or ""),
                    str(item.get("mime") or ""),
                ]
            )
        )
    payload = "\n".join(parts).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def message_fingerprint(
    *,
    channel: Any,
    direction: Any,
    author_kind: Any,
    provider_message_id: Any,
    text: Any,
    attachments: Iterable[Mapping[str, Any]] | None,
    lead_id: Any = None,
) -> str:
    content_hash = content_fingerprint(text, attachments)
    payload = "|".join(
        [
            normalize_channel(channel, default="unknown"),
            normalize_direction(direction),
            normalize_author_kind(author_kind),
            str(provider_message_id or "").strip(),
            str(lead_id or "").strip(),
            content_hash,
        ]
    ).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def build_envelope(
    *,
    tenant_id: int,
    lead_id: int,
    source_channel: Any,
    dialog_channel: Any | None = None,
    direction: Any = "incoming",
    author_kind: Any = "lead",
    provider_message_id: Any = None,
    text: Any = "",
    attachments: Iterable[Mapping[str, Any]] | None = None,
    trigger_bot: bool | None = None,
    peer: Any = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    direction_value = normalize_direction(direction)
    author_kind_value = normalize_author_kind(author_kind)
    source_channel_value = normalize_channel(source_channel)
    dialog_channel_value = normalize_channel(dialog_channel or source_channel_value)
    normalized_attachments = normalize_attachments(attachments)
    text_value = str(text or "").strip()
    message_kind = detect_message_kind(text_value, normalized_attachments)
    if trigger_bot is None:
        trigger_bot = direction_value == "incoming" and author_kind_value == "lead"
    envelope: dict[str, Any] = {
        "normalized_envelope_version": 1,
        "tenant_id": int(tenant_id),
        "tenant": int(tenant_id),
        "lead_id": int(lead_id),
        "channel": dialog_channel_value,
        "ch": dialog_channel_value,
        "source_channel": source_channel_value,
        "dialog_channel": dialog_channel_value,
        "direction": direction_value,
        "author_kind": author_kind_value,
        "message_id": str(provider_message_id or "").strip(),
        "provider_message_id": str(provider_message_id or "").strip(),
        "text": text_value,
        "attachments": normalized_attachments,
        "message_kind": message_kind,
        "trigger_bot": bool(trigger_bot),
        "message_fingerprint": message_fingerprint(
            channel=dialog_channel_value,
            direction=direction_value,
            author_kind=author_kind_value,
            provider_message_id=provider_message_id,
            text=text_value,
            attachments=normalized_attachments,
            lead_id=lead_id,
        ),
    }
    envelope["trace_id"] = f"msg:{envelope['message_fingerprint']}"
    if peer is not None:
        peer_value = str(peer).strip()
        if peer_value:
            envelope["peer"] = peer_value
    if extra:
        for key, value in extra.items():
            if value is not None:
                envelope[key] = value
    return envelope


def enrich_with_meta(target: MutableMapping[str, Any], **fields: Any) -> MutableMapping[str, Any]:
    for key, value in fields.items():
        if value is not None:
            target[key] = value
    return target
