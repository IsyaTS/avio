from __future__ import annotations

from typing import Any, Mapping, Sequence


def extract_llm_response_text(response: Any) -> str:
    """Extract visible assistant text from common OpenAI response shapes."""

    text = _first_text(getattr(response, "output_text", None))
    if text:
        return text
    choices = getattr(response, "choices", None)
    if isinstance(choices, Sequence) and not isinstance(choices, (str, bytes, bytearray)):
        for choice in choices:
            text = _extract_choice_text(choice)
            if text:
                return text
    return ""


def _extract_choice_text(choice: Any) -> str:
    text = _first_text(getattr(choice, "text", None))
    if text:
        return text
    message = getattr(choice, "message", None)
    if message is None and isinstance(choice, Mapping):
        message = choice.get("message")
    if message is None:
        return ""
    for key in ("content", "refusal", "parsed"):
        value = getattr(message, key, None)
        if value is None and isinstance(message, Mapping):
            value = message.get(key)
        text = _first_text(value)
        if text:
            return text
    return ""


def _first_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("text", "content", "value", "output_text"):
            text = _first_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        parts: list[str] = []
        for item in value:
            text = _first_text(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    return str(value or "").strip()
