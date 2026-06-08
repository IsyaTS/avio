from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping


_TG_USERNAME_RE = re.compile(r"^@?(?=.*[A-Za-z])[A-Za-z0-9_]{5,32}$")
_TG_LINK_RE = re.compile(r"^(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z0-9_]{5,32})/?$", re.IGNORECASE)


@dataclass(frozen=True)
class LandingContact:
    name: str
    contact: str
    message: str


@dataclass(frozen=True)
class LandingContactError:
    detail: str
    message: str


def normalize_landing_contact(raw_value: str) -> tuple[str, str | None]:
    value = str(raw_value or "").strip()
    if not value:
        return "", "contact_required"
    link_match = _TG_LINK_RE.match(value)
    if link_match:
        return f"@{link_match.group(1)}", None
    if _TG_USERNAME_RE.match(value):
        return value if value.startswith("@") else f"@{value}", None
    digits = re.sub(r"\D+", "", value)
    if digits:
        if len(digits) != 11:
            return "", "invalid_phone_length"
        if digits.startswith("8"):
            return f"+7{digits[1:]}", None
        if digits.startswith("7"):
            return f"+7{digits[1:]}", None
        return f"+{digits}", None
    return "", "invalid_contact_format"


def parse_landing_contact_payload(payload: Mapping[str, Any] | None) -> LandingContact:
    data = payload if isinstance(payload, Mapping) else {}
    contact = str(
        data.get("contact")
        or data.get("phone_or_telegram")
        or data.get("phoneOrTelegram")
        or data.get("phone")
        or data.get("telegram")
        or ""
    ).strip()
    return LandingContact(
        name=str(data.get("name") or "").strip()[:120],
        contact=contact,
        message=str(data.get("message") or data.get("project") or "").strip()[:2000],
    )


def validate_landing_contact(raw: LandingContact) -> tuple[LandingContact | None, LandingContactError | None]:
    normalized_contact, error = normalize_landing_contact(raw.contact)
    if error == "contact_required":
        return None, LandingContactError(error, "Укажите телефон или Telegram.")
    if error == "invalid_phone_length":
        return None, LandingContactError(
            error,
            "Телефон должен содержать 11 цифр. Либо укажите Telegram (@username).",
        )
    if error:
        return None, LandingContactError(
            "invalid_contact_format",
            "Укажите телефон (11 цифр) или Telegram (@username).",
        )
    return LandingContact(name=raw.name, contact=normalized_contact[:200], message=raw.message), None
