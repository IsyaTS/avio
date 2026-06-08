from __future__ import annotations

import re
from typing import Any, Callable


SyncFn = Callable[..., Any]

_MAX_HUMAN_NAME_RE = re.compile(r"[A-Za-zА-Яа-яЁё]")
_TG_USERNAME_URL_RE = re.compile(r"(?iu)(?:https?://)?(?:t(?:elegram)?\.me)/([a-z][a-z0-9_]{4,31})")
_TG_USERNAME_AT_RE = re.compile(r"(?iu)(?<![\w.])@([a-z][a-z0-9_]{4,31})(?![\w])")
_TG_USERNAME_RESERVED = {
    "joinchat",
    "addstickers",
    "addemoji",
    "share",
    "s",
    "iv",
    "proxy",
    "login",
    "c",
}


def normalize_max_human_name(
    value: Any,
    *,
    sanitize_display_name_fn: SyncFn,
    peer_value: str | None = None,
    max_user_id: int | None = None,
) -> str | None:
    cleaned = sanitize_display_name_fn(value)
    if not cleaned:
        return None
    lowered = cleaned.lower()
    if cleaned.isdigit():
        return None
    if lowered.startswith(("max:", "max_personal:", "max:id")):
        return None
    peer_norm = str(peer_value or "").strip()
    if peer_norm and cleaned == peer_norm:
        return None
    if max_user_id is not None and cleaned == str(int(max_user_id)):
        return None
    if not _MAX_HUMAN_NAME_RE.search(cleaned):
        return None
    return cleaned


def extract_ru_phone(text: str) -> str:
    if not text:
        return ""
    digits = re.sub(r"\D", "", text)
    if digits.startswith("8") and len(digits) == 11:
        digits = f"7{digits[1:]}"
    elif digits.startswith("7") and len(digits) == 10:
        digits = f"7{digits}"
    if len(digits) == 11 and digits.startswith("7"):
        return f"+{digits}"
    return ""


def extract_tg_username(text: str, *, normalize_username_fn: SyncFn) -> str:
    if not text:
        return ""
    raw = str(text).strip()
    if not raw:
        return ""

    for match in _TG_USERNAME_URL_RE.finditer(raw):
        candidate = _normalized_tg_candidate(match.group(1), normalize_username_fn)
        if candidate:
            return candidate

    for match in _TG_USERNAME_AT_RE.finditer(raw):
        candidate = _normalized_tg_candidate(match.group(1), normalize_username_fn)
        if candidate:
            return candidate
    return ""


def _normalized_tg_candidate(raw: Any, normalize_username_fn: SyncFn) -> str:
    candidate = str(raw or "").strip()
    if not candidate:
        return ""
    if candidate.lower() in _TG_USERNAME_RESERVED:
        return ""
    return str(normalize_username_fn(candidate) or "")
