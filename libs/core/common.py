from __future__ import annotations

from dataclasses import dataclass
import csv
import io
import os
import re
from typing import Any, FrozenSet, Mapping, MutableMapping

from libs.core.transport import WhatsAppAddressError, normalize_e164_digits
from . import sales_core as sales_core_module


OUTBOX_QUEUE_KEY = "outbox:send"
OUTBOX_DLQ_KEY = "outbox:dlq"

_FALSE_TOKENS = {"0", "false", "no", "off", "disabled"}
_TRUE_TOKENS = {"1", "true", "yes", "on", "enabled"}


def _coerce_bool(value: Any | None, default: bool | None = None) -> bool | None:
    """Convert env/config values to booleans with lenient parsing (supports default)."""

    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if not lowered:
            return default
        if lowered in _FALSE_TOKENS:
            return False
        if lowered in _TRUE_TOKENS:
            return True
        return bool(lowered)
    return bool(value)


_SMART_REPLY_ENV = os.getenv("SMART_REPLY_ENABLED")
_AI_ENABLED_ENV = os.getenv("AI_ENABLED")

_SMART_REPLY_DEFAULT = _coerce_bool(_SMART_REPLY_ENV)
if _SMART_REPLY_DEFAULT is None:
    _SMART_REPLY_DEFAULT = _coerce_bool(_AI_ENABLED_ENV)

SMART_REPLY_ENABLED_DEFAULT = (
    True if _SMART_REPLY_DEFAULT is None else bool(_SMART_REPLY_DEFAULT)
)
AI_ENABLED_DEFAULT = SMART_REPLY_ENABLED_DEFAULT

try:
    _HANDOFF_SILENCE_TTL = int(os.getenv("HANDOFF_SILENCE_TTL_SECONDS", "86400"))
except Exception:
    _HANDOFF_SILENCE_TTL = 86400
if _HANDOFF_SILENCE_TTL <= 0:
    _HANDOFF_SILENCE_TTL = 86400
HANDOFF_SILENCE_TTL_SECONDS = _HANDOFF_SILENCE_TTL


def handoff_silence_key(tenant: int, lead_id: int) -> str:
    """Redis key that mutes smart replies after a manual takeover."""

    return f"handoff:silence:{int(tenant)}:{int(lead_id)}"


def handoff_silence_meta_key(tenant: int, lead_id: int) -> str:
    """Redis key for storing silence reason metadata."""

    return f"handoff:silence:meta:{int(tenant)}:{int(lead_id)}"

try:
    _AVITO_BOT_ECHO_TTL = int(os.getenv("AVITO_BOT_ECHO_TTL_SECONDS", "120"))
except Exception:
    _AVITO_BOT_ECHO_TTL = 120
if _AVITO_BOT_ECHO_TTL <= 0:
    _AVITO_BOT_ECHO_TTL = 120
AVITO_BOT_ECHO_TTL_SECONDS = _AVITO_BOT_ECHO_TTL


def avito_bot_echo_key(tenant: int, chat_id: str) -> str:
    """Redis key storing last bot message to detect Avito self-echo."""

    return f"avito:bot_echo:{int(tenant)}:{chat_id}"


def normalize_echo_text(text: str) -> str:
    """Normalize outgoing text for echo matching (lower + collapse whitespace)."""

    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())

_FALLBACK_REPLY_DEFAULT = (
    os.getenv("SMART_REPLY_FALLBACK_TEXT")
    or "Принял запрос. Скидываю весь каталог. Если нужно PDF — напишите «каталог pdf»."
)


def default_fallback_reply(_tenant: int | None = None) -> str:
    """Return configurable fallback reply text used when LLM is unavailable."""

    return _FALLBACK_REPLY_DEFAULT


@dataclass(frozen=True)
class OutboxWhitelist:
    """Parsed whitelist configuration for the outbox worker."""

    allow_all: bool
    ids: FrozenSet[int]
    usernames: FrozenSet[str]
    numbers: FrozenSet[str]
    numbers_with_plus: FrozenSet[str]
    raw_tokens: FrozenSet[str]
    raw_value: str


def normalize_username(value: str | None) -> str | None:
    """Normalize Telegram usernames to an ``@user`` form."""

    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if not cleaned.startswith("@"):
        cleaned = f"@{cleaned.lstrip('@')}"
    return cleaned


def _parse_whitelist_tokens(raw_value: str) -> list[str]:
    if not raw_value:
        return []
    reader = csv.reader(io.StringIO(raw_value), skipinitialspace=True)
    tokens: list[str] = []
    for row in reader:
        for token in row:
            cleaned = token.strip()
            if cleaned:
                tokens.append(cleaned)
    return tokens


def _try_normalize_number(token: str) -> str | None:
    try:
        return normalize_e164_digits(token)
    except WhatsAppAddressError:
        return None


def get_outbox_whitelist(
    env: Mapping[str, str] | MutableMapping[str, str] | None = None,
) -> OutboxWhitelist:
    """Read and parse ``OUTBOX_WHITELIST`` from the provided env mapping."""

    source: Mapping[str, str] | MutableMapping[str, str]
    if env is None:
        source = os.environ
    else:
        source = env

    raw_value = source.get("OUTBOX_WHITELIST", "")
    if raw_value is None:
        raw_value = ""

    tokens = _parse_whitelist_tokens(raw_value)
    raw_tokens = frozenset(tokens)
    if not tokens or "*" in raw_tokens:
        return OutboxWhitelist(
            allow_all=True,
            ids=frozenset(),
            usernames=frozenset(),
            numbers=frozenset(),
            numbers_with_plus=frozenset(),
            raw_tokens=raw_tokens,
            raw_value=raw_value,
        )

    ids = set()
    usernames = set()
    numbers = set()
    numbers_with_plus = set()
    for token in tokens:
        try:
            ids.add(int(token))
            continue
        except ValueError:
            pass
        normalized_number = _try_normalize_number(token)
        if normalized_number:
            numbers.add(normalized_number)
            numbers_with_plus.add(f"+{normalized_number}")
        normalized = normalize_username(token)
        if normalized:
            lowered = normalized.lower()
            usernames.add(lowered)
            usernames.add(lowered.lstrip("@"))

    return OutboxWhitelist(
        allow_all=False,
        ids=frozenset(ids),
        usernames=frozenset(usernames),
        numbers=frozenset(numbers),
        numbers_with_plus=frozenset(numbers_with_plus),
        raw_tokens=raw_tokens,
        raw_value=raw_value,
    )


def whitelist_contains_number(whitelist: OutboxWhitelist, digits: str) -> bool:
    """Check whether the canonical E.164 digits are allowed by the whitelist."""

    if whitelist.allow_all:
        return True

    candidate = (digits or "").strip()
    if not candidate:
        return False

    if candidate in whitelist.numbers:
        return True

    plus_form = f"+{candidate}"
    if plus_form in whitelist.numbers_with_plus:
        return True

    if candidate in whitelist.raw_tokens:
        return True
    if plus_form in whitelist.raw_tokens:
        return True
    if f"{candidate}@c.us" in whitelist.raw_tokens:
        return True

    return False


def smart_reply_enabled(tenant: int | None = None) -> bool:
    """Determine whether AI-powered replies are enabled for the given tenant."""

    if tenant is None:
        return SMART_REPLY_ENABLED_DEFAULT

    cfg: Mapping[str, Any] | None = None
    try:
        cfg = sales_core_module.read_tenant_config(int(tenant))
    except Exception:
        cfg = None

    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            for key in ("smart_reply_enabled", "ai_enabled", "ai"):
                if key in behavior:
                    flag = _coerce_bool(behavior.get(key))
                    if flag is not None:
                        return bool(flag)
    return SMART_REPLY_ENABLED_DEFAULT


def _parse_int_set(raw: str) -> set[int]:
    result: set[int] = set()
    for token in _parse_whitelist_tokens(raw):
        try:
            candidate = int(token)
        except Exception:
            continue
        if candidate:
            result.add(candidate)
    return result


def _parse_str_set(raw: str) -> set[str]:
    result: set[str] = set()
    for token in _parse_whitelist_tokens(raw):
        cleaned = token.strip()
        if cleaned:
            result.add(cleaned)
    return result


_MANAGER_TG_IDS = _parse_int_set(os.getenv("MANAGER_TELEGRAM_IDS", ""))
_MANAGER_WA_NUMBERS = _parse_str_set(os.getenv("MANAGER_WHATSAPP_NUMBERS", ""))
_NOTIFY_DEFAULT_CHAT_IDS = _parse_int_set(os.getenv("NOTIFY_DEFAULT_CHAT_IDS", ""))


def is_manager_telegram(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return int(user_id) in _MANAGER_TG_IDS


def is_manager_whatsapp(digits: str | None) -> bool:
    if not digits:
        return False
    normalized = normalize_e164_digits(digits)
    candidates = {normalized, normalized.lstrip("+"), f"+{normalized.lstrip('+')}"}
    return any(item in _MANAGER_WA_NUMBERS for item in candidates)


def _normalize_chat_ids(raw: Any) -> list[int]:
    """Coerce chat ids from config/env to a list of ints."""

    if raw is None:
        return []

    if isinstance(raw, (int, float)):
        try:
            candidate = int(raw)
        except Exception:
            return []
        return [candidate] if candidate else []

    if isinstance(raw, str):
        if not raw.strip():
            return []
        return sorted(_parse_int_set(raw))

    if isinstance(raw, (list, tuple, set)):
        result: list[int] = []
        for item in raw:
            try:
                candidate = int(item)
            except Exception:
                continue
            if candidate:
                result.append(candidate)
        return result

    return []


def _read_notifications_config(tenant: int) -> Mapping[str, Any]:
    try:
        cfg = sales_core_module.read_tenant_config(int(tenant))
    except Exception:
        return {}
    if not isinstance(cfg, Mapping):
        return {}
    raw = cfg.get("notifications")
    return raw if isinstance(raw, Mapping) else {}


def notification_chat_ids(tenant: int, event: str | None = None) -> list[int]:
    """Resolve chat ids for notifications (event-specific -> tenant-wide -> env default)."""

    notifications = _read_notifications_config(tenant)
    chat_ids: list[int] = []
    if event:
        event_cfg = notifications.get(event)
        if isinstance(event_cfg, Mapping):
            chat_ids = _normalize_chat_ids(event_cfg.get("chat_ids"))
    if not chat_ids:
        chat_ids = _normalize_chat_ids(notifications.get("chat_ids"))
    if not chat_ids and _NOTIFY_DEFAULT_CHAT_IDS:
        chat_ids = sorted(_NOTIFY_DEFAULT_CHAT_IDS)
    return chat_ids


def notification_event_enabled(tenant: int, event: str) -> bool:
    """Check whether notification event enabled for tenant (defaults to False)."""

    notifications = _read_notifications_config(tenant)
    if not isinstance(notifications, Mapping):
        return False
    event_cfg = notifications.get(event)
    if isinstance(event_cfg, Mapping) and "enabled" in event_cfg:
        return _coerce_bool(event_cfg.get("enabled"), False)

    global_flag = notifications.get("enabled")
    if global_flag is not None:
        return _coerce_bool(global_flag, False)
    return False


__all__ = [
    "OUTBOX_QUEUE_KEY",
    "OUTBOX_DLQ_KEY",
    "OutboxWhitelist",
    "get_outbox_whitelist",
    "whitelist_contains_number",
    "normalize_username",
    "smart_reply_enabled",
    "notification_event_enabled",
    "notification_chat_ids",
    "default_fallback_reply",
    "SMART_REPLY_ENABLED_DEFAULT",
    "AI_ENABLED_DEFAULT",
    "HANDOFF_SILENCE_TTL_SECONDS",
    "handoff_silence_key",
    "handoff_silence_meta_key",
    "AVITO_BOT_ECHO_TTL_SECONDS",
    "avito_bot_echo_key",
    "normalize_echo_text",
    "is_manager_telegram",
    "is_manager_whatsapp",
]
