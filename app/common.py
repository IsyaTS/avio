from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, FrozenSet, Mapping, MutableMapping


OUTBOX_QUEUE_KEY = "outbox:send"
OUTBOX_DLQ_KEY = "outbox:dlq"

_FALSE_TOKENS = {"0", "false", "no", "off", "disabled"}
_TRUE_TOKENS = {"1", "true", "yes", "on", "enabled"}


def _coerce_bool(value: Any | None) -> bool | None:
    """Convert env/config values to booleans with lenient parsing."""

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if not lowered:
            return None
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


@dataclass(frozen=True)
class OutboxWhitelist:
    """Parsed whitelist configuration for the outbox worker."""

    allow_all: bool
    ids: FrozenSet[int]
    usernames: FrozenSet[str]
    raw_tokens: FrozenSet[str]


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

    tokens = [token.strip() for token in raw_value.split(",") if token.strip()]
    raw_tokens = frozenset(tokens)
    if not tokens or "*" in raw_tokens:
        return OutboxWhitelist(
            allow_all=True,
            ids=frozenset(),
            usernames=frozenset(),
            raw_tokens=raw_tokens,
        )

    ids = set()
    usernames = set()
    for token in tokens:
        try:
            ids.add(int(token))
            continue
        except ValueError:
            pass
        normalized = normalize_username(token)
        if normalized:
            lowered = normalized.lower()
            usernames.add(lowered)
            usernames.add(lowered.lstrip("@"))

    return OutboxWhitelist(
        allow_all=False,
        ids=frozenset(ids),
        usernames=frozenset(usernames),
        raw_tokens=raw_tokens,
    )


def smart_reply_enabled(tenant: int | None = None) -> bool:
    """Determine whether AI-powered replies are enabled for the given tenant."""

    if tenant is None:
        return SMART_REPLY_ENABLED_DEFAULT

    cfg: Mapping[str, Any] | None = None
    try:  # delayed import to avoid circular references during startup
        from app.web import common as web_common

        cfg = web_common.read_tenant_config(tenant)
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


__all__ = [
    "OUTBOX_QUEUE_KEY",
    "OUTBOX_DLQ_KEY",
    "OutboxWhitelist",
    "get_outbox_whitelist",
    "normalize_username",
    "smart_reply_enabled",
    "SMART_REPLY_ENABLED_DEFAULT",
    "AI_ENABLED_DEFAULT",
]
