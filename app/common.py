from __future__ import annotations

"""Shared application-level constants and utilities."""

from dataclasses import dataclass
import os
from typing import FrozenSet, Mapping, MutableMapping


OUTBOX_QUEUE_KEY = "outbox:send"
OUTBOX_DLQ_KEY = "outbox:dlq"


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


def get_outbox_whitelist(env: Mapping[str, str] | MutableMapping[str, str] | None = None) -> OutboxWhitelist:
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
        return OutboxWhitelist(allow_all=True, ids=frozenset(), usernames=frozenset(), raw_tokens=raw_tokens)

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


__all__ = [
    "OUTBOX_QUEUE_KEY",
    "OUTBOX_DLQ_KEY",
    "OutboxWhitelist",
    "get_outbox_whitelist",
    "normalize_username",
]
