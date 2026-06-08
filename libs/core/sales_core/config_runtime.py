from __future__ import annotations

import re
from typing import Any

_TRUE_TOKENS = {"1", "true", "yes", "on"}
_AVITO_SCOPE_SPLIT_RE = re.compile(r"[,\s]+")
_AVITO_DEFAULT_SCOPES: tuple[str, ...] = ("messenger:read", "messenger:write")


def env_bool(raw_value: str | None, default: bool = False) -> bool:
    if raw_value is None:
        return default
    return str(raw_value).strip().lower() in _TRUE_TOKENS


def coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in _TRUE_TOKENS
    return default


def build_avito_scope_value(raw_scope: str | None) -> str:
    ordered: list[str] = []
    seen: set[str] = set()

    source = str(raw_scope or "").strip()
    tokens = _AVITO_SCOPE_SPLIT_RE.split(source) if source else _AVITO_DEFAULT_SCOPES
    for token in tokens:
        value = token.strip()
        if not value or value in seen:
            continue
        ordered.append(value)
        seen.add(value)

    return ",".join(ordered)
