from __future__ import annotations

from typing import Any


def coerce_int(
    value: Any,
    *,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int | None:
    """Convert arbitrary input to int and apply optional bounds."""

    if value is None:
        return None
    try:
        parsed = int(str(value).strip())
    except Exception:
        return None
    if min_value is not None and parsed < min_value:
        return None
    if max_value is not None and parsed > max_value:
        return None
    return parsed
