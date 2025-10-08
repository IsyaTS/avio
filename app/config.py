"""Lightweight configuration helpers for cross-app services."""

from __future__ import annotations

from dataclasses import dataclass
import os
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TelegramConfig:
    api_id: int
    api_hash: str
    sessions_dir: Path


def _resolve_sessions_dir(raw: str | None) -> Path:
    candidate = Path(raw or "/app/tg_sessions")
    try:
        candidate.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Fall back to a writable directory inside the container.
        alt = Path("/tmp/tg_sessions")
        alt.mkdir(parents=True, exist_ok=True)
        return alt
    return candidate


@lru_cache(maxsize=1)
def telegram_config() -> TelegramConfig:
    raw_id = os.getenv("TELEGRAM_API_ID", "0").strip() or "0"
    try:
        api_id = int(raw_id)
    except ValueError:
        api_id = 0

    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
    sessions_dir = _resolve_sessions_dir(os.getenv("TG_SESSIONS_DIR"))

    return TelegramConfig(api_id=api_id, api_hash=api_hash, sessions_dir=sessions_dir)


__all__ = ["TelegramConfig", "telegram_config"]
