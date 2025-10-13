"""Lightweight configuration helpers for cross-app services."""
from __future__ import annotations

from dataclasses import dataclass
import os
from functools import lru_cache
from pathlib import Path


DEFAULT_TG_WORKER_URL = "http://tgworker:8085"


def _normalize_worker_url(raw: str | None) -> str:
    if not raw:
        return DEFAULT_TG_WORKER_URL
    cleaned = raw.strip()
    if not cleaned:
        return DEFAULT_TG_WORKER_URL
    return cleaned.rstrip("/") or DEFAULT_TG_WORKER_URL


TG_WORKER_URL = _normalize_worker_url(os.getenv("TG_WORKER_URL") or os.getenv("TGWORKER_URL"))


@dataclass(frozen=True, slots=True)
class TelegramConfig:
    api_id: int
    api_hash: str
    sessions_dir: Path
    device_model: str
    system_version: str
    app_version: str
    lang_code: str
    system_lang_code: str


def _resolve_sessions_dir(raw: str | None) -> Path:
    candidate = Path(raw or "/app/tg-sessions")
    try:
        candidate.mkdir(parents=True, exist_ok=True)
    except OSError:
        alt = Path("/tmp/tg-sessions")
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

    device_model = os.getenv("TG_DEVICE_MODEL", "Avio tgworker").strip() or "Avio tgworker"
    system_version = os.getenv("TG_SYSTEM_VERSION", "1.0").strip() or "1.0"
    app_version = os.getenv("TG_APP_VERSION", "1.0").strip() or "1.0"
    lang = os.getenv("TG_LANG", "ru").strip() or "ru"

    return TelegramConfig(
        api_id=api_id,
        api_hash=api_hash,
        sessions_dir=sessions_dir,
        device_model=device_model,
        system_version=system_version,
        app_version=app_version,
        lang_code=lang,
        system_lang_code=lang,
    )


@lru_cache(maxsize=1)
def tg_worker_url() -> str:
    """Return base URL for the Telegram worker service."""

    raw = os.getenv("TG_WORKER_URL") or os.getenv("TGWORKER_URL") or TG_WORKER_URL
    return _normalize_worker_url(raw)


__all__ = ["TelegramConfig", "telegram_config", "tg_worker_url"]
