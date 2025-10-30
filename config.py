"""Lightweight configuration helpers for cross-app services."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


class _CoreSettingsProxy:
    __slots__ = ()

    def _resolve(self) -> Any:
        from app.core import settings as core_settings  # type: ignore[attr-defined]

        return core_settings

    def __getattribute__(self, name: str) -> Any:
        if name in {"_resolve"}:
            return object.__getattribute__(self, name)
        if name == "__class__":
            return self._resolve().__class__
        target = self._resolve()
        return getattr(target, name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._resolve(), name, value)

    def __delattr__(self, name: str) -> None:
        delattr(self._resolve(), name)

    def __dir__(self) -> list[str]:
        target = self._resolve()
        return sorted(set(dir(target)))

    def __repr__(self) -> str:
        return repr(self._resolve())


settings = _CoreSettingsProxy()


DEFAULT_WORKER_BASE_URL = "http://worker:8000"
DEFAULT_WA_WEB_URL = "http://waweb:9001"
DEFAULT_APP_INTERNAL_URL = "http://app:8000"


def _coerce_int(value: str | None, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(str(value).strip() or default)
    except ValueError:
        return default


def _normalize_worker_url(raw: str | None) -> str:
    if not raw:
        return DEFAULT_WORKER_BASE_URL
    cleaned = raw.strip()
    if not cleaned:
        return DEFAULT_WORKER_BASE_URL
    return cleaned.rstrip("/") or DEFAULT_WORKER_BASE_URL


def _normalize_wa_url(raw: str | None) -> str:
    if not raw:
        return DEFAULT_WA_WEB_URL
    cleaned = raw.strip()
    if not cleaned:
        return DEFAULT_WA_WEB_URL
    return cleaned.rstrip("/") or DEFAULT_WA_WEB_URL


def _normalize_internal_url(raw: str | None) -> str:
    if not raw:
        return DEFAULT_APP_INTERNAL_URL
    cleaned = raw.strip()
    if not cleaned:
        return DEFAULT_APP_INTERNAL_URL
    return cleaned.rstrip("/") or DEFAULT_APP_INTERNAL_URL


TELEGRAM_API_ID = _coerce_int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = (os.getenv("TELEGRAM_API_HASH") or "").strip()
PUBLIC_KEY = (os.getenv("PUBLIC_KEY") or "").strip()
PUBLIC_KEY_REQUIRED_FOR_PUBLIC_ENDPOINTS = True
WA_WEB_URL = _normalize_wa_url(os.getenv("WA_WEB_URL"))
APP_INTERNAL_URL = _normalize_internal_url(os.getenv("APP_INTERNAL_URL"))


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
    qr_ttl: float
    qr_poll_interval: float


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
def _parse_duration(raw: str | None, *, default: float) -> float:
    if not raw:
        return default
    cleaned = raw.strip().lower()
    if not cleaned:
        return default
    if cleaned.endswith("s"):
        cleaned = cleaned[:-1]
    try:
        return float(cleaned)
    except ValueError:
        return default


def telegram_config() -> TelegramConfig:
    api_id = TELEGRAM_API_ID
    api_hash = TELEGRAM_API_HASH
    sessions_dir = _resolve_sessions_dir(os.getenv("TG_SESSIONS_DIR"))

    device_model = os.getenv("TG_DEVICE_MODEL", "Avio tgworker").strip() or "Avio tgworker"
    system_version = os.getenv("TG_SYSTEM_VERSION", "1.0").strip() or "1.0"
    app_version = os.getenv("TG_APP_VERSION", "1.0").strip() or "1.0"
    lang = os.getenv("TG_LANG", "ru").strip() or "ru"

    qr_ttl = _parse_duration(os.getenv("TELEGRAM_QR_TTL"), default=120.0)
    qr_poll_interval = _parse_duration(
        os.getenv("TELEGRAM_QR_POLL_INTERVAL"), default=1.0
    )

    return TelegramConfig(
        api_id=api_id,
        api_hash=api_hash,
        sessions_dir=sessions_dir,
        device_model=device_model,
        system_version=system_version,
        app_version=app_version,
        lang_code=lang,
        system_lang_code=lang,
        qr_ttl=qr_ttl,
        qr_poll_interval=qr_poll_interval,
    )


@lru_cache(maxsize=1)
def tg_worker_url() -> str:
    """Return base URL for the Telegram worker service."""

    base = getattr(settings, "WORKER_BASE_URL", "") or ""
    if base:
        return _normalize_worker_url(str(base))
    return DEFAULT_WORKER_BASE_URL


CHANNEL_ENDPOINTS = {
    "telegram": f"{tg_worker_url()}/send",
    "whatsapp": f"{WA_WEB_URL}/send",
}

__all__ = [
    "TelegramConfig",
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "PUBLIC_KEY",
    "PUBLIC_KEY_REQUIRED_FOR_PUBLIC_ENDPOINTS",
    "CHANNEL_ENDPOINTS",
    "WA_WEB_URL",
    "APP_INTERNAL_URL",
    "telegram_config",
    "tg_worker_url",
    "settings",
]
