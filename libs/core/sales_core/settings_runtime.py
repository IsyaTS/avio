from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable

import redis.asyncio as redis_async


@dataclass(frozen=True)
class SettingsRuntimeDeps:
    data_dir: Any
    resolve_public_key: Callable[[str], str]
    env_bool: Callable[[str, bool], bool]
    build_avito_scope_value: Callable[[str | None], str]
    default_worker_base_url: str


def create_settings(deps: SettingsRuntimeDeps) -> Any:
    class Settings:
        DEFAULT_WORKER_BASE_URL = deps.default_worker_base_url
        APP_VERSION = os.getenv("APP_VERSION", "v21.0")
        SEND = os.getenv("SEND_ENABLED", "true").lower() == "true"

        REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
        r = redis_async.from_url(REDIS_URL, decode_responses=True)

        APP_PUBLIC_URL = (os.getenv("APP_PUBLIC_URL") or "").rstrip("/")
        APP_INTERNAL_URL = os.getenv("APP_INTERNAL_URL", "http://app:8000").rstrip("/")

        WA_WEB_URL = (os.getenv("WA_WEB_URL", "http://waweb:9001") or "http://waweb:9001").rstrip("/")
        WA_PREFETCH_START = deps.env_bool("WA_PREFETCH_START", True)
        try:
            WA_QR_FETCH_ATTEMPTS = int(os.getenv("WA_QR_FETCH_ATTEMPTS", "3"))
        except ValueError:
            WA_QR_FETCH_ATTEMPTS = 3
        if WA_QR_FETCH_ATTEMPTS < 1:
            WA_QR_FETCH_ATTEMPTS = 1
        try:
            WA_QR_FETCH_RETRY_DELAY = float(os.getenv("WA_QR_FETCH_RETRY_DELAY", "0.7"))
        except ValueError:
            WA_QR_FETCH_RETRY_DELAY = 0.7
        if WA_QR_FETCH_RETRY_DELAY < 0:
            WA_QR_FETCH_RETRY_DELAY = 0.0
        BAILEYS_URL = (
            os.getenv("BAILEYS_URL") or os.getenv("WABAILEYS_URL") or "http://wabaileys:9002"
        ).rstrip("/")
        MAX_PERSONAL_WORKER_URL = (
            os.getenv("MAX_PERSONAL_WORKER_URL")
            or os.getenv("MAXWORKER_URL")
            or "http://maxworker:9010"
        ).rstrip("/")
        _provider_default = (os.getenv("WHATSAPP_PROVIDER_DEFAULT") or "waweb").strip().lower()
        if _provider_default not in {"waweb", "baileys"}:
            _provider_default = "waweb"
        WHATSAPP_PROVIDER_DEFAULT = _provider_default

        _PDF_TABLES_ENGINE_RAW = (os.getenv("PDF_TABLES_ENGINE") or "plumber").strip().lower()
        PDF_TABLES_ENGINE = (
            _PDF_TABLES_ENGINE_RAW if _PDF_TABLES_ENGINE_RAW in {"plumber", "camelot"} else "plumber"
        )
        try:
            PDF_RENDER_DPI = int(os.getenv("PDF_RENDER_DPI", "220"))
        except ValueError:
            PDF_RENDER_DPI = 220

        ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()
        _WORKER_BASE_RAW = (
            os.getenv("WORKER_BASE_URL")
            or os.getenv("TGWORKER_BASE_URL")
            or os.getenv("TG_WORKER_URL")
            or os.getenv("TGWORKER_URL")
            or deps.default_worker_base_url
        )
        WORKER_BASE_URL = str(_WORKER_BASE_RAW).strip().rstrip("/") or deps.default_worker_base_url
        TGWORKER_BASE_URL = WORKER_BASE_URL
        PUBLIC_KEY = deps.resolve_public_key(ADMIN_TOKEN)
        WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET", "") or "").strip()

        AVITO_CLIENT_ID = (os.getenv("AVITO_CLIENT_ID") or "1OuyOIqOV6Pi6ewYI3mi").strip()
        AVITO_CLIENT_SECRET = (
            os.getenv("AVITO_CLIENT_SECRET") or "t-JCi261jbPfuvx1d5x0EP8Y9wKxyvDBwKU8sdTe"
        ).strip()
        AVITO_REDIRECT_URL = (
            os.getenv("AVITO_REDIRECT_URL") or "https://hub.avio.website/v1/oauth/avito/callback"
        ).strip()
        AVITO_AUTH_URL = (os.getenv("AVITO_AUTH_URL") or "https://avito.ru/oauth").strip()
        AVITO_TOKEN_URL = (os.getenv("AVITO_TOKEN_URL") or "https://api.avito.ru/token/").strip()
        AVITO_SCOPE = deps.build_avito_scope_value(os.getenv("AVITO_SCOPE"))
        try:
            AVITO_TIMEOUT = float(os.getenv("AVITO_TIMEOUT", "10"))
        except ValueError:
            AVITO_TIMEOUT = 10.0

        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
        OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        try:
            OPENAI_TIMEOUT_SECONDS = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "4"))
        except ValueError:
            OPENAI_TIMEOUT_SECONDS = 4.0
        try:
            OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.3"))
        except ValueError:
            OPENAI_TEMPERATURE = 0.3
        if OPENAI_TEMPERATURE < 0:
            OPENAI_TEMPERATURE = 0.0
        if OPENAI_TEMPERATURE > 2:
            OPENAI_TEMPERATURE = 2.0
        try:
            LOWERCASE_OPENING_CHANCE = float(os.getenv("LOWERCASE_OPENING_CHANCE", "0.3"))
        except ValueError:
            LOWERCASE_OPENING_CHANCE = 0.3
        if LOWERCASE_OPENING_CHANCE < 0:
            LOWERCASE_OPENING_CHANCE = 0.0
        if LOWERCASE_OPENING_CHANCE > 1:
            LOWERCASE_OPENING_CHANCE = 1.0

        AGENT_NAME = os.getenv("AGENT_NAME", "Акакий")
        BRAND_NAME = os.getenv("BRAND_NAME", "Гермес")
        WHATSAPP_LINK = os.getenv("WHATSAPP_LINK", "https://wa.me/7XXXXXXXXXX")
        CITY = os.getenv("CITY", "Уфа")

        PERSONA_MD = os.getenv("PERSONA_MD") or str(deps.data_dir / "persona.md")

    return Settings()
