from __future__ import annotations
import os
import json
import re
import csv
import asyncio
import pathlib
import time
import random
import hashlib
import logging
from typing import List, Dict, Any, Optional, Tuple, Mapping, Sequence
from dataclasses import dataclass, field
import urllib.request
import urllib.error
import yaml

# Redis (асинхронный клиент можно использовать при необходимости)
import redis.asyncio as redis_async
import redis as redis_sync
from redis import exceptions as redis_ex

# OpenAI как опциональная зависимость
try:
    import openai  # type: ignore
except Exception:  # библиотека может быть не установлена
    openai = None  # type: ignore

try:  # pragma: no cover - зависимость optional
    from openai import APITimeoutError  # type: ignore
except Exception:  # pragma: no cover

    class APITimeoutError(Exception):  # type: ignore
        """Fallback timeout error when OpenAI SDK недоступен."""

        pass


from ..brain import planner, quality

try:
    from ..catalog import retriever as catalog_retriever  # type: ignore
except Exception:  # pragma: no cover
    catalog_retriever = None
try:
    from ..training import retriever as training_retriever  # type: ignore
except Exception:  # pragma: no cover
    training_retriever = None

logger = logging.getLogger(__name__)

try:
    from openpyxl import load_workbook  # type: ignore
except Exception:  # опциональная зависимость для Excel
    load_workbook = None  # type: ignore


BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
ROOT_DIR = BASE_DIR.parent
DATA_DIR = pathlib.Path(os.getenv("APP_DATA_DIR") or (BASE_DIR / "data"))
TENANTS_CONFIG_PATH = pathlib.Path(
    os.getenv("TENANTS_CONFIG_PATH") or (BASE_DIR.parent / "config" / "tenants.yml")
)


def _resolve_public_key(admin_token: str) -> str:
    """Return PUBLIC_KEY keeping ADMIN_TOKEN intact when empty."""

    _ = admin_token  # kept for backward compatibility with older imports
    raw_value = os.getenv("PUBLIC_KEY")
    if raw_value is None:
        return ""

    normalized = str(raw_value).strip()
    return normalized


def _resolve_tenants_dir() -> pathlib.Path:
    env_value = os.getenv("TENANTS_DIR")
    if env_value:
        return pathlib.Path(env_value)

    repo_data = ROOT_DIR.parent / "data" / "tenants"
    try:
        repo_data.mkdir(parents=True, exist_ok=True)
        return repo_data
    except OSError:
        pass

    data_tenants = ROOT_DIR / "data" / "tenants"
    try:
        data_tenants.mkdir(parents=True, exist_ok=True)
        return data_tenants
    except OSError:
        pass

    app_tenants = ROOT_DIR / "app" / "tenants"
    if app_tenants.exists():
        return app_tenants

    fallback = DATA_DIR / "tenants"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


TENANTS_DIR = _resolve_tenants_dir()
TENANT_CONFIG_DIR = ROOT_DIR / "config" / "tenants"

# Lightweight in-memory caches (mtime-based invalidation)
_TENANT_CONFIG_CACHE: Dict[int, Tuple[float, float, dict]] = {}
_TENANT_PERSONA_CACHE: Dict[int, Tuple[float, str]] = {}
_PERSONA_RULES_CACHE: Dict[str, "PersonaCompiledRules"] = {}
# Key: (tenant or None, tuple of (path, mtime, size)) -> parsed, normalized items
_CATALOG_CACHE: Dict[
    Tuple[Optional[int], Tuple[Tuple[str, float, int], ...]], List[Dict[str, Any]]
] = {}
_TENANTS_CONFIG_CACHE: Dict[int, Dict[str, Any]] = {}

CTA_COOLDOWN_SECONDS = float(os.getenv("CTA_COOLDOWN_SECONDS", "180"))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default


# Precompiled regexes reused across hot paths
_FIELD_CLEAN_RE = re.compile(r"[^0-9a-zA-Zа-яА-ЯёЁ]+")
_PERSONA_HINTS_KEY_RE = re.compile(
    r"^(greeting|приветств(?:ие|уй)|cta|призыв|closing|завершение|tone|тон|language|язык|max(?:imum)?\s*(?:questions|вопросов|уточнений))\s*[:\-]\s*(.+)$",
    re.IGNORECASE,
)


_DEFAULT_WORKER_BASE_URL = "http://worker:8000"


class Settings:
    DEFAULT_WORKER_BASE_URL = _DEFAULT_WORKER_BASE_URL
    APP_VERSION = os.getenv("APP_VERSION", "v21.0")
    SEND = os.getenv("SEND_ENABLED", "true").lower() == "true"

    REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
    r = redis_async.from_url(REDIS_URL, decode_responses=True)

    # Публичный URL API (для вебхука waweb)
    APP_PUBLIC_URL = (os.getenv("APP_PUBLIC_URL") or "").rstrip("/")
    APP_INTERNAL_URL = os.getenv("APP_INTERNAL_URL", "http://app:8000").rstrip("/")

    # waweb
    WA_WEB_URL = (os.getenv("WA_WEB_URL", "http://waweb:9001") or "http://waweb:9001").rstrip("/")
    WA_PREFETCH_START = _env_bool("WA_PREFETCH_START", True)
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

    # Админка
    ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()
    _WORKER_BASE_RAW = (
        os.getenv("WORKER_BASE_URL")
        or os.getenv("TGWORKER_BASE_URL")
        or os.getenv("TG_WORKER_URL")
        or os.getenv("TGWORKER_URL")
        or _DEFAULT_WORKER_BASE_URL
    )
    WORKER_BASE_URL = str(_WORKER_BASE_RAW).strip().rstrip("/") or _DEFAULT_WORKER_BASE_URL
    TGWORKER_BASE_URL = WORKER_BASE_URL
    PUBLIC_KEY = _resolve_public_key(ADMIN_TOKEN)
    WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET", "") or "").strip()

    # Avito OAuth
    AVITO_CLIENT_ID = (os.getenv("AVITO_CLIENT_ID") or "1OuyOIqOV6Pi6ewYI3mi").strip()
    AVITO_CLIENT_SECRET = (
        os.getenv("AVITO_CLIENT_SECRET") or "t-JCi261jbPfuvx1d5x0EP8Y9wKxyvDBwKU8sdTe"
    ).strip()
    AVITO_REDIRECT_URL = (
        os.getenv("AVITO_REDIRECT_URL") or "https://hub.avio.website/v1/oauth/avito/callback"
    ).strip()
    AVITO_AUTH_URL = (os.getenv("AVITO_AUTH_URL") or "https://www.avito.ru/oauth").strip()
    AVITO_TOKEN_URL = (os.getenv("AVITO_TOKEN_URL") or "https://api.avito.ru/token/").strip()
    AVITO_SCOPE = (os.getenv("AVITO_SCOPE") or "messenger:read,messenger:write,user:read").strip()
    try:
        AVITO_TIMEOUT = float(os.getenv("AVITO_TIMEOUT", "10"))
    except ValueError:
        AVITO_TIMEOUT = 10.0

    # LLM
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

    # Бизнес-поля
    AGENT_NAME = os.getenv("AGENT_NAME", "Акакий")
    BRAND_NAME = os.getenv("BRAND_NAME", "Гермес")
    WHATSAPP_LINK = os.getenv("WHATSAPP_LINK", "https://wa.me/7XXXXXXXXXX")
    CITY = os.getenv("CITY", "Уфа")

    # Персоны/промпты с диска
    PERSONA_MD = os.getenv("PERSONA_MD") or str(DATA_DIR / "persona.md")


settings = Settings()


_openai_client: Any | None = None
_openai_client_key: str | None = None

_sync_redis_client: redis_sync.Redis | None = None


def tenant_config(tenant: int) -> Dict[str, Any]:
    try:
        tenant_key = int(tenant)
    except Exception:
        return {}
    raw = _TENANTS_CONFIG_CACHE.get(tenant_key) or {}
    return dict(raw)


def tenant_waweb_url(tenant: int | None) -> str:
    """
    Return waweb base URL for a tenant. Falls back to a generated host name or default settings.
    """

    if tenant is None:
        return settings.WA_WEB_URL

    try:
        tenant_key = int(tenant)
    except Exception:
        return settings.WA_WEB_URL

    cfg = tenant_config(tenant_key)
    waweb_cfg = cfg.get("waweb") if isinstance(cfg.get("waweb"), dict) else {}

    url_value = ""
    if waweb_cfg:
        url_value = str(waweb_cfg.get("url") or "").strip()
        if not url_value:
            host_value = str(waweb_cfg.get("host") or "").strip()
            port_value = waweb_cfg.get("port")
            if host_value:
                if port_value:
                    try:
                        port_int = int(str(port_value).strip())
                    except Exception:
                        port_int = None
                    if port_int:
                        url_value = f"http://{host_value}:{port_int}"
                if not url_value:
                    url_value = f"http://{host_value}"

    if url_value:
        return url_value.rstrip("/")

    # Default naming convention for managed waweb containers
    default_host = f"waweb-{tenant_key}"
    return f"http://{default_host}:9001"


def tenant_whatsapp_provider(tenant: int | None) -> str:
    """
    Resolve the WhatsApp transport provider for a tenant (\"waweb\" or \"baileys\").
    """

    default_provider = getattr(settings, "WHATSAPP_PROVIDER_DEFAULT", "waweb")
    if tenant is None:
        return default_provider
    try:
        tenant_key = int(tenant)
    except Exception:
        return default_provider

    cfg = tenant_config(tenant_key)
    whatsapp_cfg = cfg.get("whatsapp") if isinstance(cfg, Mapping) else {}
    if not isinstance(whatsapp_cfg, Mapping):
        whatsapp_cfg = {}
    provider = str(whatsapp_cfg.get("provider") or "").strip().lower()
    if provider in {"waweb", "baileys"}:
        return provider

    try:
        cfg = read_tenant_config(tenant_key)
    except Exception:
        cfg = {}
    whatsapp_cfg = cfg.get("whatsapp") if isinstance(cfg, Mapping) else {}
    if not isinstance(whatsapp_cfg, Mapping):
        whatsapp_cfg = {}
    provider = str(whatsapp_cfg.get("provider") or "").strip().lower()
    if provider in {"waweb", "baileys"}:
        return provider

    return default_provider


def _resolve_chat_completion_callable(obj: Any):
    chat = getattr(obj, "chat", None)
    if chat is None:
        return None
    completions = getattr(chat, "completions", None)
    if completions is None:
        return None
    create_fn = getattr(completions, "create", None)
    if not callable(create_fn):
        return None
    return create_fn


def _get_openai_client() -> Any | None:
    """Return an OpenAI client compatible with chat.completions.create."""

    global _openai_client, _openai_client_key

    if not (openai and settings.OPENAI_API_KEY):
        return None

    if hasattr(openai, "OpenAI"):
        if _openai_client is None or _openai_client_key != settings.OPENAI_API_KEY:
            try:
                _openai_client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)  # type: ignore[attr-defined]
            except TypeError:
                _openai_client = openai.OpenAI()  # type: ignore[attr-defined]
            except Exception as exc:  # pragma: no cover - сетевые/валидационные ошибки
                logger.warning("openai client init failed: %s", exc)
                _openai_client = None
                return None
            _openai_client_key = settings.OPENAI_API_KEY

        if _openai_client is None:
            return None

        if _resolve_chat_completion_callable(_openai_client) is None:
            logger.warning("openai client missing chat.completions.create")
            return None

        return _openai_client

    if not hasattr(openai, "OpenAI"):
        _openai_client = None
        _openai_client_key = None

    try:
        setattr(openai, "api_key", settings.OPENAI_API_KEY)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - старые клиенты без api_key
        logger.warning("failed to set openai api_key: %s", exc)
        return None

    if _resolve_chat_completion_callable(openai) is None:
        logger.warning("openai module missing chat.completions.create")
        return None

    return openai


def _redis_sync_client() -> redis_sync.Redis:
    global _sync_redis_client
    if _sync_redis_client is None:
        _sync_redis_client = redis_sync.from_url(settings.REDIS_URL, decode_responses=True)
    return _sync_redis_client


def _with_sync_redis(func, default=None):
    global _sync_redis_client
    for _ in range(2):
        try:
            return func(_redis_sync_client())
        except redis_ex.ConnectionError:
            _sync_redis_client = None
        except redis_ex.RedisError:
            return default
    return default


# Куки и ключи
ADMIN_COOKIE = "admin_token"
TENANT_PUBKEYS_HASH = "tenant_pubkeys"


# --------------------------- состояние диалогов -----------------------------
STATE_KEY_PREFIX = "sales_state"
STATE_TTL_SECONDS = int(os.getenv("STATE_TTL_SECONDS", str(8 * 3600)))
_STATE_CACHE: Dict[str, "SalesState"] = {}
_STATE_STORE_UNAVAILABLE = object()


def _reset_state_store() -> None:
    def _clear(client: redis_sync.Redis) -> None:
        pattern = f"{STATE_KEY_PREFIX}:*"
        cursor = 0
        while True:
            cursor, keys = client.scan(cursor=cursor, match=pattern, count=500)
            if keys:
                client.delete(*keys)
            if cursor == 0:
                break

    _with_sync_redis(_clear, None)
    _STATE_CACHE.clear()


if _env_bool("RESET_SALES_STATE_ON_START", False):
    _reset_state_store()


def _state_key(tenant: int | None, contact_id: int | None) -> str:
    tenant_id = int(tenant or 0)
    contact = int(contact_id or 0)
    return f"{STATE_KEY_PREFIX}:{tenant_id}:{contact}"


def _state_store_read(key: str) -> Optional[dict]:
    try:
        raw = _with_sync_redis(lambda client: client.get(key), _STATE_STORE_UNAVAILABLE)
        if raw is _STATE_STORE_UNAVAILABLE:
            cached = _STATE_CACHE.get(key)
            if cached:
                return cached.to_dict()
            return None
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        cached = _STATE_CACHE.get(key)
        if cached:
            return cached.to_dict()
        return None


def _state_store_write(key: str, payload: dict) -> None:
    _with_sync_redis(
        lambda client: client.setex(
            key, STATE_TTL_SECONDS, json.dumps(payload, ensure_ascii=False)
        ),
        None,
    )


@dataclass
class SalesState:
    tenant: int
    contact_id: int
    channel: str = "whatsapp"
    needs: Dict[str, Any] = field(default_factory=dict)
    spin: Dict[str, str] = field(
        default_factory=lambda: {stage: "pending" for stage in ("s", "p", "i", "n")}
    )
    bant: Dict[str, Any] = field(default_factory=dict)
    asked_questions: List[str] = field(default_factory=list)
    asked_question_fingerprints: List[str] = field(default_factory=list)
    challenger_cursor: int = 0
    social_proof_cursor: int = 0
    scarcity_cursor: int = 0
    reciprocity_cursor: int = 0
    history: List[Dict[str, str]] = field(default_factory=list)
    last_items: List[Dict[str, Any]] = field(default_factory=list)
    last_bot_reply: str = ""
    last_user_text: str = ""
    last_updated_ts: float = field(default_factory=lambda: time.time())
    conversion_score: float = 0.0
    catalog_sent: bool = False
    catalog_sent_at: float = 0.0
    catalog_delivery_mode: str = ""
    last_plan: Dict[str, Any] = field(default_factory=dict)
    profile: Dict[str, Any] = field(default_factory=dict)
    sentiment_score: float = 0.0
    user_message_count: int = 0
    last_question_text: str = ""
    cta_last_text: str = ""
    cta_last_sent_ts: float = 0.0
    known_slots: Dict[str, str] = field(default_factory=dict)
    pending_slot: str = ""
    recent_fact_fingerprints: List[str] = field(default_factory=list)
    facts: Dict[str, str] = field(default_factory=dict)
    pending_fact_key: str = ""

    def to_dict(self) -> dict:
        return {
            "tenant": self.tenant,
            "contact_id": self.contact_id,
            "channel": self.channel,
            "needs": self.needs,
            "spin": self.spin,
            "bant": self.bant,
            "asked_questions": self.asked_questions,
            "asked_question_fingerprints": self.asked_question_fingerprints[-32:],
            "challenger_cursor": self.challenger_cursor,
            "social_proof_cursor": self.social_proof_cursor,
            "scarcity_cursor": self.scarcity_cursor,
            "reciprocity_cursor": self.reciprocity_cursor,
            "history": self.history[-20:],
            "last_items": self.last_items[-8:],
            "last_bot_reply": self.last_bot_reply,
            "last_user_text": self.last_user_text,
            "last_updated_ts": self.last_updated_ts,
            "conversion_score": self.conversion_score,
            "catalog_sent": self.catalog_sent,
            "catalog_sent_at": self.catalog_sent_at,
            "catalog_delivery_mode": self.catalog_delivery_mode,
            "last_plan": self.last_plan,
            "profile": self.profile,
            "sentiment_score": self.sentiment_score,
            "user_message_count": self.user_message_count,
            "last_question_text": self.last_question_text,
            "cta_last_text": self.cta_last_text,
            "cta_last_sent_ts": self.cta_last_sent_ts,
            "known_slots": self.known_slots,
            "pending_slot": self.pending_slot,
            "recent_fact_fingerprints": self.recent_fact_fingerprints[-64:],
            "facts": self.facts,
            "pending_fact_key": self.pending_fact_key,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "SalesState":
        payload = payload or {}
        tenant = int(payload.get("tenant", 0))
        contact_id = int(payload.get("contact_id", 0))
        obj = cls(tenant=tenant, contact_id=contact_id)
        obj.channel = payload.get("channel", obj.channel)
        obj.needs = payload.get("needs", {}) or {}
        obj.spin = payload.get("spin", obj.spin) or {
            stage: "pending" for stage in ("s", "p", "i", "n")
        }
        obj.bant = payload.get("bant", {}) or {}
        obj.asked_questions = payload.get("asked_questions", []) or []
        obj.asked_question_fingerprints = payload.get("asked_question_fingerprints", []) or []
        obj.challenger_cursor = int(payload.get("challenger_cursor", 0))
        obj.social_proof_cursor = int(payload.get("social_proof_cursor", 0))
        obj.scarcity_cursor = int(payload.get("scarcity_cursor", 0))
        obj.reciprocity_cursor = int(payload.get("reciprocity_cursor", 0))
        obj.history = payload.get("history", []) or []
        obj.last_items = payload.get("last_items", []) or []
        obj.last_bot_reply = payload.get("last_bot_reply", "") or ""
        obj.last_user_text = payload.get("last_user_text", "") or ""
        obj.last_updated_ts = float(payload.get("last_updated_ts", time.time()))
        obj.conversion_score = float(payload.get("conversion_score", 0.0))
        obj.catalog_sent = bool(payload.get("catalog_sent", False))
        obj.catalog_sent_at = float(payload.get("catalog_sent_at", 0.0) or 0.0)
        obj.catalog_delivery_mode = payload.get("catalog_delivery_mode", "") or ""
        obj.last_plan = payload.get("last_plan", {}) or {}
        obj.profile = payload.get("profile", {}) or {}
        try:
            obj.sentiment_score = float(payload.get("sentiment_score", 0.0))
        except Exception:
            obj.sentiment_score = 0.0
        obj.user_message_count = int(payload.get("user_message_count", 0))
        obj.last_question_text = payload.get("last_question_text", "") or ""
        obj.cta_last_text = payload.get("cta_last_text", "") or ""
        try:
            obj.cta_last_sent_ts = float(payload.get("cta_last_sent_ts", 0.0) or 0.0)
        except Exception:
            obj.cta_last_sent_ts = 0.0
        obj.known_slots = payload.get("known_slots", {}) or {}
        if not isinstance(obj.known_slots, dict):
            obj.known_slots = {}
        obj.pending_slot = payload.get("pending_slot", "") or ""
        obj.recent_fact_fingerprints = payload.get("recent_fact_fingerprints", []) or []
        if not isinstance(obj.recent_fact_fingerprints, list):
            obj.recent_fact_fingerprints = []
        obj.facts = payload.get("facts", {}) or {}
        if not isinstance(obj.facts, dict):
            obj.facts = {}
        obj.pending_fact_key = str(payload.get("pending_fact_key", "") or "").strip()
        return obj

    def append_history(self, role: str, content: str) -> None:
        if not content:
            return
        content = content.strip()
        if not content:
            return
        if (
            self.history
            and self.history[-1].get("role") == role
            and self.history[-1].get("content") == content
        ):
            return
        self.history.append({"role": role, "content": content})
        if len(self.history) > 24:
            self.history = self.history[-24:]

    def mark_spin_stage(self, stage: str, status: str) -> None:
        if stage not in self.spin:
            self.spin[stage] = status
        else:
            order = {"pending": 0, "asked": 1, "covered": 2}
            if order.get(status, 0) >= order.get(self.spin.get(stage, "pending"), 0):
                self.spin[stage] = status


@dataclass
class PersonaStepRule:
    fact_key: str
    source_line: str
    question: str = ""


@dataclass
class PersonaConditionalRule:
    source_line: str
    condition_text: str
    action_text: str
    fact_key: str = ""
    expected_tokens: List[str] = field(default_factory=list)


@dataclass
class PersonaDeliveryRule:
    source_line: str
    channel_scope: List[str] = field(default_factory=list)
    condition_text: str = ""
    expected_tokens: List[str] = field(default_factory=list)
    wants_handle: bool = False
    wants_phone: bool = False
    wants_link: bool = False
    min_assistant_gap: int = 2


@dataclass
class PersonaCompiledRules:
    steps: List[PersonaStepRule] = field(default_factory=list)
    conditionals: List[PersonaConditionalRule] = field(default_factory=list)
    delivery_rules: List[PersonaDeliveryRule] = field(default_factory=list)
    contact_artifacts: List[str] = field(default_factory=list)


def _remember_question_state(state: SalesState, question: str) -> None:
    clean = (question or "").strip()
    if not clean:
        return
    if clean not in state.asked_questions:
        state.asked_questions.append(clean)
        if len(state.asked_questions) > 24:
            state.asked_questions = state.asked_questions[-24:]
    fingerprint = quality.question_fingerprint(clean)
    if fingerprint:
        if fingerprint not in (state.asked_question_fingerprints or []):
            state.asked_question_fingerprints.append(fingerprint)
            if len(state.asked_question_fingerprints) > 32:
                state.asked_question_fingerprints = state.asked_question_fingerprints[-32:]
    state.last_question_text = clean


def _remember_cta_state(state: SalesState, cta_text: str) -> None:
    clean = (cta_text or "").strip()
    if not clean:
        return
    state.cta_last_text = clean
    state.cta_last_sent_ts = time.time()


def _cta_allowed(state: SalesState, channel_name: str | None) -> bool:
    if not isinstance(state, SalesState):
        return True
    if (state.user_message_count or 0) <= 1:
        return False
    if state.sentiment_score <= -1.2:
        return False
    now_ts = time.time()
    if state.cta_last_sent_ts and (now_ts - state.cta_last_sent_ts) < CTA_COOLDOWN_SECONDS:
        return False
    if channel_name and channel_name.lower() == "avito":
        return True
    return True


def _max_questions_limit(persona_hints: Optional[PersonaHints], default: int = 1) -> int:
    if persona_hints and persona_hints.max_questions is not None:
        try:
            return max(0, int(persona_hints.max_questions))
        except Exception:
            return max(0, default)
    return max(0, default)


_ROBOTIC_BANNED_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bваш запрос принят\b", re.IGNORECASE),
    re.compile(r"\bпринял(?:\s+ваш)?\s+запрос\b", re.IGNORECASE),
    re.compile(r"\bблагодар(?:ю|им)\s+за\s+обращение\b", re.IGNORECASE),
    re.compile(r"\bв рамках вашего запроса\b", re.IGNORECASE),
)

_GRATITUDE_RE = re.compile(r"\b(спасибо|благодарю|благодарим)\b", re.IGNORECASE)
_GREETING_PREFIX_RE = re.compile(
    r"^\s*(здравствуйте|добрый день|добрый вечер|привет)\b[!,. ]*", re.IGNORECASE
)
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
_GRATITUDE_PHRASE_RE = re.compile(
    r"\b(спасибо(?:\s+за\s+обращение)?|благодарю(?:\s+за\s+обращение)?|благодарим(?:\s+за\s+обращение)?)\b[,.! ]*",
    re.IGNORECASE,
)
_OPENING_HEY_RE = re.compile(r"^\s*привет[!,. ]*", re.IGNORECASE)
_QUESTION_THIS_OR_RE = re.compile(r"^\s*это\s+(.+?)\?\s*$", re.IGNORECASE)
_OPENING_WORD_RE = re.compile(r"^\s*([A-Za-zА-Яа-яЁё]+)")
_LOWERCASE_OPENING_BLOCKED = {
    "здравствуйте",
    "привет",
    "добрый",
    "уважаемый",
    "уважаемая",
    "ваалейкум",
}
_ENTITY_ACK_PREFIX_RE = re.compile(
    r"^\s*([a-zа-яё0-9][a-zа-яё0-9\-\s]{1,42})\s*[,—-]\s*(понял|принял|услышал|принято)\b[,.! ]*",
    re.IGNORECASE,
)
_NEIGHBOR_CLAIM_RE = re.compile(
    r"\b(соседн(?:ем|ий|яя)\s+(?:доме|подъезде)|ставили\s+рядом|недавно\s+ставили)\b",
    re.IGNORECASE,
)
_STOP_INTENT_RE = re.compile(
    r"\b(стоп|останов(?:и|ите|ка)|не\s+пиш(?:и|ите)|не\s+беспокой|отпис(?:ка|ать|ыва)|больше\s+не\s+пиш)\b",
    re.IGNORECASE,
)
_URGENT_TODAY_RE = re.compile(
    r"\b(срочн\w*|сегодня|как\s+можно\s+быстрее|прямо\s+сейчас)\b",
    re.IGNORECASE,
)
_ETA_INTENT_RE = re.compile(
    r"(?iu)\b(через\s+сколько|когда\s+приед|когда\s+можно\s+приех|когда\s+приехать|"
    r"сколько\s+ждать|во\s+сколько|к\s+какому\s+времени|можете\s+завтра|"
    r"завтра\s+с\s+утра|утром|вечером)\b"
)
_INSTRUCTION_LEAK_LINE_RE = re.compile(
    r"(?im)^\s*(после\s+приветствия\s+последовательно\s+уточни:?\s*|"
    r"диалог-скрипт\s*(?:\(.*?\))?:?\s*|"
    r"шаблон\s+реплик:?\s*|"
    r"правила\s+ответа:?\s*)$"
)
_INSTRUCTION_LIST_LINE_RE = re.compile(
    r"(?iu)^\s*\d+[)\.]\s*(город|адрес|тип\s+объекта|тип\s+помещения|квартир|частн(?:ый|ого)\s+дом|"
    r"модель|вариант|каталог|бюджет|срок|контакт|телефон|размер|про[её]м|замер)\b.*$"
)
_SHORTLIST_LEAK_RE = re.compile(r"(?iu)\bсобрал\s+коротк\w*\s+шорт[-\s]?лист\b")
_ORDER_INTENT_RE = re.compile(
    r"(?iu)\b(оформ(ить|им|ление)|заказ(ать|а|у|ом)?|купить|беру|готов\s+оформ)\b"
)
_CATALOG_REQUEST_RE = re.compile(
    r"(?iu)\b(каталог|ассортимент|прайс|прайс[-\s]?лист|модел[ьи]|ссылк[ау]|photo|фото)\b"
)
_OFFTOPIC_SMALLTALK_RE = re.compile(
    r"(?iu)\b(как\s+дела|кто\s+ты|погода|анекдот|шутк|гороскоп|курс\s+доллара|футбол|музыка)\b"
)
_QUESTION_CUE_RE = re.compile(
    r"(?iu)^\s*(подскаж(?:и|ите)|уточн(?:и|ите)|скаж(?:и|ите)|"
    r"в\s+каком|в\s+какие|какой|какая|какие|где|когда|сколько|"
    r"нужн(?:а|ы|о)?\s+ли|выбираете|пришл(?:и|ите))\b"
)
_LOW_SIGNAL_USER_REPLY_RE = re.compile(
    r"(?iu)^\s*(да|ага|ок|okay|окей|давай|показывай|скидывай|погнали|го|угу|угу+)\s*$"
)
_LOW_SIGNAL_CONTEXT_RE = re.compile(
    r"(?iu)\b("
    r"не\s+могу\s+откры(ть|ться)|"
    r"не\s+открыва(ется|еться|лось)|"
    r"долго\s+груз(ит|ится)|"
    r"пока\s+не\s+могу|"
    r"сейчас\s+не\s+могу|"
    r"позже\s+скину|"
    r"позже\s+пришлю"
    r")\b"
)
_CATALOG_UNAVAILABLE_RE = re.compile(
    r"(?iu)\b("
    r"каталог\s+(?:еще|ещё)?\s*груз|"
    r"каталог\s+не\s+груз|"
    r"каталог\s+не\s+открыва|"
    r"не\s+могу\s+откры(ть|ться)\s+каталог|"
    r"не\s+могу\s+пока\s+посмотре(ть|ть\s+каталог)|"
    r"не\s+могу\s+посмотре(ть|ть\s+каталог)|"
    r"не\s+вижу\s+каталог|"
    r"пока\s+не\s+могу\s+посмотреть\s+каталог"
    r")\b"
)
_WHY_QUESTION_RE = re.compile(
    r"(?iu)\b(зачем|почему|для\s+чего|для\s+чего\s+это|почему\s+нужен|почему\s+нужна|"
    r"зачем\s+нужен|зачем\s+нужна|зачем\s+вам)\b"
)
_REPAIR_TURN_RE = re.compile(
    r"(?iu)^\s*(чего|что\??|в\s+смысле|не\s+понял(?:а)?|не\s+поняли|"
    r"не\s+разобрал(?:а)?|не\s+ясно|не\s+очень\s+понятно|"
    r"я\s+же\s+говорил(?:а)?(?:\s+уже)?|уже\s+говорил(?:а)?|"
    r"я\s+же\s+писал(?:а)?|уже\s+писал(?:а)?|"
    r"вы\s+уже\s+спрашивали|опять\s+спрашиваете|зачем\s+повторяете)\s*$"
)
_NOISE_NEED_RE = re.compile(r"(?iu)\b(тих\w*|шумк\w*|шумоизоляц\w*|без\s+шума)\b")
_INSULATION_NEED_RE = re.compile(
    r"(?iu)\b(дует|сквозняк|промерз\w*|продува\w*|холод\w*|утеплен\w*|теплоизоляц\w*|"
    r"терморазрыв\w*|термо\s*разрыв\w*|термодвер\w*)\b"
)
_OBJECT_TYPE_HINT_RE = re.compile(
    r"(?iu)\b(квартир\w*|дом\w*|помещен\w*|офис\w*|склад\w*|коммерч\w*|студи\w*|комнат\w*|этаж\w*)\b"
)
_HUMAN_STYLE_FEW_SHOT = (
    "Формат живого ответа (пример):\n"
    "Клиент: «Здравствуйте»\n"
    "Менеджер: «Здравствуйте. В каком городе планируете установку?»\n\n"
    "Клиент: «Уфа»\n"
    "Менеджер: «Уточню: для квартиры или частного дома?»\n\n"
    "Клиент: «Пока нет фото и размеров»\n"
    "Менеджер: «Без проблем, можно начать без фото. Дам предварительный вариант, а размеры уточним позже.»\n\n"
    "Клиент: «Зачем две модели?»\n"
    "Менеджер: «Тогда дам один лучший вариант под ваш запрос.»\n\n"
    "Антишаблоны (не использовать): «Спасибо, понял», «Ваш запрос принят», "
    "«Если что-то ещё интересует — спрашивайте», «Понял», «Поняла»."
)


def _apply_conversational_phrasing(
    text: str,
    *,
    persona_hints: PersonaHints | None = None,
) -> str:
    out = (text or "").strip()
    if not out:
        return out

    # Structural cleanup only: no phrase-level hardcoded rewrites.
    # Prevent technical placeholders like event_format leaking to user text.
    out = re.sub(
        r"\b([a-z]+_[a-z0-9_]+)\b", lambda m: m.group(1).replace("_", " "), out, flags=re.IGNORECASE
    )
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    out = "\n".join(lines).strip()
    if _OPENING_HEY_RE.match(out):
        out = _OPENING_HEY_RE.sub("Здравствуйте. ", out, count=1).strip()
    out = _normalize_entity_ack_opening(out)

    return out


def _recent_gratitude_count(state: SalesState, tail: int = 6) -> int:
    if not isinstance(state, SalesState):
        return 0
    recent_assistant = [
        str(item.get("content") or "")
        for item in (state.history or [])
        if item.get("role") == "assistant"
    ]
    if tail > 0:
        recent_assistant = recent_assistant[-tail:]
    count = 0
    for text in recent_assistant:
        if _GRATITUDE_RE.search(text or ""):
            count += 1
    return count


def _trim_redundant_gratitude_opening(text: str, state: SalesState) -> str:
    candidate = (text or "").strip()
    if not candidate:
        return candidate
    match = _GRATITUDE_PHRASE_RE.match(candidate)
    if not match:
        return candidate
    opening = match.group(0).strip().lower()
    should_trim = "за обращение" in opening or _recent_gratitude_count(state) >= 1
    if not should_trim:
        return candidate
    tail = candidate[match.end() :].strip()
    if not tail:
        return candidate
    if tail and tail[0].isalpha():
        tail = tail[0].upper() + tail[1:]
    return tail


def _limit_questions(text: str, max_questions: int = 1) -> str:
    if not text:
        return text
    if max_questions < 0:
        max_questions = 0
    out: list[str] = []
    questions_left = max_questions
    for ch in text:
        if ch == "?":
            if questions_left <= 0:
                out.append(".")
                continue
            questions_left -= 1
        out.append(ch)
    return "".join(out)


def _enforce_exclamation_budget(text: str, max_exclamations: int = 1) -> str:
    candidate = str(text or "")
    if not candidate:
        return candidate
    if max_exclamations < 0:
        max_exclamations = 0
    out: list[str] = []
    left = max_exclamations
    for ch in candidate:
        if ch == "!":
            if left <= 0:
                out.append(".")
                continue
            left -= 1
        out.append(ch)
    return "".join(out)


def _rotate_greeting(text: str, state: SalesState) -> str:
    if not text:
        return text
    match = _GREETING_PREFIX_RE.match(text)
    if not match:
        return text
    last = (state.last_bot_reply or "").strip()
    if not last:
        return text
    last_match = _GREETING_PREFIX_RE.match(last)
    if not last_match:
        return text
    current = match.group(1).lower()
    previous = last_match.group(1).lower()
    if current != previous:
        return text
    alternatives = ("Здравствуйте", "Добрый день", "Добрый вечер", "Приветствую")
    try:
        seed_src = (state.last_user_text or "") + "|" + (state.last_bot_reply or "")
        digest = hashlib.sha1(seed_src.encode("utf-8")).hexdigest()
        seed = int(digest[:8], 16)
    except Exception:
        seed = int(time.time())
    replacement = alternatives[seed % len(alternatives)]
    if replacement.lower() == current:
        replacement = alternatives[(seed + 1) % len(alternatives)]
    return text[: match.start()] + replacement + text[match.end() :]


def _has_address_fact(state: SalesState) -> bool:
    if not isinstance(state, SalesState):
        return False
    known_slots = dict(state.known_slots or {})
    facts = dict(state.facts or {})
    candidates = (
        str(known_slots.get("address") or "").strip(),
        str(facts.get("address") or "").strip(),
        str(facts.get("адрес") or "").strip(),
    )
    return any(bool(item) for item in candidates)


def _strip_unverified_local_claims(text: str, state: SalesState) -> str:
    candidate = (text or "").strip()
    if not candidate:
        return candidate
    if _has_address_fact(state):
        return candidate

    parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(candidate) if part.strip()]
    if not parts:
        return candidate

    kept: list[str] = []
    removed = False
    for part in parts:
        if _NEIGHBOR_CLAIM_RE.search(part):
            removed = True
            continue
        kept.append(part)
    if not removed:
        return candidate
    rebuilt = " ".join(kept).strip()
    return rebuilt or "Продолжаем подбор."


def _normalize_entity_ack_opening(text: str) -> str:
    candidate = (text or "").strip()
    if not candidate:
        return candidate
    match = _ENTITY_ACK_PREFIX_RE.match(candidate)
    if not match:
        return candidate
    entity = str(match.group(1) or "").strip()
    if len([tok for tok in entity.split() if tok]) > 3:
        return candidate
    tail = candidate[match.end() :].strip()
    if tail.lower().startswith("что "):
        return candidate
    if not tail:
        return candidate
    if tail and tail[0].isalpha():
        tail = tail[0].upper() + tail[1:]
    return tail.strip()


def _looks_like_contextual_short_followup(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    if "?" in raw:
        return False
    if re.search(r"\d", raw):
        return False
    if _looks_like_address_value(raw):
        return False
    low = _normalize_text(raw)
    if _OBJECT_TYPE_HINT_RE.search(low):
        return False
    tokens = [tok for tok in _FACT_TOKEN_RE.findall(raw) if tok]
    if not tokens or len(tokens) > 2:
        return False
    if _MODEL_NAME_INTENT_RE.search(raw) or _VARIANTS_USER_HINT_RE.search(raw):
        return False
    return True


def _is_unsubscribe_intent(text: str) -> bool:
    return bool(_STOP_INTENT_RE.search(str(text or "")))


def _is_quota_or_rate_limit_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    if "insufficient_quota" in message or "rate limit" in message:
        return True
    rate_limit_cls = getattr(openai, "RateLimitError", None)
    if rate_limit_cls and isinstance(exc, rate_limit_cls):
        return True
    return False


def _unsubscribe_ack_text() -> str:
    return ""


def _strip_instruction_leaks(text: str) -> str:
    candidate = str(text or "")
    if not candidate.strip():
        return ""
    # Remove unresolved placeholders from persona examples.
    candidate = re.sub(r"<\s*[^>\n]{1,40}\s*>", "", candidate)
    # Drop markdown/code fence artifacts that should never be shown to leads.
    candidate = candidate.replace("`", " ")
    out = _INSTRUCTION_LEAK_LINE_RE.sub("", candidate)
    out = _SHORTLIST_LEAK_RE.sub("", out)
    # Remove meta tails about formatting/delivery that should never be shown to leads.
    out = re.sub(
        r"(?iu)\bсначала\b[^.?!\n]{0,220}\bотдельн\w*\s+сообщени\w*[^.?!\n]*",
        "",
        out,
    )
    out = re.sub(
        r"(?iu)[,;:\-\s]*\bотвечайт\w*\s+(?:разв[её]рнут\w*|подробн\w*|не\s+односложн\w*)\b[.!?]*",
        "",
        out,
    )
    out = re.sub(
        r"(?iu)[,;:\-\s]*\bне\s+одн\w*\s+строк\w*\b[.!?]*",
        "",
        out,
    )
    out = _strip_embedded_operator_tail(out)
    out = re.sub(
        r"(?iu)\bпосле\s+приветствия\s+последовательно\s+уточни:?\s*",
        "",
        out,
    )
    out = re.sub(
        r"(?iu)[,;:\-\s]*\bотдельн\w*\s+сообщени\w*\b[^.?!\n]*",
        "",
        out,
    )
    out = re.sub(
        r"(?iu)\bв\s+этом\s+же\s+ответе\s+[^.?!\n]*(?:[.?!]|$)",
        "",
        out,
    )
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    cleaned_lines: List[str] = []
    for line in lines:
        if _INSTRUCTION_LIST_LINE_RE.match(line):
            continue
        cleaned_lines.append(line)
    lines = cleaned_lines
    out = "\n".join(lines)
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", out) if part.strip()]
    if parts:
        filtered_parts: list[str] = []
        for part in parts:
            low = part.lower().replace("ё", "е")
            if _is_operator_instruction_sentence(part):
                continue
            if _is_response_format_instruction_sentence(part):
                continue
            if _is_sequence_process_instruction_sentence(part):
                continue
            if ("?" not in part) and re.match(
                r"(?iu)^\s*(поздоровайт\w*|поприветствуйт\w*)\b",
                low,
            ):
                continue
            if ("?" not in part) and re.match(
                r"(?iu)^\s*(скажите|спросите|уточните|напишите)\s+что\b",
                low,
            ):
                continue
            part = _strip_embedded_operator_tail(part)
            if not part:
                continue
            low = part.lower().replace("ё", "е")
            if re.search(
                r"(?iu)\b(без\s+лишних\s+уточнен|без\s+повтора|дайте\s+адрес\s+сразу|"
                r"при\s+известном\s+городе)\b",
                low,
            ):
                continue
            if re.search(r"(?iu)^(pdf|пдф)\b.*\b(каталог|ссылк)\b", low):
                continue
            if re.match(
                r"(?iu)^\s*(честно\s+)?(сообщайте|предлагайте|предложите|уточните|спросите|попросите|"
                r"примите|извлеките|фиксируйте|передайте|дайте)\b",
                low,
            ) and ("?" not in part):
                continue
            filtered_parts.append(part)
        out = " ".join(filtered_parts).strip()
    out = re.sub(r"\s{2,}", " ", out)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    out = re.sub(
        r"(?iu)\b(работаем\s+по\s+каталогу\s+и\s+выездом)\s*,?\s*без\s+адресов\s+магазин\w*\b",
        r"\1",
        out,
    )
    out = re.sub(r"(?iu)\bот\s+цена\s+по\s+каталогу\b", "цена по каталогу", out)
    out = re.sub(r"(?iu)\bза\s+цена\s+по\s+каталогу\b", "цена по каталогу", out)
    out = re.sub(r"(?iu)\bв\s+цена\s+по\s+каталогу\b", "цена по каталогу", out)
    out = re.sub(r"(?iu)\bк\s+цена\s+по\s+каталогу\b", "цена по каталогу", out)
    catalog_price_placeholder = "цена по каталогу"
    if catalog_price_placeholder and catalog_price_placeholder in out.lower().replace("ё", "е"):
        sentence_parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(out) if part.strip()]
        if sentence_parts:
            rewritten_parts: list[str] = []
            has_placeholder_sentence = False
            for part in sentence_parts:
                low = part.lower().replace("ё", "е")
                if catalog_price_placeholder in low:
                    needs_rewrite = bool(
                        re.search(
                            r"(?iu)\b(обойд|будет|итог|примерно|с\s+уч[её]том\s+скид|"
                            r"после\s+скид|со\s+скидк)\w*\b",
                            low,
                        )
                    )
                    if needs_rewrite:
                        has_placeholder_sentence = True
                        rewritten_parts.append("Точную цену по выбранной модели уточню и сразу напишу.")
                        continue
                rewritten_parts.append(part)
            if has_placeholder_sentence:
                final_parts: list[str] = []
                for part in rewritten_parts:
                    low = part.lower().replace("ё", "е")
                    if ("?" in part) and re.search(r"(?iu)\b(оформ|подтверд|берем|берём|заказ)\w*\b", low):
                        continue
                    final_parts.append(part)
                out = " ".join(final_parts).strip()
    out = re.sub(r"(?u)[\"«»]\s*([,.;:!?])", r"\1", out)
    out = re.sub(r"(?u)[\"«»]+\s*$", "", out)
    out = re.sub(r"(?u)\s+[\"«»]+\s*$", "", out)
    quote_count = out.count("\"") + out.count("«") + out.count("»")
    if quote_count % 2 != 0:
        out = out.replace("\"", "").replace("«", "").replace("»", "")
    out = re.sub(r"(\d)\)(?=\s|$)", r"\1", out)
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out


def _safe_minimal_fallback_reply(
    *,
    tenant: int | None,
    channel_name: str,
    contact_ref: int,
    last_user_message: str,
) -> str:
    state = load_sales_state(tenant, contact_ref)
    persona_hints = load_persona_hints(tenant, channel_name)
    persona_text = load_persona(tenant, channel_name)
    branding = _branding_for_tenant(tenant, channel_name)
    known_facts = _state_facts_snapshot(state)
    turn_intent = _classify_turn_intent(last_user_message, known_facts=known_facts)
    direct_persona_reply = _persona_direct_reply_for_user_turn(
        persona_text,
        last_user_message=last_user_message,
        known_facts=known_facts,
        state=state,
    )
    grounding = _build_reply_grounding(
        tenant=tenant,
        state=state,
        user_text=last_user_message,
    )
    catalog_items = _grounding_catalog_items(grounding)
    if (not catalog_items) and (tenant is not None):
        try:
            catalog_items = read_all_catalog(tenant=tenant)
        except Exception:
            catalog_items = []
    effective_grounding: dict[str, Any] = dict(grounding or {})
    if catalog_items and not _grounding_catalog_items(grounding):
        effective_grounding["catalog_items"] = [dict(item) for item in list(catalog_items or [])[:40]]
    selected = _selected_item_from_grounding(grounding, catalog_items)
    last_user_low = _normalize_text(str(last_user_message or ""))
    model_repair_turn = bool(
        _LOW_SIGNAL_CONTEXT_RE.search(str(last_user_message or ""))
        or _CATALOG_UNAVAILABLE_RE.search(str(last_user_message or ""))
        or ("груз" in last_user_low)
        or ("открыва" in last_user_low)
        or ("открыл" in last_user_low and "не" in last_user_low)
    )
    if state.pending_fact_key == "model" and model_repair_turn:
        intro = _persona_catalog_unavailable_reply(persona_text)
        preview = ""
        preview_items: list[Mapping[str, Any]] = []
        grounding_items = _grounding_catalog_items(grounding)
        if grounding_items:
            preview_items = [dict(item) for item in list(grounding_items or [])[:2]]
            preview = _shortlist_preview_text(preview_items, limit=2)
        if (not preview_items) and tenant is not None:
            try:
                # Build fresh retrieval needs from confirmed facts and current user turn.
                # Avoid stale free-form state.needs tokens degrading catalog match quality.
                needs: Dict[str, Any] = {}
                for key in ("city", "object_type", "address"):
                    fact_val = str((state.facts or {}).get(key) or "").strip()
                    if fact_val:
                        needs[key] = fact_val
                query_needs = infer_user_needs(last_user_message)
                safe_need_keys = {"keywords", "budget_max", "price_order", "color", "object_type", "dimensions", "model"}
                for key, value in dict(query_needs or {}).items():
                    if value in (None, "", [], {}, ()):
                        continue
                    if key not in safe_need_keys:
                        continue
                    needs[key] = value
                preview_items = list(
                    search_catalog(
                        needs,
                        limit=2,
                        tenant=tenant,
                        query=last_user_message,
                    )
                    or []
                )
                preview = _shortlist_preview_text(preview_items, limit=2)
                if not preview_items:
                    catalog_items = list(read_all_catalog(tenant=tenant) or [])
                    probe = _extract_attribute_probe(last_user_message)
                    if probe:
                        probe_items = _items_with_attribute(catalog_items, probe)
                        if probe_items:
                            catalog_items = [dict(item) for item in probe_items]
                    budget_cap = _extract_budget_cap_from_needs(query_needs)
                    if budget_cap:
                        filtered = [
                            dict(item)
                            for item in catalog_items
                            if isinstance(_item_price_int(dict(item)), int)
                            and int(_item_price_int(dict(item)) or 0) > 0
                            and int(_item_price_int(dict(item)) or 0) <= int(budget_cap)
                        ]
                        if filtered:
                            catalog_items = filtered
                    order = _extract_price_order_intent(last_user_message)
                    if order in {"asc", "desc"}:
                        reverse = order == "desc"
                        priced = [
                            dict(item)
                            for item in catalog_items
                            if isinstance(_item_price_int(dict(item)), int)
                            and int(_item_price_int(dict(item)) or 0) > 0
                        ]
                        if priced:
                            catalog_items = sorted(
                                priced,
                                key=lambda item: int(_item_price_int(dict(item)) or 0),
                                reverse=reverse,
                            )
                    preview_items = [dict(item) for item in list(catalog_items or [])[:2]]
                    preview = _shortlist_preview_text(preview_items, limit=2)
            except Exception:
                preview = ""
                preview_items = []
        if preview and intro:
            reply = f"{intro.rstrip('.!?')}: {preview}."
        elif preview:
            reply = f"{preview}."
        else:
            reply = intro or _persona_driven_question_for_fact(persona_text, "model", state=state)
        if preview_items:
            effective_grounding["catalog_items"] = [dict(item) for item in preview_items[:8]]
            state.last_items = [dict(item) for item in preview_items[:2]]
        state.pending_fact_key = "model"
        reply = _apply_base_answer_quality_floor(
            reply,
            state=state,
            persona_hints=persona_hints,
            grounding=effective_grounding,
            user_text=last_user_message,
        )
        reply = _ensure_dialog_greeting_on_first_reply(reply, state, persona_context=persona_text)
        state.last_bot_reply = reply
        state.append_history("assistant", reply)
        state.last_updated_ts = time.time()
        save_sales_state(state)
        return reply
    current_pending = _canonical_fact_key(str(state.pending_fact_key or ""))
    unresolved_model_followup = bool(
        current_pending == "model"
        and selected is None
        and bool(state.last_items)
        and (
            bool(_extract_attribute_probe(last_user_message))
            or _looks_like_contextual_short_followup(last_user_message)
            or bool(
                state.last_items
                and _is_shortlist_feedback_turn(last_user_message, known_facts=known_facts)
            )
        )
    )
    if unresolved_model_followup:
        shortlist_answer, shortlist_items = _shortlist_comparison_followup_plan(
            last_user_message,
            state.last_items or [],
            tenant=tenant,
            persona_context=persona_text,
            state=state,
        )
        if not shortlist_answer:
            shortlist_answer = _shortlist_attribute_answer(last_user_message, state.last_items or [])
        if shortlist_answer:
            reply = shortlist_answer
            if shortlist_items:
                state.last_items = [dict(item) for item in shortlist_items[:2]]
        else:
            reply = _fallback_contextual_question(
                last_user_message,
                state=state,
                persona_context=persona_text,
            ) or _persona_driven_question_for_fact(persona_text, "model", state=state)
        state.pending_fact_key = "model"
        reply = _apply_base_answer_quality_floor(
            reply,
            state=state,
            persona_hints=persona_hints,
            grounding=effective_grounding,
            user_text=last_user_message,
        )
        state.last_bot_reply = reply
        state.append_history("assistant", reply)
        state.last_updated_ts = time.time()
        save_sales_state(state)
        return reply
    if current_pending == "model" and selected is None:
        model_probe = _extract_attribute_probe(last_user_message)
        if model_probe:
            probe_items: list[Mapping[str, Any]] = []
            source_items = _grounding_catalog_items(grounding)
            if not source_items and tenant is not None:
                try:
                    source_items = read_all_catalog(tenant=tenant)
                except Exception:
                    source_items = []
            if source_items:
                probe_items = _items_with_attribute(source_items, model_probe)
            if not probe_items and source_items:
                probe_items = [dict(item) for item in list(source_items or [])[:2]]
            if probe_items:
                shortlist = [dict(item) for item in list(probe_items or [])[:2]]
                preview = _shortlist_preview_text(shortlist, limit=2)
                if preview:
                    reply = _render_shortlist_preview_reply(
                        preview,
                        ask_detail=True,
                        persona_context=persona_text,
                        state=state,
                        user_text=last_user_message,
                    )
                    state.last_items = shortlist[:2]
                    state.pending_fact_key = "model"
                    effective_grounding["catalog_items"] = [dict(item) for item in shortlist[:8]]
                    reply = _apply_base_answer_quality_floor(
                        reply,
                        state=state,
                        persona_hints=persona_hints,
                        grounding=effective_grounding,
                        user_text=last_user_message,
                    )
                    reply = _ensure_dialog_greeting_on_first_reply(reply, state, persona_context=persona_text)
                    state.last_bot_reply = reply
                    state.append_history("assistant", reply)
                    state.last_updated_ts = time.time()
                    save_sales_state(state)
                    return reply

    required = _required_facts_from_persona_text(persona_text)
    missing = _missing_required_facts(required, known_facts)
    missing = _prioritize_missing_facts(missing, turn_intent=turn_intent)
    script_questions = _persona_script_questions(persona_text)
    script_question = _persona_primary_script_question(persona_text, state=state)
    suppress_followup_questions = False
    catalog_intent = bool(
        _is_price_intent(last_user_message)
        or _MODEL_NAME_INTENT_RE.search(str(last_user_message or ""))
        or _VARIANTS_USER_HINT_RE.search(str(last_user_message or ""))
    )
    selected_followup_intent = bool(
        selected is not None
        and (
            _is_price_intent(last_user_message)
            or bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or "")))
            or bool(_VARIANTS_USER_HINT_RE.search(str(last_user_message or "")))
            or bool(_extract_attribute_probe(last_user_message))
        )
    )
    if direct_persona_reply and turn_intent not in {"repair", "catalog_problem"}:
        reply = direct_persona_reply
    elif selected_followup_intent:
        state.pending_fact_key = ""
        reply = _selected_item_attribute_answer(last_user_message, selected)
        if not reply and bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or ""))):
            reply = _selected_item_brief_answer(selected)
        if not reply and _is_price_intent(last_user_message):
            selected_name = _item_label(dict(selected))
            selected_price = _item_price_int(dict(selected))
            if selected_name and selected_price:
                reply = f"{selected_name} {_format_rub_price(selected_price)}".strip()
        if not reply:
            reply = _selected_item_brief_answer(selected)
    elif turn_intent == "why_question":
        target_key = current_pending or (missing[0] if missing else "")
        reply = _explain_missing_fact_need(target_key, persona_context=persona_text)
        if not reply:
            reply = _persona_driven_question_for_fact(persona_text, target_key or "model", state=state)
        followup_question = ""
        if target_key:
            followup_question = _persona_driven_question_for_fact(
                persona_text,
                target_key,
                state=state,
            )
        if reply and followup_question and not _is_repeated_question_against_state(followup_question, state):
            reply = f"{reply.rstrip()} {followup_question}".strip()
        elif not reply and followup_question:
            reply = followup_question
        if target_key:
            state.pending_fact_key = _canonical_fact_key(target_key)
    elif turn_intent == "offtopic":
        reply = "Готов помочь по вашему запросу."
        suppress_followup_questions = True
        missing = []
        state.pending_fact_key = ""
    elif turn_intent == "repair":
        target_key = current_pending or (missing[0] if missing else "")
        if target_key:
            if _canonical_fact_key(target_key) == "model" and state.last_items:
                shortlist_answer, shortlist_items = _shortlist_comparison_followup_plan(
                    last_user_message,
                    state.last_items or [],
                    tenant=tenant,
                    persona_context=persona_text,
                    state=state,
                )
                if not shortlist_answer:
                    shortlist_answer = _shortlist_attribute_answer(
                        last_user_message, state.last_items or []
                    )
                if shortlist_answer:
                    reply = shortlist_answer
                    if shortlist_items:
                        state.last_items = [dict(item) for item in shortlist_items[:2]]
                else:
                    reply = _fallback_contextual_question(
                        last_user_message,
                        state=state,
                        persona_context=persona_text,
                    )
            else:
                reply = _persona_driven_question_for_fact(persona_text, target_key, state=state)
            state.pending_fact_key = _canonical_fact_key(target_key)
        else:
            reply = _fallback_contextual_question(
                last_user_message,
                state=state,
                persona_context=persona_text,
            ) or _persona_primary_script_question(persona_text, state=state)
    elif selected is not None and not missing:
        state.pending_fact_key = ""
        reply = _selected_item_attribute_answer(last_user_message, selected)
        if not reply:
            explicit_match = _best_catalog_item_match(last_user_message, catalog_items)
            if explicit_match is not None:
                reply = _selected_item_brief_answer(explicit_match)
        if not reply:
            reply = _selected_item_brief_answer(selected)
    elif catalog_intent:
        reply = ""
        if selected is not None and _is_price_intent(last_user_message):
            selected_name = _item_label(dict(selected))
            selected_price = _item_price_int(dict(selected))
            if selected_name and selected_price:
                reply = f"По каталогу {selected_name} стоит {_format_rub_price(selected_price)}."
        if not reply and _is_price_intent(last_user_message):
            min_price = _catalog_min_price(catalog_items)
            if min_price:
                reply = f"По каталогу есть варианты от {_format_rub_price(min_price)}."
        if not reply:
            preferred_question = script_question if missing else ""
            if preferred_question and (
                _is_price_intent(last_user_message)
                or bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or "")))
                or bool(_VARIANTS_USER_HINT_RE.search(str(last_user_message or "")))
            ):
                if not (
                    _question_covers_fact(preferred_question, "model")
                    or _question_covers_fact(preferred_question, "budget")
                ):
                    preferred_question = ""
            reply = preferred_question or _fallback_contextual_question(
                last_user_message,
                state=state,
                persona_context=persona_text,
            )
        if missing:
            selected_key = missing[0]
            selected_question = _persona_driven_question_for_fact(
                persona_text,
                selected_key,
                state=state,
            )
            if selected_question and ("?" not in reply):
                reply = f"{reply} {selected_question}".strip()
                state.pending_fact_key = _canonical_fact_key(selected_key)
    elif missing and not suppress_followup_questions:
        selected_key = missing[0]
        selected_question = _persona_driven_question_for_fact(
            persona_text,
            selected_key,
            state=state,
        )
        for key in missing:
            q = _persona_driven_question_for_fact(persona_text, key, state=state)
            if not _is_repeated_question_against_state(q, state):
                selected_key = key
                selected_question = q
                break
        if _is_repeated_question_against_state(selected_question, state):
            non_address = [k for k in missing if _canonical_fact_key(k) != "address"]
            for key in non_address:
                q = _persona_driven_question_for_fact(persona_text, key, state=state)
                if not _is_repeated_question_against_state(q, state):
                    selected_key = key
                    selected_question = q
                    break
        state.pending_fact_key = _canonical_fact_key(selected_key)
        reply = selected_question
    elif turn_intent == "catalog_request" and str(branding.get("CATALOG_URL") or "").strip():
        reply = f"Вот каталог: {str(branding.get('CATALOG_URL') or '').strip()}"
    elif _ORDER_INTENT_RE.search(str(last_user_message or "")):
        if str(known_facts.get("contact") or "").strip():
            reply = "Продолжаем оформление. Подтверждение отправлю по вашему контакту."
        else:
            state.pending_fact_key = "contact"
            reply = "Чтобы оформить, оставьте, пожалуйста, телефон или удобный мессенджер."
    elif known_facts.get("model") and not missing:
        state.pending_fact_key = ""
        reply = ""
        if selected is not None:
            attr_answer = _selected_item_attribute_answer(last_user_message, selected)
            if attr_answer:
                reply = attr_answer
            elif bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or ""))):
                reply = _selected_item_brief_answer(selected)
        if not reply:
            reply = _fallback_contextual_question(
                last_user_message,
                state=state,
                persona_context=persona_text,
            ) or _selected_item_brief_answer(selected if isinstance(selected, Mapping) else {})
    else:
        if script_question and missing:
            reply = script_question
            for key in _fact_keys_from_line(script_question):
                canonical = _canonical_fact_key(key)
                if canonical:
                    state.pending_fact_key = canonical
                    break
        else:
            reply = _fallback_contextual_question(
                last_user_message,
                state=state,
                persona_context=persona_text,
            )
    reply = _apply_base_answer_quality_floor(
        reply,
        state=state,
        persona_hints=persona_hints,
        grounding=effective_grounding,
        user_text=last_user_message,
    )
    reply = _apply_persona_sequence_obligations(
        reply,
        persona_context=persona_text,
        last_user_message=last_user_message,
        known_facts=known_facts,
        state=state,
    )
    reply = _apply_persona_delivery_obligations(
        reply,
        persona_context=persona_text,
        channel_name=channel_name,
        last_user_message=last_user_message,
        known_facts=known_facts,
        state=state,
    )
    if len((reply or "").strip()) <= 8 and not suppress_followup_questions:
        preferred_short_question = str(script_question or "").strip()
        if preferred_short_question and _is_repeated_question_against_state(preferred_short_question, state):
            preferred_short_question = ""
        if (not preferred_short_question) and missing:
            inferred = _persona_driven_question_for_fact(persona_text, missing[0], state=state)
            if inferred and not _is_repeated_question_against_state(inferred, state):
                preferred_short_question = inferred
        reply = preferred_short_question or _fallback_contextual_question(
            last_user_message,
            state=state,
            persona_context=persona_text,
        )
    reply = _apply_base_answer_quality_floor(
        reply,
        state=state,
        persona_hints=persona_hints,
        grounding=effective_grounding,
        user_text=last_user_message,
    )
    if direct_persona_reply:
        direct_questions = _extract_questions_from_text(reply)
        if direct_questions:
            pending_key = ""
            for question in direct_questions:
                inferred = _normalize_slot_name("", question=question)
                canonical = _canonical_fact_key(inferred)
                if canonical:
                    pending_key = canonical
                    break
            if not pending_key:
                for miss_key in missing:
                    if any(_question_covers_fact(question, miss_key) for question in direct_questions):
                        pending_key = _canonical_fact_key(miss_key)
                        break
            state.pending_fact_key = pending_key
    if state is not None and script_questions and _is_repeated_question_against_state(reply, state):
        preferred_pending = _canonical_fact_key(str(state.pending_fact_key or ""))
        if preferred_pending:
            pending_question = _persona_driven_question_for_fact(
                persona_text,
                preferred_pending,
                state=state,
            )
            if pending_question and not _is_repeated_question_against_state(pending_question, state):
                reply = pending_question
        if _is_repeated_question_against_state(reply, state) and not preferred_pending:
            for question in script_questions:
                if not _is_repeated_question_against_state(question, state):
                    reply = question
                    break
    if not str(reply or "").strip():
        preferred_script = str(script_question or "").strip()
        if preferred_script and not _is_repeated_question_against_state(preferred_script, state):
            reply = preferred_script
        preferred_key = (
            _canonical_fact_key(str(state.pending_fact_key or ""))
            or _canonical_fact_key(current_pending)
            or (_canonical_fact_key(missing[0]) if missing else "")
            or ""
        )
        if preferred_key == "model" and not catalog_items:
            non_model_missing = [
                _canonical_fact_key(key)
                for key in missing
                if _canonical_fact_key(key) and _canonical_fact_key(key) != "model"
            ]
            preferred_key = (non_model_missing[0] if non_model_missing else "") or "budget"
        if (not preferred_key) and catalog_items:
            preferred_key = "model"
        if not preferred_key:
            preferred_key = "budget"
        if not str(reply or "").strip():
            reply = _persona_driven_question_for_fact(persona_text, preferred_key, state=state)
        if not str(reply or "").strip():
            reply = _generic_question_for_fact(preferred_key)
    reply = _ensure_dialog_greeting_on_first_reply(reply, state, persona_context=persona_text)
    state.last_bot_reply = reply
    state.append_history("assistant", reply)
    state.last_updated_ts = time.time()
    _update_fact_memory(state, reply)
    _remember_questions_from_reply(state, reply)
    save_sales_state(state)
    return reply


def _llm_unavailable_reply(
    *,
    user_text: str = "",
    grounding: Mapping[str, Any] | None = None,
) -> str:
    items = _grounding_catalog_items(grounding) if isinstance(grounding, Mapping) else []
    user_raw = str(user_text or "")
    turn_intent = _classify_turn_intent(user_raw)
    user_norm = _normalize_text(user_raw)
    preview = _shortlist_preview_text(items, limit=2) if items else ""
    asks_variants = bool(_VARIANTS_USER_HINT_RE.search(user_raw))
    asks_price = _is_price_intent(user_raw) or _looks_like_price_objection(user_raw)
    attribute_like_turn = bool(
        re.search(
            r"(?iu)(\?|какой|какая|какие|тонк|толщ|металл|наполн|характерист|цвет|зеркал|замок)",
            user_norm,
        )
    )
    asks_attributes = bool(_extract_attribute_probe(user_raw)) if attribute_like_turn else False
    repetition_complaint = bool(
        re.search(r"(?iu)\b(тоже\s+сам|одно\s+и\s+то\s+же|повтор|опять)\b", user_norm)
    )

    if asks_attributes and items:
        top = dict(items[0])
        name = _display_item_label(top) or _item_label(top)
        if name:
            return f"По характеристикам могу дать конкретику по модели {name}. Если нужно, покажу другой вариант из каталога."

    if items and _is_price_intent(user_raw):
        min_price = _catalog_min_price(items)
        max_price = _catalog_max_price(items)
        if min_price and max_price and min_price != max_price:
            return (
                f"По цене ориентир такой: от {_format_rub_price(min_price)} до {_format_rub_price(max_price)}. "
                "Могу сразу предложить подходящие варианты из каталога."
            )
    if items and (asks_variants or asks_price or repetition_complaint or turn_intent in {"repair", "catalog_problem"}):
        if turn_intent in {"repair", "catalog_problem"} and preview:
            return f"Могу сразу предложить подходящие модели: {preview}. Продолжим подбор по вашему запросу."
        if repetition_complaint and preview:
            return f"Покажу другой вариант: {preview}. Что важнее по характеристикам для следующего подбора?"
        if asks_price and preview:
            return f"По цене и альтернативам могу предложить: {preview}. Если нужно дешевле, подберу другой вариант."
        if preview:
            return f"Могу предложить варианты: {preview}. Продолжим подбор по вашему запросу."

    snippet = re.sub(r"\s+", " ", user_raw).strip()
    if len(snippet) > 80:
        snippet = snippet[:80].rstrip() + "..."
    if snippet:
        return f"Сейчас высокая нагрузка. Фиксирую запрос: {snippet}. Продолжаю подбор."
    return "Сейчас высокая нагрузка. Продолжаю подбор по вашему запросу."


def _apply_optional_lowercase_opening(
    text: str,
    state: SalesState,
    *,
    persona_hints: PersonaHints | None = None,
) -> str:
    candidate = (text or "").strip()
    if not candidate:
        return candidate
    if (state.user_message_count or 0) <= 1:
        return candidate
    if _GREETING_PREFIX_RE.match(candidate):
        return candidate
    chance = float(getattr(settings, "LOWERCASE_OPENING_CHANCE", 0.0) or 0.0)
    if chance <= 0:
        return candidate
    match = _OPENING_WORD_RE.match(candidate)
    if not match:
        return candidate
    opening = match.group(1)
    lower_opening = opening.lower()
    if lower_opening in _LOWERCASE_OPENING_BLOCKED:
        return candidate
    if len(opening) >= 3 and opening.isupper():
        return candidate
    # Keep sentence case only for explicitly formal style.
    tone = (persona_hints.tone or "").lower() if persona_hints else ""
    if any(token in tone for token in ("формал", "официал")):
        return candidate
    if random.random() > chance:
        return candidate
    lowered = opening[0].lower() + opening[1:]
    return candidate[: match.start(1)] + lowered + candidate[match.end(1) :]


def _humanize_reply_text(
    reply: str,
    *,
    state: SalesState,
    persona_hints: PersonaHints | None = None,
) -> str:
    text = (reply or "").strip()
    if not text:
        return text
    text = _strip_instruction_leaks(text)
    if not text:
        return ""

    # Remove technical multi-variant labels if they slip into final output.
    text = re.sub(r"(?im)^\s*Вариант\s+\d+\s*:\s*", "", text).strip()

    # Trim obvious bureaucratic tails.
    text = re.sub(r"(?im)^\s*с уважением[,.! ]*$", "", text).strip()
    text = re.sub(r"(?im)^\s*обращайтесь в любое время[,.! ]*$", "", text).strip()

    # Final cleanup (minimal and non-destructive).
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    text = re.sub(r"\?\.+", "?", text)
    text = re.sub(r"!\.+", "!", text)
    text = re.sub(r"\.\?+", "?", text)
    text = re.sub(r"([,;:])\1+", r"\1", text)
    text = _apply_conversational_phrasing(text, persona_hints=persona_hints)
    text = _trim_redundant_gratitude_opening(text, state)
    text = _strip_unverified_local_claims(text, state)
    text = _normalize_entity_ack_opening(text)
    text = _limit_questions(text, max_questions=_max_questions_limit(persona_hints))
    text = _apply_optional_lowercase_opening(text, state, persona_hints=persona_hints)

    return text.strip()


def _persona_requires_first_greeting(persona_context: str) -> bool:
    low = str(persona_context or "").lower().replace("ё", "е")
    if not low:
        return False
    has_greeting_tokens = any(
        token in low for token in ("здорова", "приветств", "добрый день", "здравствуйте")
    )
    if not has_greeting_tokens:
        return False
    patterns = (
        r"перв\w+\s+сообщени\w+[^\n]{0,120}(здорова|приветств)",
        r"сначала[^\n]{0,120}(здорова|приветств)",
        r"обязатель\w+[^\n]{0,120}(здорова|приветств)",
        r"начина\w+[^\n]{0,120}с\s+приветств",
        r"на\s+старт[^\n]{0,120}(здравствуйте|здорова|приветств)",
    )
    return any(re.search(pattern, low) is not None for pattern in patterns)


def _ensure_dialog_greeting_on_first_reply(
    text: str,
    state: SalesState,
    persona_context: str = "",
) -> str:
    candidate = str(text or "").strip()
    if not candidate:
        return candidate
    force_greeting = str(getattr(settings, "FORCE_FIRST_GREETING", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    persona_force_greeting = _persona_requires_first_greeting(persona_context)
    if not force_greeting and not persona_force_greeting:
        return candidate
    has_bot_history = bool(str(getattr(state, "last_bot_reply", "") or "").strip())
    if has_bot_history:
        return candidate
    if re.match(r"^\s*(https?://|@[\w\d_]+)", candidate):
        return candidate
    greeting = "Здравствуйте."
    persona_low = str(persona_context or "").lower().replace("ё", "е")
    if "добрый день" in persona_low:
        greeting = "Добрый день."
    elif "добрый вечер" in persona_low:
        greeting = "Добрый вечер."
    if _OPENING_HEY_RE.match(candidate):
        body = _OPENING_HEY_RE.sub("", candidate, count=1).strip()
        return f"{greeting} {body}".strip() if body else greeting
    if _GREETING_PREFIX_RE.match(candidate):
        return candidate
    body = str(candidate or "").strip()
    if not body:
        return greeting
    return f"{greeting} {body}".strip()


def _answer_is_too_robotic(text: str) -> bool:
    candidate = (text or "").strip()
    if not candidate:
        return True
    for pattern in _ROBOTIC_BANNED_PATTERNS:
        if pattern.search(candidate):
            return True
    lowered = candidate.lower()
    if re.match(r"^\s*(понял|принял|здравствуйте)[,.! ]*$", lowered):
        return True
    return False


def _count_sentences(text: str) -> int:
    chunks = [part.strip() for part in re.split(r"[.!?]+", text or "") if part.strip()]
    return len(chunks)


def _normalize_numbered_list_punctuation(text: str) -> str:
    candidate = str(text or "").strip()
    if not candidate:
        return candidate
    # Prevent sentence over-splitting on enumerations like "1. ... 2. ..."
    return re.sub(r"(?<![0-9A-Za-zА-Яа-яЁё])(\d{1,2})\.\s+", r"\1) ", candidate)


def _enforce_sentence_budget(
    text: str,
    max_sentences: int = 4,
    max_chars: int = 420,
) -> str:
    candidate = _normalize_numbered_list_punctuation(text)
    if not candidate:
        return candidate
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
    limit_sent = max(1, int(max_sentences or 1))
    clipped = candidate
    if len(parts) > limit_sent:
        kept = parts[:limit_sent]
        clipped = " ".join(kept).strip()
    if clipped and clipped[-1] not in ".!?":
        clipped = clipped + "."
    limit_chars = max(120, int(max_chars or 120))
    if len(clipped) <= limit_chars:
        return clipped
    hard = clipped[:limit_chars].rstrip()
    tail_punct = max(hard.rfind("."), hard.rfind("?"), hard.rfind("!"))
    if tail_punct >= max(0, limit_chars - 120):
        hard = hard[: tail_punct + 1].rstrip()
    else:
        tail_space = hard.rfind(" ")
        if tail_space >= max(0, limit_chars - 80):
            hard = hard[:tail_space].rstrip()
        if hard and hard[-1] not in ".!?":
            hard = hard + "."
    return hard


def _question_token_set(question: str) -> set[str]:
    fp = quality.question_fingerprint(str(question or ""))
    if not fp:
        return set()
    return {token for token in fp.split() if token}


def _extract_questions_from_text(text: str) -> list[str]:
    candidate = (text or "").strip()
    if not candidate:
        return []
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
    questions: list[str] = []
    for part in parts:
        if "?" in part:
            first_q = part.find("?")
            if first_q < 0:
                continue
            question = part[: first_q + 1].strip()
            if len(question) < 4:
                continue
            questions.append(question)
            continue
        # Some channels/users omit '?' — keep slot tracking robust for imperative questions.
        if _QUESTION_CUE_RE.search(part):
            clean = part.strip().rstrip(".!,:;")
            if len(clean) >= 4:
                questions.append(clean + "?")
    return questions


def _is_repeated_question_against_state(question: str, state: SalesState) -> bool:
    q_tokens = _question_token_set(question)
    if not q_tokens:
        return False
    core_fact_keys = ("city", "address", "object_type", "model", "budget", "timeline", "dimensions", "contact")

    def _question_fact_set(text: str) -> set[str]:
        out: set[str] = set()
        for key in core_fact_keys:
            if _question_covers_fact(text, key):
                out.add(key)
        return out

    current_facts = _question_fact_set(question)
    previous_fps = [
        str(item or "").strip()
        for item in (state.asked_question_fingerprints or [])
        if str(item or "").strip()
    ]
    previous_questions = [
        str(item or "").strip()
        for item in (state.asked_questions or [])
        if str(item or "").strip()
    ]
    last_question = str(state.last_question_text or "").strip()
    if last_question:
        last_fp = quality.question_fingerprint(last_question)
        if last_fp:
            previous_fps.append(last_fp)
            previous_questions.append(last_question)
    prev_fact_map: dict[str, set[str]] = {}
    for q_text in previous_questions[-24:]:
        fp = quality.question_fingerprint(q_text)
        if not fp:
            continue
        prev_fact_map[fp] = _question_fact_set(q_text)
    for prev_fp in previous_fps[-24:]:
        prev_tokens = {token for token in prev_fp.split() if token}
        if not prev_tokens:
            continue
        overlap = len(q_tokens & prev_tokens)
        if overlap <= 0:
            continue
        coverage_cur = overlap / max(1, len(q_tokens))
        coverage_prev = overlap / max(1, len(prev_tokens))
        jaccard = overlap / max(1, len(q_tokens | prev_tokens))
        if coverage_cur >= 0.72 or coverage_prev >= 0.72 or jaccard >= 0.62:
            prev_facts = prev_fact_map.get(prev_fp, set())
            if current_facts and prev_facts and current_facts.isdisjoint(prev_facts):
                continue
            return True
    return False


def _reply_has_repeated_question(text: str, state: SalesState) -> bool:
    for question in _extract_questions_from_text(text):
        if _is_repeated_question_against_state(question, state):
            return True
    return False


def _drop_repeated_questions_from_reply(text: str, state: SalesState) -> str:
    candidate = (text or "").strip()
    if not candidate:
        return candidate
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
    kept: list[str] = []
    removed_count = 0
    for part in parts:
        if "?" not in part:
            kept.append(part)
            continue
        first_q = part.find("?")
        if first_q < 0:
            kept.append(part)
            continue
        question = part[: first_q + 1].strip()
        known_facts = _state_facts_snapshot(state)
        if any(
            _question_covers_fact(question, key) and str(known_facts.get(key) or "").strip()
            for key in ("city", "address", "object_type", "model")
        ):
            removed_count += 1
            continue
        if _is_repeated_question_against_state(question, state):
            removed_count += 1
            continue
        kept.append(part)
    cleaned = " ".join(kept).strip()
    if cleaned:
        return cleaned
    if removed_count > 0:
        # If all fragments were removed as repeated questions, keep the original text
        # to avoid injecting robotic canned fallbacks.
        return candidate
    return candidate


def _remember_questions_from_reply(state: SalesState, text: str) -> None:
    for question in _extract_questions_from_text(text):
        _remember_question_state(state, question)


def _render_passes_rubric(text: str, state: SalesState) -> bool:
    candidate = (text or "").strip()
    if not candidate:
        return False
    if _count_sentences(candidate) > 3:
        return False
    if candidate.count("?") > 1:
        return False
    low = candidate.lower()
    # Ban service-like stock phrases that make replies robotic.
    banned = (
        "спасибо, понял",
        "спасибо за подтверждение",
        "ваш запрос принят",
        "если что-то еще интересует",
        "рад вас видеть",
        "чем могу помочь",
        "после приветствия последовательно уточни",
        "собрал короткий шорт-лист",
    )
    if any(phrase in low for phrase in banned):
        return False
    for part in [part.strip() for part in _SENTENCE_SPLIT_RE.split(candidate) if part.strip()]:
        if _is_operator_instruction_sentence(part):
            return False
    # Avoid repeating previous bot reply almost verbatim.
    prev = (state.last_bot_reply or "").strip().lower()
    if prev and len(prev) > 20 and candidate.lower() == prev:
        return False
    if _reply_has_repeated_question(candidate, state):
        return False
    return True


def _apply_base_answer_quality_floor(
    answer: str,
    *,
    state: SalesState,
    persona_hints: PersonaHints | None,
    grounding: Mapping[str, Any] | None,
    user_text: str,
) -> str:
    candidate = str(answer or "").strip()
    if not candidate:
        return ""
    candidate = re.sub(r"(?<=\d)\.(?=\d)", ",", candidate)
    candidate = _normalize_catalog_name_case(candidate, grounding=grounding)
    candidate = _dedupe_repeated_fact_sentences(candidate, state)
    candidate = _strip_instruction_leaks(candidate)
    candidate = _drop_repeated_questions_from_reply(candidate, state)
    candidate = _limit_questions(candidate, max_questions=min(1, _max_questions_limit(persona_hints)))
    candidate = _enforce_exclamation_budget(candidate, max_exclamations=1)
    candidate = _enforce_sentence_budget(candidate, max_sentences=2)
    return candidate.strip()


def _prefer_refined_answer(
    *,
    answer: str,
    refined: str,
    state: SalesState,
    persona_hints: PersonaHints | None,
    grounding: Mapping[str, Any] | None,
    user_text: str,
) -> str:
    base_candidate = _apply_base_answer_quality_floor(
        answer,
        state=state,
        persona_hints=persona_hints,
        grounding=grounding,
        user_text=user_text,
    )
    refined_candidate = _apply_base_answer_quality_floor(
        refined,
        state=state,
        persona_hints=persona_hints,
        grounding=grounding,
        user_text=user_text,
    )
    if not refined_candidate:
        return base_candidate or refined_candidate
    if not base_candidate:
        return refined_candidate
    if refined_candidate == base_candidate:
        return refined_candidate

    base_ok = _render_passes_rubric(base_candidate, state)
    refined_ok = _render_passes_rubric(refined_candidate, state)
    if base_ok and (not refined_ok):
        return base_candidate

    if len(base_candidate) >= 40 and len(refined_candidate) < max(20, int(len(base_candidate) * 0.45)):
        return base_candidate
    if _answer_is_too_robotic(refined_candidate) and not _answer_is_too_robotic(base_candidate):
        return base_candidate

    refined_parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(refined_candidate) if part.strip()]
    base_parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(base_candidate) if part.strip()]
    refined_instructional = any(
        _is_operator_instruction_sentence(part)
        or _is_response_format_instruction_sentence(part)
        or _is_sequence_process_instruction_sentence(part)
        for part in refined_parts
    )
    base_instructional = any(
        _is_operator_instruction_sentence(part)
        or _is_response_format_instruction_sentence(part)
        or _is_sequence_process_instruction_sentence(part)
        for part in base_parts
    )
    if refined_instructional and not base_instructional:
        return base_candidate

    base_low = _normalize_text(base_candidate)
    refined_low = _normalize_text(refined_candidate)
    if (
        "ориентир по времени" in refined_low
        and "ориентир по времени" not in base_low
        and not _ETA_INTENT_RE.search(str(user_text or ""))
    ):
        return base_candidate
    if (
        "выезд сегодня возможен" in refined_low
        and "выезд сегодня возможен" not in base_low
        and not _URGENT_TODAY_RE.search(str(user_text or ""))
    ):
        return base_candidate

    if refined_low.count("модель из каталога") > base_low.count("модель из каталога"):
        return base_candidate
    if refined_low.count("вариант") >= 3 and base_low.count("вариант") <= 1:
        return base_candidate
    if re.search(
        r"(?iu)\b(цвет|модель|тип|стиль|оттенок)\w*\s+вариант\b",
        refined_low,
    ) and not re.search(
        r"(?iu)\b(цвет|модель|тип|стиль|оттенок)\w*\s+вариант\b",
        base_low,
    ):
        return base_candidate

    grounding_items = _grounding_catalog_items(grounding)
    if grounding_items:
        direct_catalog_intent = bool(
            _is_price_intent(str(user_text or ""))
            or _VARIANTS_USER_HINT_RE.search(str(user_text or ""))
            or _MODEL_NAME_INTENT_RE.search(str(user_text or ""))
        )
        if direct_catalog_intent:
            if _reply_mentions_catalog_item(base_candidate, grounding_items) and not _reply_mentions_catalog_item(
                refined_candidate, grounding_items
            ):
                return base_candidate

    return refined_candidate


def _safe_json_load(raw: str) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {}
        try:
            data = json.loads(match.group(0))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}


_LLM_MIN_CALL_GAP_SECONDS = max(
    0.0,
    float(os.getenv("LLM_MIN_CALL_GAP_SECONDS", "0.25") or 0.25),
)
_LLM_CALL_GUARD = asyncio.Lock()
_LLM_NEXT_ALLOWED_TS = 0.0


async def _llm_rate_limit_gate() -> None:
    global _LLM_NEXT_ALLOWED_TS
    if _LLM_MIN_CALL_GAP_SECONDS <= 0:
        return
    async with _LLM_CALL_GUARD:
        now = time.monotonic()
        wait_for = _LLM_NEXT_ALLOWED_TS - now
        if wait_for > 0:
            await asyncio.sleep(wait_for)
            now = time.monotonic()
        _LLM_NEXT_ALLOWED_TS = max(now, _LLM_NEXT_ALLOWED_TS) + _LLM_MIN_CALL_GAP_SECONDS


async def _llm_call_with_deadline(
    create_fn: Any,
    *,
    timeout_seconds: float,
    **kwargs: Any,
) -> Any:
    async def _invoke_once(hard_deadline: float, call_kwargs: Mapping[str, Any]) -> Any:
        try:
            import anyio  # type: ignore

            with anyio.fail_after(hard_deadline):
                return await anyio.to_thread.run_sync(lambda: create_fn(**dict(call_kwargs)))
        except Exception:
            return await asyncio.wait_for(
                asyncio.to_thread(create_fn, **dict(call_kwargs)),
                timeout=hard_deadline,
            )

    timeout_value = max(2.0, float(timeout_seconds or 0.0))
    hard_deadline = timeout_value + 2.0
    base_kwargs = dict(kwargs or {})
    base_model = str(base_kwargs.get("model") or "").strip()
    model_candidates: list[str] = []
    if base_model:
        model_candidates.append(base_model)
    for extra_raw in (
        os.getenv("OPENAI_MODEL_FALLBACKS", ""),
        os.getenv("LEAD_CLASSIFIER_MODEL", ""),
        "gpt-4o-mini",
    ):
        for model in [part.strip() for part in str(extra_raw or "").split(",") if part.strip()]:
            if model and model not in model_candidates:
                model_candidates.append(model)
    if not model_candidates:
        model_candidates.append("")

    retries = 2
    last_exc: Exception | None = None
    for model in model_candidates:
        call_kwargs = dict(base_kwargs)
        if model:
            call_kwargs["model"] = model
        for attempt in range(retries + 1):
            await _llm_rate_limit_gate()
            try:
                return await _invoke_once(hard_deadline, call_kwargs)
            except Exception as exc:
                last_exc = exc
                if _is_quota_or_rate_limit_error(exc):
                    if attempt < retries:
                        await asyncio.sleep(0.6 + attempt * 0.8)
                        continue
                    break
                raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("llm call failed without exception")


_FACT_TOKEN_RE = re.compile(r"[0-9a-zа-яё]+", re.IGNORECASE)
_CONTACT_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_CONTACT_HANDLE_RE = re.compile(r"(?<!\w)@[\w\d_]{4,}")
_CONTACT_PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d[\d\-\s()]{8,}\d)(?!\d)")
_PRICE_INLINE_RE = re.compile(
    r"(?<!\d)(?:\d{1,3}(?:[ \u00A0\u202F]\d{3})+|\d{4,7})(?!\d)(?:\s*(?:₽|руб(?:\.|ля|лей)?))?",
    re.IGNORECASE,
)
_PRICE_THOUSANDS_RE = re.compile(r"(?iu)\b(\d{1,3})\s*(?:тыс(?:\.|яч)?|тысяч(?:а|и)?|к)\b")
_MODEL_QUOTED_MENTION_RE = re.compile(r'(?iu)\b(модель|вариант|дверь)\s*[«"]([^"»]{2,80})[»"]')
_GENERIC_FACT_STOPWORDS = {
    "и",
    "или",
    "в",
    "на",
    "по",
    "с",
    "для",
    "это",
    "как",
    "что",
    "вам",
    "вас",
    "ваш",
    "ваша",
    "ваше",
    "ваши",
    "у",
    "к",
    "же",
    "ли",
    "чего",
    "зачем",
    "почему",
    "тоже",
    "самое",
    "этот",
    "эта",
    "эти",
    "этого",
    "мы",
    "вы",
}
_SLOT_ALIASES = {
    "location": ("город", "район", "адрес", "локац", "доставк"),
    "object": ("квартир", "дом", "объект", "помещен"),
    "model": ("модель", "вариант", "артикул", "позици", "катал"),
    "budget": ("бюдж", "цен", "стоим"),
    "timeline": ("когда", "срок", "сегодня", "завтра", "дат"),
    "dimensions": ("размер", "проем", "ширин", "высот", "замер"),
    "contact": ("телефон", "контакт", "мессендж", "whatsapp", "telegram", "телеграм"),
    "quantity": ("сколько", "количеств"),
    "color": ("цвет", "оттен", "тон"),
}


def _is_plausible_contact_phone(token: str) -> bool:
    raw = str(token or "").strip()
    if not raw:
        return False
    digits = re.sub(r"\D+", "", raw)
    if len(digits) < 10 or len(digits) > 15:
        return False
    # Bare numeric tokens in prompts are often IDs, not phones.
    if re.fullmatch(r"\d+", raw):
        return len(digits) == 11
    # Explicit E.164-ish token.
    if re.fullmatch(r"\+\d+", raw):
        return 10 <= len(digits) <= 15
    return True


_QUESTION_TOPIC_TO_SLOT = {
    "location": "location",
    "object": "object",
    "model": "model",
    "budget": "budget",
    "timeline": "timeline",
    "dimensions": "dimensions",
    "contact": "contact",
    "quantity": "quantity",
    "color": "color",
}
_GENERIC_MODEL_WORDS = {
    "есть",
    "цена",
    "сколько",
    "нужно",
    "надо",
    "подскажите",
    "модель",
    "вариант",
}


def _safe_short_text(value: str, limit: int = 120) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _normalize_fact_key(value: str) -> str:
    key = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    key = re.sub(r"[^a-z0-9а-яё_]+", "", key, flags=re.IGNORECASE)
    key = re.sub(r"_+", "_", key).strip("_")
    return key[:48]


_FACT_CANONICAL_ALIASES: Dict[str, set[str]] = {
    "city": {"city", "город", "location", "локация", "населенный_пункт", "населенныйпункт"},
    "address": {"address", "адрес"},
    "object_type": {"object", "object_type", "тип_объекта", "тип_помещения", "помещение"},
    "model": {"model", "модель", "вариант"},
    "dimensions": {"dimensions", "размер", "размеры", "проем", "проём", "замер"},
    "budget": {"budget", "бюджет"},
    "timeline": {"timeline", "срок", "дата"},
    "contact": {"contact", "контакт", "телефон", "мессенджер"},
}


def _canonical_fact_key(value: str) -> str:
    key = _normalize_fact_key(value)
    if not key:
        return ""
    for canonical, aliases in _FACT_CANONICAL_ALIASES.items():
        if key == canonical or key in aliases:
            return canonical
    return key


def _normalize_required_facts(raw: Any) -> List[str]:
    if isinstance(raw, str):
        items = [raw]
    elif isinstance(raw, Sequence):
        items = [str(x) for x in raw]
    else:
        items = []
    out: List[str] = []
    for item in items:
        key = _canonical_fact_key(item)
        if key and key not in out:
            out.append(key)
    return out[:12]


def _missing_required_facts(required: Sequence[str], facts: Mapping[str, str]) -> List[str]:
    if not required:
        return []
    missing: List[str] = []
    normalized_facts: Dict[str, str] = {}
    for raw_key, raw_val in dict(facts or {}).items():
        key = _canonical_fact_key(str(raw_key))
        val = str(raw_val or "").strip()
        if key and val:
            normalized_facts[key] = val
    for raw in required:
        key = _canonical_fact_key(raw)
        if not key:
            continue
        if not str(normalized_facts.get(key) or "").strip():
            missing.append(key)
    return missing


def _prioritize_missing_facts(missing: Sequence[str], *, turn_intent: str = "") -> List[str]:
    items = [_canonical_fact_key(item) for item in missing]
    clean = [item for item in items if item]
    if not clean:
        return []
    if str(turn_intent or "").strip().lower() == "order":
        order = (
            "contact",
            "city",
            "address",
            "object_type",
            "model",
            "budget",
            "timeline",
            "dimensions",
        )
    else:
        order = (
            "city",
            "object_type",
            "address",
            "model",
            "budget",
            "timeline",
            "dimensions",
            "contact",
        )
    weight = {key: idx for idx, key in enumerate(order)}
    return sorted(clean, key=lambda key: (weight.get(key, len(order)), clean.index(key)))


def _question_covers_fact(question: str, fact_key: str) -> bool:
    low = str(question or "").lower()
    key = _canonical_fact_key(fact_key)
    if not low or not key:
        return False
    token_map: Dict[str, tuple[str, ...]] = {
        "city": ("город", "населен", "локац"),
        "address": ("адрес", "улиц", "подъезд", "дом", "корп", "кв."),
        "object_type": ("квартир", "дом", "помещени"),
        "model": ("каталог", "модель", "вариант"),
        "dimensions": ("размер", "проем", "проём", "замер", "фото проема", "фото проёма"),
        "budget": ("бюдж", "цена", "стоим"),
        "timeline": ("срок", "когда", "дата", "сегодня", "завтра"),
        "contact": ("контакт", "телефон", "мессендж"),
    }
    for token in token_map.get(key, (key,)):
        if token in low:
            return True
    return False


def _question_lists_catalog_options(
    question: str,
    items: Sequence[Mapping[str, Any]],
) -> bool:
    low = _normalize_text(question)
    if not low or "или" not in low:
        return False
    if not items:
        return False

    hits: set[str] = set()
    for item in list(items)[:120]:
        probes: list[str] = []
        label = str(_item_label(item) or "").strip()
        if label:
            probes.append(_normalize_model_alias(label))
            label_tokens = [tok for tok in _normalize_model_alias(label).split() if tok]
            if len(label_tokens) >= 2:
                probes.append(" ".join(label_tokens[:2]))
        color_val = str(item.get("color") or "").strip()
        if color_val:
            probes.append(_normalize_model_alias(color_val))
        for probe in probes:
            clean = str(probe or "").strip()
            if len(clean) < 3:
                continue
            if clean in low:
                hits.add(clean)
                if len(hits) >= 2:
                    return True
    return False


def _replace_reply_question(reply: str, new_question: str) -> str:
    candidate = str(reply or "").strip()
    question = str(new_question or "").strip()
    if not question:
        return candidate
    if not candidate:
        return question
    parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(candidate) if part.strip()]
    if not parts:
        return question
    kept = [part for part in parts if "?" not in part]
    if kept:
        return " ".join(kept + [question]).strip()
    return question


def _generic_question_for_fact(fact_key: str) -> str:
    key = _canonical_fact_key(fact_key)
    prompts = {
        "city": "Подскажите, пожалуйста, в каком городе нужна установка?",
        "object_type": "Подскажите тип объекта: квартира или частный дом?",
        "address": "Подскажите, пожалуйста, адрес установки.",
        "model": "Подскажите, какая модель или тип из каталога интересует?",
        "budget": "Какой бюджет рассматриваете?",
        "timeline": "Когда планируете установку?",
        "dimensions": "Подскажите размеры проема, если уже есть замер.",
        "contact": "Оставьте, пожалуйста, удобный контакт для связи.",
    }
    return str(prompts.get(str(key or "").strip(), "") or "").strip()


def _persona_driven_question_for_fact(
    persona_context: str,
    fact_key: str,
    *,
    state: SalesState | None = None,
) -> str:
    question = _persona_question_for_fact(persona_context, fact_key)
    if question:
        return question
    script_question = _persona_primary_script_question(persona_context, state=state)
    if script_question and _question_covers_fact(script_question, fact_key):
        return script_question
    return _generic_question_for_fact(fact_key)


def _has_substantive_non_question_payload(text: str) -> bool:
    candidate = str(text or "").strip()
    if not candidate:
        return False
    segments = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
    if not segments:
        segments = [candidate]
    for segment in segments:
        if "?" in segment:
            continue
        probe = str(segment or "").strip()
        if len(probe) < 16:
            continue
        if (
            _PRICE_INLINE_RE.search(probe)
            or _PRICE_THOUSANDS_RE.search(probe)
            or "%" in probe
            or "₽" in probe
        ):
            return True
        tokens = [
            tok
            for tok in _FACT_TOKEN_RE.findall(_normalize_text(probe))
            if len(tok) >= 3 and tok not in _GENERIC_FACT_STOPWORDS
        ]
        if len(tokens) >= 3:
            return True
    return False


def _enforce_next_required_fact_question(
    reply: str,
    *,
    state: SalesState,
    persona_context: str,
    known_facts: Mapping[str, str] | None = None,
    user_text: str = "",
    grounding: Mapping[str, Any] | None = None,
) -> tuple[str, str]:
    required = _required_facts_from_persona_text(persona_context)
    if not required:
        return (reply or "").strip(), ""
    facts = dict(known_facts or _state_facts_snapshot(state))
    missing = _missing_required_facts(required, facts)
    if not missing:
        return (reply or "").strip(), ""

    next_key = _canonical_fact_key(missing[0]) or ""
    if not next_key:
        return (reply or "").strip(), ""

    user_raw = str(user_text or "").strip()
    turn_intent = _classify_turn_intent(user_raw, known_facts=facts)
    candidate = (reply or "").strip()
    if _reply_contains_unconfirmed_required_claim(
        candidate,
        missing_required=missing,
        known_facts=facts,
        user_text=user_raw,
        persona_context=persona_context,
    ):
        candidate = ""
    if turn_intent == "offtopic":
        if candidate:
            return candidate, ""
        return "Готов помочь по вашему запросу.", ""
    user_low = user_raw.lower()
    candidate_substantive = _has_substantive_non_question_payload(candidate) or (
        len(candidate) >= 72 and candidate.count("?") <= 1
    ) or (len(candidate) >= 46 and bool(_extract_questions_from_text(candidate)))
    payment_intent = _is_payment_intent(user_raw)
    store_address_intent = _is_store_address_intent(user_raw)
    handoff_intent = _is_channel_handoff_intent(user_raw)
    deferral_intent = _is_deferral_message(user_raw)
    grounding_items = _grounding_catalog_items(grounding)
    selected_item = _selected_item_from_grounding(grounding, grounding_items)
    direct_followup_intent = (
        _is_price_intent(user_raw)
        or bool(_MODEL_NAME_INTENT_RE.search(user_raw))
        or bool(_extract_attribute_probe(user_raw))
    )
    # Guard conflict resolver: do not overwrite a direct grounded answer
    # with a mandatory next-step question.
    if candidate and selected_item is not None and direct_followup_intent:
        return candidate, ""

    # Payment/requisites dialogues should not be derailed by mandatory qualification questions.
    if payment_intent and candidate:
        payment_questions: List[str] = []
        for q in _extract_questions_from_text(candidate):
            if (
                _question_covers_fact(q, "city")
                or _question_covers_fact(q, "address")
                or _question_covers_fact(q, "object_type")
            ):
                continue
            payment_questions.append(q.strip())
        if payment_questions:
            return " ".join(payment_questions), ""
        return candidate, ""
    if handoff_intent and candidate:
        return candidate, ""

    if store_address_intent:
        city_map = _extract_store_addresses_from_persona(persona_context)
        known_city = (
            str(
                facts.get("city")
                or (state.known_slots.get("city") if isinstance(state.known_slots, dict) else "")
                or ""
            )
            .strip()
            .lower()
            .replace("ё", "е")
        )
        if (not known_city) and city_map and isinstance(state.history, list):
            for item in reversed(state.history):
                if str(item.get("role") or "").strip().lower() != "user":
                    continue
                txt = str(item.get("content") or "").lower().replace("ё", "е")
                for city_key in city_map.keys():
                    if city_key and city_key in txt:
                        known_city = city_key
                        break
                if known_city:
                    break
        if (not known_city) and isinstance(state.history, list):
            for item in reversed(state.history):
                if str(item.get("role") or "").strip().lower() != "user":
                    continue
                txt_raw = str(item.get("content") or "").strip()
                hint = _extract_city_hint(txt_raw, allow_standalone=True)
                if hint:
                    known_city = hint.lower().replace("ё", "е")
                    break
        if known_city:
            for city_key, address in city_map.items():
                if city_key and (city_key in known_city or known_city in city_key):
                    return address, ""
        # Address/store request without city: ask city directly, but keep persona wording.
        return _persona_driven_question_for_fact(persona_context, "city", state=state), "city"

    if next_key == "model":
        items = grounding_items
        asks_price_or_name = _is_price_intent(user_raw) or bool(
            _MODEL_NAME_INTENT_RE.search(user_raw)
        )
        asks_model_options = bool(_VARIANTS_USER_HINT_RE.search(user_raw)) or asks_price_or_name
        asks_selected_attribute = bool(selected_item) and bool(
            _selected_item_attribute_answer(user_raw, selected_item)
        )
        user_explicit_model = bool(_best_catalog_item_match(user_raw, items)) if items else False
        candidate_has_substance = candidate_substantive
        persona_model_question = _persona_driven_question_for_fact(
            persona_context,
            "model",
            state=state,
        )
        existing_questions = _extract_questions_from_text(candidate)
        if (
            existing_questions
            and (not asks_model_options)
            and (not user_explicit_model)
            and any(_question_lists_catalog_options(q, items) for q in existing_questions)
        ):
            return _replace_reply_question(candidate, persona_model_question), "model"

        # Guard conflict resolver:
        # if the user asked a direct factual follow-up and reply already contains a substantive answer,
        # do not override with generic "what model did you like?" question.
        if (
            candidate
            and candidate_has_substance
            and (asks_price_or_name or asks_selected_attribute or user_explicit_model)
        ):
            return candidate, ""

        cannot_open_catalog = bool(_CATALOG_UNAVAILABLE_RE.search(user_low))
        if (not cannot_open_catalog) and "каталог" in user_low:
            if _LOW_SIGNAL_CONTEXT_RE.search(user_low):
                cannot_open_catalog = True
            elif ("не могу" in user_low or "не получается" in user_low) and "посмотр" in user_low:
                cannot_open_catalog = True
        if cannot_open_catalog and candidate_substantive:
            return candidate, ""
        if candidate_substantive:
            # LLM produced substantive guidance; do not overwrite with rigid next-step prompt.
            if any(_question_covers_fact(item, "model") for item in _extract_questions_from_text(candidate)):
                return candidate, "model"
            return candidate, ""

    # Keep strict progression only for core qualification facts.
    # For domain-specific flows (education, legal, etc.) avoid overriding natural reply logic.
    if next_key not in {"city", "address", "object_type", "model"}:
        return (reply or "").strip(), ""

    if next_key == "city" and _is_price_intent(user_raw):
        cand_low = _normalize_text(candidate)
        if candidate and ("город" in cand_low or "населен" in cand_low):
            return candidate, ""
        if candidate and not _extract_questions_from_text(candidate):
            city_question = _persona_driven_question_for_fact(persona_context, "city", state=state)
            if city_question:
                return f"{candidate} {city_question}".strip(), "city"
            return candidate, ""

    if deferral_intent and candidate:
        return candidate, ""

    if next_key == "city" and _is_price_intent(user_raw):
        if candidate_substantive:
            return candidate, ""
        price_floor = _catalog_min_price(grounding_items)
        if price_floor:
            lead = f"Цена зависит от модели, старт от {_format_rub_price(price_floor)}."
        else:
            lead = "Цена зависит от модели."
        question = _persona_driven_question_for_fact(persona_context, "city", state=state)
        if question:
            return f"{lead} {question}".strip(), "city"
        return lead, ""

    question = _persona_driven_question_for_fact(persona_context, next_key, state=state)
    if not question:
        return candidate, ""
    existing_questions = _extract_questions_from_text(candidate)
    if existing_questions:
        # If reply is already meaningful and asks a coherent question,
        # do not hard-replace it with a scripted mandatory slot question.
        if candidate_substantive:
            if any(_question_covers_fact(item, next_key) for item in existing_questions):
                return candidate, next_key
            return candidate, ""
        if any(_question_covers_fact(item, next_key) for item in existing_questions):
            if candidate_substantive:
                return candidate, next_key
            for item in existing_questions:
                if _question_covers_fact(item, next_key):
                    # Keep the actual qualification question and drop service preambles.
                    return item.strip(), next_key
            return candidate, next_key
        # For core qualification chain we enforce the missing step question to keep
        # deterministic progression from persona script.
        if next_key in {"city", "address", "object_type", "model"}:
            if candidate_substantive:
                return candidate, ""
            return question, next_key
        # For non-core keys keep substantive guidance.
        if candidate_substantive:
            return candidate, ""
        return question, next_key
    if not candidate:
        return question, next_key
    if len(candidate) <= 120 and not candidate_substantive:
        # Keep short informative text and append one required question instead of replacing it.
        if _extract_questions_from_text(candidate):
            return candidate, next_key
        return f"{candidate} {question}".strip(), next_key
    if candidate_substantive:
        return candidate, ""
    return question, next_key


def _fact_keys_from_line(line: str) -> List[str]:
    low = str(line or "").lower().replace("ё", "е")
    if not low:
        return []
    mapping: Dict[str, tuple[str, ...]] = {
        "city": ("город", "населен", "локац"),
        "address": ("адрес", "улиц", "подъезд", "дом", "корп", "кв."),
        "object_type": ("квартир", "частный дом", "тип помещения", "тип объекта", "объект"),
        "model": ("что из каталога", "модель", "вариант"),
        "dimensions": ("размер", "проем", "проём", "фото проема", "фото проёма", "замер"),
        "budget": ("бюджет", "стоим", "цена"),
        "timeline": ("срок", "сегодня", "завтра", "дата"),
        "contact": ("контакт", "телефон", "мессендж"),
    }
    keys: List[str] = []
    for fact_key, tokens in mapping.items():
        if any(token in low for token in tokens):
            keys.append(fact_key)
    return keys


def _required_facts_from_persona_text(persona_context: str) -> List[str]:
    raw_text = str(persona_context or "")
    if not raw_text.strip():
        return []

    lines = [ln.strip() for ln in raw_text.splitlines()]
    primary_lines: List[str] = []
    secondary_lines: List[str] = []
    in_primary_block = False
    in_secondary_block = False

    for line in lines:
        if not line:
            continue
        low = line.lower().replace("ё", "е")
        is_heading = low.startswith("#")
        if is_heading and in_primary_block:
            in_primary_block = False
        if is_heading and in_secondary_block:
            in_secondary_block = False
        if any(
            token in low for token in ("диалог-скрипт", "скрипт диалога", "последовательно уточни")
        ):
            in_primary_block = True
            in_secondary_block = False
            continue
        if all(token in low for token in ("шаблон", "реплик")):
            in_secondary_block = True
            in_primary_block = False
            continue
        if in_primary_block:
            if (
                re.match(r"^\d+\)", low)
                or re.match(r"^\d+\.", low)
                or low.startswith("-")
                or low.startswith("•")
            ):
                primary_lines.append(line)
                continue
            primary_lines.append(line)
        elif in_secondary_block:
            if (
                re.match(r"^\d+\)", low)
                or re.match(r"^\d+\.", low)
                or low.startswith("-")
                or low.startswith("•")
            ):
                secondary_lines.append(line)
                continue
            secondary_lines.append(line)

    script_lines = primary_lines[:] if primary_lines else secondary_lines[:]

    # If both script sections missing, use only imperative lines as fallback.
    if not script_lines:
        for line in lines:
            low = line.lower().replace("ё", "е")
            if any(token in low for token in ("уточни", "спроси", "узнай", "получи", "собери")):
                script_lines.append(line)

    required: List[str] = []
    for line in script_lines:
        low = line.lower().replace("ё", "е")
        normalized = re.sub(r"^[\-\s•\d\).\(\"']+", "", low).strip()
        if normalized.startswith("если "):
            continue
        # Only explicit data-collection actions are treated as required.
        action_prefixes = (
            "уточ",
            "спрос",
            "узна",
            "получ",
            "собер",
            "подскаж",
            "пришли",
            "пришлите",
        )
        has_action = any(normalized.startswith(prefix) for prefix in action_prefixes)
        has_question = "?" in line
        if not has_action and not has_question:
            continue
        for key in _fact_keys_from_line(normalized):
            # By default we avoid hard-gating dialogue on fields that are often unknown early.
            if key in {"dimensions"} and "обязательно" not in normalized:
                continue
            required.append(key)

    seen: set[str] = set()
    ordered: List[str] = []
    for key in required:
        canonical = _canonical_fact_key(key)
        if canonical and canonical not in seen:
            seen.add(canonical)
            ordered.append(canonical)
    return ordered[:12]


def _persona_rules_cache_key(persona_text: str) -> str:
    raw = str(persona_text or "")
    return hashlib.sha1(raw.encode("utf-8")).hexdigest() if raw else ""


def _line_to_question(line: str) -> str:
    txt = str(line or "").strip()
    if not txt:
        return ""
    txt = re.sub(r"^[\-\s•\d\).\(\"']+", "", txt).strip()
    if not txt:
        return ""
    if "?" in txt:
        parts = _extract_questions_from_text(txt)
        if parts:
            return parts[0].strip()
    # Domain-agnostic imperative conversion for free-form persona scripts.
    imperative = re.match(
        r"(?iu)^(?:уточни(?:те)?|уточняй(?:те)?|уточнить|спроси(?:те)?|"
        r"спрашивай(?:те)?|спросить|узнай(?:те)?|узнавай(?:те)?|узнать|"
        r"выясни(?:те)?|выясняй(?:те)?|выяснить|получи(?:те)?|получай(?:те)?|"
        r"получить|собери(?:те)?|собирай(?:те)?|собрать|попроси(?:те)?|"
        r"попросить|определи(?:те)?|определить)\s*[,:-]?\s+(.+)$",
        txt,
    )
    if imperative:
        tail = str(imperative.group(1) or "").strip(" .")
        if tail:
            tail = re.sub(r"\([^)]{1,80}\)", "", tail).strip(" .")
            tail = re.sub(
                r"(?iu)\b(?:если\s+не\s+назван|если\s+не\s+указан|"
                r"при\s+известн\w+\s+город\w*|если\s+город\s+уже\s+назван)\b",
                "",
                tail,
            ).strip(" .,:;-")
            tail = re.sub(r"(?iu)^(?:у\s+клиента\s+)?", "", tail).strip(" .")
            if tail:
                tail_tokens = [tok for tok in _FACT_TOKEN_RE.findall(_normalize_text(tail)) if tok]
                if len(tail_tokens) <= 2:
                    inferred_keys = _fact_keys_from_line(txt)
                    if inferred_keys:
                        generic = _generic_question_for_fact(inferred_keys[0])
                        if generic:
                            return generic
                if tail[-1] not in "?!":
                    tail += "?"
                lead = tail[0].lower() + tail[1:] if tail else ""
                if lead.startswith("какая "):
                    return str(lead or "").strip()
                if lead.startswith("какой "):
                    return str(lead or "").strip()
                if lead.startswith("какие "):
                    return str(lead or "").strip()
                return str(lead or "").strip() if tail else ""
    return ""


def _infer_persona_template_condition_and_action(line: str) -> tuple[str, str]:
    clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(line or "")).strip()
    if ":" not in clean:
        return "", ""
    lhs, rhs = clean.split(":", 1)
    lhs = lhs.strip()
    rhs = rhs.strip()
    if not lhs:
        return "", ""
    low = lhs.lower().replace("ё", "е")
    if not (
        low.startswith("на ")
        or "ожидаемого ответа" in low
        or "на сообщение" in low
        or "на фразу" in low
    ):
        return "", ""
    quoted = [
        str(part or "").strip()
        for part in re.findall(r"[\"«]([^\"»]{1,220})[\"»]", lhs)
        if str(part or "").strip()
    ]
    if not quoted:
        return "", ""
    return quoted[0], rhs


def _persona_safe_uncertain_reply(persona_context: str) -> str:
    _ = str(persona_context or "")
    return ""


def _persona_direct_reply_for_user_turn(
    persona_context: str,
    *,
    last_user_message: str,
    known_facts: Mapping[str, str] | None = None,
    state: SalesState | None = None,
) -> str:
    persona_text = str(persona_context or "").strip()
    if not persona_text:
        return ""
    raw = str(last_user_message or "").strip()
    facts = dict(known_facts or {})
    if str(facts.get("model") or "").strip():
        if (
            "?" in raw
            or bool(_MODEL_NAME_INTENT_RE.search(raw))
            or bool(_VARIANTS_USER_HINT_RE.search(raw))
            or bool(_extract_attribute_probe(raw))
        ):
            return ""
    compiled = _compile_persona_rules(persona_text)
    if not compiled.conditionals:
        return ""
    for rule in compiled.conditionals:
        if not _conditional_rule_matches(
            rule,
            last_user_message=last_user_message,
            known_facts={},
            state=None,
        ):
            continue
        quoted = [
            str(part or "").strip()
            for part in re.findall(r"[\"«]([^\"»]{8,260})[\"»]", str(rule.action_text or ""))
            if str(part or "").strip()
        ]
        normalized: list[str] = []
        for part in quoted:
            chunk = re.sub(r"<\s*[^>\n]{1,40}\s*>", "", part).strip()
            if not chunk:
                continue
            if chunk[-1] not in ".!?":
                chunk += "."
            normalized.append(chunk)
        if normalized:
            return " ".join(normalized).strip()
    return ""


def _is_operator_like_question(question: str) -> bool:
    low = _normalize_text(question)
    if not low:
        return False
    if re.search(
        r"(?iu)\b(предложите|предложи|напишите|позвоните|перейдите|"
        r"переведите|зафиксируйте|оформите|согласуйте|свяжитесь)\b",
        low,
    ):
        return True
    if re.search(r"(?iu)\bи\b", low) and re.search(
        r"(?iu)\b(предлож|напиш|перевед|соглас|оформ|подтверд)\w*",
        low,
    ):
        return True
    return False


def _is_operator_instruction_sentence(text: str) -> bool:
    low = _normalize_text(text)
    if not low:
        return False
    if not re.search(r"(?iu)\b(сначала|затем|потом|далее|после\s+этого|в\s+этом\s+же\s+ответе)\b", low):
        return False
    has_delivery_hint = bool(
        re.search(
            r"(?iu)\b(отдельн\w*\s+сообщени\w*|только\s+ссылк\w*|"
            r"только\s+номер\w*|ссылк\w*|контакт\w*|телефон\w*|username|@)\b",
            low,
        )
    )
    if not has_delivery_hint:
        return False
    # If it is a natural user-facing question, keep it.
    if "?" in str(text or ""):
        return False
    return True


def _is_response_format_instruction_sentence(text: str) -> bool:
    low = _normalize_text(text)
    if not low:
        return False
    if "?" in str(text or ""):
        return False
    if re.fullmatch(r"(?iu)\s*не\s+одн\w*\s+строк\w*\s*\.?\s*", low):
        return True
    if re.search(
        r"(?iu)\b(отвечайт\w*|пишит\w*|напишит\w*)\b[^.?!]{0,64}\b"
        r"(развернут\w*|подробн\w*|односложн\w*|коротк\w*|одн\w*\s+строк\w*)\b",
        low,
    ):
        return True
    return False


def _is_sequence_process_instruction_sentence(text: str) -> bool:
    low = _normalize_text(text)
    if not low:
        return False
    if "?" in str(text or ""):
        return False
    if not re.search(r"(?iu)\b(сначала|затем|потом|далее|после\s+этого)\b", low):
        return False
    if not re.search(
        r"(?iu)\b(уточняйте|давайте|следуйте|спрашивайте|задавайте|предлагайте|предложите|фиксируйте|собирайте)\b",
        low,
    ):
        return False
    if re.search(
        r"(?iu)\b(ответ|скрипт|сценари\w*|персон\w*|географ\w*|этап\w*|шаг\w*|логик\w*|правил\w*)\b",
        low,
    ):
        return True
    return False


def _strip_embedded_operator_tail(text: str) -> str:
    candidate = str(text or "").strip()
    if not candidate:
        return candidate
    out = candidate
    # Remove leaked internal operator tails while preserving the useful head.
    out = re.sub(
        r"(?iu)[,;:\-\s]*(поздоровайт\w*|поприветствуйт\w*)\b[^.?!\n]*$",
        "",
        out,
    )
    out = re.sub(
        r"(?iu)[,;:\-\s]*(скажите|спросите|уточните|напишите)\s+что\b[^.?!\n]*$",
        "",
        out,
    )
    out = re.sub(
        r"(?iu)[,;:\-\s]*давайте\s+ответ\b[^.?!\n]*$",
        "",
        out,
    )
    out = re.sub(
        r"(?iu)[,;:\-\s]*ответ\s+строго\b[^.?!\n]*$",
        "",
        out,
    )
    out = re.sub(r"\s{2,}", " ", out).strip(" ,;:-")
    return out


def _extract_primary_script_lines(persona_text: str) -> List[str]:
    lines = [ln.strip() for ln in str(persona_text or "").splitlines()]
    primary_lines: List[str] = []
    in_primary_block = False
    for line in lines:
        if not line:
            continue
        low = line.lower().replace("ё", "е")
        is_heading = low.startswith("#")
        if is_heading and in_primary_block:
            in_primary_block = False
        if any(
            token in low
            for token in ("диалог-скрипт", "скрипт диалога", "последовательно уточни", "порядок диалога")
        ):
            in_primary_block = True
            continue
        if in_primary_block:
            if (
                re.match(r"^\d+\)", low)
                or re.match(r"^\d+\.", low)
                or low.startswith("-")
                or low.startswith("•")
            ):
                primary_lines.append(line)
                continue
            primary_lines.append(line)
    return primary_lines


def _persona_script_questions(persona_text: str) -> List[str]:
    candidates = _extract_primary_script_lines(persona_text)
    if not candidates:
        for line in str(persona_text or "").splitlines():
            clean = str(line or "").strip()
            if not clean:
                continue
            low = clean.lower().replace("ё", "е")
            if any(
                token in low
                for token in (
                    "уточни",
                    "уточняй",
                    "спроси",
                    "спрашивай",
                    "узнай",
                    "узнавай",
                    "получи",
                    "получай",
                    "собери",
                    "собирай",
                    "попроси",
                    "выясни",
                    "выясняй",
                )
            ):
                candidates.append(clean)
    questions: List[str] = []
    seen: set[str] = set()
    for raw in candidates:
        question = _line_to_question(raw)
        if not question:
            continue
        if _is_operator_like_question(question):
            continue
        key = _normalize_text(question)
        if not key or key in seen:
            continue
        seen.add(key)
        questions.append(question)
    return questions


def _persona_primary_script_question(
    persona_text: str,
    *,
    state: SalesState | None = None,
) -> str:
    questions = _persona_script_questions(persona_text)
    for question in questions:
        if not question:
            continue
        if state is not None and _is_repeated_question_against_state(question, state):
            continue
        return question
    if questions and state is None:
        return questions[0]
    return ""


def _persona_catalog_unavailable_reply(persona_text: str) -> str:
    lines = [str(line or "").strip() for line in str(persona_text or "").splitlines() if str(line or "").strip()]
    for line in lines:
        low = _normalize_text(line)
        if "каталог" not in low:
            continue
        if not (
            "не открыл" in low
            or "не открыва" in low
            or "пока не открыл" in low
            or "груз" in low
        ):
            continue
        quoted = [
            str(part or "").strip()
            for part in re.findall(r"[\"«]([^\"»]{8,260})[\"»]", line)
            if str(part or "").strip()
        ]
        if quoted:
            reply = quoted[0].strip()
            if reply and reply[-1] not in ".!?":
                reply += "."
            return reply
        cleaned = re.sub(r"^[\-\s•\d\).:]+", "", line).strip()
        cleaned = re.sub(r"(?iu)^нормально:\s*", "", cleaned).strip()
        if cleaned and cleaned[-1] not in ".!?":
            cleaned += "."
        return cleaned
    return ""


def _explain_missing_fact_need(
    fact_key: str,
    *,
    persona_context: str = "",
) -> str:
    key = _canonical_fact_key(fact_key)
    if not key:
        return ""
    overrides = {
        "city": "Город нужен, чтобы сразу подсказать адрес магазина, условия и ближайший вариант установки.",
        "object_type": "Уточняю тип объекта, потому что для квартиры и частного дома подходят разные варианты.",
        "address": "Адрес нужен, чтобы не ошибиться по выезду, установке и дальнейшему подбору.",
        "model": "Хочу понять, какой вариант Вам ближе, чтобы не промахнуться по цене и характеристикам.",
        "contact": "Контакт нужен, чтобы подтвердить заказ и отправить точные детали.",
    }
    reply = str(overrides.get(key) or "").strip()
    if not reply:
        return ""
    if reply[-1] not in ".!?":
        reply += "."
    return reply


def _fallback_contextual_question(
    user_text: str,
    *,
    state: SalesState | None = None,
    persona_context: str = "",
) -> str:
    raw = str(user_text or "").strip()
    known_facts = _state_facts_snapshot(state) if state is not None else {}
    current_pending = _canonical_fact_key(str(getattr(state, "pending_fact_key", "") or ""))
    if known_facts.get("model"):
        if (
            bool(_extract_attribute_probe(raw))
            or bool(_MODEL_NAME_INTENT_RE.search(raw))
            or bool(_VARIANTS_USER_HINT_RE.search(raw))
        ):
            return ""
    if persona_context and state is not None:
        required = _required_facts_from_persona_text(persona_context)
        missing = _missing_required_facts(required, known_facts)
        if missing:
            persona_question = _persona_driven_question_for_fact(
                persona_context,
                missing[0],
                state=state,
            )
            if persona_question and not _is_repeated_question_against_state(persona_question, state):
                return persona_question
    if "?" in raw and not (state is not None and current_pending == "model" and bool(state.last_items)):
        if state is not None and current_pending:
            direct = _persona_driven_question_for_fact(persona_context, current_pending, state=state)
            if direct and not _is_repeated_question_against_state(direct, state):
                return direct
    topic_tokens = [
        tok
        for tok in _FACT_TOKEN_RE.findall(_normalize_text(raw))
        if len(tok) >= 4 and tok not in NEEDS_STOPWORDS and tok not in _GENERIC_FACT_STOPWORDS
    ]
    topic = topic_tokens[0] if topic_tokens else ""
    candidates: list[str]
    if not raw:
        candidates = [""]
    elif _ORDER_INTENT_RE.search(raw):
        candidates = [""]
    elif _is_price_intent(raw):
        candidates = [""]
    elif _MODEL_NAME_INTENT_RE.search(raw) or _VARIANTS_USER_HINT_RE.search(raw):
        candidates = [""]
    elif _ETA_INTENT_RE.search(raw):
        candidates = [""]
    else:
        candidates = [""]
    for candidate in candidates:
        if not candidate:
            continue
        if state is not None and _is_repeated_question_against_state(candidate, state):
            continue
        return candidate
    return ""


def _extract_expected_tokens_from_condition(text: str) -> List[str]:
    raw = str(text or "").strip().lower().replace("ё", "е")
    if not raw:
        return []
    tokens = [tok for tok in re.findall(r"[a-zа-я0-9\-]{2,}", raw)]
    stop = {
        "если",
        "клиент",
        "когда",
        "после",
        "при",
        "то",
        "или",
        "и",
        "а",
        "не",
        "из",
        "в",
        "во",
        "на",
        "по",
        "город",
        "адрес",
        "квартира",
        "дом",
        "помещение",
        "тип",
        "объект",
        "модель",
        "бюджет",
        "срок",
        "контакт",
        "просит",
        "попросил",
        "попросила",
        "пишет",
        "написал",
        "написала",
        "ответил",
        "ответила",
        "сказал",
        "сказала",
        "указал",
        "указала",
        "уточнил",
        "уточнила",
        "нужно",
        "нужен",
        "нужна",
        "нужны",
        "можно",
        "давайте",
        "давай",
        "надо",
        "хочу",
        "отправить",
        "отправляй",
        "отправляйте",
        "скинуть",
        "скинь",
        "скиньте",
        "перейти",
        "продолжить",
        "продолжим",
        "пользователь",
        "название",
        "формате",
        "ответьте",
        "естественно",
        "извлеките",
        "назвал",
        "назван",
        "затем",
        "дает",
        "дал",
        "только",
        "любой",
        "любого",
        "города",
        "городов",
        "республики",
        "этих",
        "включая",
        "направлении",
        "известен",
        "известном",
    }
    out: List[str] = []
    for tok in tokens:
        if tok in stop:
            continue
        if len(tok) < 3:
            continue
        if tok not in out:
            out.append(tok)
    return out[:8]


def _extract_contact_artifacts(text: str) -> List[str]:
    raw = str(text or "")
    if not raw:
        return []

    def _clean_artifact_token(kind: str, token: str) -> str:
        out = str(token or "").strip()
        if not out:
            return out
        if kind == "url":
            out = out.strip("<>[](){}")
            out = out.rstrip(".,;:!?\"'»")
        elif kind == "handle":
            out = out.rstrip(".,;:!?\"'»")
        elif kind == "phone":
            out = re.sub(r"\s+", " ", out).strip()
        return out

    def _artifact_dedupe_key(kind: str, token: str) -> str:
        cleaned = _clean_artifact_token(kind, token)
        if kind == "phone":
            return re.sub(r"\D+", "", cleaned)
        return cleaned.lower()

    found: List[tuple[int, str, str]] = []
    for match in _CONTACT_URL_RE.finditer(raw):
        token = _clean_artifact_token("url", str(match.group(0) or ""))
        if token:
            found.append((match.start(), "url", token))
    for match in _CONTACT_HANDLE_RE.finditer(raw):
        token = _clean_artifact_token("handle", str(match.group(0) or ""))
        if token:
            found.append((match.start(), "handle", token))
    for match in _CONTACT_PHONE_RE.finditer(raw):
        token = _clean_artifact_token("phone", str(match.group(0) or ""))
        if token and _is_plausible_contact_phone(token):
            found.append((match.start(), "phone", token))
    if not found:
        return []
    found.sort(key=lambda item: item[0])
    out: List[str] = []
    seen: set[str] = set()
    for _, kind, token in found:
        normalized = _artifact_dedupe_key(kind, token)
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(token)
    return out[:12]


def _detect_persona_line_channels(text: str) -> List[str]:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return []
    channels: List[str] = []
    markers = (
        ("avito", ("avito", "авито")),
        ("telegram", ("telegram", "телеграм", "телега", "тг")),
        ("whatsapp", ("whatsapp", "ватсап", "вотсап")),
        ("max", (" max ", " max.", " max,", " max)", " max/")),
    )
    for name, variants in markers:
        if any(variant in f" {low} " for variant in variants):
            channels.append(name)
    return channels


def _is_delivery_directive_line(text: str) -> bool:
    low = str(text or "").strip().lower().replace("ё", "е")
    if not low:
        return False
    if re.search(r"(?iu)\bне\s+(предлаг|отправ|скид|пиш|дава|прос|остав)", low):
        return False
    contact_markers = (
        "telegram",
        "телеграм",
        "тг",
        "whatsapp",
        "ватсап",
        "вотсап",
        "контакт",
        "номер",
        "телефон",
        "мессендж",
        "ссылк",
        "@",
    )
    action_markers = (
        "предлаг",
        "продолж",
        "перейд",
        "отправ",
        "скиды",
        "скинь",
        "напиш",
        "пиши",
        "остав",
        "попрос",
        "просите",
        "укаж",
        "дайте",
        "связ",
    )
    return any(marker in low for marker in contact_markers) and any(
        marker in low for marker in action_markers
    )


def _delivery_rule_from_line(
    *,
    source_line: str,
    channel_scope: List[str] | None = None,
    condition_text: str = "",
) -> PersonaDeliveryRule:
    clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(source_line or "")).strip()
    low = clean.lower().replace("ё", "е")
    wants_handle = "@" in low or any(
        marker in low for marker in ("username", "юзернейм", "ник", "логин")
    )
    wants_phone = any(marker in low for marker in ("номер", "телефон", "звон", "контакт"))
    wants_link = any(marker in low for marker in ("ссылк", "http"))
    if not (wants_handle or wants_phone or wants_link):
        wants_handle = True
        wants_phone = True
    gap = 2
    gap_match = re.search(r"(?iu)не\s+чаще[^\d]*(\d+)", low)
    if gap_match:
        try:
            gap = max(1, int(gap_match.group(1)))
        except Exception:
            gap = 2
    return PersonaDeliveryRule(
        source_line=clean,
        channel_scope=list(channel_scope or []),
        condition_text=str(condition_text or "").strip(),
        expected_tokens=_extract_expected_tokens_from_condition(condition_text),
        wants_handle=wants_handle,
        wants_phone=wants_phone,
        wants_link=wants_link,
        min_assistant_gap=gap,
    )


def _infer_delivery_condition_from_line(source_line: str) -> str:
    clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(source_line or "")).strip()
    if ":" not in clean:
        return ""
    lhs, rhs = clean.split(":", 1)
    lhs = lhs.strip()
    rhs = rhs.strip()
    if not lhs or not rhs:
        return ""
    # Heuristic: treat the left side as trigger phrase only when it clearly looks
    # like user wording/examples (quoted alternatives), not an instruction header.
    has_quote = any(mark in lhs for mark in ('"', "«", "»", "`", "'"))
    has_alternatives = "/" in lhs
    if not (has_quote or has_alternatives):
        return ""
    lhs = lhs.strip(" \t'\"`«»")
    if not lhs or len(lhs) > 180:
        return ""
    return lhs


def _compile_persona_rules(persona_text: str) -> PersonaCompiledRules:
    key = _persona_rules_cache_key(persona_text)
    if key:
        cached = _PERSONA_RULES_CACHE.get(key)
        if cached is not None:
            return cached
    text = str(persona_text or "")
    compiled = PersonaCompiledRules()
    compiled.contact_artifacts = _extract_contact_artifacts(text)

    def _append_conditional_rule(
        *,
        condition_text: str,
        action_text: str,
        source_line: str,
        channel_scope: List[str] | None = None,
    ) -> None:
        cond = str(condition_text or "").strip()
        action = str(action_text or "").strip()
        if not cond or not action:
            return
        cond_keys = _fact_keys_from_line(cond)
        fact_key = _canonical_fact_key(cond_keys[0]) if cond_keys else ""
        compiled.conditionals.append(
            PersonaConditionalRule(
                source_line=str(source_line or "").strip() or f"{cond}: {action}",
                condition_text=cond,
                action_text=action,
                fact_key=fact_key,
                expected_tokens=_extract_expected_tokens_from_condition(cond),
            )
        )
        if _is_delivery_directive_line(action):
            compiled.delivery_rules.append(
                _delivery_rule_from_line(
                    source_line=action,
                    channel_scope=list(channel_scope or []),
                    condition_text=cond,
                )
            )

    for line in _extract_primary_script_lines(text):
        clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", line).strip()
        if not clean:
            continue
        low = clean.lower().replace("ё", "е")
        if low.startswith("если "):
            continue
        keys = _fact_keys_from_line(clean)
        if not keys:
            continue
        question = _line_to_question(clean)
        canonical = ""
        question_low = question.lower().replace("ё", "е") if question else ""
        canonical_keys = [_canonical_fact_key(raw_key) for raw_key in keys]
        if (
            "object_type" in canonical_keys
            and question_low
            and ("квартир" in question_low or "частн" in question_low or "тип объект" in question_low)
        ):
            canonical = "object_type"
        elif "address" in canonical_keys and question_low and _question_covers_fact(question, "address"):
            canonical = "address"
        elif "model" in canonical_keys and question_low and _question_covers_fact(question, "model"):
            canonical = "model"
        if question and not canonical:
            for raw_key in keys:
                probe = _canonical_fact_key(raw_key)
                if probe and _question_covers_fact(question, probe):
                    canonical = probe
                    break
        if not canonical:
            canonical = _canonical_fact_key(keys[0])
        if not canonical:
            continue
        if any(step.fact_key == canonical for step in compiled.steps):
            continue
        compiled.steps.append(
            PersonaStepRule(
                fact_key=canonical,
                source_line=clean,
                question=question,
            )
        )

    section_scope: List[str] = []
    pending_conditional_text = ""
    pending_conditional_source = ""
    pending_conditional_scope: List[str] = []
    pending_template_text = ""
    pending_template_source = ""
    pending_template_scope: List[str] = []
    for raw_line in text.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        clean_line = re.sub(r"^[\-\s•\d\).\(\"']+", "", line).strip()
        clean_low = clean_line.lower().replace("ё", "е")
        if line.startswith("#"):
            pending_conditional_text = ""
            pending_conditional_source = ""
            pending_conditional_scope = []
            pending_template_text = ""
            pending_template_source = ""
            pending_template_scope = []
            section_scope = _detect_persona_line_channels(line)
            continue
        line_scope = _detect_persona_line_channels(line)
        effective_scope = section_scope or line_scope
        if pending_conditional_text:
            # Support two-line persona rules:
            # "Если ...:" on one line and action on the next line.
            if not clean_low.startswith("если "):
                _append_conditional_rule(
                    condition_text=pending_conditional_text,
                    action_text=clean_line,
                    source_line=f"{pending_conditional_source} {clean_line}".strip(),
                    channel_scope=pending_conditional_scope or effective_scope,
                )
                pending_conditional_text = ""
                pending_conditional_source = ""
                pending_conditional_scope = []
                continue
            pending_conditional_text = ""
            pending_conditional_source = ""
            pending_conditional_scope = []
        if pending_template_text:
            if not clean_low.startswith("если "):
                _append_conditional_rule(
                    condition_text=pending_template_text,
                    action_text=clean_line,
                    source_line=f"{pending_template_source} {clean_line}".strip(),
                    channel_scope=pending_template_scope or effective_scope,
                )
                pending_template_text = ""
                pending_template_source = ""
                pending_template_scope = []
                continue
            pending_template_text = ""
            pending_template_source = ""
            pending_template_scope = []
        if clean_line and (not clean_low.startswith("если ")) and _is_delivery_directive_line(clean_line):
            inferred_cond = _infer_delivery_condition_from_line(clean_line)
            compiled.delivery_rules.append(
                _delivery_rule_from_line(
                    source_line=clean_line,
                    channel_scope=effective_scope,
                    condition_text=inferred_cond,
                )
            )
        if clean_line and (not clean_low.startswith("если ")):
            inferred_cond, inferred_action = _infer_persona_template_condition_and_action(clean_line)
            if inferred_cond and inferred_action:
                _append_conditional_rule(
                    condition_text=inferred_cond,
                    action_text=inferred_action,
                    source_line=clean_line,
                    channel_scope=effective_scope,
                )
            elif inferred_cond:
                pending_template_text = inferred_cond
                pending_template_source = clean_line
                pending_template_scope = list(effective_scope or [])
        if not clean_low.startswith("если "):
            continue
        cond = ""
        action = ""
        if ":" in clean_line:
            cond, action = clean_line.split(":", 1)
        elif " - " in clean_line:
            cond, action = clean_line.split(" - ", 1)
        else:
            m_to = re.search(r"(?iu)\bто\b", clean_low)
            if m_to:
                idx = m_to.start()
                cond = clean_line[:idx].strip()
                action = clean_line[m_to.end() :].strip(" -:")
        cond = cond.strip()
        action = action.strip()
        if not cond:
            continue
        if not action:
            pending_conditional_text = cond
            pending_conditional_source = clean_line
            pending_conditional_scope = list(effective_scope or [])
            continue
        _append_conditional_rule(
            condition_text=cond,
            action_text=action,
            source_line=clean_line,
            channel_scope=effective_scope,
        )

    if compiled.delivery_rules:
        unique_delivery: List[PersonaDeliveryRule] = []
        seen_delivery: set[str] = set()
        for rule in compiled.delivery_rules:
            signature = "|".join(
                [
                    ",".join(sorted(rule.channel_scope or [])),
                    (rule.condition_text or "").lower(),
                    (rule.source_line or "").lower(),
                    "h" if rule.wants_handle else "-",
                    "p" if rule.wants_phone else "-",
                    "l" if rule.wants_link else "-",
                ]
            )
            if signature in seen_delivery:
                continue
            seen_delivery.add(signature)
            unique_delivery.append(rule)
        compiled.delivery_rules = unique_delivery

    if key:
        _PERSONA_RULES_CACHE[key] = compiled
        if len(_PERSONA_RULES_CACHE) > 128:
            # Keep cache bounded without expensive LRU.
            for stale_key in list(_PERSONA_RULES_CACHE.keys())[:32]:
                _PERSONA_RULES_CACHE.pop(stale_key, None)
    return compiled


def _persona_question_for_fact(persona_context: str, fact_key: str) -> str:
    compiled = _compile_persona_rules(persona_context)
    canonical = _canonical_fact_key(fact_key)
    if not canonical:
        return ""
    for step in compiled.steps:
        if step.fact_key == canonical and step.question:
            return step.question
    return ""


def _resolve_persona_rules_context(
    *,
    tenant: int | None,
    channel_name: str,
    fallback_context: str = "",
) -> str:
    """Return clean persona text for rule extraction (without system metadata)."""
    if tenant is not None:
        try:
            raw = str(load_persona(int(tenant), channel_name) or "").strip()
            if raw:
                return raw
        except Exception:
            pass
    return str(fallback_context or "").strip()


def _conditional_rule_matches(
    rule: PersonaConditionalRule,
    *,
    last_user_message: str,
    known_facts: Mapping[str, str] | None = None,
    state: SalesState | None = None,
) -> bool:
    cond = _normalize_text(rule.condition_text)
    if not cond:
        return False
    haystack = _normalize_text(last_user_message)
    facts = dict(known_facts or {})
    if isinstance(state, SalesState) and isinstance(state.facts, dict):
        for k, v in state.facts.items():
            if k not in facts and str(v or "").strip():
                facts[k] = str(v)
    if rule.fact_key:
        fact_val = _normalize_text(str(facts.get(rule.fact_key) or ""))
        if fact_val:
            haystack = f"{haystack} {fact_val}".strip()
    if not haystack:
        return False
    if not rule.expected_tokens:
        return False
    fact_probe = _normalize_probe_token(str(facts.get(rule.fact_key) or "")) if rule.fact_key else ""
    hay_tokens = [_normalize_probe_token(tok) for tok in _FACT_TOKEN_RE.findall(haystack)]

    def _near_token_form(a: str, b: str) -> bool:
        aa = str(a or "").strip().lower()
        bb = str(b or "").strip().lower()
        if not aa or not bb:
            return False
        if aa == bb:
            return True
        if len(aa) >= 3 and len(bb) >= 3 and aa[:-1] == bb[:-1]:
            return True
        if len(aa) >= 4 and len(bb) >= 4 and aa[:3] == bb[:3] and abs(len(aa) - len(bb)) <= 1:
            return True
        return False

    for token in rule.expected_tokens:
        tok = str(token or "").strip().lower()
        if not tok:
            continue
        if re.search(rf"(?iu)(?<![\w-]){re.escape(tok)}(?![\w-])", haystack):
            return True
        tok_probe = _normalize_probe_token(tok)
        if tok_probe and any(tok_probe == h or tok_probe in h or h in tok_probe for h in hay_tokens if h):
            return True
        if tok_probe and any(_near_token_form(tok_probe, h) for h in hay_tokens if h):
            return True
        if fact_probe and tok_probe and (tok_probe in fact_probe or fact_probe in tok_probe):
            return True
        if fact_probe and tok_probe and _near_token_form(tok_probe, fact_probe):
            return True
    return False


def _apply_persona_sequence_obligations(
    reply: str,
    *,
    persona_context: str,
    last_user_message: str,
    known_facts: Mapping[str, str] | None = None,
    state: SalesState | None = None,
) -> str:
    candidate = (reply or "").strip()
    persona_text = str(persona_context or "").strip()
    if not candidate or not persona_text:
        return candidate
    facts = dict(known_facts or {})
    turn_intent = _classify_turn_intent(last_user_message, known_facts=facts)
    if turn_intent in {"why_question", "repair", "catalog_problem"}:
        return _strip_instruction_leaks(candidate)
    if isinstance(state, SalesState) and _canonical_fact_key(str(state.pending_fact_key or "")) == "model":
        low_candidate = _normalize_text(candidate)
        if (
            "модел" in low_candidate
            or "каталог" in low_candidate
            or "вариант" in low_candidate
        ):
            return _strip_instruction_leaks(candidate)
    if str(facts.get("model") or "").strip():
        if (
            bool(_extract_attribute_probe(last_user_message))
            or bool(_MODEL_NAME_INTENT_RE.search(str(last_user_message or "")))
            or bool(_VARIANTS_USER_HINT_RE.search(str(last_user_message or "")))
        ):
            return _strip_instruction_leaks(candidate)
    compiled = _compile_persona_rules(persona_text)
    if not compiled.conditionals:
        return candidate

    def _normalize_action_for_reply(action_text: str) -> str:
        raw = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(action_text or "")).strip()
        raw = raw.replace("`", " ")
        if not raw:
            return ""
        low = raw.lower().replace("ё", "е")
        if low.startswith("не "):
            return ""
        # Persona sometimes stores meta-instructions like:
        # "в этом же ответе добавьте: "..." и "...""
        # Extract quoted user-facing fragments first.
        quoted_parts = [
            str(part or "").strip()
            for part in re.findall(r"[\"«]([^\"»]{3,220})[\"»]", raw)
            if str(part or "").strip()
        ]
        if quoted_parts:
            normalized_chunks: list[str] = []
            for chunk in quoted_parts:
                raw_chunk = str(chunk or "")
                if re.search(r"<\s*[^>\n]{1,40}\s*>", raw_chunk):
                    continue
                chunk_clean = re.sub(r"<\s*[^>\n]{1,40}\s*>", "", raw_chunk).strip()
                if not chunk_clean:
                    continue
                if chunk_clean[-1] not in ".!?":
                    chunk_clean += "."
                normalized_chunks.append(chunk_clean)
            merged = " ".join(normalized_chunks).strip()
            if merged:
                return merged[:220].strip()
        # Question-like directives should stay in planning, not in final text.
        if re.match(
            r"(?iu)^\s*(?:сначала|затем|потом|далее|обязательно|просто)?\s*"
            r"(спросите|уточните|попросите|напишите|задайте)\b",
            low,
        ):
            return ""
        # Internal infinitive instructions should not leak into user text.
        if re.match(
            r"(?iu)^\s*(?:сначала|затем|потом|далее|обязательно|просто)?\s*"
            r"(предупредить|уточнить|добавить|сообщить|указать|"
            r"предложить|спросить|попросить|написать|отправить|"
            r"перевести|соотнести|проверить|зафиксировать)\b",
            low,
        ):
            return ""
        # Purely internal delivery instructions should not leak to user text.
        if re.search(
            r"(?iu)\b(в\s+этом\s+же\s+ответе|в\s+текущем\s+сообщении|"
            r"только\s+потом|переходить\s+к\s+уточнениям|"
            r"добавьте\s*:\s*$|дать\s+адрес\s+магазина\s+и\s+условие\s+скидки)\b",
            low,
        ):
            return ""
        # Pure operator directives ("how to ask") must not leak into user text.
        if re.search(
            r"(?iu)\b(при\s+известном\s+городе|без\s+повтора|"
            r"сразу\s+давайте\s+адрес|затем\s+один\s+уточняющ)\b",
            low,
        ):
            return ""
        # Convert imperative instruction stems to neutral phrasing.
        stripped = re.sub(
            r"(?iu)^\s*(?:честно\s+|сразу\s+|коротко\s+|обязательно\s+)*"
            r"(предлагайте|сообщайте|добавьте|укажите|пишите|отправляйте)\s+",
            "",
            raw,
        ).strip(" ,")
        if not stripped:
            stripped = raw
        stripped = _strip_embedded_operator_tail(stripped).strip()
        if not stripped:
            return ""
        low_stripped = stripped.lower().replace("ё", "е")
        # Root-cause guard: never allow operator imperatives to be appended as
        # user-visible text from persona conditional actions.
        if ("?" not in stripped) and re.search(
            r"(?iu)\b(поздоровайт\w*|поприветствуйт\w*|скажите|спросите|"
            r"уточните|напишите|дайте|предложите|предлагайте|попросите|задайте)\b",
            low_stripped,
        ):
            return ""
        # Minor normalization for common promo wording.
        if low_stripped.startswith("скидку "):
            stripped = "Действует скидка " + stripped[len("скидку ") :]
        elif low_stripped.startswith("скидка "):
            stripped = "Действует " + stripped
        if stripped and stripped[-1] not in ".!?":
            stripped += "."
        return stripped[:220].strip()

    def _reply_covers_action(reply_text: str, action_text: str) -> bool:
        rep = _normalize_text(reply_text)
        act = _normalize_text(action_text)
        if not rep or not act:
            return False
        action_numbers = set(re.findall(r"\d{2,}", action_text))
        if action_numbers and not action_numbers.issubset(set(re.findall(r"\d{2,}", reply_text))):
            return False
        action_tokens = [
            tok
            for tok in _FACT_TOKEN_RE.findall(act)
            if len(tok) >= 4 and tok not in _GENERIC_FACT_STOPWORDS
        ]
        if not action_tokens:
            return False
        hits = sum(1 for tok in action_tokens if tok in rep)
        return hits >= max(1, int(len(action_tokens) * 0.45))

    def _append_action_sentence(reply_text: str, sentence: str) -> str:
        base = str(reply_text or "").strip()
        addon = str(sentence or "").strip()
        if not addon:
            return base
        if not base:
            return addon
        if _count_sentences(base) >= 3:
            parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(base) if part.strip()]
            if parts:
                parts[-1] = f"{parts[-1]} {addon}".strip()
                return " ".join(parts).strip()
            return f"{base} {addon}".strip()
        if base.endswith((".", "!", "?")):
            return f"{base} {addon}".strip()
        return f"{base}. {addon}".strip()

    def _assistant_recently_covers_action(action_text: str, window: int = 6) -> bool:
        if not isinstance(state, SalesState):
            return False
        history = state.history if isinstance(state.history, list) else []
        if not history:
            return False
        checked = 0
        for item in reversed(history):
            role = str(item.get("role") or "").strip().lower()
            if role != "assistant":
                continue
            content = str(item.get("content") or "").strip()
            if not content:
                continue
            checked += 1
            if _reply_covers_action(content, action_text):
                return True
            if checked >= max(1, int(window)):
                break
        return False

    def _user_explicitly_requests_action(action_text: str) -> bool:
        user_msg = str(last_user_message or "").strip()
        if not user_msg:
            return False
        if _reply_covers_action(user_msg, action_text):
            return True
        action_tokens = [
            tok
            for tok in _FACT_TOKEN_RE.findall(_normalize_text(action_text))
            if len(tok) >= 4 and tok not in _GENERIC_FACT_STOPWORDS
        ]
        if not action_tokens:
            return False
        user_norm = _normalize_text(user_msg)
        hits = sum(1 for tok in action_tokens if tok in user_norm)
        user_tokens = [tok for tok in _FACT_TOKEN_RE.findall(user_norm) if len(tok) >= 3]
        if user_tokens and len(user_tokens) <= 5 and hits >= 1:
            return True
        return hits >= max(1, int(len(action_tokens) * 0.4))

    def _action_recently_in_memory(action_text: str) -> bool:
        if not isinstance(state, SalesState):
            return False
        fp = _fact_fingerprint(action_text)
        if not fp:
            return False
        recent = list(state.recent_fact_fingerprints or [])
        if fp in recent:
            return True
        cur_tokens = set(fp.split())
        if not cur_tokens:
            return False
        for prev in recent:
            prev_tokens = set(str(prev or "").split())
            if not prev_tokens:
                continue
            overlap = len(cur_tokens & prev_tokens)
            if overlap < 3:
                continue
            ratio = overlap / max(1, min(len(cur_tokens), len(prev_tokens)))
            if ratio >= 0.72:
                return True
        return False

    appended_fingerprints: set[str] = set()
    out = candidate
    for rule in compiled.conditionals:
        if not _conditional_rule_matches(
            rule,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        ):
            continue
        action = str(rule.action_text or "").strip()
        if not action:
            continue
        action_for_reply = _normalize_action_for_reply(action)
        if not action_for_reply:
            continue
        if _assistant_recently_covers_action(action_for_reply) and (
            not _user_explicitly_requests_action(action_for_reply)
        ):
            continue
        action_parts = [
            part.strip()
            for part in _SENTENCE_SPLIT_RE.split(action_for_reply)
            if part and part.strip()
        ]
        if not action_parts:
            action_parts = [action_for_reply]
        for part in action_parts:
            part_fp = _fact_fingerprint(part)
            if part_fp and part_fp in appended_fingerprints:
                continue
            if _reply_covers_action(out, part):
                continue
            if _action_recently_in_memory(part) and (not _user_explicitly_requests_action(part)):
                continue
            out = _append_action_sentence(out, part)
            if part_fp:
                appended_fingerprints.add(part_fp)
    candidate = _strip_instruction_leaks(out)
    return candidate


def _is_contact_artifact_token(value: str) -> bool:
    token = str(value or "").strip()
    if not token:
        return False
    return bool(
        _CONTACT_URL_RE.search(token)
        or _CONTACT_HANDLE_RE.search(token)
        or _CONTACT_PHONE_RE.search(token)
    )


def _reply_has_contact_artifact(reply: str, artifacts: Sequence[str]) -> bool:
    text = str(reply or "")
    if not text:
        return False
    if any(_is_contact_artifact_token(item) and item in text for item in artifacts):
        return True
    return _is_contact_artifact_token(text)


def _select_contact_artifacts_for_rule(
    rule: PersonaDeliveryRule,
    artifacts: Sequence[str],
) -> List[str]:
    picked: List[str] = []
    for token in artifacts:
        item = str(token or "").strip()
        if not item:
            continue
        is_handle = bool(_CONTACT_HANDLE_RE.search(item))
        is_phone = bool(_CONTACT_PHONE_RE.search(item))
        is_link = bool(_CONTACT_URL_RE.search(item))
        if rule.wants_link and is_link:
            picked.append(item)
            continue
        if rule.wants_handle and is_handle:
            picked.append(item)
            continue
        if rule.wants_phone and is_phone:
            picked.append(item)
            continue
        if not (rule.wants_link or rule.wants_handle or rule.wants_phone):
            picked.append(item)
    if picked:
        # Preserve order and remove duplicates.
        out: List[str] = []
        seen: set[str] = set()
        for item in picked:
            marker = item.lower()
            if marker in seen:
                continue
            seen.add(marker)
            out.append(item)
        return out
    return [str(item).strip() for item in artifacts if str(item or "").strip()]


def _is_contact_request_text(text: str) -> bool:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return False
    markers = (
        "телеграм",
        "telegram",
        "тг",
        "контакт",
        "номер",
        "телефон",
        "ссылка",
        "каталог",
        "pdf",
        "пдф",
        "прайс",
        "фото",
        "whatsapp",
        "ватсап",
        "вотсап",
    )
    return any(marker in low for marker in markers)


def _assistant_messages_since_contact(state: SalesState, artifacts: Sequence[str]) -> int:
    history = state.history if isinstance(state.history, list) else []
    if not history:
        return 10
    seen_assistant = 0
    for item in reversed(history):
        role = str(item.get("role") or "").strip().lower()
        if role != "assistant":
            continue
        seen_assistant += 1
        text = str(item.get("content") or "").strip()
        if _reply_has_contact_artifact(text, artifacts):
            return seen_assistant - 1
    return seen_assistant


def _delivery_rule_matches(
    rule: PersonaDeliveryRule,
    *,
    channel_name: str,
    last_user_message: str,
    known_facts: Mapping[str, str] | None = None,
    state: SalesState | None = None,
) -> bool:
    current_channel = str(channel_name or "").strip().lower()
    if rule.channel_scope and current_channel not in {ch.lower() for ch in rule.channel_scope}:
        return False
    if rule.condition_text:
        temp = PersonaConditionalRule(
            source_line=rule.source_line,
            condition_text=rule.condition_text,
            action_text=rule.source_line,
            expected_tokens=list(rule.expected_tokens or []),
        )
        return _conditional_rule_matches(
            temp,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
    return True


def _delivery_intro_text(rule: PersonaDeliveryRule, channel_name: str) -> str:
    return ""


def _strip_unsolicited_links(reply: str, last_user_message: str) -> str:
    candidate = str(reply or "").strip()
    if not candidate:
        return candidate
    user_text = str(last_user_message or "")
    if _is_contact_request_text(user_text) or re.search(r"(?iu)https?://", user_text):
        return candidate
    if not re.search(r"(?iu)https?://", candidate):
        return candidate
    cleaned_lines: list[str] = []
    removed = False
    for raw_line in candidate.splitlines():
        line = str(raw_line or "")
        if re.fullmatch(r"(?iu)\s*https?://\S+\s*", line):
            removed = True
            continue
        cleaned_lines.append(line)
    out = "\n".join(line for line in cleaned_lines if line.strip()).strip()
    out = re.sub(r"(?iu)\s*https?://\S+", "", out).strip()
    out = re.sub(r"\s{2,}", " ", out).strip()
    if removed and out:
        return out
    return out or candidate


def _apply_persona_delivery_obligations(
    reply: str,
    *,
    persona_context: str,
    channel_name: str,
    last_user_message: str,
    known_facts: Mapping[str, str] | None = None,
    state: SalesState | None = None,
) -> str:
    candidate = str(reply or "").strip()
    persona_text = str(persona_context or "").strip()
    if not candidate or not persona_text:
        return candidate
    compiled = _compile_persona_rules(persona_text)
    if not compiled.delivery_rules or not compiled.contact_artifacts:
        return candidate
    out = candidate
    for rule in compiled.delivery_rules:
        if not _delivery_rule_matches(
            rule,
            channel_name=channel_name,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        ):
            continue
        # Formatting directives like "send links as a separate message" should
        # not force-link emission on every turn when the rule is unconditional.
        if (
            rule.wants_link
            and not rule.wants_handle
            and not rule.wants_phone
            and not str(rule.condition_text or "").strip()
            and not _reply_has_contact_artifact(out, compiled.contact_artifacts)
            and not _is_contact_request_text(last_user_message)
        ):
            continue
        chosen_artifacts = _select_contact_artifacts_for_rule(rule, compiled.contact_artifacts)
        chosen_artifacts = [item for item in chosen_artifacts if _is_contact_artifact_token(item)]
        if not chosen_artifacts:
            continue
        if _reply_has_contact_artifact(out, chosen_artifacts):
            continue
        if isinstance(state, SalesState):
            since_contact = _assistant_messages_since_contact(state, chosen_artifacts)
            if since_contact < max(
                1, int(rule.min_assistant_gap or 1)
            ) and not _is_contact_request_text(last_user_message):
                continue
        intro = _delivery_intro_text(rule, channel_name)
        payload_parts: List[str] = []
        if intro and intro not in out:
            payload_parts.append(intro)
        payload_parts.extend(chosen_artifacts)
        payload = "\n".join(part for part in payload_parts if part).strip()
        if not payload:
            continue
        out = f"{out}\n{payload}".strip()
    return _strip_unsolicited_links(out, last_user_message)


def _state_facts_snapshot(state: SalesState) -> Dict[str, str]:
    facts: Dict[str, str] = {}
    if isinstance(state.facts, dict):
        for raw_k, raw_v in state.facts.items():
            key = _canonical_fact_key(str(raw_k))
            val = _safe_short_text(str(raw_v or ""), 180)
            if key and val:
                facts[key] = val
    for raw_k, raw_v in (state.known_slots or {}).items():
        key = _canonical_fact_key(str(raw_k))
        val = _safe_short_text(str(raw_v or ""), 180)
        if key and val and key not in facts:
            facts[key] = val
        slot_norm = _normalize_slot_name(str(raw_k))
        if slot_norm == "location" and val:
            if _is_plausible_city_text(val):
                facts.setdefault("city", val)
        elif slot_norm == "object" and val:
            facts.setdefault("object_type", val)
        elif slot_norm == "model" and val:
            facts.setdefault("model", val)
    return facts


def _looks_like_address_value(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    low = raw.lower().replace("ё", "е")
    # Numeric house marker is expected in most practical addresses.
    has_digit = bool(re.search(r"\d", low))
    if not has_digit:
        tokens = [tok for tok in re.split(r"[\s,.;:()]+", low) if tok]
        if len(tokens) < 1 or len(tokens) > 4:
            return False
        explicit_markers = (
            "ул",
            "улиц",
            "просп",
            "пр-",
            "переул",
            "пер",
            "шоссе",
            "бульвар",
            "наб",
        )
        if any(marker in low for marker in explicit_markers):
            return True
        if _OBJECT_TYPE_HINT_RE.search(low):
            return False
        # Allow partial street-like address like "хмельницкого", "ленина", "коммунистическая"
        blocked_tokens = {
            "для",
            "частного",
            "частный",
            "дома",
            "дом",
            "квартиры",
            "квартира",
            "помещения",
            "помещение",
        }
        suffix_hits = 0
        for tok in tokens:
            if tok in blocked_tokens or tok in NEEDS_STOPWORDS:
                continue
            if re.search(r"(ского|ской|ская|ский|ина|ова|ева|овка|евка)$", tok):
                suffix_hits += 1
        return suffix_hits >= 1 and 2 <= len(tokens) <= 3
    # Typical address markers across free-form user input.
    markers = (
        "ул",
        "улиц",
        "просп",
        "пр-",
        "дом",
        "д.",
        "корп",
        "к.",
        "стр",
        "с.",
        "переул",
        "пер",
        "шоссе",
        "бульвар",
        "наб",
        "/",
        "-",
    )
    if any(marker in low for marker in markers):
        return True
    # Conservative fallback for numeric strings without explicit street markers.
    # Example accepted: "космонавтов 2"; rejected: "2 двери", "для частного дома 2".
    tokens = [tok for tok in re.split(r"[\s,]+", low) if tok]
    if len(tokens) < 2 or len(tokens) > 5:
        return False
    has_letters = any(bool(re.search(r"[a-zа-я]", tok, re.IGNORECASE)) for tok in tokens)
    street_like = any(
        bool(re.search(r"(ского|ской|ская|ский|ина|ова|ева|овка|евка)$", tok))
        for tok in tokens
    )
    if _OBJECT_TYPE_HINT_RE.search(low):
        return False
    return has_letters and has_digit and street_like


def _is_plausible_city_text(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    low_raw = raw.lower().replace("ё", "е")
    if _GREETING_PREFIX_RE.match(raw):
        return False
    if re.search(r"(?iu)\b(здравств\w*|привет\w*|добр\w+\s+д\w*|салам\w*|hello|hi)\b", low_raw):
        return False
    if low_raw in {"здравствуйте", "добрый день", "добрый вечер", "привет", "салам", "hello", "hi"}:
        return False
    if "?" in raw:
        return False
    if _looks_like_address_value(raw):
        return False
    tokens = [tok for tok in re.split(r"[\s,.;:()]+", raw) if tok]
    if not tokens or len(tokens) > 3:
        return False
    normalized_tokens = [
        str(tok).lower().replace("ё", "е")
        for tok in tokens
        if str(tok).strip()
    ]
    non_city_markers = {
        "зачем",
        "почему",
        "когда",
        "куда",
        "как",
        "что",
        "кто",
        "чего",
        "чем",
        "вам",
        "мне",
        "тебе",
        "адрес",
        "установка",
        "установки",
        "нужно",
        "надо",
    }
    if any(tok in non_city_markers for tok in normalized_tokens):
        return False
    if normalized_tokens and all(tok in NEEDS_STOPWORDS for tok in normalized_tokens):
        return False
    if all(re.fullmatch(r"\d+", tok) for tok in tokens):
        return False
    if not any(re.search(r"[A-Za-zА-Яа-яЁё]", tok) for tok in tokens):
        return False
    # Filter obvious verb-like single-word inputs (e.g. "летать"),
    # which are frequently false positives in city extraction.
    for token in tokens:
        tok = str(token).strip()
        if not re.search(r"[А-Яа-яЁё]", tok):
            continue
        low = tok.lower()
        if low.endswith("ться") or low.endswith("ть") or low.endswith("чь"):
            return False
    return True


def _extract_city_hint(text: str, *, allow_standalone: bool = False) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    # Common natural patterns: "город <...>", "я из <...>".
    m = re.search(r"(?iu)\bгород\s+([A-Za-zА-Яа-яЁё\- ]{2,40})", raw)
    if m:
        candidate = m.group(1).strip(" ,.;:!?")
        if _is_plausible_city_text(candidate):
            return candidate
    m = re.search(r"(?iu)\bиз\s+([A-Za-zА-Яа-яЁё\- ]{2,40})", raw)
    if m:
        candidate = m.group(1).strip(" ,.;:!?")
        if _is_plausible_city_text(candidate):
            return candidate
    if allow_standalone and _is_plausible_city_text(raw):
        return raw
    return ""


def _extract_standalone_city_hint(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    if "?" in raw:
        return ""
    low = _normalize_text(raw)
    if _OBJECT_TYPE_HINT_RE.search(low):
        return ""
    if _is_price_intent(raw) or _is_store_address_intent(raw):
        return ""
    tokens = [tok for tok in _FACT_TOKEN_RE.findall(raw) if tok]
    if not tokens or len(tokens) > 3:
        return ""
    return _extract_city_hint(raw, allow_standalone=True)


def _reply_contains_unconfirmed_required_claim(
    reply: str,
    *,
    missing_required: Sequence[str],
    known_facts: Mapping[str, str] | None = None,
    user_text: str = "",
    persona_context: str = "",
) -> bool:
    candidate = str(reply or "").strip()
    if not candidate:
        return False
    missing = {_canonical_fact_key(item) for item in (missing_required or []) if str(item or "").strip()}
    missing.discard("")
    if not missing:
        return False

    facts = dict(known_facts or {})
    user_raw = str(user_text or "").strip()
    user_city = _extract_city_hint(user_raw, allow_standalone=True) or _extract_standalone_city_hint(user_raw)
    user_city_norm = _normalize_text(user_city)
    user_obj = _canonical_object_type_hint(
        (infer_user_needs(user_raw) or {}).get("object_type") or _object_type_from_turn_text(user_raw)
    )
    reply_questions = _extract_questions_from_text(candidate)
    has_city_question = any(_question_covers_fact(q, "city") for q in reply_questions)
    has_object_question = any(_question_covers_fact(q, "object_type") for q in reply_questions)

    if "city" in missing and (not str(facts.get("city") or "").strip()) and not has_city_question:
        reply_city_norm = ""
        city_map = _extract_store_addresses_from_persona(persona_context)
        if city_map:
            low_reply = _normalize_text(candidate)
            for city_key in city_map.keys():
                city_norm = _normalize_text(city_key)
                if not city_norm:
                    continue
                if re.search(rf"(?iu)(?<![\w-]){re.escape(city_norm)}(?![\w-])", low_reply):
                    reply_city_norm = city_norm
                    break
        if not reply_city_norm:
            extracted = _extract_city_hint(candidate, allow_standalone=True)
            if extracted:
                reply_city_norm = _normalize_text(extracted)
        if reply_city_norm:
            if user_city_norm and reply_city_norm != user_city_norm:
                return True
            if not user_city_norm:
                return True

    if "object_type" in missing and (not str(facts.get("object_type") or "").strip()) and not has_object_question:
        reply_obj = _canonical_object_type_hint(
            (infer_user_needs(candidate) or {}).get("object_type") or _object_type_from_turn_text(candidate)
        )
        if reply_obj:
            if user_obj and reply_obj != user_obj:
                return True
            if not user_obj:
                return True

    return False


def _capture_pending_fact_answer(state: SalesState, user_text: str) -> None:
    key = _canonical_fact_key(state.pending_fact_key)
    if not key:
        return
    text = str(user_text or "").strip()
    if not text:
        return
    if not isinstance(state.facts, dict):
        state.facts = {}
    turn_intent = _classify_turn_intent(text, known_facts=_state_facts_snapshot(state))
    if turn_intent in {
        "unsubscribe",
        "payment",
        "store_address",
        "handoff",
        "catalog_request",
        "offtopic",
        "why_question",
        "catalog_problem",
        "repair",
    }:
        return
    city_hint = _extract_city_hint(text, allow_standalone=(key == "city"))
    if city_hint:
        state.facts["city"] = _safe_short_text(city_hint, 180)
    tokens = [tok for tok in _FACT_TOKEN_RE.findall(text) if tok]
    if "?" in text and len(tokens) > 8:
        return
    low = _normalize_text(text)
    if key in {"city", "object_type", "model"} and "?" in text:
        return
    if key in {"city", "object_type", "model"} and len(tokens) > 6:
        return
    if key == "city" and len(tokens) > 3:
        return
    if key == "address" and not _looks_like_address_value(text):
        return
    if key in {"city", "object_type"} and _looks_like_address_value(text):
        return
    if key == "object_type" and not _OBJECT_TYPE_HINT_RE.search(low):
        return
    if key == "budget":
        if _extract_budget(low) is None and not _extract_price_spans(text):
            return
    if key == "dimensions":
        has_size_pattern = bool(
            re.search(
                r"(?iu)\b\d{2,4}\s*[xх*]\s*\d{2,4}(?:\s*[xх*]\s*\d{2,4})?\b",
                text,
            )
        )
        has_dimension_words = bool(
            re.search(r"(?iu)\b(размер|проем|проём|ширин|высот|фото)\w*\b", low)
        )
        if not (has_size_pattern or has_dimension_words):
            return
    if key == "model":
        if _LOW_SIGNAL_CONTEXT_RE.search(low) or _CATALOG_UNAVAILABLE_RE.search(low):
            return
        noisy = {"каталог", "грузится", "не", "могу", "пока", "позже"}
        if tokens and sum(1 for tok in tokens if _normalize_text(tok) in noisy) >= max(
            1, len(tokens) // 2
        ):
            return
        tenant = state.tenant if isinstance(getattr(state, "tenant", None), int) else None
        if tenant is None:
            return
        try:
            catalog_items = _read_catalog(int(tenant))
        except Exception:
            return
        match = _best_catalog_item_match(text, catalog_items or [])
        if not match:
            return
        label = _item_label(match)
        if not label:
            return
        state.facts[key] = _safe_short_text(label, 180)
        if not isinstance(state.known_slots, dict):
            state.known_slots = {}
        state.known_slots["model"] = _safe_short_text(label, 120)
        state.pending_fact_key = ""
        return
    if key == "object_type":
        normalized_obj = _object_type_from_turn_text(text)
        if not normalized_obj:
            return
        state.facts[key] = normalized_obj
    elif key == "city":
        if not _is_plausible_city_text(text):
            return
        state.facts[key] = _safe_short_text(text, 180)
    else:
        state.facts[key] = _safe_short_text(text, 180)
    state.pending_fact_key = ""


def _merge_fact_updates(
    state: SalesState,
    updates: Mapping[str, Any] | None,
    *,
    user_text: str = "",
) -> None:
    if not updates:
        return
    if not isinstance(state.facts, dict):
        state.facts = {}
    user_raw = str(user_text or "").strip()
    user_city = _extract_city_hint(user_raw, allow_standalone=True) or _extract_standalone_city_hint(user_raw)
    user_city_norm = _normalize_text(user_city)
    user_obj = _canonical_object_type_hint(
        (infer_user_needs(user_raw) or {}).get("object_type") or _object_type_from_turn_text(user_raw)
    )
    def _fact_value_is_valid(key: str, value: str) -> bool:
        raw = str(value or "").strip()
        if not raw:
            return False
        if key == "city":
            return _is_plausible_city_text(raw)
        if key == "address":
            return _looks_like_address_value(raw)
        if key == "object_type":
            return bool(_OBJECT_TYPE_HINT_RE.search(_normalize_text(raw)))
        if key == "model":
            low = _normalize_text(raw)
            tokens = [tok for tok in _FACT_TOKEN_RE.findall(low) if tok]
            if not tokens:
                return False
            if all(tok in _GENERIC_MODEL_WORDS or tok in NEEDS_STOPWORDS for tok in tokens):
                return False
            tenant = state.tenant if isinstance(getattr(state, "tenant", None), int) else None
            if tenant is None:
                return False
            try:
                catalog_items = _read_catalog(int(tenant))
            except Exception:
                return False
            return _best_catalog_item_match(raw, catalog_items or []) is not None
        if key == "budget":
            return _extract_budget(_normalize_text(raw)) is not None
        return True

    def _fact_update_supported_by_user_turn(key: str, value: str) -> bool:
        # Core factual updates must be grounded in the current user turn.
        if key == "city":
            if not user_city_norm:
                return False
            value_norm = _normalize_text(value)
            if not value_norm:
                return False
            return bool(value_norm == user_city_norm or value_norm in user_city_norm or user_city_norm in value_norm)
        if key == "object_type":
            value_obj = _canonical_object_type_hint(value)
            return bool(value_obj and user_obj and value_obj == user_obj)
        if key == "address":
            return _looks_like_address_value(user_raw)
        if key == "model":
            tenant = state.tenant if isinstance(getattr(state, "tenant", None), int) else None
            if tenant is None:
                return False
            try:
                catalog_items = _read_catalog(int(tenant))
            except Exception:
                return False
            return _best_catalog_item_match(user_raw, catalog_items or []) is not None
        return True

    for raw_key, raw_value in dict(updates or {}).items():
        key = _canonical_fact_key(str(raw_key))
        if not key:
            continue
        value = _safe_short_text(str(raw_value or ""), 180)
        if not value:
            continue
        if not _fact_value_is_valid(key, value):
            continue
        if not _fact_update_supported_by_user_turn(key, value):
            continue
        if key == "object_type":
            normalized_obj = _object_type_from_turn_text(value)
            if not normalized_obj:
                continue
            value = normalized_obj
        elif key == "model":
            tenant = state.tenant if isinstance(getattr(state, "tenant", None), int) else None
            if tenant is None:
                continue
            try:
                catalog_items = _read_catalog(int(tenant))
            except Exception:
                continue
            match = _best_catalog_item_match(user_raw or value, catalog_items or [])
            if not match:
                continue
            label = _item_label(match)
            if not label:
                continue
            value = _safe_short_text(label, 180)
        state.facts[key] = value


def _all_required_facts_present(required: Sequence[str], facts: Mapping[str, str]) -> bool:
    if not required:
        return True
    for raw_key in required:
        key = _normalize_fact_key(str(raw_key))
        if not key:
            continue
        value = str(facts.get(key) or "").strip()
        if not value:
            return False
    return True


def _normalize_slot_name(raw_slot: str, question: str = "") -> str:
    raw = str(raw_slot or "").strip().lower().replace("-", "_").replace(" ", "_")
    if raw in {"", "none", "null", "n/a", "na"}:
        raw = ""
    if raw in _SLOT_ALIASES:
        return raw
    for canonical, aliases in _SLOT_ALIASES.items():
        if raw and any(token in raw for token in aliases):
            return canonical
    topic = _question_topic(question)
    return _QUESTION_TOPIC_TO_SLOT.get(topic, "other")


def _question_topic(question: str) -> str:
    low = str(question or "").lower()
    if not low:
        return "other"
    for topic, aliases in _SLOT_ALIASES.items():
        if any(token in low for token in aliases):
            return topic
    return "other"


def _topic_has_confirmed_fact(topic: str, state: SalesState) -> bool:
    if not isinstance(state, SalesState):
        return False
    facts = _state_facts_snapshot(state)
    mapping: Dict[str, tuple[str, ...]] = {
        "location": ("city", "address"),
        "object": ("object_type",),
        "model": ("model",),
        "budget": ("budget",),
        "timeline": ("timeline",),
        "dimensions": ("dimensions",),
        "contact": ("contact",),
    }
    keys = mapping.get(str(topic or "").strip().lower(), ())
    for key in keys:
        if str(facts.get(key) or "").strip():
            return True
    return False


def _fact_fingerprint(sentence: str) -> str:
    tokens = []
    for token in _FACT_TOKEN_RE.findall((sentence or "").lower().replace("ё", "е")):
        if len(token) < 3:
            continue
        if token in _GENERIC_FACT_STOPWORDS:
            continue
        tokens.append(token)
    if not tokens:
        return ""
    return " ".join(sorted(set(tokens)))


def _dedupe_repeated_fact_sentences(text: str, state: SalesState) -> str:
    raw = (text or "").strip()
    if not raw:
        return raw
    recent = set(state.recent_fact_fingerprints or [])
    if not recent:
        return raw
    recent_token_sets = [set(fp.split()) for fp in recent if fp]
    parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(raw) if part.strip()]
    kept: list[str] = []
    for part in parts:
        if "?" in part:
            kept.append(part)
            continue
        fp = _fact_fingerprint(part)
        if not fp:
            kept.append(part)
            continue
        if fp in recent:
            continue
        current_tokens = set(fp.split())
        is_near_duplicate = False
        if current_tokens:
            for prev_tokens in recent_token_sets:
                if not prev_tokens:
                    continue
                overlap = len(current_tokens & prev_tokens)
                if overlap < 3:
                    continue
                ratio = overlap / max(1, min(len(current_tokens), len(prev_tokens)))
                if ratio >= 0.7:
                    is_near_duplicate = True
                    break
        if not is_near_duplicate:
            kept.append(part)
    if not kept:
        return raw
    rebuilt = " ".join(kept).strip()
    if raw.endswith("?") and not rebuilt.endswith("?"):
        rebuilt = rebuilt + "?"
    return rebuilt or raw


def _update_fact_memory(state: SalesState, text: str) -> None:
    parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(text or "") if part.strip()]
    for part in parts:
        if "?" in part:
            continue
        fp = _fact_fingerprint(part)
        if not fp:
            continue
        if fp not in state.recent_fact_fingerprints:
            state.recent_fact_fingerprints.append(fp)
            if len(state.recent_fact_fingerprints) > 64:
                state.recent_fact_fingerprints = state.recent_fact_fingerprints[-64:]


def _capture_pending_slot_answer(state: SalesState, user_text: str) -> None:
    slot = _normalize_slot_name(state.pending_slot)
    if not slot or slot == "other":
        state.pending_slot = ""
        return
    text = str(user_text or "").strip()
    if not text:
        return
    turn_intent = _classify_turn_intent(text, known_facts=_state_facts_snapshot(state))
    if turn_intent in {
        "unsubscribe",
        "payment",
        "store_address",
        "handoff",
        "catalog_request",
        "offtopic",
        "why_question",
        "catalog_problem",
        "repair",
    }:
        return
    token_count = len(_FACT_TOKEN_RE.findall(text))
    if "?" in text and token_count > 8:
        return
    if slot in {"object", "location", "model"} and "?" in text:
        return
    low = _normalize_text(text)
    # Guard against slot conflicts: do not save obviously mismatched values.
    if slot in {"object", "location"} and _looks_like_address_value(text):
        return
    if slot == "location" and not _is_plausible_city_text(text):
        return
    if slot == "object" and not _OBJECT_TYPE_HINT_RE.search(low):
        return
    if slot == "model":
        if _LOW_SIGNAL_CONTEXT_RE.search(low) or _CATALOG_UNAVAILABLE_RE.search(low):
            return
        generic_model_noise = {"не", "могу", "пока", "грузится", "грузится", "каталог", "потом"}
        tokens = [tok for tok in _FACT_TOKEN_RE.findall(low) if tok]
        if tokens and sum(1 for tok in tokens if tok in generic_model_noise) >= max(
            1, len(tokens) // 2
        ):
            return
        tenant = state.tenant if isinstance(getattr(state, "tenant", None), int) else None
        if tenant is None:
            return
        try:
            catalog_items = _read_catalog(int(tenant))
        except Exception:
            return
        match = _best_catalog_item_match(text, catalog_items or [])
        if not match:
            return
        label = _item_label(match)
        if not label:
            return
        state.known_slots[slot] = _safe_short_text(label, limit=140)
        if not isinstance(state.facts, dict):
            state.facts = {}
        state.facts["model"] = _safe_short_text(label, limit=180)
        state.pending_slot = ""
        return
    state.known_slots[slot] = _safe_short_text(text, limit=140)
    if not isinstance(state.facts, dict):
        state.facts = {}
    canonical = _canonical_fact_key(slot)
    if canonical:
        state.facts[canonical] = _safe_short_text(text, limit=180)
    state.facts[_normalize_fact_key(slot)] = _safe_short_text(text, limit=180)
    state.pending_slot = ""


def _item_price_int(item: Mapping[str, Any]) -> Optional[int]:
    raw = str(item.get("price") or "").strip()
    if not raw:
        return None
    candidates: list[int] = []
    for match in re.finditer(r"\d[\d\s.,]*", raw):
        digits = re.sub(r"\D", "", str(match.group(0) or ""))
        if not digits:
            continue
        try:
            value = int(digits)
        except Exception:
            continue
        candidates.append(value)
    if not candidates:
        return None
    for value in candidates:
        if 1000 <= value <= 1_000_000:
            return value
    lowered = raw.lower()
    has_currency = any(token in lowered for token in ("₽", "руб", "rub", "$", "€", "usd", "eur"))
    for value in candidates:
        if 1 <= value < 1000 and has_currency:
            return value
    if len(candidates) == 1 and candidates[0] > 0:
        return candidates[0]
    return None


def _item_label(item: Mapping[str, Any]) -> str:
    for key in ("title", "name", "model", "sku", "id"):
        value = str(item.get(key) or "").strip()
        if value:
            return value
    return ""


def _display_item_label(item: Mapping[str, Any]) -> str:
    label = _item_label(item)
    if not label:
        return ""
    if len(label) >= 3 and label.upper() == label:
        parts = []
        for token in label.split():
            if token.isupper() and any(ch.isalpha() for ch in token):
                parts.append(token.capitalize())
            else:
                parts.append(token)
        return " ".join(parts).strip()
    return label


def _shortlist_preview_text(
    items: Sequence[Mapping[str, Any]],
    *,
    limit: int = 2,
) -> str:
    parts: list[str] = []
    for item in list(items or [])[: max(1, int(limit))]:
        name = _item_label(dict(item))
        price = _item_price_int(dict(item))
        if not name:
            continue
        if price:
            parts.append(f"{name} — {_format_rub_price(price)}")
        else:
            parts.append(name)
    return "; ".join(parts).strip()


def _render_shortlist_preview_reply(
    preview: str,
    *,
    ask_detail: bool = True,
    persona_context: str = "",
    state: Any = None,
    fact_key: str = "model",
    user_text: str = "",
) -> str:
    text = str(preview or "").strip()
    if not text:
        return ""
    base = f"Варианты: {text}."
    followup = _fallback_contextual_question(
        user_text,
        state=state,
        persona_context=persona_context,
    ) or _persona_driven_question_for_fact(persona_context, fact_key, state=state)
    followup = str(followup or "").strip()
    if (
        followup
        and state is not None
        and fact_key
        and bool(getattr(state, "last_items", None))
        and _question_covers_fact(followup, fact_key)
    ):
        alt_followup = _fallback_contextual_question(
            user_text,
            state=state,
            persona_context="",
        )
        alt_followup = str(alt_followup or "").strip()
        if alt_followup and not _question_covers_fact(alt_followup, fact_key):
            followup = alt_followup
        else:
            followup = ""
    if followup and state is not None and _is_repeated_question_against_state(followup, state):
        generic_q = _generic_question_for_fact(fact_key)
        generic_q = str(generic_q or "").strip()
        if generic_q and not _is_repeated_question_against_state(generic_q, state):
            followup = generic_q
        else:
            followup = ""
    if ask_detail and followup:
        return f"{base} {followup}".strip()
    if followup:
        return f"{base} {followup}".strip()
    return base


def _best_numeric_attribute_delta_line(
    current_items: Sequence[Mapping[str, Any]],
    alternative_items: Sequence[Mapping[str, Any]],
) -> str:
    cur = [dict(item) for item in list(current_items or [])[:2] if isinstance(item, Mapping)]
    alt = [dict(item) for item in list(alternative_items or [])[:2] if isinstance(item, Mapping)]
    if not cur or not alt:
        return ""
    numeric_by_key_cur: dict[str, list[float]] = {}
    numeric_by_key_alt: dict[str, list[float]] = {}
    value_samples: dict[str, str] = {}
    ignored_keys = {"price", "цена", "cost", "стоимость", "stock", "id", "sku", "_search_text"}
    for item in cur:
        for raw_key, raw_val in item.items():
            key = str(raw_key or "").strip()
            val = str(raw_val or "").strip()
            if not key or not val:
                continue
            key_norm = _normalize_text(key)
            if key_norm in ignored_keys or key_norm.startswith("_"):
                continue
            num = _first_number_value(val)
            if num is None:
                continue
            numeric_by_key_cur.setdefault(key, []).append(float(num))
            value_samples.setdefault(key, val)
    for item in alt:
        for raw_key, raw_val in item.items():
            key = str(raw_key or "").strip()
            val = str(raw_val or "").strip()
            if not key or not val:
                continue
            key_norm = _normalize_text(key)
            if key_norm in ignored_keys or key_norm.startswith("_"):
                continue
            num = _first_number_value(val)
            if num is None:
                continue
            numeric_by_key_alt.setdefault(key, []).append(float(num))
            value_samples.setdefault(key, val)
    best_key = ""
    best_score = 0.0
    best_cur_avg = 0.0
    best_alt_avg = 0.0
    for key, cur_vals in numeric_by_key_cur.items():
        alt_vals = numeric_by_key_alt.get(key) or []
        if not cur_vals or not alt_vals:
            continue
        cur_avg = sum(cur_vals) / max(1, len(cur_vals))
        alt_avg = sum(alt_vals) / max(1, len(alt_vals))
        gain = alt_avg - cur_avg
        if gain <= 0:
            continue
        sample = str(value_samples.get(key) or "")
        score = gain
        if re.search(r"\b(?:мм|cm|kg|кг|см|шт|pcs|mm)\b", sample, re.IGNORECASE):
            score += 4.0
        if _is_dimension_like_value(sample):
            score -= 6.0
        if score > best_score:
            best_key = key
            best_score = score
            best_cur_avg = cur_avg
            best_alt_avg = alt_avg
    if not best_key or best_score <= 0:
        return ""
    cur_str = str(int(round(best_cur_avg)))
    alt_str = str(int(round(best_alt_avg)))
    return f"{best_key}: {cur_str} -> {alt_str}."


def _shortlist_attribute_answer(
    user_text: str,
    items: Sequence[Mapping[str, Any]],
) -> str:
    shortlist = [dict(item) for item in list(items or [])[:2] if isinstance(item, Mapping)]
    if not shortlist:
        return ""
    per_item: list[tuple[str, str]] = []
    for item in shortlist:
        name = _display_item_label(item) or _item_label(item)
        answer = _selected_item_attribute_answer(user_text, item)
        if not answer:
            continue
        answer = answer.strip()
        if answer.endswith("."):
            answer = answer[:-1].strip()
        if not name or not answer:
            continue
        per_item.append((name, answer))
    if not per_item:
        return ""
    unique_answers = {ans.lower() for _, ans in per_item}
    if len(unique_answers) == 1:
        common = per_item[0][1].strip()
        if common and common[-1] not in ".!?":
            common += "."
        return common
    parts = [f"{name}: {answer}" for name, answer in per_item]
    merged = ". ".join(part.strip() for part in parts if part.strip()).strip()
    if merged and merged[-1] not in ".!?":
        merged += "."
    return merged


def _item_mm_value(item: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        raw = str(item.get(key) or "").strip()
        if not raw:
            continue
        match = re.search(r"(\d+(?:[.,]\d+)?)", raw)
        if not match:
            continue
        try:
            return float(match.group(1).replace(",", "."))
        except Exception:
            continue
    return None


def _first_number_value(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    match = re.search(r"(\d+(?:[.,]\d+)?)", raw.replace(" ", ""))
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except Exception:
        return None


def _item_number_value(item: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        raw = str(item.get(key) or "").strip()
        if not raw:
            continue
        match = re.search(r"(\d+(?:[.,]\d+)?)", raw.replace(" ", ""))
        if not match:
            continue
        try:
            return float(match.group(1).replace(",", "."))
        except Exception:
            continue
    return 0.0


def _shortlist_comparison_followup_plan(
    user_text: str,
    items: Sequence[Mapping[str, Any]],
    *,
    tenant: int | None,
    persona_context: str = "",
    state: Any = None,
) -> tuple[str, list[dict[str, Any]]]:
    shortlist = [dict(item) for item in list(items or [])[:4] if isinstance(item, Mapping)]
    if not shortlist:
        return "", []
    low = _normalize_text(user_text)
    turn_intent = _classify_turn_intent(user_text)
    if turn_intent == "repair":
        repair = _fallback_contextual_question(
            user_text,
            state=state,
            persona_context=persona_context,
        ) or _generic_question_for_fact("model")
        return (repair, shortlist[:2])
    current_thicknesses = [
        val
        for val in (
            _item_mm_value(item, "Толщина полотна", "Толщина короба")
            for item in shortlist
        )
        if val is not None
    ]
    if not current_thicknesses:
        return "", shortlist[:2]
    current_max = max(current_thicknesses)
    current_min = min(current_thicknesses)

    def _build_non_repeating_alternatives(
        *,
        prefer_thicker: bool,
        min_thickness_floor: float | None = None,
    ) -> list[dict[str, Any]]:
        if tenant is None:
            return []
        try:
            catalog_items = read_all_catalog(tenant=tenant)
        except Exception:
            catalog_items = []
        if not catalog_items:
            return []
        shortlist_ids = {_catalog_item_identity(dict(item)) for item in shortlist}
        alternatives: list[Mapping[str, Any]] = []
        for item in catalog_items:
            identity = _catalog_item_identity(dict(item))
            if identity in shortlist_ids:
                continue
            thickness = _item_mm_value(item, "Толщина полотна", "Толщина короба")
            if prefer_thicker:
                if thickness is None or thickness <= current_max:
                    continue
            if min_thickness_floor is not None:
                if thickness is None or thickness < min_thickness_floor:
                    continue
            alternatives.append(item)
        if not alternatives:
            return []
        alternatives = sorted(
            alternatives,
            key=lambda item: (
                _item_number_value(item, "price", "Цена"),
                _item_mm_value(item, "Толщина полотна", "Толщина короба") or 0,
            ),
        )
        return [dict(item) for item in alternatives[:2]]

    attr_probe = _extract_attribute_probe(user_text)
    asks_attribute = bool(attr_probe)
    asks_variants = _is_shortlist_feedback_turn(user_text)
    if _looks_like_contextual_short_followup(user_text):
        # Avoid repeating the same shortlist forever on short confirmations.
        prefer_thicker = current_max <= 50
        min_floor = current_min if current_min >= 60 else None
        alternatives = _build_non_repeating_alternatives(
            prefer_thicker=prefer_thicker,
            min_thickness_floor=min_floor,
        )
        if alternatives:
            preview = _shortlist_preview_text(alternatives, limit=2)
            if preview:
                delta = _best_numeric_attribute_delta_line(shortlist[:2], alternatives[:2])
                preview_text = f"{preview} {delta}".strip()
                return (
                    _render_shortlist_preview_reply(
                        preview_text,
                        ask_detail=True,
                        persona_context=persona_context,
                        state=state,
                        user_text=user_text,
                    ),
                    alternatives,
                )
        preview = _shortlist_preview_text(shortlist[:2], limit=2)
        if preview:
            return (
                _render_shortlist_preview_reply(
                    preview,
                    ask_detail=True,
                    persona_context=persona_context,
                    state=state,
                    user_text=user_text,
                ),
                shortlist[:2],
            )
        return "", shortlist[:2]
    if _is_price_intent(user_text) or _looks_like_price_objection(user_text):
        asks_cheaper = _extract_price_order_intent(user_text) == "asc"
        prices = [_item_price_int(dict(item)) for item in shortlist]
        valid_prices = [int(price) for price in prices if isinstance(price, int) and price > 0]
        price_order = _extract_price_order_intent(user_text)
        if (price_order == "asc" or asks_cheaper) and valid_prices and tenant is not None:
            try:
                catalog_items = read_all_catalog(tenant=tenant)
            except Exception:
                catalog_items = []
            if catalog_items:
                shortlist_ids = {_catalog_item_identity(dict(item)) for item in shortlist}
                cheaper: list[Mapping[str, Any]] = []
                price_floor = min(valid_prices)
                for item in catalog_items:
                    identity = _catalog_item_identity(dict(item))
                    if identity in shortlist_ids:
                        continue
                    item_price = _item_price_int(dict(item))
                    if not isinstance(item_price, int) or item_price <= 0:
                        continue
                    if item_price < price_floor:
                        cheaper.append(item)
                if cheaper:
                    cheaper = sorted(
                        cheaper,
                        key=lambda item: _item_number_value(item, "price", "Цена"),
                    )
                    cheaper_items = [dict(item) for item in cheaper[:2]]
                    preview = _shortlist_preview_text(cheaper_items, limit=2)
                    if preview:
                        return (
                            _render_shortlist_preview_reply(
                                preview,
                                ask_detail=True,
                                persona_context=persona_context,
                                state=state,
                                user_text=user_text,
                            ),
                            cheaper_items,
                        )
        if valid_prices:
            min_price = min(valid_prices)
            max_price = max(valid_prices)
            budget_question = _persona_driven_question_for_fact(
                persona_context,
                "budget",
                state=state,
            )
            budget_question = str(budget_question or "").strip()
            if asks_cheaper:
                price_text = f"Сейчас самый доступный вариант — {_format_rub_price(min_price)}."
                if budget_question:
                    return (f"{price_text} {budget_question}".strip(), shortlist[:2])
                return (price_text, shortlist[:2])
            if min_price == max_price:
                price_text = f"Цена: {_format_rub_price(min_price)}."
            else:
                price_text = f"Цена: {_format_rub_price(min_price)} - {_format_rub_price(max_price)}."
            if budget_question:
                return (f"{price_text} {budget_question}".strip(), shortlist[:2])
            return (price_text, shortlist[:2])
        budget_question = _persona_driven_question_for_fact(persona_context, "budget", state=state)
        if budget_question:
            return (budget_question, shortlist[:2])
        return (_generic_question_for_fact("budget"), shortlist[:2])
    if asks_attribute:
        probe = _extract_attribute_probe(user_text)
        if probe:
            matched_items = _items_with_attribute(shortlist[:2], probe)
            if matched_items:
                matched_preview = _shortlist_preview_text(matched_items[:2], limit=2)
                if matched_preview:
                    return (
                        _render_shortlist_preview_reply(
                            matched_preview,
                            ask_detail=True,
                            persona_context=persona_context,
                            state=state,
                            user_text=user_text,
                        ),
                        [dict(item) for item in matched_items[:2]],
                    )
        attr_answer = _shortlist_attribute_answer(user_text, shortlist[:2])
        attr_answer_norm = _normalize_text(attr_answer)
        last_reply_norm = _normalize_text(str(getattr(state, "last_bot_reply", "") or "")) if state is not None else ""
        alternatives = _build_non_repeating_alternatives(
            prefer_thicker=(current_max <= 50),
            min_thickness_floor=current_min if current_min >= 50 else None,
        )
        if alternatives:
            preview = _shortlist_preview_text(alternatives, limit=2)
            if preview:
                delta = _best_numeric_attribute_delta_line(shortlist[:2], alternatives[:2])
                preview_text = f"{preview} {delta}".strip()
                return (
                    _render_shortlist_preview_reply(
                        preview_text,
                        ask_detail=True,
                        persona_context=persona_context,
                        state=state,
                        user_text=user_text,
                    ),
                    alternatives,
                )
        if attr_answer and attr_answer_norm and attr_answer_norm != last_reply_norm:
            return (attr_answer, shortlist[:2])
        if attr_answer:
            return (attr_answer, shortlist[:2])
        preview = _shortlist_preview_text(shortlist[:2], limit=2)
        if preview:
            return (
                _render_shortlist_preview_reply(
                    preview,
                    ask_detail=True,
                    persona_context=persona_context,
                    state=state,
                    user_text=user_text,
                ),
                shortlist[:2],
            )
        return "", shortlist[:2]

    if not asks_variants:
        return "", shortlist[:2]
    alternatives = _build_non_repeating_alternatives(prefer_thicker=False)
    if alternatives:
        preview = _shortlist_preview_text(alternatives, limit=2)
        if preview:
            delta = _best_numeric_attribute_delta_line(shortlist[:2], alternatives[:2])
            preview_text = f"{preview} {delta}".strip()
            return (
                _render_shortlist_preview_reply(
                    preview_text,
                    ask_detail=True,
                    persona_context=persona_context,
                    state=state,
                    user_text=user_text,
                ),
                alternatives,
            )
    preview = _shortlist_preview_text(shortlist[:2], limit=2)
    if preview:
        return (
            _render_shortlist_preview_reply(
                preview,
                ask_detail=True,
                persona_context=persona_context,
                state=state,
                user_text=user_text,
            ),
            shortlist[:2],
        )
    return "", shortlist[:2]

    if tenant is None:
        q = _generic_question_for_fact("model")
        return (q, shortlist[:2])
    try:
        catalog_items = read_all_catalog(tenant=tenant)
    except Exception:
        catalog_items = []
    if not catalog_items:
        q = _generic_question_for_fact("model")
        return (q, shortlist[:2])
    alternatives: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for item in catalog_items:
        identity = _catalog_item_identity(dict(item))
        if identity in seen:
            continue
        seen.add(identity)
        thickness = _item_mm_value(item, "Толщина полотна", "Толщина короба")
        if thickness is None or thickness <= current_max:
            continue
        alternatives.append(item)
    if not alternatives:
        return "", shortlist[:2]
    alternatives = sorted(
        alternatives,
        key=lambda item: (
            _item_mm_value(item, "Толщина полотна", "Толщина короба") or 0,
            _item_number_value(item, "price", "Цена"),
        ),
    )
    preview = _shortlist_preview_text(alternatives[:2], limit=2)
    if preview:
        return _render_shortlist_preview_reply(
            preview,
            ask_detail=False,
            persona_context=persona_context,
            state=state,
            user_text=user_text,
        ), [dict(item) for item in alternatives[:2]]
    q = _generic_question_for_fact("model")
    return (q, shortlist[:2])


def _item_aliases(item: Mapping[str, Any]) -> list[str]:
    aliases: set[str] = set()
    for key in ("title", "name", "model", "sku", "id"):
        value = str(item.get(key) or "").strip()
        if not value:
            continue
        norm = value.lower().replace("ё", "е")
        aliases.add(norm)
        compact = re.sub(r"[^0-9a-zа-яё]+", " ", norm, flags=re.IGNORECASE).strip()
        if compact:
            aliases.add(compact)
    return sorted(alias for alias in aliases if len(alias) >= 3)


def _token_overlap_score(query: str, alias: str) -> float:
    q_tokens = {
        tok
        for tok in _FACT_TOKEN_RE.findall((query or "").lower().replace("ё", "е"))
        if (
            len(tok) >= 2
            and (not tok.isdigit())
            and tok not in NEEDS_STOPWORDS
            and tok not in _GENERIC_MODEL_WORDS
        )
    }
    a_tokens = {
        tok
        for tok in _FACT_TOKEN_RE.findall((alias or "").lower().replace("ё", "е"))
        if len(tok) >= 2 and (not tok.isdigit())
    }
    if not q_tokens or not a_tokens:
        return 0.0
    overlap = len(q_tokens & a_tokens)
    if overlap == 0:
        return 0.0
    return overlap / max(1, min(len(q_tokens), len(a_tokens)))


def _best_catalog_item_match(
    query: str, items: Sequence[Mapping[str, Any]]
) -> Optional[Mapping[str, Any]]:
    text = str(query or "").strip().lower().replace("ё", "е")
    if not text:
        return None
    best_item: Optional[Mapping[str, Any]] = None
    best_score = 0.0
    for item in items:
        aliases = _item_aliases(item)
        score = 0.0
        for alias in aliases:
            if alias in text and len(alias) >= 4:
                score = max(score, 2.5)
                break
            if text in alias and len(text) >= 4:
                score = max(score, 2.0)
            score = max(score, _token_overlap_score(text, alias))
        if score > best_score:
            best_score = score
            best_item = item
    if best_score < 0.5:
        return None
    return best_item


def _strict_catalog_item_match(
    query: str, items: Sequence[Mapping[str, Any]]
) -> Optional[Mapping[str, Any]]:
    probe = _normalize_model_alias(query)
    if not probe:
        return None
    for item in items:
        for alias in _item_aliases(item):
            if _normalize_model_alias(alias) == probe:
                return item
    return None


def _collect_grounding_items(
    tenant: int | None, state: SalesState, user_text: str
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _append(items: Sequence[Mapping[str, Any]]) -> None:
        for item in items:
            identity = _catalog_item_identity(dict(item))
            if identity in seen:
                continue
            merged.append(dict(item))
            seen.add(identity)

    if state.last_items:
        _append(state.last_items)

    effective_query = str(user_text or "").strip()

    def _is_low_signal_reply(text: str) -> bool:
        value = str(text or "").strip()
        if not value:
            return True
        if _LOW_SIGNAL_USER_REPLY_RE.match(value):
            return True
        if _LOW_SIGNAL_CONTEXT_RE.search(value):
            return True
        tokens = [tok for tok in _FACT_TOKEN_RE.findall(value.lower().replace("ё", "е")) if tok]
        if len(tokens) <= 1 and len(value) <= 16:
            return True
        return False

    if _is_low_signal_reply(effective_query):
        # Keep strong intent from recent meaningful message (e.g. "нужна тихая дверь"),
        # so "давай/ок" does not reset grounding to utility text like "долго грузится".
        for entry in reversed(state.history or []):
            if str(entry.get("role") or "").strip().lower() != "user":
                continue
            content = str(entry.get("content") or "").strip()
            if not content:
                continue
            if _NOISE_NEED_RE.search(content):
                effective_query = content
                break
        if effective_query == str(user_text or "").strip():
            # Fallback to last meaningful user phrase.
            for entry in reversed(state.history or []):
                if str(entry.get("role") or "").strip().lower() != "user":
                    continue
                content = str(entry.get("content") or "").strip()
                if not content or content == effective_query:
                    continue
                if _is_low_signal_reply(content):
                    continue
                if len(_FACT_TOKEN_RE.findall(content.lower().replace("ё", "е"))) < 2:
                    continue
                effective_query = content
                break

    if tenant is not None:
        try:
            needs: Dict[str, Any] = dict(state.needs or {})
            for key in ("object_type", "city", "address", "model"):
                fact_val = str((state.facts or {}).get(key) or "").strip()
                if fact_val and key not in needs:
                    needs[key] = fact_val
            query_needs = infer_user_needs(effective_query or user_text)
            for key, value in query_needs.items():
                if value in (None, "", [], {}, ()):
                    continue
                if key == "keywords":
                    merged_tokens: List[str] = [
                        str(x) for x in (needs.get("keywords") or []) if str(x).strip()
                    ]
                    for token in value if isinstance(value, list) else [value]:
                        token_str = str(token).strip()
                        if token_str and token_str not in merged_tokens:
                            merged_tokens.append(token_str)
                    if merged_tokens:
                        needs["keywords"] = merged_tokens[:8]
                    continue
                needs[key] = value
            extra = search_catalog(
                needs, limit=8, tenant=tenant, query=effective_query or user_text
            )
            _append(extra)
        except Exception:
            pass
        if not merged:
            try:
                _append(_read_catalog(int(tenant))[:8])
            except Exception:
                pass

        # If recent dialogue already mentioned specific models, keep them in grounding priority.
        try:
            catalog_items = _read_catalog(int(tenant))
        except Exception:
            catalog_items = []
        if catalog_items and state.history:
            hay = _normalize_text(
                " ".join(str(entry.get("content") or "") for entry in (state.history or [])[-8:])
            )
            if hay:
                hinted: List[Dict[str, Any]] = []
                hinted_seen: set[str] = set()
                for item in catalog_items:
                    aliases = _item_aliases(item)
                    if not aliases:
                        continue
                    matched = False
                    for alias in aliases:
                        alias_norm = _normalize_model_alias(alias)
                        if len(alias_norm) < 4:
                            continue
                        if alias_norm in hay:
                            matched = True
                            break
                    if not matched:
                        continue
                    identity = _catalog_item_identity(dict(item))
                    if identity in hinted_seen:
                        continue
                    hinted_seen.add(identity)
                    hinted.append(dict(item))
                    if len(hinted) >= 6:
                        break
                if hinted:
                    merged = _merge_catalog_results(hinted[:], merged, 12)
                    return merged[:12]
    return merged[:12]


def _model_root_tokens(item: Mapping[str, Any]) -> set[str]:
    label = (_item_label(item) or "").lower().replace("ё", "е")
    if not label:
        return set()
    color_tokens: set[str] = set()
    for key, aliases in _GLOBAL_COLOR_ALIASES.items():
        color_tokens.add(_normalize_color_token(key))
        color_tokens.update(_normalize_color_token(alias) for alias in aliases)
    tokens = []
    for token in _FACT_TOKEN_RE.findall(label):
        if len(token) < 2:
            continue
        if token in color_tokens:
            continue
        tokens.append(token)
    return set(tokens)


def _has_single_color_variant(
    selected_item: Mapping[str, Any], catalog_items: Sequence[Mapping[str, Any]]
) -> bool:
    root = _model_root_tokens(selected_item)
    if not root:
        return False
    color_values: set[str] = set()
    for item in catalog_items:
        tokens = _model_root_tokens(item)
        if not tokens:
            continue
        overlap = len(root & tokens)
        if overlap == 0:
            continue
        if overlap < max(1, min(len(root), len(tokens)) // 2):
            continue
        color = _normalize_color_token(str(item.get("color") or ""))
        if color:
            color_values.add(color)
    if not color_values:
        return False
    return len(color_values) <= 1


def _build_reply_grounding(
    *,
    tenant: int | None,
    state: SalesState,
    user_text: str,
) -> Dict[str, Any]:
    items = _collect_grounding_items(tenant, state, user_text)
    full_catalog: List[Dict[str, Any]] = []
    if tenant is not None:
        try:
            full_catalog = [dict(item) for item in _read_catalog(int(tenant))]
        except Exception:
            full_catalog = []
    selected_query = (
        state.known_slots.get("model")
        or str((state.facts or {}).get("model") or "").strip()
        or user_text
    )
    selected_item = _best_catalog_item_match(selected_query, full_catalog or items)
    forbidden_topics: set[str] = set()

    if (
        selected_item is not None
        and full_catalog
        and _has_single_color_variant(selected_item, full_catalog)
    ):
        forbidden_topics.add("color")

    model_aliases: set[str] = set()
    source_for_aliases = full_catalog or items
    for item in source_for_aliases:
        for alias in _item_aliases(item):
            normalized = re.sub(
                r"[^0-9a-zа-яё]+", " ", str(alias).lower().replace("ё", "е")
            ).strip()
            if normalized and len(normalized) >= 3:
                model_aliases.add(normalized)

    needs_payload: Dict[str, Any] = dict(state.needs or {})
    if not needs_payload:
        inferred_current = infer_user_needs(user_text or "")
        if isinstance(inferred_current, dict):
            needs_payload.update({k: v for k, v in inferred_current.items() if v not in (None, "", [], {}, ())})
    for key in ("object_type", "city", "address", "model"):
        fact_val = str((state.facts or {}).get(key) or "").strip()
        if fact_val and key not in needs_payload:
            needs_payload[key] = fact_val
    if "object_type" not in needs_payload:
        for entry in reversed(state.history or []):
            if str(entry.get("role") or "").strip().lower() != "user":
                continue
            content = str(entry.get("content") or "").strip()
            if not content:
                continue
            probe = infer_user_needs(content)
            obj = str((probe or {}).get("object_type") or "").strip()
            if obj:
                needs_payload["object_type"] = obj
                break

    return {
        "items": items,
        "catalog_items": full_catalog[:500],
        "needs": needs_payload,
        "selected_item": dict(selected_item) if isinstance(selected_item, Mapping) else None,
        "forbid_question_topics": sorted(forbidden_topics),
        "model_aliases": sorted(model_aliases),
    }


def _maybe_store_model_slot(state: SalesState, tenant: int | None, user_text: str) -> None:
    if tenant is None:
        return
    text = str(user_text or "").strip()
    if not text:
        return
    if re.match(r"(?iu)^\s*(здрав\w*|привет\w*|добрый|салам|hello|hi)\b", text):
        return
    if _classify_turn_intent(text) == "offtopic":
        return
    low = text.lower().replace("ё", "е")
    if "?" in text and len(_FACT_TOKEN_RE.findall(text)) > 6:
        return
    tokens = [tok for tok in _FACT_TOKEN_RE.findall(low) if len(tok) >= 2]
    if not tokens:
        return
    if len(tokens) <= 2 and not _MODEL_NAME_INTENT_RE.search(text):
        return
    if all(tok in _GENERIC_MODEL_WORDS for tok in tokens):
        return
    try:
        catalog_items = _read_catalog(int(tenant))
    except Exception:
        return
    if not catalog_items:
        return
    match = _best_catalog_item_match(text, catalog_items)
    if not match:
        return
    label = _item_label(match)
    if not label:
        return
    state.known_slots["model"] = _safe_short_text(label, limit=120)


def _enforce_semantic_plan_guards(
    plan: Dict[str, Any],
    *,
    state: SalesState,
    grounding: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    guarded = dict(plan or {})
    question = str(guarded.get("question") or "").strip()
    slot = _normalize_slot_name(str(guarded.get("question_slot") or ""), question=question)
    if slot == "other" and question:
        slot = _QUESTION_TOPIC_TO_SLOT.get(_question_topic(question), "other")
    forbidden = set(str(topic) for topic in ((grounding or {}).get("forbid_question_topics") or []))

    if question:
        fp = quality.question_fingerprint(question)
        already_asked = fp in set(state.asked_question_fingerprints or [])
        slot_filled = bool(slot and slot not in {"none", "other"} and state.known_slots.get(slot))
        topic = _question_topic(question)
        recent_topics = [
            _question_topic(str(item or ""))
            for item in (state.asked_questions or [])[-8:]
            if str(item or "").strip()
        ]
        asked_topic_with_fact = bool(
            topic not in {"", "none", "other"}
            and topic in recent_topics
            and _topic_has_confirmed_fact(topic, state)
        )
        topic_forbidden = topic in forbidden
        if already_asked or slot_filled or topic_forbidden or asked_topic_with_fact:
            question = ""
            slot = "none"

    guarded["question"] = question
    guarded["question_slot"] = slot if slot else "none"
    guarded["required_facts"] = _normalize_required_facts(guarded.get("required_facts"))
    return guarded


def _compose_reply_from_policy_blocks(
    plan: Mapping[str, Any],
    *,
    state: SalesState,
    persona_context: str = "",
    known_facts: Mapping[str, str] | None = None,
    required_facts: Sequence[str] | None = None,
    block_requires_override: Mapping[int, Sequence[str]] | None = None,
    block_allowance_override: Mapping[int, bool] | None = None,
) -> tuple[str, str]:
    blocks = plan.get("blocks")
    if not isinstance(blocks, list):
        return "", ""
    facts = dict(known_facts or {})
    missing_required = _missing_required_facts(required_facts or [], facts)
    out: list[str] = []
    question_used = False
    next_question_key = ""
    for idx, raw in enumerate(blocks[:8]):
        if (
            block_allowance_override
            and idx in block_allowance_override
            and not bool(block_allowance_override[idx])
        ):
            continue
        if not isinstance(raw, Mapping):
            continue
        text = str(raw.get("text") or "").strip()
        if not text:
            continue
        requires_raw = raw.get("requires")
        if isinstance(requires_raw, str):
            requires = [requires_raw]
        elif isinstance(requires_raw, Sequence):
            requires = [str(x) for x in requires_raw]
        else:
            requires = []
        if block_requires_override and idx in block_requires_override:
            requires.extend(
                str(x) for x in (block_requires_override.get(idx) or []) if str(x).strip()
            )
        if not _all_required_facts_present(requires, facts):
            continue
        block_type = str(raw.get("type") or "").strip().lower()
        # Allow contextual info blocks while still blocking commercial offer/CTA
        # until required facts are collected.
        if missing_required and block_type in {"offer", "cta"}:
            continue
        if block_type == "question" or "?" in text:
            if question_used:
                continue
            q_fp = quality.question_fingerprint(text)
            if q_fp and q_fp in set(state.asked_question_fingerprints or []):
                continue
            q_key = _canonical_fact_key(str(raw.get("question_key") or ""))
            if missing_required:
                if q_key and q_key not in missing_required:
                    continue
                if not q_key:
                    matched = None
                    for miss_key in missing_required:
                        if _question_covers_fact(text, miss_key):
                            matched = miss_key
                            break
                    if not matched:
                        continue
                    q_key = matched
            question_used = True
            if q_key:
                next_question_key = q_key
        out.append(text)
        if len(out) >= 3:
            break
    reply = " ".join(out).strip()
    return reply, next_question_key


def _extract_prices(sentence: str) -> list[int]:
    return [item[2] for item in _extract_price_spans(sentence)]


def _extract_price_spans(sentence: str) -> list[tuple[int, int, int]]:
    raw = str(sentence or "")
    if not raw:
        return []
    spans: list[tuple[int, int, int]] = []
    for match in _PRICE_INLINE_RE.finditer(raw):
        digits = re.sub(r"\D", "", match.group(0))
        if len(digits) < 4:
            continue
        try:
            price = int(digits)
        except Exception:
            continue
        spans.append((match.start(), match.end(), price))
    for match in _PRICE_THOUSANDS_RE.finditer(raw):
        try:
            base = int(str(match.group(1) or "0"))
        except Exception:
            continue
        if base <= 0:
            continue
        spans.append((match.start(), match.end(), base * 1000))
    if not spans:
        return []
    spans.sort(key=lambda item: (item[0], -(item[1] - item[0])))
    merged: list[tuple[int, int, int]] = []
    for span in spans:
        if not merged:
            merged.append(span)
            continue
        prev = merged[-1]
        if span[0] < prev[1]:
            continue
        merged.append(span)
    return merged


def _format_rub_price(value: int) -> str:
    return f"{int(value):,}".replace(",", " ") + " ₽"


def _mentioned_catalog_items_in_order(
    sentence: str,
    items: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    normalized_sentence = _normalize_model_alias(sentence)
    if not normalized_sentence:
        return []
    hits: list[tuple[int, Mapping[str, Any]]] = []
    for item in items:
        aliases = _item_aliases(item)
        if not aliases:
            continue
        best_pos: int | None = None
        for alias in aliases:
            alias_norm = _normalize_model_alias(alias)
            if len(alias_norm) < 3:
                continue
            pos = normalized_sentence.find(alias_norm)
            if pos < 0:
                continue
            if best_pos is None or pos < best_pos:
                best_pos = pos
        if best_pos is None:
            continue
        hits.append((best_pos, item))
    if not hits:
        return []
    hits.sort(key=lambda item: item[0])
    ordered: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for _, item in hits:
        identity = _catalog_item_identity(dict(item))
        if identity in seen:
            continue
        seen.add(identity)
        ordered.append(item)
    return ordered


def _catalog_item_is_two_panel(item: Mapping[str, Any]) -> bool:
    return False


def _normalize_model_alias(value: str) -> str:
    return re.sub(r"[^0-9a-zа-яё]+", " ", str(value or "").lower().replace("ё", "е")).strip()


def _grounding_catalog_items(grounding: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for bucket_name in ("items", "catalog_items"):
        for raw_item in (grounding or {}).get(bucket_name) or []:
            if not isinstance(raw_item, Mapping):
                continue
            item = dict(raw_item)
            identity = _catalog_item_identity(item)
            if identity in seen:
                continue
            seen.add(identity)
            merged.append(item)
    return merged


def _enforce_catalog_model_grounding(
    text: str,
    *,
    grounding: Mapping[str, Any] | None = None,
) -> str:
    base = (text or "").strip()
    if not base:
        return base
    aliases = {
        _normalize_model_alias(str(item))
        for item in ((grounding or {}).get("model_aliases") or [])
        if str(item or "").strip()
    }
    aliases = {item for item in aliases if len(item) >= 3}
    if not aliases:
        return base
    items = _grounding_catalog_items(grounding)

    def _replace(match: re.Match[str]) -> str:
        noun = str(match.group(1) or "").strip()
        model_name = str(match.group(2) or "").strip()
        normalized = _normalize_model_alias(model_name)
        if normalized and normalized in aliases:
            return match.group(0)
        if items and _strict_catalog_item_match(model_name, items) is not None:
            return match.group(0)
        if noun:
            return str(noun or "").strip()
        return ""

    return _MODEL_QUOTED_MENTION_RE.sub(_replace, base)


def _enforce_catalog_price_grounding(
    text: str,
    *,
    grounding: Mapping[str, Any] | None = None,
) -> str:
    base = (text or "").strip()
    if not base:
        return base
    base = _enforce_catalog_model_grounding(base, grounding=grounding)
    items = _grounding_catalog_items(grounding)
    if not items:
        return base
    sentences = [part.strip() for part in _SENTENCE_SPLIT_RE.split(base) if part.strip()] or [base]
    out: list[str] = []
    selected_item = _selected_item_from_grounding(grounding, items)
    selected_price = _item_price_int(dict(selected_item)) if isinstance(selected_item, Mapping) else None
    catalog_prices = {
        int(price)
        for price in (_item_price_int(dict(item)) for item in items)
        if isinstance(price, int) and price > 0
    }
    for sentence in sentences:
        price_spans = [span for span in _extract_price_spans(sentence) if _is_likely_price_value(span[2])]
        if not price_spans:
            out.append(sentence)
            continue
        discount_spans: list[tuple[int, int, int]] = []
        for span in price_spans:
            window = sentence[max(0, span[0] - 32): min(len(sentence), span[1] + 16)]
            if re.search(r"(?iu)\b(скидк|акци|промокод|купон)\w*\b", window):
                discount_spans.append(span)
        price_spans = [span for span in price_spans if span not in discount_spans]
        if not price_spans:
            out.append(sentence)
            continue
        mentioned_items = _mentioned_catalog_items_in_order(sentence, items)
        if not mentioned_items:
            item = _best_catalog_item_match(sentence, items)
            if item is not None:
                mentioned_items = [item]
        if (not mentioned_items) and isinstance(selected_price, int) and selected_price > 0:
            mentioned_items = [dict(selected_item or {})]
        expected_prices: list[int] = []
        for item in mentioned_items:
            price = _item_price_int(item)
            if price:
                expected_prices.append(price)

        if not expected_prices:
            if catalog_prices and all(int(span[2]) in catalog_prices for span in price_spans):
                out.append(sentence)
                continue
            patched = sentence
            for start, end, value in sorted(price_spans, key=lambda item: item[0], reverse=True):
                if catalog_prices and int(value) in catalog_prices:
                    continue
                patched = patched[:start] + "цена по каталогу" + patched[end:]
            out.append(patched)
            continue

        replacements: list[tuple[int, int, str]] = []
        for idx, span in enumerate(price_spans):
            expected = expected_prices[min(idx, len(expected_prices) - 1)]
            if span[2] == int(expected):
                continue
            replacements.append((span[0], span[1], _format_rub_price(int(expected))))
        if not replacements:
            out.append(sentence)
            continue
        patched = sentence
        for start, end, value in sorted(replacements, key=lambda item: item[0], reverse=True):
            patched = patched[:start] + value + patched[end:]
        out.append(patched)
    rebuilt = " ".join(out).strip()
    return rebuilt or base


_VARIANTS_USER_HINT_RE = re.compile(
    r"(?iu)\b(вариант|варианты|подбер|покажи|покажите|скинь|скинуть|где варианты|что есть)\b"
)
_VARIANTS_PROMISE_RE = re.compile(
    r"(?iu)\b(скину|скинуть|покажу|подберу|отправлю|сейчас скину|сейчас подберу)\b"
)
_PRICE_INTENT_RE = re.compile(
    r"(?iu)\b(сколько\s+стоит|сколько\s+стоят|сколько\s+цена|цена|ценник|по\s*ч[её]м|поч[её]м|чо\s+по\s+чем|"
    r"от\s+скольк[аи]|от\s+какой\s+цены|по\s+какой\s+цене|что\s+за\s+дверь\s+за\s*\d+|за\s*\d+\s*$)\b"
)
_MIN_PRICE_INTENT_RE = re.compile(
    r"(?iu)\b(от\s+скольк|начина(?:ется|ются)|минимальн\w+|сам\w*\s+дешев\w*)\b"
)
_MAX_PRICE_INTENT_RE = re.compile(
    r"(?iu)\b(сам\w*\s+дорог\w*|наибол\w*\s+дорог\w*|максимальн\w*\s+цен\w*|подороже)\b"
)
_MODEL_NAME_INTENT_RE = re.compile(
    r"(?iu)\b(назван\w*|название\s+модел\w*|какая\s+модел\w*|что\s+за\s+модел\w*|как\s+называет\w*)\b"
)
def _reply_mentions_catalog_item(text: str, items: Sequence[Mapping[str, Any]]) -> bool:
    hay = _normalize_model_alias(text)
    if not hay:
        return False
    for item in items:
        label = _normalize_model_alias(_item_label(item))
        if not label:
            continue
        if label in hay:
            return True
        tokens = [token for token in label.split() if token]
        if len(tokens) >= 2:
            short = f"{tokens[0]} {tokens[1]}"
            if len(short) >= 5 and short in hay:
                return True
    return False


def _quote_likely_model_reference(source: str, quote_start: int) -> bool:
    raw = str(source or "")
    if not raw:
        return False
    left = raw[max(0, int(quote_start) - 56) : int(quote_start)]
    if not left:
        return False
    near_left = left[-28:]
    # Attribute context near quote (color/style/finish) is usually not a model mention.
    if re.search(
        r"(?iu)\b(цвет|оттен|стиль|фактур|дизайн|панел|внутри|снаружи|наружн|внутрен)\w*\b",
        near_left,
    ):
        return False
    return bool(
        re.search(
            r"(?iu)\b(модель|вариант|двер[ья]|позиц(?:ия|ии|ий)?|"
            r"предлож(?:у|ить|им|ите)?|подбер(?:у|ем|ите)?|"
            r"рекоменд(?:ую|уем|ует)|покаж(?:у|ем|ите)?)\b",
            left,
        )
    )


def _reply_mentions_unknown_model(text: str, items: Sequence[Mapping[str, Any]]) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    fragments = re.findall(
        r"(?iu)\b(?:модель|вариант)\s+[«\"]?([a-zа-яё0-9][^\"»\n,.!?;:]{1,80})",
        raw,
    )
    # Also catch quoted names in generic phrasing like:
    # "могу предложить "Гарда Белая" ..."
    lower_raw = raw.lower().replace("ё", "е")
    if (
        _extract_price_spans(raw)
        or re.search(r"(?iu)\b(предлож|двер|модель|вариант)\w*\b", lower_raw)
    ):
        for match in re.finditer(r"[«\"]([^\"»\n]{2,80})[»\"]", raw):
            token = str(match.group(1) or "").strip()
            if len(token) < 3:
                continue
            if not _quote_likely_model_reference(raw, match.start()):
                continue
            fragments.append(token)
    # Catch plain-name proposals without quotes:
    # "могу предложить гарда белая и эмалит белая ..."
    for m in re.finditer(
        r"(?iu)\b(?:предлож(?:у|ить|им|ите)?|подбер(?:у|ем|ите)?|"
        r"рекоменд(?:ую|уем|ует)|покаж(?:у|ем|ите)?)\s+"
        r"([a-zа-яё0-9][a-zа-яё0-9\s./-]{2,80})",
        raw,
    ):
        probe = str(m.group(1) or "").strip()
        if probe:
            fragments.append(probe)
    if not fragments:
        return False
    for fragment in fragments:
        probe = str(fragment or "").strip(" -—\t")
        if len(probe) < 3:
            continue
        if not _looks_like_model_reference_fragment(probe):
            continue
        probe_norm = _normalize_text(probe)
        # Ignore long descriptive fragments accidentally captured after
        # "модель/вариант" markers; validate only compact model-like names.
        if len(_FACT_TOKEN_RE.findall(probe_norm)) > 6:
            continue
        if any(
            marker in probe_norm
            for marker in ("по каталогу", "из каталога", "самый", "самая", "самое")
        ):
            continue
        if _strict_catalog_item_match(probe, items) is None:
            # Allow descriptor-heavy mentions that still point to known catalog model,
            # e.g. "гарда 8 с зеркалом" -> "гарда 8".
            stripped = re.sub(r"(?iu)\bс\s+зеркал\w*\b", " ", probe)
            stripped = re.sub(r"(?iu)\bзеркал\w*\b", " ", stripped)
            stripped = re.sub(r"\s{2,}", " ", stripped).strip()
            if stripped and _strict_catalog_item_match(stripped, items) is not None:
                continue
            return True
    return False


def _looks_like_model_reference_fragment(fragment: str) -> bool:
    probe = _normalize_text(fragment)
    if not probe:
        return False
    if re.search(r"(?iu)https?://|@[a-z0-9_]{2,}", probe):
        return False
    if re.search(
        r"(?iu)\b(telegram|телеграм|whatsapp|ватсап|вотсап|max|"
        r"контакт|телефон|номер|связ|детал|обсуд|удобн|перейд|"
        r"продолж|напис|позвон|каталог)\w*\b",
        probe,
    ):
        return False
    tokens = [tok for tok in _FACT_TOKEN_RE.findall(probe) if tok]
    if not tokens:
        return False
    if len(tokens) > 6:
        return False
    if tokens[0] in {
        "и",
        "или",
        "а",
        "но",
        "что",
        "чтобы",
        "как",
        "для",
        "в",
        "на",
        "с",
        "по",
    }:
        return False
    verb_like = sum(
        1
        for tok in tokens
        if re.search(r"(?iu)(ть|ться|йте|ете|ешь|ем|ут|ют|им|ите)$", tok)
    )
    if verb_like >= 1 and len(tokens) >= 3:
        return False
    content_tokens = [
        tok
        for tok in tokens
        if tok not in NEEDS_STOPWORDS and tok not in _GENERIC_MODEL_WORDS
    ]
    if not content_tokens:
        return False
    return True


def _neutralize_unknown_model_mentions(
    text: str,
    items: Sequence[Mapping[str, Any]],
) -> str:
    base = str(text or "").strip()
    if not base:
        return base

    def _known_or_close(fragment: str) -> bool:
        probe = str(fragment or "").strip(" -—\t")
        if len(probe) < 3:
            return True
        if _strict_catalog_item_match(probe, items) is not None:
            return True
        stripped = re.sub(r"(?iu)\bс\s+зеркал\w*\b", " ", probe)
        stripped = re.sub(r"(?iu)\bзеркал\w*\b", " ", stripped)
        stripped = re.sub(r"\s{2,}", " ", stripped).strip()
        if stripped and _strict_catalog_item_match(stripped, items) is not None:
            return True
        return False

    out = base
    prefixed_re = re.compile(
        r"(?iu)\b(модель|вариант)\s+[«\"]?([^\"»\n,.!?;:]{2,80})[»\"]?"
    )

    def _replace_prefixed(match: re.Match[str]) -> str:
        noun = str(match.group(1) or "").strip()
        fragment = str(match.group(2) or "").strip()
        if not _looks_like_model_reference_fragment(fragment):
            return match.group(0)
        if _known_or_close(fragment):
            return match.group(0)
        return ""

    out = prefixed_re.sub(_replace_prefixed, out)

    quote_re = re.compile(r"[«\"]([^\"»\n]{2,80})[»\"]")
    src = out

    def _replace_quoted(match: re.Match[str]) -> str:
        fragment = str(match.group(1) or "").strip()
        if _known_or_close(fragment):
            return match.group(0)
        if _quote_likely_model_reference(src, match.start()):
            return ""
        return match.group(0)

    out = quote_re.sub(_replace_quoted, out)
    verb_re = re.compile(
        r"(?iu)\b((?:могу\s+)?(?:предложить|предложу|подберу|рекомендую|покажу))\s+"
        r"([a-zа-яё0-9][a-zа-яё0-9\s./-]{2,80})"
    )

    def _replace_verb_phrase(match: re.Match[str]) -> str:
        lead = str(match.group(1) or "").strip()
        fragment = str(match.group(2) or "").strip()
        if not _looks_like_model_reference_fragment(fragment):
            return match.group(0)
        if _known_or_close(fragment):
            return match.group(0)
        return str(lead or "").strip()

    out = verb_re.sub(_replace_verb_phrase, out)
    out = re.sub(r"(?iu)\bвариант\s+вариант\b", "вариант", out)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out


def _neutralize_unverified_priced_labels(
    text: str,
    items: Sequence[Mapping[str, Any]],
) -> str:
    raw = str(text or "").strip()
    if not raw:
        return raw

    def _replace(match: re.Match[str]) -> str:
        label_raw = str(match.group(1) or "").strip()
        tokens = [
            tok
            for tok in _FACT_TOKEN_RE.findall(_normalize_text(label_raw))
            if len(tok) >= 3 and tok not in _GENERIC_PRICE_LABEL_TOKENS
        ]
        if not tokens:
            return match.group(0)
        probe = " ".join(tokens[-5:])
        if _strict_catalog_item_match(probe, items) is not None:
            return match.group(0)
        return ""

    out = re.sub(
        r"(?iu)([a-zа-яё0-9][a-zа-яё0-9\s./-]{1,80})\s*(?:—|-|:)\s*(\d{1,3}(?:[ \u00A0\u202F]\d{3})+|\d{4,7})",
        _replace,
        raw,
    )
    return re.sub(r"\s{2,}", " ", out).strip()


def _neutralize_catalog_model_mentions(
    text: str,
    items: Sequence[Mapping[str, Any]],
) -> str:
    raw = str(text or "").strip()
    if not raw or not items:
        return raw
    out = raw
    seen: set[str] = set()
    for item in items:
        label = str(_item_label(item) or "").strip()
        if len(label) < 3:
            continue
        key = _normalize_model_alias(label)
        if not key or key in seen:
            continue
        seen.add(key)
        out = re.sub(re.escape(label), "модель из каталога", out, flags=re.IGNORECASE)
        tokens = [token for token in label.split() if token]
        if len(tokens) >= 2:
            short = f"{tokens[0]} {tokens[1]}".strip()
            if len(short) >= 5:
                out = re.sub(re.escape(short), "модель из каталога", out, flags=re.IGNORECASE)
    out = re.sub(r"(?iu)(модель из каталога[\s,;:]*){2,}", "модель из каталога ", out)
    return re.sub(r"\s{2,}", " ", out).strip()


def _format_short_catalog_variants(items: Sequence[Mapping[str, Any]], limit: int = 2) -> str:
    chunks: list[str] = []
    for item in list(items)[: max(1, int(limit))]:
        label = _item_label(item)
        if not label:
            continue
        price = _item_price_int(item)
        if price:
            price_text = f"{price:,}".replace(",", " ") + " ₽"
            chunks.append(f"{label} — {price_text}")
        else:
            chunks.append(label)
    return "; ".join(chunks)


def _normalize_catalog_name_case(
    text: str,
    *,
    grounding: Mapping[str, Any] | None = None,
) -> str:
    candidate = (text or "").strip()
    if not candidate:
        return candidate
    items = _grounding_catalog_items(grounding)
    full_items = list((grounding or {}).get("catalog_items") or [])
    if full_items:
        items = _merge_catalog_results(full_items, items, 500)
    if not items:
        return candidate
    out = candidate
    seen: set[str] = set()
    for item in items:
        label = str(_item_label(item) or "").strip()
        if len(label) < 3:
            continue
        key = label.lower().replace("ё", "е")
        if key in seen:
            continue
        seen.add(key)
        lower_label = label.lower()
        out = re.sub(re.escape(label), lower_label, out, flags=re.IGNORECASE)
    return out


def _selected_item_from_grounding(
    grounding: Mapping[str, Any] | None,
    items: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    raw = (grounding or {}).get("selected_item")
    if isinstance(raw, Mapping):
        selected = dict(raw)
        label = _item_label(selected)
        if label:
            for item in items:
                if _normalize_model_alias(_item_label(item)) == _normalize_model_alias(label):
                    return item
        return selected
    return None


def _normalize_probe_token(token: str) -> str:
    value = _normalize_text(token)
    for suffix in (
        "ая",
        "яя",
        "ое",
        "ее",
        "ый",
        "ий",
        "ой",
        "ую",
        "юю",
        "ые",
        "ие",
        "ого",
        "его",
        "ому",
        "ему",
        "ыми",
        "ими",
        "ых",
        "их",
        "ость",
        "ности",
        "ом",
        "ем",
        "ам",
        "ям",
        "ах",
        "ях",
        "ами",
        "ями",
        "у",
        "ю",
        "а",
        "я",
        "е",
        "ы",
        "и",
    ):
        if len(value) > len(suffix) + 2 and value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _extract_attribute_probe(user_text: str) -> str:
    tokens = [
        tok
        for tok in _FACT_TOKEN_RE.findall(_normalize_text(str(user_text or "")))
        if len(tok) >= 3 and not tok.isdigit() and tok not in _GENERIC_FACT_STOPWORDS
    ]
    candidates: list[tuple[int, int, str]] = []
    for idx, token in enumerate(tokens):
        normalized = _normalize_probe_token(token)
        if not normalized or normalized in _GENERIC_FACT_STOPWORDS:
            continue
        score = len(normalized)
        candidates.append((score, idx, normalized))
    if candidates:
        # Prefer more informative probe tokens over short functional words.
        candidates.sort(key=lambda row: (row[0], row[1]))
        return str(candidates[-1][2] or "").strip()
    return ""


def _is_noisy_attribute_value(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return True
    token_count = len(_FACT_TOKEN_RE.findall(_normalize_text(raw)))
    if token_count >= 24:
        return True
    if raw.count(":") >= 3:
        return True
    if raw.count(";") >= 3:
        return True
    return False


def _is_dimension_like_value(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    if re.search(r"\d{2,5}\s*[xх*]\s*\d{2,5}", raw, re.IGNORECASE):
        return True
    if re.search(r"\d{2,5}\s*/\s*\d{2,5}", raw):
        return True
    return False


def _iter_item_attribute_pairs(
    item_map: Mapping[str, Any],
    *,
    blocked: set[str],
) -> list[tuple[str, str, str, str]]:
    pairs: list[tuple[str, str, str, str]] = []
    for raw_key, raw_val in item_map.items():
        key = str(raw_key or "").strip()
        val = str(raw_val or "").strip()
        if not key or not val:
            continue
        key_norm = _normalize_text(key)
        if not key_norm or key_norm in blocked or key_norm.startswith("_"):
            continue
        if _is_noisy_attribute_value(val):
            continue
        val_norm = _normalize_text(val)
        if len(val_norm) < 2:
            continue
        pairs.append((key, val, key_norm, val_norm))
    return pairs


def _format_attribute_pairs(pairs: Sequence[tuple[str, str]], *, max_pairs: int = 2) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for key, val in pairs:
        key_clean = str(key or "").strip()
        val_clean = str(val or "").strip()
        if not key_clean or not val_clean:
            continue
        dedupe = f"{_normalize_text(key_clean)}::{_normalize_text(val_clean)}"
        if dedupe in seen:
            continue
        seen.add(dedupe)
        out.append(f"{key_clean}: {val_clean}")
        if len(out) >= max(1, int(max_pairs or 1)):
            break
    return "; ".join(out).strip()


def _selected_item_attribute_answer(
    user_text: str,
    selected_item: Mapping[str, Any],
) -> str:
    raw_user = str(user_text or "").strip()
    if not raw_user:
        return ""
    question_like = bool(
        ("?" in raw_user)
        or re.match(r"(?iu)^\s*(какой|какая|какие|какое|сколько|чем|как|почему|зачем)\b", raw_user)
    )
    probe = _extract_attribute_probe(raw_user)
    user_tokens = [tok for tok in _tokenize_query(raw_user) if tok]
    if not probe and not question_like:
        return ""
    source_tokens = _tokenize_query(probe) if probe else user_tokens
    probe_tokens = [
        _normalize_probe_token(tok)
        for tok in source_tokens
        if tok and len(tok) >= 3 and tok not in _GENERIC_FACT_STOPWORDS
    ]
    probe_tokens = [tok for tok in probe_tokens if tok and tok not in _GENERIC_FACT_STOPWORDS]
    if not probe_tokens:
        return ""

    blocked = {
        "id",
        "sku",
        "article",
        "title",
        "name",
        "price",
        "cost",
        "url",
        "image",
        "photo",
        "link",
        "stock",
        "color",
        "tags",
    }
    item_map = dict(selected_item)
    entries = _iter_item_attribute_pairs(item_map, blocked=blocked)
    candidates: list[tuple[float, str, str]] = []
    for key, val, _, _ in entries:
        hay_tokens = {
            _normalize_probe_token(tok)
            for tok in _tokenize_query(f"{key} {val}")
            if tok and len(tok) >= 3
        }
        if not hay_tokens:
            continue
        score = 0.0
        for tok in probe_tokens:
            probe_tok = _normalize_probe_token(tok)
            if not probe_tok:
                continue
            if probe_tok in hay_tokens:
                score += 2.0
                continue
            if any(probe_tok in h or h in probe_tok for h in hay_tokens):
                score += 0.8
        if score > 0:
            candidates.append((score, key, val))
    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        selected_pairs: list[tuple[str, str]] = []
        for _, key, value in candidates:
            clean = str(value or "").strip()
            if not clean:
                continue
            pair = (str(key or "").strip(), clean)
            if pair in selected_pairs:
                continue
            selected_pairs.append(pair)
            if len(selected_pairs) >= 2:
                break
        if len(selected_pairs) == 1 and "?" in raw_user:
            selected_values_norm = {_normalize_text(val) for _, val in selected_pairs}
            for key, val, key_norm, _ in entries:
                if key_norm in blocked:
                    continue
                if _normalize_text(val) in selected_values_norm:
                    continue
                selected_pairs.append((key, val))
                break
        rendered = _format_attribute_pairs(selected_pairs, max_pairs=2)
        if rendered:
            return rendered

    if question_like and entries:
        fallback_values: list[tuple[float, str, str, bool]] = []
        for key, val, _, val_norm in entries:
            score = float(min(len(val_norm), 20))
            has_digits = bool(re.search(r"\d", val))
            if has_digits:
                score += 8.0
            if re.search(r"\b(?:мм|cm|kg|кг|см|шт|pcs|mm)\b", val, re.IGNORECASE):
                score += 3.0
            if _is_dimension_like_value(val):
                score -= 12.0
            token_count = len(_FACT_TOKEN_RE.findall(val_norm))
            if token_count >= 10:
                score -= 8.0
            if "," in val:
                score -= 10.0
            if len(val_norm) > 56:
                score -= 10.0
            fallback_values.append((score, key, val, has_digits))
        if fallback_values:
            fallback_values.sort(key=lambda x: x[0], reverse=True)
            picked_pairs: list[tuple[str, str]] = []
            top_numeric: tuple[str, str] | None = None
            top_text: tuple[str, str] | None = None
            for _, key, val, has_digits in fallback_values:
                pair = (str(key or "").strip(), str(val or "").strip())
                if not pair[0] or not pair[1]:
                    continue
                if has_digits and top_numeric is None:
                    top_numeric = pair
                if (not has_digits) and top_text is None:
                    top_text = pair
                if top_numeric is not None and top_text is not None:
                    break
            if top_numeric is not None:
                picked_pairs.append(top_numeric)
            if top_text is not None and top_text not in picked_pairs:
                picked_pairs.append(top_text)
            rendered = _format_attribute_pairs(picked_pairs, max_pairs=2)
            if rendered:
                return rendered
    return ""


def _selected_item_brief_answer(selected_item: Mapping[str, Any]) -> str:
    item_map = dict(selected_item)
    name = _display_item_label(item_map)
    if not name:
        return ""
    parts = [name]
    price = _item_price_int(item_map)
    if price:
        parts.append(f"{_format_rub_price(price)}")
    inside = str(item_map.get("Цвет внутренней панели") or item_map.get("color") or "").strip()
    if inside:
        parts.append(f"внутри {inside}")
    lock_count = str(item_map.get("Количество замков") or "").strip()
    if lock_count:
        parts.append(f"{lock_count} замка")
    return ". ".join(parts[:3]) + "."


def _items_with_attribute(
    items: Sequence[Mapping[str, Any]],
    probe: str,
) -> List[Mapping[str, Any]]:
    needle = _normalize_probe_token(probe)
    if not needle:
        return []
    direct: List[Mapping[str, Any]] = []
    for item in items:
        text = _normalize_text(_collect_item_text(dict(item)))
        if not text:
            continue
        has_attribute = needle in text
        if has_attribute:
            direct.append(item)
    if direct:
        return direct

    # Fallback: semantic token match for probes that are phrased differently
    # from catalog values (e.g. inflections/compounds), while remaining
    # data-driven and tenant-agnostic.
    probe_tokens = [tok for tok in _tokenize_query(probe) if tok]
    if not probe_tokens:
        return []
    semantic: List[Mapping[str, Any]] = []
    for item in items:
        try:
            score = _text_match_score(dict(item), probe_tokens)
        except Exception:
            score = 0.0
        if score > 0:
            semantic.append(item)
    return semantic


def _items_with_attribute_direct(
    items: Sequence[Mapping[str, Any]],
    probe: str,
) -> List[Mapping[str, Any]]:
    needle = _normalize_probe_token(probe)
    if not needle:
        return []
    direct: List[Mapping[str, Any]] = []
    for item in items:
        text = _normalize_text(_collect_item_text(dict(item)))
        if not text:
            continue
        has_attribute = needle in text
        if has_attribute:
            direct.append(item)
    return direct


def _narrow_catalog_items_by_user_text(
    items: Sequence[Mapping[str, Any]],
    user_text: str,
) -> List[Mapping[str, Any]]:
    if not items:
        return []
    tokens = [
        tok
        for tok in _tokenize_query(user_text)
        if tok and (tok not in NEEDS_STOPWORDS) and (not tok.isdigit())
    ]
    if not tokens:
        return list(items)
    generic = {
        "товар",
        "товары",
        "услуга",
        "услуги",
        "модель",
        "модели",
        "вариант",
        "варианты",
        "дороже",
        "дешевле",
    }
    generic_stems = (
        "двер",
        "квартир",
        "дом",
        "частн",
        "дорог",
        "дешев",
        "сам",
        "покаж",
    )
    raw_low = _normalize_text(user_text)
    neg_probes = _negative_attribute_probes(raw_low)
    source_items: List[Mapping[str, Any]] = list(items)
    if neg_probes:
        narrowed = []
        for item in source_items:
            text = _normalize_text(_collect_item_text(dict(item)))
            if not text:
                continue
            if any(probe and probe in text for probe in neg_probes):
                continue
            narrowed.append(item)
        if narrowed:
            source_items = narrowed
    best: List[Mapping[str, Any]] | None = None
    for token in tokens[:8]:
        token_norm = _normalize_probe_token(token)
        if (
            token in generic
            or any(token_norm.startswith(stem) for stem in generic_stems)
            or (token_norm in neg_probes)
        ):
            continue
        matched: List[Mapping[str, Any]] = []
        matched = _items_with_attribute_direct(source_items, token_norm)
        if not matched:
            continue
        if best is None or len(matched) < len(best):
            best = list(matched)
    return best if best is not None else source_items


def _negative_attribute_probes(text: str) -> set[str]:
    low = _normalize_text(text)
    return {
        _normalize_probe_token(match.group(1))
        for match in re.finditer(r"(?iu)\bбез\s+([a-zа-яё0-9-]{3,})", low)
        if match.group(1)
    }


def _exclude_items_with_negative_probes(
    items: Sequence[Mapping[str, Any]],
    probes: set[str],
) -> List[Mapping[str, Any]]:
    if not items or not probes:
        return list(items)
    out: List[Mapping[str, Any]] = []
    for item in items:
        text = _normalize_text(_collect_item_text(dict(item)))
        if not text:
            continue
        if any(probe and probe in text for probe in probes):
            continue
        out.append(item)
    return out


def _canonical_object_type_hint(value: Any) -> str:
    low = _normalize_text(value)
    if not low:
        return ""
    is_apartment = bool(re.search(r"(?iu)\b(apartment|flat|квартир\w*|кв\.)\b", low))
    is_house = bool(re.search(r"(?iu)\b(house|home|частн\w*|коттедж\w*|дом\w*)\b", low))
    if is_apartment and not is_house:
        return "apartment"
    if is_house and not is_apartment:
        return "house"
    return ""


def _object_type_from_turn_text(text: str) -> str:
    low = _normalize_text(text)
    if not low:
        return ""
    apartment_markers = (
        "квартир",
        "кв.",
        "кв ",
        "апартамент",
        "flat",
        "apartment",
    )
    house_markers = (
        "частн",
        "дом",
        "коттедж",
        "таунхаус",
        "house",
        "home",
    )
    is_apartment = any(marker in low for marker in apartment_markers)
    is_house = any(marker in low for marker in house_markers)
    if is_apartment and not is_house:
        return "apartment"
    if is_house and not is_apartment:
        return "house"
    return ""


def _item_object_type_hint(item: Mapping[str, Any]) -> str:
    direct = _canonical_object_type_hint(item.get("object_type"))
    if direct:
        return direct

    tags = item.get("tags") or []
    tag_tokens = {
        _normalize_text(tag)
        for tag in (tags if isinstance(tags, Sequence) and not isinstance(tags, (str, bytes)) else [tags])
        if str(tag or "").strip()
    }
    if "house_ready" in tag_tokens:
        return "house"
    if "apartment_ready" in tag_tokens:
        return "apartment"

    for raw_key, raw_val in item.items():
        key = _normalize_text(raw_key)
        if not key:
            continue
        if any(token in key for token in ("object", "usage", "назнач", "помещ", "тип")):
            kind = _canonical_object_type_hint(raw_val)
            if kind:
                return kind

    # Fallback: infer from rich item text only when signal is unambiguous.
    hay = _normalize_text(_collect_item_text(dict(item)))
    if hay:
        kind = _canonical_object_type_hint(hay)
        if kind:
            return kind
    return ""


def _filter_items_by_object_type_need(
    items: Sequence[Mapping[str, Any]],
    needs: Mapping[str, Any] | None,
) -> List[Mapping[str, Any]]:
    if not items or not isinstance(needs, Mapping):
        return list(items)
    target = _normalize_text(needs.get("object_type") or "")
    if target not in {"apartment", "house"}:
        return list(items)

    hints = [_item_object_type_hint(item) for item in items]
    if target == "apartment":
        # For apartment requests, at minimum exclude explicitly house-marked items.
        if any(hint == "house" for hint in hints):
            out = [item for item, hint in zip(items, hints) if hint != "house"]
            if out:
                return out
        if any(hint == "apartment" for hint in hints):
            out = [item for item, hint in zip(items, hints) if hint == "apartment"]
            if out:
                return out
        return list(items)

    if any(hint == "house" for hint in hints):
        out = [item for item, hint in zip(items, hints) if hint == "house"]
        if out:
            return out
    return list(items)


def _extract_budget_cap_from_needs(needs: Mapping[str, Any] | None) -> Optional[int]:
    if not isinstance(needs, Mapping):
        return None
    for key in ("budget_max", "budget", "budget_to", "max_budget"):
        raw = needs.get(key)
        if raw in (None, ""):
            continue
        try:
            if isinstance(raw, (int, float)):
                val = int(raw)
            else:
                match = re.search(r"\d[\d\s.,]*", str(raw))
                if not match:
                    continue
                val = int(re.sub(r"\D", "", match.group(0)))
            if val > 0:
                return val
        except Exception:
            continue
    return None


def _catalog_min_price(items: Sequence[Mapping[str, Any]]) -> int | None:
    vals = [_item_price_int(dict(item)) for item in items]
    clean = [int(v) for v in vals if isinstance(v, int) and v > 0]
    if not clean:
        return None
    return min(clean)


def _catalog_max_price(items: Sequence[Mapping[str, Any]]) -> int | None:
    vals = [_item_price_int(dict(item)) for item in items]
    clean = [int(v) for v in vals if isinstance(v, int) and v > 0]
    if not clean:
        return None
    return max(clean)


def _catalog_extreme_item_by_price(
    items: Sequence[Mapping[str, Any]],
    *,
    highest: bool,
) -> Mapping[str, Any] | None:
    best: Mapping[str, Any] | None = None
    best_price: int | None = None
    for item in items:
        price = _item_price_int(dict(item))
        if not isinstance(price, int) or price <= 0:
            continue
        if best is None or best_price is None:
            best = item
            best_price = price
            continue
        if highest and price > best_price:
            best = item
            best_price = price
        if (not highest) and price < best_price:
            best = item
            best_price = price
    return best


def _is_price_intent(text: str) -> bool:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return False
    patterns = (
        r"\bсколько\s+стоит\b",
        r"\bсколько\s+стоят\b",
        r"\bкакая\s+цена\b",
        r"\bкакую\s+цен[уы]\b",
        r"\bпо\s+какой\s+цене\b",
        r"\bпо\s+ч[её]м\b",
        r"\bпоч[её]м\b",
        r"\bот\s+сколь",
        r"\bцена\b",
        r"\bцены\b",
        r"\bценник\b",
        r"\bстоимость\b",
        r"\bстоит\b",
        r"\bстоят\b",
        r"\bсам\w*\s+дорог\w*\b",
        r"\bсам\w*\s+дешев\w*\b",
        r"\bподороже\b",
        r"\bподешевле\b",
        r"\bмаксимальн\w*\s+цен\w*\b",
        r"\bминимальн\w*\s+цен\w*\b",
    )
    return any(re.search(pattern, low) for pattern in patterns)


def _is_payment_intent(text: str) -> bool:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return False
    markers = (
        "оплат",
        "оплата",
        "перевод",
        "реквиз",
        "карта",
        "номер карты",
        "остаток",
        "договор",
        "чек",
    )
    return any(token in low for token in markers)


def _is_store_address_intent(text: str) -> bool:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return False
    return (
        ("адрес" in low and ("магаз" in low or "наход" in low or "где вы" in low))
        or ("часы работы" in low)
        or ("график" in low and ("работ" in low or "магаз" in low))
    )


def _is_channel_handoff_intent(text: str) -> bool:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return False
    return any(
        token in low for token in ("telegram", "телег", "тг", "whatsapp", "ватсап", "вотсап")
    )


def _is_catalog_request_intent(text: str) -> bool:
    return bool(_CATALOG_REQUEST_RE.search(str(text or "")))


def _is_offtopic_message(text: str, *, known_facts: Mapping[str, str] | None = None) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    if re.search(r"(?iu)\b(здравств|добрый|привет|салам|hi|hello)\b", raw):
        return False
    if _ORDER_INTENT_RE.search(raw):
        return False
    if _is_price_intent(raw) or _is_payment_intent(raw) or _is_store_address_intent(raw):
        return False
    if _is_channel_handoff_intent(raw) or _is_catalog_request_intent(raw):
        return False
    if _MODEL_NAME_INTENT_RE.search(raw):
        return False
    if _extract_city_hint(raw):
        return False
    if _OFFTOPIC_SMALLTALK_RE.search(raw):
        return True
    return False


def _classify_turn_intent(text: str, *, known_facts: Mapping[str, str] | None = None) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "unknown"
    if _is_unsubscribe_intent(raw):
        return "unsubscribe"
    if _WHY_QUESTION_RE.search(raw):
        return "why_question"
    if _CATALOG_UNAVAILABLE_RE.search(raw) or _LOW_SIGNAL_CONTEXT_RE.search(raw):
        return "catalog_problem"
    if _REPAIR_TURN_RE.search(raw):
        return "repair"
    if _is_payment_intent(raw):
        return "payment"
    if _is_store_address_intent(raw):
        return "store_address"
    if _is_channel_handoff_intent(raw):
        return "handoff"
    if _is_catalog_request_intent(raw):
        return "catalog_request"
    if _is_offtopic_message(raw, known_facts=known_facts):
        return "offtopic"
    return "product"


def _is_shortlist_feedback_turn(
    text: str,
    *,
    known_facts: Mapping[str, str] | None = None,
) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    if _MODEL_NAME_INTENT_RE.search(raw):
        return False
    if _looks_like_address_value(raw):
        return False
    if _extract_city_hint(raw, allow_standalone=True):
        return False
    if _object_type_from_turn_text(raw):
        return False
    if _ORDER_INTENT_RE.search(raw) or _is_payment_intent(raw):
        return False
    if _is_channel_handoff_intent(raw) or _is_store_address_intent(raw):
        return False
    if _is_catalog_request_intent(raw):
        return False
    if _is_price_intent(raw) or _extract_attribute_probe(raw):
        return True
    turn_intent = _classify_turn_intent(raw, known_facts=known_facts)
    return turn_intent in {"product", "repair", "catalog_problem", "why_question"}


def _is_deferral_message(text: str) -> bool:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return False
    return any(
        token in low
        for token in ("позже", "потом", "вечером", "завтра", "позднее", "как буду", "как смогу")
    )


def _extract_store_addresses_from_persona(persona_context: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    text = str(persona_context or "")
    if not text:
        return mapping
    for raw in text.splitlines():
        line = raw.strip().strip("-").strip()
        if not line:
            continue
        m = re.match(r"(?u)^([A-Za-zА-Яа-яЁё/\s]{2,40})\s*[—-]\s*(.{3,120})$", line)
        if not m:
            continue
        city = str(m.group(1) or "").strip().lower().replace("ё", "е")
        addr = str(m.group(2) or "").strip()
        if not city or not addr:
            continue
        if any(bad in city for bad in ("магаз", "адрес", "telegram", "гарант", "бесплат")):
            continue
        mapping[city] = addr
    return mapping


def _extract_price_target_hint(text: str) -> int | None:
    raw = str(text or "")
    if not raw.strip():
        return None
    spans = _extract_price_spans(raw)
    if spans:
        return int(spans[0][2])
    m_k = re.search(r"(?iu)\b(\d{1,3})\s*(?:тыс(?:\.|яч)?|тысяч(?:а|и)?|к)\b", raw)
    if m_k:
        try:
            return int(m_k.group(1)) * 1000
        except Exception:
            return None
    m_plain = re.search(r"(?iu)\bза\s*(\d{1,3})\b", raw)
    if m_plain:
        try:
            return int(m_plain.group(1)) * 1000
        except Exception:
            return None
    return None


def _closest_catalog_item_by_price(
    items: Sequence[Mapping[str, Any]],
    target_price: int,
) -> Mapping[str, Any] | None:
    best: Mapping[str, Any] | None = None
    best_diff: int | None = None
    for item in items:
        price = _item_price_int(dict(item))
        if not price:
            continue
        diff = abs(int(price) - int(target_price))
        if best is None or best_diff is None or diff < best_diff:
            best = item
            best_diff = diff
    return best


def _is_likely_price_value(value: int) -> bool:
    try:
        number = int(value)
    except Exception:
        return False
    if number <= 0:
        return False
    # Ignore long identifiers/phone-like numbers.
    if len(str(abs(number))) >= 9:
        return False
    return True


def _is_catalog_price_candidate(value: int, catalog_prices: set[int] | None) -> bool:
    if not _is_likely_price_value(value):
        return False
    prices = {int(v) for v in (catalog_prices or set()) if isinstance(v, int) and v > 0}
    if not prices:
        return True
    min_catalog = min(prices)
    # Ignore low promo/discount numbers when catalog prices are materially higher.
    dynamic_floor = max(300, int(min_catalog * 0.33))
    try:
        return int(value) >= dynamic_floor
    except Exception:
        return False


_GENERIC_PRICE_KEYWORD_PREFIXES = (
    "цен",
    "стоим",
    "скольк",
    "дорог",
    "дешев",
    "сам",
    "покаж",
    "вариант",
    "модел",
    "двер",
    "квартир",
    "дом",
    "частн",
)


def _is_specific_catalog_keyword(keyword: str) -> bool:
    token = _normalize_probe_token(keyword)
    if len(token) < 4:
        return False
    if any(token.startswith(prefix) for prefix in _GENERIC_PRICE_KEYWORD_PREFIXES):
        return False
    return True


_GENERIC_PRICE_LABEL_TOKENS = {
    "самый",
    "самая",
    "самое",
    "дорогой",
    "дорогая",
    "дешевый",
    "дешевая",
    "доступный",
    "доступная",
    "вариант",
    "варианты",
    "каталог",
    "каталогу",
    "модель",
    "модели",
    "дверь",
    "цена",
    "стоит",
    "за",
    "от",
    "до",
}


def _catalog_has_object_type_evidence(
    items: Sequence[Mapping[str, Any]],
    object_type_need: str,
) -> bool:
    kind = _normalize_text(object_type_need)
    if kind not in {"apartment", "house"}:
        return True
    hints = ("квартир", "apartment", "flat") if kind == "apartment" else ("частн", "дом", "house")
    for item in items:
        text = _normalize_text(_collect_item_text(dict(item)))
        if any(hint in text for hint in hints):
            return True
    return False


def _extract_explicit_model_probe(user_text: str) -> str:
    raw = str(user_text or "").strip()
    if not raw:
        return ""
    low = _normalize_text(raw)
    if not re.search(r"(?iu)\b(есть|имеется|в наличии|подойдет|подойд[её]т)\b", low):
        return ""
    if len(raw) > 55:
        return ""
    stop = {
        "есть",
        "имеется",
        "наличии",
        "в",
        "на",
        "для",
        "или",
        "и",
        "а",
        "подойдет",
        "подойдет",
        "подойдет",
        "подойдет",
        "подойдёт",
        "квартиры",
        "квартира",
        "дома",
        "дом",
        "частного",
    }
    tokens = [tok for tok in _FACT_TOKEN_RE.findall(low) if len(tok) >= 3 and tok not in stop]
    if len(tokens) < 2 or len(tokens) > 4:
        return ""
    return " ".join(tokens[:5])


def _has_unverified_priced_labels(
    text: str,
    items: Sequence[Mapping[str, Any]],
) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    for match in re.finditer(
        r"(?iu)([a-zа-яё0-9][a-zа-яё0-9\s./-]{1,80})\s*(?:—|-|:)\s*(\d{1,3}(?:[ \u00A0\u202F]\d{3})+|\d{4,7})",
        raw,
    ):
        label_raw = str(match.group(1) or "").strip()
        tokens = [
            tok
            for tok in _FACT_TOKEN_RE.findall(_normalize_text(label_raw))
            if len(tok) >= 3 and tok not in _GENERIC_PRICE_LABEL_TOKENS
        ]
        if not tokens:
            continue
        probe = " ".join(tokens[-5:])
        if _strict_catalog_item_match(probe, items) is not None:
            continue
        return True
    return False


def _enforce_catalog_truth_guard(
    text: str,
    *,
    grounding: Mapping[str, Any] | None = None,
    user_text: str = "",
) -> str:
    base = (text or "").strip()
    if not base:
        return base
    items = _grounding_catalog_items(grounding)
    user_raw = str(user_text or "")
    if not items:
        if (
            _is_price_intent(user_raw)
            or bool(_MODEL_NAME_INTENT_RE.search(user_raw))
            or bool(_VARIANTS_USER_HINT_RE.search(user_raw))
        ):
            patched = base
            spans = [span for span in _extract_price_spans(patched) if _is_likely_price_value(span[2])]
            if re.search(r"(?iu)\bскидк\w*\b", patched):
                spans = [
                    span
                    for span in spans
                    if not re.search(r"(?iu)\bскидк\w*\b", patched[max(0, span[0] - 32): span[1]])
                ]
            for start, end, _ in sorted(spans, key=lambda item: item[0], reverse=True):
                patched = patched[:start] + "цена по каталогу" + patched[end:]
            if patched.strip():
                return patched
        return base

    normalized = _enforce_catalog_model_grounding(base, grounding=grounding)
    normalized = _enforce_catalog_price_grounding(normalized, grounding=grounding)

    low = normalized.lower()
    mentions_known = _reply_mentions_catalog_item(normalized, items)
    has_unknown_model_marker = "модель из каталога" in low
    asks_price = _is_price_intent(user_raw)
    asks_min_price_global = bool(_MIN_PRICE_INTENT_RE.search(user_raw))
    asks_max_price_global = bool(_MAX_PRICE_INTENT_RE.search(user_raw))
    asks_variants = bool(_VARIANTS_USER_HINT_RE.search(user_raw))
    asks_model_name = bool(_MODEL_NAME_INTENT_RE.search(user_raw))
    catalog_prices = {
        int(price)
        for price in (_item_price_int(dict(item)) for item in items)
        if isinstance(price, int) and price > 0
    }
    price_spans = [
        span
        for span in _extract_price_spans(normalized)
        if _is_catalog_price_candidate(span[2], catalog_prices)
    ]
    has_price_tokens = bool(price_spans)
    needs_ctx = dict((grounding or {}).get("needs") or {})
    prefers_insulation = bool(
        needs_ctx.get("insulation_priority") or needs_ctx.get("noise_priority")
    )
    object_type_need = _normalize_text(needs_ctx.get("object_type") or "")
    turn_object_type = _object_type_from_turn_text(user_raw)
    if turn_object_type in {"apartment", "house"}:
        object_type_need = turn_object_type
        needs_ctx["object_type"] = turn_object_type
    candidate_items = list(items)
    filtered_by_object = _filter_items_by_object_type_need(candidate_items, needs_ctx)
    if filtered_by_object:
        candidate_items = filtered_by_object
    object_filter_restricted = len(candidate_items) < len(items)
    selected_item = _selected_item_from_grounding(grounding, items)
    if selected_item is not None and object_type_need in {"apartment", "house"}:
        selected_hint = _item_object_type_hint(selected_item)
        if selected_hint and selected_hint != object_type_need:
            selected_item = None
    object_type_has_evidence = _catalog_has_object_type_evidence(items, object_type_need)
    if prefers_insulation and object_type_need == "apartment" and not (
        asks_min_price_global or asks_max_price_global
    ):
        two_panel = [item for item in candidate_items if _catalog_item_is_two_panel(item)]
        if two_panel:
            candidate_items = two_panel

    kw_values = needs_ctx.get("keywords")
    has_specific_kw = False
    if asks_price:
        query_needs = infer_user_needs(user_text)
        q_keywords = query_needs.get("keywords") if isinstance(query_needs, Mapping) else None
        if isinstance(q_keywords, Sequence):
            q_specific = [kw for kw in q_keywords if _is_specific_catalog_keyword(str(kw or ""))]
            if q_specific:
                kw_values = q_specific
    if isinstance(kw_values, Sequence):
        has_specific_kw = any(_is_specific_catalog_keyword(str(kw or "")) for kw in kw_values)
    if isinstance(kw_values, Sequence):
        best_kw_match: List[Mapping[str, Any]] | None = None
        for raw_kw in kw_values:
            kw = str(raw_kw or "").strip()
            if len(kw) < 4:
                continue
            kw_norm = _normalize_probe_token(kw)
            allow_semantic_kw = not any(
                kw_norm.startswith(stem)
                for stem in ("квартир", "двер", "дом", "частн", "дорог", "дешев", "сам")
            )
            matched_by_kw = _items_with_attribute_direct(candidate_items, kw)
            if not matched_by_kw and allow_semantic_kw:
                matched_by_kw = _items_with_attribute(candidate_items, kw)
            if not matched_by_kw and (not object_filter_restricted):
                matched_by_kw = _items_with_attribute_direct(items, kw)
            if not matched_by_kw and allow_semantic_kw and (not object_filter_restricted):
                matched_by_kw = _items_with_attribute(items, kw)
            if matched_by_kw:
                if best_kw_match is None or len(matched_by_kw) < len(best_kw_match):
                    best_kw_match = list(matched_by_kw)
        if best_kw_match:
            candidate_items = list(best_kw_match)

    invalid_prices = [
        span[2]
        for span in price_spans
        if catalog_prices
        and _is_catalog_price_candidate(span[2], catalog_prices)
        and span[2] not in catalog_prices
    ]
    if invalid_prices:
        patched = normalized
        replacements: list[tuple[int, int, str]] = []
        for start, end, value in price_spans:
            if catalog_prices and int(value) not in catalog_prices:
                replacements.append((start, end, "цена по каталогу"))
        if replacements:
            for start, end, value in sorted(replacements, key=lambda item: item[0], reverse=True):
                patched = patched[:start] + value + patched[end:]
            normalized = patched
        if asks_price or asks_variants or asks_model_name:
            pass

    if _reply_mentions_unknown_model(normalized, candidate_items or items):
        normalized = _neutralize_unknown_model_mentions(normalized, candidate_items or items)
        parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(normalized) if part.strip()]
        kept = [
            part
            for part in parts
            if not _reply_mentions_unknown_model(part, candidate_items or items)
        ]
        if kept:
            normalized = " ".join(kept).strip()
        else:
            normalized = _neutralize_unknown_model_mentions(base, candidate_items or items).strip()
            normalized = re.sub(r"\s{2,}", " ", normalized).strip()
        if _reply_mentions_unknown_model(normalized, candidate_items or items):
            if not (asks_variants or asks_price or asks_model_name):
                return normalized

    if asks_variants and (not mentions_known):
        return normalized

    explicit_probe = _extract_explicit_model_probe(user_raw)
    if explicit_probe and _strict_catalog_item_match(explicit_probe, candidate_items or items) is None:
        return normalized

    if asks_price and has_price_tokens:
        if _has_unverified_priced_labels(normalized, candidate_items or items):
            normalized = _neutralize_unverified_priced_labels(
                normalized,
                candidate_items or items,
            )
            normalized = _neutralize_unknown_model_mentions(normalized, candidate_items or items)
            return normalized

    if has_unknown_model_marker:
        return normalized

    selected_attr_answer = ""
    asked_probe = _extract_attribute_probe(user_text)
    if selected_item is not None and (not asks_price) and (not asks_model_name):
        selected_attr_answer = _selected_item_attribute_answer(user_text, selected_item)
        if selected_attr_answer:
            return selected_attr_answer
    if asked_probe and (not asks_price) and (not asks_model_name):
        probe_norm = _normalize_probe_token(asked_probe)
        source_for_attr = list((grounding or {}).get("catalog_items") or items)
        if probe_norm.startswith("двухпанел"):
            source_for_attr = list(candidate_items)
        attr_items = _items_with_attribute(source_for_attr, asked_probe)
        if not attr_items:
            attr_items = _items_with_attribute(items, asked_probe)
        budget_cap = _extract_budget_cap_from_needs(dict((grounding or {}).get("needs") or {}))
        if budget_cap and attr_items:
            limited = [
                it for it in attr_items if (_item_price_int(dict(it)) or 10**9) <= budget_cap
            ]
            if limited:
                attr_items = limited
            else:
                nearest_attr = _closest_catalog_item_by_price(attr_items, budget_cap)
                if nearest_attr is not None:
                    nm = str(_item_label(dict(nearest_attr)) or "").lower()
                    pr = _item_price_int(dict(nearest_attr))
                    if nm and pr:
                        return f"{nm} {_format_rub_price(pr)}".strip()
        if selected_item is not None:
            selected_attr_answer = _selected_item_attribute_answer(user_text, selected_item)
            if selected_attr_answer:
                return selected_attr_answer
        if probe_norm:
            return normalized

    if asks_price and (
        (not mentions_known)
        or has_price_tokens
        or asks_min_price_global
        or asks_max_price_global
    ):
        if (
            object_type_need in {"apartment", "house"}
            and (not object_filter_restricted)
            and (not object_type_has_evidence)
            and (not has_specific_kw)
        ):
            guarded = _neutralize_catalog_model_mentions(normalized, items)
            for start, end, value in sorted(
                _extract_price_spans(guarded), key=lambda item: item[0], reverse=True
            ):
                if _is_catalog_price_candidate(value, None):
                    guarded = guarded[:start] + "цена по каталогу" + guarded[end:]
            return re.sub(r"\s{2,}", " ", guarded).strip()
        if (
            object_type_need in {"apartment", "house"}
            and (asks_min_price_global or asks_max_price_global)
            and len(candidate_items) == len(items)
            and (not has_specific_kw)
            and not _catalog_has_object_type_evidence(candidate_items, object_type_need)
        ):
            return normalized
        narrowed_by_text = _narrow_catalog_items_by_user_text(candidate_items or items, user_text)
        if narrowed_by_text:
            candidate_items = list(narrowed_by_text)
        neg_probes = _negative_attribute_probes(user_text)
        if neg_probes:
            neg_filtered = _exclude_items_with_negative_probes(candidate_items or items, neg_probes)
            if not neg_filtered:
                neg_filtered = _exclude_items_with_negative_probes(items, neg_probes)
            if neg_filtered:
                candidate_items = list(neg_filtered)
        asks_min_price = asks_min_price_global
        asks_max_price = asks_max_price_global
        normalized_words = len(re.findall(r"(?u)\b\w+\b", normalized))
        normalized_sentences = len([s for s in re.split(r"[.!?]+", normalized) if s.strip()])
        if (not asks_min_price) and (not asks_max_price):
            if normalized_words >= 10 and (normalized_sentences >= 2 or "?" in normalized):
                spans = _extract_price_spans(normalized)
                if not spans:
                    return normalized
                if mentions_known and all(int(span[2]) in {int(p) for p in [_item_price_int(dict(it)) for it in items] if p} for span in spans):
                    return normalized
        if asks_max_price:
            max_item = _catalog_extreme_item_by_price(candidate_items or items, highest=True)
            if max_item is not None:
                max_name = _item_label(dict(max_item))
                max_price = _item_price_int(dict(max_item))
                if max_name and max_price:
                    return f"{max_name} {_format_rub_price(max_price)}".strip()
        if asks_min_price:
            min_item = _catalog_extreme_item_by_price(candidate_items or items, highest=False)
            if min_item is not None:
                min_name = _item_label(dict(min_item))
                min_price = _item_price_int(dict(min_item))
                if min_name and min_price:
                    return f"{min_name} {_format_rub_price(min_price)}".strip()
        if selected_item is not None and (not asks_min_price):
            selected_name = _item_label(dict(selected_item))
            selected_price = _item_price_int(dict(selected_item))
            if selected_name and selected_price:
                return f"{selected_name} {_format_rub_price(selected_price)}".strip()
        target_price = _extract_price_target_hint(user_text)
        if target_price:
            nearest = _closest_catalog_item_by_price(candidate_items, target_price)
            if nearest is not None:
                name = _item_label(dict(nearest))
                price = _item_price_int(dict(nearest))
                if name and price:
                    return f"{name} {_format_rub_price(price)}".strip()
        min_price = _catalog_min_price(candidate_items)
        if min_price:
            return _format_rub_price(min_price)

    if asks_model_name:
        if (
            object_type_need in {"apartment", "house"}
            and (not object_filter_restricted)
            and (not object_type_has_evidence)
            and (not has_specific_kw)
        ):
            return _neutralize_catalog_model_mentions(normalized, items)
        if selected_item is not None:
            selected_name = _item_label(dict(selected_item))
            selected_price = _item_price_int(dict(selected_item))
            if selected_name and selected_price:
                return f"{selected_name} {_format_rub_price(selected_price)}".strip()
        target_price = _extract_price_target_hint(user_text)
        ref_item: Mapping[str, Any] | None = None
        if target_price:
            ref_item = _closest_catalog_item_by_price(candidate_items, target_price)
        if ref_item is None and candidate_items:
            ref_item = candidate_items[0]
        if ref_item is not None:
            name = _item_label(dict(ref_item))
            price = _item_price_int(dict(ref_item))
            if name and price:
                return f"{name} {_format_rub_price(price)}".strip()
            if name:
                return str(name or "").strip()

    return normalized


def _ensure_concrete_variants_in_reply(
    text: str,
    *,
    grounding: Mapping[str, Any] | None = None,
    user_text: str = "",
) -> str:
    base = (text or "").strip()
    if not base:
        return base
    items = _grounding_catalog_items(grounding)
    if not items:
        return base
    user_has_variants_intent = bool(_VARIANTS_USER_HINT_RE.search(str(user_text or "")))
    bot_promised_variants = bool(_VARIANTS_PROMISE_RE.search(base))
    has_unknown_model = _reply_mentions_unknown_model(base, items)
    # Do not override a valid persona step question (city/address/object/model clarifications).
    for question in _extract_questions_from_text(base):
        if any(_question_covers_fact(question, fact) for fact in ("city", "address", "object_type")):
            return base
        if _question_covers_fact(question, "model"):
            # If client explicitly asks for variants/prices (or bot promised to show them),
            # prefer concrete shortlist over another model-clarifying question.
            if not (user_has_variants_intent or bot_promised_variants or has_unknown_model):
                return base
    return base


def _rewrite_loses_context_anchors(
    candidate: str,
    rewrite: str,
    dialogue_tail: Sequence[Mapping[str, Any]],
) -> bool:
    cand = str(candidate or "").strip()
    rew = str(rewrite or "").strip()
    if not cand or not rew:
        return False
    cand_norm = _normalize_model_alias(cand)
    rew_norm = _normalize_model_alias(rew)
    if not cand_norm or not rew_norm:
        return False

    cand_nums = set(re.findall(r"\d{2,}", cand))
    rew_nums = set(re.findall(r"\d{2,}", rew))
    if cand_nums and not (cand_nums & rew_nums):
        return True

    anchor_tokens: set[str] = set()
    for item in dialogue_tail or []:
        if str(item.get("role") or "").strip().lower() != "user":
            continue
        content = str(item.get("content") or "")
        for token in _FACT_TOKEN_RE.findall(content.lower().replace("ё", "е")):
            if len(token) < 4:
                continue
            if token in _GENERIC_FACT_STOPWORDS:
                continue
            anchor_tokens.add(token)
    if not anchor_tokens:
        return False
    cand_hits = {token for token in anchor_tokens if token in cand_norm}
    if not cand_hits:
        return False
    rew_hits = {token for token in anchor_tokens if token in rew_norm}
    return not rew_hits


async def _audit_policy_block_requirements(
    create_fn: Any,
    *,
    model: str,
    timeout_seconds: float,
    persona_context: str,
    blocks: Sequence[Mapping[str, Any]],
    known_facts: Mapping[str, str],
    last_user_message: str,
) -> Dict[int, List[str]]:
    prepared_blocks: List[Dict[str, Any]] = []
    for idx, raw in enumerate(blocks[:8]):
        if not isinstance(raw, Mapping):
            continue
        text = str(raw.get("text") or "").strip()
        if not text:
            continue
        block_type = str(raw.get("type") or "").strip().lower()
        declared_requires = raw.get("requires")
        if isinstance(declared_requires, str):
            declared = [_normalize_fact_key(declared_requires)]
        elif isinstance(declared_requires, Sequence):
            declared = [_normalize_fact_key(str(x)) for x in declared_requires]
        else:
            declared = []
        prepared_blocks.append(
            {
                "index": idx,
                "type": block_type,
                "text": text,
                "declared_requires": [x for x in declared if x],
            }
        )
    if not prepared_blocks:
        return {}

    persona_excerpt = (persona_context or "").strip()
    if len(persona_excerpt) > 4000:
        persona_excerpt = persona_excerpt[:4000]

    system_prompt = (
        "Ты проверяешь блоки ответа менеджера на корректные зависимости от фактов клиента. "
        "Верни только JSON-объект формата: "
        '{"audited":[{"index":0,"requires":["city","address"]}]}. '
        "Для каждого блока укажи минимально необходимые facts, без которых блок нельзя отправлять. "
        "Если блок безопасен без фактов, верни пустой requires. "
        "Не добавляй вымышленные ключи: используй только осмысленные простые ключи facts (city,address,source,budget,model и т.п.)."
    )
    user_prompt = (
        f"Персона/политика:\n{persona_excerpt}\n\n"
        f"Последнее сообщение клиента: {last_user_message}\n"
        f"Известные facts: {json.dumps(dict(known_facts or {}), ensure_ascii=False)}\n"
        f"Блоки для аудита: {json.dumps(prepared_blocks, ensure_ascii=False)}"
    )
    try:
        resp = await _llm_call_with_deadline(
            create_fn,
            timeout_seconds=timeout_seconds,
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=420,
            response_format={"type": "json_object"},
            timeout=timeout_seconds,
        )
        choices = getattr(resp, "choices", None)
        if not (isinstance(choices, list) and choices):
            return {}
        msg = getattr(choices[0], "message", None)
        payload = _safe_json_load(str(getattr(msg, "content", "") or ""))
        audited = payload.get("audited")
        if not isinstance(audited, list):
            return {}
        overrides: Dict[int, List[str]] = {}
        for item in audited:
            if not isinstance(item, Mapping):
                continue
            try:
                idx = int(item.get("index"))
            except Exception:
                continue
            req = item.get("requires")
            if isinstance(req, str):
                keys = [_normalize_fact_key(req)]
            elif isinstance(req, Sequence):
                keys = [_normalize_fact_key(str(x)) for x in req]
            else:
                keys = []
            keys = [x for x in keys if x]
            if keys:
                overrides[idx] = keys
        return overrides
    except Exception:
        return {}


async def _audit_policy_block_allowance(
    create_fn: Any,
    *,
    model: str,
    timeout_seconds: float,
    persona_context: str,
    blocks: Sequence[Mapping[str, Any]],
    known_facts: Mapping[str, str],
    last_user_message: str,
) -> Dict[int, bool]:
    prepared_blocks: List[Dict[str, Any]] = []
    for idx, raw in enumerate(blocks[:8]):
        if not isinstance(raw, Mapping):
            continue
        text = str(raw.get("text") or "").strip()
        if not text:
            continue
        prepared_blocks.append(
            {
                "index": idx,
                "type": str(raw.get("type") or "").strip().lower(),
                "text": text,
                "requires": raw.get("requires") or [],
            }
        )
    if not prepared_blocks:
        return {}

    persona_excerpt = (persona_context or "").strip()
    if len(persona_excerpt) > 4000:
        persona_excerpt = persona_excerpt[:4000]

    system_prompt = (
        "Ты валидатор блоков ответа менеджера. Верни только JSON: "
        '{"audited":[{"index":0,"allow":true,"missing":["address"]}]}. '
        "allow=false, если блок нельзя отправлять при текущих известных facts и правилах персоны. "
        "Запрещай любые утверждения/обещания, которые не подтверждены facts или требуют условий, которые не выполнены. "
        "Если у блока type=question и вопрос уместен, allow=true."
    )
    user_prompt = (
        f"Правила персоны:\n{persona_excerpt}\n\n"
        f"Последнее сообщение клиента: {last_user_message}\n"
        f"Известные facts: {json.dumps(dict(known_facts or {}), ensure_ascii=False)}\n"
        f"Кандидатные блоки: {json.dumps(prepared_blocks, ensure_ascii=False)}"
    )
    try:
        resp = await _llm_call_with_deadline(
            create_fn,
            timeout_seconds=timeout_seconds,
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=420,
            response_format={"type": "json_object"},
            timeout=timeout_seconds,
        )
        choices = getattr(resp, "choices", None)
        if not (isinstance(choices, list) and choices):
            return {}
        msg = getattr(choices[0], "message", None)
        payload = _safe_json_load(str(getattr(msg, "content", "") or ""))
        audited = payload.get("audited")
        if not isinstance(audited, list):
            return {}
        result: Dict[int, bool] = {}
        for item in audited:
            if not isinstance(item, Mapping):
                continue
            try:
                idx = int(item.get("index"))
            except Exception:
                continue
            allow = bool(item.get("allow"))
            result[idx] = allow
        return result
    except Exception:
        return {}


def _fallback_semantic_plan(last_user_message: str) -> Dict[str, Any]:
    return {
        "_fallback": True,
        "intent": "clarify",
        "ack": "",
        "core": "",
        "question": "",
        "question_slot": "none",
        "required_facts": [],
        "facts_update": {},
        "blocks": [],
    }


async def _semantic_plan(
    create_fn: Any,
    *,
    model: str,
    timeout_seconds: float,
    messages: List[Dict[str, str]],
    last_user_message: str,
    known_slots: Mapping[str, str] | None = None,
    known_facts: Mapping[str, str] | None = None,
    forbidden_question_topics: Sequence[str] | None = None,
    grounding_items: Sequence[Mapping[str, Any]] | None = None,
) -> Dict[str, Any]:
    dialogue = [
        {"role": str(m.get("role") or ""), "content": str(m.get("content") or "")}
        for m in (messages or [])
        if str(m.get("role") or "") in {"user", "assistant"}
    ][-8:]
    persona_chunks: list[str] = []
    for item in messages or []:
        if str(item.get("role") or "").strip().lower() == "system":
            chunk = str(item.get("content") or "").strip()
            if chunk:
                persona_chunks.append(chunk)
    persona_context = "\n\n".join(persona_chunks)
    if len(persona_context) > 6000:
        persona_context = persona_context[:6000]
    known_slots = dict(known_slots or {})
    known_facts = dict(known_facts or {})
    forbidden_question_topics = [
        str(x) for x in (forbidden_question_topics or []) if str(x).strip()
    ]
    catalog_context = format_items_for_prompt(
        [dict(item) for item in (grounding_items or [])[:5]], "₽"
    )
    if not (grounding_items or []):
        catalog_context = ""
    plan_system = (
        "Ты планировщик ответа менеджера. Верни только JSON-объект. "
        "Схема: {"
        '"intent":"greet|clarify|offer|answer|next_step",'
        '"ack":"короткое подтверждение",'
        '"core":"главная мысль ответа",'
        '"question":"один уместный вопрос или пусто",'
        '"question_slot":"location|object|model|budget|timeline|dimensions|contact|quantity|color|other|none",'
        '"required_facts": ["city","address","object_type", ...],'
        '"facts_update": {"key":"value", ...},'
        '"blocks": ['
        '{"text":"фраза для ответа","requires":["fact_key"],"type":"ack|info|offer|question|cta",'
        '"question_key":"ключ факта для question, иначе пусто"}'
        "]"
        "}. "
        "Опирайся на правила и порядок из контекста персоны. "
        "Если в персоне задан первый квалифицирующий шаг, следуй ему, но сначала отвечай на текущий вопрос клиента. "
        "Не используй штампы: 'Спасибо, понял', 'Понял', 'Поняла', 'Хороший выбор', 'Ваш запрос принят'. "
        "Не используй общую фразу 'Чем могу помочь?' как основной вопрос. "
        "Не задавай вопрос, который не связан с последней репликой клиента. "
        "Если слот уже известен, не задавай вопрос про него повторно. "
        "Не выходи за рамки каталога: не предлагай категории/товары, которых нет в catalog context. "
        "Для любой утверждающей фразы, которая зависит от условий (город, адрес, срок, скидка, источник и т.п.), "
        "обязательно укажи requires с нужными фактами. "
        "Заполни required_facts: это минимальные данные, которые нужно собрать по персоне до персонализированных утверждений/офферов. "
        "Если факт не подтвержден, не придумывай и не добавляй такую фразу в blocks."
    )
    plan_user = (
        f"Контекст персоны и правил:\n{persona_context}\n\n"
        f"Уже известные слоты: {json.dumps(known_slots, ensure_ascii=False)}\n"
        f"Уже известные факты: {json.dumps(known_facts, ensure_ascii=False)}\n"
        f"Запрещённые темы вопроса: {json.dumps(forbidden_question_topics, ensure_ascii=False)}\n"
        f"Релевантные позиции каталога:\n{catalog_context or 'нет'}\n\n"
        f"Последнее сообщение клиента: {last_user_message}\n"
        f"Недавний диалог: {json.dumps(dialogue, ensure_ascii=False)}"
    )
    try:
        resp = await _llm_call_with_deadline(
            create_fn,
            timeout_seconds=timeout_seconds,
            model=model,
            messages=[
                {"role": "system", "content": plan_system},
                {"role": "user", "content": plan_user},
            ],
            temperature=0.0,
            max_tokens=220,
            response_format={"type": "json_object"},
            timeout=timeout_seconds,
        )
        choices = getattr(resp, "choices", None)
        if isinstance(choices, list) and choices:
            msg = getattr(choices[0], "message", None)
            payload = _safe_json_load(str(getattr(msg, "content", "") or ""))
            if payload:
                return payload
    except Exception:
        pass
    return _fallback_semantic_plan(last_user_message)


async def _render_from_semantic_plan(
    create_fn: Any,
    *,
    model: str,
    timeout_seconds: float,
    prepared_messages: List[Dict[str, str]],
    plan: Dict[str, Any],
    known_slots: Mapping[str, str] | None = None,
    forbidden_question_topics: Sequence[str] | None = None,
) -> str:
    known_slots = dict(known_slots or {})
    forbidden_question_topics = [
        str(x) for x in (forbidden_question_topics or []) if str(x).strip()
    ]
    known_block = (
        "Известные данные клиента: " + json.dumps(known_slots, ensure_ascii=False)
        if known_slots
        else "Известные данные клиента: {}"
    )
    forbid_block = (
        "Не задавай вопрос по темам: " + ", ".join(forbidden_question_topics)
        if forbidden_question_topics
        else "Запрещённых тем вопроса нет."
    )
    style_system = (
        "Рендерни финальное сообщение менеджера строго по плану. "
        "1-3 коротких предложения, максимум 1 вопрос. "
        "Живой разговорный тон, без канцелярита и без шаблонов. "
        "Не начинай ответ с 'Понял', 'Поняла', 'Спасибо, что уточнили'. "
        "Не начинай с повтора сущности клиента + подтверждения. "
        "Не начинай с оценочного клише после выбора клиента. "
        "Не переспрашивай уже известные данные. "
        f"{known_block} {forbid_block}"
    )
    render_messages: List[Dict[str, str]] = []
    if prepared_messages:
        render_messages.append(prepared_messages[0])
        render_messages.extend(prepared_messages[1:])
    else:
        render_messages.append({"role": "system", "content": ""})
    render_messages.append({"role": "system", "content": style_system})
    render_messages.append(
        {
            "role": "user",
            "content": "План ответа (JSON): " + json.dumps(plan, ensure_ascii=False),
        }
    )
    resp = await _llm_call_with_deadline(
        create_fn,
        timeout_seconds=timeout_seconds,
        model=model,
        messages=render_messages,
        max_tokens=180,
        temperature=settings.OPENAI_TEMPERATURE,
        top_p=0.95,
        frequency_penalty=0.08,
        presence_penalty=0.04,
        timeout=timeout_seconds,
    )
    choices = getattr(resp, "choices", None)
    if isinstance(choices, list) and choices:
        msg = getattr(choices[0], "message", None)
        return str(getattr(msg, "content", "") or "").strip()
    return ""


async def _render_direct_reply(
    create_fn: Any,
    *,
    model: str,
    timeout_seconds: float,
    prepared_messages: List[Dict[str, str]],
) -> str:
    direct_messages: List[Dict[str, str]] = []
    if prepared_messages:
        direct_messages.extend(prepared_messages[-10:])
    direct_messages.append(
        {
            "role": "system",
            "content": (
                "Ответь клиенту строго по персоне и контексту диалога. "
                "1-3 коротких предложения, максимум 1 вопрос. "
                "Без канцелярита и без шаблонов."
            ),
        }
    )
    resp = await _llm_call_with_deadline(
        create_fn,
        timeout_seconds=timeout_seconds,
        model=model,
        messages=direct_messages,
        max_tokens=180,
        temperature=settings.OPENAI_TEMPERATURE,
        top_p=0.95,
        frequency_penalty=0.08,
        presence_penalty=0.04,
        timeout=timeout_seconds,
    )
    choices = getattr(resp, "choices", None)
    if isinstance(choices, list) and choices:
        msg = getattr(choices[0], "message", None)
        return str(getattr(msg, "content", "") or "").strip()
    return ""


async def _audit_and_rewrite_persona_reply(
    create_fn: Any,
    *,
    model: str,
    timeout_seconds: float,
    prepared_messages: List[Dict[str, str]],
    answer: str,
    last_user_message: str,
    state: SalesState | None = None,
    grounding: Mapping[str, Any] | None = None,
    policy: Mapping[str, Any] | None = None,
) -> str:
    # Two-stage quality gate:
    # 1) deterministic cleanup
    # 2) LLM judge + optional bounded rewrite
    candidate = (answer or "").strip()
    if not candidate:
        return candidate

    dialogue_tail = [
        {"role": str(m.get("role") or ""), "content": str(m.get("content") or "")}
        for m in (prepared_messages or [])
        if str(m.get("role") or "").strip().lower() in {"user", "assistant"}
    ][-6:]

    # Deterministic cleanup only.
    rewrite = _strip_instruction_leaks(candidate)
    rewrite = quality._dedupe_repeated_blocks(rewrite)
    rewrite = re.sub(r"[ \t]+", " ", rewrite)
    rewrite = re.sub(r"\n{3,}", "\n\n", rewrite).strip()
    if not rewrite:
        return candidate

    # Do not lose factual anchors from user context.
    if _rewrite_loses_context_anchors(candidate, rewrite, dialogue_tail):
        return candidate

    # Do not drop contact artifacts if they already existed in answer.
    original_artifacts: set[str] = set()
    for token in _CONTACT_URL_RE.findall(candidate):
        original_artifacts.add(token)
    for token in _CONTACT_HANDLE_RE.findall(candidate):
        original_artifacts.add(token)
    for token in _CONTACT_PHONE_RE.findall(candidate):
        original_artifacts.add(token.strip())
    if original_artifacts and not all(artifact in rewrite for artifact in original_artifacts):
        return candidate

    # Keep substantive answer if cleanup accidentally over-shrunk text.
    if len(rewrite) < 8 and len(candidate) > 24:
        return candidate
    if isinstance(state, SalesState) and _reply_has_repeated_question(rewrite, state):
        # Rewriting should not reintroduce repeated question loops.
        return candidate

    candidate = rewrite

    persona_context = ""
    for message in prepared_messages or []:
        if str(message.get("role") or "").strip().lower() != "system":
            continue
        chunk = str(message.get("content") or "").strip()
        if chunk:
            persona_context = chunk
            break
    if len(persona_context) > 5000:
        persona_context = persona_context[:5000]
    grounding_items = _grounding_catalog_items(grounding)
    grounding_preview = format_items_for_prompt(
        [dict(item) for item in list(grounding_items or [])[:6]], "₽"
    )
    selected_item = _selected_item_from_grounding(grounding, grounding_items)
    selected_label = _item_label(dict(selected_item)) if isinstance(selected_item, Mapping) else ""
    policy_tags = {
        str(tag or "").strip().lower()
        for tag in (policy or {}).get("intent_tags", [])
        if str(tag or "").strip()
    }
    user_raw = str(last_user_message or "")
    user_norm = _normalize_text(user_raw)
    user_tokens = [
        tok
        for tok in _FACT_TOKEN_RE.findall(user_norm)
        if len(tok) >= 3 and not tok.isdigit() and tok not in _GENERIC_FACT_STOPWORDS
    ]
    greeting_like = bool(_GREETING_PREFIX_RE.match(user_raw))
    attr_probe = _extract_attribute_probe(user_raw)
    has_attr_intent_cue = bool(_QUESTION_CUE_RE.search(user_raw) or "?" in user_raw)
    lexical_attribute_intent = bool(attr_probe) and not greeting_like and (
        has_attr_intent_cue
        or bool(_MODEL_NAME_INTENT_RE.search(user_raw))
        or bool(_VARIANTS_USER_HINT_RE.search(user_raw))
        or bool(_is_price_intent(user_raw))
        or len(user_tokens) >= 2
    )
    intent_flags = {
        "price_intent": bool("price" in policy_tags) or bool(_is_price_intent(last_user_message)),
        "variants_intent": bool({"variants", "selection"} & policy_tags)
        or bool(_VARIANTS_USER_HINT_RE.search(str(last_user_message or ""))),
        "repair_intent": bool(
            {"repair", "complaint"} & policy_tags
            or
            _REPAIR_TURN_RE.match(str(last_user_message or ""))
            or _CATALOG_UNAVAILABLE_RE.search(str(last_user_message or ""))
        ),
        "attribute_intent": bool("attributes" in policy_tags) or lexical_attribute_intent,
    }

    judge_prompt = (
        "Ты quality-judge ответа менеджера. Верни только JSON: "
        '{"ok":true|false,"rewrite_needed":true|false,"issues":["..."],'
        '"needs":{"price":true|false,"variants":true|false,"attributes":true|false,"catalog_recovery":true|false}}. '
        "Критерии: "
        "1) ответ уместен последней реплике клиента, "
        "2) нет повтора уже заданного вопроса, "
        "3) нет служебной/инструктивной речи, "
        "4) соблюдены ограничения персоны по тону и шагам, "
        "5) нет неподтверждённых factual-утверждений, которых нет в grounded catalog context, "
        "6) нет роботизированных стартов вроде 'Понял', 'Поняла', 'Спасибо, что уточнили', "
        "7) если клиент просит варианты/цену/характеристики, ответ должен содержать конкретику из grounded catalog context, "
        "8) максимум один вопрос в ответе. "
        "9) если intent_flags.price_intent=true, ответ должен явно закрывать вопрос цены "
        "(цена/диапазон/дешевле/альтернатива) без ухода в общие фразы. "
        "10) если intent_flags.variants_intent=true, ответ должен показать конкретные варианты/модели "
        "или сразу перейти к показу, без абстрактного ответа. "
        "11) если intent_flags.repair_intent=true, ответ должен вернуть диалог к каталогу/вариантам, "
        "а не зависать в пустом уточнении. "
        "12) выстави needs.* на основе последней реплики клиента и хвоста диалога: "
        "price=true если ждут ответ по цене/дешевле/диапазону; "
        "variants=true если ждут показать/предложить варианты; "
        "attributes=true если ждут характеристики/качество/параметры; "
        "catalog_recovery=true если диалог нужно вернуть к каталогу/подбору после сбоя/непонимания. "
        "Если ответ уже хороший — ok=true."
    )
    judge_user = (
        f"Персона:\n{persona_context or 'нет'}\n\n"
        f"Grounded catalog context:\n{grounding_preview or 'нет'}\n"
        f"Selected item: {selected_label or 'нет'}\n\n"
        f"Intent flags: {json.dumps(intent_flags, ensure_ascii=False)}\n\n"
        f"Последняя реплика клиента: {last_user_message}\n"
        f"Текущий ответ: {candidate}\n"
        f"Хвост диалога: {json.dumps(dialogue_tail, ensure_ascii=False)}"
    )
    try:
        judge_resp = await _llm_call_with_deadline(
            create_fn,
            timeout_seconds=timeout_seconds,
            model=model,
            messages=[
                {"role": "system", "content": judge_prompt},
                {"role": "user", "content": judge_user},
            ],
            temperature=0.0,
            max_tokens=120,
            response_format={"type": "json_object"},
            timeout=timeout_seconds,
        )
        judge_choices = getattr(judge_resp, "choices", None)
        judge_payload: dict[str, Any] = {}
        if isinstance(judge_choices, list) and judge_choices:
            judge_msg = getattr(judge_choices[0], "message", None)
            judge_payload = _safe_json_load(str(getattr(judge_msg, "content", "") or "")) or {}
        judge_ok = bool(judge_payload.get("ok"))
        rewrite_needed = bool(judge_payload.get("rewrite_needed"))
        issues_raw = judge_payload.get("issues")
        if isinstance(issues_raw, list):
            issues = [str(item).strip() for item in issues_raw if str(item).strip()]
        else:
            issues = []
        needs_payload = judge_payload.get("needs")
        needs_map = needs_payload if isinstance(needs_payload, Mapping) else {}
        needs_price = bool(needs_map.get("price")) or bool(intent_flags.get("price_intent"))
        needs_variants = bool(needs_map.get("variants")) or bool(intent_flags.get("variants_intent"))
        needs_recovery = bool(needs_map.get("catalog_recovery")) or bool(intent_flags.get("repair_intent"))
        needs_attributes = bool(needs_map.get("attributes")) or bool(intent_flags.get("attribute_intent"))

        candidate_low = _normalize_text(candidate)
        has_price_specific = bool(_PRICE_INLINE_RE.search(candidate)) or bool(
            re.search(r"(?iu)\b(цен|диапаз|дешев|альтернатив)\w*\b", candidate_low)
        )
        has_catalog_specific = has_price_specific or _reply_mentions_catalog_item(
            candidate, grounding_items
        )
        if needs_variants and not has_catalog_specific:
            rewrite_needed = True
            judge_ok = False
            issues.append("variants_without_catalog_specifics")
        if needs_price and not has_price_specific:
            rewrite_needed = True
            judge_ok = False
            issues.append("price_intent_without_price_specifics")
        if needs_recovery:
            has_repair_recovery = bool(
                re.search(r"(?iu)\b(модел|вариант|каталог|цен|характерист)\w*\b", candidate_low)
            ) or has_catalog_specific
            if not has_repair_recovery:
                rewrite_needed = True
                judge_ok = False
                issues.append("repair_intent_without_catalog_recovery")
        if needs_attributes:
            probe = attr_probe
            probe_norm = _normalize_text(probe)
            attribute_terms: set[str] = set()
            for item in list(grounding_items or [])[:4]:
                for key, value in dict(item).items():
                    key_norm = _normalize_text(key)
                    if not key_norm or key_norm.startswith("_"):
                        continue
                    if key_norm in {"title", "name", "sku", "id", "url", "price"}:
                        continue
                    for token in _FACT_TOKEN_RE.findall(key_norm):
                        if len(token) >= 4 and token not in _GENERIC_FACT_STOPWORDS:
                            attribute_terms.add(token)
                    val_norm = _normalize_text(value)
                    for token in _FACT_TOKEN_RE.findall(val_norm):
                        if len(token) >= 4 and token not in _GENERIC_FACT_STOPWORDS:
                            attribute_terms.add(token)
            has_attribute_terms = any(token in candidate_low for token in list(attribute_terms)[:30])
            has_attribute_specific = (
                bool(re.search(r"\d", candidate))
                or (bool(probe_norm) and probe_norm in candidate_low)
                or has_attribute_terms
            )
            if not has_attribute_specific:
                rewrite_needed = True
                judge_ok = False
                issues.append("attribute_intent_without_specifics")
        if judge_ok or not rewrite_needed:
            return candidate

        rewrite_system = (
            "Перепиши ответ менеджера строго в рамках смысла текущего ответа. "
            "Не добавляй новые факты, цены или обещания. "
            "Не используй стартовые шаблоны вроде 'Понял', 'Поняла', 'Спасибо, что уточнили'. "
            "Если факт не подтверждён grounded catalog context, не утверждай его как факт. "
            "Если клиент просит варианты/цену/характеристики, добавь конкретику из grounded catalog context. "
            "Если intent_flags.price_intent=true, обязательно закрой вопрос цены (цена/диапазон/дешевле/альтернатива). "
            "Если intent_flags.variants_intent=true, покажи конкретные варианты/модели или явно предложи показать варианты сейчас. "
            "Если intent_flags.repair_intent=true, верни диалог к подбору из каталога с конкретикой. "
            "Если needs.attributes=true, обязательно дай конкретные параметры из grounded context "
            "(например толщина/материал/наполнение или другой подтверждённый атрибут). "
            "Ответь на последнюю реплику клиента в живом тоне. "
            "1-2 коротких предложения, максимум 1 вопрос."
        )
        rewrite_user = (
            f"Персона:\n{persona_context or 'нет'}\n\n"
            f"Grounded catalog context:\n{grounding_preview or 'нет'}\n"
            f"Selected item: {selected_label or 'нет'}\n\n"
            f"Intent flags: {json.dumps(intent_flags, ensure_ascii=False)}\n\n"
            f"Needs: {json.dumps(needs_map, ensure_ascii=False)}\n\n"
            f"Последняя реплика клиента: {last_user_message}\n"
            f"Проблемы текущего ответа: {json.dumps(issues, ensure_ascii=False)}\n"
            f"Текущий ответ: {candidate}\n"
            "Сделай улучшенный ответ."
        )
        rewrite_resp = await _llm_call_with_deadline(
            create_fn,
            timeout_seconds=timeout_seconds,
            model=model,
            messages=[
                {"role": "system", "content": rewrite_system},
                {"role": "user", "content": rewrite_user},
            ],
            temperature=0.2,
            max_tokens=180,
            timeout=timeout_seconds,
        )
        rewrite_choices = getattr(rewrite_resp, "choices", None)
        if isinstance(rewrite_choices, list) and rewrite_choices:
            rewrite_msg = getattr(rewrite_choices[0], "message", None)
            improved = str(getattr(rewrite_msg, "content", "") or "").strip()
            if improved:
                improved = _strip_instruction_leaks(improved)
                improved = quality._dedupe_repeated_blocks(improved)
                improved = re.sub(r"[ \t]+", " ", improved)
                improved = re.sub(r"\n{3,}", "\n\n", improved).strip()
                improved = _enforce_sentence_budget(improved, max_sentences=2)
                if improved and not _rewrite_loses_context_anchors(candidate, improved, dialogue_tail):
                    if not (isinstance(state, SalesState) and _reply_has_repeated_question(improved, state)):
                        return improved
    except Exception:
        return candidate
    return candidate


def _apply_plan_alignment_to_state(
    state: SalesState,
    context: "quality.EnforcementContext",
    previous_fingerprints: set[str],
) -> None:
    if not isinstance(state, SalesState):
        return
    new_fingerprints = set(context.asked_fingerprints or []) - set(previous_fingerprints or set())
    for fingerprint in new_fingerprints:
        question = context.fingerprint_map.get(fingerprint)
        if question:
            _remember_question_state(state, question)
    for question in context.applied_questions or []:
        _remember_question_state(state, question)
    if context.applied_cta:
        _remember_cta_state(state, context.applied_cta)


def _make_enforcement_context(
    state: SalesState,
    persona_hints: Optional[PersonaHints],
    channel_name: str,
) -> "quality.EnforcementContext":
    asked = set(state.asked_question_fingerprints or [])
    ctx = quality.EnforcementContext(
        channel=channel_name,
        max_questions=_max_questions_limit(persona_hints),
        asked_fingerprints=set(asked),
        persona_cta=persona_hints.cta if persona_hints else "",
        allow_cta=_cta_allowed(state, channel_name),
        recent_cta=state.cta_last_text,
        recent_cta_ts=state.cta_last_sent_ts,
    )
    return ctx


@dataclass
class PersonaHints:
    greeting: str = ""
    cta: str = ""
    closing: str = ""
    tone: str = ""
    language: str = ""
    max_questions: Optional[int] = None
    style_short: bool = False
    style_friendly: bool = False
    no_emoji: bool = False

    def wants_short(self) -> bool:
        if self.style_short:
            return True
        tone = (self.tone or "").lower()
        return any(token in tone for token in ("корот", "лакон", "brief", "concise", "short"))

    def wants_friendly(self) -> bool:
        if self.style_friendly:
            return True
        tone = (self.tone or "").lower()
        return any(token in tone for token in ("дружелюб", "тепл", "friendly", "human"))


def _clean_persona_line(line: str) -> str:
    return re.sub(r"^[\-•*\s]+", "", line or "").strip()


def extract_persona_hints(persona: str) -> PersonaHints:
    hints = PersonaHints()
    if not persona:
        return hints

    lines = [_clean_persona_line(line) for line in persona.splitlines()]
    persona_lower = persona.lower()

    for raw in lines:
        if not raw:
            continue
        m = _PERSONA_HINTS_KEY_RE.match(raw)
        if not m:
            continue
        key, value = m.group(1).lower(), m.group(2).strip()
        if key.startswith("greeting") or key.startswith("приветств"):
            hints.greeting = value
        elif key == "cta" or key.startswith("призыв"):
            hints.cta = value
        elif key.startswith("closing") or key.startswith("заверш"):
            hints.closing = value
        elif key.startswith("tone") or key.startswith("тон"):
            hints.tone = value
        elif key.startswith("language") or key.startswith("язык"):
            hints.language = value
        elif "max" in key:
            digits = re.findall(r"\d+", value)
            if digits:
                try:
                    hints.max_questions = int(digits[0])
                except Exception:
                    pass

    if not hints.greeting:
        for raw in lines:
            if not raw or raw.startswith("#"):
                continue
            low = raw.lower()
            if low.startswith(("правила", "техники")):
                continue
            if any(token in low for token in ("привет", "здрав", "меня зовут")):
                hints.greeting = raw
                break
        if not hints.greeting:
            for raw in lines:
                if raw and not raw.startswith(("#", "-", "*")):
                    hints.greeting = raw
                    break

    if not hints.cta:
        m = re.search(r"cta[^\n]*?:\s*(.+)", persona, re.IGNORECASE)
        if m:
            hints.cta = m.group(1).strip()

    if hints.max_questions is None:
        m = re.search(r"≤\s*(\d+)\s*(?:уточн|вопрос)", persona_lower)
        if m:
            try:
                hints.max_questions = int(m.group(1))
            except Exception:
                pass

    if any(
        token in persona_lower
        for token in ("коротко", "кратко", "лаконич", "brief", "concise", "short")
    ):
        hints.style_short = True
    if any(token in persona_lower for token in ("дружелюб", "тепл", "friendly", "улыб")):
        hints.style_friendly = True
    if any(
        token in persona_lower
        for token in ("без смай", "без эмодзи", "без emoji", "без эмоджи", "без эмодзи")
    ):
        hints.no_emoji = True

    return hints


_PERSONA_HINTS_CACHE: Dict[tuple[int | None, str], Tuple[str, PersonaHints]] = {}


def _persona_hints_cache_key(tenant: int | None, channel: str | None) -> tuple[int | None, str]:
    channel_key = (channel or "").strip().lower()
    try:
        tenant_key = int(tenant) if tenant is not None else None
    except Exception:
        tenant_key = None
    return tenant_key, channel_key


def _clear_persona_hints_cache(tenant: int | None) -> None:
    if tenant is None:
        _PERSONA_HINTS_CACHE.clear()
        return
    try:
        tenant_key = int(tenant)
    except Exception:
        _PERSONA_HINTS_CACHE.clear()
        return
    for cache_key in list(_PERSONA_HINTS_CACHE.keys()):
        if cache_key[0] == tenant_key:
            _PERSONA_HINTS_CACHE.pop(cache_key, None)


def load_persona_hints(tenant: int | None = None, channel: str | None = None) -> PersonaHints:
    persona_text = load_persona(tenant, channel)
    fingerprint = hashlib.sha1(persona_text.encode("utf-8")).hexdigest() if persona_text else ""
    cache_key = _persona_hints_cache_key(tenant, channel)
    cached = _PERSONA_HINTS_CACHE.get(cache_key)
    if cached and cached[0] == fingerprint:
        return cached[1]
    hints = extract_persona_hints(persona_text)
    _PERSONA_HINTS_CACHE[cache_key] = (fingerprint, hints)
    return hints


def load_sales_state(tenant: int | None, contact_id: int | None) -> SalesState:
    key = _state_key(tenant, contact_id)
    payload = _state_store_read(key)
    if payload:
        state = SalesState.from_dict(payload)
        _STATE_CACHE[key] = state
    else:
        _STATE_CACHE.pop(key, None)
        state = SalesState(tenant=int(tenant or 0), contact_id=int(contact_id or 0))
    return state


def save_sales_state(state: SalesState) -> None:
    key = _state_key(state.tenant, state.contact_id)
    payload = state.to_dict()
    _STATE_CACHE[key] = state
    _state_store_write(key, payload)


def reset_sales_state(tenant: int | None, contact_id: int | None) -> None:
    key = _state_key(tenant, contact_id)
    _STATE_CACHE.pop(key, None)
    _with_sync_redis(lambda client: client.delete(key), None)


# --------------------------- хранилище ключей (Redis) ------------------------


def get_tenant_pubkey(tenant: int) -> str:
    return _with_sync_redis(
        lambda client: client.hget(TENANT_PUBKEYS_HASH, str(int(tenant))) or "",
        "",
    )


def set_tenant_pubkey(tenant: int, key: str) -> None:
    key_norm = (key or "").strip().lower()

    def _apply(client: redis_sync.Redis) -> None:
        if key_norm:
            client.hset(TENANT_PUBKEYS_HASH, str(int(tenant)), key_norm)
        else:
            client.hdel(TENANT_PUBKEYS_HASH, str(int(tenant)))

    _with_sync_redis(_apply, None)


# ----------------------------- утилиты HTTP ---------------------------------
def http_json(method: str, url: str, data: dict | None = None, timeout: float = 8.0):
    body = None
    headers = {"Accept": "application/json"}
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ctype = resp.headers.get("Content-Type", "")
            raw = resp.read()
            return resp.status, ctype, raw
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("Content-Type", ""), e.read()
    except Exception as e:
        return 599, "text/plain", str(e).encode("utf-8")


# ----------------------- данные и файлы арендаторов -------------------------
DEFAULT_TENANT_JSON = {
    "passport": {
        "tenant_id": 0,
        "brand": "Мой Бренд",
        "agent_name": "Менеджер",
        "city": "Город",
        "currency": "₽",
        "channel": "WhatsApp",
        "whatsapp_link": "https://wa.me/7XXXXXXXXXX",
        "phone": "",
        "contact": "",
        "preferred_messenger": "",
    },
    "behavior": {
        "always_full_catalog": True,
        "send_catalog_as_pages": True,
        "max_clarifying_questions": 1,
        "tone": "коротко-дружелюбно",
        "anti_repeat_window": 6,
        "dedupe_catalog_titles": True,
        "allow_filter_commands": True,
        "pdf_one_item_per_page": False,
        "explain": False,
        "use_universal_pdf_pipeline": False,
    },
    "catalogs": [],
    "funnel": {
        "avito_to_wa": {
            "enabled": True,
            "trigger_phrases": ["напишите в whatsapp", "скину в ватсап"],
        }
    },
    "learning": {
        "enabled": True,
        "retriever": "tfidf",
        "top_k": 2,
        "max_tokens": 320,
        "min_chars": 15,
        "track_outcomes": True,
        "auto_vitrine": True,
        "memory_window_dialogs": 50,
        "pinned_items": [],
        "negatives": ["дорого", "долго"],
    },
    "limits": {
        "catalog_page_size": 8,
        "max_pages_per_reply": 5,
        "rate_limit_per_contact_min": 1,
        "send_throttle_ms": 250,
    },
    "integrations": {
        "pdf_catalog_url": "",
        "crm_webhook": "",
        "analytics_pixel": "",
        "ga_id": "",
        "uploaded_catalog": "",
    },
    "channels": {
        "whatsapp": {"enabled": True},
        "telegram": {"enabled": True},
        "max": {"enabled": True},
    },
}

_SAMPLE_CATALOG_PATH = DATA_DIR / "catalog_sample.csv"
if _SAMPLE_CATALOG_PATH.exists():
    DEFAULT_TENANT_JSON["catalogs"].append(
        {
            "name": "catalog",
            "path": str(_SAMPLE_CATALOG_PATH),
            "type": "csv",
            "delimiter": ",",
            "encoding": "utf-8",
            "fields": {
                "id": "id",
                "title": "name",
                "price": "price",
                "brand": "brand",
                "material": "material",
                "color": "color",
                "stock": "stock",
                "image": "image",
                "url": "url",
                "tags": "tags",
            },
            "ranking": {
                "boost_tags": ["хит", "новинка", "склад", "топ"],
                "boost_stock": 1.0,
                "boost_margin": 0.2,
                "min_stock": 0,
                "min_score": 0,
                "sort": [
                    {"by": "score", "order": "desc"},
                    {"by": "price", "order": "asc"},
                ],
                "filters_default": {"stock": [">", 0]},
            },
            "presentation": {
                "price_format": "{price} {CUR}",
                "line_format": "{title} — {price} {CUR}. Цвет: {color}. Материал: {material}. [{url}]",
                "group_by": "brand",
            },
        }
    )

PERSONA_DEFAULT_PATH = ROOT_DIR / "agents" / "persona_default_ru.md"
try:
    DEFAULT_PERSONA_MD = PERSONA_DEFAULT_PATH.read_text(encoding="utf-8")
except Exception:
    DEFAULT_PERSONA_MD = """Продукт/услуга: опишите ключевые товары и услуги.
Аудитория: частные клиенты и B2B (по ситуации).
Фокус ценности: сроки, удобство, качество, гарантия, понятные условия.
Типовые возражения: дорого, долго, сомнения в качестве.

Стиль и качество:
- Общение на «Вы», коротко и по делу.
- НЕ задавай очевидные вопросы и НЕ повторяй то, что уже сказал клиент.
- За одно сообщение: 1 вопрос максимум.
- Сначала подтверждение запроса, потом уточнение, затем предложение.

Уточняющие вопросы (используй по ситуации):
- «Что именно нужно: вариант/размер/задача?»
- «В какие сроки планируете?»
- «Есть ориентир по бюджету?»
- «В каком городе/районе нужна доставка или установка?»

Предложение:
- 2–3 варианта: название → 2 выгоды → цена/ориентир → следующий шаг.
- Если нет точных параметров — предложи 2 сценария (бюджетный/оптимальный).

Возражения:
- Цена: объяснить ценность, дать альтернативу дешевле.
- Сроки: назвать реальные сроки и варианты ускорения.

Примеры стартовых сообщений:
- «Здравствуйте! Чем могу помочь?»
- ""

Скрипт диалога (шаги):
- Понять запрос → уточнить параметр → предложить варианты → следующий шаг.

Частые вопросы:
- Вопрос: Сколько стоит? → Ответ: Цена зависит от параметров. Уточните, что именно нужно — подберу варианты.
- Вопрос: Есть в наличии? → Ответ: Уточните, какой вариант нужен, я проверю наличие и отвечу.

Отложенные сообщения:
- Один вежливый напоминатель, если клиент не ответил.
"""


def _ensure_passport_public_key(cfg: dict[str, Any] | None) -> bool:
    if not isinstance(cfg, dict):
        return False

    public_key = str(getattr(settings, "PUBLIC_KEY", "") or "").strip()
    if not public_key:
        return False

    passport = cfg.get("passport")
    mutated = False
    if not isinstance(passport, dict):
        passport = {}
        cfg["passport"] = passport
        mutated = True

    current_raw = passport.get("public_key")
    current_value = str(current_raw).strip() if current_raw else ""
    if current_value:
        if isinstance(current_raw, str) and current_raw == current_value:
            return mutated
        passport["public_key"] = current_value
        return True

    passport["public_key"] = public_key
    return True


def tenant_dir(tenant: int) -> pathlib.Path:
    return TENANTS_DIR / str(int(tenant))


def ensure_tenant_files(tenant: int) -> pathlib.Path:
    td = tenant_dir(tenant)
    td.mkdir(parents=True, exist_ok=True)
    tj = td / "tenant.json"
    pm = td / "persona.md"

    if not tj.exists() or tj.stat().st_size == 0:
        cfg = json.loads(json.dumps(DEFAULT_TENANT_JSON, ensure_ascii=False))
        cfg.setdefault("passport", {})["tenant_id"] = int(tenant)
        _ensure_passport_public_key(cfg)
        with open(tj, "w", encoding="utf-8") as fh:
            json.dump(cfg, fh, ensure_ascii=False, indent=2)
    else:
        try:
            with open(tj, "r", encoding="utf-8") as fh:
                existing_cfg = json.load(fh)
        except Exception:
            existing_cfg = {}

        if not isinstance(existing_cfg, dict):
            existing_cfg = {}
        channels = existing_cfg.get("channels") if isinstance(existing_cfg, dict) else None
        mutated = False
        if not isinstance(channels, dict):
            channels = {"whatsapp": {"enabled": True}}
            existing_cfg["channels"] = channels
            mutated = True
        else:
            whatsapp_cfg = channels.get("whatsapp")
            if not isinstance(whatsapp_cfg, dict):
                channels["whatsapp"] = {"enabled": True}
                mutated = True
            elif "enabled" not in whatsapp_cfg:
                whatsapp_cfg["enabled"] = True
                mutated = True

        if _ensure_passport_public_key(existing_cfg):
            mutated = True

        if mutated:
            try:
                with open(tj, "w", encoding="utf-8") as fh:
                    json.dump(existing_cfg, fh, ensure_ascii=False, indent=2)
            except Exception:
                pass

    if not pm.exists() or pm.stat().st_size == 0:
        with open(pm, "w", encoding="utf-8") as fh:
            fh.write(DEFAULT_PERSONA_MD)

    return td


def _merge_dicts(base: Mapping[str, Any] | dict, overlay: Mapping[str, Any] | dict) -> dict:
    result = dict(base or {})
    for key, value in dict(overlay or {}).items():
        base_value = result.get(key)
        if isinstance(base_value, dict) and isinstance(value, dict):
            result[key] = _merge_dicts(base_value, value)
        else:
            result[key] = value
    return result


def _load_external_tenant_config(tenant: int) -> tuple[float, dict]:
    directory = TENANT_CONFIG_DIR
    if not directory.exists():
        return 0.0, {}
    tenant_str = str(int(tenant))
    candidates = (
        directory / f"{tenant_str}.yaml",
        directory / f"{tenant_str}.yml",
        directory / f"{tenant_str}.json",
    )
    for path in candidates:
        if not path.exists():
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = 0.0
        try:
            if path.suffix.lower() == ".json":
                with open(path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
            else:
                with open(path, "r", encoding="utf-8") as fh:
                    data = yaml.safe_load(fh)  # type: ignore[arg-type]
        except Exception:
            logger.warning("failed to load tenant override path=%s", path, exc_info=True)
            return mtime, {}
        if isinstance(data, dict):
            return mtime, data
        logger.warning("tenant override not a mapping path=%s", path)
        return mtime, {}
    return 0.0, {}


def _normalize_tenant_config(cfg: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(cfg or {})
    behavior_raw = normalized.get("behavior")
    behavior: dict[str, Any] = {}
    if isinstance(behavior_raw, dict):
        behavior.update(behavior_raw)

    auto_flag = behavior.get("auto_reply")
    if auto_flag is None:
        auto_flag = behavior.get("auto_reply_enabled")
    behavior["auto_reply"] = bool(auto_flag)
    behavior["auto_reply_enabled"] = behavior["auto_reply"]

    explain_flag = _coerce_bool(behavior.get("explain"), False)
    behavior["explain"] = explain_flag

    text_raw = behavior.get("auto_reply_text")
    if isinstance(text_raw, str):
        text_value = text_raw
    elif text_raw is None:
        text_value = ""
    else:
        text_value = str(text_raw)
    behavior["auto_reply_text"] = text_value

    triggers_raw = behavior.get("triggers")
    triggers: list[dict[str, Any]] = []
    if isinstance(triggers_raw, list):
        for item in triggers_raw:
            if not isinstance(item, Mapping):
                continue
            phrases_raw = item.get("phrases") or item.get("keywords") or []
            phrases: list[str] = []
            if isinstance(phrases_raw, (list, tuple, set)):
                for ph in phrases_raw:
                    if isinstance(ph, str) and ph.strip():
                        phrases.append(ph.strip())
            elif isinstance(phrases_raw, str) and phrases_raw.strip():
                for ph in phrases_raw.split(","):
                    if ph.strip():
                        phrases.append(ph.strip())
            if not phrases:
                continue
            channels_raw = item.get("channels") or ["telegram", "avito", "whatsapp", "max"]
            channels: list[str] = []
            if isinstance(channels_raw, (list, tuple, set)):
                for ch in channels_raw:
                    if isinstance(ch, str) and ch.strip():
                        channels.append(ch.strip().lower())
            elif isinstance(channels_raw, str) and channels_raw.strip():
                channels.append(channels_raw.strip().lower())
            if not channels:
                channels = ["telegram", "avito", "whatsapp", "max"]
            silence_flag = _coerce_bool(item.get("silence"), True)
            notify_flag = _coerce_bool(item.get("notify"), False)
            triggers.append(
                {
                    "phrases": phrases,
                    "channels": channels,
                    "silence": silence_flag,
                    "notify": notify_flag,
                }
            )
    behavior["triggers"] = triggers

    # Настройка ожидания фото/файла после заданного вопроса.
    photo_markers_raw = (
        behavior.get("photo_expected_markers") or behavior.get("photo_markers") or []
    )
    photo_markers: list[str] = []
    if isinstance(photo_markers_raw, (list, tuple, set)):
        for ph in photo_markers_raw:
            if isinstance(ph, str) and ph.strip():
                photo_markers.append(ph.strip())
    elif isinstance(photo_markers_raw, str) and photo_markers_raw.strip():
        for ph in photo_markers_raw.split(","):
            if ph.strip():
                photo_markers.append(ph.strip())
    behavior["photo_expected_markers"] = photo_markers
    photo_reply_raw = behavior.get("photo_expected_reply") or behavior.get("photo_reply") or ""
    behavior["photo_expected_reply"] = (
        photo_reply_raw if isinstance(photo_reply_raw, str) else str(photo_reply_raw or "")
    )
    try:
        ttl_value = int(behavior.get("photo_expected_ttl") or 0)
    except Exception:
        ttl_value = 0
    behavior["photo_expected_ttl"] = ttl_value if ttl_value > 0 else 0

    # Каталог первым сообщением в Telegram (по умолчанию включено для сохранения текущего поведения).
    send_catalog_flag = behavior.get("send_catalog_on_first_message")
    if send_catalog_flag is None:
        behavior["send_catalog_on_first_message"] = True
    else:
        behavior["send_catalog_on_first_message"] = _coerce_bool(send_catalog_flag, True)

    # Смарт-реплай для Avito (по умолчанию отключён как и раньше).
    avito_ai_flag = behavior.get("avito_smart_reply_enabled")
    behavior["avito_smart_reply_enabled"] = _coerce_bool(avito_ai_flag, False)

    whatsapp_cfg = normalized.get("whatsapp")
    whatsapp: dict[str, Any] = {}
    if isinstance(whatsapp_cfg, dict):
        whatsapp.update(whatsapp_cfg)
    provider_value = str(whatsapp.get("provider") or "").strip().lower()
    default_provider = getattr(settings, "WHATSAPP_PROVIDER_DEFAULT", "waweb")
    if provider_value not in {"waweb", "baileys"}:
        provider_value = default_provider
    whatsapp["provider"] = provider_value
    normalized["whatsapp"] = whatsapp

    notifications_raw = normalized.get("notifications")
    if isinstance(notifications_raw, dict):
        notifications = dict(notifications_raw)
    else:
        notifications = {}
    normalized["notifications"] = notifications

    normalized["behavior"] = behavior
    return normalized


def read_tenant_config(tenant: int) -> dict:
    ensure_tenant_files(tenant)
    path = tenant_dir(tenant) / "tenant.json"
    try:
        primary_mtime = path.stat().st_mtime
    except Exception:
        primary_mtime = 0.0

    overlay_mtime, overlay_cfg = _load_external_tenant_config(tenant)
    cached = _TENANT_CONFIG_CACHE.get(int(tenant))
    if cached and cached[0] == primary_mtime and cached[1] == overlay_mtime:
        return cached[2]

    if path.exists():
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    else:
        data = {}

    merged = _merge_dicts(data, overlay_cfg)
    normalized = _normalize_tenant_config(merged)
    _TENANT_CONFIG_CACHE[int(tenant)] = (primary_mtime, overlay_mtime, normalized)
    return normalized


def write_tenant_config(tenant: int, cfg: dict) -> None:
    ensure_tenant_files(tenant)
    path = tenant_dir(tenant) / "tenant.json"
    _ensure_passport_public_key(cfg)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)
    try:
        mtime = path.stat().st_mtime
    except Exception:
        mtime = 0.0
    overlay_mtime, overlay_cfg = _load_external_tenant_config(tenant)
    merged = _merge_dicts(cfg, overlay_cfg)
    normalized = _normalize_tenant_config(merged)
    try:
        _TENANT_CONFIG_CACHE[int(tenant)] = (mtime, overlay_mtime, normalized)
    except Exception:
        _TENANT_CONFIG_CACHE.pop(int(tenant), None)


def _persist_pdf_index_metadata(
    tenant: int,
    source_key: str,
    rel_index_path: str,
    index_meta: Dict[str, Any],
) -> None:
    try:
        cfg = read_tenant_config(tenant)
    except Exception:
        return

    catalogs = cfg.get("catalogs")
    catalogs = catalogs if isinstance(catalogs, list) else []
    source_candidates = {source_key.strip()}
    source_candidates.add(index_meta.get("source_path", ""))
    resolved: set[str] = set()
    for token in list(source_candidates):
        if not token:
            continue
        resolved.add(token)
        try:
            abs_path = str((tenant_dir(tenant) / token).resolve())
            resolved.add(abs_path)
        except Exception:
            pass
    resolved = {value for value in resolved if value}

    for entry in catalogs:
        entry_path = str(entry.get("path") or "").strip()
        if not entry_path:
            continue
        candidate_set = {entry_path}
        try:
            candidate_set.add(str((tenant_dir(tenant) / entry_path).resolve()))
        except Exception:
            pass
        if candidate_set & resolved:
            entry["index_path"] = rel_index_path
            entry["indexed_at"] = index_meta.get("generated_at")
            entry["chunk_count"] = index_meta.get("chunk_count")
            entry["sha1"] = index_meta.get("sha1")
            break

    integrations = cfg.setdefault("integrations", {})
    uploaded = integrations.get("uploaded_catalog")
    if isinstance(uploaded, dict) and (uploaded.get("path") in resolved):
        uploaded["index"] = {
            "path": rel_index_path,
            "generated_at": index_meta.get("generated_at"),
            "chunks": index_meta.get("chunk_count"),
            "pages": index_meta.get("page_count"),
            "sha1": index_meta.get("sha1"),
        }

    try:
        write_tenant_config(tenant, cfg)
    except Exception:
        pass


def _persona_cache_key(tenant: int, channel: str | None) -> tuple[int, str]:
    return int(tenant), (channel or "").strip().lower()


def _persona_path(tenant: int, channel: str | None) -> pathlib.Path:
    base = tenant_dir(tenant)
    channel_name = (channel or "").strip().lower()
    if channel_name:
        return base / f"persona_{channel_name}.md"
    return base / "persona.md"


def read_persona(tenant: int, channel: str | None = None) -> str:
    ensure_tenant_files(tenant)
    path = _persona_path(tenant, channel)
    if channel and not path.exists():
        path = _persona_path(tenant, None)
    try:
        mtime = path.stat().st_mtime
        cached = _TENANT_PERSONA_CACHE.get(_persona_cache_key(int(tenant), channel))
        if cached and cached[0] == mtime:
            return cached[1]
    except Exception:
        mtime = 0.0
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    try:
        _TENANT_PERSONA_CACHE[_persona_cache_key(int(tenant), channel)] = (mtime, text)
    except Exception:
        pass
    try:
        _clear_persona_hints_cache(int(tenant))
    except Exception:
        _PERSONA_HINTS_CACHE.clear()
    return text


def write_persona(tenant: int, text: str, channel: str | None = None) -> None:
    ensure_tenant_files(tenant)
    path = _persona_path(tenant, channel)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text or "")
    try:
        mtime = path.stat().st_mtime
        _TENANT_PERSONA_CACHE[_persona_cache_key(int(tenant), channel)] = (mtime, text or "")
    except Exception:
        _TENANT_PERSONA_CACHE.pop(_persona_cache_key(int(tenant), channel), None)
    _clear_persona_hints_cache(int(tenant))


def load_tenant(tenant: int) -> dict:
    try:
        return read_tenant_config(tenant)
    except Exception:
        cfg = json.loads(json.dumps(DEFAULT_TENANT_JSON, ensure_ascii=False))
        cfg.setdefault("passport", {})["tenant_id"] = int(tenant)
        return cfg


def _branding_for_tenant(tenant: int | None = None, channel: str | None = None) -> Dict[str, str]:
    passport: Dict[str, Any] = {}
    integrations: Dict[str, Any] = {}
    if tenant is not None:
        try:
            cfg = read_tenant_config(tenant)
        except Exception:
            cfg = {}
    else:
        cfg = {}

    if isinstance(cfg, dict):
        raw_passport = cfg.get("passport")
        if isinstance(raw_passport, dict):
            passport = raw_passport
        raw_integrations = cfg.get("integrations")
        if isinstance(raw_integrations, dict):
            integrations = raw_integrations

    agent_name = str(passport.get("agent_name") or "").strip()
    brand = str(passport.get("brand") or "").strip()
    city = str(passport.get("city") or "").strip()
    whatsapp_link = str(
        passport.get("whatsapp_link") or integrations.get("whatsapp_link") or ""
    ).strip()
    catalog_url = str(
        integrations.get("catalog_url")
        or integrations.get("pdf_catalog_url")
        or passport.get("catalog_url")
        or ""
    ).strip()

    if tenant is None:
        agent_name = agent_name or getattr(settings, "AGENT_NAME", "")
        brand = brand or getattr(settings, "BRAND_NAME", "")
        city = city or getattr(settings, "CITY", "")
        whatsapp_link = whatsapp_link or getattr(settings, "WHATSAPP_LINK", "")

    currency = str(passport.get("currency") or "₽").strip() or "₽"
    resolved_channel = str(channel or passport.get("channel") or "").strip()
    if not resolved_channel:
        resolved_channel = "WhatsApp"

    return {
        "AGENT_NAME": agent_name,
        "BRAND": brand,
        "BRAND_NAME": brand,
        "WHATSAPP_LINK": whatsapp_link,
        "CATALOG_URL": catalog_url,
        "CITY": city,
        "CHANNEL": resolved_channel,
        "CURRENCY": currency,
    }


# ------------------------------ промпты --------------------------------------
PERSONA_MD = """# Персона «Универсальный продавец»

- Роль: онлайн-менеджер (не раскрывайся как ИИ).
- Представление: «Здравствуйте! Меня зовут {AGENT_NAME}, {BRAND}.»
- Тон: кратко, естественно, без лишних вопросов.
- Цель: дать пользу и один понятный CTA.
- Правила:
  - Всегда прикладывай ВЕСЬ КАТАЛОГ. Если не помещается — отправляй постранично подряд, без ожидания «продолжать?».
  - ≤1 уточнение на ход, только если повышает точность следующего шага.
  - Используй техники продаж: AIDA, PAS, лёгкий SPIN (≤1 вопрос), LAER, якорение (лучше/оптимально/бюджетно), соцдоказательство, мягкий up/cross-sell (≤1/ход), микрокоммит.
- Канал: {CHANNEL}. Если Avito — в конце можно мягко предложить перейти в WhatsApp. Если WhatsApp — работай по делу.
- Локаль: {CITY}, валюта: {CURRENCY}.
"""

RULES_YAML = """
take_control: true
cta:
  - "Сформирую подбор. Назначим замер на завтра: утро/день/вечер?"
  - "Готов оформить заказ сегодня. Подходит?"
  - "Забронирую цену на сутки. Идём дальше?"
"""


# ---------------------------- персонализация ---------------------------------
def load_persona(tenant: int | None = None, channel: str | None = None) -> str:
    """Возвращает persona.md с подстановкой брендинга."""
    if tenant is not None:
        try:
            persona = read_persona(tenant, channel)
            if not persona.strip():
                persona = DEFAULT_PERSONA_MD
        except Exception:
            persona = DEFAULT_PERSONA_MD
    else:
        try:
            with open(settings.PERSONA_MD, "r", encoding="utf-8") as fh:
                persona = fh.read()
        except Exception:
            persona = PERSONA_MD

    tokens = _branding_for_tenant(tenant, channel)
    for key, value in tokens.items():
        persona = persona.replace(f"{{{key}}}", value or "")
    return persona


def load_persona_structured(tenant: int | None = None) -> Dict[str, Any]:
    """Парсит persona.md как YAML и возвращает структуру."""
    text = load_persona(tenant)
    if not text.strip():
        return {}
    try:
        parsed = yaml.safe_load(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def persona_meta_config(tenant: int | None = None) -> Dict[str, Any]:
    structured = load_persona_structured(tenant)
    meta = structured.get("meta") if isinstance(structured, dict) else {}
    if isinstance(meta, dict) and meta:
        return meta

    # Fallback: extract meta block from persona.md when the whole file is not valid YAML.
    try:
        raw = load_persona(tenant)
        lines = raw.splitlines()
        meta_lines: list[str] = []
        in_meta = False
        for line in lines:
            if line.strip().startswith("meta:"):
                in_meta = True
            if in_meta:
                if line and not line.startswith(" "):
                    # Reached next top-level heading/block
                    if not line.strip().startswith("meta:"):
                        break
                meta_lines.append(line)
        if meta_lines:
            parsed = yaml.safe_load("\n".join(meta_lines))
            if isinstance(parsed, dict):
                block = parsed.get("meta") if "meta" in parsed else parsed
                if isinstance(block, dict):
                    return block
    except Exception:
        pass
    return {}


def _resolve_persona_relative_path(tenant: int, raw_path: str) -> Optional[pathlib.Path]:
    candidate = (raw_path or "").strip()
    if not candidate:
        return None
    candidate = candidate.replace("\\", "/")
    candidate = candidate.lstrip("/")
    if ".." in candidate.split("/"):
        return None
    tenant_root = tenant_dir(tenant)
    target = tenant_root / candidate
    if target.exists() and target.is_file():
        return target
    return None


def persona_catalog_pdf(tenant: int) -> Optional[Dict[str, Any]]:
    meta = persona_meta_config(tenant)
    raw_path = meta.get("catalog_pdf_path") if isinstance(meta, dict) else None
    if not isinstance(raw_path, str):
        return None
    target = _resolve_persona_relative_path(tenant, raw_path)
    if not target:
        return None
    return {
        "type": "pdf",
        "path": str(target.relative_to(tenant_dir(tenant))),
        "original": target.name,
        "mime": "application/pdf",
    }


def persona_catalog_csv(tenant: int) -> Optional[pathlib.Path]:
    meta = persona_meta_config(tenant)
    raw_path = meta.get("catalog_csv_path") if isinstance(meta, dict) else None
    if not isinstance(raw_path, str):
        return None
    return _resolve_persona_relative_path(tenant, raw_path)


def _normalize_catalog_pdf_candidate(
    tenant: int,
    candidate: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    path_value = candidate.get("path") or candidate.get("relative_path")
    if not isinstance(path_value, str):
        return None
    cleaned = path_value.replace("\\", "/").strip()
    if not cleaned:
        return None
    try:
        safe = pathlib.PurePosixPath(cleaned)
    except Exception:
        return None
    if safe.is_absolute() or ".." in safe.parts:
        return None
    tenant_root = tenant_dir(tenant)
    target = tenant_root / str(safe)
    if not target.exists() or not target.is_file():
        return None
    try:
        stat = target.stat()
    except OSError:
        return None

    type_hint = str(candidate.get("type") or candidate.get("kind") or "").strip().lower()
    mime_hint = (
        str(candidate.get("mime") or candidate.get("mime_type") or candidate.get("mimetype") or "")
        .strip()
        .lower()
    )
    extension = safe.suffix.lower()
    if type_hint and type_hint not in {"pdf", "document"}:
        return None
    if not type_hint and extension not in {".pdf", ".pdfx"} and "pdf" not in mime_hint:
        return None

    filename = str(candidate.get("original") or candidate.get("filename") or safe.name)
    mime = (
        str(
            candidate.get("mime")
            or candidate.get("mime_type")
            or candidate.get("mimetype")
            or "application/pdf"
        ).strip()
        or "application/pdf"
    )
    return {
        "relative_path": str(safe),
        "absolute_path": str(target),
        "filename": filename,
        "mime": mime,
        "size": stat.st_size,
        "updated_at": int(stat.st_mtime),
    }


def resolve_catalog_pdf_meta(tenant: int, cfg: Optional[dict] = None) -> Optional[Dict[str, Any]]:
    if cfg is None:
        try:
            cfg = load_tenant(tenant)
        except Exception:
            cfg = {}

    candidates: list[Mapping[str, Any]] = []
    if isinstance(cfg, dict):
        integrations = cfg.get("integrations")
        if isinstance(integrations, Mapping):
            uploaded = integrations.get("uploaded_catalog")
            if isinstance(uploaded, Mapping):
                candidates.append(uploaded)
            for alt_key in ("uploaded_catalog_pdf", "catalog_pdf", "pdf_catalog"):
                alt_meta = integrations.get(alt_key)
                if isinstance(alt_meta, Mapping):
                    candidates.append(alt_meta)
        raw_catalogs = cfg.get("catalogs")
        if isinstance(raw_catalogs, list):
            for entry in raw_catalogs:
                if not isinstance(entry, Mapping):
                    continue
                entry_type = str(entry.get("type") or "").strip().lower()
                if entry_type == "pdf":
                    candidates.append(entry)

    for candidate in candidates:
        normalized = _normalize_catalog_pdf_candidate(tenant, candidate)
        if normalized:
            return normalized

    persona_meta = persona_catalog_pdf(tenant)
    if persona_meta:
        normalized = _normalize_catalog_pdf_candidate(tenant, persona_meta)
        if normalized:
            return normalized

    default_path = tenant_dir(tenant) / "uploads" / "catalog.pdf"
    if default_path.exists() and default_path.is_file():
        try:
            stat = default_path.stat()
        except OSError:
            stat = None
        if stat:
            return {
                "relative_path": "uploads/catalog.pdf",
                "absolute_path": str(default_path),
                "filename": default_path.name,
                "mime": "application/pdf",
                "size": stat.st_size,
                "updated_at": int(stat.st_mtime),
            }
    return None


# ---------------------- простая rule-based логика ----------------------------
CATALOG_CSV = DATA_DIR / "catalog_sample.csv"


def _canonicalize_field_name(name: str) -> str:
    return _FIELD_CLEAN_RE.sub("", (name or "").lower())


_FIELD_SYNONYMS: Dict[str, List[str]] = {
    "title": [
        "title",
        "name",
        "product",
        "productname",
        "item",
        "itemname",
        "goods",
        "model",
        "модель",
        "товар",
        "наименование",
        "название",
        "позиция",
        "описание",
        "характеристика",
    ],
    "price": [
        "price",
        "cost",
        "стоимость",
        "цена",
        "ценаактуальная",
        "ценапродажи",
        "ценаруб",
        "ценазасистему",
        "ценазасчет",
        "ценаскидкой",
        "ценабезскидки",
        "ценазам2",
        "ценазамкв",
        "ценазаметры",
        "ценарозничная",
        "ценазапозицию",
    ],
    "sku": [
        "sku",
        "код",
        "кодтовара",
        "артикул",
        "арт",
        "код1с",
        "идентификатор",
        "id",
        "article",
    ],
    "url": [
        "url",
        "link",
        "urlтовара",
        "ссылка",
        "hyperlink",
        "страница",
    ],
    "brand": [
        "brand",
        "бренд",
        "марка",
        "производитель",
        "manufacturer",
    ],
    "stock": [
        "stock",
        "наличие",
        "остаток",
        "остатки",
        "количество",
        "qty",
        "quantity",
        "available",
    ],
    "image": [
        "image",
        "photo",
        "img",
        "picture",
        "изображение",
        "картинка",
        "фото",
        "фотография",
    ],
    "description": [
        "description",
        "описание",
        "details",
        "характеристики",
        "features",
        "comment",
    ],
    "color": [
        "color",
        "colour",
        "цвет",
        "цветпанели",
        "цветвнутреннейпанели",
        "цветвнутренней",
        "цветснутри",
        "colorinside",
    ],
    "finish": [
        "finish",
        "coating",
        "цветнаружнойпанели",
        "цветнаружи",
        "coloroutside",
        "цветпокраски",
        "покраска",
        "наружнаяпокраска",
        "цветвнешнейпанели",
    ],
    "shade": [
        "shade",
        "оттенок",
        "расцветка",
        "цветподбор",
    ],
    "object_type": [
        "objecttype",
        "object",
        "target",
        "usage",
        "назначение",
        "типобъекта",
        "типпомещения",
        "типпомещ",
        "помещение",
        "длякого",
        "длячего",
        "application",
    ],
}


_FIELD_TOKEN_MAP: Dict[str, List[str]] = {
    key: sorted(
        {_canonicalize_field_name(token) for token in tokens if token}, key=len, reverse=True
    )
    for key, tokens in _FIELD_SYNONYMS.items()
}


def _merge_csv_mapping_meta(
    meta: Mapping[str, Any] | None,
    persona_meta: Mapping[str, Any] | None,
) -> dict[str, Any]:
    result: dict[str, Any] = dict(meta or {})
    persona_csv = persona_meta.get("csv_mapping") if isinstance(persona_meta, Mapping) else None
    if not isinstance(persona_csv, Mapping):
        return result

    merged = dict(result.get("csv_mapping") or {})
    persona_columns = persona_csv.get("columns")
    existing_columns = dict(merged.get("columns") or {})

    if isinstance(persona_columns, Mapping):
        for canonical, aliases in persona_columns.items():
            key = str(canonical).strip()
            if not key:
                continue
            alias_list: list[str] = []
            if isinstance(aliases, str):
                alias_list = [aliases]
            elif isinstance(aliases, Mapping):
                alias_list = [str(val) for val in aliases.values() if val]
            elif isinstance(aliases, Sequence):
                alias_list = [str(val) for val in aliases if val]
            else:
                alias_list = [str(aliases)]
            bucket = list(existing_columns.get(key, []))
            for alias in alias_list:
                cleaned = str(alias).strip()
                if cleaned and cleaned not in bucket:
                    bucket.append(cleaned)
            if bucket:
                existing_columns[key] = bucket

    if existing_columns:
        merged["columns"] = existing_columns

    for extra_key, extra_value in persona_csv.items():
        if extra_key == "columns":
            continue
        merged.setdefault(extra_key, extra_value)

    if merged:
        result["csv_mapping"] = merged
    return result


def _prepare_field_mapping(meta: Dict[str, Any], items: List[Dict[str, Any]]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not items:
        return mapping

    sample_cols = list(items[0].keys())
    sample_canon = {col: _canonicalize_field_name(col) for col in sample_cols}

    csv_mapping_meta = meta.get("csv_mapping") if isinstance(meta, Mapping) else {}
    csv_mapping_columns = (
        csv_mapping_meta.get("columns") if isinstance(csv_mapping_meta, Mapping) else {}
    )

    meta_fields = meta.get("fields") if isinstance(meta, dict) else None
    if isinstance(meta_fields, dict):
        for canonical, source in meta_fields.items():
            if not isinstance(canonical, str) or not isinstance(source, str):
                continue
            cleaned_source = source.strip()
            if cleaned_source in sample_cols:
                key_norm = canonical.strip()
                key_lower = key_norm.lower()
                mapping[key_lower] = cleaned_source
                if key_norm != key_lower:
                    mapping.setdefault(key_norm, cleaned_source)

    used_sources = set(mapping.values())

    def _find_column(
        tokens: List[str],
        preferred: List[str] | None = None,
        raw_names: List[str] | None = None,
    ) -> str | None:
        preferred = preferred or []
        raw_lower = [name.lower() for name in raw_names] if raw_names else []
        for col in sample_cols:
            if col in used_sources:
                continue
            canon = sample_canon.get(col) or ""
            if raw_lower and col.lower() in raw_lower:
                used_sources.add(col)
                return col
            for p in preferred:
                if p and (canon == p or canon.startswith(p) or p in canon):
                    used_sources.add(col)
                    return col
            for token in tokens:
                if not token:
                    continue
                if canon == token or canon.startswith(token) or token in canon:
                    used_sources.add(col)
                    return col
        return None

    custom_aliases: Dict[str, List[str]] = {}
    if isinstance(csv_mapping_columns, Mapping):
        for canonical, aliases in csv_mapping_columns.items():
            key = str(canonical).strip()
            if not key:
                continue
            if isinstance(aliases, str):
                custom_aliases[key] = [aliases]
            elif isinstance(aliases, Mapping):
                custom_aliases[key] = [str(val) for val in aliases.values() if val]
            elif isinstance(aliases, Sequence):
                custom_aliases[key] = [str(val) for val in aliases if val]
            else:
                custom_aliases[key] = [str(aliases)]

    for field_name, tokens in _FIELD_TOKEN_MAP.items():
        if field_name in mapping:
            continue
        preferred_aliases = custom_aliases.get(field_name, [])
        normalized_tokens = [
            _canonicalize_field_name(alias) for alias in preferred_aliases if alias
        ] + list(tokens)
        column = _find_column(
            normalized_tokens,
            preferred=normalized_tokens,
            raw_names=preferred_aliases,
        )
        if column:
            mapping[field_name] = column

    for canonical, aliases in custom_aliases.items():
        key_norm = canonical.strip()
        key_lower = key_norm.lower()
        if key_lower in mapping or key_norm in mapping:
            continue
        normalized_tokens = [_canonicalize_field_name(alias) for alias in aliases if alias]
        column = _find_column(
            normalized_tokens,
            preferred=normalized_tokens,
            raw_names=[alias.strip() for alias in aliases if isinstance(alias, str)],
        )
        if column:
            mapping[key_norm] = column
            mapping.setdefault(key_lower, column)

    # Extra heuristics if price or title still missing
    if "price" not in mapping:
        numeric_candidates: List[str] = []
        for col in sample_cols:
            if col in used_sources:
                continue
            canon = sample_canon.get(col) or ""
            if any(
                token in canon
                for token in ("цен", "price", "cost", "стоим", "руб", "uah", "usd", "eur")
            ):
                mapping["price"] = col
                used_sources.add(col)
                break
            # Look at data if no obvious hints
            values = [str((row.get(col) or "")).strip() for row in items[:5]]
            digits = [re.sub(r"\D", "", val) for val in values if val]
            if any(len(d) >= 4 for d in digits):
                numeric_candidates.append(col)
        if "price" not in mapping and numeric_candidates:
            mapping["price"] = numeric_candidates[0]
            used_sources.add(numeric_candidates[0])

    if "title" not in mapping:
        for col in sample_cols:
            if col in used_sources:
                continue
            canon = sample_canon.get(col) or ""
            if any(
                token in canon
                for token in ("name", "товар", "пози", "model", "тип", "item", "наимен")
            ):
                mapping["title"] = col
                used_sources.add(col)
                break

    return mapping


def _has_price_digits(value: Any) -> bool:
    text = str(value or "")
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 4:
        return True
    lowered = text.lower()
    if len(digits) >= 3 and any(
        tok in lowered for tok in ("руб", "uah", "eur", "usd", "$", "€", "₽")
    ):
        return True
    try:
        # Attempt to parse decimal values like "99.5"
        normalized = text.replace(" ", "").replace(",", ".")
        float(normalized)
        return True
    except Exception:
        return False


def _normalize_csv_delimiter(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text == "\\t":
        return "\t"
    return text[0]


def _csv_delimiter_candidates(sample: str, configured: Any) -> List[str]:
    candidates: List[str] = []

    def _append(delim: str | None) -> None:
        if not delim:
            return
        if delim not in candidates:
            candidates.append(delim)

    _append(_normalize_csv_delimiter(configured))
    try:
        sniffed = csv.Sniffer().sniff(sample or "", delimiters=",;\t|")
        _append(_normalize_csv_delimiter(getattr(sniffed, "delimiter", None)))
    except Exception:
        pass
    for delim in (",", ";", "\t", "|"):
        _append(delim)
    return candidates or [","]


def _read_csv_rows_with_delimiter(
    path: pathlib.Path,
    *,
    encoding: str,
    delimiter: str,
    row_limit: int = 500,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    with open(path, "r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        header: List[str] = []
        for raw_header in reader:
            if not raw_header or not any((cell or "").strip() for cell in raw_header):
                continue
            header = raw_header
            break
        if not header:
            return [], []

        normalized: List[str] = []
        seen_headers: Dict[str, int] = {}
        for idx_h, cell in enumerate(header):
            name = (cell or "").strip().lstrip("\ufeff")
            if not name:
                name = f"column_{idx_h + 1}"
            if name in seen_headers:
                seen_headers[name] += 1
                name = f"{name}_{seen_headers[name]}"
            else:
                seen_headers[name] = 0
            normalized.append(name)

        columns = normalized[:]
        local_items: List[Dict[str, Any]] = []
        for row in reader:
            if not row or not any(
                (val.strip() if isinstance(val, str) else str(val or "").strip()) for val in row
            ):
                continue
            while len(columns) < len(row):
                columns.append(f"column_{len(columns) + 1}")
            record: Dict[str, Any] = {}
            for idx_col, value in enumerate(row):
                key = columns[idx_col]
                clean = value.strip() if isinstance(value, str) else str(value or "").strip()
                record[key] = clean
            if any(record.values()):
                local_items.append(record)
            if len(local_items) >= max(1, int(row_limit)):
                break
    return columns, local_items


def _score_csv_rows(columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> float:
    if not columns or not rows:
        return -1.0
    col_count = max(1, len(columns))
    row_count = len(rows)
    non_empty_counts: List[int] = []
    price_like_rows = 0
    collapsed_blob_hits = 0
    for row in rows:
        values = [str(v or "").strip() for v in row.values()]
        non_empty = [v for v in values if v]
        non_empty_counts.append(len(non_empty))
        if any(_has_price_digits(v) for v in non_empty):
            price_like_rows += 1
        if col_count == 1:
            first = values[0] if values else ""
            if any(token in first for token in (",", ";", "\t", "|")):
                collapsed_blob_hits += 1

    avg_non_empty = (sum(non_empty_counts) / len(non_empty_counts)) if non_empty_counts else 0.0
    multi_field_ratio = (
        sum(1 for val in non_empty_counts if val >= 2) / len(non_empty_counts)
        if non_empty_counts
        else 0.0
    )
    price_ratio = price_like_rows / max(1, row_count)
    score = (
        (col_count * 6.0)
        + (avg_non_empty * 3.0)
        + (multi_field_ratio * 20.0)
        + (price_ratio * 15.0)
        + (min(row_count, 500) / 25.0)
    )
    if col_count == 1:
        blob_ratio = collapsed_blob_hits / max(1, row_count)
        score -= 50.0 + (blob_ratio * 40.0)
    return score


def _read_csv_rows_best(
    path: pathlib.Path,
    *,
    encoding: str,
    delimiter: Any,
    row_limit: int = 500,
) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding=encoding, newline="") as fh:
            sample = fh.read(4096)
    except Exception:
        sample = ""

    best_rows: List[Dict[str, Any]] = []
    best_score = float("-inf")
    for cand_delim in _csv_delimiter_candidates(sample, delimiter):
        try:
            columns, rows = _read_csv_rows_with_delimiter(
                path,
                encoding=encoding,
                delimiter=cand_delim,
                row_limit=row_limit,
            )
        except UnicodeDecodeError:
            raise
        except Exception:
            continue
        if not rows:
            continue
        score = _score_csv_rows(columns, rows)
        if score > best_score:
            best_score = score
            best_rows = rows
    return best_rows


def _normalize_catalog_item(record: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    normalized = dict(record)
    for target, source in mapping.items():
        if not source:
            continue
        if target in normalized and str(normalized[target]).strip():
            continue
        value = record.get(source)
        if value is None:
            continue
        normalized[target] = value

    def _ensure_title() -> None:
        title_candidates = [normalized.get("title"), normalized.get("name")]
        for candidate in title_candidates:
            if candidate and str(candidate).strip():
                normalized.setdefault("name", candidate)
                if not str(normalized.get("title") or "").strip():
                    normalized["title"] = candidate
                return

        for key, value in record.items():
            if key in {"price", mapping.get("price", "")}:  # avoid grabbing price column
                continue
            text = str(value or "").strip()
            if len(text) >= 3 and not text.isdigit():
                normalized.setdefault("title", text)
                normalized.setdefault("name", text)
                return

    def _ensure_price() -> None:
        current = normalized.get("price")
        if current and _has_price_digits(current):
            return

        if current and isinstance(current, str) and current.strip():
            digits = re.sub(r"\D", "", current)
            if digits and len(digits) >= 4:
                return

        preferred_columns = [mapping.get("price")]
        for key, value in record.items():
            if key in preferred_columns:
                preferred_columns.append(key)
        seen = set(filter(None, preferred_columns))
        for key in preferred_columns:
            if not key:
                continue
            text = str(record.get(key) or "").strip()
            if _has_price_digits(text):
                normalized["price"] = text
                return

        for key, value in record.items():
            if key in seen:
                continue
            text = str(value or "").strip()
            if _has_price_digits(text):
                normalized["price"] = text
                return

    def _canonical_object_type_value(raw_value: Any) -> str:
        low = _normalize_text(raw_value)
        if not low:
            return ""
        is_apartment = bool(re.search(r"(?iu)\b(apartment|flat|квартир\w*|кв\.)\b", low))
        is_house = bool(re.search(r"(?iu)\b(house|home|частн\w*|коттедж\w*|дом\w*)\b", low))
        if is_apartment and not is_house:
            return "apartment"
        if is_house and not is_apartment:
            return "house"
        return ""

    def _ensure_object_type() -> None:
        direct = _canonical_object_type_value(normalized.get("object_type"))
        if direct:
            normalized["object_type"] = direct
            return

        probe_keys = (
            "object_type",
            "object",
            "target",
            "usage",
            "назначение",
            "тип помещения",
            "тип помещения/объекта",
            "тип объекта",
            "помещение",
        )
        for key in probe_keys:
            val = normalized.get(key)
            kind = _canonical_object_type_value(val)
            if kind:
                normalized["object_type"] = kind
                return
            val = record.get(key)
            kind = _canonical_object_type_value(val)
            if kind:
                normalized["object_type"] = kind
                return

    _ensure_title()
    _ensure_price()
    _ensure_object_type()
    return normalized


def _normalize_catalog_items(
    items: List[Dict[str, Any]], meta: Dict[str, Any] | Any
) -> List[Dict[str, Any]]:
    if not items:
        return items
    meta_dict = meta if isinstance(meta, dict) else {}
    mapping = _prepare_field_mapping(meta_dict, items)
    if not mapping:
        # Even without explicit mapping try to enrich titles and prices
        return [_normalize_catalog_item(record, {}) for record in items]
    return [_normalize_catalog_item(record, mapping) for record in items]


def _apply_catalog_attribute_rules(
    items: List[Dict[str, Any]], persona_meta: Mapping[str, Any] | None
) -> None:
    if not items or not isinstance(persona_meta, Mapping):
        return
    rules_section = persona_meta.get("catalog_tags") or persona_meta.get("catalog_attributes")
    if isinstance(rules_section, Mapping):
        candidates = rules_section.get("tag_rules") or rules_section.get("rules") or []
    else:
        candidates = rules_section or []
    if not isinstance(candidates, Sequence):
        return
    for item in items:
        for rule in candidates:
            if not isinstance(rule, Mapping):
                continue
            if not _catalog_rule_matches(rule, item):
                continue
            name = str(rule.get("name") or "").strip() or None
            tags_to_add = []
            rule_tags = rule.get("tags")
            if isinstance(rule_tags, str):
                tags_to_add = [rule_tags]
            elif isinstance(rule_tags, Sequence):
                tags_to_add = [str(tag) for tag in rule_tags if tag]
            elif name:
                tags_to_add = [name]
            elif rule.get("name"):
                tags_to_add = [str(rule["name"])]
            if not tags_to_add and name:
                tags_to_add = [name]
            if tags_to_add:
                bucket = item.setdefault("tags", [])
                if isinstance(bucket, list):
                    for tag in tags_to_add:
                        if tag not in bucket:
                            bucket.append(tag)
            set_fields = rule.get("set") or {}
            if isinstance(set_fields, Mapping):
                for field, value in set_fields.items():
                    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                        bucket = item.setdefault(str(field), [])
                        if isinstance(bucket, list):
                            for val in value:
                                if val not in bucket:
                                    bucket.append(val)
                    else:
                        current = item.get(field)
                        if not current:
                            item[field] = value


def _catalog_rule_matches(rule: Mapping[str, Any], item: Mapping[str, Any]) -> bool:
    if not item:
        return False
    any_rules = rule.get("any")
    all_rules = rule.get("all")
    matched = True
    if isinstance(any_rules, Sequence) and any_rules:
        matched = any(
            _catalog_condition_matches(item, cond)
            for cond in any_rules
            if isinstance(cond, Mapping)
        )
    if matched and isinstance(all_rules, Sequence) and all_rules:
        matched = all(
            _catalog_condition_matches(item, cond)
            for cond in all_rules
            if isinstance(cond, Mapping)
        )
    return bool(matched)


def _catalog_condition_matches(item: Mapping[str, Any], condition: Mapping[str, Any]) -> bool:
    field = str(condition.get("field") or "").strip()
    values: List[str] = []
    if field:
        raw_value = item.get(field)
        if isinstance(raw_value, (list, tuple, set)):
            values = [str(val or "") for val in raw_value if val]
        else:
            values = [str(raw_value or "")]
    else:
        values = [" ".join(str(val or "") for val in item.values())]
    lowered = [value.casefold() for value in values if value]
    keyed = [_match_key(value) for value in values if value]
    contains = condition.get("contains")
    if contains:
        needles = contains
        if isinstance(needles, str):
            needles = [needles]
        if isinstance(needles, Sequence):
            normalized_needles = [
                str(needle or "").strip().casefold() for needle in needles if needle
            ]
            keyed_needles = [_match_key(needle) for needle in needles if needle]
            for hay in lowered:
                if any(needle in hay for needle in normalized_needles):
                    return True
            for hay in keyed:
                if any(needle and needle in hay for needle in keyed_needles):
                    return True
    regex = condition.get("regex")
    if regex:
        patterns = (
            regex
            if isinstance(regex, Sequence) and not isinstance(regex, (str, bytes))
            else [regex]
        )
        for pattern in patterns:
            try:
                compiled = re.compile(str(pattern), re.IGNORECASE)
            except re.error:
                continue
            for value in values:
                if compiled.search(value):
                    return True
    equals = condition.get("equals")
    if equals is not None:
        eq_values = (
            equals
            if isinstance(equals, Sequence) and not isinstance(equals, (str, bytes))
            else [equals]
        )
        normalized = [str(val or "").strip().casefold() for val in eq_values]
        keyed_equals = [_match_key(val) for val in eq_values]
        for hay in lowered:
            if hay in normalized:
                return True
        for hay in keyed:
            if hay and hay in keyed_equals:
                return True
    return False


def _filter_catalog_items_by_rules(
    items: List[Dict[str, Any]],
    needs: Mapping[str, Any],
    persona_meta: Mapping[str, Any] | None,
) -> List[Dict[str, Any]]:
    if not items or not isinstance(persona_meta, Mapping):
        return items
    rules = persona_meta.get("sales_rules")
    if not isinstance(rules, Sequence):
        return items
    filtered = list(items)
    for raw_rule in rules:
        if not isinstance(raw_rule, Mapping):
            continue
        rule_needs = raw_rule.get("needs")
        if rule_needs and not _needs_block_matches(rule_needs, needs):
            continue
        require_tags = _ensure_list(str, raw_rule.get("require_tags"))
        forbid_tags = _ensure_list(str, raw_rule.get("forbid_tags"))
        require_fields = raw_rule.get("require_fields")
        if not require_tags and not forbid_tags and not require_fields:
            continue
        current: List[Dict[str, Any]] = []
        for item in filtered:
            tags = {str(tag) for tag in (item.get("tags") or []) if tag}
            if require_tags and not tags.issuperset(require_tags):
                continue
            if forbid_tags and tags.intersection(forbid_tags):
                continue
            if require_fields and not _item_fields_match(item, require_fields):
                continue
            current.append(item)
        if current:
            filtered = current
        else:
            filtered = []
            break
    return filtered


def _ensure_list(caster, value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = [value]
    normalized = set()
    for val in values:
        try:
            converted = caster(val)
        except Exception:
            continue
        clean = str(converted).strip()
        if clean:
            normalized.add(clean)
    return normalized


def _needs_block_matches(rule_needs: Mapping[str, Any], actual: Mapping[str, Any]) -> bool:
    if not isinstance(rule_needs, Mapping):
        return True
    for key, expected in rule_needs.items():
        if expected is None:
            continue
        actual_value = actual.get(key)
        if isinstance(expected, (list, tuple, set)):
            normalized = {str(val).casefold() for val in expected}
            if str(actual_value).casefold() not in normalized:
                return False
        else:
            if str(actual_value).casefold() != str(expected).casefold():
                return False
    return True


def _item_fields_match(item: Mapping[str, Any], requirements: Mapping[str, Any]) -> bool:
    if not isinstance(requirements, Mapping):
        return True
    for field_name, expected in requirements.items():
        value = item.get(field_name)
        if isinstance(expected, (list, tuple, set)):
            norm_expected = {str(val).casefold() for val in expected}
            if str(value).casefold() not in norm_expected:
                return False
        else:
            if str(value).casefold() != str(expected).casefold():
                return False
    return True


def _read_catalog(tenant: int | None = None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    candidates: List[tuple[pathlib.Path, Dict[str, Any]]] = []
    persona_meta: Dict[str, Any] = {}

    if tenant is not None:
        try:
            cfg = load_tenant(tenant)
            persona_meta = persona_meta_config(int(tenant))
            catalogs = cfg.get("catalogs") or []
            if isinstance(catalogs, list):
                for entry in catalogs:
                    if not isinstance(entry, dict):
                        continue

                    def _resolve_path(raw: str | pathlib.Path | None) -> pathlib.Path | None:
                        if not raw:
                            return None
                        path = pathlib.Path(str(raw))
                        if not path.is_absolute():
                            path = tenant_dir(tenant) / path
                        return path

                    raw_csv = entry.get("csv_path")
                    csv_path = _resolve_path(raw_csv)
                    if csv_path:
                        csv_meta = dict(entry)
                        csv_meta["type"] = "csv"
                        csv_meta["path"] = raw_csv
                        merged_csv_meta = _merge_csv_mapping_meta(csv_meta, persona_meta)
                        candidates.append((csv_path, merged_csv_meta))
                        # Continue processing original entry as fallback (PDF/Excel)

                    raw_path = entry.get("path")
                    path = _resolve_path(raw_path)
                    if not path:
                        continue
                    merged_meta = _merge_csv_mapping_meta(entry, persona_meta)
                    candidates.append((path, merged_meta))
        except Exception:
            pass
        persona_csv_path = persona_catalog_csv(int(tenant))
        if persona_csv_path:
            csv_delimiter = str(persona_meta.get("catalog_csv_delimiter") or "").strip()
            csv_encoding = (
                str(persona_meta.get("catalog_csv_encoding") or "utf-8").strip() or "utf-8"
            )
            meta = _merge_csv_mapping_meta(
                {
                    "type": "csv",
                    "delimiter": csv_delimiter or None,
                    "encoding": csv_encoding,
                },
                persona_meta,
            )
            candidate_tuple = (
                persona_csv_path,
                meta,
            )
            if candidate_tuple not in candidates:
                candidates.insert(0, candidate_tuple)

    if not candidates:
        default_meta = _merge_csv_mapping_meta(
            {"delimiter": ",", "encoding": "utf-8"}, persona_meta
        )
        candidates.append((CATALOG_CSV, default_meta))

    # mtime/size-based cache key to avoid repeated heavy parsing
    key_fps: List[Tuple[str, float, int]] = []
    try:
        for pth, meta in candidates:
            meta = meta if isinstance(meta, dict) else {}
            meta_type = (meta.get("type") or pth.suffix.lstrip(".")).lower()
            stat_target = pth
            if pth.suffix.lower() == ".pdf" or meta_type == "pdf":
                idx_val = meta.get("index_path")
                if idx_val and tenant is not None:
                    cand = pathlib.Path(str(idx_val))
                    if not cand.is_absolute():
                        cand = tenant_dir(int(tenant)) / cand
                    if cand.exists():
                        stat_target = cand
            if stat_target.exists():
                st = stat_target.stat()
                key_fps.append(
                    (str(stat_target.resolve()), st.st_mtime, int(getattr(st, "st_size", 0) or 0))
                )
    except Exception:
        key_fps = []
    cache_key: Tuple[Optional[int], Tuple[Tuple[str, float, int], ...]] = (
        (int(tenant) if tenant is not None else None),
        tuple(sorted(key_fps)),
    )
    cached = _CATALOG_CACHE.get(cache_key)
    if cached:
        return cached

    for path, meta in candidates:
        try:
            if not path.exists():
                continue

            meta = meta if isinstance(meta, dict) else {}
            encoding = meta.get("encoding", "utf-8")
            meta_type = (meta.get("type") or path.suffix.lstrip(".")).lower()

            if path.suffix.lower() in {".xlsx", ".xls"} or meta_type == "excel":
                if load_workbook is None:
                    continue
                wb = load_workbook(filename=str(path), read_only=True, data_only=True)
                ws = wb.active
                headers = []
                for cell in next(ws.iter_rows(min_row=1, max_row=1), []):
                    headers.append(str(cell.value or "").strip())
                if not headers:
                    wb.close()
                    continue
                collected: List[Dict[str, Any]] = []
                for idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=1):
                    record = {}
                    for h, val in zip(headers, row):
                        record[str(h or "").strip()] = str(val).strip() if val is not None else ""
                    if any(record.values()):
                        collected.append(record)
                    if idx >= 500:
                        break
                wb.close()
                if collected:
                    items = _normalize_catalog_items(collected, meta)
                    if items:
                        break
                continue

            if path.suffix.lower() == ".pdf" or meta_type == "pdf":
                if not isinstance(meta, dict):
                    continue
                raw_source_key = str(meta.get("path") or path)
                index_path_value = meta.get("index_path")
                index_path_obj: pathlib.Path | None = None

                if index_path_value:
                    candidate = pathlib.Path(str(index_path_value))
                    if not candidate.is_absolute() and tenant is not None:
                        candidate = tenant_dir(int(tenant)) / candidate
                    if candidate.exists():
                        index_path_obj = candidate

                if index_path_obj is None and tenant is not None:
                    try:
                        from catalog_index import build_pdf_index

                        try:
                            rel_source = str(path.relative_to(tenant_dir(int(tenant))))
                        except Exception:
                            rel_source = str(meta.get("path") or path.name)

                        index_dir = tenant_dir(int(tenant)) / "indexes"
                        built_index = build_pdf_index(
                            path,
                            output_dir=index_dir,
                            source_relpath=rel_source,
                            original_name=path.name,
                        )

                        index_path_obj = built_index.index_path
                        try:
                            rel_index_path = str(
                                index_path_obj.relative_to(tenant_dir(int(tenant)))
                            )
                        except Exception:
                            rel_index_path = str(index_path_obj)

                        meta["index_path"] = rel_index_path
                        meta["indexed_at"] = built_index.generated_at
                        meta["chunk_count"] = built_index.chunk_count
                        meta["sha1"] = built_index.sha1

                        _persist_pdf_index_metadata(
                            int(tenant),
                            raw_source_key,
                            rel_index_path,
                            {
                                "generated_at": built_index.generated_at,
                                "chunk_count": built_index.chunk_count,
                                "sha1": built_index.sha1,
                                "page_count": built_index.page_count,
                                "source_path": built_index.source_path,
                            },
                        )
                    except Exception:
                        continue

                if not index_path_obj:
                    continue

                try:
                    from catalog_index import load_index, index_to_catalog_items

                    index = load_index(index_path_obj)
                    indexed_items = index_to_catalog_items(index)
                    if indexed_items:
                        items = indexed_items
                        break
                except Exception:
                    continue
                continue

            # CSV и подобные
            delimiter = meta.get("delimiter")
            enc_candidates: List[str] = []
            if isinstance(encoding, str) and encoding:
                enc_candidates.append(encoding)
            # Try declared encoding first, then common Russian CSV fallbacks.
            for fallback in ("utf-8", "utf-8-sig", "cp1251", "windows-1251", "koi8-r"):
                if fallback not in enc_candidates:
                    enc_candidates.append(fallback)

            used_items: List[Dict[str, Any]] = []
            for enc in enc_candidates or ["utf-8"]:
                try:
                    local_items = _read_csv_rows_best(
                        path,
                        encoding=enc,
                        delimiter=delimiter,
                        row_limit=500,
                    )
                    if local_items:
                        used_items = local_items
                        break
                except UnicodeDecodeError:
                    continue
            if used_items:
                items = _normalize_catalog_items(used_items, meta)
                break
        except Exception:
            continue

    if not items:
        return []

    try:
        _apply_catalog_attribute_rules(items, persona_meta)
    except Exception:
        logger.debug("catalog_attribute_rules_failed", exc_info=True)
    try:
        _enrich_catalog_color_aliases(items, persona_meta)
    except Exception:
        logger.debug("catalog_color_alias_failed", exc_info=True)

    # Precompute normalized search text for matching speed
    try:
        for it in items:
            if isinstance(it, dict):
                it["_search_text"] = _normalize_text(_collect_item_text(it))
    except Exception:
        pass

    # Store in cache
    try:
        _CATALOG_CACHE[cache_key] = items
    except Exception:
        pass

    return items


def read_all_catalog(
    cfg: Optional[Dict[str, Any]] = None, tenant: int | None = None
) -> List[Dict[str, Any]]:
    """Возвращает список позиций каталога для арендатора."""
    tenant_id: Optional[int] = None
    if tenant is not None:
        try:
            tenant_id = int(tenant)
        except Exception:
            tenant_id = None
    elif isinstance(cfg, dict):
        passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
        raw_id = passport.get("tenant_id")
        try:
            tenant_id = int(raw_id) if raw_id is not None else None
        except Exception:
            tenant_id = None
    return _read_catalog(tenant_id)


def paginate_catalog_text(
    items: List[Dict[str, Any]],
    cfg: Optional[Dict[str, Any]] = None,
    page_size: int = 10,
) -> List[str]:
    """Формирует текстовые страницы каталога."""
    if not items:
        return []

    try:
        page_size = int(page_size)
    except Exception:
        page_size = 10
    if page_size <= 0:
        page_size = 10

    currency = "₽"
    if isinstance(cfg, dict):
        passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
        cur = passport.get("currency")
        if cur:
            currency = str(cur)

    formatted_lines = format_items_for_prompt(items, currency).splitlines()
    pages: List[str] = []
    for idx in range(0, len(formatted_lines), page_size):
        chunk = formatted_lines[idx : idx + page_size]
        if not chunk:
            continue
        page_no = idx // page_size + 1
        header = f"Каталог, страница {page_no}:"
        pages.append("\n".join([header, *chunk]))
    return pages


NEEDS_STOPWORDS = {
    "нужно",
    "нужна",
    "нужен",
    "нужны",
    "ищу",
    "ищем",
    "ищет",
    "ищете",
    "ищите",
    "хочу",
    "интересует",
    "интересуют",
    "каталог",
    "про",
    "для",
    "подбор",
    "бюджет",
    "стоимость",
    "цена",
    "цену",
    "ценник",
    "до",
    "подберите",
    "подбер",
    "посоветуйте",
    "подскажите",
    "расскажите",
    "рассмотрите",
    "еще",
    "ещё",
    "можно",
    "пожалуйста",
    "дайте",
    "заказ",
    "самый",
    "самая",
    "самое",
    "самые",
    "дорогой",
    "дорогая",
    "дешевый",
    "дешевая",
    "подороже",
    "подешевле",
    "добрый",
    "вечер",
    "день",
    "привет",
    "меня",
    "интересуют",
    "надо",
    "пока",
    "под",
    "есть",
    "или",
    "и",
    "в",
    "на",
    "с",
    "как",
    "что",
    "так",
    "же",
    "подбор",
}

COLOR_STEMS = {
    "бел": "белый",
    "черн": "чёрный",
    "чёр": "чёрный",
    "чер": "чёрный",
    "венг": "венге",
    "дуб": "дуб",
    "сер": "серый",
    "корич": "коричневый",
    "красн": "красный",
    "син": "синий",
    "голуб": "голубой",
    "зел": "зелёный",
    "зол": "золотой",
    "сталь": "стальной",
    "беж": "бежевый",
    "медн": "коричневый",
    "шокол": "коричневый",
    "орех": "коричневый",
    "мокк": "коричневый",
    "букл": "коричневый",
    "графит": "чёрный",
    "бетон": "серый",
    "айвор": "белый",
    "жемч": "белый",
    "перлам": "белый",
    "слон": "бежевый",
    "антрац": "чёрный",
}


def _normalize_color_token(value: str | None) -> str:
    return str(value or "").strip().casefold().replace("ё", "е")


_GLOBAL_COLOR_ALIAS_RAW = {
    "белый": [
        "белый",
        "white",
        "snow",
        "снежный",
        "молочный",
        "молочн",
        "жемчужный",
        "перламутровый",
        "айвори",
        "ivory",
        "слоновая кость",
        "сливочный",
    ],
    "чёрный": [
        "чёрный",
        "черный",
        "черн",
        "black",
        "coal",
        "obsidian",
        "антрацит",
        "антрацитовый",
        "графит",
        "черный муар",
        "onyx",
        "графитовый",
        "каменный",
    ],
    "серый": [
        "серый",
        "серебристый",
        "серебро",
        "серебр",
        "metallic",
        "металлик",
        "grey",
        "gray",
        "бетон",
        "бетонный",
        "cement",
        "стальной",
        "steel",
        "никель",
        "chrome",
    ],
    "коричневый": [
        "коричневый",
        "корич",
        "brown",
        "шоколад",
        "шоколадный",
        "какао",
        "coffee",
        "кофейный",
        "капучино",
        "медный",
        "медный антик",
        "антик медь",
        "коньяк",
        "терракот",
        "терракотовый",
        "орех",
        "венге",
        "мокка",
        "мокко",
        "букле шоколад",
        "каштан",
        "махагон",
        "бурбон",
    ],
    "бежевый": [
        "бежевый",
        "беж",
        "beige",
        "кремовый",
        "крем",
        "песочный",
        "sand",
        "linen",
        "слоновая",
        "ваниль",
        "ретро",
        "сахарный",
        "карамель",
    ],
    "красный": [
        "красный",
        "красн",
        "red",
        "бордо",
        "бордовый",
        "марсала",
        "винный",
        "burgundy",
        "кирпичный",
        "терракота",
        "вишня",
        "carmin",
    ],
    "синий": [
        "синий",
        "син",
        "blue",
        "navy",
        "индиго",
        "ультрамарин",
        "кобальт",
        "лазурь",
    ],
    "голубой": [
        "голубой",
        "голуб",
        "teal",
        "бирюзовый",
        "аквамарин",
        "cyan",
        "лазурный",
        "небесный",
    ],
    "зелёный": [
        "зелёный",
        "зеленый",
        "green",
        "хаки",
        "оливковый",
        "olive",
        "мятный",
        "бирюзово-зелёный",
        "салатовый",
        "forest",
    ],
    "жёлтый": [
        "жёлтый",
        "желтый",
        "yellow",
        "янтарный",
        "amber",
        "горчичный",
        "охра",
        "солнечный",
    ],
    "серебристый": [
        "серебристый",
        "silver",
        "metallic",
        "алюминий",
        "стальной",
        "chrome",
    ],
    "золотой": [
        "gold",
        "golden",
        "золотой",
        "латунный",
        "бронзовый",
        "бронза",
    ],
    "венге": [
        "венге",
        "венге шоколад",
        "венге темный",
        "венге светлый",
    ],
    "орех": [
        "орех",
        "итальянский орех",
        "золотой орех",
        "темный орех",
        "светлый орех",
    ],
    "дуб": [
        "дуб",
        "дуб рустикальный",
        "дуб светлый",
        "дуб темный",
    ],
    "букле": [
        "букле",
        "букле антрацит",
        "букле шоколад",
        "букле серый",
    ],
}


def _normalize_alias_map(raw_map: Mapping[str, Sequence[str]]) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for key, aliases in raw_map.items():
        base = _normalize_color_token(key)
        if not base:
            continue
        bucket: set[str] = set()
        for alias in aliases:
            alias_norm = _normalize_color_token(alias)
            if alias_norm:
                bucket.add(alias_norm)
        normalized[base] = sorted(bucket)
    return normalized


_GLOBAL_COLOR_ALIASES = _normalize_alias_map(_GLOBAL_COLOR_ALIAS_RAW)


def _persona_color_alias_map(persona_meta: Mapping[str, Any] | None) -> dict[str, list[str]]:
    if not isinstance(persona_meta, Mapping):
        return {}
    raw = persona_meta.get("color_aliases")
    if not raw:
        return {}
    mapping: dict[str, list[str]] = {}
    if isinstance(raw, Mapping):
        iterator = raw.items()
    else:
        iterator = []
    for base, aliases in iterator:
        base_norm = _normalize_color_token(base)
        if not base_norm:
            continue
        if isinstance(aliases, str):
            alias_list = [aliases]
        elif isinstance(aliases, Sequence):
            alias_list = [str(val) for val in aliases if val]
        elif isinstance(aliases, Mapping):
            alias_list = [str(val) for val in aliases.values() if val]
        else:
            alias_list = [str(aliases)]
        bucket: set[str] = set()
        for alias in alias_list:
            normalized = _normalize_color_token(alias)
            if normalized:
                bucket.add(normalized)
        if bucket:
            mapping[base_norm] = sorted(bucket)
    return mapping


def _augment_color_needs(needs: Dict[str, Any], persona_meta: Mapping[str, Any] | None) -> None:
    color_value = needs.get("color")
    if not color_value:
        needs.pop("_color_tokens", None)
        return
    canonical = _normalize_color_token(color_value)
    if not canonical:
        needs.pop("_color_tokens", None)
        return
    tokens: set[str] = {canonical}
    tokens.update(_GLOBAL_COLOR_ALIASES.get(canonical, []))
    persona_map = _persona_color_alias_map(persona_meta)
    tokens.update(persona_map.get(canonical, []))
    needs["_color_tokens"] = sorted(tokens)


def _build_color_lookup_map(persona_meta: Mapping[str, Any] | None) -> dict[str, str]:
    lookup: dict[str, str] = {}

    def _register(canonical: str, aliases: Sequence[str]) -> None:
        canon = _normalize_color_token(canonical)
        if not canon:
            return
        lookup.setdefault(canon, canon)
        for alias in aliases:
            alias_norm = _normalize_color_token(alias)
            if alias_norm:
                lookup.setdefault(alias_norm, canon)

    for base, synonyms in _GLOBAL_COLOR_ALIASES.items():
        _register(base, synonyms)

    persona_map = _persona_color_alias_map(persona_meta)
    for base, synonyms in persona_map.items():
        _register(base, synonyms)

    return lookup


def _collect_color_text(item: Mapping[str, Any]) -> str:
    values: list[str] = []
    for key in ("color", "finish", "shade", "title", "name", "tags", "description", "features"):
        value = item.get(key)
        if not value:
            continue
        if isinstance(value, str):
            values.append(value)
        elif isinstance(value, (list, tuple, set)):
            values.extend(str(val) for val in value if val)
    return " ".join(values)


def _enrich_catalog_color_aliases(
    items: List[Dict[str, Any]],
    persona_meta: Mapping[str, Any] | None,
) -> None:
    if not items:
        return
    lookup = _build_color_lookup_map(persona_meta)
    if not lookup:
        return
    alias_items = sorted(lookup.items(), key=lambda kv: len(kv[0]), reverse=True)
    for item in items:
        text = _collect_color_text(item)
        if not text:
            continue
        normalized = _normalize_color_token(text)
        if not normalized:
            continue
        matched: set[str] = set()
        for alias, canonical in alias_items:
            if alias and alias in normalized:
                matched.add(canonical)
        if not matched:
            continue
        current = item.get("_color_aliases")
        color_set: set[str] = set(current or [])
        color_set.update(matched)
        item["_color_aliases"] = sorted(color_set)
        tags = item.setdefault("tags", [])
        if isinstance(tags, list):
            for canonical in matched:
                tag = f"color:{canonical}"
                if tag not in tags:
                    tags.append(tag)


SIZE_PATTERN = re.compile(
    r"(?P<value>\d{2,4})(?:\s|\-)?(?P<unit>см|mm|мм|cm|м|kg|кг|g|гр|ml|мл|l|л)", re.IGNORECASE
)


def _extract_budget(text: str) -> Optional[int]:
    if not text:
        return None
    lowered = text.lower()
    candidates: List[int] = []
    for match in re.finditer(r"\d+[\d\s]*", lowered):
        raw_number = match.group(0)
        digits = re.sub(r"\D", "", raw_number)
        if not digits:
            continue
        try:
            value = int(digits)
        except Exception:
            continue

        suffix = lowered[match.end() : match.end() + 4]
        prefix = lowered[max(0, match.start() - 12) : match.start()]

        def _has_token(container: str, tokens: Tuple[str, ...]) -> bool:
            return any(token in container for token in tokens)

        thousand_tokens = ("k", "к", "тыс", "т.", "т ", "тысяч")
        million_tokens = ("млн", "mln")
        currency_tokens = ("₽", "р", "rub", "руб", "eur", "€", "usd", "$")
        context_tokens = ("цен", "стоим", "бюдж", "до", "≈", "~", "max", "за ", "по ")

        if _has_token(suffix, thousand_tokens):
            value *= 1000
        elif _has_token(suffix, million_tokens):
            value *= 1_000_000

        has_currency = _has_token(suffix, currency_tokens) or _has_token(prefix, currency_tokens)
        has_context = _has_token(prefix, context_tokens)

        if value < 100:
            continue
        if not has_currency and not has_context and value < 1000:
            continue

        candidates.append(value)

    if not candidates:
        return None
    return max(candidates)


def _extract_price_order_intent(text: str) -> Optional[str]:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return None
    if _MAX_PRICE_INTENT_RE.search(low):
        return "desc"
    if _MIN_PRICE_INTENT_RE.search(low) or ("подешев" in low) or ("дешев" in low):
        return "asc"
    return None


def _looks_like_price_objection(text: str) -> bool:
    low = _normalize_text(text)
    if not low:
        return False
    return any(stem in low for stem in ("дорог", "дешев", "переплат", "цен"))


def infer_user_needs(text: str) -> Dict[str, Any]:
    raw = text or ""
    lowered = raw.lower()
    needs: Dict[str, Any] = {}
    is_address_like_turn = _looks_like_address_value(raw)

    tokens = [] if is_address_like_turn else _tokenize_query(raw)
    keywords = [tok for tok in tokens if tok and tok not in NEEDS_STOPWORDS and not tok.isdigit()]
    if keywords:
        needs["keywords"] = keywords[:6]
        needs["type"] = keywords[0]
        needs["focus"] = " ".join(keywords[:3])

    size_match = SIZE_PATTERN.search(lowered)
    if size_match:
        value = size_match.group("value")
        unit = size_match.group("unit").lower()
        normalized_unit = {
            "mm": "мм",
            "cm": "см",
            "m": "м",
            "kg": "кг",
            "g": "г",
            "gr": "г",
            "l": "л",
        }.get(unit, unit)
        needs["size"] = f"{value} {normalized_unit}"
        if normalized_unit in {"см", "mm", "мм"}:
            needs["width"] = value

    budget = _extract_budget(lowered)
    if budget:
        needs["budget_max"] = budget
    price_order = _extract_price_order_intent(lowered)
    if price_order:
        needs["price_order"] = price_order

    if not is_address_like_turn:
        for stem, title in COLOR_STEMS.items():
            if stem in lowered:
                needs["color"] = title
                break

    detected_object_type = _object_type_from_turn_text(lowered)
    if detected_object_type:
        needs["object_type"] = detected_object_type
    elif re.search(r"(?iu)\bэтаж\w*\b", lowered):
        # Heuristic: floor mention most often indicates apartment context.
        needs["object_type"] = "apartment"

    if _NOISE_NEED_RE.search(lowered):
        needs["noise_priority"] = True
    if _INSULATION_NEED_RE.search(lowered):
        needs["insulation_priority"] = True

    return needs


def _value_matches(item: Dict[str, Any], fields: Tuple[str, ...], needle: str) -> bool:
    for field_name in fields:
        val = item.get(field_name)
        if not val:
            continue
        if isinstance(val, (list, tuple, set)):
            texts = [str(v) for v in val if v]
        else:
            texts = [str(val)]
        for text in texts:
            if needle in _normalize_text(text):
                return True
    return False


def _score(item: Dict[str, Any], needs: Dict[str, Any]) -> float:
    s = 0.0
    haystack_text = _normalize_text(_collect_item_text(item))

    primary = needs.get("type")
    if primary:
        needle = _normalize_text(primary)
        if needle and (
            _value_matches(item, ("type", "category", "segment", "group"), needle)
            or needle in haystack_text
        ):
            s += 3.0

    keywords = needs.get("keywords") or []
    if keywords:
        for kw in keywords[:3]:
            needle = _normalize_text(kw)
            if needle and needle in haystack_text:
                s += 1.0

    size = needs.get("size") or needs.get("width")
    if size:
        size_str = _normalize_text(str(size))
        if _value_matches(
            item, ("size", "width", "dimensions", "length", "height", "depth"), size_str
        ):
            s += 1.5

    color_tokens = needs.get("_color_tokens") or []
    if color_tokens:
        item_color_aliases = set(
            str(alias) for alias in (item.get("_color_aliases") or []) if alias
        )
        for token in color_tokens:
            if _value_matches(item, ("color", "finish", "shade", "title", "name", "tags"), token):
                s += 0.8
                break
            if item_color_aliases and token in item_color_aliases:
                s += 0.8
                break

    budget = needs.get("budget_max")
    if budget:
        try:
            price = int(re.sub(r"\D", "", str(item.get("price") or "0")))
            if price and price <= int(budget):
                s += 1.5
        except Exception:
            pass

    return s


_WORD_TOKEN_RE = re.compile(r"[0-9a-zа-яё]+", re.IGNORECASE)


def _normalize_text(value: Any) -> str:
    text = str(value or "")
    return text.casefold().replace("ё", "е")


_LAT_TO_LAT = {chr(c): chr(c) for c in range(ord("a"), ord("z") + 1)}
_CYR_TO_LAT = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "i",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "c",
    "ч": "ch",
    "ш": "sh",
    "щ": "sch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}


def _match_key(value: Any) -> str:
    text = _normalize_text(value)
    out: List[str] = []
    for ch in text:
        if ch in _LAT_TO_LAT:
            out.append(_LAT_TO_LAT[ch])
            continue
        mapped = _CYR_TO_LAT.get(ch)
        if mapped is not None:
            out.append(mapped)
            continue
        if ch.isdigit():
            out.append(ch)
            continue
        if ch in {" ", "-", "_", "/"}:
            out.append(" ")
    return re.sub(r"\s+", " ", "".join(out)).strip()


def _collect_item_text(item: Dict[str, Any]) -> str:
    cached = item.get("_search_text")
    if isinstance(cached, str) and cached:
        return cached
    parts: List[str] = []
    known_keys = {
        "title",
        "name",
        "sku",
        "id",
        "brand",
        "collection",
        "category",
        "series",
        "model",
        "color",
        "material",
        "decor",
        "finish",
        "tags",
        "description",
        "notes",
        "features",
    }
    for key in (
        "title",
        "name",
        "sku",
        "id",
        "brand",
        "collection",
        "category",
        "series",
        "model",
        "color",
        "material",
        "decor",
        "finish",
        "tags",
        "description",
        "notes",
        "features",
    ):
        if key not in item:
            continue
        value = item.get(key)
        if isinstance(value, (list, tuple, set)):
            parts.extend(str(v) for v in value if v)
        elif value:
            parts.append(str(value))
    # Include additional populated fields from arbitrary CSV headers so retrieval
    # can use tenant-specific attributes without hardcoding column names.
    for key, value in item.items():
        if key in known_keys or str(key).startswith("_"):
            continue
        if isinstance(value, (list, tuple, set)):
            for val in value:
                text = str(val or "").strip()
                if text:
                    parts.append(f"{key} {text}")
        else:
            text = str(value or "").strip()
            if text:
                parts.append(f"{key} {text}")
    return " ".join(parts)


def _tokenize_query(text: str | None) -> List[str]:
    if not text:
        return []
    cleaned = _normalize_text(text)
    tokens: List[str] = []
    for raw in _WORD_TOKEN_RE.findall(cleaned):
        token = raw.strip()
        if not token:
            continue
        if token.isdigit():
            tokens.append(token)
            continue
        if len(token) >= 3:
            tokens.append(token)
    return tokens[:12]


def _tag_boost(item: Dict[str, Any]) -> float:
    return 0.0


def _text_match_score(item: Dict[str, Any], tokens: List[str]) -> float:
    if not tokens:
        return 0.0
    haystack = _normalize_text(_collect_item_text(item))
    if not haystack:
        return 0.0
    hay_tokens = set(_WORD_TOKEN_RE.findall(haystack))
    score = 0.0
    for token in tokens:
        if not token:
            continue
        if token in hay_tokens:
            score += 2.5
            continue
        if token.isdigit() and token in haystack:
            score += 1.5
            continue
        if len(token) >= 4:
            prefix = token[:4]
            if prefix in haystack:
                score += 0.75
                continue
    return score


def _legacy_rank_catalog(
    items: List[Dict[str, Any]],
    needs: Dict[str, Any],
    limit: int,
    query: str | None,
) -> List[Dict[str, Any]]:
    query_tokens = _tokenize_query(query)
    query_low = _normalize_text(query or "")
    wants_noise = False
    wants_insulation = False
    object_type = str(needs.get("object_type") or "").strip().lower()
    price_order = str(needs.get("price_order") or "").strip().lower()
    if price_order not in {"asc", "desc"}:
        price_order = ""
    price_values = [_item_price_int(dict(item)) for item in items]
    clean_prices = [int(v) for v in price_values if isinstance(v, int) and v > 0]
    price_floor = min(clean_prices) if clean_prices else None
    price_ceil = max(clean_prices) if clean_prices else None

    def _item_has_outer_mdf(item: Mapping[str, Any]) -> bool:
        return False

    def _item_has_thermal_break(item: Mapping[str, Any]) -> bool:
        return False

    def _noise_preference_score(item: Dict[str, Any]) -> float:
        return 0.0

    def _total_score(item: Dict[str, Any]) -> float:
        base = _score(item, needs)
        matched = _text_match_score(item, query_tokens)
        tag_bonus = _tag_boost(item)
        noise_bonus = _noise_preference_score(item)
        price_bias = 0.0
        if price_order and price_floor is not None and price_ceil is not None and price_ceil > price_floor:
            item_price = _item_price_int(dict(item))
            if isinstance(item_price, int) and item_price > 0:
                ratio = (item_price - price_floor) / max(1, (price_ceil - price_floor))
                if price_order == "desc":
                    price_bias = 3.0 * ratio
                else:
                    price_bias = 3.0 * (1.0 - ratio)
        return base + matched + tag_bonus + noise_bonus + price_bias

    scored = sorted(items, key=_total_score, reverse=True)
    if limit <= 0:
        return scored
    return scored[:limit]


def _catalog_item_identity(item: Dict[str, Any]) -> str:
    for key in ("id", "sku", "title", "name"):
        val = item.get(key)
        if val:
            return str(val)
    return json.dumps(item, ensure_ascii=False, sort_keys=True)


def _merge_catalog_results(
    base: List[Dict[str, Any]],
    fallback: List[Dict[str, Any]],
    limit: int,
) -> List[Dict[str, Any]]:
    if limit <= 0:
        return base
    seen = {_catalog_item_identity(item) for item in base}
    for item in fallback:
        identity = _catalog_item_identity(item)
        if identity in seen:
            continue
        base.append(item)
        seen.add(identity)
        if len(base) >= limit:
            break
    return base


def _sort_catalog_by_price_order(
    items: Sequence[Mapping[str, Any]],
    order: str,
) -> List[Dict[str, Any]]:
    normalized_order = str(order or "").strip().lower()
    if normalized_order not in {"asc", "desc"}:
        return [dict(item) for item in items]
    indexed = list(enumerate(items))

    def _key(entry: Tuple[int, Mapping[str, Any]]) -> Tuple[int, int, int]:
        idx, item = entry
        price = _item_price_int(dict(item))
        if not isinstance(price, int) or price <= 0:
            return (1, 0, idx)
        if normalized_order == "desc":
            return (0, -price, idx)
        return (0, price, idx)

    indexed.sort(key=_key)
    return [dict(item) for _, item in indexed]


def search_catalog(
    needs: Dict[str, Any],
    limit: int = 5,
    tenant: int | None = None,
    query: str | None = None,
) -> List[Dict[str, Any]]:
    needs = dict(needs or {})
    query_price_order = _extract_price_order_intent(str(query or ""))
    if query_price_order and "price_order" not in needs:
        needs["price_order"] = query_price_order
    items = _read_catalog(tenant)
    if not items:
        items = _read_catalog(None)
    persona_meta: Dict[str, Any] = {}
    if tenant is not None:
        try:
            persona_meta = persona_meta_config(int(tenant))
        except Exception:
            persona_meta = {}
    _augment_color_needs(needs, persona_meta)
    filtered = _filter_catalog_items_by_rules(items, needs, persona_meta) if items else []
    if filtered:
        items = filtered

    explicit_price_order = str(needs.get("price_order") or "").strip().lower()
    if explicit_price_order in {"asc", "desc"} and items:
        ranked = _legacy_rank_catalog(items, needs, 0, query)
        ordered = _sort_catalog_by_price_order(ranked, explicit_price_order)
        if limit <= 0:
            return ordered
        return ordered[:limit]

    advanced: List[Dict[str, Any]] = []
    if catalog_retriever and items:
        try:
            advanced = catalog_retriever.retrieve_context(
                items=items,
                needs=needs,
                query=query or "",
                tenant=tenant,
                limit=limit,
            )
        except Exception as exc:
            logger.exception("catalog retriever failed", exc_info=exc)

    if advanced:
        wants_noise = bool(_NOISE_NEED_RE.search(_normalize_text(query or "")))
        if wants_noise:
            fallback_noise = _legacy_rank_catalog(items, needs, max(limit, 8), query)
            blended_noise = _merge_catalog_results(list(advanced), fallback_noise, max(limit, 8))
            ranked_noise = _legacy_rank_catalog(blended_noise, needs, limit, query)
            if ranked_noise:
                return ranked_noise[:limit]
        if limit <= 0:
            return advanced
        if len(advanced) < limit:
            fallback = _legacy_rank_catalog(items, needs, limit, query)
            if fallback:
                advanced = _merge_catalog_results(advanced, fallback, limit)
        return advanced[:limit]

    return _legacy_rank_catalog(items, needs, limit, query)


def format_items_for_prompt(items: List[Dict[str, Any]], currency: str = "₽") -> str:
    if not items:
        return ""
    out = []
    for idx, it in enumerate(items, start=1):
        title = (
            it.get("title") or it.get("name") or it.get("sku") or it.get("id") or f"Позиция {idx}"
        )
        raw_price = str(it.get("price") or "").strip()
        # Берём только первую числовую группу, чтобы не склеивать все цифры из строки/CSV.
        price_match = re.search(r"\d[\d\s.,]*", raw_price)
        digits = re.sub(r"\D", "", price_match.group(0)) if price_match else ""
        if digits:
            try:
                price_fmt = f"{int(digits):,}".replace(",", " ")
            except Exception:
                price_fmt = raw_price
        else:
            price_fmt = raw_price or "цена по запросу"

        details: List[str] = []
        if it.get("brand"):
            details.append(str(it.get("brand")).strip())
        if it.get("width"):
            details.append(f"{it['width']} см")
        if it.get("color"):
            details.append(str(it.get("color")).strip())
        stock = it.get("stock")
        if stock is not None and str(stock).strip():
            try:
                stock_val = int(str(stock).strip())
                if stock_val > 0:
                    details.append("в наличии")
            except Exception:
                details.append(str(stock))
        url = (it.get("url") or "").strip()
        meta = f" ({', '.join(details)})" if details else ""
        line = f"{idx}. {title} — {price_fmt} {currency}{meta}"
        rag_score = it.get("_rag_score")
        if isinstance(rag_score, (int, float)) and rag_score > 0:
            line += f" (релевантность {rag_score:.2f})"
        if url:
            line += f" · {url}"
        excerpt = str(it.get("_match_excerpt") or "").strip()
        if excerpt:
            line = f"{line}\n   ↳ {excerpt}"
        out.append(line)
    return "\n".join(out)


def format_needs_for_prompt(needs: Dict[str, Any]) -> str:
    if not needs:
        return ""
    parts = []
    for k in ["type", "width", "color", "budget_max"]:
        if k in needs:
            parts.append(f"{k}={needs[k]}")
    return ", ".join(parts) if parts else ""


def pick_cta(contact_id: int, channel: str | None, stage: str = "intro") -> Dict[str, str]:
    opts = [
        "Зафиксирую лучшие условия и пришлю подбор сегодня. Подходит?",
        "Готов обсудить детали и оформить заказ без задержек. Продолжаем?",
        "Забронирую цену на сутки и подготовлю договор. Двигаемся?",
    ]
    return {"text": opts[hash(contact_id) % len(opts)]}


SPIN_TEMPLATES = {
    "s": [
        "Чтобы точно попасть в цель, подскажите, где будете использовать {focus} и на что делаете упор?",
        "Расскажите, какие модели нравились раньше и что хотите сохранить в {focus}?",
    ],
    "p": [
        "Что хотелось бы улучшить по сравнению с тем, что есть сейчас?",
        "Каких возможностей не хватает текущему решению — удобства, дизайна или сервиса?",
    ],
    "i": [
        "Если подобрать подходящую модель, какой результат почувствуете первым делом?",
        "Чем быстрее закроем вопрос с {focus}, что это даст вашей команде или дому?",
    ],
    "n": [
        "По каким двум критериям поймёте, что решение идеально подошло?",
        "Что должно случиться, чтобы вы сказали: «берём»?",
    ],
}

BANT_TEMPLATES = {
    "budget": [
        "В какой диапазон по {currency} хотите уложиться, чтобы я показал точные варианты?",
        "",
    ],
    "authority": [
        "Кого ещё стоит подключить к обсуждению, чтобы быстро согласовать заказ?",
        "Принимаете решение сами или подключим коллегу для финального слова?",
    ],
    "need": [
        "Что должно быть обязательно — тишина, дизайн, дополнительные опции?",
        "Какой ключевой результат хотите получить после установки?",
    ],
    "timeline": [
        "К какому сроку хотелось бы запустить поставку — на этой неделе, в течение месяца?",
        "Когда удобно получить решение, чтобы вписаться в ваши планы?",
    ],
}

PROBLEM_KEYWORDS = ("проблем", "сложн", "не устраивает", "жалоб", "минус", "трудно", "болит")
IMPLICATION_KEYWORDS = ("теря", "штраф", "простой", "срыв", "дороже", "рискуем", "потер")
NEED_PAYOFF_KEYWORDS = ("хочу", "нужно", "важно", "интересует", "ищу", "готов")
POSITIVE_KEYWORDS = ("давайте", "беру", "подходит", "соглас", "старт", "нравится")
NEGATIVE_KEYWORDS = ("дорого", "позже", "не сейчас", "сомнева", "подум", "не готов")
AUTHORITY_KEYWORDS = ("я решаю", "сам", "директор", "руковод", "владел", "согласую")

SENTIMENT_POSITIVE_HINTS = (
    "спасибо",
    "класс",
    "отлично",
    "супер",
    "идеально",
    "🔥",
    "😍",
)
SENTIMENT_NEGATIVE_HINTS = (
    "не нравится",
    "разочаров",
    "расстро",
    "недоволен",
    "плохо",
    "ужас",
    "проблема",
    "печаль",
    "😔",
    "😢",
)

EMPATHY_NEGATIVE_TEMPLATES = (
    "Понимаю, что ситуация неприятная — сосредотачиваюсь на надежных решениях для {focus}.",
    "Сожалею, что предыдущий опыт подвёл — подберу спокойные варианты по {focus}.",
)
EMPATHY_POSITIVE_TEMPLATES = (
    "Здорово, что вам откликается идея с {focus} — ускорю подбор.",
    "Классно слышать ваш энтузиазм по {focus}, покажу топовые позиции сразу.",
)


def analyze_sentiment_delta(text: str) -> float:
    raw = text or ""
    if not raw.strip():
        return 0.0

    lowered = raw.lower()
    score = 0.0

    for word in POSITIVE_KEYWORDS:
        if word in lowered:
            score += 0.7
    for hint in SENTIMENT_POSITIVE_HINTS:
        if hint in lowered or hint in raw:
            score += 0.6

    for word in NEGATIVE_KEYWORDS:
        if word in lowered:
            score -= 0.8
    for hint in SENTIMENT_NEGATIVE_HINTS:
        if hint in lowered or hint in raw:
            score -= 0.7

    exclamation_bonus = raw.count("!") * 0.1
    score += min(exclamation_bonus, 0.3)

    question_penalty = raw.count("?") * 0.05
    score -= min(question_penalty, 0.2)

    return max(-3.0, min(3.0, score))


TIMELINE_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"сегодня|сейчас|в ближайшие сутки", re.IGNORECASE), "сегодня"),
    (re.compile(r"завтра", re.IGNORECASE), "завтра"),
    (re.compile(r"(на|в течение) этой недели", re.IGNORECASE), "на этой неделе"),
    (re.compile(r"следующ(ий|ая) (недел|месяц)", re.IGNORECASE), "в следующем периоде"),
    (re.compile(r"до\s+(конца|\d{1,2})", re.IGNORECASE), "до указанного срока"),
]

SOCIAL_PROOF_TEMPLATES = [
    "{brand} оформил десятки заказов по этой категории в {city} — клиенты отмечают стабильные сроки и качество.",
    "9 из 10 покупателей в {city} возвращаются за повторными заказами — поделюсь отзывами по запросу.",
    "Эти модели чаще берут команды из {city}, когда нужна надёжность без переплаты.",
]

SCARCITY_TEMPLATES = [
    "На складе сейчас {stock} комплектов в нужной комплектации. Держу резерв на сутки, если подтверждаем.",
    "Ближайшая поставка — {slot}. Зафиксирую слот, чтобы не потерять очередь.",
    "Популярные модели быстро разбирают. Могу забрать под вас остаток на 24 часа.",
]

RECIPROCITY_TEMPLATES = [
    "Вышлю чек-лист по установке и подготовлю бонус на дополнительные аксессуары.",
    "Поделюсь шаблоном коммерческого предложения и памяткой по монтажу, чтобы пройти путь без лишних шагов.",
]

UPSELL_TEMPLATES = [
    "При желании добавим комплект фурнитуры и сервис — получите решение под ключ.",
    "Могу предложить расширенную гарантию и послепродажную поддержку, чтобы не думать о сервисе.",
]

CHALLENGER_PLAYBOOK = {
    "default": [
        {
            "teach": "Собрал короткий шорт-лист по {focus}: только позиции с лучшими отзывами и наличием.",
            "tailor": "Смотрю на реальные сроки для {city}, чтобы запуститься без задержек.",
            "control": "Готов отправить финальные цены и бонусы. Подойдёт, если сразу перейдём к оформлению?",
        },
        {
            "teach": "По этой категории клиенты в {city} чаще выбирают модели, где сочетаются дизайн и шумоизоляция.",
            "tailor": "Я оставил варианты, которые точно впишутся в ваш запрос и бюджет.",
            "control": "Если такой формат ок, резервирую условия и собираю документы. Продолжаем?",
        },
        {
            "teach": "Отслеживаю наличие по {focus} ежедневно — сейчас есть позиции, которые можно отгрузить сразу.",
            "tailor": "В подборку попали решения с проверенной логистикой и поддержкой.",
            "control": "Готов закрепить цену и отправить договор. Подтвердите — сделаю резерв.",
        },
    ],
}


class SalesConversationEngine:
    def __init__(
        self,
        state: SalesState,
        branding: Dict[str, str],
        tenant_cfg: Dict[str, Any],
        channel_name: str,
        persona_hints: Optional[PersonaHints] = None,
    ) -> None:
        self.state = state
        self.branding = branding
        self.cfg = tenant_cfg if isinstance(tenant_cfg, dict) else {}
        self.channel_name = channel_name.strip() or branding.get("CHANNEL", "WhatsApp")
        self.persona_hints = persona_hints or PersonaHints()

    # ------------------------ анализ входящего текста --------------------
    def observe_user(self, text: str) -> None:
        incoming = (text or "").strip()
        if not incoming:
            return
        if incoming == self.state.last_user_text:
            return
        self.state.last_user_text = incoming
        self.state.append_history("user", incoming)
        self.state.last_updated_ts = time.time()
        self.state.user_message_count += 1
        if self.state.last_question_text:
            self.state.last_question_text = self.state.last_question_text.strip()

        self._touch_profile()

        needs = infer_user_needs(incoming)
        if needs:
            for key, value in needs.items():
                if value:
                    self.state.needs[key] = value
        self._update_spin(incoming)
        self._update_bant(incoming)
        self._update_conversion_score(incoming)
        self._update_sentiment(incoming)
        self._update_profile_preferences()

    def _update_conversion_score(self, text: str) -> None:
        score = self.state.conversion_score * 0.9  # лёгкое затухание — RL-подход
        low = text.lower()
        if any(word in low for word in POSITIVE_KEYWORDS):
            score += 0.8
        if any(word in low for word in NEGATIVE_KEYWORDS):
            score -= 0.9
        score = max(-3.0, min(5.0, score))
        self.state.conversion_score = score

    def _update_spin(self, text: str) -> None:
        low = text.lower()
        if self.state.needs and self.state.spin.get("s") != "covered":
            self.state.mark_spin_stage("s", "covered")
        if any(word in low for word in PROBLEM_KEYWORDS):
            self.state.mark_spin_stage("p", "covered")
        if any(word in low for word in IMPLICATION_KEYWORDS):
            self.state.mark_spin_stage("i", "covered")
        if any(word in low for word in NEED_PAYOFF_KEYWORDS):
            self.state.mark_spin_stage("n", "covered")

    def _update_bant(self, text: str) -> None:
        budget = self._extract_budget(text)
        if budget:
            self.state.bant["budget"] = budget
            self.state.bant.pop("_asked_budget", None)
            self.state.conversion_score += 0.2

        if any(key in text.lower() for key in AUTHORITY_KEYWORDS):
            self.state.bant["authority"] = "decision_maker"
            self.state.bant.pop("_asked_authority", None)

        if any(word in text.lower() for word in NEED_PAYOFF_KEYWORDS):
            self.state.bant["need"] = True
            self.state.bant.pop("_asked_need", None)

        timeline = self._extract_timeline(text)
        if timeline:
            self.state.bant["timeline"] = timeline
            self.state.bant.pop("_asked_timeline", None)

    def _touch_profile(self) -> None:
        if not isinstance(self.state.profile, dict):
            self.state.profile = {}
        profile = self.state.profile
        now_ts = time.time()
        profile["last_seen_ts"] = now_ts
        day_bucket = int(now_ts // 86400)
        last_bucket = int(profile.get("last_visit_day", -1) or -1)
        if last_bucket != day_bucket:
            profile["visits"] = int(profile.get("visits", 0)) + 1
            profile["last_visit_day"] = day_bucket

        channels = profile.setdefault("channels", [])
        if self.channel_name and self.channel_name not in channels:
            channels.append(self.channel_name)
            if len(channels) > 6:
                profile["channels"] = channels[-6:]

    def _update_sentiment(self, text: str) -> None:
        delta = analyze_sentiment_delta(text)
        if delta == 0.0:
            self.state.sentiment_score *= 0.85
            return
        blended = self.state.sentiment_score * 0.6 + delta
        self.state.sentiment_score = max(-3.0, min(3.0, blended))

    def _update_profile_preferences(self) -> None:
        profile = self.state.profile
        if not isinstance(profile, dict):
            profile = {}
            self.state.profile = profile
        prefs = profile.setdefault("preferences", {})

        color = self.state.needs.get("color")
        if color:
            colors = prefs.setdefault("colors", [])
            if color not in colors:
                colors.append(color)
                if len(colors) > 5:
                    prefs["colors"] = colors[-5:]

        keywords = self.state.needs.get("keywords") or []
        if keywords:
            pref_keywords = prefs.setdefault("keywords", [])
            for kw in keywords[:4]:
                if kw not in pref_keywords:
                    pref_keywords.append(kw)
            if len(pref_keywords) > 10:
                prefs["keywords"] = pref_keywords[-10:]

        budget = self.state.needs.get("budget_max")
        if budget:
            try:
                budget_val = int(budget)
            except Exception:
                budget_val = None
            if budget_val:
                prev = prefs.get("budget_max")
                if not prev:
                    prefs["budget_max"] = budget_val
                else:
                    try:
                        prev_val = int(prev)
                    except Exception:
                        prefs["budget_max"] = budget_val
                    else:
                        prefs["budget_max"] = min(prev_val, budget_val)

    @staticmethod
    def _extract_budget(text: str) -> Optional[int]:
        raw = text.lower()
        matches = re.finditer(r"(\d+[\s\d]*)\s*(k|тыс|тысяч)?", raw)
        best: Optional[int] = None
        for m in matches:
            digits = m.group(1).replace(" ", "")
            try:
                val = int(digits)
            except Exception:
                continue
            if m.group(2):
                val *= 1000
            if val < 100:  # вероятно указали тысячи
                continue
            if not best or val > best:
                best = val
        return best

    @staticmethod
    def _extract_timeline(text: str) -> Optional[str]:
        for pattern, label in TIMELINE_PATTERNS:
            if pattern.search(text):
                return label
        return None

    # ------------------------ формирование предложения -------------------
    def register_recommendations(self, items: List[Dict[str, Any]]) -> None:
        if items:
            self.state.last_items = items[:]

    def _explain_mode_enabled(self) -> bool:
        behavior_cfg: Mapping[str, Any] | dict = {}
        if isinstance(self.cfg, dict):
            raw_behavior = self.cfg.get("behavior")
            if isinstance(raw_behavior, Mapping):
                behavior_cfg = raw_behavior
        explain_value = behavior_cfg.get("explain") if behavior_cfg else False
        return _coerce_bool(explain_value, False) or _env_bool("EXPLAIN_MODE", False)

    def _focus_phrase(self) -> str:
        focus = str(self.state.needs.get("focus") or "").strip()
        if focus:
            return focus
        tokens: List[str] = []
        if self.state.needs.get("type"):
            tokens.append(str(self.state.needs["type"]).strip())
        if self.state.needs.get("width"):
            tokens.append(f"{self.state.needs['width']} см")
        if self.state.needs.get("color"):
            tokens.append(f"цвет {self.state.needs['color']}")
        if self.state.needs.get("keywords"):
            tokens.extend(str(x) for x in self.state.needs["keywords"][:2])
        focus_line = " ".join(token for token in tokens if token).strip()
        return focus_line or "вашей задаче"

    def _active_listening_line(self, text: str) -> str:
        cleaned = re.sub(r"\s+", " ", (text or "").strip())
        if len(cleaned) > 120:
            cleaned = cleaned[:117] + "..."
        focus = self._focus_phrase()
        empathy = self._empathy_prefix()
        explain_mode = self._explain_mode_enabled()
        if cleaned:
            base = f"Понял запрос: {cleaned}. Держу в фокусе {focus}." if explain_mode else ""
        else:
            base = f"Учитываю частые запросы по {focus} и сразу показываю сильные позиции."
        if empathy:
            return f"{empathy} {base}".strip()
        return base

    def _empathy_prefix(self) -> str:
        score = self.state.sentiment_score
        focus = self._focus_phrase()
        if score <= -0.5 and EMPATHY_NEGATIVE_TEMPLATES:
            idx = max(0, (self.state.user_message_count - 1)) % len(EMPATHY_NEGATIVE_TEMPLATES)
            return EMPATHY_NEGATIVE_TEMPLATES[idx].format(focus=focus)
        if score >= 1.2 and EMPATHY_POSITIVE_TEMPLATES:
            idx = max(0, (self.state.user_message_count - 1)) % len(EMPATHY_POSITIVE_TEMPLATES)
            return EMPATHY_POSITIVE_TEMPLATES[idx].format(focus=focus)
        visits = int((self.state.profile or {}).get("visits", 0))
        if visits > 1:
            return ""
        return ""

    def _format_price(self, price: Optional[int], currency: str) -> str:
        if price is None:
            return ""
        return f"{price:,}".replace(",", " ") + f" {currency}"

    def _price_from_item(self, item: Dict[str, Any]) -> Optional[int]:
        raw = str(item.get("price") or "").strip()
        digits = re.sub(r"\D", "", raw)
        if not digits:
            return None
        try:
            return int(digits)
        except Exception:
            return None

    def _fab_line(self, item: Dict[str, Any], idx: int, currency: str) -> str:
        title = (
            item.get("title")
            or item.get("name")
            or item.get("sku")
            or item.get("id")
            or f"Позиция {idx}"
        )
        price_val = self._price_from_item(item)
        price_text = self._format_price(price_val, currency)

        highlight_bits = []
        if item.get("brand"):
            highlight_bits.append(f"бренд {item['brand']}")
        if item.get("material"):
            highlight_bits.append(f"материал {item['material']}")
        if item.get("color"):
            highlight_bits.append(f"цвет {item['color']}")
        if item.get("width"):
            highlight_bits.append(f"ширина {item['width']} см")

        stock_note: List[str] = []
        stock = item.get("stock")
        try:
            stock_val = int(str(stock)) if stock is not None and str(stock).strip() else None
        except Exception:
            stock_val = None
        if stock_val is not None and stock_val > 0:
            stock_note.append("в наличии, отправим без ожидания")
        if item.get("tags") and "хит" in str(item.get("tags")).lower():
            stock_note.append("хит продаж")

        benefit = self._benefit_hint(item)

        line = f"{idx}. {title} — {price_text}"
        if highlight_bits:
            line += f"; {', '.join(highlight_bits)}"
        if stock_note:
            line += f"; {', '.join(stock_note)}"
        line += f". {benefit}"

        url = (item.get("url") or "").strip()
        if url:
            line += f" [{url}]"
        return line

    def _benefit_hint(self, item: Dict[str, Any]) -> str:
        needs = self.state.needs
        if needs.get("budget_max"):
            return ""
        keywords = needs.get("keywords") or []
        if keywords:
            return (
                f"Помогает с {keywords[0]} и экономит время на выборе."
                if keywords
                else "Подходит под ваш запрос."
            )
        focus = needs.get("focus") or needs.get("type")
        if focus:
            return str(focus or "").strip()
        return ""

    def _fab_block(self, items: List[Dict[str, Any]], currency: str) -> str:
        if not items:
            return (
                "1. Базовый вариант — подберу после пары уточнений "
                "(параметры, бюджет, сроки). Укладывается в целевой бюджет."
            )
        lines = [self._fab_line(item, idx, currency) for idx, item in enumerate(items, start=1)]
        return "\n".join(lines)

    def _next_spin_question(self) -> Optional[str]:
        focus = self._focus_phrase()
        for stage in ("s", "p", "i", "n"):
            status = self.state.spin.get(stage, "pending")
            if status == "pending":
                template = random.choice(SPIN_TEMPLATES[stage])
                question = template.format(focus=focus)
                self.state.mark_spin_stage(stage, "asked")
                return question
        return None

    def _next_bant_question(self, currency: str) -> Optional[str]:
        order = ["budget", "need", "timeline", "authority"]
        focus = self._focus_phrase()
        for key in order:
            value = self.state.bant.get(key)
            asked_flag = self.state.bant.get(f"_asked_{key}")
            if value or asked_flag:
                continue
            template = random.choice(BANT_TEMPLATES[key])
            question = template.format(
                currency=currency, focus=focus, city=self.branding.get("CITY", "")
            )
            self.state.bant[f"_asked_{key}"] = True
            return question
        return None

    def _choose_question(self, currency: str, max_per_turn: int) -> Optional[str]:
        if max_per_turn <= 0:
            return None
        question = self._next_bant_question(currency)
        if not question:
            question = self._next_spin_question()
        if not question:
            return None
        self._remember_question(question)
        return question

    def _remember_question(self, question: str) -> None:
        _remember_question_state(self.state, question)

    def _remember_cta(self, cta_text: str) -> None:
        _remember_cta_state(self.state, cta_text)

    def pending_question(self) -> Optional[str]:
        return None

    def _challenger_block(self) -> Tuple[str, str, str]:
        key = str(self.state.needs.get("type") or "default").lower()
        options = CHALLENGER_PLAYBOOK.get(key) or CHALLENGER_PLAYBOOK["default"]
        idx = self.state.challenger_cursor % len(options)
        play = options[idx]
        self.state.challenger_cursor += 1
        focus = self._focus_phrase()
        teach = play["teach"].format(city=self.branding.get("CITY", ""), focus=focus)
        tailor = play["tailor"].format(city=self.branding.get("CITY", ""), focus=focus)
        control = play["control"].format(city=self.branding.get("CITY", ""), focus=focus)
        return teach, tailor, control

    def _choose_social_proof(self, items: List[Dict[str, Any]]) -> Optional[str]:
        template = SOCIAL_PROOF_TEMPLATES[
            self.state.social_proof_cursor % len(SOCIAL_PROOF_TEMPLATES)
        ]
        self.state.social_proof_cursor += 1
        return template.format(
            brand=self.branding.get("BRAND", "Бренд"), city=self.branding.get("CITY", "")
        )

    def _choose_scarcity(self, items: List[Dict[str, Any]]) -> Optional[str]:
        stock_values = []
        for it in items:
            stock = it.get("stock")
            try:
                val = int(str(stock))
            except Exception:
                continue
            if val >= 0:
                stock_values.append(val)
        template = SCARCITY_TEMPLATES[self.state.scarcity_cursor % len(SCARCITY_TEMPLATES)]
        self.state.scarcity_cursor += 1
        slot = "завтра"
        if self.state.bant.get("timeline"):
            slot = self.state.bant["timeline"]
        stock_text = (
            "несколько"
            if not stock_values
            else (str(min(stock_values)) if min(stock_values) > 0 else "последние")
        )
        return template.format(stock=stock_text, city=self.branding.get("CITY", ""), slot=slot)

    def _choose_reciprocity(self) -> Optional[str]:
        template = RECIPROCITY_TEMPLATES[self.state.reciprocity_cursor % len(RECIPROCITY_TEMPLATES)]
        self.state.reciprocity_cursor += 1
        return template

    def _choose_upsell(self) -> str:
        template = random.choice(UPSELL_TEMPLATES)
        integrations = self.cfg.get("integrations", {}) if isinstance(self.cfg, dict) else {}
        if integrations.get("pdf_catalog_url"):
            template += " Могу отправить PDF/Excel с полным каталогом — скажите формат."
        return template

    def _choose_cta(self, cta_primary: str, cta_fallback: str) -> str:
        if self.persona_hints.cta:
            return self.persona_hints.cta
        score = self.state.conversion_score
        timeline = self.state.bant.get("timeline")
        handoff = (
            (self.cfg.get("cta") or {}).get("handoff_wa") if isinstance(self.cfg, dict) else ""
        )
        sentiment = self.state.sentiment_score
        if sentiment <= -1.2:
            return cta_primary or cta_fallback
        if score >= 1.5 and timeline:
            return str(timeline or "").strip()
        if sentiment >= 1.2 and score >= 1.0:
            return cta_primary or cta_fallback
        if score <= -1:
            return cta_fallback or cta_primary
        if self.channel_name.lower() == "avito" and handoff:
            return handoff
        candidate = cta_primary or cta_fallback
        return candidate

    def _personalized_greeting(self) -> str:
        default_greeting = f"Здравствуйте! Меня зовут {self.branding.get('AGENT_NAME', 'Менеджер')}, {self.branding.get('BRAND', '')}."
        greeting = (self.persona_hints.greeting or default_greeting).strip()
        if not greeting:
            greeting = default_greeting
        visits = int((self.state.profile or {}).get("visits", 0))
        if visits > 1:
            addon = "Рады снова вас видеть и продолжить подбор."
            if addon not in greeting:
                if greeting.endswith((".", "!", "…")):
                    greeting = f"{greeting.rstrip('.')}. {addon}"
                else:
                    greeting = f"{greeting}. {addon}"
        return greeting

    def _loyalty_line(self) -> Optional[str]:
        profile = self.state.profile if isinstance(self.state.profile, dict) else {}
        prefs = profile.get("preferences") or {}
        pieces: List[str] = []
        colors = prefs.get("colors") or []
        if colors:
            pieces.append(f"цвету {colors[-1]}")
        keywords = prefs.get("keywords") or []
        if keywords:
            pieces.append(f"темам «{keywords[-1]}»")
        budget = prefs.get("budget_max")
        if budget:
            pieces.append(
                f"бюджету до {self._format_price(int(budget), self.branding.get('CURRENCY', '₽'))}"
            )

        if pieces:
            joined = ", ".join(pieces)
            return (
                f"Помню ваши предпочтения по {joined} — покажу то, что действительно откликается."
            )

        visits = int(profile.get("visits", 0) or 0)
        if visits > 1:
            return None
        return None

    def build_reply(
        self,
        items: List[Dict[str, Any]],
        cta_primary: str,
        cta_fallback: str,
        currency: str,
        last_user_text: str,
    ) -> str:
        self.register_recommendations(items)
        max_questions_cfg = int(
            (self.cfg.get("behavior", {}) or {}).get("max_clarifying_questions", 1)
        )
        if self.persona_hints.max_questions is not None:
            try:
                max_questions_cfg = max(0, int(self.persona_hints.max_questions))
            except Exception:
                pass
        current_turn = max(1, self.state.user_message_count)

        question_line = self._choose_question(currency, max_questions_cfg)
        greeting = self._personalized_greeting()
        loyalty_line = self._loyalty_line()
        cta_line = ""

        if current_turn <= 1:
            intro_parts = [greeting]
            if loyalty_line:
                intro_parts.append(loyalty_line)
            if question_line:
                intro_parts.append(question_line)
            reply_intro = "\n\n".join(part.strip() for part in intro_parts if part and part.strip())
            if not reply_intro:
                reply_intro = greeting or (question_line or "")
            reply_intro = reply_intro.strip()
            self.state.last_bot_reply = reply_intro
            self.state.append_history("assistant", reply_intro)
            self.state.last_updated_ts = time.time()
            return reply_intro

        teach, tailor, _ = self._challenger_block()
        listening_line = self._active_listening_line(last_user_text)
        fab_block = self._fab_block(items, currency)
        social_proof = self._choose_social_proof(items)
        scarcity = self._choose_scarcity(items)
        reciprocity = self._choose_reciprocity()
        upsell = self._choose_upsell()
        if _cta_allowed(self.state, self.channel_name):
            cta_line = self._choose_cta(cta_primary, cta_fallback).strip()
            if cta_line:
                self._remember_cta(cta_line)
        else:
            cta_line = ""
        message_parts = {
            "greeting": greeting,
            "teach": teach,
            "listening": listening_line,
            "question": question_line or "",
            "tailor": tailor,
            "loyalty": loyalty_line or "",
            "fab": fab_block,
            "social": social_proof,
            "scarcity": scarcity,
            "upsell": upsell,
            "reciprocity": reciprocity,
            "cta": cta_line or "",
            "closing": self.persona_hints.closing or "",
        }

        ordered_keys = [
            "greeting",
            "teach",
            "listening",
            "question",
            "loyalty",
            "tailor",
            "fab",
            "social",
            "scarcity",
            "upsell",
            "reciprocity",
            "cta",
            "closing",
        ]

        cleaned = [
            message_parts[key].strip()
            for key in ordered_keys
            if message_parts[key] and message_parts[key].strip()
        ]
        if self.persona_hints.wants_short():
            prioritized = [
                message_parts.get("greeting", ""),
                message_parts.get("listening", ""),
                message_parts.get("question", ""),
                message_parts.get("loyalty", ""),
                message_parts.get("fab", ""),
                message_parts.get("cta", ""),
                message_parts.get("closing", ""),
            ]
            cleaned = [part.strip() for part in prioritized if part and part.strip()]

        reply = "\n\n".join(cleaned)
        self.state.last_bot_reply = reply
        self.state.append_history("assistant", reply)
        self.state.last_updated_ts = time.time()
        return reply

    def summary_for_llm(self) -> str:
        needs_summary = format_needs_for_prompt(self.state.needs)
        bant_parts = []
        for key in ("budget", "need", "timeline", "authority"):
            if key in self.state.bant and not str(key).startswith("_"):
                bant_parts.append(f"{key}={self.state.bant[key]}")
        if not bant_parts:
            bant_parts.append("недостаточно данных")
        spin_parts = [
            f"{stage.upper()}={self.state.spin.get(stage, 'pending')}"
            for stage in ("s", "p", "i", "n")
        ]
        pending_question = self.pending_question()
        summary = [
            f"Needs: {needs_summary}",
            f"BANT: {', '.join(bant_parts)}",
            f"SPIN: {', '.join(spin_parts)}",
            f"Score={round(self.state.conversion_score, 2)}",
        ]
        if pending_question:
            summary.append(f"Следующий вопрос: {pending_question}")
        return "; ".join(summary)


def _apply_persona_need_mappings(state: SalesState, tenant: int | None, text: str) -> None:
    if not text or not state or tenant in (None, 0):
        return
    try:
        persona_meta = persona_meta_config(int(tenant))
    except Exception:
        persona_meta = {}
    mappings = persona_meta.get("needs_mapping") if isinstance(persona_meta, Mapping) else None
    if not isinstance(mappings, Mapping):
        return
    lowered = text.lower()
    for need_key, options in mappings.items():
        if not isinstance(options, Mapping):
            continue
        for target_value, spec in options.items():
            if not isinstance(spec, Mapping):
                continue
            matched = False
            keywords = spec.get("keywords")
            if isinstance(keywords, str):
                keywords = [keywords]
            if isinstance(keywords, Sequence):
                for kw in keywords:
                    cleaned = str(kw or "").strip().lower()
                    if cleaned and cleaned in lowered:
                        matched = True
                        break
            if not matched:
                patterns = spec.get("regex") or spec.get("patterns")
                if isinstance(patterns, str):
                    patterns = [patterns]
                if isinstance(patterns, Sequence):
                    for pattern in patterns:
                        try:
                            if pattern and re.search(str(pattern), text, re.IGNORECASE):
                                matched = True
                                break
                        except re.error:
                            continue
            if matched:
                state.needs[str(need_key)] = target_value


def observe_user_message(
    contact_id: int,
    tenant: int | None,
    channel: str | None,
    text: str,
    tenant_cfg: Optional[dict] = None,
    branding: Optional[Dict[str, str]] = None,
    persona_hints: Optional[PersonaHints] = None,
) -> SalesState:
    cfg = tenant_cfg
    if cfg is None:
        cfg = load_tenant(tenant or 0)
    brand = branding or _branding_for_tenant(tenant, channel)
    state = load_sales_state(tenant, contact_id)
    channel_name = (channel or brand["CHANNEL"]).strip() or "WhatsApp"
    hints = persona_hints or load_persona_hints(tenant, channel_name)
    engine = SalesConversationEngine(state, brand, cfg, channel_name, persona_hints=hints)
    engine.observe_user(text or "")
    _apply_persona_need_mappings(state, tenant, text or "")
    save_sales_state(state)
    return state


def summarize_sales_state(
    contact_id: int,
    tenant: int | None,
    channel: str | None,
    tenant_cfg: Optional[dict] = None,
    branding: Optional[Dict[str, str]] = None,
) -> str:
    cfg = tenant_cfg if tenant_cfg is not None else load_tenant(tenant or 0)
    brand = branding or _branding_for_tenant(tenant, channel)
    state = load_sales_state(tenant, contact_id)
    channel_name = (channel or brand["CHANNEL"]).strip() or "WhatsApp"
    hints = load_persona_hints(tenant, channel_name)
    engine = SalesConversationEngine(state, brand, cfg, channel_name, persona_hints=hints)
    return engine.summary_for_llm()


def record_bot_reply(
    contact_id: int,
    tenant: int | None,
    channel: str | None,
    reply: str,
    tenant_cfg: Optional[dict] = None,
    branding: Optional[Dict[str, str]] = None,
) -> None:
    cfg = tenant_cfg if tenant_cfg is not None else load_tenant(tenant or 0)
    brand = branding or _branding_for_tenant(tenant, channel)
    state = load_sales_state(tenant, contact_id)
    channel_name = (channel or brand["CHANNEL"]).strip() or "WhatsApp"
    hints = load_persona_hints(tenant, channel_name)
    SalesConversationEngine(state, brand, cfg, channel_name, persona_hints=hints)
    if reply:
        state.last_bot_reply = reply.strip()
        state.append_history("assistant", reply.strip())
        state.last_updated_ts = time.time()
    save_sales_state(state)


def make_rule_based_reply(
    last_user_text: str,
    channel: str | None,
    contact_id: int,
    tenant: int | None = None,
) -> str:
    branding = _branding_for_tenant(tenant, channel)
    channel_name = (channel or branding["CHANNEL"]).strip() or "WhatsApp"
    channel_name = (channel or branding["CHANNEL"]).strip() or "WhatsApp"

    cfg = json.loads(json.dumps(DEFAULT_TENANT_JSON, ensure_ascii=False))
    if tenant is not None:
        try:
            cfg = load_tenant(tenant)
        except Exception:
            cfg = json.loads(json.dumps(DEFAULT_TENANT_JSON, ensure_ascii=False))

    persona_hints = load_persona_hints(tenant, channel_name)
    state = load_sales_state(tenant, contact_id)
    engine = SalesConversationEngine(
        state, branding, cfg, channel_name, persona_hints=persona_hints
    )
    engine.observe_user(last_user_text or "")

    needs = state.needs if state.needs else infer_user_needs(last_user_text or "")
    currency = branding["CURRENCY"]
    items = search_catalog(needs, limit=4, tenant=tenant, query=last_user_text)

    reply = engine.build_reply(items, "", "", currency, last_user_text or "")
    save_sales_state(state)
    return reply


# ----------------------- интерфейс для main.py -------------------------------
async def build_llm_messages(
    contact_id: int,
    last_user_text: str,
    channel: str | None = None,
    tenant: int | None = None,
):
    """Build minimal prompt: persona + recent dialogue + optional catalog context."""
    persona = load_persona(tenant, channel)
    persona_hints = extract_persona_hints(persona)
    fingerprint = hashlib.sha1(persona.encode("utf-8")).hexdigest() if persona else ""
    cache_key = _persona_hints_cache_key(tenant, channel)
    _PERSONA_HINTS_CACHE[cache_key] = (fingerprint, persona_hints)
    branding = _branding_for_tenant(tenant, channel)
    channel_name = (channel or branding["CHANNEL"]).strip() or "WhatsApp"
    user_text = (last_user_text or "").strip()

    state = load_sales_state(tenant, contact_id)
    if user_text and user_text != (state.last_user_text or "").strip():
        if state.pending_fact_key:
            _capture_pending_fact_answer(state, user_text)
        city_hint = _extract_city_hint(user_text, allow_standalone=False)
        if not city_hint:
            city_hint = _extract_standalone_city_hint(user_text)
        if city_hint and _is_plausible_city_text(city_hint):
            if not isinstance(state.facts, dict):
                state.facts = {}
            current_city = str(state.facts.get("city") or "").strip()
            if not current_city or not _is_plausible_city_text(current_city):
                city_value = _safe_short_text(city_hint, limit=80)
                state.facts["city"] = city_value
                state.known_slots["city"] = city_value
            if _canonical_fact_key(state.pending_fact_key) == "city":
                state.pending_fact_key = ""
        state.last_user_text = user_text
        state.append_history("user", user_text)
        state.last_updated_ts = time.time()
        state.user_message_count += 1
        save_sales_state(state)

    def _looks_like_greeting(text: str) -> bool:
        low = (text or "").strip().lower()
        if not low:
            return False
        return any(token in low for token in ("привет", "здрав", "салам", "добрый", "hello", "hi"))

    system_blocks: list[str] = []
    if persona.strip():
        system_blocks.append(persona.strip())
    if state.known_slots:
        system_blocks.append(
            "Уже известные данные клиента (не переспрашивай их без причины):\n"
            + json.dumps(state.known_slots, ensure_ascii=False)
        )
    known_facts = _state_facts_snapshot(state)
    if known_facts:
        system_blocks.append(
            "Подтверждённые факты клиента (не выдумывай новые и не противоречь):\n"
            + json.dumps(known_facts, ensure_ascii=False)
        )
    try:
        summary_line = summarize_sales_state(contact_id, tenant, channel_name)
    except Exception:
        summary_line = ""
    if summary_line:
        system_blocks.append(f"Сводка состояния диалога:\n{summary_line}")
    if state.pending_slot:
        system_blocks.append(
            f"Последний уточняющий вопрос уже был по слоту '{_normalize_slot_name(state.pending_slot)}'. "
            "Если клиент ответил, переходи к следующему шагу и не повторяй этот вопрос."
        )
    system_blocks.append(
        "Правила ответа:\n"
        "- Следуй персоне буквально.\n"
        "- Пиши естественно и коротко.\n"
        "- Не используй «Привет» как рабочее приветствие.\n"
        "- Избегай служебных штампов («Спасибо, понял», «Ваш запрос принят»).\n"
        "- Не начинай ответ с повтора сущности клиента (город/район/модель/имя) + подтверждение.\n"
        "- После выбора клиента не используй оценочные клише. Вместо этого сразу давай конкретный следующий шаг.\n"
        "- Не начинай ответ с повтора города/района в формате «Уфа, понял».\n"
        "- Не начинай ответ с «Понял», «Поняла», «Спасибо, что уточнили».\n"
        "- Сначала отвечай на текущий вопрос клиента, потом задавай следующий уместный шаг.\n"
        "- Не закрывай диалог пустой фразой, всегда давай следующий полезный шаг.\n"
        "- Если факт/срок не подтвержден, честно скажи, что уточняешь."
    )
    system_blocks.append(f"Канал: {channel_name}")
    system_blocks.append(f"Идентификатор контакта: {contact_id}")

    if tenant is not None and user_text and not _looks_like_greeting(user_text):
        try:
            needs_snapshot = infer_user_needs(user_text)
            context_items = search_catalog(
                needs_snapshot,
                limit=6,
                tenant=tenant,
                query=user_text,
            )
        except Exception:
            context_items = []
        if context_items:
            catalog_block = format_items_for_prompt(context_items, branding["CURRENCY"])
            system_blocks.append("Релевантные позиции каталога:\n" f"{catalog_block}")
    if tenant is not None:
        try:
            catalog_preview = read_all_catalog(tenant=tenant)[:5]
        except Exception:
            catalog_preview = []
        if catalog_preview:
            preview_block = format_items_for_prompt(
                [dict(item) for item in list(catalog_preview or [])[:5]],
                branding["CURRENCY"],
            )
            system_blocks.append(
                "Работайте только в рамках каталога этого тенанта. "
                "Не переходите в другие товарные категории и не придумывайте ассортимент вне каталога."
            )
            system_blocks.append("Примеры позиций из каталога:\n" f"{preview_block}")

    history_tail = [
        item
        for item in (state.history[-12:] if state.history else [])
        if item.get("role") in {"user", "assistant"} and str(item.get("content") or "").strip()
    ]

    sys = "\n\n".join(block for block in system_blocks if block)
    messages: List[Dict[str, str]] = [{"role": "system", "content": sys}]

    if history_tail:
        trimmed = history_tail[:-1] if history_tail[-1].get("role") == "user" else history_tail
        for msg in trimmed:
            messages.append({"role": msg["role"], "content": str(msg["content"])})

    messages.append({"role": "user", "content": user_text})
    return messages


class LLMReply(str):
    """String wrapper that carries planner/enforcement diagnostics for logging."""

    __slots__ = ("llm_plan", "llm_raw_answer")

    def __new__(
        cls,
        content: str,
        *,
        plan: Optional[Dict[str, Any]] = None,
        raw_answer: Optional[str] = None,
    ) -> "LLMReply":
        obj = str.__new__(cls, content)
        obj.llm_plan = plan
        obj.llm_raw_answer = raw_answer
        return obj


def _wrap_llm_reply(
    text: str,
    *,
    plan: Optional[planner.GeneratedPlan | Dict[str, Any]] = None,
    raw_answer: Optional[str] = None,
) -> LLMReply:
    plan_payload: Optional[Dict[str, Any]] = None
    if plan is not None:
        if isinstance(plan, dict):
            plan_payload = dict(plan)
        elif hasattr(plan, "to_dict"):
            try:
                plan_payload = plan.to_dict()  # type: ignore[attr-defined]
            except Exception:
                plan_payload = None
    return LLMReply(
        text, plan=plan_payload, raw_answer=raw_answer if raw_answer is not None else text
    )


async def _direct_llm_reply(
    client: Any,
    messages: List[Dict[str, str]],
    persona_hints: PersonaHints | None,
    state: SalesState,
    channel_name: str,
    contact_ref: int,
    tenant: int | None,
    last_user_message: str,
) -> str:
    try:
        create_fn = _resolve_chat_completion_callable(client)
        if not create_fn:
            raise RuntimeError("openai client missing chat.completions.create")
        grounding = _build_reply_grounding(
            tenant=tenant,
            state=state,
            user_text=last_user_message,
        )

        variants: List[str] = []
        for _ in range(1):
            resp = await _llm_call_with_deadline(
                create_fn,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                model=settings.OPENAI_MODEL,
                messages=messages,
                max_tokens=100,
                temperature=settings.OPENAI_TEMPERATURE,
                top_p=0.9,
                frequency_penalty=0.2,
                presence_penalty=0.05,
                timeout=settings.OPENAI_TIMEOUT_SECONDS,
            )
            choices = getattr(resp, "choices", None)
            if isinstance(choices, list):
                for choice in choices:
                    message = getattr(choice, "message", None)
                    content = getattr(message, "content", "") if message is not None else ""
                    text = str(content or "").strip()
                    if text:
                        variants.append(text)
                        break
        if not variants:
            answer = ""
        elif len(variants) == 1:
            answer = variants[0]
        else:
            answer = "\n\n".join(
                [f"Вариант {idx}: {text}" for idx, text in enumerate(variants, start=1)]
            )
        answer = _enforce_catalog_truth_guard(
            answer,
            grounding=grounding,
            user_text=last_user_message,
        )
        dummy_plan = planner.GeneratedPlan()
        enforcement_ctx = _make_enforcement_context(state, persona_hints, channel_name)
        existing_fp = set(enforcement_ctx.asked_fingerprints)
        final_answer = str(answer or "").strip()
        _apply_plan_alignment_to_state(state, enforcement_ctx, existing_fp)
        _update_fact_memory(state, final_answer)
        _remember_questions_from_reply(state, final_answer)
        save_sales_state(state)
        result = _wrap_llm_reply(final_answer, plan=dummy_plan, raw_answer=answer)
        record_bot_reply(contact_ref, tenant, channel_name, str(result))
        return result
    except APITimeoutError as exc:
        logger.warning("direct llm timeout: %s", exc)
    except Exception as exc:
        if _is_quota_or_rate_limit_error(exc):
            logger.warning("direct llm quota/rate limited, fallback enabled")
        else:
            logger.exception("direct llm call failed", exc_info=exc)

    fallback = _llm_unavailable_reply(
        user_text=last_user_message,
        grounding=grounding,
    )
    return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)


def _human_reply_mode_enabled(tenant: int | None, cfg: Mapping[str, Any] | None = None) -> bool:
    if _env_bool("HUMAN_REPLY_MODE", False):
        return True
    if tenant is None:
        return False
    cfg_map: Mapping[str, Any] | None = cfg
    if cfg_map is None:
        try:
            cfg_map = load_tenant(int(tenant))
        except Exception:
            cfg_map = None
    if not isinstance(cfg_map, Mapping):
        return False
    behavior = cfg_map.get("behavior")
    if not isinstance(behavior, Mapping):
        return False
    return _coerce_bool(behavior.get("human_reply_mode"), False)


def _resolve_brain_mode(tenant: int | None, cfg: Mapping[str, Any] | None = None) -> str:
    if _env_bool("HUMAN_REPLY_MODE", False):
        return "classic"
    cfg_map: Mapping[str, Any] | None = cfg
    if cfg_map is None and tenant is not None:
        try:
            cfg_map = load_tenant(int(tenant))
        except Exception:
            cfg_map = None
    if not isinstance(cfg_map, Mapping):
        return "classic"
    behavior = cfg_map.get("behavior")
    if not isinstance(behavior, Mapping):
        return "classic"
    raw_mode = str(behavior.get("brain_mode") or "").strip().lower()
    if raw_mode == "smart":
        return "smart"
    if raw_mode in {"classic", "prod", "legacy"}:
        return "classic"
    if _coerce_bool(behavior.get("human_reply_mode"), False):
        return "classic"
    return "classic"


def _build_human_mode_messages(messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
    prepared: List[Dict[str, str]] = []
    system_chunks: List[str] = []
    for item in messages:
        role = str(item.get("role") or "").strip().lower()
        content = str(item.get("content") or "").strip()
        if role == "system" and content:
            system_chunks.append(content)
        elif role in {"user", "assistant"} and content:
            prepared.append({"role": role, "content": content})
    style_guard = (
        "Следуй персоне буквально. "
        "Пиши живо и по-человечески, без канцелярита. "
        "Не начинай ответ с 'Понял', 'Поняла', 'Спасибо, что уточнили'. "
        "Сообщение: 1-3 коротких предложения, максимум 1 вопрос."
    )
    merged_system = "\n\n".join(chunk for chunk in system_chunks if chunk)
    if merged_system:
        merged_system = f"{merged_system}\n\nПравила стиля:\n{style_guard}"
    else:
        merged_system = style_guard
    out: List[Dict[str, str]] = [{"role": "system", "content": merged_system}]
    out.extend(prepared[-10:])
    return out


async def _human_llm_reply(
    client: Any,
    messages: List[Dict[str, str]],
    persona_hints: PersonaHints | None,
    state: SalesState,
    channel_name: str,
    contact_ref: int,
    tenant: int | None,
    last_user_message: str,
) -> str:
    try:
        create_fn = _resolve_chat_completion_callable(client)
        if not create_fn:
            raise RuntimeError("openai client missing chat.completions.create")
        human_messages = _build_human_mode_messages(messages)
        grounding = _build_reply_grounding(
            tenant=tenant,
            state=state,
            user_text=last_user_message,
        )
        plan, answer = await planner.generate_sales_reply(
            human_messages,
            openai_module=client,
            model=settings.OPENAI_MODEL,
            timeout=settings.OPENAI_TIMEOUT_SECONDS,
            persona_language=(persona_hints.language if persona_hints else None),
        )
        answer = str(answer or "").strip()
        if not answer:
            raise RuntimeError("empty human llm answer")
        answer = _enforce_catalog_price_grounding(answer, grounding=grounding)
        answer = _enforce_catalog_truth_guard(
            answer,
            grounding=grounding,
            user_text=last_user_message,
        )
        final_answer = str(answer or "").strip()
        _update_fact_memory(state, final_answer)
        _remember_questions_from_reply(state, final_answer)
        save_sales_state(state)
        result = _wrap_llm_reply(final_answer, plan=plan.to_dict(), raw_answer=answer)
        record_bot_reply(contact_ref, tenant, channel_name, str(result))
        return result
    except APITimeoutError as exc:
        logger.warning("human llm timeout: %s", exc)
    except Exception as exc:
        if _is_quota_or_rate_limit_error(exc):
            logger.warning("human llm quota/rate limited, fallback enabled")
        else:
            logger.exception("human llm failed", exc_info=exc)
    fallback = _llm_unavailable_reply(
        user_text=last_user_message,
        grounding=grounding,
    )
    return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)


async def _single_llm_reply(
    client: Any,
    messages: List[Dict[str, str]],
    persona_hints: PersonaHints | None,
    state: SalesState,
    channel_name: str,
    contact_ref: int,
    tenant: int | None,
    last_user_message: str,
) -> str:
    def _default_policy() -> Dict[str, Any]:
        return {
            "action": "respond",
            "intent": "general",
            "intent_tags": [],
            "respond_to_user_question_first": True,
            "continue_flow": True,
            "question_strategy": {
                "should_ask": False,
                "question_goal": "",
                "question_fact_key": "",
            },
            "claims": [],
            "fact_updates": [],
            "selected_item_ref": "",
            "reply_plan": {
                "tone": "persona",
                "brief": True,
                "ack": True,
            },
        }

    def _build_policy_grounding() -> Dict[str, Any]:
        merged: List[Dict[str, Any]] = []
        seen: set[str] = set()
        search_needs: Dict[str, Any] = {}

        def _append(items: Sequence[Mapping[str, Any]]) -> None:
            for raw in items or []:
                item = dict(raw)
                identity = _catalog_item_identity(item)
                if identity in seen:
                    continue
                seen.add(identity)
                merged.append(item)

        if state.last_items:
            _append(state.last_items)
        if isinstance(state.facts, Mapping):
            for key in ("city", "address", "object_type", "model", "budget", "timeline", "dimensions", "color"):
                value = _safe_short_text(str(state.facts.get(key) or ""), 120)
                if value:
                    search_needs[key] = value
        if isinstance(state.needs, Mapping):
            for key in ("keywords", "budget_max", "price_order", "object_type", "color", "dimensions"):
                value = state.needs.get(key)
                if value in (None, "", [], {}, ()):
                    continue
                search_needs[key] = value
        try:
            turn_needs = infer_user_needs(last_user_message)
        except Exception:
            turn_needs = {}
        if isinstance(turn_needs, Mapping):
            for key in ("keywords", "budget_max", "price_order", "object_type", "color", "dimensions"):
                value = turn_needs.get(key)
                if value in (None, "", [], {}, ()):
                    continue
                search_needs[key] = value
        if tenant is not None:
            try:
                _append(search_catalog(search_needs, limit=8, tenant=tenant, query=last_user_message))
            except Exception:
                pass
            if not merged:
                try:
                    _append(read_all_catalog(tenant=tenant)[:8])
                except Exception:
                    pass

        selected_item: Dict[str, Any] | None = None
        selected_hint = str((state.facts or {}).get("model") or "").strip()
        if selected_hint and merged:
            matched = _best_catalog_item_match(selected_hint, merged)
            if isinstance(matched, Mapping):
                selected_item = dict(matched)
        return {
            "items": [dict(item) for item in merged[:8]],
            "selected_item": dict(selected_item) if isinstance(selected_item, Mapping) else None,
        }

    def _coerce_policy(raw: Mapping[str, Any] | None) -> Dict[str, Any]:
        plan = _default_policy()
        if not isinstance(raw, Mapping):
            return plan
        plan["action"] = str(raw.get("action") or "respond").strip().lower() or "respond"
        plan["intent"] = str(raw.get("intent") or "general").strip().lower() or "general"
        raw_tags = raw.get("intent_tags")
        tags: List[str] = []
        if isinstance(raw_tags, Sequence):
            for tag in raw_tags[:8]:
                normalized = str(tag or "").strip().lower()
                if normalized and normalized not in tags:
                    tags.append(normalized)
        heuristic_tags: list[str] = []
        if _is_price_intent(last_user_message) or _looks_like_price_objection(last_user_message):
            heuristic_tags.append("price")
        if _VARIANTS_USER_HINT_RE.search(last_user_message):
            heuristic_tags.append("variants")
        if _extract_attribute_probe(last_user_message):
            heuristic_tags.append("attributes")
        turn_intent_hint = _classify_turn_intent(last_user_message, known_facts=state.facts)
        if turn_intent_hint in {"repair", "catalog_problem"}:
            heuristic_tags.append("repair")
        if re.search(r"(?iu)\b(тоже\s+сам|одно\s+и\s+то\s+же|опять\s+то\s+же|повтор)\b", last_user_message):
            heuristic_tags.append("complaint")
        for tag in heuristic_tags:
            if tag not in tags:
                tags.append(tag)
        plan["intent_tags"] = tags
        plan["respond_to_user_question_first"] = bool(raw.get("respond_to_user_question_first", True))
        plan["continue_flow"] = bool(raw.get("continue_flow", True))
        qs_raw = raw.get("question_strategy")
        if isinstance(qs_raw, Mapping):
            plan["question_strategy"] = {
                "should_ask": bool(qs_raw.get("should_ask", False)),
                "question_goal": str(qs_raw.get("question_goal") or "").strip(),
                "question_fact_key": _canonical_fact_key(str(qs_raw.get("question_fact_key") or "")) or "",
            }
        rp_raw = raw.get("reply_plan")
        if isinstance(rp_raw, Mapping):
            plan["reply_plan"] = {
                "tone": str(rp_raw.get("tone") or "persona").strip() or "persona",
                "brief": bool(rp_raw.get("brief", True)),
                "ack": bool(rp_raw.get("ack", True)),
            }
        claims: List[Dict[str, Any]] = []
        raw_claims = raw.get("claims")
        if isinstance(raw_claims, Sequence):
            for item in raw_claims[:16]:
                if not isinstance(item, Mapping):
                    continue
                claims.append(
                    {
                        "type": str(item.get("type") or "").strip().lower(),
                        "subject": str(item.get("subject") or "").strip(),
                        "attribute": str(item.get("attribute") or "").strip(),
                        "value": str(item.get("value") or "").strip(),
                        "confidence": float(item.get("confidence") or 0.0),
                    }
                )
        plan["claims"] = claims
        updates: List[Dict[str, str]] = []
        raw_updates = raw.get("fact_updates")
        if isinstance(raw_updates, Sequence):
            for item in raw_updates[:16]:
                if not isinstance(item, Mapping):
                    continue
                fact_key = _canonical_fact_key(str(item.get("fact_key") or "")) or _normalize_fact_key(
                    str(item.get("fact_key") or "")
                )
                value = _safe_short_text(str(item.get("value") or ""), 160)
                if not fact_key or not value:
                    continue
                updates.append(
                    {
                        "fact_key": fact_key,
                        "value": value,
                        "source": str(item.get("source") or "model_inferred").strip() or "model_inferred",
                    }
                )
        plan["fact_updates"] = updates
        plan["selected_item_ref"] = str(raw.get("selected_item_ref") or "").strip()
        return plan

    def _match_grounded_item(
        ref: str,
        items: Sequence[Mapping[str, Any]],
        selected_item: Mapping[str, Any] | None,
    ) -> Dict[str, Any] | None:
        value = str(ref or "").strip()
        if not value and isinstance(selected_item, Mapping):
            return dict(selected_item)
        if not value:
            return None
        value_norm = _normalize_model_alias(value)
        if not value_norm:
            return None
        for item in items:
            item_map = dict(item)
            identity = _normalize_model_alias(_catalog_item_identity(item_map))
            if identity and identity == value_norm:
                return item_map
            for alias in _item_aliases(item_map):
                alias_norm = _normalize_model_alias(alias)
                if not alias_norm:
                    continue
                if alias_norm == value_norm or alias_norm in value_norm or value_norm in alias_norm:
                    return item_map
        strict = _strict_catalog_item_match(value, items)
        if isinstance(strict, Mapping):
            return dict(strict)
        best = _best_catalog_item_match(value, items)
        if isinstance(best, Mapping):
            return dict(best)
        return None

    def _validate_policy_claims(
        policy: Dict[str, Any],
        grounding_map: Mapping[str, Any],
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        allowed_types = {
            "catalog_attribute",
            "catalog_price",
            "catalog_item_identity",
            "catalog_shortlist_offer",
            "state_ack",
            "business_rule",
            "channel_action",
            "handoff_action",
            "soft_sales_claim",
        }
        items = [dict(item) for item in _grounding_catalog_items(grounding_map)]
        selected_item = _selected_item_from_grounding(grounding_map, items)
        validated: List[Dict[str, Any]] = []
        dropped: List[str] = []

        for raw in list(policy.get("claims") or []):
            if not isinstance(raw, Mapping):
                continue
            claim_type = str(raw.get("type") or "").strip().lower()
            if claim_type not in allowed_types:
                dropped.append("unsupported_claim_type")
                continue
            subject = str(raw.get("subject") or "").strip()
            attribute = str(raw.get("attribute") or "").strip()
            value = str(raw.get("value") or "").strip()
            target = _match_grounded_item(subject, items, selected_item)

            if claim_type == "catalog_shortlist_offer":
                if not items:
                    dropped.append("shortlist_without_grounding")
                    continue
                validated.append(
                    {
                        "type": claim_type,
                        "subject": subject,
                        "attribute": "",
                        "value": value,
                        "confidence": float(raw.get("confidence") or 0.0),
                    }
                )
                continue

            if claim_type == "catalog_item_identity":
                if not target:
                    dropped.append("identity_without_match")
                    continue
                validated.append(
                    {
                        "type": claim_type,
                        "subject": _display_item_label(target) or _item_label(target),
                        "attribute": "",
                        "value": value,
                        "confidence": float(raw.get("confidence") or 0.0),
                    }
                )
                continue

            if claim_type == "catalog_price":
                if not target:
                    dropped.append("price_without_match")
                    continue
                price_value = _item_price_int(target)
                if not isinstance(price_value, int) or price_value <= 0:
                    dropped.append("price_without_numeric_value")
                    continue
                if value:
                    spans = _extract_price_spans(value)
                    claim_price = int(spans[0][2]) if spans else None
                    if claim_price is not None and claim_price != int(price_value):
                        dropped.append("price_value_mismatch")
                        continue
                validated.append(
                    {
                        "type": claim_type,
                        "subject": _display_item_label(target) or _item_label(target),
                        "attribute": "price",
                        "value": _format_rub_price(int(price_value)),
                        "confidence": float(raw.get("confidence") or 0.0),
                    }
                )
                continue

            if claim_type == "catalog_attribute":
                if not target:
                    dropped.append("attribute_without_match")
                    continue
                attr_norm = _normalize_text(attribute)
                val_norm = _normalize_text(value)
                resolved_attr = ""
                resolved_val = ""
                for k, v in dict(target).items():
                    key = str(k or "").strip()
                    val = str(v or "").strip()
                    if not key or not val:
                        continue
                    key_norm = _normalize_text(key)
                    if key_norm in {"title", "name", "id", "sku", "url", "price", "_search_text"}:
                        continue
                    attr_match = bool(attr_norm and (attr_norm in key_norm or key_norm in attr_norm))
                    val_match = bool(val_norm and (val_norm in _normalize_text(val) or _normalize_text(val) in val_norm))
                    if attr_norm and val_norm and not (attr_match and val_match):
                        continue
                    if attr_norm and not attr_match:
                        continue
                    if val_norm and not val_match:
                        continue
                    resolved_attr = key
                    resolved_val = val
                    break
                if not resolved_attr:
                    dropped.append("attribute_not_grounded")
                    continue
                validated.append(
                    {
                        "type": claim_type,
                        "subject": _display_item_label(target) or _item_label(target),
                        "attribute": resolved_attr,
                        "value": resolved_val,
                        "confidence": float(raw.get("confidence") or 0.0),
                    }
                )
                continue

            validated.append(
                {
                    "type": claim_type,
                    "subject": subject,
                    "attribute": attribute,
                    "value": value,
                    "confidence": float(raw.get("confidence") or 0.0),
                }
            )
        return validated, dropped

    def _constrain_claims_by_turn_intent(
        policy: Dict[str, Any],
        validated_claims: Sequence[Mapping[str, Any]],
    ) -> List[Dict[str, Any]]:
        tags = {
            str(tag or "").strip().lower()
            for tag in (policy.get("intent_tags") or [])
            if str(tag or "").strip()
        }
        # Catalog claims are allowed only when user intent explicitly requires
        # factual catalog grounding (price/variants/attributes/repair/selection).
        allow_catalog_claims = bool(
            {"price", "variants", "attributes", "repair", "complaint", "selection"} & tags
        )
        allowed_without_catalog = {"state_ack", "business_rule", "channel_action", "handoff_action"}
        constrained: List[Dict[str, Any]] = []
        for raw in list(validated_claims or []):
            claim = dict(raw)
            claim_type = str(claim.get("type") or "").strip().lower()
            if allow_catalog_claims:
                constrained.append(claim)
                continue
            if claim_type in allowed_without_catalog:
                constrained.append(claim)
        return constrained

    def _apply_policy_state_updates(policy: Dict[str, Any], grounding_map: Mapping[str, Any]) -> None:
        if not isinstance(state.facts, dict):
            state.facts = {}
        if not isinstance(state.known_slots, dict):
            state.known_slots = {}

        for item in list(policy.get("fact_updates") or []):
            if not isinstance(item, Mapping):
                continue
            key = _canonical_fact_key(str(item.get("fact_key") or "")) or _normalize_fact_key(
                str(item.get("fact_key") or "")
            )
            value = _safe_short_text(str(item.get("value") or ""), 160)
            if not key or not value:
                continue
            state.facts[key] = value
            if key in {"city", "address", "object_type", "model", "budget", "timeline", "contact", "quantity", "color"}:
                state.known_slots[key] = value

        selected_ref = str(policy.get("selected_item_ref") or "").strip()
        if selected_ref:
            items = [dict(item) for item in _grounding_catalog_items(grounding_map)]
            selected = _selected_item_from_grounding(grounding_map, items)
            matched = _match_grounded_item(selected_ref, items, selected)
            if isinstance(matched, Mapping):
                model_label = _display_item_label(matched) or _item_label(matched)
                if model_label:
                    state.facts["model"] = _safe_short_text(model_label, 180)
                    state.known_slots["model"] = _safe_short_text(model_label, 120)
                reorder: List[Dict[str, Any]] = [dict(matched)]
                matched_identity = _catalog_item_identity(dict(matched))
                for candidate in items:
                    identity = _catalog_item_identity(dict(candidate))
                    if identity == matched_identity:
                        continue
                    reorder.append(dict(candidate))
                state.last_items = reorder[:8]

    async def _llm_policy_decision(
        create_fn: Any,
        prepared_messages: List[Dict[str, str]],
        known_facts: Mapping[str, str],
        grounding_map: Mapping[str, Any],
        persona_context: str,
    ) -> Dict[str, Any]:
        dialogue_tail = [
            {"role": str(item.get("role") or ""), "content": str(item.get("content") or "")}
            for item in prepared_messages
            if str(item.get("role") or "").strip().lower() in {"user", "assistant"}
        ][-10:]
        persona_excerpt = str(persona_context or "").strip()
        if len(persona_excerpt) > 7000:
            persona_excerpt = persona_excerpt[:7000]
        pending_fact_key = _canonical_fact_key(str(state.pending_fact_key or "")) or ""
        grounding_preview = format_items_for_prompt(
            [dict(item) for item in _grounding_catalog_items(grounding_map)[:5]],
            "₽",
        )
        policy_system = (
            "You are a policy planner for a sales chatbot. "
            "You MUST follow persona instructions from context as hard contract. "
            "Prioritize current user intent first and never repeat already answered question. "
            "Return JSON only with schema keys: "
            "action,intent,intent_tags,respond_to_user_question_first,continue_flow,"
            "question_strategy,claims,fact_updates,selected_item_ref,reply_plan. "
            "intent_tags: array with zero or more tags from "
            "[price,variants,attributes,repair,complaint,handoff,selection]. "
            "question_strategy keys: should_ask,question_goal,question_fact_key. "
            "claims item keys: type,subject,attribute,value,confidence. "
            "fact_updates item keys: fact_key,value,source. "
            "Allowed claim types: catalog_attribute,catalog_price,catalog_item_identity,"
            "catalog_shortlist_offer,state_ack,business_rule,channel_action,handoff_action,soft_sales_claim. "
            "Keep claims empty unless they are needed to answer the current user turn. "
            "For greeting/rapport turns without product question, do not output catalog claims. "
            "Set question_strategy.should_ask=false when the user asked a direct factual question and it is answerable from grounding. "
            "Always prioritize current user turn over scripted next step."
        )
        policy_user = (
            f"Persona contract:\n{persona_excerpt or 'none'}\n\n"
            f"Known facts: {json.dumps(dict(known_facts or {}), ensure_ascii=False)}\n"
            f"Current pending_fact_key: {pending_fact_key or 'none'}\n"
            f"Last assistant reply: {str(state.last_bot_reply or '').strip() or 'none'}\n"
            f"Grounded catalog preview:\n{grounding_preview or 'none'}\n"
            f"Last user message: {last_user_message}\n"
            f"Recent dialogue: {json.dumps(dialogue_tail, ensure_ascii=False)}"
        )
        try:
            response = await _llm_call_with_deadline(
                create_fn,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                model=settings.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": policy_system},
                    {"role": "user", "content": policy_user},
                ],
                temperature=0.0,
                max_tokens=480,
                response_format={"type": "json_object"},
                timeout=settings.OPENAI_TIMEOUT_SECONDS,
            )
            choices = getattr(response, "choices", None)
            if isinstance(choices, list) and choices:
                msg = getattr(choices[0], "message", None)
                parsed = _safe_json_load(str(getattr(msg, "content", "") or ""))
                return _coerce_policy(parsed)
        except Exception:
            pass
        return _default_policy()

    async def _render_policy_reply(
        create_fn: Any,
        prepared_messages: List[Dict[str, str]],
        policy: Mapping[str, Any],
        grounding_map: Mapping[str, Any],
    ) -> str:
        grounding_preview = format_items_for_prompt(
            [dict(item) for item in _grounding_catalog_items(grounding_map)[:6]],
            "₽",
        )
        render_system = (
            "Сформируй финальный ответ клиенту. "
            "Сначала ответь на текущий смысл последней реплики, затем один уместный следующий шаг. "
            "Следуй персоне и не используй служебные фразы. "
            "Не начинай с «Понял/Поняла/Спасибо, что уточнили». "
            "Не выдумывай товары/характеристики вне grounded catalog context. "
            "Если intent_tags содержит variants или selection — покажи минимум один конкретный альтернативный вариант из каталога и назови его как вариант/модель. "
            "Если intent_tags содержит price — обязательно закрой вопрос цены явным ценовым объяснением. "
            "Если intent_tags содержит attributes — обязательно дай конкретный параметр/характеристику. "
            "Если intent_tags содержит repair или complaint — сначала обработай это и затем верни к подбору с конкретными моделями/вариантами; явно покажи, что можешь сразу предложить подходящие модели. "
            "Если в policy есть missing_coverage — исправь именно эти пробелы. "
            "1-3 коротких предложения, максимум 1 вопрос."
        )
        render_user = (
            f"Политика (JSON): {json.dumps(dict(policy or {}), ensure_ascii=False)}\n\n"
            f"Grounded catalog context:\n{grounding_preview or 'none'}\n\n"
            f"Последняя реплика клиента: {last_user_message}"
        )
        response = await _llm_call_with_deadline(
            create_fn,
            timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
            model=settings.OPENAI_MODEL,
            messages=[
                prepared_messages[0] if prepared_messages else {"role": "system", "content": ""},
                *prepared_messages[1:],
                {"role": "system", "content": render_system},
                {"role": "user", "content": render_user},
            ],
            temperature=0.1,
            top_p=0.9,
            max_tokens=220,
            timeout=settings.OPENAI_TIMEOUT_SECONDS,
        )
        choices = getattr(response, "choices", None)
        if not (isinstance(choices, list) and choices):
            return ""
        message = getattr(choices[0], "message", None)
        return str(getattr(message, "content", "") or "").strip()

    def _mentioned_catalog_ids(text: str, items: Sequence[Mapping[str, Any]]) -> set[str]:
        candidate = _normalize_text(text)
        if not candidate:
            return set()
        hits: set[str] = set()
        for raw_item in items:
            item = dict(raw_item)
            identity = _catalog_item_identity(item)
            if not identity:
                continue
            for alias in _item_aliases(item):
                alias_norm = _normalize_model_alias(alias)
                if len(alias_norm) < 4:
                    continue
                if alias_norm in candidate:
                    hits.add(identity)
                    break
        return hits

    def _missing_intent_coverage(
        text: str,
        policy: Mapping[str, Any],
        grounding_map: Mapping[str, Any],
    ) -> List[str]:
        candidate = str(text or "").strip()
        if not candidate:
            return ["empty"]
        tags = [
            str(tag or "").strip().lower()
            for tag in (policy.get("intent_tags") or [])
            if str(tag or "").strip()
        ]
        items = [dict(item) for item in _grounding_catalog_items(grounding_map)]
        missing: List[str] = []
        variant_like = ("variants" in tags) or ("selection" in tags)
        if variant_like and items:
            mentions_item = _reply_mentions_catalog_item(candidate, items)
            has_price = bool(_PRICE_INLINE_RE.search(candidate))
            if not mentions_item and not has_price:
                missing.append("variants")
            if not re.search(r"(?iu)\b(вариант|модель)\w*\b", candidate):
                missing.append("variants_wording")
            prev_ids = _mentioned_catalog_ids(str(state.last_bot_reply or ""), items)
            current_ids = _mentioned_catalog_ids(candidate, items)
            all_ids = {_catalog_item_identity(dict(item)) for item in items}
            available_new = {item_id for item_id in all_ids if item_id and item_id not in prev_ids}
            if prev_ids and current_ids and current_ids.issubset(prev_ids):
                if len(items) > len(current_ids):
                    missing.append("variants_alternative")
            if prev_ids and current_ids and available_new and (current_ids & prev_ids):
                missing.append("variants_no_repeat")
        if "price" in tags:
            has_price_signal = bool(
                _PRICE_INLINE_RE.search(candidate)
                or re.search(r"(?iu)\b(цена|диапазон|дешевле|альтернатив\w*)\b", candidate)
            )
            if not has_price_signal:
                missing.append("price")
        if "attributes" in tags:
            has_attribute_signal = bool(re.search(r"\d", candidate)) or ":" in candidate
            if not has_attribute_signal:
                missing.append("attributes")
        if ("repair" in tags or "complaint" in tags) and items:
            has_catalog_recovery = _reply_mentions_catalog_item(candidate, items) or bool(
                _PRICE_INLINE_RE.search(candidate)
            )
            if not has_catalog_recovery:
                missing.append("repair_catalog_recovery")
            has_recovery_offer = bool(
                re.search(
                    r"(?iu)\b(могу\s+сразу\s+предлож\w*|подходящ\w*\s+(модел\w*|вариант\w*))\b",
                    candidate,
                )
            )
            if not has_recovery_offer:
                missing.append("repair_offer")
        if "complaint" in tags and items:
            has_alternative_signal = bool(
                re.search(r"(?iu)\b(друг\w*\s+вариант\w*|альтернатив\w*|что\s+важнее)\b", candidate)
            )
            if not has_alternative_signal:
                missing.append("complaint_alternative")
        return missing

    try:
        create_fn = _resolve_chat_completion_callable(client)
        if not create_fn:
            raise RuntimeError("openai client missing chat.completions.create")

        prepared_messages = _build_human_mode_messages(messages)
        grounding = _build_policy_grounding()
        known_facts = _state_facts_snapshot(state)
        persona_context = ""
        if prepared_messages and str(prepared_messages[0].get("role") or "").strip().lower() == "system":
            persona_context = str(prepared_messages[0].get("content") or "")
        persona_rules_context = _resolve_persona_rules_context(
            tenant=tenant,
            channel_name=channel_name,
            fallback_context=persona_context,
        )

        policy = await _llm_policy_decision(
            create_fn,
            prepared_messages,
            known_facts,
            grounding,
            persona_rules_context,
        )
        validated_claims, dropped_claims = _validate_policy_claims(policy, grounding)
        policy["claims"] = _constrain_claims_by_turn_intent(policy, validated_claims)
        if dropped_claims:
            policy["dropped_claims"] = dropped_claims

        _apply_policy_state_updates(policy, grounding)

        answer = await _render_policy_reply(create_fn, prepared_messages, policy, grounding)
        if not answer:
            answer = await _render_direct_reply(
                create_fn,
                model=settings.OPENAI_MODEL,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                prepared_messages=prepared_messages,
            )
        if not answer:
            raise RuntimeError("empty llm render")
        missing_coverage = _missing_intent_coverage(answer, policy, grounding)
        if missing_coverage:
            retry_policy = dict(policy or {})
            retry_policy["missing_coverage"] = list(missing_coverage)
            retry_answer = await _render_policy_reply(
                create_fn,
                prepared_messages,
                retry_policy,
                grounding,
            )
            if retry_answer:
                answer = retry_answer

        answer = _strip_instruction_leaks(answer)
        final_answer = str(answer or "").strip()
        final_answer = await _audit_and_rewrite_persona_reply(
            create_fn,
            model=settings.OPENAI_MODEL,
            timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
            prepared_messages=prepared_messages,
            answer=final_answer,
            last_user_message=last_user_message,
            state=state,
            grounding=grounding,
            policy=policy,
        )
        final_answer = _ensure_dialog_greeting_on_first_reply(
            final_answer,
            state,
            persona_context=persona_rules_context,
        )
        final_answer = _apply_base_answer_quality_floor(
            final_answer,
            state=state,
            persona_hints=persona_hints,
            grounding=grounding,
            user_text=last_user_message,
        )
        if not final_answer:
            final_answer = str(answer or "").strip()

        actual_questions = _extract_questions_from_text(final_answer)
        question_strategy = policy.get("question_strategy")
        pending_key = ""
        if isinstance(question_strategy, Mapping) and bool(question_strategy.get("should_ask")) and actual_questions:
            pending_key = _canonical_fact_key(str(question_strategy.get("question_fact_key") or "")) or ""
        if not pending_key and actual_questions:
            inferred = _normalize_slot_name("", question=actual_questions[0])
            pending_key = _canonical_fact_key(inferred) or ""
        state.pending_fact_key = pending_key
        state.pending_slot = ""
        state.last_plan = dict(policy or {})
        _update_fact_memory(state, final_answer)
        _remember_questions_from_reply(state, final_answer)
        save_sales_state(state)
        result = _wrap_llm_reply(final_answer, plan=dict(policy or {}), raw_answer=answer)
        record_bot_reply(contact_ref, tenant, channel_name, str(result))
        return result
    except APITimeoutError as exc:
        logger.warning("single llm timeout: %s", exc)
    except Exception as exc:
        if _is_quota_or_rate_limit_error(exc):
            logger.warning("single llm quota/rate limited, fallback enabled")
        else:
            logger.exception("single llm failed", exc_info=exc)
    fallback = _llm_unavailable_reply(
        user_text=last_user_message,
    )
    return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)


async def ask_llm(
    messages: List[Dict[str, str]],
    tenant: int | None = None,
    contact_id: int | None = None,
    channel: str | None = None,
) -> str:
    """
    Если задан OPENAI_API_KEY — спросим модель. Если нет — сгенерируем быстрый rule-based ответ.
    """
    # Попытка понять последний запрос и канал
    last = ""
    for m in reversed(messages):
        if m.get("role") == "user":
            last = m.get("content") or ""
            break

    channel_name = channel or "whatsapp"
    contact_ref = int(contact_id or 0)

    if _is_unsubscribe_intent(last):
        reply = _unsubscribe_ack_text()
        try:
            state = load_sales_state(tenant, contact_ref)
            state.last_bot_reply = reply
            state.append_history("assistant", reply)
            state.last_updated_ts = time.time()
            save_sales_state(state)
        except Exception:
            pass
        return _wrap_llm_reply(reply, plan={"intent": "unsubscribe"}, raw_answer=reply)

    # Без ключа — быстрый локальный ответ
    client = _get_openai_client()
    if client is None:
        state = load_sales_state(tenant, contact_ref)
        if last and state.pending_fact_key:
            _capture_pending_fact_answer(state, last)
        grounding = _build_reply_grounding(
            tenant=tenant,
            state=state,
            user_text=last,
        )
        fallback = _llm_unavailable_reply(
            user_text=last,
            grounding=grounding,
        )
        state.last_bot_reply = fallback
        state.append_history("assistant", fallback)
        state.last_updated_ts = time.time()
        _update_fact_memory(state, fallback)
        _remember_questions_from_reply(state, fallback)
        save_sales_state(state)
        return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)

    try:
        if openai is not None:
            openai.api_key = settings.OPENAI_API_KEY  # type: ignore
        persona_hints = load_persona_hints(tenant, channel_name)
        state = load_sales_state(tenant, contact_ref)
        if last:
            if state.pending_fact_key:
                _capture_pending_fact_answer(state, last)
            save_sales_state(state)
        return await _single_llm_reply(
            client,
            messages,
            persona_hints,
            state,
            channel_name,
            contact_ref,
            tenant,
            last,
        )
    except Exception as exc:
        if _is_quota_or_rate_limit_error(exc):
            logger.warning("ask_llm quota/rate limited, fallback enabled")
        else:
            logger.exception("ask_llm unified path failed", exc_info=exc)
        fallback = _llm_unavailable_reply(
            user_text=last,
        )
        return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)


__all__ = [
    "Settings",
    "settings",
    "tenant_config",
    "tenant_waweb_url",
    "tenant_whatsapp_provider",
    "ADMIN_COOKIE",
    "get_tenant_pubkey",
    "set_tenant_pubkey",
    "http_json",
    "tenant_dir",
    "ensure_tenant_files",
    "read_tenant_config",
    "write_tenant_config",
    "read_persona",
    "write_persona",
    "load_tenant",
    "load_persona",
    "PersonaHints",
    "extract_persona_hints",
    "load_persona_hints",
    "load_persona_structured",
    "persona_meta_config",
    "persona_catalog_pdf",
    "persona_catalog_csv",
    "resolve_catalog_pdf_meta",
    "build_llm_messages",
    "ask_llm",
    # helpers ниже могут понадобиться в других частях
    "infer_user_needs",
    "search_catalog",
    "format_needs_for_prompt",
    "format_items_for_prompt",
    "pick_cta",
    "load_sales_state",
    "save_sales_state",
    "observe_user_message",
    "record_bot_reply",
    "summarize_sales_state",
    "read_all_catalog",
    "paginate_catalog_text",
]
