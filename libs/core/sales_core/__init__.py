from __future__ import annotations
import os, json, re, csv, asyncio, pathlib, time, random, hashlib, logging
from typing import List, Dict, Any, Optional, Tuple, Mapping, Sequence
from dataclasses import dataclass, field
import urllib.request, urllib.error
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
_CATALOG_CACHE: Dict[Tuple[Optional[int], Tuple[Tuple[str, float, int], ...]], List[Dict[str, Any]]] = {}
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
    APP_VERSION   = os.getenv("APP_VERSION", "v21.0")
    SEND          = os.getenv("SEND_ENABLED", "true").lower() == "true"

    REDIS_URL     = os.getenv("REDIS_URL", "redis://redis:6379/0")
    r = redis_async.from_url(REDIS_URL, decode_responses=True)

    # Публичный URL API (для вебхука waweb)
    APP_PUBLIC_URL   = (os.getenv("APP_PUBLIC_URL") or "").rstrip("/")
    APP_INTERNAL_URL = os.getenv("APP_INTERNAL_URL", "http://app:8000").rstrip("/")

    # waweb
    WA_WEB_URL    = (os.getenv("WA_WEB_URL", "http://waweb:9001") or "http://waweb:9001").rstrip("/")
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
    BAILEYS_URL = (os.getenv("BAILEYS_URL") or os.getenv("WABAILEYS_URL") or "http://wabaileys:9002").rstrip("/")
    _provider_default = (os.getenv("WHATSAPP_PROVIDER_DEFAULT") or "waweb").strip().lower()
    if _provider_default not in {"waweb", "baileys"}:
        _provider_default = "waweb"
    WHATSAPP_PROVIDER_DEFAULT = _provider_default

    _PDF_TABLES_ENGINE_RAW = (os.getenv("PDF_TABLES_ENGINE") or "plumber").strip().lower()
    PDF_TABLES_ENGINE = _PDF_TABLES_ENGINE_RAW if _PDF_TABLES_ENGINE_RAW in {"plumber", "camelot"} else "plumber"
    try:
        PDF_RENDER_DPI = int(os.getenv("PDF_RENDER_DPI", "220"))
    except ValueError:
        PDF_RENDER_DPI = 220

    # Админка
    ADMIN_TOKEN   = (os.getenv("ADMIN_TOKEN") or "").strip()
    _WORKER_BASE_RAW = (
        os.getenv("WORKER_BASE_URL")
        or os.getenv("TGWORKER_BASE_URL")
        or os.getenv("TG_WORKER_URL")
        or os.getenv("TGWORKER_URL")
        or _DEFAULT_WORKER_BASE_URL
    )
    WORKER_BASE_URL = str(_WORKER_BASE_RAW).strip().rstrip("/") or _DEFAULT_WORKER_BASE_URL
    TGWORKER_BASE_URL = WORKER_BASE_URL
    PUBLIC_KEY    = _resolve_public_key(ADMIN_TOKEN)
    WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET", "") or "").strip()

    # Avito OAuth
    AVITO_CLIENT_ID = (os.getenv("AVITO_CLIENT_ID") or "1OuyOIqOV6Pi6ewYI3mi").strip()
    AVITO_CLIENT_SECRET = (os.getenv("AVITO_CLIENT_SECRET") or "t-JCi261jbPfuvx1d5x0EP8Y9wKxyvDBwKU8sdTe").strip()
    AVITO_REDIRECT_URL = (os.getenv("AVITO_REDIRECT_URL") or "https://hub.avio.website/v1/oauth/avito/callback").strip()
    AVITO_AUTH_URL = (os.getenv("AVITO_AUTH_URL") or "https://www.avito.ru/oauth").strip()
    AVITO_TOKEN_URL = (os.getenv("AVITO_TOKEN_URL") or "https://api.avito.ru/token/").strip()
    AVITO_SCOPE = (os.getenv("AVITO_SCOPE") or "messenger:read,messenger:write,user:read").strip()
    try:
        AVITO_TIMEOUT = float(os.getenv("AVITO_TIMEOUT", "10"))
    except ValueError:
        AVITO_TIMEOUT = 10.0

    # LLM
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
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
    AGENT_NAME    = os.getenv("AGENT_NAME", "Акакий")
    BRAND_NAME    = os.getenv("BRAND_NAME", "Гермес")
    WHATSAPP_LINK = os.getenv("WHATSAPP_LINK", "https://wa.me/7XXXXXXXXXX")
    CITY          = os.getenv("CITY", "Уфа")

    # Персоны/промпты с диска
    PERSONA_MD    = os.getenv("PERSONA_MD") or str(DATA_DIR / "persona.md")


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
ADMIN_COOKIE        = "admin_token"
TENANT_PUBKEYS_HASH = "tenant_pubkeys"


# --------------------------- состояние диалогов -----------------------------
STATE_KEY_PREFIX = "sales_state"
STATE_TTL_SECONDS = int(os.getenv("STATE_TTL_SECONDS", str(8 * 3600)))
_STATE_CACHE: Dict[str, "SalesState"] = {}


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
        raw = _with_sync_redis(lambda client: client.get(key), None)
        if not raw:
            cached = _STATE_CACHE.get(key)
            if cached:
                return cached.to_dict()
            return None
        return json.loads(raw)
    except Exception:
        return None


def _state_store_write(key: str, payload: dict) -> None:
    _with_sync_redis(
        lambda client: client.setex(key, STATE_TTL_SECONDS, json.dumps(payload, ensure_ascii=False)),
        None,
    )


@dataclass
class SalesState:
    tenant: int
    contact_id: int
    channel: str = "whatsapp"
    needs: Dict[str, Any] = field(default_factory=dict)
    spin: Dict[str, str] = field(default_factory=lambda: {stage: "pending" for stage in ("s", "p", "i", "n")})
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
        obj.spin = payload.get("spin", obj.spin) or {stage: "pending" for stage in ("s", "p", "i", "n")}
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
        if self.history and self.history[-1].get("role") == role and self.history[-1].get("content") == content:
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
_GREETING_PREFIX_RE = re.compile(r"^\s*(здравствуйте|добрый день|добрый вечер|привет)\b[!,. ]*", re.IGNORECASE)
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
    r"^\s*([a-zа-яё0-9][a-zа-яё0-9\-\s]{1,42}),\s*(понял|принял|услышал)\b[,.! ]*",
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
_URGENT_REPLY_MARKERS = ("сегодня", "выезд", "мастер", "интервал", "приед")
_ETA_INTENT_RE = re.compile(
    r"(?iu)\b(через\s+сколько|когда\s+приед|когда\s+можно\s+приех|когда\s+приехать|"
    r"сколько\s+ждать|во\s+сколько|к\s+какому\s+времени)\b"
)
_ETA_REPLY_MARKERS = ("мин", "час", "ориентир", "интервал", "окно", "к ", "до ")
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
_NOISE_NEED_RE = re.compile(r"(?iu)\b(тих\w*|шумк\w*|шумоизоляц\w*|без\s+шума)\b")
_INSULATION_NEED_RE = re.compile(
    r"(?iu)\b(дует|сквозняк|промерз\w*|продува\w*|холод\w*|утеплен\w*|теплоизоляц\w*)\b"
)
_OBJECT_TYPE_HINT_RE = re.compile(
    r"(?iu)\b(квартир\w*|дом\w*|помещен\w*|офис\w*|склад\w*|коммерч\w*|студи\w*|комнат\w*|этаж\w*)\b"
)
_HUMAN_STYLE_FEW_SHOT = (
    "Формат живого ответа (пример):\n"
    "Клиент: «Здравствуйте»\n"
    "Менеджер: «Здравствуйте. В каком городе планируете установку?»\n\n"
    "Клиент: «Уфа»\n"
    "Менеджер: «Понял. Выбираете для квартиры или частного дома?»\n\n"
    "Клиент: «Пока нет фото и размеров»\n"
    "Менеджер: «Без проблем, можно начать без фото. Дам предварительный вариант, а размеры уточним позже.»\n\n"
    "Клиент: «Зачем две модели?»\n"
    "Менеджер: «Понял. Тогда дам один лучший вариант под ваш запрос.»\n\n"
    "Антишаблоны (не использовать): «Спасибо, понял», «Ваш запрос принят», "
    "«Если что-то ещё интересует — спрашивайте»."
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
    out = re.sub(r"\b([a-z]+_[a-z0-9_]+)\b", lambda m: m.group(1).replace("_", " "), out, flags=re.IGNORECASE)
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    out = "\n".join(lines).strip()
    out = _normalize_entity_ack_opening(out)

    return out


def _recent_gratitude_count(state: SalesState, tail: int = 6) -> int:
    if not isinstance(state, SalesState):
        return 0
    recent_assistant = [str(item.get("content") or "") for item in (state.history or []) if item.get("role") == "assistant"]
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
    return rebuilt or "Понял, продолжаем подбор."


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
        return "Понял."
    if tail and tail[0].isalpha():
        tail = tail[0].upper() + tail[1:]
    return f"Понял. {tail}".strip()


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
    return "Понял, остановил сообщения. Больше не пишем."


def _ensure_urgent_same_day_ack(reply: str, last_user_message: str) -> str:
    candidate = (reply or "").strip()
    if not candidate:
        return candidate
    if not _URGENT_TODAY_RE.search(str(last_user_message or "")):
        return candidate
    low = candidate.lower()
    if any(marker in low for marker in _URGENT_REPLY_MARKERS):
        return candidate
    return f"Понял, выезд сегодня возможен. {candidate}".strip()


def _ensure_eta_guidance(reply: str, last_user_message: str) -> str:
    candidate = (reply or "").strip()
    if not candidate:
        return candidate
    if not _ETA_INTENT_RE.search(str(last_user_message or "")):
        return candidate
    low = candidate.lower()
    if any(marker in low for marker in _ETA_REPLY_MARKERS):
        return candidate
    return f"{candidate} Ориентир по времени: напишу интервал после подтверждения адреса.".strip()


def _strip_instruction_leaks(text: str) -> str:
    candidate = str(text or "")
    if not candidate.strip():
        return ""
    out = _INSTRUCTION_LEAK_LINE_RE.sub("", candidate)
    out = _SHORTLIST_LEAK_RE.sub("", out)
    out = re.sub(
        r"(?iu)\bпосле\s+приветствия\s+последовательно\s+уточни:?\s*",
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
    if lines and all(re.match(r"^\d+[).]\s*", ln) for ln in lines):
        out = ""
    else:
        out = "\n".join(lines)
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

    required = _required_facts_from_persona_text(persona_text)
    missing = _missing_required_facts(required, known_facts)
    if turn_intent == "offtopic":
        reply = "Давайте вернемся к вашему запросу. Что нужно по товару или услуге?"
    elif missing:
        selected_key = missing[0]
        selected_question = _persona_question_for_fact(persona_text, selected_key) or _generic_question_for_fact(selected_key)
        for key in missing:
            q = _persona_question_for_fact(persona_text, key) or _generic_question_for_fact(key)
            if not _is_repeated_question_against_state(q, state):
                selected_key = key
                selected_question = q
                break
        if _is_repeated_question_against_state(selected_question, state):
            non_address = [k for k in missing if _canonical_fact_key(k) != "address"]
            for key in non_address:
                q = _persona_question_for_fact(persona_text, key) or _generic_question_for_fact(key)
                if not _is_repeated_question_against_state(q, state):
                    selected_key = key
                    selected_question = q
                    break
        state.pending_fact_key = _canonical_fact_key(selected_key)
        reply = f"Понял. {selected_question}"
    elif turn_intent == "catalog_request" and str(branding.get("CATALOG_URL") or "").strip():
        reply = f"Вот каталог: {str(branding.get('CATALOG_URL') or '').strip()}"
    elif _ORDER_INTENT_RE.search(str(last_user_message or "")):
        if str(known_facts.get("contact") or "").strip():
            reply = "Понял, продолжаем оформление. Подтверждение отправлю по вашему контакту."
        else:
            state.pending_fact_key = "contact"
            reply = "Понял. Чтобы оформить, оставьте, пожалуйста, телефон или удобный мессенджер."
    else:
        reply = "Понял. Уточните, пожалуйста, что именно нужно сейчас."
    reply = _humanize_reply_text(reply, state=state, persona_hints=persona_hints)
    reply = _drop_repeated_questions_from_reply(reply, state)
    reply = _ensure_urgent_same_day_ack(reply, last_user_message)
    reply = _ensure_eta_guidance(reply, last_user_message)
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
    if len((reply or "").strip()) <= 8:
        reply = "Понял. Уточните, пожалуйста, что именно нужно сейчас."
    _remember_questions_from_reply(state, reply)
    save_sales_state(state)
    return reply


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
        return "Понял. Уточните, пожалуйста, что именно нужно."

    # Remove technical multi-variant labels if they slip into final output.
    text = re.sub(r"(?im)^\s*Вариант\s+\d+\s*:\s*", "", text).strip()

    # Trim obvious bureaucratic tails.
    text = re.sub(r"(?im)^\s*с уважением[,.! ]*$", "", text).strip()
    text = re.sub(r"(?im)^\s*обращайтесь в любое время[,.! ]*$", "", text).strip()

    # Final cleanup (minimal and non-destructive).
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
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
    has_greeting_tokens = any(token in low for token in ("здорова", "приветств", "добрый день", "здравствуйте"))
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
    if _GREETING_PREFIX_RE.match(candidate):
        return candidate
    has_bot_history = bool(str(getattr(state, "last_bot_reply", "") or "").strip())
    if has_bot_history:
        return candidate
    if re.match(r"^\s*(https?://|@[\w\d_]+)", candidate):
        return candidate
    return f"Здравствуйте. {candidate}".strip()


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


def _enforce_sentence_budget(text: str, max_sentences: int = 4) -> str:
    candidate = _normalize_numbered_list_punctuation(text)
    if not candidate:
        return candidate
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
    if len(parts) <= max(1, int(max_sentences or 1)):
        return candidate
    kept = parts[: max(1, int(max_sentences or 1))]
    clipped = " ".join(kept).strip()
    if clipped and clipped[-1] not in ".!?":
        clipped = clipped + "."
    return clipped


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
    previous_fps = [str(item or "").strip() for item in (state.asked_question_fingerprints or []) if str(item or "").strip()]
    last_question = str(state.last_question_text or "").strip()
    if last_question:
        last_fp = quality.question_fingerprint(last_question)
        if last_fp:
            previous_fps.append(last_fp)
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
    # Avoid repeating previous bot reply almost verbatim.
    prev = (state.last_bot_reply or "").strip().lower()
    if prev and len(prev) > 20 and candidate.lower() == prev:
        return False
    if _reply_has_repeated_question(candidate, state):
        return False
    return True


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


async def _llm_call_with_deadline(
    create_fn: Any,
    *,
    timeout_seconds: float,
    **kwargs: Any,
) -> Any:
    timeout_value = max(2.0, float(timeout_seconds or 0.0))
    hard_deadline = timeout_value + 2.0
    try:
        import anyio  # type: ignore

        with anyio.fail_after(hard_deadline):
            return await anyio.to_thread.run_sync(lambda: create_fn(**kwargs))
    except Exception:
        return await asyncio.wait_for(
            asyncio.to_thread(create_fn, **kwargs),
            timeout=hard_deadline,
        )


_FACT_TOKEN_RE = re.compile(r"[0-9a-zа-яё]+", re.IGNORECASE)
_CONTACT_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_CONTACT_HANDLE_RE = re.compile(r"(?<!\w)@[\w\d_]{4,}")
_CONTACT_PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d[\d\-\s()]{8,}\d)(?!\d)")
_PRICE_INLINE_RE = re.compile(
    r"(?<!\d)(?:\d{1,3}(?:[ \u00A0]\d{3})+|\d{4,7})(?:\s*(?:₽|руб(?:\.|ля|лей)?))?",
    re.IGNORECASE,
)
_PRICE_THOUSANDS_RE = re.compile(
    r"(?iu)\b(\d{1,3})\s*(?:тыс(?:\.|яч)?|тысяч(?:а|и)?|к)\b"
)
_MODEL_QUOTED_MENTION_RE = re.compile(
    r'(?iu)\b(модель|вариант|дверь)\s*[«"]([^"»]{2,80})[»"]'
)
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


def _generic_question_for_fact(fact_key: str) -> str:
    key = _canonical_fact_key(fact_key)
    prompts = {
        "city": "В каком городе планируете установку?",
        "address": "Подскажите адрес установки",
        "object_type": "Для квартиры или частного дома выбираете?",
        "model": "Что из каталога приглянулось?",
        "dimensions": "Подскажите размеры проема или пришлите фото",
        "budget": "Есть ориентир по бюджету?",
        "timeline": "На какие сроки ориентируетесь?",
        "contact": "Оставьте контакт для связи",
    }
    return prompts.get(key, f"Подскажите, пожалуйста, {key}.")


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
    if turn_intent == "offtopic":
        if candidate:
            return candidate, ""
        return "Давайте вернемся к вашему запросу. Что нужно по товару или услуге?", ""
    user_low = user_raw.lower()
    candidate_substantive = len(candidate) >= 36
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
            if _question_covers_fact(q, "city") or _question_covers_fact(q, "address") or _question_covers_fact(q, "object_type"):
                continue
            payment_questions.append(q.strip())
        if payment_questions:
            return " ".join(payment_questions), ""
        return candidate, ""
    if handoff_intent and candidate:
        return candidate, ""

    if store_address_intent:
        city_map = _extract_store_addresses_from_persona(persona_context)
        known_city = str(
            facts.get("city")
            or (state.known_slots.get("city") if isinstance(state.known_slots, dict) else "")
            or ""
        ).strip().lower().replace("ё", "е")
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
                hint = _extract_city_hint(txt_raw)
                if hint:
                    known_city = hint.lower().replace("ё", "е")
                    break
        if known_city:
            for city_key, address in city_map.items():
                if city_key and (city_key in known_city or known_city in city_key):
                    return address, ""
        # Address/store request without city: ask city directly.
        return _generic_question_for_fact("city"), "city"

    if next_key == "model":
        items = grounding_items
        asks_price_or_name = _is_price_intent(user_raw) or bool(_MODEL_NAME_INTENT_RE.search(user_raw))
        asks_selected_attribute = bool(selected_item) and bool(_selected_item_attribute_answer(user_raw, selected_item))
        user_explicit_model = bool(_best_catalog_item_match(user_raw, items)) if items else False
        candidate_has_substance = len(candidate) >= 24 and not _question_covers_fact(candidate, "model")

        # Guard conflict resolver:
        # if the user asked a direct factual follow-up and reply already contains a substantive answer,
        # do not override with generic "what model did you like?" question.
        if candidate and candidate_has_substance and (asks_price_or_name or asks_selected_attribute or user_explicit_model):
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
            return f"{candidate} {_generic_question_for_fact('city')}".strip(), "city"

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
        question = _generic_question_for_fact("city")
        return f"{lead} {question}".strip(), "city"

    question = _persona_question_for_fact(persona_context, next_key) or _generic_question_for_fact(next_key)
    existing_questions = _extract_questions_from_text(candidate)
    if existing_questions:
        if any(_question_covers_fact(item, next_key) for item in existing_questions):
            for item in existing_questions:
                if _question_covers_fact(item, next_key):
                    # Keep the actual qualification question and drop service preambles.
                    return item.strip(), next_key
            return candidate, next_key
        # For core qualification chain we enforce the missing step question to keep
        # deterministic progression from persona script.
        if next_key in {"city", "address", "object_type", "model"}:
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
        if any(token in low for token in ("диалог-скрипт", "скрипт диалога", "последовательно уточни")):
            in_primary_block = True
            in_secondary_block = False
            continue
        if "шаблон реплик" in low:
            in_secondary_block = True
            in_primary_block = False
            continue
        if in_primary_block:
            if re.match(r"^\d+\)", low) or re.match(r"^\d+\.", low) or low.startswith("-") or low.startswith("•"):
                primary_lines.append(line)
                continue
            primary_lines.append(line)
        elif in_secondary_block:
            if re.match(r"^\d+\)", low) or re.match(r"^\d+\.", low) or low.startswith("-") or low.startswith("•"):
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
        action_prefixes = ("уточ", "спрос", "узна", "получ", "собер", "подскаж", "пришли", "пришлите")
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
    # Convert imperative script line into a neutral question via generic fact map.
    fact_keys = _fact_keys_from_line(txt)
    if fact_keys:
        return _generic_question_for_fact(fact_keys[0])
    return ""


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
        if any(token in low for token in ("диалог-скрипт", "скрипт диалога", "последовательно уточни")):
            in_primary_block = True
            continue
        if in_primary_block:
            if re.match(r"^\d+\)", low) or re.match(r"^\d+\.", low) or low.startswith("-") or low.startswith("•"):
                primary_lines.append(line)
                continue
            primary_lines.append(line)
    return primary_lines


def _extract_expected_tokens_from_condition(text: str) -> List[str]:
    raw = str(text or "").strip().lower().replace("ё", "е")
    if not raw:
        return []
    tokens = [tok for tok in re.findall(r"[a-zа-я0-9\-]{2,}", raw)]
    stop = {
        "если", "клиент", "когда", "после", "при", "то", "или", "и", "а", "не", "из", "в", "во", "на", "по",
        "город", "адрес", "квартира", "дом", "помещение", "тип", "объект", "модель", "бюджет", "срок", "контакт",
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
    return any(marker in low for marker in contact_markers) and any(marker in low for marker in action_markers)


def _delivery_rule_from_line(
    *,
    source_line: str,
    channel_scope: List[str] | None = None,
    condition_text: str = "",
) -> PersonaDeliveryRule:
    clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(source_line or "")).strip()
    low = clean.lower().replace("ё", "е")
    wants_handle = "@" in low or any(marker in low for marker in ("username", "юзернейм", "ник", "логин"))
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


def _compile_persona_rules(persona_text: str) -> PersonaCompiledRules:
    key = _persona_rules_cache_key(persona_text)
    if key:
        cached = _PERSONA_RULES_CACHE.get(key)
        if cached is not None:
            return cached
    text = str(persona_text or "")
    compiled = PersonaCompiledRules()
    compiled.contact_artifacts = _extract_contact_artifacts(text)

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
        canonical = _canonical_fact_key(keys[0])
        if not canonical:
            continue
        if any(step.fact_key == canonical for step in compiled.steps):
            continue
        compiled.steps.append(
            PersonaStepRule(
                fact_key=canonical,
                source_line=clean,
                question=_line_to_question(clean),
            )
        )

    section_scope: List[str] = []
    for raw_line in text.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line.startswith("#"):
            section_scope = _detect_persona_line_channels(line)
            continue
        low = line.lower().replace("ё", "е")
        line_scope = _detect_persona_line_channels(line)
        effective_scope = section_scope or line_scope
        if (not low.startswith("если ")) and _is_delivery_directive_line(line):
            compiled.delivery_rules.append(
                _delivery_rule_from_line(
                    source_line=line,
                    channel_scope=effective_scope,
                )
            )
        if not low.startswith("если "):
            continue
        cond = ""
        action = ""
        if ":" in line:
            cond, action = line.split(":", 1)
        elif " - " in line:
            cond, action = line.split(" - ", 1)
        elif " то " in low:
            idx = low.find(" то ")
            cond = line[:idx].strip()
            action = line[idx + 4 :].strip()
        cond = cond.strip()
        action = action.strip()
        if not cond or not action:
            continue
        cond_keys = _fact_keys_from_line(cond)
        fact_key = _canonical_fact_key(cond_keys[0]) if cond_keys else ""
        compiled.conditionals.append(
            PersonaConditionalRule(
                source_line=line,
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
                    channel_scope=effective_scope,
                    condition_text=cond,
                )
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
        # Generic fallback: condition mentions "если", so require at least one token overlap.
        cond_tokens = [tok for tok in re.findall(r"[a-zа-я0-9\-]{3,}", cond)]
        return any(tok in haystack for tok in cond_tokens[:4])
    # Persona conditions often list alternatives (cities/channels/etc), so
    # matching by ANY token is more robust than requiring all of them.
    return any(tok in haystack for tok in rule.expected_tokens)


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
    compiled = _compile_persona_rules(persona_text)
    if not compiled.conditionals:
        return candidate
    out = candidate
    out_norm = _normalize_text(out)
    existing_questions = _extract_questions_from_text(out)
    existing_topics = {
        _question_topic(item)
        for item in existing_questions
        if str(item or "").strip()
    }
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
        action_norm = _normalize_text(action)
        if not action_norm:
            continue
        if action_norm in out_norm:
            continue
        action_questions = _extract_questions_from_text(action)
        if action_questions and existing_questions:
            action_topics = {
                _question_topic(item)
                for item in action_questions
                if str(item or "").strip()
            }
            if action_topics & existing_topics:
                continue
        out_tokens = set(_FACT_TOKEN_RE.findall(out_norm))
        action_tokens = set(_FACT_TOKEN_RE.findall(action_norm))
        if out_tokens and action_tokens:
            overlap = len(out_tokens & action_tokens)
            coverage = overlap / max(1, len(action_tokens))
            if coverage >= 0.7:
                continue
        if len(action) > 260:
            continue
        out = f"{out} {action}".strip()
        out_norm = _normalize_text(out)
        existing_questions = _extract_questions_from_text(out)
        existing_topics = {
            _question_topic(item)
            for item in existing_questions
            if str(item or "").strip()
        }
    candidate = out
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
        chosen_artifacts = _select_contact_artifacts_for_rule(rule, compiled.contact_artifacts)
        chosen_artifacts = [item for item in chosen_artifacts if _is_contact_artifact_token(item)]
        if not chosen_artifacts:
            continue
        if _reply_has_contact_artifact(out, chosen_artifacts):
            continue
        if isinstance(state, SalesState):
            since_contact = _assistant_messages_since_contact(state, chosen_artifacts)
            if since_contact < max(1, int(rule.min_assistant_gap or 1)) and not _is_contact_request_text(last_user_message):
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
    return out


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
        if len(tokens) < 2 or len(tokens) > 6:
            return False
        explicit_markers = (
            "ул",
            "улиц",
            "просп",
            "пр-",
            "дом",
            "корп",
            "стр",
            "переул",
            "пер",
            "шоссе",
            "бульвар",
            "наб",
        )
        if any(marker in low for marker in explicit_markers):
            return True
        # Allow partial address like "хмельницкого", "ленина", "коммунистическая"
        suffix_hits = 0
        for tok in tokens:
            if re.search(r"(ского|ской|ская|ский|ина|ова|ева|ого|ая|ый|ий)$", tok):
                suffix_hits += 1
        return suffix_hits >= 1
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
    # Fallback: at least two tokens and one token with letters.
    tokens = [tok for tok in re.split(r"[\s,]+", low) if tok]
    if len(tokens) < 2:
        return False
    has_letters = any(bool(re.search(r"[a-zа-я]", tok, re.IGNORECASE)) for tok in tokens)
    return has_letters


def _is_plausible_city_text(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    if "?" in raw:
        return False
    if _looks_like_address_value(raw):
        return False
    tokens = [tok for tok in re.split(r"[\s,.;:()]+", raw) if tok]
    if not tokens or len(tokens) > 3:
        return False
    if all(re.fullmatch(r"\d+", tok) for tok in tokens):
        return False
    if not any(re.search(r"[A-Za-zА-Яа-яЁё]", tok) for tok in tokens):
        return False
    return True


def _extract_city_hint(text: str) -> str:
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
    if _is_plausible_city_text(raw):
        return raw
    return ""


def _capture_pending_fact_answer(state: SalesState, user_text: str) -> None:
    key = _canonical_fact_key(state.pending_fact_key)
    if not key:
        return
    text = str(user_text or "").strip()
    if not text:
        return
    if not isinstance(state.facts, dict):
        state.facts = {}
    city_hint = _extract_city_hint(text)
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
    if key == "model":
        if _LOW_SIGNAL_CONTEXT_RE.search(low) or _CATALOG_UNAVAILABLE_RE.search(low):
            return
        noisy = {"каталог", "грузится", "не", "могу", "пока", "позже"}
        if tokens and sum(1 for tok in tokens if _normalize_text(tok) in noisy) >= max(1, len(tokens) // 2):
            return
    state.facts[key] = _safe_short_text(text, 180)
    state.pending_fact_key = ""


def _merge_fact_updates(state: SalesState, updates: Mapping[str, Any] | None) -> None:
    if not updates:
        return
    if not isinstance(state.facts, dict):
        state.facts = {}
    for raw_key, raw_value in dict(updates or {}).items():
        key = _canonical_fact_key(str(raw_key))
        if not key:
            continue
        value = _safe_short_text(str(raw_value or ""), 180)
        if not value:
            continue
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
    parts = [part.strip() for part in _SENTENCE_SPLIT_RE.split(raw) if part.strip()]
    kept: list[str] = []
    for part in parts:
        if "?" in part:
            kept.append(part)
            continue
        low = part.lower()
        # Do not drop factual numeric lines (price/discount/amount) even if they repeat.
        if re.search(r"\d", part) and any(token in low for token in ("цен", "скид", "руб", "₽", "стоим")):
            kept.append(part)
            continue
        fp = _fact_fingerprint(part)
        if not fp or fp not in recent:
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
        if tokens and sum(1 for tok in tokens if tok in generic_model_noise) >= max(1, len(tokens) // 2):
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
    match = re.search(r"\d[\d\s.,]*", raw)
    if not match:
        return None
    digits = re.sub(r"\D", "", match.group(0))
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def _item_label(item: Mapping[str, Any]) -> str:
    for key in ("title", "name", "model", "sku", "id"):
        value = str(item.get(key) or "").strip()
        if value:
            return value
    return ""


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
    q_tokens = {tok for tok in _FACT_TOKEN_RE.findall((query or "").lower().replace("ё", "е")) if len(tok) >= 2}
    a_tokens = {tok for tok in _FACT_TOKEN_RE.findall((alias or "").lower().replace("ё", "е")) if len(tok) >= 2}
    if not q_tokens or not a_tokens:
        return 0.0
    overlap = len(q_tokens & a_tokens)
    if overlap == 0:
        return 0.0
    return overlap / max(1, len(q_tokens))


def _best_catalog_item_match(query: str, items: Sequence[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
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


def _collect_grounding_items(tenant: int | None, state: SalesState, user_text: str) -> List[Dict[str, Any]]:
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
            query_needs = infer_user_needs(effective_query or user_text)
            for key, value in query_needs.items():
                if value in (None, "", [], {}, ()):
                    continue
                if key == "keywords":
                    merged_tokens: List[str] = [str(x) for x in (needs.get("keywords") or []) if str(x).strip()]
                    for token in value if isinstance(value, list) else [value]:
                        token_str = str(token).strip()
                        if token_str and token_str not in merged_tokens:
                            merged_tokens.append(token_str)
                    if merged_tokens:
                        needs["keywords"] = merged_tokens[:8]
                    continue
                needs[key] = value
            extra = search_catalog(needs, limit=8, tenant=tenant, query=effective_query or user_text)
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


def _has_single_color_variant(selected_item: Mapping[str, Any], catalog_items: Sequence[Mapping[str, Any]]) -> bool:
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
    selected_query = state.known_slots.get("model") or user_text
    selected_item = _best_catalog_item_match(selected_query, full_catalog or items)
    forbidden_topics: set[str] = set()

    if selected_item is not None and full_catalog and _has_single_color_variant(selected_item, full_catalog):
        forbidden_topics.add("color")

    model_aliases: set[str] = set()
    source_for_aliases = full_catalog or items
    for item in source_for_aliases:
        for alias in _item_aliases(item):
            normalized = re.sub(r"[^0-9a-zа-яё]+", " ", str(alias).lower().replace("ё", "е")).strip()
            if normalized and len(normalized) >= 3:
                model_aliases.add(normalized)

    return {
        "items": items,
        "catalog_items": full_catalog[:500],
        "needs": dict(state.needs or {}),
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
    low = text.lower().replace("ё", "е")
    if "?" in text and len(_FACT_TOKEN_RE.findall(text)) > 6:
        return
    tokens = [tok for tok in _FACT_TOKEN_RE.findall(low) if len(tok) >= 2]
    if not tokens:
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
        if block_allowance_override and idx in block_allowance_override and not bool(block_allowance_override[idx]):
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
            requires.extend(str(x) for x in (block_requires_override.get(idx) or []) if str(x).strip())
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
    if missing_required and not question_used:
        miss_key = missing_required[0]
        out.append(_generic_question_for_fact(miss_key))
        next_question_key = _canonical_fact_key(miss_key)
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
    hay = _normalize_text(_collect_item_text(dict(item)))
    if "2мдф" in hay or "двухпанел" in hay:
        return True
    for raw_key, raw_val in item.items():
        key = _normalize_text(raw_key)
        val = _normalize_text(raw_val)
        if not key or not val:
            continue
        if "мдф" in key and ("снаруж" in key or "наруж" in key):
            if re.search(r"\d", val):
                return True
    return False


def _normalize_model_alias(value: str) -> str:
    return re.sub(r"[^0-9a-zа-яё]+", " ", str(value or "").lower().replace("ё", "е")).strip()


def _grounding_catalog_items(grounding: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for bucket_name in ("items", "catalog_items"):
        for raw_item in ((grounding or {}).get(bucket_name) or []):
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
        if items and _best_catalog_item_match(model_name, items) is not None:
            return match.group(0)
        if noun:
            return f"{noun} из каталога"
        return "модель из каталога"

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
    sentences = [part.strip() for part in _SENTENCE_SPLIT_RE.split(base) if part.strip()]
    if not sentences:
        return base
    out: list[str] = []
    for sentence in sentences:
        low = sentence.lower()
        if "?" in sentence:
            out.append(sentence)
            continue
        has_price_hints = any(token in low for token in ("цена", "стоит", "руб", "₽", "тыс", "тысяч", " к "))
        price_spans = _extract_price_spans(sentence)
        if not has_price_hints and not price_spans:
            out.append(sentence)
            continue
        mentioned_items = _mentioned_catalog_items_in_order(sentence, items)
        if not mentioned_items:
            item = _best_catalog_item_match(sentence, items)
            if item is not None:
                mentioned_items = [item]
        if not mentioned_items:
            out.append(sentence)
            continue
        expected_prices: list[int] = []
        for item in mentioned_items:
            price = _item_price_int(item)
            if price:
                expected_prices.append(price)
        if not expected_prices:
            out.append(sentence)
            continue
        if not price_spans:
            out.append(sentence)
            continue
        replacements: list[tuple[int, int, str]] = []
        if len(expected_prices) >= 2 and len(price_spans) >= 2:
            for idx, span in enumerate(price_spans):
                if idx >= len(expected_prices):
                    break
                expected = expected_prices[idx]
                if span[2] == expected:
                    continue
                replacements.append((span[0], span[1], _format_rub_price(expected)))
        else:
            expected = expected_prices[0]
            first = price_spans[0]
            if first[2] != expected:
                replacements.append((first[0], first[1], _format_rub_price(expected)))
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
    r"(?iu)\b(сколько\s+стоит|сколько\s+цена|цена|ценник|по\s*ч[её]м|поч[её]м|чо\s+по\s+чем|"
    r"от\s+скольк[аи]|от\s+какой\s+цены|по\s+какой\s+цене|что\s+за\s+дверь\s+за\s*\d+|за\s*\d+\s*$)\b"
)
_MIN_PRICE_INTENT_RE = re.compile(
    r"(?iu)\b(от\s+скольк|начина(?:ется|ются)|минимальн\w+|сам\w*\s+дешев\w*)\b"
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


def _reply_mentions_unknown_model(text: str, items: Sequence[Mapping[str, Any]]) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    fragments = re.findall(
        r"(?iu)\b(?:модель|вариант)\s+[«\"]?([a-zа-яё0-9][^\"»\n,.!?;:]{1,80})",
        raw,
    )
    if not fragments:
        return False
    for fragment in fragments:
        probe = str(fragment or "").strip(" -—\t")
        if len(probe) < 3:
            continue
        if _best_catalog_item_match(probe, items) is None:
            # Allow descriptor-heavy mentions that still point to known catalog model,
            # e.g. "гарда 8 с зеркалом" -> "гарда 8".
            stripped = re.sub(r"(?iu)\bс\s+зеркал\w*\b", " ", probe)
            stripped = re.sub(r"(?iu)\bзеркал\w*\b", " ", stripped)
            stripped = re.sub(r"\s{2,}", " ", stripped).strip()
            if stripped and _best_catalog_item_match(stripped, items) is not None:
                continue
            return True
    return False


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
        "ая", "яя", "ое", "ее", "ый", "ий", "ой", "ую", "юю", "ые", "ие",
        "ого", "его", "ому", "ему", "ыми", "ими", "ых", "их", "ость", "ности",
        "ом", "ем", "ам", "ям", "ах", "ях", "ами", "ями", "у", "ю", "а", "я", "е", "ы", "и",
    ):
        if len(value) > len(suffix) + 2 and value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _extract_attribute_probe(user_text: str) -> str:
    raw = str(user_text or "")
    if "?" not in raw:
        return ""
    tokens = [tok for tok in _FACT_TOKEN_RE.findall(_normalize_text(raw)) if len(tok) >= 4]
    if not tokens:
        return ""
    stop = {
        "сколько", "стоит", "цена", "цена", "модель", "название", "какая", "какой", "какие",
        "подойдет", "подойдёт", "подходит", "она", "эта", "этот", "это", "ли", "и", "а",
        "почему", "зачем", "именно", "предложили", "предложил", "их", "эти", "варианты",
        "так", "тогда", "вообще", "просто",
    }
    for token in tokens:
        if token in stop or token.isdigit():
            continue
        return token
    return ""


def _selected_item_attribute_answer(
    user_text: str,
    selected_item: Mapping[str, Any],
) -> str:
    probe = _extract_attribute_probe(user_text)
    if not probe:
        return ""
    name = _item_label(dict(selected_item))
    if not name:
        return ""
    hay = _normalize_text(_collect_item_text(dict(selected_item)))
    probe_norm = _normalize_probe_token(probe)
    has_attribute = bool(probe_norm and probe_norm in hay)
    if probe_norm.startswith("двухпанел"):
        has_attribute = _catalog_item_is_two_panel(selected_item)
    if has_attribute:
        probe_text = _safe_short_text(str(probe or "").strip().lower(), 24)
        name_norm = _normalize_text(name)
        if probe_norm and probe_norm in name_norm:
            return "Да, есть в наличии."
        if probe_text:
            return "Да, есть."
        return "Да, есть в наличии."
    return ""


def _items_with_attribute(
    items: Sequence[Mapping[str, Any]],
    probe: str,
) -> List[Mapping[str, Any]]:
    needle = _normalize_probe_token(probe)
    if not needle:
        return []
    out: List[Mapping[str, Any]] = []
    for item in items:
        text = _normalize_text(_collect_item_text(dict(item)))
        if not text:
            continue
        has_attribute = needle in text
        if needle.startswith("двухпанел"):
            has_attribute = _catalog_item_is_two_panel(item)
        if has_attribute:
            out.append(item)
    return out


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


def _is_price_intent(text: str) -> bool:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return False
    patterns = (
        r"\bсколько\s+стоит\b",
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
    return any(token in low for token in ("telegram", "телег", "тг", "whatsapp", "ватсап", "вотсап"))


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


def _is_deferral_message(text: str) -> bool:
    low = str(text or "").lower().replace("ё", "е")
    if not low:
        return False
    return any(token in low for token in ("позже", "потом", "вечером", "завтра", "позднее", "как буду", "как смогу"))


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
    if not items:
        return base

    normalized = _enforce_catalog_model_grounding(base, grounding=grounding)
    normalized = _enforce_catalog_price_grounding(normalized, grounding=grounding)

    low = normalized.lower()
    mentions_known = _reply_mentions_catalog_item(normalized, items)
    has_unknown_model_marker = "модель из каталога" in low
    asks_price = _is_price_intent(user_text)
    asks_model_name = bool(_MODEL_NAME_INTENT_RE.search(str(user_text or "")))
    has_price_tokens = bool(_extract_price_spans(normalized))
    needs_ctx = dict((grounding or {}).get("needs") or {})
    prefers_insulation = bool(needs_ctx.get("insulation_priority") or needs_ctx.get("noise_priority"))
    candidate_items = list(items)
    selected_item = _selected_item_from_grounding(grounding, items)
    if prefers_insulation:
        two_panel = [item for item in candidate_items if _catalog_item_is_two_panel(item)]
        if two_panel:
            candidate_items = two_panel

    # Do not inject templated shortlist replies here.
    # Guard should validate/correct facts, while wording stays LLM-driven.
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
        # For structural attributes keep narrowed shortlist; otherwise use full catalog.
        if probe_norm.startswith("двухпанел"):
            source_for_attr = list(candidate_items)
        attr_items = _items_with_attribute(source_for_attr, asked_probe)
        if not attr_items:
            attr_items = _items_with_attribute(items, asked_probe)
        budget_cap = _extract_budget_cap_from_needs(dict((grounding or {}).get("needs") or {}))
        if budget_cap and attr_items:
            limited = [it for it in attr_items if (_item_price_int(dict(it)) or 10**9) <= budget_cap]
            if limited:
                attr_items = limited
            else:
                nearest_attr = _closest_catalog_item_by_price(attr_items, budget_cap)
                if nearest_attr is not None:
                    nm = str(_item_label(dict(nearest_attr)) or "").lower()
                    pr = _item_price_int(dict(nearest_attr))
                    if nm and pr:
                        return (
                            f"Ближайший по цене вариант — {nm} за {_format_rub_price(pr)}."
                        )
        if selected_item is not None:
            selected_attr_answer = _selected_item_attribute_answer(user_text, selected_item)
            if selected_attr_answer:
                return selected_attr_answer
        # If there is no confident fact response, keep model text as-is.
        if probe_norm:
            return normalized

    if asks_price and (not mentions_known or has_price_tokens):
        normalized_words = len(re.findall(r"(?u)\b\w+\b", normalized))
        normalized_sentences = len([s for s in re.split(r"[.!?]+", normalized) if s.strip()])
        # Keep rich model answers for detailed user prompts instead of forcing one-line min-price fallback.
        if normalized_words >= 10 and (normalized_sentences >= 2 or "?" in normalized):
            return normalized
        asks_min_price = bool(_MIN_PRICE_INTENT_RE.search(str(user_text or "")))
        if selected_item is not None and (not asks_min_price):
            selected_name = _item_label(dict(selected_item))
            selected_price = _item_price_int(dict(selected_item))
            if selected_name and selected_price:
                return (
                    f"По каталогу {selected_name} стоит {_format_rub_price(selected_price)}. "
                    "Рассказать подробнее по этой модели?"
                )
        target_price = _extract_price_target_hint(user_text)
        if target_price:
            nearest = _closest_catalog_item_by_price(candidate_items, target_price)
            if nearest is not None:
                name = _item_label(dict(nearest))
                price = _item_price_int(dict(nearest))
                if name and price:
                    return (
                        f"Ближайшая модель по цене — {name} за {_format_rub_price(price)}."
                    )
        min_price = _catalog_min_price(candidate_items)
        if min_price:
            return f"По каталогу цены начинаются от {_format_rub_price(min_price)}."

    if asks_model_name:
        if selected_item is not None:
            selected_name = _item_label(dict(selected_item))
            selected_price = _item_price_int(dict(selected_item))
            if selected_name and selected_price:
                return (
                    f"Название модели — {selected_name}. "
                    f"Цена по каталогу — {_format_rub_price(selected_price)}. "
                    "Рассказать подробнее по характеристикам?"
                )
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
                return (
                    f"Название модели — {name}. "
                    f"Цена по каталогу — {_format_rub_price(price)}."
                )
            if name:
                return f"Название модели — {name}. Рассказать подробнее?"

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
    # Do not override a valid persona step question (city/address/object/model clarifications).
    for question in _extract_questions_from_text(base):
        if any(
            _question_covers_fact(question, fact)
            for fact in ("city", "address", "object_type", "model")
        ):
            return base
    needs_ctx = dict((grounding or {}).get("needs") or {})
    needs_mirror = bool(
        re.search(r"(?iu)\bзеркал\w*\b", str(user_text or ""))
        or re.search(r"(?iu)\bзеркал\w*\b", " ".join(str(x) for x in (needs_ctx.get("keywords") or [])))
    )

    if "двухпанель" in _normalize_text(base):
        mentioned = _mentioned_catalog_items_in_order(base, items)
        invalid = [item for item in mentioned if not _catalog_item_is_two_panel(item)]
        if invalid:
            two_panel_items = [item for item in items if _catalog_item_is_two_panel(item)]
            source = two_panel_items if two_panel_items else items
            shortlist = _format_short_catalog_variants(source, limit=2)
            if shortlist:
                return f"Для этой задачи лучше двухпанельные варианты: {shortlist}. Какой ближе?"

    user_has_variants_intent = bool(_VARIANTS_USER_HINT_RE.search(str(user_text or "")))
    bot_promised_variants = bool(_VARIANTS_PROMISE_RE.search(base))
    has_unknown_model = _reply_mentions_unknown_model(base, items)
    if not user_has_variants_intent and not bot_promised_variants:
        if not has_unknown_model:
            return base
    if _reply_mentions_catalog_item(base, items) and not has_unknown_model:
        return base
    # If answer already contains a meaningful priced shortlist, don't append duplicate tail.
    if len(_extract_price_spans(base)) >= 2:
        return base

    source_items = list(items)
    if needs_mirror:
        mirror_items = _items_with_attribute(source_items, "зеркало")
        if mirror_items:
            source_items = mirror_items
    shortlist = _format_short_catalog_variants(source_items, limit=2)
    if not shortlist:
        return base
    if has_unknown_model:
        return f"По каталогу могу предложить: {shortlist}. Какой вариант показать подробнее?"
    if "?" in base:
        suffix = f"Варианты из каталога: {shortlist}"
    else:
        suffix = f"Варианты из каталога: {shortlist}. Какой ближе?"
    if suffix.lower() in base.lower():
        return base
    return f"{base} {suffix}".strip()


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
    for item in (dialogue_tail or []):
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
        "{\"audited\":[{\"index\":0,\"requires\":[\"city\",\"address\"]}]}. "
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
        "{\"audited\":[{\"index\":0,\"allow\":true,\"missing\":[\"address\"]}]}. "
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
    text = (last_user_message or "").strip()
    low = _normalize_text(text)
    turn_intent = _classify_turn_intent(text)
    if turn_intent == "offtopic":
        return {
            "_fallback": True,
            "intent": "clarify",
            "ack": "Понял.",
            "core": "Верну диалог к рабочему запросу.",
            "question": "",
            "question_slot": "none",
            "required_facts": [],
            "facts_update": {},
            "blocks": [
                {"text": "Понял.", "requires": [], "type": "ack"},
                {
                    "text": "Давайте вернемся к вашему запросу. Что нужно по товару или услуге?",
                    "requires": [],
                    "type": "info",
                },
            ],
        }
    inferred_slot = "other"
    if _MODEL_NAME_INTENT_RE.search(text) or (
        low
        and not _is_price_intent(text)
        and not any(token in low for token in ("город", "район", "адрес", "квартир", "дом", "срок", "бюдж"))
        and len([tok for tok in _FACT_TOKEN_RE.findall(low) if tok]) <= 5
    ):
        inferred_slot = "model"
    if not text:
        return {
            "_fallback": True,
            "intent": "clarify",
            "ack": "Понял.",
            "core": "Уточню запрос коротко.",
            "question": "Уточните, пожалуйста, что именно нужно сейчас?",
            "question_slot": "other",
            "required_facts": [],
            "facts_update": {},
            "blocks": [
                {"text": "Понял.", "requires": [], "type": "ack"},
                {
                    "text": "Уточните, пожалуйста, что именно нужно сейчас?",
                    "requires": [],
                    "type": "question",
                    "question_key": "intent_detail",
                },
            ],
        }
    return {
        "_fallback": True,
        "intent": "clarify",
        "ack": "Понял.",
        "core": "",
        "question": "Уточните, пожалуйста, что именно нужно сейчас?",
        "question_slot": inferred_slot,
        "required_facts": [],
        "facts_update": {},
        "blocks": [
            {"text": "Понял.", "requires": [], "type": "ack"},
            {
                "text": "Уточните, пожалуйста, что именно нужно сейчас?",
                "requires": [],
                "type": "question",
                "question_key": "intent_detail",
            },
        ],
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
    for item in (messages or []):
        if str(item.get("role") or "").strip().lower() == "system":
            chunk = str(item.get("content") or "").strip()
            if chunk:
                persona_chunks.append(chunk)
    persona_context = "\n\n".join(persona_chunks)
    if len(persona_context) > 6000:
        persona_context = persona_context[:6000]
    known_slots = dict(known_slots or {})
    known_facts = dict(known_facts or {})
    forbidden_question_topics = [str(x) for x in (forbidden_question_topics or []) if str(x).strip()]
    catalog_context = format_items_for_prompt([dict(item) for item in (grounding_items or [])[:5]], "₽")
    if not (grounding_items or []):
        catalog_context = ""
    plan_system = (
        "Ты планировщик ответа менеджера. Верни только JSON-объект. "
        "Схема: {"
        "\"intent\":\"greet|clarify|offer|answer|next_step\","
        "\"ack\":\"короткое подтверждение\","
        "\"core\":\"главная мысль ответа\","
        "\"question\":\"один уместный вопрос или пусто\","
        "\"question_slot\":\"location|object|model|budget|timeline|dimensions|contact|quantity|color|other|none\","
        "\"required_facts\": [\"city\",\"address\",\"object_type\", ...],"
        "\"facts_update\": {\"key\":\"value\", ...},"
        "\"blocks\": ["
        "{\"text\":\"фраза для ответа\",\"requires\":[\"fact_key\"],\"type\":\"ack|info|offer|question|cta\","
        "\"question_key\":\"ключ факта для question, иначе пусто\"}"
        "]"
        "}. "
        "Опирайся на правила и порядок из контекста персоны. "
        "Если в персоне задан первый квалифицирующий шаг, следуй ему. "
        "Не используй штампы: 'Спасибо, понял', 'Хороший выбор', 'Ваш запрос принят'. "
        "Не используй общую фразу 'Чем могу помочь?' как основной вопрос. "
        "Если слот уже известен, не задавай вопрос про него повторно. "
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
    forbidden_question_topics = [str(x) for x in (forbidden_question_topics or []) if str(x).strip()]
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
) -> str:
    candidate = (answer or "").strip()
    if not candidate:
        return candidate
    persona_context = ""
    for msg in (prepared_messages or []):
        if str(msg.get("role") or "").strip().lower() == "system":
            persona_context = str(msg.get("content") or "").strip()
            break
    if len(persona_context) > 5000:
        persona_context = persona_context[:5000]
    dialogue_tail = [
        {"role": str(m.get("role") or ""), "content": str(m.get("content") or "")}
        for m in (prepared_messages or [])
        if str(m.get("role") or "").strip().lower() in {"user", "assistant"}
    ][-6:]
    system_prompt = (
        "Ты QA-валидатор ответа менеджера. Проверь, что ответ следует правилам персоны и явным условиям "
        "(особенно формулировкам с 'если', 'после', 'когда'). "
        "Если в персоне есть правило вида 'после X обязательно Y', то при X в сообщении клиента "
        "ответ обязан перейти к Y, без нового базового уточнения не по правилу. "
        "Считай ошибкой старт ответа, где сначала повторяется сущность клиента (например город/район/модель), "
        "а потом идёт подтверждение. "
        "Считай ошибкой повтор того же уточняющего вопроса, который уже задавали ранее в диалоге. "
        "Считай ошибкой оценочные клише в начале ответа после выбора клиента (вместо полезного действия). "
        "Если ответ пропустил обязательный шаг, нарушил условие или звучит явно роботизированно — перепиши ответ. "
        "Верни только JSON: {\"ok\":true|false,\"rewrite\":\"...\",\"issues\":[\"...\"]}. "
        "rewrite должен быть 1-3 коротких предложения, максимум 1 вопрос."
    )
    recent_questions = []
    if isinstance(state, SalesState):
        recent_questions = [str(q or "").strip() for q in (state.asked_questions or []) if str(q or "").strip()][-8:]
    user_prompt = (
        f"Контекст персоны:\n{persona_context}\n\n"
        f"Последнее сообщение клиента: {last_user_message}\n"
        f"Ранее заданные вопросы (последние): {json.dumps(recent_questions, ensure_ascii=False)}\n"
        f"Короткая история диалога: {json.dumps(dialogue_tail, ensure_ascii=False)}\n"
        f"Кандидат ответа: {candidate}"
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
            max_tokens=260,
            response_format={"type": "json_object"},
            timeout=timeout_seconds,
        )
        choices = getattr(resp, "choices", None)
        if not (isinstance(choices, list) and choices):
            return candidate
        msg = getattr(choices[0], "message", None)
        payload = _safe_json_load(str(getattr(msg, "content", "") or ""))
        if not isinstance(payload, dict):
            return candidate
        ok = bool(payload.get("ok"))
        rewrite = str(payload.get("rewrite") or "").strip()
        if ok or not rewrite:
            return candidate
        if _rewrite_loses_context_anchors(candidate, rewrite, dialogue_tail):
            return candidate
        return rewrite
    except Exception:
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
                if raw and not raw.startswith(('#', '-', '*')):
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

    if any(token in persona_lower for token in ("коротко", "кратко", "лаконич", "brief", "concise", "short")):
        hints.style_short = True
    if any(token in persona_lower for token in ("дружелюб", "тепл", "friendly", "улыб")):
        hints.style_friendly = True
    if any(token in persona_lower for token in ("без смай", "без эмодзи", "без emoji", "без эмоджи", "без эмодзи")):
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
    if key in _STATE_CACHE:
        return _STATE_CACHE[key]
    payload = _state_store_read(key)
    if payload:
        state = SalesState.from_dict(payload)
    else:
        state = SalesState(tenant=int(tenant or 0), contact_id=int(contact_id or 0))
    _STATE_CACHE[key] = state
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
- «Здравствуйте! Подскажите, что нужно: консультация, расчёт или подбор?»

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
    photo_markers_raw = behavior.get("photo_expected_markers") or behavior.get("photo_markers") or []
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
    behavior["photo_expected_reply"] = photo_reply_raw if isinstance(photo_reply_raw, str) else str(photo_reply_raw or "")
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
        passport.get("whatsapp_link")
        or integrations.get("whatsapp_link")
        or ""
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
    mime_hint = str(
        candidate.get("mime")
        or candidate.get("mime_type")
        or candidate.get("mimetype")
        or ""
    ).strip().lower()
    extension = safe.suffix.lower()
    if type_hint and type_hint not in {"pdf", "document"}:
        return None
    if not type_hint and extension not in {".pdf", ".pdfx"} and "pdf" not in mime_hint:
        return None

    filename = str(candidate.get("original") or candidate.get("filename") or safe.name)
    mime = str(
        candidate.get("mime")
        or candidate.get("mime_type")
        or candidate.get("mimetype")
        or "application/pdf"
    ).strip() or "application/pdf"
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
        "цветвнутри",
    ],
    "finish": [
        "finish",
        "coating",
        "цветнаружнойпанели",
        "цветнаружи",
        "цветснаружи",
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
}


_FIELD_TOKEN_MAP: Dict[str, List[str]] = {
    key: sorted({_canonicalize_field_name(token) for token in tokens if token}, key=len, reverse=True)
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

    for field, tokens in _FIELD_TOKEN_MAP.items():
        if field in mapping:
            continue
        preferred_aliases = custom_aliases.get(field, [])
        normalized_tokens = [
            _canonicalize_field_name(alias) for alias in preferred_aliases if alias
        ] + list(tokens)
        column = _find_column(
            normalized_tokens,
            preferred=normalized_tokens,
            raw_names=preferred_aliases,
        )
        if column:
            mapping[field] = column

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
            if any(token in canon for token in ("цен", "price", "cost", "стоим", "руб", "uah", "usd", "eur")):
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
            if any(token in canon for token in ("name", "товар", "пози", "model", "тип", "item", "наимен")):
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
    if len(digits) >= 3 and any(tok in lowered for tok in ("руб", "uah", "eur", "usd", "$", "€", "₽")):
        return True
    try:
        # Attempt to parse decimal values like "99.5"
        normalized = text.replace(" ", "").replace(",", ".")
        float(normalized)
        return True
    except Exception:
        return False


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

    _ensure_title()
    _ensure_price()
    return normalized


def _normalize_catalog_items(items: List[Dict[str, Any]], meta: Dict[str, Any] | Any) -> List[Dict[str, Any]]:
    if not items:
        return items
    meta_dict = meta if isinstance(meta, dict) else {}
    mapping = _prepare_field_mapping(meta_dict, items)
    if not mapping:
        # Even without explicit mapping try to enrich titles and prices
        return [_normalize_catalog_item(record, {}) for record in items]
    return [_normalize_catalog_item(record, mapping) for record in items]


def _apply_catalog_attribute_rules(items: List[Dict[str, Any]], persona_meta: Mapping[str, Any] | None) -> None:
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
        matched = any(_catalog_condition_matches(item, cond) for cond in any_rules if isinstance(cond, Mapping))
    if matched and isinstance(all_rules, Sequence) and all_rules:
        matched = all(_catalog_condition_matches(item, cond) for cond in all_rules if isinstance(cond, Mapping))
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
    contains = condition.get("contains")
    if contains:
        needles = contains
        if isinstance(needles, str):
            needles = [needles]
        if isinstance(needles, Sequence):
            normalized_needles = [str(needle or "").strip().casefold() for needle in needles if needle]
            for hay in lowered:
                if any(needle in hay for needle in normalized_needles):
                    return True
    regex = condition.get("regex")
    if regex:
        patterns = regex if isinstance(regex, Sequence) and not isinstance(regex, (str, bytes)) else [regex]
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
        eq_values = equals if isinstance(equals, Sequence) and not isinstance(equals, (str, bytes)) else [equals]
        normalized = [str(val or "").strip().casefold() for val in eq_values]
        for hay in lowered:
            if hay in normalized:
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
    for field, expected in requirements.items():
        value = item.get(field)
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
    has_custom_catalogs = False
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
                        has_custom_catalogs = True
                        # Continue processing original entry as fallback (PDF/Excel)

                    raw_path = entry.get("path")
                    path = _resolve_path(raw_path)
                    if not path:
                        continue
                    merged_meta = _merge_csv_mapping_meta(entry, persona_meta)
                    candidates.append((path, merged_meta))
                    has_custom_catalogs = True
        except Exception:
            pass
        persona_csv_path = persona_catalog_csv(int(tenant))
        if persona_csv_path:
            csv_delimiter = str(persona_meta.get("catalog_csv_delimiter") or ";").strip() or ";"
            csv_encoding = str(persona_meta.get("catalog_csv_encoding") or "utf-8").strip() or "utf-8"
            meta = _merge_csv_mapping_meta(
                {
                    "type": "csv",
                    "delimiter": csv_delimiter,
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
            has_custom_catalogs = True

    if not candidates:
        default_meta = _merge_csv_mapping_meta({"delimiter": ",", "encoding": "utf-8"}, persona_meta)
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
                key_fps.append((str(stat_target.resolve()), st.st_mtime, int(getattr(st, 'st_size', 0) or 0)))
    except Exception:
        key_fps = []
    cache_key: Tuple[Optional[int], Tuple[Tuple[str, float, int], ...]] = (
        (int(tenant) if tenant is not None else None), tuple(sorted(key_fps))
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
                            rel_index_path = str(index_path_obj.relative_to(tenant_dir(int(tenant))))
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
                    with open(path, "r", encoding=enc, newline="") as fh:
                        local_delimiter = delimiter
                        if not local_delimiter:
                            sample = fh.read(2048)
                            fh.seek(0)
                            try:
                                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                                local_delimiter = dialect.delimiter
                            except Exception:
                                local_delimiter = ","

                        reader = csv.reader(fh, delimiter=local_delimiter or ",")
                        header: List[str] = []
                        for raw_header in reader:
                            if not raw_header or not any((cell or "").strip() for cell in raw_header):
                                continue
                            header = raw_header
                            break
                        if not header:
                            continue

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
                            if not row or not any((val.strip() if isinstance(val, str) else str(val or "").strip()) for val in row):
                                continue
                            while len(columns) < len(row):
                                columns.append(f"column_{len(columns) + 1}")
                            record: Dict[str, Any] = {}
                            for idx_col, value in enumerate(row):
                                key = columns[idx_col]
                                if isinstance(value, str):
                                    clean = value.strip()
                                else:
                                    clean = str(value or "").strip()
                                record[key] = clean
                            if any(record.values()):
                                local_items.append(record)
                            if len(local_items) >= 500:
                                break
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
                parts: List[str] = []
                for k in (
                    "title","name","sku","id","brand","collection","category","series","model","color","material","decor","finish","tags","description","notes","features",
                ):
                    v = it.get(k)
                    if isinstance(v, (list, tuple, set)):
                        parts.extend(str(x) for x in v if x)
                    elif v:
                        parts.append(str(v))
                it["_search_text"] = (" ".join(parts)).casefold().replace("ё", "е")
    except Exception:
        pass

    # Store in cache
    try:
        _CATALOG_CACHE[cache_key] = items
    except Exception:
        pass

    return items


def read_all_catalog(cfg: Optional[Dict[str, Any]] = None, tenant: int | None = None) -> List[Dict[str, Any]]:
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

SIZE_PATTERN = re.compile(r"(?P<value>\d{2,4})(?:\s|\-)?(?P<unit>см|mm|мм|cm|м|kg|кг|g|гр|ml|мл|l|л)", re.IGNORECASE)


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


def infer_user_needs(text: str) -> Dict[str, Any]:
    raw = text or ""
    lowered = raw.lower()
    needs: Dict[str, Any] = {}

    tokens = _tokenize_query(raw)
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

    for stem, title in COLOR_STEMS.items():
        if stem in lowered:
            needs["color"] = title
            break

    if "квартир" in lowered:
        needs["object_type"] = "apartment"
    elif "частн" in lowered and "дом" in lowered:
        needs["object_type"] = "house"
    elif "этаж" in lowered:
        # Heuristic: floor mention most often indicates apartment context.
        needs["object_type"] = "apartment"

    if _NOISE_NEED_RE.search(lowered):
        needs["noise_priority"] = True
    if _INSULATION_NEED_RE.search(lowered):
        needs["insulation_priority"] = True

    return needs


def _value_matches(item: Dict[str, Any], fields: Tuple[str, ...], needle: str) -> bool:
    for field in fields:
        val = item.get(field)
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
        if _value_matches(item, ("size", "width", "dimensions", "length", "height", "depth"), size_str):
            s += 1.5

    color_tokens = needs.get("_color_tokens") or []
    if color_tokens:
        item_color_aliases = set(str(alias) for alias in (item.get("_color_aliases") or []) if alias)
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


def _collect_item_text(item: Dict[str, Any]) -> str:
    cached = item.get("_search_text")
    if isinstance(cached, str) and cached:
        return cached
    parts: List[str] = []
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
    tags = item.get("tags")
    if not tags:
        return 0.0
    if isinstance(tags, str):
        normalized = _normalize_text(tags)
        tags_iterable = [normalized]
    else:
        tags_iterable = [_normalize_text(tag) for tag in tags if tag]
    bonus = 0.0
    for tag in tags_iterable:
        if "хит" in tag:
            bonus += 0.4
        if "новин" in tag:
            bonus += 0.2
        if "склад" in tag:
            bonus += 0.1
    return bonus


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
    wants_noise = bool(_NOISE_NEED_RE.search(query_low) or needs.get("noise_priority"))
    wants_insulation = bool(_INSULATION_NEED_RE.search(query_low) or needs.get("insulation_priority"))
    object_type = str(needs.get("object_type") or "").strip().lower()

    def _item_has_outer_mdf(item: Mapping[str, Any]) -> bool:
        for raw_key, raw_val in item.items():
            key = _normalize_text(raw_key)
            val = _normalize_text(raw_val)
            if not key or not val:
                continue
            if "мдф" in key and ("снаруж" in key or "наруж" in key):
                if re.search(r"\d", val):
                    return True
        return False

    def _item_has_thermal_break(item: Mapping[str, Any]) -> bool:
        hay = _normalize_text(_collect_item_text(dict(item)))
        if any(token in hay for token in ("термо", "терма", "винарит", "арктик")):
            return True
        for raw_key, raw_val in item.items():
            key = _normalize_text(raw_key)
            val = _normalize_text(raw_val)
            if "терморазрыв" in key and val and val not in {"нет", "0", "false"}:
                return True
        return False

    def _noise_preference_score(item: Dict[str, Any]) -> float:
        if not wants_noise and not wants_insulation:
            return 0.0
        score = 0.0
        hay = _normalize_text(_collect_item_text(item))
        has_noise_feature = False
        has_outer_mdf = _item_has_outer_mdf(item)
        has_thermal_break = _item_has_thermal_break(item)
        if has_outer_mdf:
            score += 1.8
            has_noise_feature = True
        if any(token in hay for token in ("шумо", "шумк", "тих", "акуст")):
            score += 2.5
            has_noise_feature = True
        contour_val: int | None = None
        has_mdf_panel = has_outer_mdf
        for raw_key, raw_val in item.items():
            key = _normalize_text(raw_key)
            val = _normalize_text(raw_val)
            if not key or not val:
                continue
            if "мдф" in key and re.search(r"\d", val):
                score += 1.2
                has_noise_feature = True
                has_mdf_panel = True
            if "контур" in key and "уплотн" in key:
                digits = re.findall(r"\d+", val)
                if digits:
                    try:
                        contour_val = int(digits[0])
                        if contour_val >= 2:
                            score += 1.0
                            has_noise_feature = True
                    except Exception:
                        pass
            if "толщина полотна" in key:
                digits = re.findall(r"\d+", val)
                if digits:
                    try:
                        if int(digits[0]) >= 70:
                            score += 0.8
                            has_noise_feature = True
                    except Exception:
                        pass
        if wants_insulation:
            if has_outer_mdf:
                score += 2.2
            else:
                score -= 5.0
            # For apartments, prefer warm two-panel doors without thermal-break bias.
            if object_type == "apartment" and has_thermal_break:
                score -= 0.8
        # If user asked for quiet/noise and item has no noise-related attributes,
        # down-rank it so silent-focused models come first.
        if not has_noise_feature:
            score -= 8.0
        if contour_val is not None and contour_val <= 1 and not has_mdf_panel:
            score -= 2.0
        return score

    def _total_score(item: Dict[str, Any]) -> float:
        base = _score(item, needs)
        matched = _text_match_score(item, query_tokens)
        tag_bonus = _tag_boost(item)
        noise_bonus = _noise_preference_score(item)
        return base + matched + tag_bonus + noise_bonus

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


def search_catalog(
    needs: Dict[str, Any],
    limit: int = 5,
    tenant: int | None = None,
    query: str | None = None,
) -> List[Dict[str, Any]]:
    needs = dict(needs or {})
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
        return "— подходящих позиций не найдено."
    out = []
    for idx, it in enumerate(items, start=1):
        title = (
            it.get("title")
            or it.get("name")
            or it.get("sku")
            or it.get("id")
            or f"Позиция {idx}"
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
        return "не распознано"
    parts = []
    for k in ["type", "width", "color", "budget_max"]:
        if k in needs:
            parts.append(f"{k}={needs[k]}")
    return ", ".join(parts) if parts else "не распознано"

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
        "Подскажите предел по стоимости, чтобы держать баланс цена/качество?",
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
            return "Рада снова помочь — учитываю прошлые пожелания."
        return ""

    def _format_price(self, price: Optional[int], currency: str) -> str:
        if price is None:
            return "цена по запросу"
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
            return "Укладывается в ваш бюджет и закрывает задачу без скрытых доплат."
        keywords = needs.get("keywords") or []
        if keywords:
            return f"Помогает с {keywords[0]} и экономит время на выборе." if keywords else "Подходит под ваш запрос."
        focus = needs.get("focus") or needs.get("type")
        if focus:
            return f"Даёт готовое решение по направлению «{focus}»."
        return "Экономит время и даёт предсказуемый результат."

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
            question = template.format(currency=currency, focus=focus, city=self.branding.get("CITY", ""))
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
        template = SOCIAL_PROOF_TEMPLATES[self.state.social_proof_cursor % len(SOCIAL_PROOF_TEMPLATES)]
        self.state.social_proof_cursor += 1
        return template.format(brand=self.branding.get("BRAND", "Бренд"), city=self.branding.get("CITY", ""))

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
        stock_text = "несколько" if not stock_values else (str(min(stock_values)) if min(stock_values) > 0 else "последние")
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
        handoff = (self.cfg.get("cta") or {}).get("handoff_wa") if isinstance(self.cfg, dict) else ""
        sentiment = self.state.sentiment_score
        if sentiment <= -1.2:
            soothing = "Могу предложить более мягкий вариант или подключить эксперта по нюансам. Продолжим подбор?"
            return soothing
        if score >= 1.5 and timeline:
            return f"Зафиксирую запуск на {timeline}. Подтверждаем — оформляю?"
        if sentiment >= 1.2 and score >= 1.0:
            return "Готов сразу оформить заказ и зафиксировать цену. Даем старт?"
        if score <= -1:
            return "Могу предложить более бюджетный пакет или рассрочку. Продолжаем подбор?"
        if self.channel_name.lower() == "avito" and handoff:
            return handoff
        candidate = cta_primary or cta_fallback or "Готов подключиться и довести до заказа — двигаемся?"
        return candidate

    def _personalized_greeting(self) -> str:
        default_greeting = f"Здравствуйте! Меня зовут {self.branding.get('AGENT_NAME', 'Менеджер')}, {self.branding.get('BRAND', '')}."
        greeting = (self.persona_hints.greeting or default_greeting).strip()
        if not greeting:
            greeting = default_greeting
        friendly = False
        visits = int((self.state.profile or {}).get("visits", 0))
        if visits > 1:
            addon = "Рады снова вас видеть и продолжить подбор."
            if addon not in greeting:
                if greeting.endswith(('.', '!', '…')):
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
            return f"Помню ваши предпочтения по {joined} — покажу то, что действительно откликается."

        visits = int(profile.get("visits", 0) or 0)
        if visits > 1:
            return "Учитываю прошлые диалоги и подберу обновлённые варианты."
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
        max_questions_cfg = int((self.cfg.get("behavior", {}) or {}).get("max_clarifying_questions", 1))
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

        cleaned = [message_parts[key].strip() for key in ordered_keys if message_parts[key] and message_parts[key].strip()]
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
        spin_parts = [f"{stage.upper()}={self.state.spin.get(stage, 'pending')}" for stage in ("s", "p", "i", "n")]
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
    engine = SalesConversationEngine(state, brand, cfg, channel_name, persona_hints=hints)
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
    engine = SalesConversationEngine(state, branding, cfg, channel_name, persona_hints=persona_hints)
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
        if state.pending_slot:
            _capture_pending_slot_answer(state, user_text)
        _maybe_store_model_slot(state, tenant, user_text)
        needs_update = infer_user_needs(user_text)
        if needs_update:
            if not isinstance(state.needs, dict):
                state.needs = {}
            for key, value in needs_update.items():
                if value in (None, "", [], {}, ()):
                    continue
                if key == "keywords":
                    existing = [str(x) for x in (state.needs.get("keywords") or []) if str(x).strip()]
                    for token in value if isinstance(value, list) else [value]:
                        token_str = str(token).strip()
                        if token_str and token_str not in existing:
                            existing.append(token_str)
                    if existing:
                        state.needs["keywords"] = existing[:8]
                    continue
                state.needs[key] = value
                # Promote stable inferred facts into fact memory to avoid repeated clarifications.
                canonical = _canonical_fact_key(key)
                if canonical in {"city", "object_type"}:
                    if not isinstance(state.facts, dict):
                        state.facts = {}
                    current_val = str(state.facts.get(canonical) or "").strip()
                    candidate_val = _safe_short_text(str(value), 80)
                    if canonical == "city":
                        if not _is_plausible_city_text(candidate_val):
                            continue
                        if (not current_val) or (not _is_plausible_city_text(current_val)):
                            state.facts[canonical] = candidate_val
                    else:
                        if not current_val:
                            state.facts[canonical] = candidate_val
        city_hint = _extract_city_hint(user_text)
        if city_hint and _is_plausible_city_text(city_hint):
            if not isinstance(state.facts, dict):
                state.facts = {}
            city_val = _safe_short_text(city_hint, limit=80)
            existing_city = str(state.facts.get("city") or "").strip()
            if (not existing_city) or (not _is_plausible_city_text(existing_city)):
                state.facts["city"] = city_val
            if state.pending_fact_key == "city":
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
        "- Не закрывай диалог пустой фразой, всегда давай следующий полезный шаг.\n"
        "- Если факт/срок не подтвержден, честно скажи, что уточняешь."
    )
    system_blocks.append(_HUMAN_STYLE_FEW_SHOT)
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
            system_blocks.append(
                "Релевантные позиции каталога:\n"
                f"{catalog_block}"
            )

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

    __slots__ = ("llm_plan", "llm_raw_answer", "llm_refined")

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
        obj.llm_refined = content
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
    return LLMReply(text, plan=plan_payload, raw_answer=raw_answer if raw_answer is not None else text)


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
            answer = "\n\n".join([f"Вариант {idx}: {text}" for idx, text in enumerate(variants, start=1)])
        dummy_plan = planner.GeneratedPlan()
        enforcement_ctx = _make_enforcement_context(state, persona_hints, channel_name)
        existing_fp = set(enforcement_ctx.asked_fingerprints)
        refined_answer = quality.enforce_plan_alignment(
            answer,
            dummy_plan,
            persona_hints,
            context=enforcement_ctx,
        )
        if not refined_answer.strip():
            refined_answer = answer
        refined_answer = _humanize_reply_text(refined_answer, state=state, persona_hints=persona_hints)
        refined_answer = _drop_repeated_questions_from_reply(refined_answer, state)
        persona_context = ""
        if messages and str(messages[0].get("role") or "").strip().lower() == "system":
            persona_context = str(messages[0].get("content") or "")
        persona_rules_context = _resolve_persona_rules_context(
            tenant=tenant,
            channel_name=channel_name,
            fallback_context=persona_context,
        )
        known_facts = _state_facts_snapshot(state)
        refined_answer = _apply_persona_sequence_obligations(
            refined_answer,
            persona_context=persona_rules_context,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        refined_answer = _apply_persona_delivery_obligations(
            refined_answer,
            persona_context=persona_rules_context,
            channel_name=channel_name,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        refined_answer = _drop_repeated_questions_from_reply(refined_answer, state)
        refined_answer = _enforce_sentence_budget(refined_answer, max_sentences=3)
        _apply_plan_alignment_to_state(state, enforcement_ctx, existing_fp)
        _remember_questions_from_reply(state, refined_answer)
        save_sales_state(state)
        result = _wrap_llm_reply(refined_answer, plan=dummy_plan, raw_answer=answer)
        record_bot_reply(contact_ref, tenant, channel_name, str(result))
        return result
    except APITimeoutError as exc:
        logger.warning("direct llm timeout: %s", exc)
    except Exception as exc:
        if _is_quota_or_rate_limit_error(exc):
            logger.warning("direct llm quota/rate limited, fallback enabled")
        else:
            logger.exception("direct llm call failed", exc_info=exc)

    fallback = _safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name=channel_name,
        contact_ref=contact_ref,
        last_user_message=last_user_message,
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
        human_messages = _build_human_mode_messages(messages)
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
        answer = quality.enforce_plan_alignment(
            answer,
            plan,
            persona_hints,
        )
        refined = _humanize_reply_text(answer, state=state, persona_hints=persona_hints)
        refined = _drop_repeated_questions_from_reply(refined, state)
        persona_context = ""
        if human_messages and str(human_messages[0].get("role") or "").strip().lower() == "system":
            persona_context = str(human_messages[0].get("content") or "")
        persona_rules_context = _resolve_persona_rules_context(
            tenant=tenant,
            channel_name=channel_name,
            fallback_context=persona_context,
        )
        known_facts = _state_facts_snapshot(state)
        refined = _apply_persona_sequence_obligations(
            refined,
            persona_context=persona_rules_context,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        refined = _apply_persona_delivery_obligations(
            refined,
            persona_context=persona_rules_context,
            channel_name=channel_name,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        refined = _drop_repeated_questions_from_reply(refined, state)
        refined = _enforce_sentence_budget(refined, max_sentences=3)
        _remember_questions_from_reply(state, refined)
        save_sales_state(state)
        result = _wrap_llm_reply(refined, plan=plan.to_dict(), raw_answer=answer)
        record_bot_reply(contact_ref, tenant, channel_name, str(result))
        return result
    except APITimeoutError as exc:
        logger.warning("human llm timeout: %s", exc)
    except Exception as exc:
        if _is_quota_or_rate_limit_error(exc):
            logger.warning("human llm quota/rate limited, fallback enabled")
        else:
            logger.exception("human llm failed", exc_info=exc)
    fallback = _safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name=channel_name,
        contact_ref=contact_ref,
        last_user_message=last_user_message,
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
    """
    Unified reply pipeline:
    one LLM call path for all tenants/channels to simplify behavior tuning.
    """
    try:
        create_fn = _resolve_chat_completion_callable(client)
        if not create_fn:
            raise RuntimeError("openai client missing chat.completions.create")
        prepared_messages = _build_human_mode_messages(messages)
        grounding = _build_reply_grounding(tenant=tenant, state=state, user_text=last_user_message)
        known_slots = dict(state.known_slots or {})
        known_facts = _state_facts_snapshot(state)
        turn_intent = _classify_turn_intent(last_user_message, known_facts=known_facts)
        if turn_intent == "offtopic":
            refined = "Давайте вернемся к вашему запросу. Что нужно по товару или услуге?"
            refined = _humanize_reply_text(refined, state=state, persona_hints=persona_hints)
            refined = _drop_repeated_questions_from_reply(refined, state)
            state.pending_slot = ""
            state.pending_fact_key = ""
            state.last_plan = {"intent": "offtopic_redirect"}
            _remember_questions_from_reply(state, refined)
            save_sales_state(state)
            result = _wrap_llm_reply(refined, plan={"intent": "offtopic_redirect"}, raw_answer=refined)
            record_bot_reply(contact_ref, tenant, channel_name, str(result))
            return result
        semantic = await _semantic_plan(
            create_fn,
            model=settings.OPENAI_MODEL,
            timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
            messages=prepared_messages,
            last_user_message=last_user_message,
            known_slots=known_slots,
            known_facts=known_facts,
            forbidden_question_topics=grounding.get("forbid_question_topics") or [],
            grounding_items=grounding.get("items") or [],
        )
        semantic_fallback = bool(semantic.get("_fallback"))
        if semantic_fallback:
            answer = await _render_direct_reply(
                create_fn,
                model=settings.OPENAI_MODEL,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                prepared_messages=prepared_messages,
            )
            if not answer:
                raise RuntimeError("empty llm direct fallback render")
            answer = _enforce_catalog_price_grounding(answer, grounding=grounding)
            answer = _dedupe_repeated_fact_sentences(answer, state)
            refined = _humanize_reply_text(answer, state=state, persona_hints=persona_hints)
            refined = _enforce_catalog_price_grounding(refined, grounding=grounding)
            refined = _dedupe_repeated_fact_sentences(refined, state)
            refined = await _audit_and_rewrite_persona_reply(
                create_fn,
                model=settings.OPENAI_MODEL,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                prepared_messages=prepared_messages,
                answer=refined,
                last_user_message=last_user_message,
                state=state,
            )
            refined = _humanize_reply_text(refined, state=state, persona_hints=persona_hints)
            refined = _enforce_catalog_price_grounding(refined, grounding=grounding)
            refined = _dedupe_repeated_fact_sentences(refined, state)
            refined = _drop_repeated_questions_from_reply(refined, state)
            refined = _ensure_urgent_same_day_ack(refined, last_user_message)
            refined = _ensure_eta_guidance(refined, last_user_message)
            refined = _enforce_catalog_truth_guard(
                refined,
                grounding=grounding,
                user_text=last_user_message,
            )
            refined = _enforce_catalog_truth_guard(
                refined,
                grounding=grounding,
                user_text=last_user_message,
            )
            refined = _normalize_catalog_name_case(refined, grounding=grounding)
            refined = _humanize_reply_text(refined, state=state, persona_hints=persona_hints)
            persona_context = ""
            if prepared_messages and str(prepared_messages[0].get("role") or "") == "system":
                persona_context = str(prepared_messages[0].get("content") or "")
            persona_rules_context = _resolve_persona_rules_context(
                tenant=tenant,
                channel_name=channel_name,
                fallback_context=persona_context,
            )
            refined = _apply_persona_sequence_obligations(
                refined,
                persona_context=persona_rules_context,
                last_user_message=last_user_message,
                known_facts=known_facts,
                state=state,
            )
            known_facts = _state_facts_snapshot(state)
            refined, forced_required_key = _enforce_next_required_fact_question(
                refined,
                state=state,
                persona_context=persona_rules_context,
                known_facts=known_facts,
                user_text=last_user_message,
                grounding=grounding,
            )
            refined = _ensure_dialog_greeting_on_first_reply(
                refined,
                state,
                persona_context=persona_rules_context,
            )
            refined = _enforce_sentence_budget(refined, max_sentences=3)
            refined = _apply_persona_delivery_obligations(
                refined,
                persona_context=persona_rules_context,
                channel_name=channel_name,
                last_user_message=last_user_message,
                known_facts=known_facts,
                state=state,
            )
            refined = _drop_repeated_questions_from_reply(refined, state)
            state.pending_slot = ""
            if forced_required_key and _extract_questions_from_text(refined):
                state.pending_fact_key = _canonical_fact_key(forced_required_key)
            else:
                state.pending_fact_key = ""
            state.last_plan = dict(semantic or {})
            _update_fact_memory(state, refined)
            _remember_questions_from_reply(state, refined)
            save_sales_state(state)
            result = _wrap_llm_reply(refined, plan=semantic, raw_answer=answer)
            record_bot_reply(contact_ref, tenant, channel_name, str(result))
            return result
        semantic = _enforce_semantic_plan_guards(semantic, state=state, grounding=grounding)
        facts_update = semantic.get("facts_update")
        if isinstance(facts_update, Mapping):
            _merge_fact_updates(state, facts_update)
            known_facts = _state_facts_snapshot(state)
        block_requires_override: Dict[int, List[str]] = {}
        semantic_blocks = semantic.get("blocks")
        persona_context = ""
        if prepared_messages and str(prepared_messages[0].get("role") or "") == "system":
            persona_context = str(prepared_messages[0].get("content") or "")
        persona_rules_context = _resolve_persona_rules_context(
            tenant=tenant,
            channel_name=channel_name,
            fallback_context=persona_context,
        )
        required_facts = _normalize_required_facts(semantic.get("required_facts"))
        persona_required = _required_facts_from_persona_text(persona_rules_context)
        for fact_key in persona_required:
            canonical = _canonical_fact_key(fact_key)
            if canonical and canonical not in required_facts:
                required_facts.append(canonical)
        if isinstance(semantic_blocks, list) and semantic_blocks:
            block_requires_override = await _audit_policy_block_requirements(
                create_fn,
                model=settings.OPENAI_MODEL,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                persona_context=persona_context,
                blocks=semantic_blocks,
                known_facts=known_facts,
                last_user_message=last_user_message,
            )
            block_allowance_override = await _audit_policy_block_allowance(
                create_fn,
                model=settings.OPENAI_MODEL,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                persona_context=persona_context,
                blocks=semantic_blocks,
                known_facts=known_facts,
                last_user_message=last_user_message,
            )
        else:
            block_allowance_override = {}
        semantic["_audited_requires"] = dict(block_requires_override or {})
        semantic["_audited_allowance"] = dict(block_allowance_override or {})
        semantic["required_facts"] = list(required_facts)
        composed_answer, next_question_key = _compose_reply_from_policy_blocks(
            semantic,
            state=state,
            known_facts=known_facts,
            required_facts=required_facts,
            block_requires_override=block_requires_override,
            block_allowance_override=block_allowance_override,
        )
        answer = composed_answer
        if not answer:
            answer = await _render_from_semantic_plan(
                create_fn,
                model=settings.OPENAI_MODEL,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                prepared_messages=prepared_messages,
                plan=semantic,
                known_slots=known_slots,
                forbidden_question_topics=grounding.get("forbid_question_topics") or [],
            )
        if not answer:
            raise RuntimeError("empty llm render")
        answer = _enforce_catalog_price_grounding(answer, grounding=grounding)
        answer = _dedupe_repeated_fact_sentences(answer, state)
        if not _render_passes_rubric(answer, state):
            retry_system = (
                "Перепиши ответ по-человечески. Коротко, без шаблонов, максимум 1 вопрос. "
                "Запрещены фразы: 'Спасибо, понял', 'Хороший выбор', 'Ваш запрос принят'. "
                "Не задавай повторный вопрос по уже известным данным."
            )
            retry_messages = list(prepared_messages)
            retry_messages.append({"role": "system", "content": retry_system})
            retry_messages.append(
                {
                    "role": "user",
                    "content": "План ответа (JSON): " + json.dumps(semantic, ensure_ascii=False),
                }
            )
            retry_resp = await _llm_call_with_deadline(
                create_fn,
                timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                model=settings.OPENAI_MODEL,
                messages=retry_messages,
                max_tokens=180,
                temperature=settings.OPENAI_TEMPERATURE,
                top_p=0.9,
                frequency_penalty=0.06,
                presence_penalty=0.03,
                timeout=settings.OPENAI_TIMEOUT_SECONDS,
            )
            retry_choices = getattr(retry_resp, "choices", None)
            if isinstance(retry_choices, list) and retry_choices:
                retry_msg = getattr(retry_choices[0], "message", None)
                retry_answer = str(getattr(retry_msg, "content", "") or "").strip()
                if retry_answer:
                    answer = _enforce_catalog_price_grounding(retry_answer, grounding=grounding)
                    answer = _dedupe_repeated_fact_sentences(answer, state)
        refined = _humanize_reply_text(answer, state=state, persona_hints=persona_hints)
        refined = _enforce_catalog_price_grounding(refined, grounding=grounding)
        refined = _dedupe_repeated_fact_sentences(refined, state)
        refined = await _audit_and_rewrite_persona_reply(
            create_fn,
            model=settings.OPENAI_MODEL,
            timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
            prepared_messages=prepared_messages,
            answer=refined,
            last_user_message=last_user_message,
            state=state,
        )
        refined = _humanize_reply_text(refined, state=state, persona_hints=persona_hints)
        refined = _enforce_catalog_price_grounding(refined, grounding=grounding)
        refined = _dedupe_repeated_fact_sentences(refined, state)
        refined = _drop_repeated_questions_from_reply(refined, state)
        refined = _ensure_urgent_same_day_ack(refined, last_user_message)
        refined = _ensure_eta_guidance(refined, last_user_message)
        refined = _enforce_catalog_truth_guard(
            refined,
            grounding=grounding,
            user_text=last_user_message,
        )
        refined = _enforce_catalog_truth_guard(
            refined,
            grounding=grounding,
            user_text=last_user_message,
        )
        refined = _normalize_catalog_name_case(refined, grounding=grounding)
        refined = _humanize_reply_text(refined, state=state, persona_hints=persona_hints)
        refined = _apply_persona_sequence_obligations(
            refined,
            persona_context=persona_rules_context,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        refined, forced_required_key = _enforce_next_required_fact_question(
            refined,
            state=state,
            persona_context=persona_rules_context,
            known_facts=known_facts,
            user_text=last_user_message,
            grounding=grounding,
        )
        refined = _ensure_dialog_greeting_on_first_reply(
            refined,
            state,
            persona_context=persona_rules_context,
        )
        refined = _enforce_sentence_budget(refined, max_sentences=3)
        refined = _apply_persona_delivery_obligations(
            refined,
            persona_context=persona_rules_context,
            channel_name=channel_name,
            last_user_message=last_user_message,
            known_facts=known_facts,
            state=state,
        )
        refined = _drop_repeated_questions_from_reply(refined, state)
        actual_questions = _extract_questions_from_text(refined)
        question_text = str(semantic.get("question") or "").strip()
        question_slot = _normalize_slot_name(str(semantic.get("question_slot") or ""), question=question_text)
        if actual_questions:
            if question_text and question_slot and question_slot not in {"none", "other"}:
                state.pending_slot = question_slot
            else:
                state.pending_slot = ""
            if forced_required_key:
                state.pending_fact_key = _canonical_fact_key(forced_required_key)
            else:
                state.pending_fact_key = _canonical_fact_key(next_question_key) if next_question_key else ""
            _remember_questions_from_reply(state, refined)
        else:
            state.pending_slot = ""
            state.pending_fact_key = ""
        state.last_plan = dict(semantic or {})
        _update_fact_memory(state, refined)
        save_sales_state(state)
        result = _wrap_llm_reply(refined, plan=semantic, raw_answer=answer)
        record_bot_reply(contact_ref, tenant, channel_name, str(result))
        return result
    except APITimeoutError as exc:
        logger.warning("single llm timeout: %s", exc)
    except Exception as exc:
        if _is_quota_or_rate_limit_error(exc):
            logger.warning("single llm quota/rate limited, fallback enabled")
        else:
            logger.exception("single llm failed", exc_info=exc)
    fallback = _safe_minimal_fallback_reply(
        tenant=tenant,
        channel_name=channel_name,
        contact_ref=contact_ref,
        last_user_message=last_user_message,
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

    channel_name = (channel or "whatsapp")
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
        fallback = _safe_minimal_fallback_reply(
            tenant=tenant,
            channel_name=channel_name,
            contact_ref=contact_ref,
            last_user_message=last,
        )
        return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)

    try:
        openai.api_key = settings.OPENAI_API_KEY  # type: ignore
        persona_hints = load_persona_hints(tenant, channel_name)
        state = load_sales_state(tenant, contact_ref)
        tenant_cfg = None
        if tenant is not None:
            try:
                tenant_cfg = load_tenant(int(tenant))
            except Exception:
                tenant_cfg = None
        if last:
            if state.pending_fact_key:
                _capture_pending_fact_answer(state, last)
            if state.pending_slot:
                _capture_pending_slot_answer(state, last)
            _maybe_store_model_slot(state, tenant, last)
            save_sales_state(state)
        known_facts = _state_facts_snapshot(state)
        if _classify_turn_intent(last, known_facts=known_facts) == "offtopic":
            redirect = "Давайте вернемся к вашему запросу. Что нужно по товару или услуге?"
            redirect = _humanize_reply_text(redirect, state=state, persona_hints=persona_hints)
            redirect = _drop_repeated_questions_from_reply(redirect, state)
            redirect = _enforce_sentence_budget(redirect, max_sentences=3)
            state.pending_slot = ""
            state.pending_fact_key = ""
            state.last_plan = {"intent": "offtopic_redirect"}
            _remember_questions_from_reply(state, redirect)
            save_sales_state(state)
            result = _wrap_llm_reply(redirect, plan={"intent": "offtopic_redirect"}, raw_answer=redirect)
            record_bot_reply(contact_ref, tenant, channel_name, str(result))
            return result
        brain_mode = _resolve_brain_mode(tenant, tenant_cfg)
        if brain_mode == "classic":
            return await _human_llm_reply(
                client,
                messages,
                persona_hints,
                state,
                channel_name,
                contact_ref,
                tenant,
                last,
            )
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
        fallback = _safe_minimal_fallback_reply(
            tenant=tenant,
            channel_name=channel_name,
            contact_ref=contact_ref,
            last_user_message=last,
        )
        return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)


__all__ = [
    "Settings", "settings",
    "tenant_config", "tenant_waweb_url", "tenant_whatsapp_provider",
    "ADMIN_COOKIE",
    "get_tenant_pubkey", "set_tenant_pubkey",
    "http_json",
    "tenant_dir", "ensure_tenant_files",
    "read_tenant_config", "write_tenant_config",
    "read_persona", "write_persona",
    "load_tenant", "load_persona", "PersonaHints", "extract_persona_hints", "load_persona_hints",
    "load_persona_structured", "persona_meta_config", "persona_catalog_pdf", "persona_catalog_csv",
    "resolve_catalog_pdf_meta",
    "build_llm_messages", "ask_llm",
    # helpers ниже могут понадобиться в других частях
    "infer_user_needs", "search_catalog", "format_needs_for_prompt",
    "format_items_for_prompt", "pick_cta",
    "load_sales_state", "save_sales_state", "observe_user_message",
    "record_bot_reply", "summarize_sales_state",
    "read_all_catalog", "paginate_catalog_text",
]
