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


_PERSONA_HINTS_CACHE: Dict[int | None, Tuple[str, PersonaHints]] = {}


def load_persona_hints(tenant: int | None = None) -> PersonaHints:
    persona_text = load_persona(tenant)
    fingerprint = hashlib.sha1(persona_text.encode("utf-8")).hexdigest() if persona_text else ""
    key: int | None
    try:
        key = int(tenant) if tenant is not None else None
    except Exception:
        key = None
    cached = _PERSONA_HINTS_CACHE.get(key)
    if cached and cached[0] == fingerprint:
        return cached[1]
    hints = extract_persona_hints(persona_text)
    _PERSONA_HINTS_CACHE[key] = (fingerprint, hints)
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

PERSONA_DEFAULT_PATH = pathlib.Path(__file__).resolve().parent.parent / "agents" / "persona_default_ru.md"
try:
    DEFAULT_PERSONA_MD = PERSONA_DEFAULT_PATH.read_text(encoding="utf-8")
except Exception:
    DEFAULT_PERSONA_MD = (
        "Контекст: {AGENT_NAME} из {BRAND}, {CITY}. Канал: {CHANNEL}. Цель: быстрый переход к покупке. Минимум трения. Максимум ясности.\n\n"
        "Базовые правила:\n"
        "-Обращение на «Вы». Тон — вежливый, деловой, дружелюбный.\n"
        "-Сообщение: 1–3 коротких предложения или список на 3–5 пунктов.\n"
        "-За сообщение: ≤1 вопрос, ≤1 CTA.\n"
        "-Фокус на выгодах, не на характеристиках.\n"
        "-Без повторов вступлений и одинаковых товаров подряд.\n"
        "-Не выдавать себя за ИИ. Честность, без ложного дефицита.\n\n"
        "Алгоритм диалога:\n"
        "-Приветствие → потребность\n"
        "«Здравствуйте. Чем помочь? Что важно в {категория}? бюджет/сроки/использование?»\n"
        "-Квалификация (1–2 уточнения)\n"
        "«Правильно понимаю: для {цель}, бюджет ~{сумма}?»\n"
        "-Предложение (до 3 вариантов)\n"
        "Шаблон варианта: Название → 2–3 выгоды → ориентир цены → соцдоказательство/гарантия → CTA.\n"
        "-Возражения → снятие сомнений\n"
        "Эмпатия → факт/выгода/гарантия/альтернатива → мини-рекап → CTA.\n"
        "-Закрытие\n"
        "«Готовы оформить? Нужны ФИО, телефон, адрес/самовывоз, способ оплаты.»\n"
        "-Мягкая допродажа (1 попытка)\n"
        "Только релевантный доп. Выгода в 1 фразе. Опциональность.\n"
        "-Фоллоу-ап, если тишина\n"
        "1 напоминание с новой формулировкой и ценностью.\n\n"
        "Паттерны сообщений:\n"
        "-Приветствие:\n"
        "«Здравствуйте. Помогу выбрать {категория}. Что для вас важнее: цена, качество или сроки?»\n"
        "-Уточнение:\n"
        "«Для кого/какой задачи берёте? Есть ориентир по бюджету?»\n\n"
        "Предложение товара/услуги:\n"
        "-«Рекомендую {Модель/Услуга A}. Получите: {выгода1}, {выгода2}. Сейчас {цена}{CURRENCY}. Отзывы 4.8/5. Оформим?»\n"
        "«Если надо дешевле — {B}: ключевое отличие {…}. Если мощнее — {C}: добавите {…}. Что выбираем?»\n\n"
        "Каталог по запросу:\n"
        "«Топ-позиции по вашему запросу:\n"
        "{A} — {2 выгоды}\n"
        "{B} — {2 выгоды}\n"
        "{C} — {2 выгоды}\n"
        "Полный каталог: {CATALOG_URL}. Нужна отправка лучших в чат с фото?»\n\n"
        "Возражения:\n"
        "-Цена: «Понимаю. Здесь платите за {ключевая ценность}, в итоге экономите на {издержка}. Есть рассрочка/акция {…}. Оформим по спеццене сегодня?»\n"
        "-Качество/доверие: «Понимаю. Сертификация {…}, гарантия {…}, отзывы {…}. Этого достаточно, чтобы решиться?»\n"
        "-«Надо подумать»: «С чем сомневаетесь: цена/срок/функции? Отвечу точечно, чтобы решить верно.»\n\n"
        "Закрытие:\n"
        "-«Готовы оформить? Напишите ФИО, телефон, адрес/самовывоз, удобную оплату. Сразу зафиксирую цену.»\n\n"
        "Допродажа:\n"
        "-«К этому {товару} обычно берут {аксессуар/услуга} — {выгода в 1 фразе}. Сейчас {цена}. Добавить?»\n\n"
        "Фоллоу-ап:\n"
        "-«Актуально ли закрыть вопрос по {товару}? Могу удержать цену/наличие сегодня. Какие остались вопросы?»\n\n"
        "Тактики:\n"
        "-Выгоды вместо ТТХ: «что получите/сэкономите/избежите».\n"
        "-Соцдоказательство: «бестселлер», «N клиентов выбрали», «рейтинг/кейс».\n"
        "-Срочность/дефицит (честно): «акция до {дата}», «осталось N шт.».\n"
        "-Якорение цены: «обычно {выше}, сейчас {цена}.»\n"
        "-Гарантии: «возврат/обмен {условия}», «официальная гарантия {срок}».\n\n"
        "Антидублирование:\n"
        "-Память о: имя, задача, бюджет, показанные модели, закрытые возражения.\n"
        "-Не повторять приветствие и одинаковые CTA.\n"
        "-Повторять мысль только перефразом и с новой ценностью.\n\n"
        "Канальные нюансы:\n"
        "-Avito: короче тексты, ссылку даём аккуратно; при интересе — «удобно продолжить в WhatsApp?» → {WHATSAPP_LINK}.\n"
        "-WhatsApp/Telegram: можно списки, фото, документы. Каталог — сразу ключевые позиции + ссылка.\n\n"
        "Политика честности:\n"
        "-Не занижать сроки/цену. Не очернять конкурентов. Не обещать того, чего нет.\n"
    )


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
            channels_raw = item.get("channels") or ["telegram", "avito", "whatsapp"]
            channels: list[str] = []
            if isinstance(channels_raw, (list, tuple, set)):
                for ch in channels_raw:
                    if isinstance(ch, str) and ch.strip():
                        channels.append(ch.strip().lower())
            elif isinstance(channels_raw, str) and channels_raw.strip():
                channels.append(channels_raw.strip().lower())
            if not channels:
                channels = ["telegram", "avito", "whatsapp"]
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
        _PERSONA_HINTS_CACHE.pop(int(tenant), None)
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
    _PERSONA_HINTS_CACHE.pop(int(tenant), None)


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

    def _total_score(item: Dict[str, Any]) -> float:
        base = _score(item, needs)
        matched = _text_match_score(item, query_tokens)
        tag_bonus = _tag_boost(item)
        return base + matched + tag_bonus

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
            return "Пока без точных позиций — готов подобрать после пары уточнений."
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
        return None

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
    hints = persona_hints or load_persona_hints(tenant)
    engine = SalesConversationEngine(state, brand, cfg, channel or brand["CHANNEL"], persona_hints=hints)
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
    hints = load_persona_hints(tenant)
    engine = SalesConversationEngine(state, brand, cfg, channel or brand["CHANNEL"], persona_hints=hints)
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
    hints = load_persona_hints(tenant)
    engine = SalesConversationEngine(state, brand, cfg, channel or brand["CHANNEL"], persona_hints=hints)
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

    persona_hints = load_persona_hints(tenant)
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
    """Собираем системный промпт с учётом брендинга арендатора."""
    persona = load_persona(tenant, channel)
    persona_hints = extract_persona_hints(persona)
    cache_key: int | None
    try:
        cache_key = int(tenant) if tenant is not None else None
    except Exception:
        cache_key = None
    fingerprint = hashlib.sha1(persona.encode("utf-8")).hexdigest() if persona else ""
    _PERSONA_HINTS_CACHE[cache_key] = (fingerprint, persona_hints)
    branding = _branding_for_tenant(tenant, channel)
    channel_name = (channel or branding["CHANNEL"]).strip() or "WhatsApp"

    cfg = json.loads(json.dumps(DEFAULT_TENANT_JSON, ensure_ascii=False))
    if tenant is not None:
        try:
            cfg = load_tenant(tenant)
        except Exception:
            pass

    state = observe_user_message(
        contact_id,
        tenant,
        channel_name,
        last_user_text or "",
        tenant_cfg=cfg,
        branding=branding,
        persona_hints=persona_hints,
    )
    engine = SalesConversationEngine(state, branding, cfg, channel_name, persona_hints=persona_hints)
    summary = engine.summary_for_llm()

    limits_cfg = cfg.get("limits", {}) if isinstance(cfg, dict) else {}

    try:
        catalog_window = int(limits_cfg.get("catalog_page_size", 8))
    except Exception:
        catalog_window = 8
    preview_limit = min(12, max(4, catalog_window))
    needs_snapshot: Dict[str, Any] = dict(state.needs) if state.needs else {}
    if not needs_snapshot and last_user_text:
        needs_snapshot = infer_user_needs(last_user_text)
    context_items = search_catalog(
        needs_snapshot,
        limit=preview_limit,
        tenant=tenant,
        query=last_user_text,
    )
    if context_items:
        engine.register_recommendations(context_items)

    try:
        logger.info(
            "event=build_llm_messages_diag contact_id=%s tenant=%s channel=%s needs=%s context_items=%s",
            contact_id,
            tenant,
            channel_name,
            json.dumps(needs_snapshot, ensure_ascii=False),
            json.dumps(context_items, ensure_ascii=False),
        )
    except Exception:
        logger.debug("context_items_diag_log_failed", exc_info=True)

    system_blocks = [persona.strip()]
    system_blocks.append(
        " | ".join(
            filter(
                None,
                [
                    f"Бренд: {branding['BRAND']} ({branding['CITY']})",
                    f"Канал: {channel_name}",
                    f"Каталог на ответ: {limits_cfg.get('catalog_page_size', 8)} позиций",
                ],
            )
        )
    )
    system_blocks.append(summary)

    if context_items:
        catalog_block = format_items_for_prompt(context_items, branding["CURRENCY"])
        system_blocks.append(f"Релевантные позиции каталога:\n{catalog_block}")
        system_blocks.append(
            "Используй только перечисленные модели и цены из каталога. "
            "Не придумывай новых позиций и не меняй стоимость."
        )

    # Добавим обучающие примеры диалогов (1–2) из базы арендатора
    if training_retriever and tenant is not None and (last_user_text or "").strip():
        try:
            block = training_retriever.build_examples_block(int(tenant), last_user_text)
        except Exception:
            block = ""
        if block.strip():
            system_blocks.append(block)

    history_limit = 12
    history_tail = [
        item
        for item in (
            state.history[-history_limit:] if state.history else []
        )
        if item.get("role") in {"user", "assistant"}
    ]
    if history_tail:
        trimmed = history_tail[:-1] if history_tail and history_tail[-1].get("role") == "user" else history_tail
        if trimmed:
            transcript = "\n".join(f"{msg['role']}: {msg['content']}" for msg in trimmed)
            if transcript.strip():
                system_blocks.append(f"Недавний диалог:\n{transcript}")

    cta_allowed = False
    reply_rules: list[str] = []
    if channel_name.lower() in {"whatsapp", "telegram"}:
        reply_rules.append(
            f"Мы уже общаемся в {channel_name}. Не предлагай менять канал и не спрашивай, где удобнее общаться."
        )
    if not cta_allowed:
        reply_rules.append("В этом ответе не используй CTA и не закрывай сделку.")
    if reply_rules:
        system_blocks.append("Правила текущего ответа:\n- " + "\n- ".join(reply_rules))

    system_blocks.append(f"Идентификатор контакта: {contact_id}")

    sys = "\n\n".join(block for block in system_blocks if block)
    messages: List[Dict[str, str]] = [{"role": "system", "content": sys}]

    if history_tail:
        trimmed = history_tail[:-1] if history_tail and history_tail[-1].get("role") == "user" else history_tail
        for msg in trimmed:
            messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({"role": "user", "content": (last_user_text or "")})
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

        resp = await asyncio.to_thread(
            create_fn,
            model=settings.OPENAI_MODEL,
            messages=messages,
            max_tokens=260,
            temperature=0.7,
            top_p=0.9,
            frequency_penalty=0.2,
            presence_penalty=0.05,
            timeout=settings.OPENAI_TIMEOUT_SECONDS,
        )
        answer = resp.choices[0].message.content.strip()  # type: ignore
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
        _apply_plan_alignment_to_state(state, enforcement_ctx, existing_fp)
        save_sales_state(state)
        result = _wrap_llm_reply(refined_answer, plan=dummy_plan, raw_answer=answer)
        record_bot_reply(contact_ref, tenant, channel_name, str(result))
        return result
    except APITimeoutError as exc:
        logger.warning("direct llm timeout: %s", exc)
    except Exception as exc:
        logger.exception("direct llm call failed", exc_info=exc)

    fallback = make_rule_based_reply(last_user_message, channel_name, contact_ref, tenant=tenant)
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

    # Без ключа — быстрый локальный ответ
    client = _get_openai_client()
    if client is None:
        fallback = make_rule_based_reply(last, channel_name, contact_ref, tenant=tenant)
        return _wrap_llm_reply(fallback, plan=None, raw_answer=fallback)

    try:
        openai.api_key = settings.OPENAI_API_KEY  # type: ignore

        persona_hints = load_persona_hints(tenant)
        state = load_sales_state(tenant, contact_ref)

        # 1. План + ответ через двухшаговый пайплайн
        planner_attempts = int(os.getenv("PLANNER_ATTEMPTS", "2") or "1")
        if planner_attempts < 1:
            planner_attempts = 1
        last_exc: Exception | None = None
        plan = None
        answer = ""
        for attempt in range(1, planner_attempts + 1):
            try:
                plan, answer = await planner.generate_sales_reply(
                    messages,
                    openai_module=client,
                    model=settings.OPENAI_MODEL,
                    timeout=settings.OPENAI_TIMEOUT_SECONDS,
                    persona_language=persona_hints.language if persona_hints and persona_hints.language else None,
                )
                break
            except planner.PlannerError as exc:  # type: ignore[attr-defined]
                last_exc = exc
                logger.warning("planner failed attempt=%s: %s", attempt, exc)
                break
            except APITimeoutError as exc:
                last_exc = exc
                logger.warning("planner timeout attempt=%s reason=%s", attempt, exc or "timeout")
                if attempt < planner_attempts:
                    await asyncio.sleep(1.0)
            except Exception as exc:
                last_exc = exc
                logger.exception("llm planner error", exc_info=exc)
                break
        else:
            if last_exc:
                raise last_exc

        if not plan or not answer:
            raise last_exc or RuntimeError("planner failed without exception")

        enforcement_ctx = _make_enforcement_context(state, persona_hints, channel_name)
        existing_fp = set(enforcement_ctx.asked_fingerprints)
        refined = quality.enforce_plan_alignment(
            answer,
            plan,
            persona_hints,
            context=enforcement_ctx,
        )
        if not refined.strip():
            logger.warning(
                "planner_refined_empty tenant=%s contact=%s channel=%s",
                tenant,
                contact_ref,
                channel_name,
            )
            return await _direct_llm_reply(
                client,
                messages,
                persona_hints,
                state,
                channel_name,
                contact_ref,
                tenant,
                last,
            )
        _apply_plan_alignment_to_state(state, enforcement_ctx, existing_fp)
        state.last_plan = plan.to_dict()
        save_sales_state(state)
        result = _wrap_llm_reply(refined, plan=plan, raw_answer=answer)
        record_bot_reply(contact_ref, tenant, channel_name, str(result))
        return result
    except planner.PlannerError as exc:  # type: ignore[attr-defined]
        logger.warning("planner failed: %s", exc)
    except APITimeoutError as exc:
        logger.warning("planner timeout: %s", exc)
    except Exception as exc:
        logger.exception("llm planner error", exc_info=exc)

    return await _direct_llm_reply(
        client,
        messages,
        persona_hints,
        state,
        channel_name,
        contact_ref,
        tenant,
        last,
    )


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
