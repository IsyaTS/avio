from __future__ import annotations
import os
import json
import re
import csv
import asyncio
import pathlib
import logging
from typing import List, Dict, Any, Optional, Tuple, Mapping, Sequence
import yaml
from .brain_mode import (
    human_reply_mode_enabled as _brain_mode_human_reply_enabled,
    resolve_brain_mode as _brain_mode_resolve,
)
from .bootstrap_runtime import resolve_public_key as _bootstrap_resolve_public_key
from .bootstrap_runtime import resolve_tenants_dir as _bootstrap_resolve_tenants_dir
from .conversation_playbook import (
    AUTHORITY_KEYWORDS, BANT_TEMPLATES, CHALLENGER_PLAYBOOK, EMPATHY_NEGATIVE_TEMPLATES,
    EMPATHY_POSITIVE_TEMPLATES, IMPLICATION_KEYWORDS, NEED_PAYOFF_KEYWORDS, NEGATIVE_KEYWORDS,
    POSITIVE_KEYWORDS, PROBLEM_KEYWORDS, RECIPROCITY_TEMPLATES, SCARCITY_TEMPLATES,
    SOCIAL_PROOF_TEMPLATES, SPIN_TEMPLATES, TIMELINE_PATTERNS, UPSELL_TEMPLATES,
    analyze_sentiment_delta as _playbook_analyze_sentiment_delta, pick_cta as _playbook_pick_cta,
)
from .conversation_entrypoints import apply_persona_need_mappings as _entry_apply_persona_need_mappings
from .conversation_entrypoints import make_rule_based_reply as _entry_make_rule_based_reply
from .conversation_entrypoints import observe_user_message as _entry_observe_user_message
from .conversation_entrypoints import record_bot_reply as _entry_record_bot_reply
from .conversation_entrypoints import summarize_sales_state as _entry_summarize_sales_state
from .config_runtime import build_avito_scope_value as _config_build_avito_scope_value
from .config_runtime import coerce_bool as _config_coerce_bool
from .config_runtime import env_bool as _config_env_bool
from .constants import (
    _CATALOG_REQUEST_RE, _CATALOG_UNAVAILABLE_RE, _CONTACT_HANDLE_RE, _CONTACT_PHONE_RE, _CONTACT_URL_RE,
    _ENTITY_ACK_PREFIX_RE, _ETA_INTENT_RE, _FACT_CANONICAL_ALIASES, _FACT_TOKEN_RE, _GENERIC_FACT_STOPWORDS,
    _GENERIC_MODEL_WORDS, _GRATITUDE_PHRASE_RE, _GRATITUDE_RE, _GREETING_PREFIX_RE, _INSTRUCTION_LEAK_LINE_RE,
    _INSTRUCTION_LIST_LINE_RE, _INSULATION_NEED_RE, _LOWERCASE_OPENING_BLOCKED, _LOW_SIGNAL_CONTEXT_RE,
    _LOW_SIGNAL_USER_REPLY_RE, _MODEL_QUOTED_MENTION_RE, _NEIGHBOR_CLAIM_RE, _NOISE_NEED_RE, _OBJECT_TYPE_HINT_RE,
    _OFFTOPIC_SMALLTALK_RE, _OPENING_HEY_RE, _OPENING_WORD_RE, _ORDER_INTENT_RE, _PRICE_INLINE_RE,
    _PRICE_THOUSANDS_RE, _QUESTION_CUE_RE, _QUESTION_THIS_OR_RE, _QUESTION_TOPIC_TO_SLOT, _REPAIR_TURN_RE,
    _REPLY_STYLE_GUARD, _ROBOTIC_BANNED_PATTERNS, _SENTENCE_SPLIT_RE, _SHORTLIST_LEAK_RE, _SLOT_ALIASES,
    _STOP_INTENT_RE, _URGENT_TODAY_RE, _WHY_QUESTION_RE,
)
from .needs_runtime import GLOBAL_COLOR_ALIASES as _NEEDS_GLOBAL_COLOR_ALIASES
from .persona_hints_runtime import PersonaHints
from .persona_hints_runtime import clear_persona_hints_cache as _persona_hints_clear
from .persona_hints_runtime import load_persona_hints as _persona_hints_load
from .persona_hints_runtime import persona_hints_cache_key as _persona_hints_cache_key_runtime
from .llm_runtime import LLMRuntime
from .models import PersonaCompiledRules
from .models import PersonaConditionalRule
from .models import PersonaDeliveryRule
from .models import PersonaStepRule
from .models import SalesState
from .prompt_format import format_items_for_prompt as _prompt_format_items_for_prompt
from .prompt_format import format_needs_for_prompt as _prompt_format_needs_for_prompt
from .state_runtime import (
    cta_allowed as _state_runtime_cta_allowed,
    apply_plan_alignment_to_state as _state_runtime_apply_plan_alignment_to_state,
    make_enforcement_context as _state_runtime_make_enforcement_context,
    max_questions_limit as _state_runtime_max_questions_limit,
    remember_cta_state as _state_runtime_remember_cta_state,
    remember_question_state as _state_runtime_remember_question_state,
)
from .settings_runtime import SettingsRuntimeDeps
from .settings_runtime import create_settings as _create_settings
from .facade_helpers import apply_plan_alignment_to_state as _facade_apply_plan_alignment_to_state
from .facade_helpers import bind_named_delegates as _facade_bind_named_delegates
from .facade_helpers import bind_private_delegates as _facade_bind_private_delegates
from .facade_helpers import delegate_async_runtime_method as _facade_delegate_async_runtime_method
from .facade_helpers import delegate_runtime_method as _facade_delegate_runtime_method
from .facade_helpers import make_enforcement_context as _facade_make_enforcement_context
from .facade_core_bindings import install_core_delegate_bindings
from .facade_quality_bindings import install_quality_bindings
from .facade_post_bindings import install_post_runtime_bindings
from .runtime_getters import install_runtime_getters
from .tenant_defaults import build_default_tenant_json
from .tenant_defaults import load_default_persona_md
from .catalog_text import collect_item_text as _catalog_collect_item_text
from .catalog_text import text_match_score as _catalog_text_match_score
from .catalog_text import tokenize_query as _catalog_tokenize_query
from .exports import SALES_CORE_EXPORTS
from .facade_defaults import NEEDS_STOPWORDS
from .facade_defaults import PERSONA_MD
from .facade_defaults import RULES_YAML
from .text_norm import match_key as _text_norm_match_key
from .text_norm import normalize_text as _text_norm_normalize

# Redis (асинхронный клиент можно использовать при необходимости)
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


_resolve_public_key = _bootstrap_resolve_public_key
_resolve_tenants_dir = lambda: _bootstrap_resolve_tenants_dir(root_dir=ROOT_DIR, data_dir=DATA_DIR)


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


_env_bool = lambda name, default=False: _config_env_bool(os.getenv(name), default)
_coerce_bool = _config_coerce_bool


# Precompiled regexes reused across hot paths
_FIELD_CLEAN_RE = re.compile(r"[^0-9a-zA-Zа-яА-ЯёЁ]+")
_PERSONA_HINTS_KEY_RE = re.compile(
    r"^(greeting|приветств(?:ие|уй)|cta|призыв|closing|завершение|tone|тон|language|язык|max(?:imum)?\s*(?:questions|вопросов|уточнений))\s*[:\-]\s*(.+)$",
    re.IGNORECASE,
)


_DEFAULT_WORKER_BASE_URL = "http://worker:8000"


_build_avito_scope_value = _config_build_avito_scope_value


settings = _create_settings(
    SettingsRuntimeDeps(
        data_dir=DATA_DIR,
        resolve_public_key=_resolve_public_key,
        env_bool=_env_bool,
        build_avito_scope_value=_build_avito_scope_value,
        default_worker_base_url=_DEFAULT_WORKER_BASE_URL,
    )
)
Settings = type(settings)


_sync_redis_client: redis_sync.Redis | None = None


def tenant_config(tenant: int) -> Dict[str, Any]:
    try:
        tenant_key = int(tenant)
    except Exception:
        return {}
    raw = _TENANTS_CONFIG_CACHE.get(tenant_key) or {}
    return dict(raw)


def tenant_waweb_url(tenant: int | None) -> str:
    return _transport_runtime().tenant_waweb_url(
        tenant,
        tenant_config_fn=tenant_config,
    )


def tenant_whatsapp_provider(tenant: int | None) -> str:
    return _transport_runtime().tenant_whatsapp_provider(
        tenant,
        tenant_config_fn=tenant_config,
        read_tenant_config_fn=read_tenant_config,
    )


def _resolve_chat_completion_callable(obj: Any):
    return _openai_client_runtime().resolve_chat_completion_callable(obj)


def _get_openai_client() -> Any | None:
    return _openai_client_runtime().get_openai_client(api_key=settings.OPENAI_API_KEY)


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
    _state_io_runtime().reset_state_store()


def _state_key(tenant: int | None, contact_id: int | None) -> str:
    return _state_io_runtime().state_key(tenant, contact_id)


def _state_store_read(key: str) -> Optional[dict]:
    return _state_io_runtime().state_store_read(key)


def _state_store_write(key: str, payload: dict) -> None:
    _state_io_runtime().state_store_write(key, payload)


def _remember_question_state(state: SalesState, question: str) -> None:
    _state_runtime_remember_question_state(
        state,
        question,
        question_fingerprint_fn=quality.question_fingerprint,
    )


def _remember_cta_state(state: SalesState, cta_text: str) -> None:
    _state_runtime_remember_cta_state(state, cta_text)


def _cta_allowed(state: SalesState, channel_name: str | None) -> bool:
    return _state_runtime_cta_allowed(
        state,
        channel_name,
        cta_cooldown_seconds=CTA_COOLDOWN_SECONDS,
    )


def _max_questions_limit(persona_hints: Optional[PersonaHints], default: int = 1) -> int:
    return _state_runtime_max_questions_limit(persona_hints, default)


_delegate_runtime_method = _facade_delegate_runtime_method
_delegate_async_runtime_method = _facade_delegate_async_runtime_method
_bind_private_delegates = lambda runtime_getter, *method_names: _facade_bind_private_delegates(
    globals(), _delegate_runtime_method, runtime_getter, *method_names
)
_bind_named_delegates = lambda runtime_getter, mapping: _facade_bind_named_delegates(
    globals(), _delegate_runtime_method, runtime_getter, mapping
)


install_quality_bindings(
    globals(),
    delegate_sync=_delegate_runtime_method,
    llm_runtime_cls=LLMRuntime,
    getenv=os.getenv,
    openai_module=openai,
    stop_intent_re=_STOP_INTENT_RE,
    robotic_banned_patterns=_ROBOTIC_BANNED_PATTERNS,
    sentence_split_re=_SENTENCE_SPLIT_RE,
)


install_core_delegate_bindings(
    globals(),
    bind_private=_bind_private_delegates,
    delegate_sync=_delegate_runtime_method,
    delegate_async=_delegate_async_runtime_method,
    load_persona_fn=lambda tenant, channel: load_persona(tenant, channel),
    state_cls=SalesState,
    quality_module=quality,
    persona_hints_cache_key=_persona_hints_cache_key_runtime,
    persona_hints_clear=_persona_hints_clear,
    persona_hints_load=_persona_hints_load,
    facade_apply_plan_alignment_to_state=_facade_apply_plan_alignment_to_state,
    facade_make_enforcement_context=_facade_make_enforcement_context,
    state_runtime_apply_plan_alignment=_state_runtime_apply_plan_alignment_to_state,
    state_runtime_make_enforcement_context=_state_runtime_make_enforcement_context,
    needs_global_color_aliases=_NEEDS_GLOBAL_COLOR_ALIASES,
)


install_runtime_getters(
    globals(),
    settings_obj=settings,
    logger_obj=logger,
    openai_module=openai,
    api_timeout_error_cls=APITimeoutError,
    style_guard=_REPLY_STYLE_GUARD,
)

if _env_bool("RESET_SALES_STATE_ON_START", False):
    _reset_state_store()

# --------------------------- хранилище ключей (Redis) ------------------------
get_tenant_pubkey = _delegate_runtime_method(lambda: _io_runtime(), "get_tenant_pubkey")
set_tenant_pubkey = _delegate_runtime_method(lambda: _io_runtime(), "set_tenant_pubkey")


# ----------------------------- утилиты HTTP ---------------------------------
http_json = _delegate_runtime_method(lambda: _io_runtime(), "http_json")


install_post_runtime_bindings(
    globals(),
    data_dir=DATA_DIR,
    root_dir=ROOT_DIR,
    build_default_tenant_json=build_default_tenant_json,
    load_default_persona_md=load_default_persona_md,
    bind_private=_bind_private_delegates,
    bind_named=_bind_named_delegates,
    delegate_sync=_delegate_runtime_method,
    delegate_async=_delegate_async_runtime_method,
    format_items_for_prompt=_prompt_format_items_for_prompt,
    format_needs_for_prompt=_prompt_format_needs_for_prompt,
    pick_cta=_playbook_pick_cta,
    analyze_sentiment_delta=_playbook_analyze_sentiment_delta,
    normalize_text=_text_norm_normalize,
    match_key=_text_norm_match_key,
    collect_item_text=_catalog_collect_item_text,
    tokenize_query=_catalog_tokenize_query,
    text_match_score=_catalog_text_match_score,
    global_color_aliases=_NEEDS_GLOBAL_COLOR_ALIASES,
)
__all__ = SALES_CORE_EXPORTS
