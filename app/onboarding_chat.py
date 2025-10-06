from __future__ import annotations

import asyncio
import json
import time
import pathlib
from typing import Any, Dict, List, Tuple

from core import (
    settings,
    tenant_dir,
    read_tenant_config,
    write_tenant_config,
    read_persona,
    paginate_catalog_text,
    read_all_catalog,
    ask_llm,
    get_tenant_pubkey,
)

# ----------------------------- Conversation storage -----------------------------

OnboardingMessage = Dict[str, Any]
OnboardingConversation = Dict[str, Any]

_ONBOARDING_FILENAME = "conversation.json"
try:
    _LLM_TIMEOUT_SECONDS = float(getattr(settings, "OPENAI_TIMEOUT_SECONDS", 4.0))
except Exception:
    _LLM_TIMEOUT_SECONDS = 4.0
if _LLM_TIMEOUT_SECONDS <= 0:
    _LLM_TIMEOUT_SECONDS = 4.0

_FALLBACK_STEPS: List[Dict[str, str]] = [
    {
        "ask": "Подскажите 2–3 ключевые категории или коллекции из каталога, которые должны быть на первом экране для клиентов?",
        "key": "top_categories",
    },
    {
        "ask": "Какие характеристики обязательно проговаривать, чтобы клиент понял ценность товара (материалы, комплектация, гарантия)?",
        "key": "critical_features",
    },
    {
        "ask": "Назовите артикулы или названия товаров-хедлайнеров, которые менеджеру нужно знать наизусть.",
        "key": "hero_products",
    },
    {
        "ask": "С какими возражениями вы сталкиваетесь чаще всего и как обычно закрываете их?",
        "key": "objections",
    },
    {
        "ask": "Есть ли сезонные офферы, акции или ограничения по складу, которые важно держать в голове прямо сейчас?",
        "key": "promo_notes",
    },
]


def _dir_for(tenant: int) -> pathlib.Path:
    base = tenant_dir(int(tenant)) / "onboarding"
    base.mkdir(parents=True, exist_ok=True)
    return base


def conversation_path(tenant: int) -> pathlib.Path:
    return _dir_for(int(tenant)) / _ONBOARDING_FILENAME


def load_conversation(tenant: int) -> OnboardingConversation:
    path = conversation_path(tenant)
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, dict):
                    data.setdefault("messages", [])
                    data.setdefault("status", "in_progress")
                    data.setdefault("insights", {})
                    data.setdefault("cursor", 0)
                    data.setdefault("mode", "auto")
                    data.setdefault("created_at", data.get("created_at", time.time()))
                    data.setdefault("updated_at", data.get("updated_at", time.time()))
                    return data
        except Exception:
            pass
    now = time.time()
    return {
        "status": "new",
        "messages": [],
        "insights": {},
        "cursor": 0,
        "mode": "auto",
        "created_at": now,
        "updated_at": now,
    }


def save_conversation(tenant: int, convo: OnboardingConversation) -> None:
    convo["updated_at"] = time.time()
    path = conversation_path(tenant)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(convo, fh, ensure_ascii=False, indent=2)


def reset_conversation(tenant: int) -> None:
    path = conversation_path(tenant)
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


# ----------------------------- Preconditions helpers -----------------------------

def _passport_ready(cfg: Dict[str, Any]) -> bool:
    passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
    brand = (passport.get("brand") or "").strip()
    agent = (passport.get("agent_name") or "").strip()
    return bool(brand and agent)


def _persona_ready(text: str) -> bool:
    return bool(text and len(text.strip()) >= 80)


def _catalog_ready(cfg: Dict[str, Any]) -> bool:
    integrations = cfg.get("integrations") if isinstance(cfg.get("integrations"), dict) else {}
    uploaded = integrations.get("uploaded_catalog")
    if isinstance(uploaded, dict) and uploaded.get("path"):
        return True
    catalogs = cfg.get("catalogs") if isinstance(cfg.get("catalogs"), list) else []
    return any(isinstance(entry, dict) and entry.get("path") for entry in catalogs)


def _session_ready(meta: Dict[str, Any]) -> bool:
    """Heuristically determine if the WA session metadata represents a live link."""

    if not meta:
        return False

    # Explicit ready flags are the strongest signal
    for key in ("ready", "is_ready", "isReady", "connected"):
        value = meta.get(key)
        if isinstance(value, bool) and value:
            return True

    # Some integrations report nested state objects
    nested = meta.get("session") if isinstance(meta.get("session"), dict) else None
    if nested is meta:
        nested = None
    if nested and _session_ready(nested):
        return True

    status_fields = ("status", "state", "phase", "connection", "stage", "last_event", "lastEvent")
    positive_states = {
        "ready",
        "connected",
        "online",
        "authenticated",
        "synced",
        "synced_ready",
        "launched",
        "launch",
        "active",
        "open",
        "open_ready",
        "pair_success",
        "paired",
        "established",
    }

    for field in status_fields:
        raw = meta.get(field)
        if isinstance(raw, str) and raw.strip().lower() in positive_states:
            return True

    # Availability of a direct link usually means the session is active
    session_link = meta.get("link") or meta.get("invite") or meta.get("url")
    if isinstance(session_link, str) and session_link.strip():
        return True

    return False


def _channels_ready(cfg: Dict[str, Any], tenant: int) -> bool:
    integrations = cfg.get("integrations") if isinstance(cfg.get("integrations"), dict) else {}
    passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}

    link_candidates = [
        integrations.get("whatsapp_link"),
        integrations.get("wa_link"),
        passport.get("whatsapp_link"),
        passport.get("contact_link"),
    ]
    for candidate in link_candidates:
        if isinstance(candidate, str) and candidate.strip():
            return True

    session_meta = integrations.get("wa_session") if isinstance(integrations.get("wa_session"), dict) else {}
    if _session_ready(session_meta):
        return True
    # Some deployments keep session state under a list of tenants
    if isinstance(session_meta, dict):
        for value in session_meta.values():
            if isinstance(value, dict) and _session_ready(value):
                return True

    primary_key = (get_tenant_pubkey(int(tenant)) or "").strip()
    if primary_key:
        return True

    extra_keys = integrations.get("client_keys")
    if isinstance(extra_keys, (list, tuple)):
        for raw in extra_keys:
            if isinstance(raw, str) and raw.strip():
                return True

    return False


def evaluate_preconditions(tenant: int) -> Tuple[Dict[str, bool], Dict[str, Any]]:
    cfg = read_tenant_config(tenant)
    persona = read_persona(tenant)
    checks = {
        "channels": _channels_ready(cfg, tenant),
        "passport": _passport_ready(cfg),
        "persona": _persona_ready(persona),
        "catalog": _catalog_ready(cfg),
    }
    return checks, {"cfg": cfg, "persona": persona}


def preconditions_met(checks: Dict[str, bool]) -> bool:
    return all(checks.values())


# ----------------------------- Insights merge -----------------------------

def _merge_dict(base: Dict[str, Any], delta: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in (delta or {}).items():
        if value is None:
            continue
        if isinstance(value, dict):
            target = base.setdefault(key, {}) if isinstance(base.get(key), dict) else {}
            base[key] = _merge_dict(target, value)
        elif isinstance(value, list):
            existing = base.setdefault(key, []) if isinstance(base.get(key), list) else []
            # ensure unique items while preserving order of delta
            merged = existing[:]
            for item in value:
                if item not in merged:
                    merged.append(item)
            base[key] = merged
        else:
            base[key] = value
    return base


def update_tenant_insights(tenant: int, status: str, insights_delta: Dict[str, Any] | None) -> None:
    cfg = read_tenant_config(tenant)
    section = cfg.setdefault("onboarding", {})
    if insights_delta:
        current = section.setdefault("insights", {}) if isinstance(section.get("insights"), dict) else {}
        section["insights"] = _merge_dict(current, insights_delta or {})
    section["updated_at"] = int(time.time())
    section.setdefault("started_at", int(time.time()))
    section["status"] = status
    write_tenant_config(tenant, cfg)


# ----------------------------- LLM helpers -----------------------------

def _truncate(text: str, limit: int = 1200) -> str:
    if not text:
        return ""
    clean = text.strip()
    if len(clean) <= limit:
        return clean
    return clean[: limit - 3] + "..."


def _catalog_excerpt(cfg: Dict[str, Any], tenant: int) -> str:
    try:
        items = read_all_catalog(cfg=cfg, tenant=tenant)
    except Exception:
        items = []
    if not items:
        return ""
    subset = items[:40]
    pages = paginate_catalog_text(subset, cfg=cfg, page_size=8)
    return "\n\n".join(pages[:2])


def _insights_excerpt(insights: Dict[str, Any]) -> str:
    if not insights:
        return ""
    try:
        return json.dumps(insights, ensure_ascii=False, indent=2)
    except Exception:
        return str(insights)


def _build_system_prompt(tenant: int, cfg: Dict[str, Any], persona: str, insights: Dict[str, Any]) -> str:
    passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
    brand = passport.get("brand") or "(бренд не указан)"
    agent = passport.get("agent_name") or "менеджер"
    city = passport.get("city") or "—"
    catalog_excerpt = _truncate(_catalog_excerpt(cfg, tenant), 1800)
    persona_excerpt = _truncate(persona, 1200)
    insights_text = _truncate(_insights_excerpt(insights), 800)
    return (
        "Ты — Avio Onboarding Copilot. Ты общаешься на русском языке с менеджером компании, "
        "чтобы зафиксировать знания о товарах и бренде в структурированном виде. "
        "Задавай по одному уточняющему вопросу за раз, основываясь на каталоге и уже собранных заметках.\n\n"
        f"Бренд: {brand}\nМенеджер: {agent}\nРегион: {city}\n\n"
        "Контекст каталога (фрагмент):\n" + (catalog_excerpt or "(нет каталога)") + "\n\n"
        "Persona (фрагмент):\n" + (persona_excerpt or "(persona.md пока пустой)") + "\n\n"
        "Уже собранные инсайты:\n" + (insights_text or "—") + "\n\n"
        "Формат ответа строго в JSON: {\"ask\": \"следующий вопрос человеку\", \"insights\": {...}, \"complete\": false}.\n"
        "Если ты уверен, что собрал достаточно данных для тонкой настройки, установи complete=true и дай короткое завершающее сообщение в поле ask."
    )


def _parse_llm_payload(raw: str) -> Tuple[str, Dict[str, Any], bool]:
    raw = (raw or "").strip()
    if not raw:
        return "", {}, False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            try:
                data = json.loads(raw[start : end + 1])
            except Exception:
                data = {"ask": raw}
        else:
            data = {"ask": raw}
    if not isinstance(data, dict):
        return str(data), {}, False
    ask = str(data.get("ask") or data.get("question") or raw).strip()
    insights = data.get("insights") if isinstance(data.get("insights"), dict) else {}
    complete = bool(data.get("complete"))
    return ask or raw, insights, complete


async def _llm_step(
    tenant: int,
    convo: OnboardingConversation,
    cfg: Dict[str, Any],
    persona: str,
    user_message: str,
) -> Tuple[str, Dict[str, Any], bool]:
    system_prompt = _build_system_prompt(tenant, cfg, persona, convo.get("insights", {}))
    history: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
    for msg in convo.get("messages", [])[-12:]:
        role = msg.get("role")
        content = msg.get("content")
        if role in {"assistant", "user"} and isinstance(content, str):
            history.append({"role": role, "content": content})
    history.append({"role": "user", "content": user_message})
    try:
        raw = await asyncio.wait_for(
            ask_llm(history, tenant=tenant, contact_id=0, channel="onboarding"),
            timeout=_LLM_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError as exc:
        raise TimeoutError("llm_timeout") from exc
    return _parse_llm_payload(raw)


def _fallback_question(convo: OnboardingConversation) -> Tuple[str, Dict[str, Any], bool]:
    cursor = int(convo.get("cursor", 0))
    if cursor >= len(_FALLBACK_STEPS):
        return (
            "Отлично, записала все ключевые заметки. Если что-то захочется изменить — можно вернуться позже.",
            {},
            True,
        )
    step = _FALLBACK_STEPS[cursor]
    convo["cursor"] = cursor + 1
    return step["ask"], {}, False


def _fallback_apply_answer(convo: OnboardingConversation, answer: str) -> Dict[str, Any]:
    if not answer:
        return {}
    cursor = max(0, int(convo.get("cursor", 0)) - 1)
    if cursor >= len(_FALLBACK_STEPS):
        cursor = len(_FALLBACK_STEPS) - 1
    key = _FALLBACK_STEPS[cursor].get("key")
    if not key:
        return {}
    insights_bucket = convo.setdefault("insights", {})
    if key not in insights_bucket or not isinstance(insights_bucket[key], list):
        insights_bucket[key] = []
    normalized = answer.strip()
    if normalized:
        insights_bucket[key].append(normalized)
    return {key: [normalized] if normalized else []}


async def next_assistant_turn(
    tenant: int,
    convo: OnboardingConversation,
    cfg: Dict[str, Any],
    persona: str,
    user_message: str,
) -> Tuple[str, Dict[str, Any], bool]:
    mode = convo.get("mode") or "auto"
    if mode == "fallback" or not settings.OPENAI_API_KEY:
        delta = _fallback_apply_answer(convo, user_message)
        ask, _, complete = _fallback_question(convo)
        return ask, delta, complete
    try:
        ask, delta, complete = await _llm_step(tenant, convo, cfg, persona, user_message)
        if not ask:
            raise ValueError("empty llm response")
        return ask, delta, complete
    except Exception:
        convo["mode"] = "fallback"
        delta = _fallback_apply_answer(convo, user_message)
        ask, _, complete = _fallback_question(convo)
        return ask, delta, complete


async def initial_assistant_turn(
    tenant: int,
    convo: OnboardingConversation,
    cfg: Dict[str, Any],
    persona: str,
) -> Tuple[str, Dict[str, Any], bool]:
    if convo.get("mode") == "fallback" or not settings.OPENAI_API_KEY:
        ask, _, complete = _fallback_question(convo)
        return ask, {}, complete
    try:
        starter = "Начинаем онбординг. Оцени загруженный каталог и задавай первый уточняющий вопрос."
        ask, delta, complete = await _llm_step(tenant, convo, cfg, persona, starter)
        if not ask:
            raise ValueError("empty llm response")
        return ask, delta, complete
    except Exception:
        convo["mode"] = "fallback"
        ask, _, complete = _fallback_question(convo)
        return ask, {}, complete


def add_user_message(convo: OnboardingConversation, text: str) -> None:
    if not text:
        return
    entry: OnboardingMessage = {
        "role": "user",
        "content": text.strip(),
        "ts": int(time.time()),
    }
    convo.setdefault("messages", []).append(entry)
    if convo.get("status") == "new":
        convo["status"] = "in_progress"


def add_assistant_message(
    convo: OnboardingConversation,
    text: str,
    *,
    insights: Dict[str, Any] | None = None,
    complete: bool = False,
) -> None:
    entry: OnboardingMessage = {
        "role": "assistant",
        "content": text.strip(),
        "ts": int(time.time()),
    }
    meta: Dict[str, Any] = {}
    if insights:
        entry["insights"] = insights
        meta["insights"] = insights
    if complete:
        entry["complete"] = True
        convo["status"] = "completed"
    convo.setdefault("messages", []).append(entry)
    if insights:
        convo.setdefault("insights", {})
        _merge_dict(convo["insights"], insights)
    if not complete and convo.get("status") == "new":
        convo["status"] = "in_progress"


def public_messages(convo: OnboardingConversation) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for msg in convo.get("messages", []):
        role = msg.get("role")
        content = msg.get("content")
        if role not in {"assistant", "user"} or not isinstance(content, str):
            continue
        out.append(
            {
                "role": role,
                "content": content,
                "ts": int(msg.get("ts") or 0),
                "complete": bool(msg.get("complete")),
            }
        )
    return out


__all__ = [
    "load_conversation",
    "save_conversation",
    "reset_conversation",
    "evaluate_preconditions",
    "preconditions_met",
    "initial_assistant_turn",
    "next_assistant_turn",
    "add_user_message",
    "add_assistant_message",
    "public_messages",
    "update_tenant_insights",
]
