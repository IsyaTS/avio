from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from .models import SalesState


@dataclass(frozen=True)
class ConversationEntrypointDeps:
    persona_meta_config: Callable[[int], Dict[str, Any]]
    load_tenant: Callable[[int], Dict[str, Any]]
    branding_for_tenant: Callable[[int | None, str | None], Dict[str, str]]
    load_sales_state: Callable[[int | None, int], SalesState]
    load_persona_hints: Callable[[int | None, str | None], Any]
    save_sales_state: Callable[[SalesState], None]
    default_tenant_json: Dict[str, Any]
    infer_user_needs: Callable[[str], Dict[str, Any]]
    search_catalog: Callable[..., list[Dict[str, Any]]]
    pick_cta: Callable[[int, str | None], Dict[str, str]]
    engine_builder: Callable[[SalesState, Dict[str, str], Dict[str, Any], str, Any], Any]


def apply_persona_need_mappings(
    state: SalesState,
    tenant: int | None,
    text: str,
    *,
    deps: ConversationEntrypointDeps,
) -> None:
    if not text or not state or tenant in (None, 0):
        return
    try:
        persona_meta = deps.persona_meta_config(int(tenant))
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
    *,
    deps: ConversationEntrypointDeps,
    tenant_cfg: Optional[dict] = None,
    branding: Optional[Dict[str, str]] = None,
    persona_hints: Optional[Any] = None,
) -> SalesState:
    cfg = tenant_cfg
    if cfg is None:
        cfg = deps.load_tenant(tenant or 0)
    brand = branding or deps.branding_for_tenant(tenant, channel)
    state = deps.load_sales_state(tenant, contact_id)
    channel_name = (channel or brand["CHANNEL"]).strip() or "WhatsApp"
    hints = persona_hints or deps.load_persona_hints(tenant, channel_name)
    engine = deps.engine_builder(state, brand, cfg, channel_name, hints)
    engine.observe_user(text or "")
    apply_persona_need_mappings(state, tenant, text or "", deps=deps)
    deps.save_sales_state(state)
    return state


def summarize_sales_state(
    contact_id: int,
    tenant: int | None,
    channel: str | None,
    *,
    deps: ConversationEntrypointDeps,
    tenant_cfg: Optional[dict] = None,
    branding: Optional[Dict[str, str]] = None,
) -> str:
    cfg = tenant_cfg if tenant_cfg is not None else deps.load_tenant(tenant or 0)
    brand = branding or deps.branding_for_tenant(tenant, channel)
    state = deps.load_sales_state(tenant, contact_id)
    channel_name = (channel or brand["CHANNEL"]).strip() or "WhatsApp"
    hints = deps.load_persona_hints(tenant, channel_name)
    engine = deps.engine_builder(state, brand, cfg, channel_name, hints)
    return engine.summary_for_llm()


def record_bot_reply(
    contact_id: int,
    tenant: int | None,
    channel: str | None,
    reply: str,
    *,
    deps: ConversationEntrypointDeps,
    tenant_cfg: Optional[dict] = None,
    branding: Optional[Dict[str, str]] = None,
) -> None:
    cfg = tenant_cfg if tenant_cfg is not None else deps.load_tenant(tenant or 0)
    brand = branding or deps.branding_for_tenant(tenant, channel)
    state = deps.load_sales_state(tenant, contact_id)
    channel_name = (channel or brand["CHANNEL"]).strip() or "WhatsApp"
    hints = deps.load_persona_hints(tenant, channel_name)
    deps.engine_builder(state, brand, cfg, channel_name, hints)
    if reply:
        state.last_bot_reply = reply.strip()
        state.append_history("assistant", reply.strip())
        state.last_updated_ts = time.time()
    deps.save_sales_state(state)


def make_rule_based_reply(
    last_user_text: str,
    channel: str | None,
    contact_id: int,
    *,
    deps: ConversationEntrypointDeps,
    tenant: int | None = None,
) -> str:
    branding = deps.branding_for_tenant(tenant, channel)
    channel_name = (channel or branding["CHANNEL"]).strip() or "WhatsApp"
    cfg = json.loads(json.dumps(deps.default_tenant_json, ensure_ascii=False))
    if tenant is not None:
        try:
            cfg = deps.load_tenant(tenant)
        except Exception:
            cfg = json.loads(json.dumps(deps.default_tenant_json, ensure_ascii=False))

    persona_hints = deps.load_persona_hints(tenant, channel_name)
    state = deps.load_sales_state(tenant, contact_id)
    engine = deps.engine_builder(
        state,
        branding,
        cfg,
        channel_name,
        persona_hints,
    )
    engine.observe_user(last_user_text or "")

    needs = state.needs if state.needs else deps.infer_user_needs(last_user_text or "")
    currency = branding["CURRENCY"]
    items = deps.search_catalog(needs, limit=4, tenant=tenant, query=last_user_text)
    cta_pick = deps.pick_cta(contact_id, channel_name)
    cta_text = str((cta_pick or {}).get("text") or "").strip()

    reply = engine.build_reply(items, cta_text, cta_text, currency, last_user_text or "")
    deps.save_sales_state(state)
    return reply
