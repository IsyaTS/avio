from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, MutableMapping, Optional, Tuple


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


def persona_hints_cache_key(tenant: int | None, channel: str | None) -> tuple[int | None, str]:
    channel_key = (channel or "").strip().lower()
    try:
        tenant_key = int(tenant) if tenant is not None else None
    except Exception:
        tenant_key = None
    return tenant_key, channel_key


def clear_persona_hints_cache(
    cache: MutableMapping[tuple[int | None, str], Tuple[str, PersonaHints]],
    tenant: int | None,
) -> None:
    if tenant is None:
        cache.clear()
        return
    try:
        tenant_key = int(tenant)
    except Exception:
        cache.clear()
        return
    for cache_key in list(cache.keys()):
        if cache_key[0] == tenant_key:
            cache.pop(cache_key, None)


def load_persona_hints(
    cache: MutableMapping[tuple[int | None, str], Tuple[str, PersonaHints]],
    *,
    tenant: int | None,
    channel: str | None,
    load_persona_fn: Callable[[int | None, str | None], str],
    extract_persona_hints_fn: Callable[[str], PersonaHints],
) -> PersonaHints:
    persona_text = load_persona_fn(tenant, channel)
    fingerprint = hashlib.sha1(persona_text.encode("utf-8")).hexdigest() if persona_text else ""
    cache_key = persona_hints_cache_key(tenant, channel)
    cached = cache.get(cache_key)
    if cached and cached[0] == fingerprint:
        return cached[1]
    hints = extract_persona_hints_fn(persona_text)
    cache[cache_key] = (fingerprint, hints)
    return hints
