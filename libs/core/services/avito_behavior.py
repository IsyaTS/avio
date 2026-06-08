from __future__ import annotations

from typing import Any, Mapping


def extract_avito_auto_reply_text(cfg: Mapping[str, Any] | None) -> str:
    if not isinstance(cfg, Mapping):
        return ""
    behavior = cfg.get("behavior")
    if not isinstance(behavior, Mapping):
        return ""

    auto_flag = behavior.get("auto_reply")
    if auto_flag is not None and not bool(auto_flag):
        return ""
    text_value = behavior.get("auto_reply_text")
    if isinstance(text_value, str) and text_value.strip():
        return text_value.strip()
    return ""


def extract_avito_phone_tg_template(
    cfg: Mapping[str, Any] | None,
    persona_meta: Mapping[str, Any] | None,
) -> str:
    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            text_value = behavior.get("avito_phone_tg_template")
            if isinstance(text_value, str) and text_value.strip():
                return text_value.strip()

    if not isinstance(persona_meta, Mapping):
        return ""
    for key in (
        "avito_phone_tg_template",
        "meta.avito_phone_tg_template",
        "persona.meta.avito_phone_tg_template",
    ):
        value = persona_meta.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def avito_smart_reply_enabled(cfg: Mapping[str, Any] | None) -> bool:
    if not isinstance(cfg, Mapping):
        return False
    behavior = cfg.get("behavior")
    if not isinstance(behavior, Mapping):
        return False
    for key in ("avito_smart_reply_enabled", "avito_ai_enabled"):
        flag = behavior.get(key)
        if flag is not None:
            return bool(flag)
    return False


__all__ = [
    "avito_smart_reply_enabled",
    "extract_avito_auto_reply_text",
    "extract_avito_phone_tg_template",
]
