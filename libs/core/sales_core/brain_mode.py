from __future__ import annotations

from typing import Any, Callable, Mapping


def human_reply_mode_enabled(
    tenant: int | None,
    *,
    cfg: Mapping[str, Any] | None = None,
    env_bool: Callable[[str, bool], bool],
    coerce_bool: Callable[[Any, bool], bool],
    load_tenant: Callable[[int], Mapping[str, Any] | None],
) -> bool:
    if env_bool("HUMAN_REPLY_MODE", False):
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
    return coerce_bool(behavior.get("human_reply_mode"), False)


def resolve_brain_mode(
    tenant: int | None,
    *,
    cfg: Mapping[str, Any] | None = None,
    env_bool: Callable[[str, bool], bool],
    coerce_bool: Callable[[Any, bool], bool],
    load_tenant: Callable[[int], Mapping[str, Any] | None],
) -> str:
    if env_bool("HUMAN_REPLY_MODE", False):
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
    if coerce_bool(behavior.get("human_reply_mode"), False):
        return "classic"
    return "classic"
