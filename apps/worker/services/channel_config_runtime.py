from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class ChannelConfigDeps:
    read_tenant_config_fn: Callable[[int], Mapping[str, Any] | dict[str, Any]]
    max_personal_service_module: Any


@dataclass(frozen=True)
class AvitoUserNameDeps:
    redis_client: Any
    avito_integration_module: Any
    log_fn: Callable[..., None]
    coerce_int_fn: Callable[[Any], int | None]


def coerce_bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes", "y", "on"}:
            return True
        if token in {"0", "false", "no", "n", "off"}:
            return False
    return None


def telegram_reply_enabled(tenant_id: int, *, deps: ChannelConfigDeps) -> bool:
    cfg = _tenant_config(tenant_id, deps=deps)
    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            for key in (
                "telegram_reply_enabled",
                "telegram_smart_reply_enabled",
                "telegram_ai_enabled",
            ):
                flag = coerce_bool_value(behavior.get(key))
                if flag is not None:
                    return bool(flag)
        root_flag = coerce_bool_value(cfg.get("telegram_reply_enabled"))
        if root_flag is not None:
            return bool(root_flag)
    return True


def max_reply_enabled(tenant_id: int, *, deps: ChannelConfigDeps) -> bool:
    cfg = _tenant_config(tenant_id, deps=deps)
    if isinstance(cfg, Mapping):
        behavior = cfg.get("behavior")
        if isinstance(behavior, Mapping):
            for key in ("max_reply_enabled", "max_smart_reply_enabled", "max_ai_enabled"):
                flag = coerce_bool_value(behavior.get(key))
                if flag is not None:
                    return bool(flag)
        root_flag = coerce_bool_value(cfg.get("max_reply_enabled"))
        if root_flag is not None:
            return bool(root_flag)
    return True


def max_personal_reply_enabled(tenant_id: int, *, deps: ChannelConfigDeps) -> bool:
    cfg = _tenant_config(tenant_id, deps=deps)
    if isinstance(cfg, Mapping):
        integration_enabled = _max_personal_integration_enabled(cfg)
        integration_reply = _max_personal_integration_reply_enabled(cfg)
        if integration_reply is not None:
            return bool(integration_reply)
        behavior_flag = _max_personal_behavior_flag(cfg)
        if behavior_flag is not None:
            return bool(behavior_flag)
        root_flag = coerce_bool_value(cfg.get("max_personal_reply_enabled"))
        if root_flag is not None:
            return bool(root_flag)
        if integration_enabled is not None:
            return integration_enabled
    try:
        return bool(deps.max_personal_service_module.integration_enabled(int(tenant_id)))
    except Exception:
        return False


def behavior_triggers(tenant_id: int, *, deps: ChannelConfigDeps) -> list[dict[str, Any]]:
    cfg = _tenant_config(tenant_id, deps=deps)
    behavior = cfg.get("behavior") if isinstance(cfg, Mapping) else None
    if not isinstance(behavior, Mapping):
        return []
    triggers = behavior.get("triggers")
    if not isinstance(triggers, list):
        return []
    result: list[dict[str, Any]] = []
    for item in triggers:
        normalized = _normalize_behavior_trigger(item)
        if normalized:
            result.append(normalized)
    return result


def match_behavior_trigger(
    tenant_id: int,
    channel: str,
    text: str,
    *,
    deps: ChannelConfigDeps,
) -> dict[str, Any] | None:
    if not text or not channel:
        return None
    candidates = behavior_triggers(tenant_id, deps=deps)
    if not candidates:
        return None
    lowered = text.lower()
    channel_norm = channel.strip().lower()
    for rule in candidates:
        channels = rule.get("channels") or []
        if channels and channel_norm not in channels:
            continue
        phrases = rule.get("phrases") or []
        for phrase in phrases:
            if isinstance(phrase, str) and phrase.strip() and phrase.strip().lower() in lowered:
                return rule
    return None


def photo_expectation_config(tenant_id: int, *, deps: ChannelConfigDeps) -> tuple[list[str], str, int]:
    cfg = _tenant_config(tenant_id, deps=deps)
    behavior = cfg.get("behavior") if isinstance(cfg, Mapping) else None
    if not isinstance(behavior, Mapping):
        return [], "", 0
    markers = _normalize_photo_markers(behavior.get("photo_expected_markers") or [])
    reply_text = behavior.get("photo_expected_reply")
    reply = reply_text if isinstance(reply_text, str) else str(reply_text or "")
    try:
        ttl_val = int(behavior.get("photo_expected_ttl") or 0)
    except Exception:
        ttl_val = 0
    return markers, reply, ttl_val if ttl_val > 0 else 0


def extract_avito_user_name(
    payload: Mapping[str, Any],
    *,
    author_id: int | None,
    account_id: int | None,
    deps: AvitoUserNameDeps,
) -> str:
    users = payload.get("users")
    if not isinstance(users, list):
        return ""
    if author_id is not None:
        name = _matching_avito_user_name(users, author_id, deps=deps)
        if name:
            return name
    for user in users:
        if not isinstance(user, Mapping):
            continue
        uid = _avito_user_id_value(user, deps=deps)
        if account_id is not None and uid == account_id:
            continue
        name = _avito_user_name_value(user)
        if name:
            return name
    for user in users:
        if isinstance(user, Mapping):
            name = _avito_user_name_value(user)
            if name:
                return name
    return ""


async def resolve_avito_user_name(
    tenant_id: int,
    *,
    account_id: int | None,
    chat_id: str,
    author_id: int | None,
    deps: AvitoUserNameDeps,
) -> str:
    if not chat_id:
        return ""
    cache_key = f"cache:avito_user_name:{tenant_id}:{author_id}" if author_id is not None else None
    if cache_key:
        cached_name = await _cached_avito_user_name(cache_key, deps=deps)
        if cached_name:
            return cached_name
    try:
        info = await deps.avito_integration_module.resolve_chat_participant_profile(
            int(tenant_id),
            account_id=account_id,
            chat_id=chat_id,
            author_id=author_id,
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_user_name_request_failed tenant=%s chat_id=%s error=%s"
            % (tenant_id, chat_id, exc)
        )
        return ""
    name = str((info or {}).get("name") or "").strip()
    if name and cache_key:
        try:
            await deps.redis_client.set(cache_key, name, ex=3600 * 24 * 7)
        except Exception:
            pass
    return name


def _tenant_config(tenant_id: int, *, deps: ChannelConfigDeps) -> Mapping[str, Any] | dict[str, Any] | None:
    try:
        return deps.read_tenant_config_fn(int(tenant_id))
    except Exception:
        return None


def _max_personal_integration_enabled(cfg: Mapping[str, Any]) -> bool | None:
    integrations = cfg.get("integrations")
    if not isinstance(integrations, Mapping):
        return None
    mp = integrations.get("max_personal")
    if not isinstance(mp, Mapping):
        return None
    enabled_flag = coerce_bool_value(mp.get("enabled"))
    if enabled_flag is not None and not enabled_flag:
        return False
    if enabled_flag is not None:
        return bool(enabled_flag)
    return None


def _max_personal_integration_reply_enabled(cfg: Mapping[str, Any]) -> bool | None:
    integrations = cfg.get("integrations")
    mp = integrations.get("max_personal") if isinstance(integrations, Mapping) else None
    if not isinstance(mp, Mapping):
        return None
    reply_flag = coerce_bool_value(mp.get("reply_enabled"))
    return bool(reply_flag) if reply_flag is not None else None


def _max_personal_behavior_flag(cfg: Mapping[str, Any]) -> bool | None:
    behavior = cfg.get("behavior")
    if not isinstance(behavior, Mapping):
        return None
    for key in (
        "max_personal_reply_enabled",
        "max_personal_smart_reply_enabled",
        "max_personal_ai_enabled",
    ):
        flag = coerce_bool_value(behavior.get(key))
        if flag is not None:
            return bool(flag)
    shared_max_flag = coerce_bool_value(behavior.get("max_reply_enabled"))
    return bool(shared_max_flag) if shared_max_flag is not None else None


def _normalize_behavior_trigger(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    phrases = [p.strip() for p in item.get("phrases", []) if isinstance(p, str) and p.strip()]
    if not phrases:
        return None
    channels = _normalize_behavior_channels(item.get("channels"))
    return {
        "phrases": phrases,
        "channels": channels,
        "silence": bool(item.get("silence", True)),
        "notify": bool(item.get("notify", False)),
    }


def _normalize_behavior_channels(raw_channels: Any) -> list[str]:
    default_channels = ["telegram", "avito", "whatsapp", "max", "max_personal"]
    channels: list[str] = []
    raw = raw_channels or default_channels
    if isinstance(raw, (list, tuple, set)):
        channels = [str(ch).strip().lower() for ch in raw if isinstance(ch, str) and ch.strip()]
    elif isinstance(raw, str) and raw.strip():
        channels = [raw.strip().lower()]
    return channels or default_channels


def _normalize_photo_markers(raw_markers: Any) -> list[str]:
    markers: list[str] = []
    if isinstance(raw_markers, (list, tuple, set)):
        markers.extend(ph.strip() for ph in raw_markers if isinstance(ph, str) and ph.strip())
    elif isinstance(raw_markers, str) and raw_markers.strip():
        markers.extend(ph.strip() for ph in raw_markers.split(",") if ph.strip())
    return markers


def _matching_avito_user_name(
    users: list[Any],
    author_id: int,
    *,
    deps: AvitoUserNameDeps,
) -> str:
    for user in users:
        if not isinstance(user, Mapping):
            continue
        if _avito_user_id_value(user, deps=deps) == author_id:
            name = _avito_user_name_value(user)
            if name:
                return name
    return ""


def _avito_user_id_value(user: Mapping[str, Any], *, deps: AvitoUserNameDeps) -> int | None:
    profile = user.get("public_user_profile")
    return deps.coerce_int_fn(
        user.get("id")
        or user.get("user_id")
        or (profile.get("user_id") if isinstance(profile, Mapping) else None)
    )


def _avito_user_name_value(user: Mapping[str, Any]) -> str:
    name = user.get("name") or user.get("username") or user.get("login")
    return str(name).strip() if name else ""


async def _cached_avito_user_name(cache_key: str, *, deps: AvitoUserNameDeps) -> str:
    try:
        cached = await deps.redis_client.get(cache_key)
    except Exception:
        cached = None
    if isinstance(cached, (bytes, bytearray)):
        cached = cached.decode("utf-8", errors="ignore")
    if isinstance(cached, str) and cached.strip():
        return cached.strip()
    return ""
