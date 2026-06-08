from __future__ import annotations

from typing import Any

TG_SLOT_MIN = 1
TG_SLOT_MAX = 5
TG_SLOT_MULTIPLIER = 1000


def normalize_tg_slot(value: Any) -> int:
    try:
        slot = int(value)
    except Exception:
        return TG_SLOT_MIN
    if slot < TG_SLOT_MIN:
        return TG_SLOT_MIN
    if slot > TG_SLOT_MAX:
        return TG_SLOT_MAX
    return slot


def virtual_tenant_id(tenant_id: int, slot: int) -> int:
    normalized = normalize_tg_slot(slot)
    if normalized == TG_SLOT_MIN:
        return int(tenant_id)
    return int(tenant_id) * TG_SLOT_MULTIPLIER + normalized


def decode_virtual_tenant(raw_tenant: int) -> tuple[int, int]:
    if raw_tenant <= 0:
        return 0, TG_SLOT_MIN
    base = raw_tenant // TG_SLOT_MULTIPLIER
    remainder = raw_tenant % TG_SLOT_MULTIPLIER
    if base > 0 and TG_SLOT_MIN <= remainder <= TG_SLOT_MAX:
        return base, remainder
    return raw_tenant, TG_SLOT_MIN
