from __future__ import annotations

import json
from typing import Any, Callable, Mapping


def tenant_from_redis_state(raw_value: Any) -> int | None:
    if isinstance(raw_value, bytes):
        try:
            raw_value = raw_value.decode("utf-8")
        except Exception:
            raw_value = None
    if not isinstance(raw_value, str):
        return None
    try:
        payload = json.loads(raw_value)
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    try:
        tenant_id = int(str(payload.get("tenant")).strip())
    except Exception:
        return None
    return tenant_id if tenant_id > 0 else None


def resolve_tenant_from_state(
    *,
    raw_value: Any,
    state: str,
    verify_signed_state: Callable[[str], Mapping[str, Any] | None],
) -> int | None:
    tenant_id = tenant_from_redis_state(raw_value)
    if tenant_id is not None:
        return tenant_id

    signed_payload = verify_signed_state(state)
    if signed_payload is None:
        return None
    try:
        tenant_id = int(str(signed_payload.get("tenant")).strip())
    except Exception:
        return None
    return tenant_id if tenant_id > 0 else None


__all__ = ["resolve_tenant_from_state", "tenant_from_redis_state"]
