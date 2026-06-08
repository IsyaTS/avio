from __future__ import annotations

import time
from typing import Any, Mapping


class AvitoTokenPayloadError(ValueError):
    pass


def build_token_update_payload(
    token_payload: Mapping[str, Any],
    *,
    now: int | None = None,
) -> dict[str, Any]:
    access_token = str(token_payload.get("access_token") or "").strip()
    if not access_token:
        raise AvitoTokenPayloadError("access_token_missing")

    refresh_token = str(token_payload.get("refresh_token") or "").strip()
    issued_at = int(now if now is not None else time.time())
    update_payload: dict[str, Any] = {
        "access_token": access_token,
        "refresh_token": refresh_token or None,
        "obtained_at": issued_at,
        "account_id": None,
        "account_login": None,
    }

    expires_at = token_payload.get("expires_at")
    if expires_at is not None:
        try:
            update_payload["expires_at"] = int(expires_at)
        except Exception:
            update_payload["expires_at"] = None
    else:
        expires_in = token_payload.get("expires_in")
        try:
            exp_value = int(expires_in)
        except Exception:
            exp_value = None
        if exp_value and exp_value > 0:
            update_payload["expires_at"] = issued_at + exp_value
        else:
            update_payload["expires_at"] = None

    scope_value = token_payload.get("scope")
    if isinstance(scope_value, str) and scope_value.strip():
        update_payload["scope"] = scope_value.strip()

    return update_payload


__all__ = ["AvitoTokenPayloadError", "build_token_update_payload"]
