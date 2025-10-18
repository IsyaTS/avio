from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Mapping

from app.models import ProviderToken

try:  # pragma: no cover - fallback for runtime package layout
    from app import db as db_module  # type: ignore
except ImportError:  # pragma: no cover - scripts/tests may import as top-level
    import db as db_module  # type: ignore


logger = logging.getLogger(__name__)


async def _fetchrow(sql: str, *args: Any):
    fetchrow = getattr(db_module, "_fetchrow", None)
    if fetchrow is None:
        logger.debug("provider_token_fetchrow_skip reason=no_driver")
        return None
    return await fetchrow(sql, *args)


def _row_to_token(row: Mapping[str, Any] | Any) -> ProviderToken | None:
    try:
        data = dict(row)
    except Exception:
        if isinstance(row, Mapping):
            data = dict(row.items())
        else:
            return None
    tenant_raw = data.get("tenant_id") or data.get("tenant")
    token_value = data.get("token")
    created_raw = data.get("created_at")
    if tenant_raw is None or token_value is None or not str(token_value).strip():
        return None
    try:
        tenant_id = int(tenant_raw)
    except Exception:
        logger.warning("provider_token_row_invalid tenant=%s", tenant_raw)
        return None
    token = str(token_value)
    created_at: datetime
    if isinstance(created_raw, datetime):
        created_at = created_raw
    elif created_raw:
        try:
            created_at = datetime.fromisoformat(str(created_raw))
        except Exception:
            created_at = datetime.utcnow()
    else:
        created_at = datetime.utcnow()
    return ProviderToken(tenant_id=tenant_id, token=token, created_at=created_at)


async def get_by_tenant(tenant_id: int) -> ProviderToken | None:
    row = await _fetchrow(
        "SELECT tenant AS tenant_id, token, created_at FROM provider_tokens WHERE tenant = $1",
        int(tenant_id),
    )
    if not row:
        return None
    return _row_to_token(row)


async def create_for_tenant(tenant_id: int, token: str) -> ProviderToken | None:
    row = await _fetchrow(
        """
        INSERT INTO provider_tokens (tenant, token)
        VALUES ($1, $2)
        ON CONFLICT (tenant) DO NOTHING
        RETURNING tenant AS tenant_id, token, created_at
        """,
        int(tenant_id),
        str(token),
    )
    if row:
        return _row_to_token(row)
    # If the insert was skipped due to existing row, fetch the current value.
    return await get_by_tenant(int(tenant_id))


async def upsert(tenant_id: int, token: str) -> ProviderToken | None:
    row = await _fetchrow(
        """
        INSERT INTO provider_tokens (tenant, token)
        VALUES ($1, $2)
        ON CONFLICT (tenant)
        DO UPDATE SET token = EXCLUDED.token, created_at = now()
        RETURNING tenant AS tenant_id, token, created_at
        """,
        int(tenant_id),
        str(token),
    )
    if not row:
        return None
    return _row_to_token(row)


__all__ = ["get_by_tenant", "create_for_tenant", "upsert"]
