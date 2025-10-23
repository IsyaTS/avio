from __future__ import annotations

import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from app.models import ProviderToken

try:  # pragma: no cover - fallback for runtime package layout
    from app import db as db_module  # type: ignore
except ImportError:  # pragma: no cover - scripts/tests may import as top-level
    import db as db_module  # type: ignore

try:  # pragma: no cover - optional alias when core already imported
    from app.core import ensure_tenant_files, tenant_dir  # type: ignore
except ImportError:  # pragma: no cover - fallback during bootstrap/tests
    try:
        import core as core_module  # type: ignore
    except ImportError:
        core_module = None  # type: ignore[assignment]

    def _fallback_ensure(tenant: int) -> Path:
        base = Path("tenants") / str(int(tenant))
        base.mkdir(parents=True, exist_ok=True)
        return base

    def _fallback_dir(tenant: int) -> Path:
        return Path("tenants") / str(int(tenant))

    if core_module is not None:
        ensure_tenant_files = getattr(core_module, "ensure_tenant_files", _fallback_ensure)  # type: ignore[assignment]
        tenant_dir = getattr(core_module, "tenant_dir", _fallback_dir)  # type: ignore[assignment]
    else:  # pragma: no cover - ultimate fallback
        ensure_tenant_files = _fallback_ensure  # type: ignore[assignment]
        tenant_dir = _fallback_dir  # type: ignore[assignment]


logger = logging.getLogger(__name__)

_PRIMARY_FILENAME = "provider_token.json"
_LEGACY_FILENAME = "provider_token.txt"


async def ensure_schema() -> None:
    runner = getattr(db_module, "ensure_provider_tokens_schema", None)
    if runner is None:
        logger.debug("provider_tokens_ensure_skip reason=no_runner")
        return
    await runner()


async def _fetchrow(sql: str, *args: Any):
    fetchrow = getattr(db_module, "_fetchrow", None)
    if fetchrow is None:
        logger.debug("provider_token_fetchrow_skip reason=no_driver")
        return None
    asyncpg_module = getattr(db_module, "asyncpg", None)
    undefined_table_error = getattr(asyncpg_module, "UndefinedTableError", None)
    try:
        return await fetchrow(sql, *args)
    except Exception as exc:
        if undefined_table_error and isinstance(exc, undefined_table_error):
            await ensure_schema()
            try:
                return await fetchrow(sql, *args)
            except Exception as retry_exc:
                if undefined_table_error and isinstance(retry_exc, undefined_table_error):
                    raise
                raise
        raise


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
        fs_token = _fs_read(int(tenant_id))
        if fs_token:
            return fs_token
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
    fs_token = _fs_write(int(tenant_id), str(token))
    if fs_token:
        return fs_token
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
        fs_token = _fs_write(int(tenant_id), str(token))
        if fs_token:
            return fs_token
        return None
    return _row_to_token(row)


def _fs_paths(tenant: int) -> tuple[Path, Path]:
    try:
        ensure_tenant_files(int(tenant))
    except Exception:
        logger.debug("provider_token_fs_ensure_failed tenant=%s", tenant, exc_info=True)
    try:
        base = tenant_dir(int(tenant))
    except Exception:
        base = Path("tenants") / str(int(tenant))
    primary = Path(base) / _PRIMARY_FILENAME
    legacy = Path(base) / _LEGACY_FILENAME
    return primary, legacy


def _fs_read(tenant: int) -> ProviderToken | None:
    primary, legacy = _fs_paths(tenant)
    if primary.exists():
        candidates = [primary]
    elif legacy.exists():
        candidates = [legacy]
    else:
        return None
    try:
        path = candidates[0]
        text = path.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    token = ""
    created_at = None
    if not text:
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        token = text
    else:
        token = str(data.get("token") or "").strip()
        created_raw = data.get("created_at")
        if created_raw:
            try:
                created_at = datetime.fromisoformat(str(created_raw))
            except Exception:
                created_at = None
    if not token:
        return None
    if created_at is None:
        try:
            created_at = datetime.utcfromtimestamp(path.stat().st_mtime)
        except Exception:
            created_at = datetime.utcnow()
    # migrate legacy txt -> json
    if path.name == _LEGACY_FILENAME:
        _fs_write(tenant, token, created_at)
        try:
            path.unlink()
        except OSError:
            pass
    return ProviderToken(tenant_id=int(tenant), token=token, created_at=created_at)


def _fs_write(tenant: int, token: str, created_at: datetime | None = None) -> ProviderToken | None:
    token = (token or "").strip()
    if not token:
        return None
    primary, _ = _fs_paths(tenant)
    if created_at is None:
        created_at = datetime.utcnow()
    payload = {"token": token, "created_at": created_at.isoformat()}
    try:
        primary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        logger.warning("provider_token_fs_write_failed tenant=%s", tenant, exc_info=True)
        return None
    return ProviderToken(tenant_id=int(tenant), token=token, created_at=created_at)


__all__ = ["ensure_schema", "get_by_tenant", "create_for_tenant", "upsert"]
