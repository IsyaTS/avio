from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)

_TESTING = (os.getenv("TESTING") or "").strip() == "1"
_MEMORY_ENABLED = (os.getenv("AUTH_INMEMORY", "1") or "").strip().lower() not in {
    "0",
    "false",
    "no",
}

_MEM = {
    "tenant_seq": 0,
    "user_seq": 0,
    "token_seq": 0,
    "session_seq": 0,
    "tenants": {},  # id -> row
    "users": {},  # id -> row
    "tokens": {},  # id -> row
    "sessions": {},  # id -> row
}


def _memory_mode() -> bool:
    return _TESTING and _MEMORY_ENABLED


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    try:
        return dict(row)
    except Exception:
        if isinstance(row, Mapping):
            return dict(row.items())
    return None


async def ensure_schema() -> None:
    runner = getattr(db_module, "ensure_auth_schema", None)
    if runner is None:
        logger.debug("auth_schema_skip reason=no_runner")
        return
    await runner()


async def _fetchrow(sql: str, *args: Any):
    fetchrow = getattr(db_module, "_fetchrow", None)
    if fetchrow is None:
        logger.debug("auth_fetchrow_skip reason=no_driver")
        return None
    asyncpg_module = getattr(db_module, "asyncpg", None)
    undefined_table_error = getattr(asyncpg_module, "UndefinedTableError", None)
    try:
        return await fetchrow(sql, *args)
    except Exception as exc:
        if undefined_table_error and isinstance(exc, undefined_table_error):
            await ensure_schema()
            return await fetchrow(sql, *args)
        raise


async def _exec(sql: str, *args: Any) -> int:
    exec_fn = getattr(db_module, "_exec", None)
    if exec_fn is None:
        logger.debug("auth_exec_skip reason=no_driver")
        return 0
    asyncpg_module = getattr(db_module, "asyncpg", None)
    undefined_table_error = getattr(asyncpg_module, "UndefinedTableError", None)
    try:
        return await exec_fn(sql, *args)
    except Exception as exc:
        if undefined_table_error and isinstance(exc, undefined_table_error):
            await ensure_schema()
            return await exec_fn(sql, *args)
        raise


async def _fetch(sql: str, *args: Any):
    fetch_fn = getattr(db_module, "_fetch", None)
    if fetch_fn is None:
        logger.debug("auth_fetch_skip reason=no_driver")
        return []
    asyncpg_module = getattr(db_module, "asyncpg", None)
    undefined_table_error = getattr(asyncpg_module, "UndefinedTableError", None)
    try:
        return await fetch_fn(sql, *args)
    except Exception as exc:
        if undefined_table_error and isinstance(exc, undefined_table_error):
            await ensure_schema()
            return await fetch_fn(sql, *args)
        raise


async def create_tenant() -> int:
    if _memory_mode():
        _MEM["tenant_seq"] += 1
        tenant_id = _MEM["tenant_seq"]
        _MEM["tenants"][tenant_id] = {"id": tenant_id, "created_at": _now()}
        return tenant_id

    row = await _fetchrow(
        "INSERT INTO auth_tenants DEFAULT VALUES RETURNING id",
    )
    if not row:
        raise db_module.DatabaseUnavailableError("tenant_create_failed")
    data = _row_to_dict(row) or {}
    tenant_id = data.get("id")
    return int(tenant_id or 0)


async def delete_tenant(tenant_id: int) -> None:
    if _memory_mode():
        _MEM["tenants"].pop(int(tenant_id), None)
        return
    await _exec("DELETE FROM auth_tenants WHERE id = $1", int(tenant_id))


async def create_user(
    email: str,
    password_hash: str,
    tenant_id: int,
    *,
    contact: str = "",
    preferred_messenger: str = "",
) -> dict[str, Any] | None:
    if _memory_mode():
        for user in _MEM["users"].values():
            if user.get("email") == email:
                return None
            if user.get("tenant_id") == tenant_id:
                return None
        _MEM["user_seq"] += 1
        user_id = _MEM["user_seq"]
        row = {
            "id": user_id,
            "email": email,
            "password_hash": password_hash,
            "tenant_id": int(tenant_id),
            "is_verified": False,
            "status": "active",
            "created_at": _now(),
            "last_login_at": None,
            "contact": contact,
            "preferred_messenger": preferred_messenger,
        }
        _MEM["users"][user_id] = row
        return dict(row)

    row = await _fetchrow(
        """
        INSERT INTO users (email, password_hash, tenant_id, contact, preferred_messenger)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        RETURNING id, email, password_hash, tenant_id, is_verified, status, created_at, last_login_at, contact, preferred_messenger
        """,
        email,
        password_hash,
        int(tenant_id),
        contact,
        preferred_messenger,
    )
    if not row:
        return None
    return _row_to_dict(row)


async def get_user_by_email(email: str) -> dict[str, Any] | None:
    if _memory_mode():
        for user in _MEM["users"].values():
            if user.get("email") == email:
                return dict(user)
        return None
    row = await _fetchrow(
        """
        SELECT id, email, password_hash, tenant_id, is_verified, status, created_at, last_login_at, contact, preferred_messenger
        FROM users
        WHERE email = $1
        """,
        email,
    )
    return _row_to_dict(row) if row else None


async def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    if _memory_mode():
        user = _MEM["users"].get(int(user_id))
        return dict(user) if user else None
    row = await _fetchrow(
        """
        SELECT id, email, password_hash, tenant_id, is_verified, status, created_at, last_login_at, contact, preferred_messenger
        FROM users
        WHERE id = $1
        """,
        int(user_id),
    )
    return _row_to_dict(row) if row else None


async def set_user_verified(user_id: int) -> None:
    if _memory_mode():
        user = _MEM["users"].get(int(user_id))
        if user:
            user["is_verified"] = True
        return
    await _exec("UPDATE users SET is_verified = TRUE WHERE id = $1", int(user_id))


async def update_last_login(user_id: int) -> None:
    if _memory_mode():
        user = _MEM["users"].get(int(user_id))
        if user:
            user["last_login_at"] = _now()
        return
    await _exec("UPDATE users SET last_login_at = now() WHERE id = $1", int(user_id))


async def set_password(user_id: int, password_hash: str) -> None:
    if _memory_mode():
        user = _MEM["users"].get(int(user_id))
        if user:
            user["password_hash"] = password_hash
        return
    await _exec(
        "UPDATE users SET password_hash = $1 WHERE id = $2",
        password_hash,
        int(user_id),
    )


async def create_token(
    user_id: int,
    token_hash: str,
    token_type: str,
    expires_at: datetime,
    request_ip: str | None = None,
) -> dict[str, Any]:
    if _memory_mode():
        _MEM["token_seq"] += 1
        token_id = _MEM["token_seq"]
        row = {
            "id": token_id,
            "user_id": int(user_id),
            "token_hash": token_hash,
            "token_type": token_type,
            "expires_at": expires_at,
            "used_at": None,
            "created_at": _now(),
            "request_ip": request_ip or "",
        }
        _MEM["tokens"][token_id] = row
        return dict(row)

    row = await _fetchrow(
        """
        INSERT INTO user_tokens (user_id, token_hash, token_type, expires_at, request_ip)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, user_id, token_hash, token_type, expires_at, used_at, created_at, request_ip
        """,
        int(user_id),
        token_hash,
        token_type,
        expires_at,
        request_ip,
    )
    data = _row_to_dict(row)
    if not data:
        raise db_module.DatabaseUnavailableError("token_create_failed")
    return data


async def get_token(token_hash: str, token_type: str) -> dict[str, Any] | None:
    if _memory_mode():
        for token in _MEM["tokens"].values():
            if token.get("token_hash") == token_hash and token.get("token_type") == token_type:
                return dict(token)
        return None

    row = await _fetchrow(
        """
        SELECT id, user_id, token_hash, token_type, expires_at, used_at, created_at, request_ip
        FROM user_tokens
        WHERE token_hash = $1 AND token_type = $2
        """,
        token_hash,
        token_type,
    )
    return _row_to_dict(row) if row else None


async def mark_token_used(token_id: int) -> None:
    if _memory_mode():
        token = _MEM["tokens"].get(int(token_id))
        if token:
            token["used_at"] = _now()
        return
    await _exec("UPDATE user_tokens SET used_at = now() WHERE id = $1", int(token_id))


async def list_tokens_for_user(user_id: int, token_type: str) -> list[dict[str, Any]]:
    if _memory_mode():
        out: list[dict[str, Any]] = []
        for token in _MEM["tokens"].values():
            if token.get("user_id") == int(user_id) and token.get("token_type") == token_type:
                out.append(dict(token))
        return out
    rows = await _fetch(
        """
        SELECT id, user_id, token_hash, token_type, expires_at, used_at, created_at, request_ip
        FROM user_tokens
        WHERE user_id = $1 AND token_type = $2
        ORDER BY created_at DESC
        """,
        int(user_id),
        token_type,
    )
    return [dict(row) for row in rows] if rows else []


async def create_session(
    user_id: int,
    session_id_hash: str,
    expires_at: datetime,
    ip: str | None = None,
    user_agent: str | None = None,
) -> dict[str, Any]:
    if _memory_mode():
        _MEM["session_seq"] += 1
        session_id = _MEM["session_seq"]
        row = {
            "id": session_id,
            "user_id": int(user_id),
            "session_id_hash": session_id_hash,
            "created_at": _now(),
            "expires_at": expires_at,
            "revoked_at": None,
            "user_agent": user_agent or "",
            "ip": ip or "",
        }
        _MEM["sessions"][session_id] = row
        return dict(row)

    row = await _fetchrow(
        """
        INSERT INTO user_sessions (user_id, session_id_hash, expires_at, ip, user_agent)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, user_id, session_id_hash, created_at, expires_at, revoked_at, ip, user_agent
        """,
        int(user_id),
        session_id_hash,
        expires_at,
        ip,
        user_agent,
    )
    data = _row_to_dict(row)
    if not data:
        raise db_module.DatabaseUnavailableError("session_create_failed")
    return data


async def get_active_session_with_user(session_id_hash: str) -> dict[str, Any] | None:
    now = _now()
    if _memory_mode():
        for session in _MEM["sessions"].values():
            if session.get("session_id_hash") != session_id_hash:
                continue
            if session.get("revoked_at"):
                continue
            expires = session.get("expires_at")
            if isinstance(expires, datetime) and expires <= now:
                continue
            user = _MEM["users"].get(int(session.get("user_id") or 0))
            if not user or user.get("status") != "active":
                continue
            merged = dict(session)
            merged["user"] = dict(user)
            return merged
        return None

    row = await _fetchrow(
        """
        SELECT s.id AS session_id,
               s.user_id AS user_id,
               s.session_id_hash,
               s.created_at AS session_created_at,
               s.expires_at AS session_expires_at,
               s.revoked_at AS session_revoked_at,
               s.ip AS session_ip,
               s.user_agent AS session_user_agent,
               u.id AS user_id,
               u.email,
               u.password_hash,
               u.tenant_id,
               u.is_verified,
               u.status,
               u.created_at AS user_created_at,
               u.last_login_at
        FROM user_sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.session_id_hash = $1
          AND s.revoked_at IS NULL
          AND s.expires_at > now()
          AND u.status = 'active'
        """,
        session_id_hash,
    )
    data = _row_to_dict(row)
    if not data:
        return None
    user = {
        "id": data.get("user_id"),
        "email": data.get("email"),
        "password_hash": data.get("password_hash"),
        "tenant_id": data.get("tenant_id"),
        "is_verified": data.get("is_verified"),
        "status": data.get("status"),
        "created_at": data.get("user_created_at"),
        "last_login_at": data.get("last_login_at"),
    }
    session = {
        "id": data.get("session_id"),
        "user_id": data.get("user_id"),
        "session_id_hash": data.get("session_id_hash"),
        "created_at": data.get("session_created_at"),
        "expires_at": data.get("session_expires_at"),
        "revoked_at": data.get("session_revoked_at"),
        "ip": data.get("session_ip"),
        "user_agent": data.get("session_user_agent"),
        "user": user,
    }
    return session


async def revoke_session(session_id_hash: str) -> None:
    if _memory_mode():
        for session in _MEM["sessions"].values():
            if session.get("session_id_hash") == session_id_hash:
                session["revoked_at"] = _now()
        return
    await _exec(
        "UPDATE user_sessions SET revoked_at = now() WHERE session_id_hash = $1",
        session_id_hash,
    )


async def revoke_user_sessions(user_id: int) -> None:
    if _memory_mode():
        for session in _MEM["sessions"].values():
            if session.get("user_id") == int(user_id) and not session.get("revoked_at"):
                session["revoked_at"] = _now()
        return
    await _exec(
        "UPDATE user_sessions SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL",
        int(user_id),
    )


__all__ = [
    "ensure_schema",
    "create_tenant",
    "delete_tenant",
    "create_user",
    "get_user_by_email",
    "get_user_by_id",
    "set_user_verified",
    "update_last_login",
    "set_password",
    "create_token",
    "get_token",
    "mark_token_used",
    "list_tokens_for_user",
    "create_session",
    "get_active_session_with_user",
    "revoke_session",
    "revoke_user_sessions",
]
