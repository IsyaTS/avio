from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from libs.core import db as db_module
from libs.core.crypto import encrypt_str, decrypt_str, EncryptionError
from libs.core.models.avito_analytics import AvitoAnalyticsToken

logger = logging.getLogger(__name__)


async def ensure_schema() -> None:
    """Ensure the avito_analytics_tokens table exists (idempotent)."""

    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("avito_analytics_tokens_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS avito_analytics_tokens (
          account_id         BIGINT PRIMARY KEY,
          display_name       TEXT,
          scopes             TEXT,
          token_type         TEXT,
          access_token_enc   TEXT,
          refresh_token_enc  TEXT,
          expires_at         TIMESTAMPTZ,
          obtained_at        TIMESTAMPTZ,
          created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
          last_error         TEXT,
          raw_payload        JSONB
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_avito_analytics_tokens_updated ON avito_analytics_tokens(updated_at DESC)",
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception(
                "avito_analytics_tokens_ensure_failed statement=%s", stmt.strip().split("\n", 1)[0]
            )
            raise


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _row_to_model(row: Mapping[str, Any] | Any) -> AvitoAnalyticsToken | None:
    if not row:
        return None
    try:
        data = dict(row)
    except Exception:
        return None
    account_raw = data.get("account_id") or data.get("account")
    try:
        account_id = int(account_raw)
    except Exception:
        return None
    access_enc = data.get("access_token_enc")
    refresh_enc = data.get("refresh_token_enc")
    access_token = None
    refresh_token = None
    try:
        if access_enc:
            access_token = decrypt_str(str(access_enc))
        if refresh_enc:
            refresh_token = decrypt_str(str(refresh_enc))
    except EncryptionError:
        logger.warning(
            "avito_analytics_token_decrypt_failed account_id=%s", account_id, exc_info=True
        )
    created_raw = data.get("created_at")
    updated_raw = data.get("updated_at")
    expires_raw = data.get("expires_at")
    obtained_raw = data.get("obtained_at")

    def _dt(val: Any) -> datetime | None:
        if isinstance(val, datetime):
            return val
        if val is None:
            return None
        try:
            return datetime.fromisoformat(str(val))
        except Exception:
            return None

    raw_payload = data.get("raw_payload")
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except Exception:
            raw_payload = None
    return AvitoAnalyticsToken(
        account_id=account_id,
        display_name=data.get("display_name"),
        scopes=data.get("scopes"),
        token_type=data.get("token_type"),
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=_dt(expires_raw),
        obtained_at=_dt(obtained_raw),
        created_at=_dt(created_raw),
        updated_at=_dt(updated_raw),
        last_error=data.get("last_error"),
        raw_payload=raw_payload if isinstance(raw_payload, Mapping) else None,
    )


async def list_tokens() -> list[AvitoAnalyticsToken]:
    fetch_fn = getattr(db_module, "_fetch", None)
    if not fetch_fn:
        return []
    try:
        rows = await fetch_fn(
            """
            SELECT account_id, display_name, scopes, token_type,
                   access_token_enc, refresh_token_enc,
                   expires_at, obtained_at, created_at, updated_at,
                   last_error, raw_payload
            FROM avito_analytics_tokens
            ORDER BY updated_at DESC NULLS LAST
            """
        )
    except Exception:
        logger.exception("avito_analytics_tokens_list_failed")
        raise
    return [model for model in (_row_to_model(row) for row in rows) if model]


async def get(account_id: int) -> AvitoAnalyticsToken | None:
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            SELECT account_id, display_name, scopes, token_type,
                   access_token_enc, refresh_token_enc,
                   expires_at, obtained_at, created_at, updated_at,
                   last_error, raw_payload
            FROM avito_analytics_tokens
            WHERE account_id = $1
            """,
            int(account_id),
        )
    except Exception:
        logger.exception("avito_analytics_tokens_get_failed account_id=%s", account_id)
        raise
    return _row_to_model(row)


async def upsert(
    account_id: int,
    *,
    display_name: str | None,
    scopes: str | None,
    token_type: str | None,
    access_token: str | None,
    refresh_token: str | None,
    expires_at: datetime | None,
    obtained_at: datetime | None,
    raw_payload: Mapping[str, Any] | None = None,
) -> AvitoAnalyticsToken | None:
    """Store or update tokens."""

    if not refresh_token:
        raise EncryptionError("refresh_token is required")
    enc_access = encrypt_str(access_token) if access_token else None
    enc_refresh = encrypt_str(refresh_token)
    exec_fn = getattr(db_module, "_fetchrow", None)
    if not exec_fn:
        raise RuntimeError("database_unavailable")
    payload_json = json.dumps(raw_payload, ensure_ascii=False) if raw_payload else None
    now = _now()
    try:
        row = await exec_fn(
            """
            INSERT INTO avito_analytics_tokens (
              account_id, display_name, scopes, token_type,
              access_token_enc, refresh_token_enc,
              expires_at, obtained_at, created_at, updated_at,
              last_error, raw_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, NULL, $10::jsonb)
            ON CONFLICT (account_id) DO UPDATE SET
              display_name = EXCLUDED.display_name,
              scopes = EXCLUDED.scopes,
              token_type = EXCLUDED.token_type,
              access_token_enc = EXCLUDED.access_token_enc,
              refresh_token_enc = EXCLUDED.refresh_token_enc,
              expires_at = EXCLUDED.expires_at,
              obtained_at = EXCLUDED.obtained_at,
              updated_at = $9,
              raw_payload = COALESCE(EXCLUDED.raw_payload, avito_analytics_tokens.raw_payload),
              last_error = NULL
            RETURNING account_id, display_name, scopes, token_type,
                      access_token_enc, refresh_token_enc,
                      expires_at, obtained_at, created_at, updated_at,
                      last_error, raw_payload
            """,
            int(account_id),
            display_name,
            scopes,
            token_type,
            enc_access,
            enc_refresh,
            expires_at,
            obtained_at,
            now,
            payload_json,
        )
    except Exception:
        logger.exception("avito_analytics_tokens_upsert_failed account_id=%s", account_id)
        raise
    return _row_to_model(row)


async def update_tokens(
    account_id: int,
    *,
    access_token: str | None,
    refresh_token: str | None,
    expires_at: datetime | None,
    obtained_at: datetime | None,
    token_type: str | None = None,
) -> AvitoAnalyticsToken | None:
    enc_access = encrypt_str(access_token) if access_token else None
    enc_refresh = encrypt_str(refresh_token) if refresh_token else None
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        raise RuntimeError("database_unavailable")
    now = _now()
    try:
        row = await fetchrow(
            """
            UPDATE avito_analytics_tokens
            SET access_token_enc = COALESCE($2, access_token_enc),
                refresh_token_enc = COALESCE($3, refresh_token_enc),
                expires_at = $4,
                obtained_at = $5,
                token_type = COALESCE($6, token_type),
                updated_at = $7,
                last_error = NULL
            WHERE account_id = $1
            RETURNING account_id, display_name, scopes, token_type,
                      access_token_enc, refresh_token_enc,
                      expires_at, obtained_at, created_at, updated_at,
                      last_error, raw_payload
            """,
            int(account_id),
            enc_access,
            enc_refresh,
            expires_at,
            obtained_at,
            token_type,
            now,
        )
    except Exception:
        logger.exception("avito_analytics_tokens_update_failed account_id=%s", account_id)
        raise
    return _row_to_model(row)


async def delete(account_id: int) -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    try:
        await exec_fn("DELETE FROM avito_analytics_tokens WHERE account_id = $1", int(account_id))
    except Exception:
        logger.exception("avito_analytics_tokens_delete_failed account_id=%s", account_id)
        raise


async def mark_error(account_id: int, error: str) -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    try:
        await exec_fn(
            "UPDATE avito_analytics_tokens SET last_error = $2, updated_at = $3 WHERE account_id = $1",
            int(account_id),
            error,
            _now(),
        )
    except Exception:
        logger.exception("avito_analytics_tokens_mark_error_failed account_id=%s", account_id)
        raise


def redact_token(value: str | None, keep_tail: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= keep_tail:
        return "***"
    return f"{'*' * (len(value) - keep_tail)}{value[-keep_tail:]}"


def summary_from_tokens(tokens: Iterable[AvitoAnalyticsToken]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for entry in tokens:
        summary.append(
            {
                "account_id": entry.account_id,
                "display_name": entry.display_name or "",
                "scopes": entry.scopes or "",
                "expires_at": entry.expires_at.isoformat() if entry.expires_at else None,
                "updated_at": entry.updated_at.isoformat() if entry.updated_at else None,
                "last_error": entry.last_error,
            }
        )
    return summary


__all__ = [
    "ensure_schema",
    "list_tokens",
    "get",
    "upsert",
    "update_tokens",
    "delete",
    "mark_error",
    "redact_token",
    "summary_from_tokens",
]
