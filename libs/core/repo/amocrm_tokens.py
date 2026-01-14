from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Mapping

from libs.core import db as db_module
from libs.core.crypto import encrypt_str, decrypt_str, EncryptionError
from libs.core.models.amocrm import AmoCRMToken

logger = logging.getLogger(__name__)


async def ensure_schema() -> None:
    """Ensure the amocrm_tokens table exists (idempotent)."""

    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("amocrm_tokens_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS amocrm_tokens (
          tenant_id        INTEGER PRIMARY KEY,
          access_token_enc TEXT,
          refresh_token_enc TEXT,
          expires_at       TIMESTAMPTZ,
          obtained_at      TIMESTAMPTZ,
          created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
          last_error       TEXT,
          raw_payload      JSONB
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_amocrm_tokens_updated ON amocrm_tokens(updated_at DESC)",
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception("amocrm_tokens_ensure_failed statement=%s", stmt.strip().split("\n", 1)[0])
            raise


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _row_to_model(row: Mapping[str, Any] | Any) -> AmoCRMToken | None:
    if not row:
        return None
    try:
        data = dict(row)
    except Exception:
        return None
    tenant_raw = data.get("tenant_id") or data.get("tenant")
    try:
        tenant_id = int(tenant_raw)
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
        logger.warning("amocrm_token_decrypt_failed tenant_id=%s", tenant_id, exc_info=True)
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
    return AmoCRMToken(
        tenant_id=tenant_id,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=_dt(expires_raw),
        obtained_at=_dt(obtained_raw),
        created_at=_dt(created_raw),
        updated_at=_dt(updated_raw),
        last_error=data.get("last_error"),
        raw_payload=raw_payload if isinstance(raw_payload, Mapping) else None,
    )


async def get(tenant_id: int) -> AmoCRMToken | None:
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    try:
        row = await fetchrow(
            """
            SELECT tenant_id,
                   access_token_enc,
                   refresh_token_enc,
                   expires_at,
                   obtained_at,
                   created_at,
                   updated_at,
                   last_error,
                   raw_payload
            FROM amocrm_tokens
            WHERE tenant_id = $1
            """,
            int(tenant_id),
        )
    except Exception:
        logger.exception("amocrm_tokens_get_failed tenant_id=%s", tenant_id)
        raise
    return _row_to_model(row)


async def upsert(
    tenant_id: int,
    *,
    access_token: str | None,
    refresh_token: str | None,
    expires_at: datetime | None,
    obtained_at: datetime | None,
    raw_payload: Mapping[str, Any] | None = None,
) -> AmoCRMToken | None:
    """Store or update AmoCRM tokens."""

    if not access_token:
        raise EncryptionError("access_token is required")
    enc_access = encrypt_str(access_token) if access_token else None
    enc_refresh = encrypt_str(refresh_token) if refresh_token else None
    exec_fn = getattr(db_module, "_fetchrow", None)
    if not exec_fn:
        raise RuntimeError("database_unavailable")
    payload_json = json.dumps(raw_payload, ensure_ascii=False) if raw_payload else None
    now = _now()
    try:
        row = await exec_fn(
            """
            INSERT INTO amocrm_tokens (
              tenant_id,
              access_token_enc,
              refresh_token_enc,
              expires_at,
              obtained_at,
              created_at,
              updated_at,
              last_error,
              raw_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $6, NULL, $7::jsonb)
            ON CONFLICT (tenant_id) DO UPDATE SET
              access_token_enc = EXCLUDED.access_token_enc,
              refresh_token_enc = COALESCE(EXCLUDED.refresh_token_enc, amocrm_tokens.refresh_token_enc),
              expires_at = EXCLUDED.expires_at,
              obtained_at = EXCLUDED.obtained_at,
              updated_at = $6,
              raw_payload = COALESCE(EXCLUDED.raw_payload, amocrm_tokens.raw_payload),
              last_error = NULL
            RETURNING tenant_id,
                      access_token_enc,
                      refresh_token_enc,
                      expires_at,
                      obtained_at,
                      created_at,
                      updated_at,
                      last_error,
                      raw_payload
            """,
            int(tenant_id),
            enc_access,
            enc_refresh,
            expires_at,
            obtained_at,
            now,
            payload_json,
        )
    except Exception:
        logger.exception("amocrm_tokens_upsert_failed tenant_id=%s", tenant_id)
        raise
    return _row_to_model(row)


async def update_tokens(
    tenant_id: int,
    *,
    access_token: str | None,
    refresh_token: str | None,
    expires_at: datetime | None,
    obtained_at: datetime | None,
) -> AmoCRMToken | None:
    exec_fn = getattr(db_module, "_fetchrow", None)
    if not exec_fn:
        raise RuntimeError("database_unavailable")
    enc_access = encrypt_str(access_token) if access_token else None
    enc_refresh = encrypt_str(refresh_token) if refresh_token else None
    now = _now()
    try:
        row = await exec_fn(
            """
            UPDATE amocrm_tokens
            SET access_token_enc = COALESCE($2, access_token_enc),
                refresh_token_enc = COALESCE($3, refresh_token_enc),
                expires_at = COALESCE($4, expires_at),
                obtained_at = COALESCE($5, obtained_at),
                updated_at = $6,
                last_error = NULL
            WHERE tenant_id = $1
            RETURNING tenant_id,
                      access_token_enc,
                      refresh_token_enc,
                      expires_at,
                      obtained_at,
                      created_at,
                      updated_at,
                      last_error,
                      raw_payload
            """,
            int(tenant_id),
            enc_access,
            enc_refresh,
            expires_at,
            obtained_at,
            now,
        )
    except Exception:
        logger.exception("amocrm_tokens_update_failed tenant_id=%s", tenant_id)
        raise
    return _row_to_model(row)


async def mark_error(tenant_id: int, error: str | None) -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    try:
        await exec_fn(
            "UPDATE amocrm_tokens SET last_error = $2, updated_at = $3 WHERE tenant_id = $1",
            int(tenant_id),
            (error or "")[:2000],
            _now(),
        )
    except Exception:
        logger.exception("amocrm_tokens_mark_error_failed tenant_id=%s", tenant_id)


async def delete(tenant_id: int) -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    try:
        await exec_fn("DELETE FROM amocrm_tokens WHERE tenant_id = $1", int(tenant_id))
    except Exception:
        logger.exception("amocrm_tokens_delete_failed tenant_id=%s", tenant_id)


def redact_token(value: str | None, keep_tail: int = 4) -> str:
    if not value:
        return ""
    token = str(value)
    if len(token) <= keep_tail:
        return "*" * len(token)
    return "*" * (len(token) - keep_tail) + token[-keep_tail:]


__all__ = ["get", "upsert", "update_tokens", "mark_error", "delete", "redact_token", "ensure_schema"]
