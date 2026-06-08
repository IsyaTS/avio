import os
import hashlib
import json
import time
import logging
import pathlib
import threading
import re
import asyncio
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, AsyncIterator, Sequence
from collections.abc import Mapping

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    asyncpg = None  # type: ignore[assignment]

# DSN: допускаем вид postgresql+asyncpg:// и нормализуем
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+asyncpg://avio:AvioPg_2025_strong@postgres:5432/avio"
)
DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

IS_TESTING = os.getenv("TESTING") == "1"


def _env_int(name: str, default: int, *, min_value: int = 0, max_value: int = 1000) -> int:
    try:
        value = int(os.getenv(name, str(default)) or default)
    except Exception:
        value = int(default)
    value = max(min_value, value)
    value = min(max_value, value)
    return value


DB_POOL_MIN_SIZE = _env_int("DB_POOL_MIN_SIZE", 5, min_value=1, max_value=100)
DB_POOL_MAX_SIZE = _env_int("DB_POOL_MAX_SIZE", 20, min_value=1, max_value=200)
if DB_POOL_MAX_SIZE < DB_POOL_MIN_SIZE:
    DB_POOL_MAX_SIZE = DB_POOL_MIN_SIZE
try:
    DB_POOL_ACQUIRE_TIMEOUT_SECONDS = float(
        os.getenv("DB_POOL_ACQUIRE_TIMEOUT_SECONDS", "8.0") or "8.0"
    )
except Exception:
    DB_POOL_ACQUIRE_TIMEOUT_SECONDS = 8.0
if DB_POOL_ACQUIRE_TIMEOUT_SECONDS < 1.0:
    DB_POOL_ACQUIRE_TIMEOUT_SECONDS = 1.0


def _is_testing_env() -> bool:
    return os.getenv("TESTING") == "1"


_pool: Any = None
_log = logging.getLogger("db")

from libs.core import sales_core as core_module  # type: ignore

if IS_TESTING:
    _OFFLINE_DIR = pathlib.Path(
        os.getenv("OFFLINE_DIALOGS_DIR")
        or (getattr(core_module, "DATA_DIR", pathlib.Path("./data")) / "offline_dialogs")
    )
    _OFFLINE_THREADS_FILE = _OFFLINE_DIR / "threads.jsonl"
else:
    _OFFLINE_DIR = None
    _OFFLINE_THREADS_FILE = None

_OFFLINE_LOCK = threading.Lock()
_OFFLINE_MAX_RECORDS = int(os.getenv("OFFLINE_DIALOGS_MAX_RECORDS", "5000"))


def _normalize_e164_digits(value: str | int) -> str:
    raw = str(value or "").strip()
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("8") and len(digits) == 11:
        digits = f"7{digits[1:]}"
    if len(digits) < 10 or len(digits) > 15:
        raise ValueError("invalid_phone")
    return digits


class DatabaseUnavailableError(RuntimeError):
    """Raised when PostgreSQL is required but unavailable."""


_WHATSAPP_PRIVATE_SUFFIX = "@s.whatsapp.net"
_WHATSAPP_GROUP_SUFFIX = "@g.us"


def _normalize_e164_number(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return raw.strip()
    if raw.startswith("00") and len(digits) > 2:
        return f"+{digits[2:]}"
    if raw.startswith("+"):
        return f"+{digits}"
    if digits.startswith("8") and len(digits) == 11:
        return f"+7{digits[1:]}"
    if digits.startswith("7") and len(digits) == 11:
        return f"+{digits}"
    if digits.startswith("9") and len(digits) == 10:
        return f"+7{digits}"
    return f"+{digits}"


def _normalize_whatsapp_jid(raw: Optional[str], is_group: bool = False) -> str:
    if not raw:
        return ""
    candidate = raw.strip().lower()
    if not candidate:
        return ""
    if candidate.endswith(_WHATSAPP_GROUP_SUFFIX) or is_group:
        if candidate.endswith(_WHATSAPP_GROUP_SUFFIX):
            return candidate
        return f"{candidate}{_WHATSAPP_GROUP_SUFFIX}"
    local = candidate.split("@", 1)[0]
    normalized = _normalize_e164_number(local)
    return f"{normalized.lower()}{_WHATSAPP_PRIVATE_SUFFIX}"


def _max_tenant_id_fs() -> int:
    base = getattr(core_module, "TENANTS_DIR", None)
    if not base:
        return 0
    try:
        entries = list(pathlib.Path(base).iterdir())
    except Exception:
        return 0
    max_id = 0
    for entry in entries:
        if not entry.is_dir():
            continue
        name = entry.name
        if not name.isdigit():
            continue
        try:
            value = int(name)
        except Exception:
            continue
        if value > max_id:
            max_id = value
    return max_id


async def _ensure_pool() -> Any:
    """Ленивое создание пула. Вернёт None, если БД не настроена или недоступна."""
    global _pool
    if _pool is not None:
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        pool_loop = getattr(_pool, "_loop", None)
        pool_stale = bool(pool_loop and getattr(pool_loop, "is_closed", lambda: False)())
        pool_foreign = bool(pool_loop and current_loop and pool_loop is not current_loop)
        if not pool_stale and not pool_foreign:
            return _pool
        try:
            await _pool.close()
        except Exception:
            pass
        _pool = None
    if asyncpg is None or not DATABASE_URL:
        return None
    try:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=DB_POOL_MIN_SIZE,
            max_size=DB_POOL_MAX_SIZE,
            timeout=DB_POOL_ACQUIRE_TIMEOUT_SECONDS,
        )
        return _pool
    except Exception:
        _pool = None
        return None


# Утилиты-обёртки: не валятся, если БД недоступна
async def _exec(sql: str, *args) -> int:
    pool = await _ensure_pool()
    if not pool:
        return 0
    if _log.isEnabledFor(logging.DEBUG):
        _log.debug("sql_exec query=%s params=%s", sql, args)
    async with pool.acquire() as con:
        return await con.execute(sql, *args)  # type: ignore[return-value]


async def _fetchrow(sql: str, *args):
    pool = await _ensure_pool()
    if not pool:
        return None
    if _log.isEnabledFor(logging.DEBUG):
        _log.debug("sql_fetchrow query=%s params=%s", sql, args)
    async with pool.acquire() as con:
        return await con.fetchrow(sql, *args)


async def _fetch(sql: str, *args):
    pool = await _ensure_pool()
    if not pool:
        return []
    async with pool.acquire() as con:
        return await con.fetch(sql, *args)


async def ensure_provider_tokens_schema() -> None:
    pool = await _ensure_pool()
    if not pool:
        _log.info("provider_tokens_migration_skip reason=no_pool")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS provider_tokens (
            tenant INTEGER PRIMARY KEY,
            token TEXT UNIQUE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'provider_tokens'
                  AND column_name = 'tenant_id'
            ) THEN
                EXECUTE 'ALTER TABLE provider_tokens RENAME COLUMN tenant_id TO tenant';
            END IF;
        END $$
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'provider_tokens'::regclass
                  AND conname = 'provider_tokens_token_key'
            ) THEN
                EXECUTE 'ALTER TABLE provider_tokens ADD CONSTRAINT provider_tokens_token_key UNIQUE (token)';
            END IF;
        END $$
        """,
        "ALTER TABLE provider_tokens ALTER COLUMN created_at SET DEFAULT now()",
    )
    async with pool.acquire() as con:
        for statement in statements:
            try:
                await con.execute(statement)
            except Exception:
                _log.exception(
                    "provider_tokens_migration_failed statement=%s",
                    statement.strip().split("\n", 1)[0],
                )
                raise


async def ensure_auth_schema() -> None:
    pool = await _ensure_pool()
    if not pool:
        _log.info("auth_schema_skip reason=no_pool")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS auth_tenants (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            tenant_id BIGINT NOT NULL UNIQUE REFERENCES auth_tenants(id) ON DELETE CASCADE,
            contact TEXT,
            preferred_messenger TEXT,
            is_verified BOOLEAN NOT NULL DEFAULT FALSE,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_login_at TIMESTAMPTZ
        )
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'users'
                  AND column_name = 'contact'
            ) THEN
                EXECUTE 'ALTER TABLE users ADD COLUMN contact TEXT';
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'users'
                  AND column_name = 'preferred_messenger'
            ) THEN
                EXECUTE 'ALTER TABLE users ADD COLUMN preferred_messenger TEXT';
            END IF;
        END $$;
        """,
        """
        CREATE TABLE IF NOT EXISTS user_tokens (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            token_hash TEXT NOT NULL,
            token_type TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            used_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            request_ip TEXT
        )
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_user_tokens_hash ON user_tokens(token_hash)",
        "CREATE INDEX IF NOT EXISTS idx_user_tokens_user ON user_tokens(user_id)",
        """
        CREATE TABLE IF NOT EXISTS user_sessions (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            session_id_hash TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at TIMESTAMPTZ NOT NULL,
            revoked_at TIMESTAMPTZ,
            user_agent TEXT,
            ip TEXT
        )
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_user_sessions_hash ON user_sessions(session_id_hash)",
        "CREATE INDEX IF NOT EXISTS idx_user_sessions_user ON user_sessions(user_id)",
    )
    async with pool.acquire() as con:
        for statement in statements:
            try:
                await con.execute(statement)
            except Exception:
                _log.exception(
                    "auth_schema_failed statement=%s",
                    statement.strip().split("\n", 1)[0],
                )
                raise

        max_fs = _max_tenant_id_fs()
        if max_fs > 0:
            try:
                row = await con.fetchrow(
                    "SELECT pg_get_serial_sequence('auth_tenants', 'id') AS seq"
                )
                seq = None
                if row:
                    seq = row.get("seq") if hasattr(row, "get") else row[0]
                if seq:
                    await con.execute(
                        """
                        SELECT setval(
                            $1::regclass,
                            GREATEST((SELECT COALESCE(MAX(id), 0) FROM auth_tenants), $2),
                            true
                        )
                        """,
                        seq,
                        int(max_fs),
                    )
            except Exception:
                _log.exception("auth_schema_sequence_adjust_failed")


async def current_alembic_revision() -> Optional[str]:
    pool = await _ensure_pool()
    if not pool:
        return None
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT version_num FROM alembic_version LIMIT 1")
    if not row:
        return None
    value = row[0]
    getter = getattr(row, "get", None)
    if callable(getter):
        value = getter("version_num", value)
    if value is None:
        return None
    return str(value)


def _offline_enabled() -> bool:
    """Return True only when offline fixtures are allowed (tests)."""
    return _is_testing_env() and (asyncpg is None or _pool is None)


def _offline_trim() -> None:
    if not _is_testing_env() or _OFFLINE_THREADS_FILE is None:
        return
    if _OFFLINE_MAX_RECORDS <= 0:
        return
    try:
        with _OFFLINE_LOCK:
            if not _OFFLINE_THREADS_FILE.exists():
                return
            with _OFFLINE_THREADS_FILE.open("r", encoding="utf-8") as handle:
                lines = handle.readlines()
            if len(lines) <= _OFFLINE_MAX_RECORDS:
                return
            trimmed = lines[-_OFFLINE_MAX_RECORDS:]
            with _OFFLINE_THREADS_FILE.open("w", encoding="utf-8") as handle:
                handle.writelines(trimmed)
    except Exception:
        pass


def _offline_append_message(
    lead_id: int,
    text: str,
    direction: int,
    tenant_id: Optional[int] = None,
    *,
    is_bot: bool = False,
) -> None:
    if not text:
        return
    try:
        lead = int(lead_id or 0)
    except Exception:
        lead = 0
    if lead <= 0:
        return
    try:
        tenant_val = int(tenant_id or 0)
    except Exception:
        tenant_val = 0

    record = {
        "lead_id": lead,
        "direction": int(direction),
        "text": text,
        "ts": time.time(),
        "from_me": bool(direction == 1),
        "tenant_id": tenant_val,
        "is_bot": bool(is_bot),
    }
    try:
        with _OFFLINE_LOCK:
            if _OFFLINE_DIR is None or _OFFLINE_THREADS_FILE is None:
                return
            _OFFLINE_DIR.mkdir(parents=True, exist_ok=True)
            with _OFFLINE_THREADS_FILE.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        _offline_trim()
    except Exception:
        pass


def _offline_fetch_threads(
    since_ts: Optional[float], limit: int, tenant_id: Optional[int]
) -> List[Dict[str, Any]]:
    if not _is_testing_env() or _OFFLINE_THREADS_FILE is None:
        return []
    try:
        with _OFFLINE_LOCK:
            if not _OFFLINE_THREADS_FILE.exists():
                return []
            with _OFFLINE_THREADS_FILE.open("r", encoding="utf-8") as handle:
                raw_lines = handle.readlines()
    except Exception:
        return []

    records: List[Dict[str, Any]] = []
    for line in raw_lines:
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except Exception:
            continue

    if since_ts is not None:
        try:
            cutoff = float(since_ts)
            records = [r for r in records if float(r.get("ts") or 0.0) >= cutoff]
        except Exception:
            pass

    tenant_filter: Optional[int] = None
    if tenant_id is not None:
        try:
            tenant_filter = int(tenant_id)
        except Exception:
            tenant_filter = None
    if tenant_filter is not None:
        filtered: List[Dict[str, Any]] = []
        for record in records:
            rec_tenant = record.get("tenant_id")
            if rec_tenant in (tenant_filter, 0, None):
                filtered.append(record)
        records = filtered

    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for record in records:
        try:
            lid = int(record.get("lead_id") or 0)
        except Exception:
            continue
        if lid <= 0:
            continue
        msg = {
            "lead_id": lid,
            "direction": int(record.get("direction", 0)),
            "text": record.get("text", ""),
            "ts": float(record.get("ts") or 0.0),
            "from_me": bool(record.get("from_me")),
        }
        contact_id = record.get("contact_id")
        msg["contact_id"] = contact_id
        grouped.setdefault(lid, []).append(msg)

    threads: List[Dict[str, Any]] = []
    for lid, msgs in grouped.items():
        msgs_sorted = sorted(msgs, key=lambda m: (m.get("ts") or 0.0, m.get("direction", 0)))
        contact_id = None
        for m in msgs_sorted:
            cid = m.get("contact_id")
            if cid is not None:
                contact_id = cid
                break
        sanitized = []
        for m in msgs_sorted:
            sanitized.append({k: v for k, v in m.items() if k not in {"contact_id"}})
        threads.append({"lead_id": lid, "contact_id": contact_id, "messages": sanitized})

    threads.sort(key=lambda t: max((m.get("ts") or 0.0) for m in t["messages"]), reverse=True)
    return threads[: max(1, int(limit))]


def _offline_threads_to_dialogs(
    threads: List[Dict[str, Any]],
    since_cutoff: Optional[float],
    per_limit: Optional[int],
) -> List[Dict[str, Any]]:
    exported: List[Dict[str, Any]] = []
    for thread in threads:
        lead_id = thread.get("lead_id")
        if lead_id is None:
            continue
        raw_messages = list(thread.get("messages") or [])
        filtered: List[Dict[str, Any]] = []
        for msg in raw_messages:
            ts_raw = msg.get("ts")
            try:
                ts_val = float(ts_raw) if ts_raw is not None else 0.0
            except (TypeError, ValueError):
                ts_val = 0.0
            if since_cutoff is not None and ts_val < since_cutoff:
                continue
            filtered.append({"msg": msg, "ts": ts_val})
        if not filtered:
            continue
        if per_limit is not None:
            filtered = filtered[-per_limit:]
        normalized: List[Dict[str, Any]] = []
        for item in filtered:
            payload = item["msg"]
            direction = payload.get("direction")
            try:
                direction_val = int(direction if direction is not None else 0)
            except (TypeError, ValueError):
                direction_val = 0
            role = "assistant" if direction_val == 1 else "user"
            text = (payload.get("text") or "").strip()
            normalized.append(
                {
                    "role": role,
                    "content": text,
                    "text": text,
                    "ts": item["ts"],
                    "direction": direction_val,
                }
            )
        contact_raw = thread.get("contact_id")
        try:
            contact_val = int(contact_raw) if contact_raw is not None else None
        except (TypeError, ValueError):
            contact_val = None
        last_ts = normalized[-1]["ts"] if normalized else None
        exported.append(
            {
                "lead_id": int(lead_id),
                "contact_id": contact_val,
                "whatsapp_phone": None,
                "title": "",
                "messages": normalized,
                "last_message_ts": last_ts,
            }
        )
    return exported


# Явная инициализация по желанию
async def init_db():
    await _ensure_pool()


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


# -------- Leads / sources --------


async def find_lead_by_peer(
    tenant_id: Optional[int],
    channel: str,
    peer: str,
) -> Optional[Mapping[str, Any]]:
    try:
        tenant_val = int(tenant_id) if tenant_id is not None else 0
    except Exception:
        tenant_val = 0

    channel_val = (channel or "avito").strip().lower() or "avito"
    peer_text = (peer or "").strip()
    if not peer_text:
        return None

    peer_lookup = peer_text[:255]

    return await _fetchrow(
        """
        SELECT id, tenant_id
        FROM leads
        WHERE ($1 = 0 OR tenant_id = $1)
          AND channel = $2
          AND peer = $3
        LIMIT 1;
        """,
        tenant_val,
        channel_val,
        peer_lookup,
    )


async def upsert_lead(
    lead_id: Optional[int],
    channel: str = "avito",
    source_real_id: Optional[int] = None,
    tenant_id: Optional[int] = None,
    telegram_user_id: Optional[int] = None,
    telegram_username: Optional[str] = None,
    *,
    peer_id: Optional[int] = None,
    peer: Optional[str] = None,
    contact: Optional[str] = None,
    title: Optional[str] = None,
) -> int:
    """Ensure that a lead record exists and refresh metadata."""

    try:
        tenant_val = int(tenant_id) if tenant_id is not None else 0
    except Exception:
        tenant_val = 0

    channel_val = (channel or "avito").strip().lower() or "avito"
    username_val = (telegram_username or "").strip() or None

    peer_str = (peer or "").strip()
    if not peer_str and peer_id is not None:
        try:
            peer_str = str(int(peer_id))
        except Exception:
            peer_str = str(peer_id).strip()
    contact_val = (contact or "").strip() or None

    def _normalize_int(value: Optional[int]) -> Optional[int]:
        try:
            if value is None:
                return None
            coerced = int(value)
        except Exception:
            return None
        return coerced if coerced != 0 else None

    telegram_val = _normalize_int(telegram_user_id)
    peer_val = _normalize_int(peer_id)
    lead_val = _normalize_int(lead_id)

    if telegram_val is not None:
        lead_val = telegram_val
    elif lead_val is None:
        lead_val = peer_val

    if source_real_id is None and peer_val is not None:
        source_real_id = peer_val

    def _normalize_int32(value: Optional[int]) -> Optional[int]:
        val = _normalize_int(value)
        if val is None:
            return None
        if val < -(2**31) or val > 2**31 - 1:
            return None
        return val

    source_val = _normalize_int32(source_real_id)

    title_val = (title or "").strip() or None
    peer_text = (peer_str or (str(peer_val) if peer_val is not None else "")).strip()
    if not peer_text:
        peer_text = None
    elif len(peer_text) > 255:
        peer_text = peer_text[:255]

    existing: Optional[Dict[str, Any]] = None
    if telegram_val is not None:
        existing = await _fetchrow(
            """
            SELECT id, tenant_id
            FROM leads
            WHERE tenant_id = $1
              AND telegram_user_id = $2::bigint
            LIMIT 1;
            """,
            tenant_val,
            telegram_val,
        )
    peer_first = channel_val == "avito"

    if existing is None and peer_first and peer_text and source_val is not None:
        existing = await _fetchrow(
            """
            SELECT id, tenant_id
            FROM leads
            WHERE tenant_id = $1
              AND channel = $2
              AND peer = $3
              AND source_real_id = $4
            LIMIT 1;
            """,
            tenant_val,
            channel_val,
            peer_text,
            source_val,
        )
    if existing is None and peer_first and peer_text and source_val is None:
        existing = await find_lead_by_peer(tenant_val, channel_val, peer_text)
    if existing is None and lead_val is not None:
        existing = await _fetchrow(
            """
            SELECT id, tenant_id
            FROM leads
            WHERE id = $1::bigint
              AND ($2 = 0 OR tenant_id = $2)
            LIMIT 1;
            """,
            lead_val,
            tenant_val,
        )
    if existing is None and source_val is not None and channel_val != "avito":
        existing = await _fetchrow(
            """
            SELECT id, tenant_id
            FROM leads
            WHERE source_real_id = $1
              AND ($2 = 0 OR tenant_id = $2)
            LIMIT 1;
            """,
            source_val,
            tenant_val,
        )
    if existing is None and peer_text and not (peer_first and source_val is not None):
        existing = await find_lead_by_peer(tenant_val, channel_val, peer_text)

    if existing is not None:
        existing_id = existing.get("id")
        try:
            existing_id_val = int(existing_id) if existing_id is not None else 0
        except Exception:
            existing_id_val = 0
        target_id = existing_id_val
        existing_tenant = existing.get("tenant_id")
        try:
            existing_tenant_val = int(existing_tenant) if existing_tenant is not None else 0
        except Exception:
            existing_tenant_val = 0
        tenant_update = existing_tenant_val if existing_tenant_val > 0 else tenant_val
        if telegram_val is not None:
            await _exec(
                """
                UPDATE leads
                SET channel = CASE WHEN $2::text <> '' THEN $2::text ELSE channel END,
                    source_real_id = COALESCE($3::int, source_real_id),
                    tenant_id = CASE WHEN $4::int > 0 THEN $4::int ELSE tenant_id END,
                    telegram_user_id = $5::bigint,
                    telegram_username = COALESCE(NULLIF($6::text, ''), telegram_username),
                    peer = COALESCE(NULLIF($7::text, ''), peer),
                    contact = COALESCE(NULLIF($8::text, ''), contact),
                    title = COALESCE(NULLIF($9::text, ''), title),
                    updated_at = now()
                WHERE id = $1::bigint;
                """,
                existing_id_val,
                channel_val,
                source_val,
                tenant_update,
                telegram_val,
                username_val,
                peer_text,
                contact_val,
                title_val,
            )
        else:
            await _exec(
                """
                UPDATE leads
                SET channel = CASE WHEN $2::text <> '' THEN $2::text ELSE channel END,
                    source_real_id = COALESCE($3::int, source_real_id),
                    tenant_id = CASE WHEN $4::int > 0 THEN $4::int ELSE tenant_id END,
                    peer = COALESCE(NULLIF($5::text, ''), peer),
                    contact = COALESCE(NULLIF($6::text, ''), contact),
                    title = COALESCE(NULLIF($7::text, ''), title),
                    updated_at = now()
                WHERE id = $1::bigint;
                """,
                existing_id_val,
                channel_val,
                source_val,
                tenant_update,
                peer_text,
                contact_val,
                title_val,
            )
        return target_id or existing_id_val

    if lead_val is None:
        raise ValueError("lead_id or telegram_user_id must be provided")

    row = await _fetchrow(
        """
        INSERT INTO leads(id, title, channel, source_real_id, tenant_id, telegram_user_id, telegram_username, peer, contact)
        VALUES($1::bigint, $2::text, $3::text, $4::int, $5::int, $6::bigint, $7::text, $8::text, $9::text)
        ON CONFLICT (id)
        DO UPDATE SET channel = EXCLUDED.channel,
                      source_real_id = COALESCE(EXCLUDED.source_real_id, leads.source_real_id),
                      tenant_id = CASE
                          WHEN EXCLUDED.tenant_id > 0 THEN EXCLUDED.tenant_id
                          ELSE leads.tenant_id
                      END,
                      telegram_user_id = COALESCE(EXCLUDED.telegram_user_id, leads.telegram_user_id),
                      telegram_username = COALESCE(NULLIF(EXCLUDED.telegram_username, ''), leads.telegram_username),
                      peer = COALESCE(NULLIF(EXCLUDED.peer, ''), leads.peer),
                      contact = COALESCE(NULLIF(EXCLUDED.contact, ''), leads.contact),
                      title = COALESCE(EXCLUDED.title, leads.title),
                      updated_at = now()
        RETURNING id;
        """,
        lead_val,
        title_val,
        channel_val,
        source_val,
        tenant_val,
        telegram_val,
        username_val,
        peer_text,
        contact_val,
    )
    if row and "id" in row and row["id"] is not None:
        try:
            return int(row["id"])
        except Exception:
            pass
    return lead_val


async def upsert_source_cache(lead_id: int, real_id: int):
    await _exec(
        """
        INSERT INTO source_cache(lead_id, real_id)
        VALUES($1, $2)
        ON CONFLICT (lead_id)
        DO UPDATE SET real_id = EXCLUDED.real_id,
                      updated_at = now();
    """,
        lead_id,
        real_id,
    )


async def lead_exists(lead_id: int, tenant_id: Optional[int] = None) -> bool:
    try:
        lead_val = int(lead_id)
    except Exception:
        return False
    if lead_val <= 0:
        return False
    try:
        tenant_val = int(tenant_id) if tenant_id is not None else 0
    except Exception:
        tenant_val = 0
    row = await _fetchrow(
        "SELECT 1 FROM leads WHERE id = $1::bigint AND ($2 = 0 OR tenant_id = $2) LIMIT 1",
        lead_val,
        tenant_val,
    )
    return bool(row)


# -------- Contacts / linking --------


async def resolve_or_create_contact(
    tenant_id: Optional[int] = None,
    phone: Optional[str] = None,
    whatsapp_phone: Optional[str] = None,
    avito_user_id: Optional[int] = None,
    avito_login: Optional[str] = None,
    telegram_user_id: Optional[int] = None,
    telegram_username: Optional[str] = None,
    max_user_id: Optional[int] = None,
    max_username: Optional[str] = None,
) -> int:
    # поиск по приоритету: whatsapp_phone -> avito_user_id -> avito_login
    contact_id: int | None = None
    telegram_username_norm = (telegram_username or "").strip() or None
    max_username_norm = (max_username or "").strip() or None
    avito_login_norm = (avito_login or "").strip() or None
    phone_norm = None
    tenant_val = 0
    if tenant_id is not None:
        try:
            tenant_val = int(tenant_id)
        except Exception:
            tenant_val = 0
    use_tenant_scope = tenant_val > 0
    if phone:
        try:
            phone_norm = _normalize_e164_digits(phone)
        except Exception:
            phone_norm = None
    if whatsapp_phone:
        if use_tenant_scope:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE tenant_id=$1::int AND whatsapp_phone=$2",
                tenant_val,
                whatsapp_phone,
            )
        else:
            row = await _fetchrow("SELECT id FROM contacts WHERE whatsapp_phone=$1", whatsapp_phone)
        if row:
            contact_id = row["id"]
    if contact_id is None and phone_norm:
        if use_tenant_scope:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE tenant_id=$1::int AND phone=$2",
                tenant_val,
                phone_norm,
            )
        else:
            row = await _fetchrow("SELECT id FROM contacts WHERE phone=$1", phone_norm)
        if row:
            contact_id = row["id"]
    if contact_id is None and avito_user_id:
        if use_tenant_scope:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE tenant_id=$1::int AND avito_user_id=$2",
                tenant_val,
                avito_user_id,
            )
        else:
            row = await _fetchrow("SELECT id FROM contacts WHERE avito_user_id=$1", avito_user_id)
        if row:
            contact_id = row["id"]
    if contact_id is None and avito_login_norm:
        if use_tenant_scope:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE tenant_id=$1::int AND avito_login=$2 LIMIT 1",
                tenant_val,
                avito_login_norm,
            )
        else:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE avito_login=$1 LIMIT 1", avito_login_norm
            )
        if row:
            contact_id = row["id"]
    if contact_id is None and telegram_user_id:
        if use_tenant_scope:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE tenant_id=$1::int AND telegram_user_id=$2::bigint",
                tenant_val,
                telegram_user_id,
            )
        else:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE telegram_user_id=$1::bigint", telegram_user_id
            )
        if row:
            contact_id = row["id"]
    if contact_id is None and max_user_id:
        if use_tenant_scope:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE tenant_id=$1::int AND max_user_id=$2::bigint",
                tenant_val,
                max_user_id,
            )
        else:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE max_user_id=$1::bigint", max_user_id
            )
        if row:
            contact_id = row["id"]
    if contact_id is None and max_username_norm:
        if use_tenant_scope:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE tenant_id=$1::int AND max_username=$2 LIMIT 1",
                tenant_val,
                max_username_norm,
            )
        else:
            row = await _fetchrow(
                "SELECT id FROM contacts WHERE max_username=$1 LIMIT 1", max_username_norm
            )
        if row:
            contact_id = row["id"]

    if contact_id is not None:
        if telegram_user_id:
            await _exec(
                """
                UPDATE contacts
                SET telegram_user_id = COALESCE(telegram_user_id, $2::bigint),
                    updated_at = now()
                WHERE id = $1;
                """,
                contact_id,
                telegram_user_id,
            )
        if telegram_username_norm:
            await _exec(
                """
                UPDATE contacts
                SET telegram_username = COALESCE(NULLIF($2, ''), telegram_username),
                    updated_at = now()
                WHERE id = $1;
                """,
                contact_id,
                telegram_username_norm,
            )
        if max_user_id:
            await _exec(
                """
                UPDATE contacts
                SET max_user_id = COALESCE(max_user_id, $2::bigint),
                    updated_at = now()
                WHERE id = $1;
                """,
                contact_id,
                max_user_id,
            )
        if max_username_norm:
            await _exec(
                """
                UPDATE contacts
                SET max_username = COALESCE(NULLIF($2, ''), max_username),
                    updated_at = now()
                WHERE id = $1;
                """,
                contact_id,
                max_username_norm,
            )
        if phone_norm:
            await _exec(
                """
                UPDATE contacts
                SET phone = COALESCE(phone, $2),
                    whatsapp_phone = COALESCE(whatsapp_phone, $2),
                    updated_at = now()
                WHERE id = $1;
                """,
                contact_id,
                phone_norm,
            )
        return int(contact_id)

    if not any(
        (
            phone_norm,
            whatsapp_phone,
            avito_user_id,
            avito_login_norm,
            telegram_user_id,
            telegram_username_norm,
            max_user_id,
            max_username_norm,
        )
    ):
        return 0

    row = await _fetchrow(
        """
        INSERT INTO contacts(
            tenant_id,
            phone,
            whatsapp_phone,
            avito_user_id,
            avito_login,
            telegram_user_id,
            telegram_username,
            max_user_id,
            max_username
        )
        VALUES($1::int,$2,$3,$4,$5,$6::bigint,$7,$8::bigint,$9)
        RETURNING id
    """,
        tenant_val,
        phone_norm or whatsapp_phone,
        whatsapp_phone or phone_norm,
        avito_user_id,
        avito_login_norm,
        telegram_user_id,
        telegram_username_norm,
        max_user_id,
        max_username_norm,
    )
    # если БД недоступна — вернём фиктивный id, чтобы не падал вызов
    return int(row["id"]) if row and "id" in row else 0


async def link_lead_contact(
    lead_id: int,
    contact_id: int,
    *,
    channel: Optional[str] = None,
    peer: Optional[str] = None,
):
    channel_val = (channel or "").strip() or None
    peer_val = (peer or "").strip() or None
    await _exec(
        """
        INSERT INTO lead_contacts(lead_id, contact_id, channel, peer)
        VALUES($1::bigint, $2, $3, $4)
        ON CONFLICT (lead_id)
        DO UPDATE SET contact_id = EXCLUDED.contact_id,
                      channel = COALESCE(NULLIF(EXCLUDED.channel, ''), lead_contacts.channel),
                      peer = COALESCE(NULLIF(EXCLUDED.peer, ''), lead_contacts.peer),
                      linked_at = now();
    """,
        lead_id,
        contact_id,
        channel_val,
        peer_val,
    )


async def get_contact_id_by_lead(lead_id: int) -> Optional[int]:
    row = await _fetchrow("SELECT contact_id FROM lead_contacts WHERE lead_id=$1::bigint", lead_id)
    return row["contact_id"] if row else None


async def get_contact_phone_by_lead(lead_id: int) -> Optional[str]:
    row = await _fetchrow(
        """
        SELECT COALESCE(c.phone, c.whatsapp_phone) AS phone
        FROM contacts c
        JOIN lead_contacts lc ON lc.contact_id = c.id
        WHERE lc.lead_id = $1::bigint
        LIMIT 1
        """,
        lead_id,
    )
    if row and row.get("phone"):
        return row["phone"]
    return None


async def get_contact_id_by_phone(phone: str | None, tenant_id: int | None = None) -> Optional[int]:
    """Return contact id that owns the phone (either phone or whatsapp_phone)."""
    if not phone:
        return None
    from libs.core.transport import normalize_e164_digits  # local import to avoid circular dep

    try:
        digits = normalize_e164_digits(phone)
    except Exception:
        # fallback to stripping non-digits
        digits = re.sub(r"\D", "", str(phone))
    if not digits:
        return None
    if tenant_id is not None:
        row = await _fetchrow(
            "SELECT id FROM contacts WHERE tenant_id=$1::int AND (whatsapp_phone=$2 OR phone=$2) LIMIT 1",
            int(tenant_id),
            digits,
        )
    else:
        row = await _fetchrow(
            "SELECT id FROM contacts WHERE whatsapp_phone=$1 OR phone=$1 LIMIT 1",
            digits,
        )
    return int(row["id"]) if row and "id" in row and row["id"] is not None else None


# -------- Outbox --------


async def ensure_outbox_queued(
    lead_id: int,
    text: str,
    *,
    tenant_id: Optional[int] = None,
) -> str:
    dedup = sha1(text)
    try:
        tenant_val = int(tenant_id) if tenant_id is not None else 0
    except Exception:
        tenant_val = 0
    await _exec(
        """
        INSERT INTO outbox(lead_id, text, dedup_hash, status)
        SELECT $1::bigint, $2, $3, 'queued'
        WHERE EXISTS (
            SELECT 1
            FROM leads
            WHERE id = $1::bigint
              AND ($4 = 0 OR tenant_id = $4)
        )
        ON CONFLICT (lead_id, dedup_hash) DO NOTHING;
        """,
        lead_id,
        text,
        dedup,
        tenant_val,
    )
    return dedup


async def bump_attempt(lead_id: int, d: str, error: Optional[str] = None):
    await _exec(
        """
        UPDATE outbox
        SET attempts = attempts + 1,
            last_error = left($3, 2000),
            status = 'retry',
            updated_at = now()
        WHERE lead_id = $1::bigint AND dedup_hash = $2;
    """,
        lead_id,
        d,
        error or "",
    )


async def mark_sent(lead_id: int, d: str):
    await _exec(
        """
        UPDATE outbox
        SET status = 'sent',
            sent_at = now(),
            updated_at = now(),
            last_error = NULL
        WHERE lead_id = $1::bigint AND dedup_hash = $2;
    """,
        lead_id,
        d,
    )


async def mark_failed(lead_id: int, d: str, error: str):
    await _exec(
        """
        UPDATE outbox
        SET status = 'failed',
            last_error = left($3, 2000),
            updated_at = now()
        WHERE lead_id = $1::bigint AND dedup_hash = $2;
    """,
        lead_id,
        d,
        error,
    )


async def take_outbox_batch(limit: int = 10) -> list[Dict[str, Any]]:
    pool = await _ensure_pool()
    if not pool:
        return []
    async with pool.acquire() as con:
        async with con.transaction():
            rows = await con.fetch(
                """
                WITH next AS (
                    SELECT o.id
                    FROM outbox o
                    WHERE o.status IN ('queued', 'retry')
                    ORDER BY COALESCE(o.updated_at, o.created_at) DESC
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ),
                updated AS (
                    UPDATE outbox o
                    SET status = 'processing',
                        updated_at = now()
                    WHERE o.id IN (SELECT id FROM next)
                    RETURNING o.id, o.lead_id, o.text, o.dedup_hash, o.attempts
                )
                SELECT u.id,
                       u.lead_id,
                       u.text,
                       u.dedup_hash,
                       u.attempts,
                       l.tenant_id,
                       l.telegram_user_id,
                       l.channel
                FROM updated u
                JOIN leads l ON l.id = u.lead_id;
                """,
                limit,
            )
    return [dict(row) for row in rows]


# -------- Messages --------


async def insert_message_in(
    lead_id: int,
    text: str,
    status: str = "received",
    tenant_id: Optional[int] = None,
    telegram_user_id: Optional[int] = None,
    provider_msg_id: Optional[str] = None,
    *,
    is_bot: bool = False,
    attachments: Optional[list[dict[str, Any]]] = None,
    source: Optional[str] = None,
) -> int:
    if _offline_enabled():
        _offline_append_message(lead_id, text, direction=0, tenant_id=tenant_id, is_bot=is_bot)
        return 0
    tenant_val = int(tenant_id or 0)
    telegram_val = 0
    if telegram_user_id is not None:
        try:
            telegram_val = int(telegram_user_id)
        except Exception:
            telegram_val = 0
    if telegram_val <= 0:
        telegram_val = 0
    row = await _fetchrow(
        """
        INSERT INTO messages(lead_id, direction, text, provider_msg_id, status, tenant_id, telegram_user_id, is_bot, attachments, source)
        VALUES($1::bigint, 0, $2, $3, $4, $5, $6::bigint, $7::boolean, $8::jsonb, $9)
        RETURNING id;
    """,
        lead_id,
        text,
        provider_msg_id,
        status,
        tenant_val,
        telegram_val,
        bool(is_bot),
        json.dumps(attachments, ensure_ascii=False) if attachments else None,
        source,
    )
    return int(row["id"]) if row and "id" in row and row["id"] is not None else 0


async def has_recent_incoming_message(
    lead_id: int,
    tenant_id: Optional[int] = None,
    *,
    within_seconds: int = 24 * 60 * 60,
) -> bool:
    """Check whether the lead has inbound messages within the given window."""

    if lead_id is None or int(lead_id) <= 0:
        return False

    try:
        interval = float(within_seconds)
    except Exception:
        interval = 0.0
    if interval <= 0:
        return False

    params: list[Any] = [int(lead_id), interval]
    tenant_clause = ""
    if tenant_id is not None:
        try:
            tenant_value = int(tenant_id)
        except Exception:
            tenant_value = None
        if tenant_value is not None:
            params.append(tenant_value)
            tenant_clause = f" AND tenant_id = ${len(params)}"

    sql = (
        "SELECT 1"
        " FROM messages"
        " WHERE lead_id = $1::bigint"
        "   AND direction = 0"
        "   AND created_at >= now() - ($2::double precision * INTERVAL '1 second')"
        f"{tenant_clause}"
        " LIMIT 1"
    )
    row = await _fetchrow(sql, *params)
    return bool(row)


async def insert_message_out(
    lead_id: int,
    text: str,
    provider_msg_id: Optional[str],
    status: str = "sent",
    tenant_id: Optional[int] = None,
    channel: str | None = None,
    telegram_user_id: Optional[int] = None,
    telegram_username: Optional[str] = None,
    *,
    title: Optional[str] = None,
    is_bot: bool = False,
    attachments: Optional[list[dict[str, Any]]] = None,
    source: Optional[str] = None,
) -> int:
    upsert_kwargs = {
        "channel": channel or "whatsapp",
        "tenant_id": tenant_id,
        "telegram_username": telegram_username,
        "title": title,
        "peer_id": telegram_user_id,
    }
    if telegram_user_id is not None:
        try:
            upsert_kwargs["telegram_user_id"] = int(telegram_user_id)
        except Exception:
            pass
    resolved_lead_id = await upsert_lead(
        lead_id,
        **upsert_kwargs,
    )
    lead_ref = resolved_lead_id or lead_id
    if _offline_enabled():
        _offline_append_message(lead_ref, text, direction=1, tenant_id=tenant_id, is_bot=is_bot)
        return 0
    tenant_val = int(tenant_id or 0)
    telegram_val = 0
    if telegram_user_id is not None:
        try:
            telegram_val = int(telegram_user_id)
        except Exception:
            telegram_val = 0
    if telegram_val <= 0:
        telegram_val = 0
    row = await _fetchrow(
        """
        INSERT INTO messages(lead_id, direction, text, provider_msg_id, status, tenant_id, telegram_user_id, is_bot, attachments, source)
        VALUES($1::bigint, 1, $2, $3, $4, $5, $6::bigint, $7::boolean, $8::jsonb, $9)
        RETURNING id;
    """,
        lead_ref,
        text,
        provider_msg_id,
        status,
        tenant_val,
        telegram_val,
        bool(is_bot),
        json.dumps(attachments, ensure_ascii=False) if attachments else None,
        source,
    )
    return int(row["id"]) if row and "id" in row and row["id"] is not None else 0


async def update_message_status(
    message_id: int,
    status: str,
    *,
    provider_msg_id: Optional[str] = None,
) -> None:
    await _exec(
        """
        UPDATE messages
        SET status = $2,
            provider_msg_id = COALESCE($3, provider_msg_id)
        WHERE id = $1;
    """,
        message_id,
        status,
        provider_msg_id,
    )


async def get_lead_dialog_metadata(lead_id: int) -> Optional[Mapping[str, Any]]:
    try:
        lead_ref = int(lead_id)
    except Exception:
        return None
    if lead_ref <= 0:
        return None

    row = await _fetchrow(
        """
        SELECT leads.id,
               leads.tenant_id,
               leads.channel,
               leads.peer,
               leads.contact,
               leads.title,
               leads.source_real_id,
               leads.telegram_user_id,
               leads.telegram_username,
               lc.contact_id,
               c.phone,
               c.whatsapp_phone,
               c.avito_login,
               c.avito_user_id,
               c.telegram_username AS contact_telegram_username,
               c.max_user_id,
               c.max_username
        FROM leads
        LEFT JOIN lead_contacts lc ON lc.lead_id = leads.id
        LEFT JOIN contacts c ON c.id = lc.contact_id
        WHERE leads.id = $1::bigint
        LIMIT 1;
        """,
        lead_ref,
    )
    if not row:
        return None
    try:
        return dict(row)
    except Exception:
        return row if isinstance(row, Mapping) else None


async def fetch_dialogs_for_tenant(
    tenant_id: int,
    *,
    channels: Optional[list[str]] = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return []
    if tenant_val <= 0:
        return []

    limit_val = limit if isinstance(limit, int) and limit > 0 else 200
    channel_list: list[str] = []
    if channels:
        for ch in channels:
            if isinstance(ch, str) and ch.strip():
                channel_list.append(ch.strip().lower())

    params: list[Any] = [tenant_val]
    sql = """
        SELECT l.id,
               l.channel,
               l.title,
               l.contact,
               l.peer,
               l.source_real_id,
               c.avito_login,
               c.telegram_username,
               c.max_user_id,
               c.max_username,
               last_msg.text AS last_message,
               last_msg.created_at AS last_ts
        FROM leads l
        LEFT JOIN lead_contacts lc ON lc.lead_id = l.id
        LEFT JOIN contacts c ON c.id = lc.contact_id
        LEFT JOIN LATERAL (
            SELECT m.text, m.created_at
            FROM messages m
            WHERE m.lead_id = l.id
            ORDER BY m.created_at DESC, m.id DESC
            LIMIT 1
        ) AS last_msg ON TRUE
        WHERE l.tenant_id = $1
    """
    if channel_list:
        params.append(channel_list)
        sql += f" AND l.channel = ANY(${len(params)})"
    params.append(limit_val)
    sql += f" ORDER BY COALESCE(last_msg.created_at, l.updated_at) DESC NULLS LAST, l.id DESC LIMIT ${len(params)}"

    rows = await _fetch(sql, *params)
    results: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            results.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                results.append(dict(row.items()))
    return results


async def list_messages_for_lead(
    tenant_id: int,
    lead_id: int,
    *,
    limit: int = 50,
    before: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    try:
        tenant_val = int(tenant_id)
        lead_ref = int(lead_id)
    except Exception:
        return []
    if tenant_val <= 0 or lead_ref <= 0:
        return []

    params: list[Any] = [tenant_val, lead_ref]
    sql_parts = [
        "SELECT id, lead_id, direction, text, status, created_at, is_bot, attachments, source",
        "FROM messages",
        "WHERE tenant_id = $1 AND lead_id = $2",
    ]
    if before is not None:
        params.append(before)
        sql_parts.append(f"AND created_at < ${len(params)}")
    limit_val = limit if isinstance(limit, int) and limit > 0 else 50
    params.append(limit_val)
    sql_parts.append(f"ORDER BY created_at DESC, id DESC LIMIT ${len(params)}")
    rows = await _fetch(" ".join(sql_parts), *params)
    messages: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            messages.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                messages.append(dict(row.items()))
    messages.reverse()  # chronological order
    return messages


async def list_recent_messages(
    tenant_id: int,
    *,
    since: Optional[datetime] = None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return []
    if tenant_val <= 0:
        return []
    limit_val = limit if isinstance(limit, int) and limit > 0 else 2000
    params: list[Any] = [tenant_val]
    sql_parts = [
        "SELECT lead_id, direction, is_bot, text, created_at",
        "FROM messages",
        "WHERE tenant_id = $1",
        "AND text IS NOT NULL",
    ]
    if since is not None:
        params.append(since)
        sql_parts.append(f"AND created_at >= ${len(params)}")
    params.append(limit_val)
    sql_parts.append("ORDER BY created_at ASC, id ASC")
    sql_parts.append(f"LIMIT ${len(params)}")
    rows = await _fetch(" ".join(sql_parts), *params)
    results: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            results.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                results.append(dict(row.items()))
    return results


async def get_tenant_message_stats(
    tenant_id: int,
    *,
    days: int = 7,
    limit: int = 20000,
) -> dict[str, Any]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return {}
    if tenant_val <= 0:
        return {}
    try:
        days_val = int(days)
    except Exception:
        days_val = 7
    if days_val <= 0:
        days_val = 7
    limit_val = limit if isinstance(limit, int) and limit > 0 else 20000
    limit_val = min(limit_val, 50000)

    rows = await _fetch(
        """
        SELECT m.id,
               m.lead_id,
               m.direction,
               m.is_bot,
               m.source,
               m.text,
               m.created_at,
               l.channel
        FROM messages m
        JOIN leads l ON l.id = m.lead_id
        WHERE m.tenant_id = $1
          AND m.created_at >= now() - ($2::int || ' days')::interval
        ORDER BY m.created_at ASC, m.id ASC
        LIMIT $3;
        """,
        tenant_val,
        days_val,
        limit_val,
    )
    return {"rows": [dict(row) for row in rows or []], "days": days_val, "limit": limit_val}


async def list_recent_inbound_texts(
    tenant_id: int,
    lead_id: int,
    *,
    limit: int = 5,
) -> list[str]:
    try:
        tenant_val = int(tenant_id)
        lead_val = int(lead_id)
    except Exception:
        return []
    if tenant_val <= 0 or lead_val <= 0:
        return []
    limit_val = limit if isinstance(limit, int) and limit > 0 else 5
    rows = await _fetch(
        """
        SELECT text
        FROM messages
        WHERE tenant_id = $1
          AND lead_id = $2
          AND direction = 0
        ORDER BY created_at DESC, id DESC
        LIMIT $3
        """,
        tenant_val,
        lead_val,
        limit_val,
    )
    texts: list[str] = []
    for row in rows or []:
        if not row:
            continue
        value = row.get("text") if isinstance(row, Mapping) else None
        if value is None:
            try:
                value = row[0]
            except Exception:
                value = None
        if value is None:
            continue
        texts.append(str(value))
    texts.reverse()
    return texts


async def list_recent_stage_router_texts(
    tenant_id: int,
    lead_id: int,
    *,
    limit: int = 8,
) -> list[str]:
    try:
        tenant_val = int(tenant_id)
        lead_val = int(lead_id)
    except Exception:
        return []
    if tenant_val <= 0 or lead_val <= 0:
        return []
    limit_val = limit if isinstance(limit, int) and limit > 0 else 8
    rows = await _fetch(
        """
        SELECT direction, source, text
        FROM messages
        WHERE tenant_id = $1
          AND lead_id = $2
          AND text IS NOT NULL
          AND btrim(text) <> ''
          AND (
              direction = 0
              OR lower(coalesce(source, '')) = 'manager'
              OR lower(coalesce(source, '')) LIKE 'manager:%'
              OR lower(coalesce(source, '')) = 'bot'
              OR lower(coalesce(source, '')) LIKE 'bot:%'
          )
        ORDER BY created_at DESC, id DESC
        LIMIT $3
        """,
        tenant_val,
        lead_val,
        limit_val,
    )
    items: list[str] = []
    for row in rows or []:
        if not row:
            continue
        if isinstance(row, Mapping):
            text_val = row.get("text")
            direction_val = row.get("direction")
            source_val = row.get("source")
        else:
            try:
                text_val = row[2]
                direction_val = row[0]
                source_val = row[1]
            except Exception:
                continue
        text = str(text_val or "").strip()
        if not text:
            continue
        try:
            direction_int = int(direction_val)
        except Exception:
            direction_int = 0
        source_norm = str(source_val or "").strip().lower()
        if direction_int == 0:
            role = "client"
        elif source_norm == "manager" or source_norm.startswith("manager:"):
            role = "manager"
        else:
            role = "bot"
        items.append(f"{role}: {text}")
    items.reverse()
    return items


async def create_message_feedback(
    tenant_id: int,
    message_id: int,
    rating: str,
    comment: Optional[str] = None,
    *,
    lead_id: Optional[int] = None,
    expected_answer: Optional[str] = None,
    created_by: Optional[str] = None,
) -> int:
    try:
        tenant_val = int(tenant_id)
        message_ref = int(message_id)
    except Exception:
        return 0
    if tenant_val <= 0 or message_ref <= 0:
        return 0
    row = await _fetchrow(
        """
        INSERT INTO message_feedback(tenant_id, lead_id, message_id, rating, comment, expected_answer, created_by)
        VALUES($1, $2, $3, $4, $5, $6, $7)
        RETURNING id;
        """,
        tenant_val,
        lead_id,
        message_ref,
        rating,
        comment,
        expected_answer,
        created_by,
    )
    if not row:
        return 0
    try:
        return int(row["id"])
    except Exception:
        return 0


async def get_message_metadata(message_id: int) -> Optional[Mapping[str, Any]]:
    try:
        msg_ref = int(message_id)
    except Exception:
        return None
    if msg_ref <= 0:
        return None
    row = await _fetchrow(
        """
        SELECT id,
               lead_id,
               tenant_id,
               direction,
               is_bot,
               text,
               created_at
        FROM messages
        WHERE id = $1
        LIMIT 1;
        """,
        msg_ref,
    )
    if not row:
        return None
    try:
        return dict(row)
    except Exception:
        return row if isinstance(row, Mapping) else None


async def get_previous_incoming_message(
    tenant_id: int,
    lead_id: int,
    *,
    before: datetime,
) -> Optional[Mapping[str, Any]]:
    try:
        tenant_val = int(tenant_id)
        lead_ref = int(lead_id)
    except Exception:
        return None
    if tenant_val <= 0 or lead_ref <= 0:
        return None
    rows = await _fetch(
        """
        SELECT id, text, created_at
        FROM messages
        WHERE tenant_id = $1
          AND lead_id = $2
          AND direction = 0
          AND created_at <= $3
        ORDER BY created_at DESC, id DESC
        LIMIT 5;
        """,
        tenant_val,
        lead_ref,
        before,
    )
    if not rows:
        return None

    def _clean_text(value: Any) -> str:
        if not value:
            return ""
        text = str(value).strip()
        # Ignore very short or non-informative fragments.
        if len(text) < 4:
            return ""
        if all(ch in ".-_,!? " for ch in text):
            return ""
        return text

    selected: list[Mapping[str, Any]] = []
    for row in rows:
        try:
            candidate = row if isinstance(row, dict) else dict(row)
        except Exception:
            if isinstance(row, Mapping):
                candidate = dict(row)
            else:
                continue
        if not isinstance(candidate, Mapping):
            continue
        text = _clean_text(candidate.get("text"))
        if text:
            selected.append({**candidate, "text": text})
        if len(selected) >= 3:
            break

    if not selected:
        return None

    selected_reversed = list(reversed(selected))
    combined_text = " ".join(
        item.get("text", "").strip() for item in selected_reversed if item.get("text")
    )
    latest = selected[0]
    return {
        "id": latest.get("id"),
        "text": combined_text,
        "created_at": latest.get("created_at"),
    }


async def record_training_example(
    tenant_id: int,
    *,
    lead_id: Optional[int],
    message_id: Optional[int],
    source: str,
    source_feedback_id: Optional[int],
    q_text: str,
    a_text: str,
    is_bad: bool = False,
    is_active: bool = True,
    embedding_status: str = "pending",
    embedding_model: Optional[str] = None,
    fingerprint: Optional[str] = None,
) -> int:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return 0
    if tenant_val <= 0:
        return 0
    lead_ref = None
    msg_ref = None
    fb_ref = None
    try:
        lead_ref = int(lead_id) if lead_id is not None else None
    except Exception:
        lead_ref = None
    try:
        msg_ref = int(message_id) if message_id is not None else None
    except Exception:
        msg_ref = None
    try:
        fb_ref = int(source_feedback_id) if source_feedback_id is not None else None
    except Exception:
        fb_ref = None

    sig = fingerprint or sha1(f"{q_text.strip()}\t{a_text.strip()}")
    existing = await _fetchrow(
        """
        SELECT id
        FROM training_examples
        WHERE tenant_id = $1
          AND fingerprint = $2
          AND is_bad = FALSE
        ORDER BY updated_at DESC
        LIMIT 1;
        """,
        tenant_val,
        sig,
    )
    if existing and "id" in existing and existing["id"] is not None:
        try:
            existing_id = int(existing["id"])
            if bool(is_active):
                await _exec(
                    """
                    UPDATE training_examples
                    SET updated_at = now(),
                        a_text = $2,
                        source = COALESCE($3, source)
                    WHERE id = $1;
                    """,
                    existing_id,
                    a_text,
                    source,
                )
            return existing_id
        except Exception:
            pass

    row = await _fetchrow(
        """
        INSERT INTO training_examples (
            tenant_id, lead_id, message_id, source, source_feedback_id,
            q_text, a_text, fingerprint, is_bad, is_active,
            embedding_status, embedding_model
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING id;
        """,
        tenant_val,
        lead_ref,
        msg_ref,
        source,
        fb_ref,
        q_text,
        a_text,
        sig,
        bool(is_bad),
        bool(is_active),
        embedding_status,
        embedding_model,
    )
    if not row:
        return 0
    try:
        return int(row["id"])
    except Exception:
        return 0


async def mark_bad_bot_message(
    tenant_id: int,
    message_id: int,
    *,
    feedback_id: Optional[int] = None,
    reason: Optional[str] = None,
) -> None:
    try:
        tenant_val = int(tenant_id)
        msg_ref = int(message_id)
    except Exception:
        return
    if tenant_val <= 0 or msg_ref <= 0:
        return
    fb_ref = None
    try:
        fb_ref = int(feedback_id) if feedback_id is not None else None
    except Exception:
        fb_ref = None
    await _exec(
        """
        INSERT INTO bad_bot_messages(tenant_id, message_id, feedback_id, reason)
        VALUES($1, $2, $3, $4)
        ON CONFLICT (tenant_id, message_id) DO NOTHING;
        """,
        tenant_val,
        msg_ref,
        fb_ref,
        reason,
    )


async def get_training_examples_for_retrieval(
    tenant_id: int,
    *,
    limit: int = 200,
    require_embedding: bool = False,
) -> list[dict[str, Any]]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return []
    if tenant_val <= 0:
        return []
    limit_val = limit if isinstance(limit, int) and limit > 0 else 200
    params: list[Any] = [tenant_val]
    sql_parts = [
        "SELECT id, q_text, a_text, source, embedding, embedding_model, embedding_status, times_used, updated_at, created_at, is_bad",
        "FROM training_examples",
        "WHERE tenant_id = $1 AND is_active = TRUE AND is_bad = FALSE",
    ]
    if require_embedding:
        sql_parts.append("AND embedding_status = 'ready'")
    params.append(limit_val)
    sql_parts.append(f"ORDER BY updated_at DESC, id DESC LIMIT ${len(params)}")
    rows = await _fetch(" ".join(sql_parts), *params)
    results: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            results.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                results.append(dict(row.items()))
    return results


async def activate_training_examples(example_ids: list[int]) -> None:
    if not example_ids:
        return
    ids: list[int] = []
    for val in example_ids:
        try:
            coerced = int(val)
        except Exception:
            continue
        if coerced > 0:
            ids.append(coerced)
    if not ids:
        return
    await _exec(
        """
        UPDATE training_examples
        SET is_active = TRUE,
            embedding_status = 'pending',
            updated_at = now()
        WHERE id = ANY($1::bigint[]);
        """,
        ids,
    )


async def get_tenant_model(tenant_id: int) -> Optional[Mapping[str, Any]]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return None
    if tenant_val <= 0:
        return None
    row = await _fetchrow(
        """
        SELECT tenant_id, base_model, finetune_model, use_finetune, updated_at
        FROM tenant_models
        WHERE tenant_id = $1
        LIMIT 1;
        """,
        tenant_val,
    )
    if not row:
        return None
    try:
        return dict(row)
    except Exception:
        return row if isinstance(row, Mapping) else None


async def get_feedback_counts(tenant_id: int) -> dict[str, int]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return {"like": 0, "dislike": 0}
    if tenant_val <= 0:
        return {"like": 0, "dislike": 0}
    rows = await _fetch(
        """
        SELECT rating, COUNT(*) AS total
        FROM message_feedback
        WHERE tenant_id = $1
        GROUP BY rating;
        """,
        tenant_val,
    )
    result = {"like": 0, "dislike": 0}
    for row in rows or []:
        try:
            rating = str(row.get("rating") or "").strip().lower()
            total = int(row.get("total") or 0)
        except Exception:
            try:
                rating = str(row["rating"]).strip().lower()
                total = int(row["total"])
            except Exception:
                continue
        if rating in result:
            result[rating] = total
    return result


async def increment_training_examples_usage(example_ids: list[int]) -> None:
    if not example_ids:
        return
    ids: list[int] = []
    for val in example_ids:
        try:
            coerced = int(val)
        except Exception:
            continue
        if coerced > 0:
            ids.append(coerced)
    if not ids:
        return
    await _exec(
        "UPDATE training_examples SET times_used = times_used + 1, updated_at = now() WHERE id = ANY($1::bigint[])",
        ids,
    )


async def fetch_pending_training_examples(limit: int = 10) -> list[dict[str, Any]]:
    lim = limit if isinstance(limit, int) and limit > 0 else 10
    rows = await _fetch(
        """
        SELECT id, tenant_id, q_text, a_text, embedding_model
        FROM training_examples
        WHERE embedding_status = 'pending'
          AND is_active = TRUE
          AND is_bad = FALSE
        ORDER BY updated_at ASC, id ASC
        LIMIT $1;
        """,
        lim,
    )
    results: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            results.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                results.append(dict(row.items()))
    return results


async def set_training_embedding(
    example_id: int,
    embedding: Optional[list[float]],
    *,
    embedding_model: Optional[str] = None,
    status: str = "ready",
    error: Optional[str] = None,
) -> None:
    try:
        example_ref = int(example_id)
    except Exception:
        return
    if example_ref <= 0:
        return
    await _exec(
        """
        UPDATE training_examples
        SET embedding = $2,
            embedding_model = COALESCE($3, embedding_model),
            embedding_status = $4,
            embedding_error = $5,
            updated_at = now()
        WHERE id = $1;
        """,
        example_ref,
        embedding,
        embedding_model,
        status,
        error,
    )


async def feedback_exists(tenant_id: int, message_id: int) -> bool:
    try:
        tenant_val = int(tenant_id)
        message_ref = int(message_id)
    except Exception:
        return False
    if tenant_val <= 0 or message_ref <= 0:
        return False
    row = await _fetchrow(
        """
        SELECT 1
        FROM message_feedback
        WHERE tenant_id = $1
          AND message_id = $2
        LIMIT 1;
        """,
        tenant_val,
        message_ref,
    )
    return bool(row)


async def list_feedback_message_ids(tenant_id: int, message_ids: list[int]) -> set[int]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return set()
    if tenant_val <= 0 or not message_ids:
        return set()
    ids: list[int] = []
    for val in message_ids:
        try:
            coerced = int(val)
        except Exception:
            continue
        if coerced > 0:
            ids.append(coerced)
    if not ids:
        return set()
    rows = await _fetch(
        """
        SELECT message_id
        FROM message_feedback
        WHERE tenant_id = $1
          AND message_id = ANY($2::bigint[])
        """,
        tenant_val,
        ids,
    )
    found: set[int] = set()
    for row in rows or []:
        try:
            found.add(int(row.get("message_id") if isinstance(row, dict) else row["message_id"]))
        except Exception:
            try:
                found.add(int(row["message_id"]))
            except Exception:
                continue
    return found


async def list_recent_disliked_feedback(tenant_id: int, limit: int = 50) -> list[dict[str, Any]]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        return []
    if tenant_val <= 0:
        return []
    try:
        limit_val = int(limit)
    except Exception:
        limit_val = 50
    if limit_val <= 0:
        limit_val = 50
    limit_val = min(limit_val, 200)
    rows = await _fetch(
        """
        SELECT
            f.id AS feedback_id,
            f.message_id,
            f.lead_id,
            f.expected_answer,
            f.comment,
            f.created_at AS feedback_created_at,
            m.text AS bot_text,
            m.created_at AS bot_created_at,
            (
                SELECT text
                FROM messages mi
                WHERE mi.lead_id = m.lead_id
                  AND mi.direction = 0
                  AND mi.created_at <= m.created_at
                ORDER BY mi.created_at DESC
                LIMIT 1
            ) AS user_text
        FROM message_feedback f
        JOIN messages m ON m.id = f.message_id
        WHERE f.tenant_id = $1
          AND f.rating = 'dislike'
          AND (f.expected_answer IS NULL OR btrim(f.expected_answer) = '')
          AND (f.comment IS NULL OR btrim(f.comment) = '')
        ORDER BY f.created_at DESC
        LIMIT $2;
        """,
        tenant_val,
        limit_val,
    )
    out: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            out.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                out.append(dict(row.items()))
    return out


async def find_lead_by_telegram(
    tenant_id: int,
    telegram_user_id: int,
    *,
    channel: str = "telegram",
) -> Optional[int]:
    try:
        tenant_val = int(tenant_id)
    except Exception:
        tenant_val = 0
    try:
        telegram_val = int(telegram_user_id)
    except Exception:
        return None
    if telegram_val <= 0:
        return None
    _ = channel  # channel retained for compatibility; lookup relies on tenant/user identifiers
    row = await _fetchrow(
        """
        SELECT id
        FROM leads
        WHERE tenant_id = $1
          AND telegram_user_id = $2::bigint
        LIMIT 1;
    """,
        tenant_val,
        telegram_val,
    )
    if row and "id" in row and row["id"] is not None:
        try:
            return int(row["id"])
        except Exception:
            return None
    return None


async def get_telegram_user_id_by_lead(lead_id: int) -> Optional[int]:
    try:
        lead_ref = int(lead_id)
    except Exception:
        return None
    if lead_ref <= 0:
        return None
    row = await _fetchrow(
        """
        SELECT telegram_user_id
        FROM leads
        WHERE id = $1::bigint
        LIMIT 1;
    """,
        lead_ref,
    )
    if not row:
        return None
    value = None
    if "telegram_user_id" in row and row["telegram_user_id"] is not None:
        value = row["telegram_user_id"]
    elif getattr(row, "get", None):  # pragma: no branch - defensive access
        value = row.get("telegram_user_id")
    if value is None:
        return None
    try:
        coerced = int(value)
    except Exception:
        return None
    if coerced <= 0:
        return None
    return coerced


async def get_lead_peer(lead_id: int, channel: Optional[str] = None) -> Optional[str]:
    try:
        lead_ref = int(lead_id)
    except Exception:
        return None
    if lead_ref <= 0:
        return None

    channel_val = (channel or "").strip().lower()
    row = await _fetchrow(
        """
        SELECT COALESCE(NULLIF(lc.peer, ''), NULLIF(l.peer, '')) AS peer
        FROM leads l
        LEFT JOIN lead_contacts lc
            ON lc.lead_id = l.id
            AND ($2 = '' OR lc.channel = $2)
        WHERE l.id = $1::bigint
          AND ($2 = '' OR l.channel = $2)
        LIMIT 1;
        """,
        lead_ref,
        channel_val,
    )
    if not row:
        return None
    value: Optional[str]
    if isinstance(row, dict):
        value = row.get("peer")  # type: ignore[assignment]
    else:
        value = getattr(row, "peer", None)
        if value is None and getattr(row, "get", None):  # pragma: no branch - mapping-like row
            value = row.get("peer")  # type: ignore[assignment]
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str or None


async def get_recent_dialog_by_contact(contact_id: int, limit: int = 40) -> List[Dict[str, Any]]:
    rows = await _fetch(
        """
        SELECT m.direction, m.text, m.created_at
        FROM messages m
        JOIN lead_contacts lc ON lc.lead_id = m.lead_id
        WHERE lc.contact_id = $1
        ORDER BY m.id DESC
        LIMIT $2
    """,
        contact_id,
        limit,
    )
    data = list(reversed([dict(r) for r in rows]))
    return data


async def stream_whatsapp_dialogs(
    tenant_val: int,
    since_ts: Optional[float],
    until_ts: Optional[float],
    limit_dialogs: Optional[int],
    channel: str = "whatsapp",
    per_message_limit: Optional[int] = None,
    batch_size_dialogs: int = 200,
    message_batch_size: int = 1000,
) -> tuple[
    AsyncIterator[tuple[Dict[str, Any], AsyncIterator[List[Dict[str, Any]]]]], Dict[str, Any]
]:
    """Yield WhatsApp dialogs with batched message loaders and export metadata."""

    if channel not in {"whatsapp", "wa"}:
        channel = "whatsapp"

    now_ts = time.time()
    lower_limit = float(since_ts) if since_ts is not None else None
    upper_limit = float(until_ts) if until_ts is not None else now_ts

    try:
        limit_int = int(limit_dialogs) if limit_dialogs is not None else None
    except (TypeError, ValueError):
        limit_int = None
    if limit_int is not None and limit_int <= 0:
        limit_int = None

    try:
        per_limit_int = int(per_message_limit) if per_message_limit is not None else None
    except (TypeError, ValueError):
        per_limit_int = None
    if per_limit_int is not None and per_limit_int < 0:
        per_limit_int = None

    try:
        batch_size = int(batch_size_dialogs)
    except (TypeError, ValueError):
        batch_size = 200
    if batch_size <= 0:
        batch_size = 200

    try:
        message_batch = int(message_batch_size)
    except (TypeError, ValueError):
        message_batch = 1000
    if message_batch <= 0:
        message_batch = 1000

    pool = await _ensure_pool()
    if not pool:
        raise DatabaseUnavailableError("postgres_pool_unavailable")

    base_params = [tenant_val]
    base_conditions = [
        "COALESCE(l.tenant_id, 0) = $1",
        "l.channel IN ('whatsapp', 'wa')",
    ]
    if lower_limit is not None:
        base_params.append(float(lower_limit))
        base_conditions.append(f"m.created_at >= to_timestamp(${len(base_params)})")
    if upper_limit is not None:
        base_params.append(float(upper_limit))
        base_conditions.append(f"m.created_at <= to_timestamp(${len(base_params)})")

    candidate_sql_template = """
        SELECT
            m.lead_id,
            lc.contact_id,
            c.whatsapp_phone,
            c.is_group,
            l.title,
            MAX(m.created_at) AS last_created_at
        FROM messages m
        JOIN leads l ON l.id = m.lead_id
        LEFT JOIN lead_contacts lc ON lc.lead_id = m.lead_id
        LEFT JOIN contacts c ON c.id = lc.contact_id
        WHERE {conditions}
        GROUP BY m.lead_id, lc.contact_id, c.whatsapp_phone, c.is_group, l.title
        ORDER BY last_created_at DESC
        LIMIT ${limit_idx} OFFSET ${offset_idx}
    """

    candidate_rows: List[Dict[str, Any]] = []
    offset = 0
    remaining = limit_int
    candidate_total = 0
    while True:
        if remaining is not None and remaining <= 0:
            break
        limit_current = batch_size if remaining is None else min(batch_size, remaining)
        if limit_current <= 0:
            break

        params = list(base_params)
        limit_idx = len(params) + 1
        offset_idx = limit_idx + 1
        sql = candidate_sql_template.format(
            conditions=" AND ".join(base_conditions),
            limit_idx=limit_idx,
            offset_idx=offset_idx,
        )
        params.extend([int(limit_current), int(offset)])
        rows = await _fetch(sql, *params)
        batch_list = [dict(row) for row in rows]
        candidate_total += len(batch_list)
        if not batch_list:
            break
        for row in batch_list:
            candidate_rows.append(row)
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    break
        if remaining is not None and remaining <= 0:
            break
        offset += len(batch_list)

    meta: Dict[str, Any] = {
        "tenant": tenant_val,
        "since_ts": lower_limit,
        "until_ts": upper_limit,
        "limit_dialogs": limit_dialogs,
        "filtered_groups": 0,
        "candidate_chats": candidate_total,
        "dialog_count": 0,
        "messages_in_range": 0,
        "messages_exported": 0,
    }

    if not candidate_rows:
        params_groups = [tenant_val]
        group_conditions = [
            "COALESCE(l.tenant_id, 0) = $1",
            "l.channel IN ('whatsapp', 'wa')",
        ]
        if lower_limit is not None:
            params_groups.append(float(lower_limit))
            group_conditions.append(f"m.created_at >= to_timestamp(${len(params_groups)})")
        if upper_limit is not None:
            params_groups.append(float(upper_limit))
            group_conditions.append(f"m.created_at <= to_timestamp(${len(params_groups)})")

        count_row = await _fetchrow(
            f"""
            SELECT COUNT(*) AS msg_count
            FROM messages m
            JOIN leads l ON l.id = m.lead_id
            WHERE {' AND '.join(group_conditions)}
            """,
            *params_groups,
        )
        if count_row and "msg_count" in count_row:
            try:
                meta["messages_in_range"] = int(count_row["msg_count"] or 0)
            except (TypeError, ValueError):
                meta["messages_in_range"] = 0
        meta.setdefault("distinct_chat_ids", [])
        meta.setdefault("top_chats", [])

        async def _empty_message_batches() -> AsyncIterator[List[Dict[str, Any]]]:
            if False:  # pragma: no cover - type guard
                yield []

        async def _empty_generator() -> (
            AsyncIterator[tuple[Dict[str, Any], AsyncIterator[List[Dict[str, Any]]]]]
        ):
            if False:  # pragma: no cover - type guard
                yield {}, _empty_message_batches()

        _log.info(
            "[db] wa_export no_candidates tenant=%s since_ts=%s until_ts=%s messages_in_range=%s",
            tenant_val,
            lower_limit,
            upper_limit,
            meta.get("messages_in_range"),
        )
        return _empty_generator(), meta

    summaries: List[Dict[str, Any]] = []
    for row in candidate_rows:
        lead_id_raw = row.get("lead_id")
        try:
            lead_id = int(lead_id_raw)
        except (TypeError, ValueError):
            continue
        contact_id_raw = row.get("contact_id")
        try:
            contact_id = int(contact_id_raw) if contact_id_raw is not None else None
        except (TypeError, ValueError):
            contact_id = None
        whatsapp_phone = row.get("whatsapp_phone")
        jid = _normalize_whatsapp_jid(whatsapp_phone, bool(row.get("is_group")))
        if not jid and contact_id is not None:
            chat_id = f"contact:{contact_id}"
        elif jid:
            chat_id = jid
        else:
            chat_id = f"chat:{lead_id}"
        last_created = row.get("last_created_at")
        if isinstance(last_created, datetime):
            last_ts = (
                last_created.replace(tzinfo=last_created.tzinfo or timezone.utc)
                .astimezone(timezone.utc)
                .timestamp()
            )
        else:
            try:
                last_ts = float(last_created) if last_created is not None else 0.0
            except (TypeError, ValueError):
                last_ts = 0.0
        summaries.append(
            {
                "lead_id": lead_id,
                "contact_id": contact_id,
                "whatsapp_phone": jid,
                "title": (row.get("title") or "").strip(),
                "chat_id": chat_id,
                "last_ts": last_ts,
            }
        )

    summaries.sort(key=lambda item: item.get("last_ts") or 0.0, reverse=True)

    lead_ids = [summary["lead_id"] for summary in summaries]
    params_messages: List[Any] = [tenant_val, lead_ids]
    where_parts = [
        "m.lead_id = ANY($2::BIGINT[])",
        "COALESCE(l.tenant_id, 0) = $1",
        "l.channel IN ('whatsapp', 'wa')",
    ]
    if lower_limit is not None:
        params_messages.append(float(lower_limit))
        where_parts.append(f"m.created_at >= to_timestamp(${len(params_messages)})")
    if upper_limit is not None:
        params_messages.append(float(upper_limit))
        where_parts.append(f"m.created_at <= to_timestamp(${len(params_messages)})")

    count_sql = f"""
        SELECT m.lead_id, COUNT(*) AS msg_count
        FROM messages m
        JOIN leads l ON l.id = m.lead_id
        WHERE {' AND '.join(where_parts)}
        GROUP BY m.lead_id
    """

    count_rows = await _fetch(count_sql, *params_messages)
    message_counts: Dict[int, int] = {}
    for row in count_rows:
        try:
            lead_id = int(row.get("lead_id"))
        except (TypeError, ValueError):
            continue
        try:
            message_counts[lead_id] = int(row.get("msg_count") or 0)
        except (TypeError, ValueError):
            message_counts[lead_id] = 0

    selected_dialogs: List[Dict[str, Any]] = []
    total_messages = 0
    total_exported = 0
    for summary in summaries:
        lead_id = summary["lead_id"]
        count = message_counts.get(lead_id, 0)
        if count <= 0:
            continue
        total_messages += count
        limit_for_lead = count
        if per_limit_int is not None and per_limit_int > 0:
            limit_for_lead = min(count, per_limit_int)
        total_exported += limit_for_lead
        selected_dialogs.append(
            {**summary, "message_limit": limit_for_lead, "message_total": count}
        )

    distinct_chat_ids = [dialog["chat_id"] for dialog in selected_dialogs]
    meta.update(
        {
            "dialog_count": len(selected_dialogs),
            "messages_in_range": total_messages,
            "messages_exported": total_exported,
            "distinct_chat_ids": distinct_chat_ids,
            "top_chats": [
                {"chat_id": dialog.get("chat_id"), "last_ts": dialog.get("last_ts")}
                for dialog in selected_dialogs[:5]
            ],
        }
    )

    _log.info(
        "[db] wa_export summary tenant=%s distinct=%s filtered_groups=%s top5=%s",
        tenant_val,
        len(distinct_chat_ids),
        meta.get("filtered_groups", 0),
        meta.get("top_chats", [])[:5],
    )

    async def _message_batches(
        lead_id: int,
        max_messages: Optional[int],
        skip_messages: int = 0,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        remaining = max_messages if (max_messages is not None and max_messages > 0) else None
        skip_remaining = max(skip_messages, 0)
        last_created_at: Optional[datetime] = None
        last_message_id: Optional[int] = None
        while True:
            if remaining is not None and remaining <= 0:
                break
            limit_current = message_batch if remaining is None else min(message_batch, remaining)
            if limit_current <= 0:
                break

            params = [tenant_val, lead_id]
            conditions = [
                "COALESCE(l.tenant_id, 0) = $1",
                "m.lead_id = $2",
                "l.channel IN ('whatsapp', 'wa')",
            ]
            if lower_limit is not None:
                params.append(float(lower_limit))
                conditions.append(f"m.created_at >= to_timestamp(${len(params)})")
            if upper_limit is not None:
                params.append(float(upper_limit))
                conditions.append(f"m.created_at <= to_timestamp(${len(params)})")
            if last_created_at is not None and last_message_id is not None:
                params.append(last_created_at)
                idx_created = len(params)
                params.append(int(last_message_id))
                idx_id = len(params)
                conditions.append(
                    f"(m.created_at > ${idx_created} OR (m.created_at = ${idx_created} AND m.id > ${idx_id}))"
                )

            limit_idx = len(params) + 1
            params.append(int(limit_current))

            sql = f"""
                SELECT
                    m.id AS message_id,
                    m.direction,
                    m.text,
                    m.created_at,
                    extract(epoch FROM m.created_at) AS ts
                FROM messages m
                JOIN leads l ON l.id = m.lead_id
                WHERE {' AND '.join(conditions)}
                ORDER BY m.created_at ASC, m.id ASC
                LIMIT ${limit_idx}
            """

            rows = await _fetch(sql, *params)
            if not rows:
                break

            batch_messages: List[Dict[str, Any]] = []
            for row in rows:
                if skip_remaining > 0:
                    skip_remaining -= 1
                else:
                    ts_raw = row.get("ts")
                    try:
                        ts_val = float(ts_raw) if ts_raw is not None else 0.0
                    except (TypeError, ValueError):
                        ts_val = 0.0
                    direction_raw = row.get("direction")
                    try:
                        direction_val = int(direction_raw if direction_raw is not None else 0)
                    except (TypeError, ValueError):
                        direction_val = 0
                    text = (row.get("text") or "").strip()
                    batch_messages.append(
                        {
                            "ts": ts_val,
                            "direction": direction_val,
                            "text": text,
                        }
                    )
                    if remaining is not None:
                        remaining -= 1
                created_at = row.get("created_at")
                if isinstance(created_at, datetime):
                    last_created_at = created_at
                message_id_raw = row.get("message_id")
                try:
                    last_message_id = (
                        int(message_id_raw) if message_id_raw is not None else last_message_id
                    )
                except (TypeError, ValueError):
                    last_message_id = last_message_id

            if batch_messages:
                yield batch_messages

            if remaining is not None and remaining <= 0:
                break

            if len(rows) < limit_current and skip_remaining <= 0:
                break

    async def _dialog_generator() -> (
        AsyncIterator[tuple[Dict[str, Any], AsyncIterator[List[Dict[str, Any]]]]]
    ):
        for dialog in selected_dialogs:
            max_messages = dialog.get("message_limit")
            skip_messages = max(dialog.get("message_total", 0) - (max_messages or 0), 0)
            yield dialog, _message_batches(dialog["lead_id"], max_messages, skip_messages)

    return _dialog_generator(), meta


async def _load_whatsapp_dialogs(
    tenant_val: int,
    since_ts: Optional[float],
    until_ts: Optional[float],
    limit_dialogs: Optional[int],
    channel: str = "whatsapp",
    per_message_limit: Optional[int] = None,
    allow_offline: bool = True,
    batch_size_dialogs: int = 200,
    message_batch_size: int = 1000,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Compatibility wrapper that materializes WhatsApp dialogs into memory."""

    dialog_iter, meta = await stream_whatsapp_dialogs(
        tenant_val=tenant_val,
        since_ts=since_ts,
        until_ts=until_ts,
        limit_dialogs=limit_dialogs,
        channel=channel,
        per_message_limit=per_message_limit,
        batch_size_dialogs=batch_size_dialogs,
        message_batch_size=message_batch_size,
    )

    dialogs: List[Dict[str, Any]] = []
    exported_messages = 0

    async for dialog, message_batches in dialog_iter:
        messages: List[Dict[str, Any]] = []
        async for batch in message_batches:
            messages.extend(batch)
        if not messages:
            continue
        exported_messages += len(messages)
        last_ts = messages[-1]["ts"] if messages else dialog.get("last_ts")
        dialogs.append(
            {
                "lead_id": dialog.get("lead_id"),
                "contact_id": dialog.get("contact_id"),
                "whatsapp_phone": dialog.get("whatsapp_phone"),
                "title": dialog.get("title") or "",
                "messages": messages,
                "last_ts": last_ts,
                "chat_id": dialog.get("chat_id"),
            }
        )

    meta = dict(meta)
    meta["dialog_count"] = len(dialogs)
    meta["messages_exported"] = exported_messages
    if "distinct_chat_ids" not in meta:
        meta["distinct_chat_ids"] = [dialog.get("chat_id") for dialog in dialogs]

    return dialogs, meta


async def export_dialogs(
    tenant_id: Optional[int],
    channel: str = "whatsapp",
    exclude_groups: bool = True,
    since_ts: Optional[float] = None,
    max_conversations: int = 100,
    per_conversation_limit: int = 0,
) -> List[Dict[str, Any]]:
    """Collect recent dialogues grouped by lead for export."""

    try:
        tenant_val = int(tenant_id or 0)
    except (TypeError, ValueError):
        tenant_val = 0

    since_cutoff: Optional[float]
    if since_ts is not None:
        try:
            since_cutoff = float(since_ts)
        except (TypeError, ValueError):
            since_cutoff = None
    else:
        since_cutoff = None

    try:
        conv_limit_int = int(max_conversations) if max_conversations is not None else None
    except (TypeError, ValueError):
        conv_limit_int = None
    if conv_limit_int is not None and conv_limit_int <= 0:
        conv_limit_int = None

    try:
        per_limit_int = int(per_conversation_limit) if per_conversation_limit is not None else None
    except (TypeError, ValueError):
        per_limit_int = None
    if per_limit_int is not None and per_limit_int <= 0:
        per_limit_int = None

    until_ts = time.time()

    dialogs_raw, meta = await _load_whatsapp_dialogs(
        tenant_val=tenant_val,
        since_ts=since_cutoff,
        until_ts=until_ts,
        limit_dialogs=conv_limit_int,
        channel=channel,
        per_message_limit=per_limit_int,
        allow_offline=False,
    )

    result: List[Dict[str, Any]] = []
    for dialog in dialogs_raw:
        messages = dialog.get("messages") or []
        formatted: List[Dict[str, Any]] = []
        for message in messages:
            direction = message.get("direction")
            try:
                direction_val = int(direction if direction is not None else 0)
            except (TypeError, ValueError):
                direction_val = 0
            role = "assistant" if direction_val == 1 else "user"
            text = message.get("text") or ""
            formatted.append(
                {
                    "role": role,
                    "content": text,
                    "text": text,
                    "ts": message.get("ts"),
                    "direction": direction_val,
                }
            )
        if not formatted:
            continue
        result.append(
            {
                "lead_id": dialog.get("lead_id"),
                "contact_id": dialog.get("contact_id"),
                "whatsapp_phone": dialog.get("whatsapp_phone"),
                "title": dialog.get("title") or "",
                "messages": formatted,
                "last_message_ts": formatted[-1]["ts"],
            }
        )

    _log.info(
        "[db] export_dialogs tenant=%s channel=%s convos=%s messages=%s distinct=%s filtered_groups=%s",
        tenant_val,
        channel,
        len(result),
        sum(len(d.get("messages") or []) for d in result),
        len(meta.get("distinct_chat_ids", [])) if isinstance(meta, dict) else 0,
        meta.get("filtered_groups") if isinstance(meta, dict) else 0,
    )
    return result


async def fetch_whatsapp_dialogs(
    tenant_id: int,
    since: datetime,
    until: datetime,
    limit_dialogs: Optional[int] = None,
    per_message_limit: Optional[int] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Load WhatsApp conversations with metadata for the WhatsApp export pipeline."""

    try:
        tenant_val = int(tenant_id or 0)
    except (TypeError, ValueError):
        tenant_val = 0

    def _to_epoch(value: datetime) -> float:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).timestamp()

    since_ts = _to_epoch(since)
    until_ts = _to_epoch(until)

    dialogs, meta = await _load_whatsapp_dialogs(
        tenant_val=tenant_val,
        since_ts=since_ts,
        until_ts=until_ts,
        limit_dialogs=limit_dialogs,
        channel="whatsapp",
        per_message_limit=per_message_limit,
        allow_offline=_is_testing_env(),
    )

    if isinstance(meta, dict):
        meta.setdefault("since_ts", since_ts)
        meta.setdefault("until_ts", until_ts)
        if limit_dialogs is not None:
            meta["limit_dialogs"] = limit_dialogs
        meta.setdefault("per_message_limit", per_message_limit if per_message_limit else None)

    return dialogs, meta


# -------- Training export: thread fetch (no joins) --------


async def fetch_threads(
    tenant: int,
    provider: Optional[str] = None,
    since_ts: Optional[float] = None,
    limit: int = 2000,
) -> List[Dict[str, Any]]:
    """Fetch recent messages flat and group by lead_id in memory.

    Notes:
    - Our schema stores message time in TIMESTAMPTZ column `created_at`.
      We treat `since_ts` as seconds since epoch and rely on to_timestamp().
    - The `provider` filter is ignored here because `messages` has no provider column.
      Left as a placeholder for future schema changes.
    - No JOINs are used; contact_id is unknown here and left as None.
    """
    # Log time units used
    units = "s"
    _log.info("[db] units=%s table=messages col=created_at", units)

    pool = await _ensure_pool()
    tenant_int = int(tenant)
    if not pool:
        if _offline_enabled():
            return _offline_fetch_threads(since_ts, limit, tenant_id=tenant_int)
        raise DatabaseUnavailableError("postgres_pool_unavailable")

    params: list[Any] = [tenant_int]  # type: ignore[name-defined]
    where = ["COALESCE(m.tenant_id, 0) = $1"]
    if since_ts is not None:
        where.append(f"m.created_at >= to_timestamp(${len(params) + 1})")
        params.append(float(since_ts))
    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = f"WHERE {where_sql}"
    sql = f"""
        SELECT m.lead_id, m.id, m.direction, m.text, extract(epoch from m.created_at) AS ts,
               COALESCE(m.tenant_id, 0) AS tenant_id,
               lc.contact_id,
               l.tenant_id AS lead_tenant
        FROM messages m
        LEFT JOIN leads l ON l.id = m.lead_id
        LEFT JOIN lead_contacts lc ON lc.lead_id = m.lead_id
        {where_sql}
        {"AND" if where_sql else "WHERE"} COALESCE(l.tenant_id, 0) = $1
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT ${len(params) + 1}
    """
    params.append(int(limit))
    rows = await _fetch(sql, *params)
    # Group by lead_id and preserve chronological order (ascending)
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        lid = int(r["lead_id"])
        msg_tenant = int(r.get("tenant_id") or 0)
        lead_tenant = int(r.get("lead_tenant") or 0)
        if msg_tenant not in (0, tenant_int) and lead_tenant not in (0, tenant_int):
            continue
        msgs = grouped.setdefault(lid, [])
        msgs.append(
            {
                "lead_id": lid,
                "direction": int(r["direction"]) if r.get("direction") is not None else 0,
                "text": r.get("text") or "",
                "ts": float(r.get("ts") or 0.0),
                "contact_id": r.get("contact_id"),
            }
        )
    out: List[Dict[str, Any]] = []
    for lid, msgs in grouped.items():
        # reverse to chronological
        msgs_sorted = list(reversed(msgs))
        contact_id = None
        for m in msgs_sorted:
            if m.get("contact_id") is not None:
                contact_id = m.get("contact_id")
                break
        sanitized = []
        for m in msgs_sorted:
            sanitized.append({k: v for k, v in m.items() if k != "contact_id"})
        out.append(
            {
                "lead_id": lid,
                "contact_id": contact_id,
                "messages": sanitized,
            }
        )
    return out


# -------- Webhook log --------


async def insert_webhook_event(
    provider: str, event_type: str, lead_id: Optional[int], payload: dict
):
    # Пытаемся писать в БД. Если пула нет — пишем в файл, чтобы вебхук не падал.
    pool = await _ensure_pool()
    if not pool:
        try:
            os.makedirs("/app/data", exist_ok=True)
            with open("/app/data/webhooks.log", "a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "provider": provider,
                            "event_type": event_type,
                            "lead_id": lead_id,
                            "payload": payload,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        except Exception:
            pass
        return

    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO webhook_events(provider, event_type, lead_id, payload)
            VALUES($1, $2, $3, $4::jsonb);
        """,
            provider,
            event_type,
            lead_id,
            json.dumps(payload, ensure_ascii=False),
        )


async def update_contact_phone(contact_id: int, phone: str | None) -> None:
    """Update contact phone if provided."""
    if not contact_id or not phone:
        return
    from libs.core.transport import (
        normalize_e164_digits,
    )  # local import to avoid circular dependency

    raw_value = phone.decode() if isinstance(phone, (bytes, bytearray)) else str(phone)
    digits = None
    try:
        digits = normalize_e164_digits(raw_value)
    except Exception as exc:
        # Fallback: strip all non-digits to avoid silent drops.
        stripped = re.sub(r"\D", "", raw_value)
        if not stripped:
            _log.warning(
                "contact_phone_normalize_failed contact_id=%s phone=%s error=%s",
                contact_id,
                phone,
                exc,
            )
            return
        digits = stripped
        _log.debug(
            "contact_phone_normalize_fallback contact_id=%s phone=%s digits=%s error=%s",
            contact_id,
            phone,
            digits,
            exc,
        )
    await _exec(
        "UPDATE contacts SET phone = $1, whatsapp_phone = COALESCE(whatsapp_phone, $1), updated_at = now() WHERE id = $2",
        digits,
        contact_id,
    )


async def update_contact_telegram(
    contact_id: int, telegram_user_id: int | None, telegram_username: str | None
) -> None:
    """Update contact with Telegram identifiers if missing."""
    if not contact_id:
        return
    await _exec(
        """
        UPDATE contacts
        SET telegram_user_id = COALESCE(telegram_user_id, $2::bigint),
            telegram_username = COALESCE(NULLIF($3, ''), telegram_username),
            updated_at = now()
        WHERE id = $1;
        """,
        contact_id,
        telegram_user_id,
        telegram_username,
    )


async def update_contact_avito_login(contact_id: int, avito_login: str | None) -> None:
    """Update contact with Avito login if provided."""
    if not contact_id:
        return
    login = (avito_login or "").strip()
    if not login:
        return
    await _exec(
        """
        UPDATE contacts
        SET avito_login = COALESCE(NULLIF($2, ''), avito_login),
            updated_at = now()
        WHERE id = $1;
        """,
        contact_id,
        login,
    )


async def update_contact_max(
    contact_id: int, max_user_id: int | None, max_username: str | None
) -> None:
    """Update contact with MAX identifiers if missing."""
    if not contact_id:
        return
    await _exec(
        """
        UPDATE contacts
        SET max_user_id = COALESCE(max_user_id, $2::bigint),
            max_username = COALESCE(NULLIF($3, ''), max_username),
            updated_at = now()
        WHERE id = $1;
        """,
        contact_id,
        max_user_id,
        max_username,
    )


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False)


def _jsonb_object(value: Any) -> str:
    if isinstance(value, Mapping):
        payload: Any = dict(value)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            payload = {}
        else:
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {}
            payload = dict(parsed) if isinstance(parsed, Mapping) else {}
    else:
        payload = {}
    return _jsonb(payload)


async def get_recent_lead_messages(
    tenant_id: int,
    lead_id: int,
    *,
    limit: int = 40,
) -> list[dict[str, Any]]:
    try:
        tenant_val = int(tenant_id)
        lead_val = int(lead_id)
    except Exception:
        return []
    if tenant_val <= 0 or lead_val <= 0:
        return []
    limit_val = limit if isinstance(limit, int) and limit > 0 else 40
    if _offline_enabled():
        return []
    rows = await _fetch(
        """
        SELECT id, lead_id, direction, is_bot, source, text, status, created_at
        FROM messages
        WHERE tenant_id = $1
          AND lead_id = $2
          AND text IS NOT NULL
          AND btrim(text) <> ''
        ORDER BY created_at ASC, id ASC
        LIMIT $3
        """,
        tenant_val,
        lead_val,
        limit_val,
    )
    out: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            out.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                out.append(dict(row.items()))
    return out


async def create_dialogue_state_snapshot(snapshot: Mapping[str, Any]) -> int:
    if not isinstance(snapshot, Mapping):
        return 0
    if _offline_enabled():
        return 0
    try:
        tenant_id = int(snapshot.get("tenant_id") or 0)
        lead_id = int(snapshot.get("lead_id") or 0)
        contact_id = int(snapshot.get("contact_id") or 0)
    except Exception:
        return 0
    row = await _fetchrow(
        """
        INSERT INTO dialogue_state_snapshots(
            tenant_id, lead_id, contact_id, channel, feature_version, fingerprint, snapshot_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        RETURNING id
        """,
        tenant_id,
        lead_id,
        contact_id,
        str(snapshot.get("channel") or ""),
        str(snapshot.get("feature_version") or "learning_v2:1"),
        str(snapshot.get("fingerprint") or ""),
        _jsonb(dict(snapshot)),
    )
    try:
        return int(row["id"]) if row else 0
    except Exception:
        return 0


async def create_intervention_episode(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    source_event: str,
    trigger_user_text: str,
    pre_bot_snapshot_id: int | None,
    pre_manager_snapshot_id: int | None,
    bot_message_id: int | None,
    manager_message_id: int | None,
    bot_reply_text: str,
    manager_reply_text: str,
    bot_action: Mapping[str, Any],
    manager_action: Mapping[str, Any],
    stitched_dialogue: Sequence[Mapping[str, Any]],
    policy_key: str,
) -> int:
    if _offline_enabled():
        return 0
    row = await _fetchrow(
        """
        INSERT INTO intervention_episodes(
            tenant_id, lead_id, channel, source_event, trigger_user_text,
            pre_bot_snapshot_id, pre_manager_snapshot_id, bot_message_id, manager_message_id,
            bot_reply_text, manager_reply_text, bot_action, manager_action, stitched_dialogue,
            policy_key
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb, $14::jsonb, $15)
        RETURNING id
        """,
        int(tenant_id),
        int(lead_id),
        str(channel or ""),
        str(source_event or "manager_outgoing"),
        str(trigger_user_text or ""),
        int(pre_bot_snapshot_id) if pre_bot_snapshot_id else None,
        int(pre_manager_snapshot_id) if pre_manager_snapshot_id else None,
        int(bot_message_id) if bot_message_id else None,
        int(manager_message_id) if manager_message_id else None,
        str(bot_reply_text or ""),
        str(manager_reply_text or ""),
        _jsonb(dict(bot_action or {})),
        _jsonb(dict(manager_action or {})),
        _jsonb(list(stitched_dialogue or [])),
        str(policy_key or "") or None,
    )
    try:
        return int(row["id"]) if row else 0
    except Exception:
        return 0


async def insert_episode_labels(episode_id: int, *, labels: Sequence[Mapping[str, Any]]) -> None:
    if _offline_enabled() or not labels:
        return
    try:
        episode_ref = int(episode_id)
    except Exception:
        return
    if episode_ref <= 0:
        return
    episode_row = await _fetchrow(
        "SELECT tenant_id FROM intervention_episodes WHERE id = $1 LIMIT 1",
        episode_ref,
    )
    if not episode_row:
        return
    tenant_id = int(episode_row.get("tenant_id") if hasattr(episode_row, "get") else episode_row[0])
    for item in labels:
        await _exec(
            """
            INSERT INTO episode_labels(episode_id, tenant_id, label_type, label_key, label_value, confidence)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6)
            """,
            episode_ref,
            tenant_id,
            str(item.get("label_type") or "generic"),
            str(item.get("label_key") or "label"),
            _jsonb(item.get("label_value") or {}),
            float(item.get("confidence") or 0.0),
        )


async def list_open_intervention_episodes(
    tenant_id: int,
    lead_id: int,
    *,
    limit: int = 20,
    older_than_minutes: int = 180,
) -> list[dict[str, Any]]:
    if _offline_enabled():
        return []
    lookback_minutes = max(max(older_than_minutes, 5) * 4, 24 * 60)
    rows = await _fetch(
        """
        SELECT id, tenant_id, lead_id, manager_message_id, trigger_user_text, manager_reply_text, created_at
        FROM intervention_episodes
        WHERE tenant_id = $1
          AND lead_id = $2
          AND status IN ('captured', 'pending')
          AND created_at >= now() - ($3::int || ' minutes')::interval
        ORDER BY created_at ASC, id ASC
        LIMIT $4
        """,
        int(tenant_id),
        int(lead_id),
        lookback_minutes,
        max(1, int(limit)),
    )
    out: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            out.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                out.append(dict(row.items()))
    return out


async def finalize_intervention_episode(
    episode_id: int,
    outcome_payload: Mapping[str, Any],
    *,
    reward: float,
    status: str = "finalized",
) -> None:
    if _offline_enabled():
        return
    try:
        episode_ref = int(episode_id)
    except Exception:
        return
    if episode_ref <= 0:
        return
    await _exec(
        """
        UPDATE intervention_episodes
        SET outcome_payload = $2::jsonb,
            reward = $3,
            status = $4,
            updated_at = now()
        WHERE id = $1
        """,
        episode_ref,
        _jsonb(dict(outcome_payload or {})),
        float(reward),
        str(status or "finalized"),
    )


async def upsert_policy_candidate_from_episode(
    episode_id: int,
    *,
    reward: float,
    signals: Mapping[str, Any],
) -> dict[str, Any] | None:
    if _offline_enabled():
        return None
    try:
        episode_ref = int(episode_id)
    except Exception:
        return None
    row = await _fetchrow(
        """
        SELECT id, tenant_id, lead_id, policy_key, pre_manager_snapshot_id, bot_action, manager_action
        FROM intervention_episodes
        WHERE id = $1
        LIMIT 1
        """,
        episode_ref,
    )
    if not row:
        return None
    tenant_id = int(row.get("tenant_id") if hasattr(row, "get") else row[1])
    lead_id = int(row.get("lead_id") if hasattr(row, "get") else row[2])
    policy_key = str(row.get("policy_key") if hasattr(row, "get") else row[3] or "").strip()
    if not policy_key:
        return None
    snapshot_id = int(row.get("pre_manager_snapshot_id") if hasattr(row, "get") else row[4] or 0)
    snapshot = await _fetchrow(
        "SELECT snapshot_json FROM dialogue_state_snapshots WHERE id = $1 LIMIT 1",
        snapshot_id,
    )
    if not snapshot:
        return None
    snapshot_json = snapshot.get("snapshot_json") if hasattr(snapshot, "get") else snapshot[0]
    if isinstance(snapshot_json, str):
        try:
            snapshot_payload = json.loads(snapshot_json)
        except Exception:
            snapshot_payload = {}
    else:
        snapshot_payload = dict(snapshot_json or {}) if isinstance(snapshot_json, Mapping) else {}
    manager_action_map = row.get("manager_action") if hasattr(row, "get") else row[6]
    bot_action_map = row.get("bot_action") if hasattr(row, "get") else row[5]
    manager_action = dict(manager_action_map or {}) if isinstance(manager_action_map, Mapping) else {}
    bot_action = dict(bot_action_map or {}) if isinstance(bot_action_map, Mapping) else {}
    recommended_action = str(manager_action.get("action") or "answer_direct")
    avoid_action = str(bot_action.get("action") or "").strip() or None
    fingerprint_payload = snapshot_payload.get("fingerprint_payload") if isinstance(snapshot_payload, Mapping) else {}
    if not isinstance(fingerprint_payload, Mapping):
        fingerprint_payload = {}
    style_hints = manager_action.get("style_hints") if isinstance(manager_action, Mapping) else {}
    if not isinstance(style_hints, Mapping):
        style_hints = {}
    existing = await _fetchrow(
        "SELECT id, evidence_count, distinct_leads_count, reward_delta, negative_evidence FROM policy_candidates WHERE tenant_id = $1 AND policy_key = $2 LIMIT 1",
        tenant_id,
        policy_key,
    )
    candidate_id = 0
    if not existing:
        created = await _fetchrow(
            """
            INSERT INTO policy_candidates(
                tenant_id, policy_key, fingerprint_payload, recommended_action, avoid_action,
                discouraged_actions, style_hints, evidence_count, distinct_leads_count,
                reward_delta, confidence, freshness, negative_evidence, active, last_episode_id
            )
            VALUES ($1, $2, $3::jsonb, $4, $5, $6::jsonb, $7::jsonb, 0, 0, 0, 0, 0, 0, FALSE, $8)
            RETURNING id
            """,
            tenant_id,
            policy_key,
            _jsonb(dict(fingerprint_payload)),
            recommended_action,
            avoid_action,
            _jsonb([avoid_action] if avoid_action else []),
            _jsonb(dict(style_hints)),
            episode_ref,
        )
        if not created:
            return None
        candidate_id = int(created["id"])
    else:
        candidate_id = int(existing["id"])

    await _exec(
        """
        INSERT INTO policy_candidate_evidence(candidate_id, episode_id, tenant_id, lead_id, reward, positive)
        VALUES($1, $2, $3, $4, $5, $6)
        ON CONFLICT (candidate_id, episode_id) DO NOTHING
        """,
        candidate_id,
        episode_ref,
        tenant_id,
        lead_id,
        float(reward),
        bool(reward > 0),
    )
    stats = await _fetchrow(
        """
        SELECT
            COUNT(*)::int AS evidence_count,
            COUNT(DISTINCT lead_id)::int AS distinct_leads_count,
            COALESCE(AVG(reward), 0)::double precision AS reward_delta,
            COUNT(*) FILTER (WHERE reward <= 0)::int AS negative_evidence
        FROM policy_candidate_evidence
        WHERE candidate_id = $1
        """,
        candidate_id,
    )
    evidence_count = int(stats.get("evidence_count") if hasattr(stats, "get") else stats[0] or 0)
    distinct_leads_count = int(stats.get("distinct_leads_count") if hasattr(stats, "get") else stats[1] or 0)
    reward_delta = float(stats.get("reward_delta") if hasattr(stats, "get") else stats[2] or 0.0)
    negative_evidence = int(stats.get("negative_evidence") if hasattr(stats, "get") else stats[3] or 0)
    confidence = max(0.0, min(1.0, 0.45 + min(evidence_count, 8) * 0.06 + max(reward_delta, 0.0) * 0.25 - min(negative_evidence, 5) * 0.08))
    freshness = max(0.0, min(1.0, 0.4 + min(evidence_count, 10) * 0.04))
    await _exec(
        """
        UPDATE policy_candidates
        SET fingerprint_payload = $2::jsonb,
            recommended_action = $3,
            avoid_action = $4,
            discouraged_actions = $5::jsonb,
            style_hints = $6::jsonb,
            evidence_count = $7,
            distinct_leads_count = $8,
            reward_delta = $9,
            confidence = $10,
            freshness = $11,
            negative_evidence = $12,
            last_episode_id = $13,
            updated_at = now()
        WHERE id = $1
        """,
        candidate_id,
        _jsonb(dict(fingerprint_payload)),
        recommended_action,
        avoid_action,
        _jsonb([avoid_action] if avoid_action else []),
        _jsonb(dict(style_hints)),
        evidence_count,
        distinct_leads_count,
        reward_delta,
        confidence,
        freshness,
        negative_evidence,
        episode_ref,
    )
    out = await _fetchrow("SELECT * FROM policy_candidates WHERE id = $1 LIMIT 1", candidate_id)
    return dict(out) if out else None


async def promote_or_demote_policy_candidate(
    candidate_id: int,
    *,
    min_evidence: int,
    min_distinct_leads: int,
    min_reward_delta: float,
    max_negative_evidence: int,
) -> None:
    if _offline_enabled():
        return
    try:
        candidate_ref = int(candidate_id)
    except Exception:
        return
    row = await _fetchrow("SELECT * FROM policy_candidates WHERE id = $1 LIMIT 1", candidate_ref)
    if not row:
        return
    candidate = dict(row)
    tenant_id = int(candidate.get("tenant_id") or 0)
    rule_key = str(candidate.get("policy_key") or "").strip()
    should_activate = (
        int(candidate.get("evidence_count") or 0) >= int(min_evidence)
        and int(candidate.get("distinct_leads_count") or 0) >= int(min_distinct_leads)
        and float(candidate.get("reward_delta") or 0.0) >= float(min_reward_delta)
        and int(candidate.get("negative_evidence") or 0) <= int(max_negative_evidence)
    )
    await _exec(
        "UPDATE policy_candidates SET active = $2, updated_at = now() WHERE id = $1",
        candidate_ref,
        bool(should_activate),
    )
    existing = await _fetchrow(
        "SELECT id FROM tenant_policy_rules WHERE tenant_id = $1 AND rule_key = $2 LIMIT 1",
        tenant_id,
        rule_key,
    )
    status = "active" if should_activate else "disabled"
    if existing:
        await _exec(
            """
            UPDATE tenant_policy_rules
            SET candidate_id = $2,
                fingerprint_payload = $3::jsonb,
                recommended_action = $4,
                avoid_action = $5,
                style_hints = $6::jsonb,
                confidence = $7,
                evidence_count = $8,
                status = $9,
                active = $10,
                updated_at = now(),
                promoted_at = CASE WHEN $10 THEN now() ELSE promoted_at END,
                demoted_at = CASE WHEN NOT $10 THEN now() ELSE demoted_at END
            WHERE id = $1
            """,
            int(existing.get("id") or 0),
            candidate_ref,
            _jsonb_object(candidate.get("fingerprint_payload")),
            str(candidate.get("recommended_action") or ""),
            str(candidate.get("avoid_action") or "") or None,
            _jsonb_object(candidate.get("style_hints")),
            float(candidate.get("confidence") or 0.0),
            int(candidate.get("evidence_count") or 0),
            status,
            bool(should_activate),
        )
    else:
        await _exec(
            """
            INSERT INTO tenant_policy_rules(
                tenant_id, candidate_id, rule_key, fingerprint_payload,
                recommended_action, avoid_action, style_hints, confidence,
                evidence_count, status, active, shadow_only, promoted_at, demoted_at
            )
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::jsonb, $8, $9, $10, $11, TRUE,
                    CASE WHEN $11 THEN now() ELSE NULL END,
                    CASE WHEN NOT $11 THEN now() ELSE NULL END)
            ON CONFLICT (tenant_id, rule_key) DO NOTHING
            """,
            tenant_id,
            candidate_ref,
            rule_key,
            _jsonb_object(candidate.get("fingerprint_payload")),
            str(candidate.get("recommended_action") or ""),
            str(candidate.get("avoid_action") or "") or None,
            _jsonb_object(candidate.get("style_hints")),
            float(candidate.get("confidence") or 0.0),
            int(candidate.get("evidence_count") or 0),
            status,
            bool(should_activate),
        )


async def list_tenant_policy_rules(
    tenant_id: int,
    *,
    active_only: bool = True,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if _offline_enabled():
        return []
    sql = [
        "SELECT id, tenant_id, candidate_id, rule_key, fingerprint_payload, recommended_action, avoid_action, style_hints, confidence, evidence_count, status, active, shadow_only, updated_at",
        "FROM tenant_policy_rules",
        "WHERE tenant_id = $1",
    ]
    params: list[Any] = [int(tenant_id)]
    if active_only:
        sql.append("AND active = TRUE")
        sql.append("AND status <> 'disabled'")
    params.append(max(1, int(limit)))
    sql.append(f"ORDER BY confidence DESC, evidence_count DESC, updated_at DESC LIMIT ${len(params)}")
    rows = await _fetch(" ".join(sql), *params)
    out: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            out.append(dict(row))
        except Exception:
            if isinstance(row, Mapping):
                out.append(dict(row.items()))
    return out


async def create_policy_decision(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    snapshot: Mapping[str, Any],
    status: str,
    mode: str,
    reason: str,
    similarity: float,
    confidence: float,
    recommended_action: str,
    avoid_action: str,
    style_hints: Mapping[str, Any],
    rule_id: int | None,
) -> int:
    if _offline_enabled():
        return 0
    snapshot_id = await create_dialogue_state_snapshot(snapshot)
    row = await _fetchrow(
        """
        INSERT INTO policy_decisions(
            tenant_id, lead_id, channel, snapshot_id, rule_id, mode, status, reason,
            similarity, confidence, recommended_action, avoid_action, style_hints
        )
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
        RETURNING id
        """,
        int(tenant_id),
        int(lead_id),
        str(channel or ""),
        int(snapshot_id) if snapshot_id else None,
        int(rule_id) if rule_id else None,
        str(mode or "shadow"),
        str(status or "skipped"),
        str(reason or ""),
        float(similarity),
        float(confidence),
        str(recommended_action or ""),
        str(avoid_action or "") or None,
        _jsonb(dict(style_hints or {})),
    )
    try:
        return int(row["id"]) if row else 0
    except Exception:
        return 0


async def mark_policy_decision_applied(decision_id: int, *, applied: bool) -> None:
    if _offline_enabled():
        return
    try:
        decision_ref = int(decision_id)
    except Exception:
        return
    await _exec(
        "UPDATE policy_decisions SET applied = $2 WHERE id = $1",
        decision_ref,
        bool(applied),
    )


async def get_recent_policy_decision_for_lead(
    tenant_id: int,
    lead_id: int,
    *,
    within_minutes: int = 180,
) -> dict[str, Any] | None:
    if _offline_enabled():
        return None
    row = await _fetchrow(
        """
        SELECT id, recommended_action, status, mode, created_at
        FROM policy_decisions
        WHERE tenant_id = $1
          AND lead_id = $2
          AND created_at >= now() - ($3::int || ' minutes')::interval
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        int(tenant_id),
        int(lead_id),
        max(5, int(within_minutes)),
    )
    if not row:
        return None
    try:
        return dict(row)
    except Exception:
        if isinstance(row, Mapping):
            return dict(row.items())
    return None


async def create_policy_outcome(
    *,
    tenant_id: int,
    lead_id: int,
    episode_id: int,
    decision_id: int,
    reward: float,
    outcome_payload: Mapping[str, Any],
    manager_agreement: bool | None,
    manager_action: str | None,
) -> int:
    if _offline_enabled():
        return 0
    row = await _fetchrow(
        """
        INSERT INTO policy_outcomes(
            tenant_id, lead_id, episode_id, decision_id, reward,
            manager_agreement, manager_action, outcome_payload
        )
        VALUES($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
        RETURNING id
        """,
        int(tenant_id),
        int(lead_id),
        int(episode_id),
        int(decision_id) if decision_id else None,
        float(reward),
        manager_agreement,
        str(manager_action or "") or None,
        _jsonb(dict(outcome_payload or {})),
    )
    try:
        return int(row["id"]) if row else 0
    except Exception:
        return 0


async def get_learning_policy_stats(tenant_id: int, *, days: int = 30) -> dict[str, Any]:
    if _offline_enabled():
        return {}
    row = await _fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE source_event = 'manager_outgoing')::int AS manager_takeovers,
            COUNT(*) FILTER (WHERE (outcome_payload->>'repeated_question_stopped') = 'false')::int AS repeated_question_failures,
            COUNT(*) FILTER (WHERE (outcome_payload->>'no_fallback_spiral') = 'false')::int AS fallback_failures,
            COALESCE(AVG(reward), 0)::double precision AS outcome_lift_estimate
        FROM intervention_episodes
        WHERE tenant_id = $1
          AND created_at >= now() - ($2::int || ' days')::interval
        """,
        int(tenant_id),
        max(1, int(days)),
    )
    decisions = await _fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE mode = 'shadow')::int AS policy_shadow_decisions,
            COUNT(*) FILTER (WHERE applied = TRUE)::int AS policy_apply_decisions,
            COUNT(*) FILTER (WHERE status = 'skipped')::int AS policy_regret_events,
            COUNT(*) FILTER (WHERE active = TRUE)::int AS active_rule_count
        FROM policy_decisions d
        LEFT JOIN tenant_policy_rules r ON r.id = d.rule_id
        WHERE d.tenant_id = $1
          AND d.created_at >= now() - ($2::int || ' days')::interval
        """,
        int(tenant_id),
        max(1, int(days)),
    )
    out: dict[str, Any] = {}
    for source in (row, decisions):
        if not source:
            continue
        try:
            out.update(dict(source))
        except Exception:
            if isinstance(source, Mapping):
                out.update(dict(source.items()))
    return out
