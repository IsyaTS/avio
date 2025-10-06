import os, hashlib, json, time, logging, pathlib, threading, re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    asyncpg = None  # type: ignore[assignment]

# DSN: допускаем вид postgresql+asyncpg:// и нормализуем
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://avio:AvioPg_2025_strong@postgres:5432/avio")
DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

IS_TESTING = os.getenv("TESTING") == "1"


def _is_testing_env() -> bool:
    return os.getenv("TESTING") == "1"

_pool: Any = None
_log = logging.getLogger("db")

try:  # Prefer package-qualified import to reuse core settings
    from . import core as core_module  # type: ignore
except Exception:  # pragma: no cover - fallback for script-style imports
    import core as core_module  # type: ignore

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


async def _ensure_pool() -> Any:
    """Ленивое создание пула. Вернёт None, если БД не настроена или недоступна."""
    global _pool
    if _pool is not None:
        return _pool
    if asyncpg is None or not DATABASE_URL:
        return None
    try:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        return _pool
    except Exception:
        _pool = None
        return None

# Утилиты-обёртки: не валятся, если БД недоступна
async def _exec(sql: str, *args) -> int:
    pool = await _ensure_pool()
    if not pool:
        return 0
    async with pool.acquire() as con:
        return await con.execute(sql, *args)  # type: ignore[return-value]

async def _fetchrow(sql: str, *args):
    pool = await _ensure_pool()
    if not pool:
        return None
    async with pool.acquire() as con:
        return await con.fetchrow(sql, *args)

async def _fetch(sql: str, *args):
    pool = await _ensure_pool()
    if not pool:
        return []
    async with pool.acquire() as con:
        return await con.fetch(sql, *args)


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


def _offline_append_message(lead_id: int, text: str, direction: int, tenant_id: Optional[int] = None) -> None:
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


def _offline_fetch_threads(since_ts: Optional[float], limit: int, tenant_id: Optional[int]) -> List[Dict[str, Any]]:
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

async def upsert_lead(
    lead_id: int,
    channel: str = "avito",
    source_real_id: Optional[int] = None,
    tenant_id: Optional[int] = None,
):
    try:
        tenant_val = int(tenant_id) if tenant_id is not None else 0
    except Exception:
        tenant_val = 0
    await _exec(
        """
        INSERT INTO leads(lead_id, channel, source_real_id, tenant_id)
        VALUES($1, $2, $3, $4)
        ON CONFLICT (lead_id)
        DO UPDATE SET channel = EXCLUDED.channel,
                      source_real_id = COALESCE(EXCLUDED.source_real_id, leads.source_real_id),
                      tenant_id = CASE
                          WHEN EXCLUDED.tenant_id IS NULL OR EXCLUDED.tenant_id = 0 THEN leads.tenant_id
                          WHEN leads.tenant_id IS NULL OR leads.tenant_id = 0 THEN EXCLUDED.tenant_id
                          ELSE leads.tenant_id
                      END,
                      updated_at = now();
    """,
        lead_id,
        channel,
        source_real_id,
        tenant_val,
    )

async def upsert_source_cache(lead_id: int, real_id: int):
    await _exec("""
        INSERT INTO source_cache(lead_id, real_id)
        VALUES($1, $2)
        ON CONFLICT (lead_id)
        DO UPDATE SET real_id = EXCLUDED.real_id,
                      updated_at = now();
    """, lead_id, real_id)

# -------- Contacts / linking --------

async def resolve_or_create_contact(
    whatsapp_phone: Optional[str] = None,
    avito_user_id: Optional[int] = None,
    avito_login: Optional[str] = None,
) -> int:
    # поиск по приоритету: whatsapp_phone -> avito_user_id -> avito_login
    if whatsapp_phone:
        row = await _fetchrow("SELECT id FROM contacts WHERE whatsapp_phone=$1", whatsapp_phone)
        if row: return row["id"]
    if avito_user_id:
        row = await _fetchrow("SELECT id FROM contacts WHERE avito_user_id=$1", avito_user_id)
        if row: return row["id"]
    if avito_login:
        row = await _fetchrow("SELECT id FROM contacts WHERE avito_login=$1 LIMIT 1", avito_login)
        if row: return row["id"]

    row = await _fetchrow("""
        INSERT INTO contacts(whatsapp_phone, avito_user_id, avito_login)
        VALUES($1,$2,$3)
        RETURNING id
    """, whatsapp_phone, avito_user_id, avito_login)
    # если БД недоступна — вернём фиктивный id, чтобы не падал вызов
    return int(row["id"]) if row and "id" in row else 0

async def link_lead_contact(lead_id: int, contact_id: int):
    await _exec("""
        INSERT INTO lead_contacts(lead_id, contact_id)
        VALUES($1, $2)
        ON CONFLICT (lead_id) DO UPDATE SET contact_id=EXCLUDED.contact_id, linked_at=now();
    """, lead_id, contact_id)

async def get_contact_id_by_lead(lead_id: int) -> Optional[int]:
    row = await _fetchrow("SELECT contact_id FROM lead_contacts WHERE lead_id=$1", lead_id)
    return row["contact_id"] if row else None

# -------- Outbox --------

async def ensure_outbox_queued(lead_id: int, text: str) -> str:
    d = sha1(text)
    await _exec("""
        INSERT INTO outbox(lead_id, text, dedup_hash, status)
        VALUES($1, $2, $3, 'queued')
        ON CONFLICT (lead_id, dedup_hash) DO NOTHING;
    """, lead_id, text, d)
    return d

async def bump_attempt(lead_id: int, d: str, error: Optional[str] = None):
    await _exec("""
        UPDATE outbox
        SET attempts = attempts + 1,
            last_error = left($3, 2000),
            status = 'retry',
            updated_at = now()
        WHERE lead_id = $1 AND dedup_hash = $2;
    """, lead_id, d, error or "")

async def mark_sent(lead_id: int, d: str):
    await _exec("""
        UPDATE outbox
        SET status = 'sent',
            sent_at = now(),
            updated_at = now(),
            last_error = NULL
        WHERE lead_id = $1 AND dedup_hash = $2;
    """, lead_id, d)

async def mark_failed(lead_id: int, d: str, error: str):
    await _exec("""
        UPDATE outbox
        SET status = 'failed',
            last_error = left($3, 2000),
            updated_at = now()
        WHERE lead_id = $1 AND dedup_hash = $2;
    """, lead_id, d, error)

# -------- Messages --------

async def insert_message_in(lead_id: int, text: str, status: str = "received", tenant_id: Optional[int] = None):
    if _offline_enabled():
        _offline_append_message(lead_id, text, direction=0, tenant_id=tenant_id)
        return
    tenant_val = int(tenant_id or 0)
    await _exec(
        """
        INSERT INTO messages(lead_id, direction, text, provider_msg_id, status, tenant_id)
        VALUES($1, 0, $2, NULL, $3, $4);
    """,
        lead_id,
        text,
        status,
        tenant_val,
    )


async def insert_message_out(
    lead_id: int,
    text: str,
    provider_msg_id: Optional[str],
    status: str = "sent",
    tenant_id: Optional[int] = None,
):
    if _offline_enabled():
        _offline_append_message(lead_id, text, direction=1, tenant_id=tenant_id)
        return
    tenant_val = int(tenant_id or 0)
    await _exec(
        """
        INSERT INTO messages(lead_id, direction, text, provider_msg_id, status, tenant_id)
        VALUES($1, 1, $2, $3, $4, $5);
    """,
        lead_id,
        text,
        provider_msg_id,
        status,
        tenant_val,
    )

async def get_recent_dialog_by_contact(contact_id: int, limit: int = 40) -> List[Dict[str, Any]]:
    rows = await _fetch("""
        SELECT m.direction, m.text, m.created_at
        FROM messages m
        JOIN lead_contacts lc ON lc.lead_id = m.lead_id
        WHERE lc.contact_id = $1
        ORDER BY m.id DESC
        LIMIT $2
    """, contact_id, limit)
    data = list(reversed([dict(r) for r in rows]))
    return data

async def _load_whatsapp_dialogs(
    tenant_val: int,
    since_ts: Optional[float],
    until_ts: Optional[float],
    limit_dialogs: Optional[int],
    channel: str = "whatsapp",
    per_message_limit: Optional[int] = None,
    allow_offline: bool = True,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch WhatsApp conversations with normalization and diagnostics."""

    now_ts = time.time()
    lower_limit = float(since_ts) if since_ts is not None else None
    upper_limit = float(until_ts) if until_ts is not None else now_ts
    meta: Dict[str, Any] = {
        "tenant": tenant_val,
        "since_ts": lower_limit,
        "until_ts": upper_limit,
        "limit_dialogs": limit_dialogs,
        "filtered_groups": 0,
        "candidate_chats": 0,
        "dialog_count": 0,
        "messages_in_range": 0,
        "messages_exported": 0,
    }

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
    if per_limit_int is not None and per_limit_int <= 0:
        per_limit_int = None

    pool = await _ensure_pool()
    if not pool:
        raise DatabaseUnavailableError("postgres_pool_unavailable")

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

    filtered_groups_row = await _fetchrow(
        f"""
        SELECT COUNT(DISTINCT m.lead_id) AS group_count
        FROM messages m
        JOIN leads l ON l.lead_id = m.lead_id
        LEFT JOIN lead_contacts lc ON lc.lead_id = m.lead_id
        LEFT JOIN contacts c ON c.id = lc.contact_id
        WHERE {' AND '.join(group_conditions)}
          AND (
                COALESCE(c.is_group, FALSE) = TRUE
             OR COALESCE(c.whatsapp_phone, '') LIKE '%@g.us'
          )
        """,
        *params_groups,
    )
    if filtered_groups_row and "group_count" in filtered_groups_row:
        try:
            meta["filtered_groups"] = int(filtered_groups_row["group_count"] or 0)
        except (TypeError, ValueError):
            meta["filtered_groups"] = 0

    params = [tenant_val]
    conditions = [
        "COALESCE(l.tenant_id, 0) = $1",
        "l.channel IN ('whatsapp', 'wa')",
        "COALESCE(c.is_group, FALSE) = FALSE",
        "COALESCE(c.whatsapp_phone, '') NOT LIKE '%@g.us'",
    ]
    if lower_limit is not None:
        params.append(float(lower_limit))
        conditions.append(f"m.created_at >= to_timestamp(${len(params)})")
    if upper_limit is not None:
        params.append(float(upper_limit))
        conditions.append(f"m.created_at <= to_timestamp(${len(params)})")

    limit_clause = ""
    if limit_int is not None:
        params.append(limit_int)
        limit_clause = f" LIMIT ${len(params)}"

    candidate_sql = f"""
        WITH candidate_chats AS (
            SELECT
                m.lead_id,
                lc.contact_id,
                c.whatsapp_phone,
                c.is_group,
                l.title,
                MAX(m.created_at) AS last_created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(NULLIF(lower(c.whatsapp_phone), ''), CONCAT('lead:', m.lead_id::text))
                    ORDER BY MAX(m.created_at) DESC
                ) AS chat_rank
            FROM messages m
            JOIN leads l ON l.lead_id = m.lead_id
            LEFT JOIN lead_contacts lc ON lc.lead_id = m.lead_id
            LEFT JOIN contacts c ON c.id = lc.contact_id
            WHERE {' AND '.join(conditions)}
            GROUP BY m.lead_id, lc.contact_id, c.whatsapp_phone, c.is_group, l.title
        )
        SELECT *
        FROM candidate_chats
        WHERE chat_rank = 1
        ORDER BY last_created_at DESC
        {limit_clause}
    """

    rows = await _fetch(candidate_sql, *params)
    meta["candidate_chats"] = len(rows)

    if not rows:
        count_row = await _fetchrow(
            f"""
            SELECT COUNT(*) AS msg_count
            FROM messages m
            JOIN leads l ON l.lead_id = m.lead_id
            WHERE {' AND '.join(group_conditions)}
            """,
            *params_groups,
        )
        if count_row and "msg_count" in count_row:
            try:
                meta["messages_in_range"] = int(count_row["msg_count"] or 0)
            except (TypeError, ValueError):
                meta["messages_in_range"] = 0
        _log.info(
            "[db] wa_export no_candidates tenant=%s since_ts=%s until_ts=%s messages_in_range=%s",
            tenant_val,
            lower_limit,
            upper_limit,
            meta.get("messages_in_range"),
        )
        meta.setdefault("distinct_chat_ids", [])
        meta.setdefault("top_chats", [])
        return [], meta

    summaries: List[Dict[str, Any]] = []
    for row in rows:
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
            last_ts = last_created.replace(tzinfo=last_created.tzinfo or timezone.utc).astimezone(timezone.utc).timestamp()
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
    if limit_int is not None:
        summaries = summaries[:limit_int]

    lead_ids = [summary["lead_id"] for summary in summaries]
    message_rows: List[Dict[str, Any]] = []
    if lead_ids:
        params_messages: List[Any] = [tenant_val, lead_ids]
        where_parts = [
            "m.lead_id = ANY($2::BIGINT[])",
            "COALESCE(l.tenant_id, 0) = $1",
        ]
        where_parts.append("l.channel IN ('whatsapp', 'wa')")
        if lower_limit is not None:
            params_messages.append(float(lower_limit))
            where_parts.append(f"m.created_at >= to_timestamp(${len(params_messages)})")
        if upper_limit is not None:
            params_messages.append(float(upper_limit))
            where_parts.append(f"m.created_at <= to_timestamp(${len(params_messages)})")

        message_sql = f"""
            SELECT m.lead_id, m.direction, m.text, extract(epoch FROM m.created_at) AS ts
            FROM messages m
            JOIN leads l ON l.lead_id = m.lead_id
            WHERE {' AND '.join(where_parts)}
            ORDER BY m.lead_id ASC, m.created_at ASC, m.id ASC
        """
        message_rows = await _fetch(message_sql, *params_messages)

    meta["messages_in_range"] = len(message_rows)

    grouped_messages: Dict[int, List[Dict[str, Any]]] = {}
    for row in message_rows:
        lead_id_raw = row.get("lead_id")
        try:
            lead_id = int(lead_id_raw)
        except (TypeError, ValueError):
            continue
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
        grouped_messages.setdefault(lead_id, []).append(
            {
                "ts": ts_val,
                "direction": direction_val,
                "text": text,
            }
        )

    dialogs: List[Dict[str, Any]] = []
    exported_chat_ids: List[str] = []
    for summary in summaries:
        lead_id = summary["lead_id"]
        messages = grouped_messages.get(lead_id, [])
        if not messages:
            continue
        if per_limit_int is not None:
            messages = messages[-per_limit_int:]
        dialogs.append(
            {
                "lead_id": lead_id,
                "contact_id": summary.get("contact_id"),
                "whatsapp_phone": summary.get("whatsapp_phone"),
                "title": summary.get("title") or "",
                "messages": messages,
                "last_ts": messages[-1]["ts"] if messages else summary.get("last_ts"),
                "chat_id": summary.get("chat_id"),
            }
        )
        exported_chat_ids.append(summary.get("chat_id"))

    meta.update(
        {
            "dialog_count": len(dialogs),
            "messages_exported": sum(len(d.get("messages") or []) for d in dialogs),
            "distinct_chat_ids": exported_chat_ids,
            "top_chats": [
                {"chat_id": d.get("chat_id"), "last_ts": d.get("last_ts")}
                for d in dialogs[:5]
            ],
        }
    )

    _log.info(
        "[db] wa_export summary tenant=%s distinct=%s filtered_groups=%s top5=%s",
        tenant_val,
        len(exported_chat_ids),
        meta.get("filtered_groups", 0),
        meta.get("top_chats", [])[:5],
    )

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
        LEFT JOIN leads l ON l.lead_id = m.lead_id
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
        msgs.append({
            "lead_id": lid,
            "direction": int(r["direction"]) if r.get("direction") is not None else 0,
            "text": r.get("text") or "",
            "ts": float(r.get("ts") or 0.0),
            "contact_id": r.get("contact_id"),
        })
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
        out.append({
            "lead_id": lid,
            "contact_id": contact_id,
            "messages": sanitized,
        })
    return out

# -------- Webhook log --------

async def insert_webhook_event(provider: str, event_type: str, lead_id: Optional[int], payload: dict):
    # Пытаемся писать в БД. Если пула нет — пишем в файл, чтобы вебхук не падал.
    pool = await _ensure_pool()
    if not pool:
        try:
            os.makedirs("/app/data", exist_ok=True)
            with open("/app/data/webhooks.log", "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "provider": provider,
                    "event_type": event_type,
                    "lead_id": lead_id,
                    "payload": payload,
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        return

    async with pool.acquire() as con:
        await con.execute("""
            INSERT INTO webhook_events(provider, event_type, lead_id, payload)
            VALUES($1, $2, $3, $4::jsonb);
        """, provider, event_type, lead_id, json.dumps(payload, ensure_ascii=False))
