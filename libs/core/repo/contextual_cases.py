from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from libs.core import db as db_module


logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _json(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), ensure_ascii=False)


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    try:
        data = dict(row)
    except Exception:
        return None
    for key in ("domain_schema", "business_rules_draft", "context", "dialog", "reply_facts", "applicability", "quality"):
        value = data.get(key)
        if isinstance(value, str):
            try:
                data[key] = json.loads(value)
            except Exception:
                data[key] = {}
        elif not isinstance(value, dict):
            data[key] = {}
    return data


async def ensure_schema() -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("contextual_cases_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS contextual_case_sets (
          id BIGSERIAL PRIMARY KEY,
          tenant_id INTEGER NOT NULL,
          set_id TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT 'avito',
          source_export_job_id TEXT,
          domain_schema_id TEXT,
          domain_schema JSONB NOT NULL DEFAULT '{}',
          business_rules_draft JSONB NOT NULL DEFAULT '{}',
          status TEXT NOT NULL DEFAULT 'imported',
          cases_count INTEGER NOT NULL DEFAULT 0,
          active_cases_count INTEGER NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          activated_at TIMESTAMPTZ,
          UNIQUE(tenant_id, set_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS contextual_cases (
          id BIGSERIAL PRIMARY KEY,
          tenant_id INTEGER NOT NULL,
          set_id TEXT NOT NULL,
          case_id TEXT NOT NULL,
          domain_schema_id TEXT,
          domain TEXT,
          intent TEXT,
          mode TEXT,
          context JSONB NOT NULL DEFAULT '{}',
          dialog JSONB NOT NULL DEFAULT '{}',
          reply_facts JSONB NOT NULL DEFAULT '{}',
          applicability JSONB NOT NULL DEFAULT '{}',
          quality JSONB NOT NULL DEFAULT '{}',
          search_text TEXT NOT NULL,
          fingerprint CHAR(40),
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          embedding DOUBLE PRECISION[],
          embedding_model TEXT,
          embedding_status TEXT NOT NULL DEFAULT 'pending',
          embedding_error TEXT,
          times_used INTEGER NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(tenant_id, case_id)
        )
        """,
        "ALTER TABLE contextual_cases ADD COLUMN IF NOT EXISTS embedding DOUBLE PRECISION[]",
        "ALTER TABLE contextual_cases ADD COLUMN IF NOT EXISTS embedding_model TEXT",
        "ALTER TABLE contextual_cases ADD COLUMN IF NOT EXISTS embedding_status TEXT NOT NULL DEFAULT 'pending'",
        "ALTER TABLE contextual_cases ADD COLUMN IF NOT EXISTS embedding_error TEXT",
        "CREATE INDEX IF NOT EXISTS idx_contextual_case_sets_tenant_created ON contextual_case_sets(tenant_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_contextual_cases_tenant_active_domain_intent ON contextual_cases(tenant_id, is_active, domain, intent)",
        "CREATE INDEX IF NOT EXISTS idx_contextual_cases_tenant_set ON contextual_cases(tenant_id, set_id)",
        "CREATE INDEX IF NOT EXISTS idx_contextual_cases_tenant_embedding_status ON contextual_cases(tenant_id, embedding_status)",
        "CREATE INDEX IF NOT EXISTS idx_contextual_cases_tenant_fingerprint ON contextual_cases(tenant_id, fingerprint)",
    )
    for stmt in statements:
        await exec_fn(stmt)


async def create_case_set(
    *,
    tenant_id: int,
    set_id: str,
    source_export_job_id: str | None = None,
    domain_schema_id: str | None = None,
    domain_schema: Mapping[str, Any] | None = None,
    business_rules_draft: Mapping[str, Any] | None = None,
    cases_count: int = 0,
    active_cases_count: int = 0,
    status: str = "imported",
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return {
            "tenant_id": int(tenant_id),
            "set_id": set_id,
            "source_export_job_id": source_export_job_id,
            "domain_schema_id": domain_schema_id,
            "domain_schema": dict(domain_schema or {}),
            "business_rules_draft": dict(business_rules_draft or {}),
            "cases_count": int(cases_count),
            "active_cases_count": int(active_cases_count),
            "status": status,
        }
    row = await fetchrow(
        """
        INSERT INTO contextual_case_sets (
          tenant_id, set_id, source, source_export_job_id, domain_schema_id,
          domain_schema, business_rules_draft, status, cases_count, active_cases_count
        )
        VALUES ($1, $2, 'avito', $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9)
        ON CONFLICT (tenant_id, set_id) DO UPDATE SET
          source_export_job_id = EXCLUDED.source_export_job_id,
          domain_schema_id = EXCLUDED.domain_schema_id,
          domain_schema = EXCLUDED.domain_schema,
          business_rules_draft = EXCLUDED.business_rules_draft,
          status = EXCLUDED.status,
          cases_count = EXCLUDED.cases_count,
          active_cases_count = EXCLUDED.active_cases_count
        RETURNING *
        """,
        int(tenant_id),
        str(set_id),
        source_export_job_id,
        domain_schema_id,
        _json(domain_schema),
        _json(business_rules_draft),
        status,
        int(cases_count),
        int(active_cases_count),
    )
    return _row_to_dict(row)


async def upsert_contextual_cases(*, tenant_id: int, set_id: str, cases: Sequence[Mapping[str, Any]]) -> int:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return len(cases)
    count = 0
    for case in cases:
        row = await fetchrow(
            """
            INSERT INTO contextual_cases (
              tenant_id, set_id, case_id, domain_schema_id, domain, intent, mode,
              context, dialog, reply_facts, applicability, quality,
              search_text, fingerprint, is_active, embedding_status, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb,
                    $11::jsonb, $12::jsonb, $13, $14, TRUE, 'pending', $15)
            ON CONFLICT (tenant_id, case_id) DO UPDATE SET
              set_id = EXCLUDED.set_id,
              domain_schema_id = EXCLUDED.domain_schema_id,
              domain = EXCLUDED.domain,
              intent = EXCLUDED.intent,
              mode = EXCLUDED.mode,
              context = EXCLUDED.context,
              dialog = EXCLUDED.dialog,
              reply_facts = EXCLUDED.reply_facts,
              applicability = EXCLUDED.applicability,
              quality = EXCLUDED.quality,
              search_text = EXCLUDED.search_text,
              fingerprint = EXCLUDED.fingerprint,
              is_active = TRUE,
              updated_at = EXCLUDED.updated_at
            RETURNING id
            """,
            int(tenant_id),
            str(set_id),
            str(case.get("case_id") or ""),
            case.get("domain_schema_id"),
            case.get("domain"),
            case.get("intent"),
            case.get("mode"),
            _json(case.get("context") if isinstance(case.get("context"), Mapping) else {}),
            _json(case.get("dialog") if isinstance(case.get("dialog"), Mapping) else {}),
            _json(case.get("reply_facts") if isinstance(case.get("reply_facts"), Mapping) else {}),
            _json(case.get("applicability") if isinstance(case.get("applicability"), Mapping) else {}),
            _json(case.get("quality") if isinstance(case.get("quality"), Mapping) else {}),
            str(case.get("search_text") or ""),
            case.get("fingerprint"),
            _now(),
        )
        if row:
            count += 1
    return count


async def activate_case_set(tenant_id: int, set_id: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        UPDATE contextual_case_sets SET status = 'active', activated_at = $3
        WHERE tenant_id = $1 AND set_id = $2
        RETURNING *
        """,
        int(tenant_id),
        str(set_id),
        _now(),
    )
    return _row_to_dict(row)


async def get_active_domain_schema(tenant_id: int) -> dict[str, Any]:
    latest = await get_latest_active_case_set(tenant_id)
    if not latest:
        return {}
    schema = latest.get("domain_schema")
    return dict(schema) if isinstance(schema, Mapping) else {}


async def list_active_cases_for_retrieval(
    tenant_id: int,
    *,
    limit: int = 500,
    require_embedding: bool = False,
) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    sql = """
        SELECT *
        FROM contextual_cases
        WHERE tenant_id = $1 AND is_active = TRUE
          AND mode <> 'review' AND mode <> 'reject'
    """
    args: list[Any] = [int(tenant_id)]
    if require_embedding:
        sql += " AND embedding_status = 'ready' AND embedding IS NOT NULL"
    sql += " ORDER BY times_used ASC, updated_at DESC LIMIT $2"
    args.append(max(1, int(limit)))
    rows = await fetch(sql, *args)
    return [item for row in rows or [] if (item := _row_to_dict(row))]


async def fetch_pending_contextual_case_embeddings(limit: int = 20) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    rows = await fetch(
        """
        SELECT id, tenant_id, search_text, embedding_model
        FROM contextual_cases
        WHERE embedding_status = 'pending' AND is_active = TRUE
        ORDER BY updated_at ASC, id ASC
        LIMIT $1
        """,
        max(1, int(limit or 20)),
    )
    return [dict(row) for row in rows or []]


async def set_contextual_case_embedding(
    case_db_id: int,
    embedding: list[float] | None,
    *,
    embedding_model: str | None = None,
    status: str = "ready",
    error: str | None = None,
) -> None:
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    await exec_fn(
        """
        UPDATE contextual_cases SET
          embedding = $2,
          embedding_model = COALESCE($3, embedding_model),
          embedding_status = $4,
          embedding_error = $5,
          updated_at = now()
        WHERE id = $1
        """,
        int(case_db_id),
        embedding,
        embedding_model,
        status,
        error,
    )


async def increment_contextual_case_usage(ids: Sequence[int]) -> None:
    coerced = [int(item) for item in ids if str(item).isdigit()]
    if not coerced:
        return
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    await exec_fn(
        "UPDATE contextual_cases SET times_used = times_used + 1, updated_at = now() WHERE id = ANY($1::bigint[])",
        coerced,
    )


async def get_case_set_status(tenant_id: int, set_id: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT s.*,
          COALESCE(SUM(CASE WHEN c.embedding_status = 'ready' THEN 1 ELSE 0 END), 0)::int AS embedding_ready_count,
          COALESCE(SUM(CASE WHEN c.embedding_status = 'pending' THEN 1 ELSE 0 END), 0)::int AS embedding_pending_count
        FROM contextual_case_sets s
        LEFT JOIN contextual_cases c ON c.tenant_id = s.tenant_id AND c.set_id = s.set_id AND c.is_active = TRUE
        WHERE s.tenant_id = $1 AND s.set_id = $2
        GROUP BY s.id
        """,
        int(tenant_id),
        str(set_id),
    )
    return _row_to_dict(row)


async def get_latest_active_case_set(tenant_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT s.*,
          COALESCE(SUM(CASE WHEN c.embedding_status = 'ready' THEN 1 ELSE 0 END), 0)::int AS embedding_ready_count,
          COALESCE(SUM(CASE WHEN c.embedding_status = 'pending' THEN 1 ELSE 0 END), 0)::int AS embedding_pending_count
        FROM contextual_case_sets s
        LEFT JOIN contextual_cases c ON c.tenant_id = s.tenant_id AND c.set_id = s.set_id AND c.is_active = TRUE
        WHERE s.tenant_id = $1 AND s.status = 'active'
        GROUP BY s.id
        ORDER BY s.activated_at DESC NULLS LAST, s.created_at DESC
        LIMIT 1
        """,
        int(tenant_id),
    )
    return _row_to_dict(row)


async def deactivate_old_sets(tenant_id: int, keep_set_id: str) -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    await exec_fn(
        "UPDATE contextual_case_sets SET status = 'imported' WHERE tenant_id = $1 AND set_id <> $2 AND status = 'active'",
        int(tenant_id),
        str(keep_set_id),
    )
