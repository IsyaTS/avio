from __future__ import annotations

import json
import logging
import pathlib
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from libs.core import db as db_module

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


async def ensure_schema() -> None:
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        logger.debug("avito_history_exports_ensure_skip reason=no_db")
        return
    statements = (
        """
        CREATE TABLE IF NOT EXISTS avito_history_export_jobs (
          job_id              TEXT PRIMARY KEY,
          tenant_id           INTEGER NOT NULL,
          status              TEXT NOT NULL,
          target_dialogs      INTEGER NOT NULL DEFAULT 0,
          candidates_seen     INTEGER NOT NULL DEFAULT 0,
          dialogs_accepted    INTEGER NOT NULL DEFAULT 0,
          dialogs_rejected    INTEGER NOT NULL DEFAULT 0,
          reject_reasons      JSONB,
          file_path           TEXT,
          file_size           INTEGER NOT NULL DEFAULT 0,
          contextual_file_path TEXT,
          contextual_file_size INTEGER NOT NULL DEFAULT 0,
          contextual_cases_count INTEGER NOT NULL DEFAULT 0,
          review_cases_file_path TEXT,
          review_cases_file_size INTEGER NOT NULL DEFAULT 0,
          review_cases_count INTEGER NOT NULL DEFAULT 0,
          rejected_cases_summary_path TEXT,
          rejected_cases_summary_size INTEGER NOT NULL DEFAULT 0,
          domain_schema_path TEXT,
          domain_schema_size INTEGER NOT NULL DEFAULT 0,
          business_rules_draft_path TEXT,
          business_rules_draft_size INTEGER NOT NULL DEFAULT 0,
          dialog_dataset_file_path TEXT,
          dialog_dataset_file_size INTEGER NOT NULL DEFAULT 0,
          dialog_dataset_count INTEGER NOT NULL DEFAULT 0,
          export_summary_path TEXT,
          export_summary_size INTEGER NOT NULL DEFAULT 0,
          export_pipeline_version TEXT,
          ai_schema_calls_count INTEGER NOT NULL DEFAULT 0,
          legacy_contextual_enabled BOOLEAN NOT NULL DEFAULT FALSE,
          checkpoint_path TEXT,
          checkpoint_available BOOLEAN NOT NULL DEFAULT FALSE,
          checkpoint_stage TEXT,
          domain_key TEXT,
          domain_label TEXT,
          domain_slots_count INTEGER NOT NULL DEFAULT 0,
          domain_schema_summary JSONB,
          contextual_quality_summary JSONB,
          contextual_mode      TEXT,
          ai_extracted_count   INTEGER NOT NULL DEFAULT 0,
          rule_fallback_count  INTEGER NOT NULL DEFAULT 0,
          context_bound_count  INTEGER NOT NULL DEFAULT 0,
          direct_example_count INTEGER NOT NULL DEFAULT 0,
          clarify_first_count  INTEGER NOT NULL DEFAULT 0,
          style_only_count     INTEGER NOT NULL DEFAULT 0,
          review_count         INTEGER NOT NULL DEFAULT 0,
          reject_count         INTEGER NOT NULL DEFAULT 0,
          training_file_path  TEXT,
          training_file_size  INTEGER NOT NULL DEFAULT 0,
          training_examples_count INTEGER NOT NULL DEFAULT 0,
          review_file_path    TEXT,
          review_file_size    INTEGER NOT NULL DEFAULT 0,
          review_examples_count INTEGER NOT NULL DEFAULT 0,
          summary_file_path   TEXT,
          summary_file_size   INTEGER NOT NULL DEFAULT 0,
          rejected_examples_count INTEGER NOT NULL DEFAULT 0,
          hard_rejected_count INTEGER NOT NULL DEFAULT 0,
          ai_rejected_count   INTEGER NOT NULL DEFAULT 0,
          ai_reviewed_count   INTEGER NOT NULL DEFAULT 0,
          ai_failed_count     INTEGER NOT NULL DEFAULT 0,
          quality_summary     JSONB,
          quality_mode        TEXT,
          api_errors_summary  JSONB,
          error_code          TEXT,
          selected_account_id BIGINT,
          selected_account_login TEXT,
          account_count       INTEGER NOT NULL DEFAULT 1,
          accounts_processed  INTEGER NOT NULL DEFAULT 0,
          created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
          finished_at         TIMESTAMPTZ,
          updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_avito_history_export_jobs_tenant_created
          ON avito_history_export_jobs(tenant_id, created_at DESC)
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS contextual_file_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS contextual_file_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS contextual_cases_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS review_cases_file_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS review_cases_file_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS review_cases_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS rejected_cases_summary_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS rejected_cases_summary_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS domain_schema_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS domain_schema_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS business_rules_draft_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS business_rules_draft_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS dialog_dataset_file_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS dialog_dataset_file_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS dialog_dataset_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS export_summary_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS export_summary_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS export_pipeline_version TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS ai_schema_calls_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS legacy_contextual_enabled BOOLEAN NOT NULL DEFAULT FALSE
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS checkpoint_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS checkpoint_available BOOLEAN NOT NULL DEFAULT FALSE
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS checkpoint_stage TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS domain_key TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS domain_label TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS domain_slots_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS domain_schema_summary JSONB
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS contextual_quality_summary JSONB
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS contextual_mode TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS ai_extracted_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS rule_fallback_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS context_bound_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS direct_example_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS clarify_first_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS style_only_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS review_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS reject_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS training_file_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS training_file_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS training_examples_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS review_file_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS review_file_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS review_examples_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS summary_file_path TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS summary_file_size INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS rejected_examples_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS hard_rejected_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS ai_rejected_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS ai_reviewed_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS ai_failed_count INTEGER NOT NULL DEFAULT 0
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS quality_summary JSONB
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS quality_mode TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS selected_account_id BIGINT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS selected_account_login TEXT
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS account_count INTEGER NOT NULL DEFAULT 1
        """,
        """
        ALTER TABLE avito_history_export_jobs
          ADD COLUMN IF NOT EXISTS accounts_processed INTEGER NOT NULL DEFAULT 0
        """,
    )
    for stmt in statements:
        try:
            await exec_fn(stmt)
        except Exception:
            logger.exception(
                "avito_history_exports_ensure_failed statement=%s",
                stmt.strip().split("\n", 1)[0],
            )
            raise


def _json(value: Mapping[str, Any] | None) -> str | None:
    if not value:
        return None
    return json.dumps(dict(value), ensure_ascii=False)


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    try:
        data = dict(row)
    except Exception:
        return None
    for key in ("reject_reasons", "api_errors_summary", "quality_summary", "contextual_quality_summary", "domain_schema_summary"):
        value = data.get(key)
        if isinstance(value, str):
            try:
                data[key] = json.loads(value)
            except Exception:
                data[key] = {}
        elif not isinstance(value, dict):
            data[key] = {}
    return data


async def create_job(
    *,
    job_id: str,
    tenant_id: int,
    target_dialogs: int,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    now = _now()
    if not fetchrow:
        logger.debug("avito_history_exports_create_skip reason=no_db")
        return {
            "job_id": job_id,
            "tenant_id": int(tenant_id),
            "status": "queued",
            "target_dialogs": int(target_dialogs),
            "created_at": now,
            "updated_at": now,
        }
    row = await fetchrow(
        """
        INSERT INTO avito_history_export_jobs (
          job_id, tenant_id, status, target_dialogs, created_at, updated_at
        )
        VALUES ($1, $2, 'queued', $3, $4, $4)
        RETURNING *
        """,
        str(job_id),
        int(tenant_id),
        int(target_dialogs),
        now,
    )
    return _row_to_dict(row)


async def claim_job(job_id: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return {
            "job_id": job_id,
            "status": "running",
            "updated_at": _now(),
        }
    row = await fetchrow(
        """
        UPDATE avito_history_export_jobs SET
          status = 'running',
          updated_at = $2
        WHERE job_id = $1
          AND status = 'queued'
        RETURNING *
        """,
        str(job_id),
        _now(),
    )
    return _row_to_dict(row)


async def get_active_job(tenant_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT *
        FROM avito_history_export_jobs
        WHERE tenant_id = $1
          AND status IN ('queued', 'running')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        int(tenant_id),
    )
    return _row_to_dict(row)


async def cancel_job(tenant_id: int, job_id: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        UPDATE avito_history_export_jobs SET
          status = 'cancelled',
          error_code = 'cancelled',
          finished_at = $3,
          updated_at = $3
        WHERE tenant_id = $1
          AND job_id = $2
          AND status IN ('queued', 'running')
        RETURNING *
        """,
        int(tenant_id),
        str(job_id),
        _now(),
    )
    return _row_to_dict(row)


async def update_progress(
    *,
    job_id: str,
    candidates_seen: int = 0,
    dialogs_accepted: int = 0,
    dialogs_rejected: int = 0,
    reject_reasons: Mapping[str, Any] | None = None,
    contextual_cases_count: int = 0,
    review_cases_count: int = 0,
    ai_extracted_count: int = 0,
    rule_fallback_count: int = 0,
    context_bound_count: int = 0,
    direct_example_count: int = 0,
    clarify_first_count: int = 0,
    style_only_count: int = 0,
    review_count: int = 0,
    reject_count: int = 0,
    contextual_mode: str | None = None,
    dialog_dataset_count: int = 0,
    export_pipeline_version: str | None = None,
    ai_schema_calls_count: int = 0,
    legacy_contextual_enabled: bool = False,
    checkpoint_available: bool = False,
    checkpoint_stage: str | None = None,
    api_errors_summary: Mapping[str, Any] | None = None,
    error_code: str | None = None,
    selected_account_id: int | None = None,
    selected_account_login: str | None = None,
    account_count: int = 1,
    accounts_processed: int = 0,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    now = _now()
    if not fetchrow:
        logger.debug("avito_history_exports_progress_skip reason=no_db")
        return {
            "job_id": job_id,
            "status": "running",
            "candidates_seen": int(candidates_seen),
            "dialogs_accepted": int(dialogs_accepted),
            "dialogs_rejected": int(dialogs_rejected),
            "reject_reasons": dict(reject_reasons or {}),
            "contextual_cases_count": int(contextual_cases_count),
            "review_cases_count": int(review_cases_count),
            "ai_extracted_count": int(ai_extracted_count),
            "rule_fallback_count": int(rule_fallback_count),
            "context_bound_count": int(context_bound_count),
            "direct_example_count": int(direct_example_count),
            "clarify_first_count": int(clarify_first_count),
            "style_only_count": int(style_only_count),
            "review_count": int(review_count),
            "reject_count": int(reject_count),
            "contextual_mode": contextual_mode,
            "dialog_dataset_count": int(dialog_dataset_count),
            "export_pipeline_version": export_pipeline_version,
            "ai_schema_calls_count": int(ai_schema_calls_count),
            "legacy_contextual_enabled": bool(legacy_contextual_enabled),
            "checkpoint_available": bool(checkpoint_available),
            "checkpoint_stage": checkpoint_stage,
            "api_errors_summary": dict(api_errors_summary or {}),
            "error_code": error_code,
            "updated_at": now,
        }
    row = await fetchrow(
        """
        UPDATE avito_history_export_jobs SET
          status = CASE WHEN status = 'running' THEN 'running' ELSE status END,
          candidates_seen = $2,
          dialogs_accepted = $3,
          dialogs_rejected = $4,
          reject_reasons = $5::jsonb,
          contextual_cases_count = $6,
          review_cases_count = $7,
          ai_extracted_count = $8,
          rule_fallback_count = $9,
          context_bound_count = $10,
          direct_example_count = $11,
          clarify_first_count = $12,
          style_only_count = $13,
          review_count = $14,
          reject_count = $15,
          contextual_mode = COALESCE($16, contextual_mode),
          dialog_dataset_count = $17,
          export_pipeline_version = COALESCE($18, export_pipeline_version),
          ai_schema_calls_count = $19,
          legacy_contextual_enabled = $20,
          checkpoint_available = $21,
          checkpoint_stage = COALESCE($22, checkpoint_stage),
          api_errors_summary = $23::jsonb,
          error_code = $24,
          updated_at = $25
        WHERE job_id = $1
        RETURNING *
        """,
        str(job_id),
        int(candidates_seen),
        int(dialogs_accepted),
        int(dialogs_rejected),
        _json(reject_reasons),
        int(contextual_cases_count),
        int(review_cases_count),
        int(ai_extracted_count),
        int(rule_fallback_count),
        int(context_bound_count),
        int(direct_example_count),
        int(clarify_first_count),
        int(style_only_count),
        int(review_count),
        int(reject_count),
        contextual_mode,
        int(dialog_dataset_count),
        export_pipeline_version,
        int(ai_schema_calls_count),
        bool(legacy_contextual_enabled),
        bool(checkpoint_available),
        checkpoint_stage,
        _json(api_errors_summary),
        error_code,
        now,
    )
    return _row_to_dict(row)


async def finish_job(
    *,
    job_id: str,
    status: str,
    candidates_seen: int = 0,
    dialogs_accepted: int = 0,
    dialogs_rejected: int = 0,
    reject_reasons: Mapping[str, Any] | None = None,
    file_path: str | None = None,
    file_size: int = 0,
    contextual_file_path: str | None = None,
    contextual_file_size: int = 0,
    contextual_cases_count: int = 0,
    review_cases_file_path: str | None = None,
    review_cases_file_size: int = 0,
    review_cases_count: int = 0,
    rejected_cases_summary_path: str | None = None,
    rejected_cases_summary_size: int = 0,
    domain_schema_path: str | None = None,
    domain_schema_size: int = 0,
    business_rules_draft_path: str | None = None,
    business_rules_draft_size: int = 0,
    dialog_dataset_file_path: str | None = None,
    dialog_dataset_file_size: int = 0,
    dialog_dataset_count: int = 0,
    export_summary_path: str | None = None,
    export_summary_size: int = 0,
    export_pipeline_version: str | None = None,
    ai_schema_calls_count: int = 0,
    legacy_contextual_enabled: bool = False,
    checkpoint_path: str | None = None,
    checkpoint_available: bool = False,
    checkpoint_stage: str | None = None,
    domain_key: str | None = None,
    domain_label: str | None = None,
    domain_slots_count: int = 0,
    domain_schema_summary: Mapping[str, Any] | None = None,
    contextual_quality_summary: Mapping[str, Any] | None = None,
    contextual_mode: str | None = None,
    ai_extracted_count: int = 0,
    rule_fallback_count: int = 0,
    context_bound_count: int = 0,
    direct_example_count: int = 0,
    clarify_first_count: int = 0,
    style_only_count: int = 0,
    review_count: int = 0,
    reject_count: int = 0,
    training_file_path: str | None = None,
    training_file_size: int = 0,
    training_examples_count: int = 0,
    review_file_path: str | None = None,
    review_file_size: int = 0,
    review_examples_count: int = 0,
    summary_file_path: str | None = None,
    summary_file_size: int = 0,
    rejected_examples_count: int = 0,
    hard_rejected_count: int = 0,
    ai_rejected_count: int = 0,
    ai_reviewed_count: int = 0,
    ai_failed_count: int = 0,
    quality_summary: Mapping[str, Any] | None = None,
    quality_mode: str | None = None,
    api_errors_summary: Mapping[str, Any] | None = None,
    error_code: str | None = None,
    selected_account_id: int | None = None,
    selected_account_login: str | None = None,
    account_count: int = 1,
    accounts_processed: int = 0,
    target_dialogs: int | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    now = _now()
    if not fetchrow:
        logger.debug("avito_history_exports_finish_skip reason=no_db")
        return {
            "job_id": job_id,
            "status": status,
            "target_dialogs": target_dialogs,
            "candidates_seen": int(candidates_seen),
            "dialogs_accepted": int(dialogs_accepted),
            "dialogs_rejected": int(dialogs_rejected),
            "reject_reasons": dict(reject_reasons or {}),
            "file_path": file_path,
            "file_size": int(file_size),
            "contextual_file_path": contextual_file_path,
            "contextual_file_size": int(contextual_file_size),
            "contextual_cases_count": int(contextual_cases_count),
            "review_cases_file_path": review_cases_file_path,
            "review_cases_file_size": int(review_cases_file_size),
            "review_cases_count": int(review_cases_count),
            "rejected_cases_summary_path": rejected_cases_summary_path,
            "rejected_cases_summary_size": int(rejected_cases_summary_size),
            "domain_schema_path": domain_schema_path,
            "domain_schema_size": int(domain_schema_size),
            "business_rules_draft_path": business_rules_draft_path,
            "business_rules_draft_size": int(business_rules_draft_size),
            "dialog_dataset_file_path": dialog_dataset_file_path,
            "dialog_dataset_file_size": int(dialog_dataset_file_size),
            "dialog_dataset_count": int(dialog_dataset_count),
            "export_summary_path": export_summary_path,
            "export_summary_size": int(export_summary_size),
            "export_pipeline_version": export_pipeline_version,
            "ai_schema_calls_count": int(ai_schema_calls_count),
            "legacy_contextual_enabled": bool(legacy_contextual_enabled),
            "checkpoint_path": checkpoint_path,
            "checkpoint_available": bool(checkpoint_available),
            "checkpoint_stage": checkpoint_stage,
            "domain_key": domain_key,
            "domain_label": domain_label,
            "domain_slots_count": int(domain_slots_count),
            "domain_schema_summary": dict(domain_schema_summary or {}),
            "contextual_quality_summary": dict(contextual_quality_summary or {}),
            "contextual_mode": contextual_mode,
            "ai_extracted_count": int(ai_extracted_count),
            "rule_fallback_count": int(rule_fallback_count),
            "context_bound_count": int(context_bound_count),
            "direct_example_count": int(direct_example_count),
            "clarify_first_count": int(clarify_first_count),
            "style_only_count": int(style_only_count),
            "review_count": int(review_count),
            "reject_count": int(reject_count),
            "training_file_path": training_file_path,
            "training_file_size": int(training_file_size),
            "training_examples_count": int(training_examples_count),
            "review_file_path": review_file_path,
            "review_file_size": int(review_file_size),
            "review_examples_count": int(review_examples_count),
            "summary_file_path": summary_file_path,
            "summary_file_size": int(summary_file_size),
            "rejected_examples_count": int(rejected_examples_count),
            "hard_rejected_count": int(hard_rejected_count),
            "ai_rejected_count": int(ai_rejected_count),
            "ai_reviewed_count": int(ai_reviewed_count),
            "ai_failed_count": int(ai_failed_count),
            "quality_summary": dict(quality_summary or {}),
            "quality_mode": quality_mode,
            "api_errors_summary": dict(api_errors_summary or {}),
            "error_code": error_code,
            "selected_account_id": selected_account_id,
            "selected_account_login": selected_account_login,
            "account_count": int(account_count),
            "accounts_processed": int(accounts_processed),
            "finished_at": now,
            "updated_at": now,
        }
    row = await fetchrow(
        """
        UPDATE avito_history_export_jobs SET
          status = $2,
          candidates_seen = $3,
          dialogs_accepted = $4,
          dialogs_rejected = $5,
          reject_reasons = $6::jsonb,
          file_path = $7,
          file_size = $8,
          training_file_path = $9,
          training_file_size = $10,
          training_examples_count = $11,
          review_file_path = $12,
          review_file_size = $13,
          review_examples_count = $14,
          summary_file_path = $15,
          summary_file_size = $16,
          rejected_examples_count = $17,
          hard_rejected_count = $18,
          ai_rejected_count = $19,
          ai_reviewed_count = $20,
          ai_failed_count = $21,
          quality_summary = $22::jsonb,
          quality_mode = $23,
          api_errors_summary = $24::jsonb,
          error_code = $25,
          finished_at = $26,
          updated_at = $26
        WHERE job_id = $1
        RETURNING *
        """,
        str(job_id),
        str(status),
        int(candidates_seen),
        int(dialogs_accepted),
        int(dialogs_rejected),
        _json(reject_reasons),
        file_path,
        int(file_size),
        training_file_path,
        int(training_file_size),
        int(training_examples_count),
        review_file_path,
        int(review_file_size),
        int(review_examples_count),
        summary_file_path,
        int(summary_file_size),
        int(rejected_examples_count),
        int(hard_rejected_count),
        int(ai_rejected_count),
        int(ai_reviewed_count),
        int(ai_failed_count),
        _json(quality_summary),
        quality_mode,
        _json(api_errors_summary),
        error_code,
        now,
    )
    if row is not None:
        row = await fetchrow(
            """
            UPDATE avito_history_export_jobs SET
              contextual_file_path = $2,
              contextual_file_size = $3,
              contextual_cases_count = $4,
              review_cases_file_path = $5,
              review_cases_file_size = $6,
              review_cases_count = $7,
              rejected_cases_summary_path = $8,
              rejected_cases_summary_size = $9,
              domain_schema_path = $10,
              domain_schema_size = $11,
              business_rules_draft_path = $12,
              business_rules_draft_size = $13,
              dialog_dataset_file_path = $14,
              dialog_dataset_file_size = $15,
              dialog_dataset_count = $16,
              export_summary_path = $17,
              export_summary_size = $18,
              export_pipeline_version = $19,
              ai_schema_calls_count = $20,
              legacy_contextual_enabled = $21,
              checkpoint_path = $22,
              checkpoint_available = $23,
              checkpoint_stage = $24,
              domain_key = $25,
              domain_label = $26,
              domain_slots_count = $27,
              domain_schema_summary = $28::jsonb,
              contextual_quality_summary = $29::jsonb,
              contextual_mode = $30,
              ai_extracted_count = $31,
              rule_fallback_count = $32,
              context_bound_count = $33,
              direct_example_count = $34,
              clarify_first_count = $35,
              style_only_count = $36,
              review_count = $37,
              reject_count = $38,
              selected_account_id = $39,
              selected_account_login = $40,
              account_count = $41,
              accounts_processed = $42,
              updated_at = $43
            WHERE job_id = $1
            RETURNING *
            """,
            str(job_id),
            contextual_file_path,
            int(contextual_file_size),
            int(contextual_cases_count),
            review_cases_file_path,
            int(review_cases_file_size),
            int(review_cases_count),
            rejected_cases_summary_path,
            int(rejected_cases_summary_size),
            domain_schema_path,
            int(domain_schema_size),
            business_rules_draft_path,
            int(business_rules_draft_size),
            dialog_dataset_file_path,
            int(dialog_dataset_file_size),
            int(dialog_dataset_count),
            export_summary_path,
            int(export_summary_size),
            export_pipeline_version,
            int(ai_schema_calls_count),
            bool(legacy_contextual_enabled),
            checkpoint_path,
            bool(checkpoint_available),
            checkpoint_stage,
            domain_key,
            domain_label,
            int(domain_slots_count),
            _json(domain_schema_summary),
            _json(contextual_quality_summary),
            contextual_mode,
            int(ai_extracted_count),
            int(rule_fallback_count),
            int(context_bound_count),
            int(direct_example_count),
            int(clarify_first_count),
            int(style_only_count),
            int(review_count),
            int(reject_count),
            selected_account_id,
            selected_account_login,
            int(account_count),
            int(accounts_processed),
            now,
        )
    return _row_to_dict(row)


async def reset_interrupted_jobs(*, stale_after_seconds: int = 5) -> int:
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return 0
    cutoff = _now() - timedelta(seconds=max(1, int(stale_after_seconds or 5)))
    result = await exec_fn(
        """
        UPDATE avito_history_export_jobs SET
          status = 'queued',
          updated_at = $1
        WHERE status = 'running'
          AND updated_at <= $2
        """,
        _now(),
        cutoff,
    )
    try:
        return int(str(result).rsplit(" ", 1)[-1])
    except Exception:
        return 0


async def list_queued_jobs(*, limit: int = 10) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    safe_limit = max(1, min(int(limit or 10), 100))
    rows = await fetch(
        """
        SELECT *
        FROM avito_history_export_jobs
        WHERE status = 'queued'
        ORDER BY created_at ASC
        LIMIT $1
        """,
        safe_limit,
    )
    return [row for row in (_row_to_dict(row) for row in rows or []) if row]


async def get_job(tenant_id: int, job_id: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT *
        FROM avito_history_export_jobs
        WHERE tenant_id = $1 AND job_id = $2
        LIMIT 1
        """,
        int(tenant_id),
        str(job_id),
    )
    return _row_to_dict(row)


async def get_latest_file_job(tenant_id: int) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await fetchrow(
        """
        SELECT *
        FROM avito_history_export_jobs
        WHERE tenant_id = $1
          AND status IN ('completed', 'partial')
          AND (
            (file_path IS NOT NULL AND file_size > 0)
            OR (contextual_file_path IS NOT NULL AND contextual_file_size > 0)
            OR (review_cases_file_path IS NOT NULL AND review_cases_file_size > 0)
            OR (rejected_cases_summary_path IS NOT NULL AND rejected_cases_summary_size > 0)
            OR (domain_schema_path IS NOT NULL AND domain_schema_size > 0)
            OR (business_rules_draft_path IS NOT NULL AND business_rules_draft_size > 0)
            OR (dialog_dataset_file_path IS NOT NULL AND dialog_dataset_file_size > 0)
            OR (export_summary_path IS NOT NULL AND export_summary_size > 0)
            OR (training_file_path IS NOT NULL AND training_file_size > 0)
            OR (review_file_path IS NOT NULL AND review_file_size > 0)
            OR (summary_file_path IS NOT NULL AND summary_file_size > 0)
          )
        ORDER BY finished_at DESC NULLS LAST, created_at DESC
        LIMIT 1
        """,
        int(tenant_id),
    )
    return _row_to_dict(row)


async def list_file_jobs(tenant_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    safe_limit = max(1, min(int(limit or 50), 100))
    rows = await fetch(
        """
        SELECT *
        FROM avito_history_export_jobs
        WHERE tenant_id = $1
          AND status IN ('completed', 'partial')
          AND (
            (file_path IS NOT NULL AND file_size > 0)
            OR (contextual_file_path IS NOT NULL AND contextual_file_size > 0)
            OR (review_cases_file_path IS NOT NULL AND review_cases_file_size > 0)
            OR (rejected_cases_summary_path IS NOT NULL AND rejected_cases_summary_size > 0)
            OR (domain_schema_path IS NOT NULL AND domain_schema_size > 0)
            OR (business_rules_draft_path IS NOT NULL AND business_rules_draft_size > 0)
            OR (dialog_dataset_file_path IS NOT NULL AND dialog_dataset_file_size > 0)
            OR (export_summary_path IS NOT NULL AND export_summary_size > 0)
            OR (training_file_path IS NOT NULL AND training_file_size > 0)
            OR (review_file_path IS NOT NULL AND review_file_size > 0)
            OR (summary_file_path IS NOT NULL AND summary_file_size > 0)
          )
        ORDER BY finished_at DESC NULLS LAST, created_at DESC
        LIMIT $2
        """,
        int(tenant_id),
        safe_limit,
    )
    return [row for row in (_row_to_dict(row) for row in rows or []) if row]


async def delete_file_job(tenant_id: int, job_id: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    row = await get_job(int(tenant_id), str(job_id))
    file_keys = (
        "file_path",
        "contextual_file_path",
        "review_cases_file_path",
        "rejected_cases_summary_path",
        "domain_schema_path",
        "business_rules_draft_path",
        "dialog_dataset_file_path",
        "export_summary_path",
        "checkpoint_path",
        "training_file_path",
        "review_file_path",
        "summary_file_path",
    )
    if not row or not any(row.get(key) for key in file_keys):
        return None

    for key in file_keys:
        path_value = row.get(key)
        if not path_value:
            continue
        file_path = pathlib.Path(str(path_value or ""))
        try:
            if file_path.is_file():
                file_path.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            logger.exception(
                "avito_history_export_file_delete_failed tenant=%s job=%s kind=%s",
                tenant_id,
                job_id,
                key,
            )
            raise

    now = _now()
    updated = await fetchrow(
        """
        UPDATE avito_history_export_jobs SET
          status = 'deleted',
          file_path = NULL,
          file_size = 0,
          contextual_file_path = NULL,
          contextual_file_size = 0,
          contextual_cases_count = 0,
          review_cases_file_path = NULL,
          review_cases_file_size = 0,
          review_cases_count = 0,
          rejected_cases_summary_path = NULL,
          rejected_cases_summary_size = 0,
          domain_schema_path = NULL,
          domain_schema_size = 0,
          business_rules_draft_path = NULL,
          business_rules_draft_size = 0,
          dialog_dataset_file_path = NULL,
          dialog_dataset_file_size = 0,
          dialog_dataset_count = 0,
          export_summary_path = NULL,
          export_summary_size = 0,
          export_pipeline_version = NULL,
          ai_schema_calls_count = 0,
          legacy_contextual_enabled = FALSE,
          checkpoint_path = NULL,
          checkpoint_available = FALSE,
          checkpoint_stage = NULL,
          domain_key = NULL,
          domain_label = NULL,
          domain_slots_count = 0,
          domain_schema_summary = NULL,
          contextual_quality_summary = NULL,
          contextual_mode = NULL,
          ai_extracted_count = 0,
          rule_fallback_count = 0,
          context_bound_count = 0,
          direct_example_count = 0,
          clarify_first_count = 0,
          style_only_count = 0,
          review_count = 0,
          reject_count = 0,
          training_file_path = NULL,
          training_file_size = 0,
          training_examples_count = 0,
          review_file_path = NULL,
          review_file_size = 0,
          review_examples_count = 0,
          summary_file_path = NULL,
          summary_file_size = 0,
          rejected_examples_count = 0,
          hard_rejected_count = 0,
          ai_rejected_count = 0,
          ai_reviewed_count = 0,
          ai_failed_count = 0,
          quality_summary = NULL,
          quality_mode = NULL,
          updated_at = $3
        WHERE tenant_id = $1 AND job_id = $2
        RETURNING *
        """,
        int(tenant_id),
        str(job_id),
        now,
    )
    return _row_to_dict(updated)


__all__ = [
    "cancel_job",
    "claim_job",
    "create_job",
    "delete_file_job",
    "finish_job",
    "get_active_job",
    "get_job",
    "get_latest_file_job",
    "list_file_jobs",
    "list_queued_jobs",
    "reset_interrupted_jobs",
    "update_progress",
    "ensure_schema",
]
