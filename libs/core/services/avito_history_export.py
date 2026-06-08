from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Mapping

from libs.core.integrations import avito_analytics as avito_api
from libs.core.services import (
    avito_context_extractor,
    avito_contextual_ai_extractor,
    avito_contextual_case_builder,
    avito_contextual_case_policy,
    avito_contextual_case_writer,
    avito_dialog_dataset_writer,
    avito_domain_context_extractor,
    avito_domain_schema_discovery,
    avito_dialog_export_writer,
    avito_dialog_filter,
    avito_export_checkpoint,
)
from libs.core.services.avito_dialog_filter import AvitoDialogMessage
from libs.core.services.avito_history_probe import (
    AvitoHistoryProbeError,
    _call_avito_with_rate_backoff,
    _chunks,
    _coerce_int,
    _error_code,
    _extract_chat_id,
    _extract_item_ids,
    _extract_items,
    _message_datetime,
    _payload_has_more,
    _record_api_error,
    _resolve_access_token,
)

logger = logging.getLogger(__name__)


class AvitoHistoryExportError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class AvitoHistoryExportResult:
    status: str
    target_dialogs: int
    candidates_seen: int = 0
    dialogs_accepted: int = 0
    dialogs_rejected: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)
    file_path: str | None = None
    file_size: int = 0
    contextual_file_path: str | None = None
    contextual_file_size: int = 0
    contextual_cases_count: int = 0
    review_cases_file_path: str | None = None
    review_cases_file_size: int = 0
    review_cases_count: int = 0
    rejected_cases_summary_path: str | None = None
    rejected_cases_summary_size: int = 0
    domain_schema_path: str | None = None
    domain_schema_size: int = 0
    business_rules_draft_path: str | None = None
    business_rules_draft_size: int = 0
    dialog_dataset_file_path: str | None = None
    dialog_dataset_file_size: int = 0
    dialog_dataset_count: int = 0
    export_summary_path: str | None = None
    export_summary_size: int = 0
    export_pipeline_version: str | None = None
    ai_schema_calls_count: int = 0
    legacy_contextual_enabled: bool = False
    checkpoint_path: str | None = None
    checkpoint_available: bool = False
    checkpoint_stage: str | None = None
    domain_key: str | None = None
    domain_label: str | None = None
    domain_slots_count: int = 0
    domain_schema_summary: dict[str, Any] = field(default_factory=dict)
    contextual_quality_summary: dict[str, Any] = field(default_factory=dict)
    contextual_mode: str | None = None
    ai_extracted_count: int = 0
    rule_fallback_count: int = 0
    context_bound_count: int = 0
    direct_example_count: int = 0
    clarify_first_count: int = 0
    style_only_count: int = 0
    review_count: int = 0
    reject_count: int = 0
    training_file_path: str | None = None
    training_file_size: int = 0
    training_examples_count: int = 0
    review_file_path: str | None = None
    review_file_size: int = 0
    review_examples_count: int = 0
    summary_file_path: str | None = None
    summary_file_size: int = 0
    rejected_examples_count: int = 0
    hard_rejected_count: int = 0
    ai_rejected_count: int = 0
    ai_reviewed_count: int = 0
    ai_failed_count: int = 0
    quality_summary: dict[str, Any] = field(default_factory=dict)
    quality_mode: str | None = None
    api_errors_summary: dict[str, int] = field(default_factory=dict)
    error_code: str | None = None
    selected_account_id: int | None = None
    selected_account_login: str | None = None
    account_count: int = 1
    accounts_processed: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "target_dialogs": self.target_dialogs,
            "candidates_seen": self.candidates_seen,
            "dialogs_accepted": self.dialogs_accepted,
            "dialogs_rejected": self.dialogs_rejected,
            "reject_reasons": dict(self.reject_reasons),
            "file_path": self.file_path,
            "file_size": self.file_size,
            "contextual_file_path": self.contextual_file_path,
            "contextual_file_size": self.contextual_file_size,
            "contextual_cases_count": self.contextual_cases_count,
            "review_cases_file_path": self.review_cases_file_path,
            "review_cases_file_size": self.review_cases_file_size,
            "review_cases_count": self.review_cases_count,
            "rejected_cases_summary_path": self.rejected_cases_summary_path,
            "rejected_cases_summary_size": self.rejected_cases_summary_size,
            "domain_schema_path": self.domain_schema_path,
            "domain_schema_size": self.domain_schema_size,
            "business_rules_draft_path": self.business_rules_draft_path,
            "business_rules_draft_size": self.business_rules_draft_size,
            "dialog_dataset_file_path": self.dialog_dataset_file_path,
            "dialog_dataset_file_size": self.dialog_dataset_file_size,
            "dialog_dataset_count": self.dialog_dataset_count,
            "export_summary_path": self.export_summary_path,
            "export_summary_size": self.export_summary_size,
            "export_pipeline_version": self.export_pipeline_version,
            "ai_schema_calls_count": self.ai_schema_calls_count,
            "legacy_contextual_enabled": self.legacy_contextual_enabled,
            "checkpoint_path": self.checkpoint_path,
            "checkpoint_available": self.checkpoint_available,
            "checkpoint_stage": self.checkpoint_stage,
            "domain_key": self.domain_key,
            "domain_label": self.domain_label,
            "domain_slots_count": self.domain_slots_count,
            "domain_schema_summary": dict(self.domain_schema_summary),
            "contextual_quality_summary": dict(self.contextual_quality_summary),
            "contextual_mode": self.contextual_mode,
            "ai_extracted_count": self.ai_extracted_count,
            "rule_fallback_count": self.rule_fallback_count,
            "context_bound_count": self.context_bound_count,
            "direct_example_count": self.direct_example_count,
            "clarify_first_count": self.clarify_first_count,
            "style_only_count": self.style_only_count,
            "review_count": self.review_count,
            "reject_count": self.reject_count,
            "training_file_path": self.training_file_path,
            "training_file_size": self.training_file_size,
            "training_examples_count": self.training_examples_count,
            "review_file_path": self.review_file_path,
            "review_file_size": self.review_file_size,
            "review_examples_count": self.review_examples_count,
            "summary_file_path": self.summary_file_path,
            "summary_file_size": self.summary_file_size,
            "rejected_examples_count": self.rejected_examples_count,
            "hard_rejected_count": self.hard_rejected_count,
            "ai_rejected_count": self.ai_rejected_count,
            "ai_reviewed_count": self.ai_reviewed_count,
            "ai_failed_count": self.ai_failed_count,
            "quality_summary": dict(self.quality_summary),
            "quality_mode": self.quality_mode,
            "api_errors_summary": dict(self.api_errors_summary),
            "error_code": self.error_code,
            "selected_account_id": self.selected_account_id,
            "selected_account_login": self.selected_account_login,
            "account_count": self.account_count,
            "accounts_processed": self.accounts_processed,
        }


@dataclass(frozen=True)
class AvitoHistoryExportDeps:
    common_module: Any
    avito_module: Any
    avito_api_module: Any = avito_api
    filter_module: Any = avito_dialog_filter
    writer_module: Any = avito_dialog_export_writer
    dialog_dataset_writer_module: Any = avito_dialog_dataset_writer
    checkpoint_module: Any = avito_export_checkpoint
    contextual_builder_module: Any = avito_contextual_case_builder
    context_extractor_module: Any = avito_context_extractor
    domain_context_extractor_module: Any = avito_domain_context_extractor
    domain_schema_discovery_module: Any = avito_domain_schema_discovery
    contextual_ai_extractor_module: Any = avito_contextual_ai_extractor
    contextual_policy_module: Any = avito_contextual_case_policy
    contextual_writer_module: Any = avito_contextual_case_writer
    contextual_ai_client: Any | None = None
    quality_review_enabled: bool = True
    legacy_contextual_cases_enabled: bool = False
    export_root: str | None = None
    chat_page_limit: int = 100
    message_page_limit: int = 50
    max_message_pages_per_chat: int = 50
    chat_concurrency: int = 50
    item_page_limit: int = 100
    item_ids_per_chat_query: int = 10
    item_chat_filter_concurrency: int = 2
    max_candidates_multiplier: int = 20
    rate_limit_retries: int = 2
    rate_limit_backoff_seconds: float = 60.0
    progress_callback: Callable[[AvitoHistoryExportResult], Awaitable[None]] | None = None
    cancel_callback: Callable[[], Awaitable[bool]] | None = None
    logger: Any = logger


async def run_export(
    tenant_id: int,
    *,
    target_dialogs: int,
    job_id: str,
    account_id: int | None = None,
    all_accounts: bool = False,
    deps: AvitoHistoryExportDeps,
) -> AvitoHistoryExportResult:
    target = max(1, min(int(target_dialogs or 100), 10000))
    try:
        accounts = await _resolve_export_accounts(
            int(tenant_id),
            account_id=account_id,
            all_accounts=all_accounts,
            deps=deps,
        )
    except AvitoHistoryProbeError as exc:
        raise AvitoHistoryExportError(exc.code, str(exc)) from exc

    result = AvitoHistoryExportResult(status="running", target_dialogs=target)
    result.account_count = max(1, len(accounts))
    if len(accounts) == 1:
        result.selected_account_id = accounts[0][1]
        result.selected_account_login = accounts[0][2]
    await _write_checkpoint(result, tenant_id=int(tenant_id), job_id=job_id, target=target, deps=deps, stage="scanning")
    accepted_dialogs: list[list[AvitoDialogMessage]] = []
    seen_chat_ids: set[str] = set()
    max_candidates = max(100_000, target * max(1, int(deps.max_candidates_multiplier or 20)))

    if await _is_cancelled(deps):
        _mark_cancelled(result)
        await _publish_progress(result, deps)
        return result

    for token, current_account_id, _login in accounts:
        seen_for_account: set[str] = set()
        remaining = target - result.dialogs_accepted
        if remaining <= 0:
            break
        hit_offset_cap = await _scan_global_chats(
            result,
            accepted_dialogs,
            seen_for_account,
            token=token,
            account_id=current_account_id,
            target_dialogs=target,
            max_candidates=max_candidates,
            deps=deps,
        )
        seen_chat_ids.update(f"{current_account_id}:{chat}" for chat in seen_for_account)
        if hit_offset_cap and result.dialogs_accepted < target and not result.error_code:
            await _scan_item_filtered_chats(
                result,
                accepted_dialogs,
                seen_for_account,
                token=token,
                account_id=current_account_id,
                target_dialogs=target,
                max_candidates=max_candidates,
                deps=deps,
            )
        result.accounts_processed += 1

    if await _is_cancelled(deps):
        _mark_cancelled(result)
    elif result.error_code == "cancelled":
        result.status = "cancelled"
    elif result.error_code == "rate_limited":
        result.status = "rate_limited"
    elif result.error_code == "not_connected":
        result.status = "no_connection"
    elif result.error_code:
        result.status = "failed"
    elif result.dialogs_accepted >= target:
        result.status = "completed"
    elif result.dialogs_accepted > 0:
        result.status = "partial"
    else:
        result.status = "failed"
        result.error_code = "empty"

    if accepted_dialogs and result.status != "cancelled":
        result.contextual_mode = "writing_markdown"
        await _write_checkpoint(
            result,
            tenant_id=int(tenant_id),
            job_id=job_id,
            target=target,
            deps=deps,
            stage="writing_markdown",
        )
        await _publish_progress(result, deps)
        write_result = deps.writer_module.write_markdown_export(
            tenant_id=int(tenant_id),
            job_id=job_id,
            dialogs=accepted_dialogs,
            export_root=deps.export_root,
        )
        result.file_path = write_result.file_path
        result.file_size = int(write_result.file_size)
        await _write_checkpoint(
            result,
            tenant_id=int(tenant_id),
            job_id=job_id,
            target=target,
            deps=deps,
            stage="discovering_domain",
            artifact_paths={"markdown": result.file_path},
        )
        if deps.legacy_contextual_cases_enabled:
            result.legacy_contextual_enabled = True
            await _build_legacy_contextual_artifacts(
                result,
                accepted_dialogs,
                tenant_id=int(tenant_id),
                job_id=job_id,
                deps=deps,
            )
        else:
            await _build_dialog_level_artifacts(
                result,
                accepted_dialogs,
                tenant_id=int(tenant_id),
                job_id=job_id,
                target=target,
                deps=deps,
            )
    await _publish_progress(result, deps)
    return result


async def _resolve_export_accounts(
    tenant_id: int,
    *,
    account_id: int | None,
    all_accounts: bool,
    deps: AvitoHistoryExportDeps,
) -> list[tuple[str, int | None, str | None]]:
    if account_id is not None:
        ensure_for_account = getattr(deps.avito_module, "ensure_access_token_for_account", None)
        if callable(ensure_for_account):
            token, account = await ensure_for_account(int(tenant_id), int(account_id))
            return [
                (
                    str(token),
                    int(account_id),
                    _account_display_name(account),
                )
            ]
    if all_accounts:
        list_accounts = getattr(deps.avito_module, "list_accounts", None)
        ensure_for_account = getattr(deps.avito_module, "ensure_access_token_for_account", None)
        if callable(list_accounts) and callable(ensure_for_account):
            accounts = await list_accounts(int(tenant_id))
            resolved: list[tuple[str, int | None, str | None]] = []
            for account in accounts:
                current_id = _coerce_int((account or {}).get("account_id"))
                if current_id is None:
                    continue
                token, refreshed = await ensure_for_account(int(tenant_id), int(current_id))
                resolved.append(
                    (
                        str(token),
                        int(current_id),
                        _account_display_name(refreshed or account),
                    )
                )
            if resolved:
                return resolved
    token, primary_account_id = await _resolve_access_token(int(tenant_id), deps)  # type: ignore[arg-type]
    return [(str(token), primary_account_id, None)]


def _account_display_name(account: Mapping[str, Any] | None) -> str | None:
    data = account or {}
    return (
        str(data.get("display_name") or "").strip()
        or str(data.get("account_login") or "").strip()
        or None
    )


async def _build_dialog_level_artifacts(
    result: AvitoHistoryExportResult,
    accepted_dialogs: list[list[AvitoDialogMessage]],
    *,
    tenant_id: int,
    job_id: str,
    target: int,
    deps: AvitoHistoryExportDeps,
) -> None:
    result.export_pipeline_version = avito_dialog_dataset_writer.PIPELINE_VERSION
    result.legacy_contextual_enabled = False
    result.contextual_mode = "discovering_domain"
    await _publish_progress(result, deps)
    discovery = await deps.domain_schema_discovery_module.discover_domain_schema(
        accepted_dialogs,
        tenant_id=int(tenant_id),
        enabled=bool(deps.quality_review_enabled),
    )
    domain_schema = dict(getattr(discovery, "domain_schema", {}) or {})
    business_rules_draft = dict(getattr(discovery, "business_rules_draft", {}) or {})
    result.ai_schema_calls_count = 1 if bool(getattr(discovery, "ai_extracted", False)) else 0
    _apply_domain_discovery_result(result, discovery, domain_schema)
    await _write_checkpoint(
        result,
        tenant_id=tenant_id,
        job_id=job_id,
        target=target,
        deps=deps,
        stage="writing_dialog_dataset",
        artifact_paths={"markdown": result.file_path},
        domain_schema_ready=True,
    )
    result.contextual_mode = "writing_dialog_dataset"
    await _publish_progress(result, deps)
    dataset_result = deps.dialog_dataset_writer_module.write_dialog_dataset_export(
        tenant_id=int(tenant_id),
        job_id=job_id,
        dialogs=accepted_dialogs,
        domain_schema_id=str(domain_schema.get("domain_schema_id") or "") or None,
        export_root=deps.export_root,
    )
    result.dialog_dataset_file_path = dataset_result.dialog_dataset_file_path
    result.dialog_dataset_file_size = int(dataset_result.dialog_dataset_file_size)
    result.dialog_dataset_count = int(dataset_result.dialog_dataset_count)
    await _write_checkpoint(
        result,
        tenant_id=tenant_id,
        job_id=job_id,
        target=target,
        deps=deps,
        stage="writing_artifacts",
        artifact_paths={
            "markdown": result.file_path,
            "dialog_dataset": result.dialog_dataset_file_path,
        },
        domain_schema_ready=True,
        dataset_rows_written=result.dialog_dataset_count,
    )
    result.contextual_mode = "writing_artifacts"
    await _publish_progress(result, deps)

    domain_write = deps.dialog_dataset_writer_module.write_json_artifact(
        tenant_id=int(tenant_id),
        job_id=job_id,
        prefix="domain_schema",
        data=domain_schema,
        export_root=deps.export_root,
    )
    rules_write = deps.dialog_dataset_writer_module.write_json_artifact(
        tenant_id=int(tenant_id),
        job_id=job_id,
        prefix="business_rules_draft",
        data=business_rules_draft,
        export_root=deps.export_root,
    )
    summary = _build_dialog_level_summary(result, discovery)
    summary_write = deps.dialog_dataset_writer_module.write_json_artifact(
        tenant_id=int(tenant_id),
        job_id=job_id,
        prefix="export_summary",
        data=summary,
        export_root=deps.export_root,
    )
    result.domain_schema_path = domain_write.file_path
    result.domain_schema_size = int(domain_write.file_size)
    result.business_rules_draft_path = rules_write.file_path
    result.business_rules_draft_size = int(rules_write.file_size)
    result.export_summary_path = summary_write.file_path
    result.export_summary_size = int(summary_write.file_size)
    result.quality_summary = summary
    result.quality_mode = "schema_only"
    result.contextual_quality_summary = {
        "pipeline_version": result.export_pipeline_version,
        "estimated_ai_mode": "schema_only",
    }
    result.contextual_mode = "schema_only"
    await _write_checkpoint(
        result,
        tenant_id=tenant_id,
        job_id=job_id,
        target=target,
        deps=deps,
        stage="completed" if result.status in {"completed", "partial"} else result.status,
        artifact_paths={
            "markdown": result.file_path,
            "dialog_dataset": result.dialog_dataset_file_path,
            "domain_schema": result.domain_schema_path,
            "business_rules_draft": result.business_rules_draft_path,
            "export_summary": result.export_summary_path,
        },
        domain_schema_ready=bool(result.domain_schema_path),
        dataset_rows_written=result.dialog_dataset_count,
    )


async def _build_legacy_contextual_artifacts(
    result: AvitoHistoryExportResult,
    accepted_dialogs: list[list[AvitoDialogMessage]],
    *,
    tenant_id: int,
    job_id: str,
    deps: AvitoHistoryExportDeps,
) -> None:
    result.contextual_mode = "discovering_domain"
    await _publish_progress(result, deps)
    discovery = await deps.domain_schema_discovery_module.discover_domain_schema(
        accepted_dialogs,
        tenant_id=int(tenant_id),
        enabled=bool(deps.quality_review_enabled),
    )
    domain_schema = dict(getattr(discovery, "domain_schema", {}) or {})
    business_rules_draft = dict(getattr(discovery, "business_rules_draft", {}) or {})
    result.domain_key = str(domain_schema.get("domain") or "generic_sales")
    result.domain_label = str(domain_schema.get("domain_label") or "продажи")
    slot_keys = set(domain_schema.get("required_slots") or []) | set(domain_schema.get("optional_slots") or [])
    result.domain_slots_count = len(slot_keys)
    result.domain_schema_summary = {
        "domain": result.domain_key,
        "domain_label": result.domain_label,
        "required_slots": list(domain_schema.get("required_slots") or []),
        "optional_slots": list(domain_schema.get("optional_slots") or []),
        "mode": str(getattr(discovery, "mode", "") or ""),
    }
    result.contextual_mode = "domain_ready"
    await _publish_progress(result, deps)
    build_result = deps.contextual_builder_module.build_contextual_case_candidates(
        accepted_dialogs,
        tenant_id=int(tenant_id),
    )
    candidates = list(build_result.candidates)
    result.contextual_mode = "building_cases"
    result.rule_fallback_count = len(candidates)
    await _publish_progress(result, deps)
    extractor_module = getattr(deps, "domain_context_extractor_module", None) or deps.context_extractor_module
    rule_extractions = {
        candidate.case_id: extractor_module.extract_context(candidate, domain_schema=domain_schema)
        for candidate in candidates
    }
    result.contextual_mode = "rule_extracted"
    await _publish_progress(result, deps)
    ai_client = deps.contextual_ai_client
    if ai_client is None and deps.quality_review_enabled:
        build_default = getattr(deps.contextual_ai_extractor_module, "build_default_extractor", None)
        ai_client = build_default() if callable(build_default) else None

    ai_result = None
    contextual_mode = "rule_fallback"
    ai_candidates = _select_ai_candidates(candidates, rule_extractions)
    if deps.quality_review_enabled and ai_client is not None and ai_candidates:
        try:
            result.contextual_mode = "ai_running"
            result.rule_fallback_count = max(0, len(candidates) - len(ai_candidates))
            await _publish_progress(result, deps)

            async def ai_progress(progress: Any) -> None:
                result.ai_extracted_count = int(getattr(progress, "extracted_count", 0) or 0)
                result.ai_failed_count = int(getattr(progress, "failed_count", 0) or 0)
                result.rule_fallback_count = max(0, len(candidates) - result.ai_extracted_count)
                result.contextual_mode = "ai_running"
                await _publish_progress(result, deps)

            ai_result = await deps.contextual_ai_extractor_module.extract_cases(
                ai_candidates,
                rule_extractions=rule_extractions,
                domain_schema=domain_schema,
                extractor=ai_client,
                enabled=True,
                progress_callback=ai_progress,
            )
            if int(ai_result.failed_count or 0):
                contextual_mode = "ai_failed_fallback"
            else:
                contextual_mode = "ai_selective" if len(ai_candidates) < len(candidates) else "ai"
        except Exception:
            deps.logger.exception("avito_history_export_contextual_ai_failed")
            contextual_mode = "ai_failed_fallback"

    ai_extractions = getattr(ai_result, "extractions", {}) if ai_result is not None else {}
    ai_extracted_count = int(getattr(ai_result, "extracted_count", 0) or 0)
    ai_failed_count = int(getattr(ai_result, "failed_count", 0) or 0)
    if deps.quality_review_enabled and ai_client is not None and ai_result is None:
        ai_failed_count = len(ai_candidates)
    elif not deps.quality_review_enabled:
        contextual_mode = "disabled"
    elif ai_client is None:
        contextual_mode = "ai_disabled"

    result.contextual_mode = "classifying_cases"
    result.ai_extracted_count = int(ai_extracted_count)
    result.ai_failed_count = int(ai_failed_count)
    result.rule_fallback_count = max(0, len(candidates) - int(ai_extracted_count))
    await _publish_progress(result, deps)
    policy_result = deps.contextual_policy_module.classify_cases(
        candidates=candidates,
        rule_extractions=rule_extractions,
        ai_extractions=ai_extractions,
        ai_extracted_count=ai_extracted_count,
        ai_failed_count=ai_failed_count,
        hard_reject_reasons=dict(build_result.hard_reject_reasons),
        builder_stats=dict(build_result.stats),
        domain_schema=domain_schema,
    )
    summary = dict(policy_result.quality_summary)
    summary["contextual_mode"] = contextual_mode
    summary["domain"] = result.domain_key
    summary["domain_slots_count"] = result.domain_slots_count
    result.contextual_mode = "writing_files"
    await _publish_progress(result, deps)
    write_result = deps.contextual_writer_module.write_contextual_case_exports(
        tenant_id=int(tenant_id),
        job_id=job_id,
        contextual_cases=policy_result.contextual_cases,
        review_cases=policy_result.review_cases,
        rejected_summary=summary,
        domain_schema=domain_schema,
        business_rules_draft=business_rules_draft,
        export_root=deps.export_root,
    )

    stats = dict(policy_result.stats)
    result.contextual_file_path = write_result.contextual_file_path
    result.contextual_file_size = int(write_result.contextual_file_size)
    result.contextual_cases_count = int(write_result.contextual_cases_count)
    result.review_cases_file_path = write_result.review_cases_file_path
    result.review_cases_file_size = int(write_result.review_cases_file_size)
    result.review_cases_count = int(write_result.review_cases_count)
    result.rejected_cases_summary_path = write_result.rejected_cases_summary_path
    result.rejected_cases_summary_size = int(write_result.rejected_cases_summary_size)
    result.domain_schema_path = write_result.domain_schema_path
    result.domain_schema_size = int(write_result.domain_schema_size)
    result.business_rules_draft_path = write_result.business_rules_draft_path
    result.business_rules_draft_size = int(write_result.business_rules_draft_size)
    result.rejected_examples_count = int(summary.get("rejected_cases_count") or 0)
    result.hard_rejected_count = int(build_result.hard_rejected_count)
    result.ai_extracted_count = int(ai_extracted_count)
    result.rule_fallback_count = max(0, len(candidates) - int(ai_extracted_count))
    result.ai_reviewed_count = int(ai_extracted_count)
    result.ai_failed_count = int(ai_failed_count)
    result.context_bound_count = int(stats.get("context_bound_count") or 0)
    result.direct_example_count = int(stats.get("direct_example_count") or 0)
    result.clarify_first_count = int(stats.get("clarify_first_count") or 0)
    result.style_only_count = int(stats.get("style_only_count") or 0)
    result.review_count = int(stats.get("review_count") or 0)
    result.reject_count = int(stats.get("reject_count") or 0)
    result.contextual_quality_summary = summary
    result.contextual_mode = contextual_mode
    result.quality_summary = summary
    result.quality_mode = contextual_mode


def _select_ai_candidates(
    candidates: list[Any],
    rule_extractions: Mapping[str, Mapping[str, Any]],
) -> list[Any]:
    selected: list[Any] = []
    for candidate in candidates:
        extraction = rule_extractions.get(getattr(candidate, "case_id", "")) or {}
        context = extraction.get("context") if isinstance(extraction.get("context"), Mapping) else {}
        reply_facts = extraction.get("reply_facts") if isinstance(extraction.get("reply_facts"), Mapping) else {}
        if _needs_ai_context_extraction(candidate, context, reply_facts):
            selected.append(candidate)
    return selected


def _needs_ai_context_extraction(
    candidate: Any,
    context: Mapping[str, Any],
    reply_facts: Mapping[str, Any],
) -> bool:
    if context.get("missing_facts"):
        return True
    if reply_facts.get("mentions_address") and not context.get("client_city"):
        return True
    if reply_facts.get("mentions_price") and not context.get("product_type"):
        return True
    if reply_facts.get("mentions_contact") and not any(
        bool(reply_facts.get(key))
        for key in ("mentions_address", "mentions_price", "mentions_delivery", "mentions_installation")
    ):
        return True
    if len(getattr(candidate, "history", []) or []) >= 6:
        return True
    return False


async def _scan_global_chats(
    result: AvitoHistoryExportResult,
    accepted_dialogs: list[list[AvitoDialogMessage]],
    seen_chat_ids: set[str],
    *,
    token: str,
    account_id: int | None,
    target_dialogs: int,
    max_candidates: int,
    deps: AvitoHistoryExportDeps,
) -> bool:
    offset = 0
    page_limit = max(1, min(int(deps.chat_page_limit or 100), 100))
    while result.dialogs_accepted < target_dialogs and result.candidates_seen < max_candidates:
        if await _is_cancelled(deps):
            _mark_cancelled(result)
            return False
        try:
            payload = await _call_avito_with_rate_backoff(
                lambda: deps.avito_api_module.messenger_list_chats(
                    token,
                    account_id,
                    limit=page_limit,
                    offset=offset,
                ),
                deps=deps,  # type: ignore[arg-type]
            )
        except Exception as exc:
            _record_export_error(result, exc)
            return False
        chats = _extract_items(payload, keys=("chats", "items", "result", "data"))
        if not chats:
            return False
        await _process_chat_page(
            result,
            accepted_dialogs,
            chats=chats,
            seen_chat_ids=seen_chat_ids,
            token=token,
            account_id=account_id,
            target_dialogs=target_dialogs,
            max_candidates=max_candidates,
            deps=deps,
        )
        await _publish_progress(result, deps)
        if result.error_code or result.dialogs_accepted >= target_dialogs:
            return False
        if _payload_has_more(payload) and offset >= 1000:
            return True
        if len(chats) < page_limit or not _payload_has_more(payload):
            return False
        offset += len(chats)
    return False


async def _scan_item_filtered_chats(
    result: AvitoHistoryExportResult,
    accepted_dialogs: list[list[AvitoDialogMessage]],
    seen_chat_ids: set[str],
    *,
    token: str,
    account_id: int | None,
    target_dialogs: int,
    max_candidates: int,
    deps: AvitoHistoryExportDeps,
) -> None:
    page = 1
    item_page_limit = max(1, min(int(deps.item_page_limit or 100), 100))
    item_ids_per_query = max(1, min(int(deps.item_ids_per_chat_query or 10), 10))
    semaphore = asyncio.Semaphore(max(1, min(int(deps.item_chat_filter_concurrency or 2), 10)))
    while result.dialogs_accepted < target_dialogs and result.candidates_seen < max_candidates:
        if await _is_cancelled(deps):
            _mark_cancelled(result)
            return
        try:
            payload = await _call_avito_with_rate_backoff(
                lambda: deps.avito_api_module.list_items(
                    token,
                    page=page,
                    per_page=item_page_limit,
                ),
                deps=deps,  # type: ignore[arg-type]
            )
        except Exception as exc:
            _record_export_error(result, exc)
            return
        item_ids = _extract_item_ids(payload)
        if not item_ids:
            return
        tasks = [
            asyncio.create_task(
                _fetch_item_chat_chunk(
                    semaphore,
                    deps,
                    token=token,
                    account_id=account_id,
                    item_ids=chunk,
                )
            )
            for chunk in _chunks(item_ids, item_ids_per_query)
        ]
        for task in asyncio.as_completed(tasks):
            if await _is_cancelled(deps):
                _mark_cancelled(result)
                break
            chats, exc = await task
            if exc is not None:
                _record_export_error(result, exc)
                if result.error_code:
                    break
            if chats:
                await _process_chat_page(
                    result,
                    accepted_dialogs,
                    chats=chats,
                    seen_chat_ids=seen_chat_ids,
                    token=token,
                    account_id=account_id,
                    target_dialogs=target_dialogs,
                    max_candidates=max_candidates,
                    deps=deps,
                )
            if result.error_code or result.dialogs_accepted >= target_dialogs:
                break
        for pending in tasks:
            if not pending.done():
                pending.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await _publish_progress(result, deps)
        if result.error_code or result.dialogs_accepted >= target_dialogs:
            return
        if len(item_ids) < item_page_limit:
            return
        page += 1


async def _fetch_item_chat_chunk(
    semaphore: asyncio.Semaphore,
    deps: AvitoHistoryExportDeps,
    *,
    token: str,
    account_id: int | None,
    item_ids: list[str],
) -> tuple[list[Mapping[str, Any]], Exception | None]:
    page_limit = max(1, min(int(deps.chat_page_limit or 100), 100))
    async with semaphore:
        chats: list[Mapping[str, Any]] = []
        offset = 0
        while offset <= 1000:
            try:
                payload = await _call_avito_with_rate_backoff(
                    lambda: deps.avito_api_module.messenger_list_chats(
                        token,
                        account_id,
                        limit=page_limit,
                        offset=offset,
                        item_ids=item_ids,
                    ),
                    deps=deps,  # type: ignore[arg-type]
                )
            except Exception as exc:
                return chats, exc
            page = _extract_items(payload, keys=("chats", "items", "result", "data"))
            if not page:
                break
            chats.extend(page)
            if len(page) < page_limit or not _payload_has_more(payload):
                break
            offset += len(page)
        return chats, None


async def _process_chat_page(
    result: AvitoHistoryExportResult,
    accepted_dialogs: list[list[AvitoDialogMessage]],
    *,
    chats: list[Mapping[str, Any]],
    seen_chat_ids: set[str],
    token: str,
    account_id: int | None,
    target_dialogs: int,
    max_candidates: int,
    deps: AvitoHistoryExportDeps,
) -> None:
    if await _is_cancelled(deps):
        _mark_cancelled(result)
        return
    chat_ids: list[str] = []
    for chat in chats:
        if result.dialogs_accepted >= target_dialogs or result.candidates_seen >= max_candidates:
            break
        chat_id = _extract_chat_id(chat)
        if not chat_id or chat_id in seen_chat_ids:
            continue
        seen_chat_ids.add(chat_id)
        chat_ids.append(chat_id)
    semaphore = asyncio.Semaphore(max(1, min(int(deps.chat_concurrency or 50), 100)))
    tasks = [
        asyncio.create_task(
            _process_one_chat(
                semaphore,
                chat_id,
                token=token,
                account_id=account_id,
                deps=deps,
            )
        )
        for chat_id in chat_ids
    ]
    for task in asyncio.as_completed(tasks):
        if await _is_cancelled(deps):
            _mark_cancelled(result)
            break
        dialog_result, exc = await task
        result.candidates_seen += 1
        if exc is not None:
            _record_export_error(result, exc)
            if result.error_code:
                break
        elif dialog_result and dialog_result.accepted:
            accepted_dialogs.append(dialog_result.messages)
            result.dialogs_accepted += 1
        else:
            reason = (dialog_result.reject_reason if dialog_result else "broken_export") or "broken_export"
            result.dialogs_rejected += 1
            result.reject_reasons[reason] = result.reject_reasons.get(reason, 0) + 1
        if result.dialogs_accepted >= target_dialogs or result.candidates_seen >= max_candidates:
            break
    for pending in tasks:
        if not pending.done():
            pending.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _process_one_chat(
    semaphore: asyncio.Semaphore,
    chat_id: str,
    *,
    token: str,
    account_id: int | None,
    deps: AvitoHistoryExportDeps,
) -> tuple[Any | None, Exception | None]:
    async with semaphore:
        try:
            messages, context_complete = await _fetch_chat_messages(
                token,
                account_id,
                chat_id,
                deps=deps,
            )
            normalized = _normalize_messages(messages, account_id=account_id)
            result = deps.filter_module.evaluate_dialog(
                normalized,
                context_complete=context_complete,
            )
            return result, None
        except Exception as exc:
            return None, exc


async def _fetch_chat_messages(
    token: str,
    account_id: int | None,
    chat_id: str,
    *,
    deps: AvitoHistoryExportDeps,
) -> tuple[list[Mapping[str, Any]], bool]:
    messages: list[Mapping[str, Any]] = []
    offset = 0
    page_limit = max(1, min(int(deps.message_page_limit or 50), 100))
    max_pages = max(1, int(deps.max_message_pages_per_chat or 50))
    context_complete = True
    for page_index in range(max_pages):
        payload = await _call_avito_with_rate_backoff(
            lambda: deps.avito_api_module.messenger_get_messages(
                token,
                account_id,
                chat_id,
                limit=page_limit,
                offset=offset,
            ),
            deps=deps,  # type: ignore[arg-type]
        )
        page = _extract_items(payload, keys=("messages", "items", "result", "data"))
        if not page:
            break
        messages.extend(page)
        if len(page) < page_limit or not _payload_has_more(payload):
            break
        offset += len(page)
        if page_index >= max_pages - 1:
            context_complete = False
    return messages, context_complete


def _normalize_messages(
    messages: list[Mapping[str, Any]],
    *,
    account_id: int | None,
) -> list[AvitoDialogMessage]:
    normalized: list[AvitoDialogMessage] = []
    for message in messages:
        text = _extract_message_text(message)
        timestamp = _message_datetime(message)
        role = _extract_message_role(message, account_id=account_id)
        normalized.append(
            AvitoDialogMessage(
                role=role,
                text=text,
                timestamp=timestamp,
            )
        )
    normalized.sort(key=lambda item: item.timestamp or datetime.min)
    return normalized


def _extract_message_text(message: Mapping[str, Any]) -> str:
    for key in ("text", "body", "message"):
        value = message.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    content = message.get("content")
    if isinstance(content, Mapping):
        for key in ("text", "body", "message", "value"):
            value = content.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
    return ""


def _extract_message_role(message: Mapping[str, Any], *, account_id: int | None) -> str:
    type_value = str(message.get("type") or message.get("message_type") or "").lower()
    if "system" in type_value:
        return "system"
    for key in ("role", "sender_type", "author_type"):
        value = str(message.get(key) or "").strip().lower()
        if value in {"client", "customer", "buyer", "user"}:
            return "client"
        if value in {"manager", "seller", "assistant", "operator"}:
            return "manager"
        if value == "system":
            return "system"
    direction = str(message.get("direction") or "").strip().lower()
    if direction in {"out", "outgoing", "sent"}:
        return "manager"
    if direction in {"in", "incoming", "received"}:
        return "client"
    if message.get("from_self") is True or message.get("is_out") is True:
        return "manager"
    author_id = message.get("author_id") or message.get("user_id") or message.get("sender_id")
    if account_id is not None and author_id is not None and str(author_id) == str(account_id):
        return "manager"
    text = _extract_message_text(message).lower()
    if "системное сообщение" in text or "ссылка на объявление" in text:
        return "system"
    return "unknown"


def _apply_domain_discovery_result(
    result: AvitoHistoryExportResult,
    discovery: Any,
    domain_schema: Mapping[str, Any],
) -> None:
    result.domain_key = str(domain_schema.get("domain") or "generic_sales")
    result.domain_label = str(domain_schema.get("domain_label") or "продажи")
    slot_keys = set(domain_schema.get("required_slots") or []) | set(domain_schema.get("optional_slots") or [])
    result.domain_slots_count = len(slot_keys)
    result.domain_schema_summary = {
        "domain": result.domain_key,
        "domain_label": result.domain_label,
        "required_slots": list(domain_schema.get("required_slots") or []),
        "optional_slots": list(domain_schema.get("optional_slots") or []),
        "mode": str(getattr(discovery, "mode", "") or ""),
    }


def _build_dialog_level_summary(result: AvitoHistoryExportResult, discovery: Any) -> dict[str, Any]:
    return {
        "target_dialogs": int(result.target_dialogs),
        "dialogs_accepted": int(result.dialogs_accepted),
        "dialogs_rejected": int(result.dialogs_rejected),
        "reject_reasons": dict(result.reject_reasons),
        "dialog_dataset_count": int(result.dialog_dataset_count),
        "domain_key": result.domain_key,
        "domain_label": result.domain_label,
        "domain_slots_count": int(result.domain_slots_count),
        "ai_schema_calls_count": int(result.ai_schema_calls_count),
        "pipeline_version": result.export_pipeline_version or avito_dialog_dataset_writer.PIPELINE_VERSION,
        "estimated_ai_mode": "schema_only",
        "domain_discovery_mode": str(getattr(discovery, "mode", "") or ""),
    }


async def _write_checkpoint(
    result: AvitoHistoryExportResult,
    *,
    tenant_id: int,
    job_id: str,
    target: int,
    deps: AvitoHistoryExportDeps,
    stage: str,
    artifact_paths: Mapping[str, str | None] | None = None,
    domain_schema_ready: bool = False,
    dataset_rows_written: int = 0,
) -> None:
    try:
        checkpoint = deps.checkpoint_module.write_export_checkpoint(
            tenant_id=int(tenant_id),
            job_id=job_id,
            target_dialogs=int(target),
            accepted_dialogs_count=int(result.dialogs_accepted),
            candidates_seen=int(result.candidates_seen),
            stage=stage,
            artifact_paths=artifact_paths,
            domain_schema_ready=domain_schema_ready,
            dataset_rows_written=dataset_rows_written,
            export_root=deps.export_root,
        )
    except Exception:
        deps.logger.exception("avito_history_export_checkpoint_failed job=%s stage=%s", job_id, stage)
        return
    result.checkpoint_path = checkpoint.checkpoint_path
    result.checkpoint_available = True
    result.checkpoint_stage = checkpoint.checkpoint_stage


def _record_export_error(result: AvitoHistoryExportResult, exc: Exception) -> None:
    _record_api_error(result, exc)  # type: ignore[arg-type]
    code = _error_code(exc)
    if code in {"unauthorized", "no_permission", "rate_limited"}:
        result.error_code = code


async def _is_cancelled(deps: AvitoHistoryExportDeps) -> bool:
    callback = deps.cancel_callback
    if callback is None:
        return False
    try:
        return bool(await callback())
    except Exception:
        deps.logger.exception("avito_history_export_cancel_check_failed")
        return False


def _mark_cancelled(result: AvitoHistoryExportResult) -> None:
    result.status = "cancelled"
    result.error_code = "cancelled"


async def _publish_progress(
    result: AvitoHistoryExportResult,
    deps: AvitoHistoryExportDeps,
) -> None:
    callback = deps.progress_callback
    if callback is not None:
        await callback(result)


__all__ = [
    "AvitoHistoryExportDeps",
    "AvitoHistoryExportError",
    "AvitoHistoryExportResult",
    "run_export",
]
