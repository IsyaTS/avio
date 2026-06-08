from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


_SAFE_SUMMARY_STRING_RE = re.compile(r"^[A-Za-z0-9_./:-]{1,120}$")


@dataclass(frozen=True)
class AvitoContextualCaseWriteResult:
    contextual_file_path: str | None
    contextual_file_size: int
    contextual_cases_count: int
    review_cases_file_path: str | None
    review_cases_file_size: int
    review_cases_count: int
    rejected_cases_summary_path: str | None
    rejected_cases_summary_size: int
    domain_schema_path: str | None = None
    domain_schema_size: int = 0
    business_rules_draft_path: str | None = None
    business_rules_draft_size: int = 0


def write_contextual_case_exports(
    *,
    tenant_id: int,
    job_id: str,
    contextual_cases: Sequence[dict[str, Any]],
    review_cases: Sequence[dict[str, Any]] = (),
    rejected_summary: Mapping[str, Any] | None = None,
    domain_schema: Mapping[str, Any] | None = None,
    business_rules_draft: Mapping[str, Any] | None = None,
    export_root: str | Path | None = None,
) -> AvitoContextualCaseWriteResult:
    root = Path(export_root or "/data/tenants")
    directory = root / str(int(tenant_id)) / "uploads" / "dialogs"
    directory.mkdir(parents=True, exist_ok=True)
    safe_job_id = "".join(ch for ch in str(job_id) if ch.isalnum() or ch in {"-", "_"})[:80]
    created_stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

    domain_schema_id = str((domain_schema or {}).get("domain_schema_id") or "").strip()
    contextual_rows = _with_domain_schema_id(contextual_cases, domain_schema_id=domain_schema_id)
    review_rows = _with_domain_schema_id(review_cases, domain_schema_id=domain_schema_id)

    contextual_path = _write_jsonl(
        directory / f"contextual_cases_{len(contextual_cases)}_{created_stamp}_{safe_job_id}.jsonl",
        contextual_rows,
    )
    review_path = _write_jsonl(
        directory / f"review_cases_{len(review_cases)}_{created_stamp}_{safe_job_id}.jsonl",
        review_rows,
    )
    summary_path = directory / f"rejected_cases_summary_{created_stamp}_{safe_job_id}.json"
    summary_path.write_text(
        json.dumps(_sanitize_summary(rejected_summary or {}), ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    domain_path = _write_json(
        directory / f"domain_schema_{created_stamp}_{safe_job_id}.json",
        _sanitize_artifact(domain_schema or {}),
    )
    rules_path = _write_json(
        directory / f"business_rules_draft_{created_stamp}_{safe_job_id}.json",
        _sanitize_artifact(business_rules_draft or {}),
    )
    return AvitoContextualCaseWriteResult(
        contextual_file_path=str(contextual_path) if contextual_path else None,
        contextual_file_size=contextual_path.stat().st_size if contextual_path else 0,
        contextual_cases_count=len(contextual_cases),
        review_cases_file_path=str(review_path) if review_path else None,
        review_cases_file_size=review_path.stat().st_size if review_path else 0,
        review_cases_count=len(review_cases),
        rejected_cases_summary_path=str(summary_path),
        rejected_cases_summary_size=summary_path.stat().st_size,
        domain_schema_path=str(domain_path) if domain_path else None,
        domain_schema_size=domain_path.stat().st_size if domain_path else 0,
        business_rules_draft_path=str(rules_path) if rules_path else None,
        business_rules_draft_size=rules_path.stat().st_size if rules_path else 0,
    )


def _write_jsonl(path: Path, rows: Sequence[dict[str, Any]]) -> Path | None:
    if not rows:
        return None
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    return path


def _with_domain_schema_id(rows: Sequence[dict[str, Any]], *, domain_schema_id: str) -> list[dict[str, Any]]:
    if not rows:
        return []
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if domain_schema_id:
            item["domain_schema_id"] = domain_schema_id
        result.append(item)
    return result


def _write_json(path: Path, data: Mapping[str, Any]) -> Path | None:
    if not data:
        return None
    path.write_text(
        json.dumps(dict(data), ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    return path


def _sanitize_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in dict(summary or {}).items():
        if isinstance(value, Mapping):
            sanitized[str(key)] = _sanitize_summary(value)
        elif isinstance(value, (list, tuple, set)):
            sanitized[str(key)] = [
                item
                for item in value
                if isinstance(item, (int, float, bool, type(None)))
                or (isinstance(item, str) and _SAFE_SUMMARY_STRING_RE.match(item))
            ]
        elif isinstance(value, str):
            if _SAFE_SUMMARY_STRING_RE.match(value):
                sanitized[str(key)] = value
        elif isinstance(value, (int, float, bool, type(None))):
            sanitized[str(key)] = value
    return sanitized


def _sanitize_artifact(value: Mapping[str, Any]) -> dict[str, Any]:
    sanitized = _sanitize_artifact_value(value)
    text = json.dumps(sanitized, ensure_ascii=False)
    if any(marker in text for marker in ("Клиент:", "Менеджер:", "client:", "manager:")):
        return {}
    return sanitized if isinstance(sanitized, dict) else {}


def _sanitize_artifact_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _sanitize_artifact_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_artifact_value(item) for item in value]
    if isinstance(value, str):
        return " ".join(value.replace("\r", " ").replace("\n", " ").split())[:500]
    if isinstance(value, (int, float, bool, type(None))):
        return value
    return None


__all__ = ["AvitoContextualCaseWriteResult", "write_contextual_case_exports"]
