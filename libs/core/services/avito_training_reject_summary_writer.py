from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class AvitoTrainingRejectSummaryWriteResult:
    summary_file_path: str | None
    summary_file_size: int


def write_reject_summary_export(
    *,
    tenant_id: int,
    job_id: str,
    summary: Mapping[str, Any],
    export_root: str | Path | None = None,
) -> AvitoTrainingRejectSummaryWriteResult:
    root = Path(export_root or "/data/tenants")
    directory = root / str(int(tenant_id)) / "uploads" / "dialogs"
    directory.mkdir(parents=True, exist_ok=True)
    safe_job_id = "".join(ch for ch in str(job_id) if ch.isalnum() or ch in {"-", "_"})[:80]
    created_stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = directory / f"rejected_examples_summary_{created_stamp}_{safe_job_id}.json"
    path.write_text(json.dumps(_sanitize_summary(summary), ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    return AvitoTrainingRejectSummaryWriteResult(str(path), path.stat().st_size)


def _sanitize_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in dict(summary or {}).items():
        if isinstance(value, Mapping):
            sanitized[str(key)] = _sanitize_summary(value)
        elif isinstance(value, (list, tuple, set)):
            sanitized[str(key)] = [item for item in value if isinstance(item, (int, float, bool, type(None), str))]
        elif isinstance(value, (int, float, bool, type(None), str)):
            sanitized[str(key)] = value
    return sanitized


__all__ = ["AvitoTrainingRejectSummaryWriteResult", "write_reject_summary_export"]
