from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


@dataclass(frozen=True)
class AvitoTrainingReviewExportWriteResult:
    review_file_path: str | None
    review_file_size: int
    review_examples_count: int


def write_review_examples_export(
    *,
    tenant_id: int,
    job_id: str,
    examples: Sequence[dict[str, Any]],
    export_root: str | Path | None = None,
) -> AvitoTrainingReviewExportWriteResult:
    count = len(examples)
    if count <= 0:
        return AvitoTrainingReviewExportWriteResult(None, 0, 0)

    root = Path(export_root or "/data/tenants")
    directory = root / str(int(tenant_id)) / "uploads" / "dialogs"
    directory.mkdir(parents=True, exist_ok=True)
    safe_job_id = "".join(ch for ch in str(job_id) if ch.isalnum() or ch in {"-", "_"})[:80]
    created_stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = directory / f"review_examples_{count}_{created_stamp}_{safe_job_id}.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for example in examples:
            handle.write(json.dumps(example, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    return AvitoTrainingReviewExportWriteResult(str(path), path.stat().st_size, count)


__all__ = ["AvitoTrainingReviewExportWriteResult", "write_review_examples_export"]
