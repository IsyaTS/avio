from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


@dataclass(frozen=True)
class AvitoTrainingExportWriteResult:
    training_file_path: str | None
    training_file_size: int
    training_examples_count: int


def write_training_examples_export(
    *,
    tenant_id: int,
    job_id: str,
    examples: Sequence[dict[str, Any]],
    export_root: str | Path | None = None,
) -> AvitoTrainingExportWriteResult:
    count = len(examples)
    if count <= 0:
        return AvitoTrainingExportWriteResult(
            training_file_path=None,
            training_file_size=0,
            training_examples_count=0,
        )

    root = Path(export_root or "/data/tenants")
    directory = root / str(int(tenant_id)) / "uploads" / "dialogs"
    directory.mkdir(parents=True, exist_ok=True)
    safe_job_id = "".join(ch for ch in str(job_id) if ch.isalnum() or ch in {"-", "_"})[:80]
    created_stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = directory / f"training_examples_{count}_{created_stamp}_{safe_job_id}.jsonl"

    with path.open("w", encoding="utf-8") as handle:
        for example in examples:
            handle.write(json.dumps(example, ensure_ascii=False, sort_keys=True))
            handle.write("\n")

    return AvitoTrainingExportWriteResult(
        training_file_path=str(path),
        training_file_size=path.stat().st_size,
        training_examples_count=count,
    )


__all__ = ["AvitoTrainingExportWriteResult", "write_training_examples_export"]
