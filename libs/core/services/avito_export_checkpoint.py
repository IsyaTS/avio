from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class AvitoExportCheckpointResult:
    checkpoint_path: str
    checkpoint_size: int
    checkpoint_stage: str


def write_export_checkpoint(
    *,
    tenant_id: int,
    job_id: str,
    target_dialogs: int,
    accepted_dialogs_count: int = 0,
    candidates_seen: int = 0,
    stage: str,
    artifact_paths: Mapping[str, str | None] | None = None,
    domain_schema_ready: bool = False,
    dataset_rows_written: int = 0,
    export_root: str | Path | None = None,
) -> AvitoExportCheckpointResult:
    path = checkpoint_path(tenant_id=tenant_id, job_id=job_id, export_root=export_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    now = _iso_utc()
    payload = {
        "tenant_id": int(tenant_id),
        "job_id": str(job_id),
        "target_dialogs": int(target_dialogs),
        "accepted_dialogs_count": int(accepted_dialogs_count),
        "candidates_seen": int(candidates_seen),
        "stage": str(stage or "unknown"),
        "artifact_paths": _artifact_metadata(artifact_paths or {}),
        "domain_schema_ready": bool(domain_schema_ready),
        "dataset_rows_written": int(dataset_rows_written),
        "updated_at": now,
    }
    if not path.exists():
        payload["created_at"] = now
    else:
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            payload["created_at"] = existing.get("created_at") or now
        except Exception:
            payload["created_at"] = now

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)
    return AvitoExportCheckpointResult(
        checkpoint_path=str(path),
        checkpoint_size=path.stat().st_size,
        checkpoint_stage=str(stage or "unknown"),
    )


def checkpoint_path(
    *,
    tenant_id: int,
    job_id: str,
    export_root: str | Path | None = None,
) -> Path:
    root = Path(export_root or "/data/tenants")
    safe_job_id = "".join(ch for ch in str(job_id) if ch.isalnum() or ch in {"-", "_"})[:80]
    return root / str(int(tenant_id)) / "uploads" / "dialogs" / "checkpoints" / f"{safe_job_id}_checkpoint.json"


def delete_export_checkpoint(
    *,
    tenant_id: int,
    job_id: str,
    export_root: str | Path | None = None,
) -> bool:
    path = checkpoint_path(tenant_id=tenant_id, job_id=job_id, export_root=export_root)
    try:
        path.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def _artifact_metadata(paths: Mapping[str, str | None]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for key, value in paths.items():
        if not value:
            continue
        path = Path(str(value))
        metadata[str(key)] = {
            "file_name": path.name,
            "exists": path.is_file(),
            "size": path.stat().st_size if path.is_file() else 0,
        }
    return metadata


def _iso_utc() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


__all__ = [
    "AvitoExportCheckpointResult",
    "checkpoint_path",
    "delete_export_checkpoint",
    "write_export_checkpoint",
]
