from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.services import avito_export_checkpoint


pytestmark = pytest.mark.unit


def test_export_checkpoint_writes_atomically_and_updates_stage(tmp_path: Path) -> None:
    result = avito_export_checkpoint.write_export_checkpoint(
        tenant_id=101,
        job_id="job-1",
        target_dialogs=5000,
        accepted_dialogs_count=100,
        candidates_seen=120,
        stage="scanning",
        export_root=tmp_path,
    )

    path = Path(result.checkpoint_path)
    assert path == tmp_path / "101" / "uploads" / "dialogs" / "checkpoints" / "job-1_checkpoint.json"
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["stage"] == "scanning"
    assert data["accepted_dialogs_count"] == 100
    assert "raw" not in json.dumps(data).lower()

    updated = avito_export_checkpoint.write_export_checkpoint(
        tenant_id=101,
        job_id="job-1",
        target_dialogs=5000,
        accepted_dialogs_count=500,
        candidates_seen=620,
        stage="writing_dialog_dataset",
        artifact_paths={"dialog_dataset": str(tmp_path / "dataset.jsonl")},
        domain_schema_ready=True,
        dataset_rows_written=500,
        export_root=tmp_path,
    )

    updated_data = json.loads(Path(updated.checkpoint_path).read_text(encoding="utf-8"))
    assert updated_data["stage"] == "writing_dialog_dataset"
    assert updated_data["dataset_rows_written"] == 500
    assert updated_data["artifact_paths"]["dialog_dataset"]["file_name"] == "dataset.jsonl"
    assert updated_data["domain_schema_ready"] is True
    assert "created_at" in updated_data
    assert "updated_at" in updated_data


def test_delete_export_checkpoint(tmp_path: Path) -> None:
    result = avito_export_checkpoint.write_export_checkpoint(
        tenant_id=1,
        job_id="job-1",
        target_dialogs=10,
        stage="scanning",
        export_root=tmp_path,
    )

    assert Path(result.checkpoint_path).exists()
    assert avito_export_checkpoint.delete_export_checkpoint(
        tenant_id=1,
        job_id="job-1",
        export_root=tmp_path,
    )
    assert not Path(result.checkpoint_path).exists()
