from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.services.avito_training_export_writer import write_training_examples_export


pytestmark = pytest.mark.unit


def _example(index: int = 1) -> dict[str, object]:
    return {
        "source": "avito",
        "tenant_id": 101,
        "dialog_id": "dialog-hash",
        "example_id": f"dialog-hash_{index:04d}",
        "channel": "avito",
        "context": [{"role": "client", "text": "Нужна дверь"}],
        "ideal_reply": {"role": "manager", "text": "Ответ"},
        "quality": {"accepted": True, "source": "manager_dialog", "filter_version": "avito_dialog_filter_v1"},
        "created_at": "2026-05-22T00:00:00Z",
    }


def test_writes_valid_jsonl(tmp_path: Path) -> None:
    result = write_training_examples_export(
        tenant_id=101,
        job_id="job-1",
        examples=[_example(1), _example(2)],
        export_root=tmp_path,
    )

    assert result.training_examples_count == 2
    assert result.training_file_path
    path = Path(result.training_file_path)
    assert path.parent == tmp_path / "101" / "uploads" / "dialogs"
    assert path.name.startswith("training_examples_2_")
    assert path.suffix == ".jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert rows[0]["example_id"] == "dialog-hash_0001"
    assert result.training_file_size == path.stat().st_size


def test_does_not_write_empty_file(tmp_path: Path) -> None:
    result = write_training_examples_export(
        tenant_id=101,
        job_id="job-1",
        examples=[],
        export_root=tmp_path,
    )

    assert result.training_file_path is None
    assert result.training_file_size == 0
    assert result.training_examples_count == 0
    assert not list(tmp_path.rglob("*.jsonl"))


def test_jsonl_does_not_include_raw_metadata(tmp_path: Path) -> None:
    example = {
        **_example(1),
        "context": [{"role": "client", "text": "Нужна дверь"}],
        "ideal_reply": {"role": "manager", "text": "Ответ"},
    }

    result = write_training_examples_export(
        tenant_id=101,
        job_id="job-1",
        examples=[example],
        export_root=tmp_path,
    )

    text = Path(result.training_file_path or "").read_text(encoding="utf-8")
    assert "chat_id" not in text
    assert "account_id" not in text
    assert "item_id" not in text
