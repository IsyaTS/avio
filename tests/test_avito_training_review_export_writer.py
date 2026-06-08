from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.services import avito_training_review_export_writer as writer


pytestmark = pytest.mark.unit


def test_writes_valid_review_jsonl(tmp_path: Path) -> None:
    result = writer.write_review_examples_export(
        tenant_id=101,
        job_id="job-1",
        export_root=tmp_path,
        examples=[{"source": "avito", "context": [{"role": "client", "text": "x"}]}],
    )

    path = Path(result.review_file_path or "")
    assert path.parent == tmp_path / "101" / "uploads" / "dialogs"
    assert path.name.startswith("review_examples_1_")
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == result.review_examples_count == 1


def test_empty_review_does_not_write_file(tmp_path: Path) -> None:
    result = writer.write_review_examples_export(tenant_id=1, job_id="job", export_root=tmp_path, examples=[])
    assert result.review_file_path is None
    assert result.review_file_size == 0
