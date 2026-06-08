from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.services import avito_training_reject_summary_writer as writer


pytestmark = pytest.mark.unit


def test_writes_summary_without_raw_examples(tmp_path: Path) -> None:
    result = writer.write_reject_summary_export(
        tenant_id=101,
        job_id="job-1",
        export_root=tmp_path,
        summary={
            "rejected_examples_count": 2,
            "hard_reject_reasons": {"contact_only_reply": 1},
            "raw": {"secret": object()},
        },
    )

    path = Path(result.summary_file_path or "")
    assert path.parent == tmp_path / "101" / "uploads" / "dialogs"
    assert path.name.startswith("rejected_examples_summary_")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["rejected_examples_count"] == 2
    assert "secret" not in str(data)
