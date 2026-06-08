from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.services import avito_dialog_dataset_writer
from libs.core.services.avito_dialog_filter import AvitoDialogMessage


pytestmark = pytest.mark.unit


def test_dialog_dataset_writer_writes_one_sanitized_row_per_dialog(tmp_path: Path) -> None:
    dialogs = [
        [
            AvitoDialogMessage(role="system", text="Системное сообщение", timestamp=None),
            AvitoDialogMessage(role="client", text="Здравствуйте, мой номер 89876666133", timestamp=None),
            AvitoDialogMessage(role="manager", text="Напишите @manager или на test@example.com", timestamp=None),
        ],
        [
            AvitoDialogMessage(role="client", text="Сайт https://example.test", timestamp=None),
            AvitoDialogMessage(role="manager", text="Ответили в WhatsApp", timestamp=None),
        ],
    ]

    result = avito_dialog_dataset_writer.write_dialog_dataset_export(
        tenant_id=101,
        job_id="job-1",
        dialogs=dialogs,
        domain_schema_id="schema-1",
        export_root=tmp_path,
    )

    assert result.dialog_dataset_count == 2
    assert result.dialog_dataset_file_path
    path = Path(result.dialog_dataset_file_path)
    assert path.parent == tmp_path / "101" / "uploads" / "dialogs"
    assert path.name.startswith("dialog_dataset_2_")
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert rows[0]["schema_version"] == "avito_dialog_dataset_v1"
    assert rows[0]["quality"]["format"] == "dialog_level_v1"
    assert rows[0]["domain_schema_id"] == "schema-1"
    assert rows[0]["dialog"][0]["role"] == "client"
    text = json.dumps(rows, ensure_ascii=False)
    assert "Системное сообщение" not in text
    assert "89876666133" not in text
    assert "test@example.com" not in text
    assert "https://example.test" not in text
    assert "@manager" not in text
    assert "[PHONE]" in text
    assert "[EMAIL]" in text
    assert "[LINK]" in text
    assert "[HANDLE]" in text
    assert "chat_id" not in text
    assert "account_id" not in text


def test_dialog_dataset_writer_stable_dialog_id(tmp_path: Path) -> None:
    dialog = [
        AvitoDialogMessage(role="client", text="Здравствуйте", timestamp=None),
        AvitoDialogMessage(role="manager", text="Здравствуйте, чем помочь?", timestamp=None),
    ]

    first = avito_dialog_dataset_writer.build_dialog_dataset_rows(tenant_id=1, dialogs=[dialog])
    second = avito_dialog_dataset_writer.build_dialog_dataset_rows(tenant_id=1, dialogs=[dialog])

    assert first[0]["dialog_id"] == second[0]["dialog_id"]
