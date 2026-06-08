from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from libs.core.services.avito_dialog_filter import AvitoDialogMessage


@dataclass(frozen=True)
class AvitoDialogExportWriteResult:
    file_path: str
    file_size: int


def write_markdown_export(
    *,
    tenant_id: int,
    job_id: str,
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    export_root: str | Path | None = None,
) -> AvitoDialogExportWriteResult:
    root = Path(export_root or "/data/tenants")
    directory = root / str(int(tenant_id)) / "uploads" / "dialogs"
    directory.mkdir(parents=True, exist_ok=True)
    safe_job_id = "".join(ch for ch in str(job_id) if ch.isalnum() or ch in {"-", "_"})[:80]
    created_stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = directory / f"dialogs_{len(dialogs)}_{created_stamp}_{safe_job_id}.md"

    lines: list[str] = [
        "# Avito dialogs export",
        "",
        f"Tenant: {int(tenant_id)}",
        f"Dialogs: {len(dialogs)}",
        "",
    ]
    for index, dialog in enumerate(dialogs, start=1):
        lines.append(f"## Dialog {index}")
        for message in dialog:
            if message.role == "client":
                label = "Клиент"
            elif message.role == "manager":
                label = "Менеджер"
            else:
                continue
            text = _clean_export_text(message.text)
            if text:
                lines.append(f"{label}: {text}")
        lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return AvitoDialogExportWriteResult(file_path=str(path), file_size=path.stat().st_size)


def _clean_export_text(text: str) -> str:
    return " ".join(str(text or "").replace("\r", " ").replace("\n", " ").split()).strip()


__all__ = ["AvitoDialogExportWriteResult", "write_markdown_export"]
