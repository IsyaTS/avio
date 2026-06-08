from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from libs.core.services.avito_contextual_case_builder import mask_contacts
from libs.core.services.avito_dialog_filter import AvitoDialogMessage


SCHEMA_VERSION = "avito_dialog_dataset_v1"
PIPELINE_VERSION = "dialog_level_v1"


@dataclass(frozen=True)
class AvitoDialogDatasetWriteResult:
    dialog_dataset_file_path: str | None
    dialog_dataset_file_size: int
    dialog_dataset_count: int


@dataclass(frozen=True)
class AvitoDialogArtifactWriteResult:
    file_path: str | None
    file_size: int


def write_dialog_dataset_export(
    *,
    tenant_id: int,
    job_id: str,
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    domain_schema_id: str | None = None,
    export_root: str | Path | None = None,
    created_at: datetime | None = None,
) -> AvitoDialogDatasetWriteResult:
    rows = build_dialog_dataset_rows(
        tenant_id=int(tenant_id),
        dialogs=dialogs,
        domain_schema_id=domain_schema_id,
        created_at=created_at,
    )
    if not rows:
        return AvitoDialogDatasetWriteResult(
            dialog_dataset_file_path=None,
            dialog_dataset_file_size=0,
            dialog_dataset_count=0,
        )

    directory = _dialogs_directory(tenant_id, export_root=export_root)
    path = directory / f"dialog_dataset_{len(rows)}_{_timestamp()}_{_safe_job_id(job_id)}.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    return AvitoDialogDatasetWriteResult(
        dialog_dataset_file_path=str(path),
        dialog_dataset_file_size=path.stat().st_size,
        dialog_dataset_count=len(rows),
    )


def build_dialog_dataset_rows(
    *,
    tenant_id: int,
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    domain_schema_id: str | None = None,
    created_at: datetime | None = None,
) -> list[dict[str, Any]]:
    created = _iso_utc(created_at)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for dialog in dialogs:
        messages = _prepare_dialog(dialog)
        if not messages:
            continue
        dialog_id = _stable_dialog_id(messages)
        if dialog_id in seen:
            continue
        seen.add(dialog_id)
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "source": "avito",
                "tenant_id": int(tenant_id),
                "dialog_id": dialog_id,
                "channel": "avito",
                "domain_schema_id": str(domain_schema_id or "") or None,
                "dialog": messages,
                "quality": {
                    "status": "accepted",
                    "source": "manager_dialog",
                    "filter_version": "avito_dialog_filter_v1",
                    "format": PIPELINE_VERSION,
                },
                "created_at": created,
            }
        )
    return rows


def write_json_artifact(
    *,
    tenant_id: int,
    job_id: str,
    prefix: str,
    data: Mapping[str, Any],
    export_root: str | Path | None = None,
) -> AvitoDialogArtifactWriteResult:
    payload = _sanitize_json_artifact(data)
    if not payload:
        return AvitoDialogArtifactWriteResult(file_path=None, file_size=0)
    directory = _dialogs_directory(tenant_id, export_root=export_root)
    path = directory / f"{prefix}_{_timestamp()}_{_safe_job_id(job_id)}.json"
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    return AvitoDialogArtifactWriteResult(file_path=str(path), file_size=path.stat().st_size)


def _prepare_dialog(dialog: Sequence[AvitoDialogMessage]) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    for message in dialog:
        role = str(getattr(message, "role", "") or "").strip().lower()
        if role not in {"client", "manager"}:
            continue
        text = mask_contacts(getattr(message, "text", ""))
        if text:
            messages.append({"role": role, "text": text})
    return messages


def _stable_dialog_id(messages: Sequence[Mapping[str, str]]) -> str:
    import hashlib

    signature = "\n".join(f"{item.get('role')}:{item.get('text')}" for item in messages)
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def _dialogs_directory(tenant_id: int, *, export_root: str | Path | None = None) -> Path:
    root = Path(export_root or "/data/tenants")
    directory = root / str(int(tenant_id)) / "uploads" / "dialogs"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _safe_job_id(job_id: str) -> str:
    return "".join(ch for ch in str(job_id) if ch.isalnum() or ch in {"-", "_"})[:80]


def _timestamp() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")


def _iso_utc(value: datetime | None) -> str:
    dt = value or datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sanitize_json_artifact(value: Mapping[str, Any]) -> dict[str, Any]:
    sanitized = _sanitize_value(value)
    text = json.dumps(sanitized, ensure_ascii=False)
    if any(marker in text for marker in ("Клиент:", "Менеджер:", "client:", "manager:")):
        return {}
    return sanitized if isinstance(sanitized, dict) else {}


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, str):
        return " ".join(value.replace("\r", " ").replace("\n", " ").split())[:500]
    if isinstance(value, (int, float, bool, type(None))):
        return value
    return None


__all__ = [
    "PIPELINE_VERSION",
    "SCHEMA_VERSION",
    "AvitoDialogArtifactWriteResult",
    "AvitoDialogDatasetWriteResult",
    "build_dialog_dataset_rows",
    "write_dialog_dataset_export",
    "write_json_artifact",
]
