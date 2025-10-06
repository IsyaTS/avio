from __future__ import annotations

"""Catalog storage helpers."""

import csv
import pathlib
import re
from typing import Mapping, Sequence

from .pipeline import finalize_catalog_rows

# Import core in a way resilient to test import order and aliasing
try:  # Prefer package-qualified core
    from app import core as core_module
except Exception:  # Fallback to top-level alias if present
    import core as core_module  # type: ignore

__all__ = ["write_catalog_csv"]


def _stringify(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _sanitize_base_name(base_name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z._-]+", "_", base_name.strip())
    cleaned = cleaned.strip("._")
    return cleaned or "catalog"



def write_catalog_csv(
    tenant: int,
    normalized_rows: Sequence[Mapping[str, object]],
    base_name: str,
    meta: Mapping[str, object] | None = None,
) -> tuple[str, list[str]]:
    """Persist normalized catalog rows as a CSV file.

    The function creates `<tenant>/catalogs/` directory if it does not exist,
    writes the CSV file using UTF-8 encoding and returns the relative path to
    the stored CSV along with the ordered list of columns.
    """

    core_module.ensure_tenant_files(int(tenant))
    tenant_root = pathlib.Path(core_module.tenant_dir(int(tenant)))
    catalogs_dir = tenant_root / "catalogs"
    catalogs_dir.mkdir(parents=True, exist_ok=True)

    safe_base = _sanitize_base_name(base_name or "catalog")
    csv_path = catalogs_dir / f"{safe_base}.csv"

    rows = list(normalized_rows or [])
    finalized_rows, header, report = finalize_catalog_rows(rows)

    if isinstance(meta, dict):
        # NOTE: expose pipeline stats for status pages and logs
        meta.setdefault("pipeline", report.to_dict())
        meta.setdefault("items", report.items)
        meta.setdefault("columns", header)

    # Excel-friendly: UTF-8 with BOM and semicolon delimiter
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=header,
            extrasaction="ignore",
            delimiter=";",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for row in finalized_rows:
            # Flatten newlines/tabs to spaces to avoid broken rows in CSV viewers
            def _cell(val: object) -> str:
                text = _stringify(val)
                if not text:
                    return ""
                text = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
                # collapse runs of spaces
                text = re.sub(r"\s+", " ", text).strip()
                return text

            payload = {column: _cell(row.get(column, "")) for column in header}
            writer.writerow(payload)

    try:
        relative = str(csv_path.relative_to(tenant_root))
    except Exception:
        relative = str(csv_path)

    return relative, list(header)
