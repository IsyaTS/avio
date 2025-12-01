from __future__ import annotations

"""Catalog storage helpers."""

import csv
import pathlib
import re
from typing import Mapping, Sequence

from typing import MutableMapping

from .pipeline import finalize_catalog_rows
from libs.core import sales_core as core_module

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

    rows = [dict(row) for row in (normalized_rows or [])]
    preserve_page = bool(isinstance(meta, Mapping) and meta.get("preserve_page_column"))
    page_values: list[str] = []
    if preserve_page:
        page_values = [_stringify(row.get("page", "")) for row in rows]
    finalized_rows, header, report = finalize_catalog_rows(rows)
    if preserve_page:
        for idx, row in enumerate(finalized_rows):
            page_value = page_values[idx] if idx < len(page_values) else ""
            row["page"] = page_value
        if "page" not in header:
            header = header[:3] + ["page"] + header[3:]
            report.columns = header

    source_type = ""
    if isinstance(meta, Mapping):
        source_type = str(meta.get("type") or "").lower()

    filtered_rows: list[Mapping[str, object]] = []
    for row in finalized_rows:
        values = [_stringify(row.get(column, "")) for column in header if column != "id"]
        if not any(value.strip() for value in values):
            continue
        if source_type == "pdf":
            non_empty = [value.strip() for value in values if value.strip()]
            if len(non_empty) == 1 and non_empty[0] == ".":
                continue
        filtered_rows.append(row)

    finalized_rows = filtered_rows
    report.items = len(finalized_rows)

    writable_meta: MutableMapping[str, object] | None = meta if isinstance(meta, MutableMapping) else None
    target_encoding = "utf-8-sig"
    target_delimiter = ";"
    if isinstance(meta, Mapping):
        enc_candidate = str(meta.get("encoding") or "").strip()
        if enc_candidate:
            target_encoding = enc_candidate
        delim_candidate = meta.get("delimiter")
        if isinstance(delim_candidate, str) and delim_candidate:
            target_delimiter = delim_candidate
    if writable_meta is not None:
        writable_meta["encoding"] = target_encoding
        writable_meta["delimiter"] = target_delimiter
        writable_meta["pipeline"] = report.to_dict()
        writable_meta["items"] = report.items
        writable_meta["columns"] = list(header)

    with csv_path.open("w", encoding=target_encoding, newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=header,
            extrasaction="ignore",
            delimiter=target_delimiter,
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
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
