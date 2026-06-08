from __future__ import annotations

import pathlib
from typing import Any, Mapping

import pytest

from apps.worker.services import attachment_runtime


pytestmark = pytest.mark.unit


def _deps() -> attachment_runtime.AttachmentRuntimeDeps:
    def compress(data: bytes, _filename: str, _target_bytes: int) -> bytes:
        return data[: max(1, len(data) // 2)]

    async def download_internal(_url: str) -> tuple[None, None, str]:
        return None, None, ""

    return attachment_runtime.AttachmentRuntimeDeps(
        tg_pdf_fast_enabled=True,
        tg_pdf_fast_min_mb=0.000001,
        tg_pdf_fast_target_mb=0.000001,
        tg_pdf_fast_suffix=".fast.pdf",
        is_internal_path_fn=lambda _value: False,
        normalize_internal_urls_fn=lambda value: (value, value),
        download_internal_attachment_fn=download_internal,
        resolve_attachment_filename_fn=lambda *_args: "catalog.pdf",
        resolve_attachment_mime_fn=lambda *_args: "application/pdf",
        compress_pdf_bytes_fn=compress,
    )


def test_prepare_tg_attachments_for_send_skips_non_mapping_items() -> None:
    result = attachment_runtime.prepare_tg_attachments_for_send(
        1,
        [{"filename": "note.txt"}, "bad", {"name": "image.png"}],
        deps=_deps(),
    )

    assert result == [{"filename": "note.txt"}, {"name": "image.png"}]


def test_iter_tenants_with_catalog_pdf_finds_numeric_tenants(tmp_path: pathlib.Path) -> None:
    (tmp_path / "1" / "uploads").mkdir(parents=True)
    (tmp_path / "1" / "uploads" / "catalog.pdf").write_bytes(b"pdf")
    (tmp_path / "abc" / "uploads").mkdir(parents=True)
    (tmp_path / "abc" / "uploads" / "catalog.pdf").write_bytes(b"pdf")

    rows = attachment_runtime.iter_tenants_with_catalog_pdf(tmp_path)

    assert rows == [(1, tmp_path / "1" / "uploads" / "catalog.pdf")]


def test_warmup_single_tg_fast_pdf_logs_compressed_cache(tmp_path: pathlib.Path) -> None:
    logs: list[str] = []
    catalog = tmp_path / "catalog.pdf"
    catalog.write_bytes(b"x" * 100)

    ok = attachment_runtime.warmup_single_tg_fast_pdf(
        1,
        catalog,
        deps=_deps(),
        log_fn=logs.append,
    )

    assert ok is True
    assert (tmp_path / "catalog.fast.pdf").is_file()
    assert any("event=tg_pdf_fast_warmup_done" in item for item in logs)


@pytest.mark.anyio
async def test_warmup_tg_fast_pdf_cache_once_runs_catalogs(tmp_path: pathlib.Path) -> None:
    calls: list[tuple[Any, ...]] = []
    logs: list[str] = []
    (tmp_path / "3" / "uploads").mkdir(parents=True)
    (tmp_path / "3" / "uploads" / "catalog.pdf").write_bytes(b"x" * 100)

    async def to_thread(fn: Any, *args: Any, **kwargs: Mapping[str, Any]) -> Any:
        calls.append(args)
        return fn(*args, **kwargs)

    async def sleep(_seconds: float) -> None:
        return None

    await attachment_runtime.warmup_tg_fast_pdf_cache_once(
        enabled=True,
        warmup_enabled=True,
        delay_seconds=0,
        tenants_root=tmp_path,
        deps=_deps(),
        log_fn=logs.append,
        sleep_fn=sleep,
        to_thread_fn=to_thread,
    )

    assert calls and calls[0][0] == 3
    assert logs[0] == "event=tg_pdf_fast_warmup_start catalogs=1"
    assert logs[-1] == "event=tg_pdf_fast_warmup_finish"
