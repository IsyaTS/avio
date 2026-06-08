from __future__ import annotations

import pathlib
import tempfile

import pytest

from apps.worker.services import attachment_runtime


pytestmark = pytest.mark.unit


def _deps(**overrides):
    async def _download(_trimmed: str):
        return b"hello", {"content-type": "application/pdf"}, "https://internal/file.pdf"

    deps = dict(
        tg_pdf_fast_enabled=True,
        tg_pdf_fast_min_mb=0.000001,
        tg_pdf_fast_target_mb=0.000001,
        tg_pdf_fast_suffix=".fast.pdf",
        is_internal_path_fn=lambda value: str(value).startswith("/internal/"),
        normalize_internal_urls_fn=lambda value: (value, f"https://app{value}"),
        download_internal_attachment_fn=_download,
        resolve_attachment_filename_fn=lambda _prepared, _headers, _url: "file.pdf",
        resolve_attachment_mime_fn=lambda _prepared, _headers: "application/pdf",
        compress_pdf_bytes_fn=lambda data, _filename, _target: data[:1],
    )
    deps.update(overrides)
    return attachment_runtime.AttachmentRuntimeDeps(**deps)


@pytest.mark.anyio
async def test_prepare_internal_attachment_inlines_internal_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WA_INLINE_ATTACHMENT_LIMIT_MB", "8")
    prepared = await attachment_runtime.prepare_internal_attachment(
        {"url": "/internal/files/demo.pdf", "type": "document"},
        deps=_deps(),
    )

    assert prepared["url"] == "https://internal/file.pdf"
    assert prepared["filename"] == "file.pdf"
    assert prepared["mime"] == "application/pdf"
    assert prepared["b64"]


def test_prepare_tg_attachment_fast_pdf_uses_compressed_cache() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        src = pathlib.Path(tmpdir) / "catalog.pdf"
        src.write_bytes(b"x" * 1024)

        prepared = attachment_runtime.prepare_tg_attachment_fast_pdf(
            {"path": str(src), "mime": "application/pdf"},
            deps=_deps(),
        )

        assert prepared["path"].endswith(".fast.pdf")
        assert pathlib.Path(prepared["path"]).is_file()
        assert prepared["size"] == 1


def test_parse_disposition_filename_prefers_rfc5987_filename() -> None:
    assert (
        attachment_runtime.parse_disposition_filename(
            'attachment; filename="fallback.pdf"; filename*=UTF-8\'\'%D0%BA%D0%B0%D1%82%D0%B0%D0%BB%D0%BE%D0%B3.pdf'
        )
        == "каталог.pdf"
    )


def test_parse_disposition_filename_accepts_plain_filename() -> None:
    assert attachment_runtime.parse_disposition_filename('attachment; filename="catalog.pdf"') == "catalog.pdf"
