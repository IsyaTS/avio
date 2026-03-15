from __future__ import annotations

from pathlib import Path

from apps.tgworker import manager as manager_module


def test_try_read_internal_attachment_reads_local_catalog(tmp_path: Path, monkeypatch) -> None:
    tenant = 101
    tenant_dir = tmp_path / str(tenant) / "uploads"
    tenant_dir.mkdir(parents=True, exist_ok=True)
    catalog = tenant_dir / "catalog.pdf"
    content = b"%PDF-1.4 test"
    catalog.write_bytes(content)
    monkeypatch.setattr(manager_module, "TENANTS_DIR", tmp_path)

    data, name, mime = manager_module._try_read_internal_attachment(
        "http://app:8000/internal/tenant/101/catalog-file?path=uploads/catalog.pdf&token=abc",
        tenant=tenant,
    )

    assert data == content
    assert name == "catalog.pdf"
    assert mime == "application/pdf"


def test_try_read_internal_attachment_rejects_path_escape(tmp_path: Path, monkeypatch) -> None:
    tenant = 101
    (tmp_path / str(tenant)).mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(manager_module, "TENANTS_DIR", tmp_path)

    data, name, mime = manager_module._try_read_internal_attachment(
        "http://app:8000/internal/tenant/101/catalog-file?path=../../etc/passwd&token=abc",
        tenant=tenant,
    )

    assert data is None
    assert name is None
    assert mime is None
