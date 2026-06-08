from __future__ import annotations

from pathlib import Path

import pytest

from apps.tgworker import manager as tg_manager
from libs.core.lib.tg_slots import virtual_tenant_id


pytestmark = pytest.mark.unit


def test_resolve_local_attachment_path_accepts_virtual_tenant(monkeypatch, tmp_path: Path) -> None:
    tenants_root = tmp_path / "tenants"
    catalog_path = tenants_root / "101" / "uploads" / "catalog.pdf"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path.write_bytes(b"%PDF-1.4\nstub\n")

    monkeypatch.setattr(tg_manager, "TENANTS_DIR", tenants_root)
    virtual_tenant = virtual_tenant_id(101, 2)

    resolved = tg_manager._resolve_local_attachment_path(str(catalog_path), virtual_tenant)
    assert resolved == catalog_path.resolve()


def test_try_read_internal_attachment_accepts_virtual_tenant(monkeypatch, tmp_path: Path) -> None:
    tenants_root = tmp_path / "tenants"
    catalog_path = tenants_root / "101" / "uploads" / "catalog.pdf"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    expected_data = b"%PDF-1.4\nstub\n"
    catalog_path.write_bytes(expected_data)

    monkeypatch.setattr(tg_manager, "TENANTS_DIR", tenants_root)
    virtual_tenant = virtual_tenant_id(101, 3)

    data, filename, mime = tg_manager._try_read_internal_attachment(
        "http://app:8000/internal/tenant/101/catalog-file?path=uploads/catalog.pdf&token=test",
        virtual_tenant,
    )
    assert data == expected_data
    assert filename == "catalog.pdf"
    assert mime == "application/pdf"
