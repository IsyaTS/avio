import importlib

import pytest
from starlette_ext.requests import Request
from starlette_ext.testclient import TestClient


@pytest.fixture()
def sandbox(monkeypatch, tmp_path):
    tenants_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))
    monkeypatch.delenv("APP_INTERNAL_URL", raising=False)
    monkeypatch.delenv("APP_PUBLIC_URL", raising=False)

    from app import core as core_module
    from app import main as main_module

    importlib.reload(core_module)
    import sys
    sys.modules["core"] = core_module
    importlib.reload(main_module)

    yield core_module, main_module

    importlib.reload(core_module)
    importlib.reload(main_module)


def test_resolve_catalog_attachment_uses_request_url(sandbox):
    core, main = sandbox
    tenant = 2
    core.ensure_tenant_files(tenant)

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    (uploads / "catalog.pdf").write_bytes(b"%PDF-1.4\n")

    cfg = core.read_tenant_config(tenant)
    integrations = cfg.setdefault("integrations", {})
    integrations["uploaded_catalog"] = {
        "path": "uploads/catalog.pdf",
        "original": "catalog.pdf",
        "type": "pdf",
        "mime": "application/pdf",
    }
    core.write_tenant_config(tenant, cfg)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/webhook",
        "query_string": b"",
        "headers": [],
        "scheme": "http",
        "server": ("testserver", 80),
        "root_path": "",
        "app": main.app,
        "router": main.app.router,
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    request = Request(scope, receive)

    attachment, caption = main._resolve_catalog_attachment(cfg, tenant, request)

    assert attachment is not None
    assert attachment["url"].startswith("http://testserver/internal/tenant/2/catalog-file")
    assert caption.startswith("Каталог в PDF")


def test_read_catalog_handles_cp1251_when_marked_utf8(sandbox):
    core, _ = sandbox
    tenant = 3
    core.ensure_tenant_files(tenant)

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    path = uploads / "catalog.csv"
    path.write_bytes("sku;name\nA1;Товар".encode("cp1251"))

    cfg = core.read_tenant_config(tenant)
    cfg["catalogs"] = [
        {
            "name": "uploaded",
            "path": "uploads/catalog.csv",
            "type": "csv",
            "encoding": "utf-8",
            "delimiter": ";",
        }
    ]
    core.write_tenant_config(tenant, cfg)

    items = core._read_catalog(tenant)
    assert items
    assert items[0].get("name") == "Товар"


def test_internal_catalog_file_uses_original_name_and_normalizes_path(sandbox):
    core, main = sandbox
    tenant = 6
    core.ensure_tenant_files(tenant)

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    pdf_path = uploads / "catalog.pdf"
    payload = b"%PDF-1.4 real binary"
    pdf_path.write_bytes(payload)

    cfg = core.read_tenant_config(tenant)
    cfg.setdefault("integrations", {})["uploaded_catalog"] = {
        "path": "uploads\\catalog.pdf",
        "original": "catalog-original.pdf",
        "type": "pdf",
        "mime": "application/pdf",
    }
    core.write_tenant_config(tenant, cfg)

    client = TestClient(main.app)
    response = client.get(f"/internal/tenant/{tenant}/catalog-file", params={"path": "uploads\\catalog.pdf"})

    assert response.status_code == 200
    assert response.content == payload
    disposition = response.headers.get("content-disposition", "")
    assert "catalog-original.pdf" in disposition


def test_read_catalog_missing_custom_returns_empty(sandbox):
    core, _ = sandbox
    tenant = 4
    core.ensure_tenant_files(tenant)

    cfg = core.read_tenant_config(tenant)
    cfg["catalogs"] = [
        {
            "name": "uploaded",
            "path": "uploads/not-there.csv",
            "type": "csv",
            "encoding": "utf-8",
        }
    ]
    core.write_tenant_config(tenant, cfg)

    items = core._read_catalog(tenant)
    assert items == []


def test_read_catalog_auto_maps_russian_headers(sandbox):
    core, _ = sandbox
    tenant = 5
    core.ensure_tenant_files(tenant)

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    path = uploads / "catalog.csv"
    path.write_text(
        "Артикул;Наименование;Цена, руб.\nSKU-1;Стальная полка;25000 ₽\n",
        encoding="utf-8",
    )

    cfg = core.read_tenant_config(tenant)
    cfg["catalogs"] = [
        {
            "name": "uploaded",
            "path": "uploads/catalog.csv",
            "type": "csv",
            "encoding": "utf-8",
            "delimiter": ";",
        }
    ]
    core.write_tenant_config(tenant, cfg)

    items = core._read_catalog(tenant)
    assert items
    first = items[0]
    assert first.get("title") == "Стальная полка"
    assert first.get("name") == "Стальная полка"
    assert first.get("price") == "25000 ₽"
    assert first.get("sku") == "SKU-1"

    pages = core.paginate_catalog_text(items, cfg, page_size=1)
    assert "Стальная полка" in pages[0]
    assert "25 000" in pages[0]
