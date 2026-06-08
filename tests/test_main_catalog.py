import importlib

import pytest
from libs.core.starlette_ext.requests import Request
from libs.core.starlette_ext.testclient import TestClient


@pytest.fixture()
def sandbox(monkeypatch, tmp_path):
    tenants_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))
    monkeypatch.delenv("APP_INTERNAL_URL", raising=False)
    monkeypatch.delenv("APP_PUBLIC_URL", raising=False)
    monkeypatch.setenv("WEBHOOK_SECRET", "test-webhook-token")
    monkeypatch.setenv("ADMIN_TOKEN", "test-admin-token")
    monkeypatch.setenv("WA_WEB_TOKEN", "test-wa-token")

    from libs.core import sales_core as core_module
    from libs.core.services import catalog_flow as catalog_flow_module
    from apps.api import main as main_module

    importlib.reload(core_module)
    importlib.reload(catalog_flow_module)
    import sys
    sys.modules["core"] = core_module
    importlib.reload(main_module)

    yield core_module, main_module

    importlib.reload(core_module)
    importlib.reload(catalog_flow_module)
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
    assert attachment["url"].startswith(
        "http://testserver/internal/tenant/2/catalog-file?path=uploads/catalog.pdf"
    )
    assert "&token=" in attachment["url"]
    assert caption == ""


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
    headers = {"X-Webhook-Token": "test-webhook-token"}
    response = client.get(
        f"/internal/tenant/{tenant}/catalog-file",
        params={"path": "uploads\\catalog.pdf"},
        headers=headers,
    )

    assert response.status_code == 200
    assert response.content == payload
    disposition = response.headers.get("content-disposition", "")
    assert "catalog-original.pdf" in disposition

    head_response = client.head(
        f"/internal/tenant/{tenant}/catalog-file",
        params={"path": "uploads/catalog.pdf"},
        headers=headers,
    )

    assert head_response.status_code == 200
    assert head_response.headers.get("content-length") == str(len(payload))


def test_internal_catalog_file_requires_authorized_header(sandbox):
    core, main = sandbox
    tenant = 7
    core.ensure_tenant_files(tenant)

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    pdf_path = uploads / "catalog.pdf"
    pdf_path.write_bytes(b"sample")

    client = TestClient(main.app)
    url = f"/internal/tenant/{tenant}/catalog-file"
    params = {"path": "uploads/catalog.pdf"}
    internal_token = (
        getattr(main.C, "WA_INTERNAL_TOKEN", "")
        or getattr(main.C, "INTERNAL_SYNC_TOKEN", "")
        or getattr(main.C, "WEBHOOK_SECRET", "")
        or ""
    )

    assert client.head(url, params=params).status_code == 403

    assert (
        client.head(url, params=params, headers={"X-Webhook-Token": "test-webhook-token"}).status_code
        == 200
    )
    assert (
        client.head(url, params=params, headers={"X-Admin-Token": "test-admin-token"}).status_code
        == 200
    )
    assert (
        client.head(url, params=params, headers={"Authorization": "Bearer test-admin-token"}).status_code
        == 200
    )
    assert client.head(url, params=params, headers={"X-Auth-Token": internal_token}).status_code == 200
    assert client.head(url, params=params, headers={"X-Internal-Token": internal_token}).status_code == 200
    assert (
        client.head(
            url,
            params={"path": "uploads/catalog.pdf", "token": internal_token},
        ).status_code
        == 200
    )


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


def test_read_catalog_maps_object_type_from_russian_header(sandbox):
    core, _ = sandbox
    tenant = 9
    core.ensure_tenant_files(tenant)

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    path = uploads / "catalog.csv"
    path.write_text(
        "Наименование;Цена, руб.;Тип помещения\n"
        "VITRA;55900;частный дом\n"
        "LUXOR;39500;квартира\n",
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
    kinds = {str(item.get("title")): str(item.get("object_type")) for item in items}
    assert kinds.get("VITRA") == "house"
    assert kinds.get("LUXOR") == "apartment"


def test_read_catalog_respects_persona_csv_mapping(sandbox):
    core, _ = sandbox
    tenant = 8
    core.ensure_tenant_files(tenant)

    uploads = core.tenant_dir(tenant) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    path = uploads / "persona_catalog.csv"
    path.write_text(
        "Артикул;Название;Стоимость;Внутренний цвет\nSKU-9;«Уфа»;19990;Белый\n",
        encoding="utf-8",
    )

    persona_text = """
meta:
  csv_mapping:
    columns:
      title: ["Название"]
      price: ["Стоимость"]
      Цвет внутренней панели: ["Внутренний цвет"]
"""
    core.write_persona(tenant, persona_text)

    cfg = core.read_tenant_config(tenant)
    cfg["catalogs"] = [
        {
            "name": "uploaded",
            "path": "uploads/persona_catalog.csv",
            "type": "csv",
            "encoding": "utf-8",
            "delimiter": ";",
        }
    ]
    core.write_tenant_config(tenant, cfg)

    items = core._read_catalog(tenant)
    assert items
    row = items[0]
    assert row.get("title") == "«Уфа»"
    assert row.get("price") == "19990"
    assert row.get("color") == "Белый"


def test_read_catalog_persona_csv_without_delimiter_autodetects_comma(sandbox):
    core, _ = sandbox
    tenant = 9
    core.ensure_tenant_files(tenant)

    catalogs = core.tenant_dir(tenant) / "catalogs"
    catalogs.mkdir(parents=True, exist_ok=True)
    path = catalogs / "catalog.csv"
    path.write_text(
        "Название,Цена,Цвет\n"
        "ТЕРМО ЭВЕРЕСТ,39900,Бетон снежный\n"
        "ПРОТЕРМО,37400,Шоколад\n",
        encoding="utf-8",
    )

    core.write_persona(
        tenant,
        """
meta:
  catalog_csv_path: "catalogs/catalog.csv"
""",
    )

    items = core._read_catalog(tenant)
    assert items
    assert str(items[0].get("title") or "").strip() == "ТЕРМО ЭВЕРЕСТ"
    assert core._item_price_int(items[0]) == 39900
    assert core._item_price_int(items[1]) == 37400


def test_item_price_int_prefers_plausible_price_when_model_name_contains_digits(sandbox):
    core, _ = sandbox
    item = {
        "title": "3Д ТЕРМО СТЕКЛО",
        "price": '3Д ТЕРМО СТЕКЛО,66500,"1,5 ММ",110 ММ',
    }
    assert core._item_price_int(item) == 66500
