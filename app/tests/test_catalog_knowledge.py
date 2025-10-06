import importlib
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
INNER = ROOT / "app"
for candidate in (ROOT, INNER):
    value = str(candidate)
    if value not in sys.path:
        sys.path.append(value)

import core


@pytest.fixture()
def catalog_core(monkeypatch, tmp_path):
    tenants_dir = tmp_path / "tenants"
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))
    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    importlib.reload(core)

    tenant_id = 5
    core.ensure_tenant_files(tenant_id)
    cfg = core.load_tenant(tenant_id)
    cfg["catalogs"] = [
        {
            "name": "custom",
            "path": "uploads/catalog.csv",
            "type": "csv",
            "delimiter": ",",
            "encoding": "utf-8",
        }
    ]
    core.write_tenant_config(tenant_id, cfg)

    uploads = core.tenant_dir(tenant_id) / "uploads"
    uploads.mkdir(parents=True, exist_ok=True)
    catalog_path = uploads / "catalog.csv"
    catalog_path.write_text(
        "title,price,brand,color,tags\n"
        "Milano 10,12500,Verda,белый,хит\n"
        "Sirius Pro,21500,Ultra,черный,новинка\n"
        "Nord 70,9900,Volga,венге,склад\n",
        encoding="utf-8",
    )

    try:
        yield tenant_id
    finally:
        importlib.reload(core)


def test_search_catalog_ranks_by_keywords(catalog_core):
    tenant_id = catalog_core
    needs = {"type": "освещение"}
    results = core.search_catalog(
        needs,
        limit=2,
        tenant=tenant_id,
        query="Ищу модель Milano 10, желательно белую",
    )

    assert results, "expected catalog search to return items"
    assert results[0].get("title") == "Milano 10"


@pytest.mark.anyio
async def test_build_llm_messages_embed_catalog_context(catalog_core):
    tenant_id = catalog_core
    contact_id = 77
    core.reset_sales_state(tenant_id, contact_id)

    messages = await core.build_llm_messages(
        contact_id,
        "Расскажите подробнее про Sirius Pro",
        channel="whatsapp",
        tenant=tenant_id,
    )

    system_text = messages[0]["content"]
    assert "Релевантные позиции каталога" in system_text
    assert "Sirius Pro" in system_text


def test_rule_based_reply_mentions_requested_item(catalog_core):
    tenant_id = catalog_core
    contact_id = 99
    core.reset_sales_state(tenant_id, contact_id)

    reply = core.make_rule_based_reply(
        "Нужна модель Sirius Pro с быстрой поставкой",
        "whatsapp",
        contact_id,
        tenant=tenant_id,
    )

    assert "Sirius Pro" in reply
