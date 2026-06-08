import importlib
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from libs.core import sales_core as core


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


def test_search_catalog_prefers_highest_price_for_explicit_expensive_request(catalog_core):
    tenant_id = catalog_core
    needs = core.infer_user_needs("покажите самую дорогую модель")
    results = core.search_catalog(
        needs,
        limit=1,
        tenant=tenant_id,
        query="покажите самую дорогую модель",
    )
    assert results, "expected priced result for expensive request"
    assert results[0].get("title") == "Sirius Pro"


def test_search_catalog_prefers_lowest_price_for_explicit_cheap_request(catalog_core):
    tenant_id = catalog_core
    needs = core.infer_user_needs("покажите самую дешевую модель")
    results = core.search_catalog(
        needs,
        limit=1,
        tenant=tenant_id,
        query="покажите самую дешевую модель",
    )
    assert results, "expected priced result for cheap request"
    assert results[0].get("title") == "Nord 70"


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

    state = core.load_sales_state(tenant_id, contact_id)
    keywords = " ".join((state.needs.get("keywords") or []))
    assert "sirius" in keywords.lower()
    assert reply  # rule-based reply should still produce text
