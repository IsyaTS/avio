import importlib

import pytest

from apps.api.web import common as common_module
from libs.core import sales_core as core  # type: ignore[attr-defined]


pytestmark = pytest.mark.unit


def _reload_core():
    importlib.reload(core)


def test_tenant_waweb_url_from_config(monkeypatch):
    _reload_core()
    monkeypatch.setitem(
        core._TENANTS_CONFIG_CACHE,  # type: ignore[attr-defined]
        1234,
        {"waweb": {"host": "waweb-custom", "port": 9105}},
    )

    url = core.tenant_waweb_url(1234)
    assert url == "http://waweb-custom:9105"


def test_tenant_waweb_url_default(monkeypatch):
    _reload_core()
    monkeypatch.setitem(
        core._TENANTS_CONFIG_CACHE,  # type: ignore[attr-defined]
        9999,
        {"waweb": {"host": "", "port": ""}},
    )

    url = core.tenant_waweb_url(42)
    assert url == "http://waweb-42:9001"


def test_wa_base_url_prefers_tenant(monkeypatch):
    monkeypatch.setattr(
        common_module,
        "tenant_waweb_url",
        lambda tenant: f"http://custom-{tenant}:9100",
        raising=False,
    )

    url = common_module.wa_base_url(7)
    assert url == "http://custom-7:9100"


def test_tenant_whatsapp_provider_reads_tenant_config(monkeypatch):
    _reload_core()
    tenant_id = 9100

    core._TENANTS_CONFIG_CACHE.pop(tenant_id, None)  # type: ignore[attr-defined]
    core._TENANT_CONFIG_CACHE.pop(tenant_id, None)  # type: ignore[attr-defined]

    cfg = core.read_tenant_config(tenant_id)
    whatsapp_cfg = cfg.get("whatsapp")
    if not isinstance(whatsapp_cfg, dict):
        whatsapp_cfg = {}
    assert whatsapp_cfg.get("provider") == "baileys"

    assert core.tenant_whatsapp_provider(tenant_id) == "baileys"
