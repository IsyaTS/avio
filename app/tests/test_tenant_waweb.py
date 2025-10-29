import importlib


def test_tenant_waweb_url_from_config(monkeypatch):
    core = importlib.import_module("app.core")
    monkeypatch.setitem(
        core._TENANTS_CONFIG_CACHE,  # type: ignore[attr-defined]
        1234,
        {"waweb": {"host": "waweb-custom", "port": 9105}},
    )

    url = core.tenant_waweb_url(1234)
    assert url == "http://waweb-custom:9105"


def test_tenant_waweb_url_default(monkeypatch):
    core = importlib.import_module("app.core")
    monkeypatch.setitem(
        core._TENANTS_CONFIG_CACHE,  # type: ignore[attr-defined]
        9999,
        {"waweb": {"host": "", "port": ""}},
    )

    url = core.tenant_waweb_url(42)
    assert url == "http://waweb-42:9001"


def test_wa_base_url_prefers_tenant(monkeypatch):
    common = importlib.import_module("app.web.common")

    monkeypatch.setattr(
        common,
        "tenant_waweb_url",
        lambda tenant: f"http://custom-{tenant}:9100",
        raising=False,
    )

    url = common.wa_base_url(7)
    assert url == "http://custom-7:9100"
