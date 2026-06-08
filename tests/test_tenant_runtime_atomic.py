import json
from types import SimpleNamespace

import pytest

from libs.core import sales_core as core
from libs.core.sales_core import runtime_composition
from libs.core.sales_core import tenant_runtime
from libs.core.sales_core.tenant_runtime import TenantRuntime, TenantRuntimeDeps


pytestmark = pytest.mark.integration


def test_write_tenant_config_replace_failure_preserves_existing_file(tmp_path, monkeypatch):
    tenants_dir = tmp_path / "tenants"
    tenant_dir = tenants_dir / "1"
    tenant_dir.mkdir(parents=True)
    tenant_json = tenant_dir / "tenant.json"
    original = {
        "passport": {"tenant_id": 1, "public_key": "key"},
        "channels": {"whatsapp": {"enabled": True}},
        "behavior": {"avito_smart_reply_enabled": True},
    }
    tenant_json.write_text(json.dumps(original, ensure_ascii=False, indent=2), encoding="utf-8")

    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))

    def _fail_replace(src, dst):
        raise OSError("replace failed")

    monkeypatch.setattr(tenant_runtime.os, "replace", _fail_replace)

    with pytest.raises(OSError, match="replace failed"):
        core.write_tenant_config(
            1,
            {
                "passport": {"tenant_id": 1, "public_key": "key"},
                "channels": {"whatsapp": {"enabled": False}},
                "integrations": {"avito": {"access_token": "new"}},
            },
        )

    assert json.loads(tenant_json.read_text(encoding="utf-8")) == original
    assert not list(tenant_dir.glob(".tenant.json.*.tmp"))


def _runtime_with_db(tmp_path, store):
    tenants_dir = tmp_path / "tenants"

    def _db_get(tenant):
        item = store.get(int(tenant))
        if item is None:
            return None
        return json.loads(json.dumps(item[0])), float(item[1])

    def _db_upsert(tenant, cfg):
        marker = float(store.get(int(tenant), ({}, 0.0))[1]) + 1.0
        store[int(tenant)] = (json.loads(json.dumps(dict(cfg))), marker)
        return True

    return TenantRuntime(
        TenantRuntimeDeps(
            settings=type("Settings", (), {"PUBLIC_KEY": "pub", "WHATSAPP_PROVIDER_DEFAULT": "waweb"})(),
            logger=type("Logger", (), {"warning": lambda *a, **k: None})(),
            yaml_module=type("Yaml", (), {"safe_load": staticmethod(lambda fh: {})})(),
            root_dir=tmp_path,
            data_dir=tmp_path / "data",
            tenants_dir=tenants_dir,
            tenant_config_dir=tmp_path / "config",
            default_tenant_json={"passport": {}, "behavior": {}, "channels": {"whatsapp": {"enabled": True}}},
            default_persona_md="persona",
            persona_md_fallback="persona",
            tenant_config_cache={},
            tenant_persona_cache={},
            persona_hints_cache={},
            clear_persona_hints_cache=lambda tenant: None,
            coerce_bool=lambda value, default=False: bool(value) if value is not None else default,
            tenant_config_db_get=_db_get,
            tenant_config_db_upsert=_db_upsert,
        )
    )


def test_tenant_config_reads_db_when_json_file_is_missing(tmp_path):
    store = {
        3: (
            {
                "passport": {"tenant_id": 3, "public_key": "tenant-key"},
                "behavior": {"avito_smart_reply_enabled": True},
                "integrations": {"avito": {"access_token": "access", "refresh_token": "refresh"}},
                "follow_up": [{"text": "later"}],
            },
            10.0,
        )
    }
    runtime = _runtime_with_db(tmp_path, store)

    cfg = runtime.read_tenant_config(3)

    assert cfg["behavior"]["avito_smart_reply_enabled"] is True
    assert cfg["integrations"]["avito"]["access_token"] == "access"
    assert cfg["follow_up"] == [{"text": "later"}]


def test_tenant_config_write_survives_json_replace_failure_when_db_write_succeeds(tmp_path, monkeypatch):
    store = {}
    runtime = _runtime_with_db(tmp_path, store)
    runtime.ensure_tenant_files(1)

    def _fail_replace(src, dst):
        raise OSError("replace failed")

    monkeypatch.setattr(tenant_runtime.os, "replace", _fail_replace)

    runtime.write_tenant_config(
        1,
        {
            "passport": {"tenant_id": 1, "public_key": "key"},
            "behavior": {"avito_smart_reply_enabled": True},
            "integrations": {"avito": {"access_token": "access"}},
        },
    )

    assert store[1][0]["behavior"]["avito_smart_reply_enabled"] is True
    assert store[1][0]["integrations"]["avito"]["access_token"] == "access"


def test_tenant_runtime_deps_lazy_loads_tenant_config_repo(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_composition, "tenant_configs", None)
    ctx = {
        "yaml": SimpleNamespace(safe_load=lambda fh: {}),
        "ROOT_DIR": tmp_path,
        "DATA_DIR": tmp_path / "data",
        "TENANTS_DIR": tmp_path / "tenants",
        "TENANT_CONFIG_DIR": tmp_path / "config",
        "DEFAULT_TENANT_JSON": {"passport": {}, "behavior": {}, "channels": {}},
        "DEFAULT_PERSONA_MD": "persona",
        "_TENANT_CONFIG_CACHE": {},
        "_TENANT_PERSONA_CACHE": {},
        "_PERSONA_HINTS_CACHE": {},
        "_clear_persona_hints_cache": lambda tenant: None,
        "_coerce_bool": lambda value, default=False: bool(value) if value is not None else default,
    }

    deps = runtime_composition.build_tenant_runtime_deps(
        ctx,
        settings_obj=SimpleNamespace(PUBLIC_KEY="pub", WHATSAPP_PROVIDER_DEFAULT="waweb"),
        logger_obj=SimpleNamespace(warning=lambda *args, **kwargs: None),
    )

    assert deps.tenant_config_db_get is not None
    assert deps.tenant_config_db_upsert is not None
