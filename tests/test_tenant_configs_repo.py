import logging
from types import SimpleNamespace

import pytest

from libs.core.repo import tenant_configs


pytestmark = pytest.mark.unit


def test_tenant_config_db_failure_uses_short_circuit(monkeypatch, caplog):
    calls = {"count": 0}

    monkeypatch.setenv("TENANT_CONFIG_DB_ENABLED", "1")
    monkeypatch.setattr(tenant_configs, "_UNAVAILABLE_UNTIL", 0.0)
    monkeypatch.setattr(tenant_configs, "_LAST_FAILURE_LOG_AT", 0.0)
    monkeypatch.setattr(tenant_configs, "_UNAVAILABLE_TTL_SECONDS", 30.0)
    monkeypatch.setattr(tenant_configs, "_FAILURE_LOG_INTERVAL_SECONDS", 300.0)

    def _fail_connect(*_args, **_kwargs):
        calls["count"] += 1
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(tenant_configs, "psycopg", SimpleNamespace(connect=_fail_connect))
    monkeypatch.setattr(tenant_configs, "_database_url", lambda: "postgresql://user:pass@postgres/db")

    with caplog.at_level(logging.WARNING):
        assert tenant_configs.get(101) is None
        assert tenant_configs.get(101) is None

    assert calls["count"] == 1
    assert caplog.text.count("tenant_config_db_read_failed tenant=101") == 1
