import importlib
import json
import os
import sys
import time
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _install_sklearn_stub() -> None:
    """Provide a minimal sklearn stub so app imports succeed in test env."""

    if "sklearn" in sys.modules:
        return

    sklearn_mod = types.ModuleType("sklearn")
    feature_mod = types.ModuleType("sklearn.feature_extraction")
    text_mod = types.ModuleType("sklearn.feature_extraction.text")

    class _Vectorizer:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def fit(self, *args, **kwargs):  # pragma: no cover - simple stub
            return self

        def fit_transform(self, *args, **kwargs):  # pragma: no cover - simple stub
            return []

    text_mod.TfidfVectorizer = _Vectorizer
    feature_mod.text = text_mod
    sklearn_mod.feature_extraction = feature_mod

    sys.modules["sklearn"] = sklearn_mod
    sys.modules["sklearn.feature_extraction"] = feature_mod
    sys.modules["sklearn.feature_extraction.text"] = text_mod


@pytest.fixture()
def api_client(tmp_path, monkeypatch):
    tenants_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))

    _install_sklearn_stub()

    import core
    import app.main
    import app.web.common as web_common
    import app.web.client as web_client
    import app.web.public as web_public

    importlib.reload(core)
    importlib.reload(web_common)
    importlib.reload(web_client)
    importlib.reload(web_public)
    importlib.reload(app.main)

    monkeypatch.setattr(web_client.C, "valid_key", lambda tenant, key: key == "secret")
    client = TestClient(app.main.app)
    try:
        yield client
    finally:
        client.close()


def _wait_for_status(path: Path, *, timeout: float = 3.0) -> dict:
    deadline = time.time() + timeout
    last_payload: dict | None = None
    while time.time() < deadline:
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                time.sleep(0.05)
                continue
            state = (payload.get("state") or "").lower()
            if state in {"done", "failed"}:
                return payload
            last_payload = payload
        time.sleep(0.05)
    if last_payload is not None:
        return last_payload
    raise AssertionError(f"status file {path} not created")


def test_public_upload_ignores_legacy_catalog_entries(api_client):
    tenants_dir = Path(os.getenv("TENANTS_DIR", ""))
    tenant_root = tenants_dir / "1"
    uploads_dir = tenant_root / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    legacy_config = {
        "passport": {"tenant_id": 1, "public_key": "secret"},
        "catalogs": ["legacy_entry", {"name": "prev", "path": "uploads/old.csv", "type": "csv"}],
    }
    tenant_root.mkdir(parents=True, exist_ok=True)
    (tenant_root / "tenant.json").write_text(json.dumps(legacy_config, ensure_ascii=False), encoding="utf-8")

    response = api_client.post(
        "/pub/catalog/upload?k=secret&tenant=1",
        files={"file": ("catalog.csv", "title,price\nProduct,100", "text/csv")},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    job_id = payload["job_id"]

    status_path = tenant_root / "catalog_jobs" / job_id / "status.json"
    status = _wait_for_status(status_path)
    assert status.get("state") == "done", status

    cfg = json.loads((tenant_root / "tenant.json").read_text(encoding="utf-8"))
    catalogs = cfg.get("catalogs")
    assert isinstance(catalogs, list)
    assert catalogs[0]["path"] == status["source_path"]
    assert all(isinstance(entry, dict) for entry in catalogs)
    assert any(entry.get("path") == "uploads/old.csv" for entry in catalogs)

    integrations = cfg.get("integrations", {})
    uploaded = integrations.get("uploaded_catalog")
    assert isinstance(uploaded, dict)
    assert uploaded.get("csv_path") == status.get("csv_path")


def test_public_upload_coerces_non_dict_integrations(api_client):
    tenants_dir = Path(os.getenv("TENANTS_DIR", ""))
    tenant_root = tenants_dir / "1"
    tenant_root.mkdir(parents=True, exist_ok=True)

    broken_cfg = {
        "passport": {"tenant_id": 1, "public_key": "secret"},
        "integrations": "",
    }
    (tenant_root / "tenant.json").write_text(json.dumps(broken_cfg, ensure_ascii=False), encoding="utf-8")

    response = api_client.post(
        "/pub/catalog/upload?k=secret&tenant=1",
        files={"file": ("catalog.csv", "title,price\nProduct,100", "text/csv")},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    job_id = payload["job_id"]

    status_path = tenant_root / "catalog_jobs" / job_id / "status.json"
    status = _wait_for_status(status_path)
    assert status.get("state") == "done", status

    cfg = json.loads((tenant_root / "tenant.json").read_text(encoding="utf-8"))
    integrations = cfg.get("integrations")
    assert isinstance(integrations, dict)
    uploaded = integrations.get("uploaded_catalog")
    assert isinstance(uploaded, dict)
    assert uploaded.get("csv_path") == status.get("csv_path")
