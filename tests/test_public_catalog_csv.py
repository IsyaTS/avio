from fastapi import FastAPI
from fastapi.testclient import TestClient

from apps.api.web import client as client_module
from apps.api.web import public as public_module


def test_detect_csv_delimiter_semicolon():
    assert public_module._detect_csv_delimiter("id;name;price") == ";"


def test_detect_csv_delimiter_comma():
    assert public_module._detect_csv_delimiter("id,name,price") == ","


def test_detect_csv_delimiter_tab():
    assert public_module._detect_csv_delimiter("id\tname\tprice") == "\t"


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(public_module.router)
    return TestClient(app)


def test_public_catalog_csv_get_returns_table(tmp_path, monkeypatch):
    sample = tmp_path / "catalog.csv"
    sample.write_text("name;price\nChair;100\nTable;200\n", encoding="utf-8")

    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: tenant == 1 and key == "secret")
    monkeypatch.setattr(public_module.C, "read_tenant_config", lambda tenant: {}, raising=False)
    monkeypatch.setattr(client_module, "_catalog_csv_path", lambda tenant, cfg=None: (sample, "utf-8", "catalog.csv"))

    client = _build_client()
    response = client.get("/pub/catalog/csv", params={"tenant": 1, "k": "secret"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["columns"] == ["name", "price"]
    assert payload["rows"] == [["Chair", "100"], ["Table", "200"]]
    assert payload["path"] == "catalog.csv"
    assert payload["delimiter"] == ";"

    client.close()


def test_public_catalog_csv_post_updates_file(tmp_path, monkeypatch):
    sample = tmp_path / "catalog.csv"
    sample.write_text("name;price\nOld;10\n", encoding="utf-8")

    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: tenant == 2 and key == "secret")
    monkeypatch.setattr(public_module.C, "read_tenant_config", lambda tenant: {}, raising=False)
    monkeypatch.setattr(client_module, "_catalog_csv_path", lambda tenant, cfg=None: (sample, "utf-8", "catalog.csv"))

    client = _build_client()

    post_payload = {
        "columns": ["name", "price"],
        "rows": [["New", "50"], {"name": "Chair", "price": 70}],
    }
    response = client.post("/pub/catalog/csv", params={"tenant": 2, "k": "secret"}, json=post_payload)
    assert response.status_code == 200
    assert response.json() == {"ok": True, "rows": 2}

    get_response = client.get("/pub/catalog/csv", params={"tenant": 2, "k": "secret"})
    assert get_response.status_code == 200
    data = get_response.json()
    assert data["rows"] == [["New", "50"], ["Chair", "70"]]

    client.close()


def test_public_catalog_csv_requires_valid_key(monkeypatch):
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: False)

    client = _build_client()
    response = client.get("/pub/catalog/csv", params={"tenant": 1, "k": "bad"})
    assert response.status_code == 401

    response_post = client.post("/pub/catalog/csv", params={"tenant": 1, "k": "bad"}, json={})
    assert response_post.status_code == 401

    client.close()


def test_public_catalog_csv_returns_404_when_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: True)
    monkeypatch.setattr(public_module.C, "read_tenant_config", lambda tenant: {}, raising=False)
    monkeypatch.setattr(client_module, "_catalog_csv_path", lambda tenant, cfg=None: (None, None, None))

    client = _build_client()
    response = client.get("/pub/catalog/csv", params={"tenant": 3, "k": "any"})
    assert response.status_code == 404
    assert response.json() == {"detail": "csv_not_ready"}

    response_post = client.post(
        "/pub/catalog/csv",
        params={"tenant": 3, "k": "any"},
        json={"columns": [], "rows": []},
    )
    assert response_post.status_code == 404
    assert response_post.json() == {"detail": "csv_not_ready"}

    client.close()


def test_public_catalog_csv_accepts_global_and_tenant_keys(tmp_path, monkeypatch):
    sample = tmp_path / "catalog.csv"
    sample.write_text("name;price\nChair;100\n", encoding="utf-8")

    monkeypatch.setattr(public_module.settings, "PUBLIC_KEY", "GLOBAL")
    config = {"passport": {"public_key": "TENANT_KEY"}}
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: dict(config))
    monkeypatch.setattr(public_module.common, "get_tenant_pubkey", lambda tenant: "")
    monkeypatch.setattr(
        public_module.common,
        "valid_key",
        lambda tenant, key: key in {"GLOBAL", "TENANT_KEY"},
    )
    monkeypatch.setattr(client_module, "_catalog_csv_path", lambda tenant, cfg=None: (sample, "utf-8", "catalog.csv"))

    client = _build_client()

    ok_global = client.get("/pub/catalog/csv", params={"tenant": 1, "k": "GLOBAL"})
    assert ok_global.status_code == 200

    ok_tenant = client.get("/pub/catalog/csv", params={"tenant": 1, "k": "TENANT_KEY"})
    assert ok_tenant.status_code == 200

    payload = {"columns": ["name", "price"], "rows": [["Desk", "150"]]}
    post_global = client.post("/pub/catalog/csv", params={"tenant": 1, "k": "GLOBAL"}, json=payload)
    assert post_global.status_code == 200

    post_tenant = client.post("/pub/catalog/csv", params={"tenant": 1, "k": "TENANT_KEY"}, json=payload)
    assert post_tenant.status_code == 200

    denied = client.get("/pub/catalog/csv", params={"tenant": 1, "k": "bad"})
    assert denied.status_code == 401

    client.close()


def test_catalog_csv_path_falls_back_to_catalogs_dir_when_metadata_missing(tmp_path, monkeypatch):
    tenant = 77
    tenant_root = tmp_path / str(tenant)
    catalogs_dir = tenant_root / "catalogs"
    catalogs_dir.mkdir(parents=True, exist_ok=True)
    sample = catalogs_dir / "catalog.csv"
    sample.write_text("name;price\nChair;100\n", encoding="utf-8")

    monkeypatch.setattr(client_module.C, "tenant_dir", lambda _tenant: str(tenant_root))
    monkeypatch.setattr(
        client_module.C,
        "read_tenant_config",
        lambda _tenant: {"passport": {"public_key": "k"}, "integrations": {}},
    )

    path, encoding, relative = client_module._catalog_csv_path(tenant)
    assert path == sample
    assert encoding == "utf-8-sig"
    assert relative == "catalogs/catalog.csv"


def test_catalog_csv_path_prefers_catalog_csv_over_other_discovered_files(tmp_path, monkeypatch):
    tenant = 88
    tenant_root = tmp_path / str(tenant)
    catalogs_dir = tenant_root / "catalogs"
    catalogs_dir.mkdir(parents=True, exist_ok=True)
    preferred = catalogs_dir / "catalog.csv"
    preferred.write_text("name;price\nPreferred;1\n", encoding="utf-8")
    (catalogs_dir / "zeta.csv").write_text("name;price\nOther;2\n", encoding="utf-8")

    monkeypatch.setattr(client_module.C, "tenant_dir", lambda _tenant: str(tenant_root))
    monkeypatch.setattr(client_module.C, "read_tenant_config", lambda _tenant: {"integrations": {}})

    path, encoding, relative = client_module._catalog_csv_path(tenant)
    assert path == preferred
    assert encoding == "utf-8-sig"
    assert relative == "catalogs/catalog.csv"
