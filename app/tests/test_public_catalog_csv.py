from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.web import client as client_module
from app.web import public as public_module


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
