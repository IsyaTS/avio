from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main
from app.web import admin as admin_module
from app.web import common as common_module
import app.core as core_module


class DummyRedis:
    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    def get(self, key: str) -> str | None:
        return self.strings.get(key)

    def set(self, key: str, value: str) -> bool:
        self.strings[key] = value
        return True

    def delete(self, key: str) -> int:
        removed = 0
        if key in self.strings:
            del self.strings[key]
            removed += 1
        if key in self.hashes:
            del self.hashes[key]
            removed += 1
        return removed

    def hget(self, key: str, field: str) -> str | None:
        return self.hashes.get(key, {}).get(field)

    def hset(self, key: str, field: str, value: str) -> int:
        bucket = self.hashes.setdefault(key, {})
        bucket[field] = value
        return 1

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    def hdel(self, key: str, *fields: str) -> int:
        bucket = self.hashes.get(key)
        if not bucket:
            return 0
        removed = 0
        for field in fields:
            if field in bucket:
                del bucket[field]
                removed += 1
        if not bucket:
            self.hashes.pop(key, None)
        return removed


@pytest.fixture
def admin_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[TestClient, DummyRedis]:
    redis_stub = DummyRedis()

    monkeypatch.setattr(common_module, "_redis_client", redis_stub, raising=False)
    monkeypatch.setattr(common_module, "redis_client", lambda: redis_stub, raising=False)
    monkeypatch.setattr(core_module, "_sync_redis_client", redis_stub, raising=False)
    monkeypatch.setattr(core_module, "_redis_sync_client", lambda: redis_stub, raising=False)

    monkeypatch.setattr(common_module.settings, "ADMIN_TOKEN", "admin-token", raising=False)
    monkeypatch.setattr(core_module.settings, "ADMIN_TOKEN", "admin-token", raising=False)
    monkeypatch.setattr(main.settings, "ADMIN_TOKEN", "admin-token", raising=False)

    def fake_ensure_tenant_files(tenant: int):
        tenant_dir = tmp_path / str(tenant)
        tenant_dir.mkdir(parents=True, exist_ok=True)
        return tenant_dir

    monkeypatch.setattr(core_module, "ensure_tenant_files", fake_ensure_tenant_files, raising=False)
    monkeypatch.setattr(common_module, "ensure_tenant_files", fake_ensure_tenant_files, raising=False)
    monkeypatch.setattr(common_module, "read_persona", lambda tenant: "", raising=False)
    monkeypatch.setattr(common_module, "read_tenant_config", lambda tenant: {"passport": {"brand": "Test"}}, raising=False)

    async def fake_get_by_tenant(tenant_id: int):
        return None

    monkeypatch.setattr(admin_module.provider_tokens_repo, "get_by_tenant", fake_get_by_tenant, raising=False)

    with TestClient(main.app) as client:
        yield client, redis_stub


def _auth_params() -> dict[str, str]:
    return {"token": "admin-token"}


def test_admin_key_get_creates_single_key(admin_client: tuple[TestClient, DummyRedis]) -> None:
    client, _ = admin_client

    resp = client.get("/admin/key/get", params={"tenant": 1, **_auth_params()})
    assert resp.status_code == 200
    key1 = (resp.json().get("key") or "").strip()
    assert key1

    second = client.get("/admin/key/get", params={"tenant": 1, **_auth_params()})
    assert second.status_code == 200
    assert (second.json().get("key") or "").strip() == key1

    listing = client.get("/admin/keys/list", params={"tenant": 1, **_auth_params()})
    assert listing.status_code == 200
    items = listing.json().get("items")
    assert isinstance(items, list) and len(items) == 1
    assert (items[0].get("key") or "").strip() == key1


def test_admin_key_conflicts_and_delete(admin_client: tuple[TestClient, DummyRedis]) -> None:
    client, _ = admin_client

    created = client.get("/admin/key/get", params={"tenant": 2, **_auth_params()})
    assert created.status_code == 200
    key_value = (created.json().get("key") or "").strip()
    assert key_value

    conflict = client.post("/admin/keys/generate", params=_auth_params(), json={"tenant": 2, "label": "auto"})
    assert conflict.status_code == 409
    assert conflict.json().get("error") == "key_already_exists"

    manual_conflict = client.post("/admin/keys/save", params=_auth_params(), json={"tenant": 2, "key": "another", "label": "x"})
    assert manual_conflict.status_code == 409
    assert manual_conflict.json().get("error") == "key_already_exists"

    manual_update = client.post("/admin/keys/save", params=_auth_params(), json={"tenant": 2, "key": key_value, "label": "demo"})
    assert manual_update.status_code == 200
    assert manual_update.json().get("ok")

    legacy_conflict = client.post("/admin/key/generate", params={"tenant": 2, **_auth_params()})
    assert legacy_conflict.status_code == 409
    assert legacy_conflict.json().get("error") == "key_already_exists"

    removal = client.post("/admin/keys/delete", params=_auth_params(), json={"tenant": 2, "key": key_value})
    assert removal.status_code == 200
    assert removal.json().get("ok")

    listing = client.get("/admin/keys/list", params={"tenant": 2, **_auth_params()})
    assert listing.status_code == 200
    assert listing.json().get("items") == []

    recreated = client.get("/admin/key/get", params={"tenant": 2, **_auth_params()})
    assert recreated.status_code == 200
    new_key = (recreated.json().get("key") or "").strip()
    assert new_key and new_key != key_value


def test_connect_wa_accepts_admin_header(admin_client: tuple[TestClient, DummyRedis]) -> None:
    client, _ = admin_client

    resp = client.get("/admin/key/get", params={"tenant": 3, **_auth_params()})
    assert resp.status_code == 200

    connect = client.get("/connect/wa", params={"tenant": 3}, headers={"X-Admin-Token": "admin-token"})
    assert connect.status_code == 200
    assert "Подключение WhatsApp" in connect.text
