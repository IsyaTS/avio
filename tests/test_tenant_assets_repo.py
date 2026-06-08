from __future__ import annotations

import pytest

from libs.core.repo import tenant_assets


pytestmark = pytest.mark.unit


@pytest.mark.anyio
async def test_sync_legacy_photo_asset_without_db_returns_safe_asset(monkeypatch) -> None:
    monkeypatch.setattr(tenant_assets.db_module, "_fetchrow", None, raising=False)
    monkeypatch.setattr(tenant_assets.db_module, "_exec", None, raising=False)

    asset = await tenant_assets.sync_legacy_photo_asset(
        7,
        {
            "id": "p1",
            "title": "Каталог дверей с зеркалом для Казани",
            "usage": "для Казани",
            "path": "uploads/photos/photo.jpg",
            "mime": "image/jpeg",
            "size": 10,
            "auto": True,
        },
    )

    assert asset is not None
    assert asset["tenant_id"] == 7
    assert asset["asset_type"] == "photo"
    assert asset["status"] == "active"
    assert asset["legacy_photo_id"] == "p1"


def test_stable_asset_id_is_deterministic() -> None:
    first = tenant_assets.stable_asset_id(1, legacy_photo_id="p1", relative_path="a.jpg", title="A")
    second = tenant_assets.stable_asset_id(1, legacy_photo_id="p1", relative_path="a.jpg", title="A")
    assert first == second
