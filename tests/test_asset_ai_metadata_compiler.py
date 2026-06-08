from __future__ import annotations

import pytest

from libs.core.services.asset_ai_metadata_compiler import AssetCompileInput, compile_asset_metadata


pytestmark = pytest.mark.unit


@pytest.mark.anyio
async def test_compile_asset_metadata_rule_fallback_extracts_city_and_product() -> None:
    result = await compile_asset_metadata(
        AssetCompileInput(
            tenant_id=101,
            asset_id="asset-1",
            asset_type="photo",
            title="Каталог дверей с зеркалом для Казани",
            allowed_channels=("avito", "telegram"),
        )
    )

    metadata = result.metadata
    assert metadata["conditions"]["all"] == [
        {"slot": "city", "operator": "equals", "value": "Казань"},
        {"slot": "product", "operator": "contains", "value": "дверь с зеркалом"},
    ]
    assert metadata["action"]["type"] == "send_asset"
    assert metadata["guards"]["requires_known_slots"] == ["city", "product"]
    assert metadata["needs_review"] is False


@pytest.mark.anyio
async def test_compile_asset_metadata_respects_without_mirror() -> None:
    result = await compile_asset_metadata(
        AssetCompileInput(
            tenant_id=101,
            asset_id="asset-no-mirror",
            asset_type="photo",
            title="каталог квартирных дверей без зеркала для уфы",
            allowed_channels=("avito",),
        )
    )

    conditions = result.metadata["conditions"]["all"]
    assert {"slot": "product", "operator": "contains", "value": "квартирная дверь без зеркала"} in conditions


@pytest.mark.anyio
async def test_compile_asset_metadata_ambiguous_title_needs_review() -> None:
    result = await compile_asset_metadata(
        AssetCompileInput(tenant_id=1, asset_id="asset-2", asset_type="photo", title="Фото 1")
    )

    assert result.metadata["needs_review"] is True
    assert result.metadata["confidence"] < 0.75


@pytest.mark.anyio
async def test_compile_asset_metadata_invalid_ai_falls_back(monkeypatch) -> None:
    monkeypatch.setenv("ASSET_RULES_AI_ENABLED", "1")

    async def broken(_prompt: str):
        raise RuntimeError("bad json")

    result = await compile_asset_metadata(
        AssetCompileInput(tenant_id=1, asset_id="asset-3", asset_type="photo", title="Двери для Уфы"),
        json_reviewer_fn=broken,
    )

    assert result.used_ai is False
    assert result.metadata["conditions"]["all"][0]["value"] == "Уфа"
