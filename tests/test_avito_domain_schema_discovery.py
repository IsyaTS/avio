from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from libs.core.services.avito_dialog_filter import AvitoDialogMessage
from libs.core.services.avito_domain_schema_discovery import (
    AvitoDomainDiscoveryConfig,
    AvitoDomainSchemaDiscoverer,
    discover_domain_schema,
)


pytestmark = pytest.mark.unit


class FakeResponses:
    def __init__(self, payload: dict[str, Any] | str | BaseException) -> None:
        self.payload = payload
        self.calls = 0

    def create(self, **_kwargs: Any) -> SimpleNamespace:
        self.calls += 1
        if isinstance(self.payload, BaseException):
            raise self.payload
        if isinstance(self.payload, str):
            return SimpleNamespace(output_text=self.payload)
        return SimpleNamespace(output_text=json.dumps(self.payload, ensure_ascii=False))


class FakeClient:
    def __init__(self, payload: dict[str, Any] | str | BaseException) -> None:
        self.responses = FakeResponses(payload)


def _m(role: str, text: str) -> AvitoDialogMessage:
    return AvitoDialogMessage(role=role, text=text)


def _discoverer(payload: dict[str, Any] | str | BaseException) -> AvitoDomainSchemaDiscoverer:
    return AvitoDomainSchemaDiscoverer(
        client=FakeClient(payload),
        config=AvitoDomainDiscoveryConfig(model="gpt-5.2", timeout_seconds=1),
    )


@pytest.mark.asyncio
async def test_doors_schema_from_ai_payload() -> None:
    payload = {
        "domain_schema": {
            "domain": "entrance_doors",
            "domain_label": "входные двери",
            "primary_offer": "продажа и установка дверей",
            "required_slots": ["client_city", "door_type"],
            "optional_slots": ["premise_type", "size", "thermal_break"],
            "slot_definitions": {
                "client_city": "город клиента",
                "door_type": "тип двери",
                "premise_type": "тип помещения",
                "size": "размер проема",
                "thermal_break": "нужен ли терморазрыв",
            },
            "price_depends_on": ["door_type", "size"],
            "location_depends_on": ["client_city"],
            "service_depends_on": ["premise_type"],
            "common_intents": ["price_question", "store_location"],
            "confidence": 0.88,
        },
        "business_rules_draft": {
            "rules": [
                {
                    "rule_type": "price_dependency",
                    "description": "Цена зависит от типа двери и размера",
                    "depends_on": ["door_type", "size"],
                    "confidence": 0.8,
                }
            ]
        },
    }

    result = await discover_domain_schema(
        [[_m("client", "Нужна входная дверь"), _m("manager", "Уточните город")]],
        tenant_id=101,
        discoverer=_discoverer(payload),
        created_at=datetime(2026, 5, 25, tzinfo=timezone.utc),
    )

    schema = result.domain_schema
    assert schema["domain"] == "entrance_doors"
    assert "client_city" in schema["required_slots"]
    assert "door_type" in schema["required_slots"]
    assert result.business_rules_draft["needs_human_confirmation"] is True
    assert result.ai_extracted == 1


@pytest.mark.asyncio
async def test_lawn_mowing_schema_has_domain_slots() -> None:
    payload = {
        "domain_schema": {
            "domain": "lawn_mowing",
            "domain_label": "покос травы",
            "primary_offer": "услуги покоса травы",
            "required_slots": ["area_size", "grass_height", "location"],
            "optional_slots": ["waste_removal", "urgency", "access"],
            "slot_definitions": {
                "area_size": "площадь участка",
                "grass_height": "высота травы",
                "location": "адрес или район",
                "waste_removal": "нужно ли вывозить траву",
            },
            "price_depends_on": ["area_size", "grass_height", "waste_removal"],
            "location_depends_on": ["location"],
            "service_depends_on": ["access", "grass_height"],
            "availability_depends_on": ["location", "urgency"],
            "common_intents": ["price_question", "service_area"],
            "confidence": 0.86,
        }
    }

    result = await discover_domain_schema(
        [[_m("client", "Сколько стоит покосить 10 соток травы по пояс?"), _m("manager", "Где участок?")]],
        tenant_id=101,
        discoverer=_discoverer(payload),
    )

    schema = result.domain_schema
    assert schema["domain"] == "lawn_mowing"
    assert {"area_size", "grass_height", "location"}.issubset(set(schema["required_slots"]))
    assert "waste_removal" in schema["optional_slots"]


@pytest.mark.asyncio
async def test_cleaning_schema_has_cleaning_slots() -> None:
    payload = {
        "domain_schema": {
            "domain": "cleaning",
            "domain_label": "клининг",
            "primary_offer": "уборка помещений",
            "required_slots": ["area_size", "cleaning_type", "location"],
            "optional_slots": ["rooms_count", "pollution_level", "urgency"],
            "slot_definitions": {
                "area_size": "площадь помещения",
                "cleaning_type": "тип уборки",
                "location": "район или адрес",
            },
            "price_depends_on": ["area_size", "cleaning_type"],
            "location_depends_on": ["location"],
            "common_intents": ["price_question"],
            "confidence": 0.82,
        }
    }

    result = await discover_domain_schema(
        [[_m("client", "Сколько стоит генеральная уборка квартиры 60 м2?"), _m("manager", "В каком районе?")]],
        tenant_id=101,
        discoverer=_discoverer(payload),
    )

    assert result.domain_schema["domain"] == "cleaning"
    assert "cleaning_type" in result.domain_schema["required_slots"]


@pytest.mark.asyncio
async def test_ai_unavailable_returns_generic_sales_fallback() -> None:
    result = await discover_domain_schema([], tenant_id=101, enabled=False)

    assert result.domain_schema["domain"] == "generic_sales"
    assert result.domain_schema["required_slots"] == []
    assert result.mode == "disabled"


@pytest.mark.asyncio
async def test_invalid_ai_json_returns_fallback() -> None:
    result = await discover_domain_schema(
        [[_m("client", "Здравствуйте"), _m("manager", "Здравствуйте")]],
        tenant_id=101,
        discoverer=_discoverer("{not-json"),
    )

    assert result.domain_schema["domain"] == "generic_sales"
    assert result.ai_failed == 1


@pytest.mark.asyncio
async def test_schema_and_rules_do_not_include_raw_dialog_markers() -> None:
    payload = {
        "domain_schema": {
            "domain": "doors",
            "domain_label": "Клиент: raw phrase",
            "slot_definitions": {"client_city": "город"},
            "required_slots": ["client_city"],
        },
        "business_rules_draft": {
            "rules": [
                {
                    "rule_type": "general",
                    "description": "Менеджер: raw phrase",
                    "depends_on": ["client_city"],
                    "confidence": 0.7,
                }
            ]
        },
    }

    result = await discover_domain_schema(
        [[_m("client", "raw"), _m("manager", "raw")]],
        tenant_id=101,
        discoverer=_discoverer(payload),
    )
    combined = json.dumps([result.domain_schema, result.business_rules_draft], ensure_ascii=False)

    assert "Клиент:" not in combined
    assert "Менеджер:" not in combined
    assert result.business_rules_draft["rules"] == []
