from __future__ import annotations

from typing import Any

import pytest

from libs.core.integrations.avito_analytics import AvitoAPIError
from libs.core.services import avito_item_city


pytestmark = pytest.mark.unit


class _Repo:
    def __init__(self, cached: dict[str, Any] | None = None) -> None:
        self.cached = cached
        self.rows: list[dict[str, Any]] = []
        self.lead_links: list[dict[str, Any]] = []
        self.errors: list[str] = []

    async def get_context(self, tenant_id: int, account_id: int, item_id: int):
        return self.cached

    async def upsert_context(self, tenant_id: int, account_id: int, item_id: int, **kwargs: Any):
        row = {"tenant_id": tenant_id, "account_id": account_id, "item_id": item_id, **kwargs}
        self.rows.append(row)
        return row

    async def upsert_lead_item_context(self, tenant_id: int, lead_id: int, account_id: int, item_id: int):
        self.lead_links.append(
            {"tenant_id": tenant_id, "lead_id": lead_id, "account_id": account_id, "item_id": item_id}
        )
        return self.lead_links[-1]

    async def mark_error(self, tenant_id: int, account_id: int, item_id: int, error_code_or_message: str):
        self.errors.append(error_code_or_message)
        return {
            "tenant_id": tenant_id,
            "account_id": account_id,
            "item_id": item_id,
            "status": "error",
            "source": "api",
            "last_error": error_code_or_message,
        }


class _Token:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []

    async def ensure_access_token_for_account(self, tenant_id: int, account_id: int):
        self.calls.append((tenant_id, account_id))
        return "token", {"account_id": account_id}


class _ItemApi:
    def __init__(self, payload: Any = None, exc: Exception | None = None) -> None:
        self.payload = payload if payload is not None else {"url": "https://www.avito.ru/ufa/item"}
        self.exc = exc
        self.calls: list[tuple[str, int, int]] = []

    async def get_item_info(self, token: str, account_id: int, item_id: int):
        self.calls.append((token, account_id, item_id))
        if self.exc:
            raise self.exc
        return self.payload


def test_extract_city_from_address_and_url() -> None:
    assert avito_item_city.extract_city_from_address("Уфа, Менделеева 80") == "Уфа"
    assert avito_item_city.extract_city_from_url("https://www.avito.ru/ufa/kvartiry") == "Уфа"
    assert avito_item_city.extract_city_from_url("https://www.avito.ru/sterlitamak/item") == "Стерлитамак"
    assert avito_item_city.extract_city_from_url("https://www.avito.ru/ekaterinburg/item") == "Екатеринбург"
    assert avito_item_city.extract_city_from_url("https://www.avito.ru/unknown_city/item") is None


@pytest.mark.asyncio
async def test_cache_hit_avoids_token_and_api() -> None:
    repo = _Repo({"status": "resolved", "city": "Уфа", "source": "url", "url": "https://www.avito.ru/ufa/x"})
    token = _Token()
    item_api = _ItemApi()

    result = await avito_item_city.resolve_and_store_avito_item_city(
        tenant_id=101,
        account_id=222,
        item_id=333,
        repo_module=repo,
        token_module=token,
        item_api_module=item_api,
    )

    assert result.city == "Уфа"
    assert token.calls == []
    assert item_api.calls == []


@pytest.mark.asyncio
async def test_hint_city_is_stored_without_api() -> None:
    repo = _Repo()
    token = _Token()
    item_api = _ItemApi()

    result = await avito_item_city.resolve_and_store_avito_item_city(
        tenant_id=101,
        account_id=222,
        item_id=333,
        address_hint="Салават, Ленина 1",
        repo_module=repo,
        token_module=token,
        item_api_module=item_api,
    )

    assert result.city == "Салават"
    assert repo.rows[-1]["status"] == "resolved"
    assert token.calls == []
    assert item_api.calls == []


@pytest.mark.asyncio
async def test_cached_unknown_url_can_be_reparsed_without_api() -> None:
    repo = _Repo({"status": "unknown", "url": "https://www.avito.ru/ekaterinburg/dveri", "source": "api"})
    token = _Token()
    item_api = _ItemApi()

    result = await avito_item_city.resolve_and_store_avito_item_city(
        tenant_id=101,
        account_id=222,
        item_id=333,
        repo_module=repo,
        token_module=token,
        item_api_module=item_api,
    )

    assert result.city == "Екатеринбург"
    assert repo.rows[-1]["status"] == "resolved"
    assert token.calls == []
    assert item_api.calls == []


@pytest.mark.asyncio
async def test_lead_item_link_is_stored_when_lead_id_passed() -> None:
    repo = _Repo()

    await avito_item_city.resolve_and_store_avito_item_city(
        tenant_id=101,
        account_id=222,
        item_id=333,
        lead_id=501,
        address_hint="Уфа, Менделеева 80",
        repo_module=repo,
        token_module=_Token(),
        item_api_module=_ItemApi(),
    )

    assert repo.lead_links == [{"tenant_id": 101, "lead_id": 501, "account_id": 222, "item_id": 333}]


@pytest.mark.asyncio
async def test_api_lookup_uses_specific_account_id() -> None:
    repo = _Repo()
    token = _Token()
    item_api = _ItemApi({"url": "https://www.avito.ru/ishimbay/dveri"})

    result = await avito_item_city.resolve_and_store_avito_item_city(
        tenant_id=101,
        account_id=987,
        item_id=749,
        repo_module=repo,
        token_module=token,
        item_api_module=item_api,
    )

    assert result.city == "Ишимбай"
    assert token.calls == [(101, 987)]
    assert item_api.calls == [("token", 987, 749)]


@pytest.mark.asyncio
async def test_unknown_city_stores_unknown_status() -> None:
    repo = _Repo()
    result = await avito_item_city.resolve_and_store_avito_item_city(
        tenant_id=101,
        account_id=987,
        item_id=749,
        repo_module=repo,
        token_module=_Token(),
        item_api_module=_ItemApi({"url": "https://www.avito.ru/unknown_city/dveri"}),
    )

    assert result.status == "unknown"
    assert result.city is None
    assert repo.rows[-1]["status"] == "unknown"


@pytest.mark.asyncio
async def test_api_error_marks_error_and_does_not_raise() -> None:
    repo = _Repo()
    result = await avito_item_city.resolve_and_store_avito_item_city(
        tenant_id=101,
        account_id=987,
        item_id=749,
        repo_module=repo,
        token_module=_Token(),
        item_api_module=_ItemApi(exc=AvitoAPIError("Forbidden", status=403)),
    )

    assert result.status == "error"
    assert repo.errors == ["AvitoAPIError:403"]
