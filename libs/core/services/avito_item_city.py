from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping
from urllib.parse import urlparse

from libs.core.integrations import avito as avito_integration
from libs.core.integrations import avito_analytics
from libs.core.repo import avito_item_contexts


AsyncLogFn = Callable[..., Awaitable[Any]]
LogFn = Callable[..., Any]


_CITY_SLUGS = {
    "ufa": "Уфа",
    "sterlitamak": "Стерлитамак",
    "salavat": "Салават",
    "ishimbay": "Ишимбай",
    "orenburg": "Оренбург",
    "kazan": "Казань",
    "ekaterinburg": "Екатеринбург",
    "chelyabinsk": "Челябинск",
    "perm": "Пермь",
    "samara": "Самара",
    "tyumen": "Тюмень",
    "izhevsk": "Ижевск",
    "naberezhnye_chelny": "Набережные Челны",
    "nizhnekamsk": "Нижнекамск",
    "oktyabrskiy": "Октябрьский",
    "tuymazy": "Туймазы",
    "belebey": "Белебей",
    "neftekamsk": "Нефтекамск",
    "meleuz": "Мелеуз",
    "kumertau": "Кумертау",
    "birsk": "Бирск",
    "beloretsk": "Белорецк",
    "sibay": "Сибай",
    "almetevsk": "Альметьевск",
    "almetyevsk": "Альметьевск",
    "bugulma": "Бугульма",
    "bavly": "Бавлы",
    "urussu": "Уруссу",
    "chishmy": "Чишмы",
    "dyurtyuli": "Дюртюли",
    "blagoveschensk": "Благовещенск",
    "blagoveshchensk": "Благовещенск",
    "tolbazy": "Толбазы",
    "ishimbay": "Ишимбай",
    "asha": "Аша",
    "iglino": "Иглино",
    "yazykovo": "Языково",
    "kandry": "Кандры",
}

_BAD_ADDRESS_PREFIXES = {
    "ул",
    "улица",
    "проспект",
    "пр-кт",
    "переулок",
    "район",
    "р-н",
    "дом",
    "д",
}


@dataclass(frozen=True)
class AvitoItemCityResult:
    tenant_id: int
    account_id: int
    item_id: int
    city: str | None
    address: str | None
    url: str | None
    source: str
    status: str
    error_code: str | None = None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def extract_city_from_address(address: str | None) -> str | None:
    text = _clean_text(address)
    if not text:
        return None
    first = text.split(",", 1)[0].strip(" .")
    if not first:
        return None
    lowered = first.lower().replace(".", "")
    if lowered.isdigit() or lowered in _BAD_ADDRESS_PREFIXES:
        return None
    if any(lowered.startswith(f"{prefix} ") for prefix in _BAD_ADDRESS_PREFIXES):
        return None
    if any(char.isdigit() for char in first):
        return None
    if len(first) > 40:
        return None
    return first[:1].upper() + first[1:]


def extract_city_from_url(url: str | None) -> str | None:
    text = _clean_text(url)
    if not text:
        return None
    try:
        parsed = urlparse(text)
    except Exception:
        return None
    path = parsed.path if parsed.scheme or parsed.netloc else text
    first_segment = next((segment for segment in path.split("/") if segment), "")
    if not first_segment:
        return None
    slug = first_segment.strip().lower().replace("-", "_")
    return _CITY_SLUGS.get(slug)


def _safe_error_code(exc: BaseException) -> str:
    status = getattr(exc, "status", None)
    if status:
        return f"{type(exc).__name__}:{status}"[:200]
    return type(exc).__name__[:200]


def _extract_item_url(payload: Mapping[str, Any] | list[Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    for key in ("url", "uri", "link"):
        value = _clean_text(payload.get(key))
        if value:
            return value
    item = payload.get("item") if isinstance(payload.get("item"), Mapping) else {}
    return _clean_text(item.get("url") or item.get("uri") or item.get("link"))


def _extract_item_address(payload: Mapping[str, Any] | list[Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    for key in ("address", "location"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            value = value.get("address") or value.get("name") or value.get("title")
        cleaned = _clean_text(value)
        if cleaned:
            return cleaned
    item = payload.get("item") if isinstance(payload.get("item"), Mapping) else {}
    value = item.get("address") or item.get("location")
    if isinstance(value, Mapping):
        value = value.get("address") or value.get("name") or value.get("title")
    return _clean_text(value)


def _result_from_row(row: Mapping[str, Any], *, tenant_id: int, account_id: int, item_id: int) -> AvitoItemCityResult:
    return AvitoItemCityResult(
        tenant_id=int(tenant_id),
        account_id=int(account_id),
        item_id=int(item_id),
        city=_clean_text(row.get("city")),
        address=_clean_text(row.get("address")),
        url=_clean_text(row.get("url")),
        source=_clean_text(row.get("source")) or "unknown",
        status=_clean_text(row.get("status")) or "unknown",
        error_code=_clean_text(row.get("last_error")),
    )


async def resolve_and_store_avito_item_city(
    *,
    tenant_id: int,
    account_id: int,
    item_id: int,
    lead_id: int | None = None,
    url_hint: str | None = None,
    address_hint: str | None = None,
    token_module: Any = avito_integration,
    item_api_module: Any = avito_analytics,
    repo_module: Any = avito_item_contexts,
    log_fn: LogFn | None = None,
) -> AvitoItemCityResult:
    tenant_id = int(tenant_id)
    account_id = int(account_id)
    item_id = int(item_id)

    try:
        cached = await repo_module.get_context(tenant_id, account_id, item_id)
        if lead_id:
            await repo_module.upsert_lead_item_context(tenant_id, int(lead_id), account_id, item_id)
        if cached and cached.get("status") == "resolved" and cached.get("city"):
            return _result_from_row(cached, tenant_id=tenant_id, account_id=account_id, item_id=item_id)

        address = _clean_text(address_hint) or _clean_text(cached.get("address") if cached else None)
        url = _clean_text(url_hint) or _clean_text(cached.get("url") if cached else None)
        city = extract_city_from_address(address) or extract_city_from_url(url)
        if city:
            row = await repo_module.upsert_context(
                tenant_id,
                account_id,
                item_id,
                city=city,
                address=address,
                url=url,
                source="address_hint" if extract_city_from_address(address) else "url_hint",
                status="resolved",
            )
            return _result_from_row(row or {}, tenant_id=tenant_id, account_id=account_id, item_id=item_id)

        token_result = await token_module.ensure_access_token_for_account(tenant_id, account_id)
        access_token = token_result[0] if isinstance(token_result, tuple) else token_result
        item_info = await item_api_module.get_item_info(access_token, account_id, item_id)
        address = _extract_item_address(item_info) or address
        url = _extract_item_url(item_info) or url
        city = extract_city_from_address(address) or extract_city_from_url(url)
        row = await repo_module.upsert_context(
            tenant_id,
            account_id,
            item_id,
            city=city,
            address=address,
            url=url,
            source="address" if extract_city_from_address(address) else ("url" if extract_city_from_url(url) else "api"),
            status="resolved" if city else "unknown",
        )
        return _result_from_row(row or {}, tenant_id=tenant_id, account_id=account_id, item_id=item_id)
    except Exception as exc:
        error_code = _safe_error_code(exc)
        try:
            row = await repo_module.mark_error(tenant_id, account_id, item_id, error_code)
        except Exception:
            row = None
        if log_fn:
            try:
                log_fn(
                    "event=avito_item_city_resolve_error tenant=%s account_id=%s item_id=%s error=%s",
                    tenant_id,
                    account_id,
                    item_id,
                    error_code,
                )
            except TypeError:
                log_fn(
                    "event=avito_item_city_resolve_error tenant=%s account_id=%s item_id=%s error=%s"
                    % (tenant_id, account_id, item_id, error_code)
                )
        if row:
            return _result_from_row(row, tenant_id=tenant_id, account_id=account_id, item_id=item_id)
        return AvitoItemCityResult(
            tenant_id=tenant_id,
            account_id=account_id,
            item_id=item_id,
            city=None,
            address=None,
            url=None,
            source="api",
            status="error",
            error_code=error_code,
        )


__all__ = [
    "AvitoItemCityResult",
    "extract_city_from_address",
    "extract_city_from_url",
    "resolve_and_store_avito_item_city",
]
