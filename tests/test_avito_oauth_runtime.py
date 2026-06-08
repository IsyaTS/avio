from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from apps.api.web.services import avito_oauth_runtime


pytestmark = pytest.mark.unit


class _Request:
    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self._payload = payload or {}
        self.query_params = {}
        self.cookies = {}

    async def json(self) -> dict[str, Any]:
        return dict(self._payload)


def _deps(**overrides: Any) -> avito_oauth_runtime.AvitoOAuthDeps:
    async def _auth(_request, tenant, key):
        return int(tenant or 101), key or "k"

    async def _noop_async(*_args, **_kwargs):
        return None

    avito_module = SimpleNamespace(
        AvitoOAuthError=RuntimeError,
        update_account_display_name=_noop_async,
    )
    values = dict(
        authorize_public_settings_request_fn=_auth,
        coerce_int_fn=lambda value: int(value) if value not in (None, "") else None,
        avito_module=avito_module,
        logger=SimpleNamespace(exception=lambda *a, **k: None, warning=lambda *a, **k: None, info=lambda *a, **k: None),
        common_module=SimpleNamespace(redis_client=lambda: None, ensure_tenant_files=lambda tenant: None, public_url=lambda request, path: path),
        json_module=json,
        redis_error_type=RuntimeError,
        avito_state_ttl=3600,
        avito_state_cookie="avito_oauth_state",
        avito_state_key_fn=lambda state: f"state:{state}",
        build_avito_oauth_state_fn=lambda tenant: f"state-{tenant}",
        avito_oauth_redirect_entry_url_fn=lambda request, tenant, key: f"/authorize?tenant={tenant}&k={key}",
        set_avito_state_cookie_fn=lambda response, request, state: None,
        avito_callback_html_fn=lambda ok, message, payload: "ok",
        clear_avito_state_cookie_fn=lambda response: None,
        verify_avito_oauth_state_fn=lambda state: None,
        resolve_tenant_from_state_fn=lambda **kwargs: None,
        build_token_update_payload_fn=lambda payload: payload,
        avito_token_payload_error=ValueError,
    )
    values.update(overrides)
    return avito_oauth_runtime.AvitoOAuthDeps(**values)


@pytest.mark.anyio
async def test_oauth_account_rename_updates_display_name() -> None:
    calls: list[tuple[int, int, str | None]] = []

    async def _rename(tenant_id: int, account_id: int, display_name: str | None):
        calls.append((tenant_id, account_id, display_name))
        return {
            "account_id": account_id,
            "account_login": "Сергей",
            "display_name": display_name,
            "status": "active",
            "is_primary": True,
        }

    deps = _deps(avito_module=SimpleNamespace(AvitoOAuthError=RuntimeError, update_account_display_name=_rename))

    response = await avito_oauth_runtime.oauth_account_rename(
        _Request({"display_name": " Двери Гермес "}),
        tenant=101,
        key="k",
        account_id=222,
        deps=deps,
    )

    body = json.loads(response.body.decode("utf-8"))
    assert calls == [(101, 222, "Двери Гермес")]
    assert body["account"]["display_name"] == "Двери Гермес"
    assert body["account"]["account_login"] == "Сергей"


@pytest.mark.anyio
async def test_oauth_account_rename_rejects_too_long_name() -> None:
    response = await avito_oauth_runtime.oauth_account_rename(
        _Request({"display_name": "x" * 121}),
        tenant=101,
        key="k",
        account_id=222,
        deps=_deps(),
    )

    assert response.status_code == 400


@pytest.mark.anyio
async def test_oauth_callback_registers_webhook_for_connected_account() -> None:
    calls: dict[str, Any] = {"sync_account_info": 0}

    class Redis:
        def get(self, _key: str) -> str:
            return '{"tenant": 101}'

        def delete(self, _key: str) -> None:
            return None

    async def _exchange(_tenant_id: int, _code: str) -> dict[str, Any]:
        return {"access_token": "token", "refresh_token": "refresh"}

    async def _upsert(_tenant_id: int, _payload: dict[str, Any]) -> dict[str, Any]:
        return {"tenant_id": 101, "account_id": 225109703, "is_primary": False}

    async def _sync_account_info(_tenant_id: int) -> None:
        calls["sync_account_info"] += 1

    async def _ensure_webhook(tenant_id: int, target_url: str, **kwargs: Any) -> bool:
        calls["webhook"] = (tenant_id, target_url, kwargs.get("account_id"))
        return True

    request = _Request()
    request.query_params = {}
    request.cookies = {}
    deps = _deps(
        avito_module=SimpleNamespace(
            AvitoOAuthError=RuntimeError,
            exchange_code_for_token=_exchange,
            upsert_oauth_account_from_payload=_upsert,
            update_integration=lambda tenant_id, payload: dict(payload),
            sync_account_info=_sync_account_info,
            ensure_webhook=_ensure_webhook,
        ),
        common_module=SimpleNamespace(
            redis_client=lambda: Redis(),
            ensure_tenant_files=lambda tenant: None,
            public_url=lambda request, path: f"https://dev.example{path}",
        ),
        resolve_tenant_from_state_fn=lambda **kwargs: 101,
    )

    response = await avito_oauth_runtime.oauth_callback(
        request,
        code="code",
        state="state",
        error=None,
        deps=deps,
    )

    assert response.status_code == 200
    assert calls["webhook"] == (101, "https://dev.example/webhook/avito", 225109703)
    assert calls["sync_account_info"] == 0
