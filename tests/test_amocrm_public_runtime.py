from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from apps.api.web.services import amocrm_public_runtime


pytestmark = pytest.mark.unit


class _Req:
    def __init__(self, query_params=None, headers=None):
        self.query_params = query_params or {}
        self.headers = headers or {}

    def url_for(self, name: str, **kwargs):
        if name == "amocrm_oauth_callback":
            return "https://example.test/pub/integrations/amocrm/oauth/callback"
        if name == "client_settings":
            tenant = kwargs.get("tenant", "1")
            return f"/client/settings?tenant={tenant}"
        return f"/{name}"


def _deps(**overrides):
    async def _noop_async(*_args, **_kwargs):
        return None

    async def _authorize(_request, tenant, key):
        return int(tenant or 1), key or "tenant-key"

    async def _get_token(_tenant_id):
        return None

    deps = dict(
        authorize_public_settings_request_fn=_authorize,
        read_tenant_config_fn=lambda _tenant_id: {"integrations": {"amocrm": {}}},
        write_tenant_config_fn=lambda *_args, **_kwargs: None,
        amocrm_service_module=SimpleNamespace(
            get_amocrm_cfg=lambda cfg: ((cfg or {}).get("integrations") or {}).get("amocrm") or {},
            resolve_oauth_cfg=lambda _cfg, _tenant_id: {
                "client_id": "cid",
                "client_secret": "secret",
                "redirect_url": "https://example.test/callback",
            },
            resolve_auth_url=lambda _cfg, _tenant_id: "https://tenant.amocrm.ru",
            resolve_base_url=lambda _cfg, _tenant_id: "https://tenant.amocrm.ru",
            _extract_subdomain=lambda value: "",
            resolve_api_base_url=_noop_async,
            _extract_embedded_list=lambda payload, key: list((payload or {}).get("_embedded", {}).get(key, [])),
            find_tenant_by_account=lambda _account_id, _subdomain: None,
            AMOCRM_PROVIDER="amocrm",
            amocrm_core=SimpleNamespace(AmoCRMClient=object),
            amocrm_on_outbound_message=_noop_async,
        ),
        amocrm_integration_module=SimpleNamespace(
            build_oauth_state=lambda payload, _secret: f"state-{payload['tenant_id']}",
            verify_oauth_state=lambda _state, _secret: {"tenant_id": 7, "k": "tenant-key"},
            AmoCRMClient=object,
        ),
        amocrm_tokens_module=SimpleNamespace(
            ensure_schema=_noop_async,
            upsert=_noop_async,
            get=_get_token,
            delete=_noop_async,
        ),
        amocrm_chat_service_module=SimpleNamespace(
            ensure_chat_cfg_in_tenant=lambda cfg, _tenant_id: cfg,
            ensure_connected=_noop_async,
            mask_chat_cfg=lambda _cfg, _tenant_id: {"enabled": False, "scope_id": None},
            build_webhook_url=lambda base, _cfg, tenant_id: f"{base}/amo/{tenant_id}",
            find_tenant_by_webhook_token=lambda _token: None,
            find_tenant_by_scope_id=lambda _scope_id: None,
            build_webhook_path_token=lambda _cfg, _tenant_id: "expected-token",
            extract_webhook_message=lambda _payload: {
                "event_type": "new_message",
                "text": "hello",
                "attachments": [],
                "external_message_id": "msg-1",
                "external_chat_id": "chat-1",
                "external_conversation_id": "conv-1",
            },
            AMOCRM_CHAT_PROVIDER="amocrm_chat",
        ),
        common_module=SimpleNamespace(
            public_base_url=lambda _request: "https://example.test",
            public_url=lambda _request, path: f"https://example.test{path}",
        ),
        logger=SimpleNamespace(
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            debug=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
        uuid_module=SimpleNamespace(uuid4=lambda: SimpleNamespace(hex="nonce-1")),
        time_module=SimpleNamespace(time=lambda: 1234567890),
        urlencode_fn=lambda params: "&".join(f"{k}={v}" for k, v in sorted(params.items())),
        state_secret_fn=lambda: "secret",
        httpx_module=SimpleNamespace(AsyncClient=None),
        os_module=SimpleNamespace(getenv=lambda _name: ""),
        json_module=__import__("json"),
        datetime_cls=datetime,
        timezone_utc=timezone.utc,
        timedelta_cls=__import__("datetime").timedelta,
        quote_plus_fn=lambda value: value.replace(" ", "+"),
        no_store_headers_fn=lambda extra=None: {"Cache-Control": "no-store", **(extra or {})},
        read_amocrm_webhook_payload_fn=_noop_async,
        extract_amocrm_uninstall_info_fn=lambda _payload: (None, None),
        crm_chat_links_module=SimpleNamespace(
            find_by_scope_id=_noop_async,
            find_by_external_chat=_noop_async,
            touch_message_ids=_noop_async,
        ),
        crm_links_module=SimpleNamespace(get_link=_noop_async),
        db_module=SimpleNamespace(_fetchrow=None, _fetch=None),
        get_lead_dialog_metadata_fn=_noop_async,
        get_lead_peer_fn=_noop_async,
        content_fingerprint_fn=lambda text, attachments: f"fp:{text}:{len(attachments)}",
        text_or_placeholder_fn=lambda text, _attachments: text or "Вложение",
        redis_queue=None,
        settings_module=SimpleNamespace(r=SimpleNamespace(set=_noop_async), ADMIN_TOKEN=""),
        avito_bot_echo_key_fn=lambda tenant_id, peer: f"echo:{tenant_id}:{peer}",
        avito_bot_echo_ttl_seconds=180,
        normalize_echo_text_fn=lambda value: str(value or "").strip().lower(),
        telegram_transport_module=SimpleNamespace(send=_noop_async),
        insert_message_out_fn=_noop_async,
        capture_manager_intervention_fn=_noop_async,
        handoff_silence_key_fn=lambda tenant_id, lead_id: f"silence:{tenant_id}:{lead_id}",
        handoff_silence_meta_key_fn=lambda tenant_id, lead_id: f"meta:{tenant_id}:{lead_id}",
        handoff_silence_ttl_seconds=180,
        redis_error_type=RuntimeError,
        send_avito_fn=_noop_async,
    )
    deps.update(overrides)
    return amocrm_public_runtime.AmoCRMPublicDeps(**deps)


@pytest.mark.anyio
async def test_oauth_start_redirects_to_amocrm_authorize_url() -> None:
    response = await amocrm_public_runtime.oauth_start(
        _Req(),
        tenant_id=7,
        tenant=None,
        key="tenant-key",
        deps=_deps(),
    )

    assert response.status_code in {302, 307}
    assert "https://tenant.amocrm.ru/oauth" in response.headers["location"]
    assert "client_id=cid" in response.headers["location"]
    assert "state=state-7" in response.headers["location"]


@pytest.mark.anyio
async def test_oauth_callback_returns_missing_code_html() -> None:
    response = await amocrm_public_runtime.oauth_callback(
        _Req(),
        code=None,
        state="state-7",
        deps=_deps(),
    )

    assert response.status_code == 200
    assert "missing_code" in response.body.decode("utf-8")


@pytest.mark.anyio
async def test_oauth_status_returns_cached_chat_payload() -> None:
    async def _get_token(_tenant_id):
        return None

    deps = _deps(
        amocrm_tokens_module=SimpleNamespace(
            ensure_schema=lambda: None,
            upsert=lambda *a, **k: None,
            get=_get_token,
            delete=lambda *a, **k: None,
        ),
        amocrm_chat_service_module=SimpleNamespace(
            ensure_chat_cfg_in_tenant=lambda cfg, _tenant_id: cfg,
            ensure_connected=lambda *a, **k: None,
            mask_chat_cfg=lambda _cfg, _tenant_id: {
                "enabled": True,
                "scope_id": "scope-1",
                "channel_id": "channel-1",
            },
            build_webhook_url=lambda base, _cfg, tenant_id: f"{base}/amo/{tenant_id}",
            find_tenant_by_webhook_token=lambda _token: None,
            find_tenant_by_scope_id=lambda _scope_id: None,
            build_webhook_path_token=lambda _cfg, _tenant_id: "expected-token",
            extract_webhook_message=lambda _payload: {},
            AMOCRM_CHAT_PROVIDER="amocrm_chat",
        ),
    )

    response = await amocrm_public_runtime.oauth_status(
        _Req(),
        tenant=7,
        key="tenant-key",
        deps=deps,
    )

    payload = response.body.decode("utf-8")
    assert response.status_code == 200
    assert '"connected":false' in payload
    assert '"webhook_url":"https://example.test/amo/7"' in payload


@pytest.mark.anyio
async def test_chat_webhook_returns_tenant_not_found() -> None:
    response = await amocrm_public_runtime.chat_webhook(
        _Req(),
        token="missing",
        scope_id=None,
        deps=_deps(),
    )

    assert response.status_code == 404
    assert response.body.decode("utf-8") == '{"ok":false,"detail":"tenant_not_found"}'


@pytest.mark.anyio
async def test_disconnect_returns_tenant_not_found_when_uninstall_payload_unknown() -> None:
    async def _payload(_request):
        return {"account_id": 1}

    response = await amocrm_public_runtime.disconnect(
        _Req(),
        tenant=None,
        key=None,
        deps=_deps(read_amocrm_webhook_payload_fn=_payload),
    )

    assert response.status_code == 404
    assert response.body.decode("utf-8") == '{"ok":false,"detail":"tenant_not_found"}'


@pytest.mark.anyio
async def test_pipeline_returns_and_applies_stages() -> None:
    writes: list[dict] = []

    class _Token:
        access_token = "token"

    class _Client:
        def __init__(self, **_kwargs):
            pass

        async def get_pipelines(self):
            return {"_embedded": {"pipelines": [{"id": 10, "name": "Sales"}]}}

        async def get_pipeline_stages(self, _pipeline_id, with_descriptions=False):
            assert with_descriptions is True
            return {"_embedded": {"statuses": [{"id": 20, "name": "New"}]}}

    async def _get_token(_tenant_id):
        return _Token()

    async def _resolve_api_base_url(*_args, **_kwargs):
        return "https://tenant.amocrm.ru"

    deps = _deps(
        read_tenant_config_fn=lambda _tenant_id: {
            "integrations": {"amocrm": {"enabled": True}}
        },
        write_tenant_config_fn=lambda _tenant_id, cfg: writes.append(cfg),
        amocrm_tokens_module=SimpleNamespace(
            ensure_schema=lambda: None,
            upsert=lambda *a, **k: None,
            get=_get_token,
            delete=lambda *a, **k: None,
        ),
        amocrm_integration_module=SimpleNamespace(
            build_oauth_state=lambda payload, _secret: f"state-{payload['tenant_id']}",
            verify_oauth_state=lambda _state, _secret: {"tenant_id": 7, "k": "tenant-key"},
            AmoCRMClient=_Client,
        ),
        amocrm_service_module=SimpleNamespace(
            get_amocrm_cfg=lambda cfg: cfg["integrations"]["amocrm"],
            resolve_oauth_cfg=lambda _cfg, _tenant_id: {},
            resolve_auth_url=lambda _cfg, _tenant_id: "https://tenant.amocrm.ru",
            resolve_base_url=lambda _cfg, _tenant_id: "https://tenant.amocrm.ru",
            _extract_subdomain=lambda value: "",
            resolve_api_base_url=_resolve_api_base_url,
            _extract_embedded_list=lambda payload, key: list((payload or {}).get("_embedded", {}).get(key, [])),
            build_stages_from_statuses=lambda statuses: [
                {"id": item["id"], "name": item["name"]} for item in statuses
            ],
            _merge_stages_for_pipeline=lambda stages, _cfg, _pipeline_id: stages,
            find_tenant_by_account=lambda _account_id, _subdomain: None,
            AMOCRM_PROVIDER="amocrm",
            amocrm_core=SimpleNamespace(AmoCRMClient=object),
            amocrm_on_outbound_message=lambda *a, **k: None,
        ),
    )

    response = await amocrm_public_runtime.pipeline(
        _Req(),
        tenant=7,
        key="tenant-key",
        apply=1,
        pipeline_id=None,
        deps=deps,
    )

    assert response.status_code == 200
    assert '"pipeline_id":10' in response.body.decode("utf-8")
    assert writes and writes[0]["integrations"]["amocrm"]["pipeline_id"] == 10


@pytest.mark.anyio
async def test_connection_checks_enabled_config_and_pipelines() -> None:
    class _Client:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def get_pipelines(self):
            return {"_embedded": {"pipelines": []}}

    async def _resolve_api_base_url(*_args, **_kwargs):
        return "https://tenant.amocrm.ru"

    deps = _deps(
        read_tenant_config_fn=lambda _tenant_id: {"integrations": {"amocrm": {"enabled": True}}},
        amocrm_integration_module=SimpleNamespace(
            build_oauth_state=lambda payload, _secret: f"state-{payload['tenant_id']}",
            verify_oauth_state=lambda _state, _secret: {"tenant_id": 7, "k": "tenant-key"},
            AmoCRMClient=_Client,
        ),
        amocrm_service_module=SimpleNamespace(
            get_amocrm_cfg=lambda cfg: cfg["integrations"]["amocrm"],
            resolve_oauth_cfg=lambda _cfg, _tenant_id: {
                "client_id": "cid",
                "client_secret": "secret",
                "redirect_url": "https://example.test/callback",
            },
            resolve_auth_url=lambda _cfg, _tenant_id: "https://tenant.amocrm.ru",
            resolve_base_url=lambda _cfg, _tenant_id: "https://tenant.amocrm.ru",
            _extract_subdomain=lambda value: "",
            resolve_api_base_url=_resolve_api_base_url,
            _extract_embedded_list=lambda payload, key: [],
            find_tenant_by_account=lambda _account_id, _subdomain: None,
            AMOCRM_PROVIDER="amocrm",
            amocrm_core=SimpleNamespace(AmoCRMClient=object),
            amocrm_on_outbound_message=lambda *a, **k: None,
        ),
    )

    response = await amocrm_public_runtime.test_connection(
        _Req(),
        tenant=7,
        key="tenant-key",
        deps=deps,
    )

    assert response.status_code == 200
    assert response.body.decode("utf-8") == '{"ok":true}'


@pytest.mark.anyio
async def test_connection_rejects_disabled_amocrm() -> None:
    response = await amocrm_public_runtime.test_connection(
        _Req(),
        tenant=7,
        key="tenant-key",
        deps=_deps(read_tenant_config_fn=lambda _tenant_id: {"integrations": {"amocrm": {}}}),
    )

    assert response.status_code == 400
    assert "amocrm_not_enabled" in response.body.decode("utf-8")
