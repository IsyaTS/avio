from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.responses import Response

from apps.api.web.services import avito_public_runtime


pytestmark = pytest.mark.unit


class _Clock:
    def __init__(self, value: int = 1_700_000_000):
        self.value = value

    def time(self) -> int:
        return self.value


class _Secrets:
    @staticmethod
    def token_hex(_size: int) -> str:
        return "1" * 32


class _Request:
    def __init__(self, *, query_params=None, cookies=None, headers=None, url="http://local/path"):
        self.query_params = query_params or {}
        self.cookies = cookies or {}
        self.headers = headers or {}
        self.url = url

    def url_for(self, name: str, **params):
        assert name == "client_settings"
        return f"/client/{params['tenant']}/settings"


class _Redis:
    def __init__(self):
        self.store = {
            "oauth:avito:state:one": '{"tenant": 3}',
            "oauth:avito:state:two": b'{"tenant": 4}',
            "other": '{"tenant": 3}',
        }

    def scan_iter(self, pattern):
        assert pattern == "oauth:avito:state:*"
        return [key for key in self.store if key.startswith("oauth:avito:state:")]

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            return 1
        return 0


def test_signed_hex_state_roundtrip_and_tamper_rejection() -> None:
    settings = SimpleNamespace(WEBHOOK_SECRET="secret")
    clock = _Clock()

    state = avito_public_runtime.build_oauth_state(
        3,
        settings_module=settings,
        time_module=clock,
        secrets_module=_Secrets,
    )

    assert len(state) == 82
    assert avito_public_runtime.verify_oauth_state(
        state,
        settings_module=settings,
        ttl_seconds=3600,
        coerce_int_fn=lambda value: int(value),
        time_module=clock,
    ) == {"tenant": 3, "iat": clock.value}
    assert avito_public_runtime.verify_oauth_state(
        f"{state[:-1]}0",
        settings_module=settings,
        ttl_seconds=3600,
        coerce_int_fn=lambda value: int(value),
        time_module=clock,
    ) is None


def test_redirect_entry_uses_configured_public_origin() -> None:
    settings = SimpleNamespace(AVITO_REDIRECT_URL="https://hub.avio.website/v1/oauth/avito/callback")

    url = avito_public_runtime.oauth_redirect_entry_url(
        _Request(),
        3,
        "public-key",
        settings_module=settings,
        public_base_url_fn=lambda _request: "http://fallback",
    )

    assert url == "https://hub.avio.website/v1/oauth/avito/authorize?tenant=3&redirect=1&k=public-key"


def test_state_cookie_uses_shared_avio_domain_for_hub() -> None:
    settings = SimpleNamespace(AVITO_REDIRECT_URL="https://hub.avio.website/v1/oauth/avito/callback")
    response = Response()

    avito_public_runtime.set_state_cookie(
        response,
        _Request(headers={"x-forwarded-proto": "https"}, url="https://hub.avio.website/path"),
        "state-1",
        settings_module=settings,
        cookie_name="avito_oauth_state",
        ttl_seconds=3600,
    )

    header = response.headers["set-cookie"]
    assert "avito_oauth_state=state-1" in header
    assert "Domain=.avio.website" in header
    assert "Secure" in header


def test_delete_states_for_tenant_only_deletes_matching_oauth_keys() -> None:
    redis = _Redis()

    deleted = avito_public_runtime.delete_states_for_tenant(
        redis,
        3,
        prefix="oauth:avito:state:",
        coerce_int_fn=lambda value: int(value),
    )

    assert deleted == 1
    assert "oauth:avito:state:one" not in redis.store
    assert "oauth:avito:state:two" in redis.store
    assert "other" in redis.store


def test_connect_avito_enables_auto_reply_and_renders_context() -> None:
    cfg = {"passport": {"brand": "Brand"}, "behavior": {"auto_reply": False}}
    written = {}
    rendered = {}

    common = SimpleNamespace(
        valid_key=lambda tenant, key: tenant == 3 and key == "key",
        ensure_tenant_files=lambda tenant: None,
        read_tenant_config=lambda tenant: cfg,
        write_tenant_config=lambda tenant, payload: written.update({"tenant": tenant, "payload": payload}),
        get_tenant_pubkey=lambda tenant: "primary-key",
        public_url=lambda _request, value: f"https://hub.avio.website{value}",
    )
    avito = SimpleNamespace(
        get_integration=lambda tenant: {
            "access_token": "token",
            "account_id": "123",
            "expires_at": "1700000100",
        }
    )

    def _render(template, context):
        rendered.update({"template": template, "context": context})
        return Response("ok")

    response = avito_public_runtime.connect_avito(
        3,
        _Request(query_params={"k": "key"}),
        k=None,
        key=None,
        deps=avito_public_runtime.AvitoConnectDeps(
            common_module=common,
            avito_module=avito,
            logger=SimpleNamespace(exception=lambda *args, **kwargs: None),
            render_template_fn=_render,
            quote_plus_fn=lambda value: value,
        ),
    )

    assert response.status_code == 200
    assert written["payload"]["behavior"]["auto_reply"] is True
    assert written["payload"]["behavior"]["auto_reply_enabled"] is True
    assert rendered["template"] == "connect/avito.html"
    assert rendered["context"]["key"] == "primary-key"
    assert rendered["context"]["avito"]["connected"] is True
