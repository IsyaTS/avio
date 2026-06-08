import base64
import hashlib
import hmac
import json

import pytest

from apps.api.web import public as public_module


pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _disable_session_auth(monkeypatch):
    async def _no_user(request):
        return None

    monkeypatch.setattr(public_module.auth_utils, "get_current_user", _no_user)


class DummyRequest:
    def __init__(self, *, query_params=None, cookies=None, headers=None):
        self.query_params = query_params or {}
        self.cookies = cookies or {}
        self.headers = headers or {}
        self.url = "https://hub.avio.website/v1/oauth/avito/authorize"


def _json_response_body(response):
    return json.loads(response.body.decode("utf-8"))


def test_settings_get_accepts_cookie_key(monkeypatch):
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: tenant == 7 and key == "cookie-secret")
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: {"tenant": tenant})
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant, channel=None: "persona")

    import asyncio

    response = asyncio.run(
        public_module.settings_get(
            DummyRequest(cookies={"client_key": "cookie-secret"}),
            tenant=7,
        )
    )

    assert response.status_code == 200
    assert _json_response_body(response) == {
        "ok": True,
        "cfg": {"tenant": 7},
        "persona": "persona",
        "personas": {"telegram": "persona", "avito": "persona"},
    }


def test_settings_get_accepts_query_key(monkeypatch):
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: tenant == 5 and key == "query-secret")
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: {"tenant": tenant})
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant, channel=None: "persona")

    import asyncio

    response = asyncio.run(
        public_module.settings_get(
            DummyRequest(query_params={"k": "query-secret"}),
            tenant=5,
            k="query-secret",
        )
    )

    assert response.status_code == 200
    assert _json_response_body(response) == {
        "ok": True,
        "cfg": {"tenant": 5},
        "persona": "persona",
        "personas": {"telegram": "persona", "avito": "persona"},
    }


def test_settings_get_accepts_global_and_tenant_keys(monkeypatch):
    monkeypatch.setattr(public_module.settings, "PUBLIC_KEY", "GLOBAL")
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    config = {"passport": {"public_key": "TENANT_KEY"}, "tenant": 1}
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: dict(config))
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant, channel=None: "persona")
    monkeypatch.setattr(public_module.common, "get_tenant_pubkey", lambda tenant: "")
    monkeypatch.setattr(
        public_module.common,
        "valid_key",
        lambda tenant, key: key in {"GLOBAL", "TENANT_KEY"},
    )

    import asyncio

    global_resp = asyncio.run(
        public_module.settings_get(
            DummyRequest(query_params={"k": "GLOBAL"}),
            tenant=1,
            k="GLOBAL",
        )
    )
    assert global_resp.status_code == 200

    tenant_resp = asyncio.run(
        public_module.settings_get(
            DummyRequest(query_params={"k": "TENANT_KEY"}),
            tenant=1,
            k="TENANT_KEY",
        )
    )
    assert tenant_resp.status_code == 200

    denied_resp = asyncio.run(
        public_module.settings_get(
            DummyRequest(query_params={"k": "BAD"}),
            tenant=1,
            k="BAD",
        )
    )
    assert denied_resp.status_code == 401


def test_settings_get_sets_no_cache_headers(monkeypatch):
    monkeypatch.setattr(public_module.common, "valid_key", lambda tenant, key: tenant == 3 and key == "token")
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "read_tenant_config", lambda tenant: {"tenant": tenant})
    monkeypatch.setattr(public_module.common, "read_persona", lambda tenant, channel=None: "persona")

    import asyncio

    response = asyncio.run(
        public_module.settings_get(
            DummyRequest(query_params={"k": "token"}),
            tenant=3,
            k="token",
        )
    )

    assert response.status_code == 200
    headers = response.headers
    assert headers.get("cache-control") == "no-store, must-revalidate"
    assert headers.get("pragma") == "no-cache"
    assert headers.get("expires") == "0"


def test_avito_oauth_callback_clears_stale_account_before_sync(monkeypatch):
    class DummyRedis:
        def __init__(self):
            self.store = {
                public_module._avito_state_key("state-1"): '{"tenant": 3}'
            }

        def get(self, key):
            return self.store.get(key)

        def delete(self, key):
            self.store.pop(key, None)

    captured = {}

    async def _no_user(request):
        return None

    async def _exchange(tenant, code):
        return {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
            "scope": "messenger",
        }

    def _update(tenant, payload):
        captured["tenant"] = tenant
        captured["payload"] = dict(payload)
        return dict(payload)

    async def _sync(tenant):
        captured["sync_tenant"] = tenant
        return {}

    async def _webhook(tenant, target_url):
        captured["webhook"] = (tenant, target_url)
        return True

    monkeypatch.setattr(public_module.common, "redis_client", lambda: DummyRedis())
    monkeypatch.setattr(public_module.auth_utils, "get_current_user", _no_user)
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "public_url", lambda request, url: str(url))
    monkeypatch.setattr(public_module.avito, "exchange_code_for_token", _exchange)
    monkeypatch.setattr(public_module.avito, "update_integration", _update)
    monkeypatch.setattr(public_module.avito, "sync_account_info", _sync)
    monkeypatch.setattr(public_module.avito, "ensure_webhook", _webhook)

    import asyncio

    response = asyncio.run(
        public_module.avito_oauth_callback(object(), state="state-1", code="code-1")
    )

    assert response.status_code == 200
    assert captured["tenant"] == 3
    assert captured["payload"]["access_token"] == "new-access"
    assert captured["payload"]["refresh_token"] == "new-refresh"
    assert captured["payload"]["account_id"] is None
    assert captured["payload"]["account_login"] is None
    assert captured["sync_tenant"] == 3


def test_avito_oauth_authorize_returns_redirect_entry_without_setting_cookie(monkeypatch):
    class DummyRedis:
        def __init__(self):
            self.store = {}

        def setex(self, key, ttl, value):
            self.store[key] = (ttl, value)

    redis = DummyRedis()

    async def _auth(request, tenant, k):
        return int(tenant), k or ""

    monkeypatch.setattr(public_module, "_authorize_public_settings_request", _auth)
    monkeypatch.setattr(public_module.common, "redis_client", lambda: redis)
    captured = {}

    def _authorize_url(state):
        captured["state"] = state
        return f"https://www.avito.ru/oauth?state={state}"

    monkeypatch.setattr(
        public_module.avito,
        "build_authorize_url",
        _authorize_url,
    )
    monkeypatch.setattr(
        public_module.settings,
        "AVITO_REDIRECT_URL",
        "https://hub.avio.website/v1/oauth/avito/callback",
        raising=False,
    )

    import asyncio

    response = asyncio.run(
        public_module.avito_oauth_authorize(DummyRequest(), tenant=3, k="key")
    )
    body = _json_response_body(response)

    assert body["authorize_url"] == (
        "https://hub.avio.website/v1/oauth/avito/authorize?tenant=3&redirect=1&k=key"
    )
    assert public_module.AVITO_STATE_COOKIE not in response.headers.get("set-cookie", "")
    assert len(redis.store) == 1
    ttl, payload = next(iter(redis.store.values()))
    assert ttl == public_module.AVITO_STATE_TTL
    assert payload == '{"tenant": 3}'
    assert captured["state"]
    assert public_module._verify_avito_oauth_state(captured["state"])["tenant"] == 3


def test_avito_oauth_authorize_redirect_sets_cookie_and_passes_state_to_avito_url(monkeypatch):
    class DummyRedis:
        def __init__(self):
            self.store = {}

        def setex(self, key, ttl, value):
            self.store[key] = (ttl, value)

    redis = DummyRedis()

    async def _auth(request, tenant, k):
        return int(tenant), k or ""

    monkeypatch.setattr(public_module, "_authorize_public_settings_request", _auth)
    monkeypatch.setattr(public_module.common, "redis_client", lambda: redis)
    captured = {}

    def _authorize_url(state):
        captured["state"] = state
        return f"https://www.avito.ru/oauth?state={state}"

    monkeypatch.setattr(
        public_module.avito,
        "build_authorize_url",
        _authorize_url,
    )

    import asyncio

    response = asyncio.run(
        public_module.avito_oauth_authorize(DummyRequest(), tenant=3, k="key", redirect=True)
    )

    assert response.status_code == 303
    assert response.headers["location"] == f"https://www.avito.ru/oauth?state={captured['state']}"
    assert public_module.AVITO_STATE_COOKIE in response.headers.get("set-cookie", "")
    assert len(redis.store) == 1
    assert public_module._verify_avito_oauth_state(captured["state"])["tenant"] == 3


def test_avito_oauth_callback_accepts_cookie_state_without_query_state(monkeypatch):
    class DummyRedis:
        def __init__(self):
            self.store = {public_module._avito_state_key("cookie-state"): '{"tenant": 3}'}

        def get(self, key):
            return self.store.get(key)

        def delete(self, key):
            self.store.pop(key, None)

    captured = {}

    async def _exchange(tenant, code):
        return {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
        }

    def _update(tenant, payload):
        captured["tenant"] = tenant
        captured["payload"] = dict(payload)
        return dict(payload)

    async def _sync(tenant):
        return {}

    async def _webhook(tenant, target_url):
        return True

    monkeypatch.setattr(public_module.common, "redis_client", lambda: DummyRedis())
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "public_url", lambda request, url: str(url))
    monkeypatch.setattr(public_module.avito, "exchange_code_for_token", _exchange)
    monkeypatch.setattr(public_module.avito, "update_integration", _update)
    monkeypatch.setattr(public_module.avito, "sync_account_info", _sync)
    monkeypatch.setattr(public_module.avito, "ensure_webhook", _webhook)

    import asyncio

    response = asyncio.run(
        public_module.avito_oauth_callback(
            DummyRequest(cookies={public_module.AVITO_STATE_COOKIE: "cookie-state"}),
            state=None,
            code="code-1",
        )
    )

    assert response.status_code == 200
    assert captured["tenant"] == 3
    assert captured["payload"]["access_token"] == "new-access"


def test_avito_oauth_callback_accepts_signed_state_without_redis(monkeypatch):
    captured = {}

    async def _exchange(tenant, code):
        return {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
        }

    def _update(tenant, payload):
        captured["tenant"] = tenant
        captured["payload"] = dict(payload)
        return dict(payload)

    async def _sync(tenant):
        captured["sync_tenant"] = tenant
        return {}

    async def _webhook(tenant, target_url):
        captured["webhook"] = (tenant, target_url)
        return True

    monkeypatch.setattr(public_module.settings, "WEBHOOK_SECRET", "test-secret", raising=False)
    monkeypatch.setattr(public_module.common, "redis_client", lambda: (_ for _ in ()).throw(public_module.redis_ex.RedisError()))
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "public_url", lambda request, url: str(url))
    monkeypatch.setattr(public_module.avito, "exchange_code_for_token", _exchange)
    monkeypatch.setattr(public_module.avito, "update_integration", _update)
    monkeypatch.setattr(public_module.avito, "sync_account_info", _sync)
    monkeypatch.setattr(public_module.avito, "ensure_webhook", _webhook)

    payload = json.dumps(
        {"tenant": 4, "iat": int(public_module.time.time())},
        separators=(",", ":"),
    ).encode("utf-8")
    body = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    signature = base64.urlsafe_b64encode(
        hmac.new(b"test-secret", body.encode("ascii"), hashlib.sha256).digest()
    ).decode("ascii").rstrip("=")
    state = f"v1.{body}.{signature}"

    import asyncio

    response = asyncio.run(
        public_module.avito_oauth_callback(object(), state=state, code="code-1")
    )

    assert response.status_code == 200
    assert captured["tenant"] == 4
    assert captured["payload"]["access_token"] == "new-access"
    assert captured["sync_tenant"] == 4


def test_avito_oauth_callback_accepts_new_signed_hex_state_after_redis_loss(monkeypatch):
    captured = {}

    async def _exchange(tenant, code):
        return {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
        }

    def _update(tenant, payload):
        captured["tenant"] = tenant
        captured["payload"] = dict(payload)
        return dict(payload)

    async def _sync(tenant):
        captured["sync_tenant"] = tenant
        return {}

    async def _webhook(tenant, target_url):
        return True

    monkeypatch.setattr(public_module.settings, "WEBHOOK_SECRET", "test-secret", raising=False)
    monkeypatch.setattr(
        public_module.common,
        "redis_client",
        lambda: (_ for _ in ()).throw(public_module.redis_ex.RedisError()),
    )
    monkeypatch.setattr(public_module.common, "ensure_tenant_files", lambda tenant: None)
    monkeypatch.setattr(public_module.common, "public_url", lambda request, url: str(url))
    monkeypatch.setattr(public_module.avito, "exchange_code_for_token", _exchange)
    monkeypatch.setattr(public_module.avito, "update_integration", _update)
    monkeypatch.setattr(public_module.avito, "sync_account_info", _sync)
    monkeypatch.setattr(public_module.avito, "ensure_webhook", _webhook)

    state = public_module._build_avito_oauth_state(3)

    import asyncio

    response = asyncio.run(public_module.avito_oauth_callback(object(), state=state, code="code-1"))

    assert response.status_code == 200
    assert captured["tenant"] == 3
    assert captured["payload"]["access_token"] == "new-access"
    assert captured["sync_tenant"] == 3


def test_avito_oauth_state_is_avito_compatible_random_hex(monkeypatch):
    monkeypatch.setattr(public_module.settings, "WEBHOOK_SECRET", "test-secret", raising=False)

    state = public_module._build_avito_oauth_state(3)

    assert len(state) == 82
    assert state.startswith("a1")
    assert state.isalnum()
    assert public_module._verify_avito_oauth_state(state)["tenant"] == 3
