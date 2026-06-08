import asyncio
from urllib.parse import parse_qs, urlparse

import pytest

from apps.api.web import analytics_avito


pytestmark = pytest.mark.integration


class DummyRequest:
    base_url = "https://hub.avio.website/"


class DummyRedis:
    def __init__(self):
        self.values = {}

    def setex(self, key, ttl, value):
        self.values[key] = (ttl, value)


def _json_body(response):
    import json

    return json.loads(response.body.decode("utf-8"))


def test_analytics_redirect_uses_generic_redirect_origin_with_analytics_callback(monkeypatch):
    monkeypatch.setenv("AVITO_REDIRECT_URL", "https://hub.avio.website/v1/oauth/avito/callback")
    monkeypatch.delenv("AVITO_ANALYTICS_REDIRECT_URI", raising=False)

    assert (
        analytics_avito._analytics_redirect(DummyRequest())
        == "https://hub.avio.website/v1/oauth/avito-analytics/callback"
    )


def test_analytics_authorize_uses_short_state_and_analytics_redirect(monkeypatch):
    redis = DummyRedis()
    monkeypatch.delenv("AVITO_REDIRECT_URL", raising=False)
    monkeypatch.setattr(
        analytics_avito.core_module.settings,
        "AVITO_REDIRECT_URL",
        "https://hub.avio.website/v1/oauth/avito/callback",
        raising=False,
    )

    async def _auth(request, tenant, k):
        return int(tenant), k or ""

    async def _schema_ok():
        return None

    monkeypatch.setattr(analytics_avito, "_authorize_public_settings_request", _auth)
    monkeypatch.setattr(analytics_avito, "_ensure_schema_or_503", _schema_ok)
    monkeypatch.setattr(analytics_avito.common, "redis_client", lambda: redis)

    response = asyncio.run(
        analytics_avito.avito_analytics_authorize(DummyRequest(), tenant=3, k="tenant-key")
    )

    assert response.status_code == 200
    url = _json_body(response)["authorize_url"]
    params = parse_qs(urlparse(url).query)
    state = params["state"][0]
    assert len(state) == 32
    assert all(ch in "0123456789abcdef" for ch in state)
    assert params["redirect_uri"] == ["https://hub.avio.website/v1/oauth/avito-analytics/callback"]
    ttl, payload = redis.values[f"oauth:avito:analytics:state:{state}"]
    assert ttl == 4 * 60 * 60
    assert '"tenant": 3' in payload
