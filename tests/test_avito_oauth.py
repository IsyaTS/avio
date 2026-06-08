import pytest

from libs.core.integrations import avito


pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_refresh_access_token_rejects_payload_without_access_token(monkeypatch):
    class DummyResponse:
        status_code = 200

        def json(self):
            return {"error": "invalid_client"}

    class DummyAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, *args, **kwargs):
            return DummyResponse()

    writes = []

    monkeypatch.setattr(avito.settings, "AVITO_CLIENT_ID", "client-id", raising=False)
    monkeypatch.setattr(avito.settings, "AVITO_CLIENT_SECRET", "client-secret", raising=False)
    monkeypatch.setattr(avito.httpx, "AsyncClient", DummyAsyncClient)
    monkeypatch.setattr(avito, "update_integration", lambda tenant, data: writes.append(data))

    with pytest.raises(avito.AvitoOAuthError, match="no access token"):
        await avito._refresh_access_token(1, {"refresh_token": "refresh-old"})

    assert writes == []
