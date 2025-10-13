import pytest
from fastapi.testclient import TestClient

from tgworker.api import create_app
from tgworker.manager import TwoFASubmitResult


@pytest.fixture
def tgworker_client(monkeypatch):
    class DummyManager:
        def __init__(self):
            self.result: TwoFASubmitResult | None = None

        async def start(self):  # pragma: no cover - startup hook
            return None

        async def shutdown(self):  # pragma: no cover - shutdown hook
            return None

        async def submit_password(self, tenant_id: int, password: str) -> TwoFASubmitResult:
            assert tenant_id == 1
            assert password == "secret"
            assert self.result is not None
            return self.result

    dummy = DummyManager()
    monkeypatch.setattr("tgworker.api.SessionManager", lambda *args, **kwargs: dummy)
    app = create_app()
    with TestClient(app) as client:
        yield client, dummy


def test_twofa_submit_invalid_password(tgworker_client):
    client, manager = tgworker_client
    manager.result = TwoFASubmitResult(status_code=401, body={"error": "password_invalid"})

    response = client.post("/rpc/twofa.submit", json={"tenant_id": 1, "password": "secret"})

    assert response.status_code == 401
    assert response.json() == {"error": "password_invalid"}


def test_twofa_submit_srp_invalid(tgworker_client):
    client, manager = tgworker_client
    manager.result = TwoFASubmitResult(status_code=409, body={"error": "srp_invalid"})

    response = client.post("/rpc/twofa.submit", json={"tenant_id": 1, "password": "secret"})

    assert response.status_code == 409
    assert response.json() == {"error": "srp_invalid"}


def test_twofa_submit_flood_wait(tgworker_client):
    client, manager = tgworker_client
    manager.result = TwoFASubmitResult(
        status_code=429,
        body={"error": "flood_wait", "retry_after": 30},
        headers={"Retry-After": "30"},
    )

    response = client.post("/rpc/twofa.submit", json={"tenant_id": 1, "password": "secret"})

    assert response.status_code == 429
    assert response.json() == {"error": "flood_wait", "retry_after": 30}
    assert response.headers.get("retry-after") == "30"
