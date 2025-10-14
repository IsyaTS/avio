import pytest
from fastapi.testclient import TestClient

from tgworker.api import create_app
from tgworker.manager import LoginFlowStateSnapshot, TwoFASubmitResult


@pytest.fixture
def tgworker_client(monkeypatch):
    class DummyManager:
        def __init__(self):
            self.result: TwoFASubmitResult | None = None
            self.snapshot = type(
                "Snapshot",
                (),
                {
                    "twofa_backoff_until": None,
                    "to_payload": lambda self: {
                        "status": "authorized",
                        "qr_id": None,
                        "qr_valid_until": None,
                        "twofa_pending": False,
                        "needs_2fa": False,
                    },
                },
            )()

        async def start(self):  # pragma: no cover - startup hook
            return None

        async def shutdown(self):  # pragma: no cover - shutdown hook
            return None

        async def submit_password(self, tenant_id: int, password: str) -> TwoFASubmitResult:
            assert tenant_id == 1
            assert password == "secret"
            assert self.result is not None
            return self.result

        async def get_status(self, tenant_id: int):
            assert tenant_id == 1
            return self.snapshot

        async def login_flow_state(self, tenant_id: int):
            assert tenant_id == 1
            return LoginFlowStateSnapshot(
                tenant_id=tenant_id,
                status="idle",
                qr_id=None,
                qr_login_obj=None,
                qr_png=None,
                qr_expires_at=None,
                last_error=None,
                needs_2fa=False,
                twofa_pending=False,
            )

    dummy = DummyManager()
    monkeypatch.setattr("tgworker.api.SessionManager", lambda *args, **kwargs: dummy)
    app = create_app()
    with TestClient(app) as client:
        yield client, dummy


def test_twofa_submit_invalid_password(tgworker_client):
    client, manager = tgworker_client
    manager.result = TwoFASubmitResult(
        status_code=400,
        body={"error": "password_invalid", "detail": "password_invalid"},
    )

    response = client.post("/rpc/twofa.submit", json={"tenant_id": 1, "password": "secret"})

    assert response.status_code == 400
    assert response.json() == {"error": "password_invalid", "detail": "password_invalid"}


def test_twofa_submit_srp_invalid(tgworker_client):
    client, manager = tgworker_client
    manager.result = TwoFASubmitResult(
        status_code=409,
        body={"error": "srp_invalid", "detail": "srp_invalid"},
    )

    response = client.post("/rpc/twofa.submit", json={"tenant_id": 1, "password": "secret"})

    assert response.status_code == 409
    assert response.json() == {"error": "srp_invalid", "detail": "srp_invalid"}


def test_twofa_submit_flood_wait(tgworker_client):
    client, manager = tgworker_client
    manager.snapshot.twofa_backoff_until = 1700
    manager.result = TwoFASubmitResult(
        status_code=429,
        body={"error": "flood_wait", "retry_after": 30, "detail": "flood_wait 30"},
        headers={"Retry-After": "30"},
    )

    response = client.post("/rpc/twofa.submit", json={"tenant_id": 1, "password": "secret"})

    assert response.status_code == 429
    assert response.json() == {
        "error": "flood_wait",
        "retry_after": 30,
        "detail": "flood_wait 30",
        "backoff_until": 1700,
    }
    assert response.headers.get("retry-after") == "30"
