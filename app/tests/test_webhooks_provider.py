import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.web import webhooks as webhooks_module


class _DummyAsyncRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    async def set(self, key: str, value: str, **kwargs):  # pragma: no cover - helper
        self.store[key] = value


def _build_app():
    app = FastAPI()
    app.include_router(webhooks_module.router)
    return app


def test_provider_webhook_caches_qr(monkeypatch):
    dummy = _DummyAsyncRedis()
    monkeypatch.setattr(webhooks_module, "_redis_queue", dummy, raising=False)
    monkeypatch.setattr(webhooks_module.settings, "WEBHOOK_SECRET", "secret-token", raising=False)

    app = _build_app()
    client = TestClient(app)

    payload = {
        "provider": "whatsapp",
        "event": "wa_qr",
        "tenant": 7,
        "qr_id": "1234567890",
        "svg": "<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>",
    }

    resp = client.post(
        "/webhook/provider",
        json=payload,
        headers={"X-Webhook-Token": "secret-token"},
    )

    assert resp.status_code == 204
    cached_entry = json.loads(dummy.store[f"wa:qr:7:1234567890"])
    assert cached_entry["tenant"] == 7
    assert cached_entry["qr_id"] == "1234567890"
    assert cached_entry["qr_svg"].startswith("<svg")
    assert cached_entry["provider"] == "whatsapp"
    assert cached_entry["event"] == "wa_qr"
    assert isinstance(cached_entry["updated_at"], int)
    assert dummy.store[f"wa:qr:last:7"] == "1234567890"
