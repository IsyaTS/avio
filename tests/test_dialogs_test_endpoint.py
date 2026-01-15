from fastapi.testclient import TestClient

from apps.api import main as app_main
import apps.api.web.client as client_mod
from libs.core.response_pipeline import PipelineResult


def test_dialogs_test_endpoint(monkeypatch):
    async def fake_pipeline(*args, **kwargs):
        return PipelineResult(reply_text="тестовый ответ")

    monkeypatch.setattr(client_mod, "run_response_pipeline", fake_pipeline)

    client = TestClient(app_main.app)
    response = client.post(
        "/api/dialogs/test?tenant=1&k=test-public-key",
        json={
            "text": "Привет",
            "channel": "telegram",
            "history": [{"role": "user", "text": "Здравствуйте"}],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True
    assert payload.get("reply") == "тестовый ответ"
