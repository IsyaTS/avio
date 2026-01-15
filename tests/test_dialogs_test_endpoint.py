from fastapi.testclient import TestClient

from apps.api import main as app_main
import apps.api.web.client as client_mod


def test_dialogs_test_endpoint(monkeypatch):
    async def fake_build_llm_messages(*args, **kwargs):
        return [{"role": "system", "content": "system"}]

    async def fake_ask_llm(*args, **kwargs):
        return "тестовый ответ"

    monkeypatch.setattr(client_mod, "build_llm_messages", fake_build_llm_messages)
    monkeypatch.setattr(client_mod, "ask_llm", fake_ask_llm)

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
