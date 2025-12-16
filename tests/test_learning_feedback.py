import pytest
from starlette.testclient import TestClient

from apps.api.main import app
import apps.api.web.client as client_mod
from libs.core.training import retriever as training_retriever


@pytest.fixture(autouse=True)
def stub_auth(monkeypatch):
    monkeypatch.setattr(client_mod, "_resolve_tenant_and_key", lambda request, tenant: (1, "k"))


def _stub_db(monkeypatch, *, tenant: int = 1):
    async def get_message_metadata(message_id: int):
        return {
            "id": message_id,
            "lead_id": 11,
            "tenant_id": tenant,
            "direction": 1,
            "is_bot": True,
            "created_at": "2024-01-01T00:00:00Z",
            "text": "bot answer",
        }

    monkeypatch.setattr(client_mod.db, "get_message_metadata", get_message_metadata)
    monkeypatch.setattr(client_mod.db, "get_previous_incoming_message", lambda *_a, **_k: None)
    monkeypatch.setattr(client_mod.db, "create_message_feedback", lambda *_a, **_k: 123)
    monkeypatch.setattr(client_mod.db, "record_training_example", lambda *_a, **_k: None)
    monkeypatch.setattr(client_mod.db, "mark_bad_bot_message", lambda *_a, **_k: None)


def test_feedback_dislike_requires_expected_answer(monkeypatch):
    _stub_db(monkeypatch)
    client = TestClient(app)
    resp = client.post("/api/feedback", json={"message_id": 1, "rating": "dislike"})
    assert resp.status_code == 400
    assert resp.json().get("detail") == "expected_answer_required"


def test_feedback_rejects_foreign_tenant(monkeypatch):
    _stub_db(monkeypatch, tenant=99)
    client = TestClient(app)
    resp = client.post("/api/feedback", json={"message_id": 1, "rating": "like"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_retriever_skips_bad_examples(monkeypatch):
    async def fake_get_examples(_tenant, **_kw):
        return [
            {"id": 1, "q_text": "hello", "a_text": "world", "source": "like", "embedding": None},
            {"id": 2, "q_text": "ignored", "a_text": "bad", "source": "like", "embedding": None, "is_bad": True},
        ]

    async def fake_increment(_ids):
        return None

    monkeypatch.setattr(training_retriever.db, "get_training_examples_for_retrieval", fake_get_examples)
    monkeypatch.setattr(training_retriever.db, "increment_training_examples_usage", fake_increment)

    results = await training_retriever.retrieve_examples_async(1, "hello", k=2)
    assert len(results) == 1
    assert results[0].q.startswith("hello")
