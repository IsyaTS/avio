import pytest

import apps.api.web.client as client_mod
from libs.core.training import retriever as training_retriever


pytestmark = pytest.mark.integration


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

    async def create_message_feedback(*_a, **_k):
        return 123

    async def record_training_example(*_a, **_k):
        return None

    async def mark_bad_bot_message(*_a, **_k):
        return None

    async def feedback_exists(*_a, **_k):
        return False

    monkeypatch.setattr(client_mod.db, "create_message_feedback", create_message_feedback)
    monkeypatch.setattr(client_mod.db, "record_training_example", record_training_example)
    monkeypatch.setattr(client_mod.db, "mark_bad_bot_message", mark_bad_bot_message)
    monkeypatch.setattr(client_mod.db, "feedback_exists", feedback_exists)


class _FakeRequest:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return dict(self._payload)


@pytest.mark.asyncio
async def test_feedback_dislike_requires_expected_answer(monkeypatch):
    _stub_db(monkeypatch)
    resp = await client_mod.submit_feedback_api(
        _FakeRequest({"message_id": 1, "rating": "dislike"})
    )
    assert resp.status_code == 400
    assert resp.body
    assert b"expected_answer_required" in resp.body


@pytest.mark.asyncio
async def test_feedback_rejects_foreign_tenant(monkeypatch):
    _stub_db(monkeypatch, tenant=99)
    resp = await client_mod.submit_feedback_api(_FakeRequest({"message_id": 1, "rating": "like"}))
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

    monkeypatch.setattr(
        training_retriever,
        "_read_tenant_config",
        lambda _tenant: {"learning": {"enabled": True}},
    )
    monkeypatch.setattr(training_retriever.db, "get_training_examples_for_retrieval", fake_get_examples)
    monkeypatch.setattr(training_retriever.db, "increment_training_examples_usage", fake_increment)

    results = await training_retriever.retrieve_examples_async(1, "hello", k=2)
    assert len(results) == 1
    assert results[0].q.startswith("hello")


@pytest.mark.asyncio
async def test_retriever_skips_low_similarity_db_examples(monkeypatch):
    async def fake_get_examples(_tenant, **_kw):
        return [
            {
                "id": 10,
                "q_text": "какая цена двери",
                "a_text": "Дверь стоит от 25000 рублей.",
                "source": "correction",
                "embedding": [1.0, 0.0],
                "is_bad": False,
            }
        ]

    async def fake_increment(ids):
        assert ids == []

    async def fake_embed_query(_query):
        return [0.05, 0.998]

    monkeypatch.setattr(
        training_retriever,
        "_read_tenant_config",
        lambda _tenant: {"learning": {"enabled": True, "min_score": 0.2}},
    )
    monkeypatch.setattr(training_retriever.db, "get_training_examples_for_retrieval", fake_get_examples)
    monkeypatch.setattr(training_retriever.db, "increment_training_examples_usage", fake_increment)

    from libs.core.training import embeddings as embeddings_mod

    monkeypatch.setattr(embeddings_mod, "embed_query", fake_embed_query)
    monkeypatch.setattr(training_retriever, "retrieve_examples", lambda *_a, **_k: [])

    results = await training_retriever.retrieve_examples_async(1, "цена", k=1)

    assert results == []
