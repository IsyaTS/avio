from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from apps.worker import main as worker_module
from apps.api.web import public as public_module
from libs.core import response_pipeline
from libs.core import sales_core as core
from libs.core.integrations import avito as avito_integration
from libs.core.training import retriever as training_retriever


@pytest.fixture(autouse=True)
def _isolated_tenant_files(tmp_path, monkeypatch):
    tenants_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))
    monkeypatch.setenv("PUBLIC_KEY", "test-public-key")
    core._TENANT_CONFIG_CACHE.clear()
    core._TENANT_PERSONA_CACHE.clear()
    try:
        yield tenants_dir
    finally:
        core._TENANT_CONFIG_CACHE.clear()
        core._TENANT_PERSONA_CACHE.clear()


@pytest.fixture
def public_client() -> TestClient:
    app = FastAPI()
    app.include_router(public_module.router)
    return TestClient(app)


@pytest.mark.integration
def test_public_settings_save_preserves_critical_tenant_state_over_http(public_client: TestClient):
    tenant = 91001
    key = "test-public-key"
    core.write_tenant_config(
        tenant,
        {
            "passport": {"tenant_id": tenant, "public_key": key, "brand": "Old"},
            "behavior": {
                "avito_smart_reply_enabled": True,
                "brain_mode": "smart",
            },
            "integrations": {
                "avito": {
                    "access_token": "access-old",
                    "refresh_token": "refresh-old",
                    "account_id": 123,
                }
            },
            "follow_up": [{"text": "later", "delay_minutes": 15}],
        },
    )

    response = public_client.post(
        f"/pub/settings/save?tenant={tenant}&k={key}",
        json={"cfg": {"passport": {"brand": "New"}, "behavior": {"brain_mode": "auto"}}},
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True}

    response = public_client.get(f"/pub/settings/get?tenant={tenant}&k={key}")
    assert response.status_code == 200, response.text
    payload = response.json()
    cfg = payload["cfg"]
    assert cfg["passport"]["brand"] == "New"
    assert cfg["behavior"]["brain_mode"] == "auto"
    assert cfg["behavior"]["avito_smart_reply_enabled"] is True
    assert cfg["integrations"]["avito"]["access_token"] == "access-old"
    assert cfg["integrations"]["avito"]["refresh_token"] == "refresh-old"
    assert cfg["integrations"]["avito"]["account_id"] == 123
    assert cfg["follow_up"] == [{"text": "later", "delay_minutes": 15}]


@pytest.mark.integration
def test_avito_oauth_callback_persists_tokens_with_signed_state_after_redis_loss(
    public_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
):
    tenant = 91003
    key = "test-public-key"
    core.write_tenant_config(
        tenant,
        {
            "passport": {"tenant_id": tenant, "public_key": key},
            "behavior": {"avito_smart_reply_enabled": True},
            "follow_up": [{"text": "later"}],
        },
    )

    class _RedisState:
        def __init__(self):
            self.store: dict[str, str] = {}

        def setex(self, key_name, ttl, value):
            self.store[str(key_name)] = str(value)

        def get(self, key_name):
            return self.store.get(str(key_name))

        def delete(self, key_name):
            self.store.pop(str(key_name), None)

    redis_state = _RedisState()
    captured: dict[str, str] = {}

    def _build_authorize_url(state: str) -> str:
        captured["state"] = state
        return f"https://avito.example/oauth?state={state}"

    async def _exchange(_tenant: int, code: str):
        assert _tenant == tenant
        assert code == "code-1"
        return {
            "access_token": "access-new",
            "refresh_token": "refresh-new",
            "expires_in": 3600,
            "scope": "messenger",
        }

    async def _sync(_tenant: int):
        return {}

    async def _webhook(_tenant: int, _target_url: str):
        return True

    monkeypatch.setattr(public_module.common, "redis_client", lambda: redis_state)
    monkeypatch.setattr(public_module.avito, "build_authorize_url", _build_authorize_url)
    monkeypatch.setattr(public_module.avito, "exchange_code_for_token", _exchange)
    monkeypatch.setattr(public_module.avito, "sync_account_info", _sync)
    monkeypatch.setattr(public_module.avito, "ensure_webhook", _webhook)

    response = public_client.get(
        f"/v1/oauth/avito/authorize?tenant={tenant}&k={key}&redirect=1",
        follow_redirects=False,
    )
    assert response.status_code == 303, response.text
    assert captured["state"]
    assert redis_state.store

    redis_state.store.clear()
    callback = public_client.get(
        f"/v1/oauth/avito/callback?state={captured['state']}&code=code-1"
    )
    assert callback.status_code == 200, callback.text
    assert "ok" in callback.text.lower()

    cfg = core.read_tenant_config(tenant)
    avito_cfg = avito_integration.get_integration(tenant)
    assert avito_cfg is not None
    assert avito_cfg["access_token"] == "access-new"
    assert avito_cfg["refresh_token"] == "refresh-new"
    assert cfg["behavior"]["avito_smart_reply_enabled"] is True
    assert cfg["follow_up"] == [{"text": "later"}]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_learning_examples_from_db_reach_response_pipeline_prompt(
    monkeypatch: pytest.MonkeyPatch,
):
    tenant = 91004
    core.write_tenant_config(
        tenant,
        {
            "passport": {"tenant_id": tenant, "public_key": "test-public-key"},
            "learning": {"enabled": True, "top_k": 2},
        },
    )
    captured: dict[str, object] = {}

    async def _examples(_tenant: int, *, limit: int = 200, require_embedding: bool = False):
        assert _tenant == tenant
        assert require_embedding is False
        assert limit >= 2
        return [
            {
                "id": 501,
                "q_text": "сколько стоит delta 100?",
                "a_text": "Delta 100 стоит от 25 тыс. руб., точная цена зависит от размера.",
                "source": "correction",
                "is_bad": False,
            }
        ]

    async def _increment(ids):
        captured["used_ids"] = list(ids)

    async def _build_llm_messages(*_args, **_kwargs):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(messages, **_kwargs):
        captured["messages"] = messages
        return "pipeline-ok"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    monkeypatch.setattr(training_retriever.db, "get_training_examples_for_retrieval", _examples)
    monkeypatch.setattr(training_retriever.db, "increment_training_examples_usage", _increment)
    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)

    result = await response_pipeline.run_response_pipeline(
        tenant_id=tenant,
        channel="avito",
        user_text="сколько стоит delta 100?",
        contact_id=70001,
    )

    assert result.reply_text == "pipeline-ok"
    assert captured["used_ids"] == [501]
    messages = captured["messages"]
    assert isinstance(messages, list)
    system_prompt = messages[0]["content"]
    assert "Проверенные примеры ответов менеджера для самообучения" in system_prompt
    assert "Delta 100 стоит от 25 тыс руб" in system_prompt


@pytest.mark.integration
@pytest.mark.asyncio
async def test_contextual_case_reaches_prompt_only_when_requires_satisfied(
    monkeypatch: pytest.MonkeyPatch,
):
    tenant = 91005
    core.write_tenant_config(
        tenant,
        {
            "passport": {"tenant_id": tenant, "public_key": "test-public-key"},
            "learning": {
                "enabled": True,
                "contextual_cases": {
                    "enabled": True,
                    "shadow_mode": False,
                    "apply_mode": True,
                    "top_k": 2,
                    "min_score": 0.1,
                },
            },
        },
    )
    captured: dict[str, object] = {}

    async def _domain_schema(_tenant: int):
        assert _tenant == tenant
        return {
            "domain": "lawn_mowing",
            "domain_label": "покос травы",
            "required_slots": ["area_size"],
            "slot_definitions": {"area_size": "площадь участка"},
        }

    async def _cases(_tenant: int, *, limit: int = 500, require_embedding: bool = False):
        return [
            {
                "id": 601,
                "tenant_id": tenant,
                "is_active": True,
                "domain": "lawn_mowing",
                "intent": "price_question",
                "mode": "context_bound",
                "search_text": "покос травы цена 10 соток",
                "context": {"domain": "lawn_mowing", "intent": "price_question", "slots": {"area_size": "10 соток"}},
                "dialog": {
                    "history": [{"role": "client", "text": "Сколько стоит покос 10 соток?"}],
                    "manager_reply": {"role": "manager", "text": "Цена зависит от площади участка."},
                },
                "reply_facts": {"mentions_price": True, "price_specific": True},
                "applicability": {"mode": "context_bound", "requires": ["slots.area_size"]},
            }
        ]

    async def _increment(_ids):
        return None

    async def _build_llm_messages(*_args, **_kwargs):
        return [{"role": "system", "content": "base-system"}]

    async def _ask_llm(messages, **_kwargs):
        captured["messages"] = messages
        return "pipeline-ok"

    async def _policy_ctx(**_kwargs):
        return {"enabled": False, "policy_block": ""}

    from libs.core.repo import contextual_cases as contextual_repo

    monkeypatch.setattr(contextual_repo, "get_active_domain_schema", _domain_schema)
    monkeypatch.setattr(contextual_repo, "list_active_cases_for_retrieval", _cases)
    monkeypatch.setattr(contextual_repo, "increment_contextual_case_usage", _increment)
    monkeypatch.setattr(training_retriever, "build_examples_block_async", lambda *_a, **_k: "")
    monkeypatch.setattr(response_pipeline, "build_llm_messages", _build_llm_messages)
    monkeypatch.setattr(response_pipeline, "ask_llm", _ask_llm)
    monkeypatch.setattr(response_pipeline, "prepare_runtime_policy_hint", _policy_ctx)

    await response_pipeline.run_response_pipeline(
        tenant_id=tenant,
        channel="avito",
        user_text="Сколько стоит покос 10 соток?",
        contact_id=70002,
    )
    assert "Цена зависит от площади участка" in captured["messages"][0]["content"]

    captured.clear()
    await response_pipeline.run_response_pipeline(
        tenant_id=tenant,
        channel="avito",
        user_text="Сколько стоит покос?",
        contact_id=70003,
    )
    prompt = captured["messages"][0]["content"]
    assert "Цена зависит от площади участка" not in prompt
    assert "Сначала уточни" in prompt


@pytest.mark.integration
@pytest.mark.asyncio
async def test_avito_incoming_worker_stores_message_and_enqueues_smart_reply(
    monkeypatch: pytest.MonkeyPatch,
):
    tenant = 91005
    lead_id = 91005001
    core.write_tenant_config(
        tenant,
        {
            "passport": {"tenant_id": tenant, "public_key": "test-public-key"},
            "behavior": {"avito_smart_reply_enabled": True},
            "integrations": {
                "avito": {
                    "access_token": "access",
                    "refresh_token": "refresh",
                    "account_id": 123456,
                }
            },
        },
    )

    class _Redis:
        def __init__(self):
            self.values: dict[str, str] = {}
            self.outbox: list[dict[str, object]] = []

        async def get(self, key):
            return self.values.get(str(key))

        async def set(self, key, value, ex=None):
            self.values[str(key)] = str(value)
            return True

        async def exists(self, key):
            return 1 if str(key) in self.values else 0

        async def lpush(self, key, value):
            if str(key) == worker_module.OUTBOX_QUEUE_KEY:
                self.outbox.append(json.loads(value))
            return len(self.outbox)

    fake_redis = _Redis()
    stored_incoming: list[dict[str, object]] = []

    async def _noop(*_args, **_kwargs):
        return None

    async def _false(*_args, **_kwargs):
        return False

    async def _get_or_create_by_peer(**kwargs):
        assert kwargs["tenant_id"] == tenant
        assert kwargs["channel"] == "avito"
        assert kwargs["peer"] == "chat-91005"
        return lead_id

    async def _lead_exists(_lead_id, _tenant_id):
        assert _lead_id == lead_id
        assert _tenant_id == tenant
        return True

    async def _insert_message_in(_lead_id, text, **kwargs):
        stored_incoming.append({"lead_id": _lead_id, "text": text, **kwargs})
        return 88001

    async def _resolve_contact(**_kwargs):
        return 77001

    async def _pipeline(**kwargs):
        assert kwargs["tenant_id"] == tenant
        assert kwargs["channel"] == "avito"
        assert kwargs["user_text"] == "Есть Delta 100?"
        return SimpleNamespace(reply_text="Да, Delta 100 есть. Цена от 25 000 руб.", source="llm")

    monkeypatch.setattr(worker_module, "r", fake_redis, raising=False)
    monkeypatch.setattr(worker_module, "smart_reply_enabled", lambda _tenant: True)
    monkeypatch.setattr(worker_module, "get_or_create_by_peer", _get_or_create_by_peer)
    monkeypatch.setattr(worker_module, "lead_exists", _lead_exists)
    monkeypatch.setattr(worker_module, "insert_message_in", _insert_message_in)
    monkeypatch.setattr(worker_module, "resolve_or_create_contact", _resolve_contact)
    monkeypatch.setattr(worker_module, "link_lead_contact", _noop)
    monkeypatch.setattr(worker_module, "update_contact_avito_login", _noop)
    monkeypatch.setattr(worker_module.followups, "handle_opt_out", _false)
    monkeypatch.setattr(worker_module.followups, "capture_followup_answer", _noop)
    monkeypatch.setattr(worker_module.followups, "schedule_followups", _noop)
    monkeypatch.setattr(worker_module, "_maybe_amocrm_inbound", _noop)
    monkeypatch.setattr(worker_module, "_match_behavior_trigger", lambda *_a, **_k: None)
    monkeypatch.setattr(worker_module, "_is_handoff_silenced", _false)
    monkeypatch.setattr(worker_module, "run_response_pipeline", _pipeline)

    await worker_module._handle_avito_incoming(
        {
            "tenant": tenant,
            "chat_id": "chat-91005",
            "message_id": "msg-1",
            "text": "Есть Delta 100?",
            "account_id": 123456,
            "avito_user_id": 333,
            "avito_login": "buyer",
        }
    )

    assert stored_incoming == [
        {
            "lead_id": lead_id,
            "text": "Есть Delta 100?",
            "status": "received",
            "tenant_id": tenant,
            "provider_msg_id": "msg-1",
            "source": "incoming",
        }
    ]
    assert len(fake_redis.outbox) == 1
    payload = fake_redis.outbox[0]
    assert payload["tenant"] == tenant
    assert payload["lead_id"] == lead_id
    assert payload["provider"] == "avito"
    assert payload["chat_id"] == "chat-91005"
    assert payload["account_id"] == 123456
    assert payload["text"] == "Да, Delta 100 есть цена от 25 000 руб"
