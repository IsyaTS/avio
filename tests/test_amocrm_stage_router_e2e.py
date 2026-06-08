from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from libs.core.services import amocrm as amocrm_service


@dataclass
class _LeadState:
    stage_index: int = 0
    inbound_count: int = 0
    provider_lead_id: int = 0
    provider_contact_id: int = 0
    fields: dict[str, str] = field(default_factory=dict)
    history: list[str] = field(default_factory=list)


class _RouterHarness:
    def __init__(self, *, mode: str, cooldown_seconds: int = 0) -> None:
        self.mode = mode
        self.cooldown_seconds = cooldown_seconds
        self.events: list[tuple[int, str, dict[str, Any]]] = []
        self._lead_map: dict[int, _LeadState] = {}
        self._provider_to_lead: dict[int, int] = {}
        self._decision_queue: list[dict[str, Any]] = []
        self.cfg = {
            "integrations": {
                "amocrm": {
                    "enabled": True,
                    "pipeline_id": 991001,
                    "stages": [
                        {"name": "Новый", "amo_stage_id": 771001, "type": "open", "hints": ["новый диалог"]},
                        {
                            "name": "Контакт подтверждён",
                            "amo_stage_id": 771002,
                            "type": "open",
                            "hints": ["есть контакт клиента или явный способ связи"],
                        },
                        {
                            "name": "Потребность уточнена",
                            "amo_stage_id": 771003,
                            "type": "open",
                            "hints": ["клиент описал задачу или параметры запроса"],
                        },
                        {
                            "name": "Следующий шаг согласован",
                            "amo_stage_id": 771004,
                            "type": "open",
                            "hints": ["согласованы дата, действие или следующий шаг"],
                        },
                    ],
                    "rules_options": {
                        "stage_router_mode": mode,
                        "stage_router_max_stage_jump": 1,
                        "stage_router_confidence_auto": 0.7,
                        "stage_router_confidence_semi": 0.4,
                        "stage_router_cooldown_seconds": cooldown_seconds,
                    },
                    "notes": {"enabled": False, "all_messages": False},
                }
            }
        }

    def reset_lead(self, lead_id: int) -> None:
        state = _LeadState(
            stage_index=0,
            inbound_count=0,
            provider_lead_id=100000 + int(lead_id),
            provider_contact_id=200000 + int(lead_id),
        )
        self._lead_map[int(lead_id)] = state
        self._provider_to_lead[state.provider_lead_id] = int(lead_id)

    def stage_index(self, lead_id: int) -> int:
        return int(self._lead_map[int(lead_id)].stage_index)

    def queue_decision(self, decision: dict[str, Any]) -> None:
        self._decision_queue.append(dict(decision))

    def _state(self, lead_id: int) -> _LeadState:
        lid = int(lead_id)
        if lid not in self._lead_map:
            self.reset_lead(lid)
        return self._lead_map[lid]

    async def token_get(self, *_args, **_kwargs) -> Any:
        return SimpleNamespace(access_token="token", refresh_token=None, raw_payload={})

    async def resolve_api_base_url(self, *_args, **_kwargs) -> str:
        return "https://example.amocrm.ru"

    class FakeAmoClient:
        def __init__(self, harness: "_RouterHarness", **_kwargs: Any) -> None:
            self._harness = harness

        async def get_lead(self, provider_lead_id: int) -> dict[str, Any]:
            lead_id = self._harness._provider_to_lead.get(int(provider_lead_id))
            if lead_id is None:
                return {"status_id": 771001}
            idx = self._harness._state(lead_id).stage_index
            stages = self._harness.cfg["integrations"]["amocrm"]["stages"]
            return {"status_id": int(stages[idx]["amo_stage_id"])}

    async def get_link(self, _tenant_id: int, lead_id: int, _provider: str) -> dict[str, Any]:
        state = self._state(lead_id)
        return {
            "provider_lead_id": state.provider_lead_id,
            "provider_contact_id": state.provider_contact_id,
            "stage_index": state.stage_index,
            "inbound_count": state.inbound_count,
        }

    async def increment_inbound_count(
        self,
        _tenant_id: int,
        lead_id: int,
        _provider: str,
        *,
        pipeline_id: int | None = None,
    ) -> dict[str, Any]:
        _ = pipeline_id
        state = self._state(lead_id)
        state.inbound_count += 1
        return {
            "provider_lead_id": state.provider_lead_id,
            "provider_contact_id": state.provider_contact_id,
            "stage_index": state.stage_index,
            "inbound_count": state.inbound_count,
        }

    async def update_stage_index(
        self,
        _tenant_id: int,
        lead_id: int,
        _provider: str,
        stage_index: int,
        *,
        pipeline_id: int | None = None,
    ) -> None:
        _ = pipeline_id
        self._state(lead_id).stage_index = int(stage_index)

    async def list_fields(self, _tenant_id: int, lead_id: int, _provider: str) -> list[dict[str, Any]]:
        state = self._state(lead_id)
        return [
            {"field_key": key, "field_value": value}
            for key, value in state.fields.items()
        ]

    async def upsert_field(
        self,
        _tenant_id: int,
        lead_id: int,
        _provider: str,
        *,
        field_key: str,
        field_value: str,
        amo_field_id: int | None = None,
    ) -> None:
        _ = amo_field_id
        self._state(lead_id).fields[str(field_key)] = str(field_value)

    async def enqueue(
        self,
        _tenant_id: int,
        _provider: str,
        lead_id: int,
        event: str,
        payload: dict[str, Any],
    ) -> None:
        self.events.append((int(lead_id), str(event), dict(payload)))
        if event == "move_stage":
            stage_index = payload.get("stage_index")
            if stage_index is not None:
                self._state(lead_id).stage_index = int(stage_index)

    async def has_recent_event(self, *_args, **_kwargs) -> bool:
        return False

    async def has_recent_event_type(self, *_args, **_kwargs) -> bool:
        return False

    async def cancel_pending_events(self, *_args, **_kwargs) -> None:
        return None

    async def list_recent_inbound_texts(self, _tenant_id: int, lead_id: int, *, limit: int = 6) -> list[str]:
        state = self._state(lead_id)
        items = state.history[-max(1, int(limit)):]
        return list(items)

    async def list_recent_stage_router_texts(self, _tenant_id: int, lead_id: int, *, limit: int = 6) -> list[str]:
        state = self._state(lead_id)
        items = state.history[-max(1, int(limit)):]
        return [f"client: {text}" for text in items]

    async def decide_next_stage(self, *_args, **_kwargs) -> dict[str, Any] | None:
        if self._decision_queue:
            return self._decision_queue.pop(0)
        return {
            "action": "NOOP",
            "target_stage_index": -1,
            "confidence": 0.0,
            "reason": "no_decision",
            "missing_fields": [],
            "evidence": [],
        }

    async def send_inbound(self, lead_id: int, text: str) -> None:
        state = self._state(lead_id)
        state.history.append(str(text))
        await amocrm_service._amocrm_on_message(
            101,
            int(lead_id),
            text=str(text),
            channel="avito",
            direction="in",
            attachments=[],
        )

    async def send_outbound_manager(self, lead_id: int, text: str) -> None:
        state = self._state(lead_id)
        state.history.append(str(text))
        await amocrm_service._amocrm_on_message(
            101,
            int(lead_id),
            text=str(text),
            channel="avito",
            direction="out",
            source_role="manager",
            attachments=[],
        )


def _apply_router_patches(monkeypatch: pytest.MonkeyPatch, harness: _RouterHarness) -> None:
    monkeypatch.setattr(amocrm_service.core_module, "read_tenant_config", lambda _tenant: harness.cfg)
    monkeypatch.setattr(amocrm_service.amocrm_chat, "is_enabled", lambda *_a, **_k: False)
    monkeypatch.setattr(amocrm_service.amocrm_tokens, "get", harness.token_get)
    monkeypatch.setattr(amocrm_service, "resolve_api_base_url", harness.resolve_api_base_url)
    monkeypatch.setattr(
        amocrm_service.amocrm_core,
        "AmoCRMClient",
        lambda **kwargs: _RouterHarness.FakeAmoClient(harness, **kwargs),
    )
    monkeypatch.setattr(amocrm_service, "ensure_lead_phone_field_id", _async_noop)
    monkeypatch.setattr(amocrm_service, "_remote_entity_exists", _async_true)
    monkeypatch.setattr(amocrm_service, "_resolve_lead_names", _async_name)
    monkeypatch.setattr(amocrm_service.crm_links, "get_link", harness.get_link)
    monkeypatch.setattr(amocrm_service.crm_links, "increment_inbound_count", harness.increment_inbound_count)
    monkeypatch.setattr(amocrm_service.crm_links, "update_stage_index", harness.update_stage_index)
    monkeypatch.setattr(amocrm_service.crm_fields, "list_fields", harness.list_fields)
    monkeypatch.setattr(amocrm_service.crm_fields, "upsert_field", harness.upsert_field)
    monkeypatch.setattr(amocrm_service.crm_outbox, "enqueue", harness.enqueue)
    monkeypatch.setattr(amocrm_service.crm_outbox, "has_recent_event", harness.has_recent_event)
    monkeypatch.setattr(amocrm_service.crm_outbox, "has_recent_event_type", harness.has_recent_event_type)
    monkeypatch.setattr(amocrm_service.crm_outbox, "cancel_pending_events", harness.cancel_pending_events)
    monkeypatch.setattr(amocrm_service.db_module, "list_recent_inbound_texts", harness.list_recent_inbound_texts)
    monkeypatch.setattr(
        amocrm_service.db_module,
        "list_recent_stage_router_texts",
        harness.list_recent_stage_router_texts,
    )
    monkeypatch.setattr(amocrm_service, "_decide_next_stage_llm", harness.decide_next_stage)


async def _async_noop(*_args, **_kwargs) -> None:
    return None


async def _async_true(*_args, **_kwargs) -> bool:
    return True


async def _async_name(*_args, **kwargs) -> tuple[str, str]:
    lead_id = int(kwargs.get("lead_id") or 0)
    return (f"Lead #{lead_id}", f"Lead #{lead_id}")


@pytest.mark.parametrize(
    ("message", "evidence"),
    [
        ("мой telegram @buyer_flow", ["@buyer_flow"]),
        ("мой контакт +79990001122", ["79990001122"]),
        ("можно связаться по @client_sync, так удобнее", ["@client_sync"]),
    ],
)
def test_e2e_auto_moves_stage_on_supported_contact_evidence(
    monkeypatch: pytest.MonkeyPatch,
    message: str,
    evidence: list[str],
) -> None:
    harness = _RouterHarness(mode="auto", cooldown_seconds=0)
    _apply_router_patches(monkeypatch, harness)
    lead_id = 41001
    harness.reset_lead(lead_id)
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.95,
            "reason": "контакт подтверждён",
            "missing_fields": [],
            "evidence": evidence,
        }
    )

    asyncio.run(harness.send_inbound(lead_id, message))

    assert harness.stage_index(lead_id) == 1
    assert any(event == "move_stage" for lid, event, _ in harness.events if lid == lead_id)


def test_e2e_auto_blocks_wrong_transition_then_resets_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _RouterHarness(mode="auto", cooldown_seconds=0)
    _apply_router_patches(monkeypatch, harness)
    lead_id = 42002
    harness.reset_lead(lead_id)

    # Wrong LLM decision: no supported evidence for stage move.
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.98,
            "reason": "move",
            "missing_fields": [],
            "evidence": ["контакт клиента подтверждён"],
        }
    )
    asyncio.run(harness.send_inbound(lead_id, "привет, хочу узнать цену без контактов"))
    assert harness.stage_index(lead_id) == 0

    # Context reset and retest from clean lead state.
    harness.reset_lead(lead_id)
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.93,
            "reason": "контакт подтверждён",
            "missing_fields": [],
            "evidence": ["@retry_lead"],
        }
    )
    asyncio.run(harness.send_inbound(lead_id, "пишите в telegram @retry_lead"))
    assert harness.stage_index(lead_id) == 1


def test_e2e_auto_blocks_jump_and_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _RouterHarness(mode="auto", cooldown_seconds=3600)
    _apply_router_patches(monkeypatch, harness)
    lead_id = 43003
    harness.reset_lead(lead_id)

    # Step 1: valid move 0 -> 1.
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.94,
            "reason": "контакт есть",
            "missing_fields": [],
            "evidence": ["@cool43003"],
        }
    )
    asyncio.run(harness.send_inbound(lead_id, "мой telegram @cool43003"))
    assert harness.stage_index(lead_id) == 1

    # Step 2: jump 1 -> 3 should be blocked by max_stage_jump=1.
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 3,
            "confidence": 0.99,
            "reason": "jump",
            "missing_fields": [],
            "evidence": ["завтра в 12:00 подтверждаю"],
        }
    )
    asyncio.run(harness.send_inbound(lead_id, "ок, давайте завтра в 12:00"))
    assert harness.stage_index(lead_id) == 1

    # Step 3: valid 1 -> 2 is blocked by cooldown.
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 2,
            "confidence": 0.95,
            "reason": "потребность описана",
            "missing_fields": [],
            "evidence": ["нужен расчет на команду из 15 человек"],
        }
    )
    asyncio.run(harness.send_inbound(lead_id, "нужен расчет на команду из 15 человек"))
    assert harness.stage_index(lead_id) == 1


def test_e2e_auto_blocks_move_when_stage_hints_not_met(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _RouterHarness(mode="auto", cooldown_seconds=0)
    _apply_router_patches(monkeypatch, harness)
    lead_id = 43004
    harness.reset_lead(lead_id)

    # Move to stage #2 first (contact confirmed)
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.95,
            "reason": "контакт есть",
            "missing_fields": [],
            "evidence": ["@hints_case"],
        }
    )
    asyncio.run(harness.send_inbound(lead_id, "удобно в @hints_case"))
    assert harness.stage_index(lead_id) == 1

    # Move to stage #3 (need clarified)
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 2,
            "confidence": 0.93,
            "reason": "потребность ясна",
            "missing_fields": [],
            "evidence": ["описываю задачу: нужен пакет на 20 мест"],
        }
    )
    asyncio.run(harness.send_inbound(lead_id, "описываю задачу: нужен пакет на 20 мест"))
    assert harness.stage_index(lead_id) == 2

    # Wrong attempt: LLM proposes moving to stage #4 without date/time agreement.
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 3,
            "confidence": 0.99,
            "reason": "прыжок в финал",
            "missing_fields": [],
            "evidence": ["финал"],
        }
    )
    asyncio.run(harness.send_inbound(lead_id, "давайте просто дальше"))
    assert harness.stage_index(lead_id) == 2


def test_e2e_semi_auto_adds_note_without_stage_move(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _RouterHarness(mode="semi_auto", cooldown_seconds=0)
    _apply_router_patches(monkeypatch, harness)
    lead_id = 44004
    harness.reset_lead(lead_id)
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.91,
            "reason": "контакт подтверждён",
            "missing_fields": [],
            "evidence": ["@semi_auto_flow"],
        }
    )

    asyncio.run(harness.send_inbound(lead_id, "удобно общаться в @semi_auto_flow"))

    assert harness.stage_index(lead_id) == 0
    assert any(
        event == "add_note" and str(payload.get("text", "")).startswith("[AI stage suggestion]")
        for lid, event, payload in harness.events
        if lid == lead_id
    )


def test_e2e_auto_allows_manager_outbound_transition_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _RouterHarness(mode="auto", cooldown_seconds=0)
    _apply_router_patches(monkeypatch, harness)
    lead_id = 45005
    harness.reset_lead(lead_id)
    harness.queue_decision(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.95,
            "reason": "условия отправлены менеджером",
            "missing_fields": [],
            "evidence": ["скидка 2000 ₽", "контакт @sales"],
            "stage_checks": [
                {
                    "target_stage_index": 1,
                    "ready": True,
                    "confidence": 0.95,
                    "reason": "условия отправлены менеджером",
                    "missing_fields": [],
                    "evidence": [
                        {"quote": "скидка 2000 ₽", "source_role": "manager", "is_new": True},
                    ],
                }
            ],
        }
    )

    asyncio.run(harness.send_outbound_manager(lead_id, "скидка 2000 ₽, напишите в @sales"))

    assert harness.stage_index(lead_id) == 1
    assert any(event == "move_stage" for lid, event, _ in harness.events if lid == lead_id)
