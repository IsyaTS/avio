from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from libs.core.services import amocrm as amocrm_service


def test_normalize_rules_options_defaults() -> None:
    options = amocrm_service._normalize_rules_options({})
    assert options["stage_router_mode"] in {"auto", "semi_auto", "off"}
    assert 0.0 <= float(options["stage_router_confidence_auto"]) <= 1.0
    assert 0.0 <= float(options["stage_router_confidence_semi"]) <= 1.0
    assert int(options["stage_router_max_stage_jump"]) >= 1
    assert int(options["stage_router_move_dedup_seconds"]) >= 0


def test_normalize_rules_options_custom_values() -> None:
    cfg = {
        "rules_options": {
            "stage_router_mode": "semi-auto",
            "stage_router_confidence_auto": 0.81,
            "stage_router_confidence_semi": 0.52,
            "stage_router_cooldown_seconds": 420,
            "stage_router_max_stage_jump": 2,
            "stage_router_allow_terminal_auto": True,
        }
    }
    options = amocrm_service._normalize_rules_options(cfg)
    assert options["stage_router_mode"] == "semi_auto"
    assert options["stage_router_confidence_auto"] == pytest.approx(0.81, abs=1e-6)
    assert options["stage_router_confidence_semi"] == pytest.approx(0.52, abs=1e-6)
    assert options["stage_router_cooldown_seconds"] == 420
    assert options["stage_router_max_stage_jump"] == 2
    assert options["stage_router_allow_terminal_auto"] is True


class _FakeMessage:
    def __init__(self, content: str) -> None:
        self.content = content


class _FakeChoice:
    def __init__(self, content: str) -> None:
        self.message = _FakeMessage(content)


class _FakeResponse:
    def __init__(self, content: str) -> None:
        self.choices = [_FakeChoice(content)]


class _FakeCompletions:
    def __init__(self, content: str) -> None:
        self._content = content

    def create(self, **_: object) -> _FakeResponse:
        return _FakeResponse(self._content)


class _FakeChat:
    def __init__(self, content: str) -> None:
        self.completions = _FakeCompletions(content)


class _FakeClient:
    def __init__(self, content: str) -> None:
        self.chat = _FakeChat(content)


def test_decide_next_stage_llm_accepts_allowed_target(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = json.dumps(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.88,
            "reason": "qualified",
            "missing_fields": [],
            "evidence": ["Город: Уфа"],
        }
    )
    monkeypatch.setattr(
        amocrm_service.core_module, "_get_openai_client", lambda: _FakeClient(payload)
    )
    stages = [
        {"name": "Первичный", "amo_stage_id": 10, "type": "open"},
        {"name": "Квалификация", "amo_stage_id": 11, "type": "open"},
        {"name": "Сделка", "amo_stage_id": 12, "type": "open"},
    ]
    decision = asyncio.run(
        amocrm_service._decide_next_stage_llm(
            stages,
            current_stage_index=0,
            inbound_count=3,
            last_text="Готов продолжить",
            history_text="...",
            extracted_fields={},
            options={"stage_router_max_stage_jump": 1, "stage_router_timeout_seconds": 2},
        )
    )
    assert decision is not None
    assert decision["action"] == "MOVE_STAGE"
    assert decision["target_stage_index"] == 1


def test_decide_next_stage_llm_rejects_out_of_range_target(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = json.dumps(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 3,
            "confidence": 0.9,
            "reason": "jump",
            "missing_fields": [],
            "evidence": ["подтверждение"],
        }
    )
    monkeypatch.setattr(
        amocrm_service.core_module, "_get_openai_client", lambda: _FakeClient(payload)
    )
    stages = [
        {"name": "Первичный", "amo_stage_id": 10, "type": "open"},
        {"name": "Квалификация", "amo_stage_id": 11, "type": "open"},
        {"name": "Сделка", "amo_stage_id": 12, "type": "open"},
    ]
    decision = asyncio.run(
        amocrm_service._decide_next_stage_llm(
            stages,
            current_stage_index=0,
            inbound_count=2,
            last_text="Можно дальше",
            history_text="...",
            extracted_fields={},
            options={"stage_router_max_stage_jump": 1, "stage_router_timeout_seconds": 2},
        )
    )
    assert decision is not None
    assert decision["action"] == "NOOP"


def test_decide_next_stage_llm_rejects_move_with_missing_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps(
        {
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.99,
            "reason": "можно двигать",
            "missing_fields": ["город"],
            "evidence": ["цена интересует"],
        }
    )
    monkeypatch.setattr(
        amocrm_service.core_module, "_get_openai_client", lambda: _FakeClient(payload)
    )
    stages = [
        {"name": "Новый", "amo_stage_id": 10, "type": "open"},
        {"name": "Город уточнён", "amo_stage_id": 11, "type": "open"},
    ]
    decision = asyncio.run(
        amocrm_service._decide_next_stage_llm(
            stages,
            current_stage_index=0,
            inbound_count=1,
            last_text="Сколько стоит дверь?",
            history_text="лид: сколько стоит дверь",
            extracted_fields={},
            options={"stage_router_max_stage_jump": 1, "stage_router_timeout_seconds": 2},
        )
    )
    assert decision is not None
    assert decision["action"] == "NOOP"
    assert decision["reason"] == "move_with_missing_fields"


def test_parse_json_object_forgiving_accepts_fenced_json() -> None:
    raw = """```json
{"action":"NOOP","target_stage_index":-1,"confidence":0.2,"reason":"insufficient"}
```"""
    parsed = amocrm_service._parse_json_object_forgiving(raw)
    assert isinstance(parsed, dict)
    assert parsed.get("action") == "NOOP"


def test_parse_json_object_forgiving_extracts_first_object() -> None:
    raw = 'text prefix {"action":"MOVE_STAGE","target_stage_index":1,"confidence":0.9,"reason":"ok"} tail'
    parsed = amocrm_service._parse_json_object_forgiving(raw)
    assert isinstance(parsed, dict)
    assert parsed.get("target_stage_index") == 1


def test_supported_evidence_allows_fuzzy_overlap() -> None:
    supported = amocrm_service._supported_evidence(
        ["клиент назвал город уфа и уточнил адрес установки"],
        last_text="мы из Уфы, адрес Менделеева 80",
        history_text="",
    )
    assert supported


def test_flatten_stage_hints_reads_amocrm_descriptions_payload() -> None:
    hints = amocrm_service._flatten_stage_hints(
        [
            {"for": "novice", "description": "Переводите только после подтверждения города"},
            {"for": "candidate", "description": "Нужен факт от клиента про город"},
            {"for": "master", "description": "Без факта не переводить"},
        ]
    )
    joined = " ".join(hints).lower()
    assert "подтверждения города" in joined
    assert "без факта" in joined


def test_merge_stages_for_pipeline_restores_hints_from_pipeline_cache() -> None:
    amocrm_cfg = {
        "pipeline_id": 10401938,
        "stages": [
            {"name": "Новый чат", "amo_stage_id": 82213562, "type": "", "hints": []},
            {"name": "Город уточнён", "amo_stage_id": 82213570, "type": "", "hints": []},
        ],
        "stages_by_pipeline": {
            "10401938": {
                "stages": [
                    {"name": "Новый чат", "amo_stage_id": 82213562, "type": "", "hints": []},
                    {
                        "name": "Город уточнён",
                        "amo_stage_id": 82213570,
                        "type": "",
                        "hints": ["клиент уже назвал город"],
                    },
                ]
            }
        },
    }
    merged = amocrm_service._merge_stages_for_pipeline(
        amocrm_cfg["stages"],
        amocrm_cfg,
        10401938,
    )
    assert merged[1]["hints"] == ["клиент уже назвал город"]


def test_ensure_pipeline_config_cached_path_restores_hints(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = {
        "integrations": {
            "amocrm": {
                "enabled": True,
                "pipeline_id": 10401938,
                "stages_synced_at": int(10**9),
                "stages": [
                    {"name": "Новый чат", "amo_stage_id": 82213562, "type": "", "hints": []},
                    {"name": "Город уточнён", "amo_stage_id": 82213570, "type": "", "hints": []},
                ],
                "stages_by_pipeline": {
                    "10401938": {
                        "stages": [
                            {
                                "name": "Новый чат",
                                "amo_stage_id": 82213562,
                                "type": "",
                                "hints": [],
                            },
                            {
                                "name": "Город уточнён",
                                "amo_stage_id": 82213570,
                                "type": "",
                                "hints": ["клиент назвал город"],
                            },
                        ],
                        "synced_at": int(10**9),
                    }
                },
            }
        }
    }
    writes: list[dict] = []
    monkeypatch.setattr(amocrm_service.time, "time", lambda: int(10**9) + 30)
    monkeypatch.setattr(
        amocrm_service.core_module, "write_tenant_config", lambda tenant, data: writes.append(data)
    )

    result = asyncio.run(
        amocrm_service.ensure_pipeline_config(
            101,
            cfg,
            client=SimpleNamespace(),
        )
    )

    assert result is not None
    pipeline_id, stages = result
    assert pipeline_id == 10401938
    assert stages[1]["hints"] == ["клиент назвал город"]
    assert writes
    saved_amocrm = writes[-1]["integrations"]["amocrm"]
    assert saved_amocrm["stages"][1]["hints"] == ["клиент назвал город"]


def _base_amocrm_cfg(mode: str) -> dict:
    return {
        "integrations": {
            "amocrm": {
                "enabled": True,
                "pipeline_id": 700001,
                "stages": [
                    {
                        "name": "Новый",
                        "amo_stage_id": 11001,
                        "type": "open",
                        "hints": ["первичный контакт"],
                    },
                    {
                        "name": "Город уточнён",
                        "amo_stage_id": 11002,
                        "type": "open",
                        "hints": ["клиент назвал город"],
                    },
                ],
                "rules_options": {
                    "stage_router_mode": mode,
                    "stage_router_cooldown_seconds": 0,
                    "stage_router_max_stage_jump": 1,
                    "stage_router_confidence_auto": 0.7,
                    "stage_router_confidence_semi": 0.4,
                },
                "notes": {
                    "enabled": False,
                    "all_messages": False,
                },
            }
        }
    }


def _patch_amocrm_router_context(
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str,
    llm_decision: dict,
    supported_evidence: list[str],
) -> list[tuple[str, dict]]:
    cfg = _base_amocrm_cfg(mode)
    events: list[tuple[str, dict]] = []

    class _FakeAmoClient:
        def __init__(self, **_: object) -> None:
            return None

        async def get_lead(self, _lead_id: int) -> dict:
            return {"status_id": 11001}

    async def _noop_async(*_args, **_kwargs):
        return None

    async def _fake_get_link(*_args, **_kwargs):
        return {
            "provider_lead_id": 555001,
            "provider_contact_id": 666001,
            "stage_index": 0,
            "inbound_count": 1,
        }

    async def _fake_increment(*_args, **_kwargs):
        return {
            "provider_lead_id": 555001,
            "provider_contact_id": 666001,
            "stage_index": 0,
            "inbound_count": 2,
        }

    async def _fake_enqueue(_tenant: int, _provider: str, _lead: int, event: str, payload: dict):
        events.append((event, dict(payload)))

    async def _fake_resolve_api_base_url(*_args, **_kwargs):
        return "https://example.amocrm.ru"

    async def _fake_decide(*_args, **_kwargs):
        return dict(llm_decision)

    async def _fake_recent_texts(*_args, **_kwargs):
        return ["Сколько стоит", "Я из Уфы"]

    monkeypatch.setattr(amocrm_service.core_module, "read_tenant_config", lambda _tenant: cfg)
    monkeypatch.setattr(amocrm_service.amocrm_chat, "is_enabled", lambda *_a, **_k: False)
    monkeypatch.setattr(
        amocrm_service.amocrm_tokens,
        "get",
        _as_async_return(SimpleNamespace(access_token="token", refresh_token=None, raw_payload={})),
    )
    monkeypatch.setattr(amocrm_service, "resolve_api_base_url", _fake_resolve_api_base_url)
    monkeypatch.setattr(amocrm_service.amocrm_core, "AmoCRMClient", _FakeAmoClient)
    monkeypatch.setattr(amocrm_service, "ensure_lead_phone_field_id", _noop_async)
    monkeypatch.setattr(amocrm_service.db_module, "list_recent_inbound_texts", _fake_recent_texts)
    monkeypatch.setattr(
        amocrm_service.db_module, "list_recent_stage_router_texts", _fake_recent_texts
    )
    monkeypatch.setattr(amocrm_service.crm_fields, "list_fields", _as_async_return([]))
    monkeypatch.setattr(amocrm_service.crm_fields, "upsert_field", _noop_async)
    monkeypatch.setattr(amocrm_service.crm_links, "get_link", _fake_get_link)
    monkeypatch.setattr(amocrm_service.crm_links, "increment_inbound_count", _fake_increment)
    monkeypatch.setattr(amocrm_service.crm_links, "update_stage_index", _noop_async)
    monkeypatch.setattr(amocrm_service.crm_outbox, "enqueue", _fake_enqueue)
    monkeypatch.setattr(amocrm_service.crm_outbox, "has_recent_event", _as_async_return(False))
    monkeypatch.setattr(amocrm_service, "_remote_entity_exists", _as_async_return(True))
    monkeypatch.setattr(
        amocrm_service, "_resolve_lead_names", _as_async_return(("Lead #1", "Lead #1"))
    )
    monkeypatch.setattr(amocrm_service, "_decide_next_stage_llm", _fake_decide)
    monkeypatch.setattr(
        amocrm_service,
        "_supported_evidence",
        lambda *_a, **_k: list(supported_evidence),
    )
    return events


def _as_async_return(value):
    async def _inner(*_args, **_kwargs):
        return value

    return _inner


def test_amocrm_stage_router_semi_auto_skips_note_without_supported_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _patch_amocrm_router_context(
        monkeypatch,
        mode="semi_auto",
        llm_decision={
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.92,
            "reason": "город подтвержден",
            "missing_fields": [],
            "evidence": ["клиент назвал город"],
        },
        supported_evidence=[],
    )
    asyncio.run(
        amocrm_service._amocrm_on_message(
            101,
            999001,
            text="Хочу узнать стоимость",
            channel="avito",
            direction="in",
            attachments=[],
        )
    )
    ai_suggestion_notes = [
        payload.get("text", "")
        for event, payload in events
        if event == "add_note" and str(payload.get("text", "")).startswith("[AI stage suggestion]")
    ]
    move_events = [payload for event, payload in events if event == "move_stage"]
    assert not ai_suggestion_notes
    assert not move_events


def test_amocrm_stage_router_semi_auto_adds_note_with_supported_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _patch_amocrm_router_context(
        monkeypatch,
        mode="semi_auto",
        llm_decision={
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.92,
            "reason": "город подтвержден",
            "missing_fields": [],
            "evidence": ["клиент назвал город"],
        },
        supported_evidence=["клиент назвал город"],
    )
    asyncio.run(
        amocrm_service._amocrm_on_message(
            101,
            999002,
            text="Я из Уфы",
            channel="avito",
            direction="in",
            attachments=[],
        )
    )
    ai_suggestion_notes = [
        payload.get("text", "")
        for event, payload in events
        if event == "add_note" and str(payload.get("text", "")).startswith("[AI stage suggestion]")
    ]
    move_events = [payload for event, payload in events if event == "move_stage"]
    assert ai_suggestion_notes
    assert not move_events


def test_amocrm_stage_router_auto_enqueues_move_stage_when_guard_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _patch_amocrm_router_context(
        monkeypatch,
        mode="auto",
        llm_decision={
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.92,
            "reason": "город подтвержден",
            "missing_fields": [],
            "evidence": ["клиент назвал город"],
        },
        supported_evidence=["клиент назвал город"],
    )
    asyncio.run(
        amocrm_service._amocrm_on_message(
            101,
            999003,
            text="Город Уфа",
            channel="avito",
            direction="in",
            attachments=[],
        )
    )
    move_events = [payload for event, payload in events if event == "move_stage"]
    assert move_events
    assert any(int(payload.get("stage_id") or 0) == 11002 for payload in move_events)


def test_amocrm_stage_router_semi_auto_does_not_block_on_hints_when_evidence_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _patch_amocrm_router_context(
        monkeypatch,
        mode="semi_auto",
        llm_decision={
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.95,
            "reason": "город указан",
            "missing_fields": [],
            "evidence": ["клиент написал: белорецк"],
        },
        supported_evidence=["клиент написал: белорецк"],
    )

    # First _supported_evidence call validates decision evidence.
    # Second call validates long stage hints and returns empty list.
    calls = {"n": 0}

    def _supported_side_effect(*_args, **_kwargs):
        calls["n"] += 1
        return ["клиент написал: белорецк"] if calls["n"] == 1 else []

    monkeypatch.setattr(amocrm_service, "_supported_evidence", _supported_side_effect)

    asyncio.run(
        amocrm_service._amocrm_on_message(
            101,
            999004,
            text="Белорецк",
            channel="avito",
            direction="in",
            attachments=[],
        )
    )
    ai_suggestion_notes = [
        payload.get("text", "")
        for event, payload in events
        if event == "add_note" and str(payload.get("text", "")).startswith("[AI stage suggestion]")
    ]
    assert ai_suggestion_notes


def test_amocrm_stage_router_semi_auto_adds_note_on_outbound_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _patch_amocrm_router_context(
        monkeypatch,
        mode="semi_auto",
        llm_decision={
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.91,
            "reason": "условия отправлены",
            "missing_fields": [],
            "evidence": ["доставка и установка бесплатные", "гарантия 1 год"],
        },
        supported_evidence=["доставка и установка бесплатные", "гарантия 1 год"],
    )
    asyncio.run(
        amocrm_service._amocrm_on_message(
            101,
            999005,
            text="да цена 18000, доставка и установка бесплатные, гарантия 1 год, продолжим в телеграм",
            channel="avito",
            direction="out",
            attachments=[],
        )
    )
    ai_suggestion_notes = [
        payload.get("text", "")
        for event, payload in events
        if event == "add_note" and str(payload.get("text", "")).startswith("[AI stage suggestion]")
    ]
    assert ai_suggestion_notes


def test_amocrm_stage_router_ignores_system_inbound_for_stage_eval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _patch_amocrm_router_context(
        monkeypatch,
        mode="auto",
        llm_decision={
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.95,
            "reason": "город подтвержден",
            "missing_fields": [],
            "evidence": ["Уфа"],
        },
        supported_evidence=["Уфа"],
    )
    asyncio.run(
        amocrm_service._amocrm_on_message(
            101,
            999006,
            text="[Системное сообщение] Пользователь ознакомился с вашим предложением",
            channel="avito",
            direction="in",
            attachments=[],
        )
    )
    move_events = [payload for event, payload in events if event == "move_stage"]
    ai_suggestion_notes = [
        payload.get("text", "")
        for event, payload in events
        if event == "add_note" and str(payload.get("text", "")).startswith("[AI stage suggestion]")
    ]
    assert not move_events
    assert not ai_suggestion_notes


def test_amocrm_stage_router_auto_skips_duplicate_move_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _patch_amocrm_router_context(
        monkeypatch,
        mode="auto",
        llm_decision={
            "action": "MOVE_STAGE",
            "target_stage_index": 1,
            "confidence": 0.95,
            "reason": "город подтвержден",
            "missing_fields": [],
            "evidence": ["Уфа"],
            "stage_checks": [
                {
                    "target_stage_index": 1,
                    "ready": True,
                    "confidence": 0.95,
                    "reason": "город подтвержден",
                    "missing_fields": [],
                    "evidence": [{"quote": "Уфа", "source_role": "lead", "is_new": True}],
                }
            ],
        },
        supported_evidence=["Уфа"],
    )

    async def _has_recent_event(
        _tenant: int, _provider: str, _lead: int, event: str, *_args, **_kwargs
    ) -> bool:
        return event == "move_stage"

    monkeypatch.setattr(amocrm_service.crm_outbox, "has_recent_event", _has_recent_event)
    asyncio.run(
        amocrm_service._amocrm_on_message(
            101,
            999007,
            text="Уфа",
            channel="avito",
            direction="in",
            attachments=[],
        )
    )
    move_events = [payload for event, payload in events if event == "move_stage"]
    assert not move_events
