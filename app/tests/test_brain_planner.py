import asyncio
import json

import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from brain import planner


class _StubResponse:
    def __init__(self, content: str):
        self.choices = [type("Choice", (), {"message": type("Message", (), {"content": content})()})]


@pytest.mark.anyio
async def test_generate_sales_reply_success(monkeypatch):
    calls = {"count": 0}

    async def fake_call(_openai, _model, messages, _timeout, *, temperature, max_tokens):
        calls["count"] += 1
        if calls["count"] == 1:
            payload = {
                "analysis": "Клиент хочет бюджетный телефон",
                "stage": "qualification",
                "next_questions": ["Какой бюджет рассматриваете?"],
                "cta": "Готов оформить заказ прямо сейчас",
                "tone": "дружелюбный",
            }
            return _StubResponse(json.dumps(payload, ensure_ascii=False))
        return _StubResponse("Вот финальный ответ для клиента")

    monkeypatch.setattr(planner, "_call_chat_completion", fake_call)

    plan, reply = await planner.generate_sales_reply(
        [
            {"role": "system", "content": "test"},
            {"role": "user", "content": "нужен телефон"},
        ],
        openai_module=object(),
        model="gpt",
        timeout=2.0,
        persona_language="русский",
    )

    assert plan.cta == "Готов оформить заказ прямо сейчас"
    assert "финальный ответ" in reply
    assert calls["count"] == 2


@pytest.mark.anyio
async def test_generate_sales_reply_invalid_plan(monkeypatch):
    async def fake_call(*_args, **_kwargs):
        return _StubResponse("невалидный ответ")

    monkeypatch.setattr(planner, "_call_chat_completion", fake_call)

    with pytest.raises(planner.PlannerError):
        await planner.generate_sales_reply(
            [
                {"role": "system", "content": "test"},
                {"role": "user", "content": "нужен телефон"},
            ],
            openai_module=object(),
            model="gpt",
            timeout=2.0,
        )
