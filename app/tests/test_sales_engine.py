import importlib
import pytest

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INNER = ROOT / "app"
for candidate in (ROOT, INNER):
    value = str(candidate)
    if value not in sys.path:
        sys.path.append(value)

import core


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_build_llm_messages_includes_summary():
    tenant = 0
    contact_id = 101
    core.reset_sales_state(tenant, contact_id)

    messages = await core.build_llm_messages(
        contact_id,
        "Нужна настольная лампа 30 см, чтобы было тепло",
        channel="avito",
        tenant=tenant,
    )

    assert messages, "messages should not be empty"
    system_text = messages[0]["content"]
    assert "Needs:" in system_text
    assert "BANT:" in system_text
    assert "SPIN:" in system_text


def test_rule_based_reply_uses_sales_strategies(monkeypatch):
    tenant = 0
    contact_id = 202
    monkeypatch.setenv("EXPLAIN_MODE", "1")
    core.reset_sales_state(tenant, contact_id)

    core.make_rule_based_reply("Здравствуйте", "whatsapp", contact_id, tenant=tenant)
    reply = core.make_rule_based_reply(
        "Ищу настольную лампу, бюджет до 15000, хочу потише",
        "whatsapp",
        contact_id,
        tenant=tenant,
    )

    assert "Понял запрос" in reply
    assert "1." in reply and "—" in reply
    assert "в наличии" in reply or "Укладывается" in reply
    assert "?" in reply  # должен задавать уточняющий вопрос

    state = core.load_sales_state(tenant, contact_id)
    assert state.needs.get("type")
    assert any(sub in (state.needs.get("type") or "") for sub in ("ламп", "настольн"))
    assert any("ламп" in kw for kw in (state.needs.get("keywords", []) or []))
    assert "budget" in state.bant


def test_conversion_score_tracks_sentiment():
    tenant = 0
    contact_id = 303
    core.reset_sales_state(tenant, contact_id)

    core.make_rule_based_reply("Очень нравится вариант, давайте оформим", "whatsapp", contact_id, tenant=tenant)
    state = core.load_sales_state(tenant, contact_id)
    assert state.conversion_score > 0


def test_rule_based_reply_reflects_empathy_and_history():
    tenant = 0
    contact_id = 515
    core.reset_sales_state(tenant, contact_id)

    core.make_rule_based_reply("Здравствуйте", "whatsapp", contact_id, tenant=tenant)
    reply = core.make_rule_based_reply(
        "Я расстроен прошлой покупкой, нужен надёжный диван тёплого оттенка",
        "whatsapp",
        contact_id,
        tenant=tenant,
    )

    assert "ситуация неприятная" in reply or "Сожалею" in reply

    state = core.load_sales_state(tenant, contact_id)
    prefs = (state.profile or {}).get("preferences", {})
    assert state.sentiment_score < 0
    assert prefs.get("keywords"), "keywords should be tracked in preferences"

    state.profile["last_visit_day"] = -1
    state.profile["visits"] = int(state.profile.get("visits", 0) or 1)
    core.save_sales_state(state)

    follow_up = core.make_rule_based_reply(
        "Снова я, хочу сравнить обновления по диванам",
        "whatsapp",
        contact_id,
        tenant=tenant,
    )

    assert "рады снова" in follow_up.lower()
    assert "предпочтения" in follow_up

    updated = core.load_sales_state(tenant, contact_id)
    assert int(updated.profile.get("visits", 0)) >= 2


@pytest.fixture()
def tmp_core(monkeypatch, tmp_path):
    tenants_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))
    import sys
    sys.modules.pop("core", None)
    core_module = importlib.import_module("core")
    globals()["core"] = core_module
    yield core_module
    importlib.reload(core_module)


def test_rule_based_reply_respects_persona_hints(tmp_core):
    tenant = 1
    contact_id = 404
    tmp_core.reset_sales_state(tenant, contact_id)

    tmp_core.ensure_tenant_files(tenant)
    persona_path = tmp_core.tenant_dir(tenant) / "persona.md"
    persona_path.write_text(
        "Greeting: Привет, это Мария из команды.\n"
        "CTA: Напишите номер телефона, перезвоню лично.\n"
        "Closing: Напишите номер телефона, перезвоню лично.\n"
        "Tone: коротко и дружелюбно\n",
        encoding="utf-8",
    )

    tmp_core.make_rule_based_reply("Здравствуйте", "whatsapp", contact_id, tenant=tenant)
    reply = tmp_core.make_rule_based_reply("нужен каталог", "whatsapp", contact_id, tenant=tenant)

    assert "Привет, это Мария" in reply
    assert "Напишите номер телефона, перезвоню лично." in reply

    parts = [block.strip() for block in reply.split("\n\n") if block.strip()]
    assert len(parts) <= 5


def test_rule_based_reply_omits_explain_line_without_flag(monkeypatch):
    tenant = 0
    contact_id = 606
    monkeypatch.delenv("EXPLAIN_MODE", raising=False)
    core.reset_sales_state(tenant, contact_id)

    core.make_rule_based_reply("Здравствуйте", "whatsapp", contact_id, tenant=tenant)
    reply = core.make_rule_based_reply(
        "Нужен офисный стул с поддержкой спины",
        "whatsapp",
        contact_id,
        tenant=tenant,
    )

    assert "Понял запрос" not in reply
