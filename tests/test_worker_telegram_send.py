from __future__ import annotations

from typing import Any

import pytest

from apps.worker import main as worker_module


pytestmark = pytest.mark.unit


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_split_reply_for_send_does_not_inject_contact_intro() -> None:
    parts = worker_module._split_reply_for_send(
        "Пишите в Telegram @dverigermes или звоните 89866666133",
        "telegram",
    )

    normalized = [str(part or "").strip().casefold() for part in parts]
    assert "контакты для связи" not in normalized
    assert any("@dverigermes" in str(part or "") for part in parts)


def test_split_reply_for_send_stitches_orphan_prefix_fragment() -> None:
    text = (
        "адрес нужен, чтобы рассчитать доставку и монтаж, а также подобрать ближайшие варианты "
        "по наличию для квартиры или частного дома выбираете?"
    )
    parts = worker_module._split_reply_for_send(text, "telegram")
    normalized = [str(part or "").strip().casefold() for part in parts]
    assert "адрес нужен" not in normalized
    assert parts


def test_split_reply_for_send_merges_near_duplicate_adjacent_chunks() -> None:
    text = (
        "Уточню точную цену по каталогу и сразу напишу.\n"
        "Уточню точную цену по каталогу и сразу напишу, рассмотреть что-то из них?"
    )
    parts = worker_module._split_reply_for_send(text, "telegram")
    normalized = [str(part or "").strip().casefold() for part in parts]
    assert len(parts) == 1
    assert "рассмотреть" in normalized[0]


def test_split_reply_for_send_merges_dangling_prompt_tail() -> None:
    text = (
        "Мы находимся в Уфе адрес магазина: Менделеева 80 Для клиентов из Уфы при заказе в течение недели действует скидка 2000 ₽ подскажите "
        "выбираете дверь для квартиры или частного дома?"
    )
    parts = worker_module._split_reply_for_send(text, "avito")
    normalized = [str(part or "").strip().casefold() for part in parts]
    assert not any(item.endswith("подскажите") for item in normalized)


def test_split_reply_for_send_isolates_yandex_disk_link_into_separate_part() -> None:
    text = (
        "Каталог с фото на Яндекс диске: "
        "https://disk.yandex.ru/d/TN2KZxBcWySYVA."
    )
    parts = worker_module._split_reply_for_send(text, "avito")
    assert len(parts) >= 2
    assert any("https://disk.yandex.ru/d/TN2KZxBcWySYVA" == str(part or "").strip() for part in parts)
    assert not any(str(part or "").strip() == "." for part in parts)


def test_split_reply_for_send_isolates_bare_yandex_disk_link_and_drops_dot_fragment() -> None:
    text = "вот каталог disk.yandex.ru/d/TN2KZxBcWySYVA ."
    parts = worker_module._split_reply_for_send(text, "telegram")
    normalized = [str(part or "").strip() for part in parts]
    assert any(item == "disk.yandex.ru/d/TN2KZxBcWySYVA" for item in normalized)
    assert "." not in normalized


def test_sanitize_outbound_reply_text_drops_format_meta_and_keeps_useful_tail() -> None:
    text = "отвечайте развёрнуто, не одной строкой работаем по каталогу и выездом"
    out = worker_module._sanitize_outbound_reply_text(text)
    low = out.lower()
    assert "отвечайте" not in low
    assert "не одной строкой" not in low
    assert "работаем по каталогу и выездом" in low


def test_sanitize_outbound_reply_text_drops_full_operator_instruction_line() -> None:
    text = "поздоровайтесь, скажите что для квартир в наличии около 45 моделей"
    out = worker_module._sanitize_outbound_reply_text(text)
    assert out == ""


def test_sanitize_outbound_reply_text_drops_separate_message_meta_tail() -> None:
    text = "в каком городе планируете установку двери работаем по каталогу и выездом отдельным сообщением `"
    out = worker_module._sanitize_outbound_reply_text(text)
    low = out.lower()
    assert "отдельным сообщением" not in low
    assert "`" not in out
    assert "в каком городе планируете установку" in low


def test_split_reply_for_send_drops_backtick_only_chunk() -> None:
    parts = worker_module._split_reply_for_send("нормальный текст `", "avito")
    assert parts
    assert all(str(part or "").strip() != "`" for part in parts)


def test_split_reply_for_send_splits_max_personal_like_telegram() -> None:
    text = (
        "Чтобы подобрать оптимальное решение, важно понять, какие задачи или процессы у вас "
        "отнимают больше всего ресурсов или тормозят работу. "
        "Расскажите, что для вас в приоритете, чтобы я мог предложить оптимальный вариант."
    )
    parts = worker_module._split_reply_for_send(text, "max_personal")
    assert len(parts) > 1


@pytest.mark.anyio
async def test_send_telegram_status_zero_has_no_default_retry_without_attachments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> tuple[int, str]:
        calls.append(dict(kwargs))
        return 0, "network_timeout"

    monkeypatch.setenv("TG_SEND_RETRY_ON_UNKNOWN", "0")
    monkeypatch.delenv("TG_SEND_TEXT_TIMEOUT", raising=False)
    monkeypatch.setattr(worker_module.telegram_transport, "send", fake_send)

    status, body = await worker_module.send_telegram(
        tenant_id=101,
        tg_slot=1,
        chat_id=944310340,
        peer_id=944310340,
        peer="944310340",
        telegram_user_id=944310340,
        username="@Isyyaa",
        text="тест",
        lead_id=1,
    )

    assert status == 0
    assert body == "network_timeout"
    assert len(calls) == 1
    assert float(calls[0].get("timeout") or 0.0) == pytest.approx(40.0)


@pytest.mark.anyio
async def test_send_telegram_retries_for_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_sleep(_seconds: float) -> None:
        return None

    async def fake_send(**kwargs: Any) -> tuple[int, str]:
        calls.append(dict(kwargs))
        if len(calls) < 3:
            return 500, '{"error":"send_failed"}'
        return 200, '{"ok":true}'

    monkeypatch.setattr(worker_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(worker_module.telegram_transport, "send", fake_send)

    status, _ = await worker_module.send_telegram(
        tenant_id=101,
        tg_slot=1,
        chat_id=944310340,
        peer_id=944310340,
        peer="944310340",
        telegram_user_id=944310340,
        username="@Isyyaa",
        text="тест",
        lead_id=1,
    )

    assert status == 200
    assert len(calls) == 3


@pytest.mark.anyio
async def test_send_telegram_attachment_timeout_default(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> tuple[int, str]:
        calls.append(dict(kwargs))
        return 200, '{"ok":true}'

    monkeypatch.delenv("TG_SEND_ATTACH_TIMEOUT", raising=False)
    monkeypatch.setattr(worker_module.telegram_transport, "send", fake_send)

    status, _ = await worker_module.send_telegram(
        tenant_id=101,
        tg_slot=1,
        chat_id=944310340,
        peer_id=944310340,
        peer="944310340",
        telegram_user_id=944310340,
        username="@Isyyaa",
        text="",
        attachments=[{"type": "image", "url": "https://example.com/a.jpg"}],
        lead_id=1,
    )

    assert status == 200
    assert len(calls) == 1
    assert float(calls[0].get("timeout") or 0.0) == pytest.approx(300.0)


@pytest.mark.anyio
async def test_send_telegram_status_zero_retries_with_attachments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_sleep(_seconds: float) -> None:
        return None

    async def fake_send(**kwargs: Any) -> tuple[int, str]:
        calls.append(dict(kwargs))
        if len(calls) < 3:
            return 0, "network_timeout"
        return 200, '{"ok":true}'

    monkeypatch.setattr(worker_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setenv("TG_SEND_RETRY_ON_UNKNOWN", "0")
    monkeypatch.setattr(worker_module.telegram_transport, "send", fake_send)

    status, body = await worker_module.send_telegram(
        tenant_id=101,
        tg_slot=1,
        chat_id=944310340,
        peer_id=944310340,
        peer="944310340",
        telegram_user_id=944310340,
        username="@Isyyaa",
        text="Каталог в PDF: catalog.pdf",
        attachments=[{"type": "file", "url": "https://example.com/catalog.pdf"}],
        lead_id=1,
    )

    assert status == 200
    assert body == '{"ok":true}'
    assert len(calls) == 3


@pytest.mark.anyio
async def test_send_max_personal_skips_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        worker_module.max_personal_service,
        "integration_enabled",
        lambda _tenant: False,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module.max_personal_service,
        "outbound_enabled",
        lambda _tenant: True,
        raising=False,
    )

    status, body = await worker_module.send_max_personal(
        tenant_id=101,
        lead_id=5001,
        text="тест",
    )
    assert status == 0
    assert body == "integration_disabled"


@pytest.mark.anyio
async def test_send_max_personal_resolves_chat_and_uses_idempotency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: dict[str, object] = {}

    async def _fake_send_message(tenant_id: int, **kwargs):
        sent["tenant_id"] = tenant_id
        sent.update(kwargs)
        return 200, {"ok": True, "message_id": "mx-1"}

    monkeypatch.setattr(
        worker_module.max_personal_service,
        "integration_enabled",
        lambda _tenant: True,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module.max_personal_service,
        "outbound_enabled",
        lambda _tenant: True,
        raising=False,
    )

    async def _fake_get_lead_peer(_lead_id: int, channel: str = "") -> str:
        assert channel == "max_personal"
        return "chat-900"

    monkeypatch.setattr(
        worker_module,
        "get_lead_peer",
        _fake_get_lead_peer,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module.max_personal_transport,
        "send_message",
        _fake_send_message,
        raising=False,
    )

    status, body = await worker_module.send_max_personal(
        tenant_id=101,
        lead_id=900,
        text="ответ",
        message_id="msg-77",
    )

    assert status == 200
    assert '"ok": true' in body.lower()
    assert sent["tenant_id"] == 101
    assert sent["chat_id"] == "chat-900"
    assert sent["text"] == "ответ"
    dedupe_key = str(sent["dedupe_key"])
    assert dedupe_key.startswith("101:900:msg-77:")
    assert len(dedupe_key) > len("101:900:msg-77:")
    assert sent["idempotency_key"] == dedupe_key


@pytest.mark.anyio
async def test_send_max_personal_prepares_pdf_attachments_like_telegram(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: dict[str, object] = {}

    async def _fake_send_message(tenant_id: int, **kwargs):
        sent["tenant_id"] = tenant_id
        sent.update(kwargs)
        return 200, {"ok": True, "message_id": "mx-2"}

    monkeypatch.setattr(
        worker_module.max_personal_service,
        "integration_enabled",
        lambda _tenant: True,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module.max_personal_service,
        "outbound_enabled",
        lambda _tenant: True,
        raising=False,
    )

    async def _fake_get_lead_peer(_lead_id: int, channel: str = "") -> str:
        assert channel == "max_personal"
        return "chat-901"

    monkeypatch.setattr(
        worker_module,
        "get_lead_peer",
        _fake_get_lead_peer,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module.max_personal_transport,
        "send_message",
        _fake_send_message,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module,
        "_prepare_tg_attachments_for_send",
        lambda tenant_id, attachments: [{"type": "document", "path": "/tmp/catalog.fast.pdf"}],
        raising=False,
    )

    status, body = await worker_module.send_max_personal(
        tenant_id=101,
        lead_id=901,
        text="каталог",
        attachments=[{"type": "document", "path": "/tmp/catalog.pdf"}],
    )

    assert status == 200
    assert '"ok": true' in body.lower()
    assert sent["tenant_id"] == 101
    assert sent["chat_id"] == "chat-901"
    assert sent["attachments"] == [{"type": "document", "path": "/tmp/catalog.fast.pdf"}]


@pytest.mark.anyio
async def test_send_max_personal_idempotency_distinguishes_payloads_same_message_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    async def _fake_send_message(tenant_id: int, **kwargs):
        payload = {"tenant_id": tenant_id}
        payload.update(kwargs)
        calls.append(payload)
        return 200, {"ok": True, "message_id": f"mx-{len(calls)}"}

    monkeypatch.setattr(
        worker_module.max_personal_service,
        "integration_enabled",
        lambda _tenant: True,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module.max_personal_service,
        "outbound_enabled",
        lambda _tenant: True,
        raising=False,
    )

    async def _fake_get_lead_peer(_lead_id: int, channel: str = "") -> str:
        assert channel == "max_personal"
        return "chat-902"

    monkeypatch.setattr(worker_module, "get_lead_peer", _fake_get_lead_peer, raising=False)
    monkeypatch.setattr(
        worker_module.max_personal_transport,
        "send_message",
        _fake_send_message,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module,
        "_prepare_tg_attachments_for_send",
        lambda _tenant, attachments: attachments,
        raising=False,
    )

    await worker_module.send_max_personal(
        tenant_id=101,
        lead_id=902,
        text="",
        attachments=[{"type": "document", "path": "/tmp/catalog.fast.pdf"}],
        message_id="inbound-1",
    )
    await worker_module.send_max_personal(
        tenant_id=101,
        lead_id=902,
        text="Подскажите, пожалуйста, в каком городе установка?",
        attachments=[],
        message_id="inbound-1",
    )

    assert len(calls) == 2
    first_key = str(calls[0]["idempotency_key"])
    second_key = str(calls[1]["idempotency_key"])
    assert first_key.startswith("101:902:inbound-1:")
    assert second_key.startswith("101:902:inbound-1:")
    assert first_key != second_key
