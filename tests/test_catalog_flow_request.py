from libs.core.services import catalog_flow as module
import pytest


def test_user_requested_catalog_short_keyword_true() -> None:
    assert module._user_requested_catalog("каталог")
    assert module._user_requested_catalog("pdf")


def test_user_requested_catalog_reference_phrase_false() -> None:
    text = "я незнаю из каталога он или нет, вам виднее"
    assert module._user_requested_catalog(text) is False


def test_user_requested_catalog_explicit_request_true() -> None:
    text = "скиньте, пожалуйста, каталог в пдф"
    assert module._user_requested_catalog(text) is True


def test_first_message_catalog_enabled_telegram_toggle() -> None:
    behavior = {"send_catalog_on_first_message": False}
    assert module._first_message_catalog_enabled(behavior, "telegram") is False


def test_first_message_catalog_enabled_max_uses_channel_flag() -> None:
    behavior = {
        "send_catalog_on_first_message": True,
        "send_catalog_on_first_message_max": False,
    }
    assert module._first_message_catalog_enabled(behavior, "max_personal") is False


def test_first_message_catalog_enabled_max_fallbacks_to_global() -> None:
    behavior = {"send_catalog_on_first_message": False}
    assert module._first_message_catalog_enabled(behavior, "max") is False


@pytest.mark.asyncio
async def test_explicit_catalog_request_keeps_processing_after_pdf_send(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeRedis:
        async def lpush(self, _key, _payload):
            return 1

    async def _never_sent(_cache_key, _redis_conn):
        return False

    async def _mark_sent(_cache_key, _redis_conn):
        return None

    def _fake_attachment(_cfg, _tenant, _request=None):
        return (
            {
                "type": "document",
                "path": "/data/tenants/101/uploads/catalog.pdf",
                "filename": "catalog.pdf",
                "mime": "application/pdf",
            },
            "",
        )

    monkeypatch.setattr(module, "_catalog_was_recently_sent", _never_sent)
    monkeypatch.setattr(module, "_mark_catalog_sent", _mark_sent)
    monkeypatch.setattr(module, "_resolve_catalog_attachment", _fake_attachment)
    monkeypatch.setattr(module.core, "load_tenant", lambda _tenant: {"behavior": {}})

    result = await module.handle_catalog_flow(
        tenant=101,
        lead_id=1,
        refer_id=1,
        text="каталог",
        provider="max_personal",
        resolved_provider="max_personal",
        message_id="m1",
        cache_key=(101, "max_personal:peer:1"),
        redis_conn=_FakeRedis(),
        tenant_cfg={"behavior": {}},
    )

    assert result.catalog_sent is True
    assert result.stop_processing is False
    assert result.stop_reason is None
