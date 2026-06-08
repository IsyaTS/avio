import pytest

from libs.core.services.avito_incoming import build_avito_incoming_context


pytestmark = pytest.mark.unit


def test_build_avito_incoming_context_prefers_explicit_chat():
    ctx = build_avito_incoming_context(
        {
            "tenant": "3",
            "chat_id": "chat-new",
            "peer": "chat-old",
            "message_id": 55,
            "text": " Привет ",
            "account_id": "123",
            "item_id": "749",
            "avito_user_id": "456",
            "avito_login": " buyer ",
        },
        cached_chat_id="chat-cached",
    )

    assert ctx.tenant_id == 3
    assert ctx.chat_id == "chat-new"
    assert ctx.message_id == "55"
    assert ctx.text == "Привет"
    assert ctx.account_id == 123
    assert ctx.item_id == 749
    assert ctx.user_id == 456
    assert ctx.login == "buyer"


def test_build_avito_incoming_context_uses_cached_chat_and_nested_message():
    ctx = build_avito_incoming_context(
        {
            "tenant_id": 7,
            "id": "msg-7",
            "message": {"text": "Есть в наличии?", "item_id": "321"},
            "avito": {"account_id": "987", "user_id": "654", "login": "nested-login"},
        },
        cached_chat_id="chat-cached",
    )

    assert ctx.tenant_id == 7
    assert ctx.chat_id == "chat-cached"
    assert ctx.message_id == "msg-7"
    assert ctx.text == "Есть в наличии?"
    assert ctx.account_id == 987
    assert ctx.item_id == 321
    assert ctx.user_id == 654
    assert ctx.login == "nested-login"


def test_build_avito_incoming_context_reports_invalid_tenant_and_chat():
    ctx = build_avito_incoming_context({"tenant": "bad", "text": "hi"})

    assert ctx.tenant_id == 0
    assert ctx.chat_id == ""
    assert ctx.valid_tenant is False
    assert ctx.valid_chat is False
