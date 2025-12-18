from __future__ import annotations

import sys
import types
from importlib import reload

from cryptography.fernet import Fernet

# Lightweight stubs to avoid pulling optional deps during unit tests.
if "httpx" not in sys.modules:
    sys.modules["httpx"] = types.SimpleNamespace(AsyncClient=object, HTTPError=Exception, TimeoutException=Exception)

from libs.core import crypto
from libs.core.models.avito_analytics import AvitoAnalyticsToken
from libs.core.repo import avito_analytics_tokens


def test_encrypt_decrypt_roundtrip(monkeypatch):
    key = Fernet.generate_key().decode("utf-8")
    monkeypatch.setenv("AVITO_TOKEN_ENCRYPTION_KEY", key)
    crypto._fernet_factory.cache_clear()
    secret = "sensitive-token"
    encrypted = crypto.encrypt_str(secret)
    assert encrypted and encrypted != secret
    decrypted = crypto.decrypt_str(encrypted)
    assert decrypted == secret


def test_summary_from_tokens_redacts(monkeypatch):
    entry = AvitoAnalyticsToken(
        account_id=123,
        display_name="demo",
        scopes="user:read",
        token_type="bearer",
        access_token="token",
        refresh_token="refresh",
        expires_at=None,
        obtained_at=None,
        created_at=None,
        updated_at=None,
        last_error=None,
        raw_payload={},
    )
    summary = avito_analytics_tokens.summary_from_tokens([entry])
    assert summary and summary[0]["account_id"] == 123
    assert "refresh_token" not in summary[0]
