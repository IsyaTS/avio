from __future__ import annotations

import os
from functools import lru_cache
from typing import Callable

from cryptography.fernet import Fernet, InvalidToken


class EncryptionError(RuntimeError):
    """Raised when encryption or decryption fails."""


def _env_key() -> str:
    return (os.getenv("AVITO_TOKEN_ENCRYPTION_KEY") or "").strip()


@lru_cache(maxsize=1)
def _fernet_factory() -> Callable[[], Fernet]:
    """Return cached factory for Fernet to delay import errors."""

    key = _env_key()
    if not key:
        raise EncryptionError("AVITO_TOKEN_ENCRYPTION_KEY is not configured")
    try:
        fernet = Fernet(key)
    except Exception as exc:  # pragma: no cover - invalid key
        raise EncryptionError("Invalid AVITO_TOKEN_ENCRYPTION_KEY") from exc

    def _factory() -> Fernet:
        return fernet

    return _factory


def _get_fernet() -> Fernet:
    factory = _fernet_factory()
    return factory()


def encrypt_str(value: str) -> str:
    """Encrypt a string using Fernet with the configured key."""

    if value is None:
        raise EncryptionError("Cannot encrypt None")
    fernet = _get_fernet()
    token = fernet.encrypt(value.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_str(value: str) -> str:
    """Decrypt a string using Fernet with the configured key."""

    if value is None:
        raise EncryptionError("Cannot decrypt None")
    fernet = _get_fernet()
    try:
        decrypted = fernet.decrypt(value.encode("utf-8"))
    except InvalidToken as exc:  # pragma: no cover - invalid token
        raise EncryptionError("Invalid encrypted token") from exc
    return decrypted.decode("utf-8")


__all__ = ["encrypt_str", "decrypt_str", "EncryptionError"]
