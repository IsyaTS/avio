from __future__ import annotations

import hashlib
import logging
import os
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Request
from passlib.context import CryptContext
from libs.core.repo import auth as auth_repo
from libs.core import db as db_module
from . import common as C

_log = logging.getLogger("app.web.auth")
_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

_TESTING = (os.getenv("TESTING") or "").strip() == "1"
_RATE_LIMIT_MEM: dict[str, dict[str, float | int]] = {}


def _env_flag(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def landing_enabled() -> bool:
    return _env_flag("ENABLE_PUBLIC_LANDING", False)


def auth_enabled() -> bool:
    return _env_flag("ENABLE_EMAIL_AUTH", False)


def magic_link_enabled() -> bool:
    return _env_flag("AUTH_FALLBACK_MAGIC_LINK", True)


def session_cookie_name() -> str:
    return (os.getenv("SESSION_COOKIE_NAME") or "avio_session").strip()


def session_ttl_days() -> int:
    raw = (os.getenv("SESSION_TTL_DAYS") or "14").strip()
    try:
        value = int(raw)
    except Exception:
        value = 14
    return max(1, min(value, 90))


def csrf_cookie_name() -> str:
    return "avio_csrf"


def csrf_ttl_hours() -> int:
    raw = (os.getenv("CSRF_TTL_HOURS") or "24").strip()
    try:
        value = int(raw)
    except Exception:
        value = 24
    return max(1, min(value, 168))


def _cookie_secure(request: Request) -> bool:
    if _env_flag("AUTH_COOKIE_SECURE", False):
        return True
    try:
        return request.url.scheme == "https"
    except Exception:
        return False


def cookie_params(request: Request, *, ttl_seconds: int | None = None, httponly: bool = True) -> dict[str, Any]:
    params = {
        "httponly": httponly,
        "secure": _cookie_secure(request),
        "samesite": "lax",
        "path": "/",
    }
    if ttl_seconds:
        params["max_age"] = int(ttl_seconds)
    return params


def normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def hash_password(password: str) -> str:
    return _pwd_context.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    if not password or not hashed:
        return False
    return _pwd_context.verify(password, hashed)


def password_ok(password: str) -> tuple[bool, str]:
    if not password or len(password) < 5:
        return False, "Пароль должен быть не короче 5 символов."
    if not re.search(r"[a-zA-Zа-яА-Я]", password):
        return False, "Пароль должен содержать хотя бы одну букву."
    if not re.search(r"\d", password):
        return False, "Пароль должен содержать хотя бы одну цифру."
    return True, ""


def new_token() -> str:
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_session_id() -> str:
    return secrets.token_urlsafe(32)


def request_ip(request: Request) -> str:
    try:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
    except Exception:
        pass
    client = getattr(request, "client", None)
    if client and getattr(client, "host", None):
        return client.host
    return "unknown"


def safe_redirect_path(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate.startswith("/"):
        return None
    if candidate.startswith("//"):
        return None
    if "://" in candidate:
        return None
    return candidate


def _rate_limit_key(action: str, email: str | None, ip: str | None) -> str:
    ip_part = ip or "unknown"
    email_part = ""
    if email:
        email_part = hashlib.sha256(email.encode("utf-8")).hexdigest()[:12]
    return f"auth:rl:{action}:{ip_part}:{email_part}"


def rate_limit_check(
    *,
    action: str,
    email: str | None,
    request: Request,
    limit: int = 5,
    window_seconds: int = 900,
    block_seconds: int = 900,
) -> tuple[bool, int | None]:
    ip = request_ip(request)
    key = _rate_limit_key(action, email, ip)
    block_key = f"{key}:block"
    now_ts = time.time()

    try:
        client = C.redis_client()
        if client.get(block_key):
            return False, block_seconds
        count = int(client.incr(key))
        if count == 1:
            client.expire(key, window_seconds)
        if count > limit:
            client.setex(block_key, block_seconds, "1")
            return False, block_seconds
        return True, None
    except Exception:
        if not _TESTING:
            _log.warning("rate_limit_fallback action=%s reason=redis_unavailable", action)

    if key not in _RATE_LIMIT_MEM or now_ts > _RATE_LIMIT_MEM[key].get("reset_at", 0):
        _RATE_LIMIT_MEM[key] = {"count": 0, "reset_at": now_ts + window_seconds}
    entry = _RATE_LIMIT_MEM[key]
    entry["count"] = int(entry.get("count", 0)) + 1
    if int(entry["count"]) > limit:
        return False, block_seconds
    return True, None


async def get_current_user(request: Request) -> dict[str, Any] | None:
    if not auth_enabled():
        return None
    cookie_name = session_cookie_name()
    session_id = (request.cookies.get(cookie_name) or "").strip()
    if not session_id:
        return None
    session_hash = hash_token(session_id)
    try:
        session = await auth_repo.get_active_session_with_user(session_hash)
    except db_module.DatabaseUnavailableError:
        return None
    except Exception:
        _log.exception("session_lookup_failed")
        return None
    if not session:
        return None
    user = session.get("user")
    if not isinstance(user, dict):
        return None
    if not user.get("is_verified"):
        return None
    return user


def csrf_token_from_request(request: Request) -> str:
    raw = request.cookies.get(csrf_cookie_name()) if request.cookies else ""
    value = (raw or "").strip()
    if value:
        return value
    return new_token()


def verify_csrf(request: Request, token: str | None) -> bool:
    if not token:
        return False
    cookie_value = (request.cookies.get(csrf_cookie_name()) or "").strip()
    if not cookie_value:
        return False
    return secrets.compare_digest(cookie_value, token.strip())


def build_email_base_url(request: Request | None = None) -> str:
    override = (os.getenv("PUBLIC_BASE_URL") or "").strip()
    if override:
        return override.rstrip("/")
    return C.public_base_url(request)


def session_expiry() -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=session_ttl_days())
