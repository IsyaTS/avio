from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, TypeVar

import httpx
import redis  # sync client
from redis import exceptions as redis_ex

from app.config import tg_worker_url

try:
    from core import (
        settings,
        get_tenant_pubkey,
        set_tenant_pubkey,
        tenant_dir,
        ensure_tenant_files,
        read_tenant_config,
        write_tenant_config,
        read_persona,
        write_persona,
    )
except ImportError:  # pragma: no cover - fallback when alias not yet registered
    from app import core as _core  # type: ignore

    sys.modules.setdefault("core", _core)
    from app.core import (
        settings,
        get_tenant_pubkey,
        set_tenant_pubkey,
        tenant_dir,
        ensure_tenant_files,
        read_tenant_config,
        write_tenant_config,
        read_persona,
        write_persona,
    )
    # NOTE: providing fallback alias keeps imports working during isolated tests

T = TypeVar("T")

# --- redis & integrations ---
_redis_client: redis.Redis | None = None
WA_WEB_URL = (os.getenv("WA_WEB_URL", "http://waweb:8088") or "http://waweb:8088").rstrip("/")
# Internal auth token that waweb expects in X-Auth-Token. It may be provided
# via WA_WEB_TOKEN or WEBHOOK_SECRET depending on deployment. Use either.
WA_INTERNAL_TOKEN = (
    (os.getenv("WA_WEB_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
)
TG_WORKER_URL = tg_worker_url()
TG_WORKER_TOKEN = (os.getenv("TG_WORKER_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()


def redis_client() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis_client


def _first_header(headers: dict[str, Any], *names: str) -> str:
    for name in names:
        value = headers.get(name, "") if headers else ""
        if value:
            return str(value).split(",")[0].strip()
    return ""


def public_base_url(request: Any | None = None) -> str:
    base = (settings.APP_PUBLIC_URL or "").strip()
    if base:
        return base.rstrip("/")
    if request is None:
        return ""

    headers = getattr(request, "headers", {}) or {}
    host = _first_header(headers, "x-forwarded-host", "host")
    proto = _first_header(headers, "x-forwarded-proto", "x-forwarded-scheme")

    if host:
        proto = proto or getattr(getattr(request, "url", None), "full", "").split("://", 1)[0] or "https"
        return f"{proto}://{host}".rstrip("/")

    url_obj = getattr(request, "url", None)
    full = getattr(url_obj, "full", "")
    path = getattr(url_obj, "path", "")
    if full:
        if path and path in full:
            return full[: full.index(path)].rstrip("/")
        return full.rstrip("/")
    return ""


def public_url(request: Any | None, path_or_url: str) -> str:
    if not path_or_url:
        return ""
    if str(path_or_url).lower().startswith(("http://", "https://")):
        return path_or_url
    base = public_base_url(request)
    cleaned = str(path_or_url)
    leading = cleaned if cleaned.startswith("/") else f"/{cleaned}"
    if not base:
        return leading
    return f"{base}{leading}"


def _with_redis(func: Callable[[redis.Redis], T], default: T) -> T:
    global _redis_client
    for _ in range(2):
        try:
            return func(redis_client())
        except redis_ex.ConnectionError:
            _redis_client = None
        except redis_ex.RedisError:
            return default
    return default


def http(method: str, url: str, body: bytes | None = None, timeout: float = 8.0):
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json; charset=utf-8")
    # Add internal auth header for waweb if configured
    if WA_INTERNAL_TOKEN:
        try:
            req.add_header("X-Auth-Token", WA_INTERNAL_TOKEN)
        except Exception:
            pass
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, raw.decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, raw.decode("utf-8", errors="ignore") if raw else ""
    except Exception as e:  # pragma: no cover - сетевые ошибки
        return 0, str(e)


def ensure_bytes(x: str) -> bytes:
    return x.encode("utf-8") if isinstance(x, str) else x


def webhook_url() -> str:
    base = settings.APP_INTERNAL_URL or os.getenv("APP_INTERNAL_URL") or "http://app:8000"
    url = f"{base.rstrip('/')}/webhook"
    token = settings.WEBHOOK_SECRET or os.getenv("WEBHOOK_SECRET") or ""
    if token:
        separator = "&" if "?" in url else "?"
        encoded = urllib.parse.urlencode({"token": token})
        url = f"{url}{separator}{encoded}"
    return url



async def wa_post(path: str, data: dict, timeout: float = 8.0) -> httpx.Response:
    url = f"{WA_WEB_URL}{path}"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if WA_INTERNAL_TOKEN:
        headers["X-Auth-Token"] = WA_INTERNAL_TOKEN

    async with httpx.AsyncClient(timeout=timeout) as client:
        last_response: httpx.Response | None = None
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = await client.post(url, json=data, headers=headers)
                if 500 <= response.status_code < 600:
                    last_response = response
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                return response
            except httpx.HTTPStatusError as exc:  # pragma: no cover - пробрасываем тело
                return exc.response
            except httpx.HTTPError as exc:
                last_error = exc
                await asyncio.sleep(0.5 * (attempt + 1))

        if last_response is not None:
            return last_response
        if last_error is not None:
            raise last_error
        raise RuntimeError("wa_post failed without response")


async def tg_post(path: str, data: dict, timeout: float = 8.0) -> httpx.Response:
    url = f"{TG_WORKER_URL}{path}"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if TG_WORKER_TOKEN:
        headers["X-Auth-Token"] = TG_WORKER_TOKEN
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.post(url, json=data, headers=headers)


def tg_http(
    method: str,
    path: str,
    body: bytes | None = None,
    timeout: float = 8.0,
) -> tuple[int, bytes, dict[str, str]]:
    url = f"{TG_WORKER_URL}{path}"
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json; charset=utf-8")
    if TG_WORKER_TOKEN:
        req.add_header("X-Auth-Token", TG_WORKER_TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            headers = {key: value for key, value in resp.headers.items()}
            return resp.status, raw, headers
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        headers = {key: value for key, value in getattr(exc, "headers", {}).items()} if getattr(exc, "headers", None) else {}
        return exc.code, raw, headers
    except Exception as exc:  # pragma: no cover
        return 0, str(exc).encode(), {}


# --- keys registry in Redis ---
def valid_key(tenant: int, kk: str) -> bool:
    want = (get_tenant_pubkey(int(tenant)) or "").strip().lower()
    return bool(kk) and kk.strip().lower() == want


def keys_hkey(tenant: int) -> str:
    return f"tenant:{int(tenant)}:keys"


def list_keys(tenant: int):
    raw = _with_redis(lambda client: client.hgetall(keys_hkey(tenant)) or {}, {})
    primary = (get_tenant_pubkey(int(tenant)) or "").strip().lower()
    out: list[dict[str, Any]] = []
    for k, v in raw.items():
        try:
            meta = json.loads(v) if v else {}
        except Exception:
            meta = {}
        encoded_key = urllib.parse.quote_plus(k)
        out.append(
            {
                "key": k,
                "label": meta.get("label", ""),
                "ts": meta.get("ts", 0),
                "primary": (k.strip().lower() == primary),
                "link": f"/connect/wa?tenant={int(tenant)}&k={encoded_key}",
                "settings_link": f"/client/{int(tenant)}/settings?k={encoded_key}",
            }
        )
    out.sort(key=lambda x: (not x["primary"], -(x["ts"] or 0)))
    return out


def add_key(tenant: int, key: str, label: str | None = ""):
    meta = {"label": label or "", "ts": int(time.time())}
    return _with_redis(
        lambda client: client.hset(keys_hkey(tenant), key, json.dumps(meta, ensure_ascii=False)),
        0,
    )


def del_key(tenant: int, key: str):
    return _with_redis(lambda client: client.hdel(keys_hkey(tenant), key), 0)


def set_primary(tenant: int, key: str):
    set_tenant_pubkey(int(tenant), key.strip())
    return True


__all__ = [
    "WA_WEB_URL",
    "WA_INTERNAL_TOKEN",
    "TG_WORKER_URL",
    "TG_WORKER_TOKEN",
    "redis_client",
    "http",
    "wa_post",
    "tg_post",
    "tg_http",
    "public_base_url",
    "public_url",
    "valid_key",
    "keys_hkey",
    "list_keys",
    "add_key",
    "del_key",
    "set_primary",
    "tenant_dir",
    "ensure_tenant_files",
    "read_tenant_config",
    "write_tenant_config",
    "read_persona",
    "write_persona",
    "webhook_url",
]
