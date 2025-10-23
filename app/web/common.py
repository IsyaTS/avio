from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pathlib
import subprocess
import sys

module_obj = sys.modules.get(__name__)
if module_obj is not None:
    sys.modules[__name__] = module_obj
    sys.modules["app.web.common"] = module_obj
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, TypeVar

import httpx
import redis  # sync client
from redis import exceptions as redis_ex

from config import tg_worker_url

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
WA_WEB_URL = (
    os.getenv("WAWEB_BASE_URL")
    or os.getenv("WA_WEB_URL")
    or "http://waweb:9001"
).rstrip("/")
# Internal auth token that waweb expects in X-Auth-Token. It may be provided
# via WA_WEB_TOKEN or WEBHOOK_SECRET depending on deployment. Use either.
WA_INTERNAL_TOKEN = (
    (os.getenv("WA_WEB_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()
)
TG_WORKER_URL = tg_worker_url()
TG_WORKER_TOKEN = (os.getenv("TG_WORKER_TOKEN") or os.getenv("WEBHOOK_SECRET") or "").strip()


_CLIENT_SETTINGS_VERSION: str | None = None


def _static_base_prefix() -> str:
    base = (os.getenv("STATIC_PUBLIC_BASE") or "").strip()
    if not base:
        base = "/static"
    if base != "/":
        base = base.rstrip("/")
    return base or "/static"


def static_url(request: Any | None, path: str) -> str:
    cleaned = str(path or "").lstrip("/")
    base = _static_base_prefix()
    if not cleaned:
        return base
    if base.endswith("/"):
        base = base.rstrip("/")
    if not base:
        return f"/{cleaned}"
    return f"{base}/{cleaned}"


def _client_settings_bundle_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1] / "static" / "js"


def _compute_client_settings_digest() -> str | None:
    bundle_root = _client_settings_bundle_root()
    sha1 = hashlib.sha1()
    files_found = False
    for filename in ("boot.js", "client-settings.js"):
        path = bundle_root / filename
        if not path.exists():
            continue
        try:
            sha1.update(path.read_bytes())
            files_found = True
        except OSError:
            continue
    if not files_found:
        return None
    return sha1.hexdigest()[:12]


def client_settings_version() -> str:
    global _CLIENT_SETTINGS_VERSION
    if _CLIENT_SETTINGS_VERSION:
        return _CLIENT_SETTINGS_VERSION

    build_rev = (os.getenv("BUILD_REV") or os.getenv("CLIENT_SETTINGS_VERSION") or "").strip()
    base_version = build_rev

    if not base_version:
        for env_name in ("APP_GIT_SHA", "GIT_SHA", "HEROKU_SLUG_COMMIT"):
            value = (os.getenv(env_name) or "").strip()
            if value:
                base_version = value[:8] or value
                break

    if not base_version:
        try:
            repo_root = pathlib.Path(__file__).resolve().parents[2]
            output = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(repo_root),
                stderr=subprocess.DEVNULL,
            )
            base_version = output.decode("utf-8").strip()
        except Exception:
            base_version = ""

    digest = _compute_client_settings_digest()
    if digest:
        base_version = f"{base_version}-{digest}" if base_version else digest

    if not base_version:
        base_version = str(int(time.time()))

    _CLIENT_SETTINGS_VERSION = base_version
    return _CLIENT_SETTINGS_VERSION


def _admin_token() -> str:
    return (getattr(settings, "ADMIN_TOKEN", "") or os.getenv("ADMIN_TOKEN") or "").strip()


def _build_tg_url(path: str) -> str:
    if not path:
        return TG_WORKER_URL
    lowered = path.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{TG_WORKER_URL}{path}"


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
    url = _build_tg_url(path)
    headers = {"Content-Type": "application/json; charset=utf-8"}
    if TG_WORKER_TOKEN:
        headers["X-Auth-Token"] = TG_WORKER_TOKEN
    admin_token = _admin_token()
    if admin_token:
        headers["X-Admin-Token"] = admin_token
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.post(url, json=data, headers=headers)


def tg_http(
    method: str,
    path: str,
    body: bytes | None = None,
    timeout: float = 8.0,
) -> tuple[int, bytes, dict[str, str]]:
    url = _build_tg_url(path)
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json; charset=utf-8")
    if TG_WORKER_TOKEN:
        req.add_header("X-Auth-Token", TG_WORKER_TOKEN)
    admin_token = _admin_token()
    if admin_token:
        req.add_header("X-Admin-Token", admin_token)
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
def _normalize_key(value: str | None) -> str:
    return str(value or "").strip()


def key_meta_key(tenant: int) -> str:
    return f"tenant:{int(tenant)}:key_meta"


def _load_key_meta(tenant: int) -> dict[str, Any]:
    raw = _with_redis(lambda client: client.get(key_meta_key(tenant)) or "", "")
    if not raw:
        return {}
    try:
        meta = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(meta, dict):
        return {}
    key_value = _normalize_key(meta.get("key"))
    normalized = _normalize_key(meta.get("normalized") or key_value).lower()
    label = str(meta.get("label") or "")
    ts_raw = meta.get("ts")
    try:
        ts_value = int(ts_raw)
    except Exception:
        ts_value = 0
    return {
        "key": meta.get("key") or key_value,
        "label": label,
        "ts": ts_value,
        "normalized": normalized,
    }


def _save_key_meta(tenant: int, meta: dict[str, Any] | None) -> None:
    def _apply(client: redis.Redis) -> bool:
        normalized_value = _normalize_key((meta or {}).get("key"))
        key_name = key_meta_key(tenant)
        if not normalized_value:
            client.delete(key_name)
            client.delete(keys_hkey(tenant))
            return True
        try:
            ts_value = int((meta or {}).get("ts") or int(time.time()))
        except Exception:
            ts_value = int(time.time())
        payload = json.dumps(
            {
                "key": (meta or {}).get("key") or normalized_value,
                "label": (meta or {}).get("label") or "",
                "ts": ts_value,
                "normalized": normalized_value.lower(),
            },
            ensure_ascii=False,
        )
        client.set(key_name, payload)
        client.delete(keys_hkey(tenant))
        return True

    _with_redis(_apply, True)


def _migrate_legacy_keys(tenant: int, current_meta: dict[str, Any]) -> dict[str, Any]:
    legacy = _with_redis(lambda client: client.hgetall(keys_hkey(tenant)) or {}, {})
    if not legacy:
        return current_meta

    primary_value = _normalize_key(get_tenant_pubkey(tenant))
    primary_norm = primary_value.lower()
    best: dict[str, Any] | None = None
    best_ts = -1

    for stored_key, raw_meta in legacy.items():
        try:
            parsed = json.loads(raw_meta) if raw_meta else {}
        except Exception:
            parsed = {}
        candidate_display = parsed.get("value") or parsed.get("key") or stored_key
        candidate_value = _normalize_key(candidate_display)
        if not candidate_value:
            continue
        candidate_norm = candidate_value.lower()
        try:
            ts_value = int(parsed.get("ts") or 0)
        except Exception:
            ts_value = 0
        entry = {
            "key": candidate_display,
            "label": parsed.get("label") or "",
            "ts": ts_value,
            "normalized": candidate_norm,
        }
        if primary_norm and candidate_norm == primary_norm:
            best = entry
            break
        if ts_value >= best_ts:
            best = entry
            best_ts = ts_value

    if best is None:
        for stored_key in legacy:
            candidate_value = _normalize_key(stored_key)
            if candidate_value:
                best = {
                    "key": stored_key,
                    "label": "",
                    "ts": int(time.time()),
                    "normalized": candidate_value.lower(),
                }
                break

    _with_redis(lambda client: client.delete(keys_hkey(tenant)), 0)

    if best is None:
        return current_meta

    if not primary_value:
        candidate_normalized = _normalize_key(best.get("key"))
        set_tenant_pubkey(tenant, candidate_normalized)
        stored = _normalize_key(get_tenant_pubkey(tenant))
        best["normalized"] = stored.lower()
        if not best.get("key"):
            best["key"] = stored
    else:
        best["normalized"] = primary_norm
        if not best.get("key"):
            best["key"] = primary_value

    best["key"] = _normalize_key(best.get("key")) or (primary_value if primary_value else "")

    return best


def valid_key(tenant: int, kk: str) -> bool:
    want = (get_tenant_pubkey(int(tenant)) or "").strip().lower()
    return bool(kk) and kk.strip().lower() == want


def keys_hkey(tenant: int) -> str:
    return f"tenant:{int(tenant)}:keys"


def list_keys(tenant: int):
    tenant_id = int(tenant)
    meta = _load_key_meta(tenant_id)
    primary_value = _normalize_key(get_tenant_pubkey(tenant_id))
    if not primary_value:
        meta = _migrate_legacy_keys(tenant_id, meta)
        primary_value = _normalize_key(get_tenant_pubkey(tenant_id))

    if not primary_value:
        _save_key_meta(tenant_id, {})
        return []

    normalized = primary_value.lower()
    if not meta or meta.get("normalized") != normalized:
        meta = {
            "key": meta.get("key") if isinstance(meta, dict) else primary_value,
            "label": (meta.get("label") if isinstance(meta, dict) else "") or "",
            "ts": int((meta.get("ts") if isinstance(meta, dict) else 0) or int(time.time())),
            "normalized": normalized,
        }
    else:
        meta = dict(meta)
        meta["key"] = meta.get("key") or primary_value
        meta["label"] = meta.get("label") or ""
        try:
            meta["ts"] = int(meta.get("ts") or int(time.time()))
        except Exception:
            meta["ts"] = int(time.time())
        meta["normalized"] = normalized

    _save_key_meta(tenant_id, meta)

    display_key = meta.get("key") or primary_value
    encoded = urllib.parse.quote_plus(display_key)
    return [
        {
            "key": display_key,
            "label": meta.get("label", ""),
            "ts": meta.get("ts", 0),
            "primary": True,
            "link": f"/connect/wa?tenant={tenant_id}&k={encoded}",
            "settings_link": f"/client/{tenant_id}/settings?k={encoded}",
        }
    ]


def add_key(tenant: int, key: str, label: str | None = ""):
    tenant_id = int(tenant)
    key_value = _normalize_key(key)
    if not key_value:
        return 0
    meta = _load_key_meta(tenant_id)
    meta = dict(meta) if isinstance(meta, dict) and meta else {}
    meta.update({"key": key_value, "label": label or "", "ts": int(time.time())})
    _save_key_meta(tenant_id, meta)
    return 1


def del_key(tenant: int, key: str):
    tenant_id = int(tenant)

    def _apply(client: redis.Redis) -> int:
        removed = client.delete(key_meta_key(tenant_id))
        client.delete(keys_hkey(tenant_id))
        return removed

    return _with_redis(_apply, 0)


def set_primary(tenant: int, key: str):
    tenant_id = int(tenant)
    key_value = _normalize_key(key)
    if not key_value:
        set_tenant_pubkey(tenant_id, "")
        _save_key_meta(tenant_id, {})
        return True

    meta = _load_key_meta(tenant_id)
    meta = dict(meta) if meta else {}
    if "ts" not in meta or not meta["ts"]:
        meta["ts"] = int(time.time())
    meta.setdefault("label", meta.get("label", ""))
    meta["key"] = key_value
    _save_key_meta(tenant_id, meta)
    set_tenant_pubkey(tenant_id, key_value)
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
    "key_meta_key",
    "tenant_dir",
    "ensure_tenant_files",
    "read_tenant_config",
    "write_tenant_config",
    "read_persona",
    "write_persona",
    "webhook_url",
]
