from __future__ import annotations

import csv
import importlib
import io
import json
import logging
import math
import mimetypes
import os
import pathlib
import re
import hashlib
import sys
import time
import uuid
import asyncio
from typing import Any, Iterable, Mapping

from fastapi import APIRouter, File, Request, UploadFile, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse, Response
import httpx
import urllib.request
import urllib.error


def _import_alias(module: str):
    """Load module by bare name with ``app.<module>`` fallback."""

    try:
        return importlib.import_module(module)
    except ImportError:
        fallback = importlib.import_module(f"app.{module}")
        sys.modules.setdefault(module, fallback)
        return fallback


catalog_module = _import_alias("catalog")
catalog_index = _import_alias("catalog_index")

# NOTE: reference helpers locally to keep call sites compact
write_catalog_csv = catalog_module.write_catalog_csv
CatalogIndexError = catalog_index.CatalogIndexError
build_pdf_index = catalog_index.build_pdf_index
index_to_catalog_items = catalog_index.index_to_catalog_items

try:  # pragma: no cover - optional dependency during import time
    from openpyxl import load_workbook  # type: ignore
except Exception:  # pragma: no cover - openpyxl is optional in some environments
    load_workbook = None  # type: ignore[assignment]

try:
    from app.core import _normalize_catalog_items, settings  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - fallback for legacy layout
    try:
        from core import _normalize_catalog_items, settings  # type: ignore[attr-defined]
    except ImportError:
        core_module = _import_alias("core")
        _normalize_catalog_items = core_module._normalize_catalog_items
        settings = core_module.settings

from urllib.parse import quote, quote_plus, urlencode

from redis import exceptions as redis_ex

from config import tg_worker_url

from app.core import client as C
from app.metrics import MESSAGE_IN_COUNTER, DB_ERRORS_COUNTER
from app.schemas import MessageIn, PingEvent
from app.db import insert_message_in, upsert_lead
from . import common as common
try:  # pragma: no cover - optional webhooks import
    from . import webhooks as webhook_module  # type: ignore
except ImportError:  # pragma: no cover - fallback when module alias missing
    try:
        from app.web import webhooks as webhook_module  # type: ignore
    except ImportError:
        webhook_module = None  # type: ignore[assignment]
from .ui import templates

logger = logging.getLogger(__name__)
wa_logger = logging.getLogger("wa")
# Unified incoming transport log channel
message_in_logger = logging.getLogger("app.web.message_in")
_deprecated_hits: dict[str, float] = {}
# Avoid duplicate logging of WA messages via root logger handlers
wa_logger.propagate = False

TG_WORKER_BASE = tg_worker_url()
if not hasattr(C, "valid_key"):
    setattr(C, "valid_key", common.valid_key)

NO_STORE_CACHE_VALUE = "no-store, no-cache, must-revalidate"

PASSWORD_ATTEMPT_LIMIT = 2
PASSWORD_ATTEMPT_WINDOW = 60.0
_LOCAL_PASSWORD_ATTEMPTS: dict[tuple[int, str], list[float]] = {}

INCOMING_QUEUE_KEY = getattr(webhook_module, "INCOMING_QUEUE_KEY", "inbox:message_in")


def _no_store_headers(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    headers = {
        "Cache-Control": NO_STORE_CACHE_VALUE,
        "Pragma": "no-cache",
        "Expires": "0",
    }
    if extra:
        headers.update(extra)
    return headers


class TgWorkerCallError(RuntimeError):
    __slots__ = ("url", "detail")

    def __init__(self, url: str, detail: str):
        super().__init__(f"{url}: {detail}")
        self.url = url
        self.detail = detail


def _tg_base_url() -> str:
    raw = getattr(settings, "TGWORKER_BASE_URL", "") or os.getenv("TGWORKER_BASE_URL") or ""
    base = str(raw).strip() or "http://tgworker:9000"
    return base.rstrip("/") or "http://tgworker:9000"


def _tg_make_url(path: str) -> str:
    if not path:
        return _tg_base_url()
    lowered = path.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{_tg_base_url()}{path}"


_TG_HTTP_CLIENT: httpx.AsyncClient | None = None


def _tg_admin_headers() -> dict[str, str]:
    token = getattr(settings, "ADMIN_TOKEN", "") or ""
    return {"X-Admin-Token": token}


def _tg_client() -> httpx.AsyncClient:
    global _TG_HTTP_CLIENT
    if _TG_HTTP_CLIENT is None or _TG_HTTP_CLIENT.is_closed:
        _TG_HTTP_CLIENT = httpx.AsyncClient(timeout=httpx.Timeout(10.0))
    return _TG_HTTP_CLIENT


async def _tg_call(
    method: str,
    path: str,
    *,
    params: Mapping[str, Any] | None = None,
    json: Mapping[str, Any] | None = None,
    timeout: float = 5,
    route: str | None = None,
    peer: Any | None = None,
) -> tuple[int, httpx.Response]:
    url = _tg_make_url(path)
    base_headers = _tg_admin_headers()
    request_kwargs: dict[str, Any] = {
        "params": dict(params or {}),
        "headers": base_headers,
        "follow_redirects": False,
        "timeout": httpx.Timeout(timeout),
    }
    if json is not None:
        request_kwargs["json"] = dict(json)
    try:
        client = _tg_client()
        response = await client.request(method.upper(), url, **request_kwargs)
    except httpx.HTTPError as exc:  # pragma: no cover - network failures
        detail = str(exc)
        logger.warning(
            "event=tg_proxy_error route=%s url=%s status=error detail=%s",
            route or path,
            url,
            detail,
        )
        raise TgWorkerCallError(url, detail) from exc

    status_code = int(getattr(response, "status_code", 0) or 0)
    peer_info = "-" if peer is None else str(peer)
    log_args = (route or path, url, status_code, peer_info)
    if status_code == 401:
        logger.warning(
            "event=tg_proxy_response route=%s url=%s status=%s peer=%s unauthorized",
            *log_args,
        )
    else:
        logger.info(
            "event=tg_proxy_response route=%s url=%s status=%s peer=%s",
            *log_args,
        )
    return status_code, response


def _log_deprecated(route: str) -> None:
    now = time.time()
    last = _deprecated_hits.get(route)
    if last is None or now - last >= 3600:
        _deprecated_hits[route] = now
        logger.warning("deprecated_endpoint route=%s", route)
def _stringify_detail(value: bytes | bytearray | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(value)


_JSON_DBL_PASSWORD = re.compile(r'("password"\s*:\s*")([^"\\]*)(")', re.IGNORECASE)
_JSON_SGL_PASSWORD = re.compile(r"('password'\s*:\s*')([^'\\]*)(')", re.IGNORECASE)
_QUERY_PASSWORD = re.compile(r'(password\s*=\s*)([^&\s]+)', re.IGNORECASE)


def _mask_sensitive_detail(detail: str | None) -> str:
    if not detail:
        return ""
    masked = str(detail)
    masked = _JSON_DBL_PASSWORD.sub(lambda m: f"{m.group(1)}******{m.group(3)}", masked)
    masked = _JSON_SGL_PASSWORD.sub(lambda m: f"{m.group(1)}******{m.group(3)}", masked)
    masked = _QUERY_PASSWORD.sub(r"\1******", masked)
    return masked


def _extract_json_detail(body: bytes | bytearray | str | None) -> str | None:
    if body is None:
        return None
    data: Any
    payload = body
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = payload.decode("utf-8")
        except Exception:
            return None
    if isinstance(payload, str):
        payload = payload.strip()
        if not payload:
            return None
        try:
            data = json.loads(payload)
        except Exception:
            return None
    else:
        data = payload
    if isinstance(data, dict):
        detail = data.get("detail")
        if isinstance(detail, str):
            return detail
    return None


def _log_tg_proxy(
    route: str,
    tenant: int | str | None,
    status: int,
    body: bytes | bytearray | str | None,
    *,
    error: str | None = None,
    force: bool | None = None,
) -> None:
    detail_raw = error if error is not None else _stringify_detail(body)
    detail = _mask_sensitive_detail(detail_raw)
    log_fn = logger.info if 200 <= int(status or 0) < 300 else logger.warning
    tenant_value = "-" if tenant is None else tenant
    if route == "/pub/tg/password":
        log_fn("tg_proxy route=%s tenant=%s tg_code=%s", route, tenant_value, status)
        return
    force_fragment = " force=%s" % ("1" if force else "0") if force is not None else ""
    log_fn(
        "tg_proxy route=%s tenant=%s tg_code=%s%s detail=%s",
        route,
        tenant_value,
        status,
        force_fragment,
        detail or "",
    )


def _fingerprint_public_key(raw: str | None) -> str:
    if not raw:
        return "-"
    try:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    except Exception:
        return "-"
    return digest[:10]


def _log_public_tg_request(route: str, tenant_id: int, key: str | None) -> None:
    fingerprint = _fingerprint_public_key(_normalize_public_token(key))
    logger.info(
        "tg_public_request route=%s tenant=%s key=%s",
        route,
        tenant_id,
        fingerprint,
    )


def _parse_force_flag(raw_value: str | None) -> bool:
    if raw_value is None:
        return False
    value = raw_value.strip().lower()
    return value in {"1", "true", "yes", "on"}


def _password_attempt_key(tenant_id: int, token: str) -> str:
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]
    return f"tenant:{int(tenant_id)}:twofa_attempts:{digest}"


def _build_public_tg_qr_url(
    tenant_id: int, key: str | None, qr_id: str | None = None
) -> str:
    parts: list[tuple[str, str]] = [("tenant", str(tenant_id))]
    normalized_key = _normalize_public_token(key)
    if normalized_key:
        parts.append(("k", normalized_key))
    if qr_id:
        parts.append(("qr_id", qr_id))
    return f"/pub/tg/qr.png?{urlencode(parts)}"


def _register_password_attempt(tenant_id: int, client_token: str) -> tuple[bool, int | None]:
    token = (client_token or "-").strip() or "-"
    key = _password_attempt_key(tenant_id, token)
    try:
        client = common.redis_client()
    except Exception:
        client = None

    if client is not None:
        try:
            pipe = client.pipeline()
            pipe.incr(key)
            pipe.ttl(key)
            attempts, ttl = pipe.execute()
            if attempts == 1 or ttl is None or ttl < 0:
                client.expire(key, int(PASSWORD_ATTEMPT_WINDOW))
                ttl = client.ttl(key)
            if attempts > PASSWORD_ATTEMPT_LIMIT:
                retry_after = int(ttl) if ttl and ttl > 0 else int(PASSWORD_ATTEMPT_WINDOW)
                return False, retry_after
            return True, None
        except redis_ex.RedisError:
            client = None

    now = time.monotonic()
    local_key = (int(tenant_id), token)
    entries = _LOCAL_PASSWORD_ATTEMPTS.setdefault(local_key, [])
    cutoff = now - PASSWORD_ATTEMPT_WINDOW
    filtered = [stamp for stamp in entries if stamp > cutoff]
    allowed = len(filtered) < PASSWORD_ATTEMPT_LIMIT
    retry_after: int | None = None
    if allowed:
        filtered.append(now)
    else:
        if filtered:
            remaining = PASSWORD_ATTEMPT_WINDOW - (now - filtered[0])
            retry_after = max(1, int(math.ceil(remaining))) if remaining > 0 else 1
        else:
            retry_after = int(PASSWORD_ATTEMPT_WINDOW)
    _LOCAL_PASSWORD_ATTEMPTS[local_key] = filtered
    return allowed, retry_after


def _client_identifier(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        first = forwarded.split(",")[0].strip()
        if first:
            return first
    client = getattr(request, "client", None)
    host = getattr(client, "host", None) if client else None
    if host:
        return str(host)
    return "-"

MAX_UPLOAD_SIZE_BYTES = 25 * 1024 * 1024
ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".pdf"}
CSV_ENCODING_CANDIDATES = ["utf-8", "utf-8-sig", "cp1251", "windows-1251", "koi8-r"]


def _coerce_tenant(raw: int | str | None) -> int:
    if raw is None:
        raise ValueError("missing_tenant")
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            raise ValueError("missing_tenant")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_tenant") from exc


def _normalize_headers(raw: Iterable[Any]) -> list[str]:
    normalized: list[str] = []
    seen: dict[str, int] = {}
    for idx, cell in enumerate(raw):
        text = "" if cell is None else str(cell)
        clean = text.strip().lstrip("\ufeff")
        if not clean:
            clean = f"column_{idx + 1}"
        if clean in seen:
            seen[clean] += 1
            clean = f"{clean}_{seen[clean]}"
        else:
            seen[clean] = 0
        normalized.append(clean)
    if not normalized:
        normalized.append("title")
    return normalized


def _relative_to(path: pathlib.Path, root: pathlib.Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def _make_safe_filename(filename: str, ext: str, *, fallback: str) -> str:
    base = pathlib.Path(filename).stem or fallback
    base = re.sub(r"[^0-9A-Za-z._-]+", "_", base)
    base = base.strip("._") or fallback
    return f"{base}{ext}"


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value)


def _read_csv_bytes(raw: bytes) -> tuple[list[dict[str, str]], dict[str, Any]]:
    encoding_used: str | None = None
    text: str | None = None
    for encoding in CSV_ENCODING_CANDIDATES:
        try:
            text = raw.decode(encoding)
            encoding_used = encoding
            break
        except UnicodeDecodeError:
            continue
    if text is None or encoding_used is None:
        raise ValueError("encoding_detection_failed")

    stream = io.StringIO(text)
    sample = stream.read(2048)
    stream.seek(0)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ","

    reader = csv.reader(stream, delimiter=delimiter)
    header: list[str] | None = None
    for row in reader:
        if row and any((cell or "").strip() for cell in row):
            header = _normalize_headers(row)
            break
    records: list[dict[str, str]] = []
    if header is None:
        header = ["title"]
    for row in reader:
        if not row or not any((_stringify(cell) for cell in row)):
            continue
        while len(header) < len(row):
            header.append(f"column_{len(header) + 1}")
        record: dict[str, str] = {}
        for idx, value in enumerate(row):
            key = header[idx]
            record[key] = _stringify(value)
        if any(record.values()):
            records.append(record)

    meta = {
        "type": "csv",
        "encoding": encoding_used,
        "delimiter": delimiter,
        "columns": header,
    }
    normalized = _normalize_catalog_items(records, meta)
    return normalized, meta


def _read_excel_bytes(raw: bytes) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if load_workbook is None:
        raise RuntimeError("excel_support_unavailable")

    workbook = load_workbook(filename=io.BytesIO(raw), read_only=True, data_only=True)
    try:
        sheet = workbook.active
        header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True), None)
        if not header_row:
            header = ["title"]
        else:
            header = _normalize_headers(header_row)
        records: list[dict[str, str]] = []
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if row is None:
                continue
            record: dict[str, str] = {}
            values = list(row)
            while len(header) < len(values):
                header.append(f"column_{len(header) + 1}")
            for idx, value in enumerate(values):
                key = header[idx]
                record[key] = _stringify(value)
            if any(record.values()):
                records.append(record)
    finally:
        workbook.close()

    meta = {
        "type": "excel",
        "columns": header,
        "sheet": sheet.title if sheet is not None else "Sheet1",
    }
    normalized = _normalize_catalog_items(records, meta)
    return normalized, meta


def _collapse_items_one_per_page(index, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_page: dict[str, dict[str, Any]] = {}
    def score(it: dict[str, Any]) -> tuple[int, int]:
        price = str(it.get("price") or "").strip()
        has_price = 1 if price else 0
        attr_count = len([k for k in it.keys() if k not in {"id", "title", "price"}])
        title_len = len(str(it.get("title") or ""))
        return (has_price, max(attr_count, title_len))

    def _is_attr_like_title(title: str) -> bool:
        t = (title or "").strip().lower()
        if not t:
            return True
        attr_tokens = (
            "толщина", "размер", "ширина", "высота", "диаметр",
            "материал", "цвет", "уплотнен", "замок", "замк",
        )
        if any(tok in t for tok in attr_tokens):
            # Allow if looks like model (letters+digits mixed)
            has_letter = any(ch.isalpha() for ch in t)
            has_digit = any(ch.isdigit() for ch in t)
            if has_letter and has_digit:
                return False
            return True
        return False

    def _strong_enough(it: dict[str, Any]) -> bool:
        price = str(it.get("price") or "").strip()
        has_price = bool(price)
        attr_count = len([k for k in it.keys() if k not in {"id", "title", "price", "page"}])
        if has_price:
            return True
        return attr_count >= 2 and not _is_attr_like_title(it.get("title") or "")

    for it in items:
        page = str(it.get("page") or "")
        if not page:
            continue
        current = by_page.get(page)
        # Prefer items that are strong-enough and not attribute-like titles
        if (current is None) or (score(it) > score(current)):
            by_page[page] = it

    # Optionally fabricate items for pages без распознанных блоков,
    # но не для стоп-разделов.
    try:
        STOP_RE = getattr(catalog_index, '_STOP_KEYWORDS_RE', None)
    except Exception:
        STOP_RE = None
    chunks = list(getattr(index, "chunks", []) or [])
    for ch in chunks:
        pg = str(getattr(ch, "page", "") or "")
        if not pg or pg in by_page:
            continue
        title = str(getattr(ch, "title", "") or "")
        if STOP_RE is not None and STOP_RE.search(title or ""):
            continue
        by_page[pg] = {"title": title, "price": "", "page": pg}

    def page_key(k: str) -> int:
        try:
            return int(k)
        except Exception:
            return 0

    # Build map for quick chunk title lookup
    chunk_title_by_page: dict[str, str] = {}
    for ch in getattr(index, "chunks", []) or []:
        pg = str(getattr(ch, "page", "") or "")
        if pg and pg not in chunk_title_by_page:
            chunk_title_by_page[pg] = str(getattr(ch, "title", "") or "")

    result: list[dict[str, Any]] = []
    for page in sorted(by_page.keys(), key=page_key):
        candidate = dict(by_page[page])
        # If кандидат выглядит как характеристика — заменим заголовок на заголовок чанка
        if _is_attr_like_title(candidate.get("title") or ""):
            chunk_title = chunk_title_by_page.get(page) or (candidate.get("title") or "")
            candidate["title"] = chunk_title
        # Удаляем служебные поля
        candidate.pop("page", None)
        result.append(candidate)
    return result


def _process_pdf(
    *,
    tenant: int,
    saved_path: pathlib.Path,
    tenant_root: pathlib.Path,
    saved_rel_path: pathlib.Path,
    original_name: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], str | None]:
    index_dir = tenant_root / "indexes"
    index = build_pdf_index(
        saved_path,
        output_dir=index_dir,
        source_relpath=str(saved_rel_path),
        original_name=original_name,
    )
    items = index_to_catalog_items(index)
    # Optional: collapse to exactly one item per page if enabled in tenant behavior
    try:
        cfg = common.read_tenant_config(tenant)
        one_per_page = bool((cfg.get("behavior") or {}).get("pdf_one_item_per_page"))
    except Exception:
        one_per_page = False
    if one_per_page:
        items = _collapse_items_one_per_page(index, items)
    manifest_path = index.index_path.with_suffix(".manifest.json")
    manifest_rel = _relative_to(manifest_path, tenant_root) if manifest_path.exists() else None
    try:
        rel_index = _relative_to(index.index_path, tenant_root)
    except Exception:
        rel_index = str(index.index_path)
    meta: dict[str, Any] = {
        "type": "pdf",
        "index_path": rel_index,
        "indexed_at": index.generated_at,
        "chunk_count": index.chunk_count,
        "sha1": index.sha1,
        "page_count": index.page_count,
        "source_path": str(saved_rel_path),
        "original": original_name,
        "encoding": "utf-8-sig",
    }
    normalized = _normalize_catalog_items(items, meta)
    return normalized, meta, manifest_rel

router = APIRouter()


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        candidate = int(str(value).strip())
    except Exception:
        return None
    return candidate


def _find_telegram_user_id(value: Any) -> int | None:
    candidate_keys = (
        "telegram_user_id",
        "telegramUserId",
        "user_id",
        "userId",
        "from_id",
        "fromId",
    )
    if isinstance(value, dict):
        for key in candidate_keys:
            if key in value:
                candidate = _coerce_int(value.get(key))
                if candidate and candidate > 0:
                    return candidate
        for nested in value.values():
            result = _find_telegram_user_id(nested)
            if result is not None:
                return result
    elif isinstance(value, list):
        for entry in value:
            result = _find_telegram_user_id(entry)
            if result is not None:
                return result
    return None


def _find_username(value: Any) -> str | None:
    if isinstance(value, dict):
        if isinstance(value.get("username"), str) and value["username"].strip():
            return value["username"].strip()
        for nested in value.values():
            result = _find_username(nested)
            if result:
                return result
    elif isinstance(value, list):
        for entry in value:
            result = _find_username(entry)
            if result:
                return result
    return None


@router.post("/webhook/provider")
async def provider_webhook(message: MessageIn | PingEvent, request: Request) -> JSONResponse:
    admin_header = request.headers.get("X-Admin-Token", "")
    expected_token = getattr(settings, "ADMIN_TOKEN", "") or ""
    if not admin_header or admin_header != expected_token:
        raise HTTPException(status_code=401, detail="unauthorized")

    if isinstance(message, PingEvent):
        tenant_hint = message.tenant or 0
        logger.info(
            "event=webhook_ping_received tenant=%s channel=%s", tenant_hint, message.channel or ""
        )
        return JSONResponse({"ok": True, "event": "ping"})

    ts_candidate = _coerce_int(message.ts)
    if ts_candidate is None:
        ts_candidate = int(time.time() * 1000)
    ts_ms = ts_candidate
    logger.info(
        "event=webhook_received channel=%s tenant=%s ts=%s",
        message.channel,
        message.tenant,
        ts_ms,
    )

    tenant = int(message.tenant)
    channel = (message.channel or "").lower()
    if channel != "telegram":
        logger.warning(
            "event=webhook_unsupported_channel tenant=%s channel=%s", tenant, message.channel
        )
        return JSONResponse({"ok": False, "error": "unsupported_channel"}, status_code=400)
    telegram_user_id: int | None = None
    telegram_username: str | None = None
    peer_id: int | None = None

    if channel == "telegram":
        raw_payload = message.provider_raw if isinstance(message.provider_raw, dict) else {}
        telegram_user_id = _find_telegram_user_id(raw_payload)
        peer_id = _coerce_int(raw_payload.get("peer_id"))
        if peer_id is None:
            peer_id = _coerce_int(raw_payload.get("peerId"))
        if peer_id is not None and peer_id <= 0:
            peer_id = None
        if telegram_user_id is None and peer_id is not None:
            telegram_user_id = peer_id
        if telegram_user_id is None:
            telegram_user_id = _coerce_int(message.from_id)
        if telegram_user_id is None:
            telegram_user_id = _coerce_int(raw_payload.get("from_id"))
        if telegram_user_id is not None and telegram_user_id <= 0:
            telegram_user_id = None
        username_candidate = raw_payload.get("username") if isinstance(raw_payload, dict) else None
        if isinstance(username_candidate, str) and username_candidate.strip():
            telegram_username = username_candidate.strip()
        else:
            telegram_username = _find_username(raw_payload)

    lead_hint = telegram_user_id
    if lead_hint is None:
        fallback_from = _coerce_int(message.from_id)
        if fallback_from is not None and fallback_from > 0:
            lead_hint = fallback_from

    title_hint: str | None = None
    if telegram_username:
        normalized = telegram_username.lstrip("@")
        title_hint = f"tg:@{normalized}" if normalized else None
    elif telegram_user_id is not None:
        title_hint = f"tg:id {telegram_user_id}"

    if lead_hint is None:
        logger.error(
            "event=message_in_lead_missing tenant=%s channel=%s from_id=%s", tenant, channel, message.from_id
        )
        raise HTTPException(status_code=400, detail="invalid_lead")

    text_value = (message.text or "").strip()
    if not text_value:
        attachment_count = len(message.attachments or [])
        logger.info(
            "event=skip_no_text channel=telegram tenant=%s attachments=%s",
            tenant,
            attachment_count,
        )
        return JSONResponse({"ok": True, "skipped": True, "reason": "no_text"})
    message_id = ""
    generated_from_hint = False
    if isinstance(message.provider_raw, dict):
        raw_message_id = message.provider_raw.get("message_id") or message.provider_raw.get("id")
        if raw_message_id:
            message_id = str(raw_message_id)
    if not message_id:
        fallback = lead_hint if lead_hint is not None else ts_ms
        message_id = str(fallback)
        generated_from_hint = True

    attachments_data: list[dict[str, Any]] = []
    for attachment in message.attachments or []:
        try:
            if hasattr(attachment, "model_dump"):
                attachments_data.append(attachment.model_dump(by_alias=True))
            else:
                attachments_data.append(attachment.dict(by_alias=True))
        except Exception:
            logger.debug(
                "event=webhook_attachment_normalize_failed tenant=%s lead_id=%s", tenant, lead_hint,
                exc_info=True,
            )

    resolved_channel = channel or "telegram"
    from_addr = ""
    to_addr = ""
    if message.from_id is not None:
        from_addr = str(message.from_id)
    elif telegram_user_id is not None:
        from_addr = str(telegram_user_id)
    if message.to is not None:
        to_addr = str(message.to)
    elif peer_id is not None:
        to_addr = str(peer_id)
    elif telegram_user_id is not None:
        to_addr = str(telegram_user_id)

    inbox_event = {
        "event": "messages.incoming",
        "tenant": tenant,
        "lead_id": lead_hint,
        "message_id": message_id,
        "channel": resolved_channel,
        "ch": resolved_channel,
        "provider": resolved_channel,
        "text": text_value,
        "attachments": attachments_data,
        "ts": ts_ms,
        "provider_raw": message.provider_raw or {},
        "from": from_addr,
        "to": to_addr,
        "source": {"type": resolved_channel, "tenant": tenant},
    }
    if telegram_user_id is not None:
        inbox_event["telegram_user_id"] = telegram_user_id
    if telegram_username:
        inbox_event["username"] = telegram_username
    if peer_id is not None:
        inbox_event["peer_id"] = peer_id

    log_payload = dict(inbox_event)
    log_payload["attachments"] = len(message.attachments or [])
    logger.info(
        "event=webhook_normalized channel=%s tenant=%s payload=%s",
        channel,
        tenant,
        log_payload,
    )

    try:
        upsert_kwargs = {
            "channel": channel or "telegram",
            "tenant_id": tenant,
            "telegram_username": telegram_username,
            "peer_id": peer_id,
            "title": title_hint,
        }
        if telegram_user_id is not None:
            upsert_kwargs["telegram_user_id"] = int(telegram_user_id)
        lead_id = await upsert_lead(
            lead_hint,
            **upsert_kwargs,
        )
    except Exception:
        DB_ERRORS_COUNTER.labels("upsert_lead").inc()
        logger.exception("event=message_in_lead_upsert_fail tenant=%s", tenant)
        raise HTTPException(status_code=500, detail="lead_upsert_failed")

    if generated_from_hint and lead_id and lead_id != lead_hint:
        message_id = str(lead_id)
        inbox_event["message_id"] = message_id
    inbox_event["lead_id"] = lead_id
    try:
        client = common.redis_client()
    except Exception:
        client = None
    if client is not None:
        try:
            result = client.lpush(
                INCOMING_QUEUE_KEY, json.dumps(inbox_event, ensure_ascii=False)
            )
            if asyncio.iscoroutine(result):
                await result
            logger.info(
                "event=incoming_enqueued channel=telegram tenant=%s lead_id=%s message_id=%s queue=%s",
                tenant,
                lead_id,
                message_id,
                INCOMING_QUEUE_KEY,
            )
        except redis_ex.RedisError:
            logger.debug(
                "event=webhook_enqueue_failed tenant=%s lead_id=%s", tenant, lead_hint, exc_info=True
            )
        except Exception:
            logger.debug(
                "event=webhook_enqueue_failed tenant=%s lead_id=%s", tenant, lead_hint, exc_info=True
            )

    try:
        await insert_message_in(
            lead_id,
            text_value,
            status="received",
            tenant_id=tenant,
            telegram_user_id=telegram_user_id,
            provider_msg_id=message_id,
        )
    except Exception:
        DB_ERRORS_COUNTER.labels("insert_message_in").inc()
        logger.exception("event=message_in_store_fail tenant=%s", tenant)
        raise HTTPException(status_code=500, detail="store_message_failed")

    MESSAGE_IN_COUNTER.labels(message.channel).inc()
    message_in_logger.info(
        "event=message_in channel=%s tenant=%s from=%s to=%s attachments=%s",
        message.channel,
        tenant,
        message.from_id,
        message.to,
        len(message.attachments),
    )

    return JSONResponse({"ok": True, "lead_id": lead_id})


@router.get("/connect/wa")
def connect_wa(tenant: int, request: Request, k: str | None = None, key: str | None = None):
    tenant = int(tenant)
    access_key = (k or key or request.query_params.get("k") or request.query_params.get("key") or "").strip()
    if not common.valid_key(tenant, access_key):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    common.ensure_tenant_files(tenant)
    cfg = common.read_tenant_config(tenant)
    persona = common.read_persona(tenant)
    passport = cfg.get("passport", {})
    subtitle = passport.get("brand") or "Подключение WhatsApp" if passport else "Подключение WhatsApp"
    persona_preview = "\n".join((persona or "").splitlines()[:6])

    settings_link = ""
    if access_key:
        raw_settings = request.url_for('client_settings', tenant=str(tenant))
        settings_link = common.public_url(request, f"{raw_settings}?k={quote_plus(access_key)}")

    context = {
        "request": request,
        "tenant": tenant,
        "key": access_key,
        "k": access_key,
        "timestamp": int(time.time()),
        "passport": passport,
        "persona_preview": persona_preview,
        "title": "Подключение WhatsApp",
        "subtitle": subtitle,
        "settings_link": settings_link,
        "public_base": common.public_base_url(request),
    }
    return templates.TemplateResponse(request, "connect/wa.html", context)


@router.get("/connect/tg")
def connect_tg(tenant: int, request: Request, k: str | None = None, key: str | None = None):
    tenant = int(tenant)
    access_key = (k or key or request.query_params.get("k") or request.query_params.get("key") or "").strip()
    if not common.valid_key(tenant, access_key):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    common.ensure_tenant_files(tenant)
    cfg = common.read_tenant_config(tenant)
    passport = cfg.get("passport", {}) if isinstance(cfg, dict) else {}
    brand = ""
    if isinstance(passport, dict):
        brand = str(passport.get("brand") or "").strip()

    persona_text = common.read_persona(tenant)
    persona_preview = ""
    if persona_text:
        lines = str(persona_text).splitlines()
        persona_preview = "\n".join(lines[:6]).strip()

    primary_key = (common.get_tenant_pubkey(tenant) or "").strip()
    resolved_key = primary_key or access_key

    public_key = getattr(settings, "PUBLIC_KEY", "")
    encoded_public_key = quote_plus(public_key)
    tg_qr_url = f"/pub/tg/qr.png?k={encoded_public_key}" if public_key else "/pub/tg/qr.png"
    tg_status_url = f"/pub/tg/status?k={encoded_public_key}" if public_key else "/pub/tg/status"
    tg_start_url = f"/pub/tg/start?k={encoded_public_key}" if public_key else "/pub/tg/start"
    tg_twofa_url = f"/pub/tg/2fa?k={encoded_public_key}" if public_key else "/pub/tg/2fa"

    tg_connect_config = {
        "tenant": tenant,
        "key": public_key or resolved_key,
        "urls": {
            "public_key": public_key,
            "tg_status": "/pub/tg/status",
            "tg_status_url": tg_status_url,
            "tg_start": "/pub/tg/start",
            "tg_start_url": tg_start_url,
            "tg_qr_png": tg_qr_url,
            "tg_2fa": "/pub/tg/2fa",
            "tg_2fa_url": tg_twofa_url,
            "tg_password": "/pub/tg/2fa",
        },
    }

    context = {
        "request": request,
        "tenant": tenant,
        "key": public_key or resolved_key,
        "tenant_key": access_key,
        "subtitle": brand,
        "persona_preview": persona_preview,
        "tg_connect_config": tg_connect_config,
    }
    return templates.TemplateResponse(request, "connect/tg.html", context)


@router.get("/pub/wa/status")
async def wa_status(tenant: int, k: str):
    tenant = int(tenant)
    if not common.valid_key(tenant, k):
        return JSONResponse({"ok": False, "error": "invalid_key"}, status_code=401)
    try:
        webhook = common.webhook_url()
        payload = {"tenant_id": int(tenant), "webhook_url": webhook}
        resp = await common.wa_post("/session/start", payload)
        status = int(getattr(resp, "status_code", 0) or 0)
        if status == 404:
            await common.wa_post(f"/session/{int(tenant)}/start", payload)
    except Exception:
        pass
    result = await _wa_status_impl(tenant)
    return result


async def _wa_status_impl(tenant: int) -> dict:
    # Read status from tenant-scoped endpoint with fallback to legacy global endpoint
    code, raw = common.http("GET", f"{common.WA_WEB_URL}/session/{int(tenant)}/status")
    if int(code or 0) == 404:
        code, raw = common.http("GET", f"{common.WA_WEB_URL}/session/status")
    try:
        data = json.loads(raw)
    except Exception:
        data = {}
    ready = bool(data.get("ready")) if isinstance(data, dict) else False
    qr = bool(data.get("qr")) if isinstance(data, dict) else False
    last = data.get("last") if isinstance(data, dict) else None
    if isinstance(data, dict) and "connected" in data:
        connected = bool(data.get("connected"))
    else:
        connected = ready
    return {"ok": True, "ready": ready, "connected": connected, "qr": qr, "last": last}

def _fetch_qr_bytes(url: str, timeout: float = 6.0):
    req = urllib.request.Request(url, method="GET")
    # Propagate waweb auth token if configured
    try:
        token = getattr(C, "WA_WEB_TOKEN", "") or getattr(C, "WA_INTERNAL_TOKEN", "") or ""
        if token:
            req.add_header("X-Auth-Token", token)
    except Exception:
        pass
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            ctype = resp.headers.get("Content-Type", "")
            try:
                wa_logger.info("qr_upstream ok code=%s ctype=%s len=%s", getattr(resp, 'status', 200), ctype, len(body or b""))
            except Exception:
                pass
            return resp.status, ctype, body
    except urllib.error.HTTPError as e:
        try:
            data = e.read()
        except Exception:
            data = b""
        try:
            wa_logger.info("qr_upstream http_error code=%s len=%s", getattr(e, 'code', 0), len(data or b""))
        except Exception:
            pass
        return e.code, "", data
    except Exception as exc:  # pragma: no cover
        try:
            wa_logger.exception("qr_upstream failed: %s", exc)
        except Exception:
            pass
        return 0, "", b""


def _build_qr_candidates(tenant: int, cache_bust: int) -> list[tuple[str, str]]:
    base = common.WA_WEB_URL.rstrip("/")
    ts_param = f"ts={cache_bust}"
    return [
        (f"{base}/session/{tenant}/qr?format=svg&{ts_param}", "tenant_query_svg"),
        (f"{base}/session/{tenant}/qr.svg?{ts_param}", "tenant_ext_svg"),
        (f"{base}/session/{tenant}/qr.png?{ts_param}", "tenant_ext_png"),
        (f"{base}/session/qr?format=svg&{ts_param}", "global_query_svg"),
        (f"{base}/session/qr.svg?{ts_param}", "global_ext_svg"),
        (f"{base}/session/qr?format=png&{ts_param}", "global_query_png"),
        (f"{base}/session/qr.png?{ts_param}", "global_ext_png"),
    ]


def _proxy_qr_with_fallbacks(tenant: int) -> Response:
    wa_logger.info("qr_fetch start tenant=%s", tenant)
    if getattr(settings, "WA_PREFETCH_START", True):
        try:
            hook = common.webhook_url()
            payload = json.dumps({"tenant_id": int(tenant), "webhook_url": hook}, ensure_ascii=False).encode("utf-8")
            code, _ = common.http("POST", f"{common.WA_WEB_URL}/session/{int(tenant)}/start", body=payload, timeout=4.0)
            wa_logger.info("qr_prefetch_start code=%s", code)
        except Exception:
            wa_logger.info("qr_prefetch_start_failed")

    cache_bust = int(time.time() * 1000)
    candidates = _build_qr_candidates(tenant, cache_bust)

    last_status = 0
    last_stage = ""
    last_body_present = False
    last_content_type = ""
    for url, stage in candidates:
        wa_logger.info("qr_fetch url=%s stage=%s", url, stage)
        status, ctype, body = _fetch_qr_bytes(url)
        last_status, last_stage = status, stage
        last_body_present = bool(body)
        last_content_type = (ctype or "").lower()
        wa_logger.info("upstream status=%s stage=%s", status, stage)
        if int(status or 0) == 200 and last_content_type.startswith("image/") and body:
            headers = {
                "Cache-Control": "no-store",
                "X-Debug-Stage": f"served_qr:{stage}",
            }
            wa_logger.info("return=200 len=%s ctype=%s stage=%s", len(body or b""), ctype, stage)
            return StreamingResponse(io.BytesIO(body), media_type=ctype, headers=headers)

    headers = _no_store_headers()
    headers["Cache-Control"] = "no-store"
    if int(last_status or 0) in (204, 404) or (
        int(last_status or 0) == 200 and (not last_body_present or not last_content_type.startswith("image/"))
    ):
        stage_label = last_stage or "unknown"
        headers["X-Debug-Stage"] = f"no_content:{stage_label}"
        wa_logger.info("return=204 stage=%s status=%s", last_stage, last_status)
        return Response(status_code=204, headers=headers)

    headers["X-Debug-Stage"] = f"bad_gateway:{last_stage}" if last_stage else "bad_gateway"
    wa_logger.info("return=502 stage=%s status=%s", last_stage, last_status)
    return JSONResponse({"error": "wa_unavailable"}, status_code=502, headers=headers)


def _ensure_valid_qr_request(raw_tenant: int | str | None, raw_key: str | None) -> tuple[int, str] | None:
    try:
        tenant_id = _coerce_tenant(raw_tenant)
    except ValueError:
        return None
    if not raw_key:
        return None
    key = str(raw_key)
    validator = getattr(C, "valid_key", common.valid_key)
    if not validator(tenant_id, key):
        return None
    return tenant_id, key


async def _resolve_tenant_and_key(
    request: Request | None,
    raw_tenant: int | str | None,
    raw_key: str | None,
    *,
    query_keys: tuple[str, ...] = ("key", "k"),
    allow_body: bool = True,
) -> tuple[int | str | None, str | None]:
    tenant_candidate: int | str | None = raw_tenant
    key_candidate: str | None = raw_key

    if request is not None:
        if tenant_candidate is None:
            tenant_candidate = request.query_params.get("tenant")
        if not key_candidate:
            for query_key in query_keys:
                value = request.query_params.get(query_key)
                if value:
                    key_candidate = value
                    break

        needs_body = (
            allow_body and request.method.upper() in {"POST", "PUT", "PATCH"}
        )
        if needs_body and (tenant_candidate is None or not key_candidate):
            try:
                raw_body = await request.body()
            except Exception:
                raw_body = b""

            payload: dict[str, Any] = {}
            if raw_body:
                try:
                    decoded = raw_body.decode("utf-8")
                except UnicodeDecodeError:
                    decoded = ""
                if decoded:
                    try:
                        data = json.loads(decoded)
                    except json.JSONDecodeError:
                        data = {}
                    if isinstance(data, dict):
                        payload.update(data)

            if not payload:
                try:
                    form = await request.form()
                except Exception:
                    form = None
                if form is not None:
                    payload = {}
                    for form_key, value in form.multi_items():
                        if form_key not in payload:
                            payload[form_key] = value

            if tenant_candidate is None:
                tenant_candidate = payload.get("tenant")
            if not key_candidate:
                for query_key in query_keys:
                    value = payload.get(query_key)
                    if value:
                        key_candidate = value
                        break

    return tenant_candidate, key_candidate


def require_client_key(
    raw_tenant: int | str | None,
    raw_key: str | None,
) -> tuple[int, str] | Response:
    try:
        tenant_id = _coerce_tenant(raw_tenant)
    except ValueError:
        return JSONResponse({"error": "invalid_key"}, status_code=401, headers=_no_store_headers())

    key = "" if raw_key is None else str(raw_key).strip()
    if not key or not common.valid_key(tenant_id, key):
        return JSONResponse({"error": "invalid_key"}, status_code=401, headers=_no_store_headers())

    return tenant_id, key


def _normalize_public_token(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _resolve_qr_identifier(primary: str | None, legacy: str | None = None) -> str:
    candidate = primary if primary is not None else legacy
    if candidate is None:
        return ""
    return str(candidate).strip()


def _admin_token_valid(request: Request) -> bool:
    token = request.headers.get("X-Admin-Token")
    return bool(token) and token == settings.ADMIN_TOKEN


def _has_public_tg_access(
    request: Request,
    key_candidate: str | None,
    *,
    allow_admin: bool = True,
    query_param_only: bool = False,
) -> bool:
    if allow_admin and _admin_token_valid(request):
        return True
    expected = _normalize_public_token(getattr(settings, "PUBLIC_KEY", ""))
    provided = _normalize_public_token(key_candidate)
    if query_param_only and request is not None:
        provided = _normalize_public_token(request.query_params.get("k"))
    elif not provided and request is not None:
        provided = _normalize_public_token(request.query_params.get("k"))

    if expected:
        return provided == expected

    return False


def _invalid_tenant_response(
    route: str,
    tenant_candidate: int | str | None,
    *,
    force: bool | None = None,
) -> JSONResponse:
    _log_tg_proxy(route, tenant_candidate, 400, None, error="invalid_tenant", force=force)
    return JSONResponse({"error": "invalid_tenant"}, status_code=400, headers=_no_store_headers())


def _unauthorized_response(
    route: str,
    tenant_id: int | str | None,
    *,
    force: bool | None = None,
) -> JSONResponse:
    _log_tg_proxy(route, tenant_id, 401, None, error="unauthorized", force=force)
    return JSONResponse({"error": "unauthorized"}, status_code=401, headers=_no_store_headers())


def _tg_unavailable_response(
    route: str,
    tenant_id: int | str | None,
    detail: str | Exception | None,
    *,
    force: bool | None = None,
) -> JSONResponse:
    detail_text = _stringify_detail(str(detail)) if detail not in (None, "") else "tg_unavailable"
    if not detail_text:
        detail_text = "tg_unavailable"
    _log_tg_proxy(route, tenant_id, 0, None, error=detail_text, force=force)
    headers = _no_store_headers({"X-Telegram-Upstream-Status": "-"})
    body: dict[str, Any] = {"error": "tg_unavailable"}
    if detail_text and detail_text != "tg_unavailable":
        body["detail"] = detail_text
    return JSONResponse(body, status_code=502, headers=headers)


_UPSTREAM_HEADER_MAP = {"content-type": "Content-Type", "retry-after": "Retry-After"}


def _passthrough_upstream_response(
    route: str,
    tenant_id: int | str | None,
    upstream: Any,
    *,
    success_content_type: str | None = "application/json",
    error_content_type: str | None = "application/json",
    include_no_store: bool = True,
    force: bool | None = None,
) -> Response:
    status_code = int(getattr(upstream, "status_code", 0) or 0)
    body_bytes = bytes(getattr(upstream, "content", b"") or b"")
    if 200 <= status_code < 300:
        detail = None
    else:
        detail = (
            _stringify_detail(body_bytes)
            or _stringify_detail(getattr(upstream, "text", ""))
            or f"status_{status_code}"
        )
    _log_tg_proxy(route, tenant_id, status_code, body_bytes, error=detail, force=force)

    if status_code <= 0:
        headers = _no_store_headers({"X-Telegram-Upstream-Status": "-"})
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers=headers)

    headers: dict[str, str] = {}
    if include_no_store:
        headers.update(_no_store_headers())
    headers["X-Telegram-Upstream-Status"] = str(status_code)

    upstream_headers = getattr(upstream, "headers", {}) or {}
    for name, value in upstream_headers.items():
        if not value:
            continue
        lowered = name.lower()
        mapped = _UPSTREAM_HEADER_MAP.get(lowered)
        if mapped:
            headers[mapped] = value

    default_content_type = success_content_type if 200 <= status_code < 300 else error_content_type
    if default_content_type and "Content-Type" not in headers:
        headers["Content-Type"] = default_content_type

    return Response(content=body_bytes, status_code=status_code, headers=headers)


def _proxy_headers(headers: Mapping[str, str] | None, status_code: int) -> dict[str, str]:
    allowed = {"content-type", "cache-control"}
    result: dict[str, str] = {}
    for name, value in (headers or {}).items():
        if not value:
            continue
        if name.lower() in allowed:
            result[name] = value
    result["Cache-Control"] = NO_STORE_CACHE_VALUE
    result["Pragma"] = "no-cache"
    result["Expires"] = "0"
    result["X-Telegram-Upstream-Status"] = str(status_code)
    return result


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "yes", "on"}
    return False


WA_ENABLED = _truthy_flag(os.getenv("WA_ENABLED", "true"))


def _coerce_body_bytes(body: Any) -> bytes:
    if isinstance(body, (bytes, bytearray)):
        return bytes(body)
    if isinstance(body, str):
        return body.encode("utf-8")
    if body is None:
        return b""
    try:
        return json.dumps(body, ensure_ascii=False).encode("utf-8")
    except Exception:
        return b""


@router.get("/pub/wa/qr.svg")
def wa_qr_svg(tenant: int | str | None = None, k: str | None = None):
    if not WA_ENABLED:
        return JSONResponse({"error": "wa_disabled"}, status_code=503)
    ok = _ensure_valid_qr_request(tenant, k)
    if ok is None:
        return JSONResponse({"error": "invalid_key"}, status_code=401)
    tenant_id, _ = ok
    return _proxy_qr_with_fallbacks(tenant_id)


@router.api_route("/pub/tg/start", methods=["GET", "POST"])
async def tg_start(
    request: Request,
    tenant: int | str | None = None,
    k: str | None = None,
):
    route = "/pub/tg/start"
    tenant_candidate, key_candidate = await _resolve_tenant_and_key(
        request,
        tenant,
        k,
        query_keys=("k",),
        allow_body=request.method.upper() == "POST",
    )
    try:
        tenant_id = _coerce_tenant(tenant_candidate)
    except ValueError:
        return _invalid_tenant_response(route, tenant_candidate)

    if not _has_public_tg_access(
        request,
        key_candidate,
        allow_admin=False,
        query_param_only=True,
    ):
        return _unauthorized_response(route, tenant_id)

    _log_public_tg_request(route, tenant_id, key_candidate)

    fallback_paths = ["/qr/start", "/rpc/start", "/session/start"]
    payload = {"tenant": tenant_id}
    last_error: str | None = None
    upstream: httpx.Response | None = None
    last_status: int | None = None

    for candidate in fallback_paths:
        try:
            status_code, response = await _tg_call("POST", candidate, json=payload, timeout=5.0)
        except TgWorkerCallError as exc:
            last_error = exc.detail
            continue
        upstream = response
        last_status = status_code
        break

    if upstream is None:
        reason = last_error or "tg_unavailable"
        headers = _no_store_headers({"X-Telegram-Upstream-Status": "-"})
        _log_tg_proxy(route, tenant_id, 502, None, error=reason)
        return JSONResponse(
            {"error": "tg_unavailable", "detail": reason},
            status_code=502,
            headers=headers,
        )

    status_code = int(last_status or upstream.status_code)
    body_bytes = bytes(upstream.content or b"")
    headers = _no_store_headers({"X-Telegram-Upstream-Status": str(status_code)})

    if 200 <= status_code < 300:
        content_type = upstream.headers.get("content-type")
        if content_type:
            headers["Content-Type"] = content_type
        _log_tg_proxy(route, tenant_id, status_code, body_bytes, error=None)
        return Response(content=body_bytes, status_code=status_code, headers=headers)

    detail_text = upstream.text if hasattr(upstream, "text") else ""
    reason = detail_text.strip() or f"status_{status_code}"
    _log_tg_proxy(route, tenant_id, status_code, body_bytes, error=reason)
    headers["Content-Type"] = "application/json"
    return JSONResponse(
        {"error": "tg_upstream", "detail": reason},
        status_code=status_code,
        headers=headers,
    )


async def _handle_tg_twofa(
    route: str,
    request: Request,
    tenant: int | str | None,
    key: str | None,
) -> Response:
    _log_deprecated(route)
    tenant_candidate, key_candidate = await _resolve_tenant_and_key(request, tenant, key)
    try:
        tenant_id = _coerce_tenant(tenant_candidate)
    except ValueError:
        return _invalid_tenant_response(route, tenant_candidate)

    if not _has_public_tg_access(request, key_candidate, allow_admin=False):
        return _unauthorized_response(route, tenant_id)

    _log_public_tg_request(route, tenant_id, key_candidate)

    client_token = _client_identifier(request)

    password_value: str | None = None
    try:
        data = await request.json()
    except Exception:
        data = None
    if isinstance(data, dict) and "password" in data:
        candidate = data.get("password")
        if isinstance(candidate, str):
            password_value = candidate
    if password_value is None:
        try:
            form = await request.form()
        except Exception:
            form = None
        if form is not None:
            candidate = form.get("password")
            if isinstance(candidate, str):
                password_value = candidate

    password_text = (password_value or "").strip()
    if not password_text:
        _log_tg_proxy(route, tenant_id, 400, None, error="password_required")
        return JSONResponse({"error": "password_required"}, status_code=400, headers=_no_store_headers())

    allowed, retry_after = _register_password_attempt(tenant_id, client_token)
    if not allowed:
        headers = _no_store_headers()
        if retry_after and retry_after > 0:
            headers["Retry-After"] = str(int(retry_after))
        _log_tg_proxy(route, tenant_id, 429, None, error="flood_wait")
        body = {"error": "flood_wait"}
        if retry_after and retry_after > 0:
            body["retry_after"] = int(retry_after)
        return JSONResponse(body, status_code=429, headers=headers)

    fallback_paths = ["/2fa", "/rpc/twofa.submit"]
    upstream: httpx.Response | None = None
    last_error: str | None = None
    last_status: int | None = None
    payload_body = {"tenant": tenant_id, "password": password_text}

    for candidate in fallback_paths:
        try:
            status_code, response = await _tg_call("POST", candidate, json=payload_body, timeout=5.0)
        except TgWorkerCallError as exc:
            last_error = exc.detail
            continue
        upstream = response
        last_status = status_code
        break

    if upstream is None:
        reason = last_error or "tg_unavailable"
        headers = _no_store_headers({"X-Telegram-Upstream-Status": "-"})
        _log_tg_proxy(route, tenant_id, 502, None, error=reason)
        return JSONResponse({"error": "tg_unavailable", "detail": reason}, status_code=502, headers=headers)

    status_code = int(last_status or upstream.status_code)
    body_bytes = bytes(getattr(upstream, "content", b"") or b"")

    try:
        payload = upstream.json()
    except ValueError:
        payload = {}

    error_code = str(payload.get("error") or "").strip()
    headers = _no_store_headers({"X-Telegram-Upstream-Status": str(status_code or "-")})

    if status_code <= 0:
        _log_tg_proxy(route, tenant_id, status_code, body_bytes, error="tg_unavailable")
        headers["Content-Type"] = "application/json"
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers=headers)

    if status_code == 401 or error_code == "bad_password":
        response = {"error": "bad_password"}
        detail = payload.get("detail")
        if detail:
            response["detail"] = detail
        _log_tg_proxy(route, tenant_id, 401, body_bytes, error="bad_password")
        headers["Content-Type"] = "application/json"
        return JSONResponse(response, status_code=401, headers=headers)

    if status_code == 409 and error_code:
        _log_tg_proxy(route, tenant_id, 409, body_bytes, error=error_code)
        headers["Content-Type"] = "application/json"
        return JSONResponse({"error": error_code}, status_code=409, headers=headers)

    if not (200 <= status_code < 300):
        failure = error_code or payload.get("detail") or f"status_{status_code}"
        _log_tg_proxy(route, tenant_id, status_code, body_bytes, error=failure)
        headers["Content-Type"] = "application/json"
        return JSONResponse({"error": failure}, status_code=502, headers=headers)

    state_value = str(payload.get("state") or payload.get("status") or "").strip()
    needs_twofa = bool(state_value == "need_2fa" or payload.get("needs_2fa"))
    response_payload = {
        "authorized": bool(payload.get("authorized")),
        "state": state_value,
        "needs_2fa": needs_twofa,
        "last_error": payload.get("last_error"),
        "expires_at": payload.get("expires_at"),
        "ok": bool(payload.get("ok", True)),
    }
    _log_tg_proxy(route, tenant_id, status_code, body_bytes, error=None)
    headers["Content-Type"] = "application/json"
    return JSONResponse(response_payload, headers=headers)


@router.post("/pub/tg/2fa")
async def tg_twofa(
    request: Request,
    tenant: int | str | None = None,
    k: str | None = None,
    key: str | None = None,
):
    return await _handle_tg_twofa("/pub/tg/2fa", request, tenant, k or key)


@router.post("/pub/tg/twofa.submit")
async def tg_twofa_submit(
    request: Request,
    tenant: int | str | None = None,
    k: str | None = None,
    key: str | None = None,
):
    return await _handle_tg_twofa("/pub/tg/twofa.submit", request, tenant, k or key)


@router.post("/pub/tg/password")
async def tg_password(
    request: Request,
    tenant: int | str | None = None,
    k: str | None = None,
    key: str | None = None,
):
    return await _handle_tg_twofa("/pub/tg/password", request, tenant, k or key)


@router.post("/pub/tg/restart")
async def tg_restart(
    request: Request,
    tenant: int | str | None = None,
    k: str | None = None,
    key: str | None = None,
):
    route = "/pub/tg/restart"
    _log_deprecated(route)
    tenant_candidate, key_candidate = await _resolve_tenant_and_key(request, tenant, k or key)
    try:
        tenant_id = _coerce_tenant(tenant_candidate)
    except ValueError:
        return _invalid_tenant_response(route, tenant_candidate, force=True)

    if not _has_public_tg_access(request, key_candidate):
        return _unauthorized_response(route, tenant_id, force=True)

    try:
        upstream = await C.tg_post(
            "/session/restart",
            {"tenant_id": tenant_id},
            timeout=5.0,
        )
    except httpx.HTTPError as exc:
        return _tg_unavailable_response(route, tenant_id, exc, force=True)
    except Exception as exc:
        return _tg_unavailable_response(route, tenant_id, exc, force=True)

    return _passthrough_upstream_response(route, tenant_id, upstream, force=True)

@router.get("/pub/tg/status")
async def tg_status(request: Request, tenant: int | str | None = None, k: str | None = None):
    route = "/pub/tg/status"
    tenant_candidate, key_candidate = await _resolve_tenant_and_key(
        request,
        tenant,
        k,
        query_keys=("k",),
        allow_body=False,
    )
    try:
        tenant_id = _coerce_tenant(tenant_candidate)
    except ValueError:
        return _invalid_tenant_response(route, tenant_candidate)

    if not _has_public_tg_access(
        request,
        key_candidate,
        allow_admin=False,
        query_param_only=True,
    ):
        return _unauthorized_response(route, tenant_id)

    _log_public_tg_request(route, tenant_id, key_candidate)

    fallback_paths = ["/status", "/rpc/status", "/session/status"]
    params = {"tenant": tenant_id}
    last_error: str | None = None
    upstream: httpx.Response | None = None
    last_status: int | None = None

    for candidate in fallback_paths:
        try:
            status_code, response = await _tg_call("GET", candidate, params=params, timeout=5.0)
        except TgWorkerCallError as exc:
            last_error = exc.detail
            continue
        if not (200 <= status_code < 300):
            detail_text = response.text if hasattr(response, "text") else ""
            last_error = detail_text.strip() or f"status_{status_code}"
            continue
        upstream = response
        last_status = status_code
        break

    if upstream is None:
        reason = last_error or "tg_unavailable"
        headers = _no_store_headers({"X-Telegram-Upstream-Status": "-"})
        _log_tg_proxy(route, tenant_id, 502, None, error=reason)
        return JSONResponse({"error": "tg_unavailable", "detail": reason}, status_code=502, headers=headers)

    status_code = int(last_status or upstream.status_code)
    body_bytes = bytes(upstream.content or b"")
    headers = _no_store_headers({"X-Telegram-Upstream-Status": str(status_code)})
    content_type = upstream.headers.get("content-type")
    if content_type:
        headers["Content-Type"] = content_type
    _log_tg_proxy(route, tenant_id, status_code, body_bytes, error=None)
    return Response(content=body_bytes, status_code=status_code, headers=headers)


@router.get("/pub/tg/qr.png")
async def tg_qr_png(
    request: Request,
    tenant: int | str | None = None,
    qr_id: str | None = None,
    k: str | None = None,
):
    route = "/pub/tg/qr.png"
    tenant_candidate, key_candidate = await _resolve_tenant_and_key(
        request,
        tenant,
        k,
        query_keys=("k",),
        allow_body=False,
    )
    try:
        tenant_id = _coerce_tenant(tenant_candidate)
    except ValueError:
        return _invalid_tenant_response(route, tenant_candidate)

    if not _has_public_tg_access(
        request,
        key_candidate,
        allow_admin=False,
        query_param_only=True,
    ):
        return _unauthorized_response(route, tenant_id)

    qr_identifier = _resolve_qr_identifier(qr_id, request.query_params.get("id"))
    if not qr_identifier:
        _log_tg_proxy(route, tenant_id, 400, None, error="missing_qr_id")
        return JSONResponse({"error": "missing_qr_id"}, status_code=400, headers=_no_store_headers())

    safe_qr = quote(qr_identifier, safe="")
    base_params = {"tenant": tenant_id}
    fallback_paths: list[tuple[str, dict[str, Any]]] = [
        ("/qr/png", {**base_params, "qr_id": qr_identifier}),
        (f"/session/qr/{safe_qr}.png", dict(base_params)),
    ]

    upstream: httpx.Response | None = None
    last_status: int | None = None
    last_error: str | None = None

    for candidate, params in fallback_paths:
        try:
            status_code, response = await _tg_call("GET", candidate, params=params, timeout=5.0)
        except TgWorkerCallError as exc:
            last_error = exc.detail
            continue
        if not (200 <= status_code < 300):
            detail_text = response.text if hasattr(response, "text") else ""
            last_error = detail_text.strip() or f"status_{status_code}"
            continue
        upstream = response
        last_status = status_code
        break

    if upstream is None:
        reason = last_error or "tg_unavailable"
        headers = _no_store_headers({"X-Telegram-Upstream-Status": "-"})
        _log_tg_proxy(route, tenant_id, 502, None, error=reason)
        return JSONResponse({"error": "tg_unavailable", "detail": reason}, status_code=502, headers=headers)

    status_code = int(last_status or upstream.status_code)
    body_bytes = bytes(upstream.content or b"")
    headers = _no_store_headers({"X-Telegram-Upstream-Status": str(status_code)})
    headers["Cache-Control"] = "no-store"
    content_type = upstream.headers.get("content-type") or "image/png"
    headers["Content-Type"] = content_type
    _log_tg_proxy(route, tenant_id, status_code, body_bytes, error=None)
    return Response(content=body_bytes, status_code=status_code, headers=headers)


@router.get("/pub/tg/qr.txt")
def tg_qr_txt(request: Request, qr_id: str | None = None, k: str | None = None, key: str | None = None):
    key_candidate = k or key or request.query_params.get("k") or request.query_params.get("key")
    if not _has_public_tg_access(request, key_candidate):
        return _unauthorized_response("/pub/tg/qr.txt", None)
    qr_value = _resolve_qr_identifier(qr_id, request.query_params.get("id"))
    if not qr_value:
        _log_tg_proxy("/pub/tg/qr.txt", None, 400, None, error="missing_qr_id")
        return JSONResponse(
            {"error": "missing_qr_id"},
            status_code=400,
            headers=_no_store_headers(),
        )

    safe_qr = quote(qr_value, safe="")
    status_code, body, headers = common.tg_http(
        "GET",
        f"{TG_WORKER_BASE}/session/qr/{safe_qr}.txt",
        timeout=15.0,
    )
    body_bytes = body if isinstance(body, (bytes, bytearray)) else ("" if body is None else str(body)).encode("utf-8")
    detail_from_json = _extract_json_detail(body_bytes)
    if status_code == 200:
        detail = None
    elif detail_from_json:
        detail = detail_from_json
    else:
        detail = _stringify_detail(body_bytes) or f"status_{status_code}"

    _log_tg_proxy("/pub/tg/qr.txt", None, status_code, body_bytes, error=detail)

    if status_code <= 0:
        return JSONResponse(
            {"error": "tg_unavailable"},
            status_code=502,
            headers=_no_store_headers({"X-Telegram-Upstream-Status": str(status_code)}),
        )

    if status_code == 404:
        detail_value = detail_from_json or "qr_not_found"
        headers_out = _proxy_headers(headers or {}, status_code)
        headers_out.update(_no_store_headers())
        if not body_bytes:
            body_bytes = json.dumps({"detail": detail_value}).encode("utf-8")
        media_type = headers_out.get("Content-Type") or "application/json"
        return Response(
            content=body_bytes,
            status_code=status_code,
            headers=headers_out,
            media_type=media_type,
        )

    if status_code != 200:
        headers_out = _proxy_headers(headers or {}, status_code)
        headers_out.update(_no_store_headers())
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers=headers_out)

    response_headers = _proxy_headers(headers or {}, status_code)
    response_headers.update(_no_store_headers())
    response_headers.setdefault("Content-Type", "text/plain; charset=utf-8")
    return Response(content=body_bytes, status_code=status_code, headers=response_headers)


@router.api_route("/pub/tg/logout", methods=["GET", "POST"])
async def tg_logout(
    request: Request,
    tenant: int | str | None = None,
    k: str | None = None,
    key: str | None = None,
):
    route = "/pub/tg/logout"
    tenant_candidate, key_candidate = await _resolve_tenant_and_key(request, tenant, k or key)
    try:
        tenant_id = _coerce_tenant(tenant_candidate)
    except ValueError:
        return _invalid_tenant_response(route, tenant_candidate)

    if not _has_public_tg_access(request, key_candidate):
        return _unauthorized_response(route, tenant_id)

    try:
        upstream = await C.tg_post(
            "/session/logout",
            {"tenant_id": tenant_id},
            timeout=5.0,
        )
    except httpx.HTTPError as exc:
        return _tg_unavailable_response(route, tenant_id, exc)
    except Exception as exc:
        return _tg_unavailable_response(route, tenant_id, exc)

    return _passthrough_upstream_response(route, tenant_id, upstream)


@router.get("/pub/wa/qr.png")
def wa_qr_png(tenant: int | str | None = None, k: str | None = None):
    if not WA_ENABLED:
        return JSONResponse({"error": "wa_disabled"}, status_code=503)
    ok = _ensure_valid_qr_request(tenant, k)
    if ok is None:
        return JSONResponse({"error": "invalid_key"}, status_code=401)
    tenant_id, _ = ok
    return _proxy_qr_with_fallbacks(tenant_id)


@router.post("/pub/wa/restart")
async def wa_restart(request: Request, tenant: int | None = None, k: str | None = None):
    """Force-restart waweb session to issue a fresh QR.

    Security: requires a valid public access key `k` for the tenant.
    """

    payload: dict[str, Any] = {}
    if tenant is None or not k:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

    raw_tenant = tenant if tenant is not None else payload.get("tenant")
    raw_key = k or payload.get("k") or payload.get("key")

    if raw_tenant is None or raw_key is None:
        return JSONResponse({"error": "invalid_key"}, status_code=401)

    tenant_id = int(raw_tenant)
    key = str(raw_key)

    if not common.valid_key(tenant_id, key):
        return JSONResponse({"error": "invalid_key"}, status_code=401)

    wa_logger.info("wa_restart click tenant=%s", tenant_id)

    try:
        webhook = common.webhook_url()
        start_payload = json.dumps({"tenant_id": tenant_id, "webhook_url": webhook}, ensure_ascii=False).encode("utf-8")
        empty_payload = json.dumps({}, ensure_ascii=False).encode("utf-8")

        code_restart, _ = common.http(
            "POST",
            f"{common.WA_WEB_URL}/session/{tenant_id}/restart",
            body=start_payload,
        )
        if 200 <= int(code_restart or 0) < 300:
            wa_logger.info("wa_restart success tenant=%s stage=tenant_restart code=%s", tenant_id, code_restart)
            return JSONResponse({"ok": True})

        code_logout, _ = common.http(
            "POST",
            f"{common.WA_WEB_URL}/session/{tenant_id}/logout",
            body=empty_payload,
        )
        code_start, _ = common.http(
            "POST",
            f"{common.WA_WEB_URL}/session/{tenant_id}/start",
            body=start_payload,
        )
        if 200 <= int(code_start or 0) < 300:
            wa_logger.info(
                "wa_restart success tenant=%s stage=tenant_logout_start logout=%s start=%s",
                tenant_id,
                code_logout,
                code_start,
            )
            return JSONResponse({"ok": True})

        code_global_restart, _ = common.http("POST", f"{common.WA_WEB_URL}/session/restart", body=start_payload)
        if 200 <= int(code_global_restart or 0) < 300:
            wa_logger.info(
                "wa_restart success tenant=%s stage=global_restart code=%s",
                tenant_id,
                code_global_restart,
            )
            return JSONResponse({"ok": True})

        code_global_start, _ = common.http("POST", f"{common.WA_WEB_URL}/session/start", body=start_payload)
        if 200 <= int(code_global_start or 0) < 300:
            wa_logger.info(
                "wa_restart success tenant=%s stage=global_start code=%s",
                tenant_id,
                code_global_start,
            )
            return JSONResponse({"ok": True})

        wa_logger.info(
            "wa_restart failed tenant=%s codes=%s",
            tenant_id,
            {
                "tenant_restart": code_restart,
                "tenant_logout": code_logout,
                "tenant_start": code_start,
                "global_restart": code_global_restart,
                "global_start": code_global_start,
            },
        )
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)
    except Exception as exc:  # pragma: no cover
        try:
            wa_logger.exception("wa_restart_failed: %s", exc)
        except Exception:
            pass
        return JSONResponse({"error": "wa_unavailable"}, status_code=502)


@router.get("/pub/settings/get")
def settings_get(tenant: int | str | None = None, k: str | None = None):
    try:
        tenant_id = _coerce_tenant(tenant)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    if not common.valid_key(tenant_id, k or ""):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)
    common.ensure_tenant_files(tenant_id)
    cfg = common.read_tenant_config(tenant_id)
    persona = common.read_persona(tenant_id)
    return {"ok": True, "cfg": cfg, "persona": persona}


@router.post("/pub/settings/save")
async def settings_save(request: Request, tenant: int | str | None = None, k: str | None = None):
    try:
        tenant_id = _coerce_tenant(tenant)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    if not common.valid_key(tenant_id, k or ""):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)
    common.ensure_tenant_files(tenant_id)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    cfg = common.read_tenant_config(tenant_id)
    if isinstance(payload.get("cfg"), dict):
        cfg = payload["cfg"]
    else:
        for section in ["passport", "behavior", "cta", "limits", "integrations", "learning"]:
            if isinstance(payload.get(section), dict):
                cfg.setdefault(section, {}).update(payload[section])
        if isinstance(payload.get("catalogs"), list):
            cfg["catalogs"] = payload["catalogs"]
    common.write_tenant_config(tenant_id, cfg)
    if isinstance(payload.get("persona"), str):
        common.write_persona(tenant_id, payload.get("persona") or "")
    return {"ok": True}


# Move public catalog upload off the client namespace to avoid route collisions
# with the client router. The tenant is accepted as a query parameter.
@router.post("/pub/catalog/upload")
async def catalog_upload(
    tenant: int,
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    from . import client as client_module

    tenant_id = int(tenant)
    key = client_module._resolve_key(request, request.query_params.get("k"))
    authorized = client_module._auth(tenant_id, key)
    if not authorized:
        header_key = (request.headers.get("X-Access-Key") or "").strip()
        query_key = (request.query_params.get("k") or request.query_params.get("key") or "").strip()
        if key and key == header_key:
            authorized = True
        elif key and query_key and key == query_key:
            authorized = True
    if not authorized:
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    filename = (file.filename or "").strip()
    if not filename:
        return JSONResponse({"ok": False, "error": "empty_file"}, status_code=400)

    ext = pathlib.Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return JSONResponse({"ok": False, "error": "unsupported_type"}, status_code=400)

    raw = await file.read()
    if not raw:
        return JSONResponse({"ok": False, "error": "empty_file"}, status_code=400)
    if len(raw) > MAX_UPLOAD_SIZE_BYTES:
        return JSONResponse(
            {
                "ok": False,
                "error": "file_too_large",
                "max_size_bytes": MAX_UPLOAD_SIZE_BYTES,
            },
            status_code=400,
        )

    common.ensure_tenant_files(tenant_id)
    tenant_root = pathlib.Path(common.tenant_dir(tenant_id))
    uploads_dir = tenant_root / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    safe_name = _make_safe_filename(filename, ext, fallback=f"catalog_{uuid.uuid4().hex}")
    saved_upload_path = uploads_dir / safe_name
    saved_upload_path.write_bytes(raw)
    saved_upload_rel = pathlib.Path(_relative_to(saved_upload_path, tenant_root))
    relative_path = str(saved_upload_rel)

    job_id = uuid.uuid4().hex
    job_root = tenant_root / "catalog_jobs" / job_id
    job_root.mkdir(parents=True, exist_ok=True)
    status_path = job_root / "status.json"

    status_state: dict[str, Any] = {
        "job_id": job_id,
        "state": "pending",
        "error": None,
        "log": [],
        "filename": filename,
        "message": "",
    }

    def write_status(status: str | None = None, **fields: Any) -> None:
        if status is not None:
            status_state["state"] = status
        status_state["updated_at"] = int(time.time())
        for key, value in fields.items():
            status_state[key] = value
        status_path.write_text(json.dumps(status_state, ensure_ascii=False, indent=2), encoding="utf-8")

    def append_log(level: str, message: str, **extra: Any) -> None:
        entry = {"ts": int(time.time()), "level": level, "message": message}
        if extra:
            entry.update({k: v for k, v in extra.items() if v is not None})
        status_state.setdefault("log", []).append(entry)
        write_status(None, log=status_state["log"])

    def fail(error_key: str, *, http_status: int = 400, **details: Any):
        append_log("error", error_key, **details)
        write_status("failed", error=error_key, message=error_key, **details)
        return JSONResponse({"ok": False, "error": error_key, "job_id": job_id, **details}, status_code=http_status)

    mime_type, _ = mimetypes.guess_type(filename)
    write_status("received", size=len(raw), mime=mime_type, source_path=relative_path)
    append_log("info", "file_received", size=len(raw), mime=mime_type)

    # Build background job that performs heavy processing to avoid request timeouts
    def process_job() -> None:
        try:
            write_status("processing")
            append_log("info", "job_started")
            base_name = pathlib.Path(filename).stem or f"catalog_{job_id}"
            normalized_rows: list[dict[str, Any]]
            meta: dict[str, Any]
            manifest_rel: str | None = None

            # Read back from disk to keep memory footprint small
            try:
                if ext == ".csv":
                    file_bytes = saved_upload_path.read_bytes()
                    normalized_rows, meta = _read_csv_bytes(file_bytes)
                elif ext in {".xlsx", ".xls"}:
                    file_bytes = saved_upload_path.read_bytes()
                    normalized_rows, meta = _read_excel_bytes(file_bytes)
                else:
                    normalized_rows, meta, manifest_rel = _process_pdf(
                        tenant=tenant_id,
                        saved_path=saved_upload_path,
                        tenant_root=tenant_root,
                        saved_rel_path=saved_upload_rel,
                        original_name=filename,
                    )
            except CatalogIndexError as exc:
                logger.warning("PDF indexing failed", exc_info=exc)
                fail("pdf_index_failed", detail=str(exc))
                return
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("catalog processing failed", exc_info=exc)
                fail("processing_failed", detail=str(exc))
                return

            parsed_count = len(normalized_rows)
            append_log("info", "rows_parsed", items=parsed_count)

            try:
                result = write_catalog_csv(tenant_id, normalized_rows, base_name, meta)
            except Exception as exc:  # pragma: no cover - disk errors
                logger.exception("write_catalog_csv raised", exc_info=exc)
                fail("csv_write_failed", detail=str(exc))
                return

            if not isinstance(result, tuple) or len(result) != 2:
                logger.error("write_catalog_csv returned unexpected result", extra={"result": result})
                fail("csv_write_failed")
                return

            csv_rel_path, ordered_columns = result
            pipeline_info = meta.get("pipeline") if isinstance(meta, dict) else None
            items = int(meta.get("items", parsed_count)) if isinstance(meta, dict) else parsed_count
            if manifest_rel:
                meta = dict(meta)
                meta["manifest_path"] = manifest_rel

            write_status(
                "done",
                csv_path=csv_rel_path,
                items=items,
                columns=ordered_columns,
                metadata=meta,
                source_path=relative_path,
                message="completed",
            )
            if manifest_rel:
                write_status(None, manifest_path=manifest_rel)
            append_log("info", "csv_written", items=items, columns=len(ordered_columns), pipeline=pipeline_info)

            # Persist config updates
            cfg = common.read_tenant_config(tenant_id)
            if not isinstance(cfg, dict):
                cfg = {}
            catalogs = cfg.get("catalogs") if isinstance(cfg.get("catalogs"), list) else []
            catalog_type = "pdf" if ext == ".pdf" else ("excel" if ext in {".xlsx", ".xls"} else "csv")
            catalog_entry: dict[str, Any] = {
                "name": "uploaded",
                "path": relative_path,
                "type": catalog_type,
            }
            detected_encoding = _stringify(meta.get("encoding")) if isinstance(meta, dict) else ""
            if catalog_type == "csv":
                if detected_encoding:
                    catalog_entry["encoding"] = detected_encoding
                if isinstance(meta, dict) and "delimiter" in meta:
                    catalog_entry["delimiter"] = meta.get("delimiter")
            elif detected_encoding:
                catalog_entry["encoding"] = detected_encoding
            if catalog_type == "pdf":
                if isinstance(meta, dict):
                    for key in ("index_path", "indexed_at", "chunk_count", "sha1"):
                        if meta.get(key) is not None:
                            catalog_entry[key] = meta.get(key)

            if csv_rel_path:
                catalog_entry["csv_path"] = csv_rel_path

            cfg["catalogs"] = [catalog_entry] + [entry for entry in catalogs if entry.get("path") != relative_path]

            integrations = cfg.setdefault("integrations", {})
            uploaded_meta: dict[str, Any] = {
                "path": relative_path,
                "original": filename,
                "uploaded_at": int(time.time()),
                "type": catalog_type,
                "size": len(raw),
                "mime": mime_type or "application/octet-stream",
                "csv_path": csv_rel_path,
            }
            if pipeline_info:
                uploaded_meta["pipeline"] = pipeline_info
            if catalog_type == "csv":
                if detected_encoding:
                    uploaded_meta["encoding"] = detected_encoding
                if isinstance(meta, dict) and "delimiter" in meta:
                    uploaded_meta["delimiter"] = meta.get("delimiter")
            if catalog_type == "pdf" and isinstance(meta, dict):
                index_meta = {
                    "path": meta.get("index_path"),
                    "generated_at": meta.get("indexed_at"),
                    "chunks": meta.get("chunk_count"),
                    "pages": meta.get("page_count"),
                    "sha1": meta.get("sha1"),
                }
                index_meta = {k: v for k, v in index_meta.items() if v is not None}
                if index_meta:
                    uploaded_meta["index"] = index_meta
            uploaded_meta = {k: v for k, v in uploaded_meta.items() if v is not None}
            integrations["uploaded_catalog"] = uploaded_meta

            common.write_tenant_config(tenant_id, cfg)
            append_log("info", "config_updated", catalog_type=catalog_type)
        except Exception as exc:  # final safety net
            logger.exception("catalog job crashed", exc_info=exc)
            fail("job_crashed", detail=str(exc))

    # Enqueue the job and return immediately to avoid Cloudflare 524 timeouts
    if background_tasks is not None:
        background_tasks.add_task(process_job)
    else:
        # Fallback: run inline (tests) but still return fast behavior below
        try:
            process_job()
        except Exception:
            pass

    # HTML form fallback: redirect back to settings quickly
    accept_header = (request.headers.get("accept") or "").lower()
    sec_fetch_mode = (request.headers.get("sec-fetch-mode") or "").lower()
    sec_fetch_dest = (request.headers.get("sec-fetch-dest") or "").lower()
    wants_html = (
        "text/html" in accept_header
        or "application/xhtml+xml" in accept_header
        or sec_fetch_mode == "navigate"
        or sec_fetch_dest == "document"
    )
    if wants_html and (request.headers.get("x-requested-with", "").lower() == "xmlhttprequest"):
        wants_html = False
    if wants_html:
        redirect_url = request.url_for("client_settings", tenant=str(tenant_id))
        if key:
            redirect_url = f"{redirect_url}?k={quote_plus(key)}"
        return RedirectResponse(url=redirect_url, status_code=303)

    # Return job descriptor for polling client
    return JSONResponse({"ok": True, "job_id": job_id, "state": "queued"})


# Public job status endpoint aligned with the new public upload path
@router.get("/pub/catalog/upload/status/{job_id}")
def catalog_upload_status(tenant: int, job_id: str, request: Request):
    from . import client as client_module

    tenant_id = int(tenant)
    key = client_module._resolve_key(request, request.query_params.get("k"))
    authorized = client_module._auth(tenant_id, key)
    if not authorized:
        header_key = (request.headers.get("X-Access-Key") or "").strip()
        query_key = (request.query_params.get("k") or request.query_params.get("key") or "").strip()
        if key and key == header_key:
            authorized = True
        elif key and query_key and key == query_key:
            authorized = True
    if not authorized:
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    tenant_root = pathlib.Path(common.tenant_dir(tenant_id))
    status_path = tenant_root / "catalog_jobs" / job_id / "status.json"
    if not status_path.exists():
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    try:
        data = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("status read failed", exc_info=exc)
        return JSONResponse({"ok": False, "error": "status_read_failed"}, status_code=500)
    return JSONResponse({"ok": True, **data})
