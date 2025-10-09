from __future__ import annotations

import csv
import importlib
import io
import json
import logging
import mimetypes
import pathlib
import re
import sys
import time
import uuid
from typing import Any, Iterable, Mapping

from fastapi import APIRouter, File, Request, UploadFile, BackgroundTasks
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse, Response
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
    from core import _normalize_catalog_items, settings  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - fallback for isolated imports
    core_module = _import_alias("core")
    _normalize_catalog_items = core_module._normalize_catalog_items
    settings = core_module.settings

from urllib.parse import quote, quote_plus

from . import common as C
from .ui import templates

logger = logging.getLogger(__name__)
wa_logger = logging.getLogger("wa")
# Avoid duplicate logging of WA messages via root logger handlers
wa_logger.propagate = False


def _stringify_detail(value: bytes | bytearray | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(value)


def _log_tg_proxy(
    route: str,
    tenant: int | str | None,
    status: int,
    body: bytes | bytearray | str | None,
    *,
    error: str | None = None,
) -> None:
    detail = error if error is not None else _stringify_detail(body)
    log_fn = logger.info if 200 <= int(status or 0) < 300 else logger.warning
    log_fn(
        "tg_proxy route=%s tenant=%s tg_code=%s detail=%s",
        route,
        "-" if tenant is None else tenant,
        status,
        detail or "",
    )

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
        cfg = C.read_tenant_config(tenant)
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


@router.get("/connect/wa")
def connect_wa(tenant: int, request: Request, k: str | None = None, key: str | None = None):
    tenant = int(tenant)
    access_key = (k or key or request.query_params.get("k") or request.query_params.get("key") or "").strip()
    if not C.valid_key(tenant, access_key):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    C.ensure_tenant_files(tenant)
    cfg = C.read_tenant_config(tenant)
    persona = C.read_persona(tenant)
    passport = cfg.get("passport", {})
    subtitle = passport.get("brand") or "Подключение WhatsApp" if passport else "Подключение WhatsApp"
    persona_preview = "\n".join((persona or "").splitlines()[:6])

    settings_link = ""
    if access_key:
        raw_settings = request.url_for('client_settings', tenant=str(tenant))
        settings_link = C.public_url(request, f"{raw_settings}?k={quote_plus(access_key)}")
        tg_link = C.public_url(request, f"/connect/tg?tenant={tenant}&k={quote_plus(access_key)}")
    else:
        tg_link = C.public_url(request, f"/connect/tg?tenant={tenant}")

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
        "public_base": C.public_base_url(request),
        "tg_link": tg_link,
    }
    return templates.TemplateResponse(request, "public/connect_wa.html", context)


@router.get("/connect/tg")
def connect_tg(tenant: int, request: Request, k: str | None = None, key: str | None = None):
    tenant = int(tenant)
    access_key = (k or key or request.query_params.get("k") or request.query_params.get("key") or "").strip()
    if not C.valid_key(tenant, access_key):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    C.ensure_tenant_files(tenant)
    cfg = C.read_tenant_config(tenant)
    passport = cfg.get("passport", {}) if isinstance(cfg, dict) else {}
    persona = C.read_persona(tenant)
    persona_preview = "\n".join((persona or "").splitlines()[:6])

    settings_link = ""
    if access_key:
        raw_settings = request.url_for("client_settings", tenant=str(tenant))
        settings_link = C.public_url(request, f"{raw_settings}?k={quote_plus(access_key)}")

    query_params = {"tenant": str(tenant), "k": access_key}

    context = {
        "request": request,
        "tenant": tenant,
        "key": access_key,
        "timestamp": int(time.time()),
        "passport": passport,
        "persona_preview": persona_preview,
        "title": "Подключение Telegram",
        "subtitle": passport.get("brand") or "Подключение Telegram",
        "settings_link": settings_link,
        "public_base": C.public_base_url(request),
        "query": query_params,
    }
    return templates.TemplateResponse(request, "public/connect_tg.html", context)


@router.get("/pub/wa/status")
async def wa_status(tenant: int, k: str):
    tenant = int(tenant)
    if not C.valid_key(tenant, k):
        return JSONResponse({"ok": False, "error": "invalid_key"}, status_code=401)
    try:
        webhook = C.webhook_url()
        payload = {"tenant_id": int(tenant), "webhook_url": webhook}
        resp = await C.wa_post("/session/start", payload)
        status = int(getattr(resp, "status_code", 0) or 0)
        if status == 404:
            await C.wa_post(f"/session/{int(tenant)}/start", payload)
    except Exception:
        pass
    result = await _wa_status_impl(tenant)
    return result


async def _wa_status_impl(tenant: int) -> dict:
    # Read status from tenant-scoped endpoint with fallback to legacy global endpoint
    code, raw = C.http("GET", f"{C.WA_WEB_URL}/session/{int(tenant)}/status")
    if int(code or 0) == 404:
        code, raw = C.http("GET", f"{C.WA_WEB_URL}/session/status")
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
    base = C.WA_WEB_URL.rstrip("/")
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
            hook = C.webhook_url()
            payload = json.dumps({"tenant_id": int(tenant), "webhook_url": hook}, ensure_ascii=False).encode("utf-8")
            code, _ = C.http("POST", f"{C.WA_WEB_URL}/session/{int(tenant)}/start", body=payload, timeout=4.0)
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

    headers = {"Cache-Control": "no-store"}
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
    if not C.valid_key(tenant_id, key):
        return None
    return tenant_id, key


async def _resolve_tenant_and_key(
    request: Request | None,
    raw_tenant: int | str | None,
    raw_key: str | None,
) -> tuple[int | str | None, str | None]:
    tenant_candidate: int | str | None = raw_tenant
    key_candidate: str | None = raw_key

    if request is not None:
        if tenant_candidate is None:
            tenant_candidate = request.query_params.get("tenant")
        if not key_candidate:
            key_candidate = request.query_params.get("key") or request.query_params.get("k")

        needs_body = request.method.upper() in {"POST", "PUT", "PATCH"}
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
                key_candidate = payload.get("key") or payload.get("k")

    return tenant_candidate, key_candidate


def require_client_key(
    raw_tenant: int | str | None,
    raw_key: str | None,
) -> tuple[int, str] | Response:
    try:
        tenant_id = _coerce_tenant(raw_tenant)
    except ValueError:
        return JSONResponse({"error": "invalid_key"}, status_code=401, headers={"Cache-Control": "no-store"})

    key = "" if raw_key is None else str(raw_key).strip()
    if not key or not C.valid_key(tenant_id, key):
        return JSONResponse({"error": "invalid_key"}, status_code=401, headers={"Cache-Control": "no-store"})

    return tenant_id, key


def _proxy_headers(headers: Mapping[str, str] | None, status_code: int) -> dict[str, str]:
    allowed = {"content-type", "cache-control"}
    result: dict[str, str] = {}
    for name, value in (headers or {}).items():
        if not value:
            continue
        if name.lower() in allowed:
            result[name] = value
    result.setdefault("Cache-Control", "no-store")
    result["X-Telegram-Upstream-Status"] = str(status_code)
    return result


@router.get("/pub/wa/qr.svg")
def wa_qr_svg(tenant: int | str | None = None, k: str | None = None):
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
    key: str | None = None,
):
    tenant_candidate, key_candidate = await _resolve_tenant_and_key(request, tenant, k or key)
    validation = require_client_key(tenant_candidate, key_candidate)
    if isinstance(validation, Response):
        tenant_for_log: int | str | None = tenant_candidate
        try:
            tenant_for_log = _coerce_tenant(tenant_candidate)  # type: ignore[arg-type]
        except Exception:
            tenant_for_log = tenant_candidate
        _log_tg_proxy("/pub/tg/start", tenant_for_log, getattr(validation, "status_code", 401), None, error="invalid_key")
        return validation

    tenant_id, _ = validation

    try:
        upstream = await C.tg_post("/session/start", {"tenant_id": tenant_id}, timeout=15.0)
    except Exception as exc:
        _log_tg_proxy("/pub/tg/start", tenant_id, 0, None, error=str(exc))
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers={"Cache-Control": "no-store"})

    status_code = int(getattr(upstream, "status_code", 0) or 0)
    body_bytes = bytes(getattr(upstream, "content", b"") or b"")
    if 200 <= status_code < 300:
        detail = None
    else:
        detail = _stringify_detail(body_bytes) or _stringify_detail(getattr(upstream, "text", "")) or f"status_{status_code}"

    _log_tg_proxy("/pub/tg/start", tenant_id, status_code, body_bytes, error=detail)

    if status_code <= 0:
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers={"Cache-Control": "no-store"})

    headers = _proxy_headers(getattr(upstream, "headers", {}) or {}, status_code)
    return Response(content=body_bytes, status_code=status_code, headers=headers)


@router.get("/pub/tg/status")
async def tg_status(request: Request, tenant: int | str | None = None, k: str | None = None, key: str | None = None):
    try:
        tenant_candidate, key_candidate = await _resolve_tenant_and_key(request, tenant, k or key)
        tenant_id = _coerce_tenant(tenant_candidate)
    except ValueError:
        _log_tg_proxy("/pub/tg/status", tenant, 400, None, error="invalid_tenant")
        return JSONResponse(
            {"error": "invalid_tenant"},
            status_code=400,
            headers={"Cache-Control": "no-store"},
        )

    key_value = "" if key_candidate is None else str(key_candidate).strip()
    if not key_value or not C.valid_key(tenant_id, key_value):
        _log_tg_proxy("/pub/tg/status", tenant_id, 401, None, error="invalid_key")
        return JSONResponse({"error": "invalid_key"}, status_code=401, headers={"Cache-Control": "no-store"})

    status_code, body, headers = C.tg_http("GET", f"/session/status?tenant={tenant_id}", timeout=15.0)
    body_bytes = body if isinstance(body, (bytes, bytearray)) else ("" if body is None else str(body)).encode("utf-8")
    if 200 <= status_code < 300:
        detail = None
    else:
        detail = _stringify_detail(body_bytes) or f"status_{status_code}"

    _log_tg_proxy("/pub/tg/status", tenant_id, status_code, body_bytes, error=detail)

    if status_code <= 0:
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers={"Cache-Control": "no-store"})

    response_headers = _proxy_headers(headers or {}, status_code)
    return Response(content=body_bytes, status_code=status_code, headers=response_headers)


@router.get("/pub/tg/qr.png")
def tg_qr_png(qr_id: str | None = None):
    qr_value = "" if qr_id is None else str(qr_id).strip()
    if not qr_value:
        _log_tg_proxy("/pub/tg/qr.png", None, 400, None, error="missing_qr_id")
        return JSONResponse({"error": "missing_qr_id"}, status_code=400, headers={"Cache-Control": "no-store"})

    safe_qr = quote(qr_value, safe="")
    status_code, body, headers = C.tg_http("GET", f"/session/qr/{safe_qr}.png", timeout=15.0)
    body_bytes = body if isinstance(body, (bytes, bytearray)) else ("" if body is None else str(body)).encode("utf-8")
    if status_code == 200:
        detail = None
    else:
        detail = _stringify_detail(body_bytes) or f"status_{status_code}"

    _log_tg_proxy("/pub/tg/qr.png", None, status_code, body_bytes, error=detail)

    if status_code <= 0:
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers={"Cache-Control": "no-store"})

    if status_code in (404, 410):
        headers_out = {"Cache-Control": "no-store", "X-Telegram-Upstream-Status": str(status_code)}
        return JSONResponse({"error": "qr_expired"}, status_code=status_code, headers=headers_out)

    if status_code != 200:
        headers_out = {"Cache-Control": "no-store", "X-Telegram-Upstream-Status": str(status_code)}
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers=headers_out)

    response_headers = _proxy_headers(headers or {}, status_code)
    if status_code == 200:
        response_headers.setdefault("Content-Type", "image/png")
    return Response(content=body_bytes, status_code=status_code, headers=response_headers)


@router.api_route("/pub/tg/logout", methods=["GET", "POST"])
async def tg_logout(
    request: Request,
    tenant: int | str | None = None,
    k: str | None = None,
    key: str | None = None,
):
    tenant_candidate, key_candidate = await _resolve_tenant_and_key(request, tenant, k or key)
    validation = require_client_key(tenant_candidate, key_candidate)
    if isinstance(validation, Response):
        tenant_for_log: int | str | None = tenant_candidate
        try:
            tenant_for_log = _coerce_tenant(tenant_candidate)  # type: ignore[arg-type]
        except Exception:
            tenant_for_log = tenant_candidate
        _log_tg_proxy("/pub/tg/logout", tenant_for_log, getattr(validation, "status_code", 401), None, error="invalid_key")
        return validation

    tenant_id, _ = validation

    try:
        upstream = await C.tg_post("/session/logout", {"tenant_id": tenant_id}, timeout=15.0)
    except Exception as exc:
        _log_tg_proxy("/pub/tg/logout", tenant_id, 0, None, error=str(exc))
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers={"Cache-Control": "no-store"})

    status_code = int(getattr(upstream, "status_code", 0) or 0)
    body_bytes = bytes(getattr(upstream, "content", b"") or b"")
    if 200 <= status_code < 300:
        detail = None
    else:
        detail = _stringify_detail(body_bytes) or _stringify_detail(getattr(upstream, "text", "")) or f"status_{status_code}"

    _log_tg_proxy("/pub/tg/logout", tenant_id, status_code, body_bytes, error=detail)

    if status_code <= 0:
        return JSONResponse({"error": "tg_unavailable"}, status_code=502, headers={"Cache-Control": "no-store"})

    headers = _proxy_headers(getattr(upstream, "headers", {}) or {}, status_code)
    return Response(content=body_bytes, status_code=status_code, headers=headers)


@router.get("/pub/wa/qr.png")
def wa_qr_png(tenant: int | str | None = None, k: str | None = None):
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

    if not C.valid_key(tenant_id, key):
        return JSONResponse({"error": "invalid_key"}, status_code=401)

    wa_logger.info("wa_restart click tenant=%s", tenant_id)

    try:
        webhook = C.webhook_url()
        start_payload = json.dumps({"tenant_id": tenant_id, "webhook_url": webhook}, ensure_ascii=False).encode("utf-8")
        empty_payload = json.dumps({}, ensure_ascii=False).encode("utf-8")

        code_restart, _ = C.http(
            "POST",
            f"{C.WA_WEB_URL}/session/{tenant_id}/restart",
            body=start_payload,
        )
        if 200 <= int(code_restart or 0) < 300:
            wa_logger.info("wa_restart success tenant=%s stage=tenant_restart code=%s", tenant_id, code_restart)
            return JSONResponse({"ok": True})

        code_logout, _ = C.http(
            "POST",
            f"{C.WA_WEB_URL}/session/{tenant_id}/logout",
            body=empty_payload,
        )
        code_start, _ = C.http(
            "POST",
            f"{C.WA_WEB_URL}/session/{tenant_id}/start",
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

        code_global_restart, _ = C.http("POST", f"{C.WA_WEB_URL}/session/restart", body=start_payload)
        if 200 <= int(code_global_restart or 0) < 300:
            wa_logger.info(
                "wa_restart success tenant=%s stage=global_restart code=%s",
                tenant_id,
                code_global_restart,
            )
            return JSONResponse({"ok": True})

        code_global_start, _ = C.http("POST", f"{C.WA_WEB_URL}/session/start", body=start_payload)
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
    if not C.valid_key(tenant_id, k or ""):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)
    C.ensure_tenant_files(tenant_id)
    cfg = C.read_tenant_config(tenant_id)
    persona = C.read_persona(tenant_id)
    return {"ok": True, "cfg": cfg, "persona": persona}


@router.post("/pub/settings/save")
async def settings_save(request: Request, tenant: int | str | None = None, k: str | None = None):
    try:
        tenant_id = _coerce_tenant(tenant)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    if not C.valid_key(tenant_id, k or ""):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)
    C.ensure_tenant_files(tenant_id)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    cfg = C.read_tenant_config(tenant_id)
    if isinstance(payload.get("cfg"), dict):
        cfg = payload["cfg"]
    else:
        for section in ["passport", "behavior", "cta", "limits", "integrations", "learning"]:
            if isinstance(payload.get(section), dict):
                cfg.setdefault(section, {}).update(payload[section])
        if isinstance(payload.get("catalogs"), list):
            cfg["catalogs"] = payload["catalogs"]
    C.write_tenant_config(tenant_id, cfg)
    if isinstance(payload.get("persona"), str):
        C.write_persona(tenant_id, payload.get("persona") or "")
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

    C.ensure_tenant_files(tenant_id)
    tenant_root = pathlib.Path(C.tenant_dir(tenant_id))
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
            cfg = C.read_tenant_config(tenant_id)
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

            C.write_tenant_config(tenant_id, cfg)
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

    tenant_root = pathlib.Path(C.tenant_dir(tenant_id))
    status_path = tenant_root / "catalog_jobs" / job_id / "status.json"
    if not status_path.exists():
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    try:
        data = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("status read failed", exc_info=exc)
        return JSONResponse({"ok": False, "error": "status_read_failed"}, status_code=500)
    return JSONResponse({"ok": True, **data})
