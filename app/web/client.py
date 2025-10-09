import csv
import importlib
import io
import json
import mimetypes
import os
import pathlib
import re
import subprocess
import sys
import time
import uuid
from typing import Any, Dict, Optional
from urllib.parse import quote, quote_plus, urlencode

import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, BackgroundTasks, File, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from pydantic import BaseModel, Field, ValidationError
from zoneinfo import ZoneInfo

from . import common as C
from .ui import templates


def _import_alias(module: str):
    """Load module by bare name, falling back to ``app.<module>`` when needed."""

    try:
        return importlib.import_module(module)
    except ImportError:
        fallback = importlib.import_module(f"app.{module}")
        sys.modules.setdefault(module, fallback)
        return fallback


catalog_index = _import_alias("catalog_index")
onboarding_chat = _import_alias("onboarding_chat")
training_indexer = _import_alias("training.indexer")
training_exporter = _import_alias("training.exporter")
db = _import_alias("db")
whatsapp_exporter = _import_alias("export.whatsapp")

# NOTE: expose frequently used helpers after ensuring aliases are registered
build_pdf_index = catalog_index.build_pdf_index
CatalogIndexError = catalog_index.CatalogIndexError
index_to_catalog_items = getattr(catalog_index, "index_to_catalog_items", None)
load_conversation = onboarding_chat.load_conversation
save_conversation = onboarding_chat.save_conversation
reset_conversation = onboarding_chat.reset_conversation
evaluate_preconditions = onboarding_chat.evaluate_preconditions
preconditions_met = onboarding_chat.preconditions_met
initial_assistant_turn = onboarding_chat.initial_assistant_turn
next_assistant_turn = onboarding_chat.next_assistant_turn
add_user_message = onboarding_chat.add_user_message
add_assistant_message = onboarding_chat.add_assistant_message
public_messages = onboarding_chat.public_messages
update_tenant_insights = onboarding_chat.update_tenant_insights

router = APIRouter()
_log = logging.getLogger("training")
_LOG_PREFIX = "[training]"
_wa_log = logging.getLogger("wa_export")

_CLIENT_SETTINGS_VERSION: str | None = None

MAX_UPLOAD_SIZE_BYTES = 25 * 1024 * 1024  # 25 MB safety cap for catalog uploads

DEFAULT_EXPORT_MAX_DAYS = 30
try:
    EXPORT_MAX_DAYS = int(os.getenv("EXPORT_MAX_DAYS", str(DEFAULT_EXPORT_MAX_DAYS)))
except (TypeError, ValueError):
    EXPORT_MAX_DAYS = DEFAULT_EXPORT_MAX_DAYS
if EXPORT_MAX_DAYS <= 0:
    EXPORT_MAX_DAYS = DEFAULT_EXPORT_MAX_DAYS

WHATSAPP_LIMIT_DIALOGS_MAX = 2000
WHATSAPP_PER_LIMIT_MAX = 20000
DEFAULT_WHATSAPP_BATCH_SIZE = 200


def _resolve_whatsapp_export_url(request: Request, tenant: int) -> str:
    try:
        return str(request.url_for("whatsapp_export", tenant=tenant))
    except Exception:
        try:
            base_url = str(request.url_for("whatsapp_export"))
            if "tenant=" not in base_url:
                separator = "&" if "?" in base_url else "?"
                return f"{base_url}{separator}tenant={tenant}"
            return base_url
        except Exception:
            return "/export/whatsapp"


def _client_settings_static_version() -> str:
    global _CLIENT_SETTINGS_VERSION
    if _CLIENT_SETTINGS_VERSION:
        return _CLIENT_SETTINGS_VERSION

    for env_name in ("APP_GIT_SHA", "GIT_SHA", "HEROKU_SLUG_COMMIT"):
        value = (os.getenv(env_name) or "").strip()
        if value:
            _CLIENT_SETTINGS_VERSION = value[:8] or value
            return _CLIENT_SETTINGS_VERSION

    try:
        repo_root = pathlib.Path(__file__).resolve().parents[2]
        output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
        )
        _CLIENT_SETTINGS_VERSION = output.decode("utf-8").strip()
    except Exception:
        _CLIENT_SETTINGS_VERSION = str(int(time.time()))
    return _CLIENT_SETTINGS_VERSION


class WhatsAppExportPayload(BaseModel):
    tenant: int
    key: Optional[str] = None
    days: Optional[int] = Field(default=None, ge=0)
    days_back: Optional[int] = Field(default=None, ge=0)
    limit: Optional[int] = Field(default=None, ge=0)
    limit_dialogs: Optional[int] = Field(default=None, ge=0)
    per: Optional[int] = Field(default=None, ge=0)
    per_conversation_limit: Optional[int] = Field(default=None, ge=0)
    batch_size_dialogs: Optional[int] = Field(default=None, ge=0)


def _sanitize_text(text: str) -> str:
    """Light PII scrubbing for exports: phones, emails, WA ids."""
    if not text:
        return ""
    # redact emails
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}", "<EMAIL>", text)
    # redact long digit sequences (phones, order numbers) of 5+ digits
    text = re.sub(r"(?<!\d)\d{5,}(?!\d)", "<NUMBER>", text)
    # redact whatsapp jids
    text = re.sub(r"\b\d{5,}@s\.whatsapp\.net\b", "<WA_ID>", text)
    return text


def _detect_encoding(payload: bytes) -> str:
    """Best-effort detection for common CSV encodings used by clients."""
    if not payload:
        return "utf-8"

    # Quick BOM check before falling back to heuristics
    if payload.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    candidates = [
        "utf-8",
        "utf-8-sig",
        "cp1251",
        "windows-1251",
        "koi8-r",
    ]
    for encoding in candidates:
        try:
            payload.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "utf-8"


def _resolve_key(request: Request | None, raw: str | None = None) -> str:
    candidates: list[str] = []
    if raw:
        candidates.append(raw)

    if request is not None:
        qp = request.query_params
        candidates.append(qp.get("k"))
        candidates.append(qp.get("key"))

        headers = request.headers
        for header_name in ("X-Access-Key", "X-Client-Key", "X-Auth-Key"):
            candidates.append(headers.get(header_name))

        auth_header = headers.get("Authorization")
        if auth_header:
            token = auth_header.strip()
            if token.lower().startswith("bearer "):
                token = token[7:]
            candidates.append(token)

        if request.cookies:
            candidates.append(request.cookies.get("client_key"))

    for candidate in candidates:
        if not candidate:
            continue
        value = candidate.strip()
        if value:
            return value
    return ""


def _auth(tenant: int, key: str) -> bool:
    return C.valid_key(int(tenant), key or "")


def _tenant_root(tenant: int) -> pathlib.Path:
    return pathlib.Path(C.tenant_dir(tenant))


def _safe_path(tenant: int, relative: str | pathlib.Path | None) -> pathlib.Path | None:
    if not relative:
        return None
    try:
        base = _tenant_root(tenant)
        candidate = (base / pathlib.Path(str(relative))).resolve(strict=False)
    except Exception:
        return None
    try:
        base_resolved = base.resolve(strict=False)
    except Exception:
        base_resolved = base
    if base_resolved in candidate.parents or candidate == base_resolved:
        return candidate
    return None


def _catalog_csv_path(tenant: int, cfg: dict | None = None) -> tuple[pathlib.Path | None, str | None, str | None]:
    if cfg is None or not isinstance(cfg, dict):
        cfg = C.read_tenant_config(tenant)
    if not isinstance(cfg, dict):
        return None, None, None

    catalogs = cfg.get("catalogs") if isinstance(cfg.get("catalogs"), list) else []
    for entry in catalogs:
        if not isinstance(entry, dict):
            continue
        csv_rel = entry.get("csv_path") or (entry.get("path") if entry.get("type") == "csv" else None)
        from_index = False
        if not csv_rel and entry.get("type") == "pdf" and entry.get("index_path"):
            csv_rel = str(pathlib.Path(entry["index_path"]).with_suffix(".csv"))
            from_index = True
        candidate = _safe_path(tenant, csv_rel)
        if candidate and candidate.exists():
            # CSV produced from PDF index is always UTF-8 (no BOM)
            if from_index:
                encoding = "utf-8"
            else:
                encoding = entry.get("encoding") if isinstance(entry.get("encoding"), str) else "utf-8"
            try:
                rel = str(candidate.relative_to(_tenant_root(tenant)))
            except Exception:
                rel = str(candidate)
            return candidate, encoding, rel
    # Fallback: look into integrations metadata written by public upload handler
    try:
        integrations = cfg.get("integrations") if isinstance(cfg, dict) else {}
        uploaded = integrations.get("uploaded_catalog") if isinstance(integrations, dict) else {}
        if isinstance(uploaded, dict) and uploaded.get("csv_path"):
            candidate = _safe_path(tenant, uploaded.get("csv_path"))
            if candidate and candidate.exists():
                try:
                    rel = str(candidate.relative_to(_tenant_root(tenant)))
                except Exception:
                    rel = str(candidate)
                # Use Excel-friendly default encoding (write_catalog_csv uses utf-8-sig)
                return candidate, "utf-8-sig", rel
    except Exception:
        pass
    return None, None, None


@router.get("/client/{tenant}/settings")
def client_settings(tenant: int, request: Request):
    provided_key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, provided_key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    primary_key = (C.get_tenant_pubkey(int(tenant)) or "").strip()
    key = primary_key or provided_key or ""

    C.ensure_tenant_files(tenant)
    cfg = C.read_tenant_config(tenant)
    if not isinstance(cfg, dict):
        cfg = {}

    persona = C.read_persona(tenant)

    passport_raw = cfg.get("passport", {})
    passport = passport_raw if isinstance(passport_raw, dict) else {}

    behavior_raw = cfg.get("behavior", {})
    behavior = behavior_raw if isinstance(behavior_raw, dict) else {}

    cta_raw = cfg.get("cta", {})
    cta = cta_raw if isinstance(cta_raw, dict) else {}

    integrations_raw = cfg.get("integrations", {})
    integrations = integrations_raw if isinstance(integrations_raw, dict) else {}
    telegram_state_raw = integrations.get("telegram") if isinstance(integrations, dict) else {}
    raw_state = telegram_state_raw if isinstance(telegram_state_raw, dict) else {}
    uploaded_meta = integrations.get("uploaded_catalog", {})
    if isinstance(uploaded_meta, str):
        uploaded_display = uploaded_meta
    elif isinstance(uploaded_meta, dict):
        uploaded_display = uploaded_meta.get("original") or uploaded_meta.get("path") or ""
    else:
        uploaded_display = ""

    try:
        whatsapp_export_url = str(request.url_for("whatsapp_export"))
    except Exception:
        whatsapp_export_url = _resolve_whatsapp_export_url(request, tenant)

    def _safe_public_url(name: str, fallback: str) -> str:
        try:
            return str(request.url_for(name))
        except Exception:
            return fallback

    tg_start_url = _safe_public_url("tg_start", "/pub/tg/start")
    tg_status_url = _safe_public_url("tg_status", "/pub/tg/status")
    tg_qr_png_url = _safe_public_url("tg_qr_png", "/pub/tg/qr.png")
    tg_qr_txt_url = _safe_public_url("tg_qr_txt", "/pub/tg/qr.txt")
    tg_logout_url = _safe_public_url("tg_logout", "/pub/tg/logout")
    tg_password_url = _safe_public_url("tg_password", "/pub/tg/password")

    qr_id_raw = raw_state.get("qr_id")
    if isinstance(qr_id_raw, str):
        current_qr_id = qr_id_raw.strip()
    elif qr_id_raw is None:
        current_qr_id = ""
    else:
        current_qr_id = str(qr_id_raw).strip()
    cache_bust_ms = int(time.time() * 1000)
    initial_qr_src = ""
    initial_qr_txt_href = ""
    if current_qr_id:
        qr_params = urlencode({"qr_id": current_qr_id, "t": cache_bust_ms})
        qr_separator = "&" if "?" in tg_qr_png_url else "?"
        txt_separator = "&" if "?" in tg_qr_txt_url else "?"
        initial_qr_src = f"{tg_qr_png_url}{qr_separator}{qr_params}"
        initial_qr_txt_href = f"{tg_qr_txt_url}{txt_separator}{urlencode({'qr_id': current_qr_id})}"

    urls = {
        "settings": str(request.url_for("client_settings", tenant=tenant)),
        "save_settings": str(request.url_for("save_form", tenant=tenant)),
        "save_persona": str(request.url_for("save_persona", tenant=tenant)),
        "upload_catalog": str(request.url_for("catalog_upload", tenant=tenant)),
        "csv_get": str(request.url_for("catalog_csv_get", tenant=tenant)),
        "csv_save": str(request.url_for("catalog_csv_save", tenant=tenant)),
        "training_upload": str(request.url_for("training_upload", tenant=tenant)),
        "training_status": str(request.url_for("training_status", tenant=tenant)),
        "whatsapp_export": whatsapp_export_url,
        "tg_start": tg_start_url,
        "tg_status": tg_status_url,
        "tg_qr_png": tg_qr_png_url,
        "tg_qr": tg_qr_png_url,
        "tg_qr_txt": tg_qr_txt_url,
        "tg_logout": tg_logout_url,
        "tg_password": tg_password_url,
    }

    state = {
        "tenant": tenant,
        "key": key,
        "urls": urls,
        "max_days": EXPORT_MAX_DAYS,
        "status": raw_state.get("status"),
        "qr_id": current_qr_id or "",
        "qr_valid_until": raw_state.get("qr_valid_until"),
        "needs_2fa": raw_state.get("needs_2fa"),
        "twofa_pending": raw_state.get("twofa_pending"),
        "twofa_since": raw_state.get("twofa_since"),
        "can_restart": raw_state.get("can_restart"),
        "last_error": raw_state.get("last_error"),
    }

    form_payload = {
        "brand": passport.get("brand", ""),
        "agent": passport.get("agent_name", ""),
        "city": passport.get("city", ""),
        "currency": passport.get("currency", "₽"),
        "tone": behavior.get("tone", ""),
        "cta_primary": cta.get("primary", ""),
        "cta_fallback": cta.get("fallback", ""),
        "catalog_file": uploaded_display,
    }

    state_payload = dict(state)
    state_payload["form"] = form_payload

    context = {
        "request": request,
        "tenant": tenant,
        "key": key,
        "persona": persona,
        "form": form_payload,
        "title": f"Настройки клиента · Tenant {tenant}",
        "subtitle": passport.get("brand") or "Личный кабинет клиента",
        "urls": urls,
        "state": state,
        "state_payload": state_payload,
        "max_days": EXPORT_MAX_DAYS,
        "client_settings_version": _client_settings_static_version(),
        "qr_timestamp_ms": cache_bust_ms,
        "initial_qr_src": initial_qr_src,
        "initial_qr_txt_href": initial_qr_txt_href,
    }
    return templates.TemplateResponse("client/settings.html", context)


@router.post("/client/{tenant}/settings/save")
async def save_form(tenant: int, request: Request):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    payload = await request.json()

    cfg = C.read_tenant_config(tenant)
    if not isinstance(cfg, dict):
        cfg = {}

    passport = cfg.get("passport")
    if not isinstance(passport, dict):
        passport = {}
    cfg["passport"] = passport
    passport.update(
        {
            "brand": payload.get("brand") or passport.get("brand", ""),
            "agent_name": payload.get("agent") or passport.get("agent_name", ""),
            "city": payload.get("city") or passport.get("city", ""),
            "currency": payload.get("currency") or passport.get("currency", "₽"),
        }
    )

    behavior_cfg = cfg.get("behavior")
    if not isinstance(behavior_cfg, dict):
        behavior_cfg = {}
    cfg["behavior"] = behavior_cfg
    behavior_cfg.update(
        {
            "tone": payload.get("tone") or behavior_cfg.get("tone", ""),
        }
    )

    cta_cfg = cfg.get("cta")
    if not isinstance(cta_cfg, dict):
        cta_cfg = {}
    cfg["cta"] = cta_cfg
    cta_cfg.update(
        {
            "primary": payload.get("cta_primary") or "",
            "fallback": payload.get("cta_fallback") or "",
        }
    )
    C.write_tenant_config(tenant, cfg)
    return {"ok": True}


@router.post("/client/{tenant}/settings/json")
async def save_json(tenant: int, request: Request):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    try:
        raw = await request.body()
        cfg = json.loads(raw.decode("utf-8"))
        C.write_tenant_config(tenant, cfg)
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/client/{tenant}/persona")
async def save_persona(tenant: int, request: Request):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    payload = await request.json()
    C.write_persona(tenant, payload.get("text") or "")
    return {"ok": True}


@router.post("/client/{tenant}/catalog/upload")
async def catalog_upload(tenant: int, request: Request, file: UploadFile = File(...)):
    """Upload CSV/Excel/PDF and persist a CSV for editing in the UI.

    This mirrors the robust public upload flow to ensure PDF → CSV generation
    and consistent metadata, avoiding divergence between routes.
    """
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    filename = (file.filename or "").strip()
    if not filename:
        return {"ok": False, "error": "empty_file"}

    allowed = {".csv", ".xlsx", ".xls", ".pdf"}
    ext = pathlib.Path(filename).suffix.lower()
    if ext not in allowed:
        return {"ok": False, "error": "unsupported_type"}

    raw = await file.read()
    if not raw:
        return {"ok": False, "error": "empty_file"}
    if len(raw) > MAX_UPLOAD_SIZE_BYTES:
        return {
            "ok": False,
            "error": "file_too_large",
            "max_size_bytes": MAX_UPLOAD_SIZE_BYTES,
        }

    # Persist original upload under tenant/uploads
    C.ensure_tenant_files(tenant)
    tenant_root = pathlib.Path(C.tenant_dir(tenant))
    uploads_dir = tenant_root / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"catalog_{uuid.uuid4().hex}{ext}"
    dest_path = uploads_dir / safe_name
    dest_path.write_bytes(raw)
    relative_path = str(pathlib.Path("uploads") / safe_name)

    # Parse/normalize rows using the same helpers as the public route
    try:
        from . import public as _pub
        if ext == ".csv":
            normalized_rows, meta = _pub._read_csv_bytes(raw)
        elif ext in {".xlsx", ".xls"}:
            normalized_rows, meta = _pub._read_excel_bytes(raw)
        else:
            saved_rel = dest_path.relative_to(tenant_root)
            normalized_rows, meta, manifest_rel = _pub._process_pdf(
                tenant=int(tenant),
                saved_path=dest_path,
                tenant_root=tenant_root,
                saved_rel_path=saved_rel,
                original_name=filename,
            )
    except CatalogIndexError as exc:
        return {"ok": False, "error": "catalog_index_failed", "detail": str(exc)}
    except Exception as exc:
        return {"ok": False, "error": "processing_failed", "detail": str(exc)}

    # Write canonical CSV under tenant/catalogs for consistent discovery
    from app.catalog.io import write_catalog_csv  # local import to avoid cyclical aliasing
    try:
        # Persist under a stable name to avoid UI path churn
        csv_rel_path, ordered_columns = write_catalog_csv(int(tenant), normalized_rows, "catalog", meta)
    except Exception as exc:
        return {"ok": False, "error": "csv_write_failed", "detail": str(exc)}

    # Update tenant config: newest first, preserve prior entries with different path
    cfg = C.read_tenant_config(tenant)
    if not isinstance(cfg, dict):
        cfg = {}
    catalogs = cfg.get("catalogs") if isinstance(cfg.get("catalogs"), list) else []
    catalog_type = "pdf" if ext == ".pdf" else ("excel" if ext in {".xlsx", ".xls"} else "csv")
    entry: dict[str, object] = {"name": "uploaded", "path": relative_path, "type": catalog_type, "csv_path": csv_rel_path}
    if isinstance(meta, dict):
        if meta.get("encoding"):
            entry["encoding"] = meta.get("encoding")  # type: ignore[index]
        if meta.get("delimiter") is not None:
            entry["delimiter"] = meta.get("delimiter")  # type: ignore[index]
        # For PDF persist index metadata
        for k in ("index_path", "indexed_at", "chunk_count", "sha1"):
            if meta.get(k) is not None:
                entry[k] = meta.get(k)  # type: ignore[index]
    cfg["catalogs"] = [entry] + [e for e in catalogs if isinstance(e, dict) and e.get("path") != relative_path]

    # Also surface upload metadata for UI status panel
    integrations = cfg.setdefault("integrations", {})
    uploaded_meta: dict[str, object] = {
        "path": relative_path,
        "original": filename,
        "uploaded_at": int(time.time()),
        "type": catalog_type,
        "size": len(raw),
        "mime": (mimetypes.guess_type(filename)[0] or "application/octet-stream"),
        "csv_path": csv_rel_path,
    }
    if isinstance(meta, dict):
        if meta.get("pipeline"):
            uploaded_meta["pipeline"] = meta.get("pipeline")  # type: ignore[index]
        if catalog_type == "csv" and meta.get("encoding"):
            uploaded_meta["encoding"] = meta.get("encoding")  # type: ignore[index]
        if catalog_type == "csv" and meta.get("delimiter") is not None:
            uploaded_meta["delimiter"] = meta.get("delimiter")  # type: ignore[index]
        if catalog_type == "pdf":
            # Normalize index metadata keys to a stable shape
            idx = {
                "path": meta.get("index_path"),
                "generated_at": meta.get("indexed_at"),
                "chunks": meta.get("chunk_count"),
                "pages": meta.get("page_count"),
                "sha1": meta.get("sha1"),
            }
            idx = {k: v for k, v in idx.items() if v is not None}
            if idx:
                uploaded_meta["index"] = idx
    integrations["uploaded_catalog"] = uploaded_meta
    C.write_tenant_config(tenant, cfg)

    # HTML form fallback: redirect back to settings
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
        redirect_url = request.url_for("client_settings", tenant=tenant)
        if key:
            redirect_url = f"{redirect_url}?k={quote_plus(key)}"
        return RedirectResponse(url=redirect_url, status_code=303)

    return {
        "ok": True,
        "filename": filename,
        "stored_as": safe_name,
        "csv_path": csv_rel_path,
        "path": csv_rel_path,
        "items_total": len(normalized_rows),
        "columns": ordered_columns,
    }


@router.post("/client/{tenant}/training/upload")
async def training_upload(tenant: int, request: Request, file: UploadFile = File(...)):
    """Upload dialogues (JSONL/JSON/CSV), build a per-tenant TF-IDF index, and persist manifest.

    Accepts the following formats:
    - JSONL: one JSON per line. Either {"q","a"} or {"messages":[{"role","content"}, ...]}
    - JSON: list of above objects or a single {messages:[...]}
    - CSV: columns like (q,a) or (question,answer) or (user,assistant)
    """

    started_at = time.time()
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    filename = (file.filename or "").strip()
    if not filename:
        return {"ok": False, "error": "empty_file"}

    allowed = {".jsonl", ".json", ".csv"}
    ext = pathlib.Path(filename).suffix.lower()
    if ext not in allowed:
        return {"ok": False, "error": "unsupported_type"}

    raw = await file.read()
    if not raw:
        return {"ok": False, "error": "empty_file"}
    if len(raw) > MAX_UPLOAD_SIZE_BYTES:
        return {
            "ok": False,
            "error": "file_too_large",
            "max_size_bytes": MAX_UPLOAD_SIZE_BYTES,
        }

    C.ensure_tenant_files(tenant)
    tenant_path = pathlib.Path(C.tenant_dir(tenant))
    uploads_dir = tenant_path / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"training_{uuid.uuid4().hex}{ext}"
    dest_path = uploads_dir / safe_name
    try:
        with open(dest_path, "wb") as fh:
            fh.write(raw)
    except Exception:
        _log.exception(f"{_LOG_PREFIX} failed to store upload", exc_info=True)
        return {"ok": False, "error": "store_failed"}

    # Parse examples
    try:
        _log.info(f"{_LOG_PREFIX} upload start tenant=%s filename=%s size=%s bytes", tenant, filename, len(raw))
        if ext == ".jsonl":
            examples = training_indexer.parse_jsonl(raw)
        elif ext == ".json":
            examples = training_indexer.parse_json(raw)
        else:
            examples = training_indexer.parse_csv(raw)
        _log.info(f"{_LOG_PREFIX} parsed examples tenant=%s count=%s", tenant, len(examples))
    except Exception as exc:
        _log.exception(f"{_LOG_PREFIX} parse_failed tenant=%s filename=%s", tenant, filename, exc_info=True)
        return {"ok": False, "error": "parse_failed", "detail": str(exc)}

    if not examples:
        return {"ok": False, "error": "no_examples"}

    # Build index
    index = training_indexer.build_index(examples)
    if not index:
        return {"ok": False, "error": "index_failed"}

    indexes_dir = tenant_path / "indexes"
    indexes_dir.mkdir(parents=True, exist_ok=True)
    index_path = indexes_dir / f"training_{index.sha1}.pkl"
    try:
        index.save(index_path)
    except Exception:
        _log.exception(f"{_LOG_PREFIX} index_save_failed tenant=%s path=%s", tenant, str(index_path), exc_info=True)
        return {"ok": False, "error": "index_save_failed"}

    # Save manifest and update tenant config
    relative_source = str((dest_path.relative_to(tenant_path))) if dest_path.is_relative_to(tenant_path) else str(dest_path)
    manifest = training_indexer.save_manifest(index, index_path=index_path, source_relpath=relative_source, original_name=filename)

    cfg = C.read_tenant_config(tenant)
    integrations = cfg.setdefault("integrations", {})
    integrations["uploaded_training"] = {
        "path": relative_source,
        "pairs": manifest.get("pairs"),
        "indexed_at": manifest.get("created_at"),
        "index_path": str(index_path.relative_to(tenant_path)) if index_path.is_relative_to(tenant_path) else str(index_path),
        "sha1": manifest.get("sha1"),
        "original": filename,
    }
    C.write_tenant_config(tenant, cfg)

    accept_header = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept_header)
    if wants_html:
        redirect_url = request.url_for("client_settings", tenant=tenant)
        if key:
            redirect_url = f"{redirect_url}?k={quote_plus(key)}"
        return RedirectResponse(url=redirect_url, status_code=303)

    took = time.time() - started_at
    _log.info(f"{_LOG_PREFIX} upload complete tenant=%s pairs=%s index=%s took=%.3fs", tenant, len(index.items), str(index_path), took)
    return {"ok": True, "pairs": len(index.items), "stored_as": safe_name}


def _finalize_whatsapp_export(
    stats: Dict[str, Any],
    tenant: int,
    days_back: int,
    limit_dialogs: Optional[int],
    per_limit: Optional[int],
    batch_size: int,
    started_at: float,
) -> None:
    took_ms = int((time.time() - started_at) * 1000)
    dialogs_selected = int(stats.get("dialog_count") or 0)
    messages_selected = int(stats.get("message_count") or 0)
    meta = stats.get("meta") if isinstance(stats.get("meta"), dict) else {}
    meta.update(
        {
            "dialogs_selected": dialogs_selected,
            "messages_selected": messages_selected,
            "batch_size_dialogs": batch_size,
            "duration_ms": took_ms,
        }
    )
    stats["meta"] = meta
    _wa_log.info(
        "[wa_export] stream_complete tenant=%s days_back=%s limit=%s per=%s dialogs=%s messages=%s batch_size=%s duration_ms=%s",
        tenant,
        days_back,
        limit_dialogs if limit_dialogs is not None else "none",
        per_limit if per_limit is not None else "none",
        dialogs_selected,
        messages_selected,
        batch_size,
        took_ms,
    )


def _cleanup_export_file(path: str | os.PathLike[str] | pathlib.Path) -> None:
    target = pathlib.Path(path)
    try:
        target.unlink(missing_ok=True)
    except FileNotFoundError:
        return
    except Exception as exc:
        try:
            _wa_log.warning("[wa_export] cleanup_failed path=%s error=%s", target, exc)
        except Exception:
            pass


async def _prepare_whatsapp_export_response(
    tenant_raw: int,
    key: str,
    days_back_raw: int | None,
    limit_dialogs_raw: int | None,
    per_limit_raw: int | None,
    batch_size_raw: int | None,
    background: BackgroundTasks,
    started_at: float | None = None,
):
    started = started_at if started_at is not None else time.time()

    try:
        tenant = int(tenant_raw)
    except (TypeError, ValueError):
        _wa_log.warning("[wa_export] invalid_tenant tenant=%s", tenant_raw)
        return JSONResponse({"detail": "invalid_tenant"}, status_code=422)

    if tenant <= 0:
        _wa_log.warning("[wa_export] invalid_tenant tenant=%s", tenant)
        return JSONResponse({"detail": "invalid_tenant"}, status_code=422)

    cleaned_key = (key or "").strip()
    if not _auth(tenant, cleaned_key):
        _wa_log.warning("[wa_export] unauthorized tenant=%s", tenant)
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    try:
        days_back = int(days_back_raw or 0)
    except (TypeError, ValueError):
        days_back = 0
    if days_back < 0:
        days_back = 0
    if days_back > EXPORT_MAX_DAYS:
        detail = f"days_back_too_large:max={EXPORT_MAX_DAYS};reduce_window"
        _wa_log.warning("[wa_export] days_exceeded tenant=%s days_back=%s", tenant, days_back)
        return JSONResponse({"detail": detail}, status_code=422)

    limit_dialogs: Optional[int] = None
    try:
        limit_candidate = int(limit_dialogs_raw) if limit_dialogs_raw is not None else None
    except (TypeError, ValueError):
        limit_candidate = None
    if limit_candidate is not None and limit_candidate > 0:
        if limit_candidate > WHATSAPP_LIMIT_DIALOGS_MAX:
            detail = f"limit_dialogs_too_large:max={WHATSAPP_LIMIT_DIALOGS_MAX};reduce_window_or_limit"
            _wa_log.warning(
                "[wa_export] limit_exceeded tenant=%s limit=%s", tenant, limit_candidate
            )
            return JSONResponse({"detail": detail}, status_code=422)
        limit_dialogs = limit_candidate

    per_limit: Optional[int] = None
    try:
        per_candidate = int(per_limit_raw) if per_limit_raw is not None else None
    except (TypeError, ValueError):
        per_candidate = None
    if per_candidate is not None and per_candidate > 0:
        if per_candidate > WHATSAPP_PER_LIMIT_MAX:
            detail = f"per_limit_too_large:max={WHATSAPP_PER_LIMIT_MAX};reduce_per_limit"
            _wa_log.warning(
                "[wa_export] per_exceeded tenant=%s per=%s", tenant, per_candidate
            )
            return JSONResponse({"detail": detail}, status_code=422)
        per_limit = per_candidate

    try:
        batch_size_candidate = (
            int(batch_size_raw) if batch_size_raw is not None else DEFAULT_WHATSAPP_BATCH_SIZE
        )
    except (TypeError, ValueError):
        batch_size_candidate = DEFAULT_WHATSAPP_BATCH_SIZE
    if batch_size_candidate <= 0:
        batch_size_candidate = DEFAULT_WHATSAPP_BATCH_SIZE
    batch_size_dialogs = batch_size_candidate

    now_utc = datetime.now(timezone.utc)
    since = now_utc - timedelta(days=days_back)

    cfg = C.read_tenant_config(tenant)
    passport = cfg.get("passport", {}) if isinstance(cfg, dict) else {}
    default_agent = getattr(C.settings, "AGENT_NAME", "Менеджер")
    agent_name = (passport.get("agent_name") or default_agent).strip() or default_agent

    tenant_tz: ZoneInfo | None = None
    if isinstance(cfg, dict):
        settings_raw = cfg.get("settings")
        settings_cfg = settings_raw if isinstance(settings_raw, dict) else {}
        tz_candidates = [
            cfg.get("timezone"),
            cfg.get("tz"),
            settings_cfg.get("timezone"),
            passport.get("timezone"),
        ]
        for tz_candidate in tz_candidates:
            if not tz_candidate:
                continue
            name = str(tz_candidate).strip()
            if not name:
                continue
            try:
                tenant_tz = ZoneInfo(name)
                break
            except Exception:
                _wa_log.warning("[wa_export] invalid_timezone tenant=%s tz=%s", tenant, name)
                continue

    try:
        zip_path, stats = await whatsapp_exporter.build_whatsapp_zip(
            tenant=tenant,
            since=since,
            until=now_utc,
            limit_dialogs=limit_dialogs,
            per_message_limit=per_limit,
            agent_name=agent_name,
            tz=tenant_tz,
            batch_size_dialogs=batch_size_dialogs,
        )
    except getattr(db, "DatabaseUnavailableError", RuntimeError) as exc:
        _wa_log.error("[wa_export] db_unavailable tenant=%s error=%s", tenant, exc)
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    except getattr(whatsapp_exporter, "ExportSafetyError", RuntimeError) as exc:
        _wa_log.error("[wa_export] safety_abort tenant=%s reason=%s", tenant, exc)
        return JSONResponse({"detail": "export_blocked", "reason": str(exc)}, status_code=409)
    except Exception:  # pragma: no cover - unexpected errors are surfaced
        _wa_log.exception("[wa_export] export_failed tenant=%s", tenant)
        return JSONResponse({"detail": "export_failed"}, status_code=500)

    stats = stats if isinstance(stats, dict) else {}
    meta = stats.get("meta") if isinstance(stats.get("meta"), dict) else {}

    if zip_path is None:
        _wa_log.info(
            "[wa_export] empty tenant=%s days_back=%s limit=%s dialogs=%s messages=%s",
            tenant,
            days_back,
            limit_dialogs if limit_dialogs is not None else "none",
            meta.get("dialog_count", 0),
            meta.get("messages_in_range", 0),
        )
        _wa_log.info(
            "[wa_export] empty tenant=%s days_back=%s limit=%s",
            tenant,
            days_back,
            limit_dialogs if limit_dialogs is not None else "none",
        )
        return Response(status_code=204, headers={"Cache-Control": "no-store"})

    now_local = now_utc.astimezone(tenant_tz or whatsapp_exporter.EXPORT_TZ)
    filename = f"whatsapp_export_{now_local.strftime('%Y-%m-%d')}.zip"
    response = FileResponse(path=zip_path, media_type="application/zip", filename=filename)

    safe_filename = filename.replace("\"", r"\"")
    response.headers["Content-Disposition"] = (
        f"attachment; filename=\"{safe_filename}\"; filename*=UTF-8''{quote(filename)}"
    )
    response.headers["X-Dialog-Count"] = str(stats.get("dialog_count", 0))
    response.headers["X-Message-Count"] = str(stats.get("message_count", 0))
    response.headers["Cache-Control"] = "no-store"
    if "content-encoding" in response.headers:
        del response.headers["content-encoding"]

    took = time.time() - started
    _wa_log.info(
        "[wa_export] complete tenant=%s days_back=%s limit=%s per=%s dialogs=%s messages=%s took=%.3fs batch=%s top5=%s filtered_groups=%s",
        tenant,
        days_back,
        limit_dialogs if limit_dialogs is not None else "none",
        per_limit if per_limit is not None else "none",
        stats.get("dialog_count", 0),
        stats.get("message_count", 0),
        took,
        batch_size_dialogs,
        stats.get("top_five"),
        meta.get("filtered_groups") if meta else None,
    )

    background.add_task(
        _finalize_whatsapp_export,
        stats,
        tenant,
        days_back,
        limit_dialogs,
        per_limit,
        batch_size_dialogs,
        started,
    )

    background.add_task(_cleanup_export_file, pathlib.Path(zip_path))

    return response


@router.post("/export/whatsapp", name="whatsapp_export")
async def whatsapp_export(request: Request, background: BackgroundTasks):
    started_at = time.time()

    try:
        raw_body = await request.json()
    except Exception:
        raw_body = {}

    if not isinstance(raw_body, dict):
        raw_body = {}

    try:
        payload = WhatsAppExportPayload(**raw_body)
    except ValidationError as exc:
        _wa_log.warning("[wa_export] invalid_payload errors=%s", exc.errors())
        return JSONResponse({"detail": "invalid_payload", "errors": exc.errors()}, status_code=422)

    return await _prepare_whatsapp_export_response(
        tenant_raw=payload.tenant,
        key=payload.key or "",
        days_back_raw=payload.days if payload.days is not None else payload.days_back,
        limit_dialogs_raw=payload.limit if payload.limit is not None else payload.limit_dialogs,
        per_limit_raw=payload.per if payload.per is not None else payload.per_conversation_limit,
        batch_size_raw=payload.batch_size_dialogs,
        started_at=started_at,
        background=background,
    )


@router.get("/client/{tenant}/export/whatsapp")
async def client_whatsapp_export(tenant: int, request: Request, background: BackgroundTasks):
    started_at = time.time()
    qp = request.query_params
    key = _resolve_key(request, qp.get("k"))

    return await _prepare_whatsapp_export_response(
        tenant_raw=tenant,
        key=key,
        days_back_raw=qp.get("days") or qp.get("days_back"),
        limit_dialogs_raw=qp.get("limit") or qp.get("limit_dialogs"),
        per_limit_raw=qp.get("per") or qp.get("per_conversation_limit"),
        batch_size_raw=qp.get("batch_size_dialogs"),
        started_at=started_at,
        background=background,
    )


@router.get("/client/{tenant}/training/export")
async def training_export(
    tenant: int,
    request: Request,
    days: int = 30,
    limit: int = 10000,
    per: int = 0,
):
    """Export WhatsApp dialogs into a ZIP containing one text file per chat."""

    started_at = time.time()
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        _log.warning(f"{_LOG_PREFIX} unauthorized export tenant=%s", tenant)
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    qp = request.query_params

    def _parse_int(name: str, default: int, minimum: int) -> int:
        raw = qp.get(name)
        if raw is None:
            raw = default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        return value if value >= minimum else minimum

    lookback_days = _parse_int("days", days, 0)
    limit_val = _parse_int("limit", limit, 0)
    per_val = _parse_int("per", per, 0)

    now_utc = datetime.now(timezone.utc)
    since_ts = now_utc.timestamp() - (lookback_days * 86400) if lookback_days > 0 else None

    _log.info(
        "%s export start tenant=%s days=%s limit=%s per=%s",
        _LOG_PREFIX,
        tenant,
        lookback_days,
        limit_val,
        per_val,
    )

    try:
        dialogs = await db.export_dialogs(
            tenant_id=tenant,
            channel="whatsapp",
            exclude_groups=True,
            since_ts=since_ts,
            max_conversations=limit_val,
            per_conversation_limit=per_val,
        )
    except getattr(db, "DatabaseUnavailableError", RuntimeError) as exc:
        _log.error("%s export db_unavailable tenant=%s", _LOG_PREFIX, tenant, exc_info=True)
        return JSONResponse(
            {"detail": "db_unavailable", "error": str(exc)},
            status_code=503,
        )
    except Exception as exc:  # pragma: no cover
        _log.exception("%s export unexpected_error tenant=%s", _LOG_PREFIX, tenant)
        return JSONResponse({"detail": "export_failed", "error": str(exc)}, status_code=500)

    dialog_count = len(dialogs)
    message_count = sum(len(d.get("messages") or []) for d in dialogs)
    _log.info(
        "%s export fetched tenant=%s dialogs=%s messages=%s since_ts=%s",
        _LOG_PREFIX,
        tenant,
        dialog_count,
        message_count,
        since_ts if since_ts is not None else "none",
    )

    if dialog_count == 0:
        _log.info(
            "%s export empty tenant=%s days=%s reason=no_private_dialogs",
            _LOG_PREFIX,
            tenant,
            lookback_days,
        )
        return Response(status_code=204, headers={"Cache-Control": "no-store"})

    archive_buffer, filenames = training_exporter.build_text_archive(dialogs)
    file_count = len(filenames)
    archive_bytes = archive_buffer.getvalue()

    _log.info(
        "%s export archive_ready tenant=%s files=%s dialogs=%s",
        _LOG_PREFIX,
        tenant,
        file_count,
        dialog_count,
    )

    ts_tag = now_utc.strftime("%Y%m%d_%H%M%S")
    headers = {
        "Content-Disposition": f"attachment; filename=\"training_export_{ts_tag}.zip\"",
        "X-Dialog-Count": str(dialog_count),
        "X-File-Count": str(file_count),
        "X-Message-Count": str(message_count),
        "X-Since-Ts": str(int(since_ts) if since_ts is not None else 0),
        "Cache-Control": "no-store",
    }

    took = time.time() - started_at
    _log.info(
        "%s export complete tenant=%s files=%s size_bytes=%s took=%.3fs",
        _LOG_PREFIX,
        tenant,
        file_count,
        len(archive_bytes),
        took,
    )

    return Response(content=archive_bytes, media_type="application/zip", headers=headers)


@router.get("/client/{tenant}/training/status")
async def training_status(tenant: int, request: Request):
    started_at = time.time()
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    _log.info("status tenant=%s", tenant)
    cfg = C.read_tenant_config(tenant)
    info = (cfg.get("integrations", {}) or {}).get("uploaded_training") if isinstance(cfg, dict) else None
    info = info or {}
    # Read manifest if present for richer data
    rel = (info.get("index_path") or "").strip()
    manifest = {}
    pairs = None
    sha1 = None
    size_bytes = None
    index_path_str = rel
    if rel:
        idx_path = pathlib.Path(C.tenant_dir(tenant)) / rel
        man_path = idx_path.with_suffix(".manifest.json")
        try:
            manifest = json.loads(man_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}
        try:
            if idx_path.exists():
                size_bytes = idx_path.stat().st_size
        except Exception:
            size_bytes = None
        pairs = manifest.get("pairs") if isinstance(manifest, dict) else None
        sha1 = manifest.get("sha1") if isinstance(manifest, dict) else None
        index_path_str = str(idx_path.relative_to(pathlib.Path(C.tenant_dir(tenant)))) if idx_path.exists() else rel
    # Optional export stats (dry computation) for UI
    qp = request.query_params
    try:
        lookback_days = max(0, int(qp.get("days") or 30))
    except Exception:
        lookback_days = 30
    try:
        limit_val = max(1, int(qp.get("limit") or 200))
    except Exception:
        limit_val = 200
    try:
        per_val = max(0, int(qp.get("per") or 0))
    except Exception:
        per_val = 0
    try:
        min_pairs = max(0, int(qp.get("min_turns") or 0))
    except Exception:
        min_pairs = 0
    strict_val = 1 if str(qp.get("strict") or 0).strip().lower() in {"1","true","yes","on"} else 0
    anon_flag = str(qp.get("anonymize") or 0).strip().lower() in {"1","true","yes","on"}

    since_ts = int(time.time()) - (lookback_days * 86400) if lookback_days > 0 else None
    per_limit = per_val if per_val > 0 else 0
    try:
        dialogs = await db.export_dialogs(
            tenant_id=tenant,
            channel="whatsapp",
            exclude_groups=True,
            since_ts=since_ts,
            max_conversations=limit_val,
            per_conversation_limit=per_limit,
        )
    except Exception:
        dialogs = []
    found_before = len(dialogs)
    norm_dialogs: list[dict] = []
    for d in dialogs[:limit_val]:
        msgs = []
        for m in list(d.get("messages", []) or []):
            role = (m.get("role") or "").strip()
            if not role:
                direction = m.get("direction")
                is_assistant = isinstance(direction, int) and direction == 1
                role = "assistant" if is_assistant else "user"
            text = m.get("content") or m.get("text") or ""
            if anon_flag:
                text = training_exporter.scrub(text) or "[REDACTED]"
            msgs.append({"role": role, "content": text, "ts": m.get("ts")})
        norm_dialogs.append({
            "lead_id": d.get("lead_id"),
            "contact_id": d.get("contact_id"),
            "messages": msgs,
        })
    after_anon = len(norm_dialogs)
    kept = 0
    dropped = {"only_assistant": 0, "min_turns": 0, "anonymized_empty": 0}
    for d in norm_dialogs:
        msgs = d.get("messages", []) or []
        last_user = False
        pairs = 0
        any_nonempty = any((m.get("content") or "").strip() for m in msgs)
        any_assistant = any((m.get("role") == "assistant") for m in msgs)
        for m in msgs:
            role = (m.get("role") or "").strip()
            if role == "user":
                if not last_user:
                    last_user = True
            elif role == "assistant" and last_user:
                pairs += 1
                last_user = False
        if strict_val == 1:
            if not any_assistant:
                dropped["only_assistant"] += 1
                continue
            if pairs < min_pairs:
                dropped["min_turns"] += 1
                continue
            if not any_nonempty:
                dropped["anonymized_empty"] += 1
                continue
        kept += 1

    # Read last export stats
    last_export = {}
    try:
        exp_path = pathlib.Path(C.tenant_dir(tenant)) / "exports" / "last_export.json"
        if exp_path.exists():
            last_export = json.loads(exp_path.read_text(encoding="utf-8"))
    except Exception:
        last_export = {}

    out = {
        "ok": True,
        "info": info,
        "manifest": manifest,
        "pairs": pairs,
        "sha1": sha1,
        "index_path": index_path_str,
        "size": size_bytes,
        "last_export": last_export,
        "export_stats": {
            "total_found": found_before,
            "after_anonymize": after_anon,
            "after_filters": kept,
            "dropped": dropped,
        },
    }
    _log.info(f"{_LOG_PREFIX} status done tenant=%s pairs=%s size=%sB took=%.3fs", tenant, pairs or 0, size_bytes or 0, time.time() - started_at)
    return out


@router.get("/client/{tenant}/training/dry-run")
async def training_dry_run(
    tenant: int,
    request: Request,
    days: int = 30,
    limit: int = 1000,
    per: int = 0,
    anonymize: int = 1,
    min_turns: int = 0,
    strict: int = 0,
    provider: str | None = None,
):
    started_at = time.time()
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        _log.warning(f"{_LOG_PREFIX} unauthorized dry_run tenant=%s", tenant)
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    qp = request.query_params
    lookback_days = max(0, int(qp.get("days") or days or 30))
    limit_val = max(1, int(qp.get("limit") or limit or 1000))
    per_val = max(0, int(qp.get("per") or per or 0))
    min_pairs = max(0, int(qp.get("min_turns") or min_turns or 0))
    strict_val = 1 if str(qp.get("strict") or strict or 0).strip().lower() in {"1","true","yes","on"} else 0
    anon_flag = str(qp.get("anonymize") or anonymize or 1).strip().lower() in {"1","true","yes","on"}
    provider_val = (qp.get("provider") or provider or "").strip() or None

    since_ts = int(time.time()) - (lookback_days * 86400) if lookback_days > 0 else None
    per_limit = per_val if per_val > 0 else 0
    t0 = time.time()
    dialogs = await db.export_dialogs(
        tenant_id=tenant,
        channel="whatsapp",
        exclude_groups=True,
        since_ts=since_ts,
        max_conversations=limit_val,
        per_conversation_limit=per_limit,
    )
    db_time = time.time() - t0
    found_before = len(dialogs)

    norm_dialogs: list[dict] = []
    for d in dialogs[:limit_val]:
        msgs = []
        for m in list(d.get("messages", []) or []):
            role = (m.get("role") or "").strip()
            if not role:
                direction = m.get("direction")
                role = "assistant" if (isinstance(direction, int) and direction == 1) else "user"
            text = m.get("content") or m.get("text") or ""
            if anon_flag:
                text = training_exporter.scrub(text) or "[REDACTED]"
            msgs.append({"role": role, "content": text, "ts": m.get("ts")})
        norm_dialogs.append({"lead_id": d.get("lead_id"), "messages": msgs})

    dropped = {"short_dialog": 0, "only_assistant": 0, "anonymized_empty": 0, "min_turns": 0}
    items = []
    for d in norm_dialogs:
        msgs = d.get("messages", []) or []
        last_user = False
        pairs = 0
        any_nonempty = any((m.get("content") or "").strip() for m in msgs)
        any_assistant = any((m.get("role") == "assistant") for m in msgs)
        for m in msgs:
            role = (m.get("role") or "").strip()
            if role == "user":
                if not last_user:
                    last_user = True
            elif role == "assistant" and last_user:
                pairs += 1
                last_user = False
        if strict_val == 1:
            if not any_assistant:
                dropped["only_assistant"] += 1
                continue
            if pairs < min_pairs:
                dropped["min_turns"] += 1
                continue
            if not any_nonempty:
                dropped["anonymized_empty"] += 1
                continue
        items.append(d)

    sample = items[:3]
    result = {
        "ok": True,
        "db_time": round(db_time, 3),
        "total_found": found_before,
        "after_anonymize": len(norm_dialogs),
        "after_filters": len(items),
        "dropped": dropped,
        "examples": sample,
    }
    _log.info(f"{_LOG_PREFIX} dry_run tenant=%s found=%s after=%s dropped=%s took=%.3fs", tenant, found_before, len(items), dropped, time.time() - started_at)
    return JSONResponse(result, status_code=200)


@router.get("/client/{tenant}/training/logs/tail")
async def training_logs_tail(tenant: int, request: Request, lines: int = 200):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        _log.warning(f"{_LOG_PREFIX} unauthorized logs_tail tenant=%s", tenant)
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    try:
        root = pathlib.Path(__file__).resolve().parents[2]
        log_path = root / "logs" / "training.log"
        if not log_path.exists():
            return JSONResponse({"ok": True, "log": ""}, status_code=200)
        n = max(1, min(int(lines), 2000))
        data = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        tail = "\n".join(data[-n:])
        return Response(content=tail, media_type="text/plain; charset=utf-8", headers={"Cache-Control": "no-store"})
    except Exception as exc:
        _log.exception(f"{_LOG_PREFIX} logs_tail_failed tenant=%s", tenant, exc_info=True)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@router.get("/client/{tenant}/catalog/csv")
def catalog_csv_get(tenant: int, request: Request):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    cfg = C.read_tenant_config(tenant)
    csv_path, encoding_hint, relative = _catalog_csv_path(tenant, cfg)
    if not csv_path or not csv_path.exists():
        return JSONResponse({"detail": "csv_not_ready"}, status_code=404)

    # Read and parse CSV robustly: detect encoding and delimiter, normalize header,
    # and ensure each row matches the header length (pad/trim accordingly).
    raw = csv_path.read_bytes()
    encoding = encoding_hint or _detect_encoding(raw)
    text = raw.decode(encoding or "utf-8", errors="ignore")

    # Detect delimiter using a small sample to support "," ";" "\t" and "|"
    sample = text[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ","

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)

    # Find the first non-empty row as header
    header_raw: list[str] | None = None
    for raw_header in reader:
        if not raw_header or not any((cell or "").strip() for cell in raw_header):
            continue
        header_raw = raw_header
        break

    if not header_raw:
        return {
            "ok": True,
            "columns": [],
            "rows": [],
            "encoding": encoding or "utf-8",
            "path": relative or "",
        }

    # Normalize header: trim, drop BOM, fill empties and de-duplicate with suffixes
    normalized: list[str] = []
    seen: dict[str, int] = {}
    for idx, cell in enumerate(header_raw):
        name = (cell or "").strip().lstrip("\ufeff")
        if not name:
            name = f"column_{idx + 1}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        normalized.append(name)

    columns = normalized[:]
    data_rows: list[list[str]] = []
    for row in reader:
        # Skip completely empty rows
        if not row or not any(((v.strip() if isinstance(v, str) else str(v or "").strip())) for v in row):
            continue
        # Grow columns if row has more cells than header (rare but possible)
        while len(columns) < len(row):
            columns.append(f"column_{len(columns) + 1}")
        # Build a trimmed row matching the number of columns
        ordered: list[str] = []
        for idx_col in range(len(columns)):
            if idx_col < len(row):
                val = row[idx_col]
                ordered.append((val.strip() if isinstance(val, str) else str(val or "").strip()))
            else:
                ordered.append("")
        data_rows.append(ordered)

    # Merge duplicate columns that share the same base name (e.g., "name", "name_1").
    # Preserve the first column order and join non-empty distinct values with a space.
    # This reduces visual duplication and aligns with how we want characteristics grouped.
    import re as _re
    base_to_indices: dict[str, list[int]] = {}
    for idx, col in enumerate(columns):
        base = _re.sub(r"_(\d+)$", "", col)
        base_to_indices.setdefault(base, []).append(idx)

    # Build merged columns list preserving the first occurrence order
    merged_columns: list[str] = []
    seen_bases: set[str] = set()
    for col in columns:
        base = _re.sub(r"_(\d+)$", "", col)
        if base in seen_bases:
            continue
        seen_bases.add(base)
        merged_columns.append(base)

    if any(len(idxs) > 1 for idxs in base_to_indices.values()):
        merged_rows: list[list[str]] = []
        for row in data_rows:
            merged_row: list[str] = []
            for base in merged_columns:
                indices = base_to_indices.get(base, [])
                if not indices:
                    merged_row.append("")
                    continue
                values: list[str] = []
                for i in indices:
                    if i < len(row):
                        val = row[i].strip() if isinstance(row[i], str) else str(row[i] or "").strip()
                        if val and val not in values:
                            values.append(val)
                merged_row.append(" ".join(values))
            merged_rows.append(merged_row)
        columns = merged_columns
        data_rows = merged_rows

    # Drop columns that are completely empty across all rows (common with trailing delimiters)
    if data_rows:
        keep_idx: list[int] = []
        for idx in range(len(columns)):
            any_non_empty = any(
                (row[idx].strip() if isinstance(row[idx], str) else str(row[idx] or "").strip())
                for row in data_rows
                if idx < len(row)
            )
            if any_non_empty:
                keep_idx.append(idx)
        if keep_idx and len(keep_idx) < len(columns):
            columns = [columns[i] for i in keep_idx]
            data_rows = [[(row[i] if i < len(row) else "") for i in keep_idx] for row in data_rows]

    return {
        "ok": True,
        "columns": columns,
        "rows": data_rows,
        "encoding": encoding or "utf-8",
        "path": relative or "",
    }


@router.post("/client/{tenant}/catalog/csv")
async def catalog_csv_save(tenant: int, request: Request):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    payload = await request.json()
    columns = payload.get("columns")
    rows = payload.get("rows")
    if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        return JSONResponse({"detail": "invalid_columns"}, status_code=400)
    if not isinstance(rows, list):
        return JSONResponse({"detail": "invalid_rows"}, status_code=400)

    csv_path, encoding_hint, _ = _catalog_csv_path(tenant)
    if not csv_path:
        return JSONResponse({"detail": "csv_not_ready"}, status_code=404)

    serializable_rows: list[list[str]] = []
    for row in rows:
        if isinstance(row, dict):
            ordered = [str(row.get(col, "") or "") for col in columns]
            serializable_rows.append(ordered)
        elif isinstance(row, list):
            ordered = [str(row[idx]) if idx < len(row) else "" for idx in range(len(columns))]
            serializable_rows.append(ordered)
        else:
            return JSONResponse({"detail": "invalid_row"}, status_code=400)

    # Write CSV in Excel-friendly format: UTF-8 with BOM, semicolon delimiter
    encoding = "utf-8-sig"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding=encoding, newline="") as handle:
        writer = csv.writer(handle, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        # Sanitize header
        clean_columns = []
        for col in columns:
            name = (col or "").strip().lstrip("\ufeff")
            clean_columns.append(name)
        writer.writerow(clean_columns)
        # Sanitize cells: flatten newlines/tabs and collapse spaces
        for row in serializable_rows:
            out_row: list[str] = []
            for cell in row:
                text = str(cell or "")
                if text:
                    text = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
                    text = re.sub(r"\s+", " ", text).strip()
                out_row.append(text)
            writer.writerow(out_row)

    return {"ok": True, "rows": len(serializable_rows)}


def _onboarding_error(reason: str, status_code: int = 400):
    return JSONResponse({"ok": False, "error": reason}, status_code=status_code)


async def _ensure_onboarding_started(tenant: int, convo, cfg, persona):
    if convo.get("messages"):
        return convo, None
    ask, delta, complete = await initial_assistant_turn(tenant, convo, cfg, persona)
    if ask:
        add_assistant_message(convo, ask, insights=delta or None, complete=complete)
    save_conversation(tenant, convo)
    update_tenant_insights(tenant, convo.get("status", "in_progress"), delta)
    return convo, ask


async def _collect_onboarding_context(tenant: int):
    checks, payload = evaluate_preconditions(tenant)
    cfg = payload.get("cfg") or {}
    persona = payload.get("persona") or ""
    convo = load_conversation(tenant)
    return checks, cfg, persona, convo


@router.get("/client/{tenant}/onboarding/state")
async def onboarding_state(tenant: int, request: Request):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    checks, cfg, persona, convo = await _collect_onboarding_context(tenant)
    ready = preconditions_met(checks)

    if ready:
        convo, _ = await _ensure_onboarding_started(tenant, convo, cfg, persona)

    save_conversation(tenant, convo)
    update_tenant_insights(tenant, convo.get("status", "in_progress"), None)

    return {
        "ok": True,
        "ready": ready,
        "checks": checks,
        "status": convo.get("status", "new"),
        "messages": public_messages(convo),
    }


@router.post("/client/{tenant}/onboarding/message")
async def onboarding_message(tenant: int, request: Request):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    payload = await request.json()
    user_text = (payload.get("text") or "").strip()
    if not user_text:
        return _onboarding_error("empty_message")

    checks, cfg, persona, convo = await _collect_onboarding_context(tenant)
    if not preconditions_met(checks):
        return _onboarding_error("preconditions_not_met")

    if convo.get("status") == "completed":
        return _onboarding_error("onboarding_already_completed")

    add_user_message(convo, user_text)
    ask, delta, complete = await next_assistant_turn(tenant, convo, cfg, persona, user_text)
    if ask:
        add_assistant_message(convo, ask, insights=delta or None, complete=complete)
    save_conversation(tenant, convo)
    update_tenant_insights(tenant, convo.get("status", "in_progress"), delta)

    return {
        "ok": True,
        "status": convo.get("status", "in_progress"),
        "messages": public_messages(convo),
    }


@router.post("/client/{tenant}/onboarding/reset")
async def onboarding_reset(tenant: int, request: Request):
    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)

    reset_conversation(tenant)
    update_tenant_insights(tenant, "new", None)
    return {"ok": True}
