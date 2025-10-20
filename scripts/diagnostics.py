#!/usr/bin/env python3
"""Простой диагностический скрипт для Avio.

Скрипт выполняет ключевые проверки API: генерация ключей, доступность
основных страниц интерфейса, изоляцию настроек между тенантами,
обработку каталога и экспорт/импорт диалогов. Каждая проверка выводит
понятный статус с цветовой индикацией.
"""

from __future__ import annotations

import io
import json
import os
import socket
import zipfile
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "sueta")
REQUEST_TIMEOUT = float(os.getenv("DIAG_HTTP_TIMEOUT", "10"))
_VERIFY_TLS = not _env_bool("DIAG_SKIP_TLS_VERIFY")
_SESSION = requests.Session()
_SESSION.verify = _VERIFY_TLS
_SESSION.headers.update({"User-Agent": "avio-diagnostics/1.0"})

_OK = "\033[92m[OK]\033[0m"
_FAIL = "\033[91m[FAIL]\033[0m"


def _resolve_tenants_dir() -> Path:
    candidates = [
        os.getenv("TENANTS_DIR"),
        os.path.join("app", "tenants"),
        os.path.join("data", "tenants"),
        "tenants",
    ]
    for raw in candidates:
        if not raw:
            continue
        path = Path(raw)
        if path.exists():
            return path
    return Path(os.getenv("TENANTS_DIR", os.path.join("data", "tenants")))


TENANTS_DIR = _resolve_tenants_dir()


def report(result: bool, message: str) -> None:
    prefix = _OK if result else _FAIL
    print(f"{prefix} {message}")


def _select_base_url() -> str:
    fallback = "http://127.0.0.1:8000"
    raw = os.getenv("AVIO_URL", fallback).strip()
    candidates: list[str] = []
    if raw:
        candidates.append(raw)
    if raw.rstrip("/") != fallback:
        candidates.append(fallback)

    for candidate in candidates:
        base = candidate.rstrip("/")
        parsed = urlparse(base if "://" in base else f"http://{base}")
        host = parsed.hostname
        if not host:
            continue
        try:
            socket.gethostbyname(host)
        except OSError:
            continue
        health_url = f"{base}/health"
        try:
            resp = _SESSION.get(health_url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException:
            continue
        if resp.status_code < 500:
            return base
    # Last resort – keep provided value even if probe failed
    return fallback.rstrip("/")


BASE_URL = _select_base_url()


def _make_url(path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{BASE_URL}{'/' if not path.startswith('/') else ''}{path}"  # pragma: no cover - helper


def _make_pdf(content_lines: list[str]) -> bytes:
    """Собрать минимальный PDF документ с указанными строками."""

    def _escape(text: str) -> str:
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    stream_lines = ["BT", "/F1 12 Tf", "36 760 Td"]
    for idx, line in enumerate(content_lines):
        cleaned = _escape(line)
        if idx == 0:
            stream_lines.append(f"({cleaned}) Tj")
        else:
            stream_lines.append(f"T* ({cleaned}) Tj")
    stream_lines.append("ET")
    stream = "\n".join(stream_lines).encode("utf-8")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    output = bytearray()
    output.extend(b"%PDF-1.4\n")
    offsets: list[int] = []
    for idx, obj in enumerate(objects, start=1):
        offsets.append(len(output))
        output.extend(f"{idx} 0 obj\n".encode("ascii"))
        output.extend(obj)
        output.extend(b"\nendobj\n")
    xref_pos = len(output)
    output.extend(b"xref\n")
    output.extend(f"0 {len(objects) + 1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets:
        output.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    output.extend(b"trailer\n")
    trailer = f"<< /Size {len(objects) + 1} /Root 1 0 R >>\n".encode("ascii")
    output.extend(trailer)
    output.extend(b"startxref\n")
    output.extend(f"{xref_pos}\n".encode("ascii"))
    output.extend(b"%%EOF\n")
    return bytes(output)


def check_api_key_creation(tenant_id: int) -> Optional[str]:
    url = _make_url(f"/admin/key/get?tenant={tenant_id}&token={ADMIN_TOKEN}")
    try:
        resp = _SESSION.get(url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:  # pragma: no cover - network errors
        report(False, f"Tenant {tenant_id}: запрос ключа не выполнен ({exc})")
        return None

    ok_http = resp.status_code == 200
    data = resp.json() if ok_http else {}
    key = (data.get("key") or "").strip()
    if not (ok_http and key):
        report(False, f"Tenant {tenant_id}: не удалось получить ключ (HTTP {resp.status_code})")
        return None

    list_url = _make_url(f"/admin/keys/list?tenant={tenant_id}&token={ADMIN_TOKEN}")
    items: list[dict[str, Any]] = []
    list_ok = False
    try:
        list_resp = _SESSION.get(list_url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException:
        list_resp = None
    if list_resp is not None and list_resp.status_code == 200:
        try:
            list_data = list_resp.json()
        except ValueError:
            list_data = {}
        if isinstance(list_data, dict):
            raw_items = list_data.get("items")
            if isinstance(raw_items, list):
                items = raw_items
        list_ok = True
    single_key = list_ok and len(items) == 1 and (items[0].get("key") or "").strip().lower() == key.lower()
    report(single_key, f"Tenant {tenant_id}: проверка списка ключей (найдено {len(items)})")

    tenant_dir = TENANTS_DIR / str(tenant_id)
    persona_path = tenant_dir / "persona.md"
    config_path = tenant_dir / "tenant.json"
    folder_exists = tenant_dir.is_dir()
    files_exist = persona_path.is_file() and config_path.is_file()
    tenant_id_ok = False
    if files_exist:
        try:
            with config_path.open(encoding="utf-8") as handle:
                cfg = json.load(handle)
            passport = cfg.get("passport") or {}
            tenant_id_ok = int(passport.get("tenant_id", 0) or 0) == tenant_id
        except Exception:
            tenant_id_ok = False

    success = folder_exists and files_exist and tenant_id_ok
    report(
        success,
        f"Создание ключа tenant {tenant_id}: ключ {key[:8]}..., папка={folder_exists}, файлы={files_exist}"
    )
    return key if success else None


def check_ui_endpoints(tenant_id: int, key: Optional[str]) -> None:
    if not key:
        reason = f"tenant {tenant_id}: ключ не создан"
        report(False, f"Страница /connect/wa не проверена ({reason})")
        report(False, f"Кнопка 'Обновить QR' не проверена ({reason})")
        report(False, f"Страница настроек не проверена ({reason})")
        return

    connect_url = _make_url(f"/connect/wa?tenant={tenant_id}&k={key}")
    try:
        resp = _SESSION.get(connect_url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        report(False, f"/connect/wa tenant={tenant_id}: ошибка запроса ({exc})")
        return
    ok_page = resp.status_code == 200 and "id=\"qr\"" in resp.text
    report(ok_page, f"Страница /connect/wa доступна для tenant {tenant_id}")

    qr_url = _make_url(f"/pub/wa/qr.svg?tenant={tenant_id}&k={key}")
    try:
        qr1 = _SESSION.get(qr_url, timeout=REQUEST_TIMEOUT, headers={"Cache-Control": "no-store"})
    except requests.RequestException as exc:
        report(False, f"QR tenant={tenant_id}: запрос не выполнен ({exc})")
        qr1 = None
    ok_qr = False
    if qr1 is not None:
        if qr1.status_code in {200, 204}:
            if qr1.status_code == 200:
                try:
                    qr2 = _SESSION.get(qr_url, timeout=REQUEST_TIMEOUT, headers={"Cache-Control": "no-store"})
                except requests.RequestException:
                    qr2 = None
                if qr2 is not None and qr2.status_code == 200:
                    ok_qr = qr1.content != qr2.content or qr1.headers.get("Etag") != qr2.headers.get("Etag")
                else:
                    ok_qr = True
            else:
                ok_qr = True
    report(ok_qr, f"Кнопка 'Обновить QR' для tenant {tenant_id} работает")

    settings_url = _make_url(f"/client/{tenant_id}/settings?k={key}")
    try:
        settings_resp = _SESSION.get(settings_url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        report(False, f"/client/{tenant_id}/settings: ошибка запроса ({exc})")
        return
    ok_settings = settings_resp.status_code == 200 and "<form" in settings_resp.text
    report(ok_settings, f"Страница настроек tenant {tenant_id} открывается")


def check_tenant_isolation(
    tenant1: int,
    key1: Optional[str],
    tenant2: int,
    key2: Optional[str],
) -> None:
    if key1 and key2:
        url = _make_url(f"/pub/settings/get?tenant={tenant2}&k={key1}")
        try:
            resp = _SESSION.get(url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            report(False, f"Изоляция API: запрос не выполнен ({exc})")
            resp = None
        isolated = False
        if resp is not None:
            isolated = resp.status_code == 401 or "invalid_key" in resp.text.lower()
        report(isolated, f"Изоляция API: ключ tenant {tenant1} не читает данные tenant {tenant2}")
    else:
        missing = []
        if not key1:
            missing.append(str(tenant1))
        if not key2:
            missing.append(str(tenant2))
        tenants = ", ".join(missing)
        report(False, f"Изоляция API: ключ(и) для tenant {tenants} не созданы")

    cfg1 = TENANTS_DIR / str(tenant1) / "tenant.json"
    cfg2 = TENANTS_DIR / str(tenant2) / "tenant.json"
    diff = False
    if cfg1.is_file() and cfg2.is_file():
        try:
            data1 = json.loads(cfg1.read_text(encoding="utf-8"))
            data2 = json.loads(cfg2.read_text(encoding="utf-8"))
            diff = data1 != data2
        except Exception:
            diff = False
        report(diff, f"tenant.json {tenant1} и {tenant2} различаются")
    else:
        report(False, "tenant.json не найден для одного из тенантов")


def check_catalog_upload(tenant_id: int, key: Optional[str]) -> None:
    if not key:
        reason = f"tenant {tenant_id}: ключ не создан"
        report(False, f"Загрузка CSV не проверена ({reason})")
        report(False, f"Загрузка PDF не проверена ({reason})")
        return

    csv_content = "sku,name,price\n1001,Test Product,123.45\n"
    files = {"file": ("catalog.csv", csv_content.encode("utf-8"), "text/csv")}
    url = _make_url(f"/client/{tenant_id}/catalog/upload?k={key}")
    try:
        resp_csv = _SESSION.post(url, files=files, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        report(False, f"Загрузка CSV: ошибка запроса ({exc})")
        return

    csv_ok = resp_csv.status_code == 200
    data = resp_csv.json() if csv_ok else {}
    saved_csv = data.get("csv_path")
    items_total = data.get("items_total", 0)
    csv_file_ok = False
    if saved_csv:
        csv_full = TENANTS_DIR / str(tenant_id) / saved_csv
        csv_file_ok = csv_full.is_file()
    report(csv_ok and data.get("ok") and items_total >= 1 and csv_file_ok, f"Загрузка CSV: файлов={saved_csv}, записей={items_total}")

    pdf_bytes = _make_pdf(["Test catalog", "SKU 2001", "Цена 9990"])
    files_pdf = {"file": ("catalog.pdf", pdf_bytes, "application/pdf")}
    try:
        resp_pdf = _SESSION.post(url, files=files_pdf, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        report(False, f"Загрузка PDF: ошибка запроса ({exc})")
        return

    pdf_ok = resp_pdf.status_code == 200
    pdf_data = resp_pdf.json() if pdf_ok else {}
    pdf_csv = pdf_data.get("csv_path")
    pdf_saved = False
    if pdf_csv:
        pdf_full = TENANTS_DIR / str(tenant_id) / pdf_csv
        pdf_saved = pdf_full.is_file()
    report(pdf_ok and pdf_data.get("ok") and pdf_saved, f"Загрузка PDF: сгенерирован CSV {pdf_csv}")


def check_dialogs_export_import(tenant_id: int, key: Optional[str]) -> None:
    if not key:
        reason = f"tenant {tenant_id}: ключ не создан"
        report(False, f"Экспорт диалогов не проверен ({reason})")
        report(False, f"Импорт диалогов не проверен ({reason})")
        return

    export_url = _make_url(f"/client/{tenant_id}/training/export?days=30&limit=100&format=zip&k={key}")
    try:
        resp = _SESSION.get(export_url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        report(False, f"Экспорт диалогов: ошибка запроса ({exc})")
        return

    ok_export = resp.status_code == 200 and resp.headers.get("Content-Type", "").startswith("application/zip")
    dialogs_bytes = b""
    if ok_export:
        try:
            with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
                dialogs_bytes = archive.read("dialogs.jsonl") if "dialogs.jsonl" in archive.namelist() else b""
        except Exception:
            ok_export = False
    report(ok_export and dialogs_bytes, f"Экспорт диалогов: ZIP получен ({len(dialogs_bytes)} байт)")

    if not dialogs_bytes:
        return

    files = {"file": ("dialogs.jsonl", dialogs_bytes, "application/json")}
    import_url = _make_url(f"/client/{tenant_id}/training/upload?k={key}")
    try:
        resp_import = _SESSION.post(import_url, files=files, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        report(False, f"Импорт диалогов: ошибка запроса ({exc})")
        return

    ok_import = resp_import.status_code == 200
    data = resp_import.json() if ok_import else {}
    report(ok_import and data.get("ok"), f"Импорт диалогов: {data.get('pairs', 0)} записей")


def run_diagnostics() -> None:
    print("=== Запуск диагностики Avio ===")
    try:
        health = _SESSION.get(_make_url("/health"), timeout=REQUEST_TIMEOUT)
        if health.status_code != 200:
            report(False, f"Проверка /health завершилась статусом {health.status_code}")
            print("=== Диагностика прервана ===")
            return
    except requests.RequestException as exc:
        report(False, f"API недоступен по {BASE_URL} ({exc})")
        print("=== Диагностика прервана ===")
        return
    key1 = check_api_key_creation(1)
    key2 = check_api_key_creation(2)
    check_ui_endpoints(1, key1)
    check_tenant_isolation(1, key1, 2, key2)
    check_catalog_upload(1, key1)
    check_dialogs_export_import(1, key1)
    print("=== Диагностика завершена ===")


if __name__ == "__main__":
    run_diagnostics()
