import importlib
import json
import os
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
INNER = ROOT / "app"
for path in (ROOT, INNER):
    value = str(path)
    if value not in sys.path:
        sys.path.append(value)


@pytest.fixture()
def api_client(tmp_path, monkeypatch):
    tenants_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))

    # Reload modules to pick updated TENANTS_DIR
    import core
    import app.main
    import app.web.common as web_common
    import app.web.client as web_client

    importlib.reload(core)
    importlib.reload(web_common)
    importlib.reload(web_client)
    importlib.reload(app.main)

    monkeypatch.setattr(web_client.C, "valid_key", lambda tenant, key: key == "secret")

    client = TestClient(app.main.app)
    try:
        yield client
    finally:
        client.close()


def _write_pdf(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    def escape(text: str) -> str:
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    stream_parts = ["BT", "/F1 12 Tf", "36 770 Td"]
    for idx, raw_line in enumerate(lines):
        encoded = escape(raw_line)
        if idx == 0:
            stream_parts.append(f"({encoded}) Tj")
        else:
            stream_parts.append(f"T* ({encoded}) Tj")
    stream_parts.append("ET")
    content_stream = "\n".join(stream_parts).encode("utf-8")

    objects: list[bytes] = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content_stream), content_stream),
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

    path.write_bytes(output)


def _wait_for_job_status(tenant_id: int, job_id: str, *, timeout: float = 3.0):
    tenant_root = Path(os.getenv("TENANTS_DIR", ""))
    status_path = tenant_root / str(tenant_id) / "catalog_jobs" / job_id / "status.json"
    deadline = time.time() + timeout
    last_payload = None
    while time.time() < deadline:
        if status_path.exists():
            try:
                payload = json.loads(status_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                time.sleep(0.05)
                continue
            state = str(payload.get("state") or "")
            if state.lower() in {"done", "failed"}:
                return status_path, payload
            last_payload = payload
        time.sleep(0.05)
    if status_path.exists():
        if last_payload is None:
            last_payload = json.loads(status_path.read_text(encoding="utf-8"))
        return status_path, last_payload
    raise AssertionError(f"status.json not created for job {job_id}")

def test_catalog_upload_accepts_header_only(api_client):
    response = api_client.post(
        "/client/1/catalog/upload",
        files={"file": ("catalog.csv", "sku,price\n", "text/csv")},
        headers={"X-Access-Key": "secret"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert payload["filename"] == "catalog.csv"


def test_catalog_upload_prefers_query_key(api_client):
    response = api_client.post(
        "/client/1/catalog/upload?k=secret",
        files={"file": ("catalog.csv", "sku,price\n", "text/csv")},
        headers={"X-Access-Key": "wrong"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert payload["filename"] == "catalog.csv"


def test_catalog_upload_redirects_for_html_forms(api_client):
    response = api_client.post(
        "/client/1/catalog/upload?k=secret",
        files={"file": ("catalog.csv", "sku,price\n", "text/csv")},
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers.get("location", "").endswith("/client/1/settings?k=secret")


def test_persona_save_persists_to_disk(api_client):
    payload = {"text": "## Persona\n- updated"}
    response = api_client.post(
        "/client/1/persona",
        json=payload,
        headers={"X-Access-Key": "secret"},
    )
    assert response.status_code == 200, response.text
    assert response.json().get("ok") is True

    tenants_dir = Path(os.getenv("TENANTS_DIR", ""))
    assert tenants_dir.exists()
    persona_path = tenants_dir / "1" / "persona.md"
    assert persona_path.exists()
    assert persona_path.read_text(encoding="utf-8") == payload["text"]


def test_catalog_upload_detects_cp1251_encoding(api_client):
    import core

    csv_rows = ["sku;price;name", "A1;1000;Товар"]
    raw = "\n".join(csv_rows).encode("cp1251")

    response = api_client.post(
        "/client/1/catalog/upload",
        files={"file": ("catalog.csv", raw, "text/csv")},
        headers={"X-Access-Key": "secret"},
    )
    assert response.status_code == 200, response.text

    cfg = core.read_tenant_config(1)
    entry = cfg["catalogs"][0]
    assert entry["encoding"] == "cp1251"

    items = core._read_catalog(1)
    assert items
    assert items[0].get("name") == "Товар"


def test_catalog_upload_indexes_pdf(api_client, tmp_path):
    import core

    pdf_path = tmp_path / "catalog.pdf"
    _write_pdf(pdf_path, [
        "Model ALPHA-100",
        "Цвет: дуб",
        "Размер 90х200",
    ])

    with pdf_path.open("rb") as handle:
        response = api_client.post(
            "/client/1/catalog/upload",
            files={"file": ("catalog.pdf", handle.read(), "application/pdf")},
            headers={"X-Access-Key": "secret"},
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True

    cfg = core.read_tenant_config(1)
    entry = cfg["catalogs"][0]
    assert entry["type"] == "pdf"
    assert entry.get("index_path")
    assert entry.get("chunk_count")

    uploaded_meta = cfg.get("integrations", {}).get("uploaded_catalog", {})
    assert uploaded_meta.get("index", {}).get("chunks") == entry.get("chunk_count")


def test_public_catalog_upload_csv_and_pdf_with_query_tenant(api_client, tmp_path):
    csv_response = api_client.post(
        "/pub/catalog/upload?k=secret&tenant=1",
        files={"file": ("catalog.csv", "sku,price\nA1,1000\n", "text/csv")},
    )
    assert csv_response.status_code == 200, csv_response.text
    csv_payload = csv_response.json()
    assert csv_payload["ok"] is True
    assert csv_payload.get("job_id")

    _, csv_status = _wait_for_job_status(1, csv_payload["job_id"])
    assert str(csv_status.get("tenant_source")) == "query"
    assert str(csv_status.get("file_field")) == "file"
    assert str(csv_status.get("state")).lower() == "done"
    csv_rel = csv_status.get("csv_path")
    if isinstance(csv_rel, str) and csv_rel:
        tenant_root = Path(os.getenv("TENANTS_DIR", "")) / "1"
        assert (tenant_root / csv_rel).exists()

    pdf_path = tmp_path / "public-upload.pdf"
    _write_pdf(pdf_path, ["Product ZETA", "Цена 500", "Статус: в наличии"])
    with pdf_path.open("rb") as handle:
        pdf_response = api_client.post(
            "/pub/catalog/upload?k=secret&tenant=1",
            files={"catalog": ("catalog.pdf", handle.read(), "application/pdf")},
        )
    assert pdf_response.status_code == 200, pdf_response.text
    pdf_payload = pdf_response.json()
    assert pdf_payload["ok"] is True
    assert pdf_payload.get("job_id")

    _, pdf_status = _wait_for_job_status(1, pdf_payload["job_id"])
    assert str(pdf_status.get("tenant_source")) == "query"
    assert str(pdf_status.get("file_field")) == "catalog"
    assert str(pdf_status.get("state")).lower() == "done"
    pdf_rel = pdf_status.get("csv_path")
    assert isinstance(pdf_rel, str) and pdf_rel
    tenant_root = Path(os.getenv("TENANTS_DIR", "")) / "1"
    assert (tenant_root / pdf_rel).exists()


def test_public_catalog_upload_accepts_form_tenant(api_client):
    response = api_client.post(
        "/pub/catalog/upload?k=secret",
        data={"tenant": "1"},
        files={"file": ("catalog.csv", "sku,price\nB2,2200\n", "text/csv")},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    _, status = _wait_for_job_status(1, payload["job_id"])
    assert str(status.get("tenant_source")) == "form"


def test_public_catalog_upload_missing_tenant_returns_422(api_client, monkeypatch):
    import core

    setup_response = api_client.post(
        "/pub/catalog/upload?k=secret&tenant=1",
        files={"file": ("catalog.csv", "title,price\nModel ALPHA-100,1500\n", "text/csv")},
    )
    assert setup_response.status_code == 200, setup_response.text
    setup_payload = setup_response.json()
    assert setup_payload["ok"] is True
    _wait_for_job_status(1, setup_payload["job_id"])

    monkeypatch.setenv("TENANT", "")
    response = api_client.post(
        "/pub/catalog/upload?k=secret",
        files={"file": ("catalog.csv", "sku,price\n", "text/csv")},
    )
    assert response.status_code == 422, response.text
    payload = response.json()
    assert payload["ok"] is False
    assert payload["error"] == "invalid_payload"
    assert payload["reason"] == "invalid_tenant"

    items = core._read_catalog(1)
    assert items
    titles = " ".join(item.get("title", "") for item in items)
    assert "ALPHA-100" in titles or any("ALPHA-100" in (item.get("description") or "") for item in items)
