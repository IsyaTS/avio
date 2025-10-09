import importlib
import os
import sys
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

    items = core._read_catalog(1)
    assert items
    titles = " ".join(item.get("title", "") for item in items)
    assert "ALPHA-100" in titles or any("ALPHA-100" in (item.get("description") or "") for item in items)
