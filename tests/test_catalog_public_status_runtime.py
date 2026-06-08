from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from apps.api.web.services import catalog_public_runtime


pytestmark = pytest.mark.unit


class _Common:
    def __init__(self, root):
        self.root = root

    def tenant_dir(self, tenant_id: int):
        return self.root / str(tenant_id)


def _deps(tmp_path, *, authorized: bool = True) -> catalog_public_runtime.CatalogPublicDeps:
    return catalog_public_runtime.CatalogPublicDeps(
        logger=SimpleNamespace(warning=lambda *a, **k: None),
        resolve_key_fn=lambda request, raw: raw or request.query_params.get("k") or "",
        auth_fn=lambda tenant_id, key: authorized and tenant_id == 7 and key == "pub-key",
        common_module=_Common(tmp_path),
        allowed_extensions=set(),
        max_upload_size_bytes=1,
        make_safe_filename_fn=lambda *a, **k: "",
        relative_to_fn=lambda *a, **k: "",
        read_csv_bytes_fn=lambda *a, **k: ([], {}),
        read_excel_bytes_fn=lambda *a, **k: ([], {}),
        process_pdf_fn=lambda *a, **k: ([], {}, None),
        resolve_job_metrics_fn=lambda *a, **k: {},
        catalog_index_error=Exception,
        write_catalog_csv_fn=lambda *a, **k: ("", []),
        stringify_fn=str,
        amocrm_service_module=None,
        write_tenant_config_fn=lambda *a, **k: None,
        read_tenant_config_fn=lambda *a, **k: {},
        quote_plus_fn=lambda value: value,
    )


def _request(*, key: str = "pub-key"):
    return SimpleNamespace(query_params={"k": key}, headers={})


def _write_status(tmp_path, payload: dict) -> None:
    status_dir = tmp_path / "7" / "catalog_jobs" / "job-1"
    status_dir.mkdir(parents=True)
    (status_dir / "status.json").write_text(json.dumps(payload), encoding="utf-8")


def _response_json(response) -> dict:
    return json.loads(response.body.decode("utf-8"))


def test_catalog_status_public_returns_sanitized_payload(tmp_path) -> None:
    _write_status(
        tmp_path,
        {
            "state": "done",
            "secret_path": "/opt/avio-dev/data/private",
            "source_path": "uploads/catalog.pdf",
            "log": [{"n": idx} for idx in range(60)],
        },
    )

    response = catalog_public_runtime.catalog_status_public(
        request=_request(),
        tenant="7",
        job="job-1",
        key="pub-key",
        deps=_deps(tmp_path),
    )
    body = _response_json(response)

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["state"] == "done"
    assert body["source_path"] == "uploads/catalog.pdf"
    assert "secret_path" not in body
    assert len(body["log"]) == 50
    assert body["log"][0]["n"] == 10


def test_catalog_upload_status_rejects_invalid_key(tmp_path) -> None:
    response = catalog_public_runtime.catalog_upload_status(
        tenant=7,
        job_id="job-1",
        request=_request(key=""),
        deps=_deps(tmp_path, authorized=False),
    )

    assert response.status_code == 401
    assert _response_json(response) == {"detail": "invalid_key"}


def test_load_catalog_status_payload_handles_missing_job(tmp_path) -> None:
    payload, error = catalog_public_runtime.load_catalog_status_payload(7, "missing", _deps(tmp_path))

    assert payload is None
    assert error == "not_found"
