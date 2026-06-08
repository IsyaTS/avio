from __future__ import annotations

from typing import Any

import pytest
from fastapi.responses import Response

from apps.api.web.services import client_contextual_cases_runtime


pytestmark = pytest.mark.unit


class FakeRequest:
    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self._payload = payload or {}

    async def json(self) -> dict[str, Any]:
        return self._payload


class FakeCommon:
    def __init__(self) -> None:
        self.cfg: dict[str, Any] = {"learning": {}}

    def read_tenant_config(self, tenant: int) -> dict[str, Any]:
        return dict(self.cfg)

    def write_tenant_config(self, tenant: int, cfg: dict[str, Any]) -> None:
        self.cfg = cfg


class FakeRepo:
    async def get_latest_active_case_set(self, tenant_id: int):
        return {
            "set_id": "set-1",
            "domain_schema": {"domain_label": "покос травы"},
            "cases_count": 10,
            "active_cases_count": 8,
            "embedding_ready_count": 3,
            "embedding_pending_count": 5,
        }


class FakeImport:
    async def import_from_export_job(self, **_kwargs):
        return type(
            "Result",
            (),
            {
                "set_id": "set-1",
                "imported_count": 8,
                "active_cases_count": 8,
                "domain_label": "покос травы",
            },
        )()


async def _auth(_request, tenant):
    return int(tenant), "key"


def _deps(common: FakeCommon | None = None) -> client_contextual_cases_runtime.ClientContextualCasesDeps:
    return client_contextual_cases_runtime.ClientContextualCasesDeps(
        authorize_client_settings_request_fn=_auth,
        common_module=common or FakeCommon(),
        export_repo_module=object(),
        contextual_repo_module=FakeRepo(),
        import_service_module=FakeImport(),
        logger=type("Logger", (), {"exception": lambda *a, **k: None})(),
    )


@pytest.mark.asyncio
async def test_status_returns_metadata_only() -> None:
    result = await client_contextual_cases_runtime.contextual_cases_status(7, FakeRequest(), deps=_deps())
    assert not isinstance(result, Response)
    status = result["status"]
    assert status["active_set_id"] == "set-1"
    assert status["domain_label"] == "покос травы"
    assert "file_path" not in status


@pytest.mark.asyncio
async def test_import_enables_shadow_mode_by_default() -> None:
    common = FakeCommon()
    result = await client_contextual_cases_runtime.import_contextual_cases(7, "job-1", FakeRequest(), deps=_deps(common))
    assert not isinstance(result, Response)
    assert result["active_cases_count"] == 8
    contextual = common.cfg["learning"]["contextual_cases"]
    assert contextual["enabled"] is True
    assert contextual["shadow_mode"] is True
    assert contextual["apply_mode"] is False


@pytest.mark.asyncio
async def test_settings_updates_only_contextual_cases_branch() -> None:
    common = FakeCommon()
    common.cfg = {"learning": {"enabled": True, "top_k": 2}}
    result = await client_contextual_cases_runtime.save_contextual_cases_settings(
        7,
        FakeRequest({"enabled": True, "shadow_mode": False, "apply_mode": True}),
        deps=_deps(common),
    )
    assert not isinstance(result, Response)
    assert common.cfg["learning"]["enabled"] is True
    assert common.cfg["learning"]["top_k"] == 2
    assert common.cfg["learning"]["contextual_cases"]["apply_mode"] is True
