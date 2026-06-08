from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from apps.api.web.services import catalog_public_runtime


pytestmark = pytest.mark.unit


class _Request:
    def url_for(self, name: str, **params) -> str:
        assert name == "public_catalog_file"
        return f"https://avio.test/pub/catalog/file/{params['tenant']}"


class _Core:
    def __init__(self, meta, cfg):
        self.meta = meta
        self.cfg = cfg

    def resolve_catalog_pdf_meta(self, _tenant_id: int):
        return self.meta

    def load_tenant(self, _tenant_id: int):
        return self.cfg


def _deps(core) -> catalog_public_runtime.CatalogViewDeps:
    def _render(template_name, context):
        return SimpleNamespace(template_name=template_name, context=context)

    return catalog_public_runtime.CatalogViewDeps(
        core_module=core,
        render_template_fn=_render,
        template_name="catalog.html",
        time_module=__import__("time"),
    )


def test_catalog_view_public_builds_context(tmp_path) -> None:
    pdf_path = tmp_path / "catalog.pdf"
    pdf_path.write_bytes(b"pdf")
    core = _Core(
        {
            "absolute_path": str(pdf_path),
            "filename": "catalog.pdf",
        },
        {"passport": {"brand": "Avio Doors", "agent_name": "Max", "city": "Ufa"}},
    )

    response = catalog_public_runtime.catalog_view_public(
        tenant=7,
        request=_Request(),
        deps=_deps(core),
    )
    context = response.context

    assert response.template_name == "catalog.html"
    assert context["tenant_id"] == 7
    assert context["brand"] == "Avio Doors"
    assert context["agent_name"] == "Max"
    assert context["city"] == "Ufa"
    assert context["has_catalog"] is True
    assert context["catalog_url"].startswith("https://avio.test/pub/catalog/file/7?v=")


def test_public_catalog_file_raises_404_without_meta() -> None:
    with pytest.raises(HTTPException) as exc:
        catalog_public_runtime.public_catalog_file(tenant=7, deps=_deps(_Core(None, {})))

    assert exc.value.status_code == 404
