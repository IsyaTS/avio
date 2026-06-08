from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.api.web.services import public_avatar_runtime


pytestmark = pytest.mark.unit


def _deps(*, meta=None, profile=None, http_client_cls=None):
    async def get_meta(lead_id: int):
        return meta

    async def resolve_profile(*args, **kwargs):
        return profile or {}

    return public_avatar_runtime.PublicAvatarDeps(
        get_lead_dialog_metadata_fn=get_meta,
        resolve_avito_profile_fn=resolve_profile,
        no_store_headers_fn=lambda extra=None: {"Cache-Control": "no-store"},
        http_client_cls=http_client_cls or _UnusedHttpClient,
    )


class _UnusedHttpClient:
    def __init__(self, *args, **kwargs):
        raise AssertionError("http client should not be used")


@pytest.mark.asyncio
async def test_chat_avatar_returns_404_for_missing_or_foreign_lead() -> None:
    response = await public_avatar_runtime.chat_avatar_response(
        tenant_id=7,
        lead_id=11,
        deps=_deps(meta={"tenant_id": 8}),
    )

    assert response.status_code == 404
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.asyncio
async def test_chat_avatar_builds_svg_fallback_from_display_name() -> None:
    response = await public_avatar_runtime.chat_avatar_response(
        tenant_id=7,
        lead_id=11,
        deps=_deps(meta={"tenant_id": 7, "contact": "Ivan Petrov", "channel": "telegram"}),
    )

    body = response.body.decode("utf-8")
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "image/svg+xml"
    assert ">IP<" in body


@pytest.mark.asyncio
async def test_chat_avatar_proxies_live_avito_avatar_when_available() -> None:
    class HttpClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, url: str):
            assert url == "https://cdn.example/avatar.png"
            return SimpleNamespace(
                status_code=200,
                content=b"png",
                headers={"content-type": "image/png"},
            )

    response = await public_avatar_runtime.chat_avatar_response(
        tenant_id=7,
        lead_id=11,
        deps=_deps(
            meta={
                "tenant_id": 7,
                "contact": "Avito User",
                "channel": "avito",
                "source_real_id": "100",
                "peer": "chat-1",
                "avito_user_id": "200",
            },
            profile={"avatar": "https://cdn.example/avatar.png"},
            http_client_cls=HttpClient,
        ),
    )

    assert response.status_code == 200
    assert response.body == b"png"
    assert response.headers["Content-Type"] == "image/png"
