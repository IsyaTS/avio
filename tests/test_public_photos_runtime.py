from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from apps.api.web.services import public_photos_runtime


pytestmark = pytest.mark.unit


class _Upload:
    filename = "door.jpg"
    content_type = "image/jpeg"

    def __init__(self, data: bytes):
        self.data = data

    async def read(self) -> bytes:
        return self.data


class _Request:
    def __init__(self, payload=None):
        self.payload = payload if payload is not None else {}

    async def json(self):
        return self.payload


class _Store:
    def __init__(self, root):
        self.root = root
        self.manifest: list[dict] = []

    async def authorize(self, _request, tenant, key):
        return int(tenant or 7), key or "pub-key"

    def read_manifest(self, _tenant_id: int) -> list[dict]:
        return [dict(item) for item in self.manifest]

    def write_manifest(self, _tenant_id: int, entries: list[dict]) -> None:
        self.manifest = [dict(item) for item in entries]

    def photo_url(self, _request, tenant_id: int, key: str, photo_id: str) -> str:
        return f"https://avio.test/pub/files/photos/{photo_id}?tenant={tenant_id}&k={key}"

    def photo_root(self, tenant_id: int):
        root = self.tenant_dir(tenant_id) / "uploads" / "photos"
        root.mkdir(parents=True, exist_ok=True)
        return root

    def tenant_dir(self, tenant_id: int):
        root = self.root / str(tenant_id)
        root.mkdir(parents=True, exist_ok=True)
        return root


def _deps(store: _Store) -> public_photos_runtime.PublicPhotosDeps:
    return public_photos_runtime.PublicPhotosDeps(
        authorize_fn=store.authorize,
        read_manifest_fn=store.read_manifest,
        write_manifest_fn=store.write_manifest,
        photo_url_fn=store.photo_url,
        validate_upload_fn=lambda filename, ctype: (True, ""),
        photo_root_fn=store.photo_root,
        tenant_dir_fn=store.tenant_dir,
        max_bytes=32,
        logger=SimpleNamespace(warning=lambda *a, **k: None),
    )


def _deps_with_asset_hooks(store: _Store, calls: list[tuple]) -> public_photos_runtime.PublicPhotosDeps:
    async def sync_asset(tenant_id: int, entry: dict):
        calls.append(("sync", tenant_id, entry.get("id")))
        return {"asset_id": "asset-1", "title": entry.get("title") or entry.get("original")}

    async def compile_asset(tenant_id: int, asset: dict, entry: dict):
        calls.append(("compile", tenant_id, asset.get("asset_id"), entry.get("id")))
        return {"rule_id": "rule-1"}

    deps = _deps(store)
    return public_photos_runtime.PublicPhotosDeps(
        **{**deps.__dict__, "sync_asset_fn": sync_asset, "compile_asset_fn": compile_asset}
    )


def test_photos_upload_list_update_and_delete(tmp_path) -> None:
    store = _Store(tmp_path)
    deps = _deps(store)

    upload_response = asyncio.run(
        public_photos_runtime.photos_upload(
            _Request(),
            tenant=7,
            key="pub-key",
            file=_Upload(b"jpeg-data"),
            deps=deps,
        )
    )
    photo = upload_response["photo"]
    photo_id = photo["id"]

    list_response = asyncio.run(
        public_photos_runtime.photos_list(_Request(), tenant=7, key="pub-key", deps=deps)
    )
    assert list_response["photos"][0]["url"].endswith(f"{photo_id}?tenant=7&k=pub-key")

    meta_response = asyncio.run(
        public_photos_runtime.photos_update_meta(
            photo_id,
            _Request(
                {
                    "title": "  Входная дверь ",
                    "tags": "металл, вход",
                    "channels": ["Avito", "Telegram"],
                    "auto": True,
                    "priority": "5",
                }
            ),
            tenant=7,
            key="pub-key",
            deps=deps,
        )
    )
    updated = meta_response["photo"]
    assert updated["title"] == "Входная дверь"
    assert updated["tags"] == ["металл", "вход"]
    assert updated["channels"] == ["avito", "telegram"]
    assert updated["auto"] is True
    assert updated["priority"] == 5

    delete_response = asyncio.run(
        public_photos_runtime.photos_delete(photo_id, _Request(), tenant=7, key="pub-key", deps=deps)
    )
    assert delete_response == {"ok": True}
    assert store.manifest == []


def test_photos_upload_rejects_oversized_file(tmp_path) -> None:
    response = asyncio.run(
        public_photos_runtime.photos_upload(
            _Request(),
            tenant=7,
            key="pub-key",
            file=_Upload(b"x" * 64),
            deps=_deps(_Store(tmp_path)),
        )
    )

    assert response.status_code == 400
    assert json.loads(response.body.decode("utf-8"))["error"] == "file_too_large"


def test_photos_upload_and_meta_sync_asset_hooks(tmp_path) -> None:
    store = _Store(tmp_path)
    calls: list[tuple] = []
    deps = _deps_with_asset_hooks(store, calls)

    upload_response = asyncio.run(
        public_photos_runtime.photos_upload(
            _Request(),
            tenant=7,
            key="pub-key",
            file=_Upload(b"jpeg-data"),
            deps=deps,
        )
    )
    photo_id = upload_response["photo"]["id"]
    assert calls[0][0] == "sync"

    asyncio.run(
        public_photos_runtime.photos_update_meta(
            photo_id,
            _Request({"title": "Каталог дверей с зеркалом для Казани", "auto": True}),
            tenant=7,
            key="pub-key",
            deps=deps,
        )
    )

    assert any(call[0] == "compile" for call in calls)
