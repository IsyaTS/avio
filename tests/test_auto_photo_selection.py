from apps.worker import main as worker_main
from apps.worker.services import auto_photos


def test_select_photos_by_tags_matches_text():
    candidates = [
        {
            "id": "photo-1",
            "title": "Толстяк",
            "tags": ["толстяк", "модель"],
            "usage": "Когда просят толстяк",
            "priority": 1,
        },
        {
            "id": "photo-2",
            "title": "Другая",
            "tags": ["другая"],
            "usage": "",
            "priority": 0,
        },
    ]

    selected = worker_main._select_photos_by_tags(candidates, "нужен толстяк", "", 2)
    assert selected
    assert selected[0]["id"] == "photo-1"


def test_asset_actions_config_falls_back_to_auto_photo_enabled(tmp_path):
    deps = auto_photos.AutoPhotoDeps(
        app_base_url="https://avio.test",
        read_tenant_config_fn=lambda _tenant: {"behavior": {"auto_photo_enabled": True, "auto_photo_max": 2}},
        tenant_dir_fn=lambda _tenant: tmp_path,
        log_fn=lambda *_args, **_kwargs: None,
    )

    assert auto_photos.asset_actions_config(1, deps=deps) == (True, 2)
