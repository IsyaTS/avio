from apps.worker import main as worker_main


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
