from pathlib import Path


def test_no_dialog_text_fixtures_outside_tests():
    repo_root = Path(__file__).resolve().parents[2]
    forbidden = []
    for path in repo_root.rglob("dialog_*.txt"):
        if any(part == "tests" for part in path.parts):
            continue
        forbidden.append(path)

    assert not forbidden, f"Unexpected dialog_*.txt artifacts present: {forbidden}"
