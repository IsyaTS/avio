from __future__ import annotations

import textwrap

import pytest

from scripts import release_scope_guard


pytestmark = pytest.mark.unit


def test_read_pathspec_ignores_comments_and_blank_lines(tmp_path):
    path = tmp_path / "slice.pathspec"
    path.write_text(
        textwrap.dedent(
            """
            # comment

            README.md
            scripts/example.py
            """
        ),
        encoding="utf-8",
    )

    assert release_scope_guard.read_pathspec(path) == ["README.md", "scripts/example.py"]


def test_missing_pathspec_entries_supports_files_dirs_and_globs(tmp_path):
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "a.md").write_text("x", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "tool.py").write_text("x", encoding="utf-8")

    missing = release_scope_guard.missing_pathspec_entries(
        ["docs", "scripts/*.py", "missing/*.py", "nope.txt"],
        root=tmp_path,
    )

    assert missing == ["missing/*.py", "nope.txt"]


def test_missing_pathspec_entries_ignores_accepted_deletions(tmp_path):
    missing = release_scope_guard.missing_pathspec_entries(
        [".env.example", "pyproject.toml"],
        root=tmp_path,
        accepted_missing={".env.example"},
    )

    assert missing == ["pyproject.toml"]


def test_summarize_status_lines_counts_git_porcelain_statuses():
    summary = release_scope_guard.summarize_status_lines(
        [
            " M README.md",
            "D  old.py",
            "?? new.py",
            "R  old -> new",
        ]
    )

    assert summary.modified == 2
    assert summary.deleted == 1
    assert summary.untracked == 1
    assert summary.total == 4


def test_parse_status_lines_extracts_paths_and_rename_targets():
    parsed = release_scope_guard.parse_status_lines(
        [
            " M README.md",
            "?? scripts/new_tool.py",
            "R  old.py -> apps/new.py",
        ]
    )

    assert parsed == [
        release_scope_guard.DirtyPath(status=" M", path="README.md"),
        release_scope_guard.DirtyPath(status="??", path="scripts/new_tool.py"),
        release_scope_guard.DirtyPath(status="R ", path="apps/new.py"),
    ]


def test_match_patterns_detects_local_generated_candidates():
    matched = release_scope_guard.match_patterns(
        [
            ".codex",
            ".env_recovery/pool.clean",
            "screens/a.png",
            "src/app.py",
        ],
        release_scope_guard.LOCAL_GENERATED_PATTERNS,
    )

    assert matched == [".codex", ".env_recovery/pool.clean", "screens/a.png"]


def test_path_matches_pathspec_supports_exact_dirs_and_globs(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_a.py").write_text("x", encoding="utf-8")

    assert release_scope_guard.path_matches_pathspec("README.md", "README.md", root=tmp_path)
    assert release_scope_guard.path_matches_pathspec("tests/test_a.py", "tests/", root=tmp_path)
    assert release_scope_guard.path_matches_pathspec("tests/test_a.py", "tests/*.py", root=tmp_path)
    assert not release_scope_guard.path_matches_pathspec("apps/api/main.py", "tests/", root=tmp_path)


def test_uncovered_dirty_paths_ignores_paths_in_release_scope_and_accepted_deletions(tmp_path):
    dirty = [
        release_scope_guard.DirtyPath(status=" M", path="README.md"),
        release_scope_guard.DirtyPath(status="??", path="tests/test_a.py"),
        release_scope_guard.DirtyPath(status=" D", path=".env.example"),
        release_scope_guard.DirtyPath(status="??", path="stray.tmp"),
    ]

    uncovered = release_scope_guard.uncovered_dirty_paths(
        dirty,
        ["README.md", "tests/"],
        root=tmp_path,
        accepted_deleted={".env.example"},
    )

    assert uncovered == ["stray.tmp"]


def test_build_slice_report_uses_accepted_missing_paths(tmp_path, monkeypatch):
    (tmp_path / "docs").mkdir()
    pathspec = tmp_path / "docs" / "slice.pathspec"
    pathspec.write_text(".env.example\nREADME.md\n", encoding="utf-8")
    (tmp_path / "README.md").write_text("readme", encoding="utf-8")
    monkeypatch.setattr(release_scope_guard, "status_for_pathspec", lambda _items, root: [])

    report = release_scope_guard.build_slice_report(
        pathspec,
        root=tmp_path,
        accepted_missing={".env.example"},
    )

    assert report.missing_paths == ()
