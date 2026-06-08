from __future__ import annotations

import textwrap

import pytest

from scripts import monolith_guard


pytestmark = pytest.mark.unit


def test_monolith_guard_allows_short_functions(tmp_path):
    target = tmp_path / "sample.py"
    target.write_text(
        textwrap.dedent(
            """
            def ok():
                return 1
            """
        ),
        encoding="utf-8",
    )

    assert monolith_guard.scan_file(target, max_lines=3, root=tmp_path) == []


def test_monolith_guard_reports_oversized_functions(tmp_path):
    target = tmp_path / "sample.py"
    target.write_text(
        textwrap.dedent(
            """
            def too_big():
                a = 1
                b = 2
                return a + b
            """
        ),
        encoding="utf-8",
    )

    issues = monolith_guard.scan_file(target, max_lines=3, root=tmp_path)

    assert len(issues) == 1
    assert issues[0].path == "sample.py"
    assert issues[0].name == "too_big"
    assert issues[0].lines == 4


def test_monolith_guard_scans_directories(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "a.py").write_text("def ok():\n    return 1\n", encoding="utf-8")
    (package / "b.py").write_text("async def bad():\n    x = 1\n    return x\n", encoding="utf-8")

    issues = monolith_guard.scan_paths([package], max_lines=2, root=tmp_path)

    assert [(item.path, item.name, item.lines) for item in issues] == [("pkg/b.py", "bad", 3)]


def test_monolith_guard_reports_files_over_budget(tmp_path):
    target = tmp_path / "large.py"
    target.write_text("a = 1\nb = 2\nc = 3\n", encoding="utf-8")

    issues = monolith_guard.scan_file_budgets({"large.py": 2}, root=tmp_path)

    assert issues == [monolith_guard.FileIssue(path="large.py", lines=3, limit=2)]


def test_monolith_guard_allows_files_at_budget(tmp_path):
    target = tmp_path / "large.py"
    target.write_text("a = 1\nb = 2\n", encoding="utf-8")

    assert monolith_guard.scan_file_budgets({"large.py": 2}, root=tmp_path) == []
