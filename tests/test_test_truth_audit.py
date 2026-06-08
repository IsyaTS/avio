from __future__ import annotations

import textwrap

import pytest

from scripts import test_truth_audit


pytestmark = pytest.mark.unit


def test_truth_audit_flags_unmarked_critical_mocked_file(tmp_path):
    test_file = tmp_path / "test_worker_incoming.py"
    test_file.write_text(
        textwrap.dedent(
            """
            def test_one(monkeypatch):
                monkeypatch.setattr(object(), "x", lambda: None, raising=False)
                monkeypatch.setattr(object(), "y", lambda: None, raising=False)
                monkeypatch.setattr(object(), "z", lambda: None, raising=False)
                monkeypatch.setattr(object(), "a", lambda: None, raising=False)
                monkeypatch.setattr(object(), "b", lambda: None, raising=False)
            """
        ),
        encoding="utf-8",
    )

    audit = test_truth_audit.audit_file(test_file, root=tmp_path)

    assert audit.tests == 1
    assert audit.unmarked_tests == ["test_one"]
    assert audit.mock_points == 5
    assert audit.needs_truth_review is True


def test_truth_audit_accepts_marked_low_mock_file(tmp_path):
    test_file = tmp_path / "test_worker_incoming.py"
    test_file.write_text(
        textwrap.dedent(
            """
            import pytest

            @pytest.mark.integration
            def test_one():
                assert True
            """
        ),
        encoding="utf-8",
    )

    audit = test_truth_audit.audit_file(test_file, root=tmp_path)

    assert audit.tests == 1
    assert audit.marked_tests == 1
    assert audit.needs_truth_review is False


def test_truth_audit_accepts_module_level_marker(tmp_path):
    test_file = tmp_path / "test_worker_incoming.py"
    test_file.write_text(
        textwrap.dedent(
            """
            import pytest

            pytestmark = pytest.mark.integration

            def test_one():
                assert True
            """
        ),
        encoding="utf-8",
    )

    audit = test_truth_audit.audit_file(test_file, root=tmp_path)

    assert audit.tests == 1
    assert audit.marked_tests == 1
    assert audit.unmarked_tests == []
    assert audit.needs_truth_review is False


def test_truth_audit_accepts_truth_covered_high_mock_file(tmp_path, monkeypatch):
    test_file = tmp_path / "tests" / "test_worker_incoming.py"
    test_file.parent.mkdir()
    test_file.write_text(
        textwrap.dedent(
            """
            import pytest

            pytestmark = pytest.mark.unit

            def test_one(monkeypatch):
                monkeypatch.setattr(object(), "x", lambda: None, raising=False)
                monkeypatch.setattr(object(), "y", lambda: None, raising=False)
                monkeypatch.setattr(object(), "z", lambda: None, raising=False)
                monkeypatch.setattr(object(), "a", lambda: None, raising=False)
                monkeypatch.setattr(object(), "b", lambda: None, raising=False)
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.setitem(
        test_truth_audit.TRUTH_COVERED_FILES,
        "tests/test_worker_incoming.py",
        "scripts/inbox_worker_smoke.py",
    )

    audit = test_truth_audit.audit_file(test_file, root=tmp_path)

    assert audit.mock_points == 5
    assert audit.truth_coverage == "scripts/inbox_worker_smoke.py"
    assert audit.needs_truth_review is False
