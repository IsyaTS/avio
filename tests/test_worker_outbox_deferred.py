from __future__ import annotations

import time

from apps.worker import main as worker_module


def _reset_heap() -> None:
    worker_module._DEFERRED_OUTBOX_HEAP.clear()


def test_deferred_outbox_releases_only_due_items() -> None:
    _reset_heap()
    now = time.time()
    worker_module._defer_outbox_item({"id": "future"}, now + 30.0)
    worker_module._defer_outbox_item({"id": "due"}, now - 1.0)

    first = worker_module._pop_ready_deferred_outbox(now_ts=now)
    assert first is not None
    assert first.get("id") == "due"

    second = worker_module._pop_ready_deferred_outbox(now_ts=now)
    assert second is None


def test_deferred_outbox_wait_reports_until_next_due() -> None:
    _reset_heap()
    now = time.time()
    worker_module._defer_outbox_item({"id": "a"}, now + 5.0)
    wait = worker_module._next_deferred_outbox_wait(now_ts=now)
    assert wait is not None
    assert 4.9 <= wait <= 5.1


def test_deferred_outbox_wait_none_for_empty_heap() -> None:
    _reset_heap()
    assert worker_module._next_deferred_outbox_wait(now_ts=time.time()) is None

