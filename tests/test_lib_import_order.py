from __future__ import annotations

import importlib


def test_import_order_message_envelope_then_worker_main() -> None:
    importlib.import_module("libs.core.message_envelope")
    importlib.import_module("apps.worker.main")


def test_import_order_worker_main_then_message_envelope() -> None:
    importlib.import_module("apps.worker.main")
    importlib.import_module("libs.core.message_envelope")
