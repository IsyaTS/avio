"""Test configuration utilities."""

from __future__ import annotations

import os
import sys
import asyncio
import inspect
from pathlib import Path
try:  # Ensure trio exposes MultiError for anyio's legacy expectations
    import trio  # type: ignore

    if not hasattr(trio, "MultiError"):
        class _TrioMultiError(BaseExceptionGroup):
            pass

        trio.MultiError = _TrioMultiError  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - optional dependency
    pass

try:
    import prometheus_client
    from prometheus_client import CollectorRegistry
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    class CollectorRegistry:  # type: ignore[misc]
        def __init__(self) -> None:
            self._metrics = {}

    class _DummyModule:
        REGISTRY = CollectorRegistry()

    prometheus_client = _DummyModule()  # type: ignore
    prometheus_client.registry = _DummyModule()  # type: ignore
    prometheus_client.metrics = _DummyModule()  # type: ignore

prometheus_client.REGISTRY = CollectorRegistry()  # reset default registry for tests
prometheus_client.registry.REGISTRY = prometheus_client.REGISTRY
try:  # align metrics module if already imported
    prometheus_client.metrics.REGISTRY = prometheus_client.REGISTRY
except AttributeError:  # pragma: no cover - metrics may not be imported
    pass

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("PUBLIC_KEY", "test-public-key")
os.environ.setdefault("TESTING", "1")


def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: run coroutine test via asyncio event loop")


def pytest_pyfunc_call(pyfuncitem):
    if "asyncio" not in pyfuncitem.keywords:
        return None
    if pyfuncitem.config.pluginmanager.hasplugin("asyncio"):
        return None
    test_fn = pyfuncitem.obj
    if not inspect.iscoroutinefunction(test_fn):
        return None
    kwargs = {name: pyfuncitem.funcargs[name] for name in pyfuncitem._fixtureinfo.argnames}
    asyncio.run(test_fn(**kwargs))
    return True
