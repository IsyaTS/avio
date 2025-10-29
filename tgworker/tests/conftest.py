from __future__ import annotations

try:
    import prometheus_client
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    class _DummyRegistry:
        def __init__(self) -> None:
            self._names_to_collectors = {}
            self._collector_to_names = {}

    prometheus_client = type("PrometheusModule", (), {"REGISTRY": _DummyRegistry()})()  # type: ignore

prometheus_client.REGISTRY._names_to_collectors.clear()
prometheus_client.REGISTRY._collector_to_names.clear()
