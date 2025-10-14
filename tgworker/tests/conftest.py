from __future__ import annotations

import prometheus_client

prometheus_client.REGISTRY._names_to_collectors.clear()
prometheus_client.REGISTRY._collector_to_names.clear()
