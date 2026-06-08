from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, MutableMapping


@dataclass(frozen=True)
class StateIoRuntimeDeps:
    state_key_prefix: str
    state_ttl_seconds: int
    state_store_unavailable_sentinel: object
    state_cache: MutableMapping[str, Any]
    with_sync_redis_fn: Callable[[Callable[[Any], Any], Any], Any]


class StateIoRuntime:
    def __init__(self, deps: StateIoRuntimeDeps) -> None:
        self.deps = deps

    def reset_state_store(self) -> None:
        def _clear(client: Any) -> None:
            pattern = f"{self.deps.state_key_prefix}:*"
            cursor = 0
            while True:
                cursor, keys = client.scan(cursor=cursor, match=pattern, count=500)
                if keys:
                    client.delete(*keys)
                if cursor == 0:
                    break

        self.deps.with_sync_redis_fn(_clear, None)
        self.deps.state_cache.clear()

    def state_key(self, tenant: int | None, contact_id: int | None) -> str:
        tenant_id = int(tenant or 0)
        contact = int(contact_id or 0)
        return f"{self.deps.state_key_prefix}:{tenant_id}:{contact}"

    def state_store_read(self, key: str) -> Dict[str, Any] | None:
        try:
            raw = self.deps.with_sync_redis_fn(
                lambda client: client.get(key),
                self.deps.state_store_unavailable_sentinel,
            )
            if raw is self.deps.state_store_unavailable_sentinel:
                cached = self.deps.state_cache.get(key)
                if cached:
                    return cached.to_dict()
                return None
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            cached = self.deps.state_cache.get(key)
            if cached:
                return cached.to_dict()
            return None

    def state_store_write(self, key: str, payload: Dict[str, Any]) -> None:
        self.deps.with_sync_redis_fn(
            lambda client: client.setex(
                key,
                self.deps.state_ttl_seconds,
                json.dumps(payload, ensure_ascii=False),
            ),
            None,
        )
