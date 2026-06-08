from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, MutableMapping


@dataclass(frozen=True)
class StateStoreRuntimeDeps:
    state_key_fn: Callable[[int | None, int | None], str]
    state_store_read_fn: Callable[[str], dict]
    state_store_write_fn: Callable[[str, dict], None]
    state_cache: MutableMapping[str, Any]
    with_sync_redis_fn: Callable[[Callable[[Any], Any], Any], Any]
    sales_state_cls: type


class StateStoreRuntime:
    def __init__(self, deps: StateStoreRuntimeDeps) -> None:
        self.deps = deps

    def load_sales_state(self, tenant: int | None, contact_id: int | None) -> Any:
        key = self.deps.state_key_fn(tenant, contact_id)
        payload = self.deps.state_store_read_fn(key)
        if payload:
            state = self.deps.sales_state_cls.from_dict(payload)
            self.deps.state_cache[key] = state
        else:
            self.deps.state_cache.pop(key, None)
            state = self.deps.sales_state_cls(tenant=int(tenant or 0), contact_id=int(contact_id or 0))
        return state

    def save_sales_state(self, state: Any) -> None:
        key = self.deps.state_key_fn(getattr(state, "tenant", None), getattr(state, "contact_id", None))
        payload = state.to_dict()
        self.deps.state_cache[key] = state
        self.deps.state_store_write_fn(key, payload)

    def reset_sales_state(self, tenant: int | None, contact_id: int | None) -> None:
        key = self.deps.state_key_fn(tenant, contact_id)
        self.deps.state_cache.pop(key, None)
        self.deps.with_sync_redis_fn(lambda client: client.delete(key), None)
