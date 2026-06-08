from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class IoRuntimeDeps:
    with_sync_redis_fn: Callable[[Callable[[Any], Any], Any], Any]
    tenant_pubkeys_hash: str


class IoRuntime:
    def __init__(self, deps: IoRuntimeDeps) -> None:
        self.deps = deps

    def get_tenant_pubkey(self, tenant: int) -> str:
        return self.deps.with_sync_redis_fn(
            lambda client: client.hget(self.deps.tenant_pubkeys_hash, str(int(tenant))) or "",
            "",
        )

    def set_tenant_pubkey(self, tenant: int, key: str) -> None:
        key_norm = (key or "").strip().lower()

        def _apply(client: Any) -> None:
            if key_norm:
                client.hset(self.deps.tenant_pubkeys_hash, str(int(tenant)), key_norm)
            else:
                client.hdel(self.deps.tenant_pubkeys_hash, str(int(tenant)))

        self.deps.with_sync_redis_fn(_apply, None)

    def http_json(self, method: str, url: str, data: dict | None = None, timeout: float = 8.0):
        body = None
        headers = {"Accept": "application/json"}
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                ctype = resp.headers.get("Content-Type", "")
                raw = resp.read()
                return resp.status, ctype, raw
        except urllib.error.HTTPError as exc:
            return exc.code, exc.headers.get("Content-Type", ""), exc.read()
        except Exception as exc:
            return 599, "text/plain", str(exc).encode("utf-8")
