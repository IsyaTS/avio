from __future__ import annotations

import contextlib
import fcntl
import pathlib
from collections.abc import Iterator


@contextlib.contextmanager
def smoke_tenant_lock(name: str, tenant_id: int) -> Iterator[None]:
    lock_dir = pathlib.Path("/tmp/avio-smoke-locks")
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{name}-{int(tenant_id)}.lock"
    with lock_path.open("w", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


__all__ = ["smoke_tenant_lock"]
