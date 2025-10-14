from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import prometheus_client
from prometheus_client import CollectorRegistry

prometheus_client.REGISTRY = CollectorRegistry()
prometheus_client.registry.REGISTRY = prometheus_client.REGISTRY
try:
    prometheus_client.metrics.REGISTRY = prometheus_client.REGISTRY
except AttributeError:  # pragma: no cover - metrics module may not be loaded yet
    pass

import pytest
from fastapi.testclient import TestClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:  # pragma: no cover - path setup
    sys.path.insert(0, str(ROOT_DIR))

import tgworker.api as tg_api
from tgworker.api import QRExpiredError, QRNotFoundError


@dataclass
class _StubSnapshot:
    status: str = "waiting_qr"
    qr_id: str | None = "stub-qr"
    qr_valid_until: int | None = None
    needs_2fa: bool = False
    twofa_pending: bool = False
    last_error: str | None = None
    twofa_backoff_until: int | None = None


@dataclass
class _StubFlow:
    status: str = "waiting_qr"
    qr_id: str | None = "stub-qr"
    qr_expires_at: float | None = None
    needs_2fa: bool = False
    twofa_pending: bool = False
    last_error: str | None = None
    qr_png: bytes | None = None
    qr_login_obj: object = object()


class StubSessionManager:
    def __init__(self) -> None:
        self.stats: Dict[str, int] = {
            "authorized": 0,
            "waiting": 1,
            "needs_2fa": 0,
        }
        self.raise_stats = False
        self.snapshot = _StubSnapshot()
        expiry_ms = int(time.time() * 1000) + 60_000
        self.snapshot.qr_valid_until = expiry_ms
        self.flow = _StubFlow(qr_expires_at=expiry_ms / 1000.0)
        self.qr_png = b"stub-png"
        self.start_calls: list[Tuple[int, bool]] = []

    async def start(self) -> None:  # pragma: no cover - lifecycle
        return None

    async def shutdown(self) -> None:  # pragma: no cover - lifecycle
        return None

    def stats_snapshot(self) -> Dict[str, int]:
        if self.raise_stats:
            raise RuntimeError("stats error")
        return dict(self.stats)

    async def start_session(self, tenant: int, *, force: bool = False):
        self.start_calls.append((tenant, force))
        return self.snapshot

    async def get_status(self, tenant: int):
        return self.snapshot

    async def login_flow_state(self, tenant: int):
        return self.flow

    def get_qr_png(self, qr_id: str, tenant: int | None = None) -> bytes:
        if qr_id != self.flow.qr_id:
            raise QRNotFoundError()
        if self.snapshot.qr_valid_until and self.snapshot.qr_valid_until < int(time.time() * 1000):
            raise QRExpiredError()
        return self.qr_png


@pytest.fixture
def tgworker_client(monkeypatch: pytest.MonkeyPatch):
    stub = StubSessionManager()
    monkeypatch.setattr(tg_api, "SessionManager", lambda *args, **kwargs: stub)
    app = tg_api.create_app()
    with TestClient(app) as client:
        yield client, stub
