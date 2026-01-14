from __future__ import annotations

import time
import sys
import types

if "httpx" not in sys.modules:
    sys.modules["httpx"] = types.SimpleNamespace(AsyncClient=object, HTTPError=Exception, TimeoutException=Exception)

from libs.core.integrations.amocrm import build_oauth_state, verify_oauth_state


def test_state_roundtrip():
    secret = "secret-key"
    payload = {"tenant_id": 1, "k": "abc", "ts": int(time.time())}
    state = build_oauth_state(payload, secret)
    decoded = verify_oauth_state(state, secret)
    assert decoded and decoded["tenant_id"] == 1
    assert decoded["k"] == "abc"


def test_state_rejects_tamper():
    secret = "secret-key"
    payload = {"tenant_id": 2, "k": "x", "ts": int(time.time())}
    state = build_oauth_state(payload, secret)
    assert verify_oauth_state(state + "x", secret) is None


def test_state_rejects_expired():
    secret = "secret-key"
    payload = {"tenant_id": 3, "k": "x", "ts": int(time.time()) - 120}
    state = build_oauth_state(payload, secret)
    assert verify_oauth_state(state, secret, ttl=30) is None
