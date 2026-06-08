from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.services import max_personal_service


@pytest.fixture(autouse=True)
def _reset_runtime_overrides():
    max_personal_service._RUNTIME_INTEGRATION_OVERRIDES.clear()
    yield
    max_personal_service._RUNTIME_INTEGRATION_OVERRIDES.clear()


def test_ensure_event_secret_persists(monkeypatch):
    storage: dict[int, dict] = {
        101: {"integrations": {"max_personal": {"enabled": True}}}
    }

    def _read(tenant_id: int):
        return storage.get(int(tenant_id), {})

    def _write(tenant_id: int, payload: dict):
        storage[int(tenant_id)] = payload

    monkeypatch.setattr(max_personal_service, "read_tenant_config", _read, raising=False)
    monkeypatch.setattr(max_personal_service, "write_tenant_config", _write, raising=False)

    generated = max_personal_service.ensure_event_secret(101)
    again = max_personal_service.ensure_event_secret(101)

    assert generated
    assert again == generated
    assert (
        storage[101]["integrations"]["max_personal"]["event_secret"]
        == generated
    )


def test_build_state_payload_and_flags(monkeypatch):
    storage: dict[int, dict] = {
        7: {
            "integrations": {
                "max_personal": {
                    "enabled": True,
                    "outbound_enabled": False,
                    "session_status": "waiting_qr",
                }
            }
        }
    }

    monkeypatch.setattr(
        max_personal_service,
        "read_tenant_config",
        lambda tenant_id: storage.get(int(tenant_id), {}),
        raising=False,
    )

    payload = max_personal_service.build_state_payload(7, None)
    assert payload["enabled"] is True
    assert payload["outbound_enabled"] is False
    assert payload["status"] == "waiting_qr"
    assert payload["connected"] is False

    monkeypatch.setenv("MAX_PERSONAL_KILL_SWITCH", "1")
    payload_killed = max_personal_service.build_state_payload(7, {"status": "authorized"})
    assert payload_killed["kill_switch"] is True
    assert payload_killed["enabled"] is False
    assert payload_killed["connected"] is True


def test_worker_url_and_token_resolution(monkeypatch):
    monkeypatch.delenv("MAX_PERSONAL_WORKER_URL", raising=False)
    monkeypatch.delenv("MAXWORKER_URL", raising=False)
    monkeypatch.delenv("MAX_PERSONAL_WORKER_TOKEN", raising=False)
    monkeypatch.delenv("MAXWORKER_TOKEN", raising=False)
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
    monkeypatch.setenv("MAXWORKER_URL", "http://maxworker-test:9010/")
    monkeypatch.setenv("MAXWORKER_TOKEN", "token-xyz")

    assert max_personal_service.max_personal_worker_url() == "http://maxworker-test:9010"
    assert max_personal_service.max_personal_worker_token() == "token-xyz"


def test_update_integration_falls_back_to_runtime_override_on_permission_error(monkeypatch):
    storage: dict[int, dict] = {
        101: {"integrations": {"max_personal": {"enabled": True}}}
    }
    max_personal_service._RUNTIME_INTEGRATION_OVERRIDES.clear()

    def _read(tenant_id: int):
        return storage.get(int(tenant_id), {})

    def _write(_tenant_id: int, _payload: dict):
        raise PermissionError("readonly")

    monkeypatch.setattr(max_personal_service, "read_tenant_config", _read, raising=False)
    monkeypatch.setattr(max_personal_service, "write_tenant_config", _write, raising=False)

    merged = max_personal_service.update_integration(101, {"session_status": "authorized"})
    resolved = max_personal_service.get_integration(101)

    assert merged["session_status"] == "authorized"
    assert resolved["session_status"] == "authorized"


def test_update_integration_persists_overlay_when_primary_is_readonly(
    monkeypatch,
    tmp_path: Path,
):
    storage: dict[int, dict] = {
        101: {"integrations": {"max_personal": {"session_status": "authorized"}}}
    }

    def _read(tenant_id: int):
        return storage.get(int(tenant_id), {})

    def _write(_tenant_id: int, _payload: dict):
        raise PermissionError("readonly")

    monkeypatch.setattr(max_personal_service, "read_tenant_config", _read, raising=False)
    monkeypatch.setattr(max_personal_service, "write_tenant_config", _write, raising=False)
    monkeypatch.setenv("TENANT_CONFIG_DIR", str(tmp_path))

    merged = max_personal_service.update_integration(
        101,
        {"enabled": True, "outbound_enabled": True, "event_secret": "sec-101"},
    )
    assert merged["enabled"] is True
    overlay_file = tmp_path / "101.json"
    assert overlay_file.exists()
    payload = json.loads(overlay_file.read_text(encoding="utf-8"))
    mp = payload["integrations"]["max_personal"]
    assert mp["enabled"] is True
    assert mp["outbound_enabled"] is True
    assert mp["event_secret"] == "sec-101"


def test_ensure_event_secret_prefers_session_metadata(monkeypatch, tmp_path: Path):
    storage: dict[int, dict] = {202: {"integrations": {"max_personal": {}}}}
    sessions_dir = tmp_path / "sessions"
    session_file = sessions_dir / "tenant-202" / "avio-session.json"
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "tenant": 202,
                "callback_url": "https://dev.avio.website/webhook/max_personal?tenant=202&token=session-token-202",
                "webhook_token": "session-token-202",
            }
        ),
        encoding="utf-8",
    )

    def _read(tenant_id: int):
        return storage.get(int(tenant_id), {})

    def _write(tenant_id: int, payload: dict):
        storage[int(tenant_id)] = payload

    monkeypatch.setattr(max_personal_service, "read_tenant_config", _read, raising=False)
    monkeypatch.setattr(max_personal_service, "write_tenant_config", _write, raising=False)
    monkeypatch.setenv("MAX_PERSONAL_SESSIONS_DIR", str(sessions_dir))

    secret = max_personal_service.ensure_event_secret(202)
    assert secret == "session-token-202"
    assert (
        storage[202]["integrations"]["max_personal"]["event_secret"]
        == "session-token-202"
    )


def test_integration_enabled_falls_back_to_authorized_session(monkeypatch):
    storage: dict[int, dict] = {
        303: {
            "integrations": {"max_personal": {"session_status": "authorized"}}
        }
    }
    monkeypatch.setattr(
        max_personal_service,
        "read_tenant_config",
        lambda tenant_id: storage.get(int(tenant_id), {}),
        raising=False,
    )
    monkeypatch.delenv("MAX_PERSONAL_KILL_SWITCH", raising=False)

    assert max_personal_service.integration_enabled(303) is True
