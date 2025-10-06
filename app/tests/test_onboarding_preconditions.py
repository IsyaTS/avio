import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INNER = ROOT / "app"
for path in (ROOT, INNER):
    value = str(path)
    if value not in sys.path:
        sys.path.append(value)


@pytest.fixture()
def tenant_context(tmp_path, monkeypatch):
    tenants_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenants_dir))

    import core
    import onboarding_chat

    importlib.reload(core)
    importlib.reload(onboarding_chat)

    return tenants_dir, core, onboarding_chat


def _strip_links(core, tenant_id):
    cfg = core.read_tenant_config(tenant_id)
    passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
    integrations = cfg.get("integrations") if isinstance(cfg.get("integrations"), dict) else {}
    passport.pop("whatsapp_link", None)
    integrations.pop("whatsapp_link", None)
    cfg["passport"] = passport
    cfg["integrations"] = integrations
    core.write_tenant_config(tenant_id, cfg)


def test_channels_ready_with_whatsapp_link(tenant_context):
    _, core, onboarding_chat = tenant_context
    tenant_id = 3
    core.ensure_tenant_files(tenant_id)

    checks, _ = onboarding_chat.evaluate_preconditions(tenant_id)
    assert checks["channels"] is True


def test_channels_ready_with_primary_key(monkeypatch, tenant_context):
    _, core, onboarding_chat = tenant_context
    tenant_id = 4
    core.ensure_tenant_files(tenant_id)
    _strip_links(core, tenant_id)

    monkeypatch.setattr(onboarding_chat, "get_tenant_pubkey", lambda t: "primary" if t == tenant_id else "")

    checks, _ = onboarding_chat.evaluate_preconditions(tenant_id)
    assert checks["channels"] is True


def test_channels_missing_without_link_or_key(monkeypatch, tenant_context):
    _, core, onboarding_chat = tenant_context
    tenant_id = 5
    core.ensure_tenant_files(tenant_id)
    _strip_links(core, tenant_id)

    monkeypatch.setattr(onboarding_chat, "get_tenant_pubkey", lambda t: "")

    checks, _ = onboarding_chat.evaluate_preconditions(tenant_id)
    assert checks["channels"] is False


def test_channels_ready_with_session_status(monkeypatch, tenant_context):
    _, core, onboarding_chat = tenant_context
    tenant_id = 6
    core.ensure_tenant_files(tenant_id)
    _strip_links(core, tenant_id)

    monkeypatch.setattr(onboarding_chat, "get_tenant_pubkey", lambda t: "")

    cfg = core.read_tenant_config(tenant_id)
    cfg.setdefault("integrations", {})["wa_session"] = {"status": "authenticated"}
    core.write_tenant_config(tenant_id, cfg)

    checks, _ = onboarding_chat.evaluate_preconditions(tenant_id)
    assert checks["channels"] is True


def test_channels_missing_with_inactive_session(monkeypatch, tenant_context):
    _, core, onboarding_chat = tenant_context
    tenant_id = 7
    core.ensure_tenant_files(tenant_id)
    _strip_links(core, tenant_id)

    monkeypatch.setattr(onboarding_chat, "get_tenant_pubkey", lambda t: "")

    cfg = core.read_tenant_config(tenant_id)
    cfg.setdefault("integrations", {})["wa_session"] = {
        "status": "qr",
        "ready": False,
        "last_event": "qr",
    }
    core.write_tenant_config(tenant_id, cfg)

    checks, _ = onboarding_chat.evaluate_preconditions(tenant_id)
    assert checks["channels"] is False
