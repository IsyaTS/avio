import importlib
from pathlib import Path

import importlib

import core


def test_default_tenants_dir_prefers_app_directory(monkeypatch):
    monkeypatch.delenv("TENANTS_DIR", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)

    importlib.reload(core)

    expected = Path(__file__).resolve().parents[1] / "tenants"
    assert core.TENANTS_DIR == expected
    assert expected.exists()


def test_write_persona_uses_resolved_tenants_dir(monkeypatch, tmp_path):
    monkeypatch.delenv("TENANTS_DIR", raising=False)
    monkeypatch.delenv("APP_DATA_DIR", raising=False)

    importlib.reload(core)

    fake_root = tmp_path / "repo"
    fake_root.mkdir()
    (fake_root / "data").mkdir()

    monkeypatch.setattr(core, "ROOT_DIR", fake_root, raising=False)
    monkeypatch.setattr(core, "DATA_DIR", fake_root / "app" / "data", raising=False)
    monkeypatch.setattr(core, "BASE_DIR", fake_root / "app", raising=False)

    resolved = core._resolve_tenants_dir()
    monkeypatch.setattr(core, "TENANTS_DIR", resolved, raising=False)

    core.write_persona(tenant=5, text="## Persona\n- custom")

    persona_path = resolved / "5" / "persona.md"
    assert persona_path.exists()
    assert persona_path.read_text(encoding="utf-8") == "## Persona\n- custom"

    tenant_cfg = resolved / "5" / "tenant.json"
    assert tenant_cfg.exists()


def test_write_persona_uses_default_on_empty(monkeypatch, tmp_path):
    tenant_dir = tmp_path / "tenants"
    monkeypatch.setenv("TENANTS_DIR", str(tenant_dir))
    monkeypatch.delenv("APP_DATA_DIR", raising=False)

    importlib.reload(core)

    core.write_persona(tenant=7, text="   ")

    persona_path = tenant_dir / "7" / "persona.md"
    assert persona_path.exists()
    content = persona_path.read_text(encoding="utf-8")
    assert content == core.DEFAULT_PERSONA_MD
