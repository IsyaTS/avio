from __future__ import annotations

import os
import pathlib


def resolve_public_key(admin_token: str) -> str:
    _ = admin_token  # kept for backward compatibility with older imports
    raw_value = os.getenv("PUBLIC_KEY")
    if raw_value is None:
        return ""
    return str(raw_value).strip()


def resolve_tenants_dir(*, root_dir: pathlib.Path, data_dir: pathlib.Path) -> pathlib.Path:
    env_value = os.getenv("TENANTS_DIR")
    if env_value:
        return pathlib.Path(env_value)

    repo_data = root_dir.parent / "data" / "tenants"
    try:
        repo_data.mkdir(parents=True, exist_ok=True)
        return repo_data
    except OSError:
        pass

    data_tenants = root_dir / "data" / "tenants"
    try:
        data_tenants.mkdir(parents=True, exist_ok=True)
        return data_tenants
    except OSError:
        pass

    app_tenants = root_dir / "app" / "tenants"
    if app_tenants.exists():
        return app_tenants

    fallback = data_dir / "tenants"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback
