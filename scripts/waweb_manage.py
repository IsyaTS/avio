#!/usr/bin/env python3
"""
Utility to manage dedicated waweb containers per tenant.

Usage:
    scripts/waweb_manage.py up --tenant 1
    scripts/waweb_manage.py up --all
    scripts/waweb_manage.py down --tenant 2
    scripts/waweb_manage.py status --all
    scripts/waweb_manage.py logs --tenant 3 --follow
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "docker-compose.waweb.yml"
DEFAULT_NETWORK_NAME = "avio_default"
TENANTS_CONFIG_PATH = Path(
    os.getenv("TENANTS_CONFIG_PATH") or (REPO_ROOT / "config" / "tenants.yml")
)
STATE_ROOT = Path(os.getenv("WAWEB_STATE_ROOT") or (REPO_ROOT / "data" / "wa_state"))


class TenantConfig:
    def __init__(self, tenant_id: int, record: Optional[Dict[str, object]] = None):
        self.id = tenant_id
        record = record or {}
        waweb_cfg = (
            record.get("waweb") if isinstance(record.get("waweb"), dict) else {}
        )
        self.host = str(waweb_cfg.get("host") or f"waweb-{tenant_id}")
        port_value = waweb_cfg.get("port")
        try:
            self.port = int(str(port_value).strip()) if port_value else None
        except Exception:
            self.port = None
        self.container_name = str(
            record.get("container_name") or f"waweb-{tenant_id}"
        )
        self.state_dir = STATE_ROOT / str(tenant_id)


def _load_tenant_configs() -> Dict[int, TenantConfig]:
    if TENANTS_CONFIG_PATH.exists():
        try:
            raw = yaml.safe_load(TENANTS_CONFIG_PATH.read_text("utf-8"))
        except Exception as exc:  # pragma: no cover - invalid YAML
            print(
                f"[waweb] failed to parse tenants config: {TENANTS_CONFIG_PATH} ({exc})",
                file=sys.stderr,
            )
            raw = {}
    else:
        raw = {}

    tenants = {}
    items = raw.get("tenants") if isinstance(raw, dict) else None
    if isinstance(items, list):
        for entry in items:
            if not isinstance(entry, dict) or "id" not in entry:
                continue
            try:
                tenant_id = int(str(entry["id"]).strip())
            except Exception:
                continue
            tenants[tenant_id] = TenantConfig(tenant_id, entry)
    return tenants


def _discover_tenants_from_disk(existing: Dict[int, TenantConfig]) -> Dict[int, TenantConfig]:
    discovered = dict(existing)
    if STATE_ROOT.exists():
        for child in STATE_ROOT.iterdir():
            if not child.is_dir():
                continue
            try:
                tenant_id = int(child.name)
            except Exception:
                continue
            if tenant_id not in discovered:
                discovered[tenant_id] = TenantConfig(tenant_id)
    return discovered


def _ensure_state_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    guard = path / "DO_NOT_DELETE.txt"
    if not guard.exists():
        guard.write_text(
            "ВНИМАНИЕ: это хранилище сессии WhatsApp. Не удаляйте и не очищайте каталог.\n",
            encoding="utf-8",
        )


def _compose_env(cfg: TenantConfig) -> Dict[str, str]:
    env = os.environ.copy()
    env.setdefault("APP_BASE_URL", "http://app:8000")
    env.setdefault("WAWEB_NETWORK_NAME", DEFAULT_NETWORK_NAME)
    env["WAWEB_TENANT_ID"] = str(cfg.id)
    env["WAWEB_CONTAINER_NAME"] = cfg.container_name
    env["WAWEB_HOSTNAME"] = cfg.host
    env["WAWEB_STATE_DIR"] = str(cfg.state_dir)
    if cfg.port:
        env["WAWEB_PORT"] = str(cfg.port)
    env.setdefault("WAWEB_ADMIN_TOKEN", env.get("ADMIN_TOKEN", ""))
    return env


def _run_compose(cfg: TenantConfig, compose_args: List[str]) -> int:
    env = _compose_env(cfg)
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), "--project-name", f"waweb-{cfg.id}"] + compose_args
    try:
        return subprocess.call(cmd, cwd=str(REPO_ROOT), env=env)
    except FileNotFoundError:
        print("[waweb] docker compose not found in PATH", file=sys.stderr)
        return 1


def _ensure_network() -> None:
    network_name = os.getenv("WAWEB_NETWORK_NAME", DEFAULT_NETWORK_NAME)
    try:
        subprocess.check_call(["docker", "network", "inspect", network_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        # создаём пустую сеть, если основной стек еще не поднят
        subprocess.check_call(["docker", "network", "create", network_name], stdout=subprocess.DEVNULL)


def _iter_target_tenants(args: argparse.Namespace, tenants: Dict[int, TenantConfig]) -> Iterable[TenantConfig]:
    tenants = _discover_tenants_from_disk(tenants)
    if args.tenant is not None:
        tenants = {args.tenant: tenants.get(args.tenant, TenantConfig(args.tenant))}

    if not tenants:
        print("[waweb] нет доступных арендаторов. Добавьте запись в config/tenants.yml или создайте каталог в data/wa_state/<tenant>.")
        return []

    for tenant_id in sorted(tenants.keys()):
        yield tenants[tenant_id]


def cmd_up(args: argparse.Namespace, tenants: Dict[int, TenantConfig]) -> int:
    _ensure_network()
    exit_code = 0
    for cfg in _iter_target_tenants(args, tenants):
        _ensure_state_dir(cfg.state_dir)
        print(f"[waweb] starting tenant {cfg.id} ({cfg.host})")
        code = _run_compose(cfg, ["up", "-d", "--build"])
        exit_code = exit_code or code
    return exit_code


def cmd_down(args: argparse.Namespace, tenants: Dict[int, TenantConfig]) -> int:
    exit_code = 0
    for cfg in _iter_target_tenants(args, tenants):
        print(f"[waweb] stopping tenant {cfg.id}")
        code = _run_compose(cfg, ["down"])
        exit_code = exit_code or code
    return exit_code


def cmd_restart(args: argparse.Namespace, tenants: Dict[int, TenantConfig]) -> int:
    exit_code = cmd_down(args, tenants)
    return exit_code or cmd_up(args, tenants)


def cmd_status(args: argparse.Namespace, tenants: Dict[int, TenantConfig]) -> int:
    exit_code = 0
    for cfg in _iter_target_tenants(args, tenants):
        print(f"[waweb] status for tenant {cfg.id}")
        code = _run_compose(cfg, ["ps"])
        exit_code = exit_code or code
    return exit_code


def cmd_logs(args: argparse.Namespace, tenants: Dict[int, TenantConfig]) -> int:
    exit_code = 0
    compose_args = ["logs"]
    if args.follow:
        compose_args.append("-f")
    for cfg in _iter_target_tenants(args, tenants):
        print(f"[waweb] logs tenant {cfg.id}")
        code = _run_compose(cfg, compose_args)
        exit_code = exit_code or code
    return exit_code


def cmd_purge(_: argparse.Namespace, __: Dict[int, TenantConfig]) -> int:
    print("нельзя удалять: очистка хранилища сессий запрещена", file=sys.stderr)
    return 1


COMMANDS = {
    "up": cmd_up,
    "down": cmd_down,
    "restart": cmd_restart,
    "status": cmd_status,
    "logs": cmd_logs,
    "purge": cmd_purge,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage dedicated waweb containers")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(sub):
        sub.add_argument("--tenant", type=int, help="Tenant identifier")
        sub.add_argument("--all", action="store_true", help="Operate on all known tenants")

    add_common(subparsers.add_parser("up", help="Start waweb container(s)"))
    add_common(subparsers.add_parser("down", help="Stop waweb container(s)"))
    add_common(subparsers.add_parser("restart", help="Restart waweb container(s)"))
    add_common(subparsers.add_parser("status", help="Show compose status"))

    logs_parser = subparsers.add_parser("logs", help="Stream logs from waweb container(s)")
    add_common(logs_parser)
    logs_parser.add_argument("--follow", "-f", action="store_true", help="Follow log output")

    purge_parser = subparsers.add_parser("purge", help="Attempt to remove waweb state (blocked)")
    add_common(purge_parser)

    args = parser.parse_args()
    if getattr(args, "all", False):
        args.tenant = None
    return args


def main() -> int:
    args = parse_args()
    tenants = _load_tenant_configs()
    handler = COMMANDS.get(args.command)
    if handler is None:
        print(f"[waweb] unsupported command: {args.command}", file=sys.stderr)
        return 1
    return handler(args, tenants)


if __name__ == "__main__":
    sys.exit(main())
