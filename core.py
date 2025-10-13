from app.core import *  # noqa: F401,F403

import importlib as _importlib

ADMIN_COOKIE = "admin_session"

_core_pkg = _importlib.import_module("app.core")
for _name, _value in _core_pkg.__dict__.items():
    if _name in {"__name__", "__package__", "__spec__", "__loader__", "__file__", "__path__"}:
        continue
    globals().setdefault(_name, _value)

__all__ = list(globals().get("__all__", []))
for _export in ("ADMIN_COOKIE", "settings", "get_tenant_pubkey", "set_tenant_pubkey"):
    if _export not in __all__:
        __all__.append(_export)

del _core_pkg, _importlib, _name, _value, _export
