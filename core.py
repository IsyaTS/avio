from app.core import *  # noqa: F401,F403

import importlib as _importlib

_core_pkg = _importlib.import_module("app.core")
for _name, _value in _core_pkg.__dict__.items():
    if _name in {"__name__", "__package__", "__spec__", "__loader__", "__file__", "__path__"}:
        continue
    globals().setdefault(_name, _value)

del _core_pkg, _importlib, _name, _value
