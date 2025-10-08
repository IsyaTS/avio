"""Expose the app-local Starlette compatibility shims under ``starlette_ext``."""

from importlib import import_module as _import_module
import sys as _sys

_module = _import_module("app.starlette_ext")
_sys.modules[__name__] = _module
