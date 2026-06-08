"""Shared core package for Avio services."""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import importlib.util
import pathlib
import sys
from types import ModuleType

_SALES_CORE_MODULE = "libs.core.sales_core"
_SALES_CORE_INIT = pathlib.Path(__file__).resolve().parent / "sales_core" / "__init__.py"
_SALES_CORE_SINGLETON: ModuleType | None = None


def _get_sales_core_singleton() -> ModuleType | None:
    global _SALES_CORE_SINGLETON
    if _SALES_CORE_SINGLETON is not None:
        return _SALES_CORE_SINGLETON
    existing = sys.modules.get(_SALES_CORE_MODULE)
    if isinstance(existing, ModuleType):
        _SALES_CORE_SINGLETON = existing
        return existing
    return None


class _SalesCoreStableLoader(importlib.machinery.SourceFileLoader):
    def create_module(self, spec):  # type: ignore[override]
        module = _get_sales_core_singleton()
        if module is not None:
            return module
        return None

    def exec_module(self, module: ModuleType) -> None:  # type: ignore[override]
        super().exec_module(module)
        global _SALES_CORE_SINGLETON
        _SALES_CORE_SINGLETON = module


class _SalesCoreStableFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path=None, target=None):  # type: ignore[override]
        if fullname != _SALES_CORE_MODULE:
            return None
        if not _SALES_CORE_INIT.exists():
            return None
        loader = _SalesCoreStableLoader(fullname, str(_SALES_CORE_INIT))
        return importlib.util.spec_from_file_location(
            fullname,
            str(_SALES_CORE_INIT),
            loader=loader,
            submodule_search_locations=[str(_SALES_CORE_INIT.parent)],
        )


if not any(isinstance(finder, _SalesCoreStableFinder) for finder in sys.meta_path):
    sys.meta_path.insert(0, _SalesCoreStableFinder())


from . import common as common  # re-export for backwards compatibility
