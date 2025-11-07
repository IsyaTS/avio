"""Compatibility wrapper that exposes the lightweight shims under ``app.sklearn``
as a top-level ``sklearn`` package.  This keeps runtime imports such as
``from sklearn.feature_extraction.text import TfidfVectorizer`` working without
pulling the heavy dependency into the image."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

_APP_SKLEARN = importlib.import_module("app.sklearn")


def _expose(name: str, target: str) -> ModuleType:
    module = importlib.import_module(target)
    sys.modules.setdefault(name, module)
    return module


# Ensure the bare package resolves to this module and re-export attrs.
sys.modules.setdefault("sklearn", sys.modules[__name__])
__all__ = list(getattr(_APP_SKLEARN, "__all__", []))
for attr in __all__:
    globals()[attr] = getattr(_APP_SKLEARN, attr)

# Mirror subpackages so fully-qualified imports keep working.
feature_extraction = _expose("sklearn.feature_extraction", "app.sklearn.feature_extraction")
feature_extraction.text = _expose(
    "sklearn.feature_extraction.text",
    "app.sklearn.feature_extraction.text",
)
metrics = _expose("sklearn.metrics", "app.sklearn.metrics")
metrics.pairwise = _expose("sklearn.metrics.pairwise", "app.sklearn.metrics.pairwise")

