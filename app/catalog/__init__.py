"""Catalog helpers exposed for external modules."""

import sys

from .retriever import retrieve_context
from .indexer import ensure_catalog_index, invalidate_catalog_index, clear_catalog_cache
from .io import write_catalog_csv

__all__ = [
    "retrieve_context",
    "ensure_catalog_index",
    "invalidate_catalog_index",
    "clear_catalog_cache",
    "write_catalog_csv",
]

# NOTE: register short alias so legacy imports (`import catalog`) keep working
sys.modules.setdefault("catalog", sys.modules[__name__])
