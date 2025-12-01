"""Expose FastAPI's TestClient under ``starlette_ext.testclient``."""

from fastapi.testclient import TestClient

__all__ = ["TestClient"]
