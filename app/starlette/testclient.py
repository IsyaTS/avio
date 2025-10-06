"""Expose FastAPI's TestClient under starlette.testclient."""

from fastapi.testclient import TestClient

__all__ = ["TestClient"]
