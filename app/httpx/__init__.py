"""Minimal httpx shim providing AsyncClient and Response."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional


class HTTPError(Exception):
    pass


class HTTPStatusError(HTTPError):
    def __init__(self, message: str, response: "Response") -> None:
        super().__init__(message)
        self.response = response


class Response:
    def __init__(self, status_code: int = 200, json_data: Optional[Dict[str, Any]] = None, text: str | None = None) -> None:
        self.status_code = int(status_code)
        self._json = json_data or {}
        if text is None and json_data is not None:
            import json

            text = json.dumps(json_data, ensure_ascii=False)
        self.text = text or ""
        self.content = self.text.encode("utf-8")

    def json(self) -> Dict[str, Any]:
        return dict(self._json)


class AsyncClient:
    def __init__(self, timeout: float = 5.0) -> None:
        self.timeout = timeout

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - no cleanup required
        return None

    async def post(self, url: str, json: Any | None = None, headers: Dict[str, Any] | None = None) -> Response:
        # Return a basic success response. Tests typically monkeypatch this client
        # when they need to simulate errors.
        return Response(status_code=200, json_data={"ok": True})


__all__ = [
    "AsyncClient",
    "Response",
    "HTTPError",
    "HTTPStatusError",
]
