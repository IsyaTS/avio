"""Starlette-compatible Request shim relying on the FastAPI stub."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple
from urllib.parse import parse_qsl

from fastapi import Request as _FastAPIRequest


def _decode_headers(raw: Iterable[Tuple[Any, Any]]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for key, value in raw:
        if isinstance(key, bytes):
            key = key.decode("latin-1")
        if isinstance(value, bytes):
            value = value.decode("latin-1")
        headers[str(key)] = str(value)
    return headers


class Request(_FastAPIRequest):
    def __init__(self, scope: Dict[str, Any], receive) -> None:
        app = scope.get("app")
        method = scope.get("method", "GET")
        path = scope.get("path", "/")
        raw_query = scope.get("query_string", b"")
        if isinstance(raw_query, bytes):
            query_params = dict(parse_qsl(raw_query.decode("utf-8")))
        else:
            query_params = dict(parse_qsl(str(raw_query)))
        headers = _decode_headers(scope.get("headers", []))
        scheme = scope.get("scheme", "http")
        host, port = scope.get("server", ("testserver", 80))
        if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
            base_url = f"{scheme}://{host}"
        else:
            base_url = f"{scheme}://{host}:{port}"
        super().__init__(
            app=app,
            method=method,
            path=path,
            query_params=query_params,
            headers=headers,
            cookies={},
            body=b"",
            base_url=base_url,
        )
        self.scope = scope
        self._receive = receive


__all__ = ["Request"]
