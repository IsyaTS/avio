"""httpx wrapper enforcing application defaults."""

from __future__ import annotations

import httpx as _httpx

DEFAULT_TIMEOUT = _httpx.Timeout(connect=2.0, read=10.0, write=10.0, pool=10.0)
DEFAULT_RETRIES = 2


class _PatchedAsyncClient(_httpx.AsyncClient):
    def __init__(self, *args, timeout: _httpx.Timeout | float | None = None, transport: _httpx.AsyncHTTPTransport | None = None, **kwargs) -> None:
        if timeout is None:
            timeout = DEFAULT_TIMEOUT
        if transport is None and DEFAULT_RETRIES > 0:
            transport = _httpx.AsyncHTTPTransport(retries=DEFAULT_RETRIES)
        super().__init__(*args, timeout=timeout, transport=transport, **kwargs)


# Monkey patch the globally imported httpx.AsyncClient so any "import httpx"
# consumers automatically inherit the sane defaults defined above.
_httpx.AsyncClient = _PatchedAsyncClient  # type: ignore[assignment]

# Re-export frequently used symbols for convenience.
AsyncClient = _PatchedAsyncClient
Timeout = _httpx.Timeout
Response = _httpx.Response
HTTPError = _httpx.HTTPError
HTTPStatusError = _httpx.HTTPStatusError
RequestError = _httpx.RequestError
TimeoutException = _httpx.TimeoutException


__all__ = [
    "AsyncClient",
    "Timeout",
    "Response",
    "HTTPError",
    "HTTPStatusError",
    "RequestError",
    "TimeoutException",
    "DEFAULT_TIMEOUT",
]
