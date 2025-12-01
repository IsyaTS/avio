"""httpx wrapper enforcing application defaults."""

from __future__ import annotations

import httpx as _httpx

DEFAULT_TIMEOUT = _httpx.Timeout(connect=2.0, read=10.0, write=10.0, pool=10.0)
DEFAULT_RETRIES = 2


class _PatchedAsyncClient(_httpx.AsyncClient):
    def __init__(
        self,
        *args,
        timeout: _httpx.Timeout | float | None = None,
        transport: _httpx.AsyncHTTPTransport | None = None,
        **kwargs,
    ) -> None:
        if timeout is None:
            timeout = DEFAULT_TIMEOUT
        if transport is None and DEFAULT_RETRIES > 0:
            transport = _httpx.AsyncHTTPTransport(retries=DEFAULT_RETRIES)
        super().__init__(*args, timeout=timeout, transport=transport, **kwargs)


_httpx.AsyncClient = _PatchedAsyncClient  # type: ignore[assignment]

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
