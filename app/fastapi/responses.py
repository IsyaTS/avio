"""Thin re-export layer over :mod:`starlette.responses`."""

from __future__ import annotations

from starlette.responses import (  # type: ignore[import]
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)

__all__ = [
    "Response",
    "JSONResponse",
    "HTMLResponse",
    "RedirectResponse",
    "FileResponse",
    "StreamingResponse",
]
