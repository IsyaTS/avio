"""Compatibility shims exposed under the ``starlette_ext`` namespace."""

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from . import requests


def register_transport_validation(app: FastAPI) -> None:
    """Attach consistent handlers for transport validation errors."""

    @app.exception_handler(RequestValidationError)
    async def _request_validation_handler(request, exc):  # type: ignore[unused-ignore]
        return JSONResponse({"ok": False, "error": "invalid_payload"}, status_code=422)

    @app.exception_handler(ValidationError)
    async def _pydantic_validation_handler(request, exc):  # type: ignore[unused-ignore]
        return JSONResponse({"ok": False, "error": "invalid_payload"}, status_code=400)


__all__ = ["requests", "register_transport_validation"]
