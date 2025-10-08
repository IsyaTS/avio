"""Minimal static file server used by the FastAPI shim."""

from __future__ import annotations

import mimetypes
from pathlib import Path

from .responses import Response


class StaticFiles:
    def __init__(self, *, directory: str, html: bool = False) -> None:
        self.directory = Path(directory).resolve()
        self.html = html

    def _resolve_path(self, relative_path: str) -> Path:
        clean = Path(relative_path.strip("/"))
        parts = [segment for segment in clean.parts if segment not in {"", ".", ".."}]

        if not parts:
            if self.html:
                index_candidate = (self.directory / "index.html").resolve()
                if index_candidate.exists() and index_candidate.is_file():
                    return index_candidate
            raise FileNotFoundError

        target = self.directory.joinpath(*parts).resolve()
        try:
            target.relative_to(self.directory)
        except ValueError:
            raise FileNotFoundError from None

        if target.is_dir():
            if self.html:
                index_path = (target / "index.html").resolve()
                if index_path.exists() and index_path.is_file():
                    return index_path
            raise FileNotFoundError

        if not target.is_file():
            raise FileNotFoundError

        return target

    def get_response(self, path: str, method: str = "GET") -> Response:
        target = self._resolve_path(path)
        data = target.read_bytes()
        media_type, _ = mimetypes.guess_type(target.name)
        headers = {"Cache-Control": "public, max-age=31536000, immutable"}

        if method.upper() == "HEAD":
            response = Response(b"", status_code=200, headers=headers, media_type=media_type or "application/octet-stream")
            response.headers["content-length"] = str(len(data))
            return response

        return Response(data, status_code=200, headers=headers, media_type=media_type or "application/octet-stream")


__all__ = ["StaticFiles"]
