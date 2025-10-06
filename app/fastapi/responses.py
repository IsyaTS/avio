"""Minimal response objects mimicking FastAPI/Starlette behaviour."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


class _Headers(dict):
    def __init__(self, initial: Optional[Dict[str, Any]] = None) -> None:
        super().__init__()
        if initial:
            for key, value in initial.items():
                self[key] = value

    def __setitem__(self, key: str, value: Any) -> None:  # type: ignore[override]
        super().__setitem__(key.lower(), str(value))

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        return super().get(key.lower(), default)


def _ensure_bytes(content: Any) -> bytes:
    if content is None:
        return b""
    if isinstance(content, bytes):
        return content
    if isinstance(content, bytearray):
        return bytes(content)
    if isinstance(content, str):
        return content.encode("utf-8")
    if hasattr(content, "read"):
        data = content.read()  # type: ignore[attr-defined]
        return _ensure_bytes(data)
    if isinstance(content, Iterable):
        buf = bytearray()
        for chunk in content:  # type: ignore[assignment]
            buf.extend(_ensure_bytes(chunk))
        return bytes(buf)
    return _ensure_bytes(str(content))


class Response:
    def __init__(
        self,
        content: Any = b"",
        *,
        status_code: int = 200,
        headers: Optional[Dict[str, Any]] = None,
        media_type: Optional[str] = None,
        background: Any = None,
    ) -> None:
        self.status_code = int(status_code)
        self.media_type = media_type or "text/plain"
        self.headers = _Headers(headers)
        if "content-type" not in self.headers and self.media_type:
            self.headers["content-type"] = self.media_type
        self.background = background
        self.body = _ensure_bytes(content)

    @property
    def content(self) -> bytes:
        return self.body

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        text = self.text.strip()
        if not text:
            return None
        return json.loads(text)


class JSONResponse(Response):
    def __init__(
        self,
        content: Dict[str, Any],
        *,
        status_code: int = 200,
        headers: Optional[Dict[str, Any]] = None,
        media_type: str = "application/json",
        background: Any = None,
    ) -> None:
        body = json.dumps(content, ensure_ascii=False).encode("utf-8")
        super().__init__(
            body,
            status_code=status_code,
            headers=headers,
            media_type=media_type,
            background=background,
        )


class HTMLResponse(Response):
    def __init__(self, content: str, *, status_code: int = 200, headers: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(content, status_code=status_code, headers=headers, media_type="text/html; charset=utf-8")


class RedirectResponse(Response):
    def __init__(
        self,
        url: str,
        *,
        status_code: int = 307,
        headers: Optional[Dict[str, Any]] = None,
    ) -> None:
        hdrs = dict(headers or {})
        hdrs.setdefault("location", url)
        super().__init__(b"", status_code=status_code, headers=hdrs, media_type="text/plain")


class FileResponse(Response):
    def __init__(
        self,
        path: str | Path,
        *,
        media_type: Optional[str] = None,
        filename: Optional[str] = None,
        status_code: int = 200,
        headers: Optional[Dict[str, Any]] = None,
        background: Any = None,
    ) -> None:
        file_path = Path(path)
        data = file_path.read_bytes()
        hdrs = dict(headers or {})
        if filename:
            hdrs.setdefault("content-disposition", f"attachment; filename=\"{filename}\"")
        super().__init__(
            data,
            status_code=status_code,
            headers=hdrs,
            media_type=media_type or "application/octet-stream",
            background=background,
        )


class StreamingResponse(Response):
    def __init__(
        self,
        content: Iterable[Any] | Any,
        *,
        status_code: int = 200,
        media_type: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> None:
        body = _ensure_bytes(content)
        super().__init__(body, status_code=status_code, headers=headers, media_type=media_type)


__all__ = [
    "Response",
    "JSONResponse",
    "HTMLResponse",
    "RedirectResponse",
    "FileResponse",
    "StreamingResponse",
]
