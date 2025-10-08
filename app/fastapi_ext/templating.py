"""Minimal Jinja2Templates wrapper compatible with FastAPI tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .responses import HTMLResponse


class Jinja2Templates:
    def __init__(self, *, directory: str) -> None:
        self.directory = Path(directory)
        self.env = Environment(
            loader=FileSystemLoader(str(self.directory)),
            autoescape=select_autoescape(["html", "xml"]),
        )

    def TemplateResponse(
        self,
        name: str,
        context: Dict[str, Any],
        status_code: int = 200,
    ) -> HTMLResponse:
        template = self.env.get_template(name)
        request = context.get("request")
        if request is not None:
            self.env.globals["url_for"] = request.url_for
        content = template.render(**context)
        return HTMLResponse(content, status_code=status_code)


__all__ = ["Jinja2Templates"]
