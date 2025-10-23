from __future__ import annotations

import pathlib
from datetime import datetime
from typing import Any, Mapping

from fastapi.templating import Jinja2Templates

ROOT = pathlib.Path(__file__).resolve().parent.parent
TEMPLATES_DIR = ROOT / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.auto_reload = True
templates.env.cache = {}

from .common import asset_version, static_url


def _datetimeformat(value):
    try:
        if not value:
            return "—"
        return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(value)


templates.env.filters["datetimeformat"] = _datetimeformat
templates.env.globals["static_url"] = static_url
templates.env.globals.setdefault("client_settings_version", asset_version())


def render_template(
    template_name: str,
    context: Mapping[str, Any],
    *,
    status_code: int = 200,
):
    data = dict(context)
    if "request" not in data:
        raise ValueError("template context must include 'request'")
    data.setdefault("client_settings_version", asset_version())
    response = templates.TemplateResponse(template_name, data, status_code=status_code)
    response.headers["X-Asset-Version"] = data["client_settings_version"]
    return response
