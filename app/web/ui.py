from __future__ import annotations

import pathlib
from datetime import datetime
from fastapi.templating import Jinja2Templates

ROOT = pathlib.Path(__file__).resolve().parent.parent
TEMPLATES_DIR = ROOT / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.auto_reload = True
templates.env.cache = {}

from .common import client_settings_version, static_url


def _datetimeformat(value):
    try:
        if not value:
            return "—"
        return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(value)


templates.env.filters["datetimeformat"] = _datetimeformat
templates.env.globals["static_url"] = static_url
templates.env.globals.setdefault("client_settings_version", client_settings_version())
