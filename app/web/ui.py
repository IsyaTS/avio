from __future__ import annotations

import os
import importlib
import pathlib
from datetime import datetime
from fastapi.templating import Jinja2Templates

ROOT = pathlib.Path(__file__).resolve().parent.parent
TEMPLATES_DIR = ROOT / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.auto_reload = True
templates.env.cache = {}

try:
    from core import settings  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - fallback when running via package alias
    core_module = importlib.import_module("app.core")
    settings = core_module.settings  # type: ignore[assignment]


def _datetimeformat(value):
    try:
        if not value:
            return "—"
        return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(value)


def _static_url(request, path: str) -> str:
    """Resolve static asset URL even if the /static mount is unavailable."""

    cleaned = str(path).lstrip("/")
    if request is not None:
        app = getattr(request, "app", None)
        if app is not None:
            try:
                return app.url_path_for("static", path=cleaned)
            except Exception:
                pass
        try:
            return request.url_for("static", path=cleaned)
        except Exception:
            pass

    base = getattr(settings, "STATIC_BASE_URL", None)
    if not base:
        base = os.getenv("STATIC_BASE_URL", "/static")
    base = (base or "/static").rstrip("/")
    return f"{base}/{cleaned}"


templates.env.filters["datetimeformat"] = _datetimeformat
templates.env.globals["static_url"] = _static_url
