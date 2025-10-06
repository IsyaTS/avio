from types import SimpleNamespace

from app.web.ui import templates


def _missing_url_for(name: str, **kwargs):  # pragma: no cover - helper
    raise KeyError(name)


def test_static_url_fallback_when_route_missing():
    dummy_request = SimpleNamespace(url_for=_missing_url_for)
    static_url = templates.env.globals["static_url"]
    result = static_url(dummy_request, "css/portal.css")
    assert result.endswith("/css/portal.css")
    assert result.startswith("/static")


def test_static_url_prefers_app_mount_when_present():
    class DummyApp:
        def url_path_for(self, name: str, **kwargs):
            assert name == "static"
            assert kwargs == {"path": "css/portal.css"}
            return "/app-static/css/portal.css"

    dummy_request = SimpleNamespace(app=DummyApp())
    static_url = templates.env.globals["static_url"]
    assert static_url(dummy_request, "css/portal.css") == "/app-static/css/portal.css"


def test_static_url_uses_request_when_available():
    def stub_url_for(name: str, **kwargs):
        assert name == "static"
        assert kwargs == {"path": "css/portal.css"}
        return "/mounted/static/css/portal.css"

    dummy_request = SimpleNamespace(url_for=stub_url_for)
    static_url = templates.env.globals["static_url"]
    assert static_url(dummy_request, "css/portal.css") == "/mounted/static/css/portal.css"
