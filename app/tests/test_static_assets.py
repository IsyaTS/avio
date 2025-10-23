from app.web.ui import templates


def test_static_url_defaults_to_static_prefix(monkeypatch):
    monkeypatch.delenv("STATIC_PUBLIC_BASE", raising=False)
    static_url = templates.env.globals["static_url"]
    assert static_url(None, "css/portal.css") == "/static/css/portal.css"


def test_static_url_uses_env_prefix_without_double_slash(monkeypatch):
    monkeypatch.setenv("STATIC_PUBLIC_BASE", "https://static.avio.website/static/")
    static_url = templates.env.globals["static_url"]
    assert (
        static_url(None, "/css/portal.css")
        == "https://static.avio.website/static/css/portal.css"
    )


def test_static_url_handles_empty_path(monkeypatch):
    monkeypatch.setenv("STATIC_PUBLIC_BASE", "https://cdn.example/static")
    static_url = templates.env.globals["static_url"]
    assert static_url(None, "") == "https://cdn.example/static"
