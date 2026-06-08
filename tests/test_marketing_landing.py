from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_marketing_landing_routes_use_avio_templates():
    auth_source = (ROOT / "apps/api/web/auth.py").read_text(encoding="utf-8")

    assert 'render_template("marketing/home_lovable.html", context)' in auth_source
    assert "title = \"Avio - автоответчик для авито\"" in auth_source
    assert 'RedirectResponse(url="/avtootvetchik-avito", status_code=301)' in auth_source

    assert "Financeavio" not in auth_source
    assert "Finance Avio" not in auth_source


def test_marketing_templates_do_not_reference_finance_copy():
    files = [
        ROOT / "apps/api/templates/marketing/home.html",
        ROOT / "apps/api/templates/marketing/home_lovable.html",
        ROOT / "apps/api/templates/marketing/features.html",
        ROOT / "apps/api/templates/marketing/solutions.html",
        ROOT / "apps/api/templates/marketing/pricing.html",
        ROOT / "apps/api/templates/marketing/faq.html",
        ROOT / "apps/api/templates/marketing/blog.html",
        ROOT / "apps/api/templates/marketing/avtootvetchik-avito.html",
        ROOT / "apps/api/static/landing/landing.css",
        ROOT / "apps/api/static/landing/landing.js",
    ]

    for path in files:
        text = path.read_text(encoding="utf-8")
        assert "Financeavio" not in text, path
        assert "Finance Avio" not in text, path


def test_lovable_template_contains_target_landing_copy():
    template_text = (ROOT / "apps/api/templates/marketing/home_lovable.html").read_text(
        encoding="utf-8"
    )
    bundle_path = max(
        (ROOT / "apps/api/static/landing/lovable/assets").glob("index-*.js"),
        key=lambda path: path.stat().st_mtime,
    )
    bundle_text = bundle_path.read_text(encoding="utf-8")

    assert "landing/lovable/assets" in template_text
    assert "Автоответчик" in bundle_text
    assert "для вашего бизнеса" in bundle_text
    assert "Войти" in bundle_text
    assert "Зарегистрироваться" in bundle_text
    assert "Запустить пилот" in bundle_text
    assert "White Label" in bundle_text
    assert "Что меняется после запуска" in bundle_text
    assert "Кейсы" in bundle_text
    assert "Почему Avio" in bundle_text
    assert "Главная точка входа" not in bundle_text
    assert "Revenue Control Layer" not in bundle_text
    assert "Financeavio" not in bundle_text
    assert "Finance Avio" not in bundle_text
