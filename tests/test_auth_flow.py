from __future__ import annotations

import re
from uuid import uuid4

from fastapi.testclient import TestClient


def _extract_token(html: str) -> str:
    match = re.search(r"token=([A-Za-z0-9_\-]+)", html)
    if not match:
        return ""
    return match.group(1)


def _build_client(monkeypatch):
    monkeypatch.setenv("ENABLE_EMAIL_AUTH", "1")
    monkeypatch.setenv("ENABLE_PUBLIC_LANDING", "1")
    from apps.api import main as app_main
    return TestClient(app_main.app)


def test_register_verify_flow(monkeypatch):
    sent = {}

    def _capture_email(to, subject, html, text=None):
        sent["to"] = to
        sent["subject"] = subject
        sent["html"] = html or ""

    monkeypatch.setenv("ENABLE_EMAIL_AUTH", "1")
    from apps.api.web import auth as auth_module

    monkeypatch.setattr(auth_module.emailer, "send_email", _capture_email)
    client = _build_client(monkeypatch)

    resp = client.get("/register")
    assert resp.status_code == 200
    csrf = client.cookies.get("avio_csrf")
    email = f"test-{uuid4().hex}@example.com"
    form = {
        "csrf_token": csrf,
        "email": email,
        "phone": "+79990001122",
        "contact": "@tester",
        "messenger": "telegram",
        "password": "Password123",
        "confirm_password": "Password123",
    }
    resp = client.post("/auth/register", data=form)
    assert resp.status_code == 200
    assert 'reachGoal", "register"' in resp.text
    assert "html" in sent
    token = _extract_token(sent["html"])
    assert token

    verify = client.get(f"/auth/verify?token={token}", follow_redirects=False)
    assert verify.status_code in (302, 303)
    location = verify.headers.get("location", "")
    assert location.startswith("/client/settings")

    settings = client.get("/client/settings", follow_redirects=False)
    assert settings.status_code == 200


def test_login_unverified(monkeypatch):
    sent = {}

    def _capture_email(to, subject, html, text=None):
        sent["html"] = html or ""

    monkeypatch.setenv("ENABLE_EMAIL_AUTH", "1")
    from apps.api.web import auth as auth_module

    monkeypatch.setattr(auth_module.emailer, "send_email", _capture_email)
    client = _build_client(monkeypatch)

    client.get("/register")
    csrf = client.cookies.get("avio_csrf")
    email = f"test-{uuid4().hex}@example.com"
    client.post(
        "/auth/register",
        data={
            "csrf_token": csrf,
            "email": email,
            "phone": "+79990001122",
            "contact": "@tester",
            "messenger": "telegram",
            "password": "Password123",
            "confirm_password": "Password123",
        },
    )

    client.get("/login")
    csrf = client.cookies.get("avio_csrf")
    login = client.post(
        "/auth/login",
        data={"csrf_token": csrf, "email": email, "password": "Password123"},
    )
    assert login.status_code == 403


def test_verify_invalid_token(monkeypatch):
    client = _build_client(monkeypatch)
    resp = client.get("/auth/verify?token=bad-token")
    assert resp.status_code == 400


def test_rate_limit_login(monkeypatch):
    monkeypatch.setenv("ENABLE_EMAIL_AUTH", "1")
    client = _build_client(monkeypatch)

    client.get("/login")
    csrf = client.cookies.get("avio_csrf")
    email = f"rate-{uuid4().hex}@example.com"
    for _ in range(6):
        resp = client.post(
            "/auth/login",
            data={"csrf_token": csrf, "email": email, "password": "WrongPass123"},
        )
    assert resp.status_code == 429


def test_client_settings_magic_link_still_works(monkeypatch):
    client = _build_client(monkeypatch)
    resp = client.get("/client/1/settings", params={"k": "test-public-key"})
    assert resp.status_code == 200


def test_landing_contact_requires_phone_or_telegram(monkeypatch):
    client = _build_client(monkeypatch)
    resp = client.post(
        "/api/landing/contact",
        json={"name": "Тест", "contact": "   ", "message": "Нужен пилот"},
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body.get("detail") == "contact_required"


def test_landing_contact_rejects_invalid_phone_length(monkeypatch):
    client = _build_client(monkeypatch)
    resp = client.post(
        "/api/landing/contact",
        json={"name": "Тест", "contact": "7999123456789", "message": "Нужен пилот"},
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body.get("detail") == "invalid_phone_length"


def test_landing_contact_sends_notification(monkeypatch):
    called = {}
    from apps.api.web import auth as auth_module

    async def _capture_notify(
        tenant_id: int,
        *,
        name: str,
        contact: str,
        message: str,
        source_ip: str,
        user_agent: str,
    ) -> None:
        called["tenant_id"] = tenant_id
        called["name"] = name
        called["contact"] = contact
        called["message"] = message
        called["source_ip"] = source_ip
        called["user_agent"] = user_agent

    monkeypatch.setattr(auth_module, "_notify_landing_contact", _capture_notify)
    client = _build_client(monkeypatch)

    resp = client.post(
        "/api/landing/contact",
        json={
            "name": "Иван",
            "contact": "@ivan_sales",
            "message": "Нужно подключить Avito и Telegram",
        },
    )
    assert resp.status_code == 200
    assert resp.json().get("ok") is True
    assert called.get("contact") == "@ivan_sales"
