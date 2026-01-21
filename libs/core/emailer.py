from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage

logger = logging.getLogger("app.email")


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _smtp_configured() -> bool:
    return bool(_env("SMTP_HOST"))


def send_email(to: str, subject: str, html: str, text: str | None = None) -> None:
    if not to:
        return
    if not _smtp_configured():
        logger.info("email_console to=%s subject=%s", to, subject)
        return

    host = _env("SMTP_HOST")
    port = int(_env("SMTP_PORT", "587"))
    user = _env("SMTP_USER")
    password = _env("SMTP_PASS")
    from_addr = _env("SMTP_FROM") or user or "no-reply@avio.local"
    use_ssl = (_env("SMTP_SSL", "").lower() in {"1", "true", "yes"}) or port == 465
    use_tls = (_env("SMTP_TLS", "1").lower() in {"1", "true", "yes"}) and not use_ssl

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to
    msg["Subject"] = subject
    if text:
        msg.set_content(text)
        msg.add_alternative(html, subtype="html")
    else:
        msg.set_content(html, subtype="html")

    if use_ssl:
        server = smtplib.SMTP_SSL(host, port, timeout=10)
    else:
        server = smtplib.SMTP(host, port, timeout=10)

    try:
        if use_tls:
            server.starttls()
        if user and password:
            server.login(user, password)
        server.send_message(msg)
    finally:
        try:
            server.quit()
        except Exception:
            pass


__all__ = ["send_email"]
