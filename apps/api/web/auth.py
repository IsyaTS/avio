from __future__ import annotations

import os
import logging
import re
from html import escape
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from libs.core import emailer, common as core_common
from libs.core.repo import auth as auth_repo
from libs.core import db as db_module
from . import common as C
from .ui import render_template
from . import auth_utils

router = APIRouter()
_log = logging.getLogger("app.web.auth")
_NOTIFY_BOT_TOKEN = (os.getenv("NOTIFY_BOT_TOKEN") or "").strip()
_NOTIFY_BOT_PARSE_MODE = (os.getenv("NOTIFY_BOT_PARSE_MODE") or "HTML").strip()


def _render_with_csrf(request: Request, template: str, context: dict, status_code: int = 200) -> Response:
    token = auth_utils.csrf_token_from_request(request)
    context = dict(context)
    context["csrf_token"] = token
    response = render_template(template, context, status_code=status_code)
    response.set_cookie(
        auth_utils.csrf_cookie_name(),
        token,
        **auth_utils.cookie_params(
            request,
            ttl_seconds=auth_utils.csrf_ttl_hours() * 3600,
            httponly=True,
        ),
    )
    return response


def _session_redirect_path(request: Request, tenant_id: int, next_value: str | None = None) -> str:
    next_candidate = auth_utils.safe_redirect_path(next_value or request.query_params.get("next"))
    if next_candidate:
        return next_candidate
    return "/client/settings"


def _set_session_cookies(request: Request, response: Response, session_id: str, client_key: str | None) -> None:
    ttl_seconds = auth_utils.session_ttl_days() * 86400
    response.set_cookie(
        auth_utils.session_cookie_name(),
        session_id,
        **auth_utils.cookie_params(request, ttl_seconds=ttl_seconds, httponly=True),
    )
    if client_key:
        response.set_cookie(
            "client_key",
            client_key,
            **auth_utils.cookie_params(request, ttl_seconds=ttl_seconds, httponly=True),
        )


def _auth_disabled() -> Response:
    return Response(status_code=404)


def _base_context(request: Request, title: str) -> dict:
    return {
        "request": request,
        "title": title,
        "show_auth_links": auth_utils.auth_enabled(),
    }


def _register_context(
    request: Request,
    title: str,
    *,
    email: str = "",
    phone: str = "",
    contact: str = "",
    messenger: str = "",
    error: str | None = None,
) -> dict:
    context = _base_context(request, title)
    context.update(
        {
            "email": email,
            "phone": phone,
            "contact": contact,
            "messenger": messenger,
        }
    )
    if error:
        context["error"] = error
    return context


def _email_verify_link(request: Request, token: str) -> str:
    base = auth_utils.build_email_base_url(request)
    if not base:
        base = ""
    query = urlencode({"token": token})
    return f"{base}/auth/verify?{query}"


async def _notify_registration(
    tenant_id: int,
    *,
    email: str,
    phone: str,
    contact: str,
    messenger: str,
) -> None:
    if not _NOTIFY_BOT_TOKEN:
        _log.info("event=registration_notify_skip reason=missing_token tenant=%s", tenant_id)
        return
    chat_ids = core_common.notification_chat_ids(tenant_id, "registration")
    if not chat_ids:
        _log.info("event=registration_notify_skip reason=no_chat_ids tenant=%s", tenant_id)
        return
    url = f"https://api.telegram.org/bot{_NOTIFY_BOT_TOKEN}/sendMessage"
    text = (
        "Новая регистрация\n"
        f"Tenant: {tenant_id}\n"
        f"Email: {escape(email)}\n"
        f"Телефон: {escape(phone)}\n"
        f"Контакт: {escape(contact)}\n"
        f"Мессенджер: {escape(messenger)}"
    )
    payload_base = {
        "text": text,
        "parse_mode": _NOTIFY_BOT_PARSE_MODE or "HTML",
        "disable_web_page_preview": True,
    }
    async with httpx.AsyncClient(timeout=8.0) as client:
        for chat_id in chat_ids:
            payload = dict(payload_base)
            payload["chat_id"] = int(chat_id)
            try:
                resp = await client.post(url, json=payload)
            except Exception as exc:
                _log.warning(
                    "event=registration_notify_failed tenant=%s chat_id=%s error=%s",
                    tenant_id,
                    chat_id,
                    exc,
                )
                continue
            if resp.status_code >= 300:
                _log.warning(
                    "event=registration_notify_failed tenant=%s chat_id=%s status=%s body=%s",
                    tenant_id,
                    chat_id,
                    resp.status_code,
                    resp.text,
                )


def _email_reset_link(request: Request, token: str) -> str:
    base = auth_utils.build_email_base_url(request)
    if not base:
        base = ""
    query = urlencode({"token": token})
    return f"{base}/reset?{query}"


def _send_verify_email(to_email: str, verify_url: str) -> None:
    subject = "Подтверждение регистрации в Avio"
    html = (
        f"<p>Подтвердите регистрацию в Avio.</p>"
        f"<p><a href=\"{verify_url}\">Подтвердить email</a></p>"
        f"<p>Если это были не вы — просто проигнорируйте письмо.</p>"
    )
    text = f"Подтвердите регистрацию: {verify_url}"
    emailer.send_email(to_email, subject, html, text=text)


def _send_reset_email(to_email: str, reset_url: str) -> None:
    subject = "Сброс пароля Avio"
    html = (
        f"<p>Для сброса пароля откройте ссылку:</p>"
        f"<p><a href=\"{reset_url}\">Сбросить пароль</a></p>"
        f"<p>Если это были не вы — проигнорируйте письмо.</p>"
    )
    text = f"Сброс пароля: {reset_url}"
    emailer.send_email(to_email, subject, html, text=text)


@router.get("/")
async def landing(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    context = _base_context(request, "Avio — умные продажи в мессенджерах")
    context["show_auth_links"] = auth_utils.auth_enabled()
    return render_template("marketing/home.html", context)


@router.get("/login")
async def login_form(request: Request):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    user = await auth_utils.get_current_user(request)
    if user:
        return RedirectResponse(url=_session_redirect_path(request, int(user["tenant_id"])))
    context = _base_context(request, "Вход · Avio")
    context["next"] = auth_utils.safe_redirect_path(request.query_params.get("next"))
    return _render_with_csrf(request, "auth/login.html", context)


@router.post("/auth/login")
async def login_submit(request: Request, background_tasks: BackgroundTasks):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    form = await request.form()
    if not auth_utils.verify_csrf(request, form.get("csrf_token")):
        return JSONResponse({"detail": "csrf_failed"}, status_code=400)

    email = auth_utils.normalize_email(form.get("email") or "")
    if not email or "@" not in email:
        context = _base_context(request, "Сброс пароля · Avio")
        context["error"] = "Введите корректный email."
        return _render_with_csrf(request, "auth/forgot.html", context, status_code=400)
    password = str(form.get("password") or "")
    if not email or "@" not in email or not password:
        context = _base_context(request, "Вход · Avio")
        context["error"] = "Введите корректный email и пароль."
        return _render_with_csrf(request, "auth/login.html", context, status_code=400)

    allowed, retry_after = auth_utils.rate_limit_check(
        action="login",
        email=email,
        request=request,
    )
    if not allowed:
        return JSONResponse(
            {"detail": "rate_limited", "retry_after": retry_after},
            status_code=429,
        )

    try:
        user = await auth_repo.get_user_by_email(email)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if not user:
        auth_utils.verify_password("dummy-password", auth_utils.hash_password("dummy"))
        context = _base_context(request, "Вход · Avio")
        context["error"] = "Неверный email или пароль."
        return _render_with_csrf(request, "auth/login.html", context, status_code=401)

    if not auth_utils.verify_password(password, user.get("password_hash") or ""):
        context = _base_context(request, "Вход · Avio")
        context["error"] = "Неверный email или пароль."
        return _render_with_csrf(request, "auth/login.html", context, status_code=401)

    if not user.get("is_verified"):
        resend_allowed, _ = auth_utils.rate_limit_check(
            action="resend_login",
            email=email,
            request=request,
            limit=2,
            window_seconds=900,
        )
        if resend_allowed:
            token_raw = auth_utils.new_token()
            try:
                await auth_repo.create_token(
                    int(user["id"]),
                    auth_utils.hash_token(token_raw),
                    "verify",
                    datetime.now(timezone.utc) + timedelta(hours=24),
                    request_ip=auth_utils.request_ip(request),
                )
            except db_module.DatabaseUnavailableError:
                return JSONResponse({"detail": "db_unavailable"}, status_code=503)
            verify_url = _email_verify_link(request, token_raw)
            background_tasks.add_task(_send_verify_email, email, verify_url)
        context = _base_context(request, "Вход · Avio")
        context["error"] = "Подтвердите email, мы отправили ссылку."
        context["email"] = email
        return _render_with_csrf(request, "auth/login.html", context, status_code=403)

    session_id = auth_utils.new_session_id()
    session_hash = auth_utils.hash_token(session_id)
    expires_at = auth_utils.session_expiry()
    ip = auth_utils.request_ip(request)
    user_agent = request.headers.get("user-agent", "")
    try:
        await auth_repo.create_session(
            int(user["id"]), session_hash, expires_at, ip=ip, user_agent=user_agent
        )
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    await auth_repo.update_last_login(int(user["id"]))

    tenant_id = int(user["tenant_id"])
    client_key = (C.get_tenant_pubkey(tenant_id) or "").strip()
    response = RedirectResponse(
        url=_session_redirect_path(request, tenant_id, form.get("next")),
        status_code=303,
    )
    _set_session_cookies(request, response, session_id, client_key or None)
    return response


@router.post("/auth/logout")
async def logout(request: Request):
    cookie_name = auth_utils.session_cookie_name()
    session_id = (request.cookies.get(cookie_name) or "").strip()
    if session_id:
        await auth_repo.revoke_session(auth_utils.hash_token(session_id))
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(cookie_name, path="/")
    response.delete_cookie("client_key", path="/")
    return response


@router.get("/register")
async def register_form(request: Request):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    context = _base_context(request, "Регистрация · Avio")
    return _render_with_csrf(request, "auth/register.html", context)


@router.post("/auth/register")
async def register_submit(request: Request, background_tasks: BackgroundTasks):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    form = await request.form()
    if not auth_utils.verify_csrf(request, form.get("csrf_token")):
        return JSONResponse({"detail": "csrf_failed"}, status_code=400)

    email = auth_utils.normalize_email(form.get("email") or "")
    phone = str(form.get("phone") or "").strip()
    contact = str(form.get("contact") or "").strip()
    messenger = str(form.get("messenger") or "").strip()
    password = str(form.get("password") or "")
    confirm = str(form.get("confirm_password") or "")

    allowed, retry_after = auth_utils.rate_limit_check(
        action="register",
        email=email,
        request=request,
        limit=3,
        window_seconds=900,
    )
    if not allowed:
        return JSONResponse(
            {"detail": "rate_limited", "retry_after": retry_after},
            status_code=429,
        )

    if not email or "@" not in email:
        context = _register_context(
            request,
            "Регистрация · Avio",
            email=email,
            phone=phone,
            contact=contact,
            messenger=messenger,
            error="Введите корректный email.",
        )
        return _render_with_csrf(request, "auth/register.html", context, status_code=400)

    digits = re.sub(r"\D+", "", phone)
    if len(digits) < 5:
        context = _register_context(
            request,
            "Регистрация · Avio",
            email=email,
            phone=phone,
            contact=contact,
            messenger=messenger,
            error="Введите номер телефона.",
        )
        return _render_with_csrf(request, "auth/register.html", context, status_code=400)

    if not contact:
        context = _register_context(
            request,
            "Регистрация · Avio",
            email=email,
            phone=phone,
            contact=contact,
            messenger=messenger,
            error="Укажите контакт для связи.",
        )
        return _render_with_csrf(request, "auth/register.html", context, status_code=400)

    if not messenger:
        context = _register_context(
            request,
            "Регистрация · Avio",
            email=email,
            phone=phone,
            contact=contact,
            messenger=messenger,
            error="Выберите удобный мессенджер.",
        )
        return _render_with_csrf(request, "auth/register.html", context, status_code=400)

    ok, message = auth_utils.password_ok(password)
    if not ok or password != confirm:
        context = _register_context(
            request,
            "Регистрация · Avio",
            email=email,
            phone=phone,
            contact=contact,
            messenger=messenger,
            error=message or "Пароли не совпадают.",
        )
        return _render_with_csrf(request, "auth/register.html", context, status_code=400)

    try:
        existing = await auth_repo.get_user_by_email(email)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if existing:
        if not existing.get("is_verified"):
            token_raw = auth_utils.new_token()
            try:
                await auth_repo.create_token(
                    int(existing["id"]),
                    auth_utils.hash_token(token_raw),
                    "verify",
                    datetime.now(timezone.utc) + timedelta(hours=24),
                    request_ip=auth_utils.request_ip(request),
                )
            except db_module.DatabaseUnavailableError:
                return JSONResponse({"detail": "db_unavailable"}, status_code=503)
            verify_url = _email_verify_link(request, token_raw)
            background_tasks.add_task(_send_verify_email, email, verify_url)
        context = _base_context(request, "Проверьте почту · Avio")
        context["message"] = "Если email зарегистрирован, мы отправили письмо."
        return render_template("auth/message.html", context)

    try:
        tenant_id = await auth_repo.create_tenant()
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    C.ensure_tenant_files(tenant_id)
    key = os.urandom(16).hex()
    C.add_key(tenant_id, key, email)
    C.set_primary(tenant_id, key)

    try:
        cfg = C.read_tenant_config(tenant_id)
    except Exception:
        cfg = {}
    passport = dict(cfg.get("passport") or {})
    passport["phone"] = phone
    passport["contact"] = contact
    passport["preferred_messenger"] = messenger
    cfg["passport"] = passport
    try:
        C.write_tenant_config(tenant_id, cfg)
    except Exception:
        pass

    password_hash = auth_utils.hash_password(password)
    try:
        user = await auth_repo.create_user(
            email,
            password_hash,
            tenant_id,
            contact=contact,
            preferred_messenger=messenger,
        )
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if not user:
        await auth_repo.delete_tenant(tenant_id)
        context = _register_context(
            request,
            "Регистрация · Avio",
            email=email,
            phone=phone,
            contact=contact,
            messenger=messenger,
            error="Не удалось создать пользователя. Попробуйте снова.",
        )
        return _render_with_csrf(request, "auth/register.html", context, status_code=500)

    token_raw = auth_utils.new_token()
    try:
        await auth_repo.create_token(
            int(user["id"]),
            auth_utils.hash_token(token_raw),
            "verify",
            datetime.now(timezone.utc) + timedelta(hours=24),
            request_ip=auth_utils.request_ip(request),
        )
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    verify_url = _email_verify_link(request, token_raw)
    background_tasks.add_task(_send_verify_email, email, verify_url)

    context = _base_context(request, "Проверьте почту · Avio")
    context["message"] = "Мы отправили ссылку для подтверждения на ваш email."
    return render_template("auth/message.html", context)


@router.post("/auth/resend-verify")
async def resend_verify(request: Request, background_tasks: BackgroundTasks):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    form = await request.form()
    if not auth_utils.verify_csrf(request, form.get("csrf_token")):
        return JSONResponse({"detail": "csrf_failed"}, status_code=400)

    email = auth_utils.normalize_email(form.get("email") or "")
    allowed, retry_after = auth_utils.rate_limit_check(
        action="resend",
        email=email,
        request=request,
        limit=2,
        window_seconds=900,
    )
    if not allowed:
        return JSONResponse({"detail": "rate_limited", "retry_after": retry_after}, status_code=429)

    try:
        user = await auth_repo.get_user_by_email(email)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if user and not user.get("is_verified"):
        token_raw = auth_utils.new_token()
        try:
            await auth_repo.create_token(
                int(user["id"]),
                auth_utils.hash_token(token_raw),
                "verify",
                datetime.now(timezone.utc) + timedelta(hours=24),
                request_ip=auth_utils.request_ip(request),
            )
        except db_module.DatabaseUnavailableError:
            return JSONResponse({"detail": "db_unavailable"}, status_code=503)
        verify_url = _email_verify_link(request, token_raw)
        background_tasks.add_task(_send_verify_email, email, verify_url)

    context = _base_context(request, "Проверьте почту · Avio")
    context["message"] = "Если email зарегистрирован, мы отправили письмо."
    return render_template("auth/message.html", context)


@router.get("/auth/verify")
async def verify_email(request: Request, token: str | None = None, background_tasks: BackgroundTasks | None = None):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    if not token:
        context = _base_context(request, "Ошибка подтверждения · Avio")
        context["message"] = "Ссылка недействительна или устарела."
        return render_template("auth/message.html", context, status_code=400)

    token_hash = auth_utils.hash_token(token)
    try:
        record = await auth_repo.get_token(token_hash, "verify")
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if not record:
        context = _base_context(request, "Ошибка подтверждения · Avio")
        context["message"] = "Ссылка недействительна или устарела."
        return render_template("auth/message.html", context, status_code=400)

    expires_at = record.get("expires_at")
    used_at = record.get("used_at")
    if used_at or (isinstance(expires_at, datetime) and expires_at <= datetime.now(timezone.utc)):
        context = _base_context(request, "Ошибка подтверждения · Avio")
        context["message"] = "Ссылка недействительна или устарела."
        return render_template("auth/message.html", context, status_code=400)

    user_id = int(record["user_id"])
    try:
        user = await auth_repo.get_user_by_id(user_id)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if not user:
        context = _base_context(request, "Ошибка подтверждения · Avio")
        context["message"] = "Пользователь не найден."
        return render_template("auth/message.html", context, status_code=400)

    await auth_repo.mark_token_used(int(record["id"]))
    await auth_repo.set_user_verified(user_id)

    session_id = auth_utils.new_session_id()
    session_hash = auth_utils.hash_token(session_id)
    expires_at = auth_utils.session_expiry()
    ip = auth_utils.request_ip(request)
    user_agent = request.headers.get("user-agent", "")
    try:
        await auth_repo.create_session(
            user_id, session_hash, expires_at, ip=ip, user_agent=user_agent
        )
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    await auth_repo.update_last_login(user_id)

    tenant_id = int(user["tenant_id"])
    try:
        cfg = C.read_tenant_config(tenant_id)
    except Exception:
        cfg = {}
    passport = cfg.get("passport") or {}
    phone = str(passport.get("phone") or "").strip()
    contact = str(user.get("contact") or "").strip()
    messenger = str(user.get("preferred_messenger") or "").strip()

    if background_tasks is not None:
        background_tasks.add_task(
            _notify_registration,
            tenant_id,
            email=str(user.get("email") or ""),
            phone=phone,
            contact=contact,
            messenger=messenger,
        )
    client_key = (C.get_tenant_pubkey(tenant_id) or "").strip()
    response = RedirectResponse(url=_session_redirect_path(request, tenant_id), status_code=303)
    if background_tasks is not None:
        response.background = background_tasks
    _set_session_cookies(request, response, session_id, client_key or None)
    return response


@router.get("/forgot")
async def forgot_form(request: Request):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    context = _base_context(request, "Сброс пароля · Avio")
    return _render_with_csrf(request, "auth/forgot.html", context)


@router.post("/auth/forgot")
async def forgot_submit(request: Request, background_tasks: BackgroundTasks):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    form = await request.form()
    if not auth_utils.verify_csrf(request, form.get("csrf_token")):
        return JSONResponse({"detail": "csrf_failed"}, status_code=400)

    email = auth_utils.normalize_email(form.get("email") or "")
    allowed, retry_after = auth_utils.rate_limit_check(
        action="forgot",
        email=email,
        request=request,
        limit=3,
        window_seconds=900,
    )
    if not allowed:
        return JSONResponse({"detail": "rate_limited", "retry_after": retry_after}, status_code=429)

    try:
        user = await auth_repo.get_user_by_email(email)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if user and user.get("is_verified"):
        token_raw = auth_utils.new_token()
        try:
            await auth_repo.create_token(
                int(user["id"]),
                auth_utils.hash_token(token_raw),
                "reset",
                datetime.now(timezone.utc) + timedelta(hours=2),
                request_ip=auth_utils.request_ip(request),
            )
        except db_module.DatabaseUnavailableError:
            return JSONResponse({"detail": "db_unavailable"}, status_code=503)
        reset_url = _email_reset_link(request, token_raw)
        background_tasks.add_task(_send_reset_email, email, reset_url)

    context = _base_context(request, "Проверьте почту · Avio")
    context["message"] = "Если email зарегистрирован, мы отправили письмо."
    return render_template("auth/message.html", context)


@router.get("/reset")
async def reset_form(request: Request, token: str | None = None):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    context = _base_context(request, "Новый пароль · Avio")
    context["token"] = token or ""
    return _render_with_csrf(request, "auth/reset.html", context)


@router.post("/auth/reset")
async def reset_submit(request: Request):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    form = await request.form()
    if not auth_utils.verify_csrf(request, form.get("csrf_token")):
        return JSONResponse({"detail": "csrf_failed"}, status_code=400)

    token = (form.get("token") or "").strip()
    password = str(form.get("password") or "")
    confirm = str(form.get("confirm_password") or "")

    ok, message = auth_utils.password_ok(password)
    if not ok or password != confirm:
        context = _base_context(request, "Новый пароль · Avio")
        context["token"] = token
        context["error"] = message or "Пароли не совпадают."
        return _render_with_csrf(request, "auth/reset.html", context, status_code=400)

    token_hash = auth_utils.hash_token(token)
    try:
        record = await auth_repo.get_token(token_hash, "reset")
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if not record:
        context = _base_context(request, "Ошибка сброса · Avio")
        context["message"] = "Ссылка недействительна или устарела."
        return render_template("auth/message.html", context, status_code=400)

    expires_at = record.get("expires_at")
    used_at = record.get("used_at")
    if used_at or (isinstance(expires_at, datetime) and expires_at <= datetime.now(timezone.utc)):
        context = _base_context(request, "Ошибка сброса · Avio")
        context["message"] = "Ссылка недействительна или устарела."
        return render_template("auth/message.html", context, status_code=400)

    user_id = int(record["user_id"])
    await auth_repo.mark_token_used(int(record["id"]))
    await auth_repo.set_password(user_id, auth_utils.hash_password(password))
    await auth_repo.revoke_user_sessions(user_id)

    try:
        user = await auth_repo.get_user_by_id(user_id)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if not user:
        context = _base_context(request, "Ошибка сброса · Avio")
        context["message"] = "Пользователь не найден."
        return render_template("auth/message.html", context, status_code=400)

    session_id = auth_utils.new_session_id()
    session_hash = auth_utils.hash_token(session_id)
    expires_at = auth_utils.session_expiry()
    ip = auth_utils.request_ip(request)
    user_agent = request.headers.get("user-agent", "")
    try:
        await auth_repo.create_session(
            user_id, session_hash, expires_at, ip=ip, user_agent=user_agent
        )
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    await auth_repo.update_last_login(user_id)

    tenant_id = int(user["tenant_id"])
    client_key = (C.get_tenant_pubkey(tenant_id) or "").strip()
    response = RedirectResponse(url=_session_redirect_path(request, tenant_id), status_code=303)
    _set_session_cookies(request, response, session_id, client_key or None)
    return response


@router.get("/dashboard")
async def dashboard(request: Request):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    user = await auth_utils.get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return RedirectResponse(url="/client/settings")
