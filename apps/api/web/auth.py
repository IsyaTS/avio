from __future__ import annotations

import os
import logging
import re
import json
import pathlib
from dataclasses import dataclass
from html import escape
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from typing import Any

import httpx
from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, FileResponse

from libs.core import emailer, common as core_common
from libs.core.repo import auth as auth_repo
from libs.core import db as db_module
from . import common as C
from .ui import render_template
from . import auth_utils
from .services import landing_contact_runtime

router = APIRouter()
_log = logging.getLogger("app.web.auth")
_NOTIFY_BOT_TOKEN = (os.getenv("NOTIFY_BOT_TOKEN") or "").strip()
_DEFAULT_META_DESCRIPTION = (
    "Автоответчик для Авито от Avio: отвечает за 5 секунд, отправляет каталог и фото, "
    "переводит клиента в Telegram и помогает доводить диалог до сделки."
)
_DEFAULT_TITLE = "Avio - автоответчик для авито"
_REGISTER_DESCRIPTION = "Создайте аккаунт Avio и запустите умные продажи в мессенджерах."
_LOGIN_DESCRIPTION = "Войдите в Avio, чтобы управлять каналами и диалогами."
_FORGOT_DESCRIPTION = "Восстановите доступ к Avio, если забыли пароль."
_RESET_DESCRIPTION = "Задайте новый пароль для аккаунта Avio."
_CANONICAL_BASE = "https://avio.website"
try:
    _LANDING_CONTACT_NOTIFY_TENANT_ID = int(
        (os.getenv("LANDING_CONTACT_NOTIFY_TENANT_ID") or "1").strip()
    )
except Exception:
    _LANDING_CONTACT_NOTIFY_TENANT_ID = 1
_FAQ_ITEMS = [
    {
        "question": "Сколько времени занимает подключение?",
        "answer": "От 5 до 15 минут: подключаем канал, загружаем каталог и запускаем сценарии.",
    },
    {
        "question": "Можно ли отключать автоответы для конкретного чата?",
        "answer": "Да, менеджер может отключить автоответы в диалоге и вести клиента вручную.",
    },
    {
        "question": "Что бот отправляет в чате?",
        "answer": "Только то, что вы настроите: тексты, каталоги, фото и автоматические напоминания.",
    },
    {
        "question": "Подходит ли Avio для Avito?",
        "answer": "Да, сценарии заточены под захват контакта и перевод в удобный канал.",
    },
    {
        "question": "Работает ли Avio в Telegram?",
        "answer": "Да, поддерживаем диалоги, файлы и фото прямо в Telegram.",
    },
    {
        "question": "Можно ли задать разные персоны?",
        "answer": "Да, стиль общения отдельно для Avito и Telegram.",
    },
    {
        "question": "Нужно ли обучать команду?",
        "answer": "Нет, интерфейс похож на обычный мессенджер.",
    },
    {
        "question": "Как контролировать качество ответов?",
        "answer": "В кабинете видна история сообщений, статусы и тишина по каждому чату.",
    },
    {
        "question": "Можно ли отключить автоответы вручную?",
        "answer": "Да, одним кликом менеджер переводит чат в ручной режим.",
    },
    {
        "question": "Есть ли интеграции с CRM?",
        "answer": "По запросу подключаем CRM и дополнительные интеграции.",
    },
    {
        "question": "Сколько стоит внедрение?",
        "answer": "Стоимость зависит от тарифа и объёма. Базовый запуск уже включён.",
    },
    {
        "question": "Можно ли отправлять каталоги и фото?",
        "answer": "Да, контент отправляется автоматически по условиям.",
    },
    {
        "question": "Какая скорость ответа у бота?",
        "answer": "Ответ в среднем занимает около 5 секунд.",
    },
    {
        "question": "Есть ли доступ к статистике?",
        "answer": "Да, отображаются метрики диалогов и эффективность сценариев.",
    },
]


@dataclass(frozen=True)
class RegistrationForm:
    email: str
    phone: str
    contact: str
    messenger: str
    password: str
    confirm: str

    @property
    def digits(self) -> str:
        return re.sub(r"\D+", "", self.phone)


def _canonical_url(request: Request, path: str | None = None) -> str:
    base = _CANONICAL_BASE.rstrip("/")
    if not path:
        path = getattr(getattr(request, "url", None), "path", "") or "/"
    if not path.startswith("/"):
        path = f"/{path}"
    if not base:
        return path
    if path == "/":
        return f"{base}/"
    return f"{base}{path}"


def _public_static(request: Request, path: str) -> str:
    return f"{_CANONICAL_BASE}{C.static_url(request, path)}"


def _org_schema(request: Request) -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": "Avio",
        "url": _CANONICAL_BASE,
        "logo": _public_static(request, "branding/favicon.png"),
    }


def _website_schema() -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": "Avio",
        "url": _CANONICAL_BASE,
    }


def _software_schema(
    request: Request,
    *,
    page_url: str,
    page_title: str,
    description: str,
) -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "SoftwareApplication",
        "name": "Avio",
        "applicationCategory": "BusinessApplication",
        "operatingSystem": "Web",
        "url": page_url,
        "description": description,
        "brand": {"@type": "Organization", "name": "Avio"},
        "publisher": {"@type": "Organization", "name": "Avio"},
        "image": _public_static(request, "branding/favicon.png"),
        "headline": page_title,
    }


def _faq_schema(request: Request, items: list[dict[str, str]]) -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {
                "@type": "Question",
                "name": item["question"],
                "acceptedAnswer": {"@type": "Answer", "text": item["answer"]},
            }
            for item in items
        ],
    }


def _landing_lastmod() -> str | None:
    try:
        base = pathlib.Path(__file__).resolve().parents[1]
        target = base / "templates" / "marketing" / "home.html"
        ts = target.stat().st_mtime
    except Exception:
        return None
    return datetime.utcfromtimestamp(ts).date().isoformat()


def _template_lastmod(template_name: str) -> str | None:
    try:
        base = pathlib.Path(__file__).resolve().parents[1]
        target = base / "templates" / "marketing" / template_name
        ts = target.stat().st_mtime
    except Exception:
        return None
    return datetime.utcfromtimestamp(ts).date().isoformat()


def _lovable_bundle_assets() -> tuple[str | None, str | None]:
    try:
        base = pathlib.Path(__file__).resolve().parents[1]
        assets_dir = base / "static" / "landing" / "lovable" / "assets"
        css_asset = max(assets_dir.glob("index-*.css"), key=lambda p: p.stat().st_mtime)
        js_asset = max(assets_dir.glob("index-*.js"), key=lambda p: p.stat().st_mtime)
    except Exception:
        return None, None
    return (
        f"landing/lovable/assets/{css_asset.name}",
        f"landing/lovable/assets/{js_asset.name}",
    )


def _breadcrumb_schema(items: list[tuple[str, str]]) -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {
                "@type": "ListItem",
                "position": idx + 1,
                "name": title,
                "item": url,
            }
            for idx, (title, url) in enumerate(items)
        ],
    }


def _blog_schema(
    request: Request,
    *,
    title: str,
    description: str,
    path: str,
) -> dict:
    url = _canonical_url(request, path)
    logo = _public_static(request, "branding/favicon.png")
    return {
        "@context": "https://schema.org",
        "@type": "BlogPosting",
        "headline": title,
        "description": description,
        "url": url,
        "image": logo,
        "author": {"@type": "Organization", "name": "Avio"},
        "publisher": {
            "@type": "Organization",
            "name": "Avio",
            "logo": {"@type": "ImageObject", "url": logo},
        },
    }


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


def _base_context(
    request: Request,
    title: str,
    *,
    description: str | None = None,
    og_type: str = "website",
    structured_data: list[dict] | None = None,
) -> dict:
    canonical_url = _canonical_url(request)
    meta_description = description or _DEFAULT_META_DESCRIPTION
    og_title = title or _DEFAULT_TITLE
    og_description = meta_description
    og_image = _public_static(request, "branding/favicon.png")
    structured_items = list(structured_data) if structured_data else []
    structured_items.insert(0, _website_schema())
    structured_items.insert(0, _org_schema(request))
    return {
        "request": request,
        "title": title or _DEFAULT_TITLE,
        "show_auth_links": auth_utils.auth_enabled(),
        "canonical_url": canonical_url,
        "meta_description": meta_description,
        "og_title": og_title,
        "og_description": og_description,
        "og_url": canonical_url,
        "og_type": og_type,
        "og_image": og_image,
        "structured_data": [json.dumps(item, ensure_ascii=False) for item in structured_items],
    }


def _register_context(
    request: Request,
    title: str,
    *,
    email: str = "",
    phone: str = "",
    contact: str = "",
    messenger: str = "",
    description: str | None = None,
    error: str | None = None,
) -> dict:
    context = _base_context(request, title, description=description)
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


async def _notify_landing_contact(
    tenant_id: int,
    *,
    name: str,
    contact: str,
    message: str,
    source_ip: str,
    user_agent: str,
) -> None:
    if not _NOTIFY_BOT_TOKEN:
        _log.info("event=landing_contact_notify_skip reason=missing_token tenant=%s", tenant_id)
        return
    chat_ids = core_common.notification_chat_ids(tenant_id, "landing_contact")
    if not chat_ids:
        chat_ids = core_common.notification_chat_ids(tenant_id, "registration")
    if not chat_ids:
        _log.info("event=landing_contact_notify_skip reason=no_chat_ids tenant=%s", tenant_id)
        return
    url = f"https://api.telegram.org/bot{_NOTIFY_BOT_TOKEN}/sendMessage"
    text = (
        "Новая заявка с лендинга\n"
        f"Tenant: {tenant_id}\n"
        f"Имя: {escape(name or '—')}\n"
        f"Контакт: {escape(contact)}\n"
        f"Задача: {escape(message or '—')}\n"
        f"IP: {escape(source_ip or '—')}\n"
        f"UA: {escape(user_agent or '—')}"
    )
    payload_base = {
        "text": text,
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
                    "event=landing_contact_notify_failed tenant=%s chat_id=%s error=%s",
                    tenant_id,
                    chat_id,
                    exc,
                )
                continue
            if resp.status_code >= 300:
                _log.warning(
                    "event=landing_contact_notify_failed tenant=%s chat_id=%s status=%s body=%s",
                    tenant_id,
                    chat_id,
                    resp.status_code,
                    resp.text,
                )


def _normalize_landing_contact(raw_value: str) -> tuple[str, str | None]:
    return landing_contact_runtime.normalize_landing_contact(raw_value)


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


def _render_lovable_spa(
    request: Request,
    *,
    title: str,
    description: str,
    structured_data: list[dict] | None = None,
) -> Response:
    context = _base_context(
        request,
        title,
        description=description,
        structured_data=structured_data,
    )
    context["show_auth_links"] = auth_utils.auth_enabled()
    css_asset, js_asset = _lovable_bundle_assets()
    context["lovable_css_asset"] = css_asset
    context["lovable_js_asset"] = js_asset
    return render_template("marketing/home_lovable.html", context)


@router.get("/")
async def landing(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    title = "Avio - автоответчик для авито"
    description = _DEFAULT_META_DESCRIPTION
    page_url = _canonical_url(request, "/")
    return _render_lovable_spa(
        request,
        title=title,
        description=description,
        structured_data=[
            _software_schema(
                request,
                page_url=page_url,
                page_title=title,
                description=description,
            ),
            _faq_schema(request, _FAQ_ITEMS),
        ],
    )


def _render_marketing_page(
    request: Request,
    *,
    template: str,
    title: str,
    breadcrumb_title: str,
    description: str,
    path: str,
    extra_structured: list[dict] | None = None,
) -> Response:
    breadcrumbs = _breadcrumb_schema(
        [
            ("Главная", _canonical_url(request, "/")),
            (breadcrumb_title, _canonical_url(request, path)),
        ]
    )
    structured = list(extra_structured or [])
    structured.insert(0, breadcrumbs)
    context = _base_context(
        request,
        title,
        description=description,
        structured_data=structured,
    )
    context["show_auth_links"] = auth_utils.auth_enabled()
    return render_template(f"marketing/{template}", context)


@router.get("/features")
async def marketing_features(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    return _render_lovable_spa(
        request,
        title="Возможности · Avio",
        description="Ключевые сценарии Avio: автоответы, каталоги, контроль диалогов и умные подсказки.",
    )


@router.get("/solutions")
async def marketing_solutions(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    return _render_lovable_spa(
        request,
        title="Решения · Avio",
        description="Как Avio помогает бизнесам в Avito и Telegram закрывать сделки быстрее.",
    )


@router.get("/pricing")
async def marketing_pricing(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    return _render_lovable_spa(
        request,
        title="Тарифы · Avio",
        description="Прозрачные условия и быстрый старт с Avio.",
    )


@router.get("/autoresponder")
async def marketing_autoresponder(request: Request):
    return RedirectResponse(url="/avtootvetchik-avito", status_code=301)


@router.get("/avtootvetchik-avito")
async def marketing_avito_autoreply(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    title = "Автоответчик для Авито: ответ за 5 секунд + перевод в Telegram | Avio"
    description = (
        "Как работает автоответчик для Авито: быстрые ответы, квалификация лида, отправка "
        "каталога и фото, перевод в Telegram и контроль менеджера."
    )
    return _render_lovable_spa(
        request,
        title=title,
        description=description,
        structured_data=[
            _software_schema(
                request,
                page_url=_canonical_url(request, "/avtootvetchik-avito"),
                page_title=title,
                description=description,
            )
        ],
    )


@router.get("/faq")
async def marketing_faq(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    return _render_lovable_spa(
        request,
        title="FAQ · Avio",
        description="Ответы на частые вопросы о запуске Avio.",
        structured_data=[_faq_schema(request, _FAQ_ITEMS)],
    )


@router.get("/blog")
async def marketing_blog(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    return _render_lovable_spa(
        request,
        title="Блог · Avio",
        description="Практические материалы о продажах в мессенджерах.",
    )


@router.post("/api/landing/contact")
async def landing_contact_submit(request: Request):
    if not auth_utils.landing_enabled():
        return JSONResponse({"detail": "landing_disabled"}, status_code=404)
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    allowed, retry_after = auth_utils.rate_limit_check(
        action="landing_contact",
        email="landing@anonymous.local",
        request=request,
        limit=6,
        window_seconds=600,
    )
    if not allowed:
        return JSONResponse(
            {"detail": "rate_limited", "retry_after": retry_after},
            status_code=429,
        )

    raw_contact = landing_contact_runtime.parse_landing_contact_payload(payload)
    contact, error = landing_contact_runtime.validate_landing_contact(raw_contact)
    if error is not None:
        return JSONResponse({"detail": error.detail, "message": error.message}, status_code=400)

    source_ip = auth_utils.request_ip(request)
    user_agent = str(request.headers.get("user-agent") or "").strip()
    await _notify_landing_contact(
        int(_LANDING_CONTACT_NOTIFY_TENANT_ID),
        name=contact.name if contact is not None else "",
        contact=contact.contact if contact is not None else "",
        message=contact.message if contact is not None else "",
        source_ip=source_ip,
        user_agent=user_agent,
    )

    return JSONResponse({"ok": True})


@router.get("/blog/avio-launch")
async def marketing_blog_post(request: Request):
    return RedirectResponse(url="/blog", status_code=301)


@router.get("/about")
async def marketing_about(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    return _render_marketing_page(
        request,
        template="about.html",
        title="О компании · Avio",
        breadcrumb_title="О компании",
        description="Avio — продуктовая команда, которая помогает бизнесам продавать быстрее в мессенджерах.",
        path="/about",
    )


@router.get("/contacts")
async def marketing_contacts(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    return _render_marketing_page(
        request,
        template="contacts.html",
        title="Контакты · Avio",
        breadcrumb_title="Контакты",
        description="Свяжитесь с командой Avio: почта и быстрый канал связи.",
        path="/contacts",
    )


@router.get("/policy")
async def marketing_policy(request: Request):
    if not auth_utils.landing_enabled():
        return RedirectResponse(url="/admin")
    return _render_marketing_page(
        request,
        template="policy.html",
        title="Политика · Avio",
        breadcrumb_title="Политика",
        description="Политика обработки данных Avio и принципы конфиденциальности.",
        path="/policy",
    )


@router.get("/robots.txt")
@router.head("/robots.txt")
async def robots_txt(request: Request) -> Response:
    if not auth_utils.landing_enabled():
        return Response(status_code=404)
    base = _CANONICAL_BASE
    sitemap_url = f"{base}/sitemap.xml" if base else "/sitemap.xml"
    lines = [
        "User-agent: *",
        "Allow: /",
        "Disallow: /admin",
        "Disallow: /client",
        "Disallow: /login",
        "Disallow: /register",
        "Disallow: /auth",
        "Disallow: /forgot",
        "Disallow: /reset",
        "Disallow: /dashboard",
        "Disallow: /pub",
        "Disallow: /internal",
        "Disallow: /docs",
        f"Sitemap: {sitemap_url}",
    ]
    return Response("\n".join(lines) + "\n", media_type="text/plain")


@router.get("/yandex_f8f0345ae289e20c.html")
@router.head("/yandex_f8f0345ae289e20c.html")
async def yandex_verification() -> Response:
    path = pathlib.Path(__file__).resolve().parents[1] / "static" / "yandex_f8f0345ae289e20c.html"
    if not path.exists():
        return Response(status_code=404)
    return FileResponse(str(path), media_type="text/html")


@router.get("/sitemap.xml")
@router.head("/sitemap.xml")
async def sitemap_xml(request: Request) -> Response:
    if not auth_utils.landing_enabled():
        return Response(status_code=404)
    base = _CANONICAL_BASE
    url_items = [
        {"loc": f"{base}/", "lastmod": _landing_lastmod()},
        {"loc": f"{base}/features", "lastmod": _template_lastmod("features.html")},
        {"loc": f"{base}/solutions", "lastmod": _template_lastmod("solutions.html")},
        {"loc": f"{base}/pricing", "lastmod": _template_lastmod("pricing.html")},
        {"loc": f"{base}/avtootvetchik-avito", "lastmod": _template_lastmod("avtootvetchik-avito.html")},
        {"loc": f"{base}/faq", "lastmod": _template_lastmod("faq.html")},
        {"loc": f"{base}/blog", "lastmod": _template_lastmod("blog.html")},
        {"loc": f"{base}/about", "lastmod": _template_lastmod("about.html")},
        {"loc": f"{base}/contacts", "lastmod": _template_lastmod("contacts.html")},
        {"loc": f"{base}/policy", "lastmod": _template_lastmod("policy.html")},
    ]
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for item in url_items:
        parts.append("  <url>")
        parts.append(f"    <loc>{escape(item['loc'])}</loc>")
        if item.get("lastmod"):
            parts.append(f"    <lastmod>{item['lastmod']}</lastmod>")
        parts.append("  </url>")
    parts.append("</urlset>")
    return Response("\n".join(parts), media_type="application/xml")


def _login_error(request: Request, message: str, status_code: int = 400) -> Response:
    context = _base_context(request, "Вход · Avio", description=_LOGIN_DESCRIPTION)
    context["error"] = message
    return _render_with_csrf(request, "auth/login.html", context, status_code=status_code)


async def _resend_verify_for_login(request: Request, background_tasks: BackgroundTasks, user: dict, email: str) -> None:
    resend_allowed, _ = auth_utils.rate_limit_check(
        action="resend_login",
        email=email,
        request=request,
        limit=2,
        window_seconds=900,
    )
    if not resend_allowed:
        return
    token_raw = auth_utils.new_token()
    await auth_repo.create_token(
        int(user["id"]),
        auth_utils.hash_token(token_raw),
        "verify",
        datetime.now(timezone.utc) + timedelta(hours=24),
        request_ip=auth_utils.request_ip(request),
    )
    background_tasks.add_task(_send_verify_email, email, _email_verify_link(request, token_raw))


async def _create_login_response(request: Request, user: dict, next_value: str | None) -> RedirectResponse:
    session_id = auth_utils.new_session_id()
    await auth_repo.create_session(
        int(user["id"]),
        auth_utils.hash_token(session_id),
        auth_utils.session_expiry(),
        ip=auth_utils.request_ip(request),
        user_agent=request.headers.get("user-agent", ""),
    )
    await auth_repo.update_last_login(int(user["id"]))
    tenant_id = int(user["tenant_id"])
    response = RedirectResponse(url=_session_redirect_path(request, tenant_id, next_value), status_code=303)
    _set_session_cookies(request, response, session_id, (C.get_tenant_pubkey(tenant_id) or "").strip() or None)
    return response


def _registration_form(form: Any) -> RegistrationForm:
    return RegistrationForm(
        email=auth_utils.normalize_email(form.get("email") or ""),
        phone=str(form.get("phone") or "").strip(),
        contact=str(form.get("contact") or "").strip(),
        messenger=str(form.get("messenger") or "").strip(),
        password=str(form.get("password") or ""),
        confirm=str(form.get("confirm_password") or ""),
    )


def _registration_error(request: Request, data: RegistrationForm, message: str, status_code: int = 400) -> Response:
    context = _register_context(
        request,
        "Регистрация · Avio",
        email=data.email,
        phone=data.phone,
        contact=data.contact,
        messenger=data.messenger,
        description=_REGISTER_DESCRIPTION,
        error=message,
    )
    return _render_with_csrf(request, "auth/register.html", context, status_code=status_code)


def _validate_registration(data: RegistrationForm) -> str | None:
    if not data.email or "@" not in data.email:
        return "Введите корректный email."
    if len(data.digits) < 5:
        return "Введите номер телефона."
    if not data.contact:
        return "Укажите контакт для связи."
    if not data.messenger:
        return "Выберите удобный мессенджер."
    ok, message = auth_utils.password_ok(data.password)
    if not ok or data.password != data.confirm:
        return message or "Пароли не совпадают."
    return None


async def _send_verify_token(request: Request, background_tasks: BackgroundTasks, user_id: int, email: str) -> None:
    token_raw = auth_utils.new_token()
    await auth_repo.create_token(
        int(user_id),
        auth_utils.hash_token(token_raw),
        "verify",
        datetime.now(timezone.utc) + timedelta(hours=24),
        request_ip=auth_utils.request_ip(request),
    )
    background_tasks.add_task(_send_verify_email, email, _email_verify_link(request, token_raw))


async def _existing_registration_response(
    request: Request,
    background_tasks: BackgroundTasks,
    existing: dict,
    email: str,
) -> Response:
    if not existing.get("is_verified"):
        await _send_verify_token(request, background_tasks, int(existing["id"]), email)
    context = _base_context(request, "Проверьте почту · Avio")
    context["message"] = "Если email зарегистрирован, мы отправили письмо."
    context["metric_goal"] = "register"
    return render_template("auth/message.html", context)


async def _create_registration_user(data: RegistrationForm) -> tuple[int, dict | None]:
    tenant_id = await auth_repo.create_tenant()
    C.ensure_tenant_files(tenant_id)
    key = os.urandom(16).hex()
    C.add_key(tenant_id, key, data.email)
    C.set_primary(tenant_id, key)
    _write_registration_passport(tenant_id, data)
    user = await auth_repo.create_user(
        data.email,
        auth_utils.hash_password(data.password),
        tenant_id,
        contact=data.contact,
        preferred_messenger=data.messenger,
    )
    return tenant_id, user


def _write_registration_passport(tenant_id: int, data: RegistrationForm) -> None:
    try:
        cfg = C.read_tenant_config(tenant_id)
    except Exception:
        cfg = {}
    passport = dict(cfg.get("passport") or {})
    passport["phone"] = data.phone
    passport["contact"] = data.contact
    passport["preferred_messenger"] = data.messenger
    cfg["passport"] = passport
    try:
        C.write_tenant_config(tenant_id, cfg)
    except Exception:
        return


@router.get("/login")
async def login_form(request: Request):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    user = await auth_utils.get_current_user(request)
    if user:
        return RedirectResponse(url=_session_redirect_path(request, int(user["tenant_id"])))
    context = _base_context(request, "Вход · Avio", description=_LOGIN_DESCRIPTION)
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
    password = str(form.get("password") or "")
    if not email or "@" not in email or not password:
        return _login_error(request, "Введите корректный email и пароль.", status_code=400)

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
        return _login_error(request, "Неверный email или пароль.", status_code=401)

    if not auth_utils.verify_password(password, user.get("password_hash") or ""):
        return _login_error(request, "Неверный email или пароль.", status_code=401)

    if not user.get("is_verified"):
        try:
            await _resend_verify_for_login(request, background_tasks, user, email)
        except db_module.DatabaseUnavailableError:
            return JSONResponse({"detail": "db_unavailable"}, status_code=503)
        context = _base_context(request, "Вход · Avio", description=_LOGIN_DESCRIPTION)
        context["error"] = "Подтвердите email, мы отправили ссылку."
        context["email"] = email
        return _render_with_csrf(request, "auth/login.html", context, status_code=403)

    try:
        return await _create_login_response(request, user, form.get("next"))
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)


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
    context = _register_context(
        request,
        "Регистрация · Avio",
        description=_REGISTER_DESCRIPTION,
    )
    return _render_with_csrf(request, "auth/register.html", context)


@router.post("/auth/register")
async def register_submit(request: Request, background_tasks: BackgroundTasks):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    form = await request.form()
    if not auth_utils.verify_csrf(request, form.get("csrf_token")):
        return JSONResponse({"detail": "csrf_failed"}, status_code=400)

    data = _registration_form(form)
    allowed, retry_after = auth_utils.rate_limit_check(
        action="register",
        email=data.email,
        request=request,
        limit=3,
        window_seconds=900,
    )
    if not allowed:
        return JSONResponse(
            {"detail": "rate_limited", "retry_after": retry_after},
            status_code=429,
        )

    validation_error = _validate_registration(data)
    if validation_error:
        return _registration_error(request, data, validation_error, status_code=400)

    try:
        existing = await auth_repo.get_user_by_email(data.email)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if existing:
        try:
            return await _existing_registration_response(request, background_tasks, existing, data.email)
        except db_module.DatabaseUnavailableError:
            return JSONResponse({"detail": "db_unavailable"}, status_code=503)

    try:
        tenant_id, user = await _create_registration_user(data)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)
    if not user:
        await auth_repo.delete_tenant(tenant_id)
        return _registration_error(
            request,
            data,
            "Не удалось создать пользователя. Попробуйте снова.",
            status_code=500,
        )

    try:
        await _send_verify_token(request, background_tasks, int(user["id"]), data.email)
    except db_module.DatabaseUnavailableError:
        return JSONResponse({"detail": "db_unavailable"}, status_code=503)

    context = _base_context(request, "Проверьте почту · Avio")
    context["message"] = "Мы отправили ссылку для подтверждения на ваш email."
    context["metric_goal"] = "register"
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
async def verify_email(request: Request, background_tasks: BackgroundTasks, token: str | None = None):
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
    _set_session_cookies(request, response, session_id, client_key or None)
    return response


@router.get("/forgot")
async def forgot_form(request: Request):
    if not auth_utils.auth_enabled():
        return _auth_disabled()
    context = _base_context(request, "Сброс пароля · Avio", description=_FORGOT_DESCRIPTION)
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
    context = _base_context(request, "Новый пароль · Avio", description=_RESET_DESCRIPTION)
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
        context = _base_context(request, "Новый пароль · Avio", description=_RESET_DESCRIPTION)
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
