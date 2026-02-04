from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from libs.core.sales_core import read_persona, write_persona, read_tenant_config, write_tenant_config


@dataclass(frozen=True)
class QuickstartTemplate:
    id: str
    title: str
    summary: str
    focus: str


_TEMPLATES: List[QuickstartTemplate] = [
    QuickstartTemplate(
        id="doors",
        title="Двери и металлоконструкции",
        summary="Продажи входных/межкомнатных дверей, монтаж и доп. услуги.",
        focus="Сроки установки, безопасность, гарантия, внешний вид.",
    ),
    QuickstartTemplate(
        id="renovation",
        title="Ремонт и стройка",
        summary="Ремонт, отделка, строительство, бригады и подряд.",
        focus="Сроки, этапы работ, контроль качества, смета.",
    ),
    QuickstartTemplate(
        id="furniture",
        title="Мебель и интерьер",
        summary="Кухни, шкафы, мебель на заказ и готовые решения.",
        focus="Размеры, дизайн, сроки изготовления, материалы.",
    ),
    QuickstartTemplate(
        id="services",
        title="Услуги (универсальный)",
        summary="Подходит для большинства сервисных ниш.",
        focus="Проблема клиента, выгода услуги, скорость отклика.",
    ),
    QuickstartTemplate(
        id="electronics",
        title="Техника и электроника",
        summary="Продажа техники, комплектов, расходников.",
        focus="Наличие, гарантия, характеристики, цена.",
    ),
]

_GOAL_MAP: Dict[str, str] = {
    "lead": "получить контакт клиента и перейти к персональному расчёту",
    "sale": "закрыть клиента на покупку в чате",
    "call": "записать клиента на звонок/замер",
    "handoff": "перевести диалог в удобный мессенджер",
    "consult": "дать консультацию и подготовить предложение",
}

_CTA_MAP: Dict[str, str] = {
    "lead": "Оставьте контакт или удобный канал связи — подготовлю точный расчёт сегодня.",
    "sale": "Готовы оформить заказ? Подскажите удобный способ оплаты и доставки.",
    "call": "Удобно назначить звонок или замер? Напишите телефон и время.",
    "handoff": "Чтобы быстрее отправить материалы, оставьте номер или удобный мессенджер.",
    "consult": "Опишите задачу чуть подробнее — подготовлю точный вариант.",
}


def list_quickstart_templates() -> List[Dict[str, str]]:
    return [
        {"id": t.id, "title": t.title, "summary": t.summary, "focus": t.focus}
        for t in _TEMPLATES
    ]


def _template_by_id(template_id: str | None) -> QuickstartTemplate:
    if template_id:
        for tpl in _TEMPLATES:
            if tpl.id == template_id:
                return tpl
    return _TEMPLATES[-1]


def _build_persona_text(
    *,
    template: QuickstartTemplate,
    brand: str,
    agent: str,
    offer: str,
    utp: str,
    faq: List[Dict[str, str]],
    starters: List[str],
    script: List[str],
    goal_key: str,
) -> str:
    goal_text = _GOAL_MAP.get(goal_key, _GOAL_MAP["lead"])
    return (
        f"Продукт/услуга: {offer.strip() or 'опишите ключевые товары и услуги'}.\n"
        f"Фокус ценности: {template.focus}.\n"
        f"УТП компании: {utp.strip() or 'быстрый ответ, понятные условия, честные сроки'}.\n\n"
        "Стиль и качество:\n"
        "- Общение на «Вы», коротко и по делу.\n"
        "- НЕ задавай очевидные вопросы и НЕ повторяй то, что уже сказал клиент.\n"
        "- За одно сообщение: 1 вопрос максимум.\n"
        "- Сначала подтверждение запроса, потом уточнение, затем предложение.\n\n"
        "Уточняющие вопросы (используй по ситуации):\n"
        "- «Что именно нужно: вариант/размер/задача?»\n"
        "- «В какие сроки планируете?»\n"
        "- «Есть ориентир по бюджету?»\n"
        "- «В каком городе/районе нужна доставка или установка?»\n\n"
        "Предложение:\n"
        "- 2–3 варианта: название → 2 выгоды → цена/ориентир → следующий шаг.\n"
        "- Если нет точных параметров — предложи 2 сценария (бюджетный/оптимальный).\n\n"
        "Возражения:\n"
        "- Цена: объяснить ценность, дать альтернативу дешевле.\n"
        "- Сроки: назвать реальные сроки и варианты ускорения.\n\n"
        "Примеры стартовых сообщений:\n"
        + "\n".join(f"- «{item.strip()}»" for item in starters if item.strip())
        + ("\n" if starters else "")
        + "Скрипт диалога (шаги):\n"
        + "\n".join(f"- {item.strip()}" for item in script if item.strip())
        + ("\n" if script else "")
        + "Частые вопросы:\n"
        + "\n".join(
            f"- Вопрос: {item.get('q','').strip()} → Ответ: {item.get('a','').strip()}"
            for item in faq
            if (item.get('q') or '').strip() and (item.get('a') or '').strip()
        )
        + "\n"
    )


def apply_quickstart(tenant: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    cfg = read_tenant_config(int(tenant))
    if not isinstance(cfg, dict):
        cfg = {}

    passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
    brand = str(payload.get("brand") or "").strip()
    agent = str(payload.get("agent") or "").strip()
    if brand:
        passport["brand"] = brand
    if agent:
        passport["agent_name"] = agent
    cfg["passport"] = passport

    offer = str(payload.get("offer") or "").strip()
    utp = str(payload.get("utp") or "").strip()
    raw_faq = payload.get("faq")
    faq: List[Dict[str, str]] = []
    if isinstance(raw_faq, list):
        for item in raw_faq:
            if isinstance(item, dict):
                q = str(item.get("q") or "").strip()
                a = str(item.get("a") or "").strip()
                if q and a:
                    faq.append({"q": q, "a": a})
    starters = [str(x).strip() for x in (payload.get("starters") or []) if str(x).strip()]
    script = [str(x).strip() for x in (payload.get("script") or []) if str(x).strip()]
    goal_key = "lead"
    template_id = str(payload.get("template") or "").strip()
    template = _template_by_id(template_id)

    persona_text = _build_persona_text(
        template=template,
        brand=passport.get("brand") or brand,
        agent=passport.get("agent_name") or agent,
        offer=offer,
        utp=utp,
        faq=faq,
        starters=starters,
        script=script,
        goal_key=goal_key,
    )

    behavior = cfg.get("behavior") if isinstance(cfg.get("behavior"), dict) else {}
    if "tone" not in behavior or not behavior.get("tone"):
        behavior["tone"] = "коротко-дружелюбно"
    cfg["behavior"] = behavior

    cta = cfg.get("cta") if isinstance(cfg.get("cta"), dict) else {}
    if goal_key in _CTA_MAP:
        cta["primary"] = _CTA_MAP[goal_key]
    cfg["cta"] = cta

    apply_all = bool(payload.get("apply_all", False))

    write_persona(tenant, persona_text, channel=None)

    if apply_all:
        for channel in ("telegram", "avito", "max"):
            write_persona(tenant, persona_text, channel=channel)
    else:
        for channel in ("telegram", "avito", "max"):
            existing = read_persona(tenant, channel)
            if not (existing or "").strip():
                write_persona(tenant, persona_text, channel=channel)

    cfg.setdefault("quickstart", {})
    if isinstance(cfg.get("quickstart"), dict):
        cfg["quickstart"].update(
            {
                "enabled": True,
                "auto_persona": True,
                "goal": goal_key,
                "apply_all": bool(apply_all),
            }
        )
    write_tenant_config(tenant, cfg)

    return {
        "ok": True,
        "persona": persona_text,
        "applied_to_channels": apply_all,
        "template": template.id,
    }


def _update_persona_header(text: str, *, brand: str, agent: str, goal_key: str) -> str:
    if not text:
        return text
    goal_text = _GOAL_MAP.get(goal_key, _GOAL_MAP["lead"])
    header = f"Контекст: {agent or 'Менеджер'} из {brand or 'Бренд'}. Цель: {goal_text}."
    lines = text.splitlines()
    if lines and lines[0].strip().lower().startswith("контекст:"):
        lines[0] = header
    else:
        lines = [header, ""] + lines
    return "\n".join(lines).strip() + "\n"


def refresh_persona_headers(tenant: int, cfg: Dict[str, Any]) -> None:
    qs = cfg.get("quickstart") if isinstance(cfg.get("quickstart"), dict) else {}
    passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
    brand = passport.get("brand") or ""
    agent = passport.get("agent_name") or ""
    goal_key = str(qs.get("goal") or "lead").strip().lower()

    def _needs_replace(text: str) -> bool:
        return any(token in text for token in ("{BRAND}", "{AGENT_NAME}", "{CITY}"))

    base_text = read_persona(tenant)
    if base_text and (_needs_replace(base_text) or qs.get("auto_persona")):
        write_persona(
            tenant,
            _update_persona_header(base_text, brand=brand, agent=agent, goal_key=goal_key),
        )

    if qs.get("apply_all"):
        for channel in ("telegram", "avito", "max"):
            text = read_persona(tenant, channel)
            if text and (_needs_replace(text) or qs.get("auto_persona")):
                write_persona(
                    tenant,
                    _update_persona_header(text, brand=brand, agent=agent, goal_key=goal_key),
                    channel=channel,
                )
