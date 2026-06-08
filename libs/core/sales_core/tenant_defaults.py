from __future__ import annotations

import pathlib
from typing import Any


def build_default_tenant_json(data_dir: pathlib.Path) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "passport": {
            "tenant_id": 0,
            "brand": "Мой Бренд",
            "agent_name": "Менеджер",
            "city": "Город",
            "currency": "₽",
            "channel": "WhatsApp",
            "whatsapp_link": "https://wa.me/7XXXXXXXXXX",
            "phone": "",
            "contact": "",
            "preferred_messenger": "",
        },
        "behavior": {
            "always_full_catalog": True,
            "send_catalog_as_pages": True,
            "max_clarifying_questions": 1,
            "tone": "коротко-дружелюбно",
            "anti_repeat_window": 6,
            "dedupe_catalog_titles": True,
            "allow_filter_commands": True,
            "pdf_one_item_per_page": False,
            "explain": False,
            "use_universal_pdf_pipeline": False,
        },
        "catalogs": [],
        "funnel": {
            "avito_to_wa": {
                "enabled": True,
                "trigger_phrases": ["напишите в whatsapp", "скину в ватсап"],
            }
        },
        "learning": {
            "enabled": True,
            "retriever": "tfidf",
            "top_k": 2,
            "max_tokens": 320,
            "min_chars": 15,
            "track_outcomes": True,
            "auto_vitrine": True,
            "memory_window_dialogs": 50,
            "pinned_items": [],
            "negatives": ["дорого", "долго"],
            "intervention_policy": {
                "enabled": False,
                "capture_enabled": True,
                "runtime_enabled": True,
                "shadow_mode": True,
                "apply_mode": False,
                "kill_switch": False,
                "stitch_window_seconds": 45,
                "episode_history_limit": 24,
                "runtime_history_limit": 12,
                "outcome_horizon_minutes": 180,
                "decision_window_minutes": 180,
                "max_rules": 12,
                "min_similarity": 0.64,
                "min_confidence": 0.72,
                "min_evidence": 3,
                "min_distinct_leads": 2,
                "min_reward_delta": 0.15,
                "max_negative_evidence": 2,
            },
        },
        "limits": {
            "catalog_page_size": 8,
            "max_pages_per_reply": 5,
            "rate_limit_per_contact_min": 1,
            "send_throttle_ms": 250,
        },
        "integrations": {
            "pdf_catalog_url": "",
            "crm_webhook": "",
            "analytics_pixel": "",
            "ga_id": "",
            "uploaded_catalog": "",
        },
        "channels": {
            "whatsapp": {"enabled": True},
            "telegram": {"enabled": True},
            "max": {"enabled": True},
            "max_personal": {"enabled": False},
        },
    }

    sample_catalog = data_dir / "catalog_sample.csv"
    if sample_catalog.exists():
        payload["catalogs"].append(
            {
                "name": "catalog",
                "path": str(sample_catalog),
                "type": "csv",
                "delimiter": ",",
                "encoding": "utf-8",
                "fields": {
                    "id": "id",
                    "title": "name",
                    "price": "price",
                    "brand": "brand",
                    "material": "material",
                    "color": "color",
                    "stock": "stock",
                    "image": "image",
                    "url": "url",
                    "tags": "tags",
                },
                "ranking": {
                    "boost_tags": ["хит", "новинка", "склад", "топ"],
                    "boost_stock": 1.0,
                    "boost_margin": 0.2,
                    "min_stock": 0,
                    "min_score": 0,
                    "sort": [
                        {"by": "score", "order": "desc"},
                        {"by": "price", "order": "asc"},
                    ],
                    "filters_default": {"stock": [">", 0]},
                },
                "presentation": {
                    "price_format": "{price} {CUR}",
                    "line_format": "{title} — {price} {CUR}. Цвет: {color}. Материал: {material}. [{url}]",
                    "group_by": "brand",
                },
            }
        )
    return payload


def load_default_persona_md(root_dir: pathlib.Path) -> str:
    default_path = root_dir / "agents" / "persona_default_ru.md"
    try:
        return default_path.read_text(encoding="utf-8")
    except Exception:
        return """Продукт/услуга: опишите ключевые товары и услуги.
Аудитория: частные клиенты и B2B (по ситуации).
Фокус ценности: сроки, удобство, качество, гарантия, понятные условия.
Типовые возражения: дорого, долго, сомнения в качестве.

Стиль и качество:
- Общение на «Вы», коротко и по делу.
- НЕ задавай очевидные вопросы и НЕ повторяй то, что уже сказал клиент.
- За одно сообщение: 1 вопрос максимум.
- Сначала подтверждение запроса, потом уточнение, затем предложение.

Уточняющие вопросы (используй по ситуации):
- «Что именно нужно: вариант/размер/задача?»
- «В какие сроки планируете?»
- «Есть ориентир по бюджету?»
- «В каком городе/районе нужна доставка или установка?»

Предложение:
- 2–3 варианта: название → 2 выгоды → цена/ориентир → следующий шаг.
- Если нет точных параметров — предложи 2 сценария (бюджетный/оптимальный).

Возражения:
- Цена: объяснить ценность, дать альтернативу дешевле.
- Сроки: назвать реальные сроки и варианты ускорения.

Примеры стартовых сообщений:
- «Здравствуйте! Чем могу помочь?»
- ""

Скрипт диалога (шаги):
- Понять запрос → уточнить параметр → предложить варианты → следующий шаг.

Частые вопросы:
- Вопрос: Сколько стоит? → Ответ: Цена зависит от параметров. Уточните, что именно нужно — подберу варианты.
- Вопрос: Есть в наличии? → Ответ: Уточните, какой вариант нужен, я проверю наличие и отвечу.

Отложенные сообщения:
- Один вежливый напоминатель, если клиент не ответил.
"""
