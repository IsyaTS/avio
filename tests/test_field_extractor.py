from __future__ import annotations

import sys
import types

if "httpx" not in sys.modules:
    sys.modules["httpx"] = types.SimpleNamespace(AsyncClient=object, HTTPError=Exception, TimeoutException=Exception)

from libs.core.integrations.amocrm import extract_fields, build_history_text


def test_extract_phone_normalizes():
    rules = [
        {"key": "phone", "regex": r"(\+?\d[\d\s\-()]{8,})", "amo_field_id": 123, "apply_mode": "last_inbound"},
    ]
    last_text = "Мой номер +7 (999) 123-45-67"
    results = extract_fields(rules, last_text=last_text, history_text="")
    assert results and results[0]["key"] == "phone"
    assert results[0]["value"] == "+79991234567"


def test_extract_from_history():
    rules = [
        {"key": "city", "regex": r"город[:\s]+([A-Za-zА-Яа-я]+)", "amo_field_id": 45, "apply_mode": "any_history"},
    ]
    history = build_history_text(["Привет", "город: Казань"])
    results = extract_fields(rules, last_text="Нет города", history_text=history)
    assert results and results[0]["value"] == "Казань"


def test_extract_full_match():
    rules = [
        {"key": "brand", "regex": r"Acme", "amo_field_id": 7, "apply_mode": "last_inbound"},
    ]
    results = extract_fields(rules, last_text="Интересует Acme", history_text="")
    assert results and results[0]["value"] == "Acme"
