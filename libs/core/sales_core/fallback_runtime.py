from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class FallbackRuntimeDeps:
    grounding_catalog_items: Callable[[Mapping[str, Any] | None], list[dict[str, Any]]]
    classify_turn_intent: Callable[[str], str]
    normalize_text: Callable[[Any], str]
    shortlist_preview_text: Callable[..., str]
    extract_attribute_probe: Callable[[str], str]
    display_item_label: Callable[[Mapping[str, Any]], str]
    item_label: Callable[[Mapping[str, Any]], str]
    catalog_min_price: Callable[[list[dict[str, Any]]], int | None]
    catalog_max_price: Callable[[list[dict[str, Any]]], int | None]
    format_rub_price: Callable[[int], str]
    is_price_intent: Callable[[str], bool]
    looks_like_price_objection: Callable[[str], bool]
    variants_user_hint_re: Any
    price_inline_re: Any
    price_thousands_re: Any
    fact_token_re: Any
    generic_fact_stopwords: set[str]


class FallbackRuntime:
    def __init__(self, deps: FallbackRuntimeDeps) -> None:
        self.deps = deps

    @staticmethod
    def safe_json_load(raw: str) -> dict[str, Any]:
        text = (raw or "").strip()
        if not text:
            return {}
        try:
            data = json.loads(text)
            return data if isinstance(data, dict) else {}
        except Exception:
            match = re.search(r"\{.*\}", text, flags=re.DOTALL)
            if not match:
                return {}
            try:
                data = json.loads(match.group(0))
                return data if isinstance(data, dict) else {}
            except Exception:
                return {}

    def has_substantive_non_question_payload(self, text: str) -> bool:
        candidate = str(text or "").strip()
        if not candidate:
            return False
        candidate_low = self.deps.normalize_text(candidate)
        # Treat short acknowledgement stubs as non-substantive so the
        # required-fact follow-up question is not accidentally suppressed.
        if re.match(r"(?iu)^\s*(понял|поняла|принял|приняла|услышал|принято|ок|окей)\b[,.! ]*$", candidate_low):
            return False
        if re.match(
            r"(?iu)^\s*[а-яёa-z0-9][а-яёa-z0-9\-/\s]{1,64}[,—-]\s*(понял|поняла|принял|приняла|услышал|принято)\b[,.! ]*$",
            candidate_low,
        ):
            return False
        segments = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
        if not segments:
            segments = [candidate]
        for segment in segments:
            if "?" in segment:
                continue
            probe = str(segment or "").strip()
            if len(probe) < 16:
                continue
            if (
                self.deps.price_inline_re.search(probe)
                or self.deps.price_thousands_re.search(probe)
                or "%" in probe
                or "₽" in probe
            ):
                return True
            tokens = [
                tok
                for tok in self.deps.fact_token_re.findall(self.deps.normalize_text(probe))
                if len(tok) >= 3 and tok not in self.deps.generic_fact_stopwords
            ]
            if len(tokens) >= 3:
                return True
        return False

    def llm_unavailable_reply(
        self,
        *,
        user_text: str = "",
        grounding: Mapping[str, Any] | None = None,
    ) -> str:
        text = str(user_text or "").strip()
        low = text.lower()
        if not text:
            return "Напишите, пожалуйста, что хотите уточнить."

        if re.search(r"(?iu)\b(спасибо|не нужно|не подходит|уже решили|не актуально|пока не нужно)\b", low):
            return "Хорошо, понял. Если снова понадобится — напишите, будем на связи."

        if "каталог получили" in low or "уже отправил" in low or "уже прислал" in low or "жду" in low:
            return "Понял, спасибо. Проверим и вернемся с ответом."

        if "объявлен" in low and re.search(r"(?iu)\b(снят|снято|не опублик|пропал)\b", low):
            return "Проверим актуальность объявления. Пока можете написать, что именно хотели уточнить по товару."

        if re.search(r"(?iu)\b(статус\w*|где\s+заказ|заказ\w*\s+готов|жду\s+заказ)\b", low):
            return "Статус заказа нужно проверить по заявке. Передам менеджеру, он посмотрит и ответит."

        if re.search(r"(?iu)\b(каталог\w*|прайс\w*|фото|цвет\w*)\b", low):
            if re.search(r"(?iu)\b(прайс\w*|цен\w*|стоим\w*)\b", low):
                return "Прайс и стоимость лучше смотреть по конкретной модели. Напишите категорию или вариант, и рассчитаем без лишних догадок."
            if re.search(r"(?iu)\b(\d{2,4}\s*[xх*/-]\s*\d{2,4}|размер|про[её]м|шир\w*|выс\w*)\b", low):
                return "Размер вижу. По фото и подходящим вариантам нужно проверить модель, подберем без повторного уточнения размера."
            if re.search(r"(?iu)\b(фото|цвет\w*)\b", low):
                return "Фото и варианты по цветам можно подобрать здесь. Напишите категорию или модель, которую хотите посмотреть."
            return "Можем сориентировать здесь. Напишите, какая категория или модель интересует, и подберем подходящие варианты."

        if re.search(r"(?iu)\b(материал\w*|характеристик\w*|весит|вес\b)\b", low):
            if "достав" in low:
                return "По характеристикам и доставке нужно проверить конкретную модель и адрес. Напишите модель или ссылку на объявление."
            return "По характеристикам нужно проверить конкретную модель. Напишите, какой вариант рассматриваете."

        if re.search(r"(?iu)\b(установ\w*|монтаж\w*|замер\w*|сняти\w*|демонтаж\w*|под ключ|замерщик\w*|услуг\w*)\b", low):
            if re.search(r"(?iu)\b(стоим\w*|цен\w*|сколько|достав\w*|этаж\w*)\b", low):
                return "По установке и доставке стоимость нужно рассчитать по адресу и объему работ. Напишите город и условия, проверим без догадок."
            if "размер" in low or "проем" in low or "проём" in low:
                return "По размеру и замеру нужно проверить условия на месте. Передам менеджеру, он уточнит подходящий вариант."
            if re.search(r"(?iu)\b(сам|самовывоз|забрать|заберу)\b", low):
                return "Самовывоз и выезд замерщика нужно согласовать по заказу. Передам менеджеру, он проверит условия."
            return "По услуге нужно проверить объем работ. Напишите город и что именно требуется сделать."

        if re.search(r"(?iu)\b(где|адрес\w*|шоурум\w*|магазин\w*|самовывоз\w*|забрать|заберу)\b", low):
            return "По адресу и самовывозу лучше проверить актуальные данные. Напишите ваш город, и подскажем ближайший вариант."

        if re.search(r"(?iu)\b(достав\w*|привез\w*|км|километр\w*|этаж\w*|бесплатн\w*)\b", low):
            if re.search(r"(?iu)\b(стоим\w*|цен\w*|сколько|этаж\w*|бесплатн\w*)\b", low):
                return "По доставке стоимость и условия нужно рассчитать по адресу и объему работ. Напишите город, проверим без догадок."
            return "По доставке нужно проверить адрес и условия. Напишите город или населенный пункт, рассчитаем аккуратно."

        if re.search(r"(?iu)\b(налич\w*|есть\s+в\s+наличии|откро\w*\s+заказ\w*|заказ\w*\s+на\s+выходн)\b", low):
            return "Наличие нужно проверить по конкретной модели. Напишите, какой вариант рассматриваете."

        if re.search(r"(?iu)\b(гарант\w*|гарантийн\w*)\b", low):
            return "По гарантии ответим точно после проверки модели и условий заказа."

        if re.search(r"(?iu)\b(оплат\w*|наличн\w*|безнал\w*|счет\w*|счёт\w*|перевод\w*)\b", low):
            return "По оплате варианты зависят от заказа. Уточните, пожалуйста, физлицо или юрлицо."

        if re.search(r"(?iu)\b(дорого|скидк\w*|дешев\w*|дешёв\w*|выгод\w*|оптов\w*|опт)\b", low):
            return "Можем посмотреть варианты по бюджету и условиям заказа. Напишите, на какую сумму ориентируетесь."

        if re.search(r"(?iu)\b(сколько|цен\w*|стоим\w*|85\.?000|8\.?500|распродаж\w*|расшир\w*|срез\w*|перемыч\w*)\b", low):
            if re.search(r"(?iu)\b(расшир\w*|срез\w*|перемыч\w*)\b", low):
                return "По таким работам стоимость нужно считать после оценки проема и объема работ. Напишите город и фото/размеры, проверим точно."
            return "Точную стоимость без проверки модели и условий не назову. Напишите, какой вариант рассматриваете, и рассчитаем."

        if re.search(r"(?iu)\b(размер|про[её]м|шир\w*|выс\w*|открыв\w*|замк\w*|замок|без глазка|лев\w*|прав\w*|\d{2,4}\s*[xх*/-]\s*\d{2,4}|ш\s*\d{2,4}|в\s*\d{2,4})\b", low):
            details: list[str] = []
            if re.search(r"(?iu)\b(размер|про[её]м|шир\w*|выс\w*|\d{2,4}\s*[xх*/-]\s*\d{2,4}|ш\s*\d{2,4}|в\s*\d{2,4})\b", low):
                details.append("размер")
            if re.search(r"(?iu)\b(лев\w*|прав\w*|открыв\w*)\b", low):
                details.append("открывание")
            if re.search(r"(?iu)\b(замк\w*|замок)\b", low):
                details.append("замки")
            details_text = ", ".join(details) if details else "детали"
            return f"Данные по запросу вижу: {details_text}. Уточним подходящий вариант и условия по этой модели."

        if "[phone]" in low or re.search(r"\+?\d[\d\-\s()]{6,}\d", text):
            return "Контакт получил, передам менеджеру. Он проверит детали и свяжется с вами."

        if "[email]" in low or re.search(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text):
            return "Почту получил, передам менеджеру. Он проверит информацию и ответит."

        if re.search(r"(?iu)\b(юрлиц\w*|организац\w*|документ\w*|справк\w*)\b", low):
            return "По условиям для организаций и документам можно проверить отдельно. Напишите, что именно нужно оформить."

        if re.search(r"(?iu)\b(хочу|нужн\w*|интересу\w*)\b", low):
            return "Можем подобрать варианты по каталогу. Напишите, какие требования важны: размер, бюджет или назначение."

        return "Понял ваш вопрос. Уточню информацию и отвечу по существу."
