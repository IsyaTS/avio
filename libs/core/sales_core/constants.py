from __future__ import annotations

import re
from typing import Dict

_ROBOTIC_BANNED_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bваш запрос принят\b", re.IGNORECASE),
    re.compile(r"\bпринял(?:\s+ваш)?\s+запрос\b", re.IGNORECASE),
    re.compile(r"\bблагодар(?:ю|им)\s+за\s+обращение\b", re.IGNORECASE),
    re.compile(r"\bв рамках вашего запроса\b", re.IGNORECASE),
)

_GRATITUDE_RE = re.compile(r"\b(спасибо|благодарю|благодарим)\b", re.IGNORECASE)
_GREETING_PREFIX_RE = re.compile(
    r"^\s*(здравствуйте|добрый день|добрый вечер|привет)\b[!,. ]*", re.IGNORECASE
)
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
_GRATITUDE_PHRASE_RE = re.compile(
    r"\b(спасибо(?:\s+за\s+обращение)?|благодарю(?:\s+за\s+обращение)?|благодарим(?:\s+за\s+обращение)?)\b[,.! ]*",
    re.IGNORECASE,
)
_OPENING_HEY_RE = re.compile(r"^\s*привет[!,. ]*", re.IGNORECASE)
_QUESTION_THIS_OR_RE = re.compile(r"^\s*это\s+(.+?)\?\s*$", re.IGNORECASE)
_OPENING_WORD_RE = re.compile(r"^\s*([A-Za-zА-Яа-яЁё]+)")
_LOWERCASE_OPENING_BLOCKED = {
    "здравствуйте",
    "привет",
    "добрый",
    "уважаемый",
    "уважаемая",
    "ваалейкум",
}
_ENTITY_ACK_PREFIX_RE = re.compile(
    r"^\s*([a-zа-яё0-9][a-zа-яё0-9\-\s]{1,42})\s*[,—-]\s*(понял|принял|услышал|принято)\b[,.! ]*",
    re.IGNORECASE,
)
_NEIGHBOR_CLAIM_RE = re.compile(
    r"\b(соседн(?:ем|ий|яя)\s+(?:доме|подъезде)|ставили\s+рядом|недавно\s+ставили)\b",
    re.IGNORECASE,
)
_STOP_INTENT_RE = re.compile(
    r"\b(стоп|останов(?:и|ите|ка)|не\s+пиш(?:и|ите)|не\s+беспокой|отпис(?:ка|ать|ыва)|больше\s+не\s+пиш)\b",
    re.IGNORECASE,
)
_URGENT_TODAY_RE = re.compile(
    r"\b(срочн\w*|сегодня|как\s+можно\s+быстрее|прямо\s+сейчас)\b",
    re.IGNORECASE,
)
_ETA_INTENT_RE = re.compile(
    r"(?iu)\b(через\s+сколько|когда\s+приед|когда\s+можно\s+приех|когда\s+приехать|"
    r"сколько\s+ждать|во\s+сколько|к\s+какому\s+времени|можете\s+завтра|"
    r"завтра\s+с\s+утра|утром|вечером)\b"
)
_INSTRUCTION_LEAK_LINE_RE = re.compile(
    r"(?im)^\s*(после\s+приветствия\s+последовательно\s+уточни:?\s*|"
    r"диалог-скрипт\s*(?:\(.*?\))?:?\s*|"
    r"шаблон\s+реплик:?\s*|"
    r"правила\s+ответа:?\s*)$"
)
_INSTRUCTION_LIST_LINE_RE = re.compile(
    r"(?iu)^\s*\d+[)\.]\s*(город|адрес|тип\s+объекта|тип\s+помещения|квартир|частн(?:ый|ого)\s+дом|"
    r"модель|вариант|каталог|бюджет|срок|контакт|телефон|размер|про[её]м|замер)\b.*$"
)
_SHORTLIST_LEAK_RE = re.compile(r"(?iu)\bсобрал\s+коротк\w*\s+шорт[-\s]?лист\b")
_ORDER_INTENT_RE = re.compile(
    r"(?iu)\b(оформ(ить|им|ление)|заказ(ать|а|у|ом)?|купить|беру|готов\s+оформ)\b"
)
_CATALOG_REQUEST_RE = re.compile(
    r"(?iu)\b(каталог|ассортимент|прайс|прайс[-\s]?лист|модел[ьи]|ссылк[ау]|photo|фото)\b"
)
_OFFTOPIC_SMALLTALK_RE = re.compile(
    r"(?iu)\b(как\s+дела|кто\s+ты|погода|анекдот|шутк|гороскоп|курс\s+доллара|футбол|музыка)\b"
)
_QUESTION_CUE_RE = re.compile(
    r"(?iu)^\s*(подскаж(?:и|ите)|уточн(?:и|ите)|скаж(?:и|ите)|"
    r"в\s+каком|в\s+какие|какой|какая|какие|где|когда|сколько|"
    r"нужн(?:а|ы|о)?\s+ли|выбираете|пришл(?:и|ите))\b"
)
_LOW_SIGNAL_USER_REPLY_RE = re.compile(
    r"(?iu)^\s*(да|ага|ок|okay|окей|давай|показывай|скидывай|погнали|го|угу|угу+)\s*$"
)
_LOW_SIGNAL_CONTEXT_RE = re.compile(
    r"(?iu)\b("
    r"не\s+могу\s+откры(ть|ться)|"
    r"не\s+открыва(ется|еться|лось)|"
    r"долго\s+груз(ит|ится)|"
    r"пока\s+не\s+могу|"
    r"сейчас\s+не\s+могу|"
    r"позже\s+скину|"
    r"позже\s+пришлю"
    r")\b"
)
_CATALOG_UNAVAILABLE_RE = re.compile(
    r"(?iu)\b("
    r"каталог\s+(?:еще|ещё)?\s*груз|"
    r"каталог\s+не\s+груз|"
    r"каталог\s+не\s+открыва|"
    r"не\s+могу\s+откры(ть|ться)\s+каталог|"
    r"не\s+могу\s+пока\s+посмотре(ть|ть\s+каталог)|"
    r"не\s+могу\s+посмотре(ть|ть\s+каталог)|"
    r"не\s+вижу\s+каталог|"
    r"пока\s+не\s+могу\s+посмотреть\s+каталог"
    r")\b"
)
_WHY_QUESTION_RE = re.compile(
    r"(?iu)\b(зачем|почему|для\s+чего|для\s+чего\s+это|почему\s+нужен|почему\s+нужна|"
    r"зачем\s+нужен|зачем\s+нужна|зачем\s+вам)\b"
)
_REPAIR_TURN_RE = re.compile(
    r"(?iu)^\s*(чего|что\??|в\s+смысле|не\s+понял(?:а)?|не\s+поняли|"
    r"не\s+разобрал(?:а)?|не\s+ясно|не\s+очень\s+понятно|"
    r"я\s+же\s+говорил(?:а)?(?:\s+уже)?|уже\s+говорил(?:а)?|"
    r"я\s+же\s+писал(?:а)?|уже\s+писал(?:а)?|"
    r"вы\s+уже\s+спрашивали|опять\s+спрашиваете|зачем\s+повторяете)\s*$"
)
_NOISE_NEED_RE = re.compile(r"(?iu)\b(тих\w*|шумк\w*|шумоизоляц\w*|без\s+шума)\b")
_INSULATION_NEED_RE = re.compile(
    r"(?iu)\b(дует|сквозняк|промерз\w*|продува\w*|холод\w*|утеплен\w*|теплоизоляц\w*|"
    r"терморазрыв\w*|термо\s*разрыв\w*|термодвер\w*)\b"
)
_OBJECT_TYPE_HINT_RE = re.compile(
    r"(?iu)\b(квартир\w*|дом\w*|помещен\w*|офис\w*|склад\w*|коммерч\w*|студи\w*|комнат\w*|этаж\w*)\b"
)
_REPLY_STYLE_GUARD = (
    "Следуй персоне буквально. "
    "Пиши живо и по-человечески, без канцелярита. "
    "Не начинай ответ с 'Понял', 'Поняла', 'Спасибо, что уточнили'. "
    "Сообщение: 1-3 коротких предложения, максимум 1 вопрос."
)

_FACT_TOKEN_RE = re.compile(r"[0-9a-zа-яё]+", re.IGNORECASE)
_CONTACT_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_CONTACT_HANDLE_RE = re.compile(r"(?<!\w)@[\w\d_]{4,}")
_CONTACT_PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d[\d\-\s()]{8,}\d)(?!\d)")
_PRICE_INLINE_RE = re.compile(
    r"(?<!\d)(?:\d{1,3}(?:[ \u00A0\u202F]\d{3})+|\d{4,7})(?!\d)(?:\s*(?:₽|руб(?:\.|ля|лей)?))?",
    re.IGNORECASE,
)
_PRICE_THOUSANDS_RE = re.compile(r"(?iu)\b(\d{1,3})\s*(?:тыс(?:\.|яч)?|тысяч(?:а|и)?|к)\b")
_MODEL_QUOTED_MENTION_RE = re.compile(r'(?iu)\b(модель|вариант|дверь)\s*[«"]([^"»]{2,80})[»"]')
_GENERIC_FACT_STOPWORDS = {
    "и",
    "или",
    "в",
    "на",
    "по",
    "с",
    "для",
    "это",
    "как",
    "что",
    "вам",
    "вас",
    "ваш",
    "ваша",
    "ваше",
    "ваши",
    "у",
    "к",
    "же",
    "ли",
    "чего",
    "зачем",
    "почему",
    "тоже",
    "самое",
    "этот",
    "эта",
    "эти",
    "этого",
    "мы",
    "вы",
}
_SLOT_ALIASES = {
    "location": ("город", "район", "адрес", "локац", "доставк"),
    "object": ("квартир", "дом", "объект", "помещен"),
    "model": ("модель", "вариант", "артикул", "позици", "катал"),
    "budget": ("бюдж", "цен", "стоим"),
    "timeline": ("когда", "срок", "сегодня", "завтра", "дат"),
    "dimensions": ("размер", "проем", "ширин", "высот", "замер"),
    "contact": ("телефон", "контакт", "мессендж", "whatsapp", "telegram", "телеграм"),
    "quantity": ("сколько", "количеств"),
    "color": ("цвет", "оттен", "тон"),
}
_QUESTION_TOPIC_TO_SLOT = {
    "location": "location",
    "object": "object",
    "model": "model",
    "budget": "budget",
    "timeline": "timeline",
    "dimensions": "dimensions",
    "contact": "contact",
    "quantity": "quantity",
    "color": "color",
}
_GENERIC_MODEL_WORDS = {
    "есть",
    "цена",
    "сколько",
    "нужно",
    "надо",
    "подскажите",
    "модель",
    "вариант",
}
_FACT_CANONICAL_ALIASES: Dict[str, set[str]] = {
    "city": {"city", "город", "location", "локация", "населенный_пункт", "населенныйпункт"},
    "address": {"address", "адрес"},
    "object_type": {"object", "object_type", "тип_объекта", "тип_помещения", "помещение"},
    "model": {"model", "модель", "вариант"},
    "dimensions": {"dimensions", "размер", "размеры", "проем", "проём", "замер"},
    "budget": {"budget", "бюджет"},
    "timeline": {"timeline", "срок", "дата"},
    "contact": {"contact", "контакт", "телефон", "мессенджер"},
}
