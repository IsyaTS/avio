from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class InstructionRuntimeDeps:
    normalize_text: Callable[[Any], str]
    sentence_split_re: Any
    instruction_leak_line_re: Any
    instruction_list_line_re: Any
    shortlist_leak_re: Any


class InstructionRuntime:
    def __init__(self, deps: InstructionRuntimeDeps) -> None:
        self.deps = deps

    def is_operator_like_question(self, question: str) -> bool:
        low = self.deps.normalize_text(question)
        if not low:
            return False
        if re.search(
            r"(?iu)\b(предложите|предложи|напишите|позвоните|перейдите|"
            r"переведите|зафиксируйте|оформите|согласуйте|свяжитесь)\b",
            low,
        ):
            return True
        if re.search(r"(?iu)\bи\b", low) and re.search(
            r"(?iu)\b(предлож|напиш|перевед|соглас|оформ|подтверд)\w*",
            low,
        ):
            return True
        return False

    def is_operator_instruction_sentence(self, text: str) -> bool:
        low = self.deps.normalize_text(text)
        if not low:
            return False
        if not re.search(r"(?iu)\b(сначала|затем|потом|далее|после\s+этого|в\s+этом\s+же\s+ответе)\b", low):
            return False
        has_delivery_hint = bool(
            re.search(
                r"(?iu)\b(отдельн\w*\s+сообщени\w*|только\s+ссылк\w*|"
                r"только\s+номер\w*|ссылк\w*|контакт\w*|телефон\w*|username|@)\b",
                low,
            )
        )
        if not has_delivery_hint:
            return False
        if "?" in str(text or ""):
            return False
        return True

    def is_response_format_instruction_sentence(self, text: str) -> bool:
        low = self.deps.normalize_text(text)
        if not low:
            return False
        if "?" in str(text or ""):
            return False
        if re.fullmatch(r"(?iu)\s*не\s+одн\w*\s+строк\w*\s*\.?\s*", low):
            return True
        if re.search(
            r"(?iu)\b(отвечайт\w*|пишит\w*|напишит\w*)\b[^.?!]{0,64}\b"
            r"(развернут\w*|подробн\w*|односложн\w*|коротк\w*|одн\w*\s+строк\w*)\b",
            low,
        ):
            return True
        return False

    def is_sequence_process_instruction_sentence(self, text: str) -> bool:
        low = self.deps.normalize_text(text)
        if not low:
            return False
        if "?" in str(text or ""):
            return False
        if not re.search(r"(?iu)\b(сначала|затем|потом|далее|после\s+этого)\b", low):
            return False
        if not re.search(
            r"(?iu)\b(уточняйте|давайте|следуйте|спрашивайте|задавайте|предлагайте|предложите|фиксируйте|собирайте)\b",
            low,
        ):
            return False
        if re.search(
            r"(?iu)\b(ответ|скрипт|сценари\w*|персон\w*|географ\w*|этап\w*|шаг\w*|логик\w*|правил\w*)\b",
            low,
        ):
            return True
        return False

    def strip_embedded_operator_tail(self, text: str) -> str:
        candidate = str(text or "").strip()
        if not candidate:
            return candidate
        out = candidate
        out = re.sub(
            r"(?iu)[,;:\-\s]*(поздоровайт\w*|поприветствуйт\w*)\b[^.?!\n]*$",
            "",
            out,
        )
        out = re.sub(
            r"(?iu)[,;:\-\s]*(скажите|спросите|уточните|напишите)\s+что\b[^.?!\n]*$",
            "",
            out,
        )
        out = re.sub(
            r"(?iu)[,;:\-\s]*давайте\s+ответ\b[^.?!\n]*$",
            "",
            out,
        )
        out = re.sub(
            r"(?iu)[,;:\-\s]*ответ\s+строго\b[^.?!\n]*$",
            "",
            out,
        )
        out = re.sub(r"\s{2,}", " ", out).strip(" ,;:-")
        return out

    def strip_instruction_leaks(self, text: str) -> str:
        candidate = str(text or "")
        if not candidate.strip():
            return ""
        candidate = re.sub(r"<\s*[^>\n]{1,40}\s*>", "", candidate)
        candidate = candidate.replace("`", " ")
        out = self.deps.instruction_leak_line_re.sub("", candidate)
        out = self.deps.shortlist_leak_re.sub("", out)
        out = re.sub(
            r"(?iu)\bсначала\b[^.?!\n]{0,220}\bотдельн\w*\s+сообщени\w*[^.?!\n]*",
            "",
            out,
        )
        out = re.sub(
            r"(?iu)[,;:\-\s]*\bотвечайт\w*\s+(?:разв[её]рнут\w*|подробн\w*|не\s+односложн\w*)\b[.!?]*",
            "",
            out,
        )
        out = re.sub(
            r"(?iu)[,;:\-\s]*\bне\s+одн\w*\s+строк\w*\b[.!?]*",
            "",
            out,
        )
        out = self.strip_embedded_operator_tail(out)
        out = re.sub(
            r"(?iu)\bпосле\s+приветствия\s+последовательно\s+уточни:?\s*",
            "",
            out,
        )
        out = re.sub(
            r"(?iu)[,;:\-\s]*\bотдельн\w*\s+сообщени\w*\b[^.?!\n]*",
            "",
            out,
        )
        out = re.sub(
            r"(?iu)\bв\s+этом\s+же\s+ответе\s+[^.?!\n]*(?:[.?!]|$)",
            "",
            out,
        )
        lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
        cleaned_lines: list[str] = []
        for line in lines:
            if self.deps.instruction_list_line_re.match(line):
                continue
            cleaned_lines.append(line)
        out = "\n".join(cleaned_lines)
        parts = [
            part.strip()
            for part in re.split(r"(?<=[.!?])\s+|\n+", out)
            if part.strip()
        ]
        if parts:
            filtered_parts: list[str] = []
            for part in parts:
                low = part.lower().replace("ё", "е")
                if self.is_operator_instruction_sentence(part):
                    continue
                if self.is_response_format_instruction_sentence(part):
                    continue
                if self.is_sequence_process_instruction_sentence(part):
                    continue
                if ("?" not in part) and re.match(r"(?iu)^\s*(поздоровайт\w*|поприветствуйт\w*)\b", low):
                    continue
                if ("?" not in part) and re.match(
                    r"(?iu)^\s*(скажите|спросите|уточните|напишите)\s+что\b",
                    low,
                ):
                    continue
                part = self.strip_embedded_operator_tail(part)
                if not part:
                    continue
                low = part.lower().replace("ё", "е")
                if re.search(
                    r"(?iu)\b(без\s+лишних\s+уточнен|без\s+повтора|дайте\s+адрес\s+сразу|"
                    r"при\s+известном\s+городе)\b",
                    low,
                ):
                    continue
                if re.search(r"(?iu)^(pdf|пдф)\b.*\b(каталог|ссылк)\b", low):
                    continue
                if re.match(
                    r"(?iu)^\s*(честно\s+)?(сообщайте|предлагайте|предложите|уточните|спросите|попросите|"
                    r"примите|извлеките|фиксируйте|передайте|дайте)\b",
                    low,
                ) and ("?" not in part):
                    continue
                filtered_parts.append(part)
            out = " ".join(filtered_parts).strip()
        out = re.sub(r"\s{2,}", " ", out)
        out = re.sub(r"\s+([,.;:!?])", r"\1", out)
        out = re.sub(
            r"(?iu)\b(работаем\s+по\s+каталогу\s+и\s+выездом)\s*,?\s*без\s+адресов\s+магазин\w*\b",
            r"\1",
            out,
        )
        out = re.sub(r"(?iu)\bот\s+цена\s+по\s+каталогу\b", "цена по каталогу", out)
        out = re.sub(r"(?iu)\bза\s+цена\s+по\s+каталогу\b", "цена по каталогу", out)
        out = re.sub(r"(?iu)\bв\s+цена\s+по\s+каталогу\b", "цена по каталогу", out)
        out = re.sub(r"(?iu)\bк\s+цена\s+по\s+каталогу\b", "цена по каталогу", out)
        catalog_price_placeholder = "цена по каталогу"
        if catalog_price_placeholder in out.lower().replace("ё", "е"):
            sentence_parts = [
                part.strip() for part in self.deps.sentence_split_re.split(out) if part.strip()
            ]
            if sentence_parts:
                rewritten_parts: list[str] = []
                has_placeholder_sentence = False
                for part in sentence_parts:
                    low = part.lower().replace("ё", "е")
                    if catalog_price_placeholder in low:
                        needs_rewrite = bool(
                            re.search(
                                r"(?iu)\b(обойд|будет|итог|примерно|с\s+уч[её]том\s+скид|"
                                r"после\s+скид|со\s+скидк)\w*\b",
                                low,
                            )
                        )
                        if needs_rewrite:
                            has_placeholder_sentence = True
                            rewritten_parts.append("Точную цену по выбранной модели уточню и сразу напишу.")
                            continue
                    rewritten_parts.append(part)
                if has_placeholder_sentence:
                    final_parts: list[str] = []
                    for part in rewritten_parts:
                        low = part.lower().replace("ё", "е")
                        if ("?" in part) and re.search(
                            r"(?iu)\b(оформ|подтверд|берем|берём|заказ)\w*\b",
                            low,
                        ):
                            continue
                        final_parts.append(part)
                    out = " ".join(final_parts).strip()
        out = re.sub(r"(?u)[\"«»]\s*([,.;:!?])", r"\1", out)
        out = re.sub(r"(?u)[\"«»]+\s*$", "", out)
        out = re.sub(r"(?u)\s+[\"«»]+\s*$", "", out)
        quote_count = out.count("\"") + out.count("«") + out.count("»")
        if quote_count % 2 != 0:
            out = out.replace("\"", "").replace("«", "").replace("»", "")
        out = re.sub(r"(\d)\)(?=\s|$)", r"\1", out)
        out = re.sub(r"\n{3,}", "\n\n", out).strip()
        return out
