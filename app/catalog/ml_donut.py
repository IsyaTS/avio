from __future__ import annotations

"""Donut-based OCR fallback for low-confidence pages.

References:
    * Donut (Document Understanding Transformer) - https://github.com/clovaai/donut (MIT License)
    * OCRmyPDF requirements for upstream OCR step are documented under: https://ocrmypdf.readthedocs.io/
"""

import io
import json
import logging
from typing import Dict, List, Tuple

from PIL import Image

from .ml_pipeline import normalize_price_candidate
from .text_normalize import collapse_spaces, normalize_unicode_nfkc

logger = logging.getLogger(__name__)


class DonutFallback:
    """Optional Donut-based pass that tries to structure pages lacking prices."""

    def __init__(self, model_name: str = "naver-clova-ix/donut-base") -> None:
        self._model = None
        self._processor = None
        self._torch = None
        self._device = None
        self.model_name = model_name
        self._try_initialize()

    def _try_initialize(self) -> None:
        try:
            from transformers import DonutProcessor, VisionEncoderDecoderModel  # type: ignore
            import torch  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            logger.warning("donut_dependencies_missing", exc_info=exc)
            return
        try:
            self._processor = DonutProcessor.from_pretrained(self.model_name)
            self._model = VisionEncoderDecoderModel.from_pretrained(self.model_name)
            self._torch = torch
            self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self._model.to(self._device)
        except Exception as exc:  # pragma: no cover - model download failures/offline env
            logger.warning("donut_model_init_failed", exc_info=exc)
            self._model = None
            self._processor = None
            self._torch = None
            self._device = None

    @property
    def available(self) -> bool:
        return self._model is not None and self._processor is not None and self._torch is not None

    def process(self, page_images: Dict[int, bytes]) -> Tuple[List[Dict[str, str]], List[int]]:
        """Run Donut on provided page images and return rescued items + page numbers."""

        if not self.available or not page_images:
            return [], []

        rescued_items: List[Dict[str, str]] = []
        rescued_pages: List[int] = []

        for page_num, payload in page_images.items():
            try:
                image = Image.open(io.BytesIO(payload)).convert("RGB")
            except Exception:
                continue
            data = self._infer(image)
            if not isinstance(data, dict):
                continue
            title = self._clean_text(str(data.get("title") or f"Page {page_num}"))
            price_candidate = normalize_price_candidate(str(data.get("price")), page_num)
            if not price_candidate:
                continue
            attrs_raw = data.get("attrs")
            attrs = attrs_raw if isinstance(attrs_raw, dict) else {}
            item = {"title": title, "price": price_candidate, "page": str(page_num)}
            for key, value in attrs.items():
                if isinstance(value, (str, int, float)):
                    cleaned = self._clean_text(str(value))
                    if cleaned:
                        item[str(key)] = cleaned
            rescued_items.append(item)
            rescued_pages.append(page_num)

        return rescued_items, rescued_pages

    def _infer(self, image: Image.Image) -> Dict[str, object] | None:
        if not self.available:
            return None
        assert self._processor is not None and self._model is not None and self._torch is not None
        inputs = self._processor(image, return_tensors="pt").to(self._device)  # type: ignore[arg-type]
        task_prompt = "<s_donut><s_task>extract key information<s_output>"
        prompt_ids = self._processor.tokenizer(  # type: ignore[attr-defined]
            task_prompt,
            add_special_tokens=False,
            return_tensors="pt",
        ).input_ids.to(self._device)
        outputs = self._model.generate(
            **inputs,
            decoder_input_ids=prompt_ids,
            max_length=512,
        )
        decoded = self._processor.batch_decode(outputs, skip_special_tokens=True)[0]
        cleaned = decoded.strip()
        try:
            return json.loads(cleaned)
        except Exception:
            try:
                return self._processor.token2json(cleaned)
            except Exception:
                return None

    def _clean_text(self, text: str | None) -> str:
        if not text:
            return ""
        normalized = normalize_unicode_nfkc(text)
        normalized = collapse_spaces(normalized)
        return normalized
