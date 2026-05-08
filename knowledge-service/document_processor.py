from __future__ import annotations

import io
import json
import re
from pathlib import Path
from typing import Any

import fitz
import pytesseract
from PIL import Image


class DocumentProcessor:
    def extract_text(self, file_name: str, content: bytes) -> str:
        suffix = Path(file_name).suffix.lower()
        if suffix == ".pdf":
            return self._extract_pdf(content)
        if suffix in {".png", ".jpg", ".jpeg"}:
            return self._extract_image(content)
        return content.decode("utf-8", errors="ignore")

    def _extract_pdf(self, content: bytes) -> str:
        document = fitz.open(stream=content, filetype="pdf")
        pages = [page.get_text("text") for page in document]
        text = "\n".join(pages).strip()
        if text:
            return text
        images = []
        for page in document:
            pix = page.get_pixmap()
            images.append(Image.open(io.BytesIO(pix.tobytes("png"))))
        return "\n".join(pytesseract.image_to_string(image) for image in images)

    def _extract_image(self, content: bytes) -> str:
        image = Image.open(io.BytesIO(content))
        return pytesseract.image_to_string(image)

    def parse_structured_data(self, text: str) -> dict[str, Any]:
        configs = []
        for match in re.finditer(r"(\d\s*BHK).*?(\d{3,4})\s*sq\.?ft.*?(\d+(?:\.\d+)?)\s*lakh", text, flags=re.IGNORECASE | re.DOTALL):
            bhk, area, price = match.groups()
            configs.append({"type": bhk.upper(), "carpet_area_sqft": int(area), "price_lakh": float(price)})
        amenities = re.findall(r"(clubhouse|gym|pool|garden|children.?s play area|parking)", text, flags=re.IGNORECASE)
        return {"configurations": configs, "amenities": sorted({item.lower() for item in amenities}), "raw_excerpt": text[:2000]}

    def serialize_document(self, file_name: str, text: str) -> str:
        parsed = self.parse_structured_data(text)
        return json.dumps({"file_name": file_name, "text": text, "parsed": parsed}, ensure_ascii=True)
