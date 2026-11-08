"""Hàm Python thuần. Không tự đổi đơn vị, học phí, mã ngành hay địa giới."""
from datetime import date, datetime
import hashlib
import json
import math
import re
import unicodedata


def clean_text(value) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = unicodedata.normalize("NFC", str(value))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalize_text(value, *, remove_accents: bool = False) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    text = text.casefold()
    if remove_accents:
        text = "".join(c for c in unicodedata.normalize("NFD", text)
                       if unicodedata.category(c) != "Mn").replace("đ", "d")
    return text


def parse_date(value, formats=("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y")) -> date | None:
    """Không suy đoán timestamp, ngày tương đối, MM/DD/YYYY hay chỉ có năm."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = clean_text(value)
    if text is None:
        return None
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def stable_id(*parts: str | int | None, namespace: str) -> str:
    """SHA-256 đầy đủ, serialization không nhập nhằng; caller chuẩn hóa trước.

    Không tương đương surrogate key số hay khóa nghiệp vụ đã có trong dự án.
    None khác chuỗi rỗng; 1 khác '1'. Không đổi quy tắc sau khi phát hành ID.
    """
    if not namespace or not parts or all(p is None or p == "" for p in parts):
        raise ValueError("ID cần namespace và ít nhất một thành phần định danh")
    if any(p is not None and type(p) not in {str, int} for p in parts):
        raise TypeError("Thành phần ID chỉ chấp nhận str, int hoặc None")
    payload = json.dumps([namespace, list(parts)], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
