"""Shared Unicode, null, date, numeric, and typed-column normalization."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import math
import re
import unicodedata
from collections.abc import Mapping


DEFAULT_NULL_MARKERS = frozenset({"", "null", "none", "n/a", "nan", "<na>"})
CODE_NULL_MARKERS = frozenset({""})
DEFAULT_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d")


class NormalizationError(ValueError):
    """A non-null source value could not be normalized to its declared type."""


def normalize_unicode(value, *, form: str = "NFC"):
    """Normalize Unicode representation without compatibility-folding identifiers."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Expected string for Unicode normalization, got {type(value).__name__}")
    if form not in {"NFC", "NFD", "NFKC", "NFKD"}:
        raise ValueError("form must be NFC, NFD, NFKC, or NFKD")
    return unicodedata.normalize(form, value)


def normalize_null(value, *, markers: frozenset[str] = DEFAULT_NULL_MARKERS):
    """Map empty/null marker values (including NaN) to None; leave other values typed."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, Decimal) and value.is_nan():
        return None
    if isinstance(value, str):
        normalized = normalize_unicode(value)
        compact = re.sub(r"\s+", " ", normalized).strip()
        if compact.casefold() in markers:
            return None
        return value
    return value


def normalize_whitespace(value, *, preserve_newlines: bool = False):
    """Normalize Unicode and whitespace; code points and punctuation remain intact."""
    value = normalize_null(value, markers=CODE_NULL_MARKERS)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"Expected string for whitespace normalization, got {type(value).__name__}")
    value = normalize_unicode(value)
    value = "".join(" " if unicodedata.category(ch) == "Cc" else ch for ch in value)
    if preserve_newlines:
        value = value.replace("\r\n", "\n").replace("\r", "\n")
        lines = [re.sub(r"[\t\f\v ]+", " ", line).strip() for line in value.split("\n")]
        result = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
    else:
        result = re.sub(r"\s+", " ", value).strip()
    return result or None


def normalize_text(value, *, casefold: bool = False, null_markers=DEFAULT_NULL_MARKERS):
    """Normalize human-readable text while preserving accents and punctuation."""
    value = normalize_null(value, markers=frozenset(null_markers))
    if value is None:
        return None
    result = normalize_whitespace(value)
    return result.casefold() if result is not None and casefold else result


def normalize_identifier(value, *, casefold: bool = False):
    """Normalize an identifier without parsing it as a number or stripping suffixes."""
    value = normalize_null(value, markers=CODE_NULL_MARKERS)
    if value is None:
        return None
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise TypeError("Identifiers must be strings or integers")
    result = normalize_whitespace(str(value))
    return result.casefold() if result is not None and casefold else result


def normalize_code(value, *, casefold: bool = False):
    """Code normalization preserves leading zeroes, punctuation, and alpha suffixes."""
    return normalize_identifier(value, casefold=casefold)


def parse_date(value, *, formats=DEFAULT_DATE_FORMATS):
    """Parse absolute dates only; ambiguous US dates and relative phrases are rejected."""
    value = normalize_null(value)
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError(f"Expected date/string, got {type(value).__name__}")
    text = normalize_whitespace(value)
    if text is None:
        return None
    # Remove only a parenthesized annotation after a complete absolute date.
    match = re.fullmatch(r"(.+?)(?:\s*\([^\n]*\))?", text)
    token = match.group(1).strip() if match else text
    for date_format in formats:
        try:
            return datetime.strptime(token, date_format).date()
        except ValueError:
            pass
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}.*", token):
            return datetime.fromisoformat(token.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    return None


def normalize_date(value, *, formats=DEFAULT_DATE_FORMATS, output: str = "date"):
    parsed = parse_date(value, formats=formats)
    if parsed is None:
        return None
    if output == "date":
        return parsed
    if output == "iso":
        return parsed.isoformat()
    raise ValueError("output must be 'date' or 'iso'")


def parse_decimal(value, *, locale: str = "vi", allow_suffix: bool = False) -> Decimal | None:
    """Parse a decimal using an explicit locale; never infer numeric meaning for codes.

    ``vi`` uses comma decimals and dot grouping, while ``en`` uses dot decimals
    and comma grouping. If both separators occur, the rightmost is the decimal
    separator. Alphabetic/currency suffixes require ``allow_suffix=True``.
    """
    value = normalize_null(value)
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("Boolean values are not numeric")
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    if not isinstance(value, str):
        raise TypeError(f"Expected number/string, got {type(value).__name__}")
    if locale not in {"vi", "en"}:
        raise ValueError("locale must be 'vi' or 'en'")

    text = normalize_unicode(value).strip().replace("−", "-")
    if not text:
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1].strip()

    allowed_prefix = ""
    if allow_suffix:
        start = next((index for index, char in enumerate(text) if char.isdigit()), None)
        if start is None:
            return None
        allowed_prefix, text = text[:start].strip(), text[start:]
        if allowed_prefix and not re.fullmatch(r"[\w$€£¥₫đĐ+\-\s]+", allowed_prefix, re.UNICODE):
            return None
        if allowed_prefix.rstrip().endswith("-"):
            text = "-" + text
        elif allowed_prefix.rstrip().endswith("+"):
            text = "+" + text

    match = re.match(r"^[+-]?[\d.,\s]+", text)
    if not match:
        return None
    token, suffix = match.group(0).strip(), text[match.end():].strip()
    if suffix and (not allow_suffix or not re.fullmatch(r"[^\d]+", suffix)):
        return None
    token = re.sub(r"\s+", "", token)
    if not token or not re.fullmatch(r"[+-]?[\d.,]+", token):
        return None

    sign = ""
    if token[:1] in {"+", "-"}:
        sign, token = token[0], token[1:]
    if token.count(".") and token.count(","):
        decimal_separator = "." if token.rfind(".") > token.rfind(",") else ","
        grouping_separator = "," if decimal_separator == "." else "."
        token = token.replace(grouping_separator, "")
        if token.count(decimal_separator) != 1:
            return None
        token = token.replace(decimal_separator, ".")
    else:
        separator = "." if "." in token else "," if "," in token else None
        if separator:
            count = token.count(separator)
            expected_decimal = "," if locale == "vi" else "."
            expected_group = "." if locale == "vi" else ","
            chunks = token.split(separator)
            grouped = len(chunks) > 1 and all(len(part) == 3 for part in chunks[1:]) and len(chunks[0]) <= 3
            if separator == expected_group and grouped:
                token = "".join(chunks)
            elif count > 1:
                if grouped:
                    token = "".join(chunks)
                else:
                    return None
            elif separator == expected_decimal:
                token = token.replace(separator, ".")
            elif grouped:
                token = "".join(chunks)
            else:
                token = token.replace(separator, ".")
    if not re.fullmatch(r"\d+(?:\.\d+)?", token):
        return None
    try:
        result = Decimal(sign + token)
    except InvalidOperation:
        return None
    if negative:
        result = -result
    return result if result.is_finite() else None


def normalize_number(value, *, locale: str = "vi", allow_suffix: bool = False):
    return parse_decimal(value, locale=locale, allow_suffix=allow_suffix)


def normalize_integer(value, *, locale: str = "vi", allow_suffix: bool = False) -> int | None:
    number = parse_decimal(value, locale=locale, allow_suffix=allow_suffix)
    if number is None:
        return None
    if number != number.to_integral_value():
        return None
    return int(number)


def normalize_boolean(value):
    value = normalize_null(value)
    if value is None:
        return None
    if type(value) is bool:
        return value
    if isinstance(value, (int, Decimal)) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        text = normalize_whitespace(value).casefold()
        if text in {"1", "true", "yes", "y", "có", "co", "x"}:
            return True
        if text in {"0", "false", "no", "n", "không", "khong"}:
            return False
    return None


_NORMALIZERS = {
    "text": normalize_text,
    "whitespace": normalize_whitespace,
    "code": normalize_code,
    "identifier": normalize_identifier,
    "date": normalize_date,
    "number": normalize_number,
    "decimal": normalize_number,
    "integer": normalize_integer,
    "boolean": normalize_boolean,
    "null": normalize_null,
    "unicode": normalize_unicode,
}


def normalize_record(record: Mapping, column_types: Mapping, *, preserve_raw: bool = True,
                    raw_suffix: str = "_raw", strict: bool = True) -> dict:
    """Normalize declared fields and retain each input under ``<field>_raw``.

    ``column_types`` accepts a type name or a mapping such as
    ``{"type": "number", "locale": "vi", "allow_suffix": True}``.
    """
    result = dict(record)
    for column, declaration in column_types.items():
        if column not in record:
            continue
        options = {"type": declaration} if isinstance(declaration, str) else dict(declaration)
        kind = options.pop("type", None)
        normalizer = _NORMALIZERS.get(kind)
        if normalizer is None:
            raise ValueError(f"Unknown normalization type for {column!r}: {kind!r}")
        raw_value = record[column]
        kwargs = {key: value for key, value in options.items() if key not in {"required"}}
        try:
            normalized = normalizer(raw_value, **kwargs)
        except (TypeError, ValueError) as exc:
            if strict:
                raise NormalizationError(f"Cannot normalize column {column!r} as {kind}") from exc
            normalized = None
        required = bool(options.get("required", False))
        is_null = normalize_null(raw_value) is None
        if strict and not is_null and normalized is None:
            raise NormalizationError(f"Invalid non-null value in column {column!r} for type {kind}")
        if required and normalized is None:
            raise NormalizationError(f"Required column {column!r} normalized to NULL")
        if preserve_raw:
            raw_column = column + raw_suffix
            if raw_column in record and record[raw_column] != raw_value:
                raise ValueError(f"Raw column collision for {column!r}")
            result[raw_column] = raw_value
        result[column] = normalized
    return result
