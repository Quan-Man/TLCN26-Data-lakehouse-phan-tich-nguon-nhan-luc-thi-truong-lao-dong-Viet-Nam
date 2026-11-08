"""Stable entity, SCD-version, and content keys with collision checks."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Iterable, Mapping


class KeyCollisionError(ValueError):
    """Raised when one generated key is associated with distinct identities."""


def normalize_natural_key(value, *, casefold: bool = True):
    """Normalize natural-key text recursively (NFC, collapsed whitespace, optional casefold).

    Numeric and string values remain different types. Accents and punctuation are
    preserved so that normalization does not silently merge distinct names.
    """
    if isinstance(value, str):
        normalized = re.sub(r"\s+", " ", unicodedata.normalize("NFC", value)).strip()
        return normalized.casefold() if casefold else normalized
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key.strip():
                raise TypeError("Natural-key mapping field names must be non-empty strings")
            field_name = unicodedata.normalize("NFC", key.strip())
            if field_name in result:
                raise ValueError("Natural-key field names collide after Unicode normalization")
            result[field_name] = normalize_natural_key(item, casefold=casefold)
        return result
    if isinstance(value, (list, tuple)):
        return [normalize_natural_key(item, casefold=casefold) for item in value]
    if isinstance(value, set):
        normalized = [normalize_natural_key(item, casefold=casefold) for item in value]
        return sorted(normalized, key=_serialize)
    return value


def _typed(value):
    """Encode values with explicit type tags before hashing."""
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("NaN and infinity are not valid key/hash values")
        return ["float", value.hex()]
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("NaN and infinity are not valid key/hash values")
        return ["decimal", str(value.normalize())]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat(timespec="microseconds")]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, Mapping):
        pairs = []
        for key in sorted(value):
            if not isinstance(key, str):
                raise TypeError("Mapping keys in key/hash values must be strings")
            pairs.append([key, _typed(value[key])])
        return ["mapping", pairs]
    if isinstance(value, (list, tuple)):
        return ["sequence", [_typed(item) for item in value]]
    raise TypeError(f"Unsupported key/hash value type: {type(value).__name__}")


def _serialize(value) -> str:
    return json.dumps(_typed(value), ensure_ascii=False, separators=(",", ":"))


def _validate_natural_key(value) -> None:
    if value is None or value == "" or value == [] or value == {}:
        raise ValueError("A natural key must contain at least one value")
    values = value.values() if isinstance(value, Mapping) else value if isinstance(value, (list, tuple)) else (value,)
    if not any(item is not None and not (isinstance(item, str) and not item.strip()) for item in values):
        raise ValueError("A natural key must contain at least one non-empty value")
    if isinstance(value, Mapping) and any(item is None or (isinstance(item, str) and not item.strip())
                                          for item in value.values()):
        raise ValueError("Natural-key fields must not be NULL or blank")


class KeyCollisionChecker:
    """Keep key-to-identity associations and detect a digest reused by another identity."""

    def __init__(self):
        self._identities: dict[str, str] = {}

    def register(self, key: str, identity) -> str:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        identity_token = _serialize(identity)
        previous = self._identities.get(key)
        if previous is not None and previous != identity_token:
            raise KeyCollisionError(f"Key collision detected for {key[:16]}")
        self._identities[key] = identity_token
        return key

    def __len__(self) -> int:
        return len(self._identities)


def assert_no_key_collisions(pairs: Iterable[tuple[str, object]]) -> None:
    """Validate a stream of (generated_key, natural_identity) pairs."""
    checker = KeyCollisionChecker()
    for key, identity in pairs:
        checker.register(key, identity)


def object_key(namespace: str, natural_key, *, casefold: bool = True,
               collision_checker: KeyCollisionChecker | None = None) -> str:
    """Create the stable ID for an entity; independent of run, order, or content changes."""
    if not isinstance(namespace, str) or not namespace.strip():
        raise ValueError("namespace must be a non-empty string")
    _validate_natural_key(natural_key)
    normalized = normalize_natural_key(natural_key, casefold=casefold)
    identity = ["object_id", namespace.strip(), normalized]
    key = hashlib.sha256(_serialize(identity).encode("utf-8")).hexdigest()
    if collision_checker is not None:
        collision_checker.register(key, identity)
    return key


def scd_version_key(namespace: str, natural_key, effective_from, *, version=None,
                    casefold: bool = True,
                    collision_checker: KeyCollisionChecker | None = None) -> str:
    """Create a version-row ID from object identity and its valid-time boundary."""
    if effective_from is None or effective_from == "":
        raise ValueError("effective_from is required for an SCD version key")
    parent_id = object_key(namespace, natural_key, casefold=casefold)
    boundary = normalize_natural_key(effective_from, casefold=casefold)
    discriminator = normalize_natural_key(version, casefold=casefold)
    identity = ["scd_version_id", namespace.strip(), parent_id, boundary, discriminator]
    key = hashlib.sha256(_serialize(identity).encode("utf-8")).hexdigest()
    if collision_checker is not None:
        collision_checker.register(key, identity)
    return key


def content_hash(content, *, namespace: str = "content", exclude: Iterable[str] = ()) -> str:
    """Hash canonical content separately from entity and SCD-version identity."""
    if not isinstance(namespace, str) or not namespace.strip():
        raise ValueError("namespace must be a non-empty string")
    if isinstance(content, Mapping):
        excluded = set(exclude)
        content = {key: value for key, value in content.items() if key not in excluded}
    elif tuple(exclude):
        raise ValueError("exclude is only supported for mapping content")
    identity = ["content_hash", namespace.strip(), content]
    return hashlib.sha256(_serialize(identity).encode("utf-8")).hexdigest()
