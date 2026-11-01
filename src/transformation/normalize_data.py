"""Deterministic normalisation of identifiers. Every function is pure and returns None for blank/unusable input."""
import hashlib
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Optional

EMAIL_RE = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9\-]+(\.[a-z0-9\-]+)+$")
PHONE_ALLOWED_RE = re.compile(r"^[\d\s().+\-]+$")


def clean(value: Optional[str]) -> str:
    return (value or "").strip()


@lru_cache(maxsize=None)
def normalize_email(raw: Optional[str]) -> Optional[str]:
    """trim + lowercase. Returns None when blank."""
    value = clean(raw).lower()
    return value or None


def is_valid_email(email_norm: str) -> bool:
    return bool(EMAIL_RE.match(email_norm))


@lru_cache(maxsize=None)
def normalize_phone(raw: Optional[str]) -> Optional[str]:
    """Keep digits only, drop a leading US country code. Returns None if blank or not a 10-digit number.

    '(200) 000-0021', '1-200-000-0021', '+1 200 000 0021', '200.000.0021' -> '2000000021'
    """
    value = clean(raw)
    if not value or not PHONE_ALLOWED_RE.match(value):
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else None


@lru_cache(maxsize=None)
def normalize_name_key(raw: Optional[str]) -> Optional[str]:
    """Order/case/punctuation-insensitive name key: first + last token(s), middle initials dropped.

    'Smith, Kai A', 'KAI A. SMITH', '  kai  a smith ' -> 'kai smith'.
    Needs at least two tokens, otherwise the name is too weak to match on (None).
    """
    value = clean(raw)
    if not value:
        return None
    if "," in value:
        last, _, rest = value.partition(",")
        value = f"{rest} {last}"
    value = value.lower().replace(".", " ")
    tokens = [t for t in re.split(r"\s+", value) if len(t) > 1]
    return " ".join(tokens) if len(tokens) >= 2 else None


def normalize_event_ts(raw: Optional[str]) -> Optional[str]:
    """Parse an ISO-8601 timestamp with timezone and return it as UTC 'YYYY-MM-DDTHH:MM:SSZ'. Naive/bad -> None."""
    value = clean(raw)
    if not value:
        return None
    if value.endswith(("Z", "z")):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_canonical_id(email_norm: str) -> str:
    """Stable id derived from the master's normalised email."""
    return "can_" + hashlib.sha1(email_norm.encode("utf-8")).hexdigest()[:10]


def make_event_id(*raw_fields: Optional[str]) -> str:
    """Stable id for a source record = hash of its stripped raw fields (so identical rows collapse)."""
    joined = "\x1f".join(clean(f) for f in raw_fields)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()
