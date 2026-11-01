"""Row-level validation. Returns every reason a record is invalid (empty list = valid)."""
import re
from typing import Dict, List

from config import config
from src.transformation.normalize_data import (
    clean,
    is_valid_email,
    normalize_email,
    normalize_event_ts,
    normalize_phone,
)

_SOURCE_ID_RE = re.compile(config.SOURCE_ID_PATTERN)


def validate_record(row: Dict[str, str]) -> List[str]:
    reasons: List[str] = []

    if row.get("_malformed"):
        return ["malformed_row"]

    system = clean(row.get("source_system"))
    source_id = clean(row.get("source_id"))

    if not system:
        reasons.append("missing_source_system")
    elif system not in config.VALID_SOURCE_SYSTEMS:
        reasons.append("unknown_source_system")

    if not source_id:
        reasons.append("missing_source_id")
    else:
        match = _SOURCE_ID_RE.match(source_id)
        if not match:
            reasons.append("invalid_source_id_format")
        elif system in config.VALID_SOURCE_SYSTEMS and match["system"] != system:
            reasons.append("source_id_system_mismatch")

    if normalize_event_ts(row.get("event_ts")) is None:
        reasons.append("invalid_event_ts")

    email = normalize_email(row.get("email"))
    if email is not None and not is_valid_email(email):
        reasons.append("invalid_email")

    if clean(row.get("phone")) and normalize_phone(row.get("phone")) is None:
        reasons.append("invalid_phone")

    return reasons
