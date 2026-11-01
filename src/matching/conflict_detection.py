"""Pure functions that turn a source key's mapped events into history intervals and conflicts."""
from typing import Dict, List, Optional, Sequence, Tuple

# event = (event_ts, event_id, canonical_id)
Event = Tuple[str, str, str]


def build_history(events: Sequence[Event]) -> List[Dict]:
    """Collapse events (any arrival order) into consecutive same-canonical intervals.

    Sorting by (event_ts, event_id) makes the result independent of load order, so late-arriving data and
    re-runs yield the same history.
    """
    intervals: List[Dict] = []
    for event_ts, _event_id, canonical_id in sorted(events):
        if intervals and intervals[-1]["canonical_id"] == canonical_id:
            intervals[-1]["event_count"] += 1
            continue
        if intervals:
            intervals[-1]["valid_to"] = event_ts
        intervals.append({"canonical_id": canonical_id, "valid_from": event_ts, "valid_to": None, "event_count": 1})
    for i in intervals:
        i["is_current"] = 1 if i["valid_to"] is None else 0
    return intervals


def detect_one_to_many(events: Sequence[Event]) -> Optional[Dict]:
    """A source key that maps to more than one canonical id is a one-to-many conflict."""
    canonical_ids = sorted({e[2] for e in events})
    if len(canonical_ids) < 2:
        return None
    timestamps = [e[0] for e in events]
    return {
        "conflict_type": "one_to_many",
        "canonical_count": len(canonical_ids),
        "canonical_ids": ",".join(canonical_ids),
        "first_seen": min(timestamps),
        "last_seen": max(timestamps),
    }
