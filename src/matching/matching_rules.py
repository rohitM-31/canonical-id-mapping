"""Exact and rule-based matching of a source record to a canonical customer.

Rules, in order of precedence:
  1. exact_email : normalised email equals a master email (exact)
  2. rule_phone  : normalised 10-digit phone equals a master phone (format-insensitive)
  3. rule_name   : normalised name key equals a master name key (order/case/punctuation-insensitive)

All identifiers present on the record are evaluated. If they point at different canonical ids the record is
NOT mapped (`conflicting_identifiers`) - guessing would silently create wrong links.
"""
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Set

RULE_ORDER = ("exact_email", "rule_phone", "rule_name")


@dataclass(frozen=True)
class MatchResult:
    status: str  # mapped | unresolved
    canonical_id: Optional[str] = None
    rule: Optional[str] = None
    reason: Optional[str] = None


class MasterIndex:
    """In-memory hash indexes over the canonical master (O(1) lookups instead of one SQL query per record)."""

    def __init__(self, customers: Iterable[dict]):
        self.by_email: Dict[str, Set[str]] = defaultdict(set)
        self.by_phone: Dict[str, Set[str]] = defaultdict(set)
        self.by_name: Dict[str, Set[str]] = defaultdict(set)
        self.size = 0
        for c in customers:
            self.size += 1
            self.by_email[c["email_norm"]].add(c["canonical_id"])
            if c["phone_norm"]:
                self.by_phone[c["phone_norm"]].add(c["canonical_id"])
            if c["name_key"]:
                self.by_name[c["name_key"]].add(c["canonical_id"])

    @classmethod
    def from_db(cls, conn) -> "MasterIndex":
        rows = conn.execute("SELECT canonical_id, email_norm, phone_norm, name_key FROM canonical_customer")
        return cls(dict(r) for r in rows)


def resolve(email_norm: Optional[str], phone_norm: Optional[str], name_key: Optional[str],
            index: MasterIndex) -> MatchResult:
    if not (email_norm or phone_norm or name_key):
        return MatchResult("unresolved", reason="no_identifiers")

    lookups = (
        ("exact_email", "email", email_norm, index.by_email),
        ("rule_phone", "phone", phone_norm, index.by_phone),
        ("rule_name", "name", name_key, index.by_name),
    )
    unique_hits: Dict[str, str] = {}
    ambiguous_fields = []
    for rule, label, value, table in lookups:
        if not value:
            continue
        ids = table.get(value, ())
        if len(ids) == 1:
            unique_hits[rule] = next(iter(ids))
        elif len(ids) > 1:
            ambiguous_fields.append(label)

    distinct = set(unique_hits.values())
    if len(distinct) > 1:
        return MatchResult("unresolved", reason="conflicting_identifiers")
    if len(distinct) == 1:
        rule = next(r for r in RULE_ORDER if r in unique_hits)
        return MatchResult("mapped", canonical_id=unique_hits[rule], rule=rule)
    if ambiguous_fields:
        return MatchResult("unresolved", reason=f"ambiguous_{ambiguous_fields[0]}")
    return MatchResult("unresolved", reason="no_match")
