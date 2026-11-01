import pytest

from src.matching.matching_rules import MasterIndex, resolve


@pytest.fixture
def index():
    return MasterIndex([
        {"canonical_id": "A", "email_norm": "a@x.com", "phone_norm": "2000000001", "name_key": "alice jones"},
        {"canonical_id": "B", "email_norm": "b@x.com", "phone_norm": "2000000002", "name_key": "bob brown"},
        {"canonical_id": "D1", "email_norm": "d1@x.com", "phone_norm": "2000000004", "name_key": "dan lee"},
        {"canonical_id": "D2", "email_norm": "d2@x.com", "phone_norm": "2000000005", "name_key": "dan lee"},
    ])


def test_exact_email(index):
    r = resolve("a@x.com", None, None, index)
    assert (r.status, r.canonical_id, r.rule) == ("mapped", "A", "exact_email")


def test_phone_rule(index):
    r = resolve(None, "2000000002", None, index)
    assert (r.canonical_id, r.rule) == ("B", "rule_phone")


def test_name_rule(index):
    r = resolve(None, None, "bob brown", index)
    assert (r.canonical_id, r.rule) == ("B", "rule_name")


def test_email_has_precedence_when_all_agree(index):
    r = resolve("a@x.com", "2000000001", "alice jones", index)
    assert (r.canonical_id, r.rule) == ("A", "exact_email")


def test_conflicting_identifiers_are_not_guessed(index):
    r = resolve("a@x.com", "2000000002", None, index)
    assert (r.status, r.canonical_id, r.reason) == ("unresolved", None, "conflicting_identifiers")


def test_ambiguous_name_is_unresolved(index):
    r = resolve(None, None, "dan lee", index)
    assert (r.status, r.reason) == ("unresolved", "ambiguous_name")


def test_ambiguous_name_resolved_by_stronger_identifier(index):
    r = resolve("d2@x.com", None, "dan lee", index)
    assert (r.status, r.canonical_id) == ("mapped", "D2")


def test_no_match_and_no_identifiers(index):
    assert resolve("zed@nomatch.test", None, None, index).reason == "no_match"
    assert resolve(None, None, None, index).reason == "no_identifiers"
