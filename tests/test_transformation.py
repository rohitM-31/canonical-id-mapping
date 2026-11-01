import pytest

from src.transformation.normalize_data import (
    make_canonical_id,
    make_event_id,
    normalize_email,
    normalize_event_ts,
    normalize_name_key,
    normalize_phone,
)


def test_email_trim_and_lowercase():
    assert normalize_email("  ANNA.Smith5@Example.COM ") == "anna.smith5@example.com"


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_blank_values_normalise_to_none(raw):
    assert normalize_email(raw) is None
    assert normalize_phone(raw) is None
    assert normalize_name_key(raw) is None
    assert normalize_event_ts(raw) is None


@pytest.mark.parametrize("raw", ["2000000021", "200.000.0021", "(200) 000-0021", "1-200-000-0021", "+1 200 000 0021"])
def test_phone_formats_converge(raw):
    assert normalize_phone(raw) == "2000000021"


@pytest.mark.parametrize("raw", ["12ab", "123", "20000000211", "abc"])
def test_bad_phone_is_none(raw):
    assert normalize_phone(raw) is None


@pytest.mark.parametrize("raw", ["Kai A Smith", "KAI A. SMITH", "  kai  a smith ", "Smith, Kai A", "Smith, Kai A."])
def test_name_formats_converge(raw):
    assert normalize_name_key(raw) == "kai smith"


def test_single_token_name_is_too_weak():
    assert normalize_name_key("Kai") is None


def test_timestamp_normalised_to_utc():
    assert normalize_event_ts("2025-01-01T10:00:00+02:00") == "2025-01-01T08:00:00Z"
    assert normalize_event_ts("2025-01-01T08:00:00Z") == "2025-01-01T08:00:00Z"


@pytest.mark.parametrize("raw", ["not-a-timestamp", "2025-01-01 10:00:00", "2025-13-45T00:00:00Z"])
def test_bad_or_naive_timestamp_is_none(raw):
    assert normalize_event_ts(raw) is None


def test_ids_are_deterministic():
    assert make_canonical_id("a@b.com") == make_canonical_id("a@b.com")
    assert make_canonical_id("a@b.com") != make_canonical_id("c@b.com")
    assert make_event_id("x", " y ") == make_event_id("x", "y")  # stripped before hashing
