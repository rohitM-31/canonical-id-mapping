from src.validation.validate_data import validate_record


def row(**over):
    base = {"source_system": "crm", "source_id": "crm-00000001", "event_ts": "2025-01-01T00:00:00Z",
            "email": "a@b.com", "phone": "(200) 000-0001", "full_name": "A B"}
    base.update(over)
    return base


def test_valid_record():
    assert validate_record(row()) == []


def test_all_optional_identifiers_may_be_blank():
    assert validate_record(row(email="", phone="", full_name="")) == []


def test_missing_required_fields():
    assert validate_record(row(source_system="")) == ["missing_source_system"]
    assert validate_record(row(source_id="")) == ["missing_source_id"]
    assert validate_record(row(event_ts="")) == ["invalid_event_ts"]


def test_unknown_system_and_prefix_mismatch():
    assert "unknown_source_system" in validate_record(row(source_system="legacy_erp"))
    assert validate_record(row(source_system="web")) == ["source_id_system_mismatch"]


def test_bad_formats():
    assert validate_record(row(source_id="crm-12")) == ["invalid_source_id_format"]
    assert validate_record(row(email="not-an-email")) == ["invalid_email"]
    assert validate_record(row(phone="12ab")) == ["invalid_phone"]
    assert validate_record(row(event_ts="not-a-timestamp")) == ["invalid_event_ts"]


def test_all_reasons_are_reported():
    reasons = validate_record(row(source_id="", email="x", phone="1"))
    assert reasons == ["missing_source_id", "invalid_email", "invalid_phone"]


def test_malformed_row_flag():
    assert validate_record({"_malformed": True}) == ["malformed_row"]
