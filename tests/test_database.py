import sqlite3

import pytest

from src.database.database import init_schema
from src.mapping.canonical_mapping import upsert_canonical_customers


def test_schema_init_is_idempotent(db):
    init_schema(db)
    init_schema(db)
    assert db.execute("SELECT COUNT(*) FROM canonical_customer").fetchone()[0] == 5


def test_master_reload_does_not_duplicate(db):
    upsert_canonical_customers(db, [{"email": "ALICE.ONE@example.com", "phone": "x", "full_name": "Alice B Jones"}], "r2")
    assert db.execute("SELECT COUNT(*) FROM canonical_customer").fetchone()[0] == 5


def test_master_requires_email_and_name(db):
    with pytest.raises(ValueError):
        upsert_canonical_customers(db, [{"email": "", "full_name": "x"}], "r")


def _event(canonical_id, status="mapped", reason=None):
    return ("e1", "crm", "crm-00000001", "2025-01-01T00:00:00Z", canonical_id, status, reason, "r1", "f.csv", 1)


INSERT = ("INSERT INTO source_event (event_id, source_system, source_id, event_ts, canonical_id, match_status, "
          "unresolved_reason, run_id, source_file, source_row_number) VALUES (?,?,?,?,?,?,?,?,?,?)")


def test_invalid_reference_key_is_rejected_by_foreign_key(db):
    db.execute("INSERT INTO pipeline_run (run_id, source_file, status, started_at) VALUES ('r1','f','running','t')")
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(INSERT, _event("can_does_not_exist"))


def test_outcome_check_constraint(db):
    db.execute("INSERT INTO pipeline_run (run_id, source_file, status, started_at) VALUES ('r1','f','running','t')")
    with pytest.raises(sqlite3.IntegrityError):  # unresolved without a reason
        db.execute(INSERT, _event(None, "unresolved", None))
