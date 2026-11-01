from src.mapping import canonical_mapping as cm
from src.pipeline import utc_now


def _run(db, run_id="r1"):
    db.execute("INSERT OR IGNORE INTO pipeline_run (run_id, source_file, status, started_at) VALUES (?, 'f', 'running', ?)",
               (run_id, utc_now()))


def _event(eid, ts, canonical_id, row=1):
    return {
        "event_id": eid, "source_system": "crm", "source_id": "crm-00000001", "event_ts": ts, "email_raw": None,
        "phone_raw": None, "full_name_raw": None, "email_norm": None, "phone_norm": None, "name_key": None,
        "canonical_id": canonical_id, "match_rule": "exact_email" if canonical_id else None,
        "match_status": "mapped" if canonical_id else "unresolved",
        "unresolved_reason": None if canonical_id else "no_match",
        "run_id": "r1", "source_file": "f.csv", "source_row_number": row,
    }


def canonical_ids(db):
    return [r[0] for r in db.execute("SELECT canonical_id FROM canonical_customer ORDER BY email_norm")]


def test_upsert_event_refreshes_outcome_without_duplicating(db):
    _run(db)
    a, b = canonical_ids(db)[:2]
    cm.upsert_events(db, [_event("e1", "2025-01-01T00:00:00Z", None)])
    cm.upsert_events(db, [_event("e1", "2025-01-01T00:00:00Z", a)])
    rows = db.execute("SELECT match_status, canonical_id FROM source_event").fetchall()
    assert [tuple(r) for r in rows] == [("mapped", a)]


def test_rebuild_builds_history_and_conflict_and_keeps_detection_run(db):
    _run(db, "r1")
    _run(db, "r2")
    a, b = canonical_ids(db)[:2]
    key = ("crm", "crm-00000001")
    cm.upsert_events(db, [_event("e1", "2025-01-01T00:00:00Z", a), _event("e2", "2025-02-01T00:00:00Z", b, 2),
                          _event("e3", "2025-03-01T00:00:00Z", None, 3)])
    cm.rebuild_mapping(db, [key], "r1")
    hist = db.execute("SELECT canonical_id, valid_from, valid_to, is_current FROM mapping_history ORDER BY valid_from").fetchall()
    assert [(h[0], h[2], h[3]) for h in hist] == [(a, "2025-02-01T00:00:00Z", 0), (b, None, 1)]
    assert db.execute("SELECT detected_run_id FROM mapping_conflict").fetchone()[0] == "r1"

    cm.rebuild_mapping(db, [key], "r2")  # rebuild is repeatable and keeps first detection run
    assert db.execute("SELECT COUNT(*) FROM mapping_history").fetchone()[0] == 2
    assert db.execute("SELECT detected_run_id FROM mapping_conflict").fetchone()[0] == "r1"


def test_conflict_disappears_when_events_no_longer_conflict(db):
    _run(db)
    a, b = canonical_ids(db)[:2]
    key = ("crm", "crm-00000001")
    cm.upsert_events(db, [_event("e1", "2025-01-01T00:00:00Z", a), _event("e2", "2025-02-01T00:00:00Z", b, 2)])
    cm.rebuild_mapping(db, [key], "r1")
    cm.upsert_events(db, [_event("e2", "2025-02-01T00:00:00Z", a, 2)])  # e.g. master corrected -> now both map to a
    cm.rebuild_mapping(db, [key], "r1")
    assert db.execute("SELECT COUNT(*) FROM mapping_conflict").fetchone()[0] == 0
