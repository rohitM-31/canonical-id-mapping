
import pytest

from config import config
from scripts import generate_large_dataset as gen
from src.database.database import connect, init_schema
from src.ingestion.load_data import SchemaError
from src.mapping import canonical_mapping as cm
from src.pipeline import PipelineError, load_reference, run_pipeline
from src.reporting.generate_reports import evaluate_labels, export_reports, run_reconciliation
from tests.conftest import HEADER, MASTER, rec

ALICE = "alice.one@example.com"
BOB = "bob.two@example.com"


def q(db, sql, *args):
    return [tuple(r) for r in db.execute(sql, args)]


def assert_reconciled(db):
    failed = [c for c in run_reconciliation(db) if not c["passed"]]
    assert not failed, failed


# ---------------------------------------------------------------- happy path
def test_happy_path(run, db):
    m = run([
        rec(sid="crm-00000001", email="  ALICE.ONE@example.com "),               # exact (after cleaning)
        rec(sid="crm-00000002", phone="1-200-000-0002"),                        # phone rule
        rec(sid="crm-00000003", name="White, Carol D"),                         # name rule
    ])
    assert (m["rows_read"], m["rows_mapped"], m["rows_unresolved"], m["rows_rejected"]) == (3, 3, 0, 0)
    assert q(db, "SELECT match_rule, COUNT(*) FROM source_event GROUP BY 1 ORDER BY 1") == [
        ("exact_email", 1), ("rule_name", 1), ("rule_phone", 1)]
    assert q(db, "SELECT status, rows_inserted FROM pipeline_run") == [("success", 3)]
    assert_reconciled(db)


# ---------------------------------------------------------------- empty / schema / malformed
def test_empty_batch_succeeds_with_warning(run, db):
    m = run([])
    assert m["rows_read"] == 0
    status, warn = q(db, "SELECT status, warnings FROM pipeline_run")[0]
    assert status == "success" and "empty batch" in warn
    assert_reconciled(db)


def test_unexpected_schema_change_fails_run_and_loads_nothing(db_path, write_csv, db):
    path = write_csv("drift.csv", [["crm", "crm-00000001", "2025-01-01T00:00:00Z", ALICE]], header=HEADER[:4])
    with pytest.raises(SchemaError):
        run_pipeline(path, db_path=db_path, run_id="drift_run")
    status, err = q(db, "SELECT status, error_message FROM pipeline_run WHERE run_id='drift_run'")[0]
    assert status == "failed" and "missing required columns" in err
    assert q(db, "SELECT COUNT(*) FROM source_event") == [(0,)]


def test_extra_column_is_warned_and_ignored(db_path, write_csv, db):
    path = write_csv("extra.csv", [rec(email=ALICE) + ["x"]], header=HEADER + ["new_col"])
    run_pipeline(path, db_path=db_path)
    assert "extra columns" in q(db, "SELECT warnings FROM pipeline_run")[0][0]
    assert q(db, "SELECT COUNT(*) FROM source_event") == [(1,)]


def test_malformed_and_invalid_records_are_quarantined_with_traceability(db_path, tmp_path, db):
    path = tmp_path / "mixed.csv"
    path.write_text(
        ",".join(HEADER) + "\n"
        "crm,crm-00000001,2025-01-01T00:00:00Z,alice.one@example.com,,\n"
        "crm,crm-00000002,2025-01-01T00:00:00Z,only,three\n"                 # wrong field count
        ",crm-00000003,2025-01-01T00:00:00Z,a@b.com,,\n"                     # missing system
        "web,web-00000004,not-a-timestamp,a@b.com,,\n"                       # bad timestamp
        "crm,crm-00000005,2025-01-01T00:00:00Z,not-an-email,,\n")            # bad email
    m = run_pipeline(path, db_path=db_path)
    assert (m["rows_read"], m["rows_rejected"], m["rows_mapped"]) == (5, 4, 1)
    rejects = q(db, "SELECT source_row_number, reject_reason FROM reject_record ORDER BY 1")
    assert rejects == [(2, "malformed_row"), (3, "missing_source_system"), (4, "invalid_event_ts"), (5, "invalid_email")]
    assert "not-an-email" in q(db, "SELECT raw_record FROM reject_record WHERE source_row_number=5")[0][0]
    assert_reconciled(db)


# ---------------------------------------------------------------- duplicates / nulls
def test_exact_duplicates_in_a_file_are_quarantined_once(run, db):
    row = rec(email=ALICE)
    m = run([row, list(row), [c.upper() if i == 3 else c for i, c in enumerate(row)]])  # 3rd differs in case only
    assert (m["rows_duplicate"], m["rows_inserted"]) == (1, 2)
    assert q(db, "SELECT reject_reason FROM reject_record") == [("duplicate_in_batch",)]
    assert_reconciled(db)


def test_null_identifiers_become_unresolved_not_errors(run, db):
    m = run([rec(sid="crm-00000001"), rec(sid="crm-00000002", name="Nobody Known")])
    assert m["rows_unresolved"] == 2
    assert q(db, "SELECT unresolved_reason FROM v_unresolved_mapping ORDER BY source_id") == [
        ("no_identifiers",), ("no_match",)]


# ---------------------------------------------------------------- domain edge cases
def test_one_to_many_conflict_keeps_history(run, db):
    run([rec(sid="crm-00000001", ts="2025-01-01T00:00:00Z", email=ALICE),
         rec(sid="crm-00000001", ts="2025-02-01T00:00:00Z", email=BOB),
         rec(sid="crm-00000002", ts="2025-01-01T00:00:00Z", email=ALICE)])  # many-to-one is fine
    assert q(db, "SELECT source_id, canonical_count FROM mapping_conflict") == [("crm-00000001", 2)]
    assert q(db, "SELECT COUNT(*) FROM mapping_history WHERE source_id='crm-00000001'") == [(2,)]
    cur = q(db, "SELECT has_conflict FROM v_current_mapping ORDER BY source_id")
    assert cur == [(1,), (0,)]
    assert_reconciled(db)


def test_conflicting_identifiers_on_one_record_are_exposed_as_unresolved(run, db):
    run([rec(email=ALICE, phone="(200) 000-0002")])
    assert q(db, "SELECT unresolved_reason FROM v_unresolved_mapping") == [("conflicting_identifiers",)]
    assert q(db, "SELECT COUNT(*) FROM mapping_history") == [(0,)]


def test_late_arriving_event_is_inserted_into_history_in_order(run, db):
    run([rec(ts="2025-03-01T00:00:00Z", email=BOB)], name="a.csv")
    m = run([rec(ts="2025-01-01T00:00:00Z", email=ALICE)], name="b.csv")   # older than what is already stored
    assert m["rows_late"] == 1
    hist = q(db, "SELECT canonical_id = (SELECT canonical_id FROM canonical_customer WHERE email_norm=?), "
                 "valid_from, valid_to, is_current FROM mapping_history ORDER BY valid_from", ALICE)
    assert hist == [(1, "2025-01-01T00:00:00Z", "2025-03-01T00:00:00Z", 0), (0, "2025-03-01T00:00:00Z", None, 1)]
    assert_reconciled(db)


def test_out_of_order_rows_inside_one_file(run, db):
    m = run([rec(ts="2025-03-01T00:00:00Z", email=BOB), rec(ts="2025-01-01T00:00:00Z", email=ALICE)])
    assert m["rows_late"] == 1
    assert q(db, "SELECT COUNT(*) FROM mapping_history WHERE is_current=1") == [(1,)]


def test_ambiguous_name_is_unresolved(run, db):
    run([rec(name="Dan Lee")])
    assert q(db, "SELECT unresolved_reason FROM v_unresolved_mapping") == [("ambiguous_name",)]


# ---------------------------------------------------------------- idempotency / recovery
def test_rerun_of_same_input_creates_no_duplicates(run, db):
    rows = [rec(sid="crm-00000001", email=ALICE), rec(sid="crm-00000002", email=BOB),
            rec(sid="crm-00000002", email=BOB, ts="2025-01-02T00:00:00Z"), rec(sid="crm-00000003")]
    rows.append(list(rows[0]))  # + a duplicate
    first = run(rows)
    snapshot = {t: q(db, f"SELECT * FROM {t} ORDER BY 1,2,3") for t in
                ("mapping_history", "mapping_conflict")}
    events_before = q(db, "SELECT event_id, canonical_id FROM source_event ORDER BY event_id")
    second = run(rows)
    assert (second["rows_inserted"], second["rows_existing"]) == (0, first["rows_inserted"])
    assert second["rows_late"] == 0
    assert q(db, "SELECT event_id, canonical_id FROM source_event ORDER BY event_id") == events_before
    for table, before in snapshot.items():
        after = q(db, f"SELECT * FROM {table} ORDER BY 1,2,3")
        assert [r[:-1] for r in after] == [r[:-1] for r in before]  # run_id column may move to the latest run
    assert q(db, "SELECT COUNT(*) FROM reject_record") == [(1,)]
    assert_reconciled(db)


def test_same_row_in_second_file_is_not_loaded_twice(run, db):
    run([rec(email=ALICE)], name="a.csv")
    m = run([rec(email=ALICE)], name="b.csv")
    assert (m["rows_inserted"], m["rows_existing"]) == (0, 1)
    assert q(db, "SELECT COUNT(*) FROM source_event") == [(1,)]
    assert_reconciled(db)


def test_partial_failure_rolls_back_and_retry_succeeds(db_path, write_csv, db, monkeypatch):
    path = write_csv("f.csv", [rec(email=ALICE), rec(sid="crm-00000002", email="not-an-email")])

    def boom(*a, **k):
        raise RuntimeError("disk full")

    monkeypatch.setattr(cm, "rebuild_mapping", boom)
    with pytest.raises(RuntimeError):
        run_pipeline(path, db_path=db_path, run_id="retry_me")
    assert q(db, "SELECT status, error_message FROM pipeline_run WHERE run_id='retry_me'") == [("failed", "RuntimeError: disk full")]
    assert q(db, "SELECT (SELECT COUNT(*) FROM source_event), (SELECT COUNT(*) FROM reject_record), "
                 "(SELECT COUNT(*) FROM mapping_history)") == [(0, 0, 0)]  # nothing half-loaded

    monkeypatch.undo()
    m = run_pipeline(path, db_path=db_path, run_id="retry_me")  # Airflow-style retry: same run_id
    assert m["rows_inserted"] == 1 and m["rows_rejected"] == 1
    assert q(db, "SELECT status, error_message FROM pipeline_run WHERE run_id='retry_me'") == [("success", None)]
    assert_reconciled(db)


def test_backfill_after_master_change_resolves_previously_unresolved(db_path, write_csv, db):
    path = write_csv("f.csv", [rec(email="new.person@example.com")])
    run_pipeline(path, db_path=db_path)
    assert q(db, "SELECT COUNT(*) FROM v_unresolved_mapping") == [(1,)]

    master2 = write_csv("master2.csv", MASTER + [("new.person@example.com", "", "New P Person")],
                        header=["email", "phone", "full_name"])
    m = run_pipeline(path, db_path=db_path, reference_file=master2)
    assert (m["rows_existing"], m["rows_inserted"], m["rows_mapped"]) == (1, 0, 1)
    assert q(db, "SELECT COUNT(*) FROM v_unresolved_mapping") == [(0,)]
    assert q(db, "SELECT COUNT(*) FROM mapping_history") == [(1,)]
    assert_reconciled(db)


def test_empty_master_fails_fast(tmp_path, write_csv):
    db = tmp_path / "empty.db"
    conn = connect(db)
    init_schema(conn)
    conn.close()
    with pytest.raises(PipelineError, match="master is empty"):
        run_pipeline(write_csv("f.csv", [rec()]), db_path=db)


def test_missing_input_file_marks_run_failed(db_path, tmp_path, db):
    with pytest.raises(FileNotFoundError):
        run_pipeline(tmp_path / "nope.csv", db_path=db_path, run_id="missing")
    assert q(db, "SELECT status FROM pipeline_run WHERE run_id='missing'") == [("failed",)]


# ---------------------------------------------------------------- reports
def test_reconciliation_detects_corruption(run, db):
    run([rec(email=ALICE)])
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute("UPDATE source_event SET canonical_id = 'can_ghost'")
    db.execute("UPDATE pipeline_run SET rows_read = rows_read + 1")
    failed = {c["check"] for c in run_reconciliation(db) if not c["passed"]}
    assert {"event_unknown_canonical", "run_row_balance"} <= failed


def test_export_reports_writes_files(run, db, tmp_path):
    run([rec(email=ALICE)])
    payload = export_reports(db, tmp_path / "out")
    assert payload["all_passed"]
    for name in ("current_mapping.csv", "mapping_history.csv", "mapping_conflicts.csv", "unresolved_mappings.csv",
                 "rejected_records.csv", "run_summary.csv", "reconciliation_report.md", "reconciliation_report.json"):
        assert (tmp_path / "out" / name).exists()


# ---------------------------------------------------------------- the two delivered datasets + generator
def test_provided_datasets_end_to_end(tmp_path):
    db = tmp_path / "real.db"
    conn = connect(db)
    init_schema(conn)
    load_reference(conn, config.REFERENCE_FILE)
    conn.close()
    files = sorted(config.INPUT_DIR.glob("dataset_*.csv"))
    assert len(files) == 2
    m1, m2 = (run_pipeline(f, db_path=db) for f in files)

    # independently profiled: 199 rows = 8 invalid + 6 exact duplicates + 185 events
    assert (m1["rows_read"], m1["rows_rejected"], m1["rows_duplicate"], m1["rows_inserted"]) == (199, 8, 6, 185)
    assert (m2["rows_read"], m2["rows_rejected"], m2["rows_inserted"]) == (34, 0, 34)

    conn = connect(db)
    assert q(conn, "SELECT COUNT(*) FROM source_event") == [(219,)]
    assert q(conn, "SELECT COUNT(*) FROM mapping_conflict WHERE source_id='crm-00000022'") == [(1,)]
    assert q(conn, "SELECT unresolved_reason FROM v_unresolved_mapping WHERE email_raw LIKE 'Sam.smith38%'") == [
        ("conflicting_identifiers",)]
    assert_reconciled(conn)

    for f in files:  # rerun everything
        r = run_pipeline(f, db_path=db)
        assert r["rows_inserted"] == 0
    assert q(conn, "SELECT COUNT(*) FROM source_event") == [(219,)]
    assert_reconciled(conn)
    conn.close()


def test_generated_dataset_matches_answer_key(tmp_path):
    rows, labels = gen.generate(rows=3000, customers=300, seed=7)
    gen.write_outputs(tmp_path, rows, labels)
    gen.write_reference(tmp_path / "ref.csv", 300)
    db = tmp_path / "gen.db"
    conn = connect(db)
    init_schema(conn)
    load_reference(conn, tmp_path / "ref.csv")
    conn.close()
    run_pipeline(tmp_path / "dataset_large.csv", db_path=db)

    conn = connect(db)
    report = evaluate_labels(conn, tmp_path / "expected_labels.csv", tmp_path / "expected_conflicts.csv")
    assert report["outcome_accuracy"] == 1.0, report["sample_mismatches"]
    assert report["canonical_id_accuracy_on_mapped"] == 1.0
    assert report["conflicts"]["precision"] == 1.0 and report["conflicts"]["recall"] == 1.0
    assert_reconciled(conn)
    conn.close()
