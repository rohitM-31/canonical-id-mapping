"""Outputs: CSV extracts, reconciliation checks, and (optionally) accuracy against expected labels."""
import csv
import json
import re
from pathlib import Path
from typing import Dict, List

from config import config

EXPORTS = {
    "current_mapping.csv": "SELECT * FROM v_current_mapping ORDER BY source_system, source_id",
    "mapping_history.csv": "SELECT * FROM mapping_history ORDER BY source_system, source_id, valid_from",
    "mapping_conflicts.csv": "SELECT * FROM mapping_conflict ORDER BY source_system, source_id",
    "unresolved_mappings.csv": "SELECT * FROM v_unresolved_mapping ORDER BY unresolved_reason, source_system, source_id, event_ts",
    "rejected_records.csv": "SELECT * FROM reject_record ORDER BY source_file, source_row_number",
    "run_summary.csv": "SELECT * FROM pipeline_run ORDER BY started_at",
}


def run_reconciliation(conn) -> List[Dict]:
    """Execute every check in sql/reconciliation.sql. violations == 0 means the check passed."""
    text = (config.SQL_DIR / "reconciliation.sql").read_text(encoding="utf-8")
    blocks = re.split(r"^-- check:\s*", text, flags=re.MULTILINE)[1:]
    results = []
    for block in blocks:
        header, _, sql = block.partition("\n")
        name, _, description = header.partition("|")
        violations = conn.execute(sql.strip().rstrip(";")).fetchone()[0]
        results.append({"check": name.strip(), "description": description.strip(),
                        "violations": violations, "passed": violations == 0})
    return results


def summary_tables(conn) -> Dict[str, List[Dict]]:
    def rows(sql):
        return [dict(r) for r in conn.execute(sql)]

    return {
        "events_by_source_system": rows(
            """SELECT source_system, COUNT(*) AS events,
                      SUM(match_status = 'mapped') AS mapped, SUM(match_status = 'unresolved') AS unresolved
               FROM source_event GROUP BY source_system ORDER BY source_system"""),
        "mapped_by_rule": rows(
            "SELECT match_rule, COUNT(*) AS events FROM source_event WHERE match_status = 'mapped' "
            "GROUP BY match_rule ORDER BY events DESC"),
        "unresolved_by_reason": rows(
            "SELECT unresolved_reason, COUNT(*) AS events FROM source_event WHERE match_status = 'unresolved' "
            "GROUP BY unresolved_reason ORDER BY events DESC"),
        "rejects_by_reason": rows(
            "SELECT reject_reason, COUNT(*) AS records FROM reject_record GROUP BY reject_reason ORDER BY records DESC"),
        "totals": rows(
            """SELECT (SELECT COUNT(*) FROM canonical_customer) AS canonical_customers,
                      (SELECT COUNT(*) FROM source_event) AS source_events,
                      (SELECT COUNT(*) FROM reject_record) AS quarantined_records,
                      (SELECT COUNT(*) FROM mapping_history) AS history_rows,
                      (SELECT COUNT(*) FROM mapping_conflict) AS one_to_many_conflicts,
                      (SELECT COUNT(DISTINCT source_system || '|' || source_id) FROM source_event
                         WHERE match_status = 'unresolved') AS source_keys_with_unresolved_events"""),
    }


def evaluate_labels(conn, labels_file, conflicts_file=None) -> Dict:
    """Compare pipeline outcomes with expected labels (kept in a separate file from the input data).

    labels columns: source_file, source_row_number, expected_outcome (mapped|unresolved|rejected|duplicate),
    expected_canonical_id (only for mapped).
    """
    actual = {}
    for r in conn.execute("SELECT source_file, source_row_number, match_status, canonical_id FROM source_event"):
        actual[(r[0], r[1])] = (r[2], r[3])
    for r in conn.execute("SELECT source_file, source_row_number, reject_reason FROM reject_record"):
        actual[(r[0], r[1])] = ("duplicate" if r[2] == "duplicate_in_batch" else "rejected", None)

    total = outcome_ok = mapped_total = mapped_ok = 0
    mismatches = []
    with Path(labels_file).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            total += 1
            key = (row["source_file"], int(row["source_row_number"]))
            got = actual.get(key, ("missing", None))
            expected = row["expected_outcome"]
            if got[0] == expected:
                outcome_ok += 1
            elif len(mismatches) < 20:
                mismatches.append({"key": key, "expected": expected, "got": got[0]})
            if expected == "mapped":
                mapped_total += 1
                if got[0] == "mapped" and got[1] == row["expected_canonical_id"]:
                    mapped_ok += 1
    report = {
        "labelled_rows": total,
        "outcome_accuracy": round(outcome_ok / total, 6) if total else None,
        "mapped_rows": mapped_total,
        "canonical_id_accuracy_on_mapped": round(mapped_ok / mapped_total, 6) if mapped_total else None,
        "sample_mismatches": mismatches,
    }
    if conflicts_file:
        with Path(conflicts_file).open(newline="", encoding="utf-8") as handle:
            expected_keys = {(r["source_system"], r["source_id"]) for r in csv.DictReader(handle)}
        found = {(r[0], r[1]) for r in conn.execute("SELECT source_system, source_id FROM mapping_conflict")}
        tp = len(expected_keys & found)
        report["conflicts"] = {
            "expected": len(expected_keys), "detected": len(found),
            "precision": round(tp / len(found), 6) if found else None,
            "recall": round(tp / len(expected_keys), 6) if expected_keys else None,
        }
    return report


def export_reports(conn, output_dir=None, extra: Dict = None) -> Dict:
    """Write CSV extracts + reconciliation_report.json/.md; returns the reconciliation payload."""
    out = Path(output_dir or config.OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    for filename, sql in EXPORTS.items():
        cursor = conn.execute(sql)
        with (out / filename).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([d[0] for d in cursor.description])
            writer.writerows(cursor)

    checks = run_reconciliation(conn)
    payload = {"checks": checks, "all_passed": all(c["passed"] for c in checks), "summary": summary_tables(conn)}
    if extra:
        payload.update(extra)
    (out / "reconciliation_report.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (out / "reconciliation_report.md").write_text(_markdown(payload), encoding="utf-8")
    return payload


def _table(rows: List[Dict]) -> str:
    if not rows:
        return "_none_\n"
    cols = list(rows[0])
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows]
    return "\n".join(lines) + "\n"


def _markdown(payload: Dict) -> str:
    parts = ["# Reconciliation report\n", f"Overall: **{'PASS' if payload['all_passed'] else 'FAIL'}**\n",
             "## Checks\n",
             _table([{"check": c["check"], "violations": c["violations"],
                      "result": "PASS" if c["passed"] else "FAIL", "description": c["description"]}
                     for c in payload["checks"]])]
    for name, rows in payload["summary"].items():
        parts += [f"## {name}\n", _table(rows)]
    for name in ("label_evaluation", "performance"):
        if name in payload:
            parts += [f"## {name}\n", "```json\n" + json.dumps(payload[name], indent=2) + "\n```\n"]
    return "\n".join(parts)
