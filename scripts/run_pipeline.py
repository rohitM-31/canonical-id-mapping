"""Command line runner (same code the Airflow DAG calls).

Examples
  python scripts/run_pipeline.py                      # all data/input/dataset_*.csv, in name order
  python scripts/run_pipeline.py --reset              # start from an empty database
  python scripts/run_pipeline.py --files data/large/dataset_large.csv --reference data/large/canonical_customers.csv \
         --db data/output/large.db --output-dir data/output/large --labels data/large/expected_labels.csv
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import config  # noqa: E402
from src.database.database import connect, init_schema  # noqa: E402
from src.pipeline import load_reference, run_pipeline  # noqa: E402
from src.reporting.generate_reports import evaluate_labels, export_reports  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--files", nargs="*", help="input CSVs (default: INPUT_DIR/INPUT_GLOB)")
    ap.add_argument("--reference", default=str(config.REFERENCE_FILE))
    ap.add_argument("--db", default=str(config.DB_PATH))
    ap.add_argument("--output-dir", default=str(config.OUTPUT_DIR))
    ap.add_argument("--labels", help="expected_labels.csv (answer key) to score the run against")
    ap.add_argument("--conflicts", help="expected_conflicts.csv; defaults to expected_conflicts.csv next to --labels")
    ap.add_argument("--reset", action="store_true", help="delete the database first")
    args = ap.parse_args(argv)

    logging.basicConfig(level=config.LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    files = [Path(f) for f in args.files] if args.files else sorted(config.INPUT_DIR.glob(config.INPUT_GLOB))
    if not files:
        print("no input files found", file=sys.stderr)
        return 2
    if args.reset and Path(args.db).exists():
        Path(args.db).unlink()

    conn = connect(args.db)
    init_schema(conn)
    load_reference(conn, args.reference)
    conn.close()

    failures = 0
    performance = []
    for f in files:
        try:
            m = run_pipeline(f, db_path=args.db)
            performance.append({"file": m["source_file"], "rows": m["rows_read"], **m["timings"]})
        except Exception as exc:  # already logged + recorded in pipeline_run; keep going with the other files
            failures += 1
            print(f"FAILED {f.name}: {exc}", file=sys.stderr)

    conn = connect(args.db)
    extra = {"performance": performance}
    if args.labels:
        conflicts = args.conflicts or str(Path(args.labels).with_name("expected_conflicts.csv"))
        extra["label_evaluation"] = evaluate_labels(conn, args.labels, conflicts if Path(conflicts).exists() else None)
    report = export_reports(conn, args.output_dir, extra)
    conn.close()

    print(f"\nreports written to {args.output_dir}")
    for c in report["checks"]:
        print(f"  [{'PASS' if c['passed'] else 'FAIL'}] {c['check']}")
    if "label_evaluation" in extra:
        print("label evaluation:", {k: v for k, v in extra["label_evaluation"].items() if k != "sample_mismatches"})
    return 1 if failures or not report["all_passed"] else 0


if __name__ == "__main__":
    sys.exit(main())
