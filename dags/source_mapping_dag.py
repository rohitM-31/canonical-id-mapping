"""Airflow DAG: source identifier -> canonical id mapping.

init_store -> ingest_<file 1> -> ingest_<file 2> -> ... -> publish_reports

* One ingest task per input file, chained so files load in name order (SQLite allows a single writer).
* Every task is safe to retry: a retry re-uses the Airflow run_id and the load is a single transaction.
* publish_reports fails the DAG run if any reconciliation check fails.
* Trigger manually (schedule=None):  airflow dags trigger source_mapping
  or without a scheduler:             airflow dags test source_mapping
"""
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

from config import config  # noqa: E402

INPUT_FILES = sorted(config.INPUT_DIR.glob(config.INPUT_GLOB))  # evaluated at DAG parse time


@dag(
    dag_id="source_mapping",
    description="Map source identifiers to canonical ids (exact + rule based), detect conflicts, keep history",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["mapping", "master-data"],
)
def source_mapping():
    @task
    def init_store() -> int:
        from src.database.database import connect, init_schema
        from src.pipeline import load_reference

        conn = connect(config.DB_PATH)
        init_schema(conn)
        count = load_reference(conn, config.REFERENCE_FILE)
        conn.close()
        return count

    @task
    def ingest_file(path: str, run_id=None) -> dict:
        from src.pipeline import run_pipeline

        label = re.sub(r"\W+", "_", Path(path).stem)
        metrics = run_pipeline(path, db_path=config.DB_PATH, run_id=f"{run_id}__{label}")
        return {k: v for k, v in metrics.items() if k != "timings"}

    @task
    def publish_reports() -> dict:
        from src.database.database import connect
        from src.reporting.generate_reports import export_reports

        conn = connect(config.DB_PATH)
        report = export_reports(conn, config.OUTPUT_DIR)
        conn.close()
        failed = [c["check"] for c in report["checks"] if not c["passed"]]
        if failed:
            raise RuntimeError(f"reconciliation failed: {failed}")
        return {"checks_passed": len(report["checks"]), "totals": report["summary"]["totals"][0]}

    previous = init_store()
    for path in INPUT_FILES:
        stem = re.sub(r"\W+", "_", path.stem)
        current = ingest_file.override(task_id=f"ingest_{stem}")(path=str(path))
        previous >> current
        previous = current
    previous >> publish_reports()


source_mapping()
