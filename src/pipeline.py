"""Pipeline: read -> validate -> normalise -> match -> load (one transaction) -> history/conflicts.

Restart / retry: a file is processed inside a single transaction. If anything fails, the transaction is rolled
back, the run row is marked `failed` with the reason, and the exception is re-raised. Re-running the same file is
always safe (see README, "Idempotency").
"""
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from src.database.database import connect, init_schema
from src.ingestion.load_data import read_reference_file, read_source_file
from src.mapping import canonical_mapping as cm
from src.matching.matching_rules import MasterIndex, resolve
from src.transformation.normalize_data import (
    clean,
    make_event_id,
    normalize_email,
    normalize_event_ts,
    normalize_name_key,
    normalize_phone,
)
from src.validation.validate_data import validate_record

log = logging.getLogger("mapping.pipeline")


class PipelineError(Exception):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run_id(label: str = "") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"run_{stamp}_{uuid.uuid4().hex[:6]}" + (f"_{label}" if label else "")


def load_reference(conn, reference_file, run_id: str = "reference_load") -> int:
    """Load / refresh the canonical master (idempotent)."""
    count = cm.upsert_canonical_customers(conn, read_reference_file(reference_file), run_id)
    log.info("reference loaded: %s customers from %s", count, Path(reference_file).name)
    return count


def _start_run(conn, run_id: str, source_file: str) -> None:
    # a retried run reuses its run_id (Airflow does) - reset the row instead of failing on the primary key
    conn.execute(
        """INSERT INTO pipeline_run (run_id, source_file, status, started_at) VALUES (?,?, 'running', ?)
           ON CONFLICT(run_id) DO UPDATE SET source_file=excluded.source_file, status='running',
               started_at=excluded.started_at, ended_at=NULL, error_message=NULL, warnings=NULL,
               rows_read=0, rows_rejected=0, rows_duplicate=0, rows_inserted=0, rows_existing=0,
               rows_mapped=0, rows_unresolved=0, rows_late=0""",
        (run_id, source_file, utc_now()),
    )
    conn.commit()


def _finish_run(conn, run_id: str, status: str, metrics: Dict, error: Optional[str], warnings: List[str]) -> None:
    conn.execute(
        """UPDATE pipeline_run SET status=?, ended_at=?, rows_read=?, rows_rejected=?, rows_duplicate=?,
               rows_inserted=?, rows_existing=?, rows_mapped=?, rows_unresolved=?, rows_late=?,
               error_message=?, warnings=? WHERE run_id=?""",
        (status, utc_now(), metrics.get("rows_read", 0), metrics.get("rows_rejected", 0),
         metrics.get("rows_duplicate", 0), metrics.get("rows_inserted", 0), metrics.get("rows_existing", 0),
         metrics.get("rows_mapped", 0), metrics.get("rows_unresolved", 0), metrics.get("rows_late", 0),
         error, "; ".join(warnings) or None, run_id),
    )
    conn.commit()


def run_pipeline(source_file, db_path=None, run_id: Optional[str] = None, reference_file=None) -> Dict:
    """Process one source file. Returns the run metrics; raises PipelineError/SchemaError on failure."""
    source_path = Path(source_file)
    run_id = run_id or new_run_id(source_path.stem)
    conn = connect(db_path)
    init_schema(conn)
    metrics: Dict = {"run_id": run_id, "source_file": source_path.name}
    warnings: List[str] = []
    timings: Dict[str, float] = {}
    started = time.perf_counter()

    _start_run(conn, run_id, source_path.name)
    log.info("run %s started for %s", run_id, source_path.name)
    try:
        if reference_file:
            load_reference(conn, reference_file, run_id)
        index = MasterIndex.from_db(conn)
        if index.size == 0:
            raise PipelineError("canonical master is empty - load the reference file first")

        t0 = time.perf_counter()
        source = read_source_file(source_path)
        timings["read_s"] = time.perf_counter() - t0
        if source.extra_columns:
            warnings.append(f"unexpected extra columns ignored: {source.extra_columns}")
            log.warning("%s: unexpected extra columns ignored: %s", source_path.name, source.extra_columns)
        if not source.rows:
            warnings.append("empty batch: no data rows")
            log.warning("%s: empty batch", source_path.name)

        t0 = time.perf_counter()
        events, rejects = _transform(source.rows, source_path.name, run_id, index, metrics)
        timings["transform_s"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        _load(conn, events, rejects, source_path.name, run_id, metrics)
        timings["load_s"] = time.perf_counter() - t0

        timings["total_s"] = time.perf_counter() - started
        metrics["timings"] = {k: round(v, 4) for k, v in timings.items()}
        metrics["status"] = "success"
        _finish_run(conn, run_id, "success", metrics, None, warnings)
    except Exception as exc:
        conn.rollback()
        reason = f"{type(exc).__name__}: {exc}"
        _finish_run(conn, run_id, "failed", metrics, reason, warnings)
        log.error("run %s FAILED: %s", run_id, reason)
        conn.close()
        raise
    conn.close()
    log.info("run %s finished: %s", run_id, json.dumps({k: v for k, v in metrics.items() if k != "timings"}))
    return metrics


def _transform(rows, source_name: str, run_id: str, index: MasterIndex, metrics: Dict):
    events: List[Dict] = []
    rejects: List[Dict] = []
    seen = set()
    counts = {"rows_read": 0, "rows_rejected": 0, "rows_duplicate": 0, "rows_mapped": 0, "rows_unresolved": 0}
    rejected_at = utc_now()

    def reject(row_number, record, reason):
        raw = {k: v for k, v in record.items() if not k.startswith("_") or k == "_raw_line"}
        rejects.append({
            "reject_id": make_event_id(source_name, str(row_number), json.dumps(raw, sort_keys=True), reason),
            "run_id": run_id, "source_file": source_name, "source_row_number": row_number,
            "reject_reason": reason, "raw_record": raw, "rejected_at": rejected_at,
        })

    for row_number, record in rows:
        counts["rows_read"] += 1
        reasons = validate_record(record)
        if reasons:
            counts["rows_rejected"] += 1
            reject(row_number, record, ";".join(reasons))
            continue

        event_id = make_event_id(*(record.get(c) for c in
                                   ("source_system", "source_id", "event_ts", "email", "phone", "full_name")))
        if event_id in seen:
            counts["rows_duplicate"] += 1
            reject(row_number, record, "duplicate_in_batch")
            continue
        seen.add(event_id)

        email_norm = normalize_email(record.get("email"))
        phone_norm = normalize_phone(record.get("phone"))
        name_key = normalize_name_key(record.get("full_name"))
        match = resolve(email_norm, phone_norm, name_key, index)
        counts["rows_mapped" if match.status == "mapped" else "rows_unresolved"] += 1

        events.append({
            "event_id": event_id,
            "source_system": clean(record["source_system"]),
            "source_id": clean(record["source_id"]),
            "event_ts": normalize_event_ts(record["event_ts"]),
            "email_raw": clean(record.get("email")) or None,
            "phone_raw": clean(record.get("phone")) or None,
            "full_name_raw": clean(record.get("full_name")) or None,
            "email_norm": email_norm, "phone_norm": phone_norm, "name_key": name_key,
            "canonical_id": match.canonical_id, "match_rule": match.rule, "match_status": match.status,
            "unresolved_reason": match.reason, "run_id": run_id, "source_file": source_name,
            "source_row_number": row_number,
        })
    metrics.update(counts)
    return events, rejects


def _load(conn, events: List[Dict], rejects: List[Dict], source_name: str, run_id: str, metrics: Dict) -> None:
    """Single transaction: either the whole file lands or none of it does."""
    keys = {(e["source_system"], e["source_id"]) for e in events}
    already = cm.existing_event_ids(conn, [e["event_id"] for e in events])
    newest_stored = cm.latest_event_ts_by_key(conn, keys)

    late = 0
    running_max = dict(newest_stored)
    for e in events:  # file order
        key = (e["source_system"], e["source_id"])
        if e["event_id"] not in already and key in running_max and e["event_ts"] < running_max[key]:
            late += 1
        if key not in running_max or e["event_ts"] > running_max[key]:
            running_max[key] = e["event_ts"]
    metrics["rows_existing"] = len(already)
    metrics["rows_inserted"] = len(events) - len(already)
    metrics["rows_late"] = late

    cm.replace_rejects(conn, source_name, rejects)
    cm.upsert_events(conn, events)
    metrics.update(cm.rebuild_mapping(conn, keys, run_id))
    conn.commit()
