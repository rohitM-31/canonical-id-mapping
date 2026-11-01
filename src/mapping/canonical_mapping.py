"""Persistence: canonical master, source events, mapping history and conflicts."""
import json
from collections import defaultdict
from typing import Dict, Iterable, List, Sequence, Set, Tuple

from src.matching.conflict_detection import build_history, detect_one_to_many
from src.transformation.normalize_data import (
    clean,
    make_canonical_id,
    normalize_email,
    normalize_name_key,
    normalize_phone,
)

Key = Tuple[str, str]  # (source_system, source_id)

EVENT_COLUMNS = (
    "event_id", "source_system", "source_id", "event_ts", "email_raw", "phone_raw", "full_name_raw",
    "email_norm", "phone_norm", "name_key", "canonical_id", "match_rule", "match_status",
    "unresolved_reason", "run_id", "source_file", "source_row_number",
)

_TOUCHED_DDL = ("CREATE TEMP TABLE IF NOT EXISTS _touched_key (source_system TEXT, source_id TEXT, "
                "PRIMARY KEY (source_system, source_id))")


def upsert_canonical_customers(conn, customers: Iterable[Dict[str, str]], run_id: str) -> int:
    """Load the master. Idempotent: same email -> same canonical_id -> row is updated, not duplicated."""
    rows = []
    for c in customers:
        email_norm = normalize_email(c.get("email"))
        if not email_norm or not clean(c.get("full_name")):
            raise ValueError(f"reference record needs email and full_name: {c}")
        rows.append((
            make_canonical_id(email_norm), clean(c["email"]), clean(c.get("phone")) or None, clean(c["full_name"]),
            email_norm, normalize_phone(c.get("phone")), normalize_name_key(c.get("full_name")) or "", run_id,
        ))
    conn.executemany(
        """INSERT INTO canonical_customer
               (canonical_id, email, phone, full_name, email_norm, phone_norm, name_key, loaded_run_id)
           VALUES (?,?,?,?,?,?,?,?)
           ON CONFLICT(canonical_id) DO UPDATE SET email=excluded.email, phone=excluded.phone,
               full_name=excluded.full_name, phone_norm=excluded.phone_norm, name_key=excluded.name_key,
               loaded_run_id=excluded.loaded_run_id""",
        rows,
    )
    conn.commit()
    return len(rows)


def existing_event_ids(conn, event_ids: Sequence[str]) -> Set[str]:
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _batch_event (event_id TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM _batch_event")
    conn.executemany("INSERT OR IGNORE INTO _batch_event VALUES (?)", [(e,) for e in event_ids])
    rows = conn.execute("SELECT b.event_id FROM _batch_event b JOIN source_event e USING (event_id)")
    return {r[0] for r in rows}


def latest_event_ts_by_key(conn, keys: Iterable[Key]) -> Dict[Key, str]:
    conn.execute(_TOUCHED_DDL)
    conn.execute("DELETE FROM _touched_key")
    conn.executemany("INSERT OR IGNORE INTO _touched_key VALUES (?,?)", list(keys))
    rows = conn.execute(
        """SELECT t.source_system, t.source_id, MAX(e.event_ts)
           FROM _touched_key t JOIN source_event e USING (source_system, source_id)
           GROUP BY t.source_system, t.source_id""")
    return {(r[0], r[1]): r[2] for r in rows}


def upsert_events(conn, events: List[Dict]) -> None:
    """Insert new events; for an event_id seen before, refresh the matching outcome (supports backfill)."""
    placeholders = ",".join("?" * len(EVENT_COLUMNS))
    updates = ", ".join(f"{c}=excluded.{c}" for c in EVENT_COLUMNS if c != "event_id")
    conn.executemany(
        f"INSERT INTO source_event ({','.join(EVENT_COLUMNS)}) VALUES ({placeholders}) "
        f"ON CONFLICT(event_id) DO UPDATE SET {updates}",
        [tuple(e[c] for c in EVENT_COLUMNS) for e in events],
    )


def replace_rejects(conn, source_file: str, rejects: List[Dict]) -> None:
    """Rejects always describe the latest processing of a file, so replace rather than append."""
    conn.execute("DELETE FROM reject_record WHERE source_file = ?", (source_file,))
    conn.executemany(
        "INSERT INTO reject_record (reject_id, run_id, source_file, source_row_number, reject_reason, "
        "raw_record, rejected_at) VALUES (?,?,?,?,?,?,?)",
        [(r["reject_id"], r["run_id"], r["source_file"], r["source_row_number"], r["reject_reason"],
          json.dumps(r["raw_record"], sort_keys=True), r["rejected_at"]) for r in rejects],
    )


def rebuild_mapping(conn, keys: Iterable[Key], run_id: str) -> Dict[str, int]:
    """Rebuild history + conflicts for the touched source keys from the full event log (order independent)."""
    keys = list(set(keys))
    conn.execute(_TOUCHED_DDL)
    conn.execute("DELETE FROM _touched_key")
    conn.executemany("INSERT INTO _touched_key VALUES (?,?)", keys)

    previous = {
        (r[0], r[1]): r[2]
        for r in conn.execute(
            """SELECT c.source_system, c.source_id, c.detected_run_id FROM mapping_conflict c
               JOIN _touched_key t USING (source_system, source_id)""")
    }
    for table in ("mapping_history", "mapping_conflict"):
        conn.execute(
            f"DELETE FROM {table} WHERE EXISTS (SELECT 1 FROM _touched_key t WHERE "
            f"t.source_system = {table}.source_system AND t.source_id = {table}.source_id)")

    grouped = defaultdict(list)
    for r in conn.execute(
        """SELECT e.source_system, e.source_id, e.event_ts, e.event_id, e.canonical_id
           FROM source_event e JOIN _touched_key t USING (source_system, source_id)
           WHERE e.match_status = 'mapped'"""
    ):
        grouped[(r[0], r[1])].append((r[2], r[3], r[4]))

    history_rows, conflict_rows = [], []
    for key, events in grouped.items():
        for i in build_history(events):
            history_rows.append((key[0], key[1], i["canonical_id"], i["valid_from"], i["valid_to"],
                                 i["is_current"], i["event_count"], run_id))
        conflict = detect_one_to_many(events)
        if conflict:
            conflict_rows.append((key[0], key[1], conflict["conflict_type"], conflict["canonical_count"],
                                  conflict["canonical_ids"], conflict["first_seen"], conflict["last_seen"],
                                  previous.get(key, run_id)))
    conn.executemany("INSERT INTO mapping_history VALUES (?,?,?,?,?,?,?,?)", history_rows)
    conn.executemany(
        "INSERT INTO mapping_conflict (source_system, source_id, conflict_type, canonical_count, canonical_ids, "
        "first_seen, last_seen, detected_run_id) VALUES (?,?,?,?,?,?,?,?)", conflict_rows)
    return {"history_rows": len(history_rows), "conflicts": len(conflict_rows)}
