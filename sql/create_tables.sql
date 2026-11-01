-- Mapping store. All timestamps are ISO-8601 UTC text (YYYY-MM-DDTHH:MM:SSZ).

-- Canonical master (reference data). canonical_id is deterministic: 'can_' + sha1(email_norm)[:10].
CREATE TABLE IF NOT EXISTS canonical_customer (
    canonical_id   TEXT PRIMARY KEY,
    email          TEXT NOT NULL,
    phone          TEXT,
    full_name      TEXT NOT NULL,
    email_norm     TEXT NOT NULL UNIQUE,
    phone_norm     TEXT,
    name_key       TEXT NOT NULL,
    loaded_run_id  TEXT
);

CREATE INDEX IF NOT EXISTS ix_canonical_phone ON canonical_customer (phone_norm);
CREATE INDEX IF NOT EXISTS ix_canonical_name_key ON canonical_customer (name_key);

-- One row per execution of the pipeline on one file.
CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id          TEXT PRIMARY KEY,
    source_file     TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    rows_read       INTEGER DEFAULT 0,
    rows_rejected   INTEGER DEFAULT 0,   -- failed validation
    rows_duplicate  INTEGER DEFAULT 0,   -- exact duplicate of an earlier row in the same file
    rows_inserted   INTEGER DEFAULT 0,   -- new source_event rows
    rows_existing   INTEGER DEFAULT 0,   -- already loaded by an earlier run (re-resolved, not duplicated)
    rows_mapped     INTEGER DEFAULT 0,
    rows_unresolved INTEGER DEFAULT 0,
    rows_late       INTEGER DEFAULT 0,   -- older than the newest event already stored for the same source key
    error_message   TEXT,
    warnings        TEXT
);

-- Every valid source record, with the outcome of matching. Grain: one source event.
CREATE TABLE IF NOT EXISTS source_event (
    event_id          TEXT PRIMARY KEY,   -- sha1 of the stripped raw record
    source_system     TEXT NOT NULL,
    source_id         TEXT NOT NULL,
    event_ts          TEXT NOT NULL,
    email_raw         TEXT,
    phone_raw         TEXT,
    full_name_raw     TEXT,
    email_norm        TEXT,
    phone_norm        TEXT,
    name_key          TEXT,
    canonical_id      TEXT REFERENCES canonical_customer (canonical_id),
    match_rule        TEXT,               -- exact_email | rule_phone | rule_name
    match_status      TEXT NOT NULL CHECK (match_status IN ('mapped', 'unresolved')),
    unresolved_reason TEXT,               -- no_identifiers | no_match | ambiguous_name | conflicting_identifiers
    run_id            TEXT NOT NULL REFERENCES pipeline_run (run_id),
    source_file       TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    CHECK ((match_status = 'mapped' AND canonical_id IS NOT NULL AND unresolved_reason IS NULL)
        OR (match_status = 'unresolved' AND canonical_id IS NULL AND unresolved_reason IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS ix_source_event_key ON source_event (source_system, source_id, event_ts);

-- Slowly-changing history of source key -> canonical id (built from mapped events only).
CREATE TABLE IF NOT EXISTS mapping_history (
    source_system TEXT NOT NULL,
    source_id     TEXT NOT NULL,
    canonical_id  TEXT NOT NULL REFERENCES canonical_customer (canonical_id),
    valid_from    TEXT NOT NULL,
    valid_to      TEXT,                   -- NULL = still current
    is_current    INTEGER NOT NULL CHECK (is_current IN (0, 1)),
    event_count   INTEGER NOT NULL,
    run_id        TEXT NOT NULL,          -- run that last rebuilt this key
    PRIMARY KEY (source_system, source_id, valid_from)
);

-- Source keys that point at more than one canonical id.
CREATE TABLE IF NOT EXISTS mapping_conflict (
    source_system   TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    conflict_type   TEXT NOT NULL,        -- one_to_many
    canonical_count INTEGER NOT NULL,
    canonical_ids   TEXT NOT NULL,        -- sorted, comma separated
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',
    detected_run_id TEXT NOT NULL,
    PRIMARY KEY (source_system, source_id, conflict_type)
);

-- Quarantine for records that failed validation or were exact duplicates.
CREATE TABLE IF NOT EXISTS reject_record (
    reject_id         TEXT PRIMARY KEY,   -- sha1(source_file|row_number|raw)
    run_id            TEXT NOT NULL REFERENCES pipeline_run (run_id),
    source_file       TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    reject_reason     TEXT NOT NULL,
    raw_record        TEXT NOT NULL,      -- JSON
    rejected_at       TEXT NOT NULL
);

CREATE VIEW IF NOT EXISTS v_unresolved_mapping AS
SELECT e.source_system, e.source_id, e.event_ts, e.unresolved_reason,
       e.email_raw, e.phone_raw, e.full_name_raw, e.source_file, e.source_row_number, e.run_id
FROM source_event e
WHERE e.match_status = 'unresolved';

-- Current mapping per source key; has_conflict = 1 means do not trust blindly.
CREATE VIEW IF NOT EXISTS v_current_mapping AS
SELECT h.source_system, h.source_id, h.canonical_id, h.valid_from,
       CASE WHEN c.source_id IS NULL THEN 0 ELSE 1 END AS has_conflict
FROM mapping_history h
LEFT JOIN mapping_conflict c
       ON c.source_system = h.source_system AND c.source_id = h.source_id
WHERE h.is_current = 1;
