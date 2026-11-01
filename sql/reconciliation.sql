-- Each block is one check: the query returns a single number = count of violations (0 = pass).
-- Format: "-- check: <name> | <description>" followed by exactly one SELECT.

-- check: run_row_balance | every successful run: rows_read = rejected + duplicate + inserted + existing
SELECT COUNT(*) FROM pipeline_run
WHERE status = 'success'
  AND rows_read <> rows_rejected + rows_duplicate + rows_inserted + rows_existing;

-- check: run_outcome_balance | every successful run: mapped + unresolved = inserted + existing
SELECT COUNT(*) FROM pipeline_run
WHERE status = 'success'
  AND rows_mapped + rows_unresolved <> rows_inserted + rows_existing;

-- check: reject_traceability | latest run per file: quarantined rows = rejected + duplicate counters
SELECT COUNT(*) FROM pipeline_run r
WHERE r.status = 'success'
  AND r.started_at = (SELECT MAX(started_at) FROM pipeline_run x WHERE x.source_file = r.source_file AND x.status = 'success')
  AND r.rows_rejected + r.rows_duplicate <> (SELECT COUNT(*) FROM reject_record q WHERE q.source_file = r.source_file);

-- check: event_unknown_canonical | mapped events whose canonical_id is missing from the master (referential integrity)
SELECT COUNT(*) FROM source_event e
WHERE e.canonical_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM canonical_customer c WHERE c.canonical_id = e.canonical_id);

-- check: history_unknown_canonical | history rows whose canonical_id is missing from the master
SELECT COUNT(*) FROM mapping_history h
WHERE NOT EXISTS (SELECT 1 FROM canonical_customer c WHERE c.canonical_id = h.canonical_id);

-- check: history_one_current_per_key | each source key with mapped events has exactly one current history row
SELECT COUNT(*) FROM (
  SELECT k.source_system, k.source_id,
         (SELECT COUNT(*) FROM mapping_history h
           WHERE h.source_system = k.source_system AND h.source_id = k.source_id AND h.is_current = 1) AS n_current
  FROM (SELECT DISTINCT source_system, source_id FROM source_event WHERE match_status = 'mapped') k
) WHERE n_current <> 1;

-- check: history_contiguous | consecutive intervals of a key touch (valid_to = next valid_from); current row has no valid_to
SELECT COUNT(*) FROM (
  SELECT valid_to, is_current,
         LEAD(valid_from) OVER (PARTITION BY source_system, source_id ORDER BY valid_from) AS next_from
  FROM mapping_history
) WHERE (is_current = 1 AND valid_to IS NOT NULL)
     OR (is_current = 0 AND (valid_to IS NULL OR valid_to <> next_from));

-- check: history_covers_events | sum of history event_count = number of mapped events
SELECT ABS((SELECT COALESCE(SUM(event_count), 0) FROM mapping_history)
         - (SELECT COUNT(*) FROM source_event WHERE match_status = 'mapped'));

-- check: conflicts_match_events | keys with >1 canonical id are exactly the keys in mapping_conflict
WITH expected AS (
  SELECT source_system, source_id FROM source_event WHERE match_status = 'mapped'
  GROUP BY source_system, source_id HAVING COUNT(DISTINCT canonical_id) > 1
)
SELECT (SELECT COUNT(*) FROM (SELECT * FROM expected EXCEPT SELECT source_system, source_id FROM mapping_conflict))
     + (SELECT COUNT(*) FROM (SELECT source_system, source_id FROM mapping_conflict EXCEPT SELECT * FROM expected));

-- check: unresolved_have_reason | unresolved events always carry a reason
SELECT COUNT(*) FROM source_event WHERE match_status = 'unresolved' AND unresolved_reason IS NULL;
