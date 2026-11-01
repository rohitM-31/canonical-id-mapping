# Reconciliation report

Overall: **PASS**

## Checks

| check | violations | result | description |
|---|---|---|---|
| run_row_balance | 0 | PASS | every successful run: rows_read = rejected + duplicate + inserted + existing |
| run_outcome_balance | 0 | PASS | every successful run: mapped + unresolved = inserted + existing |
| reject_traceability | 0 | PASS | latest run per file: quarantined rows = rejected + duplicate counters |
| event_unknown_canonical | 0 | PASS | mapped events whose canonical_id is missing from the master (referential integrity) |
| history_unknown_canonical | 0 | PASS | history rows whose canonical_id is missing from the master |
| history_one_current_per_key | 0 | PASS | each source key with mapped events has exactly one current history row |
| history_contiguous | 0 | PASS | consecutive intervals of a key touch (valid_to = next valid_from); current row has no valid_to |
| history_covers_events | 0 | PASS | sum of history event_count = number of mapped events |
| conflicts_match_events | 0 | PASS | keys with >1 canonical id are exactly the keys in mapping_conflict |
| unresolved_have_reason | 0 | PASS | unresolved events always carry a reason |

## events_by_source_system

| source_system | events | mapped | unresolved |
|---|---|---|---|
| billing | 62 | 61 | 1 |
| crm | 86 | 80 | 6 |
| web | 71 | 65 | 6 |

## mapped_by_rule

| match_rule | events |
|---|---|
| exact_email | 116 |
| rule_phone | 55 |
| rule_name | 35 |

## unresolved_by_reason

| unresolved_reason | events |
|---|---|
| no_match | 8 |
| no_identifiers | 4 |
| conflicting_identifiers | 1 |

## rejects_by_reason

| reject_reason | records |
|---|---|
| duplicate_in_batch | 6 |
| missing_source_id | 2 |
| invalid_email | 2 |
| unknown_source_system | 1 |
| missing_source_system | 1 |
| invalid_phone | 1 |
| invalid_event_ts | 1 |

## totals

| canonical_customers | source_events | quarantined_records | history_rows | one_to_many_conflicts | source_keys_with_unresolved_events |
|---|---|---|---|---|---|
| 50 | 219 | 14 | 198 | 22 | 13 |
