# Source-ID → Canonical-ID Mapping Pipeline

Maps source identifiers (`source_system` + `source_id`) from CRM / web / billing extracts to **canonical customer ids**
using exact and rule-based matching. It detects **one-to-many conflicts**, keeps **mapping history**, and exposes
**unresolved** and **rejected** records. Runs locally with plain Python + SQLite, orchestrated by **Airflow in WSL (no Docker)**.

* Inputs delivered with the task: `data/input/dataset_1.csv` (199 rows), `data/input/dataset_2.csv` (34 rows)
* Canonical master (reference data): `data/reference/canonical_customers.csv` (50 synthetic customers)
* Design / trade-offs: [docs/design_note.md](docs/design_note.md)

## 1. Architecture / flow

```
 data/input/dataset_*.csv ──► ingest ──► validate ──► normalise ──► match ──► load (ONE transaction) ──► reports
                              (csv,      (rules;       (email,      (exact     source_event  (upsert)     CSV extracts
                               schema     rejects →     phone,       email →    reject_record (replace)    reconciliation
                               check)     quarantine)   name,ts)     phone →    mapping_history (rebuild)  report (md/json)
                                                                     name)      mapping_conflict (rebuild)
 data/reference/canonical_customers.csv ──► canonical_customer (master, in-memory index for matching)

 Airflow DAG `source_mapping`:  init_store → ingest_dataset_1 → ingest_dataset_2 → publish_reports (fails if a check fails)
```

| Module | Responsibility |
|---|---|
| `config/config.py` | paths, source contract (required columns, valid systems), env overrides (`.env`) |
| `src/ingestion/load_data.py` | CSV readers, schema check, malformed-row flagging |
| `src/validation/validate_data.py` | row rules → list of reject reasons |
| `src/transformation/normalize_data.py` | pure, deterministic normalisers + stable id functions |
| `src/matching/matching_rules.py` | `MasterIndex` + `resolve()` (exact/rule matching, conflict/ambiguity handling) |
| `src/matching/conflict_detection.py` | pure functions: history intervals, one-to-many detection |
| `src/mapping/canonical_mapping.py` | all DB writes (master, events, rejects, history/conflict rebuild) |
| `src/pipeline.py` | orchestration, run bookkeeping, transaction/rollback |
| `src/reporting/generate_reports.py` | CSV extracts, reconciliation checks (`sql/reconciliation.sql`), label scoring |
| `dags/source_mapping_dag.py` | Airflow DAG |
| `scripts/` | `run_pipeline.py` (CLI), `generate_large_dataset.py`, `benchmark.py`, WSL/Airflow setup |

## 2. Setup & run

### A. Quick run (any OS, Python ≥ 3.9, no Airflow)
```bash
python -m venv venv && source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m pytest                                        # 82 tests
python scripts/run_pipeline.py --reset                  # both datasets -> data/output/
```
Outputs in `data/output/`: `current_mapping.csv`, `mapping_history.csv`, `mapping_conflicts.csv`,
`unresolved_mappings.csv`, `rejected_records.csv`, `run_summary.csv`, `reconciliation_report.md|json`, `mapping.db`.

### B. Airflow in WSL (no Docker)
Needs Ubuntu on WSL 2 (`wsl --install -d Ubuntu`, reboot if asked) and Python 3.8–3.12 (Ubuntu 22.04/24.04 defaults work).
```bash
cd /mnt/c/Users/<you>/OneDrive/Documents/rohith/yotaflex.ai_assignment     # or a clone inside ~/ (faster)
bash scripts/setup_airflow_wsl.sh          # venv in ~/venvs/mapping-airflow, Airflow 2.10.5 + constraints, db migrate
source scripts/airflow_env.sh
airflow dags test source_mapping           # runs the whole DAG once, no scheduler needed
airflow standalone                         # UI at http://localhost:8080, then trigger DAG `source_mapping`
```
`airflow_env.sh` puts `AIRFLOW_HOME` and the mapping DB under `~/` (Linux filesystem) because SQLite locking on `/mnt/c`
(OneDrive) is unreliable. Results land in `~/mapping_data/output/`. Override with `MAPPING_DB_PATH` / `OUTPUT_DIR`.

> The DAG uses only stock operators/TaskFlow and works on Airflow 2.x and 3.x imports. It was executed with
> `airflow dags test source_mapping` on Airflow 2.10.5 / Python 3.12 in WSL: all 4 tasks succeeded and the totals
> (219 events, 22 conflicts, 198 history rows, 10/10 reconciliation checks) match the CLI run.
> If your Ubuntu ships Python > 3.12, `setup_airflow_wsl.sh` installs a private Python 3.12 via `uv`.

### C. Larger run (100k rows) with answer key
```bash
python scripts/generate_large_dataset.py --rows 100000 --customers 20000 --seed 42 --out data/large
python scripts/run_pipeline.py --files data/large/dataset_large.csv --reference data/large/canonical_customers.csv \
       --db data/output/large.db --output-dir data/output/large --labels data/large/expected_labels.csv --reset
python scripts/benchmark.py                                  # throughput + optimisation comparison
```
The generator writes the input and the **answer key separately** (`expected_labels.csv`, `expected_conflicts.csv`).

## 3. Matching rules
Identifiers are normalised first (see §6), then matched against the canonical master in this precedence:

| Rule | Logic |
|---|---|
| `exact_email` | normalised email = master email |
| `rule_phone` | digits only, leading `1` dropped, 10 digits = master phone (`(200) 000-0021` = `1-200-000-0021` = `+1 200 000 0021`) |
| `rule_name` | name key = first+last token, case/punctuation/middle-initial/order-insensitive (`Smith, Kai A` = `KAI A. SMITH`); needs ≥ 2 tokens |

All identifiers on a record are evaluated. Outcomes:
* one canonical id (possibly confirmed by several rules) → **mapped** (rule = highest-precedence hit)
* identifiers point to **different** canonical ids → **unresolved: `conflicting_identifiers`** (never guessed)
* name matches several master customers and nothing stronger → **unresolved: `ambiguous_name`**
* nothing matches → `no_match`; nothing to match on → `no_identifiers`

**One-to-many conflict** = a `(source_system, source_id)` whose mapped events resolve to more than one canonical id
(source ids re-used for different people). Many-to-one (several source ids → one customer) is normal and not flagged.

## 4. Data dictionary
Conventions: snake_case; timestamps ISO-8601 UTC `YYYY-MM-DDTHH:MM:SSZ`; every loaded/rejected row carries `run_id` and `source_file`
(+ `source_row_number` = 1-based data-row index in that file). Canonical id = `can_` + first 10 hex of SHA-1(normalised master email) — deterministic,
so reloading the master never changes ids. `event_id` = SHA-1 of the stripped raw fields — identical rows collapse.

**Source file** (`dataset_*.csv`, grain: one source event)

| column | type | rules |
|---|---|---|
| `source_system` | text | required; one of `crm`,`web`,`billing` |
| `source_id` | text | required; `<system>-<8 digits>`, prefix must equal `source_system` |
| `event_ts` | ISO-8601 timestamp with timezone | required; converted to UTC |
| `email` | text | optional; if present must look like an email |
| `phone` | text | optional; if present must be a 10-digit number (optional `+1`/`1`, separators `()-. `) |
| `full_name` | text | optional; `First M Last`, `First M. Last`, `Last, First M`, any case/spacing |

Missing required column ⇒ file refused. Extra column ⇒ warning, ignored.

**Tables** (`sql/create_tables.sql`)

| table | grain / PK | notes |
|---|---|---|
| `canonical_customer` | one per customer · `canonical_id` | master; `email_norm` unique; indexed on phone/name |
| `pipeline_run` | one per file execution · `run_id` | `status` running/success/failed, `started_at`, `ended_at`, `rows_read/rejected/duplicate/inserted/existing/mapped/unresolved/late`, `error_message`, `warnings` |
| `source_event` | one per distinct valid record · `event_id` | raw + normalised fields, `canonical_id` (FK, null if unresolved), `match_rule`, `match_status`, `unresolved_reason`, `run_id`, `source_file`, `source_row_number`; CHECK ensures mapped⇔canonical_id, unresolved⇔reason |
| `mapping_history` | one per interval · (`source_system`,`source_id`,`valid_from`) | `canonical_id` (FK), `valid_to` (NULL = current), `is_current`, `event_count` — SCD-2 style, rebuilt from events |
| `mapping_conflict` | one per conflicted key · (`source_system`,`source_id`,`conflict_type`) | `canonical_ids`, `canonical_count`, `first_seen`, `last_seen`, `status`, `detected_run_id` |
| `reject_record` | one per quarantined row · `reject_id` | `reject_reason` (`;`-joined), `raw_record` JSON, traceable via `source_file` + `source_row_number` |

Views: `v_unresolved_mapping`, `v_current_mapping` (`has_conflict` = 1 ⇒ don't trust blindly).

Reject reasons: `malformed_row`, `missing_source_system`, `unknown_source_system`, `missing_source_id`, `invalid_source_id_format`,
`source_id_system_mismatch`, `invalid_event_ts`, `invalid_email`, `invalid_phone`, `duplicate_in_batch`.

## 5. Results on the delivered datasets
(`python scripts/run_pipeline.py --reset`; full detail in `data/output/reconciliation_report.md`)

| | dataset_1 | dataset_2 |
|---|---|---|
| rows read | 199 | 34 |
| rejected (invalid) | 8 | 0 |
| exact duplicates quarantined | 6 | 0 |
| new events loaded | 185 | 34 |
| mapped / unresolved | 173 / 12 | 33 / 1 |
| late-arriving | 1 | 6 |

Combined: 219 events, 22 one-to-many conflicts, 198 history rows, 13 unresolved events (8 `no_match`, 4 `no_identifiers`,
1 `conflicting_identifiers` — `sam.smith38@…` carrying another customer's phone). All 10 reconciliation checks pass.
Sample queries:
```sql
SELECT * FROM v_unresolved_mapping;                                   -- what could not be mapped, and why
SELECT * FROM mapping_conflict ORDER BY canonical_count DESC;         -- source ids that point to >1 customer
SELECT * FROM mapping_history WHERE source_id='crm-00000022' ORDER BY valid_from;   -- history of one key
SELECT reject_reason, COUNT(*) FROM reject_record GROUP BY 1;
```

**100k-row run** (seed 42, 20 000 customers) scored against the separate answer key: outcome accuracy 1.000,
canonical-id accuracy on mapped rows 1.000, conflicts precision/recall 1.000/1.000 (1 830 conflicts). The synthetic data is
rule-shaped, so this proves the implementation matches its spec, not that matching is perfect on real-world data.

## 6. Normalisation & edge-case handling

| Scenario | Handling | Test |
|---|---|---|
| malformed input (wrong field count, bad values) | row quarantined with reason | `test_malformed_and_invalid_records…`, `test_ingestion` |
| missing required field | `missing_*` reject | `test_validation` |
| duplicate record | first kept, later copies → `duplicate_in_batch`; across files → not re-inserted | `test_exact_duplicates…`, `test_same_row_in_second_file…` |
| unexpected schema change | missing column ⇒ run `failed`, nothing loaded; extra column ⇒ warning | `test_unexpected_schema_change…`, `test_extra_column…` |
| empty batch | run `success`, 0 rows, warning | `test_empty_batch…` |
| partial failure + retry | single transaction → rollback, run `failed`; retry with same `run_id` succeeds | `test_partial_failure_rolls_back…` |
| late / out-of-order data | history rebuilt from the full event log ordered by `event_ts`; counted in `rows_late` | `test_late_arriving…`, `test_out_of_order…` |
| invalid reference key | FK on `canonical_id`; reconciliation check | `test_invalid_reference_key…`, `test_reconciliation_detects_corruption` |
| rerun of same input | 0 inserted, identical output | `test_rerun_of_same_input…` |
| one-to-many conflict | `mapping_conflict` + `has_conflict` flag | `test_one_to_many_conflict…` |
| conflicting / ambiguous identifiers | unresolved with reason | `test_matching`, `test_conflicting_identifiers…` |
| nulls in optional identifiers | fine; no identifiers ⇒ unresolved | `test_null_identifiers…` |

## 7. Idempotency, retry, restart, backfill
* **Idempotent load:** `event_id` is a content hash → `INSERT … ON CONFLICT DO UPDATE`; history and conflicts are *rebuilt per touched key*
  from the event log (not appended); rejects are *replaced per source file*; master upsert keyed by deterministic `canonical_id`.
* **Retry/restart:** one transaction per file. Failure ⇒ rollback + `pipeline_run.status='failed'` with `error_message`. Re-running is the recovery
  procedure. Airflow retries (2×, 30 s) reuse the run_id; the run row is reset, not duplicated. A failed file does not block a manual re-trigger.
* **Backfill / reprocess:** put new/old files in `data/input/` and run again, in any order — result does not depend on file order. If the master or rules
  change, re-run the files: existing events are re-resolved (`rows_existing`) and history/conflicts re-derived (see `test_backfill_after_master_change…`).
* **Observability:** structured log line per run + `pipeline_run` (run_id, start/end, read/rejected/duplicate/inserted/existing/mapped/unresolved/late, error, warnings)
  + `reconciliation_report.md` (10 SQL assertions in `sql/reconciliation.sql`; the DAG fails if any fail).

## 8. Performance
`python scripts/benchmark.py` (100 000 rows, dev laptop, Windows, Python 3.14, SQLite):

* full pipeline ≈ **13–15 k rows/s** (~7 s; read 0.2 s, transform 1.7 s, load 5 s)
* rerun of the same file: ~9 s, 0 inserted
* **Optimisation applied:** master held in in-memory hash indexes instead of a SQL lookup per identifier per record
  → matching 20 000 records **0.07 s vs 1.4 s (≈ 21×)** (baseline already uses indexes on email/phone/name). `lru_cache` on normalisers and
  `executemany` batching are the other cheap wins.
* **Next bottleneck:** the load stage (event upsert + history rebuild). Options: chunked loads, `PRAGMA synchronous=OFF` for bulk backfills, or Postgres.

## 9. Assumptions & known limitations
* The canonical master is **synthetic reference data** (no master was supplied): 50 customers `first.smith<N>@example.com`, derived from the patterns in the two files.
  Customers present in the files but not in the master (`nobody…@nomatch.test`) are intentionally *unresolved*, not auto-created.
* Matching is deterministic and conservative; there is no fuzzy/probabilistic scoring (typos in names will not match).
* A source key whose events map to different customers is flagged, but the pipeline does **not** decide which is right (`v_current_mapping.has_conflict`).
  Conflict `status` stays `open`; there is no workflow to resolve/close it.
* Records with only a name rely on the name rule, which is the weakest signal; ambiguous names are left unresolved.
* SQLite = single writer. Fine for this scope; the DAG chains file loads and `max_active_runs=1`.
* Timestamps without a timezone are rejected rather than assumed UTC.
* `docker-compose.yaml` / `docker/` in the repo are leftovers from the original scaffold and are not used.

## 10. Possible next steps
Postgres target + partitioning; fuzzy name matching with a review queue; conflict resolution workflow (status transitions, steward overrides);
incremental file discovery (sensor) and file checksums for exact-file replay detection; data-quality metrics pushed to a monitoring tool;
Airflow task-level alerting; large-file streaming in chunks.
