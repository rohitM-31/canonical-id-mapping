# Design & trade-off note

**Goal:** a small, correct, testable vertical slice: source id → canonical id, with conflicts, history and unresolved/reject visibility.

| Decision | Why | Trade-off |
|---|---|---|
| SQLite + stdlib only | Runs anywhere, no services (user constraint: WSL, no Docker). SQL gives FKs, CHECKs and reconciliation queries for free. | Single writer; not for concurrent/large-scale loads. Schema is plain SQL → portable to Postgres. |
| Canonical master as reference table | Matching needs something to map *to*; "unresolved" only has meaning against a master. | Master is synthetic here; in production it comes from an MDM/CRM owner. |
| Conservative matching (exact → rules, never guess) | A wrong link is worse than an unresolved one; conflicts are surfaced instead of resolved silently. | Lower match rate than fuzzy matching. |
| Event log + *derived* history/conflicts (rebuild per touched key) | Order-independent: late/out-of-order files and reruns give the same answer; no hand-written SCD-2 update logic to get wrong. | Rebuild cost grows with events per key (small here); load stage is the current bottleneck. |
| Content-hash `event_id` + upsert | Idempotent reruns without needing file checksums or run bookkeeping to decide "have I seen this?". | Two rows identical in every field are the same event by definition. |
| One transaction per file | Clean failure semantics: all-or-nothing, retry = rerun. | A bad row never aborts a file (it is quarantined), but an infrastructure error aborts the whole file. |
| Rejects replaced per source file | Quarantine always reflects the latest processing of a file; no stale rejects after a fix. | Rejects of older versions of the same file name are not kept. |
| In-memory master index | ~21× faster than SQL per record at 20k customers. | Master must fit in memory (millions are fine; billions are not). |
| Airflow DAG thin, logic in `src/` | Same code path from CLI, tests, and Airflow; logic is unit-testable without Airflow. | DAG discovers files at parse time (new file ⇒ wait for re-parse). |

**Evidence:** 82 automated tests (unit + end-to-end on the two delivered files + 3 000-row generated set scored against a separate answer key),
10 SQL reconciliation assertions run on every report, 100k-row run scored 100 % against the answer key.

**Known limits:** see README §9.
