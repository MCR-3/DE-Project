# Staging quarantines bad rows, fails bad runs, and keeps every check

A landed run can be wrong in two ways: a few records are broken (a missing URN, an unreadable date), or the whole run is (the source changed its date format, a download was cut short). Staging handles them differently. Each step runs in two transactions: first its checks, written to `staging.stg_dq_result` and committed whether they pass or not; then, only if no run-level check failed, the load, in which every record with a row-level problem is written to `staging.stg_quarantine` (with the record as received and the reasons) instead of the target table. A run-level failure (empty source, fewer than 90 % of the previous full run's rows, more than 5 % of rows quarantined, an earlier step not passed) loads nothing and exits 1. A last step per source reconciles the run (every source record must be loaded, quarantined, superseded by a newer run or excluded on purpose), records it in `staging.stg_load_log` and only then deletes the run's `raw.*` copy.

## Considered Options

- **Fail the run on any bad record**: simplest, but one broken row would block a whole monthly release. Rejected.
- **Quarantine in a MongoDB collection** (the first plan, next to the landed documents): the load runs in PostgreSQL, so the quarantine could not be written in the same transaction and could disagree with what was loaded. Rejected.
- **Checks in the same transaction as the load**: a failure rolls the checks back too, losing the evidence of why the run failed. Rejected.

## Consequences

- The report's data-quality numbers come from tables (`v_dq_report`, `stg_load_log.row_counts`), for failed runs as well as successful ones.
- A record quarantined in a new run keeps its previous valid version in staging.
- Thresholds (90 %, 5 %) only compare full runs; sample runs are never compared with full ones.
- `raw.*` is a temporary buffer: re-staging a finalized run means copying it again from MongoDB.
