# Staging keeps inspection history and tracks when each row was last seen

The FSA API only exposes the current rating of a business and the Ofsted file only the latest inspections of each school, so history only exists if staging keeps what every run saw. Inspection events accumulate: `stg_fsa_inspection` has one row per business and rating date, `stg_ofsted_inspection` one row per inspection number, and a rating changed on appeal (same date) is updated in place. Schools and businesses are kept as their latest state and never deleted; instead `last_seen_run_id` / `last_seen_date` record the last run in which each appeared (even if its record was quarantined there), so a row whose last run is not the latest full run has disappeared from the source. Ratings get a `last_seen_run_id` too: a business's current rating is its rating seen last. A run staged again after a newer one never overwrites newer values.

## Considered Options

- **Stage only the latest run, replacing everything**: simple, but loses the rating history the warehouse fact table is built on, and closures become invisible. Rejected.
- **Delete rows that disappear from the source**: loses the inspections of closed schools and businesses, which the warehouse's SCD2 dimension must keep. Rejected.
- **Current rating = latest date**: wrong when a not-inspected business (no rating date, dated by its fetch date) is inspected later, since the inspection date can be earlier than that fetch date. Rejected.

## Consequences

- The warehouse detects closures and current ratings from `last_seen_run_id` and the latest full run in `stg_load_log`, never from dates.
- A not-inspected business has a single not-inspected row (unique key with NULLS NOT DISTINCT), kept as history once it is inspected.
- Staging a sample run by hand updates `last_seen_*` for only a few authorities: comparisons must always use full runs.
