# Data-quality results

Where the numbers come from: `staging.v_dq_report` (per run and step), `staging.stg_load_log.row_counts`
(reconciliation), `staging.stg_dq_result` (every check), `staging.stg_quarantine` (rows left out, with the
source record).

```sql
SELECT source, stage, checks, run_checks_failed, failed_checks, rows_quarantined, quarantine_reasons, finalized
FROM staging.v_dq_report WHERE run_id = '<run>' ORDER BY source, checked_at;
```

> **The figures below are test data**, not the report's final numbers: FSA sample run (3 authorities: Leeds,
> Bath and North East Somerset, Isles of Scilly), the full Ofsted release as at 31 October 2026 and 21,274
> postcodes. They must be regenerated from a full FSA run.

## Steps (test run, 2026-11-26)

| Source | Step | Checks | Run-level failures | Quarantined |
| --- | --- | --- | --- | --- |
| FSA | reference | 11 | 0 | 0 |
| FSA | establishments | 23 | 0 | 0 |
| FSA | fsa_measures | 10 | 0 | 0 |
| Ofsted | schools | 15 | 0 | 0 |
| Ofsted | ofsted_inspections | 20 | 0 | 0 |
| postcodes | geo | 13 | 0 | 3 (postcode not found anywhere) |

## Reconciliation (source rows = loaded + quarantined + newer + excluded)

| Table | Source | Loaded | Quarantined | Excluded |
| --- | --- | --- | --- | --- |
| stg_local_authority | 363 | 363 | 0 | 0 |
| stg_business_type | 15 | 14 | 0 | 1 (`-1 "All"`, an API filter value) |
| stg_fsa_rating / stg_fsa_score_descriptor | 11 / 17 | 11 / 17 | 0 | 0 |
| stg_food_establishment, stg_fsa_inspection | 9,682 | 9,682 | 0 | 0 |
| stg_inspection_measure (FSA overall) | 9,682 | 9,682 | 0 | 0 |
| stg_school | 21,926 | 21,926 | 0 | 0 |
| stg_ofsted_inspection (by inspection number) | 24,354 | 24,354 | 0 | 0 |
| stg_postcode | 21,274 | 21,271 | 3 | 0 |

Unexplained rows: 0 for all three runs, which were therefore logged in `stg_load_log` and their `raw.*` rows
deleted.

## Coverage figures

- Schools with `la_code`: 21,923 / 21,926 (by postcode 21,885, by reverse geocoding 38, not found 3). Reverse
  geocoding distances: median 42 m, max 276 m. 37 schools are located in a district other than their Ofsted
  authority (boundary schools).
- Ofsted events: renewed framework 3,637, EIF 2019 13,042, ungraded 7,675 (39 numbers that are both an EIF
  and an ungraded inspection are counted once, as EIF). Implied grades (`OFSTED_UNGRADED_EST`) with a value:
  5,258 / 7,675.
- Measure rows without a score (`raw_score` NULL): 10,000 Ofsted (Not applicable 4,299, Not judged 3,284,
  unclear ungraded outcome 2,417); OEIF placeholders 0 / 9 dropped: 29.
- Food businesses: coordinates from the FSA 7,313, from postcodes.io 575, none 1,794 (partial or missing
  postcode); without sub-scores 2,333 (of which not yet inspected 2,264); rated but without sub-scores 65.

## Failure handling (tested)

Each of these left the run unloaded, unlogged and its raw rows in place, with the failed check recorded:
fewer than 90 % of the previous full run's rows, more than 5 % of rows quarantined (a date format change),
an earlier step not run, a district missing from the seed, and a source record not explained at
reconciliation.
