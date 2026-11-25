# What the warehouse can rely on in `staging.*`

The warehouse reads only the `staging` schema of `project-db` (`raw.*` is deleted after each run). DDL:
`docker_scripts/staging_loader/sql/01_tables.sql`.

## Which runs to read

- `staging.stg_load_log` has one row per source (`fsa`, `ofsted`, `postcodes`) and run that was fully staged
  and reconciled. A run that is not there is in progress or failed: do not read it.
- **Latest full run of a source**: `SELECT run_id FROM staging.stg_load_log WHERE source = 'fsa' AND NOT
  sample ORDER BY run_id DESC LIMIT 1`. Sample runs only cover a few authorities and must never be used as
  the state of the source.
- Every `stg_*` row has `run_id` (the run that last wrote its values) and `loaded_at`.

## Tables

| Table | One row per | Key | Notes |
| --- | --- | --- | --- |
| `stg_local_authority` | FSA local authority (all UK) | `la_code` | `country` to keep England, `region` (Ofsted names) |
| `stg_business_type` | FSA business type | `business_type_id` | `-1 "All"` is left out |
| `stg_school` | Ofsted school, latest state | `urn` | location, district, county, `la_code` from step 4 |
| `stg_food_establishment` | FSA business, latest state (England) | `fhrs_id` | |
| `stg_ofsted_inspection` | Ofsted inspection event, accumulated | `inspection_number` | `framework` RENEWED_2025 / EIF_2019 / UNGRADED |
| `stg_fsa_inspection` | FSA rating event, accumulated | `fsa_inspection_id`, unique `(fhrs_id, rating_date)` | |
| `stg_inspection_measure` | measure of an inspection | `measure_row_id`, unique `(fsa_inspection_id, ofsted_inspection_number, measure_code)` | feeds `Fact_Inspection_Measure` |
| `stg_measure` | measure (seed) | `measure_code` | source of `Dim_Measure` |
| `stg_ofsted_level`, `stg_fsa_rating`, `stg_fsa_score_descriptor` | level of a scale | | sources of `Dim_Level` |
| `stg_postcode` | postcode looked up on postcodes.io | `postcode` | latest answer, `status` live / terminated / not_found |
| `stg_dq_result`, `stg_quarantine`, `v_dq_report` | | | data-quality evidence |

## Geography

- **`la_code` is an FSA district code and a `VARCHAR`** (`stg_local_authority.la_code`, e.g. `'857'`, the FSA
  `LocalAuthorityCode`; not the API id `LocalAuthorityId`, kept as `fsa_authority_id`). It matches
  `Dim_Geography.Local_Authority_Code` (VARCHAR).
- Schools are matched to FSA authorities **by location**, not by the Ofsted local authority name (see
  [ADR 0003](../adr/0003-schools-matched-to-fsa-districts-by-postcode.md)):
  - `stg_school.match_method`: `postcode` (district of the school's postcode), `reverse_geocode` (terminated
    postcode: district of the nearest live postcode within 2 km, accepted only if it belongs to the school's
    Ofsted local authority), or NULL (no match). `reverse_distance_m` keeps that distance, even when rejected.
  - Schools near a boundary can be located in a district other than their Ofsted (maintaining) authority
    (37 in the October 2026 file, e.g. maintained by Lambeth, located in Southwark). For counts by maintaining
    authority, use `stg_school.local_authority_name` (as written by Ofsted).
  - `admin_county_code` / `admin_county_name` are NULL for unitary authorities (London boroughs,
    metropolitan districts, unitaries); a county roll-up rule is the warehouse's choice.
- Two FSA English authorities are **port health authorities**, not districts: Hull and Goole Port (626) and
  River Tees (648). They have food premises but never a school, so they appear in authority-level KPIs (Q1,
  Q2, Q4) without a school score.
- Region names are Ofsted's (`East of England`, `Yorkshire and the Humber`, ...), mapped from the FSA and
  postcodes.io names by `seeds/region_map.csv`.

## Food businesses

- Coordinates: `coord_source` = `fsa` (from the FSA), `postcodes_io` (filled from the postcode in step 4), or
  NULL. Businesses with a partial postcode (`postcode_is_partial`, outward code only) or none have no
  coordinates and drop out of Q3 and Q5; they still count in authority-level analysis.
- **Current rating of a business = its `stg_fsa_inspection` row with the latest `last_seen_run_id`
  (= latest `last_fetch_date`)**, not the row with the latest date. A not-inspected row (AwaitingInspection,
  Exempt: `rating_date` NULL, one per business) is dated by `first_fetch_date`, which can be later than the
  real `rating_date` of the inspection that follows. The report's `v_venue_latest` (`ORDER BY d.Date DESC`)
  needs this change.
- Ratings join their level by **value**: `stg_fsa_inspection.rating_value = stg_fsa_rating.rating_key_name`.
  The FSA `/Ratings` list only has FHIS keys for the statuses (`fhis_awaiting_inspection_en-gb`), while English
  businesses use `fhrs_awaitinginspection_en-gb`, so joining on `rating_key` loses about 23 % of them.
- `AwaitingPublication` (inspected, not yet published, has a date) exists besides 0-5, AwaitingInspection and
  Exempt. Treat it as unrated in KPIs.

## Closures (SCD2)

- `stg_school` and `stg_food_establishment` are never deleted from. `last_seen_run_id` / `last_seen_date` are
  the last run in which the school or business appeared in the source (and its fetch date). **A row whose
  `last_seen_run_id` is not the latest full run of its source has disappeared from the source** -> close its
  `Dim_Institution` version (`Valid_To`, `Is_Current = false`).
- `last_seen_*` is updated even when the row was quarantined in that run (it still exists); only a record whose
  key itself is broken (URN / FHRSID) cannot be matched and looks closed (see `stg_quarantine`).
- A row quarantined in a newer run keeps its last valid values, so `run_id` can differ between rows.

## Measures and levels

- `stg_measure` (seed `measure.csv`, 20 measures; names follow the report's examples):
  - FSA: `FHRS_HYGIENE`, `FHRS_STRUCTURAL` (penalty points 0-25), `FHRS_CONF_MGMT` (0-30), lower is better;
    `FHRS_OVERALL` (0-5, higher is better, `is_overall`).
  - Renewed framework: `OFSTED_RC_*` (1-5); the six core areas are `in_composite`, early years and post-16 are
    not.
  - EIF 2019: `OFSTED_EIF_*` (1-4); the four key judgements are `in_composite`, `OFSTED_EIF_OVERALL` is
    `is_overall`, early years and sixth form are not.
  - `OFSTED_UNGRADED_EST` (1-2): the grade implied by an ungraded outcome ("School remains Outstanding..." = 1,
    "School remains Good..." = 2, anything else NULL). About a third of schools only have an ungraded
    inspection. **It is seeded with `in_composite = false`: whether it counts in the KPIs is the warehouse's
    decision.**
- `stg_inspection_measure.level_text` is the value as published; it joins its level on
  `(stg_measure.scale, level_text)`: `stg_ofsted_level` for Ofsted, `stg_fsa_rating.rating_key_name` for
  `FHRS_OVERALL`, the score itself for FSA penalty points.
- `raw_score` is NULL for Ofsted `Not applicable` / `Not judged` / unclear ungraded outcomes and for FSA
  overall ratings that are not 0-5. Average `raw_score IS NOT NULL` only. OEIF placeholders 0 and 9 give no
  row; a missing FSA sub-score gives no row; the overall row always exists.
- An inspection number that is both the OEIF graded and the ungraded inspection of a school (39 in October
  2026) is one `EIF_2019` event, without an implied-grade row.
- A measure that disappears from the source (e.g. early years no longer judged) is deleted from its event.

## Data quality

- `staging.v_dq_report`: per run and step, the number of checks, the run-level checks that failed, the rows
  quarantined and why, and whether the run was finalized.
- `staging.stg_load_log.row_counts`: the reconciliation of the run (per table: loaded / quarantined / newer /
  excluded).
- See [`dq-results.md`](dq-results.md).

## Open questions for the warehouse

1. `OFSTED_UNGRADED_EST` in the composite score or not (`stg_measure.in_composite`).
2. Keep or drop the two port health authorities in authority-level KPIs.
3. County-level reporting: add a county column to `Dim_Geography`, and the rule for unitary authorities
   (county NULL).
