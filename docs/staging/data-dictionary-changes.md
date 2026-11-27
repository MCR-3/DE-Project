# Corrections to the data dictionary (Deliverable 1, section 6)

Section 6 of the report was written before staging existed and said it would change. This is what the
implementation does instead. The reference is `docker_scripts/staging_loader/sql/01_tables.sql`.

## General

- PostgreSQL 18 (not 15+), two schemas: `raw` (a temporary JSONB copy of the MongoDB documents) and `staging`
  (the `stg_*` tables).
- Ofsted dates are **`dd/mm/yyyy`** (e.g. `09/03/2023`), not `9/3/23`. The file has **76** columns, not about
  80, and the 2019-framework columns are named `OEIF`.
- Placeholders: grade `0` is more common than `9` (OEIF early years / sixth form); both are treated as missing.
  The Ofsted category of concern uses `NULL` for "no such inspection" and `''` for "inspected, no concern".
- FSA sub-scores: Hygiene 0-25, Structural 0-25, Confidence in management 0-30, in steps of 5 (not "0-30" for
  all), checked against the FSA score descriptors.
- postcodes.io is used for the **schools and for food businesses that have a full postcode but no
  coordinates**, not for the schools only.
- Schools are matched to local authorities **by postcode -> ONS district code -> FSA authority**, not by the
  Ofsted local authority name (about 42 % of schools did not match by name: Ofsted uses counties, the FSA
  districts). Terminated postcodes use the district of the nearest live postcode within 2 km, accepted only if
  it belongs to the school's Ofsted authority. See ADR 0003.
- FSA English authorities include two port health authorities (Hull and Goole Port, River Tees) with no
  district and no school.

## Tables

- `stg_local_authority`: `la_code` INTEGER -> **VARCHAR(10)**; `ofsted_la_name` **removed** (replaced by the
  district matching); `fsa_authority_id` added; `website` is the FSA `Url`; `region` uses Ofsted names.
- `stg_school`: `la_code` VARCHAR; `source_file` -> `published_date` (the release's "as at" date) + `run_id`;
  added `admin_district_code/name`, `admin_county_code/name`, `match_method`, `reverse_distance_m`,
  `last_seen_run_id`, `last_seen_date`.
- `stg_food_establishment`: `la_code` VARCHAR; `last_extract_date` -> `last_seen_date` + `last_seen_run_id` +
  `published_date`; added `postcode_is_partial`, `coord_source`.
- `stg_ofsted_inspection`: `source_file` -> `published_date` + `run_id`; added `laestab_at_inspection`,
  `school_name_at_inspection`, `school_type_at_inspection`; an inspection number that is both the OEIF graded
  and the ungraded inspection is one `EIF_2019` event.
- `stg_fsa_inspection`: `extract_date` -> `first_fetch_date` / `last_fetch_date` + `published_date` +
  `last_seen_run_id` (the current rating is the row seen last); unique `(fhrs_id, rating_date)` with NULLS NOT
  DISTINCT (one not-inspected row per business); `rating_value` also has `AwaitingPublication`.
- `stg_inspection_measure`: `level_text` VARCHAR(30) -> 60 (ungraded outcome sentences); measures listed in
  `stg_measure`, plus `OFSTED_UNGRADED_EST`; `Not applicable` / `Not judged` give a row with `raw_score` NULL;
  OEIF 0 / 9 give no row.
- New tables: `stg_fsa_rating`, `stg_fsa_score_descriptor`, `stg_ofsted_level`, `stg_measure`,
  `stg_region_map`, `stg_district_la_map`, `stg_postcode`, `stg_load_log`, `stg_dq_result`, `stg_quarantine`;
  view `v_dq_report`.

## DQ1 (data-quality task)

- Footnote: **quarantine is a PostgreSQL table, not a MongoDB collection** (`staging.stg_quarantine`: the
  source record, the reasons, the step and the time), so that it is written in the same transaction as the
  load.
- Checks are recorded in `staging.stg_dq_result` with a severity: `run` (fails the whole run: empty source,
  fewer than 90 % of the previous full run's rows, more than 5 % quarantined), `row` (rows quarantined per
  reason), `info`. A run is only finalized when every source record is explained (loaded, quarantined,
  superseded by a newer run or excluded on purpose). See ADR 0004.
- The demo query `v_venue_latest` must pick the rating seen last (`last_seen_run_id`), not the latest date.
