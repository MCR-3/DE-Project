-- Step 2: stg_school from one Ofsted run (TODO 2-1 .. 2-12).
-- Run by `stage --step schools`, with the run in current_setting('staging.run_id').
-- Two transactions, split at the `-- @@ load` line:
--   checks  every check is written to stg_dq_result and committed, failed or not.
--           severity 'run' (whole run unusable: empty source, far fewer rows than the previous full run,
--           more than 5 % of rows quarantined) fails the step and the load does not run.
--   load    rows with a row-level problem go to stg_quarantine with their payload, the others are
--           upserted into stg_school, in the same transaction.
-- The two temp tables are built once in the checks transaction and live in the session, so the load
-- reuses them (a view would re-parse every JSON row inside the anti-join: quadratic).

-- 2-1 .. 2-9 parsing (one row per source record) -------------------------------------------------
DROP TABLE IF EXISTS school_src, school_problem;
CREATE TEMP TABLE school_src AS
SELECT r.source_key, r.sample, r.payload,
       staging.ofsted_null(r.payload->>'URN')                       AS urn_raw,
       staging.to_int(staging.ofsted_null(r.payload->>'URN'))       AS urn,
       staging.ofsted_null(r.payload->>'LAESTAB')                   AS laestab_raw,
       staging.to_int(staging.ofsted_null(r.payload->>'LAESTAB'))   AS laestab,
       staging.ofsted_null(r.payload->>'School name')               AS school_name,
       staging.ofsted_null(r.payload->>'Ofsted phase')              AS ofsted_phase,
       staging.ofsted_null(r.payload->>'Type of education')         AS type_of_education,
       staging.ofsted_null(r.payload->>'School open date')          AS open_date_raw,
       staging.parse_ofsted_date(staging.ofsted_null(r.payload->>'School open date')) AS open_date,
       staging.ofsted_null(r.payload->>'Sixth form')                AS sixth_form_raw,
       CASE staging.ofsted_null(r.payload->>'Sixth form')           -- 2-7; Not applicable / Not recorded -> NULL
            WHEN 'Has a sixth form' THEN TRUE
            WHEN 'Does not have a sixth form' THEN FALSE END        AS has_sixth_form,
       staging.normalise_postcode(staging.ofsted_null(r.payload->>'Postcode')) AS postcode,  -- 2-8
       staging.postcode_sector(staging.ofsted_null(r.payload->>'Postcode'))    AS postcode_sector,
       staging.ofsted_null(r.payload->>'Local authority')           AS local_authority_name,  -- 2-9
       staging.ofsted_null(r.payload->>'Region')                    AS region_raw,
       m.region_name                                                AS region,
       staging.ofsted_null(r.payload->>'The income deprivation affecting children index (IDACI) quintile') AS idaci_raw,
       staging.to_int(staging.ofsted_null(r.payload->>'The income deprivation affecting children index (IDACI) quintile')) AS idaci_quintile,
       staging.ofsted_null(r.payload->>'Total number of pupils')    AS total_pupils_raw,
       staging.to_int(staging.ofsted_null(r.payload->>'Total number of pupils')) AS total_pupils,
       staging.ofsted_null(r.payload->>'Statutory lowest age')      AS lowest_age_raw,
       staging.to_int(staging.ofsted_null(r.payload->>'Statutory lowest age'))  AS statutory_lowest_age,
       staging.ofsted_null(r.payload->>'Statutory highest age')     AS highest_age_raw,
       staging.to_int(staging.ofsted_null(r.payload->>'Statutory highest age')) AS statutory_highest_age,
       staging.ofsted_null(r.payload->>'Multi-academy trust name')  AS mat_name,
       staging.ofsted_null(r.payload->>'Web Link')                  AS report_url,
       CASE WHEN r.published_date ~ '^\d{4}-\d{2}-\d{2}' THEN left(r.published_date, 10)::DATE END AS published_date,
       CASE WHEN r.fetch_date ~ '^\d{4}-\d{2}-\d{2}' THEN left(r.fetch_date, 10)::DATE END AS seen_date,
       count(*) OVER (PARTITION BY staging.to_int(staging.ofsted_null(r.payload->>'URN'))) AS urn_copies
FROM raw.ofsted_schools r
LEFT JOIN staging.stg_region_map m
       ON m.source = 'ofsted' AND m.source_name = staging.ofsted_null(r.payload->>'Region')
WHERE r.run_id = current_setting('staging.run_id');

-- 2-12 row-level checks: one row per problem record, with every reason that applies ---------------
CREATE INDEX ON school_src (source_key);
ANALYZE school_src;

CREATE TEMP TABLE school_problem AS
SELECT source_key, payload, reasons FROM (
    SELECT source_key, payload, array_remove(ARRAY[
        CASE WHEN urn IS NULL THEN 'urn_invalid' END,
        CASE WHEN urn IS NOT NULL AND urn_copies > 1 THEN 'urn_duplicate' END,
        CASE WHEN school_name IS NULL THEN 'school_name_missing' END,
        CASE WHEN open_date_raw IS NOT NULL AND open_date IS NULL THEN 'open_date_invalid' END,
        CASE WHEN laestab_raw IS NOT NULL AND laestab IS NULL THEN 'laestab_invalid' END,
        CASE WHEN idaci_raw IS NOT NULL AND (idaci_quintile IS NULL OR idaci_quintile NOT BETWEEN 1 AND 5)
             THEN 'idaci_invalid' END,
        CASE WHEN total_pupils_raw IS NOT NULL AND (total_pupils IS NULL OR total_pupils < 0)
             THEN 'total_pupils_invalid' END,
        CASE WHEN (lowest_age_raw IS NOT NULL AND statutory_lowest_age IS NULL)
               OR (highest_age_raw IS NOT NULL AND statutory_highest_age IS NULL)
               OR statutory_lowest_age > statutory_highest_age THEN 'age_invalid' END,
        CASE WHEN sixth_form_raw IS NOT NULL AND sixth_form_raw NOT IN
                  ('Has a sixth form', 'Does not have a sixth form', 'Not applicable', 'Not recorded')
             THEN 'sixth_form_unknown' END,
        CASE WHEN region_raw IS NOT NULL AND region IS NULL THEN 'region_unmapped' END,
        CASE WHEN length(school_name) > 255 OR length(ofsted_phase) > 30 OR length(type_of_education) > 60
               OR length(postcode) > 10 OR length(local_authority_name) > 100 OR length(mat_name) > 150
               OR length(report_url) > 300 THEN 'value_too_long' END
    ], NULL) AS reasons
    FROM school_src
) x
WHERE cardinality(reasons) > 0;
CREATE INDEX ON school_problem (source_key);
ANALYZE school_problem;

-- checks ------------------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'schools';

WITH stats AS (
    SELECT (SELECT count(*) FROM school_src)                     AS source_rows,
           (SELECT count(*) FROM school_problem)                 AS quarantined,
           (SELECT bool_or(sample) FROM school_src)              AS sample,
           -- source rows of the latest earlier full run whose run-level checks all passed
           (SELECT d.observed_value FROM staging.stg_dq_result d
            WHERE d.stage = 'schools' AND d.check_name = 'source_rows' AND d.sample = FALSE
              AND d.run_id < current_setting('staging.run_id')
              AND NOT EXISTS (SELECT 1 FROM staging.stg_dq_result f
                              WHERE f.run_id = d.run_id AND f.stage = 'schools'
                                AND f.severity = 'run' AND f.failed_count > 0)
            ORDER BY d.run_id DESC LIMIT 1)                      AS previous_rows
), reasons(reason) AS (
    VALUES ('urn_invalid'), ('urn_duplicate'), ('school_name_missing'), ('open_date_invalid'),
           ('laestab_invalid'), ('idaci_invalid'), ('total_pupils_invalid'), ('age_invalid'),
           ('sixth_form_unknown'), ('region_unmapped'), ('value_too_long')
)
INSERT INTO staging.stg_dq_result (run_id, stage, check_name, severity, failed_count, observed_value, sample)
-- run-level: empty source, or fewer than 90 % of the previous full run's rows (full runs only)
SELECT current_setting('staging.run_id'), 'schools', 'source_rows', 'run',
       CASE WHEN source_rows = 0 THEN 1
            WHEN sample = FALSE AND previous_rows IS NOT NULL AND source_rows < 0.9 * previous_rows THEN 1
            ELSE 0 END,
       source_rows, coalesce(sample, FALSE)
FROM stats
UNION ALL
-- run-level: the landing envelope has no readable fetch date (needed for last_seen_date)
SELECT current_setting('staging.run_id'), 'schools', 'fetch_date_invalid', 'run',
       (SELECT count(*) FROM school_src WHERE seen_date IS NULL), NULL, coalesce(sample, FALSE)
FROM stats
UNION ALL
-- run-level: more than 5 % of the rows quarantined (format change, broken file)
SELECT current_setting('staging.run_id'), 'schools', 'quarantine_rate', 'run',
       CASE WHEN source_rows > 0 AND quarantined > 0.05 * source_rows THEN 1 ELSE 0 END,
       quarantined, coalesce(sample, FALSE)
FROM stats
UNION ALL
-- row-level: rows quarantined per reason (a row can have several reasons)
SELECT current_setting('staging.run_id'), 'schools', 'row_' || r.reason, 'row',
       (SELECT count(*) FROM school_problem p WHERE r.reason = ANY (p.reasons)), NULL, coalesce(s.sample, FALSE)
FROM reasons r CROSS JOIN stats s
UNION ALL
-- info for the report: schools without a full postcode (cannot be geocoded in step 4)
SELECT current_setting('staging.run_id'), 'schools', 'info_postcode_not_full', 'info',
       (SELECT count(*) FROM school_src WHERE postcode_sector IS NULL), NULL, coalesce(sample, FALSE)
FROM stats;

-- @@ load ---------------------------------------------------------------------------------------
-- (runs only if no `run` check above failed)

-- quarantine: replace this run's rows for the step
DELETE FROM staging.stg_quarantine WHERE run_id = current_setting('staging.run_id') AND stage = 'schools';
INSERT INTO staging.stg_quarantine (run_id, stage, source_key, reasons, payload)
SELECT current_setting('staging.run_id'), 'schools', source_key, reasons, payload FROM school_problem;

-- 2-11 upsert the valid rows. Step 4 fills the geo columns; they are kept unless the postcode changed.
INSERT INTO staging.stg_school AS t
    (urn, laestab, school_name, ofsted_phase, type_of_education, open_date, has_sixth_form,
     postcode, postcode_sector, local_authority_name, region, idaci_quintile, total_pupils,
     statutory_lowest_age, statutory_highest_age, mat_name, report_url, published_date, run_id,
     last_seen_run_id, last_seen_date, loaded_at)
SELECT s.urn, s.laestab, s.school_name, s.ofsted_phase, s.type_of_education, s.open_date, s.has_sixth_form,
       s.postcode, s.postcode_sector, s.local_authority_name, s.region, s.idaci_quintile, s.total_pupils,
       s.statutory_lowest_age, s.statutory_highest_age, s.mat_name, s.report_url, s.published_date,
       current_setting('staging.run_id'), current_setting('staging.run_id'), s.seen_date, now()
FROM school_src s
WHERE NOT EXISTS (SELECT 1 FROM school_problem p WHERE p.source_key = s.source_key)
ON CONFLICT (urn) DO UPDATE SET
    laestab = EXCLUDED.laestab, school_name = EXCLUDED.school_name, ofsted_phase = EXCLUDED.ofsted_phase,
    type_of_education = EXCLUDED.type_of_education, open_date = EXCLUDED.open_date,
    has_sixth_form = EXCLUDED.has_sixth_form, postcode = EXCLUDED.postcode,
    postcode_sector = EXCLUDED.postcode_sector, local_authority_name = EXCLUDED.local_authority_name,
    region = EXCLUDED.region, idaci_quintile = EXCLUDED.idaci_quintile, total_pupils = EXCLUDED.total_pupils,
    statutory_lowest_age = EXCLUDED.statutory_lowest_age, statutory_highest_age = EXCLUDED.statutory_highest_age,
    mat_name = EXCLUDED.mat_name, report_url = EXCLUDED.report_url, published_date = EXCLUDED.published_date,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at,
    last_seen_run_id = EXCLUDED.last_seen_run_id, last_seen_date = EXCLUDED.last_seen_date,
    -- a new postcode means the step 4 geocoding is stale
    latitude            = CASE WHEN t.postcode IS DISTINCT FROM EXCLUDED.postcode THEN NULL ELSE t.latitude END,
    longitude           = CASE WHEN t.postcode IS DISTINCT FROM EXCLUDED.postcode THEN NULL ELSE t.longitude END,
    la_code             = CASE WHEN t.postcode IS DISTINCT FROM EXCLUDED.postcode THEN NULL ELSE t.la_code END,
    admin_district_code = CASE WHEN t.postcode IS DISTINCT FROM EXCLUDED.postcode THEN NULL ELSE t.admin_district_code END,
    admin_district_name = CASE WHEN t.postcode IS DISTINCT FROM EXCLUDED.postcode THEN NULL ELSE t.admin_district_name END,
    admin_county_code   = CASE WHEN t.postcode IS DISTINCT FROM EXCLUDED.postcode THEN NULL ELSE t.admin_county_code END,
    admin_county_name   = CASE WHEN t.postcode IS DISTINCT FROM EXCLUDED.postcode THEN NULL ELSE t.admin_county_name END
-- an older run staged again by hand never overwrites a newer one
WHERE EXCLUDED.last_seen_date >= t.last_seen_date;

-- schools quarantined in this run still appeared in the source: they are not closed
UPDATE staging.stg_school t
SET last_seen_run_id = current_setting('staging.run_id'), last_seen_date = s.seen_date
FROM school_src s
JOIN school_problem p ON p.source_key = s.source_key
WHERE s.urn = t.urn AND s.urn_copies = 1 AND s.seen_date >= t.last_seen_date;
