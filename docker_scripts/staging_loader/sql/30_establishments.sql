-- Step 3: stg_food_establishment and stg_fsa_inspection from one FSA run (TODO 3-1 .. 3-11).
-- Run by `stage --step establishments`, with the run in current_setting('staging.run_id').
-- Same two-transaction rule as step 2 (split at `-- @@ load`): checks committed first, failed or not;
-- the load runs only if no `run` check failed, with row-level problems sent to stg_quarantine.
-- Needs step 1 (reference) staged for the same run: la_code, business types and ratings are checked
-- against it.

-- 3-1 .. 3-7 parsing (one row per source record) -------------------------------------------------
DROP TABLE IF EXISTS est_src, est_problem;
CREATE TEMP TABLE est_src AS
SELECT r.source_key, r.sample, r.payload,
       staging.to_int(r.payload->>'FHRSID')                          AS fhrs_id,
       staging.blank_to_null(r.payload->>'LocalAuthorityBusinessID') AS la_business_id,
       staging.blank_to_null(r.payload->>'BusinessName')             AS business_name,
       staging.to_int(r.payload->>'BusinessTypeID')                  AS business_type_id,
       staging.blank_to_null(r.payload->>'AddressLine1')             AS address_line_1,      -- 3-3
       staging.blank_to_null(r.payload->>'AddressLine2')             AS address_line_2,
       staging.blank_to_null(r.payload->>'AddressLine3')             AS address_line_3,
       staging.blank_to_null(r.payload->>'AddressLine4')             AS address_line_4,
       staging.normalise_postcode(r.payload->>'PostCode')            AS postcode,            -- 3-7
       staging.postcode_sector(r.payload->>'PostCode')               AS postcode_sector,
       (staging.blank_to_null(r.payload->>'PostCode') IS NOT NULL
        AND NOT staging.is_full_postcode(r.payload->>'PostCode'))    AS postcode_is_partial,
       staging.blank_to_null(r.payload->'geocode'->>'latitude')      AS lat_raw,             -- 3-5
       staging.blank_to_null(r.payload->'geocode'->>'longitude')     AS lon_raw,
       CASE WHEN btrim(r.payload->'geocode'->>'latitude') ~ '^-?\d+(\.\d+)?$'
            THEN round(btrim(r.payload->'geocode'->>'latitude')::NUMERIC, 6) END  AS latitude,
       CASE WHEN btrim(r.payload->'geocode'->>'longitude') ~ '^-?\d+(\.\d+)?$'
            THEN round(btrim(r.payload->'geocode'->>'longitude')::NUMERIC, 6) END AS longitude,
       staging.blank_to_null(r.payload->>'LocalAuthorityCode')       AS la_code,             -- 3-6 (VARCHAR)
       staging.blank_to_null(r.payload->>'RatingValue')              AS rating_value,
       staging.blank_to_null(r.payload->>'RatingKey')                AS rating_key,
       staging.blank_to_null(r.payload->>'RatingDate')               AS rating_date_raw,
       staging.parse_fsa_date(r.payload->>'RatingDate')              AS rating_date,         -- 3-4 (1901 -> NULL)
       staging.blank_to_null(r.payload->>'SchemeType')               AS scheme_type,
       CASE r.payload->>'NewRatingPending' WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE END AS new_rating_pending,
       CASE WHEN r.fetch_date ~ '^\d{4}-\d{2}-\d{2}' THEN left(r.fetch_date, 10)::DATE END AS seen_date,
       CASE WHEN r.published_date ~ '^\d{4}-\d{2}-\d{2}T[\d:.]+$' THEN r.published_date::TIMESTAMP END AS published_date,
       count(*) OVER (PARTITION BY staging.to_int(r.payload->>'FHRSID')) AS fhrs_copies
FROM raw.fsa_establishments r
WHERE r.run_id = current_setting('staging.run_id');
CREATE INDEX ON est_src (source_key);
ANALYZE est_src;

-- 3-8, 3-11 row-level checks ----------------------------------------------------------------------
CREATE TEMP TABLE est_problem AS
SELECT source_key, payload, reasons FROM (
    SELECT s.source_key, s.payload, array_remove(ARRAY[
        CASE WHEN s.fhrs_id IS NULL THEN 'fhrs_id_invalid' END,
        CASE WHEN s.fhrs_id IS NOT NULL AND s.fhrs_copies > 1 THEN 'fhrs_id_duplicate' END,
        CASE WHEN s.business_name IS NULL THEN 'business_name_missing' END,
        CASE WHEN bt.business_type_id IS NULL THEN 'business_type_unknown' END,
        CASE WHEN la.la_code IS NULL THEN 'la_code_unknown' END,
        CASE WHEN la.la_code IS NOT NULL AND la.country <> 'England' THEN 'not_england' END,
        -- the rating values of the FSA /Ratings list (its rating_key_name: 0-5, AwaitingInspection, ...)
        CASE WHEN rt.rating_key_name IS NULL THEN 'rating_value_unknown' END,
        CASE WHEN s.rating_key IS NULL THEN 'rating_key_missing' END,
        CASE WHEN s.scheme_type IS NULL THEN 'scheme_type_missing' END,
        CASE WHEN s.new_rating_pending IS NULL THEN 'new_rating_pending_invalid' END,
        CASE WHEN s.rating_date_raw IS NOT NULL AND left(s.rating_date_raw, 4) <> '1901' AND s.rating_date IS NULL
             THEN 'rating_date_invalid' END,
        -- a numeric rating comes from an inspection, so it has a date
        CASE WHEN s.rating_value ~ '^[0-5]$' AND s.rating_date IS NULL THEN 'rating_date_missing' END,
        CASE WHEN (s.lat_raw IS NULL) <> (s.lon_raw IS NULL)
               OR (s.lat_raw IS NOT NULL AND (s.latitude IS NULL OR s.latitude NOT BETWEEN 49 AND 61))
               OR (s.lon_raw IS NOT NULL AND (s.longitude IS NULL OR s.longitude NOT BETWEEN -9 AND 2.5))
             THEN 'coordinates_invalid' END,
        CASE WHEN length(s.la_business_id) > 50 OR length(s.business_name) > 255
               OR length(s.address_line_1) > 255 OR length(s.address_line_2) > 255
               OR length(s.address_line_3) > 255 OR length(s.address_line_4) > 255
               OR length(s.postcode) > 10 OR length(s.la_code) > 10 OR length(s.rating_value) > 30
               OR length(s.rating_key) > 50 OR length(s.scheme_type) > 4 THEN 'value_too_long' END
    ], NULL) AS reasons
    FROM est_src s
    LEFT JOIN staging.stg_business_type bt ON bt.business_type_id = s.business_type_id
    LEFT JOIN staging.stg_local_authority la ON la.la_code = s.la_code
    LEFT JOIN staging.stg_fsa_rating rt ON rt.rating_key_name = s.rating_value
) x
WHERE cardinality(reasons) > 0;
CREATE INDEX ON est_problem (source_key);
ANALYZE est_problem;

-- checks ------------------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'establishments';

WITH stats AS (
    SELECT (SELECT count(*) FROM est_src)                        AS source_rows,
           (SELECT count(*) FROM est_problem)                    AS quarantined,
           (SELECT bool_or(sample) FROM est_src)                 AS sample,
           (SELECT d.observed_value FROM staging.stg_dq_result d
            WHERE d.stage = 'establishments' AND d.check_name = 'source_rows' AND d.sample = FALSE
              AND d.run_id < current_setting('staging.run_id')
              AND NOT EXISTS (SELECT 1 FROM staging.stg_dq_result f
                              WHERE f.run_id = d.run_id AND f.stage = 'establishments'
                                AND f.severity = 'run' AND f.failed_count > 0)
            ORDER BY d.run_id DESC LIMIT 1)                      AS previous_rows,
           -- step 1 staged this run and passed
           EXISTS (SELECT 1 FROM staging.stg_dq_result d
                   WHERE d.run_id = current_setting('staging.run_id') AND d.stage = 'reference')
           AND NOT EXISTS (SELECT 1 FROM staging.stg_dq_result d
                           WHERE d.run_id = current_setting('staging.run_id') AND d.stage = 'reference'
                             AND d.severity = 'run' AND d.failed_count > 0) AS reference_ok
), reasons(reason) AS (
    VALUES ('fhrs_id_invalid'), ('fhrs_id_duplicate'), ('business_name_missing'), ('business_type_unknown'),
           ('la_code_unknown'), ('not_england'), ('rating_value_unknown'), ('rating_key_missing'),
           ('scheme_type_missing'), ('new_rating_pending_invalid'), ('rating_date_invalid'),
           ('rating_date_missing'), ('coordinates_invalid'), ('value_too_long')
)
INSERT INTO staging.stg_dq_result (run_id, stage, check_name, severity, failed_count, observed_value, sample)
-- run-level: step 1 must have been staged for this run
SELECT current_setting('staging.run_id'), 'establishments', 'reference_staged', 'run',
       CASE WHEN reference_ok THEN 0 ELSE 1 END, NULL, coalesce(sample, FALSE)
FROM stats
UNION ALL
-- run-level: empty source, or fewer than 90 % of the previous full run's rows (full runs only)
SELECT current_setting('staging.run_id'), 'establishments', 'source_rows', 'run',
       CASE WHEN source_rows = 0 THEN 1
            WHEN sample = FALSE AND previous_rows IS NOT NULL AND source_rows < 0.9 * previous_rows THEN 1
            ELSE 0 END,
       source_rows, coalesce(sample, FALSE)
FROM stats
UNION ALL
SELECT current_setting('staging.run_id'), 'establishments', 'fetch_date_invalid', 'run',
       (SELECT count(*) FROM est_src WHERE seen_date IS NULL), NULL, coalesce(sample, FALSE)
FROM stats
UNION ALL
-- run-level: more than 5 % of the rows quarantined
SELECT current_setting('staging.run_id'), 'establishments', 'quarantine_rate', 'run',
       CASE WHEN source_rows > 0 AND quarantined > 0.05 * source_rows THEN 1 ELSE 0 END,
       quarantined, coalesce(sample, FALSE)
FROM stats
UNION ALL
-- row-level: rows quarantined per reason
SELECT current_setting('staging.run_id'), 'establishments', 'row_' || r.reason, 'row',
       (SELECT count(*) FROM est_problem p WHERE r.reason = ANY (p.reasons)), NULL, coalesce(s.sample, FALSE)
FROM reasons r CROSS JOIN stats s
UNION ALL
-- info for the report (Q3 / Q5 coverage)
SELECT current_setting('staging.run_id'), 'establishments', check_name, 'info', n, NULL, coalesce(sample, FALSE)
FROM stats, LATERAL (VALUES
    ('info_postcode_partial', (SELECT count(*) FROM est_src WHERE postcode_is_partial)),
    ('info_postcode_missing', (SELECT count(*) FROM est_src WHERE postcode IS NULL)),
    ('info_coordinates_missing', (SELECT count(*) FROM est_src WHERE lat_raw IS NULL)),
    -- candidates for step 4 (postcodes.io): no coordinates but a full postcode
    ('info_geocodable', (SELECT count(*) FROM est_src WHERE lat_raw IS NULL AND postcode_sector IS NOT NULL)),
    ('info_not_inspected', (SELECT count(*) FROM est_src WHERE rating_date IS NULL))
) v(check_name, n);

-- @@ load ---------------------------------------------------------------------------------------
-- (runs only if no `run` check above failed)

DELETE FROM staging.stg_quarantine WHERE run_id = current_setting('staging.run_id') AND stage = 'establishments';
INSERT INTO staging.stg_quarantine (run_id, stage, source_key, reasons, payload)
SELECT current_setting('staging.run_id'), 'establishments', source_key, reasons, payload FROM est_problem;

-- 3-9 stg_food_establishment: latest state. FSA coordinates win; coordinates added by step 4
-- (postcodes.io) are kept while FSA still has none and the postcode has not changed.
INSERT INTO staging.stg_food_establishment AS t
    (fhrs_id, la_business_id, business_name, business_type_id, address_line_1, address_line_2,
     address_line_3, address_line_4, postcode, postcode_sector, postcode_is_partial, latitude, longitude,
     coord_source, la_code, published_date, run_id, last_seen_run_id, last_seen_date, loaded_at)
SELECT s.fhrs_id, s.la_business_id, s.business_name, s.business_type_id, s.address_line_1, s.address_line_2,
       s.address_line_3, s.address_line_4, s.postcode, s.postcode_sector, s.postcode_is_partial,
       s.latitude, s.longitude, CASE WHEN s.latitude IS NOT NULL THEN 'fsa' END,
       s.la_code, s.published_date, current_setting('staging.run_id'), current_setting('staging.run_id'),
       s.seen_date, now()
FROM est_src s
WHERE NOT EXISTS (SELECT 1 FROM est_problem p WHERE p.source_key = s.source_key)
ON CONFLICT (fhrs_id) DO UPDATE SET
    la_business_id = EXCLUDED.la_business_id, business_name = EXCLUDED.business_name,
    business_type_id = EXCLUDED.business_type_id, address_line_1 = EXCLUDED.address_line_1,
    address_line_2 = EXCLUDED.address_line_2, address_line_3 = EXCLUDED.address_line_3,
    address_line_4 = EXCLUDED.address_line_4, postcode = EXCLUDED.postcode,
    postcode_sector = EXCLUDED.postcode_sector, postcode_is_partial = EXCLUDED.postcode_is_partial,
    latitude = CASE WHEN EXCLUDED.latitude IS NOT NULL THEN EXCLUDED.latitude
                    WHEN t.coord_source = 'postcodes_io' AND t.postcode IS NOT DISTINCT FROM EXCLUDED.postcode
                    THEN t.latitude END,
    longitude = CASE WHEN EXCLUDED.longitude IS NOT NULL THEN EXCLUDED.longitude
                     WHEN t.coord_source = 'postcodes_io' AND t.postcode IS NOT DISTINCT FROM EXCLUDED.postcode
                     THEN t.longitude END,
    coord_source = CASE WHEN EXCLUDED.latitude IS NOT NULL THEN 'fsa'
                        WHEN t.coord_source = 'postcodes_io' AND t.postcode IS NOT DISTINCT FROM EXCLUDED.postcode
                        THEN 'postcodes_io' END,
    la_code = EXCLUDED.la_code, published_date = EXCLUDED.published_date,
    run_id = EXCLUDED.run_id, last_seen_run_id = EXCLUDED.last_seen_run_id,
    last_seen_date = EXCLUDED.last_seen_date, loaded_at = EXCLUDED.loaded_at
-- an older run staged again by hand never overwrites a newer one
WHERE EXCLUDED.last_seen_date >= t.last_seen_date;

-- businesses quarantined in this run still appeared in the source: they are not closed
UPDATE staging.stg_food_establishment t
SET last_seen_run_id = current_setting('staging.run_id'), last_seen_date = s.seen_date
FROM est_src s
JOIN est_problem p ON p.source_key = s.source_key
WHERE s.fhrs_id = t.fhrs_id AND s.fhrs_copies = 1 AND s.seen_date >= t.last_seen_date;

-- 3-10 stg_fsa_inspection: one row per (business, inspection date), accumulated over runs.
-- A rating changed on appeal (same date) is updated in place; a not-inspected business has one row
-- with rating_date NULL (NULLS NOT DISTINCT), kept as history once it is inspected.
INSERT INTO staging.stg_fsa_inspection AS t
    (fhrs_id, rating_date, rating_value, rating_key, scheme_type, new_rating_pending,
     first_fetch_date, last_fetch_date, published_date, run_id, loaded_at)
SELECT s.fhrs_id, s.rating_date, s.rating_value, s.rating_key, s.scheme_type, s.new_rating_pending,
       s.seen_date, s.seen_date, s.published_date, current_setting('staging.run_id'), now()
FROM est_src s
WHERE NOT EXISTS (SELECT 1 FROM est_problem p WHERE p.source_key = s.source_key)
ON CONFLICT ON CONSTRAINT stg_fsa_inspection_event DO UPDATE SET
    rating_value = EXCLUDED.rating_value, rating_key = EXCLUDED.rating_key,
    scheme_type = EXCLUDED.scheme_type, new_rating_pending = EXCLUDED.new_rating_pending,
    first_fetch_date = LEAST(t.first_fetch_date, EXCLUDED.first_fetch_date),
    last_fetch_date = EXCLUDED.last_fetch_date, published_date = EXCLUDED.published_date,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at
WHERE EXCLUDED.last_fetch_date >= t.last_fetch_date;

-- an older run staged after a newer one can still move first_fetch_date back
UPDATE staging.stg_fsa_inspection t
SET first_fetch_date = s.seen_date
FROM est_src s
WHERE s.fhrs_id = t.fhrs_id AND s.rating_date IS NOT DISTINCT FROM t.rating_date
  AND s.seen_date < t.first_fetch_date
  AND NOT EXISTS (SELECT 1 FROM est_problem p WHERE p.source_key = s.source_key);
