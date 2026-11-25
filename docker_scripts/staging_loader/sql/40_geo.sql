-- Step 4B: geocoding from one postcode run (TODO 4-7 .. 4-13).
-- Run by `stage --step geo --run-id <postcode run>`, with the run in current_setting('staging.run_id').
-- The run's answers are merged into stg_postcode (latest answer per postcode, any run), then EVERY
-- school and business is matched again from stg_postcode: a run that found everything in the cache
-- (no document) still refreshes the matching, e.g. for the schools of a new Ofsted run.
-- Same two-transaction rule as the other steps (split at `-- @@ load`).
--
-- Schools: la_code comes from the district of their postcode, through the seed stg_district_la_map.
--   live postcode        match_method 'postcode'
--   terminated postcode  coordinates of the terminated postcode; district of the nearest live postcode
--                        (reverse geocoding, up to 2 km), accepted only if that district belongs to the
--                        school's Ofsted local authority (the district itself for a unitary authority,
--                        or its admin_county), match_method 'reverse_geocode'; otherwise no la_code
-- Businesses: la_code comes from the FSA; only missing coordinates are filled (coord_source 'postcodes_io').

-- answers of this run, one row per postcode ---------------------------------------------------------
DROP TABLE IF EXISTS pc_new, pc_all, school_geo, pc_problem;
CREATE TEMP TABLE pc_new AS
SELECT b.source_key AS postcode,
       CASE WHEN jsonb_typeof(b.payload->'result') = 'object' THEN 'live'
            WHEN (t.payload->'response'->>'status') = '200' THEN 'terminated'
            ELSE 'not_found' END AS status,
       coalesce(b.payload->'result'->>'latitude', t.payload->'response'->'result'->>'latitude')::NUMERIC(9,6) AS latitude,
       coalesce(b.payload->'result'->>'longitude', t.payload->'response'->'result'->>'longitude')::NUMERIC(9,6) AS longitude,
       g.res->'codes'->>'admin_district'                                      AS admin_district_code,
       g.res->>'admin_district'                                               AS admin_district_name,
       CASE WHEN g.res->>'admin_county' IS NOT NULL THEN g.res->'codes'->>'admin_county' END AS admin_county_code,
       g.res->>'admin_county'                                                 AS admin_county_name,
       m.region_name                                                          AS region,
       g.res->>'region'                                                       AS region_raw,
       CASE WHEN g.is_reverse THEN g.res->>'postcode' END                     AS reverse_postcode,
       CASE WHEN g.is_reverse THEN round((g.res->>'distance')::NUMERIC, 1) END AS reverse_distance_m,
       b.fetch_date::TIMESTAMPTZ                                              AS fetched_at,
       jsonb_build_object('postcode', b.payload, 'terminated', t.payload, 'reverse', rv.payload) AS payload
FROM raw.postcodes b
LEFT JOIN raw.terminated_postcodes t ON t.run_id = b.run_id AND t.source_key = b.source_key
LEFT JOIN raw.reverse_postcodes rv ON rv.run_id = b.run_id AND rv.source_key = b.source_key
CROSS JOIN LATERAL (
    SELECT CASE WHEN jsonb_typeof(b.payload->'result') = 'object' THEN b.payload->'result'
                ELSE rv.payload->'response'->'result'->0 END AS res,       -- nearest live postcode
           jsonb_typeof(b.payload->'result') <> 'object' AS is_reverse
) g
LEFT JOIN staging.stg_region_map m ON m.source = 'postcodes_io' AND m.source_name = g.res->>'region'
WHERE b.run_id = current_setting('staging.run_id');
CREATE INDEX ON pc_new (postcode);
ANALYZE pc_new;

-- stg_postcode as it will be after this run (an older run never replaces a newer answer)
CREATE TEMP TABLE pc_all AS
SELECT DISTINCT ON (postcode) postcode, status, latitude, longitude, admin_district_code, admin_district_name,
       admin_county_code, admin_county_name, region, reverse_postcode, reverse_distance_m, fetched_at
FROM (SELECT postcode, status, latitude, longitude, admin_district_code, admin_district_name, admin_county_code,
             admin_county_name, region, reverse_postcode, reverse_distance_m, fetched_at, 1 AS pri FROM pc_new
      UNION ALL
      SELECT postcode, status, latitude, longitude, admin_district_code, admin_district_name, admin_county_code,
             admin_county_name, region, reverse_postcode, reverse_distance_m, fetched_at, 2 FROM staging.stg_postcode) u
ORDER BY postcode, fetched_at DESC, pri;
CREATE INDEX ON pc_all (postcode);
ANALYZE pc_all;

-- 4-8, 4-9 matching of every school with a postcode ------------------------------------------------
CREATE TEMP TABLE school_geo AS
SELECT s.urn, p.status, p.latitude, p.longitude, p.reverse_distance_m,
       x.accepted, x.match_method,
       CASE WHEN x.accepted THEN p.admin_district_code END AS admin_district_code,
       CASE WHEN x.accepted THEN p.admin_district_name END AS admin_district_name,
       CASE WHEN x.accepted THEN p.admin_county_code END   AS admin_county_code,
       CASE WHEN x.accepted THEN p.admin_county_name END   AS admin_county_name,
       CASE WHEN x.accepted THEN dm.la_code END            AS la_code,
       p.admin_district_code AS candidate_district_code,
       -- the district belongs to the Ofsted local authority (unitary = itself, two-tier = its county)
       (staging.name_key(p.admin_district_name) = staging.name_key(s.local_authority_name)
        OR staging.name_key(p.admin_county_name) = staging.name_key(s.local_authority_name)) AS in_ofsted_la
FROM staging.stg_school s
JOIN pc_all p ON p.postcode = s.postcode
CROSS JOIN LATERAL (
    -- coalesce: a NULL county (unitary area) must give FALSE, not NULL
    SELECT coalesce(p.status = 'live'
            OR (p.status = 'terminated' AND p.admin_district_code IS NOT NULL
                AND (staging.name_key(p.admin_district_name) = staging.name_key(s.local_authority_name)
                     OR staging.name_key(p.admin_county_name) = staging.name_key(s.local_authority_name))), FALSE) AS accepted,
           CASE WHEN p.status = 'live' THEN 'postcode'
                WHEN p.status = 'terminated' AND p.admin_district_code IS NOT NULL
                 AND (staging.name_key(p.admin_district_name) = staging.name_key(s.local_authority_name)
                      OR staging.name_key(p.admin_county_name) = staging.name_key(s.local_authority_name))
                THEN 'reverse_geocode' END AS match_method
) x
LEFT JOIN (SELECT gss_code, min(la_code) AS la_code FROM staging.stg_district_la_map GROUP BY gss_code) dm
       ON dm.gss_code = p.admin_district_code
      AND EXISTS (SELECT 1 FROM staging.stg_local_authority la WHERE la.la_code = dm.la_code);
CREATE INDEX ON school_geo (urn);
ANALYZE school_geo;

-- 4-12 postcodes found nowhere (bulk and terminated) ------------------------------------------------
CREATE TEMP TABLE pc_problem AS
SELECT postcode AS source_key, payload, ARRAY['postcode_not_found'] AS reasons FROM pc_new WHERE status = 'not_found';

-- checks ------------------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'geo';

WITH stats AS (
    SELECT (SELECT bool_or(sample) FROM raw.postcodes WHERE run_id = current_setting('staging.run_id')) AS sample,
           (SELECT count(*) FROM staging.stg_school WHERE postcode IS NOT NULL) AS schools_with_postcode,
           (SELECT count(*) FROM school_geo WHERE la_code IS NOT NULL) AS schools_matched
)
INSERT INTO staging.stg_dq_result (run_id, stage, check_name, severity, failed_count, observed_value, sample)
SELECT current_setting('staging.run_id'), 'geo', check_name, severity, n::int, observed, coalesce(sample, FALSE)
FROM stats, LATERAL (VALUES
    -- run-level: a district used by an accepted school match is missing from the seed (add it to
    -- seeds/district_la_map.csv); observed = schools affected
    ('district_unmapped', 'run',
     (SELECT count(DISTINCT candidate_district_code) FROM school_geo WHERE accepted AND la_code IS NULL),
     (SELECT count(*) FROM school_geo WHERE accepted AND la_code IS NULL)),
    -- run-level: more than 5 % of the schools with a postcode end up without la_code
    ('school_unmatched_rate', 'run',
     CASE WHEN schools_with_postcode > 0 AND schools_with_postcode - schools_matched > 0.05 * schools_with_postcode
          THEN 1 ELSE 0 END,
     schools_with_postcode - schools_matched),
    -- row-level: postcodes of this run found nowhere -> stg_quarantine
    ('row_postcode_not_found', 'row', (SELECT count(*) FROM pc_problem), NULL),
    -- info for the report
    ('info_schools_match_postcode', 'info', (SELECT count(*) FROM school_geo WHERE match_method = 'postcode'), NULL),
    ('info_schools_match_reverse_geocode', 'info', (SELECT count(*) FROM school_geo WHERE match_method = 'reverse_geocode'), NULL),
    -- terminated postcode whose nearest live postcode is in another local authority (or none within 2 km)
    ('info_schools_reverse_rejected', 'info', (SELECT count(*) FROM school_geo WHERE status = 'terminated' AND NOT accepted), NULL),
    ('info_schools_postcode_not_found', 'info', (SELECT count(*) FROM school_geo WHERE status = 'not_found'), NULL),
    ('info_schools_not_looked_up', 'info',
     (SELECT count(*) FROM staging.stg_school s WHERE s.postcode IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM pc_all p WHERE p.postcode = s.postcode)), NULL),
    -- sanity of the Ofsted-LA rule on live postcodes (they are matched anyway): should stay near 0
    ('info_schools_live_outside_ofsted_la', 'info',
     (SELECT count(*) FROM school_geo WHERE status = 'live' AND NOT coalesce(in_ofsted_la, FALSE)), NULL),
    ('info_schools_no_county', 'info', (SELECT count(*) FROM school_geo WHERE la_code IS NOT NULL AND admin_county_code IS NULL), NULL),
    ('info_postcode_region_unmapped', 'info', (SELECT count(*) FROM pc_new WHERE region_raw IS NOT NULL AND region IS NULL), NULL),
    ('info_venues_coords_from_postcodes_io', 'info',
     (SELECT count(*) FROM staging.stg_food_establishment e JOIN pc_all p ON p.postcode = e.postcode
      WHERE (e.coord_source IS NULL OR e.coord_source = 'postcodes_io') AND p.latitude IS NOT NULL), NULL),
    ('info_venues_without_coords', 'info',
     (SELECT count(*) FROM staging.stg_food_establishment e
      WHERE e.coord_source IS NULL
        AND NOT EXISTS (SELECT 1 FROM pc_all p WHERE p.postcode = e.postcode AND p.latitude IS NOT NULL)), NULL)
) v(check_name, severity, n, observed);

-- @@ load ---------------------------------------------------------------------------------------
-- (runs only if no `run` check above failed)

DELETE FROM staging.stg_quarantine WHERE run_id = current_setting('staging.run_id') AND stage = 'geo';
INSERT INTO staging.stg_quarantine (run_id, stage, source_key, reasons, payload)
SELECT current_setting('staging.run_id'), 'geo', source_key, reasons, payload FROM pc_problem;

-- 4-7 merge this run's answers into stg_postcode
INSERT INTO staging.stg_postcode AS t
    (postcode, status, latitude, longitude, admin_district_code, admin_district_name, admin_county_code,
     admin_county_name, region, reverse_postcode, reverse_distance_m, fetched_at, run_id, loaded_at)
SELECT postcode, status, latitude, longitude, admin_district_code, admin_district_name, admin_county_code,
       admin_county_name, region, reverse_postcode, reverse_distance_m, fetched_at,
       current_setting('staging.run_id'), now()
FROM pc_new
ON CONFLICT (postcode) DO UPDATE SET
    status = EXCLUDED.status, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
    admin_district_code = EXCLUDED.admin_district_code, admin_district_name = EXCLUDED.admin_district_name,
    admin_county_code = EXCLUDED.admin_county_code, admin_county_name = EXCLUDED.admin_county_name,
    region = EXCLUDED.region, reverse_postcode = EXCLUDED.reverse_postcode,
    reverse_distance_m = EXCLUDED.reverse_distance_m, fetched_at = EXCLUDED.fetched_at,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at
WHERE EXCLUDED.fetched_at >= t.fetched_at;

-- 4-8, 4-9 every school: coordinates, district, county, la_code (NULL when nothing is known)
UPDATE staging.stg_school s
SET latitude = g.latitude, longitude = g.longitude,
    admin_district_code = g.admin_district_code, admin_district_name = g.admin_district_name,
    admin_county_code = g.admin_county_code, admin_county_name = g.admin_county_name,
    la_code = g.la_code, match_method = CASE WHEN g.la_code IS NOT NULL THEN g.match_method END,
    reverse_distance_m = g.reverse_distance_m
FROM (SELECT s2.urn, sg.latitude, sg.longitude, sg.admin_district_code, sg.admin_district_name,
             sg.admin_county_code, sg.admin_county_name, sg.la_code, sg.match_method, sg.reverse_distance_m
      FROM staging.stg_school s2 LEFT JOIN school_geo sg ON sg.urn = s2.urn) g
WHERE g.urn = s.urn
  AND (s.latitude, s.longitude, s.admin_district_code, s.admin_county_code, s.la_code, s.match_method, s.reverse_distance_m)
      IS DISTINCT FROM (g.latitude, g.longitude, g.admin_district_code, g.admin_county_code, g.la_code,
                        CASE WHEN g.la_code IS NOT NULL THEN g.match_method END, g.reverse_distance_m);

-- 4-11 businesses without FSA coordinates: coordinates of their postcode (live or terminated)
UPDATE staging.stg_food_establishment e
SET latitude = p.latitude, longitude = p.longitude, coord_source = 'postcodes_io'
FROM pc_all p
WHERE p.postcode = e.postcode AND p.latitude IS NOT NULL
  AND (e.coord_source IS NULL OR e.coord_source = 'postcodes_io')
  AND (e.latitude, e.longitude) IS DISTINCT FROM (p.latitude, p.longitude);
