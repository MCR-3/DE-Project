-- Step 1: reference tables from one FSA run (TODO 1-1 .. 1-8).
-- Run by `stage --step reference` in one transaction, with the run in current_setting('staging.run_id').
-- The DQ checks come first; if one fails, the last block raises and the whole step is rolled back.

-- 1-8 DQ checks -----------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'reference';

INSERT INTO staging.stg_dq_result (run_id, stage, check_name, failed_count)
WITH a AS (
    SELECT payload FROM raw.fsa_authorities WHERE run_id = current_setting('staging.run_id')
), b AS (
    SELECT payload FROM raw.fsa_business_types WHERE run_id = current_setting('staging.run_id')
)
SELECT current_setting('staging.run_id'), 'reference', check_name, failed_count FROM (
    -- the run has authorities / business types / ratings / score descriptors at all
    SELECT 'authorities_present' AS check_name, (SELECT count(*) FROM a) = 0 AS f0, NULL::bigint AS n
    UNION ALL SELECT 'business_types_present', (SELECT count(*) FROM b) = 0, NULL
    UNION ALL SELECT 'ratings_present',
        NOT EXISTS (SELECT 1 FROM raw.fsa_ratings WHERE run_id = current_setting('staging.run_id')), NULL
    UNION ALL SELECT 'score_descriptors_present',
        NOT EXISTS (SELECT 1 FROM raw.fsa_score_descriptors WHERE run_id = current_setting('staging.run_id')), NULL
    -- la_code (LocalAuthorityIdCode) not empty, no duplicate
    UNION ALL SELECT 'la_code_missing', NULL,
        (SELECT count(*) FROM a WHERE staging.blank_to_null(payload->>'LocalAuthorityIdCode') IS NULL)
    UNION ALL SELECT 'la_code_duplicate', NULL,
        (SELECT count(*) - count(DISTINCT payload->>'LocalAuthorityIdCode') FROM a
         WHERE staging.blank_to_null(payload->>'LocalAuthorityIdCode') IS NOT NULL)
    UNION ALL SELECT 'la_name_missing', NULL,
        (SELECT count(*) FROM a WHERE staging.blank_to_null(payload->>'Name') IS NULL)
    -- every English RegionName has a row in stg_region_map (Scotland, Wales, Northern Ireland are countries)
    UNION ALL SELECT 'region_unmapped', NULL,
        (SELECT count(DISTINCT payload->>'RegionName') FROM a
         WHERE coalesce(payload->>'RegionName', '') NOT IN ('Scotland', 'Wales', 'Northern Ireland')
           AND NOT EXISTS (SELECT 1 FROM staging.stg_region_map m
                           WHERE m.source = 'fsa' AND m.source_name = a.payload->>'RegionName'))
    UNION ALL SELECT 'business_type_invalid', NULL,
        (SELECT count(*) FROM b WHERE staging.to_int(payload->>'BusinessTypeId') IS NULL
                                   OR staging.blank_to_null(payload->>'BusinessTypeName') IS NULL)
    -- the seeded district map only points to known authorities of this run
    UNION ALL SELECT 'district_map_unknown_la', NULL,
        (SELECT count(*) FROM staging.stg_district_la_map d
         WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.payload->>'LocalAuthorityIdCode' = d.la_code))
) c(check_name, f0, n)
CROSS JOIN LATERAL (SELECT coalesce(n, CASE WHEN f0 THEN 1 ELSE 0 END)::int AS failed_count) x;

DO $$
DECLARE failed TEXT;
BEGIN
    SELECT string_agg(check_name || '=' || failed_count, ', ') INTO failed
    FROM staging.stg_dq_result
    WHERE run_id = current_setting('staging.run_id') AND stage = 'reference' AND failed_count > 0;
    IF failed IS NOT NULL THEN
        RAISE EXCEPTION 'reference DQ failed for run %: %', current_setting('staging.run_id'), failed;
    END IF;
END $$;

-- 1-1 .. 1-5 stg_local_authority ------------------------------------------------------------------
INSERT INTO staging.stg_local_authority
    (la_code, fsa_authority_id, la_name, region, country, website, email, run_id, loaded_at)
SELECT btrim(r.payload->>'LocalAuthorityIdCode'),                          -- 1-2: not LocalAuthorityId
       (r.payload->>'LocalAuthorityId')::INTEGER,
       btrim(r.payload->>'Name'),
       m.region_name,                                                      -- 1-4: Ofsted name, NULL outside England
       CASE WHEN r.payload->>'RegionName' IN ('Scotland', 'Wales', 'Northern Ireland')
            THEN r.payload->>'RegionName' ELSE 'England' END,              -- 1-3
       staging.blank_to_null(r.payload->>'Url'),
       staging.blank_to_null(r.payload->>'Email'),
       r.run_id, now()
FROM raw.fsa_authorities r
LEFT JOIN staging.stg_region_map m ON m.source = 'fsa' AND m.source_name = r.payload->>'RegionName'
WHERE r.run_id = current_setting('staging.run_id')
ON CONFLICT (la_code) DO UPDATE SET
    fsa_authority_id = EXCLUDED.fsa_authority_id, la_name = EXCLUDED.la_name, region = EXCLUDED.region,
    country = EXCLUDED.country, website = EXCLUDED.website, email = EXCLUDED.email,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at;

-- 1-6 stg_business_type (-1 "All" is an API filter value, not a business type) --------------------
INSERT INTO staging.stg_business_type (business_type_id, business_type_name, run_id, loaded_at)
SELECT (payload->>'BusinessTypeId')::INTEGER, btrim(payload->>'BusinessTypeName'), run_id, now()
FROM raw.fsa_business_types
WHERE run_id = current_setting('staging.run_id') AND (payload->>'BusinessTypeId')::INTEGER <> -1
ON CONFLICT (business_type_id) DO UPDATE SET
    business_type_name = EXCLUDED.business_type_name, run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at;

-- 1-7 stg_fsa_rating, stg_fsa_score_descriptor (sources of Dim_Level) -----------------------------
INSERT INTO staging.stg_fsa_rating
    (rating_id, rating_name, rating_key, rating_key_name, scheme_type_id, run_id, loaded_at)
SELECT (payload->>'ratingId')::INTEGER, payload->>'ratingName', payload->>'ratingKey',
       staging.blank_to_null(payload->>'ratingKeyName'), (payload->>'schemeTypeId')::SMALLINT, run_id, now()
FROM raw.fsa_ratings
WHERE run_id = current_setting('staging.run_id')
ON CONFLICT (rating_id) DO UPDATE SET
    rating_name = EXCLUDED.rating_name, rating_key = EXCLUDED.rating_key,
    rating_key_name = EXCLUDED.rating_key_name, scheme_type_id = EXCLUDED.scheme_type_id,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at;

INSERT INTO staging.stg_fsa_score_descriptor
    (descriptor_id, score_category, score, description, run_id, loaded_at)
SELECT (payload->>'Id')::INTEGER, payload->>'ScoreCategory', (payload->>'Score')::SMALLINT,
       payload->>'Description', run_id, now()
FROM raw.fsa_score_descriptors
WHERE run_id = current_setting('staging.run_id')
ON CONFLICT (descriptor_id) DO UPDATE SET
    score_category = EXCLUDED.score_category, score = EXCLUDED.score, description = EXCLUDED.description,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at;
