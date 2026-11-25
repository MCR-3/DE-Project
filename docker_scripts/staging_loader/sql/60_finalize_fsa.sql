-- Step 6 (FSA): reconcile one FSA run, then record it in stg_load_log and delete its raw.* rows
-- (TODO 6-1 .. 6-5). Run by `stage --step finalize_fsa`, after steps 1, 3 and 5 (fsa_measures).
-- Same two-transaction rule as the other steps (split at `-- @@ load`).
--
-- Every source record is classified for every table it feeds:
--   loaded       written by this run
--   quarantined  left out by a row-level check of this run (stg_quarantine)
--   newer        the table already holds a newer run's version (an older run staged again)
--   excluded     left out on purpose (the business type -1 "All", an API filter value)
--   missing      none of the above: the run is not finalized
-- The raw rows are deleted only once every record is explained; MongoDB keeps the landed documents.

DROP TABLE IF EXISTS fin_rows;
CREATE TEMP TABLE fin_rows AS
WITH est AS (
    SELECT r.source_key, staging.to_int(r.payload->>'FHRSID') AS fhrs_id,
           staging.parse_fsa_date(r.payload->>'RatingDate') AS rating_date, left(r.fetch_date, 10)::DATE AS seen_date,
           EXISTS (SELECT 1 FROM staging.stg_quarantine q WHERE q.run_id = r.run_id AND q.stage = 'establishments'
                   AND q.source_key = r.source_key) AS q_est,
           EXISTS (SELECT 1 FROM staging.stg_quarantine q WHERE q.run_id = r.run_id AND q.stage = 'fsa_measures'
                   AND q.source_key = r.source_key) AS q_measure
    FROM raw.fsa_establishments r
    WHERE r.run_id = current_setting('staging.run_id')
)
SELECT 'stg_food_establishment' AS target, e.source_key,
       CASE WHEN e.q_est THEN 'quarantined'
            WHEN t.run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN t.last_seen_date > e.seen_date THEN 'newer'
            ELSE 'missing' END AS outcome
FROM est e LEFT JOIN staging.stg_food_establishment t ON t.fhrs_id = e.fhrs_id
UNION ALL
SELECT 'stg_fsa_inspection', e.source_key,
       CASE WHEN e.q_est THEN 'quarantined'
            WHEN i.last_seen_run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN i.last_fetch_date > e.seen_date THEN 'newer'
            ELSE 'missing' END
FROM est e
LEFT JOIN staging.stg_fsa_inspection i ON i.fhrs_id = e.fhrs_id AND i.rating_date IS NOT DISTINCT FROM e.rating_date
UNION ALL
-- every rating has its overall measure row
SELECT 'stg_inspection_measure', e.source_key,
       CASE WHEN e.q_est OR e.q_measure THEN 'quarantined'
            WHEN m.run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN i.last_fetch_date > e.seen_date THEN 'newer'
            ELSE 'missing' END
FROM est e
LEFT JOIN staging.stg_fsa_inspection i ON i.fhrs_id = e.fhrs_id AND i.rating_date IS NOT DISTINCT FROM e.rating_date
LEFT JOIN staging.stg_inspection_measure m ON m.fsa_inspection_id = i.fsa_inspection_id AND m.measure_code = 'FHRS_OVERALL'
UNION ALL
-- reference tables (step 1 overwrites them, so another run's version means a newer run)
SELECT 'stg_local_authority', r.source_key,
       CASE WHEN t.run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN t.la_code IS NOT NULL THEN 'newer' ELSE 'missing' END
FROM raw.fsa_authorities r
LEFT JOIN staging.stg_local_authority t ON t.la_code = btrim(r.payload->>'LocalAuthorityIdCode')
WHERE r.run_id = current_setting('staging.run_id')
UNION ALL
SELECT 'stg_business_type', r.source_key,
       CASE WHEN (r.payload->>'BusinessTypeId') = '-1' THEN 'excluded'
            WHEN t.run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN t.business_type_id IS NOT NULL THEN 'newer' ELSE 'missing' END
FROM raw.fsa_business_types r
LEFT JOIN staging.stg_business_type t ON t.business_type_id = staging.to_int(r.payload->>'BusinessTypeId')
WHERE r.run_id = current_setting('staging.run_id')
UNION ALL
SELECT 'stg_fsa_rating', r.source_key,
       CASE WHEN t.run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN t.rating_id IS NOT NULL THEN 'newer' ELSE 'missing' END
FROM raw.fsa_ratings r
LEFT JOIN staging.stg_fsa_rating t ON t.rating_id = staging.to_int(r.payload->>'ratingId')
WHERE r.run_id = current_setting('staging.run_id')
UNION ALL
SELECT 'stg_fsa_score_descriptor', r.source_key,
       CASE WHEN t.run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN t.descriptor_id IS NOT NULL THEN 'newer' ELSE 'missing' END
FROM raw.fsa_score_descriptors r
LEFT JOIN staging.stg_fsa_score_descriptor t ON t.descriptor_id = staging.to_int(r.payload->>'Id')
WHERE r.run_id = current_setting('staging.run_id');

-- already finalized and its raw rows deleted: nothing to do (keep the earlier results)
DROP TABLE IF EXISTS fin_skip;
CREATE TEMP TABLE fin_skip AS
SELECT EXISTS (SELECT 1 FROM staging.stg_load_log WHERE source = 'fsa' AND run_id = current_setting('staging.run_id'))
       AND NOT EXISTS (SELECT 1 FROM fin_rows) AS skip;

-- checks ------------------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'finalize_fsa'
  AND NOT (SELECT skip FROM fin_skip);

WITH s AS (SELECT coalesce((SELECT bool_or(sample) FROM raw.fsa_establishments
                            WHERE run_id = current_setting('staging.run_id')), FALSE) AS sample)
INSERT INTO staging.stg_dq_result (run_id, stage, check_name, severity, failed_count, observed_value, sample)
SELECT current_setting('staging.run_id'), 'finalize_fsa', c.check_name, c.severity, c.n::int, c.observed, s.sample
FROM s CROSS JOIN LATERAL (
    -- run-level: steps 1, 3 and 5 ran for this run and passed (observed = how many did not)
    SELECT 'steps_passed', 'run',
           CASE WHEN staging.stage_passed(current_setting('staging.run_id'), 'reference')
                 AND staging.stage_passed(current_setting('staging.run_id'), 'establishments')
                 AND staging.stage_passed(current_setting('staging.run_id'), 'fsa_measures') THEN 0 ELSE 1 END,
           (NOT staging.stage_passed(current_setting('staging.run_id'), 'reference'))::INT
           + (NOT staging.stage_passed(current_setting('staging.run_id'), 'establishments'))::INT
           + (NOT staging.stage_passed(current_setting('staging.run_id'), 'fsa_measures'))::INT
    UNION ALL SELECT 'source_present', 'run',
           CASE WHEN EXISTS (SELECT 1 FROM fin_rows WHERE target = 'stg_food_establishment') THEN 0 ELSE 1 END, NULL
    -- run-level: every source record is explained
    UNION ALL SELECT 'unexplained_rows', 'run', (SELECT count(*) FROM fin_rows WHERE outcome = 'missing'), NULL
    -- info: target x outcome (the reconciliation table of the report)
    UNION ALL SELECT 'count_' || target || '_' || outcome, 'info', count(*), NULL FROM fin_rows GROUP BY target, outcome
) c(check_name, severity, n, observed)
WHERE NOT (SELECT skip FROM fin_skip);

-- @@ load ---------------------------------------------------------------------------------------
-- (runs only if no `run` check above failed)

INSERT INTO staging.stg_load_log AS t (source, run_id, sample, loaded_at, row_counts)
SELECT 'fsa', current_setting('staging.run_id'),
       coalesce((SELECT bool_or(sample) FROM raw.fsa_establishments WHERE run_id = current_setting('staging.run_id')), FALSE),
       now(), (SELECT jsonb_object_agg(target, outcomes) FROM (
                   SELECT target, jsonb_object_agg(outcome, n) AS outcomes
                   FROM (SELECT target, outcome, count(*) AS n FROM fin_rows GROUP BY 1, 2) x GROUP BY target) y)
WHERE NOT (SELECT skip FROM fin_skip)
ON CONFLICT (source, run_id) DO UPDATE SET
    sample = EXCLUDED.sample, loaded_at = EXCLUDED.loaded_at, row_counts = EXCLUDED.row_counts;

-- 6-5 the run is staged and checked: its raw copy is no longer needed (MongoDB keeps the documents)
DELETE FROM raw.fsa_establishments    WHERE run_id = current_setting('staging.run_id');
DELETE FROM raw.fsa_authorities       WHERE run_id = current_setting('staging.run_id');
DELETE FROM raw.fsa_business_types    WHERE run_id = current_setting('staging.run_id');
DELETE FROM raw.fsa_ratings           WHERE run_id = current_setting('staging.run_id');
DELETE FROM raw.fsa_score_descriptors WHERE run_id = current_setting('staging.run_id');
