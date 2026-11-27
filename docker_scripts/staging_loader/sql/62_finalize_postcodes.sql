-- Step 6 (postcodes): reconcile one postcode run, then record it in stg_load_log and delete its raw.*
-- rows (TODO 6-1 .. 6-5). Run by `stage --step finalize_postcodes --run-id <postcode run>`, after step 4
-- (geo). A run whose lookups were all cached has no row: it is finalized with zero counts.
-- Same classification as 60_finalize_fsa.sql: loaded / quarantined / newer / missing.

DROP TABLE IF EXISTS fin_rows;
CREATE TEMP TABLE fin_rows AS
SELECT 'stg_postcode' AS target, r.source_key,
       CASE WHEN EXISTS (SELECT 1 FROM staging.stg_quarantine q WHERE q.run_id = r.run_id AND q.stage = 'geo'
                         AND q.source_key = r.source_key) THEN 'quarantined'
            WHEN p.run_id = r.run_id THEN 'loaded'
            WHEN p.fetched_at > r.fetch_date::TIMESTAMPTZ THEN 'newer'
            ELSE 'missing' END AS outcome
FROM raw.postcodes r
LEFT JOIN staging.stg_postcode p ON p.postcode = r.source_key
WHERE r.run_id = current_setting('staging.run_id');

-- already finalized and its raw rows deleted: nothing to do (keep the earlier results)
DROP TABLE IF EXISTS fin_skip;
CREATE TEMP TABLE fin_skip AS
SELECT EXISTS (SELECT 1 FROM staging.stg_load_log WHERE source = 'postcodes' AND run_id = current_setting('staging.run_id'))
       AND NOT EXISTS (SELECT 1 FROM fin_rows) AS skip;

-- checks ------------------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'finalize_postcodes'
  AND NOT (SELECT skip FROM fin_skip);

WITH s AS (SELECT coalesce((SELECT bool_or(sample) FROM raw.postcodes
                            WHERE run_id = current_setting('staging.run_id')), FALSE) AS sample)
INSERT INTO staging.stg_dq_result (run_id, stage, check_name, severity, failed_count, observed_value, sample)
SELECT current_setting('staging.run_id'), 'finalize_postcodes', c.check_name, c.severity, c.n::int, c.observed, s.sample
FROM s CROSS JOIN LATERAL (
    SELECT 'steps_passed', 'run',
           CASE WHEN staging.stage_passed(current_setting('staging.run_id'), 'geo') THEN 0 ELSE 1 END, NULL::BIGINT
    UNION ALL SELECT 'unexplained_rows', 'run', (SELECT count(*) FROM fin_rows WHERE outcome = 'missing'), NULL
    UNION ALL SELECT 'count_' || target || '_' || outcome, 'info', count(*), NULL FROM fin_rows GROUP BY target, outcome
) c(check_name, severity, n, observed)
WHERE NOT (SELECT skip FROM fin_skip);

-- @@ load ---------------------------------------------------------------------------------------
-- (runs only if no `run` check above failed)

INSERT INTO staging.stg_load_log AS t (source, run_id, sample, loaded_at, row_counts)
SELECT 'postcodes', current_setting('staging.run_id'),
       coalesce((SELECT bool_or(sample) FROM raw.postcodes WHERE run_id = current_setting('staging.run_id')), FALSE),
       now(), coalesce((SELECT jsonb_object_agg(target, outcomes) FROM (
                   SELECT target, jsonb_object_agg(outcome, n) AS outcomes
                   FROM (SELECT target, outcome, count(*) AS n FROM fin_rows GROUP BY 1, 2) x GROUP BY target) y), '{}')
WHERE NOT (SELECT skip FROM fin_skip)
ON CONFLICT (source, run_id) DO UPDATE SET
    sample = EXCLUDED.sample, loaded_at = EXCLUDED.loaded_at, row_counts = EXCLUDED.row_counts;

DELETE FROM raw.postcodes            WHERE run_id = current_setting('staging.run_id');
DELETE FROM raw.terminated_postcodes WHERE run_id = current_setting('staging.run_id');
DELETE FROM raw.reverse_postcodes    WHERE run_id = current_setting('staging.run_id');
