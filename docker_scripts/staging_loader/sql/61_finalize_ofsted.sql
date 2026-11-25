-- Step 6 (Ofsted): reconcile one Ofsted run, then record it in stg_load_log and delete its raw.* rows
-- (TODO 6-1 .. 6-5). Run by `stage --step finalize_ofsted`, after steps 2 and 5 (ofsted_inspections).
-- Same classification as 60_finalize_fsa.sql: loaded / quarantined / newer / missing.

DROP TABLE IF EXISTS fin_rows;
CREATE TEMP TABLE fin_rows AS
WITH sch AS (
    SELECT r.source_key, staging.to_int(staging.ofsted_null(r.payload->>'URN')) AS urn,
           left(r.fetch_date, 10)::DATE AS seen_date
    FROM raw.ofsted_schools r
    WHERE r.run_id = current_setting('staging.run_id')
), ev AS (   -- distinct inspection numbers of the run (an OEIF graded + ungraded number is one event)
    SELECT DISTINCT n.number_raw,
           CASE WHEN r.published_date ~ '^\d{4}-\d{2}-\d{2}' THEN left(r.published_date, 10)::DATE END AS release_date
    FROM raw.ofsted_schools r
    CROSS JOIN LATERAL (VALUES
        (staging.ofsted_null(r.payload->>'Inspection number of latest full inspection')),
        (staging.ofsted_null(r.payload->>'Inspection number of latest OEIF graded inspection')),
        (staging.ofsted_null(r.payload->>'Latest ungraded inspection number'))) n(number_raw)
    WHERE r.run_id = current_setting('staging.run_id') AND n.number_raw IS NOT NULL
)
SELECT 'stg_school' AS target, s.source_key,
       CASE WHEN EXISTS (SELECT 1 FROM staging.stg_quarantine q WHERE q.run_id = current_setting('staging.run_id')
                         AND q.stage = 'schools' AND q.source_key = s.source_key) THEN 'quarantined'
            WHEN t.run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN t.last_seen_date > s.seen_date THEN 'newer'
            ELSE 'missing' END AS outcome
FROM sch s LEFT JOIN staging.stg_school t ON t.urn = s.urn
UNION ALL
SELECT 'stg_ofsted_inspection', e.number_raw,
       CASE WHEN EXISTS (SELECT 1 FROM staging.stg_quarantine q WHERE q.run_id = current_setting('staging.run_id')
                         AND q.stage = 'ofsted_inspections' AND q.source_key = e.number_raw) THEN 'quarantined'
            WHEN i.run_id = current_setting('staging.run_id') THEN 'loaded'
            WHEN i.published_date > e.release_date THEN 'newer'
            ELSE 'missing' END
FROM ev e
LEFT JOIN staging.stg_ofsted_inspection i
       ON e.number_raw ~ '^\d{1,18}$' AND i.inspection_number = e.number_raw::BIGINT;

-- already finalized and its raw rows deleted: nothing to do (keep the earlier results)
DROP TABLE IF EXISTS fin_skip;
CREATE TEMP TABLE fin_skip AS
SELECT EXISTS (SELECT 1 FROM staging.stg_load_log WHERE source = 'ofsted' AND run_id = current_setting('staging.run_id'))
       AND NOT EXISTS (SELECT 1 FROM fin_rows) AS skip;

-- checks ------------------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'finalize_ofsted'
  AND NOT (SELECT skip FROM fin_skip);

WITH s AS (SELECT coalesce((SELECT bool_or(sample) FROM raw.ofsted_schools
                            WHERE run_id = current_setting('staging.run_id')), FALSE) AS sample)
INSERT INTO staging.stg_dq_result (run_id, stage, check_name, severity, failed_count, observed_value, sample)
SELECT current_setting('staging.run_id'), 'finalize_ofsted', c.check_name, c.severity, c.n::int, c.observed, s.sample
FROM s CROSS JOIN LATERAL (
    SELECT 'steps_passed', 'run',
           CASE WHEN staging.stage_passed(current_setting('staging.run_id'), 'schools')
                 AND staging.stage_passed(current_setting('staging.run_id'), 'ofsted_inspections') THEN 0 ELSE 1 END,
           (NOT staging.stage_passed(current_setting('staging.run_id'), 'schools'))::INT
           + (NOT staging.stage_passed(current_setting('staging.run_id'), 'ofsted_inspections'))::INT
    UNION ALL SELECT 'source_present', 'run',
           CASE WHEN EXISTS (SELECT 1 FROM fin_rows WHERE target = 'stg_school') THEN 0 ELSE 1 END, NULL
    UNION ALL SELECT 'unexplained_rows', 'run', (SELECT count(*) FROM fin_rows WHERE outcome = 'missing'), NULL
    UNION ALL SELECT 'count_' || target || '_' || outcome, 'info', count(*), NULL FROM fin_rows GROUP BY target, outcome
) c(check_name, severity, n, observed)
WHERE NOT (SELECT skip FROM fin_skip);

-- @@ load ---------------------------------------------------------------------------------------
-- (runs only if no `run` check above failed)

INSERT INTO staging.stg_load_log AS t (source, run_id, sample, loaded_at, row_counts)
SELECT 'ofsted', current_setting('staging.run_id'),
       coalesce((SELECT bool_or(sample) FROM raw.ofsted_schools WHERE run_id = current_setting('staging.run_id')), FALSE),
       now(), (SELECT jsonb_object_agg(target, outcomes) FROM (
                   SELECT target, jsonb_object_agg(outcome, n) AS outcomes
                   FROM (SELECT target, outcome, count(*) AS n FROM fin_rows GROUP BY 1, 2) x GROUP BY target) y)
WHERE NOT (SELECT skip FROM fin_skip)
ON CONFLICT (source, run_id) DO UPDATE SET
    sample = EXCLUDED.sample, loaded_at = EXCLUDED.loaded_at, row_counts = EXCLUDED.row_counts;

DELETE FROM raw.ofsted_schools WHERE run_id = current_setting('staging.run_id');
