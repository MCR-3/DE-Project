-- Step 5 (FSA): rows of stg_inspection_measure for the FSA ratings of one FSA run (TODO 5-9 .. 5-12).
-- Run by `stage --step fsa_measures`, after step 3 for the same run.
-- Same two-transaction rule as the other steps (split at `-- @@ load`).
--
-- Each rating seen in this run (stg_fsa_inspection.last_seen_run_id = run) gets up to 4 rows, from the
-- fields of stg_measure (source FSA): Hygiene, Structural, ConfidenceInManagement (penalty points, a
-- missing score gives no row) and the overall rating (always one row; raw_score NULL when the rating is
-- not 0-5, e.g. AwaitingInspection, Exempt). A penalty-point score must be one of the FSA score
-- descriptors of its category.

DROP TABLE IF EXISTS fsa_rated, fsa_measure, fsa_problem;
CREATE TEMP TABLE fsa_rated AS
SELECT r.source_key, r.payload, fi.fsa_inspection_id
FROM raw.fsa_establishments r
JOIN staging.stg_fsa_inspection fi
  ON fi.fhrs_id = staging.to_int(r.payload->>'FHRSID')
 AND fi.rating_date IS NOT DISTINCT FROM staging.parse_fsa_date(r.payload->>'RatingDate')
 AND fi.last_seen_run_id = current_setting('staging.run_id')         -- an older run never rewrites them
WHERE r.run_id = current_setting('staging.run_id');
CREATE INDEX ON fsa_rated (source_key);
ANALYZE fsa_rated;

CREATE TEMP TABLE fsa_measure AS
SELECT f.source_key, f.fsa_inspection_id, m.measure_code, v.value AS level_text,
       CASE WHEN v.value ~ '^\d{1,4}$' THEN v.value::SMALLINT END AS raw_score,
       m.is_overall, m.min_value, m.max_value,
       -- sub-scores: the value is a score descriptor of its category
       CASE WHEN m.is_overall THEN TRUE
            ELSE EXISTS (SELECT 1 FROM staging.stg_fsa_score_descriptor d
                         WHERE d.score_category = CASE m.source_field WHEN 'ConfidenceInManagement' THEN 'Confidence'
                                                                      ELSE m.source_field END
                           AND d.score::TEXT = v.value) END AS score_known
FROM fsa_rated f
JOIN staging.stg_measure m ON m.source = 'FSA'
CROSS JOIN LATERAL (
    SELECT CASE WHEN m.is_overall THEN staging.blank_to_null(f.payload->>'RatingValue')
                ELSE staging.blank_to_null(f.payload->'scores'->>m.source_field) END AS value
) v
WHERE v.value IS NOT NULL;                                            -- 5-10 missing sub-score: no row
-- the overall rating is a score only when it is 0-5
UPDATE fsa_measure SET raw_score = NULL WHERE is_overall AND level_text !~ '^[0-5]$';
CREATE INDEX ON fsa_measure (source_key);
ANALYZE fsa_measure;

CREATE TEMP TABLE fsa_problem AS
SELECT f.source_key, f.payload, array_remove(ARRAY[
    CASE WHEN EXISTS (SELECT 1 FROM fsa_measure m WHERE m.source_key = f.source_key AND NOT m.is_overall
                      AND (m.raw_score IS NULL OR NOT m.score_known)) THEN 'score_invalid' END,
    CASE WHEN EXISTS (SELECT 1 FROM fsa_measure m WHERE m.source_key = f.source_key
                      AND m.raw_score NOT BETWEEN m.min_value AND m.max_value) THEN 'score_out_of_range' END
], NULL) AS reasons
FROM fsa_rated f;
DELETE FROM fsa_problem WHERE cardinality(reasons) = 0;
CREATE INDEX ON fsa_problem (source_key);
ANALYZE fsa_problem;

-- checks ------------------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'fsa_measures';

WITH stats AS (
    SELECT (SELECT count(*) FROM fsa_rated) AS ratings,
           (SELECT count(*) FROM fsa_problem) AS quarantined,
           (SELECT bool_or(sample) FROM raw.fsa_establishments WHERE run_id = current_setting('staging.run_id')) AS sample,
           EXISTS (SELECT 1 FROM staging.stg_dq_result d
                   WHERE d.run_id = current_setting('staging.run_id') AND d.stage = 'establishments')
           AND NOT EXISTS (SELECT 1 FROM staging.stg_dq_result d
                           WHERE d.run_id = current_setting('staging.run_id') AND d.stage = 'establishments'
                             AND d.severity = 'run' AND d.failed_count > 0) AS establishments_ok
)
INSERT INTO staging.stg_dq_result (run_id, stage, check_name, severity, failed_count, observed_value, sample)
SELECT current_setting('staging.run_id'), 'fsa_measures', c.check_name, c.severity, c.n::int, c.observed,
       coalesce(s.sample, FALSE)
FROM stats s CROSS JOIN LATERAL (
    SELECT 'establishments_staged', 'run', CASE WHEN s.establishments_ok THEN 0 ELSE 1 END, NULL::BIGINT
    UNION ALL SELECT 'ratings_present', 'run', CASE WHEN s.ratings = 0 THEN 1 ELSE 0 END, s.ratings
    UNION ALL SELECT 'quarantine_rate', 'run',
                     CASE WHEN s.ratings > 0 AND s.quarantined > 0.05 * s.ratings THEN 1 ELSE 0 END, s.quarantined
    UNION ALL SELECT 'row_score_invalid', 'row',
                     (SELECT count(*) FROM fsa_problem WHERE 'score_invalid' = ANY (reasons)), NULL
    UNION ALL SELECT 'row_score_out_of_range', 'row',
                     (SELECT count(*) FROM fsa_problem WHERE 'score_out_of_range' = ANY (reasons)), NULL
    UNION ALL SELECT 'info_measures_' || lower(measure_code), 'info', count(*), NULL FROM fsa_measure GROUP BY measure_code
    -- a numeric rating without sub-scores (rescore not uploaded by the authority)
    UNION ALL SELECT 'info_rated_without_scores', 'info',
                     (SELECT count(*) FROM fsa_rated f
                      WHERE f.payload->>'RatingValue' ~ '^[0-5]$'
                        AND NOT EXISTS (SELECT 1 FROM fsa_measure m WHERE m.source_key = f.source_key AND NOT m.is_overall)), NULL
) c(check_name, severity, n, observed);

-- @@ load ---------------------------------------------------------------------------------------
-- (runs only if no `run` check above failed)

DELETE FROM staging.stg_quarantine WHERE run_id = current_setting('staging.run_id') AND stage = 'fsa_measures';
INSERT INTO staging.stg_quarantine (run_id, stage, source_key, reasons, payload)
SELECT current_setting('staging.run_id'), 'fsa_measures', source_key, reasons, payload FROM fsa_problem;

-- 5-11 upsert, then drop the measures of these ratings that this run no longer has
INSERT INTO staging.stg_inspection_measure AS t
    (source, fsa_inspection_id, measure_code, raw_score, level_text, run_id, loaded_at)
SELECT 'FSA', m.fsa_inspection_id, m.measure_code, m.raw_score, m.level_text, current_setting('staging.run_id'), now()
FROM fsa_measure m
WHERE NOT EXISTS (SELECT 1 FROM fsa_problem p WHERE p.source_key = m.source_key)
ON CONFLICT ON CONSTRAINT stg_inspection_measure_key DO UPDATE SET
    raw_score = EXCLUDED.raw_score, level_text = EXCLUDED.level_text,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at;

DELETE FROM staging.stg_inspection_measure t
USING fsa_rated f
WHERE t.fsa_inspection_id = f.fsa_inspection_id AND t.run_id <> current_setting('staging.run_id')
  AND NOT EXISTS (SELECT 1 FROM fsa_problem p WHERE p.source_key = f.source_key);
