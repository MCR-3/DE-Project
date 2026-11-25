-- Report views (re-created by `init`).

-- One row per run and step: how many checks ran, which run-level checks failed, how many rows were
-- quarantined and why, and whether the run was finalized (stg_load_log). The source of the DQ numbers
-- of the report.
CREATE OR REPLACE VIEW staging.v_dq_report AS
WITH d AS (
    SELECT d.*,
           CASE WHEN d.stage IN ('reference', 'establishments', 'fsa_measures', 'finalize_fsa') THEN 'fsa'
                WHEN d.stage IN ('schools', 'ofsted_inspections', 'finalize_ofsted') THEN 'ofsted'
                WHEN d.stage IN ('geo', 'finalize_postcodes') THEN 'postcodes' END AS source
    FROM staging.stg_dq_result d
)
SELECT d.source, d.run_id, d.stage, bool_or(d.sample) AS sample, max(d.checked_at) AS checked_at,
       count(*) AS checks,
       count(*) FILTER (WHERE d.severity = 'run' AND d.failed_count > 0) AS run_checks_failed,
       string_agg(d.check_name || '=' || d.failed_count, ', ' ORDER BY d.check_name)
           FILTER (WHERE d.severity = 'run' AND d.failed_count > 0) AS failed_checks,
       (SELECT count(*) FROM staging.stg_quarantine q WHERE q.run_id = d.run_id AND q.stage = d.stage) AS rows_quarantined,
       string_agg(substr(d.check_name, 5) || '=' || d.failed_count, ', ' ORDER BY d.check_name)
           FILTER (WHERE d.severity = 'row' AND d.failed_count > 0) AS quarantine_reasons,
       EXISTS (SELECT 1 FROM staging.stg_load_log l WHERE l.source = d.source AND l.run_id = d.run_id) AS finalized
FROM d
GROUP BY d.source, d.run_id, d.stage;
