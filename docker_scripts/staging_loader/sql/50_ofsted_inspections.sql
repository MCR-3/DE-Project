-- Step 5 (Ofsted): stg_ofsted_inspection and its rows of stg_inspection_measure from one Ofsted run
-- (TODO 5-1 .. 5-8, 5-11, 5-12). Run by `stage --step ofsted_inspections`, after step 2 for the same run.
-- Same two-transaction rule as the other steps (split at `-- @@ load`).
--
-- A school row gives up to three events: latest full inspection (RENEWED_2025, report card labels),
-- latest OEIF graded inspection (EIF_2019, grades 1-4) and latest ungraded inspection (UNGRADED).
-- The same inspection number can be both the OEIF graded and the ungraded inspection of a school: it is
-- one event, EIF_2019, that keeps the ungraded outcome too and gets no OFSTED_UNGRADED_EST row.
-- The measures are the source fields listed in stg_measure; their values are mapped through
-- stg_ofsted_level ('Not applicable' / 'Not judged' give a row with raw_score NULL; OEIF 0 and 9 are
-- placeholders and give no row).

-- 5-1 .. 5-3 events (one row per school and kind) -----------------------------------------------
DROP TABLE IF EXISTS ofs_event_raw, ofs_event, ofs_measure, ofs_problem;
CREATE TEMP TABLE ofs_event_raw AS
SELECT r.source_key AS school_key, r.payload, staging.to_int(r.source_key) AS urn, e.*,
       CASE WHEN r.published_date ~ '^\d{4}-\d{2}-\d{2}' THEN left(r.published_date, 10)::DATE END AS release_date
FROM raw.ofsted_schools r
CROSS JOIN LATERAL (VALUES
    (1, 'RENEWED_2025',
     staging.ofsted_null(r.payload->>'Inspection number of latest full inspection'),
     staging.ofsted_null(r.payload->>'Inspection type'),
     staging.ofsted_null(r.payload->>'Inspection type grouping'),
     staging.ofsted_null(r.payload->>'Event type grouping'),
     staging.ofsted_null(r.payload->>'Inspection start date'),
     staging.ofsted_null(r.payload->>'Publication date'),
     staging.ofsted_null(r.payload->>'Does the latest full inspection relate to the URN of the current school?'),
     staging.ofsted_null(r.payload->>'URN at time of latest full inspection'),
     staging.ofsted_null(r.payload->>'LAESTAB at time of latest full inspection'),
     staging.ofsted_null(r.payload->>'School name at time of latest full inspection'),
     staging.ofsted_null(r.payload->>'School type at time of latest full inspection'),
     r.payload->>'Category of concern',
     staging.ofsted_null(r.payload->>'Safeguarding standards'),
     NULL::TEXT),
    (2, 'EIF_2019',
     staging.ofsted_null(r.payload->>'Inspection number of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'Inspection type of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'Inspection type grouping of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'Event type grouping of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'Inspection start date of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'Publication date of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'Does the latest OEIF graded inspection relate to the URN of the current school?'),
     staging.ofsted_null(r.payload->>'URN at time of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'LAESTAB at time of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'School name at time of latest OEIF graded inspection'),
     staging.ofsted_null(r.payload->>'School type at time of latest OEIF graded inspection'),
     r.payload->>'Latest OEIF category of concern',
     staging.ofsted_null(r.payload->>'Latest OEIF safeguarding is effective?'),
     NULL::TEXT),
    (3, 'UNGRADED',
     staging.ofsted_null(r.payload->>'Latest ungraded inspection number'),
     NULL, NULL, NULL,                                  -- the MI file gives no type for ungraded inspections
     staging.ofsted_null(r.payload->>'Date of latest ungraded inspection'),
     staging.ofsted_null(r.payload->>'Ungraded inspection publication date'),
     staging.ofsted_null(r.payload->>'Does the ungraded inspection relate to the URN of the current school?'),
     staging.ofsted_null(r.payload->>'URN at time of ungraded inspection'),
     staging.ofsted_null(r.payload->>'LAESTAB at time of ungraded inspection'),
     staging.ofsted_null(r.payload->>'School name at time of ungraded inspection'),
     staging.ofsted_null(r.payload->>'School type at time of ungraded inspection'),
     NULL,
     NULL,
     staging.ofsted_null(r.payload->>'Ungraded inspection overall outcome'))
) e(priority, framework, number_raw, inspection_type, inspection_type_grouping, event_type_grouping,
    start_raw, publication_raw, relates_raw, urn_at_raw, laestab_at_raw, school_name_at, school_type_at,
    concern_raw, safeguarding_raw, ungraded_outcome)
WHERE r.run_id = current_setting('staging.run_id') AND e.number_raw IS NOT NULL;

-- one row per inspection number: the graded kind wins, the ungraded outcome is kept
CREATE TEMP TABLE ofs_event AS
SELECT DISTINCT ON (number_raw)
       number_raw, school_key, payload, urn, framework, inspection_type, inspection_type_grouping,
       event_type_grouping, start_raw, publication_raw, relates_raw, urn_at_raw, laestab_at_raw,
       school_name_at, school_type_at, concern_raw, safeguarding_raw, release_date,
       CASE WHEN number_raw ~ '^\d{1,18}$' THEN number_raw::BIGINT END                AS inspection_number,
       staging.parse_ofsted_date(start_raw)                                            AS inspection_start_date,
       staging.parse_ofsted_date(publication_raw)                                      AS publication_date,
       -- '' = inspected, no concern; NULL placeholder handled by ofsted_null
       staging.ofsted_null(concern_raw)                                                AS category_of_concern,
       CASE safeguarding_raw WHEN 'Met' THEN TRUE WHEN 'Yes' THEN TRUE
                             WHEN 'Not met' THEN FALSE WHEN 'No' THEN FALSE END       AS safeguarding_met,
       CASE relates_raw WHEN 'Yes' THEN TRUE WHEN 'No' THEN FALSE END                  AS relates_to_current_urn,
       staging.to_int(urn_at_raw)                                                      AS urn_at_inspection,
       staging.to_int(laestab_at_raw)                                                  AS laestab_at_inspection,
       max(ungraded_outcome) OVER (PARTITION BY number_raw)                            AS ungraded_outcome,
       count(*) OVER (PARTITION BY number_raw)                                         AS kinds,
       0::BIGINT                                                                       AS schools  -- set below
FROM ofs_event_raw
ORDER BY number_raw, priority;
-- count(DISTINCT) is not allowed as a window function: recompute the number of schools per number
UPDATE ofs_event e SET schools = x.n
FROM (SELECT number_raw, count(DISTINCT urn) AS n FROM ofs_event_raw GROUP BY number_raw) x
WHERE x.number_raw = e.number_raw;
CREATE INDEX ON ofs_event (number_raw);
ANALYZE ofs_event;

-- 5-5 .. 5-8 measures: the fields of stg_measure for the event's framework -------------------------
CREATE TEMP TABLE ofs_measure AS
SELECT e.number_raw, m.measure_code, v.value AS level_text, l.level_value AS raw_score,
       (l.level_text IS NULL) AS level_unknown
FROM ofs_event e
JOIN staging.stg_measure m ON m.source = 'OFSTED'
     AND (m.framework = e.framework
          -- an ungraded-only event gets the implied grade; a merged EIF event does not
          OR (m.framework = 'UNGRADED' AND e.framework = 'UNGRADED'))
CROSS JOIN LATERAL (SELECT staging.ofsted_null(e.payload->>m.source_field) AS value) v
LEFT JOIN staging.stg_ofsted_level l ON l.scale = m.scale AND l.level_text = v.value
WHERE v.value IS NOT NULL
  AND NOT (m.framework = 'EIF_2019' AND v.value IN ('0', '9'));      -- 5-6 placeholders: no row
CREATE INDEX ON ofs_measure (number_raw);
ANALYZE ofs_measure;

-- row-level checks: one row per problem event -----------------------------------------------------
CREATE TEMP TABLE ofs_problem AS
SELECT number_raw AS source_key, payload, reasons FROM (
    SELECT e.number_raw, e.payload, array_remove(ARRAY[
        CASE WHEN e.inspection_number IS NULL THEN 'inspection_number_invalid' END,
        CASE WHEN e.schools > 1 THEN 'inspection_number_conflict' END,   -- same number, two schools
        CASE WHEN e.urn IS NULL OR NOT EXISTS (SELECT 1 FROM staging.stg_school s WHERE s.urn = e.urn)
             THEN 'school_not_staged' END,
        CASE WHEN e.inspection_start_date IS NULL THEN 'start_date_invalid' END,
        CASE WHEN e.publication_raw IS NOT NULL AND e.publication_date IS NULL THEN 'publication_date_invalid' END,
        CASE WHEN e.relates_raw IS NOT NULL AND e.relates_to_current_urn IS NULL THEN 'relates_invalid' END,
        CASE WHEN e.safeguarding_raw IS NOT NULL AND e.safeguarding_met IS NULL THEN 'safeguarding_unknown' END,
        CASE WHEN (e.urn_at_raw IS NOT NULL AND e.urn_at_inspection IS NULL)
               OR (e.laestab_at_raw IS NOT NULL AND e.laestab_at_inspection IS NULL) THEN 'urn_at_inspection_invalid' END,
        CASE WHEN EXISTS (SELECT 1 FROM ofs_measure m WHERE m.number_raw = e.number_raw AND m.level_unknown)
             THEN 'level_unknown' END,
        CASE WHEN length(e.inspection_type) > 100 OR length(e.inspection_type_grouping) > 100
               OR length(e.event_type_grouping) > 100 OR length(e.category_of_concern) > 40
               OR length(e.ungraded_outcome) > 60 OR length(e.school_name_at) > 255
               OR length(e.school_type_at) > 100 THEN 'value_too_long' END
    ], NULL) AS reasons
    FROM ofs_event e
) x
WHERE cardinality(reasons) > 0;
CREATE INDEX ON ofs_problem (source_key);
ANALYZE ofs_problem;

-- checks ------------------------------------------------------------------------------------------
DELETE FROM staging.stg_dq_result WHERE run_id = current_setting('staging.run_id') AND stage = 'ofsted_inspections';

WITH stats AS (
    SELECT (SELECT count(*) FROM ofs_event) AS events,
           (SELECT count(*) FROM ofs_problem) AS quarantined,
           (SELECT bool_or(sample) FROM raw.ofsted_schools WHERE run_id = current_setting('staging.run_id')) AS sample,
           EXISTS (SELECT 1 FROM staging.stg_dq_result d
                   WHERE d.run_id = current_setting('staging.run_id') AND d.stage = 'schools')
           AND NOT EXISTS (SELECT 1 FROM staging.stg_dq_result d
                           WHERE d.run_id = current_setting('staging.run_id') AND d.stage = 'schools'
                             AND d.severity = 'run' AND d.failed_count > 0) AS schools_ok
), reasons(reason) AS (
    VALUES ('inspection_number_invalid'), ('inspection_number_conflict'), ('school_not_staged'),
           ('start_date_invalid'), ('publication_date_invalid'), ('relates_invalid'), ('safeguarding_unknown'),
           ('urn_at_inspection_invalid'), ('level_unknown'), ('value_too_long')
)
INSERT INTO staging.stg_dq_result (run_id, stage, check_name, severity, failed_count, observed_value, sample)
SELECT current_setting('staging.run_id'), 'ofsted_inspections', c.check_name, c.severity, c.n::int, c.observed,
       coalesce(s.sample, FALSE)
FROM stats s CROSS JOIN LATERAL (
    SELECT 'schools_staged', 'run', CASE WHEN s.schools_ok THEN 0 ELSE 1 END, NULL::BIGINT
    UNION ALL SELECT 'events_present', 'run', CASE WHEN s.events = 0 THEN 1 ELSE 0 END, s.events
    UNION ALL SELECT 'quarantine_rate', 'run',
                     CASE WHEN s.events > 0 AND s.quarantined > 0.05 * s.events THEN 1 ELSE 0 END, s.quarantined
    UNION ALL SELECT 'row_' || r.reason, 'row',
                     (SELECT count(*) FROM ofs_problem p WHERE r.reason = ANY (p.reasons)), NULL FROM reasons r
    UNION ALL SELECT 'info_events_' || lower(framework), 'info', count(*), NULL FROM ofs_event GROUP BY framework
    -- same number as OEIF graded and ungraded inspection: one EIF_2019 event
    UNION ALL SELECT 'info_events_merged', 'info', (SELECT count(*) FROM ofs_event WHERE kinds > 1), NULL
    UNION ALL SELECT 'info_eif_placeholder_0_9', 'info',
                     (SELECT count(*) FROM ofs_event e JOIN staging.stg_measure m
                        ON m.framework = 'EIF_2019' AND e.framework = 'EIF_2019'
                      WHERE e.payload->>m.source_field IN ('0', '9')), NULL
    UNION ALL SELECT 'info_measures', 'info', (SELECT count(*) FROM ofs_measure), NULL
    UNION ALL SELECT 'info_measures_not_rated', 'info',
                     (SELECT count(*) FROM ofs_measure WHERE raw_score IS NULL AND NOT level_unknown), NULL
) c(check_name, severity, n, observed);

-- @@ load ---------------------------------------------------------------------------------------
-- (runs only if no `run` check above failed)

DELETE FROM staging.stg_quarantine WHERE run_id = current_setting('staging.run_id') AND stage = 'ofsted_inspections';
INSERT INTO staging.stg_quarantine (run_id, stage, source_key, reasons, payload)
SELECT current_setting('staging.run_id'), 'ofsted_inspections', source_key, reasons, payload FROM ofs_problem;

-- 5-4 events, accumulated over runs; an older release staged again never overwrites a newer one
INSERT INTO staging.stg_ofsted_inspection AS t
    (inspection_number, urn, framework, inspection_type, inspection_type_grouping, event_type_grouping,
     inspection_start_date, publication_date, category_of_concern, safeguarding_met, ungraded_outcome,
     relates_to_current_urn, urn_at_inspection, laestab_at_inspection, school_name_at_inspection,
     school_type_at_inspection, published_date, run_id, loaded_at)
SELECT e.inspection_number, e.urn, e.framework, e.inspection_type, e.inspection_type_grouping, e.event_type_grouping,
       e.inspection_start_date, e.publication_date, e.category_of_concern, e.safeguarding_met, e.ungraded_outcome,
       e.relates_to_current_urn, e.urn_at_inspection, e.laestab_at_inspection, e.school_name_at,
       e.school_type_at, e.release_date, current_setting('staging.run_id'), now()
FROM ofs_event e
WHERE NOT EXISTS (SELECT 1 FROM ofs_problem p WHERE p.source_key = e.number_raw)
ON CONFLICT (inspection_number) DO UPDATE SET
    urn = EXCLUDED.urn, framework = EXCLUDED.framework, inspection_type = EXCLUDED.inspection_type,
    inspection_type_grouping = EXCLUDED.inspection_type_grouping, event_type_grouping = EXCLUDED.event_type_grouping,
    inspection_start_date = EXCLUDED.inspection_start_date, publication_date = EXCLUDED.publication_date,
    category_of_concern = EXCLUDED.category_of_concern, safeguarding_met = EXCLUDED.safeguarding_met,
    ungraded_outcome = EXCLUDED.ungraded_outcome, relates_to_current_urn = EXCLUDED.relates_to_current_urn,
    urn_at_inspection = EXCLUDED.urn_at_inspection, laestab_at_inspection = EXCLUDED.laestab_at_inspection,
    school_name_at_inspection = EXCLUDED.school_name_at_inspection,
    school_type_at_inspection = EXCLUDED.school_type_at_inspection, published_date = EXCLUDED.published_date,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at
WHERE EXCLUDED.published_date >= t.published_date OR t.published_date IS NULL;

-- 5-11 measures of the events this run wrote: upsert, then drop the ones no longer published
INSERT INTO staging.stg_inspection_measure AS t
    (source, ofsted_inspection_number, measure_code, raw_score, level_text, run_id, loaded_at)
SELECT 'OFSTED', i.inspection_number, m.measure_code, m.raw_score, m.level_text, current_setting('staging.run_id'), now()
FROM ofs_measure m
JOIN ofs_event e ON e.number_raw = m.number_raw
JOIN staging.stg_ofsted_inspection i ON i.inspection_number = e.inspection_number
     AND i.run_id = current_setting('staging.run_id')
ON CONFLICT ON CONSTRAINT stg_inspection_measure_key DO UPDATE SET
    raw_score = EXCLUDED.raw_score, level_text = EXCLUDED.level_text,
    run_id = EXCLUDED.run_id, loaded_at = EXCLUDED.loaded_at;

DELETE FROM staging.stg_inspection_measure t
USING staging.stg_ofsted_inspection i
WHERE t.ofsted_inspection_number = i.inspection_number AND i.run_id = current_setting('staging.run_id')
  AND t.run_id <> current_setting('staging.run_id');
