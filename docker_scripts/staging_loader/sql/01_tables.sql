-- Every statement is idempotent: `init` can run this file again at any time.

----------------------------------------------------------------------------------------------------
-- raw: one table per MongoDB landing collection (raw_<name> -> raw.<name>), filled by `copy-raw`.
-- Nothing is typed or cleaned here; the dates stay as the landing wrote them.
----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.fsa_authorities (
    run_id          TEXT        NOT NULL,
    source_key      TEXT        NOT NULL,
    source          TEXT        NOT NULL,
    dataset         TEXT        NOT NULL,
    sample          BOOLEAN     NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL,
    fetch_date      TEXT,
    published_date  TEXT,
    payload         JSONB       NOT NULL,
    copied_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, source_key)
);
CREATE TABLE IF NOT EXISTS raw.fsa_business_types     (LIKE raw.fsa_authorities INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.fsa_ratings            (LIKE raw.fsa_authorities INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.fsa_score_descriptors  (LIKE raw.fsa_authorities INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.fsa_establishments     (LIKE raw.fsa_authorities INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.ofsted_schools         (LIKE raw.fsa_authorities INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.postcodes              (LIKE raw.fsa_authorities INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.terminated_postcodes   (LIKE raw.fsa_authorities INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.reverse_postcodes      (LIKE raw.fsa_authorities INCLUDING ALL);

----------------------------------------------------------------------------------------------------
-- staging: reference tables
-- `run_id` on every stg_* table = the landing run that last wrote the row.
----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.stg_local_authority (
    la_code           VARCHAR(10)  PRIMARY KEY,          -- FSA LocalAuthorityIdCode (e.g. '857'), not LocalAuthorityId
    fsa_authority_id  INTEGER      NOT NULL UNIQUE,      -- FSA LocalAuthorityId (API id, --la-ids)
    la_name           VARCHAR(100) NOT NULL,
    region            VARCHAR(40),                       -- Ofsted region name; NULL outside England
    country           VARCHAR(20)  NOT NULL,             -- England / Scotland / Wales / Northern Ireland
    website           VARCHAR(255),
    email             VARCHAR(255),
    run_id            TEXT         NOT NULL,
    loaded_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.stg_business_type (
    business_type_id    INTEGER      PRIMARY KEY,        -- FSA BusinessTypeId (-1 "All" is left out)
    business_type_name  VARCHAR(100) NOT NULL,
    run_id              TEXT         NOT NULL,
    loaded_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.stg_fsa_rating (
    rating_id         INTEGER     PRIMARY KEY,           -- ratingId
    rating_name       VARCHAR(40) NOT NULL,              -- e.g. 5, Exempt, Pass
    rating_key        VARCHAR(50) NOT NULL UNIQUE,       -- e.g. fhrs_5_en-gb, joins stg_fsa_inspection.rating_key
    rating_key_name   VARCHAR(40),
    scheme_type_id    SMALLINT,                          -- 1 FHRS, 2 FHIS
    run_id            TEXT        NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.stg_fsa_score_descriptor (
    descriptor_id   INTEGER      PRIMARY KEY,            -- Id
    score_category  VARCHAR(20)  NOT NULL,               -- Hygiene / Structural / Confidence
    score           SMALLINT     NOT NULL,               -- penalty points (0 = best)
    description     VARCHAR(100) NOT NULL,               -- e.g. Very good
    run_id          TEXT         NOT NULL,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (score_category, score)
);

-- Seeds (seeds/*.csv, reloaded by `init`)
-- Measures of stg_inspection_measure (source of the warehouse Dim_Measure). Step 5 unpivots exactly the
-- source fields listed here.
CREATE TABLE IF NOT EXISTS staging.stg_measure (
    measure_code  VARCHAR(40)  PRIMARY KEY,
    source        VARCHAR(6)   NOT NULL CHECK (source IN ('FSA', 'OFSTED')),
    framework     VARCHAR(12)  NOT NULL CHECK (framework IN ('FHRS', 'RENEWED_2025', 'EIF_2019', 'UNGRADED')),
    source_field  VARCHAR(80)  NOT NULL,                 -- Ofsted column / FSA scores key (RatingValue for the overall)
    measure_name  VARCHAR(80)  NOT NULL,
    scale         VARCHAR(20)  NOT NULL,                 -- joins stg_ofsted_level.scale for Ofsted
    min_value     SMALLINT     NOT NULL,
    max_value     SMALLINT     NOT NULL,
    better_when   VARCHAR(6)   NOT NULL CHECK (better_when IN ('Lower', 'Higher')),
    is_overall    BOOLEAN      NOT NULL,
    in_composite  BOOLEAN      NOT NULL,
    UNIQUE (framework, source_field)
);

CREATE TABLE IF NOT EXISTS staging.stg_ofsted_level (
    scale        VARCHAR(20) NOT NULL CHECK (scale IN ('OFSTED_4_POINT', 'OFSTED_5_POINT', 'OFSTED_UNGRADED')),
    level_text   VARCHAR(60) NOT NULL,                   -- value as published (e.g. 2, Strong standard, School remains Good)
    level_value  SMALLINT,                               -- 1 = best; for OFSTED_UNGRADED the implied grade; NULL if not rated / unclear
    level_name   VARCHAR(40) NOT NULL,                   -- e.g. Good, Good (implied), Unclear
    is_rated     BOOLEAN     NOT NULL,
    PRIMARY KEY (scale, level_text),
    CHECK (is_rated = (level_value IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS staging.stg_region_map (
    source       VARCHAR(12) NOT NULL,                   -- fsa / postcodes_io / ofsted
    source_name  VARCHAR(60) NOT NULL,                   -- region name as the source writes it
    region_name  VARCHAR(40) NOT NULL,                   -- standard name (Ofsted)
    PRIMARY KEY (source, source_name)
);

CREATE TABLE IF NOT EXISTS staging.stg_district_la_map (
    gss_code       VARCHAR(9)   NOT NULL,                -- ONS district code from postcodes.io (e.g. E07000035)
    district_name  VARCHAR(100) NOT NULL,
    la_code        VARCHAR(10)  NOT NULL,                -- FSA la_code; no FK (seeded before authorities), checked by DQ
    match_method   VARCHAR(12)  NOT NULL CHECK (match_method IN ('exact', 'normalised', 'manual')),
                                                         -- how the row was made: same name / spelling only / by hand
    PRIMARY KEY (gss_code, la_code)                      -- 1:N allowed (2023 reorganisations)
);

----------------------------------------------------------------------------------------------------
-- staging: institutions
----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.stg_school (
    urn                    INTEGER      PRIMARY KEY,
    laestab                INTEGER,
    school_name            VARCHAR(255) NOT NULL,
    ofsted_phase           VARCHAR(30),
    type_of_education      VARCHAR(60),
    open_date              DATE,
    has_sixth_form         BOOLEAN,
    postcode               VARCHAR(10),
    postcode_sector        VARCHAR(6),
    latitude               NUMERIC(9,6),                 -- step 4 (postcodes.io)
    longitude              NUMERIC(9,6),
    la_code                VARCHAR(10)  REFERENCES staging.stg_local_authority (la_code),  -- step 4, via GSS code
    local_authority_name   VARCHAR(100),                 -- as written by Ofsted (reference only, not used to match)
    admin_district_code    VARCHAR(9),                   -- step 4
    admin_district_name    VARCHAR(100),
    admin_county_code      VARCHAR(9),
    admin_county_name      VARCHAR(100),
    region                 VARCHAR(40),
    idaci_quintile         SMALLINT     CHECK (idaci_quintile BETWEEN 1 AND 5),
    total_pupils           INTEGER,
    statutory_lowest_age   SMALLINT,
    statutory_highest_age  SMALLINT,
    mat_name               VARCHAR(150),
    report_url             VARCHAR(300),
    published_date         DATE,                         -- "as at" date of the Ofsted release (replaces source_file)
    run_id                 TEXT         NOT NULL,        -- run that last wrote the values
    last_seen_run_id       TEXT         NOT NULL,        -- last run the school appeared in (even if quarantined there)
    last_seen_date         DATE         NOT NULL,        -- fetch date of that run; older than the latest full run = gone from the source
    match_method           VARCHAR(16)  CHECK (match_method IN ('postcode', 'reverse_geocode')),  -- step 4: how la_code was found
    reverse_distance_m     NUMERIC(8,1),                 -- step 4: terminated postcode -> nearest live postcode, metres
    loaded_at              TIMESTAMPTZ  NOT NULL DEFAULT now()
);
-- tables created before step 4
ALTER TABLE staging.stg_school ADD COLUMN IF NOT EXISTS match_method VARCHAR(16)
    CHECK (match_method IN ('postcode', 'reverse_geocode'));
ALTER TABLE staging.stg_school ADD COLUMN IF NOT EXISTS reverse_distance_m NUMERIC(8,1);

-- Step 4: the latest postcodes.io answer for every postcode looked up (any postcode run), so the
-- geocoding survives the deletion of raw.* and a run that found everything in the cache.
CREATE TABLE IF NOT EXISTS staging.stg_postcode (
    postcode              VARCHAR(10)  PRIMARY KEY,      -- standard form, as sent to postcodes.io
    status                VARCHAR(10)  NOT NULL CHECK (status IN ('live', 'terminated', 'not_found')),
    latitude              NUMERIC(9,6),                  -- live: the postcode; terminated: the terminated postcode
    longitude             NUMERIC(9,6),
    admin_district_code   VARCHAR(9),                    -- live: the postcode; terminated: nearest live postcode
    admin_district_name   VARCHAR(100),
    admin_county_code     VARCHAR(9),                    -- NULL for unitary areas
    admin_county_name     VARCHAR(100),
    region                VARCHAR(40),                   -- standard (Ofsted) name via stg_region_map
    reverse_postcode      VARCHAR(10),                   -- terminated only: nearest live postcode found
    reverse_distance_m    NUMERIC(8,1),
    fetched_at            TIMESTAMPTZ  NOT NULL,         -- fetch date of the bulk lookup
    run_id                TEXT         NOT NULL,         -- postcode run of that lookup
    loaded_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.stg_food_establishment (
    fhrs_id              INTEGER      PRIMARY KEY,
    la_business_id       VARCHAR(50),
    business_name        VARCHAR(255) NOT NULL,
    business_type_id     INTEGER      NOT NULL REFERENCES staging.stg_business_type (business_type_id),
    address_line_1       VARCHAR(255),
    address_line_2       VARCHAR(255),
    address_line_3       VARCHAR(255),
    address_line_4       VARCHAR(255),
    postcode             VARCHAR(10),
    postcode_sector      VARCHAR(6),
    postcode_is_partial  BOOLEAN      NOT NULL DEFAULT FALSE,   -- outward code only (e.g. BA1)
    latitude             NUMERIC(9,6),
    longitude            NUMERIC(9,6),
    coord_source         VARCHAR(12)  CHECK (coord_source IN ('fsa', 'postcodes_io')),
    la_code              VARCHAR(10)  NOT NULL REFERENCES staging.stg_local_authority (la_code),
    published_date       TIMESTAMP,                      -- LastPublishedDate of its authority in that run
    run_id               TEXT         NOT NULL,          -- run that last wrote the values
    last_seen_run_id     TEXT         NOT NULL,          -- last run the business appeared in (even if quarantined there)
    last_seen_date       DATE         NOT NULL,          -- fetch date of that run; older than the latest full run = gone from the source
    loaded_at            TIMESTAMPTZ  NOT NULL DEFAULT now()
);

----------------------------------------------------------------------------------------------------
-- staging: inspection events (accumulate over runs, never deleted)
----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.stg_ofsted_inspection (
    inspection_number          BIGINT       PRIMARY KEY,
    urn                        INTEGER      NOT NULL REFERENCES staging.stg_school (urn),
    framework                  VARCHAR(12)  NOT NULL CHECK (framework IN ('RENEWED_2025', 'EIF_2019', 'UNGRADED')),
    inspection_type            VARCHAR(100),
    inspection_type_grouping   VARCHAR(100),
    event_type_grouping        VARCHAR(100),
    inspection_start_date      DATE         NOT NULL,
    publication_date           DATE,
    category_of_concern        VARCHAR(40),              -- NULL = no concern ('' in the source; the event exists, so it was inspected)
    safeguarding_met           BOOLEAN,
    ungraded_outcome           VARCHAR(60),
    relates_to_current_urn     BOOLEAN,
    urn_at_inspection          INTEGER,
    laestab_at_inspection      INTEGER,
    school_name_at_inspection  VARCHAR(255),
    school_type_at_inspection  VARCHAR(100),
    published_date             DATE,                     -- "as at" date of the release that last wrote the row
    run_id                     TEXT         NOT NULL,
    loaded_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.stg_fsa_inspection (
    fsa_inspection_id   BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fhrs_id             INTEGER      NOT NULL REFERENCES staging.stg_food_establishment (fhrs_id),
    rating_date         DATE,                            -- NULL = not inspected yet (source 1901-01-01)
    rating_value        VARCHAR(30)  NOT NULL,
    rating_key          VARCHAR(50)  NOT NULL,
    scheme_type         VARCHAR(4)   NOT NULL,
    new_rating_pending  BOOLEAN      NOT NULL,
    first_fetch_date    DATE         NOT NULL,           -- first run that saw this rating (date of rows without rating_date)
    last_fetch_date     DATE         NOT NULL,           -- fetch date of the last run that saw this rating
    published_date      TIMESTAMP,
    run_id              TEXT         NOT NULL,
    last_seen_run_id    TEXT         NOT NULL,           -- last run that saw this rating: the business's current
                                                         -- rating is its row seen last (not the latest rating_date)
    loaded_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- one row per (business, inspection date); NULLS NOT DISTINCT = at most one not-inspected row per business
    CONSTRAINT stg_fsa_inspection_event UNIQUE NULLS NOT DISTINCT (fhrs_id, rating_date)
);

CREATE TABLE IF NOT EXISTS staging.stg_inspection_measure (
    measure_row_id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source                    VARCHAR(6)  NOT NULL CHECK (source IN ('FSA', 'OFSTED')),
    fsa_inspection_id         BIGINT      REFERENCES staging.stg_fsa_inspection (fsa_inspection_id),
    ofsted_inspection_number  BIGINT      REFERENCES staging.stg_ofsted_inspection (inspection_number),
    measure_code              VARCHAR(40) NOT NULL,      -- e.g. FHRS_HYGIENE, OFSTED_RC_INCLUSION, OFSTED_UNGRADED_EST
    raw_score                 SMALLINT,
    level_text                VARCHAR(60),               -- 60, not 30: holds ungraded outcome sentences
    run_id                    TEXT        NOT NULL,
    loaded_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((source = 'FSA') = (fsa_inspection_id IS NOT NULL)),
    CHECK ((source = 'OFSTED') = (ofsted_inspection_number IS NOT NULL)),
    CONSTRAINT stg_inspection_measure_key UNIQUE NULLS NOT DISTINCT (fsa_inspection_id, ofsted_inspection_number, measure_code)
);

----------------------------------------------------------------------------------------------------
-- staging: bookkeeping
----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.stg_load_log (
    source      VARCHAR(10) NOT NULL,                    -- fsa / ofsted
    run_id      TEXT        NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),      -- written last: a run without a row is not staged yet
    row_counts  JSONB,
    PRIMARY KEY (source, run_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_dq_result (
    run_id          TEXT        NOT NULL,
    stage           VARCHAR(30) NOT NULL,
    check_name      VARCHAR(80) NOT NULL,
    severity        VARCHAR(10) NOT NULL DEFAULT 'run' CHECK (severity IN ('run', 'row', 'info')),
                                                          -- run: failed_count > 0 fails the step (nothing loaded)
                                                          -- row: rows sent to stg_quarantine for this reason
                                                          -- info: a number kept for the report
    failed_count    INTEGER     NOT NULL,
    observed_value  BIGINT,                               -- e.g. source row count (compared with the previous full run)
    sample          BOOLEAN,                              -- the run is a sample run
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage, check_name)               -- a rerun overwrites its own results
);
-- tables created before these columns existed
ALTER TABLE staging.stg_dq_result ADD COLUMN IF NOT EXISTS severity VARCHAR(10) NOT NULL DEFAULT 'run'
    CHECK (severity IN ('run', 'row', 'info'));
ALTER TABLE staging.stg_dq_result ADD COLUMN IF NOT EXISTS observed_value BIGINT;
ALTER TABLE staging.stg_dq_result ADD COLUMN IF NOT EXISTS sample BOOLEAN;

-- Rows left out by a step's row-level checks, with the source record as received.
-- Written in the same transaction as the step's load; a rerun of the step replaces its own rows.
CREATE TABLE IF NOT EXISTS staging.stg_quarantine (
    run_id          TEXT        NOT NULL,
    stage           VARCHAR(30) NOT NULL,
    source_key      TEXT        NOT NULL,
    reasons         TEXT[]      NOT NULL,                 -- e.g. {urn_invalid,open_date_invalid}
    payload         JSONB       NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage, source_key)
);
