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
    run_id                 TEXT         NOT NULL,
    loaded_at              TIMESTAMPTZ  NOT NULL DEFAULT now()
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
    last_fetch_date      DATE         NOT NULL,          -- fetch date of the latest run the business appeared in
    published_date       TIMESTAMP,                      -- LastPublishedDate of its authority in that run
    run_id               TEXT         NOT NULL,
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
    last_fetch_date     DATE         NOT NULL,
    published_date      TIMESTAMP,
    run_id              TEXT         NOT NULL,
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
    run_id        TEXT        NOT NULL,
    stage         VARCHAR(30) NOT NULL,
    check_name    VARCHAR(80) NOT NULL,
    failed_count  INTEGER     NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, stage, check_name)              -- a rerun overwrites its own results
);
