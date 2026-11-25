-- raw: the landed MongoDB documents copied as they are (envelope fields + payload JSONB).
-- staging: the typed, cleaned stg_* tables built from raw by the SQL files of this folder.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
