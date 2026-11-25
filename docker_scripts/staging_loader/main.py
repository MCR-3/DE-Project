"""Staging: turn the landed MongoDB documents into typed PostgreSQL tables (project-db).

Meant to run in its own container, launched by the staging DAG. Python only moves data; all the
cleaning, the data-quality checks and the unpivoting are SQL (the files of sql/, run in order).

Sub-commands:
    init        Create the `raw` and `staging` schemas, their tables and the cleaning functions
                (sql/0*.sql, all idempotent) and reload the seeds (seeds/*.csv). Safe to run again.
    copy-raw    Copy the documents of the runs to stage from MongoDB to raw.* (JSONB), streamed in
                batches. Without --run-id: every full run (`sample: false`) of the source that is not
                in staging.stg_load_log yet, oldest first. With --run-id: that run only, sample or not
                (for tests). Copying a run again is a no-op (primary key (run_id, source_key)).
    stage       Run one staging step (an SQL file of sql/) on the runs to stage, same run selection as
                copy-raw (the run must be copied first). Each SQL file has two parts, split at the
                `-- @@ load` line, run as two transactions: the checks, written to
                staging.stg_dq_result and committed even when they fail; then, only if no run-level
                check failed, the load (row-level problems go to staging.stg_quarantine in the same
                transaction). A failed run-level check exits 1 with nothing loaded.
                Steps: reference (step 1, FSA), schools (step 2, Ofsted), establishments (step 3, FSA), geo (step 4, postcode run), ofsted_inspections and fsa_measures (step 5).

  * Configuration from the environment: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD (Airflow
    connection `project-db`), MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_USER, MONGO_PASSWORD (connection
    `project-mongo`), optional MONGO_AUTH_SOURCE (default `admin`).
  * Exit code: 0 ok, 1 failure, 2 bad config (non-zero makes the Airflow task fail).
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values
from pymongo import MongoClient
from pymongo.errors import PyMongoError

log = logging.getLogger("staging_loader")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_BAD_CONFIG = 2

BATCH_SIZE = 1000
HERE = Path(__file__).resolve().parent
SQL_DIR = HERE / "sql"
SEED_DIR = HERE / "seeds"

LOAD_MARKER = re.compile(r"^-- @@ load.*$", re.MULTILINE)  # splits a step's SQL into checks / load

# seeds/<file> -> staging table (reloaded in full by `init`)
SEEDS = {
    "region_map.csv": "staging.stg_region_map",
    "district_la_map.csv": "staging.stg_district_la_map",
    "ofsted_level.csv": "staging.stg_ofsted_level",
    "measure.csv": "staging.stg_measure",
}

# staging step -> (source, SQL file, tables whose row counts are logged after the step)
STEPS = {
    "reference": ("fsa", "10_reference.sql",
                  ["staging.stg_local_authority", "staging.stg_business_type",
                   "staging.stg_fsa_rating", "staging.stg_fsa_score_descriptor"]),
    "schools": ("ofsted", "20_schools.sql", ["staging.stg_school"]),
    "establishments": ("fsa", "30_establishments.sql",
                       ["staging.stg_food_establishment", "staging.stg_fsa_inspection"]),
    # run = a postcode run (pass it with --run-id: a run whose lookups were all cached has no document
    # and is never listed as pending); the step re-matches every school and business
    "ofsted_inspections": ("ofsted", "50_ofsted_inspections.sql",
                           ["staging.stg_ofsted_inspection", "staging.stg_inspection_measure"]),
    "fsa_measures": ("fsa", "51_fsa_measures.sql", ["staging.stg_inspection_measure"]),
    "geo": ("postcodes", "40_geo.sql",
            ["staging.stg_postcode", "staging.stg_school", "staging.stg_food_establishment"]),
}


class DQFailed(Exception):
    """A run-level DQ check failed: the step was not loaded for this run."""

# source -> MongoDB collections of one of its runs; the first one decides which runs exist
SOURCES = {
    "fsa": ["raw_fsa_establishments", "raw_fsa_authorities", "raw_fsa_business_types",
            "raw_fsa_ratings", "raw_fsa_score_descriptors"],
    "ofsted": ["raw_ofsted_schools"],
    "postcodes": ["raw_postcodes", "raw_terminated_postcodes", "raw_reverse_postcodes"],
}

ENVELOPE = ["run_id", "source_key", "source", "dataset", "sample", "ingested_at",
            "fetch_date", "published_date", "payload"]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Stage the landed MongoDB data into PostgreSQL.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="create schemas, tables, functions, seeds and the quarantine index")
    copy = sub.add_parser("copy-raw", help="copy the runs to stage from MongoDB to raw.*")
    copy.add_argument("--source", required=True, choices=sorted(SOURCES))
    copy.add_argument("--run-id", help="copy this run only, sample or not (default: every pending full run)")
    stage = sub.add_parser("stage", help="run one staging step (SQL) on the runs to stage")
    stage.add_argument("--step", required=True, choices=list(STEPS))
    stage.add_argument("--run-id", help="stage this run only, sample or not (default: every pending full run)")
    return parser.parse_args(argv)


def pg_connect():
    """psycopg2 connection from DB_* environment variables (raises KeyError if unset)."""
    return psycopg2.connect(
        host=os.environ["DB_HOST"], port=int(os.environ["DB_PORT"]), dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"], connect_timeout=10,
    )


def mongo_db():
    """The project MongoDB database from MONGO_* environment variables (raises KeyError if unset)."""
    client = MongoClient(
        host=os.environ["MONGO_HOST"], port=int(os.environ["MONGO_PORT"]),
        username=os.environ["MONGO_USER"], password=os.environ["MONGO_PASSWORD"],
        authSource=os.environ.get("MONGO_AUTH_SOURCE", "admin"), serverSelectionTimeoutMS=10_000,
    )
    return client, client[os.environ["MONGO_DB"]]


def raw_table(collection):
    """raw_fsa_establishments -> raw.fsa_establishments"""
    return "raw." + collection.removeprefix("raw_")


# --------------------------------------------------------------------------------------------- init

def run_init(pg, db):
    with pg, pg.cursor() as cur:  # one transaction: all or nothing
        for path in sorted(SQL_DIR.glob("0*.sql")):
            log.info("applying %s", path.name)
            cur.execute(path.read_text(encoding="utf-8"))
        for file_name, table in SEEDS.items():
            cur.execute(f"TRUNCATE {table}")
            with (SEED_DIR / file_name).open(encoding="utf-8") as f:
                cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
            cur.execute(f"SELECT count(*) FROM {table}")
            log.info("seed %s: %d rows in %s", file_name, cur.fetchone()[0], table)


# ----------------------------------------------------------------------------------------- copy-raw

def pending_runs(pg, db, source):
    """Full runs of the source in MongoDB that staging has not finished yet, oldest first."""
    landed = db[SOURCES[source][0]].distinct("run_id", {"sample": False})
    with pg.cursor() as cur:
        cur.execute("SELECT run_id FROM staging.stg_load_log WHERE source = %s", (source,))
        staged = {row[0] for row in cur.fetchall()}
    return sorted(r for r in landed if r not in staged)


def copy_collection(pg, db, collection, run_id):
    """Copy one run of one collection; returns (documents in MongoDB, rows in raw after the copy)."""
    table = raw_table(collection)
    sql = f"INSERT INTO {table} ({', '.join(ENVELOPE)}) VALUES %s ON CONFLICT (run_id, source_key) DO NOTHING"
    cursor = db[collection].find({"run_id": run_id}, {"_id": 0}, batch_size=BATCH_SIZE)
    total, batch = 0, []
    with pg, pg.cursor() as cur:  # one transaction per collection and run
        for doc in cursor:
            missing = [f for f in ENVELOPE if f not in doc]
            if missing:
                raise ValueError(f"{collection} run {run_id}: a document has no {missing} "
                                 f"(landed before the sample / fetch_date fields?)")
            batch.append(tuple(Json(doc[f]) if f == "payload" else doc[f] for f in ENVELOPE))
            if len(batch) >= BATCH_SIZE:
                execute_values(cur, sql, batch, page_size=BATCH_SIZE)
                total, batch = total + len(batch), []
        if batch:
            execute_values(cur, sql, batch, page_size=BATCH_SIZE)
            total += len(batch)
        cur.execute(f"SELECT count(*) FROM {table} WHERE run_id = %s", (run_id,))
        copied = cur.fetchone()[0]
    return total, copied


def run_copy_raw(pg, db, source, run_id=None):
    runs = [run_id] if run_id else pending_runs(pg, db, source)
    if not runs:
        log.info("%s: no pending full run to copy", source)
        return
    log.info("%s: %d run(s) to copy: %s", source, len(runs), ", ".join(runs))
    for run in runs:
        for collection in SOURCES[source]:
            documents, copied = copy_collection(pg, db, collection, run)
            if documents != copied:
                raise ValueError(f"{collection} run {run}: {documents} documents in MongoDB "
                                 f"but {copied} rows in {raw_table(collection)}")
            if documents == 0:
                log.warning("%s run %s: no document", collection, run)
            log.info("%s run %s: %d documents -> %s", collection, run, documents, raw_table(collection))


# -------------------------------------------------------------------------------------------- stage

def run_stage(pg, db, step, run_id=None):
    source, file_name, tables = STEPS[step]
    parts = LOAD_MARKER.split((SQL_DIR / file_name).read_text(encoding="utf-8"))
    if len(parts) != 2:
        raise ValueError(f"{file_name} must have exactly one '-- @@ load' line")
    checks_sql, load_sql = parts
    runs = [run_id] if run_id else pending_runs(pg, db, source)
    if not runs:
        log.info("%s: no pending full run to stage", step)
        return
    for run in runs:
        with pg.cursor() as cur:  # session setting: seen by both transactions
            cur.execute("SELECT set_config('staging.run_id', %s, false)", (run,))
        with pg, pg.cursor() as cur:  # transaction 1: checks, committed even if they fail
            cur.execute(checks_sql)
            cur.execute("SELECT check_name, severity, failed_count, observed_value FROM staging.stg_dq_result "
                        "WHERE run_id = %s AND stage = %s ORDER BY severity, check_name", (run, step))
            checks = cur.fetchall()
        for name, severity, failed, observed in checks:
            if failed or observed is not None:
                log.info("%s run %s: DQ %-5s %-28s failed=%d%s", step, run, severity, name, failed,
                         "" if observed is None else f" observed={observed}")
        failures = [name for name, severity, failed, _ in checks if severity == "run" and failed > 0]
        if failures:
            raise DQFailed(f"{step} run {run}: run-level DQ failed ({', '.join(failures)}), nothing loaded; "
                           f"see staging.stg_dq_result")
        with pg, pg.cursor() as cur:  # transaction 2: quarantine + load
            cur.execute(load_sql)
            cur.execute("SELECT count(*) FROM staging.stg_quarantine WHERE run_id = %s AND stage = %s", (run, step))
            quarantined = cur.fetchone()[0]
            for table in tables:
                cur.execute(f"SELECT count(*), count(*) FILTER (WHERE run_id = %s) FROM {table}", (run,))
                total, from_run = cur.fetchone()
                log.info("%s run %s: %s has %d rows (%d written by this run)", step, run, table, total, from_run)
        log.info("%s run %s: loaded, %d DQ checks recorded, %d rows quarantined", step, run, len(checks), quarantined)


# --------------------------------------------------------------------------------------------- main

def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    try:
        pg = pg_connect()
        client, db = mongo_db()
    except (KeyError, ValueError) as missing:
        log.error("missing or invalid environment variable %s", missing)
        return EXIT_BAD_CONFIG
    except psycopg2.OperationalError as e:
        log.error("cannot connect to PostgreSQL: %s", e)
        return EXIT_FAILURE

    try:
        if args.command == "init":
            run_init(pg, db)
        elif args.command == "copy-raw":
            run_copy_raw(pg, db, args.source, args.run_id)
        elif args.command == "stage":
            run_stage(pg, db, args.step, args.run_id)
    except DQFailed as e:
        log.error("%s", e)
        return EXIT_FAILURE
    except (psycopg2.Error, PyMongoError, OSError, ValueError):
        log.exception("script failed")
        return EXIT_FAILURE
    finally:
        pg.close()
        client.close()
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
