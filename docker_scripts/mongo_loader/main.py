"""Load one landed run (the files written by a scraper) into MongoDB, as received.

Meant to run in its own container, launched by a landing DAG (see dags/fsa_landing.py and
dags/ofsted_landing.py). It knows nothing about the sources: everything it needs is in the
`manifest.json` that the scraper writes last in the run folder:

    {"source": "fsa", "run_id": "...",
     "datasets": [{"name": "establishments", "collection": "raw_fsa_establishments",
                   "format": "json", "records_path": "establishments", "key_field": "FHRSID",
                   "extract_date": "2026-11-20", "files": ["establishments/la_326/page_0001.json", ...]},
                  {"name": "schools", "collection": "raw_ofsted_schools", "format": "csv",
                   "encoding": "utf-8-sig", "key_field": "URN", "extract_date": "2026-10-31",
                   "files": ["schools.csv"]}]}

Each source record becomes one document, appended to the dataset's collection. The record is kept
untouched under `payload` (a CSV row is a dict of strings; nothing is typed or cleaned here):

    {"source": "fsa", "dataset": "establishments", "run_id": "...", "ingested_at": <UTC datetime>,
     "extract_date": "2026-11-20", "source_key": "1733057", "payload": {...as received...}}

The loading is append-only: a run adds its documents and never touches the previous runs, which is
how the rating history builds up. A unique index on (run_id, source_key) makes a retry safe: the
documents that a failed attempt already inserted are skipped, not duplicated.

  * Command-line arguments: filled in by the DAG through `DockerOperator(command=[...])`.
  * Configuration from the environment: MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_USER, MONGO_PASSWORD
    (the DAG injects them from the Airflow connection `project-mongo`), optional MONGO_AUTH_SOURCE
    (default `admin`, where the root user of the project-mongo service lives).
  * Exit code: 0 ok, 1 failure, 2 bad config (non-zero makes the Airflow task fail).
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ASCENDING, MongoClient
from pymongo.errors import BulkWriteError, PyMongoError

log = logging.getLogger("mongo_loader")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_BAD_CONFIG = 2

BATCH_SIZE = 1000
DUPLICATE_KEY = 11000


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Load a landed run folder (scraper output) into MongoDB.")
    parser.add_argument("--run-dir", required=True, help="run folder that contains manifest.json")
    return parser.parse_args(argv)


def get_client():
    """Build a MongoClient from MONGO_* environment variables (raises KeyError if unset)."""
    return MongoClient(
        host=os.environ["MONGO_HOST"],
        port=int(os.environ["MONGO_PORT"]),
        username=os.environ["MONGO_USER"],
        password=os.environ["MONGO_PASSWORD"],
        authSource=os.environ.get("MONGO_AUTH_SOURCE", "admin"),
        serverSelectionTimeoutMS=10_000,
    )


def read_manifest(run_dir):
    """The parsed manifest. Raises ValueError if it is missing or incomplete (unfinished run)."""
    path = run_dir / "manifest.json"
    if not path.is_file():
        raise ValueError(f"{path} not found: the scraper did not finish this run")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    for field in ("source", "run_id", "datasets"):
        if field not in manifest:
            raise ValueError(f"{path} has no {field!r}")
    for dataset in manifest["datasets"]:
        for field in ("name", "collection", "format", "key_field", "files"):
            if field not in dataset:
                raise ValueError(f"a dataset of {path} has no {field!r}")
        if dataset["format"] not in ("json", "csv"):
            raise ValueError(f"dataset {dataset['name']}: unsupported format {dataset['format']!r}")
        if dataset["format"] == "json" and "records_path" not in dataset:
            raise ValueError(f"dataset {dataset['name']}: a json dataset needs 'records_path'")
    return manifest


def read_records(run_dir, dataset, file_name):
    """Yield the records of one landed file, as received."""
    path = run_dir / file_name
    if dataset["format"] == "json":
        yield from json.loads(path.read_text(encoding="utf-8"))[dataset["records_path"]]
        return
    with path.open(encoding=dataset.get("encoding", "utf-8-sig"), newline="") as f:
        for row in csv.DictReader(f):
            if None in row:  # more values than header columns
                raise ValueError(f"{file_name}: a row has more values than the header has columns")
            yield row


def envelope(manifest, dataset, ingested_at, record):
    return {
        "source": manifest["source"],
        "dataset": dataset["name"],
        "run_id": manifest["run_id"],
        "ingested_at": ingested_at,
        "extract_date": dataset.get("extract_date"),
        "source_key": str(record[dataset["key_field"]]),
        "payload": record,
    }


def insert_batch(collection, documents):
    """Insert documents; returns (inserted, skipped as already loaded for this run)."""
    try:
        return len(collection.insert_many(documents, ordered=False).inserted_ids), 0
    except BulkWriteError as e:
        errors = e.details["writeErrors"]
        if any(err["code"] != DUPLICATE_KEY for err in errors):
            raise
        return e.details["nInserted"], len(errors)


def load_dataset(db, manifest, dataset, run_dir, ingested_at):
    collection = db[dataset["collection"]]
    collection.create_index(
        [("run_id", ASCENDING), ("source_key", ASCENDING)], unique=True, name="run_id_source_key"
    )
    inserted = skipped = 0
    batch = []
    for file_name in dataset["files"]:
        for record in read_records(run_dir, dataset, file_name):
            batch.append(envelope(manifest, dataset, ingested_at, record))
            if len(batch) >= BATCH_SIZE:
                done, dup = insert_batch(collection, batch)
                inserted, skipped, batch = inserted + done, skipped + dup, []
    if batch:
        done, dup = insert_batch(collection, batch)
        inserted, skipped = inserted + done, skipped + dup
    if inserted + skipped == 0:
        log.warning("%s: no record in %d file(s)", dataset["name"], len(dataset["files"]))
    log.info(
        "%s: %d inserted, %d already loaded for run %s, in %s.%s",
        dataset["name"], inserted, skipped, manifest["run_id"], db.name, dataset["collection"],
    )
    return inserted, skipped


def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    run_dir = Path(args.run_dir)
    try:
        manifest = read_manifest(run_dir)
    except (ValueError, OSError) as e:
        log.error("%s", e)
        return EXIT_BAD_CONFIG
    try:
        client = get_client()
        db_name = os.environ["MONGO_DB"]
    except (KeyError, ValueError) as missing:
        log.error("missing or invalid environment variable %s", missing)
        return EXIT_BAD_CONFIG

    ingested_at = datetime.now(timezone.utc)
    try:
        db = client[db_name]
        for dataset in manifest["datasets"]:
            load_dataset(db, manifest, dataset, run_dir, ingested_at)
    except (PyMongoError, OSError, ValueError, KeyError, TypeError):
        log.exception("script failed")
        return EXIT_FAILURE
    finally:
        client.close()

    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
