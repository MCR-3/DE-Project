"""Land postcodes.io lookups for the postcodes of the landed schools and food businesses, as raw files.

Meant to run in its own container, launched by the staging DAG before staging step 4. Unlike the
other scrapers, the list of what to fetch comes from MongoDB: the postcodes of one landed Ofsted run
(every school) and of one landed FSA run (only the businesses without coordinates). The responses are
saved exactly as received, one folder per run, then loaded by docker_scripts/mongo_loader:

    <output-dir>/<run-id>/
        bulk/batch_0001.json   POST /postcodes {"postcodes": [...]} response (up to 100 postcodes):
                               {"status": 200, "result": [{"query": "SP7 0PF", "result": {...} | null}, ...]}
        terminated.json        GET /terminated_postcodes/<postcode> for every postcode the bulk lookup did
                               not find: {"responses": [{"query": "...", "response": <response as received,
                               404 included>}, ...]} (only `query` is ours, the API answers one postcode at a time)
        manifest.json          what was fetched, read by mongo_loader (written last)

Datasets: `raw_postcodes` (key `query`) and `raw_terminated_postcodes` (key `query`).

The query is the postcode in its standard form (upper case, one space before the inward code), so the
same postcode written differently by two sources is fetched once. Only full postcodes are sent; a
partial one (outward code only, e.g. BA1) cannot be looked up. That normalisation only builds the
request: what is saved is the response.

Cache: a postcode whose latest lookup in `raw_postcodes` (any run) is newer than --max-age-days is not
fetched again; postcodes.io data changes every quarter (e.g. local authority reorganisations), hence
the expiry. Staging uses the latest response of each postcode.

A run built from a sample FSA run covers only part of the source, so it is a sample run too (`sample`
in the manifest is true if any source run read is a sample run).

  * Command-line arguments: filled in by the DAG through `DockerOperator(command=[...])`.
  * Configuration from the environment: MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_USER, MONGO_PASSWORD
    (Airflow connection `project-mongo`), optional MONGO_AUTH_SOURCE (default `admin`), optional
    SCRAPER_USER_AGENT.
  * Exit code: 0 ok, 1 failure, 2 bad config (non-zero makes the Airflow task fail).
"""

import argparse
import json
import logging
import os
import re
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError

log = logging.getLogger("postcode_scraper")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_BAD_CONFIG = 2

API_URL = "https://api.postcodes.io"
DEFAULT_USER_AGENT = "DEProjectPostcodeScraper/1.0 (https://github.com/MCR-3/DE-Project)"
MAX_BULK = 100  # postcodes.io limit for one bulk lookup
MAX_ATTEMPTS = 3
FULL_POSTCODE = re.compile(r"^[A-Z]{1,2}[0-9][A-Z0-9]?[0-9][A-Z]{2}$")  # without the space

# source -> (landed collection, payload field of the postcode)
SOURCES = {
    "ofsted": ("raw_ofsted_schools", "Postcode"),
    "fsa": ("raw_fsa_establishments", "PostCode"),
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Save postcodes.io lookups for the landed postcodes.")
    parser.add_argument("--output-dir", default="/data/landing/postcodes", help="folder that holds one sub-folder per run")
    parser.add_argument("--run-id", help="name of this run's folder (default: UTC timestamp, e.g. 20261120T090000Z)")
    parser.add_argument("--fsa-run", help="landed FSA run to read (default: the latest full run)")
    parser.add_argument("--ofsted-run", help="landed Ofsted run to read (default: the latest full run)")
    parser.add_argument("--max-age-days", type=int, default=90, help="fetch again a postcode looked up longer ago (default 90)")
    parser.add_argument("--pause", type=float, default=0.2, help="seconds between two API calls (default 0.2)")
    return parser.parse_args(argv)


def standard_postcode(value):
    """'sp70pf' -> 'SP7 0PF'; None if it is not a full postcode."""
    if not isinstance(value, str):
        return None
    compact = re.sub(r"\s", "", value).upper()
    if not FULL_POSTCODE.match(compact):
        return None
    return f"{compact[:-3]} {compact[-3:]}"


def mongo_db():
    client = MongoClient(
        host=os.environ["MONGO_HOST"], port=int(os.environ["MONGO_PORT"]),
        username=os.environ["MONGO_USER"], password=os.environ["MONGO_PASSWORD"],
        authSource=os.environ.get("MONGO_AUTH_SOURCE", "admin"), serverSelectionTimeoutMS=10_000,
    )
    return client, client[os.environ["MONGO_DB"]]


def resolve_run(db, source, run_id):
    """(run_id, sample) of the run to read: the given one, or the latest full run; None if there is none."""
    collection = db[SOURCES[source][0]]
    if run_id:
        doc = collection.find_one({"run_id": run_id}, {"sample": 1})
        if doc is None:
            raise ValueError(f"{source} run {run_id!r} is not in {collection.name}")
        return run_id, bool(doc.get("sample"))
    runs = collection.distinct("run_id", {"sample": False})
    return (max(runs), False) if runs else None


def wanted_postcodes(db, source, run_id):
    """Standard postcodes to look up for one source run; returns (postcodes, records read, not full)."""
    collection, field = SOURCES[source]
    query = {"run_id": run_id}
    if source == "fsa":  # only the businesses the FSA gives no coordinates for
        query["payload.geocode.latitude"] = None
    postcodes, records, not_full = set(), 0, 0
    for doc in db[collection].find(query, {"_id": 0, f"payload.{field}": 1}, batch_size=5000):
        records += 1
        raw = doc["payload"].get(field)
        postcode = standard_postcode(raw)
        if postcode:
            postcodes.add(postcode)
        elif raw not in (None, "", "NULL"):
            not_full += 1
    return postcodes, records, not_full


def fresh_postcodes(db, cutoff):
    """Postcodes whose lookup in raw_postcodes is newer than cutoff (ISO strings compare as dates)."""
    return set(db["raw_postcodes"].distinct("source_key", {"fetch_date": {"$gte": cutoff}}))


def request_with_retries(session, method, url, ok=(200,), **kwargs):
    """Send the request; returns the response once its status is in `ok` (retries 429 and 5xx)."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = session.request(method, url, timeout=60, **kwargs)
            if r.status_code in ok:
                return r
            error = f"HTTP {r.status_code}"
            if r.status_code != 429 and r.status_code < 500:
                raise RuntimeError(f"{method} {url}: {error}")
        except requests.RequestException as e:
            error = str(e)
        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(f"{method} {url}: {error}, giving up after {MAX_ATTEMPTS} attempts")
        log.warning("%s %s failed (%s), attempt %d/%d", method, url, error, attempt, MAX_ATTEMPTS)
        time.sleep(2**attempt)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    if args.max_age_days < 0 or args.pause < 0:
        log.error("--max-age-days and --pause must not be negative")
        return EXIT_BAD_CONFIG
    started = datetime.now(timezone.utc).replace(microsecond=0)
    run_id = args.run_id or started.strftime("%Y%m%dT%H%M%SZ")
    if "/" in run_id or "\\" in run_id or run_id in {".", ".."}:
        log.error("--run-id must be a plain folder name, got %r", run_id)
        return EXIT_BAD_CONFIG
    run_dir = Path(args.output_dir) / run_id
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "manifest.json").unlink(missing_ok=True)  # a rerun is only complete once it rewrites it
        shutil.rmtree(run_dir / "bulk", ignore_errors=True)
        (run_dir / "terminated.json").unlink(missing_ok=True)
        (run_dir / "bulk").mkdir()
    except OSError as e:
        log.error("cannot write in %s: %s", run_dir, e)
        return EXIT_BAD_CONFIG
    try:
        client, db = mongo_db()
    except (KeyError, ValueError) as missing:
        log.error("missing or invalid environment variable %s", missing)
        return EXIT_BAD_CONFIG

    try:
        source_runs, sample, wanted = {}, False, set()
        for source, given in (("ofsted", args.ofsted_run), ("fsa", args.fsa_run)):
            try:
                resolved = resolve_run(db, source, given)
            except ValueError as e:
                log.error("%s", e)
                return EXIT_BAD_CONFIG
            if resolved is None:
                log.warning("%s: no full run landed, skipped", source)
                continue
            source_run, source_sample = resolved
            postcodes, records, not_full = wanted_postcodes(db, source, source_run)
            log.info("%s run %s%s: %d records read, %d distinct full postcodes, %d not full (not looked up)",
                     source, source_run, " (sample)" if source_sample else "", records, len(postcodes), not_full)
            source_runs[source] = source_run
            sample = sample or source_sample
            wanted |= postcodes
        if not source_runs:
            log.error("no Ofsted or FSA run to read")
            return EXIT_BAD_CONFIG

        cutoff = (started - timedelta(days=args.max_age_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        to_fetch = sorted(wanted - fresh_postcodes(db, cutoff))
        log.info("%d postcodes wanted, %d already looked up since %s, %d to fetch",
                 len(wanted), len(wanted) - len(to_fetch), cutoff[:10], len(to_fetch))

        datasets, not_found = [], []
        with requests.Session() as session:
            session.headers["User-Agent"] = os.environ.get("SCRAPER_USER_AGENT") or DEFAULT_USER_AGENT
            files = []
            for i in range(0, len(to_fetch), MAX_BULK):
                batch = to_fetch[i:i + MAX_BULK]
                r = request_with_retries(session, "POST", f"{API_URL}/postcodes", json={"postcodes": batch})
                body = r.json()
                name = f"bulk/batch_{i // MAX_BULK + 1:04d}.json"
                (run_dir / name).write_text(json.dumps(body), encoding="utf-8")
                files.append({"path": name, "fetch_date": now_iso(), "published_date": None})
                not_found += [item["query"] for item in body["result"] if item.get("result") is None]
                if (i // MAX_BULK + 1) % 20 == 0:
                    log.info("bulk lookup: %d / %d postcodes", i + len(batch), len(to_fetch))
                time.sleep(args.pause)
            if files:
                datasets.append({"name": "postcodes", "collection": "raw_postcodes", "format": "json",
                                 "records_path": "result", "key_field": "query", "files": files})

            responses = []
            for postcode in not_found:
                r = request_with_retries(session, "GET", f"{API_URL}/terminated_postcodes/{postcode}", ok=(200, 404))
                responses.append({"query": postcode, "response": r.json()})
                time.sleep(args.pause)
            if responses:
                (run_dir / "terminated.json").write_text(json.dumps({"responses": responses}), encoding="utf-8")
                datasets.append({"name": "terminated_postcodes", "collection": "raw_terminated_postcodes",
                                 "format": "json", "records_path": "responses", "key_field": "query",
                                 "files": [{"path": "terminated.json", "fetch_date": now_iso(), "published_date": None}]})
    except (PyMongoError, OSError, RuntimeError, ValueError, KeyError):
        log.exception("script failed, no manifest written")
        return EXIT_FAILURE
    finally:
        client.close()

    terminated = sum(1 for r in responses if r["response"].get("status") == 200)
    manifest = {
        "source": "postcodes",
        "run_id": run_id,
        "sample": sample,
        "scraped_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": {"source_runs": source_runs, "max_age_days": args.max_age_days},
        "counts": {"wanted": len(wanted), "fetched": len(to_fetch), "not_found_in_bulk": len(not_found),
                   "terminated_found": terminated},
        "datasets": datasets,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log.info("done: %d postcodes fetched (%d not found, %d of them terminated postcodes), manifest in %s",
             len(to_fetch), len(not_found), terminated, run_dir)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
