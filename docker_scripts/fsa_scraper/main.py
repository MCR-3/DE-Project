"""Land the FSA food hygiene ratings (FHRS API) as raw JSON files, one folder per run.

Meant to run in its own container, launched by the `fsa_landing` DAG (see dags/fsa_landing.py).
Nothing is parsed or cleaned: every API response is written to disk exactly as received.

    <output-dir>/<run-id>/
        authorities.json                     GET /Authorities (all UK local authorities)
        business_types.json                  GET /BusinessTypes
        ratings.json                         GET /Ratings (every possible rating, FHRS and FHIS)
        score_descriptors.json               GET /ScoreDescriptors (the wording of every score value)
        establishments/la_<id>/page_0001.json   GET /Establishments?localAuthorityId=<id>&pageNumber=1 ...
        manifest.json                        what was fetched, read by docker_scripts/mongo_loader

`manifest.json` is written last: a run folder without it is an unfinished run and is not loaded.

Only English authorities are accepted (the project is limited to England): `--la-ids` is a
comma-separated list of FSA `LocalAuthorityId` values, or `all` for every English authority.

  * Command-line arguments: filled in by the DAG through `DockerOperator(command=[...])`.
  * Configuration from the environment: `SCRAPER_USER_AGENT` (optional) overrides the User-Agent.
  * Politeness (FSA best practices): page size of 200 at most, a pause between requests
    (`--sleep`), retries with back-off on 429 / 5xx.
  * Exit code: 0 ok, 1 failure, 2 bad config (non-zero makes the Airflow task fail).
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger("fsa_scraper")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_BAD_CONFIG = 2

API_URL = "https://api.ratings.food.gov.uk"
DEFAULT_USER_AGENT = "DEProjectFsaScraper/1.0 (https://github.com/MCR-3/DE-Project)"
# A small England extract: the Isles of Scilly (tiny), Bath and North East Somerset (medium), Leeds (large).
DEFAULT_LA_IDS = "321,326,397"
MAX_PAGE_SIZE = 200  # the FSA rate-limits larger pages (HTTP 429)
NON_ENGLISH_REGIONS = {"Scotland", "Wales", "Northern Ireland"}
MAX_ATTEMPTS = 5


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Save raw FSA FHRS API responses, one folder per run.")
    parser.add_argument("--output-dir", default="/data/landing/fsa", help="folder that holds one sub-folder per run")
    parser.add_argument(
        "--run-id",
        help="name of this run's folder (default: UTC timestamp, e.g. 20261120T090000Z)",
    )
    parser.add_argument(
        "--la-ids",
        default=DEFAULT_LA_IDS,
        help="comma-separated FSA LocalAuthorityId values, or 'all' for every English authority "
        f"(default: {DEFAULT_LA_IDS})",
    )
    parser.add_argument("--page-size", type=int, default=MAX_PAGE_SIZE, help=f"establishments per page, 1-{MAX_PAGE_SIZE}")
    parser.add_argument("--sleep", type=float, default=0.5, help="seconds to wait between two API calls")
    return parser.parse_args(argv)


def write_atomic(path, content):
    """Write bytes so that a reader never sees a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)


class FsaClient:
    def __init__(self, session, pause):
        self.session = session
        self.pause = pause
        self._last_call = 0.0

    def get(self, path, params=None):
        """GET an API path and return (raw bytes, parsed JSON). Retries 429, 5xx and network errors."""
        for attempt in range(1, MAX_ATTEMPTS + 1):
            wait = self.pause - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()
            try:
                r = self.session.get(API_URL + path, params=params, timeout=60)
            except requests.RequestException as e:
                error = str(e)
                delay = 2**attempt
            else:
                if r.status_code == 200:
                    return r.content, r.json()
                error = f"HTTP {r.status_code}"
                if r.status_code != 429 and r.status_code < 500:
                    r.raise_for_status()
                delay = float(r.headers.get("Retry-After", 2**attempt))
            log.warning("%s failed (%s), attempt %d/%d, waiting %.0fs", path, error, attempt, MAX_ATTEMPTS, delay)
            time.sleep(delay)
        raise RuntimeError(f"too many retries for {path}")


def extract_date(body):
    """Date part of meta.extractDate (e.g. '2026-11-20'), None if the response has none."""
    value = (body.get("meta") or {}).get("extractDate")
    return value[:10] if value else None


def select_authorities(authorities, la_ids):
    """The list of LocalAuthorityId to fetch. Raises ValueError for unknown or non-English ones."""
    english = {a["LocalAuthorityId"]: a for a in authorities if a.get("RegionName") not in NON_ENGLISH_REGIONS}
    known = {a["LocalAuthorityId"]: a for a in authorities}
    if la_ids.strip().lower() == "all":
        return [english[i] for i in sorted(english)]
    selected = []
    for raw in la_ids.split(","):
        try:
            la_id = int(raw)
        except ValueError:
            raise ValueError(f"not a LocalAuthorityId: {raw!r}") from None
        if la_id not in known:
            raise ValueError(f"unknown LocalAuthorityId {la_id}")
        if la_id not in english:
            raise ValueError(f"LocalAuthorityId {la_id} ({known[la_id]['Name']}) is not in England")
        selected.append(english[la_id])
    return selected


def land_reference(client, run_dir, path, file_name, name, collection, records_path, key_field):
    raw, body = client.get(path)
    write_atomic(run_dir / file_name, raw)
    log.info("%s: %d records saved in %s", name, len(body[records_path]), file_name)
    return {
        "name": name,
        "collection": collection,
        "format": "json",
        "records_path": records_path,
        "key_field": key_field,
        "extract_date": extract_date(body),
        "files": [file_name],
    }


def land_establishments(client, run_dir, authority, page_size):
    """Save every page of one authority. Returns (relative file names, extract date)."""
    la_id, name = authority["LocalAuthorityId"], authority["Name"]
    files, extracted, page, total_pages = [], None, 1, 1
    while page <= total_pages:
        params = {"localAuthorityId": la_id, "pageNumber": page, "pageSize": page_size}
        raw, body = client.get("/Establishments", params)
        total_pages = body["meta"]["totalPages"]
        file_name = f"establishments/la_{la_id}/page_{page:04d}.json"
        write_atomic(run_dir / file_name, raw)
        files.append(file_name)
        extracted = extracted or extract_date(body)
        log.info("%s (%d): page %d/%d, %d of %d establishments", name, la_id, page, max(total_pages, 1),
                 len(body["establishments"]), body["meta"]["totalCount"])
        page += 1
    return files, extracted


def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    if not 1 <= args.page_size <= MAX_PAGE_SIZE:
        log.error("--page-size must be between 1 and %d", MAX_PAGE_SIZE)
        return EXIT_BAD_CONFIG
    if args.sleep < 0:
        log.error("--sleep must not be negative")
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
    except OSError as e:
        log.error("cannot write in %s: %s", run_dir, e)
        return EXIT_BAD_CONFIG

    try:
        with requests.Session() as session:
            session.headers.update(
                {
                    "User-Agent": os.environ.get("SCRAPER_USER_AGENT") or DEFAULT_USER_AGENT,
                    "x-api-version": "2",  # without it the FSA API returns no data
                    "Accept": "application/json",
                }
            )
            client = FsaClient(session, args.sleep)

            authorities = land_reference(
                client, run_dir, "/Authorities", "authorities.json",
                "authorities", "raw_fsa_authorities", "authorities", "LocalAuthorityId",
            )
            business_types = land_reference(
                client, run_dir, "/BusinessTypes", "business_types.json",
                "business_types", "raw_fsa_business_types", "businessTypes", "BusinessTypeId",
            )
            ratings = land_reference(
                client, run_dir, "/Ratings", "ratings.json",
                "ratings", "raw_fsa_ratings", "ratings", "ratingId",
            )
            score_descriptors = land_reference(
                client, run_dir, "/ScoreDescriptors", "score_descriptors.json",
                "score_descriptors", "raw_fsa_score_descriptors", "scoreDescriptors", "Id",
            )
            authority_list = json.loads((run_dir / "authorities.json").read_text(encoding="utf-8"))["authorities"]
            try:
                selected = select_authorities(authority_list, args.la_ids)
            except ValueError as e:
                log.error("bad --la-ids: %s", e)
                return EXIT_BAD_CONFIG
            if not selected:
                log.error("no authority selected")
                return EXIT_BAD_CONFIG

            log.info("fetching %d authorities, page size %d", len(selected), args.page_size)
            files, extracted = [], None
            for authority in selected:
                authority_files, authority_extract = land_establishments(client, run_dir, authority, args.page_size)
                files += authority_files
                extracted = extracted or authority_extract
    except Exception:
        log.exception("script failed, no manifest written")
        return EXIT_FAILURE

    manifest = {
        "source": "fsa",
        "run_id": run_id,
        "scraped_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": {"la_ids": [a["LocalAuthorityId"] for a in selected], "page_size": args.page_size},
        "datasets": [
            authorities,
            business_types,
            ratings,
            score_descriptors,
            {
                "name": "establishments",
                "collection": "raw_fsa_establishments",
                "format": "json",
                "records_path": "establishments",
                "key_field": "FHRSID",
                "extract_date": extracted,
                "files": files,
            },
        ],
    }
    write_atomic(run_dir / "manifest.json", json.dumps(manifest, indent=2).encode("utf-8"))
    log.info("done: %d establishment pages saved in %s", len(files), run_dir)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
