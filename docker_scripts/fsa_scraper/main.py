"""Land the FSA food hygiene ratings (FHRS API) as raw JSON files, one folder per run.

Meant to run in its own containers, launched by the `fsa_landing` DAG (see dags/fsa_landing.py).
Nothing is parsed or cleaned: every API response is written to disk exactly as received.

    <output-dir>/<run-id>/
        authorities.json                     GET /Authorities (all UK local authorities)
        business_types.json                  GET /BusinessTypes
        ratings.json                         GET /Ratings (every possible rating, FHRS and FHIS)
        score_descriptors.json               GET /ScoreDescriptors (the wording of every score value)
        plan.json                            what the run has to fetch (written by `prepare`)
        establishments/la_<id>/page_0001.json   GET /Establishments?localAuthorityId=<id>&pageNumber=1 ...
        establishments/la_<id>/progress.json the pages saved so far, while the authority is being fetched
        establishments/la_<id>/done.json     written after the authority's last page
        manifest.json                        what was fetched, read by docker_scripts/mongo_loader

A run has three steps, one sub-command each (see CONTEXT.md for the words, docs/adr/0002 for why):

  * `prepare`: fetches the reference files, chooses the authorities (every English one for a full
    run, those of `--la-ids` for a sample run), writes `plan.json`, and fills the run's Authority
    queue in Redis, largest authority first, leaving out the ones that already have a `done.json`.
    Rerunning it rebuilds the queue from the files, so a retry never fetches a finished authority.
  * `fetch`: one Fetcher. It takes authorities from the queue, one at a time, under a lease it keeps
    renewing, and saves all their pages. It exits once the queue is empty and no authority is still
    leased by another Fetcher (whose lease could expire and come back to the queue). Run several in
    parallel for a faster run.
  * `finalize`: checks in the files that every authority of the plan has its `done.json`, then
    writes `manifest.json` and deletes the run's Redis keys. Missing authorities: exit 1, no manifest.

`manifest.json` is written last: a run folder without it is an unfinished run and is not loaded.
Each file it lists carries two dates: `fetch_date`, the UTC moment this run received the response,
and `published_date`, the `LastPublishedDate` of the establishments' local authority (None for the
reference files, the FSA gives them none). The API's own `meta.extractDate` is only the time of the
request, so it is left in the raw files and not used.

Resuming: an interrupted authority (crashed Fetcher, expired lease) restarts from its last saved
page, unless the authority has published new data since its first page, in which case it is fetched
again from page 1 (pages of two versions would miss or repeat establishments). An authority that
fails MAX_ATTEMPTS times is set aside, the others go on, and its Fetcher exits 1; when Airflow
retries that Fetcher, it puts the authorities set aside back in the queue first.

  * Command-line arguments: filled in by the DAG through `DockerOperator(command=[...])`.
  * Configuration from the environment: REDIS_HOST, REDIS_PORT, REDIS_PASSWORD (the DAG injects
    them from the Airflow connection `project-redis`), optional REDIS_DB (default 0), optional
    `SCRAPER_USER_AGENT` to override the User-Agent.
  * Politeness (FSA best practices): page size of 200 at most, at most `--max-rps` requests per
    second shared by every Fetcher of every run, and a 429 pauses all of them for its Retry-After.
  * Exit code: 0 ok, 1 failure, 2 bad config (non-zero makes the Airflow task fail).
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

import authority_queue
from authority_queue import AuthorityQueue, RateLimiter

log = logging.getLogger("fsa_scraper")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_BAD_CONFIG = 2

API_URL = "https://api.ratings.food.gov.uk"
DEFAULT_USER_AGENT = "DEProjectFsaScraper/1.0 (https://github.com/MCR-3/DE-Project)"
# The default sample: the Isles of Scilly (tiny), Bath and North East Somerset (medium), Leeds (large).
DEFAULT_LA_IDS = "321,326,397"
DEFAULT_MAX_RPS = 4.0
MAX_PAGE_SIZE = 200  # the FSA rate-limits larger pages (HTTP 429)
NON_ENGLISH_REGIONS = {"Scotland", "Wales", "Northern Ireland"}
MAX_HTTP_ATTEMPTS = 5
IDLE_WAIT_SECONDS = 5  # how often a Fetcher with nothing to do checks for expired leases
RENEW_EVERY_SECONDS = authority_queue.LEASE_MS / 1000 / 4


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Save raw FSA FHRS API responses, one folder per run.")
    commands = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--output-dir", default="/data/landing/fsa", help="folder that holds one sub-folder per run")
    common.add_argument("--run-id", required=True, help="name of this run's folder, the same for every step")

    limited = argparse.ArgumentParser(add_help=False)
    limited.add_argument(
        "--max-rps", type=float, default=DEFAULT_MAX_RPS,
        help=f"requests per second to the FSA, shared by every Fetcher (default: {DEFAULT_MAX_RPS})",
    )

    prepare = commands.add_parser("prepare", parents=[common, limited], help="fetch the reference files, fill the queue")
    prepare.add_argument(
        "--sample",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="--sample (default): a sample run, only the authorities of --la-ids; "
        "--no-sample: a full run, every English authority (--la-ids is ignored)",
    )
    prepare.add_argument(
        "--la-ids",
        default=DEFAULT_LA_IDS,
        help=f"comma-separated FSA LocalAuthorityId values of a sample run (default: {DEFAULT_LA_IDS})",
    )
    prepare.add_argument("--page-size", type=int, default=MAX_PAGE_SIZE, help=f"establishments per page, 1-{MAX_PAGE_SIZE}")

    fetch = commands.add_parser("fetch", parents=[common, limited], help="run one Fetcher until the queue is done")
    fetch.add_argument("--fetcher", default="1", help="name of this Fetcher in the logs")

    commands.add_parser("finalize", parents=[common], help="check every authority is done, write the manifest")
    return parser.parse_args(argv)


def write_atomic(path, content):
    """Write bytes so that a reader never sees a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)


def write_json(path, value):
    write_atomic(path, json.dumps(value, indent=2).encode("utf-8"))


def utc_now():
    """The current UTC time as an ISO 8601 string, e.g. '2026-11-20T13:14:14Z'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def file_entry(path, fetch_date, published_date):
    """One file of the manifest, with the dates the loader copies into each of its records."""
    return {"path": path, "fetch_date": fetch_date, "published_date": published_date}


def redis_from_env():
    """A Redis client from REDIS_* environment variables (raises KeyError/ValueError if unset or invalid)."""
    return authority_queue.connect(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ.get("REDIS_PORT", "6379")),
        password=os.environ.get("REDIS_PASSWORD"),
        db=int(os.environ.get("REDIS_DB", "0")),
    )


def retry_after_seconds(response, attempt):
    try:
        return float(response.headers["Retry-After"])
    except (KeyError, ValueError):
        return float(2**attempt)


class FsaClient:
    """GETs on the FHRS API, paced by the shared RateLimiter."""

    def __init__(self, session, limiter):
        self.session = session
        self.limiter = limiter

    def _wait_for_slot(self):
        while (wait_ms := self.limiter.wait_ms()) > 0:
            time.sleep(wait_ms / 1000)

    def get(self, path, params=None):
        """GET an API path and return (raw bytes, parsed JSON). Retries 429, 5xx and network errors."""
        for attempt in range(1, MAX_HTTP_ATTEMPTS + 1):
            self._wait_for_slot()
            try:
                r = self.session.get(API_URL + path, params=params, timeout=60)
            except requests.RequestException as e:
                error, delay = str(e), float(2**attempt)
            else:
                if r.status_code == 200:
                    return r.content, r.json()
                error = f"HTTP {r.status_code}"
                if r.status_code != 429 and r.status_code < 500:
                    r.raise_for_status()
                delay = retry_after_seconds(r, attempt)
                if r.status_code == 429:
                    # Every Fetcher pauses, not only this one: the limit is on our address.
                    self.limiter.cool_down(delay)
                    log.warning("%s: HTTP 429, every Fetcher pauses %.0fs (attempt %d/%d)",
                                path, delay, attempt, MAX_HTTP_ATTEMPTS)
                    continue
            log.warning("%s failed (%s), attempt %d/%d, waiting %.0fs", path, error, attempt, MAX_HTTP_ATTEMPTS, delay)
            time.sleep(delay)
        raise RuntimeError(f"too many retries for {path}")


def open_client(session, redis_client, max_rps):
    session.headers.update(
        {
            "User-Agent": os.environ.get("SCRAPER_USER_AGENT") or DEFAULT_USER_AGENT,
            "x-api-version": "2",  # without it the FSA API returns no data
            "Accept": "application/json",
        }
    )
    return FsaClient(session, RateLimiter(redis_client, max_rps))


def select_authorities(authorities, sample, la_ids):
    """The authorities to fetch: every English one for a full run, those of `la_ids` for a sample run.

    Raises ValueError for unknown or non-English ones.
    """
    english = {a["LocalAuthorityId"]: a for a in authorities if a.get("RegionName") not in NON_ENGLISH_REGIONS}
    known = {a["LocalAuthorityId"]: a for a in authorities}
    if not sample:
        return [english[i] for i in sorted(english)]
    selected = []
    for raw in la_ids.split(","):
        try:
            la_id = int(raw)
        except ValueError:
            hint = " (for every English authority, make a full run with --no-sample)" if raw.strip().lower() == "all" else ""
            raise ValueError(f"not a LocalAuthorityId: {raw!r}{hint}") from None
        if la_id not in known:
            raise ValueError(f"unknown LocalAuthorityId {la_id}")
        if la_id not in english:
            raise ValueError(f"LocalAuthorityId {la_id} ({known[la_id]['Name']}) is not in England")
        if english[la_id] not in selected:
            selected.append(english[la_id])
    return selected


def land_reference(client, run_dir, path, file_name, name, collection, records_path, key_field):
    raw, body = client.get(path)
    fetched = utc_now()
    write_atomic(run_dir / file_name, raw)
    log.info("%s: %d records saved in %s", name, len(body[records_path]), file_name)
    return {
        "name": name,
        "collection": collection,
        "format": "json",
        "records_path": records_path,
        "key_field": key_field,
        "files": [file_entry(file_name, fetched, None)],
    }


def authority_dir(run_dir, la_id):
    return run_dir / f"establishments/la_{la_id}"


def valid_files(files, run_dir):
    return (
        isinstance(files, list)
        and all(isinstance(f, dict) and {"path", "fetch_date", "published_date"} <= f.keys() for f in files)
        and all((run_dir / f["path"]).is_file() for f in files)
    )


def finished_files(run_dir, la_id, page_size):
    """The file entries of an authority already finished in this run, None if it is not."""
    done_path = authority_dir(run_dir, la_id) / "done.json"
    if not done_path.is_file():
        return None
    done = json.loads(done_path.read_text(encoding="utf-8"))
    if done.get("page_size") == page_size and valid_files(done.get("files"), run_dir):
        return done["files"]
    return None


# ---------------------------------------------------------------------------------------- prepare

def prepare(args, redis_client, run_dir):
    if not 1 <= args.page_size <= MAX_PAGE_SIZE:
        log.error("--page-size must be between 1 and %d", MAX_PAGE_SIZE)
        return EXIT_BAD_CONFIG
    started = utc_now()
    with requests.Session() as session:
        client = open_client(session, redis_client, args.max_rps)
        reference = [
            land_reference(client, run_dir, "/Authorities", "authorities.json",
                           "authorities", "raw_fsa_authorities", "authorities", "LocalAuthorityId"),
            land_reference(client, run_dir, "/BusinessTypes", "business_types.json",
                           "business_types", "raw_fsa_business_types", "businessTypes", "BusinessTypeId"),
            land_reference(client, run_dir, "/Ratings", "ratings.json",
                           "ratings", "raw_fsa_ratings", "ratings", "ratingId"),
            land_reference(client, run_dir, "/ScoreDescriptors", "score_descriptors.json",
                           "score_descriptors", "raw_fsa_score_descriptors", "scoreDescriptors", "Id"),
        ]
    authority_list = json.loads((run_dir / "authorities.json").read_text(encoding="utf-8"))["authorities"]
    try:
        selected = select_authorities(authority_list, args.sample, args.la_ids)
    except ValueError as e:
        log.error("bad --la-ids: %s", e)
        return EXIT_BAD_CONFIG
    if not selected:
        log.error("no authority selected")
        return EXIT_BAD_CONFIG

    # Largest first, so that the run does not end with one Fetcher on a big authority and the others idle.
    selected.sort(key=lambda a: a.get("EstablishmentCount") or 0, reverse=True)
    authorities = [
        {"id": a["LocalAuthorityId"], "name": a["Name"], "establishment_count": a.get("EstablishmentCount")}
        for a in selected
    ]
    write_json(run_dir / "plan.json", {
        "run_id": args.run_id,
        "sample": args.sample,
        "started_at": started,
        "page_size": args.page_size,
        "reference_datasets": reference,
        "authorities": authorities,
    })
    to_fetch = [a["id"] for a in authorities if finished_files(run_dir, a["id"], args.page_size) is None]
    AuthorityQueue(redis_client, args.run_id).reset(to_fetch)
    log.info("%s run: %d authorities, %d already finished, %d queued (page size %d)",
             "sample" if args.sample else "full", len(authorities), len(authorities) - len(to_fetch),
             len(to_fetch), args.page_size)
    return EXIT_OK


# ------------------------------------------------------------------------------------------ fetch

class LeaseLost(Exception):
    """Our lease on an authority expired and another Fetcher may have it: stop writing its files."""


class Lease:
    """Keeps our lease on one authority alive from a background thread, whatever the Fetcher waits on
    (rate limit, cooldown, back-off, a slow response)."""

    def __init__(self, queue, la_id):
        self.queue, self.la_id = queue, la_id
        self._stop = threading.Event()
        self._lost = False
        self._thread = threading.Thread(target=self._renew, daemon=True)

    def _renew(self):
        while not self._stop.wait(RENEW_EVERY_SECONDS):
            try:
                if not self.queue.renew(self.la_id):
                    self._lost = True
                    return
            except Exception as e:  # Redis unreachable for a moment: try again at the next beat
                log.warning("authority %d: could not renew the lease (%s)", self.la_id, e)

    def check(self):
        if self._lost:
            raise LeaseLost(f"lease on authority {self.la_id} lost")

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()


def land_authority(client, run_dir, la_id, page_size, lease):
    """Save every page of one authority, resuming from the pages an interrupted attempt saved if the
    authority has not published since. Ends by writing its done.json."""
    if finished_files(run_dir, la_id, page_size) is not None:
        log.info("authority %d: already finished in this run", la_id)
        return
    _, authority = client.get(f"/Authorities/{la_id}")
    name, published = authority["Name"], authority.get("LastPublishedDate")
    la_dir = authority_dir(run_dir, la_id)
    progress_path = la_dir / "progress.json"

    files, total_pages = [], 1
    if progress_path.is_file():
        progress = json.loads(progress_path.read_text(encoding="utf-8"))
        if (progress.get("page_size") == page_size and progress.get("published_date") == published
                and valid_files(progress.get("files"), run_dir)):
            files, total_pages = progress["files"], progress["total_pages"]
            log.info("%s (%d): resuming after page %d/%d", name, la_id, len(files), total_pages)
        else:
            log.info("%s (%d): published again since the interrupted attempt (or other page size), "
                     "fetching it again from page 1", name, la_id)

    page = len(files) + 1
    while page <= total_pages:
        params = {"localAuthorityId": la_id, "pageNumber": page, "pageSize": page_size}
        raw, body = client.get("/Establishments", params)
        fetched = utc_now()
        total_pages = body["meta"]["totalPages"]
        lease.check()
        file_name = f"establishments/la_{la_id}/page_{page:04d}.json"
        write_atomic(run_dir / file_name, raw)
        files.append(file_entry(file_name, fetched, published))
        write_json(progress_path, {"page_size": page_size, "published_date": published,
                                   "total_pages": total_pages, "files": files})
        log.info("%s (%d): page %d/%d, %d of %d establishments", name, la_id, page, max(total_pages, 1),
                 len(body["establishments"]), body["meta"]["totalCount"])
        page += 1
    lease.check()
    write_json(la_dir / "done.json", {"page_size": page_size, "saved_at": utc_now(),
                                      "published_date": published, "files": files})
    progress_path.unlink(missing_ok=True)


def fetch(args, redis_client, run_dir):
    plan_path = run_dir / "plan.json"
    if not plan_path.is_file():
        log.error("%s not found: run `prepare` first", plan_path)
        return EXIT_BAD_CONFIG
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    queue = AuthorityQueue(redis_client, args.run_id)
    requeued = queue.requeue_failed()
    if requeued:
        log.info("fetcher %s: %d authorities set aside by an earlier attempt are queued again", args.fetcher, requeued)

    done = given_up = 0
    with requests.Session() as session:
        client = open_client(session, redis_client, args.max_rps)
        while True:
            la_id, leased = queue.claim()
            if la_id is None:
                if leased == 0:
                    break
                log.debug("queue empty, %d authorities still leased: waiting", leased)
                time.sleep(IDLE_WAIT_SECONDS)
                continue
            try:
                with Lease(queue, la_id) as lease:
                    land_authority(client, run_dir, la_id, plan["page_size"], lease)
            except LeaseLost as e:
                log.warning("fetcher %s: %s, leaving it to the Fetcher that took it back", args.fetcher, e)
                continue
            except Exception as e:
                log.exception("fetcher %s: authority %d failed", args.fetcher, la_id)
                attempts = queue.fail(la_id, f"{type(e).__name__}: {e}")
                if attempts is not None and attempts >= authority_queue.MAX_ATTEMPTS:
                    log.error("authority %d failed %d times, set aside", la_id, attempts)
                    given_up += 1
                continue
            if not queue.release(la_id):
                log.warning("authority %d: finished, but its lease had already been taken back", la_id)
            done += 1
    log.info("fetcher %s: queue done, %d authorities fetched by this Fetcher, %d set aside",
             args.fetcher, done, given_up)
    return EXIT_FAILURE if given_up else EXIT_OK


# --------------------------------------------------------------------------------------- finalize

def finalize(args, run_dir):
    plan_path = run_dir / "plan.json"
    if not plan_path.is_file():
        log.error("%s not found: run `prepare` first", plan_path)
        return EXIT_BAD_CONFIG
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    queue = None
    try:
        queue = AuthorityQueue(redis_from_env(), args.run_id)
    except (KeyError, ValueError) as e:
        log.warning("no Redis configured (%s): the run's keys will expire on their own", e)

    files, missing = [], []
    for authority in plan["authorities"]:
        authority_files = finished_files(run_dir, authority["id"], plan["page_size"])
        if authority_files is None:
            missing.append(authority)
        else:
            files += authority_files
    if missing:
        errors = {}
        if queue is not None:
            try:
                errors = queue.failures()
            except Exception as e:
                log.warning("could not read the failures from Redis: %s", e)
        for authority in missing:
            log.error("%s (%d) not finished%s", authority["name"], authority["id"],
                      f": {errors[authority['id']]}" if authority["id"] in errors else "")
        log.error("%d of %d authorities not finished, no manifest written", len(missing), len(plan["authorities"]))
        return EXIT_FAILURE

    manifest = {
        "source": "fsa",
        "run_id": plan["run_id"],
        "sample": plan["sample"],
        "scraped_at": plan["started_at"],
        "params": {"la_ids": [a["id"] for a in plan["authorities"]], "page_size": plan["page_size"]},
        "datasets": plan["reference_datasets"] + [
            {
                "name": "establishments",
                "collection": "raw_fsa_establishments",
                "format": "json",
                "records_path": "establishments",
                "key_field": "FHRSID",
                "files": files,
            },
        ],
    }
    write_json(run_dir / "manifest.json", manifest)
    if queue is not None:
        try:
            queue.delete()
        except Exception as e:
            log.warning("could not delete the run's Redis keys (%s): they will expire on their own", e)
    log.info("done: %d authorities, %d establishment pages, manifest written in %s",
             len(plan["authorities"]), len(files), run_dir)
    return EXIT_OK


# ------------------------------------------------------------------------------------------- main

def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    if "/" in args.run_id or "\\" in args.run_id or args.run_id in {"", ".", ".."}:
        log.error("--run-id must be a plain folder name, got %r", args.run_id)
        return EXIT_BAD_CONFIG
    if getattr(args, "max_rps", 1) <= 0:
        log.error("--max-rps must be positive")
        return EXIT_BAD_CONFIG
    run_dir = Path(args.output_dir) / args.run_id
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        if args.command == "prepare":
            (run_dir / "manifest.json").unlink(missing_ok=True)  # a rerun is only complete once it rewrites it
    except OSError as e:
        log.error("cannot write in %s: %s", run_dir, e)
        return EXIT_BAD_CONFIG

    if args.command == "finalize":
        try:
            return finalize(args, run_dir)
        except Exception:
            log.exception("finalize failed, no manifest written")
            return EXIT_FAILURE

    try:
        redis_client = redis_from_env()
    except (KeyError, ValueError) as e:
        log.error("missing or invalid environment variable %s", e)
        return EXIT_BAD_CONFIG
    try:
        redis_client.ping()
        if args.command == "prepare":
            return prepare(args, redis_client, run_dir)
        return fetch(args, redis_client, run_dir)
    except Exception:
        log.exception("%s failed", args.command)
        return EXIT_FAILURE
    finally:
        redis_client.close()


if __name__ == "__main__":
    sys.exit(main())
