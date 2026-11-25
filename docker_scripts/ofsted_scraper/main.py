"""Land Ofsted's monthly "state-funded schools, latest inspections" CSV as a raw file, one folder per run.

Meant to run in its own container, launched by the `ofsted_landing` DAG (see dags/ofsted_landing.py).
The file is saved exactly as published (no rows dropped, nothing converted):

    <output-dir>/<run-id>/
        schools.csv        the CSV of the newest monthly release (about 22,000 schools, 76 columns)
        manifest.json      what was fetched, read by docker_scripts/mongo_loader (written last)

The gov.uk publication page has no stable download link (each release has a new asset id and the
file name wording changes over the years), so the script reads the page, keeps the links to
`*latest_inspections*.csv` files and takes the one with the newest "as at" date in its name.
`--url` skips that search and downloads one given file (e.g. an older month).

The manifest gives the file two dates (see CONTEXT.md): `fetch_date`, the UTC moment this run
finished downloading it, and `published_date`, the "as at" date in its name (None if it has none).

  * Command-line arguments: filled in by the DAG through `DockerOperator(command=[...])`.
  * Configuration from the environment: `SCRAPER_USER_AGENT` (optional) overrides the User-Agent.
  * Exit code: 0 ok, 1 failure, 2 bad config (non-zero makes the Airflow task fail).
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("ofsted_scraper")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_BAD_CONFIG = 2

PAGE_URL = (
    "https://www.gov.uk/government/statistical-data-sets/"
    "monthly-management-information-ofsteds-school-inspections-outcomes"
)
ASSET_PREFIX = "https://assets.publishing.service.gov.uk/media/"
DEFAULT_USER_AGENT = "DEProjectOfstedScraper/1.0 (https://github.com/MCR-3/DE-Project)"
KEY_FIELD = "URN"
MAX_ATTEMPTS = 3
MONTHS = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Save Ofsted's latest 'latest inspections' CSV, one folder per run.")
    parser.add_argument("--output-dir", default="/data/landing/ofsted", help="folder that holds one sub-folder per run")
    parser.add_argument(
        "--run-id",
        help="name of this run's folder (default: UTC timestamp, e.g. 20261120T090000Z)",
    )
    parser.add_argument("--url", help="download this CSV instead of looking for the newest one on the gov.uk page")
    parser.add_argument("--page-url", default=PAGE_URL, help="gov.uk page that lists the monthly releases")
    return parser.parse_args(argv)


def as_at_date(url):
    """Date in the file name ('..._as_at_31_October_2026.csv', '..._at_30_Sep_2026.csv'), None if absent."""
    match = re.search(r"(\d{1,2})_([A-Za-z]+)_(\d{4})\.csv$", unquote(url))
    if not match or match.group(2)[:3].lower() not in MONTHS:
        return None
    try:
        return date(int(match.group(3)), MONTHS[match.group(2)[:3].lower()], int(match.group(1)))
    except ValueError:
        return None


def find_latest_csv(html, base_url):
    """URL of the newest 'latest inspections' CSV linked in the page, None if there is none."""
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        url = urljoin(base_url, a["href"])
        name = url.rsplit("/", 1)[-1].lower()
        if url.startswith(ASSET_PREFIX) and name.endswith(".csv") and "latest_inspections" in name and url not in urls:
            urls.append(url)
    if not urls:
        return None
    # Newest by the date in the name; ties and unparsable names keep the page order (newest first).
    return max(urls, key=lambda u: as_at_date(u) or date.min)


def get_with_retries(session, url, **kwargs):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = session.get(url, timeout=120, **kwargs)
            if r.status_code == 200:
                return r
            error = f"HTTP {r.status_code}"
            r.close()
            if r.status_code != 429 and r.status_code < 500:
                raise RuntimeError(f"{url}: {error}")
        except requests.RequestException as e:
            error = str(e)
        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(f"{url}: {error}, giving up after {MAX_ATTEMPTS} attempts")
        log.warning("%s failed (%s), attempt %d/%d", url, error, attempt, MAX_ATTEMPTS)
        time.sleep(2**attempt)


def download(session, url, target):
    """Stream url into target (through a temporary file); returns the size in bytes."""
    tmp = target.with_name(target.name + ".tmp")
    r = get_with_retries(session, url, stream=True)
    size = 0
    with r, tmp.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 20):
            f.write(chunk)
            size += len(chunk)
    os.replace(tmp, target)
    return size


def inspect_csv(path):
    """(encoding, row count) of the saved CSV. Raises ValueError if it does not look like the Ofsted file."""
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            with path.open(encoding=encoding, newline="") as f:
                reader = csv.reader(f)
                header = next(reader, [])
                if KEY_FIELD not in header:
                    raise ValueError(f"column {KEY_FIELD!r} not found in the header of {path.name}: not the expected file")
                return encoding, sum(1 for _ in reader)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"{path.name} is neither UTF-8 nor cp1252")


def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    if args.url and not args.url.startswith(("http://", "https://")):
        log.error("--url must be an http(s) URL, got %r", args.url)
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
            session.headers["User-Agent"] = os.environ.get("SCRAPER_USER_AGENT") or DEFAULT_USER_AGENT
            url = args.url
            if not url:
                log.info("looking for the newest CSV on %s", args.page_url)
                url = find_latest_csv(get_with_retries(session, args.page_url).text, args.page_url)
                if not url:
                    log.error("no 'latest_inspections' CSV link found on the page: has its layout changed?")
                    return EXIT_FAILURE
            log.info("downloading %s", url)
            target = run_dir / "schools.csv"
            size = download(session, url, target)
            fetched = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        encoding, rows = inspect_csv(target)
    except Exception:
        log.exception("script failed, no manifest written")
        return EXIT_FAILURE
    if rows == 0:
        log.error("%s has a header but no row, no manifest written", target)
        return EXIT_FAILURE

    as_at = as_at_date(url)
    manifest = {
        "source": "ofsted",
        "run_id": run_id,
        "sample": False,  # the CSV is always landed whole: every Ofsted run is a full run
        "scraped_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params": {"url": url},
        "datasets": [
            {
                "name": "schools",
                "collection": "raw_ofsted_schools",
                "format": "csv",
                "encoding": encoding,
                "key_field": KEY_FIELD,
                "source_url": url,
                "files": [
                    {
                        "path": "schools.csv",
                        "fetch_date": fetched,
                        "published_date": as_at.isoformat() if as_at else None,
                    }
                ],
            }
        ],
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log.info("done: %d schools (%.1f MB, as at %s) saved in %s", rows, size / 1e6, as_at, run_dir)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
