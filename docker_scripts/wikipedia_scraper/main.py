"""Fetch random Wikipedia articles and store them in one timestamped JSON file.

Meant to run in its own container, launched by the DAG 3 DockerOperator (see dags/dag_3.py).
One run produces one file, `wikipedia_<UTC timestamp>.json`:

    {"fetched_at": "2026-11-02T16:11:47Z",
     "pages": [{"title": "...", "url": "https://en.wikipedia.org/wiki/...", "text": "..."}, ...]}

`text` is the article's plain text on a single line, with no line breaks (infobox and table
contents included). Pages are appended to
the file as they are fetched; while the run is going the file is named `*.json.part`. It is
renamed to `*.json` when all the requested articles are saved, or to `*.partial.json` (still
valid JSON) if the run stops early.

  * Command-line arguments: filled in by the DAG through `DockerOperator(command=[...])`.
  * Configuration from the environment: `WIKI_USER_AGENT` (optional) overrides the User-Agent.
  * Output: the file is written under `--output-dir`, which the DAG bind-mounts from the host's data/ folder.
  * Exit code: 0 ok, 1 failure, 2 bad config (non-zero makes the Airflow task fail).
"""

import argparse
import json
import logging
import os
import re
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("wikipedia_scraper")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_BAD_CONFIG = 2

API_URL = "https://en.wikipedia.org/w/api.php"
PAGE_URL = "https://en.wikipedia.org/wiki/"
DEFAULT_USER_AGENT = "DEProjectWikiScraper/1.0 (https://github.com/MCR-3/DE-Project)"

TITLES_PER_BATCH = 50
MAX_TITLES_PER_PAGE = 50  # give up after drawing n_pages * this many titles without reaching n_pages
MAX_CONSECUTIVE_ERRORS = 10
BLOCK_TAGS = [
    "p", "div", "br", "blockquote", "pre", "ul", "ol", "li", "dl", "dt", "dd",
    "table", "caption", "tr", "th", "td", "h1", "h2", "h3", "h4", "h5", "h6",
]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Save random Wikipedia articles in a timestamped JSON file.")
    parser.add_argument("--n-pages", type=int, default=20, help="number of articles to save")
    parser.add_argument(
        "--min-chars",
        type=int,
        default=1500,
        help="skip articles whose text is shorter than this (stubs)",
    )
    parser.add_argument("--output-dir", default="/data/wikipedia", help="folder for the JSON file")
    return parser.parse_args(argv)


class PagesWriter:
    """Appends pages to a JSON file as they come, so a run that stops early keeps what it fetched."""

    def __init__(self, path, fetched_at):
        self.count = 0
        self._file = path.open("w", encoding="utf-8")
        self._file.write(f'{{\n  "fetched_at": {json.dumps(fetched_at)},\n  "pages": [')

    def add(self, title, url, text):
        page = json.dumps({"title": title, "url": url, "text": text}, ensure_ascii=False, indent=2)
        self._file.write(("," if self.count else "") + "\n" + textwrap.indent(page, "    "))
        self._file.flush()
        self.count += 1

    def close(self):
        if not self._file.closed:
            self._file.write("\n  ]\n}\n")
            self._file.close()


def get_random_titles(session, n):
    """n random article titles (main namespace, no redirects)."""
    params = {
        "action": "query",
        "format": "json",
        "list": "random",
        "rnnamespace": 0,
        "rnlimit": n,
        "rnfilterredir": "nonredirects",
    }
    r = session.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return [p["title"] for p in r.json()["query"]["random"]]


def fetch_page(session, title):
    """(canonical title, rendered HTML) of one article, or (None, None) if it does not exist."""
    params = {
        "action": "parse",
        "page": title,
        "prop": "text",
        "redirects": 1,
        "format": "json",
        "formatversion": 2,
    }
    for attempt in range(5):
        r = session.get(API_URL, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(2**attempt)
            continue
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            return None, None
        return data["parse"]["title"], data["parse"]["text"]
    raise RuntimeError(f"too many retries for {title}")


def html_to_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["style", "script"]):
        tag.decompose()
    for tag in soup.select(".mw-editsection, .reference, .navbox, .mw-empty-elt"):
        tag.decompose()
    # Inline tags join as in the page; block-level ones get a space around them so that
    # neighbouring paragraphs, list items and table cells do not run together.
    for tag in soup.find_all(BLOCK_TAGS):
        tag.insert_before(" ")
        tag.insert_after(" ")
    return re.sub(r"\s+", " ", soup.get_text()).strip()


def page_url(title):
    return PAGE_URL + quote(title.replace(" ", "_"), safe="_()',!*:@~$&+=;-.")


def scrape(session, n_pages, min_chars, writer):
    """Add up to n_pages articles to writer."""
    examined = errors = 0
    budget = n_pages * MAX_TITLES_PER_PAGE
    seen = set()

    while writer.count < n_pages and examined < budget:
        for title in get_random_titles(session, TITLES_PER_BATCH):
            if writer.count >= n_pages or examined >= budget:
                break
            if title in seen:
                continue
            seen.add(title)
            examined += 1

            try:
                canonical, html = fetch_page(session, title)
            except (requests.RequestException, RuntimeError, KeyError, ValueError) as e:
                errors += 1
                log.warning("failed %s: %s", title, e)
                if errors >= MAX_CONSECUTIVE_ERRORS:
                    raise RuntimeError(f"{errors} errors in a row, giving up") from e
                continue
            errors = 0
            if not html:
                continue

            text = html_to_text(html)
            if len(text) < min_chars:
                log.info("skipped stub: %s", title)
                continue

            writer.add(canonical, page_url(canonical), text)
            log.info("[%d/%d] %s (%d chars)", writer.count, n_pages, canonical, len(text))
            time.sleep(0.05)


def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    if args.n_pages < 1:
        log.error("--n-pages must be at least 1")
        return EXIT_BAD_CONFIG

    started = datetime.now(timezone.utc).replace(microsecond=0)
    out_dir = Path(args.output_dir)
    stem = f"wikipedia_{started.strftime('%Y%m%dT%H%M%SZ')}"
    part = out_dir / f"{stem}.json.part"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        writer = PagesWriter(part, started.strftime("%Y-%m-%dT%H:%M:%SZ"))
    except OSError as e:
        log.error("cannot write in output dir %s: %s", out_dir, e)
        return EXIT_BAD_CONFIG

    try:
        with requests.Session() as session:
            session.headers["User-Agent"] = os.environ.get("WIKI_USER_AGENT") or DEFAULT_USER_AGENT
            scrape(session, args.n_pages, args.min_chars, writer)
    except Exception:
        log.exception("script failed")
    finally:
        writer.close()

    if writer.count == 0:
        part.unlink()
        log.error("no article saved, no file written")
        return EXIT_FAILURE
    if writer.count < args.n_pages:
        partial = out_dir / f"{stem}.partial.json"
        part.replace(partial)
        log.error("only %d of %d articles saved, kept in %s", writer.count, args.n_pages, partial)
        return EXIT_FAILURE
    final = out_dir / f"{stem}.json"
    part.replace(final)
    log.info("done: %d articles saved in %s", writer.count, final)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
