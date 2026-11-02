"""Fetch random Wikipedia articles and store each one's text in a .txt file.

Meant to run in its own container, launched by the DAG 3 DockerOperator (see dags/dag_3.py).
Each file is named after the article, its first line is the article's URL, then a blank line,
then the article text (infobox and table contents included).

  * Command-line arguments: filled in by the DAG through `DockerOperator(command=[...])`.
  * Configuration from the environment: `WIKI_USER_AGENT` (optional) overrides the User-Agent.
  * Output: files written under `--output-dir`, which the DAG bind-mounts from the host's data/ folder.
  * Exit code: 0 ok, 1 failure, 2 bad config (non-zero makes the Airflow task fail).
"""

import argparse
import logging
import os
import re
import sys
import time
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
MAX_FILENAME_CHARS = 150


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Save random Wikipedia articles as .txt files.")
    parser.add_argument("--n-pages", type=int, default=20, help="number of articles to save")
    parser.add_argument(
        "--min-chars",
        type=int,
        default=1500,
        help="skip articles whose text is shorter than this (stubs)",
    )
    parser.add_argument("--output-dir", default="/data/wikipedia", help="folder for the .txt files")
    return parser.parse_args(argv)


def safe_name(title):
    """File name for a title: no characters forbidden by Windows, no trailing dot/space."""
    name = re.sub(r'[\\/:*?"<>|]', "_", title)
    return name[:MAX_FILENAME_CHARS].rstrip(" .")


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
    text = soup.get_text(separator="\n")
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def page_url(title):
    return PAGE_URL + quote(title.replace(" ", "_"), safe="_()',!*:@~$&+=;-.")


def write_article(path, url, text):
    # Write to a temp file first so an interrupted run never leaves a half-written article.
    tmp = path.with_suffix(".txt.tmp")
    tmp.write_text(f"{url}\n\n{text}\n", encoding="utf-8")
    tmp.replace(path)


def scrape(session, n_pages, min_chars, out_dir):
    """Save n_pages articles into out_dir; returns how many were saved."""
    saved = examined = errors = 0
    budget = n_pages * MAX_TITLES_PER_PAGE
    seen = set()

    while saved < n_pages and examined < budget:
        for title in get_random_titles(session, TITLES_PER_BATCH):
            if saved >= n_pages or examined >= budget:
                break
            if title in seen:
                continue
            seen.add(title)
            examined += 1

            path = out_dir / f"{safe_name(title)}.txt"
            if path.exists():
                log.info("already saved: %s", title)
                continue

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

            write_article(path, page_url(canonical), text)
            saved += 1
            log.info("[%d/%d] %s (%d chars)", saved, n_pages, title, len(text))
            time.sleep(0.05)

    return saved


def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    if args.n_pages < 1:
        log.error("--n-pages must be at least 1")
        return EXIT_BAD_CONFIG
    out_dir = Path(args.output_dir)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        log.error("cannot create output dir %s: %s", out_dir, e)
        return EXIT_BAD_CONFIG

    try:
        with requests.Session() as session:
            session.headers["User-Agent"] = os.environ.get("WIKI_USER_AGENT") or DEFAULT_USER_AGENT
            saved = scrape(session, args.n_pages, args.min_chars, out_dir)
    except Exception:
        log.exception("script failed")
        return EXIT_FAILURE

    if saved < args.n_pages:
        log.error("only %d of %d articles saved after %d titles drawn", saved, args.n_pages, args.n_pages * MAX_TITLES_PER_PAGE)
        return EXIT_FAILURE
    log.info("done: %d articles saved in %s", saved, out_dir)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
