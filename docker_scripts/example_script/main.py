"""Example script meant to run in its own container, outside of Airflow's.

It does nothing useful on purpose: it only shows the pieces a real script usually needs.
Copy this folder (see the README) and replace `generate()` / `summarise()` with real work.

  * Own dependencies: `requirements.txt` is independent from Airflow's, so heavy or
    conflicting libraries (pandas, ML libs, ...) never touch the Airflow image.
  * Command-line arguments: the DAG passes them through `DockerOperator(command=[...])`.
  * Configuration from the environment: the DAG passes DB credentials as env vars, they are
    never hardcoded here.
  * Logging to stdout/stderr: `DockerOperator` streams it into the Airflow task log.
  * Exit code: non-zero makes the Airflow task fail, zero makes it succeed.
"""

import argparse
import logging
import os
import sys

import numpy as np
import pandas as pd

log = logging.getLogger("example_script")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_BAD_CONFIG = 2


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Example containerised script.")
    parser.add_argument("--n-rows", type=int, default=1000, help="rows of fake data")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument(
        "--output-table",
        help="write the result to this table of the database described by the DB_* "
        "environment variables; if omitted, the result is only logged",
    )
    return parser.parse_args(argv)


def get_engine():
    """Build a SQLAlchemy engine from DB_* environment variables (raises KeyError if unset)."""
    from sqlalchemy import create_engine  # only needed when writing to a database

    url = (
        f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    )
    return create_engine(url)


def generate(n_rows, seed):
    """Placeholder input: random values spread over a few groups."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "group": rng.choice(["a", "b", "c"], size=n_rows),
            "value": rng.normal(size=n_rows),
        }
    )


def summarise(df):
    """Placeholder processing: one summary row per group."""
    return (
        df.groupby("group")["value"]
        .agg(["count", "mean", "std"])
        .reset_index()
    )


def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    engine = None
    if args.output_table:
        try:
            engine = get_engine()
        except KeyError as missing:
            log.error("missing environment variable %s", missing)
            return EXIT_BAD_CONFIG

    try:
        df = generate(args.n_rows, args.seed)
        summary = summarise(df)
        log.info("summary:\n%s", summary.to_string(index=False))

        if engine is not None:
            summary.to_sql(args.output_table, engine, if_exists="replace", index=False)
            log.info("wrote %d rows to %s", len(summary), args.output_table)
    except Exception:
        log.exception("script failed")
        return EXIT_FAILURE

    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
