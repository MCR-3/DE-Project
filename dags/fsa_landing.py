"""FSA landing: fetch the food hygiene ratings of every English local authority, then store them raw in MongoDB.

  1. prepare_fsa: docker_scripts/fsa_scraper saves the reference files (authorities, business types,
     ratings, score descriptors) in data/landing/fsa/<run>/ and fills the run's Authority queue in
     project-redis, largest authority first.
  2. fetch_fsa: `fetchers` Fetchers in parallel, one container each, take authorities from the queue
     and save all their pages (see CONTEXT.md and docs/adr/0002).
  3. finalize_fsa: runs even if a Fetcher failed; checks that every authority was saved, then writes
     the manifest.json.
  4. load_into_mongo: docker_scripts/mongo_loader appends the files to the raw_fsa_* collections of
     project-mongo, wrapped with the source, the run id and the load timestamp.

Landing only: nothing is cleaned, typed or filtered here (see docs/adr/0001). Runs weekly, on
Mondays at 02:00 UTC (the FSA asks for bulk retrievals between 01:00 and 06:00); a full run is
about 500,000 establishments. Requests to the FSA are capped at `max_rps` per second for all the
Fetchers together. For a test, trigger it by hand with `sample` ticked: only the authorities of
`la_ids` (comma-separated FSA LocalAuthorityId values) are fetched, and the run is marked as a
sample run in MongoDB. Every step runs in its own container with DockerOperator. The DAG's name
lives in dags/dag_names.py.
"""

import os
from datetime import datetime, timedelta
from airflow.sdk import DAG, Connection, Param, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from dag_names import FSA_DAG_DISPLAY_NAME, FSA_DAG_ID

# The containers are started by the host's Docker daemon (through docker.sock), so the bind-mount
# source must be a path on the host, not one inside the Airflow containers. Set it in .env.
host_data_dir = os.environ.get("HOST_DATA_DIR")
if not host_data_dir:
    raise ValueError("HOST_DATA_DIR is not set: put the absolute host path of the data/ folder in .env")

# Name of this run's folder, the same for every task (and for their retries). Manual runs may have
# no logical date, so fall back on the date the run was created; no ':' so it is a valid Windows path.
RUN_ID = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y%m%dT%H%M%S') }}"
LANDING_DIR = "/data/landing/fsa"
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")  # must be the Compose network, or project-* do not resolve

with DAG(
    dag_id=FSA_DAG_ID,
    dag_display_name=FSA_DAG_DISPLAY_NAME,
    start_date=datetime(2026, 11, 1),
    schedule="0 2 * * 1",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        # Unticked for the scheduled runs: a full run, every English authority.
        "sample": Param(
            False,
            type="boolean",
            title="Sample run",
            description="Tick for a test run on the authorities of la_ids only. Unticked: a full run, every English authority.",
        ),
        # Default sample: Isles of Scilly (136 establishments), Bath and North East Somerset (~1,800), Leeds (~7,700).
        "la_ids": Param(
            "321,326,397",
            type="string",
            title="Local authorities of a sample run",
            description="Comma-separated FSA LocalAuthorityId values. Only used when 'Sample run' is ticked.",
        ),
        # Each Fetcher holds one of the airflow-worker's Celery slots for the whole run; beyond what
        # max_rps can keep busy, more Fetchers only wait on the rate limit.
        "fetchers": Param(
            4,
            type="integer",
            minimum=1,
            maximum=8,
            title="Fetchers",
            description="Number of Fetchers working in parallel, one container each.",
        ),
        "max_rps": Param(
            4,
            type="number",
            exclusiveMinimum=0,
            maximum=20,
            title="Requests per second",
            description="Requests per second to the FSA API, for all the Fetchers together.",
        ),
    },
    tags=["landing", "fsa"],
):

    mongo_conn = Connection.get("project-mongo")  # set up once in Admin -> Connections
    redis_conn = Connection.get("project-redis")  # set up once in Admin -> Connections

    # What the three FSA steps share: the scraper image, the data/ folder (written as Airflow's user,
    # so the files match Airflow's ownership) and project-redis, where the Authority queue lives.
    fsa_scraper = dict(
        image="fsa-scraper:latest",  # built from docker_scripts/fsa_scraper, see README
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        user=os.environ.get("AIRFLOW_UID", "50000"),
        mounts=[Mount(source=host_data_dir, target="/data", type="bind")],
        environment={
            "REDIS_HOST": redis_conn.host,
            "REDIS_PORT": str(redis_conn.port),
            # Optional: overrides the scraper's default User-Agent.
            "SCRAPER_USER_AGENT": os.environ.get("SCRAPER_USER_AGENT", ""),
        },
        private_environment={  # like `environment`, but hidden from the rendered task fields in the UI
            "REDIS_PASSWORD": redis_conn.password,
        },
    )

    prepare_fsa = DockerOperator(
        task_id="prepare_fsa",
        command=[
            "prepare",
            "--output-dir", LANDING_DIR,
            "--run-id", RUN_ID,
            "--max-rps", "{{ params.max_rps }}",
            "{{ '--sample' if params.sample else '--no-sample' }}",
            "--la-ids", "{{ params.la_ids }}",
        ],
        **fsa_scraper,
    )

    @task
    def fetcher_commands(landing_run, fetchers, max_rps):
        """One `fetch` command per Fetcher; its last item is the Fetcher's number, shown in the UI.

        Not `run_id`: TaskFlow refuses a parameter named like a key of the Airflow context.
        """
        return [
            ["fetch", "--output-dir", LANDING_DIR, "--run-id", landing_run, "--max-rps", str(max_rps), "--fetcher", str(i)]
            for i in range(1, int(fetchers) + 1)
        ]

    fetch_fsa = DockerOperator.partial(
        task_id="fetch_fsa",
        map_index_template="Fetcher {{ task.command[-1] }}",
        **fsa_scraper,
    ).expand(command=fetcher_commands(RUN_ID, "{{ params.fetchers }}", "{{ params.max_rps }}"))

    finalize_fsa = DockerOperator(
        task_id="finalize_fsa",
        # Even if a Fetcher failed: another one may have taken its authority back. Whether the run is
        # complete is decided from the files, not from the Fetchers' states.
        trigger_rule="all_done",
        command=["finalize", "--output-dir", LANDING_DIR, "--run-id", RUN_ID],
        **fsa_scraper,
    )

    load_into_mongo = DockerOperator(
        task_id="load_into_mongo",
        image="mongo-loader:latest",  # built from docker_scripts/mongo_loader, see README
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        user=os.environ.get("AIRFLOW_UID", "50000"),
        mounts=[Mount(source=host_data_dir, target="/data", type="bind", read_only=True)],
        command=["--run-dir", f"{LANDING_DIR}/{RUN_ID}"],
        environment={
            "MONGO_HOST": mongo_conn.host,
            "MONGO_PORT": str(mongo_conn.port),
            "MONGO_DB": mongo_conn.schema,
            "MONGO_USER": mongo_conn.login,
        },
        private_environment={
            "MONGO_PASSWORD": mongo_conn.password,
        },
    )

    prepare_fsa >> fetch_fsa >> finalize_fsa >> load_into_mongo
