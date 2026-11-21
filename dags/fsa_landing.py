"""FSA landing: fetch the food hygiene ratings of a few English local authorities, then store them raw in MongoDB.

  1. scrape_fsa: docker_scripts/fsa_scraper saves the FHRS API responses, as received, in
     data/landing/fsa/<run>/ (authorities, business types, establishments, and a manifest.json).
  2. load_into_mongo: docker_scripts/mongo_loader appends them to the raw_fsa_* collections of
     project-mongo, wrapped with the source, the run id and the load timestamp.

Landing only: nothing is cleaned or typed here. Runs daily at 02:00 UTC (the FSA asks for bulk
retrievals between 01:00 and 06:00). The `la_ids` parameter is a comma-separated list of FSA
LocalAuthorityId values, or `all` for every English authority. Both tasks run in their own
container with DockerOperator. The DAG's name lives in dags/dag_names.py.
"""

import os
from datetime import datetime, timedelta
from airflow.sdk import DAG, Connection
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from dag_names import FSA_DAG_DISPLAY_NAME, FSA_DAG_ID

# The containers are started by the host's Docker daemon (through docker.sock), so the bind-mount
# source must be a path on the host, not one inside the Airflow containers. Set it in .env.
host_data_dir = os.environ.get("HOST_DATA_DIR")
if not host_data_dir:
    raise ValueError("HOST_DATA_DIR is not set: put the absolute host path of the data/ folder in .env")

# Name of this run's folder, the same for both tasks (and for their retries). Manual runs may have
# no logical date, so fall back on the date the run was created; no ':' so it is a valid Windows path.
RUN_ID = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y%m%dT%H%M%S') }}"
LANDING_DIR = "/data/landing/fsa"

with DAG(
    dag_id=FSA_DAG_ID,
    dag_display_name=FSA_DAG_DISPLAY_NAME,
    start_date=datetime(2026, 11, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    # Default extract: Isles of Scilly (136 establishments), Bath and North East Somerset (~1,800), Leeds (~7,700).
    params={"la_ids": "321,326,397"},
    tags=["landing", "fsa"],
):

    mongo_conn = Connection.get("project-mongo")  # set up once in Admin -> Connections

    scrape_fsa = DockerOperator(
        task_id="scrape_fsa",
        image="fsa-scraper:latest",  # built from docker_scripts/fsa_scraper, see README
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        user=os.environ.get("AIRFLOW_UID", "50000"),  # so the files match Airflow's ownership
        mounts=[Mount(source=host_data_dir, target="/data", type="bind")],
        command=[
            "--output-dir",
            LANDING_DIR,
            "--run-id",
            RUN_ID,
            "--la-ids",
            "{{ params.la_ids }}",
        ],
        environment={
            # Optional: overrides the scraper's default User-Agent.
            "SCRAPER_USER_AGENT": os.environ.get("SCRAPER_USER_AGENT", ""),
        },
    )

    load_into_mongo = DockerOperator(
        task_id="load_into_mongo",
        image="mongo-loader:latest",  # built from docker_scripts/mongo_loader, see README
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ.get("DOCKER_NETWORK", ""),  # must be the Compose network, or project-mongo does not resolve
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
        private_environment={  # like `environment`, but hidden from the rendered task fields in the UI
            "MONGO_PASSWORD": mongo_conn.password,
        },
    )

    scrape_fsa >> load_into_mongo
