"""Ofsted landing: download the monthly schools inspections CSV, then store it raw in MongoDB.

  1. scrape_ofsted: docker_scripts/ofsted_scraper finds the newest "latest inspections" CSV on the
     gov.uk publication page and saves it, as published, in data/landing/ofsted/<run>/ with a manifest.json.
  2. load_into_mongo: docker_scripts/mongo_loader appends one document per school to the
     raw_ofsted_schools collection of project-mongo, wrapped with the source, the run id and the
     load timestamp.

Landing only: all rows are kept, nothing is cleaned or typed here. Runs monthly on the 14th (Ofsted
publishes the release of the previous month-end around the 10th-13th). Both tasks run in their own
container with DockerOperator. The DAG's name lives in dags/dag_names.py.
"""

import os
from datetime import datetime, timedelta
from airflow.sdk import DAG, Connection
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from dag_names import OFSTED_DAG_DISPLAY_NAME, OFSTED_DAG_ID

# The containers are started by the host's Docker daemon (through docker.sock), so the bind-mount
# source must be a path on the host, not one inside the Airflow containers. Set it in .env.
host_data_dir = os.environ.get("HOST_DATA_DIR")
if not host_data_dir:
    raise ValueError("HOST_DATA_DIR is not set: put the absolute host path of the data/ folder in .env")

# Name of this run's folder, the same for both tasks (and for their retries). Manual runs may have
# no logical date, so fall back on the date the run was created; no ':' so it is a valid Windows path.
RUN_ID = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y%m%dT%H%M%S') }}"
LANDING_DIR = "/data/landing/ofsted"

with DAG(
    dag_id=OFSTED_DAG_ID,
    dag_display_name=OFSTED_DAG_DISPLAY_NAME,
    start_date=datetime(2026, 11, 1),
    schedule="0 6 14 * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    # Leave empty to download the newest release; set a CSV url to land a specific month.
    params={"url": ""},
    tags=["landing", "ofsted"],
):

    mongo_conn = Connection.get("project-mongo")  # set up once in Admin -> Connections

    scrape_ofsted = DockerOperator(
        task_id="scrape_ofsted",
        image="ofsted-scraper:latest",  # built from docker_scripts/ofsted_scraper, see README
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
            "--url",
            # An empty value (no `params.url`) makes the scraper look for the newest file itself.
            "{{ params.url }}",
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

    scrape_ofsted >> load_into_mongo
