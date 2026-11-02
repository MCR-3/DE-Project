"""DAG 3: fetch random Wikipedia articles and store their text as .txt files in data/wikipedia/.

Runs docker_scripts/wikipedia_scraper in its own container with DockerOperator. The DAG's name
lives in dags/dag_names.py.
"""

import os
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from dag_names import PROJECT_DAG_DISPLAY_NAME, PROJECT_DAG_ID

# The container is started by the host's Docker daemon (through docker.sock), so the bind-mount
# source must be a path on the host, not one inside the Airflow containers. Set it in .env.
host_data_dir = os.environ.get("HOST_DATA_DIR")
if not host_data_dir:
    raise ValueError("HOST_DATA_DIR is not set: put the absolute host path of the data/ folder in .env")

with DAG(
    dag_id=PROJECT_DAG_ID,
    dag_display_name=PROJECT_DAG_DISPLAY_NAME,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"n_pages": 20},
    tags=["scraping"],
):

    scrape_wikipedia = DockerOperator(
        task_id="scrape_wikipedia",
        image="wikipedia-scraper:latest",  # built from docker_scripts/wikipedia_scraper, see README
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ.get("DOCKER_NETWORK", ""),
        auto_remove="success",
        mount_tmp_dir=False,  # see example_docker.py
        user=os.environ.get("AIRFLOW_UID", "50000"),  # same owner as the Airflow containers, so the files in data/ are writable
        mounts=[Mount(source=host_data_dir, target="/data", type="bind")],
        command=[
            "--n-pages",
            "{{ params.n_pages }}",
            "--output-dir",
            "/data/wikipedia",
        ],
        environment={
            # Optional: overrides the scraper's default User-Agent (put your contact in it).
            "WIKI_USER_AGENT": os.environ.get("WIKI_USER_AGENT", ""),
        },
    )
