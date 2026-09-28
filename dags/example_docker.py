"""Example DAG: run a script from docker_scripts/ in its own container with DockerOperator.

It only demonstrates the wiring (image, arguments, DB credentials, network); the script it
launches, docker_scripts/example_script, does nothing meaningful. Copy both to build a real one.
"""

import os
from datetime import datetime
from airflow.sdk import DAG, Connection
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="example_docker_script",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"n_rows": 1000},
    tags=["example"],
):

    conn = Connection.get("project-db")  # set up once in Admin -> Connections

    run_example_script = DockerOperator(
        task_id="run_example_script",
        image="example-script:latest",  # built from docker_scripts/example_script, see README
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ.get("DOCKER_NETWORK", ""),  # set DOCKER_NETWORK in .env; find yours with `docker network ls`
        auto_remove="success",
        mount_tmp_dir=False,  # avoids bind-mount errors: airflow-worker runs in its own container, so a host tmp dir it creates isn't visible to the host's dockerd
        # Passed to the script as command-line arguments (the image's ENTRYPOINT is the program).
        command=[
            "--n-rows",
            "{{ params.n_rows }}",
            "--output-table",
            "example_output",
        ],
        # Passed to the script as environment variables.
        environment={
            "DB_HOST": conn.host,
            "DB_PORT": str(conn.port),
            "DB_NAME": conn.schema,
            "DB_USER": conn.login,
        },
        private_environment={  # like `environment`, but hidden from the rendered task fields in the UI
            "DB_PASSWORD": conn.password,
        },
    )
