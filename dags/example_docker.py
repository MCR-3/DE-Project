import os
from datetime import datetime
from airflow.sdk import DAG, task, Connection
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="customer_clustering",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"n_clusters": 5},
):

    @task
    def staging_to_production_prep():
        print("make sure staging.customers is fresh before clustering")

    conn = Connection.get("project-db")  # set up once in Admin -> Connections

    run_clustering = DockerOperator(
        task_id="run_clustering",
        image="clustering-worker:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ.get("DOCKER_NETWORK", ""),  # set DOCKER_NETWORK in .env; find yours with `docker network ls`
        auto_remove="success",
        mount_tmp_dir=False,  # avoids bind-mount errors: airflow-worker runs in its own container, so a host tmp dir it creates isn't visible to the host's dockerd
        command=[
            "--n-clusters",
            "{{ params.n_clusters }}",
            "--input-table",
            "staging.customers",
            "--output-table",
            "production.customer_segments",
        ],
        environment={
            "DB_HOST": conn.host,
            "DB_PORT": str(conn.port),
            "DB_NAME": conn.schema,
            "DB_USER": conn.login,
        },
        private_environment={
            "DB_PASSWORD": conn.password,
        },
    )

    staging_to_production_prep() >> run_clustering
