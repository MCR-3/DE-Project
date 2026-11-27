"""Staging load: turn the landed FSA, Ofsted and postcodes.io runs into the staging.* tables of project-db.

Every task runs a container with DockerOperator (images built from docker_scripts/staging_loader and
docker_scripts/postcode_scraper, plus the generic mongo_loader). The steps, their rules and their order
are described in docs/staging/README.md:

  init                                schemas, tables, functions, views, seeds (idempotent)
  copy_raw_fsa / copy_raw_ofsted      landed MongoDB documents -> raw.* (JSONB)
  reference -> establishments         FSA steps 1 and 3
  schools                             Ofsted step 2
  scrape_postcodes -> load_postcodes  postcodes.io lookups for the latest runs, landed as a postcode run
  -> copy_raw_postcodes -> geo        step 4 (needs schools and establishments)
  ofsted_inspections, fsa_measures    step 5
  finalize_fsa / _ofsted / _postcodes step 6: reconcile, record in stg_load_log, delete the raw copy

The landing DAGs do not trigger it (they stay independent, see docs/adr/0001). It runs on its own after
them, every Monday at 08:00 UTC (fsa_landing: Mondays 02:00) and on the 14th at 09:00 UTC (ofsted_landing:
14th 06:00), and stages every full run that is not finalized yet; a source with nothing new makes its
steps return at once. Params `fsa_run` / `ofsted_run` stage one given run instead (a sample run too).
The postcode run is this DAG run's own run id. A failed run-level check fails its task (exit code 1).
The DAG's name lives in dags/dag_names.py.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Connection, Param
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from docker.types import Mount

from dag_names import STAGING_DAG_DISPLAY_NAME, STAGING_DAG_ID

# The containers are started by the host's Docker daemon (through docker.sock), so the bind-mount
# source must be a path on the host, not one inside the Airflow containers. Set it in .env.
host_data_dir = os.environ.get("HOST_DATA_DIR")
if not host_data_dir:
    raise ValueError("HOST_DATA_DIR is not set: put the absolute host path of the data/ folder in .env")

# This DAG run's id, used as the id of its postcode run (same format as the landing runs).
RUN_ID = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y%m%dT%H%M%S') }}"
POSTCODES_DIR = "/data/landing/postcodes"
# An empty param gives `--run-id ""`, which the scripts read as "every pending full run" / "the latest".
FSA_RUN = "{{ params.fsa_run }}"
OFSTED_RUN = "{{ params.ofsted_run }}"

with DAG(
    dag_id=STAGING_DAG_ID,
    dag_display_name=STAGING_DAG_DISPLAY_NAME,
    start_date=datetime(2026, 11, 1),
    schedule=MultipleCronTriggerTimetable("0 8 * * 1", "0 9 14 * *", timezone="UTC"),
    catchup=False,
    max_active_runs=1,  # two runs at once would stage the same pending runs
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},  # every step is idempotent
    params={
        "fsa_run": Param("", type="string", title="FSA run",
                         description="Stage this FSA run only (a sample run too). Empty: every full run not staged yet."),
        "ofsted_run": Param("", type="string", title="Ofsted run",
                            description="Stage this Ofsted run only. Empty: every full run not staged yet."),
    },
    tags=["staging"],
):

    db_conn = Connection.get("project-db")  # set up once in Admin -> Connections
    mongo_conn = Connection.get("project-mongo")
    network = os.environ.get("DOCKER_NETWORK", "")  # the Compose network, so project-db / project-mongo resolve
    mongo_env = {"MONGO_HOST": mongo_conn.host, "MONGO_PORT": str(mongo_conn.port),
                 "MONGO_DB": mongo_conn.schema, "MONGO_USER": mongo_conn.login}

    def staging(task_id, *args):
        """A task running staging_loader with these arguments."""
        return DockerOperator(
            task_id=task_id,
            image="staging-loader:latest",  # built from docker_scripts/staging_loader, see docs/staging
            docker_url="unix://var/run/docker.sock",
            network_mode=network,
            auto_remove="success",
            mount_tmp_dir=False,
            command=list(args),
            environment={"DB_HOST": db_conn.host, "DB_PORT": str(db_conn.port), "DB_NAME": db_conn.schema,
                         "DB_USER": db_conn.login, **mongo_env},
            private_environment={"DB_PASSWORD": db_conn.password, "MONGO_PASSWORD": mongo_conn.password},
        )

    init = staging("init", "init")
    copy_raw_fsa = staging("copy_raw_fsa", "copy-raw", "--source", "fsa", "--run-id", FSA_RUN)
    copy_raw_ofsted = staging("copy_raw_ofsted", "copy-raw", "--source", "ofsted", "--run-id", OFSTED_RUN)
    reference = staging("reference", "stage", "--step", "reference", "--run-id", FSA_RUN)
    establishments = staging("establishments", "stage", "--step", "establishments", "--run-id", FSA_RUN)
    schools = staging("schools", "stage", "--step", "schools", "--run-id", OFSTED_RUN)

    scrape_postcodes = DockerOperator(
        task_id="scrape_postcodes",
        image="postcode-scraper:latest",  # built from docker_scripts/postcode_scraper
        docker_url="unix://var/run/docker.sock",
        network_mode=network,
        auto_remove="success",
        mount_tmp_dir=False,
        user=os.environ.get("AIRFLOW_UID", "50000"),  # so the files match Airflow's ownership
        mounts=[Mount(source=host_data_dir, target="/data", type="bind")],
        # empty --fsa-run / --ofsted-run: the latest full run of each source
        command=["--output-dir", POSTCODES_DIR, "--run-id", RUN_ID, "--fsa-run", FSA_RUN, "--ofsted-run", OFSTED_RUN],
        environment={**mongo_env, "SCRAPER_USER_AGENT": os.environ.get("SCRAPER_USER_AGENT", "")},
        private_environment={"MONGO_PASSWORD": mongo_conn.password},
    )

    load_postcodes = DockerOperator(
        task_id="load_postcodes",
        image="mongo-loader:latest",  # built from docker_scripts/mongo_loader
        docker_url="unix://var/run/docker.sock",
        network_mode=network,
        auto_remove="success",
        mount_tmp_dir=False,
        user=os.environ.get("AIRFLOW_UID", "50000"),
        mounts=[Mount(source=host_data_dir, target="/data", type="bind", read_only=True)],
        command=["--run-dir", f"{POSTCODES_DIR}/{RUN_ID}"],
        environment=mongo_env,
        private_environment={"MONGO_PASSWORD": mongo_conn.password},
    )

    # the postcode run always needs its id: a run found entirely in the cache has no document
    copy_raw_postcodes = staging("copy_raw_postcodes", "copy-raw", "--source", "postcodes", "--run-id", RUN_ID)
    geo = staging("geo", "stage", "--step", "geo", "--run-id", RUN_ID)
    ofsted_inspections = staging("ofsted_inspections", "stage", "--step", "ofsted_inspections", "--run-id", OFSTED_RUN)
    fsa_measures = staging("fsa_measures", "stage", "--step", "fsa_measures", "--run-id", FSA_RUN)
    finalize_fsa = staging("finalize_fsa", "stage", "--step", "finalize_fsa", "--run-id", FSA_RUN)
    finalize_ofsted = staging("finalize_ofsted", "stage", "--step", "finalize_ofsted", "--run-id", OFSTED_RUN)
    finalize_postcodes = staging("finalize_postcodes", "stage", "--step", "finalize_postcodes", "--run-id", RUN_ID)

    init >> [copy_raw_fsa, copy_raw_ofsted]
    copy_raw_fsa >> reference >> establishments >> fsa_measures >> finalize_fsa
    copy_raw_ofsted >> schools >> ofsted_inspections >> finalize_ofsted
    [establishments, schools] >> scrape_postcodes >> load_postcodes >> copy_raw_postcodes >> geo >> finalize_postcodes
    # finalize removes the raw copy; geo reads staging tables only, but keep the sources finalized last
    geo >> [finalize_fsa, finalize_ofsted]
