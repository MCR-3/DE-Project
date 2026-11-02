# DE-Project

Data engineering project running on Apache Airflow 3.3.1 (CeleryExecutor) with Docker Compose.

The DAGs in `dags/` and the script in `docker_scripts/` are **examples** and templates to copy from, not an actual data pipeline. See [Writing your own script](#writing-your-own-script).

## Contents

- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [1. Configure the `.env` file](#1-configure-the-env-file)
  - [2. Initialize Airflow](#2-initialize-airflow)
  - [3. Start the stack](#3-start-the-stack)
  - [4. Configure the project database connection in the Airflow UI](#4-configure-the-project-database-connection-in-the-airflow-ui)
  - [5. Build the example script image](#5-build-the-example-script-image)
  - [6. Run a DAG](#6-run-a-dag)
  - [DAG 3: Wikipedia scraper](#dag-3-wikipedia-scraper)
- [Writing your own script](#writing-your-own-script)

## Services

| Service | Purpose | Access |
| --- | --- | --- |
| `airflow-apiserver` | Airflow UI / API | http://localhost:8080 |
| `postgres` | Airflow metadata DB | internal only |
| `redis` | Celery broker | internal only |
| `project-db` | Project data DB (Postgres 18) | `localhost:5433` from the host, `project-db:5432` from containers |

The repository's `data/` folder is mounted at `/opt/airflow/data` in every Airflow container. DAGs use it for the files they produce (its content is git-ignored).

## Setup

### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2), with at least 4 GB of RAM and 2 CPUs available to Docker.
- The Airflow containers mount `/var/run/docker.sock` so that DAGs can launch other containers with `DockerOperator`.

### 1. Configure the `.env` file

Copy `.env.example` to `.env` at the repository root (it is read by every Airflow service, and is git-ignored):

```bash
cp .env.example .env
```

Then fill in the values:

- `AIRFLOW_UID`: on Linux, use your own user id (`id -u`) so files created in `dags/`, `logs/` and `config/` are not owned by root. On Windows/macOS `50000` is fine.
- `FERNET_KEY`: encrypts the connection passwords stored in Airflow. It is required by `docker-compose.yaml`. Generate one with:

  ```bash
  docker run --rm apache/airflow:3.3.1 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```

- `DOCKER_NETWORK`: network joined by the containers that `DockerOperator` launches. Empty by default; see [step 5](#5-build-the-example-script-image).

- `HOST_DATA_DIR`: absolute path of this repository's `data/` folder **on the host** (e.g. `C:/Users/me/GitHub/DE-Project/data`). Required by DAG 3: containers started through `docker.sock` are created by the host's Docker daemon, so their bind mounts need a host path, not one inside the Airflow containers.

Optional variables: `WIKI_USER_AGENT` is the User-Agent sent by DAG 3's scraper (Wikimedia asks for a contact, e.g. `MyScraper/1.0 (you@example.com)`; by default it points at this repository). `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` change the admin account created at init (both default to `airflow`).

### 2. Initialize Airflow

Run this **once**, the first time (and again after wiping the volumes):

```bash
docker compose up airflow-init
```

This builds the custom Airflow image (see `Dockerfile` / `requirements.txt`), migrates the Airflow metadata database and creates the admin user. Wait until the container exits with code `0` (`airflow-init exited with code 0`).

### 3. Start the stack

```bash
docker compose up -d
```

This starts the API server, scheduler, DAG processor, Celery worker, triggerer, Redis, and both Postgres databases. Add `--build` after changing the `Dockerfile` or `requirements.txt`. Check that everything is healthy with:

```bash
docker compose ps
```

Then open the Airflow UI at http://localhost:8080 and log in with `airflow` / `airflow` (unless you overrode the credentials in `.env`).

To stop the stack: `docker compose down` (add `-v` to also delete the databases' volumes, which requires running step 2 again).

### 4. Configure the project database connection in the Airflow UI

The DAGs read their database credentials from an Airflow connection. There are two databases in the stack, with separate roles:

- `postgres`: Airflow's own **metadata** database. It is internal to Airflow and DAGs must not read or write project data in it, so no connection points to it.
- `project-db`: the database dedicated to this project's data. Every DAG that stores or reads data uses it, through the `project-db` connection.

In the UI go to **Admin → Connections → Add Connection** and create the following.

**`project-db`**: project data DB, used by `second_dag_stub` and by the `example_docker_script` DAG

| Field | Value |
| --- | --- |
| Connection ID | `project-db` |
| Connection Type | `Postgres` |
| Host | `project-db` |
| Schema (database name) | `project` |
| Login | `project` |
| Password | `devpassword` |
| Port | `5432` |

Notes:

- Use the **container-side** host and port, because Airflow reaches the databases over the Docker network. `project-db` is also published on `localhost:5433` for connecting from your machine (e.g. `psql -h localhost -p 5433 -U project project`); Airflow's metadata `postgres` is not published.
- The credentials above are the development values defined in `docker-compose.yaml` (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` of `project-db`). Update them here if you change the compose file.
- The connection ID must match exactly what the DAGs reference (`Connection.get("project-db")`, `conn_id="project-db"`).

### 5. Build the example script image

The `example_docker_script` DAG launches `docker_scripts/example_script` (and `dag_3` launches `docker_scripts/wikipedia_scraper`) in its own container through `DockerOperator`, so the images must exist locally. Build every image declared in `docker-compose.scripts.yaml`:

```bash
docker compose -f docker-compose.scripts.yaml build
```

The DAG reads the network the launched container joins from the `DOCKER_NETWORK` variable in `.env`, which is empty by default. For `project-db` to resolve from that container, set it to the Compose network of this project. Find it with `docker network ls` (it is `<folder-name>_default`, e.g. `de-project_default`), then recreate the containers (`docker compose up -d`) so they pick up the change.

### 6. Run a DAG

DAGs are created paused. In the UI, un-pause the DAG you want and trigger it (`example_docker_script` accepts an `n_rows` parameter). It runs the example script, which logs a summary of random data and writes it to an `example_output` table in `project-db`.

### DAG 3: Wikipedia scraper

`dag_3` (displayed as "DAG 3") runs `docker_scripts/wikipedia_scraper` in its own container. It fetches random English Wikipedia articles and saves them all in one file per run, `data/wikipedia/wikipedia_<UTC timestamp>.json` (e.g. `wikipedia_20261102T161147Z.json`). It does not use `project-db`.

```json
{
  "fetched_at": "2026-11-02T16:11:47Z",
  "pages": [{"title": "...", "url": "https://en.wikipedia.org/wiki/...", "text": "plain text of the article"}]
}
```

- Parameter `n_pages` (default 20): number of articles to save. Stubs (text shorter than 1500 characters) are skipped and do not count. Articles are not de-duplicated between runs, only within a run.
- Pages are appended to the file as they are fetched (`*.json.part` while running). If the run stops before `n_pages`, the task fails and what was fetched is kept in a valid `wikipedia_<timestamp>.partial.json`; with no page at all, no file is written.
- Needs `HOST_DATA_DIR` in `.env` (see [step 1](#1-configure-the-env-file)) and the `wikipedia-scraper` image built (see [step 5](#5-build-the-example-script-image)).
- The DAG's id and display name are defined in one file, `dags/dag_names.py`.

To run the scraper by hand, outside of Airflow:

```bash
docker run --rm -v "$PWD/data:/data" wikipedia-scraper:latest --n-pages 5 --output-dir /data/wikipedia
```

## Writing your own script

`docker_scripts/example_script` is a template for Python scripts that should run **outside of Airflow's container**, in their own image. This keeps their dependencies away from Airflow's, and lets Airflow only schedule and monitor them. The script itself does nothing useful: it generates random data and summarises it. What it shows:

| Concern | Where |
| --- | --- |
| Own dependencies, independent from Airflow's | `requirements.txt` and `Dockerfile` |
| Command-line arguments, filled in by the DAG | `parse_args()` in `main.py`, `command=[...]` in the DAG |
| Configuration and credentials from environment variables | `get_engine()` in `main.py`, `environment` / `private_environment` in the DAG |
| Logs shown in the Airflow task log | `logging` in `main.py`, `PYTHONUNBUFFERED` in the `Dockerfile` |
| Success / failure reported to Airflow through the exit code | `main()` return value in `main.py` |
| Reaching `project-db` from the container | `DOCKER_NETWORK` in `.env`, see [step 5](#5-build-the-example-script-image) |

To create a new script:

1. Copy `docker_scripts/example_script` to `docker_scripts/<your_script>`, then replace `generate()` / `summarise()` in `main.py` and adjust `requirements.txt`.
2. Declare it in `docker-compose.scripts.yaml` next to `example-script` (with its own `build:` and `image:`), and build it with `docker compose -f docker-compose.scripts.yaml build`.
3. Copy `dags/example_docker.py`, then set the `dag_id`, the `image`, the `command` arguments and the `environment` variables.

To run the example script by hand, outside of Airflow (merging the main compose file makes `project-db` resolvable):

```bash
docker compose -f docker-compose.yaml -f docker-compose.scripts.yaml run --rm example-script --n-rows 500
```
