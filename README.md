# DE-Project

Data engineering project running on Apache Airflow 3.3.1 (CeleryExecutor) with Docker Compose.

| Service | Purpose | Access |
| --- | --- | --- |
| `airflow-apiserver` | Airflow UI / API | http://localhost:8080 |
| `postgres` | Airflow metadata DB | internal only |
| `redis` | Celery broker | internal only |
| `project-db` | Project data DB (Postgres 18) | `localhost:5433` from the host, `project-db:5432` from containers |

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

- `DOCKER_NETWORK`: network joined by the containers that `DockerOperator` launches. Empty by default; see [step 5](#5-build-the-script-images-for-customer_clustering).

Optional variables: `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` change the admin account created at init (both default to `airflow`).

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

### 4. Configure the database connections in the Airflow UI

The DAGs read their database credentials from Airflow connections. There are two databases in the stack:

- `postgres`: the database Airflow spins up for its own **metadata**. `postgres_default` refers to it.
- `project-db`: a separate, side database dedicated to this project's data. `project-db` refers to it.

In the UI go to **Admin → Connections → Add Connection** and create the following.

**`project-db`**: project data DB, used by the `customer_clustering` DAG

| Field | Value |
| --- | --- |
| Connection ID | `project-db` |
| Connection Type | `Postgres` |
| Host | `project-db` |
| Schema (database name) | `project` |
| Login | `project` |
| Password | `devpassword` |
| Port | `5432` |

**`postgres_default`**: Airflow's metadata DB, used by the `SQLExecuteQueryOperator` tasks in `second_dag_stub`

If a `postgres_default` connection already exists, edit it (its pre-filled values may not match this setup); otherwise create it:

| Field | Value |
| --- | --- |
| Connection ID | `postgres_default` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Schema (database name) | `airflow` |
| Login | `airflow` |
| Password | `airflow` |
| Port | `5432` |

Notes:

- Use the **container-side** host and port, because Airflow reaches the databases over the Docker network. `project-db` is also published on `localhost:5433` for connecting from your machine (e.g. `psql -h localhost -p 5433 -U project project`); the metadata `postgres` is not published.
- The credentials above are the development values defined in `docker-compose.yaml` (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` of each database). Update them here if you change the compose file.
- Connection IDs must match exactly what the DAGs reference (`Connection.get("project-db")`, `conn_id="postgres_default"`).

### 5. Build the script images (for `customer_clustering`)

The `customer_clustering` DAG launches the clustering script in its own container through `DockerOperator`, so the image must exist locally:

```bash
docker build -t clustering-worker:latest ./docker_scripts/python_clustering
```

The DAG reads the network the launched container joins from the `DOCKER_NETWORK` variable in `.env`, which is empty by default. For `project-db` to resolve from that container, set it to the Compose network of this project. Find it with `docker network ls` (it is `<folder-name>_default`, e.g. `de-project_default`), then recreate the containers (`docker compose up -d`) so they pick up the change.

### 6. Run a DAG

DAGs are created paused. In the UI, un-pause the DAG you want and trigger it (`customer_clustering` accepts an `n_clusters` parameter).
