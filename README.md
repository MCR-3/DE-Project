# DE-Project

Data engineering project running on Apache Airflow 3.3.1 (CeleryExecutor) with Docker Compose.

The `example_*` DAGs and `docker_scripts/example_script` are **examples** and templates to copy from, see [Writing your own script](#writing-your-own-script). The two **landing DAGs** (`fsa_landing` and `ofsted_landing`) are the first real part of the project: they scrape the UK food hygiene ratings (FSA) and the school inspections (Ofsted) and store them, as received, in MongoDB. See [Landing DAGs](#landing-dags-fsa-and-ofsted-scraping-into-mongodb). Staging, warehouse and reporting are not implemented yet.

## Contents

- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [1. Configure the `.env` file](#1-configure-the-env-file)
  - [2. Initialize Airflow](#2-initialize-airflow)
  - [3. Start the stack](#3-start-the-stack)
  - [4. Configure the project database connection in the Airflow UI](#4-configure-the-project-database-connection-in-the-airflow-ui)
  - [5. Build the example script image](#5-build-the-example-script-image)
  - [6. Run a DAG](#6-run-a-dag)
  - [Landing DAGs: FSA and Ofsted scraping into MongoDB](#landing-dags-fsa-and-ofsted-scraping-into-mongodb)
- [Writing your own script](#writing-your-own-script)

## Services

| Service | Purpose | Access |
| --- | --- | --- |
| `airflow-apiserver` | Airflow UI / API | http://localhost:8080 |
| `postgres` | Airflow metadata DB | internal only |
| `redis` | Celery broker | internal only |
| `project-db` | Project data DB (Postgres 18) | `localhost:5433` from the host, `project-db:5432` from containers |
| `project-mongo` | Project document DB (MongoDB 8): the landing zone of the landing DAGs | `localhost:27018` from the host, `project-mongo:27017` from containers |

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

- `HOST_DATA_DIR`: absolute path of this repository's `data/` folder **on the host** (e.g. `C:/Users/me/GitHub/DE-Project/data`). Required by the landing DAGs: containers started through `docker.sock` are created by the host's Docker daemon, so their bind mounts need a host path, not one inside the Airflow containers.

Optional variables: `SCRAPER_USER_AGENT` is the User-Agent sent by the FSA and Ofsted scrapers (it is polite to give a contact, e.g. `MyScraper/1.0 (you@example.com)`; by default it points at this repository). `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` change the admin account created at init (both default to `airflow`).

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

**`project-mongo`**: project MongoDB, used by the second task of both landing DAGs. They do not load in the UI until this connection exists.

| Field | Value |
| --- | --- |
| Connection ID | `project-mongo` |
| Connection Type | `Generic` |
| Host | `project-mongo` |
| Schema (database name) | `project` |
| Login | `project` |
| Password | `devpassword` |
| Port | `27017` |

The credentials are the development values of the `project-mongo` service (`MONGO_INITDB_ROOT_USERNAME` / `MONGO_INITDB_ROOT_PASSWORD` in `docker-compose.yaml`). From your machine, connect with `mongosh "mongodb://project:devpassword@localhost:27018/?authSource=admin"`.

### 5. Build the example script image

The `example_docker_script` DAG launches `docker_scripts/example_script` (and the landing DAGs launch `docker_scripts/fsa_scraper` or `docker_scripts/ofsted_scraper`, then `docker_scripts/mongo_loader`) in its own container through `DockerOperator`, so the images must exist locally. Build every image declared in `docker-compose.scripts.yaml`:

```bash
docker compose -f docker-compose.scripts.yaml build
```

The DAG reads the network the launched container joins from the `DOCKER_NETWORK` variable in `.env`, which is empty by default. For `project-db` (and `project-mongo`, for the landing DAGs) to resolve from that container, set it to the Compose network of this project. Find it with `docker network ls` (it is `<folder-name>_default`, e.g. `de-project_default`), then recreate the containers (`docker compose up -d`) so they pick up the change.

### 6. Run a DAG

DAGs are created paused. In the UI, un-pause the DAG you want and trigger it (`example_docker_script` accepts an `n_rows` parameter). It runs the example script, which logs a summary of random data and writes it to an `example_output` table in `project-db`.

### Landing DAGs: FSA and Ofsted scraping into MongoDB

The landing phase fetches the two source datasets and stores them **as received** in MongoDB (nothing is typed, cleaned or filtered; that is the job of the later staging phase). There is one DAG per source, each with two tasks, each task in its own container:

| DAG | Schedule | Task 1 (scrape) | Task 2 |
| --- | --- | --- | --- |
| `fsa_landing` ("FSA landing") | weekly, Mondays at 02:00 UTC (the FSA asks for bulk retrievals between 01:00 and 06:00) | `scrape_fsa`: `docker_scripts/fsa_scraper` | `load_into_mongo`: `docker_scripts/mongo_loader` |
| `ofsted_landing` ("Ofsted landing") | monthly, on the 14th at 06:00 UTC (Ofsted publishes around the 10th-13th) | `scrape_ofsted`: `docker_scripts/ofsted_scraper` | `load_into_mongo`: `docker_scripts/mongo_loader` |

Like every DAG they are created paused and have `catchup=False`; the tasks retry twice, 5 minutes apart. The DAGs' ids and display names are defined in one file, `dags/dag_names.py`. They do not use `project-db`.

**What the scrapers save.** Each run has its own folder, `data/landing/<source>/<run>/` (`<run>` is the run's logical date, e.g. `20261120T020000`, the same for both tasks and for retries). The scraper writes `manifest.json` last: a folder without it is an unfinished run, which the loader refuses.

```
data/landing/fsa/20261120T020000/
  authorities.json                       every UK local authority (GET /Authorities)
  business_types.json                    GET /BusinessTypes
  ratings.json                           GET /Ratings: every possible rating (0-5 for FHRS, Pass etc. for FHIS)
  score_descriptors.json                 GET /ScoreDescriptors: the wording of every score value
  establishments/la_326/page_0001.json   one file per API page of 200 establishments, per authority
  establishments/la_326/done.json        written after the authority's last page, so a retry skips it
  manifest.json
data/landing/ofsted/20261114T060000/
  schools.csv                            the newest "latest inspections" CSV, byte for byte (~22,000 schools)
  manifest.json
```

- **FSA** (`fsa_scraper`): reads the [FHRS API](https://api.ratings.food.gov.uk/help) (version 2, no key), at most 200 records per page and a pause of 0.5 s between calls, with retries on 429 and 5xx. Only English authorities are accepted (the project is limited to England). A scheduled run is a **full run**: every English authority, about 500,000 establishments and 2,600 calls, one to two hours. If it fails, the retry resumes: authorities whose `done.json` exists are not fetched again. For a test, trigger `fsa_landing` by hand with **`sample`** ticked: only the authorities of the `la_ids` parameter are fetched (comma-separated FSA `LocalAuthorityId` values; default `321,326,397`: Isles of Scilly, about 140 establishments, Bath and North East Somerset, about 1,800, and Leeds, about 7,700). Why every establishment and not only the schools: [ADR 0001](docs/adr/0001-landing-keeps-the-whole-source-scope.md).
- **Ofsted** (`ofsted_scraper`): the gov.uk page has no stable download link, so the scraper reads the publication page and downloads the `latest_inspections` CSV with the newest date in its name. `ofsted_landing` has an optional parameter `url` to land a specific CSV (e.g. an older month) instead. All rows are kept.

**What lands in MongoDB.** `mongo_loader` reads the manifest and inserts one document per record into the `project` database of `project-mongo`:

| Collection | Source | One document per | `source_key` |
| --- | --- | --- | --- |
| `raw_fsa_authorities` | FSA `/Authorities` | local authority | `LocalAuthorityId` |
| `raw_fsa_business_types` | FSA `/BusinessTypes` | business type | `BusinessTypeId` |
| `raw_fsa_ratings` | FSA `/Ratings` | possible rating | `ratingId` |
| `raw_fsa_score_descriptors` | FSA `/ScoreDescriptors` | score value of a category | `Id` |
| `raw_fsa_establishments` | FSA `/Establishments` | establishment, per run | `FHRSID` |
| `raw_ofsted_schools` | Ofsted CSV | school, per run | `URN` |

```json
{
  "source": "fsa", "dataset": "establishments", "run_id": "20261120T020000", "sample": false,
  "ingested_at": "2026-11-20T02:03:14Z", "extract_date": "2026-11-20", "source_key": "1733057",
  "payload": {"FHRSID": 1733057, "BusinessName": "...", "RatingValue": "5", "RatingDate": "2026-05-12T00:00:00", "scores": {"Hygiene": 5}, "...": "as received"}
}
```

`sample` is `true` for the documents of a sample run (a test on a few authorities) and `false` for a full run; Ofsted runs are always full. Sample and full runs share the collections, so anything that reads them as the state of the source must keep only `sample: false`. The manifest must say which it is: the loader refuses a manifest without `sample`.

The loading is **append-only**: every run adds its documents and never changes the earlier ones, which is how the rating history builds up (the FSA API only exposes the latest rating of each business). A unique index on `(run_id, source_key)` makes a retry safe: documents already loaded for that run are skipped, not duplicated.

Needs `HOST_DATA_DIR` in `.env` (see [step 1](#1-configure-the-env-file)), the `project-mongo` connection ([step 4](#4-configure-the-project-database-connection-in-the-airflow-ui)), `DOCKER_NETWORK` set to the Compose network so `project-mongo` resolves, and the `fsa-scraper`, `ofsted-scraper` and `mongo-loader` images built ([step 5](#5-build-the-example-script-image)).

To run the steps by hand, outside of Airflow:

```bash
# scrape (writes data/landing/fsa/test/ and data/landing/ofsted/test/); by hand the FSA scraper
# makes a sample run by default, add --no-sample for a full run
# (--user: run as the owner of data/, i.e. AIRFLOW_UID, or the scraper cannot write there)
docker run --rm --user "$(id -u)" -v "$PWD/data:/data" fsa-scraper:latest --run-id test --la-ids 321,326
docker run --rm --user "$(id -u)" -v "$PWD/data:/data" ofsted-scraper:latest --run-id test

# load a run into Mongo (starts project-mongo, joins its network through the merged compose files)
docker compose up -d project-mongo
docker compose -f docker-compose.yaml -f docker-compose.scripts.yaml run --rm --no-deps -v "$PWD/data:/data:ro" -e MONGO_HOST=project-mongo -e MONGO_PORT=27017 -e MONGO_DB=project -e MONGO_USER=project -e MONGO_PASSWORD=devpassword mongo-loader --run-dir /data/landing/fsa/test

# look at the result
docker compose exec project-mongo mongosh -u project -p devpassword --authenticationDatabase admin project --eval "db.raw_fsa_establishments.countDocuments()"
```

A rerun with the same `--run-id` resumes: the authorities already completed in that folder (with the same page size) are reused, not fetched again. Delete the folder (`data/landing/fsa/test/`) for a fresh test.

**Documents loaded before the `sample` field existed.** Runs loaded before this field was added have no `sample`. The FSA ones were all sample runs (the three default authorities) and the Ofsted ones full runs; tag them once with:

```bash
docker compose exec project-mongo mongosh -u project -p devpassword --authenticationDatabase admin project --eval '
["raw_fsa_authorities", "raw_fsa_business_types", "raw_fsa_establishments"].forEach(c =>
  print(c, db[c].updateMany({sample: {$exists: false}}, {$set: {sample: true}}).modifiedCount));
print("raw_ofsted_schools", db.raw_ofsted_schools.updateMany({sample: {$exists: false}}, {$set: {sample: false}}).modifiedCount);'
```

Their `manifest.json` files have no `sample` either, so the loader refuses them (for instance when an old `load_into_mongo` task is cleared). Add it once with:

```bash
docker run --rm --user "$(id -u)" -v "$PWD/data:/data" --entrypoint python fsa-scraper:latest -c '
import json, pathlib
for path in pathlib.Path("/data/landing").glob("*/*/manifest.json"):
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if "sample" not in manifest:
        manifest["sample"] = manifest["source"] == "fsa"
        path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(path, manifest["sample"])'
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
