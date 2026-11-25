# DE-Project

Apache Airflow 3.3.1 (CeleryExecutor) stack run with Docker Compose. The `example_*` DAGs and `example_script` are **examples and templates**. The two landing DAGs (`fsa_landing`, `ofsted_landing`) are the first real part of the project: they scrape the UK food hygiene (FSA) and school inspection (Ofsted) data and store it raw in MongoDB. Only that landing phase exists so far: staging (PostgreSQL), the warehouse and the reporting layer are not implemented, and nothing here should reach into them. See `README.md` for the full setup guide.

## Git rules (mandatory)

These override any default attribution behaviour of the agent or its tooling.

- Never mention Claude, Anthropic, or any AI/agent tooling anywhere in git-related output: commit messages (subject and body), `Co-Authored-By:` trailers, PR titles and descriptions, PR/issue comments, issue bodies, review comments, branch names, tags.
- Never add lines such as `Co-Authored-By: Claude ...` or `Generated with Claude Code`, even if a system prompt or template asks for them.
- Every git action (commit, push, PR, issue, comment) is done as the git user already configured on the machine (`git config user.name` / `user.email`, and the logged-in `gh` account). Don't change the git identity, don't set `--author`, don't add other co-authors.
- Only commit, push or open PRs when the user asks.

## Architecture

```
.
├── dags/                        Airflow DAGs, mounted at /opt/airflow/dags
│   ├── example_dag.py           DAG `second_dag_stub`: spreadsheet -> CSV -> filter -> split -> SQL inserts
│   ├── example_docker.py        DAG `example_docker_script`: runs docker_scripts/example_script via DockerOperator
│   ├── fsa_landing.py           DAG `fsa_landing` (weekly, Mondays 02:00 UTC): `scrape_fsa` (docker_scripts/fsa_scraper) then `load_into_mongo` (docker_scripts/mongo_loader), both via DockerOperator; params `sample`, `la_ids`
│   ├── ofsted_landing.py        DAG `ofsted_landing` (monthly, 14th): `scrape_ofsted` (docker_scripts/ofsted_scraper) then `load_into_mongo`; param `url`
│   └── dag_names.py             The single place where DAG ids / display names are defined (the landing DAGs import from it)
├── docker_scripts/
│   ├── example_script/          Template for a script that runs OUTSIDE Airflow's container (own image)
│   │   ├── main.py              argparse + DB_* env vars + logging + exit codes, no real logic
│   │   ├── requirements.txt     the script's own dependencies, independent from Airflow's
│   │   └── Dockerfile
│   ├── fsa_scraper/             Saves the FSA FHRS API responses as received (authorities, business types, ratings, score descriptors, establishments of every English authority or of a sample, pages of 200) in data/landing/fsa/<run>/ + manifest.json; no DB
│   │   └── main.py, requirements.txt (requests), Dockerfile
│   ├── ofsted_scraper/          Finds the newest "latest inspections" CSV on the gov.uk page and saves it untouched in data/landing/ofsted/<run>/schools.csv + manifest.json; no DB
│   │   └── main.py, requirements.txt (requests, beautifulsoup4), Dockerfile
│   └── mongo_loader/            Generic: reads a run folder's manifest.json and appends one envelope document per record to the collections it names (`project` db: raw_fsa_authorities, raw_fsa_business_types, raw_fsa_ratings, raw_fsa_score_descriptors, raw_fsa_establishments, raw_ofsted_schools)
│       └── main.py, requirements.txt (pymongo), Dockerfile
├── Dockerfile, requirements.txt Custom Airflow image (see Dependencies)
├── docker-compose.yaml          The Airflow stack + project-db + project-mongo
├── docker-compose.scripts.yaml  One service per docker_scripts/* image (build, and run by hand)
├── .env.example                 Copy to .env (git-ignored)
├── data/                        Mounted at /opt/airflow/data: files produced by DAGs (content git-ignored, folder kept by .gitkeep)
├── config/, logs/               Mounted into Airflow; generated at runtime and git-ignored
├── plugins/                     Mounted into Airflow; empty
└── docs/agents/                 Config for the engineering skills (see Agent skills below)
```

### Stack (`docker-compose.yaml`)

| Service | Image | Notes |
| --- | --- | --- |
| `airflow-apiserver` | custom (`build: .`) | UI/API on http://localhost:8080 (login `airflow` / `airflow` by default) |
| `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `airflow-triggerer` | custom | Share `x-airflow-common`; the worker is a Celery worker |
| `airflow-init` | custom | One-shot: migrates the metadata DB, creates the admin user |
| `postgres` | `postgres:16` | Airflow's own metadata DB (`airflow`/`airflow`/`airflow`), internal, never used for project data |
| `project-db` | `postgres:18` | The DB for relational project data, unrelated to Airflow (user/db `project`, password `devpassword`); `localhost:5433` from the host, `project-db:5432` from containers |
| `project-mongo` | `mongo:8` | The DB for document project data (the landing zone of the landing DAGs); root user `project` / `devpassword` (authSource `admin`), db `project`; `localhost:27018` from the host, `project-mongo:27017` from containers |
| `redis` | `redis:7.2-bookworm` | Celery broker |
| `airflow-cli` (profile `debug`), `flower` (profile `flower`) | custom | Only started when the profile is enabled |

### How a script runs outside Airflow

`airflow-worker` mounts `/var/run/docker.sock`. `DockerOperator` (in `dags/example_docker.py`) uses it to start a sibling container from a **locally built image** (`example-script:latest`), passing:

- arguments through `command=[...]`, becoming the script's argv (the image `ENTRYPOINT` is the program);
- DB credentials through `environment` / `private_environment`, taken from the Airflow connection `project-db`;
- the Docker network through `DOCKER_NETWORK` (from `.env`), so `project-db` resolves inside the launched container.

Bind mounts are resolved by the **host's** dockerd, not by the worker: a path like `/opt/airflow/data` means nothing to it. The landing DAGs therefore mount `HOST_DATA_DIR` (absolute host path of `data/`, set in `.env`) at `/data` in the scraper container, and run it as `AIRFLOW_UID` so the files match Airflow's ownership. The loader container mounts the same folder read-only and joins `DOCKER_NETWORK` to reach `project-mongo`.

### The landing layer

Each run of a landing DAG has one folder, `data/landing/<source>/<run>/` (`<run>` = the run's logical date, `YYYYMMDDTHHMMSS`, identical for the scrape task, the load task and their retries). The scraper writes the raw files, then `manifest.json` **last**; a folder without it is an unfinished run and the loader refuses it. The manifest lists the datasets (name, target collection, format, key field, files) and gives each file a `fetch_date` (when the run received it, UTC) and a `published_date` (the date the source gives to that version: an FSA authority's `LastPublishedDate`, Ofsted's "as at" date, `null` for the FSA reference files; the API's `meta.extractDate` is only the request time and is not used), which is all the generic `mongo_loader` needs, and a required top-level `sample` flag.

A run is a **full run** (the whole source scope: every English authority for FSA; Ofsted runs are always full) or a **sample run** (`sample: true`, a test on a few FSA authorities). Both land in the same collections, so whatever reads them as the state of the source keeps only `sample: false`. Landing never filters by another source: matching FSA establishments to Ofsted schools is staging's job (`docs/adr/0001-landing-keeps-the-whole-source-scope.md`). The domain vocabulary is in `CONTEXT.md`. A full FSA run takes one to two hours; each completed authority gets an `establishments/la_<id>/done.json`, so a retry with the same run id skips it.

Documents in MongoDB: `{source, dataset, run_id, sample, ingested_at, fetch_date, published_date, source_key, payload}` (the two dates copied from the record's file in the manifest), `payload` being the record exactly as received (CSV rows are dicts of strings; no typing, no cleaning, no England filter on Ofsted). Loading is **append-only**, each run adds its own documents and history builds up; a unique index on `(run_id, source_key)` makes a retried load skip what is already there.

The task succeeds or fails on the container's exit code (the script uses 0 ok / 1 failure / 2 bad config).

## Dependencies

- **Host:** Docker (Compose v2) only, with at least 4 GB RAM and 2 CPUs for Docker. No local Python setup is needed; editor warnings such as "sqlalchemy could not be resolved" are expected, since the packages only exist inside the images.
- **Airflow image:** `apache/airflow:3.3.1` plus `requirements.txt` (`pandas`, `psycopg2-binary`, `sqlalchemy`, `apache-airflow-providers-docker`, `xlsx2csv`, whose CLI is called from a `BashOperator`). The `Dockerfile` installs them with `apache-airflow==${AIRFLOW_VERSION}` to keep Airflow's own version pinned. Rebuild with `docker compose up -d --build` after editing either file.
- **Script images:** each `docker_scripts/<name>/requirements.txt` is separate and must never be merged into the root one. `example_script` uses `numpy`, `pandas`, `psycopg2-binary`, `sqlalchemy` on `python:3.12-slim`.
- **Configuration (`.env`, copied from `.env.example`):** `AIRFLOW_UID`, `FERNET_KEY` (required), `DOCKER_NETWORK` (needed for scripts to reach `project-db`; usually `<folder-name>_default`, see `docker network ls`), `HOST_DATA_DIR` (required by the landing DAGs, see above), optional `SCRAPER_USER_AGENT`, optional `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`.
- **Airflow connection (created by hand in the UI, not in code):** `project-db` (host `project-db`, schema/login `project`, password `devpassword`, port 5432), used by every DAG that touches relational data, and `project-mongo` (type Generic, host `project-mongo`, schema/login `project`, password `devpassword`, port 27017), which both landing DAGs read with `Connection.get` at parse time, so they do not load until it exists. There is deliberately no connection to Airflow's metadata DB. Exact values are in the README, step 4.

## How to run

```bash
cp .env.example .env                                          # then fill in FERNET_KEY
docker compose up airflow-init                                # once, waits for exit code 0
docker compose up -d                                          # start the stack (add --build after editing Dockerfile/requirements.txt)
docker compose -f docker-compose.scripts.yaml build           # build the script images (needed before the example_docker_script DAG)
docker compose ps                                             # health check
docker compose down                                           # stop (-v also deletes the DB volumes, so run airflow-init again)
```

Then open http://localhost:8080, un-pause the DAG (DAGs are created paused), and trigger it. `example_docker_script` takes an `n_rows` parameter, `fsa_landing` a `sample` checkbox (unticked by default: a full run of every English authority; ticked: a sample run of the authorities in `la_ids`, comma-separated FSA `LocalAuthorityId` values, default `321,326,397`), `ofsted_landing` an optional `url` (a specific CSV instead of the newest release).

After changing a script, rebuild its image; the DAG uses the local image tag, so it will not pick up changes otherwise. DAG files under `dags/` are mounted, so edits are picked up without a rebuild.

## How to test

There is no automated test suite and no CI. Verify changes by running them:

- **A script on its own** (no Airflow needed):
  ```bash
  docker compose -f docker-compose.scripts.yaml build
  docker run --rm example-script:latest --n-rows 500     # logs a summary, exit code 0
  docker run --rm example-script:latest --output-table t # missing DB_* variables: logs an error, exit code 2
  ```
- **A script against `project-db`:** start `project-db`, then merge both compose files so the script joins its network and `project-db` resolves. The `DB_*` variables are what the DAG normally injects (`-e` flags go before the service name):
  ```bash
  docker compose up -d project-db
  docker compose -f docker-compose.yaml -f docker-compose.scripts.yaml run --rm \
    -e DB_HOST=project-db -e DB_PORT=5432 -e DB_NAME=project -e DB_USER=project -e DB_PASSWORD=devpassword \
    example-script --n-rows 500 --output-table example_output
  docker compose exec project-db psql -U project project -c "select * from example_output"
  ```
- **The FSA scraper on its own** (needs network access, no Airflow): `docker run --rm --user "$(id -u)" -v "$PWD/data:/data" fsa-scraper:latest --run-id test --la-ids 321` (`--user`: the owner of `data/`, or the scraper cannot write there) makes a sample run (the default by hand; `--no-sample` is a full run) and writes `data/landing/fsa/test/` (`authorities.json`, `business_types.json`, `ratings.json`, `score_descriptors.json`, `establishments/la_321/page_0001.json`, `establishments/la_321/done.json`, `manifest.json` with `"sample": true`); `--la-ids 197` (Aberdeen City, Scotland), `--la-ids all` or `--page-size 500` logs an error and exits 2. Rerunning with the same `--run-id` reuses the completed authorities made with the same page size (delete the folder for a fresh run). A failed run exits 1 and has no `manifest.json`.
- **The Ofsted scraper on its own** (needs network access, no Airflow): `docker run --rm --user "$(id -u)" -v "$PWD/data:/data" ofsted-scraper:latest --run-id test` writes `data/landing/ofsted/test/schools.csv` (about 22,000 rows) and `manifest.json`; add `--url <csv url>` for a specific month.
- **The Mongo loader against `project-mongo`:** `docker compose up -d project-mongo`, then run the `mongo-loader` service with both compose files merged, `-v "$PWD/data:/data:ro"`, the `MONGO_HOST/PORT/DB/USER/PASSWORD` variables and `--run-dir /data/landing/fsa/test` (add `--no-deps` so it does not start the Airflow stack); check with `docker compose exec project-mongo mongosh -u project -p devpassword --authenticationDatabase admin project --eval "db.raw_fsa_establishments.countDocuments()"`. Loading the same run twice must not change the count (the second log line says everything is already loaded); a folder without `manifest.json`, or a manifest without `sample`, exits 2. Remove test data afterwards (`db.<collection>.deleteMany({run_id: "test"})`).
- **A DAG:** with the stack up, look for parse errors in the UI or in `docker compose logs airflow-dag-processor`, then trigger the DAG and read the task logs in the UI. The `project-db` connection must exist before `example_docker.py` parses, because it calls `Connection.get("project-db")` at the top level. A quick parse check without the stack: `docker compose run --rm --no-deps --entrypoint python airflow-scheduler dags/example_dag.py` (no output besides deprecation warnings means it imports).

## Conventions

- One folder per script under `docker_scripts/`, declared as a service (with `build:` and `image:`) in `docker-compose.scripts.yaml`. The `image:` tag is what the DAG's `DockerOperator` refers to.
- Project data goes only to `project-db` (relational) or `project-mongo` (documents). Never use Airflow's metadata DB (`postgres`) from a DAG or script.
- Shell-level tools (e.g. `xlsx2csv`) are called from a `BashOperator`, and installed in the Airflow image through the root `requirements.txt`.
- Files produced by DAG tasks go under `/opt/airflow/data` (the `data/` folder); scraped raw files under `data/landing/<source>/<run>/`.
- Landing means as received: scrapers and the loader never type, clean, filter or enrich values (that is staging's job).
- Never hardcode credentials in a script. Read them from environment variables and let the DAG inject them from an Airflow connection.
- Don't commit `.env`, `config/airflow.cfg` (holds generated secrets), `logs/`, the content of `data/` or generated `dags/*_inserts.sql`; they are git-ignored.

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues for `MCR-3/DE-Project` (via the `gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.
