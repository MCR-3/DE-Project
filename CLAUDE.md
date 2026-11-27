# DE-Project

Apache Airflow 3.3.1 (CeleryExecutor) stack run with Docker Compose. The `example_*` DAGs and `example_script` are **examples and templates**. The two landing DAGs (`fsa_landing`, `ofsted_landing`) are the first real part of the project: they scrape the UK food hygiene (FSA) and school inspection (Ofsted) data and store it raw in MongoDB. **Staging** (MongoDB -> typed, checked tables in `project-db`, schemas `raw` and `staging`) is implemented by `docker_scripts/staging_loader` and `docker_scripts/postcode_scraper`, orchestrated by the `staging_load` DAG. The warehouse has only its empty DuckDB schema (`docker_scripts/warehouse_setup`); warehouse loading and reporting are not implemented. See `README.md` for the full setup guide and `docs/staging/` for staging.

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
│   ├── fsa_landing.py           DAG `fsa_landing` (weekly, Mondays 02:00 UTC): `prepare_fsa`, `fetch_fsa` (mapped, one per Fetcher), `finalize_fsa` (all docker_scripts/fsa_scraper, trigger rule `all_done` on finalize) then `load_into_mongo` (docker_scripts/mongo_loader), all via DockerOperator; params `sample`, `la_ids`, `fetchers`, `max_rps`
│   ├── ofsted_landing.py        DAG `ofsted_landing` (monthly, 14th): `scrape_ofsted` (docker_scripts/ofsted_scraper) then `load_into_mongo`; param `url`
│   ├── staging_load.py          DAG `staging_load` (Mondays 08:00 + 14th 09:00 UTC, after the landing DAGs): staging_loader steps, postcode_scraper + mongo_loader for the postcode run, all via DockerOperator; params `fsa_run`, `ofsted_run` (empty = every pending full run)
│   └── dag_names.py             The single place where DAG ids / display names are defined (the landing and staging DAGs import from it)
├── docker_scripts/
│   ├── example_script/          Template for a script that runs OUTSIDE Airflow's container (own image)
│   │   ├── main.py              argparse + DB_* env vars + logging + exit codes, no real logic
│   │   ├── requirements.txt     the script's own dependencies, independent from Airflow's
│   │   └── Dockerfile
│   ├── fsa_scraper/             Saves the FSA FHRS API responses as received (authorities, business types, ratings, score descriptors, establishments of every English authority or of a sample, pages of 200) in data/landing/fsa/<run>/ + manifest.json; sub-commands `prepare` / `fetch` / `finalize`, coordinated through the Authority queue in project-redis
│   │   └── main.py, authority_queue.py (all the Redis logic: queue, leases, rate limit), requirements.txt (requests, redis), Dockerfile
│   ├── ofsted_scraper/          Finds the newest "latest inspections" CSV on the gov.uk page and saves it untouched in data/landing/ofsted/<run>/schools.csv + manifest.json; no DB
│   │   └── main.py, requirements.txt (requests, beautifulsoup4), Dockerfile
│   ├── postcode_scraper/        Landing for postcodes.io: reads the postcodes of a landed Ofsted run and FSA run from Mongo, saves the bulk / terminated / reverse-geocoding responses as received in data/landing/postcodes/<run>/ + manifest.json (collections raw_postcodes, raw_terminated_postcodes, raw_reverse_postcodes), cached 90 days
│   │   └── main.py, requirements.txt (requests, pymongo), Dockerfile
│   ├── staging_loader/          Staging: sub-commands `init`, `copy-raw` (Mongo -> raw.* JSONB), `stage --step <step>` (runs one sql/ file); all cleaning, checks and unpivoting are SQL
│   │   └── main.py, sql/ (00-03 DDL/functions/views, 10-62 one file per step), seeds/ (region_map, district_la_map, ofsted_level, measure), requirements.txt (pymongo, psycopg2-binary), Dockerfile
│   ├── warehouse_setup/         Creates the empty DuckDB star schema in data/warehouse/warehouse.duckdb
│   └── mongo_loader/            Generic: reads a run folder's manifest.json and appends one envelope document per record to the collections it names (`project` db: raw_fsa_authorities, raw_fsa_business_types, raw_fsa_ratings, raw_fsa_score_descriptors, raw_fsa_establishments, raw_ofsted_schools)
│       └── main.py, requirements.txt (pymongo), Dockerfile
├── Dockerfile, requirements.txt Custom Airflow image (see Dependencies)
├── docker-compose.yaml          The Airflow stack + project-db + project-mongo
├── docker-compose.scripts.yaml  One service per docker_scripts/* image (build, and run by hand)
├── .env.example                 Copy to .env (git-ignored)
├── data/                        Mounted at /opt/airflow/data: files produced by DAGs (content git-ignored, folder kept by .gitkeep)
├── config/, logs/               Mounted into Airflow; generated at runtime and git-ignored
├── plugins/                     Mounted into Airflow; empty
├── docs/staging/                Staging docs (English, shared): README (build/run/rules), warehouse-contract, data-dictionary-changes, dq-results
├── docs/adr/                    Decisions (0001-0002 landing, 0003-0005 staging)
├── document/                    Git-ignored personal notes of a team member; not project documentation
└── docs/agents/                 Config for the engineering skills (see Agent skills below)
```

### Stack (`docker-compose.yaml`)

| Service | Image | Notes |
| --- | --- | --- |
| `airflow-apiserver` | custom (`build: .`) | UI/API on http://localhost:8080 (login `airflow` / `airflow` by default) |
| `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `airflow-triggerer` | custom | Share `x-airflow-common`; the worker is a Celery worker |
| `airflow-init` | custom | One-shot: migrates the metadata DB, creates the admin user |
| `postgres` | `postgres:16` | Airflow's own metadata DB (`airflow`/`airflow`/`airflow`), internal, never used for project data |
| `project-db` | `postgres:18` | The DB for relational project data, unrelated to Airflow (user/db `project`, password `devpassword`): the staging schemas `raw` and `staging`; `localhost:5433` from the host, `project-db:5432` from containers |
| `project-redis` | `redis:7.2-bookworm` | The FSA Authority queue (`docs/adr/0002`), unrelated to Airflow's `redis`; AOF persistence, password `devpassword`; `localhost:6380` from the host, `project-redis:6379` from containers |
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

Each run of a landing DAG has one folder, `data/landing/<source>/<run>/` (`<run>` = the run's logical date, `YYYYMMDDTHHMMSS`, identical for every task of the run and their retries). The scraper writes the raw files, then `manifest.json` **last**; a folder without it is an unfinished run and the loader refuses it. The manifest lists the datasets (name, target collection, format, key field, files) and gives each file a `fetch_date` (when the run received it, UTC) and a `published_date` (the date the source gives to that version: an FSA authority's `LastPublishedDate`, Ofsted's "as at" date, `null` for the FSA reference files; the API's `meta.extractDate` is only the request time and is not used), which is all the generic `mongo_loader` needs, and a required top-level `sample` flag.

A run is a **full run** (the whole source scope: every English authority for FSA; Ofsted runs are always full) or a **sample run** (`sample: true`, a test on a few FSA authorities). Both land in the same collections, so whatever reads them as the state of the source keeps only `sample: false`. Landing never filters by another source: matching FSA establishments to Ofsted schools is staging's job (`docs/adr/0001-landing-keeps-the-whole-source-scope.md`). The domain vocabulary is in `CONTEXT.md`. A full FSA run is shared by N Fetchers (DAG param `fetchers`, 1-8) through the Authority queue in `project-redis` (`docs/adr/0002`): `prepare` writes `plan.json` and queues every authority without a `done.json`, largest first; each Fetcher leases one authority at a time (2 min, renewed by a background thread, owner token checked), saves its pages with a `progress.json` so an expired lease resumes after the last page (from page 1 if the authority's `LastPublishedDate` changed), and exits when the queue and the leases are empty; `finalize` (trigger rule `all_done`) writes the manifest only if every authority of the plan has its `done.json`. **The files are the source of truth, Redis only coordinates**: never make a decision from Redis that the files contradict. The FSA request rate is capped globally (`max_rps`, shared by every Fetcher of every run) and a 429 pauses all of them.

Documents in MongoDB: `{source, dataset, run_id, sample, ingested_at, fetch_date, published_date, source_key, payload}` (the two dates copied from the record's file in the manifest), `payload` being the record exactly as received (CSV rows are dicts of strings; no typing, no cleaning, no England filter on Ofsted). Loading is **append-only**, each run adds its own documents and history builds up; a unique index on `(run_id, source_key)` makes a retried load skip what is already there.

The task succeeds or fails on the container's exit code (the script uses 0 ok / 1 failure / 2 bad config).

### The staging layer

Full description in `docs/staging/README.md` (and what the warehouse may rely on in `docs/staging/warehouse-contract.md`; decisions in `docs/adr/0003`-`0005`). The rules to keep when changing it:

- Python (`staging_loader/main.py`) only moves data (Mongo -> `raw.*`, streamed in batches); typing, cleaning, matching, checks and unpivoting are SQL files in `staging_loader/sql/`, one per step, each split at a `-- @@ load` line into two transactions: the checks (written to `staging.stg_dq_result` and committed even when they fail), then, only if no `run`-severity check failed, the load. Rows with a row-level problem go to `staging.stg_quarantine` (Postgres, not Mongo) in the load transaction; a run-level failure loads nothing and exits 1.
- Order per run: `reference` -> `establishments` -> `fsa_measures` -> `finalize_fsa` (FSA run); `schools` -> `ofsted_inspections` -> `finalize_ofsted` (Ofsted run); `geo` -> `finalize_postcodes` (postcode run, from `postcode_scraper`; `geo` must follow `schools` and `establishments`). `finalize_*` reconciles every source record, writes `staging.stg_load_log` and only then deletes the run's `raw.*` rows.
- Only full runs (`sample: false`) are staged by default; a sample run needs an explicit `--run-id`. Every step is idempotent, and a run staged again after a newer one never overwrites newer values.
- History is kept: inspection events accumulate, schools and businesses are never deleted (`last_seen_run_id` / `last_seen_date` show when they last appeared), the current FSA rating is the row seen last, not the latest date.
- Reference data is in `staging_loader/seeds/*.csv` (reloaded by `init`); `district_la_map.csv` (ONS district -> FSA `la_code`) is reviewed by hand, and a district missing from it fails the `geo` step. `la_code` is a `VARCHAR` everywhere.

## Dependencies

- **Host:** Docker (Compose v2) only, with at least 4 GB RAM and 2 CPUs for Docker. No local Python setup is needed; editor warnings such as "sqlalchemy could not be resolved" are expected, since the packages only exist inside the images.
- **Airflow image:** `apache/airflow:3.3.1` plus `requirements.txt` (`pandas`, `psycopg2-binary`, `sqlalchemy`, `apache-airflow-providers-docker`, `xlsx2csv`, whose CLI is called from a `BashOperator`). The `Dockerfile` installs them with `apache-airflow==${AIRFLOW_VERSION}` to keep Airflow's own version pinned. Rebuild with `docker compose up -d --build` after editing either file.
- **Script images:** each `docker_scripts/<name>/requirements.txt` is separate and must never be merged into the root one. `example_script` uses `numpy`, `pandas`, `psycopg2-binary`, `sqlalchemy` on `python:3.12-slim`; `staging_loader` uses `pymongo`, `psycopg2-binary`; `postcode_scraper` uses `requests`, `pymongo`.
- **Configuration (`.env`, copied from `.env.example`):** `AIRFLOW_UID`, `FERNET_KEY` (required), `DOCKER_GID` (group of `/var/run/docker.sock`, `stat -c %g`; needed on a Linux Docker Engine host or every DockerOperator task gets Permission denied, `0` on Docker Desktop), `DOCKER_NETWORK` (needed for scripts to reach `project-db`; usually `<folder-name>_default`, see `docker network ls`), `HOST_DATA_DIR` (required by the landing DAGs, see above), optional `SCRAPER_USER_AGENT`, optional `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`.
- **Airflow connection (created by hand in the UI, not in code):** `project-db` (host `project-db`, schema/login `project`, password `devpassword`, port 5432), used by every DAG that touches relational data, `project-mongo` (type Generic, host `project-mongo`, schema/login `project`, password `devpassword`, port 27017), which both landing DAGs read with `Connection.get` at parse time, so they do not load until it exists, and `project-redis` (type Generic, host `project-redis`, password `devpassword`, port 6379), read the same way by `fsa_landing`. There is deliberately no connection to Airflow's metadata DB. Exact values are in the README, step 4.

## How to run

```bash
cp .env.example .env                                          # then fill in FERNET_KEY
docker compose up airflow-init                                # once, waits for exit code 0
docker compose up -d                                          # start the stack (add --build after editing Dockerfile/requirements.txt)
docker compose -f docker-compose.scripts.yaml build           # build the script images (needed before the example_docker_script DAG)
docker compose ps                                             # health check
docker compose down                                           # stop (-v also deletes the DB volumes, so run airflow-init again)
```

Then open http://localhost:8080, un-pause the DAG (DAGs are created paused), and trigger it. `example_docker_script` takes an `n_rows` parameter, `fsa_landing` a `sample` checkbox (unticked by default: a full run of every English authority; ticked: a sample run of the authorities in `la_ids`, comma-separated FSA `LocalAuthorityId` values, default `321,326,397`), `fetchers` (1-8, default 4) and `max_rps` (default 4), `ofsted_landing` an optional `url` (a specific CSV instead of the newest release), `staging_load` optional `fsa_run` / `ofsted_run` (one run to stage, e.g. a sample run; empty stages every full run not finalized yet). Build the `staging-loader` and `postcode-scraper` images before `staging_load`.

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
- **The FSA scraper on its own** (needs network access and `project-redis`, no Airflow): `docker compose up -d project-redis`, then run the three steps with `docker run --rm --user "$(id -u)" --network de-project_default -v "$PWD/data:/data" -e REDIS_HOST=project-redis -e REDIS_PASSWORD=devpassword fsa-scraper:latest <step> --run-id test` (`--user`: the owner of `data/`, or the scraper cannot write there; `--network`: the Compose network). `prepare --run-id test --la-ids 321` makes a sample run (the default by hand; `--no-sample` is a full run) and writes the reference files and `plan.json`; `fetch --run-id test --fetcher 1` (start several at once to see them share the queue; `docker kill` one mid-authority and another resumes it about 2 minutes later) writes `establishments/la_321/page_0001.json` and `done.json`; `finalize --run-id test` writes `manifest.json` with `"sample": true`, or exits 1 listing the unfinished authorities. `prepare` with `--la-ids 197` (Aberdeen City, Scotland), `--la-ids all` or `--page-size 500` logs an error and exits 2. Rerunning `prepare` with the same `--run-id` queues only the authorities without `done.json` (delete the folder for a fresh run). Watch the queue with `docker compose exec project-redis redis-cli -a devpassword lrange fsa:test:queue 0 -1`.
- **The Ofsted scraper on its own** (needs network access, no Airflow): `docker run --rm --user "$(id -u)" -v "$PWD/data:/data" ofsted-scraper:latest --run-id test` writes `data/landing/ofsted/test/schools.csv` (about 22,000 rows) and `manifest.json`; add `--url <csv url>` for a specific month.
- **The Mongo loader against `project-mongo`:** `docker compose up -d project-mongo`, then run the `mongo-loader` service with both compose files merged, `-v "$PWD/data:/data:ro"`, the `MONGO_HOST/PORT/DB/USER/PASSWORD` variables and `--run-dir /data/landing/fsa/test` (add `--no-deps` so it does not start the Airflow stack); check with `docker compose exec project-mongo mongosh -u project -p devpassword --authenticationDatabase admin project --eval "db.raw_fsa_establishments.countDocuments()"`. Loading the same run twice must not change the count (the second log line says everything is already loaded); a folder without `manifest.json`, or a manifest without `sample`, exits 2. Remove test data afterwards (`db.<collection>.deleteMany({run_id: "test"})`).
- **Staging** (needs `project-db`, `project-mongo` and landed runs; `postcode_scraper` also needs network access): follow the command sequence in `docs/staging/README.md` with `--run-id` set to the test runs, then check `SELECT * FROM staging.v_dq_report` and the row counts. Running any step twice must not change the counts; a step whose earlier step did not pass exits 1. To test a failure path, insert a modified copy of a run into `raw.*` under another run id, stage it, then delete it (and its `stg_dq_result` / `stg_quarantine` rows).
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
