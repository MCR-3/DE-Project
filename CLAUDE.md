# DE-Project

Apache Airflow 3.3.1 (CeleryExecutor) stack run with Docker Compose. The DAGs and the script in this repo are **examples and templates**, not a real data pipeline. See `README.md` for the full setup guide.

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
│   └── example_docker.py        DAG `example_docker_script`: runs docker_scripts/example_script via DockerOperator
├── docker_scripts/
│   └── example_script/          Template for a script that runs OUTSIDE Airflow's container (own image)
│       ├── main.py              argparse + DB_* env vars + logging + exit codes, no real logic
│       ├── requirements.txt     the script's own dependencies, independent from Airflow's
│       └── Dockerfile
├── Dockerfile, requirements.txt Custom Airflow image (see Dependencies)
├── docker-compose.yaml          The Airflow stack + project-db
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
| `project-db` | `postgres:18` | The only DB for project data, unrelated to Airflow (user/db `project`, password `devpassword`); `localhost:5433` from the host, `project-db:5432` from containers |
| `redis` | `redis:7.2-bookworm` | Celery broker |
| `airflow-cli` (profile `debug`), `flower` (profile `flower`) | custom | Only started when the profile is enabled |

### How a script runs outside Airflow

`airflow-worker` mounts `/var/run/docker.sock`. `DockerOperator` (in `dags/example_docker.py`) uses it to start a sibling container from a **locally built image** (`example-script:latest`), passing:

- arguments through `command=[...]`, becoming the script's argv (the image `ENTRYPOINT` is the program);
- DB credentials through `environment` / `private_environment`, taken from the Airflow connection `project-db`;
- the Docker network through `DOCKER_NETWORK` (from `.env`), so `project-db` resolves inside the launched container.

The task succeeds or fails on the container's exit code (the script uses 0 ok / 1 failure / 2 bad config).

## Dependencies

- **Host:** Docker (Compose v2) only, with at least 4 GB RAM and 2 CPUs for Docker. No local Python setup is needed; editor warnings such as "sqlalchemy could not be resolved" are expected, since the packages only exist inside the images.
- **Airflow image:** `apache/airflow:3.3.1` plus `requirements.txt` (`pandas`, `psycopg2-binary`, `sqlalchemy`, `apache-airflow-providers-docker`, `xlsx2csv`, whose CLI is called from a `BashOperator`). The `Dockerfile` installs them with `apache-airflow==${AIRFLOW_VERSION}` to keep Airflow's own version pinned. Rebuild with `docker compose up -d --build` after editing either file.
- **Script images:** each `docker_scripts/<name>/requirements.txt` is separate and must never be merged into the root one. `example_script` uses `numpy`, `pandas`, `psycopg2-binary`, `sqlalchemy` on `python:3.12-slim`.
- **Configuration (`.env`, copied from `.env.example`):** `AIRFLOW_UID`, `FERNET_KEY` (required), `DOCKER_NETWORK` (needed for scripts to reach `project-db`; usually `<folder-name>_default`, see `docker network ls`), optional `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`.
- **Airflow connection (created by hand in the UI, not in code):** `project-db` (host `project-db`, schema/login `project`, password `devpassword`, port 5432), used by every DAG that touches data. There is deliberately no connection to Airflow's metadata DB. Exact values are in the README, step 4.

## How to run

```bash
cp .env.example .env                                          # then fill in FERNET_KEY
docker compose up airflow-init                                # once, waits for exit code 0
docker compose up -d                                          # start the stack (add --build after editing Dockerfile/requirements.txt)
docker compose -f docker-compose.scripts.yaml build           # build the script images (needed before the example_docker_script DAG)
docker compose ps                                             # health check
docker compose down                                           # stop (-v also deletes the DB volumes, so run airflow-init again)
```

Then open http://localhost:8080, un-pause the DAG (DAGs are created paused), and trigger it. `example_docker_script` takes an `n_rows` parameter.

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
- **A DAG:** with the stack up, look for parse errors in the UI or in `docker compose logs airflow-dag-processor`, then trigger the DAG and read the task logs in the UI. The `project-db` connection must exist before `example_docker.py` parses, because it calls `Connection.get("project-db")` at the top level. A quick parse check without the stack: `docker compose run --rm --no-deps --entrypoint python airflow-scheduler dags/example_dag.py` (no output besides deprecation warnings means it imports).

## Conventions

- One folder per script under `docker_scripts/`, declared as a service (with `build:` and `image:`) in `docker-compose.scripts.yaml`. The `image:` tag is what the DAG's `DockerOperator` refers to.
- Project data goes only to `project-db`. Never use Airflow's metadata DB (`postgres`) from a DAG or script.
- Shell-level tools (e.g. `xlsx2csv`) are called from a `BashOperator`, and installed in the Airflow image through the root `requirements.txt`.
- Files produced by DAG tasks go under `/opt/airflow/data` (the `data/` folder).
- Never hardcode credentials in a script. Read them from environment variables and let the DAG inject them from an Airflow connection.
- Don't commit `.env`, `config/airflow.cfg` (holds generated secrets), `logs/`, the content of `data/` or generated `dags/*_inserts.sql`; they are git-ignored.

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues for `MCR-3/DE-Project` (via the `gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.
