# Staging

Staging turns the landed MongoDB documents (see the landing DAGs) into typed, checked PostgreSQL tables in
`project-db`, ready for the warehouse. It is implemented by one script image, `docker_scripts/staging_loader`
(and, for the geocoding, the landing script `docker_scripts/postcode_scraper`). It is not wired into Airflow
yet (TODO: a `staging_load` DAG).

| Document | For |
| --- | --- |
| this file | how staging is built and run |
| [`warehouse-contract.md`](warehouse-contract.md) | what the warehouse can rely on when it reads `staging.*` |
| [`data-dictionary-changes.md`](data-dictionary-changes.md) | corrections to section 6 of the Deliverable 1 report |
| [`dq-results.md`](dq-results.md) | data-quality results (report evidence) |
| [`../adr/0003`](../adr/0003-schools-matched-to-fsa-districts-by-postcode.md), [`0004`](../adr/0004-staging-quarantines-rows-and-fails-whole-runs.md), [`0005`](../adr/0005-staging-keeps-history-and-tracks-last-seen.md) | the main decisions and why |

## Layout

```
docker_scripts/staging_loader/
├── main.py            init / copy-raw / stage (Python only moves data; all cleaning is SQL)
├── sql/
│   ├── 00_schemas.sql, 01_tables.sql, 02_functions.sql, 03_views.sql   applied by `init`
│   ├── 10_reference.sql        step 1  FSA authorities, business types, ratings, score descriptors
│   ├── 20_schools.sql          step 2  Ofsted schools
│   ├── 30_establishments.sql   step 3  FSA food businesses and their ratings
│   ├── 40_geo.sql              step 4  postcodes.io: school coordinates, district, la_code; missing business coordinates
│   ├── 50_ofsted_inspections.sql, 51_fsa_measures.sql   step 5  inspection events and one row per measure
│   └── 60_finalize_fsa.sql, 61_finalize_ofsted.sql, 62_finalize_postcodes.sql   step 6  reconcile, log, delete raw
└── seeds/             reference data kept in git, reloaded by `init`
    ├── region_map.csv        FSA / postcodes.io / Ofsted region names -> Ofsted name
    ├── district_la_map.csv   ONS (GSS) district code -> FSA la_code (296 rows, with match_method)
    ├── ofsted_level.csv      Ofsted grades and labels -> numeric level (incl. implied grade of ungraded outcomes)
    └── measure.csv           the measures unpivoted into stg_inspection_measure (source of Dim_Measure)
```

PostgreSQL schemas in `project-db`:

- `raw`: one table per MongoDB landing collection (`raw_fsa_establishments` -> `raw.fsa_establishments`), the
  envelope fields plus `payload` as JSONB. A temporary buffer: a run's rows are deleted once the run is
  finalized (MongoDB keeps the landed documents).
- `staging`: the `stg_*` tables, the bookkeeping tables (`stg_dq_result`, `stg_quarantine`, `stg_load_log`) and
  the view `v_dq_report`.

## Running it

Environment (injected from the Airflow connections `project-db` and `project-mongo`): `DB_HOST`, `DB_PORT`,
`DB_NAME`, `DB_USER`, `DB_PASSWORD`, `MONGO_HOST`, `MONGO_PORT`, `MONGO_DB`, `MONGO_USER`, `MONGO_PASSWORD`.
Exit code: 0 ok, 1 failure (including a failed run-level check), 2 bad config.

```bash
docker compose up -d project-db project-mongo
docker compose -f docker-compose.scripts.yaml build staging-loader postcode-scraper
E="--network de-project_default -e DB_HOST=project-db -e DB_PORT=5432 -e DB_NAME=project -e DB_USER=project \
   -e DB_PASSWORD=devpassword -e MONGO_HOST=project-mongo -e MONGO_PORT=27017 -e MONGO_DB=project \
   -e MONGO_USER=project -e MONGO_PASSWORD=devpassword"
L="docker run --rm $E staging-loader:latest"

$L init                                              # schemas, tables, functions, views, seeds (idempotent)

# FSA run R and Ofsted run O (landed in MongoDB by the landing DAGs)
$L copy-raw --source fsa --run-id R
$L copy-raw --source ofsted --run-id O
$L stage --step reference --run-id R                 # step 1
$L stage --step schools --run-id O                   # step 2
$L stage --step establishments --run-id R            # step 3

# postcode run P: postcodes.io lookups for the schools of O and the businesses of R without coordinates
docker run --rm --user "$(id -u)" --network de-project_default -v "$PWD/data:/data" \
  -e MONGO_HOST=project-mongo -e MONGO_PORT=27017 -e MONGO_DB=project -e MONGO_USER=project \
  -e MONGO_PASSWORD=devpassword postcode-scraper:latest --run-id P --fsa-run R --ofsted-run O
# then load data/landing/postcodes/P with mongo_loader, like any landed run
$L copy-raw --source postcodes --run-id P
$L stage --step geo --run-id P                       # step 4

$L stage --step ofsted_inspections --run-id O        # step 5
$L stage --step fsa_measures --run-id R
$L stage --step finalize_fsa --run-id R              # step 6
$L stage --step finalize_ofsted --run-id O
$L stage --step finalize_postcodes --run-id P
```

Without `--run-id`, `copy-raw` and `stage` take every full run (`sample: false`) of the source that is not in
`staging.stg_load_log` yet, oldest first. Sample runs are only staged when named with `--run-id`. Always pass
`--run-id` to the postcode steps: a postcode run whose lookups were all cached has no document and is never
listed as pending.

## Rules every step follows

- **Two transactions per run**, split at the `-- @@ load` line of the step's SQL file:
  1. *checks*: every check is written to `staging.stg_dq_result` and committed, failed or not;
  2. *load*: runs only if no run-level check failed. Rows with a row-level problem go to
     `staging.stg_quarantine` (with the source record and the reasons) in the same transaction as the load.
- **Check severities** (`stg_dq_result.severity`): `run` fails the step and nothing is loaded (exit 1); `row`
  counts the rows quarantined for one reason; `info` is a number kept for the report.
- **Run-level failures**: empty source; fewer rows than 90 % of the previous full run that passed (full runs
  only); more than 5 % of the rows quarantined; a step that needs an earlier step of the same run that did
  not pass; for step 4, a school district missing from `district_la_map.csv` or more than 5 % of schools
  without `la_code`; for step 6, a source record that is neither loaded, quarantined, superseded by a newer
  run nor excluded on purpose.
- **Idempotent**: staging the same run twice gives the same result. A run staged again after a newer one
  never overwrites the newer values.
- **Landing stays as received**: postcodes are normalised only to build postcodes.io requests; the stored
  responses are untouched.

## Open questions for landing

1. **Asset outlets.** The planned `staging_load` DAG would be scheduled on Airflow 3 Assets emitted by the
   `load_into_mongo` tasks of `fsa_landing` and `ofsted_landing` (`outlets=[Asset(...)]`). Agree who changes
   the landing DAGs.
2. **Partly loaded runs.** If `mongo_loader` fails half-way, MongoDB holds part of a run and staging cannot
   tell it from a complete one (it lists runs from MongoDB). Asset scheduling avoids it in Airflow; a
   completion marker written by the loader (for example a `landing_runs` collection) would make it explicit.
3. **Duplicate keys inside one run.** The unique index `(run_id, source_key)` makes `mongo_loader` count a
   second record with the same key in the same file as "already loaded" and drop it. Staging cannot see such
   duplicates (its duplicate checks are always 0). No duplicates exist today; counting them separately in
   the loader log would make it visible.
4. **`postcode_scraper` reads MongoDB.** It is the first landing script whose input comes from MongoDB (the
   landed postcodes) rather than an external source. It writes a normal run folder and manifest
   (`source: "postcodes"`, collections `raw_postcodes`, `raw_terminated_postcodes`, `raw_reverse_postcodes`)
   loaded by the unchanged `mongo_loader`; with everything cached it writes a manifest with `datasets: []`.
