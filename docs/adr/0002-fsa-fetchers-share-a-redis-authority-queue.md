# FSA Fetchers share an Authority queue in Redis, one container per Fetcher

A full FSA run fetches about 2,600 pages from some 300 English authorities, one after the other, in one to two hours, mostly spent waiting on the API. We split it across N Fetchers (an Airflow DAG parameter, 1 to 8), each in its own container launched by a mapped `DockerOperator`, which take authorities from an Authority queue held in a dedicated `project-redis` (persisted to disk with AOF). The queue is filled largest authority first, so the run does not end waiting on one big authority. Redis is only the coordination layer: what is finished is always decided from the landed files (pages and `done.json`), so the queue can be rebuilt from them at any time.

## Considered Options

- **Airflow dynamic task mapping, one task per authority**: no new service and native per-authority retries, but about 300 containers and task instances per run. Rejected; learning Redis as a work queue was also part of the goal.
- **One container running N Fetcher threads with an in-memory queue**: simplest, but Fetchers could not fail or be scaled independently, and the run's state would be invisible from outside. Rejected.
- **Reusing Airflow's Redis (the Celery broker)**: mixes project data with Airflow's infrastructure, as the project already forbids for Airflow's Postgres. Rejected.

## Consequences

- The request rate to the FSA is capped **globally** (`max_rps`, shared by every Fetcher and every concurrent run), and a 429 puts every Fetcher on a shared cooldown, so adding Fetchers never adds load on the FSA beyond the cap.
- A Fetcher holds an authority under a renewable lease with an owner token; an expired lease is put back in the queue by any Fetcher, and a Fetcher only exits once the queue and the in-progress set are both empty. An interrupted authority resumes from its last saved page, unless its published date changed in between, in which case it is fetched again from page 1.
- An authority that fails three times is set aside without stopping the others; `finalize_fsa` (trigger rule `all_done`) writes the manifest only if every authority has its `done.json`.
- The scraper can no longer run without `project-redis`, including by hand.
