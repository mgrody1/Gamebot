# Survivor Prediction Warehouse

This repository builds a Medallion-style data warehouse for CBS’s *Survivor*.  
It ingests the open-source [`survivoR`](https://github.com/doehm/survivoR) datasets, stores the raw data in a **bronze** schema, curates analytics-ready tables in a **silver** schema, and provides automation via Apache Airflow.  
All code runs locally with Pipenv or fully containerised via Docker Compose.

---

## 1. Requirements

- Python 3.11
- Pipenv (`pip install pipenv`)
- PostgreSQL 15+ (use the included Docker service or your own instance)
- Optional tooling
  - Docker/Docker Compose v2 (for the container stack)
  - pre-commit (`pip install pre-commit`) for repository lint checks
- Ensure the Postgres user used for ingestion can create schemas and the `uuid-ossp` extension (required on the first run).

---

## 2. Quick Start (Local Pipenv Workflow)

1. **Clone and install dependencies**
   ```bash
   git clone <repo-url>
   cd survivor_prediction
   pipenv install
   ```

2. **Select an environment and create `.env`**
   ```bash
   pipenv run python scripts/switch_env.py dev --from-example
   ```
   Edit `.env` if needed:
   - Use `DB_HOST=localhost` when connecting to a locally managed Postgres instance.
   - Use `DB_HOST=warehouse-db` when you plan to rely on the Docker Postgres service.
   - Set unique database names for development vs production (e.g., `survivor_dw_dev`, `survivor_dw`).

3. **Create or verify the database(s)**
   - Local Postgres example:
     ```bash
     createdb survivor_dw_dev
     createuser survivor_dev --pwprompt
     ```
     Grant privileges as appropriate (`psql -c "GRANT ALL PRIVILEGES ON DATABASE survivor_dw_dev TO survivor_dev;"`).
   - Docker Postgres: when you later run `docker compose up`, the `warehouse-db` service automatically creates the database defined by `DB_NAME` using the supplied credentials—no manual SQL required.

4. **Optional: enable git hooks**
   ```bash
   pipenv run pre-commit install
   ```
   The hook checks that Python and Airflow versions are aligned between the Pipfile and Dockerfile.

5. **Activate the virtual environment (when you need a shell)**
   ```bash
   pipenv shell
   ```

---

## 3. Environment Profiles (dev vs prod)

- `SURVIVOR_ENV` controls which environment is active (`dev` by default).
- `env/.env.dev.example` and `env/.env.prod.example` capture typical configuration values. Use `scripts/switch_env.py` to copy them into `.env`.
- Development runs generally point at a local database and are executed manually or via the Docker stack with `SURVIVOR_ENV=dev`.
- Production runs (typically via Docker + Airflow) set `SURVIVOR_ENV=prod` and target the containerised Postgres service (`warehouse-db`). The loader enforces that prod runs occur on the `main` git branch.
- You can maintain separate databases (e.g., `survivor_dw_dev` vs `survivor_dw`) by adjusting the `.env` files.
- Whenever `.env` changes, re-run `pipenv run python scripts/build_airflow_conn.py --write-airflow` to copy credentials into `airflow/.env` so Docker Compose picks up the same configuration.

---

## 4. Bronze Layer – Load survivoR Data

```bash
pipenv run python -m Database.load_survivor_data
```

What happens:
1. `.rda` files are downloaded from GitHub (saved in `data_cache/`).
2. `Database/create_tables.sql` is applied on first run to create the schemas.
3. Each loader run records metadata in `bronze.ingestion_runs` (environment, git branch/commit, source URL) and all bronze tables receive the associated `ingest_run_id`. Data is merged with upsert logic (no truncation in prod) and logs list inserted/updated keys.

Tip: capture loader output to `docs/run_logs/<context>_<timestamp>.log` so you can reference it in PRs or incident reviews.

Rerun the command whenever the upstream dataset changes or after a new episode airs.

---

## 5. Silver Layer – Curated Tables

The SQL scripts in `Database/sql/` transform bronze tables into dimensions and facts.

Manual refresh:
```bash
psql "$DATABASE_URL" -f Database/sql/refresh_silver_dimensions.sql
psql "$DATABASE_URL" -f Database/sql/refresh_silver_facts.sql
psql "$DATABASE_URL" -f Database/sql/refresh_gold_features.sql
```

A dedicated gold layer is generated via `Database/sql/refresh_gold_features.sql` (see the next section).

---

## 6. Gold Layer – Feature Snapshots

`Database/sql/refresh_gold_features.sql` materialises feature snapshots that capture the warehouse state at a point in time:

- `gold.feature_snapshots` – metadata about each feature refresh (ingestion run, environment, git branch/commit, timestamp).
- `gold.castaway_season_features` – season-level feature payloads for each castaway (profile, challenges, advantages, votes, TV stats, misc, jury).
- `gold.castaway_episode_features` – cumulative per-episode metrics (confessionals, challenges, votes, etc.).
- `gold.season_features` – season-wide descriptors (returnee ratios, twists, weights, cast size).

Run after the bronze/silver refresh:

```bash
psql "$DATABASE_URL" -f Database/sql/refresh_gold_features.sql
```

Every execution appends a new snapshot row so historical feature sets remain available for reproducibility and future model training.

---

## 7. Docker & Airflow Orchestration

The DAG `airflow/dags/survivor_medallion_dag.py` automates the workflow (bronze load → silver refresh → gold snapshot) on a weekly schedule. You can run the stack in **dev mode** (feature/hotfix branches, typically targeting a dev database) or **prod mode** (after merging to `main`).

### Dev run (feature / hotfix branch)

```bash
pipenv run python scripts/switch_env.py dev --from-example
# edit .env if you prefer a different DB host/name; use warehouse-db for the container DB
cd airflow
cp .env.example .env        # first time only, retains AIRFLOW_UID
pipenv run python ../scripts/build_airflow_conn.py --write-airflow
docker compose up airflow-init
docker compose up -d

# run the bronze loader inside Docker (captures the active branch state)
docker compose --profile loader run --rm survivor-loader
```

The Postgres container automatically creates the database specified by `DB_NAME`. Capture loader output for PRs, e.g.:

```bash
docker compose --profile loader run --rm survivor-loader | tee docs/run_logs/dev_$(date +%Y%m%d_%H%M%S).log
```

### Prod run (after merge to `main`)

```bash
pipenv run python scripts/switch_env.py prod --from-example
# adjust credentials if needed, ensure SURVIVOR_ENV=prod
cd airflow
cp .env.example .env        # if not already present
pipenv run python ../scripts/build_airflow_conn.py --write-airflow
docker compose up airflow-init
docker compose up -d

# once Airflow is up, the scheduled DAG or loader profile will operate against the prod DB
docker compose --profile loader run --rm survivor-loader | tee docs/run_logs/prod_$(date +%Y%m%d_%H%M%S).log
```

Prod loads require the repository to be on the `main` branch (enforced by the loader).

### Operational notes

- Services included: `warehouse-db` (warehouse Postgres, exposed on localhost:5433), Airflow metadata DB (`postgres`) and broker (`redis`), the Airflow components, and the on-demand `survivor-loader`.
- Useful commands:
  ```bash
  docker compose logs airflow-scheduler --follow   # live scheduler logs
  docker compose ps                                # verify service health
  ```
- Scheduling is handled inside Airflow once the stack is up; local Pipenv runs remain manual.
- The Docker workflow honours whichever `.env` is active—rerun `scripts/build_airflow_conn.py` after switching environments to keep the compose values aligned.

---

## 8. Repository Map

- `Database/create_tables.sql` – DDL for bronze and silver schemas.
- `Database/sql/refresh_silver_dimensions.sql` – Populate dimensions.
- `Database/sql/refresh_silver_facts.sql` – Populate fact tables.
- `Database/sql/refresh_gold_features.sql` – Populate gold feature snapshots.
- `Database/load_survivor_data.py` – Bronze ingestion entry point.
- `Utils/db_utils.py` – Database utilities with schema validation and upsert logic.
- `Utils/github_data_loader.py` – Wrapper around `pyreadr` + HTTP caching.
- `airflow/docker-compose.yaml` – Container stack for Airflow and the warehouse database.
- `scripts/check_versions.py` – Pre-commit helper ensuring Pipfile/Dockerfile version alignment.
- `scripts/build_airflow_conn.py` – Generates `AIRFLOW_CONN_SURVIVOR_POSTGRES` from `.env`.
- `scripts/switch_env.py` – Copies an environment-specific dotenv file into place.
- `docs/run_logs/` – Suggested directory for storing loader/Airflow log artifacts referenced in PRs.

---

## 9. Troubleshooting

- **Missing Python/Airflow modules in your editor**  
  Install the project's dependencies via Pipenv (`pipenv install`). Airflow (and its providers) are now part of the Pipfile so IDEs such as VS Code can resolve imports.

- **Pre-commit hook fails**  
  Run `pipenv run python scripts/check_versions.py` to see the mismatch details and update the Pipfile or Dockerfile accordingly.

- **Container database access**  
  Connect with any Postgres client using `localhost:5433` (user/password from `.env`). The mounted volume stores data files; interact with the database through SQL clients, not the raw files.

---

## 10. Pull Request Workflow

A PR template (`.github/pull_request_template.md`) guides the release checklist. Key expectations:

### Before opening a PR
- Run `pipenv run pre-commit run --all-files`.
- Execute the bronze loader locally against the dev database and capture the resulting `pipeline.log` (or equivalent) path.
- Run the Docker loader on your feature/hotfix branch (`docker compose --profile loader run --rm survivor-loader`) and save the stdout to `docs/run_logs/<branch>_<timestamp>.log` (or another accessible location). Reference these log paths in the PR description.

### After merging to `development`
- Trigger a clean Docker loader run on the updated `development` branch.
- Attach the new log location (e.g., `docs/run_logs/development_<timestamp>.log`) as a comment on the closed PR or in your release notes.

### Before releasing to `main`
- Switch to `main`, set `SURVIVOR_ENV=prod`, and run the Docker loader once more. The loader enforces the branch check for you.
- Store the prod log similarly for auditing.

These steps ensure that each environment has been exercised with the latest code and that traceable logs exist for future reference.

---

## 11. Next Steps

- Extend the Airflow DAG with modelling or scoring tasks once the feature layer solidifies.
- Incorporate NLP/confessional-text ingestion when the source data is available.
- Quantify experiment results in notebooks or dashboards using the bronze/silver tables.
