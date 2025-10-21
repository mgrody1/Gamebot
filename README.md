# Gamebot Warehouse

Gamebot builds a Medallion-style data warehouse for CBS’s *Survivor*.
It ingests the open-source [`survivoR`](https://github.com/doehm/survivoR) datasets, stores the raw data in a **bronze** schema, curates analytics-ready tables in a **silver** schema, and provides automation via Apache Airflow.
All code runs locally with Pipenv or fully containerised via Docker Compose.

> Note: The repository folder may still be named `survivor_prediction`. The project name is Gamebot.

---

## 1. Requirements

* Docker Engine or Docker Desktop (Compose v2 included)
* Make (GNU make)
* Git
* Optional (for local development without Dev Containers):

  * Python 3.11
  * Pipenv (`pip install pipenv`)
  * PostgreSQL 15+ (only if not using the provided Docker service)
  * pre-commit (`pip install pre-commit`)
* Ensure the Postgres role used for ingestion can create schemas and the `uuid-ossp` extension (first run only).

---

## 2. Choose Your Path

| Persona / Goal                                         | Recommended Path                                                                                                   |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| Analysts who just want the data                        | Use **Gamebot Lite** (`pip install gamebot-lite`, once published) or export SQLite via `scripts/export_sqlite.py`. |
| Engineers iterating on the pipeline, notebooks, or dbt | Use the **VS Code Dev Container** workflow (no local Python setup required).                                       |
| Engineers who prefer local tools                       | Use **Local Pipenv** and optionally run Airflow/Postgres via Docker.                                               |
| Operators running scheduled refreshes only             | Use the **Docker Compose stack** (Makefile `make up`) on a server; let Airflow schedule the DAG.                   |

### Dev Container vs. Local Pipenv (when to choose what)

| Feature         | Dev Container                           | Local Pipenv                         |
| --------------- | --------------------------------------- | ------------------------------------ |
| Setup effort    | Minimal (prebuilt Python 3.11 + deps)   | Requires local Python 3.11 + Pipenv  |
| Reproducibility | High (identical across machines)        | Varies with host OS and toolchain    |
| Performance     | Slightly slower on macOS                | Native speed on host                 |
| Best for        | Onboarding, consistency, parity with CI | Power users who prefer local tooling |

---

## 3. Quick Start (Docker + Makefile)

This is the simplest way to start Airflow, Postgres, and Redis. It also creates the Airflow admin user.

### Steps

1. Clone the repository

   ```bash
   git clone <repo-url>
   cd survivor_prediction
   ```

2. Create `.env` at the repository root (or copy from example)

   ```bash
   cp env/.env.dev.example .env
   # then edit values as needed; examples:
   # DB_HOST=warehouse-db
   # DB_NAME=survivor_dw_dev
   # DB_USER=user
   # DB_PASSWORD=pass
   # SURVIVOR_ENV=dev
   # AIRFLOW_PORT=8081
   ```

   The Docker stack reads this file via `docker compose --env-file ../.env` (the Makefile handles that for you).

3. Start the stack

   ```bash
   make up
   ```

   This runs `docker compose up airflow-init` then `docker compose up -d` in `airflow/`.
   It starts: `warehouse-db` (Postgres for the warehouse), `postgres` (Airflow metadata DB), `redis`, and the Airflow services.

4. Open the Airflow UI

   * URL: `http://localhost:${AIRFLOW_PORT:-8080}` (default 8080; often set to 8081 to avoid conflicts)
   * Login: `admin / admin`

5. Trigger the DAG

   * UI: Unpause and trigger `survivor_medallion_dag` from the web UI.
   * CLI:

     ```bash
     cd airflow
     docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_dag
     ```

### Handy Make targets

From the repository root:

```bash
make up           # start/initialize Airflow + Postgres + Redis
make down         # stop the stack (keeps volumes)
make clean        # stop and remove volumes (fresh start)
make logs         # follow scheduler logs
make ps           # list services and status
make loader       # run the on-demand bronze loader profile container
```

Notes:

* `make up` is idempotent. It handles the Airflow DB migration and creates the `admin` user if missing.
* If port 8080 is in use, set `AIRFLOW_PORT=8081` (or another free port) in your `.env`.

---

## 4. Dev Container Workflow (Recommended for development)

Use VS Code **Dev Containers** to avoid managing Python locally.

1. Prerequisites

   * VS Code + “Dev Containers” extension
   * Docker installed and running

2. Open in Dev Container

   * In VS Code: Command Palette → “Dev Containers: Reopen in Container”.
   * The repository is mounted at `/workspace`.
     The dev container includes Python 3.11, Pipenv, and the `gamebot` Jupyter kernel bootstrap.

3. Start Airflow/Postgres from the host (not from inside the Dev Container)

   * Use the host terminal in the repo root:

     ```bash
     make up
     ```
   * The Airflow UI remains available at `http://localhost:${AIRFLOW_PORT}` on the host.

4. Develop inside the container

   * Notebooks and scripts run with the `gamebot` kernel
   * Use Pipenv and dbt as needed:

     ```bash
     pipenv install
     pipenv run dbt deps --project-dir dbt --profiles-dir dbt
     pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
     pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
     ```

5. Optional: On-demand bronze load via Docker profile

   ```bash
   make loader
   ```

---

## 5. Local Pipenv Workflow (Alternative)

Run development locally with your own Python while still using the Dockerized Airflow/Postgres, or run everything locally.

1. Install dependencies

   ```bash
   pip install pipenv
   pipenv install
   ```

2. Select an environment and create `.env`

   ```bash
   pipenv run python scripts/switch_env.py dev --from-example
   # edit .env if you prefer different DB host/name; use DB_HOST=warehouse-db to target the Docker Postgres
   ```

3. Start orchestration with Docker (recommended even for local Pipenv)

   ```bash
   make up
   # Airflow UI at http://localhost:${AIRFLOW_PORT}
   ```

4. Run transformations locally (dbt)

   ```bash
   pipenv run dbt deps --project-dir dbt --profiles-dir dbt
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
   ```

5. Optional: Run everything locally without Docker

   * Provide your own Postgres 15+ credentials in `.env` (not `warehouse-db`)
   * Use `pipenv run python -m Database.load_survivor_data` for bronze
   * Run dbt as above
   * Skip `make up` and Airflow entirely if you don’t need scheduling

---

## 6. Environment Profiles (dev vs prod)

* `SURVIVOR_ENV` controls the environment (`dev` by default).
* `env/.env.dev.example` and `env/.env.prod.example` show typical configuration values. Use `scripts/switch_env.py` to copy them into `.env`.
* Prod runs (typically via Docker + Airflow) target the containerized Postgres service (`warehouse-db`) and enforce running from the `main` branch.
* Control pipeline depth via `GAMEBOT_TARGET_LAYER` (`bronze`, `silver`, or `gold`; defaults to `gold`).
* Whenever `.env` changes, run:

  ```bash
  pipenv run python scripts/build_airflow_conn.py --write-airflow
  ```

  to keep Airflow’s connection values aligned.

---

## 7. Bronze Layer – Load survivoR Data

```bash
pipenv run python -m Database.load_survivor_data
```

What happens:

1. `.rda` files are downloaded from GitHub (saved in `data_cache/`).
2. `Database/create_tables.sql` is applied on first run to create schemas.
3. Each loader run records metadata in `bronze.ingestion_runs` and associates `ingest_run_id` with bronze tables. Data is merged with upsert logic (no truncation in prod). Soda Core validations run on key bronze tables (results land in `docs/run_logs/`). Logs list inserted/updated keys.

Tip: capture loader output to `docs/run_logs/<context>_<timestamp>.log` for PRs or incident reviews.
Rerun when the upstream dataset changes or after a new episode.

---

## 8. Silver Layer – Curated Tables

SQL in `Database/sql/` transforms bronze into dimensions and facts.
dbt is the primary transformation workflow:

```bash
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
```

Legacy SQL remains for reference.

---

## 9. Gold Layer – Feature Snapshots

```bash
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
```

* `gold.feature_snapshots` – metadata about each feature refresh
* `gold.castaway_season_features` – season-level feature payloads per castaway
* `gold.castaway_episode_features` – cumulative per-episode metrics
* `gold.season_features` – season-wide descriptors

Each execution rebuilds gold for the most recent ingestion run. Historical metadata is preserved in `gold.feature_snapshots`.

---

## 10. Docker & Airflow Orchestration

The DAG `airflow/dags/survivor_medallion_dag.py` automates the workflow (bronze → silver → gold) on a weekly schedule.

### Start services

```bash
make up
# Airflow UI at http://localhost:${AIRFLOW_PORT:-8080} (default admin/admin)
```

### Run the DAG

* UI: Unpause and trigger `survivor_medallion_dag`.
* CLI:

  ```bash
  cd airflow
  docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_dag
  docker compose exec airflow-scheduler airflow dags list-runs -d survivor_medallion_dag
  docker compose logs -f airflow-scheduler
  ```

### Operational notes

* Services: `warehouse-db` (warehouse Postgres, localhost:5433 by default), Airflow metadata DB (`postgres`), `redis`, Airflow components, and on-demand `survivor-loader`.
* The scheduler runs `dbt deps`/`dbt build --select silver` followed by gold after each bronze load.
* Set `GAMEBOT_TARGET_LAYER` in `.env` to limit how far the DAG runs.
* Data is persisted in `warehouse-data` Docker volume.

---

## 11. Analyst Snapshot (SQLite Export)

Analysts can export the latest bronze/silver/gold to a standalone SQLite file:

```bash
pipenv run python scripts/export_sqlite.py --layer silver --output gamebot.sqlite
```

* `--layer` sets the highest layer to include (`bronze`, `silver`, or `gold`; defaults to bronze; lower layers always included)
* Writes `gamebot_ingestion_metadata` with the most recent ingestion info
* Open with pandas or any SQL client
* The exported file is `.gitignore`d by default
* See `docs/gamebot_lite.md` for friendly table names, SQL, and a data dictionary

---

## 12. Gamebot Lite Package (Work in Progress)

A future `gamebot-lite` Python package will bundle the latest SQLite snapshot for easy notebook access.

See `docs/gamebot_lite.md` for table names, examples, and data dictionary.

### Using the package (analysts)

```bash
python -m pip install --upgrade gamebot-lite
```

```python
from gamebot_lite import load_table
df_votes = load_table("vote_history_curated")
print(df_votes.head())
```

Prefer SQL? Install `duckdb` and use `from gamebot_lite import duckdb_query`.

### Preparing a new release (maintainers)

```bash
pipenv run python scripts/export_sqlite.py --layer silver --package --output gamebot_lite/data/gamebot.sqlite
pipenv run python -m gamebot_lite
# bump version, build and upload
```

---

## 13. Repository Map

* `Database/create_tables.sql` – DDL for bronze and silver schemas
* `Database/sql/refresh_silver_dimensions.sql` – Populate dimensions
* `Database/sql/refresh_silver_facts.sql` – Populate fact tables
* `Database/sql/refresh_gold_features.sql` – Populate gold feature snapshots
* `Database/load_survivor_data.py` – Bronze ingestion entry point
* `Utils/db_utils.py` – Database utilities, schema validation, upsert logic
* `Utils/github_data_loader.py` – Wrapper around `pyreadr` + HTTP caching
* `airflow/docker-compose.yaml` – Container stack for Airflow and the warehouse database
* `scripts/check_versions.py` – Pre-commit helper to ensure Pipfile/Dockerfile alignment
* `scripts/build_airflow_conn.py` – Generates `AIRFLOW_CONN_SURVIVOR_POSTGRES` from `.env`
* `scripts/switch_env.py` – Copies an environment-specific dotenv file into place
* `scripts/build_erd.py` – Generates an ERD image at `docs/erd/` (Graphviz required)
* `docs/run_logs/` – Suggested directory for loader/Airflow log artifacts
* `docs/erd/` – Generated ERD assets
* `scripts/export_sqlite.py` – Export bronze/silver/gold to SQLite
* `scripts/create_notebook.py` – Generate starter notebooks
* `templates/` – Notebook templates; generated notebooks are ignored by git
* `gamebot_lite/` – Lightweight SQLite client and packaged data
* `docs/gamebot_lite.md` – Analyst docs for table names, usage, and data dictionary
* `dbt/` – dbt project (models/tests); generate docs with `dbt docs generate` and serve/publish as needed
* `Makefile` – Orchestration wrapper for Docker Compose and helper commands

---

## 14. Troubleshooting

* Airflow login

  * Default credentials are `admin / admin` (created by `airflow-init`)
* Port conflicts on 8080

  * Set `AIRFLOW_PORT` in `.env` (e.g., `AIRFLOW_PORT=8081`) and rerun `make up`
* Missing DAG in UI

  * Verify it is present in the container:
    `docker compose -f airflow/docker-compose.yaml --env-file .env exec airflow-webserver ls /opt/airflow/dags`
* Clean restart

  * `make clean` removes the Compose volumes (fresh DB and logs)
* Logs and status

  * `make logs` follows scheduler logs; `make ps` shows service status
* Permissions on logs directory

  * Logs are persisted via a named Docker volume (`airflow-logs`), so no manual `chown` is required

---

## 15. Pull Request Workflow

### Before opening a PR

* `pipenv run pre-commit run --all-files`
* Run the bronze loader against the dev DB and capture its log
* Verify Soda validations (written during the loader run) in `docs/run_logs/`
* Run the Docker loader on your feature/hotfix branch:

  ```bash
  docker compose --profile loader run --rm survivor-loader | tee docs/run_logs/<branch>_$(date +%Y%m%d_%H%M%S).log
  ```

### After merging to `development`

* Trigger a clean Docker loader run and attach the log path in notes

### Before releasing to `main`

* Switch to `main`, set `SURVIVOR_ENV=prod`, and run the Docker loader again (branch check enforced)

---

## 16. Next Steps

* Extend the Airflow DAG with modeling or scoring tasks once the feature layer solidifies
* Incorporate NLP/confessional-text ingestion when available
* Build dashboards or notebooks on top of bronze/silver tables
