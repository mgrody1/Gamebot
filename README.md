# Survivor Prediction Warehouse

This repository builds a Medallion-style data warehouse for CBS’s *Survivor*.  
It ingests the open-source [`survivoR`](https://github.com/doehm/survivoR) datasets, stores the raw data in a **bronze** schema, curates analytics-ready tables in a **silver** schema, and provides automation via Apache Airflow.  
All code runs locally with Pipenv or fully containerised via Docker Compose.

---

## 1. Requirements

- Python 3.11 (install via Docker/Dev Container or locally with pyenv/asdf; see note below)
- Pipenv (`pip install pipenv`)
- PostgreSQL 15+ (use the included Docker service or your own instance)
- Optional tooling
  - Docker/Docker Compose v2 (for the container stack)
  - pre-commit (`pip install pre-commit`) for repository lint checks
  - VS Code Dev Containers extension (recommended IDE experience with Docker)
- Ensure the Postgres user used for ingestion can create schemas and the `uuid-ossp` extension (required on the first run).

---

## 2. Gamebot Pipelines at a Glance

| Persona / Goal | Recommended Path |
| --- | --- |
| Notebook analysts or data scientists who just want the data | Install **Gamebot Lite** (`pip install gamebot-lite`, once published) or export a fresh SQLite snapshot via `scripts/export_sqlite.py`. |
| Engineers iterating on the pipeline, notebooks, or dbt | Use the **Docker + VS Code Dev Container** workflow (below). Local Pipenv is available as an alternative. |
| Operators running scheduled refreshes only | Deploy the `airflow/docker-compose.yaml` stack on a server (set `GAMEBOT_TARGET_LAYER` to control bronze/silver/gold). Optionally publish a Docker image via CI for this use case. |

---

## 3. Recommended Quick Start (Docker + VS Code Dev Container)

Spin up the full Gamebot stack without managing local Python versions.

### Prerequisites

- Docker Desktop / Docker Engine
- VS Code with the “Dev Containers” extension

### Steps

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd survivor_prediction
   ```

2. **Create environment files**
   ```bash
   pipenv run python scripts/switch_env.py dev --from-example
   cd airflow
   cp .env.example .env
   pipenv run python ../scripts/build_airflow_conn.py --write-airflow
   cd ..
   ```
   *(If Python 3.11 is unavailable locally, run these commands after opening the Dev Container in step 3—`pipenv` is preinstalled there.)*

3. **Open the repo in the Dev Container**
   - In VS Code press `F1` (or `Ctrl+Shift+P`) to open the command palette.
   - Search for “Dev Containers: Reopen in Container” and run it. This step launches the VS Code environment; you will still execute `docker compose` manually in the next step to start the services.
   - The container installs Pipenv dependencies, registers the `gamebot` kernel, and mounts the repo at `/workspace`.

4. **Launch the stack**
   ```bash
   cd airflow
   docker compose up airflow-init
   docker compose up -d
   ```
   This starts Postgres (`warehouse-db`), Airflow, Redis, and the scheduler.

5. **Run tasks on demand (inside the container terminal)**
   ```bash
   # Bronze load
   docker compose --profile loader run --rm survivor-loader

   # dbt transformations
   pipenv run dbt deps --project-dir dbt --profiles-dir dbt
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
   ```

6. **Notebook-friendly**
   - Generate notebooks with `pipenv run python scripts/create_notebook.py adhoc` or `... model`.
   - Use the preconfigured `gamebot` kernel.

> Tip: set `GAMEBOT_TARGET_LAYER` in `.env` before running compose if you want the pipeline to stop after bronze or silver.

### Running on a server (optional)

Clone the repo on the server, copy `.env`, and execute the same `docker compose` commands over SSH. Expose port 8080 if you need the Airflow UI.

---

## 4. Alternative Quick Start (Local Pipenv)

Prefer to run everything locally? Ensure Python 3.11 is installed (e.g., via `pyenv`) and follow these steps:

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
   Adjust database credentials/hosts as needed.

3. **Create or verify the database(s)**
   - Local Postgres example:
     ```bash
     createdb survivor_dw_dev
     createuser survivor_dev --pwprompt
     ```
     Grant privileges as appropriate (`psql -c "GRANT ALL PRIVILEGES ON DATABASE survivor_dw_dev TO survivor_dev;"`).
   - Using Docker Postgres only: run `docker compose up` later; the `warehouse-db` service will create the database automatically.

4. **Optional: enable git hooks**
   ```bash
   pipenv run pre-commit install
   ```

5. **Activate the virtual environment when needed**
   ```bash
   pipenv shell
   ```

    > **Python version note**: Pipenv requires Python 3.11. Install it locally via `pyenv install 3.11.8 && pyenv local 3.11.8` (or similar) before running `pipenv install`.

---

## 5. Environment Profiles (dev vs prod)

- `SURVIVOR_ENV` controls which environment is active (`dev` by default).
- `env/.env.dev.example` and `env/.env.prod.example` capture typical configuration values. Use `scripts/switch_env.py` to copy them into `.env`.
- Development runs generally point at a local database and are executed manually or via the Docker stack with `SURVIVOR_ENV=dev`.
- Production runs (typically via Docker + Airflow) set `SURVIVOR_ENV=prod` and target the containerised Postgres service (`warehouse-db`). The loader enforces that prod runs occur on the `main` git branch.
- You can maintain separate databases (e.g., `survivor_dw_dev` vs `survivor_dw`) by adjusting the `.env` files.
- Whenever `.env` changes, re-run `pipenv run python scripts/build_airflow_conn.py --write-airflow` to copy credentials into `airflow/.env` so Docker Compose picks up the same configuration.
- Control pipeline depth via `GAMEBOT_TARGET_LAYER` (`bronze`, `silver`, or `gold`; defaults to `gold`). This value applies to both local runs and the Docker orchestrator.

---

## 6. Bronze Layer – Load survivoR Data

```bash
pipenv run python -m Database.load_survivor_data
```

What happens:
1. `.rda` files are downloaded from GitHub (saved in `data_cache/`).
2. `Database/create_tables.sql` is applied on first run to create the schemas.
3. Each loader run records metadata in `bronze.ingestion_runs` (environment, git branch/commit, source URL) and all bronze tables receive the associated `ingest_run_id`. Data is merged with upsert logic (no truncation in prod), Great Expectations validations run on key bronze tables (results land in `docs/run_logs/`), and logs list inserted/updated keys.

Tip: capture loader output to `docs/run_logs/<context>_<timestamp>.log` so you can reference it in PRs or incident reviews.

Rerun the command whenever the upstream dataset changes or after a new episode airs.

---

## 7. Silver Layer – Curated Tables

The SQL scripts in `Database/sql/` transform bronze tables into dimensions and facts.

Run the dbt models (install packages once via `dbt deps`):
```bash
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
```

Legacy SQL scripts remain under `Database/sql/` for reference, but dbt is the primary transformation workflow.

---

## 8. Gold Layer – Feature Snapshots

`pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold` materialises feature snapshots that capture the warehouse state at a point in time:

- `gold.feature_snapshots` – metadata about each feature refresh (ingestion run, environment, git branch/commit, timestamp).
- `gold.castaway_season_features` – season-level feature payloads for each castaway (profile, challenges, advantages, votes, TV stats, misc, jury).
- `gold.castaway_episode_features` – cumulative per-episode metrics (confessionals, challenges, votes, etc.).
- `gold.season_features` – season-wide descriptors (returnee ratios, twists, weights, cast size).

Every execution rebuilds the gold tables for the most recent ingestion run. Historical ingest metadata is preserved in `gold.feature_snapshots` for reproducibility.

---

## 9. Docker & Airflow Orchestration

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

> These `pipenv` commands can be executed either (a) locally with Python 3.11 available or (b) inside the VS Code Dev Container (`pipenv` is pre-installed there). If you do not have Python 3.11 on the host machine, open the repo in the Dev Container first and run the above commands from its terminal.

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
- The scheduler runs `dbt deps`/`dbt build --select silver` followed by `dbt build --select gold` after each bronze load, so new transformations are automatically applied.
- Set `GAMEBOT_TARGET_LAYER` in your environment (or `.env`) to limit how far the DAG runs. For example, `GAMEBOT_TARGET_LAYER=bronze` skips dbt tasks so analysts can work directly off bronze tables.
- Postgres data is persisted in the named Docker volume `warehouse-data`; you can inspect it with `docker volume ls`. Stopping the stack does not discard ingested data.
- Useful commands:
  ```bash
  docker compose logs airflow-scheduler --follow   # live scheduler logs
  docker compose ps                                # verify service health
  ```
- Scheduling is handled inside Airflow once the stack is up; local Pipenv runs remain manual.
- The Docker workflow honours whichever `.env` is active—rerun `scripts/build_airflow_conn.py` after switching environments to keep the compose values aligned.

---

### Production-only deployment

If you simply want an auto-refreshing warehouse hosted on your own server:

1. Copy `.env` (set `SURVIVOR_ENV=prod`, `GAMEBOT_TARGET_LAYER`, and production DB credentials).
2. Provision a host with Docker (no need for VS Code).
3. Run `docker compose up -d` inside the `airflow/` directory.
4. Use the Airflow UI (port 8080) or CLI to trigger runs as needed; the weekly schedule will refresh bronze/dbt automatically.

> Future improvement: automate publishing a minimal Docker image (built from this repo) to Docker Hub. A GitHub Action could build the image after each merge to `main` and push it for operators to pull (`docker run gamebot/gamebot-scheduler`).

---

## 10. Analyst Snapshot (SQLite Export)

Analysts who prefer to work locally (e.g., pure Jupyter workflows) can export the latest bronze (default) or curated layers to a standalone SQLite file:

```bash
pipenv run python scripts/export_sqlite.py --layer silver --output gamebot.sqlite
```

- `--layer` controls the highest layer included (`bronze`, `silver`, or `gold`). Lower layers are always included (defaults to bronze).
- The script writes a `gamebot_ingestion_metadata` table containing the most recent ingestion run information (branch, commit, timestamp) so you can see how fresh the data is.
- You can open the SQLite file in pandas (`pd.read_sql('select * from silver_fact_vote_history', 'sqlite:///gamebot.sqlite')`) or any SQL client—no Docker required.
- The exported file is `.gitignore`d by default; share it with teammates or use it as the basis for a future pip-installable “Gamebot Lite” package.
- See `docs/gamebot_lite.md` for friendly table names, SQL examples, and a data dictionary.

---

## 11. Gamebot Lite Package (Work in Progress)

The plan is to publish a `gamebot-lite` Python package that bundles the latest SQLite snapshot for easy notebook access—mirroring how the `survivoR` R package distributes data.

See `docs/gamebot_lite.md` for table names, examples, and a data dictionary.

### Using the package (analysts)

- Install or upgrade to the latest release:
  ```bash
  python -m pip install --upgrade gamebot-lite
  ```
- Example usage:
  ```python
  from gamebot_lite import load_table

  df_votes = load_table("vote_history_curated")
  print(df_votes.head())
  ```
- Prefer SQL? Install `duckdb` and use `from gamebot_lite import duckdb_query` to run SQL against the snapshot.
- Each release ships a frozen SQLite database; run the upgrade command whenever you need the newest snapshot.

### Preparing a new release (maintainers)

- Refresh the bundled data locally:
  ```bash
  pipenv run python scripts/export_sqlite.py --layer silver --package --output gamebot_lite/data/gamebot.sqlite
  pipenv run python -m gamebot_lite  # list packaged tables
  ```
- Bump the version in `pyproject.toml`, build the wheel (`pipenv run python -m build`), and upload (`python -m twine upload dist/*`).
- Publishing is manual for now; you can automate it later with CI after the weekly refresh if desired.

---

## 12. Repository Map

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
- `scripts/build_erd.py` – Generates an ERD image at `docs/erd/` (Graphviz required).
- `docs/run_logs/` – Suggested directory for storing loader/Airflow log artifacts referenced in PRs.
- `docs/erd/` – Generated ERD assets (ignored in VCS except for `.gitkeep`).
- `scripts/export_sqlite.py` – Export bronze/silver/gold tables to a local SQLite database.
- `scripts/create_notebook.py` – Generate starter notebooks for ad hoc analysis or model prototyping.
- `templates/` – Notebook templates used by the script; notebooks generated under `notebooks/` are ignored by git.
- `gamebot_lite/` – Lightweight SQLite client and packaged data for future pip installs.
- `docs/gamebot_lite.md` – Analyst documentation for table names, usage examples, and data dictionary.
- `dbt/` – dbt project (models/tests). Generate docs with `pipenv run dbt docs generate --project-dir dbt --profiles-dir dbt` and view via `pipenv run dbt docs serve` or publish the site via GitHub Pages.

---

## 13. Troubleshooting

- **Missing Python/Airflow modules in your editor**  
  Install the project's dependencies via Pipenv (`pipenv install`). Airflow (and its providers) are now part of the Pipfile so IDEs such as VS Code can resolve imports.

- **Pre-commit hook fails**  
  Run `pipenv run python scripts/check_versions.py` to see the mismatch details and update the Pipfile or Dockerfile accordingly.

- **Container database access**  
  Connect with any Postgres client using `localhost:5433` (user/password from `.env`). The mounted volume stores data files; interact with the database through SQL clients, not the raw files.
- **ERD generation fails**  
  Install Graphviz (e.g., `brew install graphviz` or `apt-get install graphviz`) before running `pipenv run python scripts/build_erd.py`.

---

## 14. Pull Request Workflow

A PR template (`.github/pull_request_template.md`) guides the release checklist. Key expectations:

### Before opening a PR
- Run `pipenv run pre-commit run --all-files`.
- Execute the bronze loader locally against the dev database and capture the resulting `pipeline.log` (or equivalent) path.
- Run Great Expectations validations (produced automatically during the loader run) and note the JSON location under `docs/run_logs/`.
- Run the Docker loader on your feature/hotfix branch (`docker compose --profile loader run --rm survivor-loader`) and save the stdout to `docs/run_logs/<branch>_<timestamp>.log` (or another accessible location). Reference these log paths in the PR description.

### After merging to `development`
- Trigger a clean Docker loader run on the updated `development` branch.
- Attach the new log location (e.g., `docs/run_logs/development_<timestamp>.log`) as a comment on the closed PR or in your release notes.

### Before releasing to `main`
- Switch to `main`, set `SURVIVOR_ENV=prod`, and run the Docker loader once more. The loader enforces the branch check for you.
- Store the prod log and the latest Great Expectations output similarly for auditing.

These steps ensure that each environment has been exercised with the latest code and that traceable logs exist for future reference.

---

## 15. Next Steps

- Extend the Airflow DAG with modelling or scoring tasks once the feature layer solidifies.
- Incorporate NLP/confessional-text ingestion when the source data is available.
- Quantify experiment results in notebooks or dashboards using the bronze/silver tables.
