# Gamebot

Gamebot is a local-first data platform for CBS’s *Survivor*. It ingests the open-source [`survivoR`](https://github.com/doehm/survivoR) datasets, lands them in a **bronze** schema, curates **silver** models, and assembles **gold** feature sets for downstream analytics and ML. The project is intentionally modular so different audiences can pick the right delivery model:

| Layer | Who uses it | How they run it | Requires this repo? | Package / Image |
| ----- | ----------- | --------------- | ------------------- | ---------------- |
| **Gamebot Island (developer studio)** | Developers & contributors | Clone repo → VS Code Dev Container or Pipenv → build, run, extend; can also run prod straight from source | Yes | `gamebot-studio` (this repo) |
| **Gamebot Warehouse** | Operators who want a turn-key stack | Pull official Docker images + Compose file; no source checkout needed | No (planned distribution) | Docker Hub `mhgrody/gamebot-warehouse`, `mhgrody/gamebot-etl` |
| **gamebot-lite** | Analysts & notebooks | `pip install gamebot-lite` (ships a SQLite snapshot + helper API) | No | PyPI `gamebot-lite` |

> Studio = repo-based development.  
> Warehouse = registry-based runtime (images baked with DAGs/code).  
> Lite = analyst package (SQLite snapshot + helper functions).

> **Status:** Gamebot Island (this repo) is the canonical experience today. The warehouse images will be published to Docker Hub under `mhgrody/*`; until then you can build identical images directly from the repo.

> Note: The repository folder may still be named `survivor_prediction`. The project name is Gamebot.

## Table of Contents

- [1. Gamebot architecture layers](#1-gamebot-architecture-layers)
  - [1.1 Gamebot Warehouse (registry deployment)](#11-gamebot-warehouse-registry-deployment)
    - [Prerequisites](#prerequisites)
    - [Compose skeleton](#compose-skeleton)
    - [Operating the stack](#operating-the-stack)
  - [1.2 Gamebot Island (developer studio)](#12-gamebot-island-developer-studio)
    - [Requirements](#requirements)
    - [Studio entry points](#studio-entry-points)
      - [Dev Container vs. Local Pipenv](#dev-container-vs-local-pipenv)
    - [Quick start (Docker + Makefile)](#quick-start-docker-makefile)
      - [Handy Make targets](#handy-make-targets)
    - [Dev Container workflow (recommended for development)](#dev-container-workflow-recommended-for-development)
    - [Local Pipenv workflow (alternative)](#local-pipenv-workflow-alternative)
  - [1.3 gamebot-lite (analyst package)](#13-gamebot-lite-analyst-package)
- [2. Environment profiles (dev vs prod)](#2-environment-profiles-dev-vs-prod)
  - [2.1 `.env` keys (cheat sheet)](#21-env-keys-cheat-sheet)
  - [2.2 Workflow tips](#22-workflow-tips)
- [3. ETL architecture](#3-etl-architecture)
  - [3.1 Bronze – load `survivoR` data](#31-bronze--load-survivor-data)
  - [3.2 Silver – curated tables](#32-silver--curated-tables)
  - [3.3 Gold – feature snapshots](#33-gold--feature-snapshots)
  - [3.4 Explore with external SQL tools](#34-explore-with-external-sql-tools)
- [6. Docker & Airflow orchestration](#6-docker-airflow-orchestration)
  - [6.1 Start services](#61-start-services)
  - [6.2 Run the DAG](#62-run-the-dag)
- [7. Gamebot Lite (analyst package)](#7-gamebot-lite-analyst-package)
- [8. Schedules & releases](#8-schedules-releases)
- [9. Studio vs. warehouse deployments](#9-studio-vs-warehouse-deployments)
- [10. Repository map](#10-repository-map)
- [11. Troubleshooting](#11-troubleshooting)
- [12. Need to dive deeper?](#12-need-to-dive-deeper)
---

## 1. Gamebot architecture layers

### 1.1 Gamebot Warehouse (registry deployment)

Use the warehouse images when you need a turn-key deployment without cloning this repo.

#### Prerequisites

* Docker Engine / Docker Desktop (Compose v2)
* A `.env` containing Postgres, Redis, and Airflow settings (same structure as the repo’s `.env`)

#### Compose skeleton

```yaml
version: "3.9"

x-env: &env
  env_file: [.env]

services:
  warehouse-db:
    image: postgres:15
    <<: *env
    environment:
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: ${DB_NAME}
    volumes:
      - warehouse-data:/var/lib/postgresql/data
    restart: unless-stopped

  redis:
    image: redis:7
    restart: unless-stopped

  airflow-init:
    image: mhgrody/gamebot-warehouse:latest
    <<: *env
    command: >
      bash -lc "airflow db upgrade &&
                airflow users create --role Admin --username admin --password admin
                --firstname Survivor --lastname Admin --email admin@example.com || true"
    volumes:
      - airflow-logs:/opt/airflow/logs
    depends_on: [warehouse-db, redis]

  airflow-webserver:
    image: mhgrody/gamebot-warehouse:latest
    <<: *env
    command: webserver
    read_only: true
    ports: ["${AIRFLOW_PORT:-8080}:8080"]
    volumes:
      - airflow-logs:/opt/airflow/logs
    depends_on: [airflow-init]
    restart: unless-stopped

  airflow-scheduler:
    image: mhgrody/gamebot-warehouse:latest
    <<: *env
    command: scheduler
    read_only: true
    volumes:
      - airflow-logs:/opt/airflow/logs
    depends_on: [airflow-init, warehouse-db, redis]
    restart: unless-stopped

  survivor-loader:
    image: mhgrody/gamebot-etl:latest
    <<: *env
    entrypoint: ["python", "-m", "Database.load_survivor_data"]
    depends_on: [warehouse-db]
    profiles: ["loader"]
    restart: "no"

volumes:
  warehouse-data:
  airflow-logs:
```

#### Operating the stack

```bash
# Start the stack (images are pulled automatically if missing)
docker compose up -d

# One-off bronze load (bronze only; silver/gold run via the Airflow DAG)
docker compose run --rm --profile loader survivor-loader
```

Open Airflow at `http://localhost:${AIRFLOW_PORT:-8080}` (default `8080`). Login with `admin / admin`, unpause `survivor_medallion_dag`, and trigger a run. The DAG orchestrates **bronze → silver → gold** in sequence. The `survivor-loader` container is only for ad-hoc bronze refreshes (for example, to ingest new raw data ahead of a scheduled DAG run).

To adjust the DAG schedule before bringing the stack online, set `GAMEBOT_DAG_SCHEDULE` (cron expression) in your `.env`. The default is `0 4 * * 1` (early Monday UTC to capture weekend data entry).

---

### 1.2 Gamebot Island (developer studio)

Clone this repository when you want to contribute, customise the pipeline, or run prod straight from source.

#### Requirements

* Docker Engine or Docker Desktop (Compose v2 included)
* Make (GNU make)
* Git
* Optional (for local Python workflows):

  * Python 3.11
  * Pipenv (`pip install pipenv`)
  * PostgreSQL 15+ (only if you are *not* using the bundled Docker services)
  * pre-commit (`pip install pre-commit`) – the Dev Container runs `pipenv install --dev`, but because the repo currently has no dev-packages, install `pre-commit` manually if you intend to use the hooks (`pipenv install --dev pre-commit && pipenv run pre-commit install`)
* If you point Gamebot at your own Postgres instance (instead of the bundled `warehouse-db`), make sure the role used for ingestion can create schemas and the `uuid-ossp` extension before the first run.

#### Studio entry points

Within Gamebot Island you can approach development a few different ways:

| Persona / Goal                                         | Recommended Path                                                                                                   |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| Analysts who just want the data                        | Run `python scripts/create_notebook.py adhoc` (or `model`) to scaffold a notebook; template appends repo root to `sys.path` and preloads helper imports. |
| Engineers iterating on the pipeline, notebooks, or dbt | Use the **VS Code Dev Container** workflow (no local Python setup required).                                       |
| Engineers who prefer local tools                       | Use **Local Pipenv** and optionally run Airflow/Postgres via Docker.                                               |
| Operators running scheduled refreshes only             | Use the bundled **Docker Compose stack** (`make up`) on a server; let Airflow schedule the DAG.                    |

##### Dev Container vs. Local Pipenv

| Feature         | Dev Container                           | Local Pipenv                         |
| --------------- | --------------------------------------- | ------------------------------------ |
| Setup effort    | Minimal (prebuilt Python 3.11 + deps)   | Requires local Python 3.11 + Pipenv  |
| Reproducibility | High (identical across machines)        | Varies with host OS and toolchain    |
| Performance     | Slightly slower on macOS                | Native speed on host                 |
| Best for        | Onboarding, consistency, parity with CI | Power users who prefer local tooling |

#### Quick start (Docker + Makefile)

> Commands use standard POSIX syntax and work on macOS, Linux, and Windows (PowerShell or Git Bash). Substitute paths as needed for your environment.

This is the fastest way to spin up Airflow, Postgres, and Redis. It also creates the Airflow admin user.

1. **Clone the repo**

   ```bash
   git clone <repo-url>
   cd survivor_prediction   # repository folder name
   ```

2. **Create `.env`**

   ```bash
   pipenv run python scripts/setup_env.py dev --from-template
   ```

   The script will create `.env` if it doesn’t exist, fill in missing values from `env/.env.dev.example`, preserve any existing shared secrets, and sync everything to `airflow/.env`.
   Run this command inside the Dev Container **or** on the host after you have installed Pipenv.

3. **Start the stack**

   ```bash
   make up
   ```

   This runs `docker compose up airflow-init` and then `docker compose up -d` from the `airflow/` directory. It starts:

   * `warehouse-db` – Gamebot warehouse Postgres
   * `postgres` – Airflow metadata database
   * `redis` – Celery broker/result backend
   * Airflow webserver / scheduler / worker / triggerer

4. **Open Airflow**

   * URL: `http://localhost:${AIRFLOW_PORT:-8080}` (set `AIRFLOW_PORT` in `.env` if you need another port)
   * Login: `admin / admin`

5. **Trigger the DAG**

   * UI: Unpause and trigger `survivor_medallion_dag`
   * CLI (from `airflow/`):

     ```bash
     docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_dag
     ```

##### Handy Make targets

```bash
make up           # start/initialize Airflow + Postgres + Redis
make down         # stop the stack (keeps volumes)
make clean        # stop and remove volumes (fresh start)
make logs         # follow scheduler logs
make ps           # list services and status
make loader       # run the on-demand bronze loader (profile) container
```

Notes:

* `make up` is idempotent—it handles Airflow DB migrations and creates the `admin` user if missing.
* If another process is bound to `8080`, set `AIRFLOW_PORT=8081` (or any free port) in `.env`.

#### Dev Container workflow (recommended for development)

> Reference: [VS Code Dev Containers documentation](https://code.visualstudio.com/docs/devcontainers/containers).

Use VS Code **Dev Containers** to avoid managing Python locally.

1. Install VS Code + “Dev Containers” extension.
2. Open the repo in VS Code. Use the Command Palette (`Ctrl/⌘` + `Shift` + `P`) → **Dev Containers: Reopen in Container**.
3. When the container attaches, the repo is mounted at `/workspace` with Python 3.11, Pipenv, and the `gamebot` Jupyter kernel preconfigured (select it from the kernel picker if VS Code prompts).
4. Run orchestration (`make up`, `make down`, etc.) from the **host** terminal. Use the Dev Container terminal for Python/dbt commands (`pipenv run ...`) once the stack is up.
5. Pre-commit hooks are installed automatically during container creation (see `.devcontainer/devcontainer.json`). If you modify the hook set later, rerun `pipenv run pre-commit install`.

> Tip: Keep one host terminal for Docker/Make commands and a Dev Container terminal for `pipenv run ...`. You don’t need a host Python install if you work entirely inside the container.

#### Local Pipenv workflow (alternative)

Run development locally with your own Python while still using the Dockerised Airflow/Postgres, or run everything locally.

1. Install dependencies

   ```bash
   pip install pipenv
   pipenv install
   ```

2. Select an environment and create `.env`

   ```bash
   pipenv run python scripts/setup_env.py dev --from-template
   # edit .env if you prefer different DB host/name; use DB_HOST=warehouse-db to target the Docker Postgres
   ```

3. Start orchestration with Docker (recommended even for local Pipenv)

   ```bash
   make up
   # Airflow UI at http://localhost:${AIRFLOW_PORT}
   ```

4. Produce a bronze load from Python (optional if you rely solely on the Airflow DAG)

   ```bash
   pipenv run python -m Database.load_survivor_data
   ```

5. Run transformations locally (dbt)

   ```bash
   pipenv run dbt deps --project-dir dbt --profiles-dir dbt
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
   ```

6. Optional: Run everything locally without Docker

   * Provide your own Postgres 15+ credentials in `.env` (not `warehouse-db`)
   * Use `pipenv run python -m Database.load_survivor_data` for bronze
   * Run dbt as above
   * Skip `make up` and Airflow entirely if you don’t need scheduling

---

### 1.3 gamebot-lite (analyst package)

```bash
pip install --upgrade gamebot-lite
```

```python
from gamebot_lite import load_table, duckdb_query
df = load_table("vote_history_curated")
```

See [docs/gamebot_lite.md](docs/gamebot_lite.md) for table inventories (bronze/silver/gold), sample queries, and packaging workflow.

---

## 2. Environment profiles (dev vs prod)

* `SURVIVOR_ENV` controls the environment (`dev` by default).
* `env/.env.dev` and `env/.env.prod` hold the canonical profiles. Run `scripts/setup_env.py` whenever you want to switch between environments (or edit the profile files manually if you prefer); the script projects the selected profile into the root `.env`.
  * Usage:

    ```bash
    # Activate the dev profile using the checked-in env file (preferred)
    pipenv run python scripts/setup_env.py dev

    # Rehydrate from the template if env/.env.dev is missing or you want to reset it
    pipenv run python scripts/setup_env.py dev --from-template

    # Same options apply for prod
    pipenv run python scripts/setup_env.py prod
    ```
  * On first run the script seeds `env/.env.<env>` from `env/.env.<env>.example`. Edit the profile files (`env/.env.dev`, `env/.env.prod`) to change environment-specific defaults (database host/name, schedule, etc.).
  * After projecting a profile, the script writes the root `.env`, syncs `airflow/.env`, and keeps the Airflow connection JSON up to date.
  * Keys that only live in the current `.env` (for example private API tokens) are preserved when switching.
  * Set `GAMEBOT_DAG_SCHEDULE` in the profile or root `.env` to control the Airflow schedule before starting the stack.
  * Airflow rate limiting defaults (`AIRFLOW__API_RATELIMIT__STORAGE=redis://redis:6379/1`) are injected automatically.
  * `airflow/.env` is synced for you—no need to run `make sync-env` separately.
* Prod runs (typically via Docker + Airflow) target the containerised Postgres service (`warehouse-db`) and enforce running from the `main` branch.
* Control pipeline depth via `GAMEBOT_TARGET_LAYER` (`bronze`, `silver`, or `gold`; defaults to `gold`).
* If you ever need to refresh the Airflow connection definition manually:

  ```bash
  pipenv run python scripts/build_airflow_conn.py --write-airflow
  ```

### 2.1 `.env` keys (cheat sheet)

| Key | Description |
| --- | --- |
| `DB_HOST` | Hostname of the warehouse Postgres instance (`warehouse-db` when using Docker). |
| `DB_NAME` | Database name for the warehouse schema. |
| `DB_USER` / `DB_PASSWORD` | Credentials used by the loader, dbt, and Airflow connections. |
| `PORT` | Postgres port (leave as `5432` unless your DB listens elsewhere). |
| `SURVIVOR_ENV` | Logical environment (`dev` or `prod`). Influences Git safety checks and optional truncation rules. |
| `GAMEBOT_TARGET_LAYER` | Upper pipeline bound (`bronze`, `silver`, or `gold`). Controls how far the DAG runs. |
| `GAMEBOT_DAG_SCHEDULE` | Cron schedule for the Airflow DAG (default `0 4 * * 1`). |
| `AIRFLOW_PORT` | Host port exposed by the Airflow webserver (default `8080`). |
| `AIRFLOW__API_RATELIMIT__STORAGE` | Flask-Limiter backend for the Airflow API (defaults to shared Redis). |
| `AIRFLOW__API_RATELIMIT__ENABLED` | Toggle for API rate limiting (keep `True` unless you know you need to disable it). |

Any additional service-specific overrides can be added to `.env`; they will flow through to `airflow/.env` via `scripts/setup_env.py`.

### 2.2 Workflow tips

* Run `scripts/setup_env.py` **inside the Dev Container** as your first step (or on the host only after Pipenv is installed). It writes `.env`, syncs `airflow/.env`, and keeps Airflow connections aligned.
* After switching environments (e.g., `dev` → `prod`), restart the Docker stack from the host (`make down && make up`) so containers pick up the new values.
* Need a brand-new warehouse database? Update `.env` first, then remove the Postgres volume before restarting:

  ```bash
  make down
  make clean    # or: cd airflow && docker compose down -v
  make up
  ```

  Without wiping the volume, Postgres keeps the existing database/user even if `.env` changes.
* The Dev Container’s Pipenv virtualenv mirrors the runtime dependencies; use the container for Python/dbt commands and the host terminal only for Docker/Make invocations.

---

## 3. ETL architecture

### 3.1 Bronze – load `survivoR` data

```bash
pipenv run python -m Database.load_survivor_data
```

What happens:

1. `.rda` files are downloaded from GitHub (saved in `data_cache/`).
2. `Database/create_tables.sql` is applied on first run to create schemas (the loader calls this automatically; no manual step needed).
3. Each loader run records metadata in `bronze.ingestion_runs` and associates `ingest_run_id` with bronze tables. Data is merged with upsert logic (no truncation in prod). Lightweight dataframe validations run on key bronze tables (results land in `docs/run_logs/`). Logs list inserted/updated keys.

Tip: capture loader output to `docs/run_logs/<context>_<timestamp>.log` for PRs or incident reviews. Rerun when the upstream dataset changes or after a new episode.

---

### 3.2 Silver – curated tables

dbt models in `dbt/models/silver/` transform bronze into dimensions and facts. Legacy hand-written SQL refresh scripts now live in `Database/sql/legacy/` for reference only (they are no longer executed by the pipeline).

```bash
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
```

Legacy SQL remains for reference.

---

### 3.3 Gold – feature snapshots

```bash
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
```

* `gold.feature_snapshots` – metadata about each feature refresh
* `gold.castaway_season_features` – season-level feature payloads per castaway
* `gold.castaway_episode_features` – cumulative per-episode metrics
* `gold.season_features` – season-wide descriptors

Each execution rebuilds gold for the most recent ingestion run. Historical metadata is preserved in `gold.feature_snapshots`.

---

### 3.4 Explore with external SQL tools

Prefer a diagram you can interact with? Spin up the stack (`make up`) and connect a desktop SQL client such as **DBeaver**, DataGrip, or psql directly to the warehouse database:

| Setting | Value (default) |
| --- | --- |
| Host | `localhost`
| Port | `5433` (or `WAREHOUSE_DB_PORT` in your `.env`)
| Database | `DB_NAME` from `.env` (e.g., `survivor_dw_dev`)
| Username | `DB_USER` from `.env` (e.g., `survivor_dev`)
| Password | `DB_PASSWORD` from `.env`

The Postgres service runs in Docker but binds to the host, so the connection works from the host OS and from within the Dev Container (use host networking). Tools like DBeaver can auto-generate ERDs once connected, which is often clearer than the static PNG produced by `scripts/build_erd.py`. Keep the Python ERD script around if you need a quick image export, but feel free to lean on your SQL IDE for richer exploration.

---

## 6. Docker & Airflow orchestration

The DAG `airflow/dags/survivor_medallion_dag.py` automates the workflow (bronze → silver → gold) on a weekly schedule.

### 6.1 Start services

```bash
make up
# Airflow UI at http://localhost:${AIRFLOW_PORT:-8080} (default admin/admin)
```

### 6.2 Run the DAG

* UI: Unpause and trigger `survivor_medallion_dag`.
* CLI:

  ```bash
  cd airflow
  docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_dag
  ```

---

## 7. Gamebot Lite (analyst package)

```bash
pip install --upgrade gamebot-lite
```

```python
from gamebot_lite import load_table, duckdb_query
df = load_table("vote_history_curated")
```

See [docs/gamebot_lite.md](docs/gamebot_lite.md) for the complete table list (bronze, silver, gold), detailed column descriptions, sample DuckDB/pandas queries, packaging workflow, and notes on upcoming confessional text models.
See also [docs/gamebot_warehouse_schema_guide.md](docs/gamebot_warehouse_schema_guide.md) for a narrative walkthrough of silver facts/dimensions and [docs/gamebot_warehouse_cheatsheet.md](docs/gamebot_warehouse_cheatsheet.md) for quick join keys and external SQL IDE tips.

---

## 8. Schedules & releases

* Airflow is scheduled for **Monday mornings** (after upstream data updates).
* After a successful run, trigger a workflow to rebuild and publish a fresh `gamebot-lite` package (via GitHub Actions or manual release).

---

## 9. Studio vs. warehouse deployments

| Aspect | Studio (source build) | Warehouse (official images) |
| ------ | --------------------- | --------------------------- |
| Source | Built locally from this repo | Pulled from Docker Hub      |
| Code/DAGs | Source is bind-mounted for live edits; custom images can be built for prod | Baked into the published images |
| DB/Logs | Named Docker volumes | Named Docker volumes |
| Use case | Iteration, notebooks, prod-from-source | Turn-key deploy |

---

## 10. Repository map

```
Database/
  ├── create_tables.sql                # Bronze + silver schema DDL
  ├── load_survivor_data.py            # Bronze ingestion entrypoint
  └── sql/
      └── legacy/                      # Historical refresh scripts (dbt supersedes these)
          ├── refresh_silver_dimensions.sql
          ├── refresh_silver_facts.sql
          └── refresh_gold_features.sql
Utils/
  ├── db_utils.py                      # Schema validation, upsert logic
  ├── github_data_loader.py            # pyreadr wrapper + HTTP caching
  └── validation.py                    # Lightweight dataframe validations
scripts/
  ├── build_airflow_conn.py            # Sync Airflow connection from .env
  ├── check_versions.py                # Pipfile / Dockerfile alignment check
  ├── create_notebook.py               # Generate starter notebooks
  ├── export_sqlite.py                 # Export bronze/silver/gold → SQLite
  ├── setup_env.py                     # Create / switch .env and airflow/.env
  └── build_erd.py                     # Generate ERD (Graphviz)
airflow/
  ├── Dockerfile                       # Custom Airflow image
  ├── dags/
  │   └── survivor_medallion_dag.py    # Medallion pipeline DAG
  └── docker-compose.yaml              # Local orchestration stack
dbt/
  ├── models/
  ├── tests/
  └── profiles.yml
docs/
  ├── run_logs/                        # Loader & validation artifacts
  ├── erd/                             # Generated ERD assets
  └── gamebot_lite.md                  # Analyst documentation
gamebot_lite/                          # Lightweight SQLite package
.devcontainer/
  └── devcontainer.json
Dockerfile                             # Base image used by make loader profile
Makefile
params.py
Pipfile / Pipfile.lock
```

---

## 11. Troubleshooting

* Run `docker compose` from the **host**, not inside the Dev Container.
* Missing DAG changes? Stop the stack, rerun `make up` (the Compose file already bind-mounts DAGs and code from this repo).
* Port conflicts? Set `AIRFLOW_PORT` in `.env`.
* Fresh start? `make clean` removes volumes and images created by the Compose stack.
* Logs and status:

  ```bash
  make logs   # follow scheduler logs
  make ps     # service status
  ```

* Scheduler warnings about Flask-Limiter’s in-memory backend are safe for dev. Production configurations should keep the Redis-backed rate limiting enabled (handled automatically by `scripts/setup_env.py`).

---

## 12. Need to dive deeper?

* `docs/gamebot_lite.md` – Analyst table dictionary.
* `gamebot_lite/` – Source for the PyPI package.
* `scripts/export_sqlite.py` – Produce fresh SQLite snapshots for analysts.
