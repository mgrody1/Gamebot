# Architecture Overview

## Gamebot Warehouse (registry deployment)

Use the warehouse images when you need a turn-key deployment without cloning this repo.

### Prerequisites

* Docker Engine / Docker Desktop (Compose v2)
* A `.env` containing Postgres, Redis, and Airflow settings (same structure as the repo’s `.env`)

### Compose skeleton

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

### Operating the stack

```bash
# Start the stack (images are pulled automatically if missing)
docker compose up -d

# One-off bronze load (bronze only; silver/gold run via the Airflow DAG)
docker compose run --rm --profile loader survivor-loader
```

Open Airflow at `http://localhost:${AIRFLOW_PORT:-8080}` (default `8080`). Login with `admin / admin`, unpause `survivor_medallion_dag`, and trigger a run. The DAG orchestrates **bronze → silver → gold** in sequence. The `survivor-loader` container is only for ad-hoc bronze refreshes (for example, to ingest new raw data ahead of a scheduled DAG run).

To adjust the DAG schedule before bringing the stack online, set `GAMEBOT_DAG_SCHEDULE` (cron expression) in your `.env`. The default is `0 4 * * 1` (early Monday UTC to capture weekend data entry).

---

## Gamebot Studio (developer environment)

Clone this repository when you want to contribute, customise the pipeline, or run prod straight from source.

### Requirements

* [Docker Engine or Docker Desktop (Compose v2 included)](https://docs.docker.com/get-started/introduction/get-docker-desktop/)
* [Make (GNU make)](https://www.gnu.org/software/make/manual/html_node/index.html)
* [Git](https://github.com/git-guides/install-git)
* Optional (for local Python workflows):

  * Python 3.11
  * Pipenv (`pip install pipenv`)
  * PostgreSQL 15+ (only if you are *not* using the bundled Docker services)
  * pre-commit (`pip install pre-commit`) – the Dev Container runs `pipenv install --dev`, but because the repo currently has no dev-packages, install `pre-commit` manually if you intend to use the hooks (`pipenv install --dev pre-commit && pipenv run pre-commit install`)
* If you point Gamebot at your own Postgres instance (instead of the bundled `warehouse-db`), make sure the role used for ingestion can create schemas and the `uuid-ossp` extension before the first run.

### Studio entry points

Within Gamebot Studio you can approach development a few different ways:

| Persona / Goal                                         | Recommended Path                                                                                                   |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| Analysts who just want the data                        | Run `python scripts/create_notebook.py adhoc` (or `model`) to scaffold a notebook; template appends repo root to `sys.path` and preloads helper imports. |
| Engineers iterating on the pipeline, notebooks, or dbt | Use the **VS Code Dev Container** workflow (no local Python setup required).                                       |
| Engineers who prefer local tools                       | Use **Local Pipenv** and optionally run Airflow/Postgres via Docker.                                               |
| Operators running scheduled refreshes only             | Use the bundled **Docker Compose stack** (`make up`) on a server; let Airflow schedule the DAG.                    |

#### Dev Container vs. Local Pipenv

| Feature         | Dev Container                           | Local Pipenv                         |
| --------------- | --------------------------------------- | ------------------------------------ |
| Setup effort    | Minimal (prebuilt Python 3.11 + deps)   | Requires local Python 3.11 + Pipenv  |
| Reproducibility | High (identical across machines)        | Varies with host OS and toolchain    |
| Performance     | Slightly slower on macOS                | Native speed on host                 |
| Best for        | Onboarding, consistency, parity with CI | Power users who prefer local tooling |

### Quick start (Docker + Makefile)

> Commands use standard POSIX syntax and work on macOS, Linux, and Windows (PowerShell or Git Bash). Substitute paths as needed for your environment.

This is the fastest way to spin up Airflow, Postgres, and Redis. It also creates the Airflow admin user.

1. **Clone the repo**

   ```bash
   git clone https://github.com/mgrody1/Gamebot.git
   cd Gamebot   # repository folder name
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

#### Handy Make targets

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

### Dev Container workflow (recommended for development)

> Reference: [VS Code Dev Containers documentation](https://code.visualstudio.com/docs/devcontainers/containers).

Use VS Code **Dev Containers** to avoid managing Python locally.

1. Install VS Code + “Dev Containers” extension.
2. Open the repo in VS Code. Use the Command Palette (`Ctrl/⌘` + `Shift` + `P`) → **Dev Containers: Reopen in Container**.
3. When the container attaches, the repo is mounted at `/workspace` with Python 3.11, Pipenv, and the `gamebot` Jupyter kernel preconfigured (select it from the kernel picker if VS Code prompts).
4. Run orchestration (`make up`, `make down`, etc.) from the **host** terminal. Use the Dev Container terminal for Python/dbt commands (`pipenv run ...`) once the stack is up.
5. Pre-commit hooks are installed automatically during container creation (see `.devcontainer/devcontainer.json`). If you modify the hook set later, rerun `pipenv run pre-commit install`.

> Tip: Keep one host terminal for Docker/Make commands and a Dev Container terminal for `pipenv run ...`. You don’t need a host Python install if you work entirely inside the container.

> Notebook workflow: See [CONTRIBUTING.md](../CONTRIBUTING.md) for how Jupytext keeps notebooks and scripts in sync (pairing commands, VS Code task, pre-commit integration). The write-up references [this tutorial](https://bielsnohr.github.io/2024/03/04/jupyter-notebook-scripts-jupytext-vscode.html) if you want more context.

### Local Pipenv workflow (alternative)

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

## gamebot-lite (analyst package)

```bash
pip install --upgrade gamebot-lite
```

```python
from gamebot_lite import load_table, duckdb_query
df = load_table("vote_history_curated")
```

See [gamebot_lite.md](gamebot_lite.md) for table inventories (bronze/silver/gold), sample queries, and packaging workflow.
