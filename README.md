# 🏝️ Survivor Prediction Warehouse

End-to-end data engineering project for analyzing CBS's *Survivor* and generating machine learning features that predict the winner after each new episode. The repository now implements a Medallion architecture (bronze ➝ silver ➝ gold) backed by PostgreSQL, automated data ingestion from the open-source [`survivoR`](https://github.com/doehm/survivoR) project, and Apache Airflow orchestration.

---

## 🧱 Architecture At A Glance

- **Bronze layer** (`bronze.*`): Raw replicas of the survivoR R datasets (RDA files). Each table stores provenance metadata and aligns 1:1 with the upstream schema.
- **Silver layer** (`silver.*`): Curated dimensions and fact tables ready for analytics—`dim_castaway`, `dim_season`, `fact_challenge_results`, etc. Supporting lookups (e.g., challenge skill bridge) normalize wide boolean flags.
- **Gold layer** (`gold.*`): Machine-learning feature stores. `gold.castaway_episode_features` tracks per-castaway/per-episode signals, while `gold.season_prediction_features` snapshots season-level aggregates for winner prediction.
- **Orchestration**: Apache Airflow DAG (`survivor_medallion_pipeline`) loads bronze data, refreshes silver dimensions/facts with SQL, and materializes gold features.

---

## ⚙️ Prerequisites & Local Setup

1. **Install dependencies**
   ```bash
   pip install pipenv
   pipenv install
   ```
   Key runtime packages: `pandas`, `SQLAlchemy`, `psycopg2-binary`, `requests`, and `pyreadr` (for reading `.rda` files).

2. **Provision PostgreSQL**
   - Create a database (e.g., `survivor_dw`).
   - Ensure the connection user can create schemas and run `TRUNCATE` / `INSERT`.

3. **Create `.env` at repo root**
   ```env
   DB_HOST=localhost
   DB_NAME=survivor_dw
   DB_USER=your_user
   DB_PASSWORD=your_password
   PORT=5432
   ```

4. **Configure loader settings** (`Database/db_run_config.json`)
   - Controls whether tables are truncated, which bronze datasets to ingest, and the GitHub source URL.
   - Defaults target the survivoR GitHub raw data and expect all bronze tables to be refreshed each run.

---

## 🥉 Bronze Layer — GitHub Ingestion

```bash
pipenv run python -m Database.load_survivor_data
```

The loader will:
1. Download the required `.rda` files from GitHub (cached in `data_cache/`).
2. Create / refresh schemas via `Database/create_tables.sql` when `first_run` is `true`.
3. Validate each dataset against the target table definition and append or truncate based on config.

Re-run the command whenever new Survivor data drops—the cache is refreshed automatically when files change or when `force_refresh` is enabled in the config.

---

## 🥈 Silver Layer — Curated Dimensions & Facts

SQL scripts in `Database/sql/` populate the curated layer from bronze tables:
- `refresh_silver_dimensions.sql`
- `refresh_silver_facts.sql`

To execute manually (example using `psql`):
```bash
psql "$DATABASE_URL" -f Database/sql/refresh_silver_dimensions.sql
psql "$DATABASE_URL" -f Database/sql/refresh_silver_facts.sql
```

---

## 🥇 Gold Layer — Feature Stores

`Database/sql/refresh_gold_features.sql` aggregates the silver facts into per-episode feature JSON and season-level snapshots tailored for ML training and inference.

```bash
psql "$DATABASE_URL" -f Database/sql/refresh_gold_features.sql
```

---

## 🪂 Airflow Orchestration

An Airflow DAG ties the full pipeline together.

1. **Prepare environment**
   ```bash
   cd airflow
   cp .env.example .env  # update credentials and connection strings
   docker compose up airflow-init
   docker compose up -d
   ```

2. **Connections**
   - The compose file reads `AIRFLOW_CONN_SURVIVOR_POSTGRES` to configure the `survivor_postgres` connection used by the DAG’s `PostgresOperator` tasks.
   - `.env` also injects the same DB credentials so Python operators (`Database.load_survivor_data`) align with `params.py`.

3. **DAG flow**
   ```
   load_bronze_layer
       ➝ refresh_silver_dimensions
       ➝ refresh_silver_facts
       ➝ refresh_gold_features
   ```
   Trigger manually from the UI or let it run on the default weekly schedule.

---

## 📁 Repository Highlights

- `Database/create_tables.sql` — DDL for bronze/silver/gold schemas (idempotent, with FK / index best practices).
- `Database/sql/*.sql` — Reusable transformation scripts for each layer.
- `Database/load_survivor_data.py` — Bronze ingestion driver.
- `Utils/github_data_loader.py` — Downloads & reads survivoR `.rda` assets.
- `Utils/db_utils.py` — DB helpers, schema validation, and ingestion routines.
- `airflow/dags/survivor_medallion_dag.py` — Airflow DAG definition.
- `airflow/docker-compose.yaml` — Local Airflow deployment (Celery executor) with PostgreSQL & Redis.

---

## ✅ Next Steps

- Run Airflow or the SQL scripts to refresh silver/gold layers after the bronze load.
- Explore the gold tables to engineer ML features or plug into notebooks/models.
- Extend the Airflow DAG with model training or scoring tasks to complete the MLOps loop.
