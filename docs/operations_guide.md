# Operations Guide

## Environment Configuration

Gamebot uses a **unified environment configuration system** with a single `.env` file that automatically adapts to different execution contexts.

### Quick Start

```bash
# 1. Copy template and customize
cp .env.example .env
# Edit .env with your database credentials and preferences

# 2. Launch complete stack
make fresh

# 3. Access Airflow UI and trigger pipeline
# http://localhost:8080 (admin/admin)
```

### Configuration File Structure

**Single Source of Truth**: The `.env` file at the repository root contains all configuration:

```bash
# .env (production-ready defaults)
DB_HOST=localhost              # Automatically overridden in containers
DB_NAME=survivor_dw_dev
DB_USER=survivor_dev
DB_PASSWORD=your_secure_password
PORT=5433                      # External port for local access
WAREHOUSE_DB_PORT=5433         # Docker port mapping

# Application settings
SURVIVOR_ENV=dev
GAMEBOT_TARGET_LAYER=gold      # Pipeline depth (bronze/silver/gold)
GAMEBOT_DAG_SCHEDULE=0 4 * * 1 # Weekly Monday 4AM UTC

# Airflow configuration
AIRFLOW_PORT=8080              # Web interface port
AIRFLOW__API_RATELIMIT__STORAGE=redis://redis:6379/1
AIRFLOW__API_RATELIMIT__ENABLED=True

# Optional integrations
GITHUB_TOKEN=                  # For release automation
```

### Context-Aware Networking

**Automatic Environment Detection**: Docker Compose automatically overrides database connection parameters for container networking:

| Context | DB_HOST | PORT | Usage |
|---------|---------|------|-------|
| **Local Development** | `localhost` | `5433` | Direct host access |
| **Container (Airflow)** | `warehouse-db` | `5432` | Internal networking |
| **External Tools** | `localhost` | `5433` | DBeaver, notebooks, etc. |

**No Manual Configuration Required**: The system detects execution context and applies appropriate connection parameters automatically.
### Configuration Keys Reference

| Key | Description | Example |
| --- | --- | --- |
| `DB_HOST` | Database hostname (context-aware) | `localhost` |
| `DB_NAME` | Warehouse database name | `survivor_dw_dev` |
| `DB_USER` / `DB_PASSWORD` | Database credentials | `survivor_dev` |
| `PORT` | Database port (context-aware) | `5433` |
| `WAREHOUSE_DB_PORT` | Docker port mapping | `5433` |
| `SURVIVOR_ENV` | Environment identifier | `dev` or `prod` |
| `GAMEBOT_TARGET_LAYER` | Pipeline depth control | `bronze`, `silver`, or `gold` |
| `GAMEBOT_DAG_SCHEDULE` | Airflow cron schedule | `0 4 * * 1` |
| `AIRFLOW_PORT` | Web interface port | `8080` |

### Operational Workflow

**Standard Operations**:

```bash
# Environment setup (one-time)
cp .env.example .env          # Create configuration
# Edit .env with your settings

# Daily operations
make fresh                    # Start complete stack
make logs                     # Monitor execution
make ps                       # Check service status
make down                     # Stop services (keep data)

# Maintenance
make clean && make fresh      # Fresh start (removes all data)
```

**Database Management**:

```bash
# Connect to warehouse database
docker compose exec warehouse-db psql -U survivor_dev survivor_dw_dev

# Database backup
docker compose exec warehouse-db pg_dump -U survivor_dev survivor_dw_dev > backup.sql

# Reset database (removes all data)
make clean && make fresh
```

### Production Considerations

**Security**:
- Change default Airflow credentials (`admin/admin`) in `.env`
- Use strong database passwords
- Enable SSL for production database connections
- Restrict network access to Airflow web interface

**Scaling**:
- Adjust `AIRFLOW__CELERY__WORKER_CONCURRENCY` for parallel task execution
- Configure external PostgreSQL for production workloads
- Use external Redis for production Celery broker
- Monitor disk usage for Docker volumes

**Monitoring**:
- Airflow UI provides comprehensive DAG execution monitoring
- Database query logs available via PostgreSQL configuration
- Container logs accessible via `docker compose logs`

### Accessing Pipeline Outputs

Each pipeline run produces two types of outputs:

**1. Validation Reports** (Business Artifacts)

Validation reports are **automatically saved to your project directory**:

```bash
# View latest validation reports
ls -la run_logs/validation/

# Reports are organized by run:
# run_logs/validation/Run 0001 - <RUN_ID> Validation Files/
#   ├── data_quality_<run_id>_<timestamp>.xlsx  (Excel report)
#   ├── validation_<table>_<timestamp>.json     (JSON per table)
#   └── .run_id                                  (run metadata)
```

**Why this works**: Validation reports are mounted to the host filesystem via Docker volumes, making them directly accessible without container commands.

**2. Airflow Task Logs** (Pipeline Execution Logs)

Task logs (bronze/silver/gold execution output) are stored in Docker volumes. **Two ways to access:**

**Method A: Airflow UI** (Recommended)
1. Navigate to http://localhost:8080
2. Select `survivor_medallion_pipeline` DAG
3. Click any task (`load_bronze_layer`, `dbt_build_silver`, etc.)
4. View logs with syntax highlighting and search

**Method B: CLI Access**
```bash
# View specific task logs
docker compose exec airflow-scheduler airflow tasks logs \
  survivor_medallion_pipeline load_bronze_layer --latest

# View dbt transformation logs
docker compose exec airflow-scheduler airflow tasks logs \
  survivor_medallion_pipeline dbt_build_silver --latest

# Copy specific log file if needed
docker compose cp gamebot-airflow-worker:/opt/airflow/logs/dag_id=survivor_medallion_pipeline ./local_logs/
```

**Why Docker Volumes for Task Logs?**
- **Performance**: Better I/O performance than bind mounts
- **Portability**: Consistent across all host operating systems
- **Standard Practice**: Matches Apache Airflow, AWS MWAA, Google Cloud Composer
- **Production Ready**: Designed for centralized logging (ELK, Splunk, CloudWatch)

This separation follows industry best practices:
- **Validation reports** (business artifacts) → Host-mounted → Direct access
- **Task logs** (operational logs) → Docker volumes → Access via UI or CLI

---

## ETL Pipeline Architecture

### Medallion Data Flow

The pipeline implements a strict **bronze → silver → gold** progression:

```
survivoR API → Bronze (Python) → Silver (dbt) → Gold (dbt) → ML Features
    ↓              ↓                ↓             ↓
Raw Data → Validation & → Feature → Production
Archive    Metadata    Engineering   ML Tables
```

### Pipeline Execution

**Automated Orchestration**: The `survivor_medallion_pipeline` DAG orchestrates the complete data flow:

1. **Data Freshness Detection**: Monitors upstream survivoR dataset changes
2. **Bronze Ingestion**: Python-based data loading with comprehensive validation
3. **Silver Transformation**: dbt models for strategic feature engineering
4. **Gold Aggregation**: ML-ready feature matrices with quality testing
5. **Metadata Persistence**: Dataset versioning and lineage tracking

**Manual Execution**:

```bash
# Trigger complete pipeline
docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline

# Run individual layers (for development/testing)
make loader                                    # Bronze only
pipenv run dbt build --select silver         # Silver only
pipenv run dbt build --select gold           # Gold only
```

### dbt Integration & Container Permissions

**Container Execution**: dbt runs successfully in Airflow containers with permission-aware configuration:

```bash
# DAG task configuration (automated)
mkdir -p /tmp/dbt_logs /tmp/dbt_target
dbt build --project-dir dbt --profiles-dir dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

**Local Development**: For direct dbt development:

```bash
# Test dbt connection
pipenv run dbt debug --project-dir dbt --profiles-dir dbt

# Run transformations locally
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
```

**Key Innovation**: The containerized dbt execution uses writable temporary directories (`/tmp/dbt_logs`, `/tmp/dbt_target`) to resolve permission conflicts between the Airflow user (uid 50000) and host-mounted volumes (uid 1000).

### Data Quality & Validation

**Bronze Layer**: Python-based validation ensures data integrity:
- Schema validation against expected survivoR structure
- Data type enforcement and conversion
- Duplicate detection and resolution
- Metadata tracking for data lineage

**Excel Validation Reports**: Each pipeline run generates comprehensive data quality reports in Excel format with detailed validation results, row counts, column analysis, and remediation notes.

**Accessing Validation Reports** (Recommended Approach):

Validation reports are **automatically accessible in your project directory** via host-mounted volumes:

```bash
# View latest validation reports (direct host access)
ls -la run_logs/validation/

# Reports are organized by run:
# run_logs/validation/Run 0001 - <RUN_ID> Validation Files/
#   ├── data_quality_<run_id>_<timestamp>.xlsx  (Excel report)
#   ├── validation_<table>_<timestamp>.json     (JSON per table)
#   └── .run_id                                  (run metadata)
```

**Why this works**: Validation reports are mounted to the host filesystem via Docker volumes, making them directly accessible without container commands.

**Report Contents**:
- **Summary sheet**: Overall validation status and key metrics
- **Table details**: Row counts, column types, and schema validation
- **Data quality**: Duplicate detection, null value analysis, referential integrity
- **Remediation notes**: Detailed explanations of any data fixes applied
- **Schema drift**: Detection of upstream structure changes

**Silver Layer**: dbt tests validate feature engineering:
- Referential integrity checks
- Non-null constraints on key columns
- Unique key validation
- Custom business logic validation

**Gold Layer**: Production ML feature validation:
- Feature completeness testing
- Statistical validation of aggregations
- Cross-table consistency checks
- ML-readiness validation

### Pipeline Results

**Successful Execution Produces**:
- **Bronze**: 21 tables with 193,000+ raw records from survivoR
- **Silver**: 8 curated tables with strategic gameplay features
- **Gold**: 2 ML-ready matrices with 4,248 observations each
- **Testing**: 13 dbt tests ensuring comprehensive data qualityAny additional service-specific overrides can be added to `.env`; they will flow through to `airflow/.env` via `scripts/setup_env.py`.

### Workflow tips

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

## ETL Architecture

### Bronze – load `survivoR` data

```bash
pipenv run python -m Database.load_survivor_data
```

What happens:

1. The loader checks both the `.rda` exports and the JSON mirrors under `dev/json/`, downloads whichever changed most recently (cached in `data_cache/`), and falls back to the `.rda` when the timestamps tie.
2. `Database/create_tables.sql` is applied on first run to create schemas (the loader calls this automatically; no manual step needed).
3. Each loader run records metadata in `bronze.ingestion_runs` and associates `ingest_run_id` with bronze tables. `bronze.dataset_versions` captures the content fingerprint, upstream commit, and whether the data came from the `.rda` or JSON export. Data is merged with upsert logic (no truncation in prod). Lightweight dataframe validations run on key bronze tables (results land in `run_logs/validation/`). Logs list inserted/updated keys.
4. Vote-history rows that reference missing `challenge_id` values are auto-remediated: first via stage-of-game (`sog_id`) alignment with `bronze.challenge_results`, with the small fallback map in `gamebot_core/db_utils.py` covering historical edge cases. Every fix is logged (sampled) so you can notify the survivoR maintainers.
5. Need to sanity-check a particular column definition? `survivoR.pdf` in the repo root is the upstream R documentation we mirror; search it for the dataset name to see the canonical description.

Tip: capture loader output to `run_logs/<context>_<timestamp>.log` for PRs or incident reviews. Zip the file (e.g., `zip run_logs/dev_branch_20250317.zip run_logs/dev_branch_20250317.log`) and attach the archive to your pull request or share a public link so reviewers can download the clean run. Schema drift warnings are also appended to `run_logs/notifications/schema_drift.log` so you can quickly see when survivoR introduces new columns or types. If survivoR publishes entirely new tables, the loader will flag them in the same log (and, when `GITHUB_REPO`/`GITHUB_TOKEN` are set, open an issue automatically), but they will not load automatically—you decide when to extend `Database/db_run_config.json` and the bronze DDL. Rerun when the upstream dataset changes or after a new episode.

#### Quick log access

* `pipenv run python scripts/show_last_run.py --tail` — show the newest artefact (validation report, schema drift, etc.).
* `make show-last-run ARGS="--tail --category validation"` — same command via Make; handy inside the Dev Container.
* Docker-only workflow? `docker compose exec devshell make show-last-run ARGS="--tail"` provides the same experience.
* Need logs elsewhere? Set `GAMEBOT_RUN_LOG_DIR=/path/on/host` before running the stack to relocate the artefacts (helpful when sharing a Docker volume).
* Each run also emits an Excel workbook at `run_logs/validation/data_quality_<timestamp>.xlsx`. Recent changes expanded it to include:
  * Per-dataset tabs with rule outcomes, uniqueness/FK checks, remediation events, and “Reference Records” tables showing the raw rows used to backfill data (e.g., journeys fuzzy matches).
  * A version-season coverage section that highlights which seasons were missing or unexpectedly present.
  * A **Metadata Summary** tab that compares survivoR’s upstream dataset catalogue against the tables we load and tracks schema drift (unexpected/missing columns). Identity/housekeeping columns (`*_id`, `ingest_run_id`, `ingested_at`) are treated as auto-managed so they don’t trigger false positives. Set `GITHUB_TOKEN` if you want the upstream comparison to work without hitting rate limits.
* To produce the workbook while running the loader in a disposable container, mount the log directory:

  ```bash
  docker compose run --rm \
    -e GAMEBOT_RUN_LOG_DIR=/workspace/run_logs \
    -v $(pwd)/run_logs:/workspace/run_logs \
    --profile loader survivor-loader
  ```
  Install `openpyxl` in your environment if the workbook export logs a warning about missing engines.
* Uniqueness guardrails: every dataset that declares a unique key in `Database/table_config.json` stops the load when duplicates appear — except `bronze.challenge_summary`. That upstream helper intentionally publishes multiple category rows per castaway/challenge, so the loader logs the overlap (and the Excel report calls it out) but continues. All other tables require manual intervention when a uniqueness breach is detected.

Only 13 survivoR tables ship by default (`Database/db_run_config.json` lists the current set). When upstream adds more tables or reshapes a schema, the drift log + optional GitHub issue tells you exactly what changed so you can opt-in intentionally.

> Optional automation: set `GITHUB_REPO` (e.g., `user/project`) and `GITHUB_TOKEN` in your `.env` to have schema drift warnings automatically open a GitHub issue for follow-up.

- GitHub metadata lookups are optional. Without `GITHUB_TOKEN`, the loader skips commit metadata to avoid rate limits; set a personal access token if you want commit hashes/timestamps recorded for every dataset. This token is only for dataset metadata — release automation continues to use the separate tokens referenced in the GitHub Actions guide (e.g., `AIRFLOW_GITHUB_TOKEN`).

---

### Silver – ML feature engineering tables

dbt models in `dbt/models/silver/` transform bronze into strategic feature categories for machine learning analysis. The silver layer creates 8 feature tables organized by gameplay dimensions: demographics, challenges, advantages, voting, social dynamics, edit analysis, jury relationships, and season context.

```bash
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
```

Key silver tables:
* `silver.castaway_profile` – Demographics and background features
* `silver.challenge_performance` – Physical and mental challenge data
* `silver.advantage_strategy` – Strategic advantage gameplay
* `silver.vote_dynamics` – Voting behavior and alliances
* `silver.social_positioning` – Social dynamics and tribe composition
* `silver.edit_features` – Production narrative and screen time
* `silver.jury_analysis` – Endgame relationships and jury votes
* `silver.season_context` – Season format and meta-game features

---

### Gold – ML-ready feature aggregations

```bash
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
```

The gold layer provides two ML-ready feature tables for different modeling approaches:
* `gold.ml_features_non_edit` – Pure gameplay features for testing if winners can be predicted without edit data
* `gold.ml_features_hybrid` – Combined gameplay and edit features for comprehensive winner prediction models

Each table aggregates features at the castaway × season level (3,133 rows) with target variables for machine learning training. Gold tables are rebuilt after silver completes successfully, ensuring downstream ML models always use consistent, up-to-date features.

---

### Explore with external SQL tools

The Postgres service runs in Docker but binds to the host, so the connection works from the host OS and from within the Dev Container (use host networking). The VS Code Dev Container attaches to the Compose-managed `devshell` service, so it automatically shares the same Docker network as Airflow/Postgres—no manual network juggling required. Tools like DBeaver can auto-generate ERDs once connected, which is often clearer than the static PNG produced by `scripts/build_erd.py`. If you’re on Gamebot Studio, you can also query the same database directly from the repo’s notebooks using the bundled Pipenv environment. Pick whichever client fits your workflow.

---

## Operations & Scheduling

Gamebot runs on a weekly Airflow cadence (`GAMEBOT_DAG_SCHEDULE`, default early Monday UTC). The API rate limiting settings (`AIRFLOW__API_RATELIMIT__*`) keep the Airflow REST endpoint safe when multiple notebooks or automations connect—raise them only if you understand the trade-offs.

Need a refresher on how Airflow's Celery executor wiring works? SparkCodeHub's [Airflow + Celery executor tutorial](https://www.sparkcodehub.com/airflow/integrations/celery-executor) walks through the moving parts and common gotchas.

The DAG `airflow/dags/survivor_medallion_dag.py` automates the workflow (bronze → silver → gold) on a weekly schedule.

> **Production guard:** when `SURVIVOR_ENV=prod`, all mutating scripts (Airflow loader, `export_sqlite`, preprocessing helpers) require the current git branch to be `main`. This prevents accidental prod runs from feature branches.

### Start services

```bash
make up
# Airflow UI at http://localhost:${AIRFLOW_PORT:-8080} (credentials come from `.env`—change the defaults before production)
```

### Run the DAG

* UI: Unpause and trigger `survivor_medallion_dag`.
* CLI:

  ```bash
  cd airflow
  docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_dag
  ```

---

## Releases

Gamebot ships three artefacts that map to the layers described earlier:

| Artefact | Layer(s) covered | Delivery channel | Typical tag |
| --- | --- | --- | --- |
| Warehouse refresh | Bronze → Silver → Gold (Airflow/dbt + notebooks) | Git branch `main`, Docker stack, notebooks | `data-YYYYMMDD` |
| Gamebot Lite snapshot | Analyst SQLite + helper API | PyPI package, `gamebot_lite/data` | `data-YYYYMMDD` (same tag as warehouse refresh) |
| Application code | Python package, Docker images, notebooks | PyPI (`gamebot-lite`), Docker Hub, repo source | `code-vX.Y.Z` |

The upstream [`survivoR`](https://github.com/doehm/survivoR) project publishes both `.rda` files (`data/`) **and** JSON mirrors (`dev/json/`). They usually move together, but the JSON branch is sometimes a little behind. Gamebot’s monitor watches both so you know when to refresh bronze.

Airflow’s scheduler keeps bronze → silver → gold fresh on a cadence, but wrapping a data drop into a tagged release (or shipping a new code version to PyPI/Docker) is still an explicit, human-in-the-loop action. The helper script `python scripts/tag_release.py` cuts the git tags for you, and future CI automation can hook into it once we’re comfortable with fully automated releases.

> The steps below can be run manually (from your terminal) **or** via the GitHub “Manual Release Tag” workflow, which simply invokes the same tagging script in CI.

### Monitor upstream survivoR updates

- A scheduled GitHub Action (`.github/workflows/upstream-survivor-monitor.yml`) runs daily and on demand. It calls `scripts/check_survivor_updates.py`, compares the recorded commits in `monitoring/survivor_upstream_snapshot.json`, and opens/updates an issue tagged `upstream-monitor` if new data appears.
- The script writes a Markdown report (`monitoring/upstream_report.md`, ignored in git) so you can review exactly which directory changed (RDA vs JSON) and the upstream commit.
- After you ingest the new data, run `python scripts/check_survivor_updates.py --update` locally to record the latest commit hashes. That keeps the nightly action green until the next upstream drop.

### Data release (warehouse + Gamebot Lite)

1. Confirm upstream data changed (via the Action or manual run of `python scripts/check_survivor_updates.py`).
2. Run the bronze loader and downstream dbt models from the Dev Container:
   ```bash
   pipenv run python -m Database.load_survivor_data
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
   ```
3. Export the refreshed SQLite snapshot and package it for analysts:
   ```bash
   pipenv run python scripts/export_sqlite.py --layer silver --package
   python scripts/smoke_gamebot_lite.py
   ```
4. Commit the changes (dbt artefacts, docs, snapshot metadata) and merge to `main`.
5. Tag the release with the helper script (defaults to today’s UTC date): `python scripts/tag_release.py data --date 20250317`
6. Want to double-check before publishing? Use `--no-push` and later run `git push origin data-20250317`.
7. Update the upstream snapshot baseline: `python scripts/check_survivor_updates.py --update` (commit the refreshed `monitoring/survivor_upstream_snapshot.json`).

### Code release (package + Docker images)

1. Bump versions (`pyproject.toml` for `gamebot-lite`, Docker image tags if applicable).
2. Re-run the verification items from the PR checklist, including `python scripts/smoke_gamebot_lite.py` if the SQLite file ships with the release.
3. Merge to `main`, then tag with the helper script: `python scripts/tag_release.py code --version v1.2.3`
4. As with data tags, you can add `--no-push` first and publish later with `git push origin code-v1.2.3`.
5. Publish artefacts (PyPI via `pipenv run python -m build` + `twine upload`, Docker images via `docker build` + `docker push`) as appropriate.

When both data and code change in the same commit, run the smoke test once, tag twice (`data-…` and `code-…`), and note both in the release notes. We now automate the repetitive git commands via `scripts/tag_release.py`; a future GitHub Action could trigger it automatically after CI—contributions welcome.

---

## Delivery Modes

| Aspect | Studio (source build) | Warehouse (official images) |
| ------ | --------------------- | --------------------------- |
| Source | Built locally from this repo | Pulled from Docker Hub      |
| Code/DAGs | Source is bind-mounted for live edits; custom images can be built for prod | Baked into the published images |
| DB/Logs | Named Docker volumes | Named Docker volumes |
| Use case | Iteration, notebooks, prod-from-source | Turn-key deploy |

---

## Automation & CI

- **CI (`.github/workflows/ci.yml`)** runs pre-commit and a lightweight compile sanity check on every PR or push.
- **Manual Release Tag (`.github/workflows/manual-tag.yml`)** triggers the same tagging script used locally so you can publish `data-YYYYMMDD` or `code-vX.Y.Z` tags from the Actions tab.
- See [docs/github_actions_quickstart.md](github_actions_quickstart.md) for a walkthrough of these workflows.

---

## Troubleshooting

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
