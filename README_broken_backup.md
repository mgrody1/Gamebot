# Gamebot

<p align="center">
  <img src="https://i.redd.it/icb7a6pmyf0c1.jpg" alt="Dabu Doodles Survivor art" width="480">
</p>

> Art by [Dabu Doodles (Erik Reichenbach)](https://dabudoodles.com/)

## What is a Gamebot in the CBS Reality Competition Show Survivor?

[*Survivor Term Glossary (search for Gamebot)*](https://insidesurvivor.com/the-ultimate-survivor-glossary-980)

[*What is a Gamebot in Survivor? Thread*](https://www.reddit.com/r/survivor/comments/37hu6i/what_is_a_gamebot/)

## What is a Gamebot Outside of the Game? **This Repository!**:

Gamebot is a production-ready Survivor analytics stack that implements a complete **medallion lakehouse architecture** using Apache Airflow + dbt + PostgreSQL. It ingests the comprehensive [`survivoR`](https://github.com/doehm/survivoR) dataset, transforms it through bronze → silver → gold layers, and delivers ML-ready features for winner prediction research.

The architecture follows a **medallion lakehouse pattern** optimized for ML feature engineering:
- **Bronze Layer** (21 tables): Raw survivoR dataset tables with comprehensive ingestion metadata and data lineage
- **Silver Layer** (8 tables + 9 tests): ML-focused feature engineering organized by strategic gameplay categories (challenges, advantages, voting dynamics, social positioning, edit analysis)
- **Gold Layer** (2 tables + 4 tests): Two production ML-ready feature matrices for different modeling approaches (gameplay-only vs hybrid gameplay+edit features)

**What makes this special**: The entire pipeline runs seamlessly in containerized Apache Airflow with automated dependency management, comprehensive data validation, and zero-configuration setup. Perfect for data scientists who want to focus on analysis rather than infrastructure.

For a detailed reference of the upstream schema we mirror, see [survivoR.pdf](survivoR.pdf) in the project root (a copy of the official survivoR R documentation).

Huge thanks to [Daniel Oehm](https://gradientdescending.com/) and the `survivoR` community; if you haven't already, please check [`survivoR`](https://github.com/doehm/survivoR) out!

### What you can explore
- [Check out these Survivor analyses with the survivoR dataset](https://gradientdescending.com/category/survivor/) as examples of the types of analyses you can now more easily accomplish in python and SQL with Gamebot.

---

## **Choose Your Adventure**

Gamebot serves three distinct user personas with different needs and technical comfort levels. **Choose the approach that best matches your goals:**

| **User Persona** | **Primary Goal** | **Technical Setup** | **What You Get** | **Quick Start** |
|------------------|------------------|-------------------|------------------|-----------------|
| **Data Analysts & Scientists** | Immediate data access for notebooks, prototyping, and analysis without infrastructure setup | Zero infrastructure - just pip install | Complete Survivor dataset as portable SQLite with pandas/DuckDB helpers. Perfect for Jupyter notebooks, research, and quick analysis. | [→ 5-Minute Setup](#gamebot-lite---immediate-data-access) |
| **Data Teams & Organizations** | Production-ready database with automated refreshes for business intelligence, dashboards, and team access | Turnkey Docker deployment | Full PostgreSQL warehouse with scheduled data pipeline, Airflow orchestration, and SQL client access. No coding required. | [→ Production Database](#gamebot-warehouse---production-database) |
| **Data Engineers & Developers** | Full pipeline customization, contribution to project, extensive EDA, or learning modern data engineering | Complete development environment | Full source repository with multiple deployment options, VS Code integration, and pipeline modification capabilities. | [→ Development Environment](#gamebot-studio---development-environment) |

---

## **Gamebot Lite** - Immediate Data Access

**Perfect for**: Data analysts and scientists who want to dive straight into analysis without any infrastructure setup.

**What you get**: Complete Survivor dataset as SQLite with pandas and DuckDB query helpers.

### Installation & Usage

```python
# Install in any Jupyter notebook or Python environment
%pip install --upgrade gamebot-lite

# Option 1: Pandas DataFrame access (recommended, works with base install)
from gamebot_lite import load_table

# Load any table from bronze, silver, or gold layers
df = load_table("castaway_details", layer="bronze")
winners_df = load_table("ml_features_hybrid", layer="gold")
print(f"Dataset contains {len(winners_df)} castaway-season observations")

# Option 2: DuckDB SQL queries (install DuckDB if not already available)
# %pip install --upgrade "gamebot-lite[duckdb]"  # if DuckDB not installed
from gamebot_lite import duckdb_query

# Analyze winners by challenge performance
duckdb_query("""
SELECT season_name, castaway, target_winner, challenges_won, vote_accuracy_rate
FROM gold.ml_features_non_edit
WHERE target_winner = 1
ORDER BY challenges_won DESC
LIMIT 5
""")
```

**What's included**: 31 tables across bronze/silver/gold layers with 200,000+ data points ready for immediate analysis.

**Documentation**: [Complete Gamebot Lite Guide](docs/gamebot_lite.md)

---

## **Gamebot Warehouse** - Production Deployment

**Perfect for**: Teams wanting a production-ready Survivor database with automated refreshes, accessed via any SQL client.

**What you get**: Complete Airflow + PostgreSQL stack with scheduled data refreshes, no code repository required.

### Quick Deployment

**Prerequisites**: Docker Engine/Desktop, basic `.env` configuration

```bash
# 1. Create project directory
mkdir survivor-warehouse && cd survivor-warehouse

# 2. Download docker-compose.yml and .env template
curl -O https://raw.githubusercontent.com/mgrody1/Gamebot/main/deploy/docker-compose.yml
curl -O https://raw.githubusercontent.com/mgrody1/Gamebot/main/deploy/.env.example

# 3. Configure environment
cp .env.example .env
# Edit .env with your database credentials

# 4. Launch production stack
docker compose up -d

# 5. Access Airflow UI and trigger pipeline
# http://localhost:8081 (admin/admin)
```

**Database Access**: Connect any SQL client to `localhost:5433` with credentials from your `.env` file.

**What runs**:
- **Bronze**: 21 raw tables (193k+ records)
- **Silver**: 8 feature engineering tables
- **Gold**: 2 ML-ready matrices (4,248 observations each)
- **Schedule**: Automatic weekly updates (configurable)

**Documentation**: [Architecture Overview](docs/architecture_overview.md) | [Operations Guide](docs/operations_guide.md)

---

## **Gamebot Studio** - Development Environment**Perfect for**: Developers customizing pipelines, contributors, researchers doing extensive EDA, or anyone wanting full control.

**What you get**: Complete source repository with multiple development workflows, VS Code integration, and notebook environment.

### Choose Your Development Style

| **Setup** | **Environment** | **Database** | **Orchestration** | **Best For** |
|-----------|----------------|--------------|-------------------|---------------|
| **Recommended** | VS Code Dev Container | Docker PostgreSQL | Full Airflow Stack | New contributors, consistent environment |
| **Quick Local** | Local Python + pipenv | Docker PostgreSQL | Full Airflow Stack | Experienced developers |
| **Manual Control** | Local Python + pipenv | External PostgreSQL | Manual execution | Full customization |
| **Cloud Dev** | VS Code Dev Container | External PostgreSQL | Manual execution | Cloud development |

### Recommended: VS Code Dev Container + Full Stack

**Perfect for**: New contributors, consistent development environment

```bash
# 1. Clone repository
git clone https://github.com/mgrody1/Gamebot.git
cd Gamebot

# 2. Configure environment
cp .env.example .env
# Edit .env with your settings

# 3. Open in VS Code with Dev Containers extension
# Command Palette → "Dev Containers: Reopen in Container"

# 4. Start complete stack (from host terminal)
make fresh

# 5. Access services
# - Airflow UI: http://localhost:8081
# - Database: localhost:5433
# - Jupyter: Select "gamebot" kernel in VS Code notebooks
```

### Quick Local Development

**Perfect for**: Experienced developers who prefer local tools

```bash
# 1. Clone and setup
git clone https://github.com/mgrody1/Gamebot.git
cd Gamebot
pip install pipenv
pipenv install

# 2. Configure environment
cp .env.example .env
# Edit .env with your settings

# 3. Start stack
make fresh

# 4. Optional: Manual pipeline execution
pipenv run python -m Database.load_survivor_data  # Bronze
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver  # Silver
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold    # Gold
```

### Full Manual Control

**Perfect for**: Custom database setups, specific deployment requirements

```bash
# 1. Clone repository
git clone https://github.com/mgrody1/Gamebot.git
cd Gamebot

# 2. Setup Python environment
pip install pipenv
pipenv install

# 3. Configure for external database
cp .env.example .env
# Edit .env with your PostgreSQL credentials (not warehouse-db)

# 4. Run pipeline manually
pipenv run python -m Database.load_survivor_data
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt build --project-dir dbt --profiles-dir dbt
```

### Notebook Development

**For EDA and analysis within the repository**:

```bash
# Ensure Jupyter kernel is available
pipenv install ipykernel
pipenv run python -m ipykernel install --user --name=gamebot

# Create analysis notebooks
pipenv run python scripts/create_notebook.py adhoc    # Quick analysis
pipenv run python scripts/create_notebook.py model    # ML modeling

# Use "gamebot" kernel in Jupyter/VS Code
```

**Studio Documentation**:
- [Development Environment Setup](docs/architecture_overview.md#development-environment)
- [CLI Commands & Workflows](docs/cli_cheatsheet.md)
- [Environment Configuration](docs/environment_guide.md)
- [Airflow + dbt Integration](docs/airflow_dbt_guide.md)

---

## **Architecture & Technical Details**

### Medallion Data Architecture

| Layer | Tables | Records | Purpose | Technology |
|-------|---------|---------|---------|------------|
| **Bronze** | 21 tables | 193,000+ | Raw survivoR data with metadata | Python + pandas |
| **Silver** | 8 tables + 9 tests | Strategic features | ML feature engineering | dbt + PostgreSQL |
| **Gold** | 2 tables + 4 tests | 4,248 observations each | Production ML matrices | dbt + PostgreSQL |

### Core Technologies

- **Orchestration**: Apache Airflow 2.9.1 with Celery executor
- **Transformation**: dbt 1.10.13 with custom macros
- **Storage**: PostgreSQL 15 with automated schema management
- **Containerization**: Docker Compose with context-aware networking
- **Data Quality**: Comprehensive validation and testing at each layer

### Pipeline Execution

**Automated Schedule**: Weekly Monday 4AM UTC (configurable via `GAMEBOT_DAG_SCHEDULE`)

**Manual Execution**:
- **Airflow UI**: http://localhost:8081 → `survivor_medallion_pipeline` → Trigger
- **CLI**: `docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline`

**Execution Time**: ~2 minutes end-to-end for complete medallion refresh

---

## **Documentation & Resources**

### Core Guides

| Resource | Audience | Description |
|----------|----------|-------------|
| [Gamebot Lite Guide](docs/gamebot_lite.md) | Analysts | Complete table dictionary and usage examples |
| [Architecture Overview](docs/architecture_overview.md) | All Users | Deployment patterns and system design |
| [CLI Cheatsheet](docs/cli_cheatsheet.md) | Studio Users | Essential commands and workflows |
| [Operations Guide](docs/operations_guide.md) | Infrastructure | Environment setup and troubleshooting |
| [Airflow + dbt Integration](docs/airflow_dbt_guide.md) | Developers | Container orchestration deep dive |

### Schema & Data References

| Resource | Description |
|----------|-------------|
| [Warehouse Schema Guide](docs/gamebot_warehouse_schema_guide.md) | ML feature categories and table relationships |
| [ERD Diagrams](docs/erd/) | Entity-relationship diagrams |
| [survivoR.pdf](survivoR.pdf) | Official upstream dataset documentation |

### Advanced Topics

| Resource | Description |
|----------|-------------|
| [Environment Configuration](docs/environment_guide.md) | Context-aware setup system |
| [GitHub Actions Guide](docs/github_actions_quickstart.md) | CI/CD and release workflows |
| [Contributing Guide](CONTRIBUTING.md) | Development workflow and PR process |

---

## **Use Cases & Examples**

### Data Analysis Examples
- **Winner Prediction Models**: Use gold layer ML features for predictive modeling
- **Strategic Analysis**: Leverage silver layer features for gameplay pattern analysis
- **Historical Trends**: Query bronze layer for comprehensive season-by-season analysis

### Integration Patterns
- **Business Intelligence**: Connect Tableau/PowerBI to PostgreSQL warehouse
- **Notebook Analysis**: Use Gamebot Lite for rapid prototyping and exploration
- **Custom Pipelines**: Extend Gamebot Studio for specialized research workflows

### Research Applications
- **Academic Research**: Comprehensive dataset for game theory and social dynamics studies
- **Data Science Education**: Production-ready pipeline for teaching modern data engineering
- **Competition Analysis**: ML feature engineering examples for prediction competitions

---

## **Configuration & Database Access**

**Single Configuration File**: Gamebot uses a unified `.env` file with context-aware overrides for different execution environments:

```bash
# .env (production-ready defaults)
DB_HOST=localhost              # Automatically overridden in containers
DB_NAME=survivor_dw_dev
DB_USER=survivor_dev
DB_PASSWORD=your_secure_password
PORT=5433                      # External port for local access
AIRFLOW_PORT=8081              # Airflow web interface
GAMEBOT_TARGET_LAYER=gold      # Pipeline depth control
```

**Database Connection**: Connect to the warehouse database for analysis:

| Setting | Value |
| --- | --- |
| Host | `localhost` |
| Port | `5433` |
| Database | `DB_NAME` from `.env` |
| Username | `DB_USER` from `.env` |
| Password | `DB_PASSWORD` from `.env` |

**Container Networking**: Docker Compose automatically handles database connectivity with container-to-container networking (`warehouse-db:5432`) while maintaining external access via `localhost:5433`.

---

## **Operations & Orchestration**

Gamebot runs with **automated Airflow orchestration** on a configurable schedule (`GAMEBOT_DAG_SCHEDULE`, default Monday 4AM UTC). The complete medallion pipeline includes data freshness detection, incremental loading, and comprehensive validation.

### Pipeline Management

```bash
# Start complete stack (Airflow + PostgreSQL + Redis)
make fresh

# Monitor pipeline execution
make logs

# Check service status
make ps

# Clean restart (removes all data)
make clean && make fresh
```

### Airflow DAG: `survivor_medallion_pipeline`


<p align="center">
  <img src="https://preview.redd.it/just-getting-into-apache-airflow-this-is-the-first-thing-v0-natxbqa7cj391.jpg?width=640&crop=smart&auto=webp&s=8de0aefa828b33e73710572479b2289abf86a1b1" alt="DAG Meme" width="480">
</p>


The DAG automatically orchestrates:
1. **Data Freshness Check**: Detects upstream survivoR dataset changes
2. **Bronze Loading**: Python-based ingestion with validation
3. **Silver Transformation**: dbt models for ML feature engineering
4. **Gold Aggregation**: Production ML-ready feature matrices
5. **Metadata Persistence**: Dataset versioning and lineage tracking

**Manual Triggering**:
- **UI**: Navigate to Airflow (`http://localhost:8081`) → Unpause and trigger DAG
- **CLI**: `docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline`

### Pipeline Results

Successful execution produces:
- **Bronze**: 21 tables with 193,000+ raw records
- **Silver**: 8 curated tables with strategic gameplay features
- **Gold**: 2 ML-ready matrices (4,248 castaway-season observations each)
- **Testing**: 13 dbt tests ensuring data quality

---

## **Release Management**Gamebot ships three artefacts that map to the layers described earlier:

| Artefact | Layer(s) covered | Delivery channel | Typical tag |
| --- | --- | --- | --- |
| Warehouse refresh | Bronze → Silver → Gold (Airflow/dbt + notebooks) | Git branch `main`, Docker stack, notebooks | `data-YYYYMMDD` |
| Gamebot Lite snapshot | Analyst SQLite + helper API | PyPI package, `gamebot_lite/data` | `data-YYYYMMDD` (same tag as warehouse refresh) |
| Application code | Python package, Docker images, notebooks | PyPI (`gamebot-lite`), Docker Hub, repo source | `code-vX.Y.Z` |

### Upstream Monitoring

A scheduled GitHub Action monitors the upstream [`survivoR`](https://github.com/doehm/survivoR) project for data updates and automatically opens issues when new data is available.

### Automated Releases

- **Data releases**: Triggered when upstream survivoR data changes
- **Code releases**: Tagged using helper script `python scripts/tag_release.py`
- **CI/CD**: GitHub Actions automate testing and release workflows

---

## **Troubleshooting**

### Common Issues

* **Port conflicts**: Set `AIRFLOW_PORT` in `.env`
* **Missing DAG changes**: Stop stack, rerun `make up` (DAGs are bind-mounted)
* **Fresh start needed**: `make clean` removes volumes and images

### Useful Commands

```bash
make logs   # Follow scheduler logs
make ps     # Service status
make show-last-run ARGS="--tail --category validation"  # Latest run artifact
```

### Data Quality Reports

Each pipeline run generates Excel validation reports with comprehensive data quality analysis:

```bash
# Find latest validation report
docker compose exec airflow-worker bash -c "
  find /opt/airflow -name 'data_quality_*.xlsx' -type f | head -5
"

# Copy latest report to host
LATEST_REPORT=$(docker compose exec airflow-worker bash -c "
  find /opt/airflow -name 'data_quality_*.xlsx' -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2
" | tr -d '\r')

docker compose cp airflow-worker:$LATEST_REPORT ./data_quality_report.xlsx
```

**Report contents**: Row counts, column types, PK/FK validations, duplicate analysis, schema drift detection, and detailed remediation notes.

---

## **Contributing**

Want to help? Read the [Contributing Guide](CONTRIBUTING.md) for:
- Trunk-based workflow and git commands
- Environment setup for contributors
- Release checklist and collaboration ideas
- PR requirements (include zipped run logs)

---

## **Repository Structure**

```
📁 Core Pipeline
├── airflow/
│   ├── dags/survivor_medallion_dag.py    # Complete orchestration pipeline
│   ├── docker-compose.yaml               # Production-ready stack definition
│   └── Dockerfile                        # Custom Airflow image
├── dbt/
│   ├── models/silver/                     # ML feature engineering (8 models)
│   ├── models/gold/                       # Production ML features (2 models)
│   ├── tests/                             # Data quality validation (13 tests)
│   ├── macros/generate_surrogate_key.sql  # Custom dbt macros
│   └── profiles.yml                       # Database connection config
├── Database/
│   ├── load_survivor_data.py              # Bronze layer ingestion
│   └── sql/                               # DDL and legacy scripts
└── gamebot_core/
    ├── db_utils.py                        # Schema validation and utilities
    ├── data_freshness.py                  # Change detection and metadata
    └── validation.py                      # Data quality validation

📁 Analysis & Distribution
├── gamebot_lite/                          # Analyst package (PyPI)
├── examples/
│   ├── example_analysis.py                # 2-minute demo
│   └── streamlit_app.py                   # Interactive data viewer
└── notebooks/                             # Analysis examples

📁 Operations & Documentation
├── docs/                                  # Comprehensive guides
├── scripts/                               # Automation and utilities
├── run_logs/                              # Validation artifacts
├── .env                                   # Single configuration file
└── Makefile                               # Simplified commands
```

### Documentation

Detailed guides available in the [docs/](docs/) folder:

- [Architecture Overview](docs/architecture_overview.md) — deployment and developer walkthroughs
- [Operations Guide](docs/operations_guide.md) — environment setup, scheduling, troubleshooting
- [CLI Cheatsheet](docs/cli_cheatsheet.md) — essential commands for managing the stack
- [Environment Guide](docs/environment_guide.md) — configuration and context-aware setup
- [Airflow + dbt Integration](docs/airflow_dbt_guide.md) — containerized execution and permissions
- [Warehouse Schema Guide](docs/gamebot_warehouse_schema_guide.md) — ML feature categories and relationships
- [Gamebot Lite](docs/gamebot_lite.md) — analyst package with table dictionary
- [Bronze validation workbook](docs/operations_guide.md#bronze-validation--metadata-summary) — how the loader’s Excel report surfaces remediations, validation checks, and upstream/warehouse schema drift.
- [ERD assets](docs/erd/) — generated entity-relationship diagrams and source Graphviz files.
- [Run logs & validation artifacts](run_logs/) — loader and validation outputs useful for PRs and incident reviews.

If you want to explore the data quickly, use the short [Try It in 5 Minutes](#try-it-in-5-minutes) cell above or see the analyst guide: [Gamebot Lite documentation](docs/gamebot_lite.md).

| Setting | Value (default) |
| --- | --- |
| Host | `localhost`
| Port | `5433` (or `WAREHOUSE_DB_PORT` in your `.env`)
| Database | `DB_NAME` from `.env` (e.g., `survivor_dw_dev`)
| Username | `DB_USER` from `.env` (e.g., `survivor_dev`)
| Password | `DB_PASSWORD` from `.env`

The Postgres service runs in Docker but binds to the host, so the connection works from the host OS and from within the Dev Container (use host networking). The VS Code Dev Container now targets the Compose-managed `devshell` service, so it automatically lands on the same Docker network as Airflow/Postgres—no manual `docker network connect` step required. Tools like DBeaver can auto-generate ERDs once connected, which is often clearer than the static PNG produced by `scripts/build_erd.py`. If you’re on Gamebot Studio, you can also query the same database directly from the repo’s notebooks using the bundled Pipenv environment. Pick whichever client fits your workflow.

---

## Operations & Orchestration

Gamebot runs with **automated Airflow orchestration** on a configurable schedule (`GAMEBOT_DAG_SCHEDULE`, default Monday 4AM UTC). The complete medallion pipeline includes data freshness detection, incremental loading, and comprehensive validation.

### Pipeline Management

```bash
# Start complete stack (Airflow + PostgreSQL + Redis)
make fresh

# Monitor pipeline execution
make logs

# Check service status
make ps

# Clean restart (removes all data)
make clean && make fresh
```

### Airflow DAG: `survivor_medallion_pipeline`

The DAG automatically orchestrates:
1. **Data Freshness Check**: Detects upstream survivoR dataset changes
2. **Bronze Loading**: Python-based ingestion with validation
3. **Silver Transformation**: dbt models for ML feature engineering
4. **Gold Aggregation**: Production ML-ready feature matrices
5. **Metadata Persistence**: Dataset versioning and lineage tracking

**Manual Triggering**:
- **UI**: Navigate to Airflow (`http://localhost:8081`) → Unpause and trigger DAG
- **CLI**: `docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline`

### Pipeline Results

Successful execution produces:
- **Bronze**: 21 tables with 193,000+ raw records
- **Silver**: 8 curated tables with strategic gameplay features
- **Gold**: 2 ML-ready matrices (4,248 castaway-season observations each)
- **Testing**: 13 dbt tests ensuring data quality---

## Gamebot Lite (analyst package)

```bash
pip install --upgrade gamebot-lite
```

```python
from gamebot_lite import load_table, duckdb_query
df = load_table("vote_history_curated")
```

See [docs/gamebot_lite.md](docs/gamebot_lite.md) for the complete table list (bronze, silver, gold), detailed column descriptions, sample DuckDB/pandas queries, packaging workflow, and notes on ML feature engineering.
See also [docs/gamebot_warehouse_schema_guide.md](docs/gamebot_warehouse_schema_guide.md) for a narrative walkthrough of silver ML feature categories and [docs/gamebot_warehouse_cheatsheet.md](docs/gamebot_warehouse_cheatsheet.md) for quick join keys and external SQL IDE tips.

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
```bash
pipenv run python -m Database.load_survivor_data
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt run --project-dir dbt --profiles-dir dbt --select silver
pipenv run dbt run --project-dir dbt --profiles-dir dbt --select gold
```
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
- See [docs/github_actions_quickstart.md](docs/github_actions_quickstart.md) for a walkthrough of these workflows.

---

## Repository Structure

```
📁 Core Pipeline
├── airflow/
│   ├── dags/survivor_medallion_dag.py    # Complete orchestration pipeline
│   ├── docker-compose.yaml               # Production-ready stack definition
│   └── Dockerfile                        # Custom Airflow image
├── dbt/
│   ├── models/silver/                     # ML feature engineering (8 models)
│   ├── models/gold/                       # Production ML features (2 models)
│   ├── tests/                             # Data quality validation (13 tests)
│   ├── macros/generate_surrogate_key.sql  # Custom dbt macros
│   └── profiles.yml                       # Database connection config
├── Database/
│   ├── load_survivor_data.py              # Bronze layer ingestion
│   └── sql/                               # DDL and legacy scripts
└── gamebot_core/
    ├── db_utils.py                        # Schema validation and utilities
    ├── data_freshness.py                  # Change detection and metadata
    └── validation.py                      # Data quality validation

📁 Analysis & Distribution
├── gamebot_lite/                          # Analyst package (PyPI)
├── examples/
│   ├── example_analysis.py                # 2-minute demo
│   └── streamlit_app.py                   # Interactive data viewer
└── notebooks/                             # Analysis examples

📁 Operations & Documentation
├── docs/                                  # Comprehensive guides
├── scripts/                               # Automation and utilities
├── run_logs/                              # Validation artifacts
├── .env                                   # Single configuration file
└── Makefile                               # Simplified commands
```

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
  make show-last-run ARGS="--tail --category validation"  # peek at latest run artefact
  ```

* **Data Quality Reports**: Each pipeline run generates Excel validation reports with comprehensive data quality analysis.

  **Access reports with standard Docker setup**:
  ```bash
  # Find latest validation report
  docker compose exec airflow-worker bash -c "
    find /opt/airflow -name 'data_quality_*.xlsx' -type f | head -5
  "

  # Copy latest report to host
  LATEST_REPORT=$(docker compose exec airflow-worker bash -c "
    find /opt/airflow -name 'data_quality_*.xlsx' -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2
  " | tr -d '\r')

  docker compose cp airflow-worker:$LATEST_REPORT ./data_quality_report.xlsx
  ```

  **For persistent reports** (optional):
  ```bash
  # Run with host-mounted logs directory
  docker compose run --rm \
    -e GAMEBOT_RUN_LOG_DIR=/workspace/run_logs \
    -v $(pwd)/run_logs:/workspace/run_logs \
    --profile loader survivor-loader

  # Reports saved to: ./run_logs/validation/data_quality_<timestamp>.xlsx
  ```

  **Report contents**: Row counts, column types, PK/FK validations, duplicate analysis, schema drift detection, and detailed remediation notes.

* Scheduler warnings about Flask-Limiter’s in-memory backend are safe for dev. Production configurations should keep the Redis-backed rate limiting enabled (handled automatically by `scripts/setup_env.py`).

---

## Contributing

Want to help? Read the [Contributing Guide](CONTRIBUTING.md) for the trunk-based workflow, recommended git commands, environment setup, and the release checklist. Remember to attach zipped run logs to your PR so reviewers can trace bronze/dbt executions. Looking for inspiration? The guide also lists open collaboration ideas.

## Need to dive deeper?

Here are the repository docs and quick links to the most useful reference pages:

| Resource | Description |
| --- | --- |
| [docs/](docs/) | Entry point for repository documentation (ERDs, notebooks, cheat sheets). |
| [CLI & Makefile Cheat Sheet](docs/cli_cheatsheet.md) | Quick reference for common commands, environments, and troubleshooting. |
| [Gamebot Lite documentation](docs/gamebot_lite.md) | Analyst table dictionary and usage examples for the packaged SQLite snapshot. |
| [Warehouse schema guide](docs/gamebot_warehouse_schema_guide.md) | Narrative walkthrough of silver facts/dimensions and how they relate. |
| [Warehouse cheatsheet & IDE tips](docs/gamebot_warehouse_cheatsheet.md) | Quick join key-reference plus instructions for connecting external SQL IDEs. |
| [Architecture Overview](docs/architecture_overview.md) | Deployment and developer walkthroughs (Warehouse vs Studio). |
| [Operations Guide](docs/operations_guide.md) | Environment profiles, ETL orchestration, scheduling, releases, and troubleshooting. |
| [GitHub Actions quickstart](docs/github_actions_quickstart.md) | CI and release workflow walkthroughs. |
| [ERD assets](docs/erd/) | Generated entity-relationship diagrams and source Graphviz files. |
| [Run logs & validation artifacts](run_logs/) | Loader and validation outputs useful for PRs and incident reviews. |
| `gamebot_lite/` | Source for the `gamebot-lite` PyPI package. |
| [Contributing Guide](CONTRIBUTING.md) | Contribution workflow, PR checklist, and release checklist. |
| `scripts/export_sqlite.py` | Produce fresh SQLite snapshots for analysts. |
