# Gamebot

<p align="center">
  <img src="https://i.redd.it/icb7a6pmyf0c1.jpg" alt="Dabu Doodles Survivor art" width="480">
</p>

> Art by [Dabu Doodles (Erik Reichenbach)](https://dabudoodles.com/)

## Getting Started

[*Survivor Term Glossary (search for Gamebot)*](https://insidesurvivor.com/the-ultimate-survivor-glossary-980)

[*What is a Gamebot in Survivor? Thread*](https://www.reddit.com/r/survivor/comments/37hu6i/what_is_a_gamebot/)

## What is a Gamebot Outside of the Game? **This Repository!**:

Gamebot is a production-ready Survivor analytics stack that implements a complete **medallion lakehouse architecture** using Apache Airflow + dbt + PostgreSQL. It primarily ingests the comprehensive [`survivoR`](https://github.com/doehm/survivoR) dataset, with plans to integrate additional reality TV data sources, transforming everything through bronze → silver → gold layers and delivering ML-ready features for winner prediction research.

The architecture follows a **medallion lakehouse pattern** optimized for ML feature engineering:
- **Bronze Layer** (21 tables): Raw survivoR dataset tables with comprehensive ingestion metadata and data lineage
- **Silver Layer** (8 tables + 9 tests): ML-focused feature engineering organized by strategic gameplay categories (challenges, advantages, voting dynamics, social positioning, edit analysis) - **these curated features don't exist in the original survivoR dataset**
- **Gold Layer** (2 tables + 4 tests): Two production ML-ready feature matrices for different modeling approaches (gameplay-only vs hybrid gameplay+edit features) - **completely new analytical constructs built on top of survivoR**

**What makes this special**: The entire pipeline runs seamlessly in containerized Apache Airflow with automated dependency management, comprehensive data validation, and zero-configuration setup. Perfect for data scientists who want to focus on analysis rather than infrastructure.

For a detailed reference of the upstream schema we mirror, see the [official survivoR documentation](https://cran.r-project.org/web/packages/survivoR/survivoR.pdf).

Huge thanks to [Daniel Oehm](https://gradientdescending.com/) and the `survivoR` community; if you haven't already, please check [`survivoR`](https://github.com/doehm/survivoR) out!

### What you can explore
- [Check out these Survivor analyses with the survivoR dataset](https://gradientdescending.com/category/survivor/) as examples of the types of analyses you can now more easily accomplish in python and SQL with Gamebot.

---

## **Choose Your Adventure**

**Looking for the fastest path to Survivor data analysis?** Pick your persona:

| **Persona** | **Goal** | **Technical Setup** | **Time to Data** | **What You Get** |
|------------------|------------------|-------------------|------------------|-----------------|
| **Data Analysts & Scientists** | Quick analysis, exploration, prototyping, academic research | Laptop + Python/pandas | 2 minutes | Pre-built SQLite snapshot with 30+ curated tables, perfect for Jupyter notebooks and rapid prototyping |
| **Data Teams & Organizations** | Production database with automated refreshes, team collaboration, BI tool integration | Docker + basic .env configuration | 5 minutes | Full PostgreSQL warehouse with Airflow orchestration, connects to Tableau/PowerBI/DBeaver |
| **Data Engineers & Developers** | Pipeline customization, contributions, research, extending to new data sources | Git + VS Code + Docker development environment | 10 minutes | Complete source code with development container, multiple deployment patterns, full customization |

### Try It in 2 Minutes - Gamebot Lite (Analysts)

**Perfect for**: Exploratory analysis, prototyping, Jupyter notebooks, academic research

**Installation**: Choose your preferred analytics approach:
```bash
# Recommended: pandas for data analysis
pip install gamebot-lite

# Alternative: with DuckDB for SQL-style analytics
pip install gamebot-lite[duckdb]
```

```python
from gamebot_lite import load_table, duckdb_query

# Load any table for pandas analysis
vote_history = load_table("vote_history_curated")
jury_votes = load_table("jury_votes")

# Or query with DuckDB for complex SQL analytics (requires duckdb extra)
results = duckdb_query("""
    SELECT season, COUNT(*) as total_votes
    FROM vote_history_curated
    WHERE vote_order = 'Tie'
    GROUP BY season ORDER BY total_votes DESC
""")
```

**Available data**: Bronze (21 raw tables), Silver (8 ML feature tables), Gold (2 production matrices) - [complete table guide](docs/analyst_guide.md)

---

## Gamebot Warehouse - Production Deployment

**Perfect for**: Teams wanting a production-ready Survivor database with automated refreshes, accessed via any SQL client. **Configurable for both development and production environments.**

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

**Documentation**: [Deployment Guide](docs/deployment_guide.md) | [Architecture Overview](docs/architecture_overview.md)

---

## Gamebot Studio - Development Environment

**Perfect for**: Developers customizing pipelines, contributors, researchers doing extensive EDA, or anyone wanting full control. **Supports multiple development and production deployment patterns.**

**What you get**: Complete source repository with multiple development workflows, VS Code integration, and notebook environment.

### Choose Your Development Style

| **Setup** | **Environment** | **Database** | **Orchestration** | **Best For** |
|-----------|----------------|--------------|-------------------|---------------|
| **Recommended** | VS Code Dev Container | Docker PostgreSQL | Full Airflow Stack | New contributors, consistent environment |
| **Quick Local** | Local Python + pipenv | Docker PostgreSQL | Full Airflow Stack | Experienced developers |
| **Manual Control** | Local Python + pipenv | External PostgreSQL | Manual execution | Full customization, existing database infrastructure |
| **External Database** | VS Code Dev Container | External PostgreSQL | Manual execution | Remote database setups, cloud databases |

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

**If using VS Code Dev Container**: Jupyter kernel is already configured - just select "gamebot" kernel in VS Code notebooks.

**If using local Python environment**:
```bash
# Setup Jupyter kernel for local development
pipenv install ipykernel
pipenv run python -m ipykernel install --user --name=gamebot

# Create analysis notebooks
pipenv run python scripts/create_notebook.py adhoc    # Quick analysis
pipenv run python scripts/create_notebook.py model    # ML modeling

# Use "gamebot" kernel in Jupyter/VS Code
```

**Studio Documentation**:
- [Developer Guide](docs/developer_guide.md)
- [CLI Commands & Workflows](docs/cli_cheatsheet.md)
- [Environment Configuration](docs/environment_guide.md)
- [Architecture Overview](docs/architecture_overview.md)

---

## Architecture & Technical Details

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

## Documentation & Resources

### Core Guides

| Resource | Audience | Description |
|----------|----------|-------------|
| [Analyst Guide](docs/analyst_guide.md) | Data Analysts & Scientists | Complete gamebot-lite usage, table dictionary, and analysis examples |
| [Deployment Guide](docs/deployment_guide.md) | Data Teams & Organizations | Production deployment, team setup, and operations |
| [Developer Guide](docs/developer_guide.md) | Data Engineers & Developers | Development environment, pipeline architecture, and contribution workflows |
| [Architecture Overview](docs/architecture_overview.md) | All Users | System design and deployment patterns |
| [CLI Cheatsheet](docs/cli_cheatsheet.md) | Studio Users | Essential commands and workflows |

### Schema & Data References

| Resource | Description |
|----------|-------------|
| [Warehouse Schema Guide](docs/gamebot_warehouse_schema_guide.md) | ML feature categories and table relationships |
| [ERD Diagrams](docs/erd/) | Entity-relationship diagrams |
| [survivoR Documentation](https://cran.r-project.org/web/packages/survivoR/survivoR.pdf) | Official upstream dataset documentation |

### Advanced Topics

| Resource | Description |
|----------|-------------|
| [Environment Configuration](docs/environment_guide.md) | Context-aware setup system |
| [GitHub Actions Guide](docs/github_actions_quickstart.md) | CI/CD and release workflows |
| [Contributing Guide](CONTRIBUTING.md) | Development workflow and PR process |

---

## Use Cases & Examples

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

## Configuration & Database Access

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
|---------|-------|
| Host | `localhost` |
| Port | `5433` |
| Database | `DB_NAME` from `.env` |
| Username | `DB_USER` from `.env` |
| Password | `DB_PASSWORD` from `.env` |

**Container Networking**: Docker Compose automatically handles database connectivity with container-to-container networking (`warehouse-db:5432`) while maintaining external access via `localhost:5433`.

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


<p align="center">
  <img src="https://preview.redd.it/just-getting-into-apache-airflow-this-is-the-first-thing-v0-natxbqa7cj391.jpg?width=640&crop=smart&auto=webp&s=8de0aefa828b33e73710572479b2289abf86a1b1" alt="DAG Meme" width="640">
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

## Release Management

Gamebot ships three artefacts that map to the layers described earlier:

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

## Troubleshooting

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

## Contributing

Want to help? Read the [Contributing Guide](CONTRIBUTING.md) for:
- Trunk-based workflow and git commands
- Environment setup for contributors
- Release checklist and collaboration ideas
- PR requirements (include zipped run logs)

---

## Repository Structure

```
📁 Root Configuration
├── .env                                   # Single configuration file
├── .env.example                           # Configuration template
├── Makefile                               # Simplified commands
├── pyproject.toml                         # Python package configuration
├── Pipfile                                # Python dependencies
├── params.py                              # Global pipeline parameters
├── Dockerfile                             # Main application container
└── README.md                              # This documentation

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

📁 Deployment & Operations
├── deploy/                                # Standalone warehouse deployment
│   ├── docker-compose.yml                # Production deployment stack
│   └── .env.example                       # Environment configuration template
├── docs/                                  # Comprehensive guides
├── scripts/                               # Automation and utilities
├── tests/                                 # Unit and integration tests
└── run_logs/                              # Validation artifacts
```
