# Gamebot for Developers & Contributors

**Quick Start**: [Development setup](#development-environment) → [Local development](#development-workflows) → [Pipeline architecture](#pipeline-architecture) → [Contributing](#contributing)

This guide covers development environment setup, pipeline architecture, contribution workflows, and technical implementation details for developers working on Gamebot.

---

## Development Environment

### Prerequisites
- **Git**: Version control for repository access
- **Docker**: Engine/Desktop for containerized development
- **VS Code**: Recommended with Dev Containers extension
- **Python 3.12+**: For local development workflows (`uv sync` fetches it)

### Setup Options

Choose your development approach based on preferences and requirements:

| **Setup** | **Environment** | **Database** | **Orchestration** | **Best For** |
|-----------|----------------|--------------|-------------------|---------------|
| **Dev Container** | VS Code Dev Container | Docker PostgreSQL | Full Airflow Stack | New contributors, consistent environment |
| **Local Python** | Local uv | Docker PostgreSQL | Full Airflow Stack | Experienced developers |
| **External DB** | Local or Container | External PostgreSQL | Manual execution | Custom database setups |
| **Cloud Development** | VS Code Dev Container | Cloud PostgreSQL | Manual execution | Remote development |

### Recommended: VS Code Dev Container

**Benefits**: Pre-configured environment, consistent tooling, isolated dependencies

```bash
# 1. Clone repository
git clone https://github.com/mgrody1/Gamebot.git
cd Gamebot

# 2. Configure environment
cp .env.example .env
# Edit .env with your development settings

# 3. Open in VS Code with Dev Containers extension
code .
# Command Palette → "Dev Containers: Reopen in Container"

# 4. Start infrastructure (from host terminal)
make up

# 5. Development services available:
# - Airflow UI: http://localhost:8080 (admin/admin)
# - Database: localhost:5433
# - Jupyter: Pre-configured "gamebot" kernel in VS Code
```

**Dev Container Features**:
- Python environment with all dependencies
- Pre-configured Jupyter kernel
- Git integration with VS Code
- Integrated terminal access
- PostgreSQL client tools

### Local Python Development

**Benefits**: Full control, use preferred tools, faster iteration

```bash
# 1. Clone and setup Python environment
git clone https://github.com/mgrody1/Gamebot.git
cd Gamebot
uv sync

# 2. Configure environment
cp .env.example .env
# Edit .env for local development

# 3. Start infrastructure
make up

# 4. Optional: Manual pipeline execution
uv run python -m Database.load_survivor_data  # Bronze layer
uv run --env-file .env dbt build --project-dir dbt --profiles-dir dbt --select silver
uv run --env-file .env dbt build --project-dir dbt --profiles-dir dbt --select gold
```

### Package Management

`pyproject.toml` and `uv.lock` define the environment. `uv sync` installs the `pipeline` and `dev` groups.

| Group | Contents | Install |
|-------|----------|---------|
| `pipeline` | Loader, dbt, and export dependencies | default |
| `dev` | pytest, ruff, pre-commit, duckdb, twine | default |
| `analysis` | Notebook and modeling libraries | `uv sync --group analysis` |
| `airflow` | Airflow 2.9.1 for DAG editing | `uv sync --no-default-groups --group airflow` |

The `airflow` and `pipeline` groups resolve separately because Airflow 2.9 pins SQLAlchemy 1.4 and pandas 2.2+ needs SQLAlchemy 2.

#### Adding Dependencies

```bash
uv add --group dev <package>        # local tooling
uv add --group pipeline <package>   # needed by the pipeline, local and in Airflow
```

The Airflow image installs `airflow/requirements.txt`. The pre-commit hook `scripts/check_requirements_sync.py` rewrites that file from the `pipeline` group, so the two stay identical.

```bash
python scripts/check_requirements_sync.py           # rewrite airflow/requirements.txt
python scripts/check_requirements_sync.py --check   # verify only
```

### Environment Configuration

**Context-Aware Setup**: Gamebot automatically detects execution context and configures connections appropriately.

```bash
# .env configuration for development
DB_HOST=localhost              # External host connection
DB_NAME=survivor_dw_dev        # Development database
DB_USER=survivor_dev           # Development user
DB_PASSWORD=dev_password       # Development password
DB_PORT=5433                   # External port mapping
SURVIVOR_ENV=dev               # Development environment flag
GAMEBOT_TARGET_LAYER=gold      # Full pipeline execution
```

**Automatic Overrides**:
- Container environments automatically use internal networking
- Production deployments override security settings
- Development mode enables additional logging and validation

---

## Development Workflows

### Code Organization

```
**Core Pipeline Components**
├── airflow/
│   ├── dags/survivor_medallion_dag.py    # Main orchestration DAG
│   ├── docker-compose.yaml               # Development stack
│   └── Dockerfile                        # Custom Airflow image
├── dbt/
│   ├── models/sources.yml                 # Bronze tables as dbt sources
│   ├── models/silver/                     # Feature engineering
│   ├── models/gold/                       # ML-ready matrices
│   ├── models/*/schema.yml                # Data quality tests
│   └── macros/                            # Custom macros
├── Database/
│   ├── load_survivor_data.py              # Bronze ingestion
│   └── create_tables.sql                  # Schema DDL
└── gamebot_core/
    ├── db_utils.py                        # Database utilities
    ├── data_freshness.py                  # Change detection
    └── validation.py                      # Data quality validation

**Distribution & Analysis**
├── gamebot_lite/                          # Analyst package
├── examples/                              # Usage examples
└── notebooks/                             # Analysis notebooks

**Operations & Development**
├── docs/                                  # Documentation
├── scripts/                               # Automation utilities
├── tests/                                 # Unit tests
└── run_logs/                              # Execution artifacts
```

### Development Commands

**Essential make commands**:
```bash
make fresh    # Destructive: deletes containers, volumes, and the warehouse, then rebuilds
make up       # Start services (preserves data)
make down     # Stop services
make logs     # Follow scheduler logs
make ps       # Service status
make clean    # Complete reset (removes volumes)
```

**Pipeline development**:
```bash
# Test bronze layer
uv run python -m Database.load_survivor_data

# Test dbt transformations
uv run --env-file .env dbt deps --project-dir dbt --profiles-dir dbt
uv run --env-file .env dbt run --project-dir dbt --profiles-dir dbt --select silver
uv run --env-file .env dbt test --project-dir dbt --profiles-dir dbt

# Test specific models
uv run --env-file .env dbt run --project-dir dbt --profiles-dir dbt --select castaway_profile
```

**Notebook development**:
```bash
# Create analysis notebooks
uv run python scripts/create_notebook.py adhoc    # Quick analysis
uv run python scripts/create_notebook.py model    # ML modeling

# Ensure Jupyter kernel (local development)
uv run python -m ipykernel install --user --name=gamebot
```

### Testing & Validation

**Data Quality Testing**:
```bash
# Run all dbt tests
uv run --env-file .env dbt test --project-dir dbt --profiles-dir dbt

# Test specific models
uv run --env-file .env dbt test --project-dir dbt --profiles-dir dbt --select castaway_profile

# Generate test documentation
uv run --env-file .env dbt docs generate --project-dir dbt --profiles-dir dbt
uv run --env-file .env dbt docs serve --project-dir dbt --profiles-dir dbt
```

**Python Testing**:
```bash
# Snapshot and gamebot-lite tests
uv run pytest tests/

# Code quality
uv run ruff check .
uv run ruff format --check .
```

---

## Pipeline Architecture

### Medallion Architecture Overview

**Design Philosophy**: Progressive data refinement optimized for ML feature engineering and analytics using **industry-standard medallion architecture**.

```
Bronze Layer: Raw Data (21 tables, 183k+ records)
├── Schema: Direct mirrors of survivoR dataset
├── Purpose: Data lineage, audit trail, source-of-truth
├── Technology: Python + pandas ingestion
└── Updates: Full refresh on upstream changes

Silver Layer: Feature Engineering (8 tables + 11 tests)
├── Schema: ML-focused strategic gameplay categories
├── Purpose: Curated features for analysis and modeling
├── Technology: dbt transformations + PostgreSQL
└── Updates: Rebuilt as tables on every dbt run

Gold Layer: ML Matrices (2 tables + 6 tests)
├── Schema: Production ML-ready feature matrices
├── Purpose: Standardized modeling datasets
├── Technology: dbt aggregations + advanced features
└── Updates: Computed from silver layer changes
```

### Core Technologies

**Orchestration**: Apache Airflow 2.9.1
- Celery executor for distributed task processing
- Docker containerization for consistency
- DAG-based workflow definition
- Automatic dependency management

**Data Transformation**: dbt 1.9.1
- SQL-based transformation logic
- Built-in testing and documentation
- Custom macros for complex operations
- Incremental processing capabilities

**Storage**: PostgreSQL 15
- Production-grade OLTP database
- Advanced indexing for analytics workloads
- Full ACID compliance
- Automated schema management

**Containerization**: Docker Compose
- Multi-service orchestration
- Context-aware networking
- Volume management for persistence
- Environment-specific configuration

### Data Processing Flow

**1. Data Freshness Detection**:
```python
# gamebot_core/data_freshness.py (outline)
def detect_dataset_changes(dataset_names, base_raw_url, json_raw_url):
    """Return current metadata and the subset that changed since last cache."""
    # Compare current commit hashes with stored baseline
    # Detect changes in data/ or dev/json/ directories
    # Trigger pipeline only if new data available
```

**2. Bronze Layer Ingestion**:
```python
# Database/load_survivor_data.py (outline)
def main():
    """Entry point that loads survivoR datasets into the bronze schema."""
    # Download latest survivoR datasets
    # Validate schema consistency
    # Load with metadata and lineage tracking
    # Generate data quality reports
```

**3. Silver Layer Transformation**:
```sql
-- dbt/models/silver/castaway_profile.sql (abridged)
{{ config(materialized='table') }}

SELECT DISTINCT
    c.castaway_id,
    c.version_season,
    c.full_name,
    c.age,
    cd.gender,
    cd.occupation,
    ss.season_name
FROM {{ source('bronze', 'castaways') }} c
LEFT JOIN {{ source('bronze', 'castaway_details') }} cd
    ON c.castaway_id = cd.castaway_id
LEFT JOIN {{ source('bronze', 'season_summary') }} ss
    ON c.version_season = ss.version_season
```

**4. Gold Layer Aggregation**:
`dbt/models/gold/ml_features_non_edit.sql` and `ml_features_hybrid.sql` aggregate the silver tables into one row per castaway-season (`castaway_id`, `version_season`) with targets such as `target_winner`, challenge features such as `individual_win_rate`, and voting features such as `vote_accuracy_rate`; the hybrid table adds the edit features.

### Container Orchestration

**Service Architecture**:
```yaml
# airflow/docker-compose.yaml
services:
  warehouse-db:         # PostgreSQL warehouse
  postgres:             # Airflow metadata database
  redis:                # Message broker for Celery
  airflow-webserver:    # Web UI and API
  airflow-scheduler:    # Task scheduling and orchestration
  airflow-worker:       # Task execution
  airflow-triggerer:    # Deferred task triggers
  airflow-init:         # One-off DB migration + admin user
  survivor-loader:      # On-demand bronze loader (profile "loader")
# .devcontainer/docker-compose.devcontainer.yaml adds:
  devshell:             # Development container (VS Code integration)
```

**Networking Strategy** (Enterprise-Grade):
- Internal container-to-container communication via Docker networks
- External access via port mapping (5433 for database, 8080 for Airflow)
- Automatic service discovery and connection management
- Environment-specific overrides for different deployment contexts

**Permission Management**:
**Industry-standard Docker practice** for handling file permissions between host and containers:

```bash
# dbt execution in Airflow containers
mkdir -p /tmp/dbt_logs /tmp/dbt_target
dbt build \
  --project-dir dbt \
  --profiles-dir dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

**Key Benefits** (Docker Best Practice):
- No permission conflicts with host-mounted volumes
- Container-local directories are always writable
- Temporary files automatically cleaned up
- Preserves dbt functionality without volume mount issues

This approach follows **Docker best practices** for containerized applications that need to write files during execution, commonly used in production data engineering environments.

---

## Custom Development

### Adding New Data Sources

**1. Extend Bronze Layer**:
```python
# Database/load_new_source.py
def load_new_data_source():
    """Template for additional data source integration"""
    # Download/API calls for new data
    # Schema validation and mapping
    # Integration with existing pipeline
    # Metadata and lineage tracking
```

**2. Create Silver Transformations**:
```sql
-- dbt/models/silver/new_source_curated.sql
{{ config(materialized='table') }}

WITH source_data AS (
  SELECT * FROM {{ source('bronze', 'new_source') }}  -- declare it in dbt/models/sources.yml
),
feature_engineering AS (
  -- Custom transformation logic
  -- Join with existing tables
  -- ML feature creation
)
SELECT * FROM feature_engineering
```

**3. Extend Gold Layer**:
```sql
-- dbt/models/gold/ml_features_enhanced.sql
SELECT
  *,
  -- Add new features to existing ML matrix
  new_feature_category
FROM {{ ref('ml_features_non_edit') }}
JOIN {{ ref('new_source_curated') }} USING (castaway_id, version_season)
```

### Custom Feature Engineering

**dbt Macro Development**:
```sql
-- dbt/macros/custom_features.sql
{% macro calculate_strategic_score(votes_cast, votes_received, betrayals) %}
  CASE
    WHEN {{ votes_cast }} = 0 THEN 0
    ELSE ({{ votes_cast }} - {{ votes_received }} + {{ betrayals }}) / {{ votes_cast }}::float
  END
{% endmacro %}
```

**Advanced Transformations**:
```sql
-- Example: Complex window function features
WITH episode_progression AS (
  SELECT
    castaway_id,
    episode,
    LAG(tribe_size, 1) OVER (
      PARTITION BY castaway_id, version_season
      ORDER BY episode, day
    ) as previous_tribe_size,
    tribe_size - LAG(tribe_size, 1) OVER (
      PARTITION BY castaway_id, version_season
      ORDER BY episode, day
    ) as tribe_size_change
  FROM {{ ref('social_positioning') }}
)
```

### Extending Gamebot Lite

**Adding New Tables**: `load_table` only accepts tables listed in `gamebot_lite/catalog.py`. Add a new bronze table to `BRONZE_TABLES`, or a new dbt model to `SILVER_FRIENDLY_NAME_OVERRIDES` / `GOLD_FRIENDLY_NAME_OVERRIDES`, then re-export the snapshot.

**Custom Analysis Functions** (in your own code, built on the public API):
```python
import pandas as pd
from gamebot_lite import load_table


def winner_prediction_features(season: str) -> pd.DataFrame:
    """Gold-layer features for one season."""
    df = load_table("ml_features_non_edit", layer="gold")
    return df[df["version_season"] == season]
```

---

## Contributing

### Git Workflow

**Trunk-Based Development**: Main branch always deployable, feature branches for development.

```bash
# 1. Fork and clone
git clone https://github.com/yourusername/Gamebot.git
cd Gamebot

# 2. Create feature branch
git checkout -b feature/new-analysis-feature

# 3. Development with regular commits
git add .
git commit -m "Add strategic voting analysis features"

# 4. Keep up to date
git fetch origin
git rebase origin/main

# 5. Submit PR
git push origin feature/new-analysis-feature
# Open PR via GitHub interface
```

### Pull Request Requirements

**Code Quality**:
- All code formatted with `ruff format`
- No `ruff check` violations
- Type hints where appropriate
- Comprehensive docstrings

**Testing**:
- dbt tests pass for all modified models
- Data validation reports included for pipeline changes
- Manual testing documented in PR description

**Documentation**:
- Update relevant documentation files
- Include usage examples for new features
- Update schema documentation for data changes

**Artifacts**:
- Include zipped run logs for pipeline modifications
- Data quality reports for validation
- Performance benchmarks for optimization changes

### Development Guidelines

**Code Style**:
```bash
# Python formatting standards (also run by pre-commit)
uv run ruff format .
uv run ruff check .
```

**SQL Style**:
```sql
-- dbt SQL formatting standards
SELECT
  column_one,
  column_two,
  aggregate_function(column_three) AS calculated_field
FROM {{ ref('source_table') }}
WHERE condition = 'value'
GROUP BY column_one, column_two
ORDER BY calculated_field DESC
```

**Documentation Standards**:
```python
def complex_function(param1: str, param2: int) -> pd.DataFrame:
    """
    Brief description of function purpose.

    Args:
        param1: Description of first parameter
        param2: Description of second parameter

    Returns:
        Description of return value

    Raises:
        ValueError: When specific error conditions occur

    Example:
        >>> result = complex_function("test", 42)
        >>> print(len(result))
        100
    """
```

### Release Process

**Data Releases** (when upstream survivoR changes):
1. Verify upstream data changes via monitoring
2. Run complete pipeline refresh and validation
3. Update SQLite export for gamebot-lite
4. Tag release: `python scripts/tag_release.py data --date YYYYMMDD`
5. Update upstream snapshot: `python scripts/check_survivor_updates.py --update`

**Code Releases** (for feature updates):
1. Bump version in `pyproject.toml`
2. Update documentation and examples
3. Run complete test suite
4. Tag release: `python scripts/tag_release.py code --version vX.Y.Z`
5. Publish to PyPI and Docker Hub as needed

---

## Advanced Topics

### Performance Optimization

**Database Tuning**:
```sql
-- Add indexes for common query patterns
-- (dbt recreates these tables on every run, which drops manual indexes;
--  declare lasting ones with the model's `indexes` config)
CREATE INDEX idx_castaway_season ON silver.castaway_profile (castaway_id, version_season);
CREATE INDEX idx_episode_progression ON silver.social_positioning (version_season, episode);
```

**dbt Optimization**:
```sql
-- All models are materialized as tables today; a large model could switch to incremental
{{ config(
    materialized='incremental',
    unique_key='challenge_performance_key',
    on_schema_change='fail'
) }}
```

**Airflow Optimization**:
```python
# DAG configuration for performance (airflow/dags/survivor_medallion_dag.py)
from datetime import timedelta

default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
# max_active_runs=1 (no concurrent executions) is set on the DAG itself, not in default_args
```

### Monitoring & Observability

**Data Quality Monitoring**:
```sql
-- dbt singular test example; the project has none yet, so this would be a new
-- dbt/tests/assert_winner_counts.sql
SELECT version_season, COUNT(*) as winner_count
FROM {{ ref('ml_features_non_edit') }}
WHERE target_winner = 1
GROUP BY version_season
HAVING COUNT(*) != 1  -- Each season should have exactly one winner
```

**Pipeline Monitoring**:
```python
# Custom Airflow operators for monitoring (sketch; the repo does not ship one)
class DataQualityOperator(BaseOperator):
    def execute(self, context):
        # Custom validation logic
        # Integration with monitoring systems
        # Alert generation for anomalies
        ...
```

### Security Considerations

**Development Security**:
- Use `.env` files for local secrets (never commit)
- Rotate development credentials regularly
- Limit database permissions for development accounts
- Use read-only connections where possible

**Production Security**:
- Environment-specific credential management
- Docker secrets for sensitive configuration
- Network segmentation for database access
- Regular security updates for dependencies

---

## Getting Help

### Documentation Resources
- **Architecture Deep Dive**: [architecture_overview.md](architecture_overview.md)
- **Schema Reference**: [gamebot_warehouse_schema_guide.md](gamebot_warehouse_schema_guide.md)
- **CLI Reference**: [cli_cheatsheet.md](cli_cheatsheet.md)
- **Environment Setup**: [environment_guide.md](environment_guide.md)

### Community & Support
- **Issues & Bugs**: [GitHub Issues](https://github.com/mgrody1/Gamebot/issues)
- **Feature Requests**: GitHub Discussions
- **Development Questions**: Community Slack/Discord (if available)

### Professional Development

**Industry Best Practices Demonstrated**:
- **Medallion Architecture**: Industry-standard lakehouse pattern (bronze → silver → gold) used by major organizations
- **Infrastructure as Code**: Docker Compose for reproducible, version-controlled environments
- **Container Orchestration**: Production-ready multi-service deployment with proper networking and security
- **Modern Data Stack**: Apache Airflow + dbt + PostgreSQL - standard enterprise data engineering toolkit
- **CI/CD Integration**: Automated testing, validation, and release workflows
- **Data Quality Engineering**: Comprehensive testing and validation at every layer
- **Environment Management**: Context-aware configuration supporting dev/staging/prod deployments

Contributing to Gamebot provides hands-on experience with:
- Modern data engineering practices (medallion architecture, dbt, Airflow)
- Container orchestration and DevOps methodologies
- ML feature engineering and data science workflows
- Open source development and collaboration
- Production data pipeline management

This experience directly translates to data engineering, analytics engineering, and ML engineering roles in the industry.
