# Gamebot for Developers & Contributors

**Quick Start**: [Development setup](#development-environment) → [Local development](#development-workflows) → [Pipeline architecture](#pipeline-architecture) → [Contributing](#contributing)

This guide covers development environment setup, pipeline architecture, contribution workflows, and technical implementation details for developers working on Gamebot.

---

## Development Environment

### Prerequisites
- **Git**: Version control for repository access
- **Docker**: Engine/Desktop for containerized development
- **VS Code**: Recommended with Dev Containers extension
- **Python 3.8+**: For local development workflows

### Setup Options

Choose your development approach based on preferences and requirements:

| **Setup** | **Environment** | **Database** | **Orchestration** | **Best For** |
|-----------|----------------|--------------|-------------------|---------------|
| **Dev Container** | VS Code Dev Container | Docker PostgreSQL | Full Airflow Stack | New contributors, consistent environment |
| **Local Python** | Local pipenv | Docker PostgreSQL | Full Airflow Stack | Experienced developers |
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
make fresh

# 5. Development services available:
# - Airflow UI: http://localhost:8081 (admin/admin)
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
pip install pipenv
pipenv install --dev

# 2. Configure environment
cp .env.example .env
# Edit .env for local development

# 3. Start infrastructure
make fresh

# 4. Optional: Manual pipeline execution
pipenv run python -m Database.load_survivor_data  # Bronze layer
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
```

### Package Management

Gamebot uses multiple requirements files for different deployment contexts:

- **`Pipfile`**: Local/dev container development dependencies (managed by pipenv)
- **`airflow/requirements.txt`**: Container/Airflow-specific dependencies

#### Adding Dependencies

**Simplified Workflow with Auto-Sync**:

1. **For local development only**: Add to `Pipfile` using `pipenv install <package>`
2. **For both local and container deployment**: Add package with `# sync-to-requirements` comment
3. **Automatic synchronization**: Pre-commit hook auto-syncs annotated packages to `airflow/requirements.txt`

**Marking Packages for Container Deployment**:

When adding a package to `Pipfile` that should also be available in Airflow containers, add the `# sync-to-requirements` comment:

```toml
[packages]
pandas = ">=1.5.0,<3.0"  # sync-to-requirements
dbt-core = "<2.0,>=1.9"  # sync-to-requirements
ipykernel = "*"  # Local development only (no comment)
```

**The pre-commit hook** (`scripts/check_requirements_sync.py`) automatically:
- Verifies version compatibility between common packages
- Auto-syncs packages marked with `# sync-to-requirements` to `airflow/requirements.txt`
- Prevents deployment issues from version mismatches

#### Dependency Sync Commands

```bash
# Check compatibility and auto-sync marked packages (default)
python scripts/check_requirements_sync.py

# Check only without modifying files
python scripts/check_requirements_sync.py --check

# Add package for both local and container use
pipenv install numpy==1.24.0
# Then add '# sync-to-requirements' comment in Pipfile
git add Pipfile
git commit -m "Add numpy dependency"  # Pre-commit hook will auto-sync

# Manual sync if needed
python scripts/check_requirements_sync.py --sync
```

### Environment Configuration

**Context-Aware Setup**: Gamebot automatically detects execution context and configures connections appropriately.

```bash
# .env configuration for development
DB_HOST=localhost              # External host connection
DB_NAME=survivor_dw_dev        # Development database
DB_USER=survivor_dev           # Development user
DB_PASSWORD=dev_password       # Development password
PORT=5433                      # External port mapping
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
📁 Core Pipeline Components
├── airflow/
│   ├── dags/survivor_medallion_dag.py    # Main orchestration DAG
│   ├── docker-compose.yaml               # Development stack
│   └── Dockerfile                        # Custom Airflow image
├── dbt/
│   ├── models/bronze/                     # Raw data models
│   ├── models/silver/                     # Feature engineering
│   ├── models/gold/                       # ML-ready matrices
│   ├── tests/                             # Data quality tests
│   └── macros/                            # Custom macros
├── Database/
│   ├── load_survivor_data.py              # Bronze ingestion
│   └── sql/                               # Schema DDL
└── gamebot_core/
    ├── db_utils.py                        # Database utilities
    ├── data_freshness.py                  # Change detection
    └── validation.py                      # Data quality validation

📁 Distribution & Analysis
├── gamebot_lite/                          # Analyst package
├── examples/                              # Usage examples
└── notebooks/                             # Analysis notebooks

📁 Operations & Development
├── docs/                                  # Documentation
├── scripts/                               # Automation utilities
├── tests/                                 # Unit tests
└── run_logs/                              # Execution artifacts
```

### Development Commands

**Essential make commands**:
```bash
make fresh    # Clean start: rebuild containers and refresh data
make up       # Start services (preserves data)
make down     # Stop services
make logs     # Follow scheduler logs
make ps       # Service status
make clean    # Complete reset (removes volumes)
```

**Pipeline development**:
```bash
# Test bronze layer
pipenv run python -m Database.load_survivor_data

# Test dbt transformations
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt run --project-dir dbt --profiles-dir dbt --select silver
pipenv run dbt test --project-dir dbt --profiles-dir dbt

# Test specific models
pipenv run dbt run --project-dir dbt --profiles-dir dbt --select castaway_profile_curated
```

**Notebook development**:
```bash
# Create analysis notebooks
pipenv run python scripts/create_notebook.py adhoc    # Quick analysis
pipenv run python scripts/create_notebook.py model    # ML modeling

# Ensure Jupyter kernel (local development)
pipenv run python -m ipykernel install --user --name=gamebot
```

### Testing & Validation

**Data Quality Testing**:
```bash
# Run all dbt tests
pipenv run dbt test --project-dir dbt --profiles-dir dbt

# Test specific models
pipenv run dbt test --project-dir dbt --profiles-dir dbt --select castaway_profile_curated

# Generate test documentation
pipenv run dbt docs generate --project-dir dbt --profiles-dir dbt
pipenv run dbt docs serve --project-dir dbt --profiles-dir dbt
```

**Python Testing**:
```bash
# Unit tests (when available)
pipenv run pytest tests/

# Code quality
pipenv run black --check .
pipenv run isort --check-only .
pipenv run flake8
```

---

## Pipeline Architecture

### Medallion Architecture Overview

**Design Philosophy**: Progressive data refinement optimized for ML feature engineering and analytics using **industry-standard medallion architecture**.

```
🥉 Bronze Layer: Raw Data (21 tables, 193k+ records)
├── Schema: Direct mirrors of survivoR dataset
├── Purpose: Data lineage, audit trail, source-of-truth
├── Technology: Python + pandas ingestion
└── Updates: Full refresh on upstream changes

🥈 Silver Layer: Feature Engineering (8 tables + 9 tests)
├── Schema: ML-focused strategic gameplay categories
├── Purpose: Curated features for analysis and modeling
├── Technology: dbt transformations + PostgreSQL
└── Updates: Incremental processing on bronze changes

🥇 Gold Layer: ML Matrices (2 tables + 4 tests)
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

**Data Transformation**: dbt 1.10.13
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
# gamebot_core/data_freshness.py
def check_upstream_changes():
    """Monitor survivoR GitHub repository for dataset updates"""
    # Compare current commit hashes with stored baseline
    # Detect changes in data/ or dev/json/ directories
    # Trigger pipeline only if new data available
```

**2. Bronze Layer Ingestion**:
```python
# Database/load_survivor_data.py
def load_bronze_tables():
    """Ingest raw survivoR data with comprehensive validation"""
    # Download latest survivoR datasets
    # Validate schema consistency
    # Load with metadata and lineage tracking
    # Generate data quality reports
```

**3. Silver Layer Transformation**:
```sql
-- dbt/models/silver/castaway_profile_curated.sql
WITH demographics AS (
  SELECT DISTINCT
    {{ generate_surrogate_key(['castaway_id']) }} as castaway_key,
    castaway_id,
    full_name,
    age,
    city,
    state
  FROM {{ ref('castaways') }}
),
strategic_features AS (
  -- Complex feature engineering logic
  -- Aggregations across multiple bronze tables
  -- ML-focused transformations
)
SELECT * FROM demographics
JOIN strategic_features USING (castaway_id)
```

**4. Gold Layer Aggregation**:
```sql
-- dbt/models/gold/ml_features_gameplay.sql
SELECT
  castaway_key,
  version_season,

  -- Challenge performance features
  challenge_win_rate,
  individual_immunity_wins,

  -- Strategic voting features
  votes_cast_total,
  strategic_vote_percentage,

  -- Alliance features
  alliance_size_avg,
  cross_tribal_connections,

  -- Target variable
  winner
FROM {{ ref('challenge_performance_curated') }} cp
JOIN {{ ref('voting_dynamics_curated') }} vd USING (castaway_key)
JOIN {{ ref('social_positioning_curated') }} sp USING (castaway_key)
```

### Container Orchestration

**Service Architecture**:
```yaml
# airflow/docker-compose.yaml
services:
  airflow-scheduler:    # Task scheduling and orchestration
  airflow-webserver:    # Web UI and API
  airflow-worker:       # Task execution
  redis:                # Message broker for Celery
  warehouse-db:         # PostgreSQL database
  devshell:            # Development container (VS Code integration)
```

**Networking Strategy** (Enterprise-Grade):
- Internal container-to-container communication via Docker networks
- External access via port mapping (5433 for database, 8081 for Airflow)
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
  SELECT * FROM {{ ref('new_source_bronze') }}
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
FROM {{ ref('ml_features_gameplay') }}
JOIN {{ ref('new_source_curated') }} USING (castaway_key)
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
    LAG(alliance_size, 1) OVER (
      PARTITION BY castaway_id, version_season
      ORDER BY episode
    ) as previous_alliance_size,
    alliance_size - LAG(alliance_size, 1) OVER (
      PARTITION BY castaway_id, version_season
      ORDER BY episode
    ) as alliance_size_change
  FROM {{ ref('social_positioning_curated') }}
)
```

### Extending Gamebot Lite

**Adding New Tables**:
```python
# gamebot_lite/client.py
class GamebotClient:
    def load_table(self, table_name: str) -> pd.DataFrame:
        """Add support for new tables"""
        if table_name in self.available_tables():
            return pd.read_sql(f"SELECT * FROM {table_name}", self.conn)
        else:
            raise ValueError(f"Table '{table_name}' not available")
```

**Custom Analysis Functions**:
```python
# gamebot_lite/analysis.py
def winner_prediction_features(season: str) -> pd.DataFrame:
    """Pre-built analysis functions for common use cases"""
    query = """
    SELECT * FROM ml_features_gameplay
    WHERE version_season = %s
    AND episode = (SELECT MAX(episode) FROM episodes WHERE version_season = %s)
    """
    return pd.read_sql(query, params=[season, season])
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
- All code formatted with `black` and `isort`
- No `flake8` violations
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
```python
# Python formatting standards
black --line-length 88 .
isort --profile black .
flake8 --max-line-length 88
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
CREATE INDEX idx_castaway_season ON silver.castaway_profile_curated (castaway_id, version_season);
CREATE INDEX idx_episode_progression ON silver.social_positioning_curated (version_season, episode);
```

**dbt Optimization**:
```sql
-- Use incremental models for large tables
{{ config(
    materialized='incremental',
    unique_key='castaway_key',
    on_schema_change='fail'
) }}
```

**Airflow Optimization**:
```python
# DAG configuration for performance
default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,  # Prevent concurrent executions
}
```

### Monitoring & Observability

**Data Quality Monitoring**:
```sql
-- dbt test examples
-- tests/assert_winner_counts.sql
SELECT version_season, COUNT(*) as winner_count
FROM {{ ref('castaway_profile_curated') }}
WHERE winner = true
GROUP BY version_season
HAVING COUNT(*) != 1  -- Each season should have exactly one winner
```

**Pipeline Monitoring**:
```python
# Custom Airflow operators for monitoring
class DataQualityOperator(BaseOperator):
    def execute(self, context):
        # Custom validation logic
        # Integration with monitoring systems
        # Alert generation for anomalies
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
