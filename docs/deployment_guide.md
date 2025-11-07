# Gamebot for Data Teams & Organizations

**Quick Start**: [Prerequisites](#prerequisites) → [Deploy warehouse](#warehouse-deployment) → [Connect tools](#database-access) → [Schedule updates](#automation) → [Monitor pipeline](#operations)

This guide covers production deployment of Gamebot Warehouse for teams requiring shared database access, automated updates, and BI tool integration.

---

## Prerequisites

### Infrastructure Requirements
- **Docker Engine/Desktop**: Version 20.10+ with Docker Compose
- **System Resources**: 4GB RAM, 10GB disk space minimum
- **Network Access**: Outbound HTTPS for data source updates
- **Ports Available**: 5433 (database), 8081 (Airflow UI)

### Team Setup
- **Database Credentials**: Shared team credentials for PostgreSQL access
- **Environment Configuration**: Centralized `.env` management
- **Backup Strategy**: Database backup procedures (recommended)

---

## Warehouse Deployment

### Option 1: Quick Deploy (Recommended)

For teams wanting immediate deployment with official Docker images:

> **Note**: This deployment follows the [official Apache Airflow Docker setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) with Gamebot-specific configurations.

```bash
# 1. Create project directory
mkdir survivor-warehouse && cd survivor-warehouse

# 2. Download production stack
curl -O https://raw.githubusercontent.com/mgrody1/Gamebot/medallion-refactor/deploy/docker-compose.yml
curl -O https://raw.githubusercontent.com/mgrody1/Gamebot/medallion-refactor/deploy/.env.example

# 3. Configure for your environment
cp .env.example .env
```

**Edit `.env` for your team**:
```bash
# Database configuration
DB_NAME=survivor_dw_prod           # Your team's database name
DB_USER=survivor_team              # Team database user
DB_PASSWORD=secure_team_password   # Strong password
PORT=5433                          # External database port

# Airflow configuration
SURVIVOR_ENV=prod                  # Mark as production
```

**Linux/Mac Users** - Set the Airflow user ID:
```bash
# On Linux, set AIRFLOW_UID to your user ID to avoid permission issues
echo -e "AIRFLOW_UID=$(id -u)" >> .env

# Create directories for pipeline artifacts
mkdir -p ./run_logs/validation ./run_logs/notifications
```

**Windows Users** - Use default UID:
```bash
# Create directories for pipeline artifacts
mkdir -p ./run_logs/validation ./run_logs/notifications

# The default AIRFLOW_UID=50000 will be used
```

```bash
# 4. Launch production stack
docker compose up -d

# 5. Verify deployment
docker compose ps
```

**Access validation reports**: After the first pipeline run, data quality reports will be available in the `./run_logs/validation/` directory:

```bash
# View latest validation reports
ls -la run_logs/validation/

# Open the most recent Excel report
# Reports are named: data_quality_<run_id>_<timestamp>.xlsx
```

### Option 2: Custom Infrastructure

For teams with existing database infrastructure:

```bash
# 1. Clone repository for custom configuration
git clone https://github.com/mgrody1/Gamebot.git
cd Gamebot

# 2. Configure for external database
cp .env.example .env
```

**Edit `.env` for external database**:
```bash
# Point to your existing PostgreSQL instance
DB_HOST=your-postgres-server.company.com
DB_NAME=survivor_warehouse
DB_USER=gamebot_service_account
DB_PASSWORD=your_service_account_password
PORT=5432                          # Standard PostgreSQL port

# Use external database (no local container)
GAMEBOT_EXTERNAL_DB=true
```

```bash
# 3. Deploy Airflow orchestration only
docker compose up -d airflow-scheduler airflow-webserver airflow-worker
```

---

## Database Access

### Connection Configuration

**Standard SQL Clients** (DBeaver, DataGrip, Tableau, PowerBI):

| Setting | Value |
|---------|-------|
| **Host** | `localhost` (or deployment server IP) |
| **Port** | `5433` |
| **Database** | Value from `DB_NAME` in `.env` |
| **Username** | Value from `DB_USER` in `.env` |
| **Password** | Value from `DB_PASSWORD` in `.env` |
| **SSL Mode** | `prefer` (optional) |

### Team Access Patterns

**For Analysts**:
- Connect via SQL IDE for ad-hoc queries
- Use bronze layer for raw data exploration
- Use silver layer for feature-rich analysis

**For Data Scientists**:
- Connect via Python/R for ML workflows
- Focus on gold layer ML-ready matrices
- Export datasets for external modeling tools

**For BI Developers**:
- Connect Tableau/PowerBI to silver layer
- Build dashboards from curated feature tables
- Leverage pre-aggregated dimensions

### Database Schema Overview

```
🗄️ Bronze Layer (21 tables, 193k+ records)
├── castaways              # Contestant demographics
├── episodes               # Season metadata
├── vote_history          # Voting records
├── challenge_results     # Performance data
└── [17 more tables...]   # Complete survivoR dataset

🏗️ Silver Layer (8 tables, strategic features)
├── castaway_profile_curated      # Demographics + engineered features
├── challenge_performance_curated # Performance metrics
├── voting_dynamics_curated       # Strategic voting patterns
├── social_positioning_curated    # Alliance relationships
└── [4 more tables...]            # ML-focused features

🏆 Gold Layer (2 tables, 4,248 observations each)
├── ml_features_gameplay   # Gameplay-only ML matrix
└── ml_features_hybrid     # Gameplay + edit ML matrix
```

---

## Automation

### Pipeline Schedule

**Default Schedule**: Every Monday at 4AM UTC
- Automatically detects upstream survivoR dataset changes
- Refreshes bronze → silver → gold layers incrementally
- Generates data quality reports

**Custom Schedule Configuration**:
```bash
# Edit .env to change schedule (cron format)
GAMEBOT_DAG_SCHEDULE=0 2 * * 0     # Sunday 2AM UTC
GAMEBOT_DAG_SCHEDULE=0 */6 * * *   # Every 6 hours
GAMEBOT_DAG_SCHEDULE=@daily        # Daily at midnight
```

### Manual Pipeline Execution

**Via Airflow UI** (recommended for teams):
1. Navigate to http://localhost:8081
2. Login: `admin` / `admin`
3. Find `survivor_medallion_pipeline` DAG
4. Click "Trigger DAG" to run immediately

**Via Command Line**:
```bash
# Trigger complete pipeline
docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline

# Run specific layer only
docker compose exec airflow-scheduler airflow tasks run survivor_medallion_pipeline silver_build $(date +%Y-%m-%d)
```

### Data Freshness Monitoring

The pipeline automatically:
- Monitors upstream survivoR GitHub repository for changes
- Detects new season data or corrections
- Triggers updates only when new data is available
- Maintains data lineage and versioning

---

## Operations

### Health Monitoring

**Service Status**:
```bash
# Check all services
docker compose ps

# Follow logs
docker compose logs -f airflow-scheduler

# Monitor specific service
docker compose logs airflow-worker
```

**Pipeline Monitoring**:
- **Airflow UI**: http://localhost:8081 → DAG view for execution history
- **Data Quality Reports**: Generated after each pipeline run
- **Error Alerts**: Check Airflow logs for failed tasks

### Data Quality Reports

Each pipeline execution generates comprehensive validation reports:

```bash
# Find latest validation report
docker compose exec airflow-worker bash -c "
  find /opt/airflow -name 'data_quality_*.xlsx' -type f | head -5
"

# Copy report to local filesystem
LATEST_REPORT=$(docker compose exec airflow-worker bash -c "
  find /opt/airflow -name 'data_quality_*.xlsx' -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2
" | tr -d '\r')

docker compose cp airflow-worker:$LATEST_REPORT ./data_quality_report.xlsx
```

**Report Contents**:
- Row counts and schema validation
- Primary/foreign key integrity checks
- Duplicate detection and remediation
- Data type consistency verification
- Schema drift detection from upstream changes

### Backup & Recovery

**Database Backups**:
```bash
# Create database backup
docker compose exec warehouse-db pg_dump -U survivor_dev survivor_dw_dev > backup_$(date +%Y%m%d).sql

# Restore from backup (if needed)
docker compose exec -T warehouse-db psql -U survivor_dev survivor_dw_dev < backup_20250101.sql
```

**Configuration Backups**:
- Store `.env` files in secure team credential management
- Version control `docker-compose.yml` modifications
- Document any custom configuration changes

### Troubleshooting

**Common Issues**:

*Port Conflicts*:
```bash
# Change ports in .env if already in use
AIRFLOW_PORT=8082              # Alternative Airflow port
PORT=5434                      # Alternative database port
```

*Missing Pipeline Updates*:
```bash
# Restart services to pick up changes
docker compose down
docker compose up -d
```

*Database Connection Issues*:
```bash
# Check database container status
docker compose exec warehouse-db psql -U survivor_dev -d survivor_dw_dev -c "\dt"
```

*Fresh Deployment*:
```bash
# Complete reset (removes all data)
docker compose down -v
docker compose up -d
```

### Performance Optimization

**For Large Teams**:
- Consider PostgreSQL connection pooling
- Monitor disk space for data growth
- Scale worker containers for faster processing

**Resource Allocation**:
```yaml
# Add to docker-compose.yml if needed
services:
  airflow-worker:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
```

---

## Integration Patterns

### Business Intelligence Tools

**Tableau Connection**:
1. Server: `localhost:5433`
2. Database: Your `DB_NAME` value
3. Authentication: Your team credentials
4. Recommended: Connect to `silver.*` schema for curated features

**PowerBI Connection**:
1. Data Source: PostgreSQL
2. Server: `localhost:5433`
3. Database: Your `DB_NAME` value
4. Data Connectivity: Import or DirectQuery

**Excel/Google Sheets**:
- Use ODBC PostgreSQL driver
- Connect to specific tables via SQL queries
- Recommended for small extracts only

### API Access

**For Custom Applications**:
```python
import psycopg2
import pandas as pd

# Connect to warehouse
conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="survivor_dw_prod",
    user="survivor_team",
    password="secure_team_password"
)

# Query data
df = pd.read_sql("SELECT * FROM silver.castaway_profile_curated", conn)
```

### Data Export

**For External Systems**:
```bash
# Export specific tables
docker compose exec warehouse-db psql -U survivor_dev -d survivor_dw_dev -c "\copy silver.voting_dynamics_curated TO '/tmp/voting_data.csv' CSV HEADER"

# Copy to host system
docker compose cp warehouse-db:/tmp/voting_data.csv ./voting_data.csv
```

---

## Security & Access Control

### Database Security

**Production Recommendations**:
- Use strong, unique passwords for database accounts
- Restrict network access to necessary IP ranges
- Enable PostgreSQL SSL connections for remote access
- Regular security updates for Docker images

**Team Access Management**:
```sql
-- Create read-only analyst accounts
CREATE USER analyst_readonly WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE survivor_dw_prod TO analyst_readonly;
GRANT USAGE ON SCHEMA bronze, silver, gold TO analyst_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze, silver, gold TO analyst_readonly;

-- Create data scientist accounts with broader access
CREATE USER data_scientist WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE survivor_dw_prod TO data_scientist;
GRANT USAGE, CREATE ON SCHEMA analysis TO data_scientist;
```

### Environment Security

**Sensitive Configuration**:
- Store `.env` files in team credential management systems
- Avoid committing credentials to version control
- Use Docker secrets for production deployments
- Regular credential rotation policies

---

## Production Architecture Standards

**Enterprise-Grade Design Patterns**:
- **Medallion Lakehouse**: Industry-standard progressive data refinement (bronze → silver → gold)
- **Container Orchestration**: Docker Compose with proper service isolation and networking
- **Database Management**: PostgreSQL with ACID compliance and automated schema management
- **Workflow Orchestration**: Apache Airflow with Celery executor for distributed processing
- **Data Quality**: Comprehensive testing and validation frameworks at every layer
- **Security**: Environment-specific credential management and access controls

**Infrastructure Best Practices**:
- Context-aware configuration management
- Automated dependency resolution
- Health monitoring and observability
- Backup and recovery procedures
- Scalable resource allocation

These patterns are standard in enterprise data engineering environments and demonstrate production-ready capabilities suitable for organizational deployment.

---

## Scaling & Production Deployment

### Resource Requirements by Team Size

| Team Size | RAM | CPU | Storage | Database Connections |
|-----------|-----|-----|---------|---------------------|
| 5-10 users | 4GB | 2 cores | 20GB | Default (100) |
| 10-25 users | 8GB | 4 cores | 50GB | 200 connections |
| 25+ users | 16GB | 8 cores | 100GB | 500+ connections |

### High Availability

**For Mission-Critical Deployments**:
- PostgreSQL replication for database redundancy
- Load balancing for Airflow web interface
- Shared file systems for pipeline artifacts
- Monitoring and alerting integration

### Cloud Deployment

**AWS/Azure/GCP Deployment**:
- Use managed PostgreSQL services (RDS, Azure Database, Cloud SQL)
- Deploy Airflow on container orchestration (EKS, AKS, GKE)
- Use cloud storage for pipeline artifacts and backups
- Leverage cloud monitoring and logging services

---

## Getting Support

### Team Training Resources
- **Architecture Overview**: [docs/architecture_overview.md](architecture_overview.md)
- **Schema Reference**: [docs/gamebot_warehouse_schema_guide.md](gamebot_warehouse_schema_guide.md)
- **Analyst Guide**: [docs/analyst_guide.md](analyst_guide.md)

### Technical Support
- **Issues**: [GitHub Issues](https://github.com/mgrody1/Gamebot/issues)
- **Documentation**: [Complete docs directory](../docs/)
- **Community**: Gamebot user community discussions

### Professional Services
For enterprise deployments requiring:
- Custom feature engineering
- Advanced security configurations
- High-availability architecture
- Team training and onboarding

Contact the Gamebot team for professional services and support options.
