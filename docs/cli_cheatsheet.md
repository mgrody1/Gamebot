# Gamebot CLI & Command Reference

This guide provides essential commands for managing the Gamebot medallion architecture pipeline. All commands include context on where to run them and what they accomplish.

## Choose Your Deployment Method

| Method | Use Case | Context | Commands Section |
|--------|----------|---------|------------------|
| **Development** | Local development, customization, contributions | Full repo with source code | [Development Commands](#development-mode-full-repository) |
| **Production** | Team deployment, automated refreshes, BI integration | Standalone deployment (no repo) | [Production Commands](#production-deployment-mode) |

---

## Production Deployment Mode

**For teams wanting a production-ready warehouse with minimal setup** - no code repository required.

### Quick Start

```bash
# 1. Create project directory
mkdir survivor-warehouse && cd survivor-warehouse

# 2. Download production stack
curl -O https://raw.githubusercontent.com/mgrody1/Gamebot/main/deploy/docker-compose.yml
curl -O https://raw.githubusercontent.com/mgrody1/Gamebot/main/deploy/.env.example

# 3. Configure for your environment
cp .env.example .env
# Edit .env with your database credentials and settings

# 4. Initialize environment (Linux/Mac: set user ID to avoid permission issues)
echo -e "AIRFLOW_UID=$(id -u)" >> .env  # Linux/Mac only
mkdir -p ./run_logs/validation ./run_logs/notifications

# 5. Launch production stack
docker compose up -d

# 6. Access Airflow UI
# Browser: http://localhost:8080
# Login: admin/admin (default - change in .env!)
```

### Essential Production Commands

| Command | Purpose | Impact |
|---------|---------|--------|
| `docker compose up -d` | Start all services in background | Preserves data |
| `docker compose down` | Stop services (keep data) | No impact |
| `docker compose ps` | Check service status | No impact |
| `docker compose logs -f airflow-scheduler` | Monitor scheduler | No impact |
| `docker compose restart <service>` | Restart specific service | No impact |

### Production Pipeline Management

**Trigger Pipeline**:
```bash
# Via Airflow UI (recommended)
# http://localhost:8080 → DAGs → survivor_medallion_pipeline → Trigger

# Or via CLI
docker compose exec gamebot-airflow-scheduler airflow dags trigger survivor_medallion_pipeline
```

**Monitor Execution**:
```bash
# Follow scheduler logs
docker compose logs -f gamebot-airflow-scheduler

# Follow worker logs (where tasks execute)
docker compose logs -f gamebot-airflow-worker

# Check all service logs
docker compose logs -f
```

**Check Pipeline Status**:
```bash
# List recent DAG runs
docker compose exec gamebot-airflow-scheduler airflow dags list-runs -d survivor_medallion_pipeline --limit 10

# Get specific run status
docker compose exec gamebot-airflow-scheduler airflow dags state survivor_medallion_pipeline <run_id>
```

### Access Pipeline Outputs

Each pipeline run produces two types of outputs:

**1. Validation Reports** (Business Artifacts)

Validation reports are **automatically saved to your local machine** in `./run_logs/`:

```bash
# List validation reports (created after each pipeline run)
ls -lh ./run_logs/validation/

# Open latest Excel report
ls -t ./run_logs/validation/*/data_quality_*.xlsx | head -1

# Directory structure
# ./run_logs/validation/Run 0001 - <RUN_ID> Validation Files/
#   ├── data_quality_<run_id>_<timestamp>.xlsx  (Excel report)
#   ├── validation_<table>_<timestamp>.json     (JSON per table)
#   └── .run_id                                  (run metadata)
```

**2. Airflow Task Logs** (Pipeline Execution Logs)

Task logs are stored in Docker volumes. **Two ways to access:**

**Method A: Airflow UI** (Recommended)
- Navigate to: http://localhost:8080 → DAG → Task → View Logs
- Full stdout/stderr with syntax highlighting
- No file copying needed

**Method B: CLI Access**
```bash
# View bronze layer logs
docker compose exec gamebot-airflow-scheduler airflow tasks logs \
  survivor_medallion_pipeline load_bronze_layer --latest

# View silver/gold transformation logs
docker compose exec gamebot-airflow-scheduler airflow tasks logs \
  survivor_medallion_pipeline dbt_build_silver --latest

# Copy entire log directory if needed
docker compose cp gamebot-airflow-worker:/opt/airflow/logs ./local_logs/
```

**Why this separation?**
- **Validation reports**: Business artifacts → Host-mounted directories → Direct access
- **Task logs**: Operational logs → Docker volumes → Better performance, access via UI/CLI

### Database Access

**Connect from your SQL client** (DBeaver, DataGrip, Tableau, PowerBI):
- **Host**: `localhost`
- **Port**: `5433` (or value from `.env`)
- **Database**: Value from `DB_NAME` in `.env`
- **Username**: Value from `DB_USER` in `.env`
- **Password**: Value from `DB_PASSWORD` in `.env`

**Quick command-line access**:
```bash
# Connect to database
docker compose exec gamebot-warehouse-db psql -U <DB_USER> -d <DB_NAME>

# Check table counts
docker compose exec gamebot-warehouse-db psql -U <DB_USER> -d <DB_NAME> -c "
  SELECT
    schemaname,
    COUNT(*) as table_count,
    SUM(n_tup_ins) as total_rows
  FROM pg_stat_user_tables
  WHERE schemaname IN ('bronze', 'public_silver', 'public_gold')
  GROUP BY schemaname
  ORDER BY schemaname;
"
```

### Production Maintenance

**Update to Latest Data**:
```bash
# Pipeline runs automatically on schedule (default: Monday 4AM UTC)
# Or trigger manually via Airflow UI

# Check schedule
docker compose exec gamebot-airflow-scheduler airflow dags list-runs -d survivor_medallion_pipeline --limit 5
```

**Backup Database**:
```bash
# Create backup
docker compose exec gamebot-warehouse-db pg_dump -U <DB_USER> <DB_NAME> > backup_$(date +%Y%m%d).sql

# Restore from backup
cat backup_20241107.sql | docker compose exec -T gamebot-warehouse-db psql -U <DB_USER> <DB_NAME>
```

**Update to Latest Gamebot Version**:
```bash
# Pull latest image
docker compose pull

# Restart with new image
docker compose down && docker compose up -d
```

**Clean Restart** (removes all data):
```bash
docker compose down -v  # -v removes volumes (deletes data!)
docker compose up -d
```

### Troubleshooting Production

**Services won't start**:
```bash
# Check logs for errors
docker compose logs gamebot-airflow-init
docker compose logs gamebot-warehouse-db

# Check if ports are in use
lsof -i :8080  # Airflow
lsof -i :5433  # Database
```

**DAG not appearing**:
```bash
# Restart scheduler
docker compose restart gamebot-airflow-scheduler

# Check scheduler logs
docker compose logs gamebot-airflow-scheduler | grep -i "medallion"
```

**Database connection errors**:
```bash
# Verify database is healthy
docker compose ps gamebot-warehouse-db

# Test connection
docker compose exec gamebot-warehouse-db pg_isready -U <DB_USER> -d <DB_NAME>
```

---

## Development Mode (Full Repository)

**For developers customizing pipelines or contributing to Gamebot** - requires full repository.

## Quick Reference

### Essential Operations

| Command | Where to Run | Purpose | Data Impact |
|---------|--------------|---------|-------------|
| `make fresh` | Host terminal | Complete clean setup (build + start + initialize) | **Creates new DB** |
| `make up` | Host terminal | Start existing stack | Preserves data |
| `make down` | Host terminal | Stop services (keep volumes) | No impact |
| `make clean` | Host terminal | Remove everything including data | **Deletes all data** |
| `make logs` | Host terminal | Monitor live Airflow execution | No impact |
| `make ps` | Host terminal | Check service status | No impact |

### Pipeline Execution

| Command | Where to Run | Purpose | Data Impact |
|---------|--------------|---------|-------------|
| **Airflow UI → Trigger DAG** | Browser | Complete pipeline execution | Updates all layers |
| `docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline` | Host terminal | Trigger pipeline via CLI | Updates all layers |
| `make loader` | Host terminal | Bronze ingestion only | Updates bronze only |

### Development & Debugging

| Command | Where to Run | Purpose |
|---------|--------------|---------|
| `docker compose exec airflow-worker bash` | Host terminal | Shell access to worker container |
| `docker compose exec warehouse-db psql -U survivor_dev survivor_dw_dev` | Host terminal | Direct database access |
| `docker compose logs -f airflow-scheduler` | Host terminal | Follow scheduler logs |
| `docker compose logs -f airflow-worker` | Host terminal | Follow worker logs |

## Where to Run Commands

**Host Terminal** (Local command prompt):
- All `make` commands
- All `docker compose` commands
- Stack management operations

**Container Execution** (via docker compose exec):
- Database operations
- Airflow CLI commands
- Debugging and troubleshooting

**Dev Container/VS Code** (Optional):
- Code editing and development
- Local Python/dbt testing
- NOT for stack management

## Step-by-Step Workflows

### First Time Setup

```bash
# 1. Clone repository
git clone https://github.com/mgrody1/Gamebot.git
cd Gamebot

# 2. Create configuration
cp .env.example .env
# Edit .env with your database credentials

# 3. Launch complete stack
make fresh

# 4. Access Airflow UI
# Browser: http://localhost:8080
# Login: admin/admin (change in .env for production)

# 5. Trigger medallion pipeline
# Airflow UI → DAGs → survivor_medallion_pipeline → Trigger
```

### Pipeline Execution**Automated (Recommended)**:
```bash
# Start services
make up

# Trigger via Airflow UI
# http://localhost:8080 → survivor_medallion_pipeline → Trigger

# Monitor execution
make logs
```

**Manual Layer Execution**:
```bash
# Bronze only (Python ingestion)
make loader

# Silver only (dbt feature engineering)
docker compose exec airflow-worker bash -c "
  cd /opt/airflow
  mkdir -p /tmp/dbt_logs /tmp/dbt_target
  /home/airflow/.local/bin/dbt build --project-dir dbt --profiles-dir dbt --select silver --log-path /tmp/dbt_logs --target-path /tmp/dbt_target
"

# Gold only (dbt ML features)
docker compose exec airflow-worker bash -c "
  cd /opt/airflow
  mkdir -p /tmp/dbt_logs /tmp/dbt_target
  /home/airflow/.local/bin/dbt build --project-dir dbt --profiles-dir dbt --select gold --log-path /tmp/dbt_logs --target-path /tmp/dbt_target
"
```

### Verification & Monitoring

**Check Pipeline Success**:
```bash
# Service status
make ps

# Live monitoring
make logs

# Database verification
docker compose exec warehouse-db psql -U survivor_dev survivor_dw_dev -c "
  SELECT schemaname, relname, n_tup_ins
  FROM pg_stat_user_tables
  WHERE schemaname IN ('bronze', 'public_silver', 'public_gold')
  ORDER BY schemaname, relname;
"

# Airflow task status
docker compose exec airflow-scheduler airflow tasks states-for-dag-run survivor_medallion_pipeline <run_id>
```

**Access Excel Validation Reports**:

```bash
# Find latest validation report in container
docker compose exec airflow-worker bash -c "
  find /opt/airflow -name 'data_quality_*.xlsx' -type f | head -5
"

# Copy latest report to host
LATEST_REPORT=$(docker compose exec airflow-worker bash -c "
  find /opt/airflow -name 'data_quality_*.xlsx' -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2
" | tr -d '\r')

docker compose cp airflow-worker:$LATEST_REPORT ./data_quality_report.xlsx

# Open with Excel/LibreOffice for detailed data quality analysis
```

**Alternative: Persistent Validation Reports**:

```bash
# Run loader with mounted logs directory for persistent reports
docker compose run --rm \
  -e GAMEBOT_RUN_LOG_DIR=/workspace/run_logs \
  -v $(pwd)/run_logs:/workspace/run_logs \
  --profile loader survivor-loader

# Reports automatically saved to: ./run_logs/validation/data_quality_<timestamp>.xlsx
```

**Expected Results** (Successful Pipeline):
- **Bronze**: 21 tables with 193,000+ records
- **Silver**: 8 tables with strategic features + 9 tests passing
- **Gold**: 2 ML-ready tables with 4,248 rows each + 4 tests passing

### Development & Debugging**Container Debugging**:
```bash
# Access worker container for dbt debugging
docker compose exec airflow-worker bash

# Check dbt configuration
cd /opt/airflow
/home/airflow/.local/bin/dbt debug --project-dir dbt --profiles-dir dbt

# Test specific dbt models
mkdir -p /tmp/dbt_logs /tmp/dbt_target
/home/airflow/.local/bin/dbt run --project-dir dbt --profiles-dir dbt --select castaway_profile --log-path /tmp/dbt_logs --target-path /tmp/dbt_target
```

**Database Operations**:
```bash
# Connect to warehouse database
docker compose exec warehouse-db psql -U survivor_dev survivor_dw_dev

# Backup database
docker compose exec warehouse-db pg_dump -U survivor_dev survivor_dw_dev > backup.sql

# Check table counts
docker compose exec warehouse-db psql -U survivor_dev survivor_dw_dev -c "\dt+ bronze.*"
```

### Maintenance

**Regular Maintenance**:
```bash
# Restart services (keep data)
make down && make up

# Update codebase and restart
git pull
make down && make up

# Monitor disk usage
docker system df
```

**Clean Reset** (Removes all data):
```bash
# Complete reset with data loss
make clean && make fresh

# Reset just database (keep container images)
make down
docker volume rm gamebot_warehouse-data
make up
```

## Common Issues & Solutions

**Port conflicts**:
```bash
# Change Airflow port in .env
AIRFLOW_PORT=8082
make down && make up
```

**Database connection issues**:
```bash
# Verify database is running
make ps | grep warehouse-db

# Test connection
docker compose exec warehouse-db pg_isready -U survivor_dev
```

**DAG not showing up**:
```bash
# Restart scheduler
make down && make up

# Check logs
make logs | grep -i dag
```

**dbt permission errors**:
```bash
# Use container execution with writable directories (already configured in DAG)
mkdir -p /tmp/dbt_logs /tmp/dbt_target
dbt <command> --log-path /tmp/dbt_logs --target-path /tmp/dbt_target
```

## Performance Tips

- Use `make up` for daily operations, `make fresh` only when needed
- Monitor container resource usage with `docker stats`
- Pipeline typically completes in 2-3 minutes with full data
- Database operations are fastest via direct PostgreSQL connection

See the [README](../README.md) for a project overview and more links.
