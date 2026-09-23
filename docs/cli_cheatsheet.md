# Gamebot CLI & Command Reference

This guide provides essential commands for managing the Gamebot medallion architecture pipeline. All commands include context on where to run them and what they accomplish.

## Choose Your Deployment Method

| Method | Use Case | Context | Commands Section |
|--------|----------|---------|------------------|
| **Development** (Gamebot Studio) | Local development, customization, contributions | Full repo with source code | [Development Commands](#development-mode-full-repository) |
| **Production (with official docker hub images)** (Gamebot Warehouse) | Team deployment, automated refreshes, BI integration | Standalone deployment (no repo) | [Production Commands](#production-deployment-mode-with-official-docker-hub-images) |

---

## Production Deployment Mode (with official docker hub images)

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
# Login: admin/admin by default (set AIRFLOW_ADMIN_USERNAME / AIRFLOW_ADMIN_PASSWORD
# and AIRFLOW_FERNET_KEY in .env before step 5 for any shared deployment)
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

> Tip Before Starting: If you are unfamiliar with the Airflow UI, you can checkout the docs [here](https://airflow.apache.org/docs/apache-airflow/stable/ui.html#home-page)

##### **Trigger Pipeline**:
###### Via Airflow UI (recommended)
Navigate to the port where Airflow is running (likely on your localhost and using port 8080 if you are running the default set-up)
Make sure you see the `survivor_medallion_pipeline` DAG. This stack creates DAGs paused, so switch it on (unpause) first; neither scheduled nor manual runs start while it is paused. Then use the "play" button in the UI to trigger a run manually
```bash
# http://localhost:8080 → DAGs → survivor_medallion_pipeline → Trigger
```
##### Or via CLI
```
docker compose exec airflow-scheduler airflow dags unpause survivor_medallion_pipeline
docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline
```

**Monitor Execution**:
```bash
# Follow scheduler logs
docker compose logs -f airflow-scheduler

# Follow worker logs (where tasks execute)
docker compose logs -f airflow-worker

# Check all service logs
docker compose logs -f
```

**Check Pipeline Status**:
```bash
# List recent DAG runs
docker compose exec airflow-scheduler airflow dags list-runs -d survivor_medallion_pipeline | head -n 15

# Get specific run status
docker compose exec airflow-scheduler airflow dags state survivor_medallion_pipeline <logical_date>
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
# List task log folders for the DAG (one per run and task)
docker compose exec airflow-worker ls /opt/airflow/logs/dag_id=survivor_medallion_pipeline

# Copy entire log directory if needed
docker compose cp airflow-worker:/opt/airflow/logs ./local_logs/
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
docker compose exec warehouse-db psql -U <DB_USER> -d <DB_NAME>

# Check table counts
docker compose exec warehouse-db psql -U <DB_USER> -d <DB_NAME> -c "
  SELECT
    schemaname,
    COUNT(*) as table_count,
    SUM(n_tup_ins) as total_rows
  FROM pg_stat_user_tables
  WHERE schemaname IN ('bronze', 'silver', 'gold')
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
docker compose exec airflow-scheduler airflow dags list-runs -d survivor_medallion_pipeline | head -n 10
```

**Backup Database**:
```bash
# Create backup
docker compose exec warehouse-db pg_dump -U <DB_USER> <DB_NAME> > backup_$(date +%Y%m%d).sql

# Restore from backup
cat backup_20241107.sql | docker compose exec -T warehouse-db psql -U <DB_USER> <DB_NAME>
```

**Update to Latest Gamebot Version**:
```bash
# Pull latest image
docker compose pull

# Restart with new image
docker compose down && docker compose up -d
```

**Clean Restart** (removes all data):

> **WARNING:** This WILL DELETE your existing database and any data you have been collecting. Only use the `-v` flag if that is what you want to do

```bash
docker compose down -v  # -v removes volumes (deletes data!)
docker compose up -d
```

### Troubleshooting Production

**Services won't start**:
```bash
# Check logs for errors
docker compose logs airflow-init
docker compose logs warehouse-db

# Check if ports are in use
lsof -i :8080  # Airflow
lsof -i :5433  # Database
```

**DAG not appearing**:
```bash
# Restart scheduler
docker compose restart airflow-scheduler

# Check scheduler logs
docker compose logs airflow-scheduler | grep -i "medallion"
```

**Database connection errors**:
```bash
# Verify database is healthy
docker compose ps warehouse-db

# Test connection
docker compose exec warehouse-db pg_isready -U <DB_USER> -d <DB_NAME>
```

---

## Development Mode (Full Repository)

**For developers customizing pipelines or contributing to Gamebot** - requires full repository.

## Quick Reference

### Essential Operations

> **WARNING:** `make fresh` and `make clean` WILL DELETE your existing database and any data you have been collecting. Only use the these commands if that is what you want to do

| Command | Where to Run | Purpose | Data Impact |
|---------|--------------|---------|-------------|
| `make fresh` | Host terminal | Complete clean setup (build + start + initialize) | **DELETES ALL EXISTING DATA & Creates new DB** |
| `make up` | Host terminal | Start existing stack | Preserves data |
| `make down` | Host terminal | Stop services (keep volumes) | No impact |
| `make clean` | Host terminal | Remove everything including data | **DELETES ALL EXISTING DAT** |
| `make logs` | Host terminal | Monitor live Airflow execution | No impact |
| `make ps` | Host terminal | Check service status | No impact |

### Pipeline Execution

| Command | Where to Run | Purpose | Data Impact |
|---------|--------------|---------|-------------|
| **Airflow UI → Trigger DAG** | Browser | Complete pipeline execution | Updates all layers |
| `docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline` | Host terminal, `airflow/` | Trigger pipeline via CLI | Updates all layers |
| `make loader` | Host terminal | Bronze ingestion only (loads the stack's `warehouse-db`, `DB_NAME` from `.env`) | Updates bronze only |

### Development & Debugging

| Command | Where to Run | Purpose |
|---------|--------------|---------|
| `docker compose exec airflow-worker bash` | Host terminal, `airflow/` | Shell access to worker container |
| `docker compose exec warehouse-db psql -U survivor_dev survivor_dw_dev` | Host terminal, `airflow/` | Direct database access |
| `docker compose logs -f airflow-scheduler` | Host terminal, `airflow/` | Follow scheduler logs |
| `docker compose logs -f airflow-worker` | Host terminal, `airflow/` | Follow worker logs |

## Where to Run Commands

**Host Terminal** (Local command prompt):
- All `make` commands
- All `docker compose` commands (from the `airflow/` directory; the Compose file lives there)
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

> **WARNING:** `make fresh` and `make clean` WILL DELETE your existing database (if one already exists) and any data you have been collecting. Only use the these commands if that is what you want to do

```bash
# 1. Clone repository
git clone https://github.com/mgrody1/Gamebot.git
cd Gamebot

# 2. Create configuration
cp .env.example .env
# Edit .env with your database credentials

# 3. Launch complete stack
make up

# 4. Access Airflow UI
# Browser: http://localhost:8080
# Login: admin/admin (change in .env for production)

# 5. Trigger medallion pipeline
# Airflow UI → DAGs → survivor_medallion_pipeline → Trigger
```

### Pipeline Execution

**Automated (Recommended)**:
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
  WHERE schemaname IN ('bronze', 'silver', 'gold')
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
# Run the on-demand loader; it mounts the repository at /app, so reports land in ./run_logs/
make loader

# Reports saved under: ./run_logs/validation/Run NNNN - <RUN_ID> Validation Files/
```

**Expected Results** (Successful Pipeline):
- **Bronze**: 21 tables with 183,000+ records
- **Silver**: 8 tables with strategic features + 11 tests passing
- **Gold**: 2 ML-ready tables with 1,441 rows each + 6 tests passing

### Development & Debugging

**Container Debugging**:
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

> **WARNING:** `make fresh` and `make clean` WILL DELETE your existing database and any data you have been collecting. Only use the these commands if that is what you want to do

```bash
# Complete reset with data loss
make clean && make fresh

# Reset just database (keep container images)
make down
docker volume rm airflow_warehouse-data   # Compose project name is the airflow/ directory
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

- Use `make up` for daily operations, `make fresh` only when needing to start completely brand new
- Monitor container resource usage with `docker stats`
- Pipeline typically completes in 2-3 minutes with full data
- Database operations are fastest via direct PostgreSQL connection

See the [README](../README.md) for a project overview and more links.
