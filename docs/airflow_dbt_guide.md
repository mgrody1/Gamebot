# Airflow + dbt Integration Guide

This guide documents the production-ready integration between Apache Airflow and dbt that powers Gamebot's medallion architecture. It covers the key technical solutions that enable seamless container-based execution.

## Architecture Overview

Gamebot successfully runs dbt transformations within Airflow containers using a combination of:

- **Custom Airflow image** with dbt pre-installed
- **Container permission management** for writable directories
- **Context-aware networking** for database connectivity
- **Automated orchestration** via the `survivor_medallion_pipeline` DAG

## Container Permission Resolution

> **Reference**: This approach follows [official Apache Airflow Docker best practices](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user) for container permissions.

### The Challenge

When running dbt inside Airflow containers, permission conflicts can arise between:
- **Airflow container user** (default uid 50000, configurable via `AIRFLOW_UID`) - container execution user
- **Host user** (varies by system) - owns volume-mounted directories
- **Write operations** - dbt needs to create logs and compilation artifacts

### The Solution

**Writable Temporary Directories**: Use container-local directories for dbt artifacts:

```bash
# DAG implementation
mkdir -p /tmp/dbt_logs /tmp/dbt_target
dbt build \
  --project-dir dbt \
  --profiles-dir dbt \
  --log-path /tmp/dbt_logs \
  --target-path /tmp/dbt_target
```

**Key Benefits**:
- No permission conflicts with host-mounted volumes
- Container-local directories are always writable
- Temporary files are automatically cleaned up
- Preserves dbt functionality without volume mount issues
- Works regardless of `AIRFLOW_UID` configuration

**Alternative Approach**: Setting `AIRFLOW_UID=$(id -u)` in `.env` (as recommended in the deployment guide) would allow dbt to write to host-mounted directories, but using `/tmp` is simpler and follows container best practices.

## DAG Implementation

### Complete Task Configuration

The `survivor_medallion_dag.py` implements bulletproof dbt execution:

```python
dbt_build_silver = BashOperator(
    task_id="dbt_build_silver",
    bash_command="""
    set -e  # Exit on any error
    cd /opt/airflow

    # Create writable directories for dbt
    mkdir -p /tmp/dbt_logs /tmp/dbt_target
    export DBT_LOG_PATH=/tmp/dbt_logs

    # Execute dbt with proper paths
    /home/airflow/.local/bin/dbt build \
      --project-dir dbt \
      --profiles-dir dbt \
      --select silver \
      --log-path /tmp/dbt_logs \
      --target-path /tmp/dbt_target
    """,
)
```

**Critical Elements**:
- `set -e`: Ensures task failure on any error
- **Explicit paths**: No ambiguity in file locations
- **Permission-safe directories**: Container-writable locations
- **Environment variables**: Proper dbt configuration

### Error Handling & Validation

The DAG includes comprehensive error handling:

```bash
# Environment validation
echo "=== Environment Check ==="
echo "Current directory: $(pwd)"
echo "DB_HOST: $DB_HOST"
echo "DB_NAME: $DB_NAME"
echo "DB_USER: $DB_USER"

# Execution with explicit error handling
set -e
/home/airflow/.local/bin/dbt build ... || exit 1
```

## Container Networking

### Database Connectivity

**Automatic Context Switching**: Docker Compose handles networking seamlessly:

| Context | DB_HOST | PORT | Usage |
|---------|---------|------|-------|
| **Local Development** | `localhost` | `5433` | Host access |
| **Airflow Containers** | `warehouse-db` | `5432` | Container networking |

### Docker Compose Configuration

```yaml
# airflow/docker-compose.yaml
x-airflow-common: &airflow-common
  env_file: [../.env]          # Base configuration
  environment:
    # Container-specific overrides
    DB_HOST: warehouse-db      # Internal networking
    DB_PORT: "5432"            # Internal port
    # Database connection string
    AIRFLOW_CONN_SURVIVOR_POSTGRES: postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@warehouse-db:5432/${DB_NAME}
```

**Benefits**:
- **No manual configuration**: Automatic context detection
- **Secure networking**: Container-to-container communication
- **External access preserved**: Host can still connect via localhost:5433
- **Production ready**: Proper service isolation

## dbt Profile Configuration

### Environment-Driven Configuration

The `dbt/profiles.yml` uses environment variables for automatic adaptation:

```yaml
survivor:
  target: default
  outputs:
    default:
      type: postgres
      host: "{{ env_var('DB_HOST') }}"      # Context-aware
      user: "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DB_PASSWORD') }}"
      dbname: "{{ env_var('DB_NAME') }}"
      port: "{{ env_var('DB_PORT') | as_number }}"  # Context-aware
      schema: "public"
      threads: 4
      keepalives_idle: 0
```

**Automatic Adaptation**:
- **Local development**: Uses `localhost:5433`
- **Container execution**: Uses `warehouse-db:5432`
- **No profile changes required**: Same configuration file works everywhere

## Testing & Validation

### Pipeline Verification

**Comprehensive Testing Strategy**:

```bash
# 1. Container environment verification
docker compose exec airflow-worker bash -c "
  cd /opt/airflow
  echo 'Testing dbt debug...'
  mkdir -p /tmp/dbt_logs
  /home/airflow/.local/bin/dbt debug --project-dir dbt --profiles-dir dbt --log-path /tmp/dbt_logs
"

# 2. Individual layer testing
docker compose exec airflow-worker bash -c "
  cd /opt/airflow
  mkdir -p /tmp/dbt_logs /tmp/dbt_target
  /home/airflow/.local/bin/dbt build --project-dir dbt --profiles-dir dbt --select silver --log-path /tmp/dbt_logs --target-path /tmp/dbt_target
"

# 3. Complete pipeline execution
docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline
```

### Expected Results

**Successful Pipeline Execution**:
- **Bronze**: 21 tables, 193,000+ records (Python ingestion)
- **Silver**: 8 tables, 9 tests passing (dbt transformations)
- **Gold**: 2 tables, 4 tests passing (dbt ML features)
- **Execution time**: ~2 minutes end-to-end## Development Workflow

### Local dbt Development

For direct dbt development and testing:

```bash
# 1. Ensure database is running
make up

# 2. Local dbt execution (uses localhost:5433)
pipenv run dbt debug --project-dir dbt --profiles-dir dbt
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver

# 3. Test in container context
docker compose exec airflow-worker bash -c "
  cd /opt/airflow
  mkdir -p /tmp/dbt_logs /tmp/dbt_target
  /home/airflow/.local/bin/dbt build --project-dir dbt --profiles-dir dbt --select silver --log-path /tmp/dbt_logs --target-path /tmp/dbt_target
"
```

### Debugging Common Issues

**Permission Errors**:
```bash
# Always use writable directories in containers
mkdir -p /tmp/dbt_logs /tmp/dbt_target
dbt <command> --log-path /tmp/dbt_logs --target-path /tmp/dbt_target
```

**Connection Issues**:
```bash
# Verify environment variables in container
docker compose exec airflow-worker env | grep DB_

# Test database connectivity
docker compose exec airflow-worker bash -c "
  cd /opt/airflow
  /home/airflow/.local/bin/dbt debug --project-dir dbt --profiles-dir dbt
"
```

**DAG Execution Failures**:
```bash
# Check Airflow logs
make logs

# Check specific task logs
docker compose exec airflow-scheduler airflow tasks logs survivor_medallion_pipeline dbt_build_silver --latest
```

## Performance Optimizations

### Container Resource Management

**Efficient Resource Usage**:
- **dbt threads**: Configured for 4 concurrent operations
- **Container memory**: Adequate for largest dbt transformations
- **Temporary directories**: Automatic cleanup on container restart
- **Network optimization**: Direct container-to-container communication

### Pipeline Optimization

**Execution Efficiency**:
- **Incremental models**: dbt supports incremental transformations
- **Test parallelization**: Multiple data quality tests run concurrently
- **Dependency management**: Proper task sequencing in Airflow DAG
- **Resource pooling**: Database connections are properly managed

## Production Considerations

### Monitoring & Alerting

**Operational Monitoring**:
- **Airflow UI**: Comprehensive pipeline execution monitoring
- **dbt logs**: Detailed transformation logs in `/tmp/dbt_logs`
- **Database monitoring**: PostgreSQL query logs and performance metrics
- **Container health**: Docker health checks for all services

### Scaling & Deployment

**Production Deployment**:
- **External database**: Replace Docker PostgreSQL with managed service
- **Distributed execution**: Airflow Celery executor supports multiple workers
- **Resource scaling**: Adjust container CPU/memory for workload
- **High availability**: Load balancer for Airflow web interface

This integration represents a production-ready solution for running dbt transformations at scale within Apache Airflow, successfully handling the complexities of containerized data pipeline execution.
