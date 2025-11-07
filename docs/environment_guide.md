# Environment Configuration Guide

## Overview

Gamebot uses a **simplified, single-file configuration system** that automatically adapts to different execution contexts without manual intervention. This approach eliminates the complexity of managing multiple environment files while ensuring seamless operation across local development, containerized execution, and production environments.

## Configuration Architecture

### Single Source of Truth: `.env`

The entire system is configured through **one file**: `.env` at the repository root.

```bash
# .env - Complete configuration in one place
DB_HOST=localhost              # Automatically context-aware
DB_NAME=survivor_dw_dev
DB_USER=survivor_dev
DB_PASSWORD=your_secure_password
PORT=5433                      # External port for local access
WAREHOUSE_DB_PORT=5433         # Docker port mapping

# Application configuration
SURVIVOR_ENV=dev
GAMEBOT_TARGET_LAYER=gold
GAMEBOT_DAG_SCHEDULE=0 4 * * 1

# Airflow configuration
AIRFLOW_PORT=8080
AIRFLOW__API_RATELIMIT__STORAGE=redis://redis:6379/1
AIRFLOW__API_RATELIMIT__ENABLED=True

# Optional integrations
GITHUB_TOKEN=
```

### Automatic Context Detection

**No Manual Switching Required**: The system automatically detects execution context and applies appropriate overrides:

| Context | Detection Method | DB_HOST Override | PORT Override |
|---------|-----------------|------------------|---------------|
| **Local Development** | Default behavior | `localhost` | `5433` |
| **Docker Containers** | Docker Compose environment | `warehouse-db` | `5432` |
| **CI/CD** | Environment detection | As configured | As configured |

### Docker Compose Integration

The `docker-compose.yaml` file automatically overrides specific variables for container networking:

```yaml
# airflow/docker-compose.yaml (automatic)
x-airflow-common: &airflow-common
  env_file: [../.env]          # Load base configuration
  environment:
    # Container-specific overrides
    DB_HOST: warehouse-db      # Internal container networking
    PORT: "5432"              # Internal PostgreSQL port
    # All other variables inherited from .env
```

**Key Benefits**:
- **Single Configuration**: Edit only `.env` file
- **Automatic Adaptation**: No manual environment switching
- **Development Friendly**: Works locally and in containers seamlessly
- **Production Ready**: Simple deployment configuration

## Setup and Usage

### Initial Configuration

```bash
# 1. Copy template
cp .env.example .env

# 2. Edit with your settings
# Update database credentials, ports, etc.

# 3. Start system - no additional configuration needed
make fresh
```

### Development Workflow

**Local Development**:
```bash
# Configuration works automatically for:
pipenv run python -m Database.load_survivor_data    # Uses localhost:5433
pipenv run dbt debug --project-dir dbt              # Uses localhost:5433
```

**Container Development**:
```bash
# Same configuration automatically adapts for:
docker compose exec airflow-worker bash             # Uses warehouse-db:5432
make loader                                          # Uses warehouse-db:5432
```

### Database Connections

**External Tools** (DBeaver, notebooks, etc.):
```bash
Host: localhost
Port: 5433
Database: survivor_dw_dev (from .env)
User: survivor_dev (from .env)
Password: (from .env)
```

**Container Services** (automatic):
```bash
Host: warehouse-db
Port: 5432
Database: survivor_dw_dev
User: survivor_dev
Password: (inherited from .env)
```

## Migration from Legacy System

If upgrading from the previous `env/` directory approach:

### Quick Migration

```bash
# 1. Copy your existing settings
# From env/.env.dev or env/.env.prod to root .env

# 2. Remove legacy files
rm -rf env/
# Remove any references to setup_env.py

# 3. Test new configuration
make fresh
```

### Key Changes

| Old System | New System | Benefit |
|------------|------------|---------|
| `env/.env.dev` + `env/.env.prod` | Single `.env` | Simplified management |
| `setup_env.py` script | Automatic detection | No manual switching |
| `airflow/.env` sync | Docker Compose `env_file` | Automatic synchronization |
| Manual context switching | Context-aware overrides | Seamless operation |

## Advanced Configuration

### Production Deployment

```bash
# .env for production
DB_HOST=your-production-db-host.com
DB_NAME=survivor_dw_prod
DB_USER=survivor_prod_user
DB_PASSWORD=secure_production_password
SURVIVOR_ENV=prod
GAMEBOT_DAG_SCHEDULE=0 4 * * 1

# Security settings
AIRFLOW__WEBSERVER__SECRET_KEY=your_secret_key
```

### Custom Database

```bash
# For external PostgreSQL
DB_HOST=custom-postgres-host
PORT=5432
DB_NAME=custom_database_name

# Docker Compose will automatically override for containers
# while external tools use your custom settings
```

### Environment Variables Reference

| Variable | Purpose | Example | Context Override |
|----------|---------|---------|------------------|
| `DB_HOST` | Database hostname | `localhost` | `warehouse-db` in containers |
| `PORT` | Database port | `5433` | `5432` in containers |
| `DB_NAME` | Database name | `survivor_dw_dev` | No override |
| `DB_USER` | Database username | `survivor_dev` | No override |
| `DB_PASSWORD` | Database password | `your_password` | No override |
| `AIRFLOW_PORT` | Web interface port | `8080` | No override |
| `GAMEBOT_TARGET_LAYER` | Pipeline depth | `gold` | No override |
| `GAMEBOT_DAG_SCHEDULE` | Execution schedule | `0 4 * * 1` | No override |

## Troubleshooting

### Connection Issues

**Problem**: Can't connect to database
```bash
# Check configuration loading
docker compose exec airflow-worker env | grep DB_

# Verify database is running
docker compose ps warehouse-db

# Test direct connection
docker compose exec warehouse-db pg_isready -U survivor_dev
```

**Problem**: Wrong database host
```bash
# For local development, ensure .env has:
DB_HOST=localhost
PORT=5433

# For container debugging, override is automatic
# Verify: docker compose exec airflow-worker env | grep DB_HOST
# Should show: DB_HOST=warehouse-db
```

### Configuration Validation

```bash
# Verify environment loading
make ps                                    # All services should be running
make logs | head -20                       # Check startup logs

# Test database connectivity
docker compose exec warehouse-db psql -U survivor_dev survivor_dw_dev -c "SELECT 1;"

# Verify Airflow configuration
# Browser: http://localhost:8080 (should load Airflow UI)
```

### Best Practices

1. **Version Control**: Never commit `.env` with real credentials
2. **Templates**: Keep `.env.example` updated with all required variables
3. **Security**: Use strong passwords and change defaults for production
4. **Documentation**: Document any custom variables in `.env.example`
5. **Testing**: Always test configuration changes with `make fresh`

The simplified environment system eliminates configuration complexity while providing robust, production-ready operation across all deployment scenarios.
