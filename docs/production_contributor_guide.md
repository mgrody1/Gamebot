# Production Runs for Contributors

This guide covers running production pipelines **from the repository** to create official gamebot-lite releases. This is for contributors (primarily maintainers) who need to produce production-grade SQLite exports for PyPI distribution.

> **Note**: This is different from the turnkey "Warehouse" deployment. Warehouse users don't need SQLite exports - they just want a PostgreSQL database. This guide is for creating the packaged SQLite snapshots distributed via PyPI.

> **Important - SQLite Database Tracking**: The `gamebot_lite/data/gamebot.sqlite` file is git-ignored by default to prevent dev databases from being committed. **On release branches only**, you must use `git add -f` to force-add the production SQLite database. This ensures only production data makes it into releases.

---

## Release Types

Gamebot has two types of releases:

| Release Type | Tag Format | Triggers | What's Released | PyPI Package |
|--------------|------------|----------|-----------------|--------------|
| **Data Release** | `data-YYYYMMDD` | New upstream data OR code changes affecting data | SQLite snapshot in `gamebot_lite/data/` | `gamebot-lite` (data only) |
| **Code Release** | `code-vX.Y.Z` | Feature additions, bug fixes, API changes | Python package with new functionality | `gamebot-lite` (code + data) |

### Release Relationship

- **Every code release MUST include a data release** - Even if no new upstream data, run the production pipeline to ensure the SQLite export reflects the latest code/schema
- **Data releases can be standalone** - When new upstream data is available but no code changes
- **Joint releases** - When both code and data change, create both tags from the same commit

---

## Production vs Development

The repository supports two environment modes controlled by `SURVIVOR_ENV` in your `.env`:

| Mode | SURVIVOR_ENV | SQLite Export | Use Case |
|------|--------------|---------------|----------|
| **Development** | `dev` | Enabled | Local testing, feature development |
| **Production** | `prod` | Enabled | Official gamebot-lite releases (contributors) |
| **Warehouse** | `prod` | **Disabled** | Turnkey deployment for teams (via GAMEBOT_CONTAINER_DEPLOYMENT=true) |

**Key Insight**: Contributors running from the repo always get SQLite export (for gamebot-lite releases). Only the turnkey `deploy/docker-compose.yml` sets `GAMEBOT_CONTAINER_DEPLOYMENT=true` to skip it.

---

## Git Workflow for Releases

### Recommended: Trunk-Based Development with Release Branches

**Branch Strategy**:
- `main` - Production-ready code, always deployable
- `feature/*` - Feature development branches (short-lived)
- `data-release/*` - Data release preparation branches (ephemeral)

**Why this works for Gamebot**:
- Simple, industry-standard approach
- `main` is always in a releasable state
- Feature branches are small and merged quickly
- Data releases are prepared on short-lived branches, merged after validation

### Workflow: Data-Only Release

**When**: New upstream survivoR data available, no code changes needed

**Starting Point**: Always start from latest `main`

```bash
# 1. Ensure you're on latest main
git checkout main
git pull origin main

# 2. Verify your environment is production-ready
# Edit .env: SURVIVOR_ENV=prod, DB_NAME=survivor_dw_prod

# 3. Start Airflow stack (if not already running)
make up

# 4. Trigger production pipeline via Airflow UI
# Navigate to http://localhost:8080
# Trigger 'survivor_medallion_pipeline' DAG
# Wait for completion (~2 minutes)

# 5. Verify SQLite export
ls -lh gamebot_lite/data/survivor_data.db
cat gamebot_lite/data/manifest.json
pipenv run python scripts/smoke_gamebot_lite.py

# 6. Create data release branch
git checkout -b data-release/$(date +%Y%m%d)

# 7. Add the SQLite database and manifest (force-add to override .gitignore)
git add -f gamebot_lite/data/gamebot.sqlite gamebot_lite/data/manifest.json

# 8. Commit with manifest metadata
git commit -m "data: Release $(date +%Y%m%d) - survivor data snapshot

Ingestion run: $(jq -r '.ingestion_run_id' gamebot_lite/data/manifest.json)
Tables exported: $(jq -r '.tables | length' gamebot_lite/data/manifest.json)
Upstream source: survivoR $(jq -r '.upstream_version // "latest"' gamebot_lite/data/manifest.json)
"

# 9. Push and create PR
git push -u origin data-release/$(date +%Y%m%d)
# Create PR on GitHub targeting main

# 10. PR Review Checklist
# - Verify validation reports in run_logs/validation/
# - Confirm smoke tests passed
# - Review manifest.json for expected metadata
# - Check for any schema changes in dbt tests

# 11. After PR approval, merge to main
# Use "Squash and merge" or "Create a merge commit" (your preference)

# 12. Tag the release
git checkout main
git pull origin main
git tag -a data-$(date +%Y%m%d) -m "Data release $(date +%Y%m%d)

$(jq -r '.tables | keys | join(", ")' gamebot_lite/data/manifest.json)
"
git push origin data-$(date +%Y%m%d)

# 13. Publish to PyPI (automated via GitHub Actions)
# GitHub Actions will automatically:
#   1. Detect the new data-YYYYMMDD tag
#   2. Build the package: python -m build
#   3. Publish to TestPyPI first
#   4. Publish to production PyPI after manual approval
#
# See: .github/workflows/publish-pypi.yml for automation details

# 14. Clean up local branch
git branch -d data-release/$(date +%Y%m%d)
```

### Workflow: Code Release (with mandatory data release)

**When**: Feature additions, bug fixes, schema changes, API updates

**Starting Point**: Feature branch merged to `main`, now need to cut release

```bash
# Assume feature is already merged to main via PR

# 1. Checkout main and ensure it's latest
git checkout main
git pull origin main

# 2. Run production pipeline to generate data with new code
# (Same steps as data-only release #2-5 above)
make up  # If stack not running
# Trigger pipeline in Airflow UI
# Verify SQLite export

# 3. Create combined release branch
VERSION="1.2.0"  # Update semantic version
git checkout -b release/v${VERSION}

# 4. Add SQLite database and manifest (force-add to override .gitignore)
git add -f gamebot_lite/data/gamebot.sqlite gamebot_lite/data/manifest.json

# 5. Update version in pyproject.toml
# Edit pyproject.toml: version = "1.2.0"
git add pyproject.toml

# 6. Commit with detailed release notes
git commit -m "release: v${VERSION} - <feature summary>

Code changes:
- <list major changes>

Data changes:
- Ingestion run: $(jq -r '.ingestion_run_id' gamebot_lite/data/manifest.json)
- Schema updates: <if applicable>
"

# 7. Push and create PR
git push -u origin release/v${VERSION}
# Create PR targeting main

# 8. PR Review Checklist (see CONTRIBUTING.md)
# - Run full test suite
# - Verify smoke tests
# - Check validation reports
# - Review changelog
# - Test package build

# 9. After approval, merge to main

# 10. Create BOTH tags from the merged main
git checkout main
git pull origin main

# Tag code release
git tag -a code-v${VERSION} -m "Code release v${VERSION}: <summary>"
git push origin code-v${VERSION}

# Tag data release
git tag -a data-$(date +%Y%m%d) -m "Data release $(date +%Y%m%d) - matches code v${VERSION}"
git push origin data-$(date +%Y%m%d)

# 11. Publish to PyPI (automated via GitHub Actions)
# GitHub Actions will automatically:
#   1. Detect the new code-vX.Y.Z tag
#   2. Build the package: python -m build
#   3. Publish to TestPyPI first
#   4. Publish to production PyPI after manual approval
#
# See: .github/workflows/publish-pypi.yml for automation details

# 12. Create GitHub Release with notes
# Use GitHub UI to create release from code-v${VERSION} tag
# Include: changelog, breaking changes, migration guide if needed

# 13. Clean up
git branch -d release/v${VERSION}
```

### Workflow: Feature Development

**Standard feature branch workflow** - this feeds into the release workflows above

```bash
# 1. Start from latest main
git checkout main
git pull origin main

# 2. Create feature branch
git checkout -b feature/add-confessional-sentiment

# 3. Develop with SURVIVOR_ENV=dev
# Make changes, test locally with make up

# 4. Commit incrementally
git add <files>
git commit -m "feat: add confessional sentiment analysis to silver layer"

# 5. Keep branch updated with main
git fetch origin
git rebase origin/main

# 6. Push and create PR
git push -u origin feature/add-confessional-sentiment
# Create PR on GitHub

# 7. PR Review (see PR checklist in CONTRIBUTING.md)
# - CI tests pass
# - Validation reports committed to run_logs/validation/
# - Smoke tests pass
# - Documentation updated

# 8. After approval, squash-merge to main
# Use GitHub "Squash and merge"

# 9. Clean up
git checkout main
git pull origin main
git branch -d feature/add-confessional-sentiment
git push origin --delete feature/add-confessional-sentiment

# 10. Later: Cut a code release following "Code Release" workflow above
```

---

## Quick Start: Production Run for gamebot-lite Release

### 1. Configure Production Environment

```bash
# Edit your .env file
cd /path/to/Gamebot

# Update these values:
# SURVIVOR_ENV=prod              # Production mode
# DB_NAME=survivor_dw_prod       # Production database name
# GAMEBOT_TARGET_LAYER=gold      # Full pipeline
```

**Complete production .env example**:
```bash
# Database configuration (production)
DB_HOST=localhost
DB_NAME=survivor_dw_prod
DB_USER=survivor_prod
DB_PASSWORD=your_secure_prod_password
DB_PORT=5433
DB_HOST_PORT=5433

# Environment marker
SURVIVOR_ENV=prod               # Production mode (SQLite export enabled by default)

# Application configuration
GAMEBOT_TARGET_LAYER=gold
GAMEBOT_DAG_SCHEDULE=0 4 * * 1
AIRFLOW_PORT=8080

# Airflow configuration
AIRFLOW__API_RATELIMIT__STORAGE=redis://redis:6379/1
AIRFLOW__API_RATELIMIT__ENABLED=True

# External integrations (for automated releases - leave empty for manual)
GITHUB_TOKEN=

# Connection strings
LOCAL_POSTGRES_CONN=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}
AIRFLOW_CONN_SURVIVOR_POSTGRES=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}
```

### 2. Run Production Pipeline

**Option A: Via Airflow UI** (Recommended)
```bash
# If stack is not running, start it (preserves existing data)
make up

# 1. Navigate to http://localhost:8080
# 2. Unpause 'survivor_medallion_pipeline' DAG
# 3. Trigger run manually
# 4. Monitor execution
```

**Option B: Via CLI**
```bash
# If stack is not running, start it (preserves existing data)
make up

# Trigger production run
docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline

# Monitor logs
make logs
```

> **Important**: Use `make up` (not `make fresh`) to preserve your production database. `make fresh` will **delete all data** and rebuild from scratch.

### 3. Verify SQLite Export

After successful pipeline completion, verify the SQLite export was created:

```bash
# Check for SQLite export
ls -lh gamebot_lite/data/*.db

# Check manifest
cat gamebot_lite/data/manifest.json

# Run smoke test
pipenv run python scripts/smoke_gamebot_lite.py
```

**Expected output location**:
```
gamebot_lite/data/
├── survivor_data.db           # Packaged SQLite database
├── manifest.json              # Export metadata
└── README.md                  # Data documentation
```

### 4. Manual Release Process (Current Approach)

Once you have a validated production SQLite export:

```bash
# 1. Verify smoke tests pass
pipenv run python scripts/smoke_gamebot_lite.py

# 2. Create git branch for data release
git checkout -b data-release-$(date +%Y%m%d)

# 3. Add the export artifacts
git add gamebot_lite/data/

# 4. Commit with release metadata
git commit -m "data: Release $(date +%Y%m%d) - survivor data snapshot

Ingestion run: <run_id from manifest>
Tables exported: <count from manifest>
Data checksum: <checksum from manifest>
"

# 5. Push branch
git push origin data-release-$(date +%Y%m%d)

# 6. Create PR on GitHub and pass to reviewer for QA. The reviewer will:

    # - Review validation reports in run_logs/validation/
    # - Verify smoke tests passed
    # - Pass PR back to developer to complete process

    The developer will then:
    # - Merge to main

# 7. Tag release
git checkout main
git pull
git tag -a data-$(date +%Y%m%d) -m "Data release $(date +%Y%m%d)"
git push origin data-$(date +%Y%m%d)

# 8. Publish to PyPI (manual for now)
# Build package
pipenv run python -m build

# Upload to PyPI
pipenv run twine upload dist/gamebot_lite-<version>*

# Or test PyPI first
pipenv run twine upload --repository testpypi dist/gamebot_lite-<version>*
```

---

## Development vs Production Database Separation

### 1: Separate Database Names

Use different database names in the same PostgreSQL instance:

```bash
# Development .env
DB_NAME=survivor_dw_dev
SURVIVOR_ENV=dev

# Production .env (for releases)
DB_NAME=survivor_dw_prod
SURVIVOR_ENV=prod
```

**Switching between dev and prod**:
```bash
# Edit .env and change:
# SURVIVOR_ENV=dev  →  SURVIVOR_ENV=prod
# DB_NAME=survivor_dw_dev  →  DB_NAME=survivor_dw_prod

# Restart stack
make down
make up
```

### 2: Git Branch Workflow

Keep development on feature branches and production on `main`:

**WARNING**: Make sure .env has SURVIVOR_ENV=dev before running `make fresh`, otherwise you risk deleting the production db.

```bash
# Development work
git checkout <feature_branch>
# .env has SURVIVOR_ENV=dev
make fresh

# Production release
git checkout main
# .env has SURVIVOR_ENV=prod
make up  # Use make up to preserve data
# Run pipeline, export SQLite, create release
```

---

## Validation Reports

Each production run generates comprehensive validation reports that are **committed to the repository** for review:

```bash
# View latest validation run directory
ls -lt run_logs/validation/ | head -5

# Each run creates a directory with multiple files:
# run_logs/validation/Run 0030 - <RUN_ID> Validation Files/
#   ├── data_quality_<run_id>_<timestamp>.xlsx  (Excel summary report)
#   ├── validation_<table>_<timestamp>.json     (Per-table JSON details)
#   └── .run_id                                  (Run metadata)

# View the Excel report from latest run
LATEST_DIR=$(ls -dt run_logs/validation/Run* | head -1)
ls -lh "$LATEST_DIR"/*.xlsx
```

**Validation report contents**:
- Row counts per table
- Data quality metrics
- Schema validation
- Duplicate analysis
- Referential integrity checks

**Important**: Validation reports are committed to git for PR reviews, not .gitignored. Include these reports when creating data release or code release PRs.

---

## Troubleshooting Production Runs

### Issue: SQLite export not created

**Diagnosis**:
```bash
# Check GAMEBOT_CONTAINER_DEPLOYMENT flag in docker-compose
docker compose exec airflow-worker env | grep GAMEBOT_CONTAINER_DEPLOYMENT
# Should NOT be set (or show empty) for contributor runs from repo

# Check DAG logs
make logs | grep -i sqlite
```

**Solution**: GAMEBOT_CONTAINER_DEPLOYMENT is NOT an environment variable in `.env` - it's only set in `deploy/docker-compose.yml` for the turnkey warehouse deployment. If you're running from the repository with `make up`, SQLite export should run automatically. If it's not running, check the DAG logs for ShortCircuitOperator output.

### Issue: Using development database in production

**Diagnosis**:
```bash
# Check active database
docker compose exec airflow-worker env | grep DB_NAME
```

**Solution**: Update `.env` with `DB_NAME=survivor_dw_prod` and restart.

### Issue: Port conflicts between dev and prod

**Solution**: Use separate database names instead of separate ports. Both can run on same PostgreSQL instance at `localhost:5433`.

---

## Future: Automated Release Workflow

Once manual releases are working reliably, we can implement automation:

1. **Trigger**: Production pipeline completes successfully
2. **Export**: SQLite package created with manifest
3. **Smoke Test**: Automated validation
4. **Git Branch**: Create `data-release/YYYYMMDD-<sha>`
5. **PR**: Automated PR with validation reports
6. **GitHub Actions**: Verify, tag, publish to PyPI
7. **Merge**: Automated merge after checks pass

**See**: `docs/github_actions_quickstart.md` for automation design (currently on hold)

---

## Best Practices

1. **Always run smoke tests** before creating releases
2. **Keep production database separate** from development (`survivor_dw_prod` vs `survivor_dw_dev`)
3. **Review validation reports** before tagging releases
4. **Use descriptive commit messages** with manifest metadata
5. **Test on TestPyPI first** before publishing to production PyPI
6. **Document schema changes** in release notes and docs
7. **Every code release needs a data release** - Run prod pipeline even if no new upstream data
8. **Cut releases FROM main** - All releases (data and code) are tagged from `main` after merging; do feature development on branches, then merge to `main` before cutting releases
9. **Attach validation reports to PRs** - Commit the run_logs/ artifacts for review (not .gitignored)

---

## Git Workflow Decision Matrix

| Scenario | Starting Branch | Release Branch | Tags Created | PyPI Publish |
|----------|----------------|----------------|--------------|--------------|
| **New upstream data only** | `main` | `data-release/YYYYMMDD` | `data-YYYYMMDD` | Yes (data update) |
| **Code changes merged** | `main` (after feature PR) | `release/vX.Y.Z` | `code-vX.Y.Z` + `data-YYYYMMDD` | Yes (full release) |
| **Hotfix** | `main` | `hotfix/vX.Y.Z` | `code-vX.Y.Z` + `data-YYYYMMDD` | Yes (patch release) |
| **Schema change** | `main` (after feature PR) | `release/vX.Y.Z` | `code-vX.Y.Z` + `data-YYYYMMDD` | Yes (may be breaking) |

---

## Quick Reference Commands

```bash
# Switch to production mode
# Edit .env: SURVIVOR_ENV=prod (no other changes needed)

# Run production pipeline (if stack not running)
make up
# Trigger in Airflow UI

# Verify export
ls -lh gamebot_lite/data/
pipenv run python scripts/smoke_gamebot_lite.py

# Manual release steps
git checkout -b data-release-$(date +%Y%m%d)
git add gamebot_lite/data/
git commit -m "data: Release $(date +%Y%m%d)"
git push
# Create PR, merge, tag

# Publish to PyPI
pipenv run python -m build
pipenv run twine upload dist/gamebot_lite-*
```
