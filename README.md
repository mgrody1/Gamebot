# Gamebot

<p align="center">
  <img src="https://i.redd.it/icb7a6pmyf0c1.jpg" alt="Dabu Doodles Survivor art" width="480">
</p>

> Art by [Dabu Doodles (Erik Reichenbach)](https://dabudoodles.com/)

## What is a Gamebot in the CBS Reality Competition Show Survivor?

[*Survivor Term Glossary (search for Gamebot)*](https://insidesurvivor.com/the-ultimate-survivor-glossary-980)

[*What is a Gamebot in Survivor? Thread*](https://www.reddit.com/r/survivor/comments/37hu6i/what_is_a_gamebot/)

## What is a Gamebot Outside of the Game? **This Repository!**:

Gamebot is a lakehouse-style Survivor analytics stack that ingests (most of) the [`survivoR`](https://github.com/doehm/survivoR) datasets, curates bronze → silver → gold tables with Airflow + dbt, and ships a zero-install SQLite snapshot for notebooks. It is designed to empower data analysts/scientists/engineers/developers who are comfortable in python and/or SQL to get started right away with their Survivor research and analyses, with a particular focus on **machine learning and predictive modeling** for winner prediction research.

The architecture follows a **medallion lakehouse pattern** optimized for ML feature engineering:
- **Bronze**: Raw survivoR dataset tables with ingestion metadata
- **Silver**: ML-focused feature engineering tables organized by strategic categories (challenges, advantages, voting, social dynamics, edit analysis, etc.)
- **Gold**: Two ML-ready feature tables for different modeling approaches (non-edit gameplay vs hybrid gameplay+edit features)

For a detailed reference of the upstream schema we mirror, see `survivoR.pdf` in the project root (a copy of the official survivoR R documentation).

Huge thanks to [Daniel Oehm](https://gradientdescending.com/) and the `survivoR` community; if you haven’t already, please check [`survivoR`](https://github.com/doehm/survivoR) out!

### What you can explore
- [Check out these Survivor analyses with the survivoR dataset](https://gradientdescending.com/category/survivor/) as examples of the types of analyses you can now more easily accomplish in python and SQL with Gamebot.

## Table of Contents

- [Try It in 5 Minutes](#try-it-in-5-minutes)
- [Architecture & Operations](#architecture--operations-short)
- [Operations & Scheduling](#operations--scheduling)
- [Gamebot Lite (analyst package)](#gamebot-lite-analyst-package)
- [Releases](#releases)
- [Delivery Modes](#delivery-modes)
- [Automation & CI](#automation--ci)
- [Repository Map](#repository-map)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Need to dive deeper?](#need-to-dive-deeper)

## 🚦 Quickstart Cheat Sheet for CLI & Makefile Commands

New to Gamebot? Start here:
- [CLI & Makefile Cheat Sheet](docs/cli_cheatsheet.md) — essential commands, environments, and troubleshooting tips for running, debugging, and managing the stack.

## Try It in 5 Minutes
Fire up a Jupyter notebook (or VS Code / JupyterLab cell) and run the following cell to install the analyst package.

```python
# Default install (recommended)
%pip install --upgrade gamebot-lite

# If you want DuckDB helpers (for `duckdb_query`) and don't already have
# DuckDB installed, swap the line above for the optional extra:
# %pip install --upgrade "gamebot-lite[duckdb]"
```

Quick examples (two ways to explore):

- DuckDB SQL (requires DuckDB to be installed):

```python
from gamebot_lite import duckdb_query

duckdb_query("""
SELECT season_name, castaway, target_winner, challenges_won, vote_accuracy_rate
FROM gold.ml_features_non_edit
WHERE target_winner = 1
ORDER BY challenges_won DESC
LIMIT 5
""")
```

- Pandas (works with the default install and uses the packaged SQLite snapshot):

```python
from gamebot_lite import load_table

df = load_table("castaway_details", layer="bronze")
print(df.head())
```

Note: if you installed without DuckDB and run the DuckDB example you'll get an ImportError; either install the `duckdb` extra or use the Pandas `load_table` example above.

## Architecture & Operations (short)

Deployment, developer, and operational runbooks live in the [docs/](docs/) folder. For full details, see:

- [Architecture Overview](docs/architecture_overview.md) — deployment and developer walkthroughs (Warehouse vs Studio).
- [Operations Guide](docs/operations_guide.md) — environment profiles, ETL orchestration, scheduling, releases, and troubleshooting.
- [Gamebot Lite documentation](docs/gamebot_lite.md) — analyst table dictionary, DuckDB examples, and packaging notes.
- [Warehouse schema guide](docs/gamebot_warehouse_schema_guide.md) — narrative walkthrough of silver ML feature categories and gold model-ready tables.
- [Warehouse cheatsheet & IDE tips](docs/gamebot_warehouse_cheatsheet.md) — quick join keys and external SQL IDE tips.
- [GitHub Actions quickstart](docs/github_actions_quickstart.md) — CI and release workflow walkthroughs.
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

## Operations & Scheduling
Gamebot runs on a weekly Airflow cadence (`GAMEBOT_DAG_SCHEDULE`, default early Monday UTC). The API rate limiting settings (`AIRFLOW__API_RATELIMIT__*`) keep the Airflow REST endpoint safe when multiple notebooks or automations connect—raise them only if you understand the trade-offs.

Need a refresher on how Airflow's Celery executor wiring works? SparkCodeHub's [Airflow + Celery executor tutorial](https://www.sparkcodehub.com/airflow/integrations/celery-executor) walks through the moving parts and common gotchas.

The DAG `airflow/dags/survivor_medallion_dag.py` automates the workflow (bronze → silver → gold) on a weekly schedule.

> **Production guard:** when `SURVIVOR_ENV=prod`, all mutating scripts (Airflow loader, `export_sqlite`, preprocessing helpers) require the current git branch to be `main`. This prevents accidental prod runs from feature branches.

### Start services

```bash
make up
# Airflow UI at http://localhost:${AIRFLOW_PORT:-8080} (credentials come from `.env`—change the defaults before production)
```

### Run the DAG

* UI: Unpause and trigger `survivor_medallion_dag`.
* CLI:

  ```bash
  cd airflow
  docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_dag
  ```

---

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

## Repository Map

```
Database/
  ├── create_tables.sql                # Bronze + silver schema DDL
  ├── load_survivor_data.py            # Bronze ingestion entrypoint
  └── sql/
      └── legacy/                      # Historical refresh scripts (dbt supersedes these)
          ├── refresh_silver_dimensions.sql
          ├── refresh_silver_facts.sql
          └── refresh_gold_features.sql
gamebot_core/
  ├── db_utils.py                      # Schema validation, upsert logic, drift notifications
  ├── data_freshness.py                # Identify upstream changes and persist metadata
  ├── github_data_loader.py            # pyreadr wrapper + HTTP caching
  ├── notifications.py                 # Optional issue + log helpers for schema drift
  ├── source_metadata.py               # Decide RDA vs JSON source & metadata
  └── validation.py                    # Lightweight dataframe validations
scripts/
  ├── build_airflow_conn.py            # Sync Airflow connection from .env
  ├── check_versions.py                # Pipfile / Dockerfile alignment check
  ├── check_survivor_updates.py        # Monitor upstream survivoR commits (RDA + JSON)
  ├── create_notebook.py               # Generate starter notebooks
  ├── export_sqlite.py                 # Export bronze/silver/gold → SQLite
  ├── smoke_gamebot_lite.py            # Ensure packaged SQLite snapshot matches catalog
  ├── show_last_run.py                 # Inspect latest artefact in run_logs/
  ├── preprocess/                      # Ad-hoc preprocessing scripts (legacy path)
  │   ├── preprocess_data.py
  │   └── preprocess_data_helper.py
  ├── tag_release.py                   # Create/push data or code release tags
  ├── setup_env.py                     # Create / switch .env and airflow/.env
  └── build_erd.py                     # Generate ERD (Graphviz)
examples/
  ├── example_analysis.py              # 2-minute Jupytext demo (bronze + gold query)
  └── streamlit_app.py                 # Minimal Streamlit viewer for the packaged SQLite
airflow/
  ├── Dockerfile                       # Custom Airflow image
  ├── dags/
  │   └── survivor_medallion_dag.py    # Medallion pipeline DAG
  └── docker-compose.yaml              # Local orchestration stack
dbt/
  ├── models/
  ├── tests/
  └── profiles.yml
docs/
  ├── erd/                             # Generated ERD assets
  └── gamebot_lite.md                  # Analyst documentation
run_logs/                              # Loader, validation, schema-drift, and Excel data-quality artefacts
monitoring/
  └── survivor_upstream_snapshot.json  # Last ingested survivoR commits (update via script)
gamebot_lite/                          # Lightweight SQLite package
.github/workflows/                     # CI + manual tagging automation
tests/
  └── test_gamebot_lite.py             # Pytest smoke tests for packaged data
Dockerfile                             # Base image used by make loader profile
Makefile
params.py
Pipfile / Pipfile.lock
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

* Data-quality workbook: each loader run writes `run_logs/validation/data_quality_<timestamp>.xlsx` with row counts, column types, PK/FK validations, and detailed remediation notes (duplicates removed, IDs backfilled, challenge fixes, etc.).
  - Dev Container/host: `make show-last-run ARGS="validation"` prints the newest file path (add `--tail` to preview the JSON summary).
  - Docker-only workflow: mount the log directory and set `GAMEBOT_RUN_LOG_DIR`, for example:

    ```bash
    docker compose run --rm \
      -e GAMEBOT_RUN_LOG_DIR=/workspace/run_logs \
      -v $(pwd)/run_logs:/workspace/run_logs \
      --profile loader survivor-loader
    ```
    (If you see a warning about Excel engines, install `openpyxl` in the runtime environment.)

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
