# Operations Guide

## Environment Profiles (dev vs prod)

> Quick start: copy a template and materialise `.env`:
> ```bash
> cp env/.env.dev.example env/.env.dev
> pipenv run python scripts/setup_env.py dev --from-template
> ```
> **Always change default credentials (`AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD`) before running in production.**

* `SURVIVOR_ENV` controls the environment (`dev` by default).
* `env/.env.dev` and `env/.env.prod` hold the canonical profiles. Run `scripts/setup_env.py` whenever you want to switch between environments (or edit the profile files manually if you prefer); the script projects the selected profile into the root `.env`.
  * Usage:

    ```bash
    # Activate the dev profile using the checked-in env file (preferred)
    pipenv run python scripts/setup_env.py dev

    # Rehydrate from the template if env/.env.dev is missing or you want to reset it
    pipenv run python scripts/setup_env.py dev --from-template

    # Same options apply for prod
    pipenv run python scripts/setup_env.py prod
    ```
  * On first run the script seeds `env/.env.<env>` from `env/.env.<env>.example`. Edit the profile files (`env/.env.dev`, `env/.env.prod`) to change environment-specific defaults (database host/name, schedule, etc.).
  * After projecting a profile, the script writes the root `.env`, syncs `airflow/.env`, and keeps the Airflow connection JSON up to date.
  * Keys that only live in the current `.env` (for example private API tokens) are preserved when switching.
  * Set `GAMEBOT_DAG_SCHEDULE` in the profile or root `.env` to control the Airflow schedule before starting the stack.
  * Airflow rate limiting defaults (`AIRFLOW__API_RATELIMIT__STORAGE=redis://redis:6379/1`) are injected automatically.
  * `airflow/.env` is synced for you—no need to run `make sync-env` separately.
* Prod runs (typically via Docker + Airflow) target the containerised Postgres service (`warehouse-db`) and enforce running from the `main` branch.
* Control pipeline depth via `GAMEBOT_TARGET_LAYER` (`bronze`, `silver`, or `gold`; defaults to `gold`).
* If you ever need to refresh the Airflow connection definition manually:

  ```bash
  pipenv run python scripts/build_airflow_conn.py --write-airflow
  ```

### `.env` keys (cheat sheet)

| Key | Description |
| --- | --- |
| `DB_HOST` | Hostname of the warehouse Postgres instance (`warehouse-db` when using Docker). |
| `DB_NAME` | Database name for the warehouse schema. |
| `DB_USER` / `DB_PASSWORD` | Credentials used by the loader, dbt, and Airflow connections. |
| `PORT` | Postgres port (leave as `5432` unless your DB listens elsewhere). |
| `SURVIVOR_ENV` | Logical environment (`dev` or `prod`). Influences Git safety checks and optional truncation rules. |
| `GAMEBOT_TARGET_LAYER` | Upper pipeline bound (`bronze`, `silver`, or `gold`). Controls how far the DAG runs. |
| `GAMEBOT_DAG_SCHEDULE` | Cron schedule for the Airflow DAG (default `0 4 * * 1`). |
| `AIRFLOW_PORT` | Host port exposed by the Airflow webserver (default `8080`). |
| `AIRFLOW__API_RATELIMIT__STORAGE` | Flask-Limiter backend for the Airflow API (defaults to shared Redis). |
| `AIRFLOW__API_RATELIMIT__ENABLED` | Toggle for API rate limiting (keep `True` unless you know you need to disable it). |

Any additional service-specific overrides can be added to `.env`; they will flow through to `airflow/.env` via `scripts/setup_env.py`.

### Workflow tips

* Run `scripts/setup_env.py` **inside the Dev Container** as your first step (or on the host only after Pipenv is installed). It writes `.env`, syncs `airflow/.env`, and keeps Airflow connections aligned.
* After switching environments (e.g., `dev` → `prod`), restart the Docker stack from the host (`make down && make up`) so containers pick up the new values.
* Need a brand-new warehouse database? Update `.env` first, then remove the Postgres volume before restarting:

  ```bash
  make down
  make clean    # or: cd airflow && docker compose down -v
  make up
  ```

  Without wiping the volume, Postgres keeps the existing database/user even if `.env` changes.
* The Dev Container’s Pipenv virtualenv mirrors the runtime dependencies; use the container for Python/dbt commands and the host terminal only for Docker/Make invocations.

---

## ETL Architecture

### Bronze – load `survivoR` data

```bash
pipenv run python -m Database.load_survivor_data
```

What happens:

1. The loader checks both the `.rda` exports and the JSON mirrors under `dev/json/`, downloads whichever changed most recently (cached in `data_cache/`), and falls back to the `.rda` when the timestamps tie.
2. `Database/create_tables.sql` is applied on first run to create schemas (the loader calls this automatically; no manual step needed).
3. Each loader run records metadata in `bronze.ingestion_runs` and associates `ingest_run_id` with bronze tables. `bronze.dataset_versions` captures the content fingerprint, upstream commit, and whether the data came from the `.rda` or JSON export. Data is merged with upsert logic (no truncation in prod). Lightweight dataframe validations run on key bronze tables (results land in `run_logs/validation/`). Logs list inserted/updated keys.
4. Vote-history rows that reference missing `challenge_id` values are auto-remediated: first via stage-of-game (`sog_id`) alignment with `bronze.challenge_results`, with the small fallback map in `gamebot_core/db_utils.py` covering historical edge cases. Every fix is logged (sampled) so you can notify the survivoR maintainers.
5. Need to sanity-check a particular column definition? `survivoR.pdf` in the repo root is the upstream R documentation we mirror; search it for the dataset name to see the canonical description.

Tip: capture loader output to `run_logs/<context>_<timestamp>.log` for PRs or incident reviews. Zip the file (e.g., `zip run_logs/dev_branch_20250317.zip run_logs/dev_branch_20250317.log`) and attach the archive to your pull request or share a public link so reviewers can download the clean run. Schema drift warnings are also appended to `run_logs/notifications/schema_drift.log` so you can quickly see when survivoR introduces new columns or types. If survivoR publishes entirely new tables, the loader will flag them in the same log (and, when `GITHUB_REPO`/`GITHUB_TOKEN` are set, open an issue automatically), but they will not load automatically—you decide when to extend `Database/db_run_config.json` and the bronze DDL. Rerun when the upstream dataset changes or after a new episode.

#### Quick log access

* `pipenv run python scripts/show_last_run.py --tail` — show the newest artefact (validation report, schema drift, etc.).
* `make show-last-run ARGS="--tail --category validation"` — same command via Make; handy inside the Dev Container.
* Docker-only workflow? `docker compose exec devshell make show-last-run ARGS="--tail"` provides the same experience.
* Need logs elsewhere? Set `GAMEBOT_RUN_LOG_DIR=/path/on/host` before running the stack to relocate the artefacts (helpful when sharing a Docker volume).
* Each run also emits an Excel workbook at `run_logs/validation/data_quality_<timestamp>.xlsx`. Recent changes expanded it to include:
  * Per-dataset tabs with rule outcomes, uniqueness/FK checks, remediation events, and “Reference Records” tables showing the raw rows used to backfill data (e.g., journeys fuzzy matches).
  * A version-season coverage section that highlights which seasons were missing or unexpectedly present.
  * A **Metadata Summary** tab that compares survivoR’s upstream dataset catalogue against the tables we load and tracks schema drift (unexpected/missing columns). Identity/housekeeping columns (`*_id`, `ingest_run_id`, `ingested_at`) are treated as auto-managed so they don’t trigger false positives. Set `GITHUB_TOKEN` if you want the upstream comparison to work without hitting rate limits.
* To produce the workbook while running the loader in a disposable container, mount the log directory:

  ```bash
  docker compose run --rm \
    -e GAMEBOT_RUN_LOG_DIR=/workspace/run_logs \
    -v $(pwd)/run_logs:/workspace/run_logs \
    --profile loader survivor-loader
  ```
  Install `openpyxl` in your environment if the workbook export logs a warning about missing engines.
* Uniqueness guardrails: every dataset that declares a unique key in `Database/table_config.json` stops the load when duplicates appear — except `bronze.challenge_summary`. That upstream helper intentionally publishes multiple category rows per castaway/challenge, so the loader logs the overlap (and the Excel report calls it out) but continues. All other tables require manual intervention when a uniqueness breach is detected.

Only 13 survivoR tables ship by default (`Database/db_run_config.json` lists the current set). When upstream adds more tables or reshapes a schema, the drift log + optional GitHub issue tells you exactly what changed so you can opt-in intentionally.

> Optional automation: set `GITHUB_REPO` (e.g., `user/project`) and `GITHUB_TOKEN` in your `.env` to have schema drift warnings automatically open a GitHub issue for follow-up.

- GitHub metadata lookups are optional. Without `GITHUB_TOKEN`, the loader skips commit metadata to avoid rate limits; set a personal access token if you want commit hashes/timestamps recorded for every dataset. This token is only for dataset metadata — release automation continues to use the separate tokens referenced in the GitHub Actions guide (e.g., `AIRFLOW_GITHUB_TOKEN`).

---

### Silver – ML feature engineering tables

dbt models in `dbt/models/silver/` transform bronze into strategic feature categories for machine learning analysis. The silver layer creates 8 feature tables organized by gameplay dimensions: demographics, challenges, advantages, voting, social dynamics, edit analysis, jury relationships, and season context.

```bash
pipenv run dbt deps --project-dir dbt --profiles-dir dbt
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
```

Key silver tables:
* `silver.castaway_profile` – Demographics and background features
* `silver.challenge_performance` – Physical and mental challenge data
* `silver.advantage_strategy` – Strategic advantage gameplay
* `silver.vote_dynamics` – Voting behavior and alliances
* `silver.social_positioning` – Social dynamics and tribe composition
* `silver.edit_features` – Production narrative and screen time
* `silver.jury_analysis` – Endgame relationships and jury votes
* `silver.season_context` – Season format and meta-game features

---

### Gold – ML-ready feature aggregations

```bash
pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
```

The gold layer provides two ML-ready feature tables for different modeling approaches:
* `gold.ml_features_non_edit` – Pure gameplay features for testing if winners can be predicted without edit data
* `gold.ml_features_hybrid` – Combined gameplay and edit features for comprehensive winner prediction models

Each table aggregates features at the castaway × season level (3,133 rows) with target variables for machine learning training. Gold tables are rebuilt after silver completes successfully, ensuring downstream ML models always use consistent, up-to-date features.

---

### Explore with external SQL tools

The Postgres service runs in Docker but binds to the host, so the connection works from the host OS and from within the Dev Container (use host networking). The VS Code Dev Container attaches to the Compose-managed `devshell` service, so it automatically shares the same Docker network as Airflow/Postgres—no manual network juggling required. Tools like DBeaver can auto-generate ERDs once connected, which is often clearer than the static PNG produced by `scripts/build_erd.py`. If you’re on Gamebot Studio, you can also query the same database directly from the repo’s notebooks using the bundled Pipenv environment. Pick whichever client fits your workflow.

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
   pipenv run python -m Database.load_survivor_data
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver
   pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold
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
- See [docs/github_actions_quickstart.md](github_actions_quickstart.md) for a walkthrough of these workflows.

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
  ```

* Scheduler warnings about Flask-Limiter’s in-memory backend are safe for dev. Production configurations should keep the Redis-backed rate limiting enabled (handled automatically by `scripts/setup_env.py`).
