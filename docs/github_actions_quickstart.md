# GitHub Actions Quickstart

This repo ships with a lightweight CI stack so contributors can verify changes and publish release tags without leaving GitHub. If you are new to Actions, here’s everything you need for Gamebot.

## Workflows in this repo

| Workflow | When it runs | What it does |
| --- | --- | --- |
| `ci.yml` | Every push and pull request | Installs the Pipenv environment, runs `pre-commit`, executes the pytest smoke tests in `tests/`, and performs a `compileall` sanity check on `gamebot_core/`, `scripts/`, and `Database/`. |
| `manual-tag.yml` | Manually via the Actions tab | Wraps `python scripts/tag_release.py` so you can cut `data-YYYYMMDD` or `code-vX.Y.Z` tags straight from GitHub. |
| `upstream-survivor-monitor.yml` | Scheduled daily + on demand | Watches the upstream `survivoR` repo for new `.rda`/JSON data and opens/updates an issue if drift is detected. |

## Running the same checks locally

```bash
pipenv install --dev
pipenv run pre-commit run --all-files
pipenv run pytest
pipenv run python -m compileall gamebot_core scripts Database
```

If those commands pass locally, the `ci` workflow should stay green.

## Triggering the Manual Release Tag workflow

1. Open the **Actions** tab in GitHub.
2. Select **Manual Release Tag**.
3. Click **Run workflow** and choose:
   - Release type (`data` or `code`).
   - For data releases: the target date (leave blank for today UTC) and whether to push the tag.
   - For code releases: the semantic version (e.g., `v1.2.3`).
4. Submit. The workflow runs `scripts/tag_release.py` with the inputs you provided and pushes the tag if requested.

> Tip: the workflow uses the same script you can run locally (`python scripts/tag_release.py ...`). Use whichever path matches your release process.

## Extending automation

If you add new workflows, place them under `.github/workflows/` and document how to run them here or in the README automation section.

## Automated data release (Airflow -> Actions)

This repository now supports an automated data-release flow designed for production runs. The high-level pattern is:

- Airflow (production) runs the ETL and exports a packaged SQLite snapshot with `scripts/export_sqlite.py --package`.
- Airflow then runs `scripts/smoke_gamebot_lite.py` to validate the export. If the smoke test passes, Airflow creates a branch `data-release/<date>-<sha>`, opens a pull request, and dispatches a `repository_dispatch` event (`event_type: data-release`) to GitHub.
- A GitHub Actions workflow (`.github/workflows/data-release.yml`) responds to the dispatch, verifies the snapshot, tags the release (`data-YYYYMMDD`), and merges the PR.

Credentials and secrets
- Do NOT store secrets in git. Recommended locations:
   - Airflow: store the GitHub token as an Airflow Variable or Connection (recommended), or as an environment variable on the worker (e.g., `AIRFLOW_GITHUB_TOKEN`). This token is used by `scripts/trigger_data_release.py` to create the PR and dispatch the workflow.
   - GitHub Actions: use repository Secrets for `PYPI_API_TOKEN` (when publishing) and rely on the built-in `GITHUB_TOKEN` for tagging/merging (ensure branch-protection allows Actions to push/merge or grant an explicit PAT in a repo secret if needed).

Required pre-setup
- Ensure the Airflow host/worker that runs the final DAG step can reach the production Postgres database and has `git` installed and configured.
- Add `IS_DEPLOYED=true` to the production `.env` (the DAG will gate the release on this flag).
- Add `AIRFLOW_GITHUB_TOKEN` (or similar) as an Airflow Variable or env var containing a token with `repo` scope so the helper script can create a PR and dispatch the workflow.

If you'd like, I can add a small DAG snippet that runs `scripts/trigger_data_release.py` at the end of your existing `survivor_medallion_dag.py` once you confirm where secrets will be stored.

## Joint code + data release (when schema or code must change)

Sometimes an upstream change requires both a data refresh and code changes (for example: schema changes, new sources, or ETL transforms). The following pattern preserves auditability while keeping the operations reproducible.

High-level flow

1. Develop and test code changes on a feature branch (unit tests + local export smoke tests). Keep data exports separate from code commits while developing.
2. When ready to integrate, open a PR that contains code changes only. CI runs the standard `ci.yml` checks.
3. Once the PR is approved and merged to `main`, run the prod ETL on `main` (Airflow) which will:
    - ingest upstream sources into Postgres, finalize an ingestion run, and record ingestion metadata in `bronze.ingestion_runs`;
    - run `scripts/export_sqlite.py --layer <layer> --package` to build the packaged SQLite snapshot and a machine-readable `gamebot_lite/data/manifest.json` describing the export (ingestion run id, exported tables, sqlite checksum, exporter git sha, timestamp).
4. If the exporter manifest differs from the last release manifest, Airflow will create a release branch (off the same `main` commit used for the prod run) containing the packaged snapshot and manifest, open a PR from that release branch into `main`, and dispatch the `data-release` Actions workflow to validate & merge the PR.
5. The Actions workflow re-runs a fast smoke test, creates the `data-YYYYMMDD` tag and (optionally) publishes the package. The PR + tag provide the audit trail.

Notes and safeguards

- The export manifest (`gamebot_lite/data/manifest.json`) is the canonical contract for releases. It contains ingestion metadata and an exported artifact checksum so both Airflow and CI can make deterministic release decisions.
- The release branch approach preserves your "run on main in prod" policy: Airflow runs ETL on `main` but does not push data commits to `main` — instead a release branch is created and PR'd so CI can validate and merge with proper checks.
- For multi-source setups (some sources not on Git), include per-source identifiers and timestamps in the manifest. This gives a clear release note for consumers even if sources originate outside Git.

Post-release rich diffs and historical comparisons

- We perform an asynchronous "rich diff" job after a release is merged/tagged. It runs on the GitHub release/tag event, compares the newly-merged `gamebot_lite` snapshot and manifest to the previous release, and produces an expanded release note (per-table row-count diffs, schema changes, and a short summary). The job updates the GitHub Release body and writes a markdown copy to `monitoring/release_notes/<tag>.md` for versioned history.
- If you use dbt snapshots, they are a great way to capture row-level history inside the warehouse and can be used as an alternative source for diffs. dbt snapshots record historical state of chosen tables and are independent of the SQLite export; they are useful when you want time-travel-like comparisons without storing entire snapshots in git.
- Note on time travel: some warehouses (e.g., Snowflake) provide native time-travel features; Postgres does not. For Postgres-based workflows we recommend either dbt snapshots (for row-level history) or committing packaged snapshots to the repo and running diffs between consecutive snapshots. For our weekly cadence the latter (SQLite snapshots committed to the repo at release time) is simple and auditable.

dbt snapshots as the primary source for rich diffs (recommended)

- Decision: use dbt snapshots as the authoritative source for post-release rich diffs whenever available. This avoids pushing large SQLite files into git long-term while giving you row-level history and efficient SQL-driven comparisons.
- What you need to enable:
   1. Add dbt snapshot configs for the tables you want to track. See dbt docs for snapshot configuration; typically you'll add `snapshots/` files and run `dbt snapshot` during your CI or on the Airflow host as part of the ETL.
   2. Ensure snapshot tables are built in the warehouse (they live alongside your other dbt models, e.g., `snapshots.my_table_snapshot`).
   3. Provide read-only DB credentials to the GitHub Actions runner that will perform the post-release diffs. Store these as GitHub Secrets (recommended names: `RELEASE_DB_HOST`, `RELEASE_DB_PORT`, `RELEASE_DB_NAME`, `RELEASE_DB_USER`, `RELEASE_DB_PASSWORD`). Use the Actions secrets interface and keep the credentials scoped to least privilege (read-only role restricted to the snapshot/schema).

- How the post-release job will use snapshots:
   - After a release is merged and tagged, the post-release Action will (by default) attempt to query the warehouse snapshot tables for the current release and the previous release window and compute:
      - per-table row-count diffs (fast),
      - schema diffs (columns added/removed/renamed), and
      - a small sample of changed rows (top-k) when feasible.
   - The job renders the extended release note and updates the GitHub Release body and writes `monitoring/release_notes/<tag>.md` in the repo (or stores the note as an artifact if you prefer not to commit it). This job runs asynchronously and does not block the initial manifest-based release.

- Fallback: if dbt snapshots are not available for a particular table, the job will fall back to comparing committed sqlite snapshots (if present) or producing a manifest-only summary.

- Retention / repo hygiene: since you prefer GitHub-only for now, consider a retention strategy for committed SQLite snapshots (for example keep the last N snapshots or archive older ones in a `data-archive/` folder and prune every X releases) to avoid repo bloat over time.

Storage and visibility

- Primary release notes live in the GitHub Release body (visible on the release page). The asynchronous job will patch the release body with the richer notes when they are ready.
- A versioned markdown file is also saved under `monitoring/release_notes/` so historical notes are discoverable by browsing the repo.
- We intentionally keep everything on GitHub (no S3) to make it straightforward for reviewers and hiring audiences to inspect the artifacts and notes.

Where to store secrets and credentials

- Contributor-facing secrets (local development): document in `README.md` what env vars and tokens contributors may need (e.g., GitHub tokens for personal workflows, local PyPI testing tokens). Do NOT store these in the repo.
- Runtime secrets (Airflow / CI): store in the appropriate platform store:
   - Airflow: Airflow Variables or Connections (recommended), or OS environment variables on the worker. Example keys: `AIRFLOW_GITHUB_TOKEN`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
   - GitHub Actions: Repository Secrets (`PYPI_API_TOKEN`, `ACTIONS_MERGE_PAT` if required by branch protection).

Recommendation: separate contributor docs (how to run locally) from runtime ops docs (credentials deployed to Airflow / CI). The `docs/` folder is a good place for the ops documentation and the `README.md` can summarize contributor needs.

Data release notes

Each automated release PR and tag should include a short human-readable release note derived from the manifest (e.g., ingestion run id, top-level changed tables, and a short diff summary if available). This is automatically possible because the manifest includes per-export metadata and the PR body generated by Airflow can include the `monitoring/upstream_report.md` summary when relevant.

A helper script is provided at `scripts/generate_release_notes.py` which reads `gamebot_lite/data/manifest.json` (and optionally `monitoring/upstream_report.md`) and emits a concise, templated release note suitable for PR bodies and release descriptions. Use it to produce the human-friendly summary included in automated release PRs.
