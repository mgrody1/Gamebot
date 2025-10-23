# Contributing Guide

Thanks for exploring Gamebot Island! This guide focuses on getting you productive quickly—understanding the environment, the trunk-based git workflow, and the release cadence that keeps bronze, silver, and gold data in sync. Notebook pairing tips still exist, but they show up later so you can dive into the actual development flow first.

## Quick-start checklist

1. Update your local `main` and branch: `git checkout main && git pull && git checkout -b feature/<summary>`.
2. Open the repo in the VS Code Dev Container (recommended) or set up Pipenv locally.
3. Install tooling: `pipenv install --dev` (runs automatically in the container).
4. Install pre-commit hooks: `pipenv run pre-commit install`.
5. Run `pipenv run pre-commit run --all-files` before each push.
6. Follow the release guidance below (data vs. code) so tags and artefacts stay tidy.

## Trunk-based workflow

- `main` is the single trunk. It must stay deployable and feeds scheduled data refreshes.
- Branch with `feature/`, `bugfix/`, or `data/` prefixes. Keep branches short-lived.
- Rebase on `main` before review, squash merge, then delete the branch.

### Release cadence

**Code releases (PyPI, Docker images)**
1. Bump versions (e.g., `pyproject.toml`, image tags).
2. Run checklist commands, including `python scripts/smoke_gamebot_lite.py` if the SQLite snapshot ships with the release.
3. Merge to `main`, tag the commit `code-vX.Y.Z`, and push the tag.

**Data releases (warehouse refresh + Gamebot Lite snapshot)**
1. Check for upstream changes: `python scripts/check_survivor_updates.py` (mirrors the daily GitHub Action watching `.rda` and JSON exports).
2. Run the bronze loader and dbt models (see README §8.2 for commands).
3. Export the SQLite snapshot (`pipenv run python scripts/export_sqlite.py --layer silver --package`) and run the smoke test.
4. Merge to `main`, tag the commit `data-YYYYMMDD`, and push the tag.
5. Refresh the upstream baseline: `python scripts/check_survivor_updates.py --update` (commit the updated `monitoring/survivor_upstream_snapshot.json`).

Both release types can happen off the same commit—run the smoke test, publish the artefact, then tag twice if you’re shipping data and code together.

### Day-to-day git flow

1. `git checkout main && git pull`
2. `git checkout -b feature/<summary>`
3. Make focused commits; run `pipenv run pre-commit run --all-files`
4. Open a draft PR early; rebase before requesting review
5. Follow the PR checklist so bronze/silver/gold, docs, and packages stay aligned
6. Squash merge, delete the branch, and tag if it’s a release

### Git command reference

```bash
# Keep your local main updated
git checkout main
git pull origin main

# Start a feature branch
git checkout -b feature/<summary>

# Stage & commit changes incrementally
git status
git add <paths>
git commit -m "feat: describe the change"

# Rebase on main before requesting review
git fetch origin
git rebase origin/main

# Resolve conflicts, then continue
git status
git add <resolved files>
git rebase --continue

# Push the branch (creates remote tracking)
git push -u origin feature/<summary>

# After merge, clean up the local branch
git checkout main
git pull origin main
git branch -d feature/<summary>
git push origin --delete feature/<summary>  # optional but recommended
```

## Working in the environment

### Dev Container (recommended)

- Launch the VS Code Dev Container. It ships with Python 3.11, dbt, Airflow CLI tools, and Pipenv preinstalled.
- `pipenv install --dev` and the pre-commit setup run automatically. If you’re on bare metal, run them manually.
- Use the container terminal for Python/dbt commands and the host terminal for Docker/Make invocations.

### Pipenv on the host

1. Install Python 3.11 and Pipenv.
2. Run `pipenv install --dev`.
3. Install pre-commit hooks: `pipenv run pre-commit install`.
4. Use `pipenv run <command>` for dbt, loader scripts, etc.

### Environment management

- Switch between dev/prod configs with `pipenv run python scripts/setup_env.py <env> [--from-template]`. That syncs `.env` and `airflow/.env`, updates connections, and preserves secrets.
- After switching environments, restart Docker services (`make down && make up`) so containers pick up new settings.
- Airflow runs from the `airflow/` directory (`docker compose up -d`). The DAG `survivor_medallion_pipeline` orchestrates bronze → silver → gold.

### Verification commands

- Bronze loader: `pipenv run python -m Database.load_survivor_data`
- dbt silver: `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver`
- dbt gold: `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold`
- Docker loader (parity check): `docker compose --profile loader run --rm survivor-loader`
- Smoke the packaged SQLite snapshot: `python scripts/smoke_gamebot_lite.py`

These match the PR checklist; run them locally before promoting changes.

### Run logs & sharing results

- Capture successful loader/dbt runs to `docs/run_logs/<context>_<timestamp>.log` (the folder is ignored by Git so your local logs stay uncluttered).
- Before opening a PR, zip the relevant log files (`zip docs/run_logs/dev_branch_20250317.zip docs/run_logs/dev_branch_20250317.log`) and either attach the archive directly to the PR comment or upload it to a public share (GitHub Gist, shared drive) and link it in the PR description.
- Repeat the process for Docker-based runs (`docs/run_logs/docker_<branch>_<timestamp>.log`) so reviewers can see parity between local and container executions.
## Notebook workflow (Jupytext)

Once you’re comfortable with the git and environment flow, use Jupytext to keep notebooks and scripts paired:

1. Pair a notebook one time:
   ```bash
   pipenv run jupytext --set-formats ipynb,py:percent notebooks/gamebot_eda.ipynb
   ```
2. Sync edits:
   ```bash
   pipenv run jupytext --sync notebooks/gamebot_eda.ipynb
   ```
   or use the VS Code “Jupytext sync” task.
3. Stage both files (`.ipynb` and `.py`) before committing—the pre-commit hook syncs and formats them automatically.

A deeper walkthrough lives in [Biel S. Nohr’s tutorial](https://bielsnohr.github.io/2024/03/04/jupyter-notebook-scripts-jupytext-vscode.html).

## Handy commands

```bash
# Run all pre-commit hooks
pipenv run pre-commit run --all-files

# Trigger the Airflow DAG from the container
cd airflow && docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline

# Export a fresh Gamebot Lite snapshot (silver layer + metadata)
pipenv run python scripts/export_sqlite.py --layer silver --package

# Monitor upstream survivoR commits locally (matches nightly GitHub Action)
python scripts/check_survivor_updates.py
```

## Collaboration ideas

Looking for a place to start? Here are ongoing ideas at varying levels of effort—feel free to open an issue or PR if you tackle one.

- **gamebot-lite automation:** script the notebook packaging flow (export → version bump → publish) and document it.
- **Additional data sources:** grab text data (like confessional transcripts and/or pre-season interviews) and [edgic](https://insidesurvivor.com/survivor-edgic-an-introduction-3094) data tables.
- **Confessional transcription & diarization:** explore tooling like [whisper](https://github.com/openai/whisper) with [pyannote-audio](https://github.com/pyannote/pyannote-audio) to tag speakers and reduce manual effort for new episodes.
- **Model development:** expand the gold layer and prototype new ML / deep learning models.
- **MLOps:** operationalise, productionise, and evaluate models once they exist.
- **API endpoints:** beyond the SQLite package, expose data or predictions via a small API.
- **Front-end/UI:** build a dashboard (Plotly, web app, etc.) to showcase interesting analyses or model results.
- **Notebook pipeline:** add automated tests for Jupytext pairing (ensure `.ipynb` ⇔ `.py` stays in sync).
- **Test harness:** integrate pytest/dbt unit tests and document how to run them locally and in CI.
- **Continuous Integration:** wire pre-commit + smoke tests into GitHub Actions (lint, dbt build, Airflow DAG check).
- **Data validation:** explore Soda Core (or similar) reintroduction for warehouse-level tests once the legacy blockers are resolved.
- **Documentation polish:** convert README sections into a docs site (MkDocs or similar) and theme it with the Tocantins palette.
- **DBeaver templates:** add sample connection configs/SQL snippets under `docs/` for analysts using external IDEs.

Have an idea? Open an issue or start a discussion—contributions of all sizes are welcome. Thanks for helping build Gamebot!
