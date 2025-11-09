# Contributing Guide

Thanks for exploring Gamebot Studio! This guide focuses on getting you productive quickly—understanding the environment, the trunk-based git workflow, and the release cadence that keeps bronze, silver, and gold data in sync. Notebook pairing tips still exist, but they show up later so you can dive into the actual development flow first.

## Quick-start checklist

1. Update your local `main` and branch: `git checkout main && git pull && git checkout -b feature/<summary>`.
2. Open the repo in the VS Code Dev Container (recommended) or set up Pipenv locally.
3. Install tooling: `pipenv install --dev` (runs automatically in the container).
4. Install pre-commit hooks: `pipenv run pre-commit install`.
5. Run the smoke test suite: `pipenv run pytest`.
6. Follow the release guidance below (data vs. code) so tags and artefacts stay tidy.
7. Remember: when `SURVIVOR_ENV=prod`, mutating scripts will refuse to run unless you’re on the `main` branch. Keep prod-only work on `main`.

## Trunk-based workflow

- `main` is the single trunk. It must stay deployable and feeds scheduled data refreshes.
- Branch with `feature/`, `bugfix/`, or `data/` prefixes. Keep branches short-lived.
- Rebase on `main` before review, squash merge, then delete the branch.

### Release cadence

**Code releases (PyPI, Docker images)**
1. Bump versions (e.g., `pyproject.toml`, image tags).
2. Run checklist commands, including `python scripts/smoke_gamebot_lite.py` if the SQLite snapshot ships with the release.
3. Merge to `main`, then tag `X.Y.Z`
4. Publish artefacts (PyPI via `pipenv run python -m build` + `twine upload`, Docker images via `docker build` + `docker push`).


## Git Workflow

### Branch Strategy

Gamebot uses **trunk-based development** with short-lived feature branches:

- **`main`**: Production-ready code, always deployable, all releases tagged from here
- **`feature/*`**: Feature development (short-lived, < 1 week ideal)
- **`bugfix/*`**: Bug fixes
- **`hotfix/*`**: Urgent production fixes
- **`data-release/*`**: Data release preparation (ephemeral, deleted after merge)
- **`release/*`**: Code release preparation (ephemeral, deleted after merge)

**Key Principles**:
- Keep `main` stable and deployable
- Merge feature branches quickly (small, focused changes)
- All releases (data and code) happen from `main`
- Delete branches after merging

### Routine git workflow

1. `git checkout main && git pull origin main`
2. `git checkout -b feature/<summary>` (use `bugfix/` or `hotfix/` prefixes when applicable)
3. Make focused commits (`git add <paths>` → `git commit -m "feat: …"`) and run `pipenv run pre-commit run --all-files`
4. Push early (`git push -u origin feature/<summary>`) and open a draft PR for visibility
5. Keep up with `main`: `git fetch origin` + `git rebase origin/main` (resolve conflicts, `git rebase --continue`)
6. Follow the PR checklist so bronze/silver/gold, docs, and packages stay aligned
7. After approval, squash-merge via the PR UI, then clean up locally (`git checkout main && git pull`, `git branch -d feature/<summary>`, `git push origin --delete feature/<summary>`)

**For releases**, see the detailed workflows in `docs/production_contributor_guide.md`.

_Rebase refresher:_ running `git rebase origin/main` replays your commits on top of the newest `main`. It keeps the history tidy for reviewers. If conflicts pop up, fix the files, `git add` them, then `git rebase --continue`. Need to start over? `git rebase --abort` resets to the pre-rebase state.

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

# Stage interactively (review each hunk)
git add -p

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

# Tag releases (helper script)
python scripts/tag_release.py data --date 20250317
python scripts/tag_release.py code --version v1.2.3
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


### Verification commands

- Bronze loader: `pipenv run python -m Database.load_survivor_data`
- dbt silver: `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver`
- dbt gold: `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold`
- Docker loader (parity check): `docker compose --profile loader run --rm survivor-loader`
- Smoke the packaged SQLite snapshot: `python scripts/smoke_gamebot_lite.py`


### Run logs & sharing results

- Capture successful loader runs to `run_logs/<context>_<timestamp>.log` (the folder is ignored by Git so your local logs stay uncluttered).
- Before opening a PR, zip the relevant log files (`zip run_logs/dev_branch_20250317.zip run_logs/dev_branch_20250317.log run_logs/validation/data_quality_*.xlsx`) and either attach the archive directly to the PR comment or upload it to a public share (GitHub Gist, shared drive) and link it in the PR description.
- Repeat the process for Docker-based runs (`run_logs/docker_<branch>_<timestamp>.log`) so reviewers can see parity between local and container executions.
## Notebook workflow (Jupytext)

Use Jupytext to keep notebooks and scripts paired:

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

Find a more in-depth walkthrough in [Biel S. Nohr’s tutorial](https://bielsnohr.github.io/2024/03/04/jupyter-notebook-scripts-jupytext-vscode.html).

## Handy commands

```bash
# Run all pre-commit hooks
pipenv run pre-commit run --all-files

# Trigger the Airflow DAG from the container
cd airflow && docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline

# Export a fresh Gamebot Lite snapshot (silver layer + metadata)
pipenv run python scripts/export_sqlite.py --layer silver --package

# Monitor upstream survivoR commits locally
python scripts/check_survivor_updates.py

# Tag releases (data or code)
python scripts/tag_release.py data --date 20250317
python scripts/tag_release.py code --version v1.2.3

# Review schema drift log (and optional GH issues)
sed -n '1,40p' run_logs/notifications/schema_drift.log

# Run smoke tests
pipenv run pytest
```

## Collaboration and ideas for future additions

Looking for a place to start? Here are ongoing ideas at varying levels of effort—feel free to open an issue or PR if you tackle one (and remove from this list if you complete the task!).

- **Exploratory data analysis:** identify interesting research questions and explore the data in a notebook to try to find insights
- **gamebot-lite automation:** script the notebook packaging flow (export → version bump → publish) and document it.
- **Additional data sources:** grab text data (like confessional transcripts and/or pre-season interviews) and [edgic](https://insidesurvivor.com/survivor-edgic-an-introduction-3094) data tables.
- **Confessional transcription & diarization:** explore tooling like [whisper](https://github.com/openai/whisper) with [pyannote-audio](https://github.com/pyannote/pyannote-audio) to tag speakers and reduce manual effort for new episodes.
>Note: some tagged confesional transcription data can be found [here](https://drive.google.com/drive/u/0/folders/0B8Xzl82K1TP8fmItS2RoYWUxeW1YSmZoUXVQSldNMTJnUEVSV1Zvd2xYaFpLYnViOWJ1RXM?resourcekey=0-lnqLgepahAhBF8fOjYeVrw)

- **Model development:** expand the gold layer and prototype new ML / deep learning models.
- **MLOps:** operationalise, productionise, and evaluate models once they exist.
- **API endpoints:** beyond the SQLite package, expose data or predictions via a small API.
- **Front-end/UI:** build a dashboard (Plotly, web app, etc.) to showcase interesting analyses or model results.
- **Notebook pipeline:** add automated tests for Jupytext pairing (ensure `.ipynb` ⇔ `.py` stays in sync).
- **Test harness:** integrate pytest/dbt unit tests and document how to run them locally and in CI.
- **Continuous Integration:** wire pre-commit + smoke tests into GitHub Actions (lint, dbt build, Airflow DAG check).
- **Data validation:** explore Soda Core (or similar) for warehouse-level tests once the legacy blockers are resolved.
- **Broader survivoR coverage:** ingest additional survivoR tables beyond the 13 currently listed in `Database/db_run_config.json` once schemas are mapped.
- **Documentation polish:** convert README sections into a docs site (MkDocs or similar) and theme it with the Tocantins palette.
- **DBeaver templates:** add sample connection configs/SQL snippets under `docs/` for analysts using external IDEs.
- **Tutorial video** create a tutorial video on how to use Gamebot-Island to partner with the docs

Have an idea? Open an issue or start a discussion—contributions of all sizes are welcome. Thanks for helping build Gamebot!
