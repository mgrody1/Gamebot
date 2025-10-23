# Contributing Guide

Thank you for exploring Gamebot Island! This project pairs Jupyter notebooks with plain Python scripts using [Jupytext](https://github.com/mwouts/jupytext). That workflow keeps notebooks version-controlled and linted without losing the interactive experience.

Below is a quick guide (inspired by [Biel S. Nohr’s tutorial](https://bielsnohr.github.io/2024/03/04/jupyter-notebook-scripts-jupytext-vscode.html)) for keeping notebooks and scripts in sync, plus general tips for code contributions.

## Quick checklist

1. Create a short-lived branch from `main` (`feature/`, `bugfix/`, or `data/` prefix).
2. Open the repository in the Dev Container (recommended) or set up Pipenv locally.
3. Run `pipenv install --dev` to install tooling (`ruff`, `pre-commit`, `jupytext`, etc.).
4. Install pre-commit hooks with `pipenv run pre-commit install`.
5. Pair notebooks with scripts (one-time per notebook) using Jupytext.
6. Use the VS Code “Jupytext sync” task or `pipenv run jupytext --sync` to keep them aligned.
7. Run `pipenv run pre-commit run --all-files` before committing to catch lint/format issues early.

## Git workflow & release cadence

We now follow a lightweight trunk-based workflow:

- `main` is the only long-lived branch. It must stay deployable and is the source for scheduled data runs and releases.
- Feature work happens in short-lived branches named `feature/<summary>`, `bugfix/<summary>`, or `data/<summary>`. Branch from `main`, open a draft PR early, and rebase before requesting review.
- Merge via squash (preferred) or rebase merges so `main` stays linear. Delete feature branches after merge.

### Release types

**Code releases (PyPI, Docker images)**
1. Bump versions (e.g., `pyproject.toml`, image tags).
2. Run the verification commands in the PR checklist, plus `scripts/smoke_gamebot_lite.py` if the SQLite snapshot ships in the release.
3. Merge to `main`, tag the commit `code-vX.Y.Z`, and push the tag (`git push origin code-vX.Y.Z`).

**Data releases (SQLite snapshot refresh)**
1. Export fresh data with `scripts/export_sqlite.py` (typically `--layer silver --package`).
2. Run `python scripts/smoke_gamebot_lite.py` to confirm the packaged database still matches the catalog.
3. Merge the export + documentation changes to `main`, tag the commit `data-YYYYMMDD`, and push the tag.

> We do not yet have a GitHub Action that polls `survivoR` for `.rda` changes. Until that lands, manually monitor the upstream repository (or subscribe to releases) and kick off the bronze loader when new data appears.

Both release types can happen off the same commit when appropriate—run the smoke test, publish the data artefact, then tag twice (`data-…`, `code-…`) if you are also cutting a code release.

## 1. Environment prerequisites

1. Launch the Dev Container (or install Pipenv locally).
2. Run `pipenv install --dev` if it hasn’t been run automatically.
3. Install the pre-commit hooks:

   ```bash
   pipenv run pre-commit install
   ```

The Dev Container’s post-create command already runs these steps, but the commands are handy if you’re working locally.

## 2. Pair a notebook with a script

For each notebook, create a paired `.py` percent-script one time:

```bash
pipenv run jupytext --set-formats ipynb,py:percent notebooks/gamebot_eda.ipynb
```

Alternatively, in VS Code you can open the Command Palette and choose **Jupytext: Pair Notebook with Percent Script**.

This generates `notebooks/gamebot_eda.py` alongside the notebook. Subsequent edits will stay in sync.

## 3. Day-to-day editing

* **VS Code Task:** The repo includes a task (`Jupytext sync`) that runs `jupytext --sync` on the active file. Trigger it via the Command Palette (`Tasks: Run Task`).
* **Pre-commit hook:** On commit, `jupytext --sync` runs automatically for every staged `.ipynb`, ensuring the paired `.py` is updated. Ruff then formats/lints the result via the standard hooks.

## 4. Committing changes

1. Edit the notebook (`.ipynb`) as usual.
2. Run the Jupytext sync task or command if you want to preview the script.
3. Stage **both** the notebook and its paired script (`git add notebooks/gamebot_eda.ipynb notebooks/gamebot_eda.py`).
4. Commit. The pre-commit hook will resync and reformat automatically.

## 5. Useful commands

```bash
# Sync a notebook/script pair manually
pipenv run jupytext --sync notebooks/gamebot_eda.ipynb

# Convert a script back to a notebook (rare, but handy)
pipenv run jupytext --to notebook notebooks/gamebot_eda.py
```

## 6. Need more detail?

Check out the original walkthrough: [“Synchronizing Jupyter Notebooks and Scripts with Jupytext in VS Code”](https://bielsnohr.github.io/2024/03/04/jupyter-notebook-scripts-jupytext-vscode.html).

## 7. Beyond notebooks: general development

- **Ruff lint & format:** `pipenv run ruff check` and `pipenv run ruff format` keep Python code compliant with the project style. Pre-commit executes both automatically.
- **Pre-commit hygiene:** Run `pipenv run pre-commit run --all-files` before pushing to ensure all hooks (Jupytext sync, Ruff, formatting) succeed locally.
- **Environment switching:** Use `pipenv run python scripts/setup_env.py <env>` (`dev` or `prod`) to project the matching profile into `.env` and sync `airflow/.env` before running the stack. See “Environment profiles” in the README for details.
- **Airflow/dbt smoke tests:** After major edits, run `make up` (host) and trigger `survivor_medallion_dag`, or execute `pipenv run dbt build --select silver` / `--select gold` inside the Dev Container to verify transformations.
- **Docs & schema updates:** Add or update entries under `docs/` (e.g., `gamebot_warehouse_schema_guide.md`, `gamebot_warehouse_cheatsheet.md`) when you introduce new tables, joins, or workflows.

## 8. Future development & collaboration ideas

- **Notebook pipeline:** add automated tests for Jupytext pairing (ensure `.ipynb` ⇔ `.py` stays in sync).
- **Test harness:** integrate pytest/dbt unit tests and document how to run them locally and in CI.
- **Continuous Integration:** wire pre-commit + smoke tests into GitHub Actions (lint, dbt build, Airflow DAG check).
- **Data validation:** explore Soda Core (or similar) reintroduction for warehouse-level tests once the legacy blockers are resolved.
- **Documentation polish:** convert README sections into a docs site (MkDocs or similar) and theme it with the Tocantins palette.
- **DBeaver templates:** add sample connection configs/SQL snippets under `docs/` for analysts using external IDEs.
- **gamebot-lite automation:** script the notebook packaging flow (export → version bump → publish) and document it.
- **Additional data sources:** grab text data (like confessional transcripts and/or pre-season interviews) and [edgic](https://insidesurvivor.com/survivor-edgic-an-introduction-3094) data tables
- **Confessional Transcription & Speaker Diarization:** not all confessional data has been kindly curated like it is [here](https://drive.google.com/drive/u/0/folders/0B8Xzl82K1TP8fmItS2RoYWUxeW1YSmZoUXVQSldNMTJnUEVSV1Zvd2xYaFpLYnViOWJ1RXM?resourcekey=0-lnqLgepahAhBF8fOjYeVrw); will also need method to get this data for new episodes ([whisper](https://github.com/openai/whisper) transcription with [pyannote-audio](https://github.com/pyannote/pyannote-audio) for speaker identification is possible approach, may not be able to **fully** automate this way, but could significantly reduce manual effort in speaker tagging)
- **Model development** further curation of the gold layer and deveopment of new machine learning and/or deep learning models off of that layer
- **MLOps** operationalize, productionalize, and track/evaluate models once they are developed
- **API Endpoints** beyond exposing sqlite via Python package, create an API people can hit for data and/or predictions
- **Front-end/UI development** create user-interface for people to see interesting data analyses and or model results (maybe like a plotly dashboard or something involving web development)

Have an idea that isn’t listed? Open an issue or start a discussion—-contributions of all sizes are welcome. Thanks for helping build Gamebot!
