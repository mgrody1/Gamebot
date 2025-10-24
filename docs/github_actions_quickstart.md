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
