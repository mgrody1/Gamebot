## Summary
- Describe the change, motivation, and relevant context.
- Link to any related issues or design docs.

## Testing
- Outline manual or automated tests performed.

## Checklist (before requesting review)

### Always
- [ ] Branch name follows the new convention (`feature/`, `bugfix/`, `data/`) and is rebased on `main`.
- [ ] `pipenv install --dev` (or `pipenv sync`) completes successfully.
- [ ] `pipenv run pre-commit run --all-files`
- [ ] Documentation/README updated if behaviour or setup changed.

### Pipeline-impacting changes
- [ ] Local bronze loader run against the dev database (`pipenv run python -m Database.load_survivor_data`)
  - Log location: `docs/run_logs/dev_<branch>_<timestamp>.log`
- [ ] Verify Great Expectations outputs for bronze tables (files under `docs/run_logs/`)
- [ ] `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver`
- [ ] `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold`
- [ ] Docker loader run on this feature/hotfix branch (`docker compose --profile loader run --rm survivor-loader`)
  - Log location: `docs/run_logs/docker_feature_<timestamp>.log`
- [ ] Attach zipped run logs (local + Docker) to this PR or provide shareable links.

### Packaging / release preparation (run when touching the SQLite snapshot or cutting a release)
- [ ] `pipenv run python scripts/export_sqlite.py --layer silver --package`
- [ ] `python scripts/smoke_gamebot_lite.py` passes for the exported database.
- [ ] `python scripts/check_survivor_updates.py --update` after ingesting new data (committed snapshot refreshed).
- [ ] Version bumps (e.g., `pyproject.toml`, image tags) staged when shipping code changes.

## Post-merge reminders
- [ ] Tag the release (`data-YYYYMMDD` and/or `code-vX.Y.Z`) once the pipeline run or packaging step completes.
