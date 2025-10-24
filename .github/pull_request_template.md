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

### Pipeline-impacting changes (attach zipped logs for each run)
- [ ] Docker bronze loader run (`docker compose --profile loader run --rm survivor-loader`)
  - Archive attached (e.g., `docs/run_logs/docker_loader_<branch>_<timestamp>.log.zip`)
- [ ] Airflow DAG run (`docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline`)
  - Archive attached (Airflow log directory zipped with timestamp)
- [ ] Runner scripts touched (list modules/commands below) have dedicated docker-based logs attached:
  - Modules/scripts: _________________________
- [ ] `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver`
- [ ] `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold`
- [ ] Review `docs/run_logs/schema_drift.log` for new columns/tables and capture any follow-up issues

### Packaging / release preparation (run when touching the SQLite snapshot or cutting a release)
- [ ] `pipenv run python scripts/export_sqlite.py --layer silver --package`
- [ ] `python scripts/smoke_gamebot_lite.py` passes for the exported database.
- [ ] `python scripts/check_survivor_updates.py --update` after ingesting new data (committed snapshot refreshed).
- [ ] Version bumps (e.g., `pyproject.toml`, image tags) staged when shipping code changes.
- [ ] Git tags created via `python scripts/tag_release.py ...` (note command in comments if applicable).

## Post-merge reminders
- [ ] Tag the release (`data-YYYYMMDD` and/or `code-vX.Y.Z`) once the pipeline run or packaging step completes.
