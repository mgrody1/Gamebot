## Summary
- Describe the change, motivation, and relevant context.
- Link to any related issues or design docs.

## Testing
- Outline manual or automated tests performed.

## Checklist (before requesting review)
- [ ] `pipenv install` (or `pipenv sync`) completes successfully.
- [ ] `pipenv run pre-commit run --all-files`
- [ ] Local bronze loader run against the dev database (`pipenv run python -m Database.load_survivor_data`)
  - Log location: `docs/run_logs/dev_<branch>_<timestamp>.log`
- [ ] Verify Great Expectations outputs for bronze tables (files under `docs/run_logs/`)
- [ ] `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select silver`
- [ ] `pipenv run dbt build --project-dir dbt --profiles-dir dbt --select gold`
- [ ] Docker loader run on this feature/hotfix branch (`docker compose --profile loader run --rm survivor-loader`)
  - Log location: `docs/run_logs/docker_feature_<timestamp>.log`
- [ ] Documentation/README updated if behaviour or setup changed.

## Post-merge reminders
- [ ] Run the Docker loader on the `development` branch after merge and attach the log path in a comment.
- [ ] Prior to releasing to `main`, run the Docker loader with `SURVIVOR_ENV=prod` and archive the log path.
- [ ] Run `pipenv run dbt build --project-dir dbt --profiles-dir dbt` on `development` and `main` when promoted and store the resulting log/output path.
