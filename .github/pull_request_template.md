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
- [ ] Docker loader run on this feature/hotfix branch (`docker compose --profile loader run --rm survivor-loader`)
  - Log location: `docs/run_logs/docker_feature_<timestamp>.log`
- [ ] Gold feature snapshot refreshed (`psql "$DATABASE_URL" -f Database/sql/refresh_gold_features.sql`) or verified via the Docker workflow
- [ ] Documentation/README updated if behaviour or setup changed.

## Post-merge reminders
- [ ] Run the Docker loader on the `development` branch after merge and attach the log path in a comment.
- [ ] Prior to releasing to `main`, run the Docker loader with `SURVIVOR_ENV=prod` and archive the log path.
