## Summary
<!-- Describe the change, motivation, and relevant context. Link to any related issues or design docs. -->

## PR Type
<!-- Check ONE that applies to this PR -->
- [ ] **Feature/Code PR** - New features, bug fixes, refactoring
- [ ] **Data Release PR** - New upstream data ingestion, no code changes
- [ ] **Code Release PR** - Version bump for PyPI/Docker release

---

## Testing
<!-- Outline manual or automated tests performed -->

---

## Checklist

<!-- Complete the checklist for YOUR PR type below. Delete sections that don't apply. -->

### All PRs (Always Complete)
- [ ] Branch name follows convention (`feature/`, `bugfix/`, `data-release/`, `release/`)
- [ ] Rebased on latest `main`: `git fetch origin && git rebase origin/main`
- [ ] Pre-commit checks pass (automated via GitHub Actions CI)
- [ ] Documentation updated if behavior or setup changed

---

### Feature/Code PR Checklist
<!-- Complete this section if this is a feature, bugfix, or refactoring PR -->

#### Pipeline Validation
- [ ] **Run complete pipeline via Airflow DAG**:
  ```bash
  # With SURVIVOR_ENV=dev in .env (for feature/bugfix branches)
  make up  # Start Airflow stack if not running

  # Trigger DAG via Airflow UI:
  # http://localhost:8080 → DAGs → survivor_medallion_pipeline → Trigger

  # OR via CLI:
  cd airflow && docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline

  # Monitor run completion in Airflow UI
  # Validation reports will be generated in run_logs/validation/
  ```

#### Validation Artifacts
- [ ] **Attach validation reports** from `run_logs/validation/` to PR:
  ```bash
  # Use helper script to zip latest validation run
  ./scripts/zip_validation_reports.sh
  # Then attach the generated .zip file to PR comment
  ```
  - OR GitHub Actions will attach as CI artifacts (if configured)
- [ ] Review validation Excel reports for anomalies
- [ ] Verify `gamebot_lite/data/manifest.json` metadata is correct
- [ ] Confirm expected row counts match

#### Documentation Updates (if applicable)
- [ ] Schema changes → Update `docs/gamebot_warehouse_schema_guide.md`
- [ ] New features → Update relevant persona guides
- [ ] Breaking changes → Add migration notes
- [ ] Example notebooks/scripts tested and updated

#### Code Quality
- [ ] Unit tests added/updated (if applicable)
- [ ] Examples run successfully if data schema changed

---

### Data Release PR Checklist
<!-- Complete this section if this is a data-release/* branch -->

#### Data Validation
- [ ] **Verify manifest**: Check `gamebot_lite/data/manifest.json` for:
  - Correct `ingestion_run_id`
  - Expected timestamp
  - Complete table list

- [ ] **Production pipeline run completed**:
  ```bash
  # With SURVIVOR_ENV=prod in .env (on data-release/* branch)
  # Branch protection allows prod on data-release/* branches
  make up
  # Trigger via Airflow UI or CLI
  cd airflow && docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline
  ```

- [ ] **Run smoke test**:
  ```bash
  pipenv run python scripts/smoke_gamebot_lite.py
  # Or from within dev container
  python scripts/smoke_gamebot_lite.py
  ```

- [ ] **Review validation report** from `run_logs/validation/` (attached to PR or in CI artifacts):
  - No unexpected data quality issues
  - Row count changes make sense
  - No schema drift errors

#### Upstream Changes (if applicable)
- [ ] Review `monitoring/upstream_report.md` if included
- [ ] Confirm expected upstream data changes

#### Post-Merge
- [ ] Will create `data-YYYYMMDD` tag after merge (automated via GitHub Actions)

---

### Code Release PR Checklist
<!-- Complete this section if this is a release/* branch -->

#### All Feature/Code PR Items
- [ ] Complete **all items** from "Feature/Code PR Checklist" above

#### Version & Release
- [ ] **Version bumped** in `pyproject.toml` following semantic versioning
- [ ] **Production pipeline run completed**:
  ```bash
  # With SURVIVOR_ENV=prod in .env (on release/* branch)
  # Branch protection allows prod on release/* branches
  make up
  # Trigger via Airflow UI or CLI to generate SQLite with new code
  cd airflow && docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline
  ```
- [ ] **Build test**: `pipenv run python -m build` succeeds
- [ ] **Changelog/Release notes**: Document all changes, breaking changes, migration guidance

#### Post-Merge
- [ ] Will create `code-vX.Y.Z` tag after merge (automated via GitHub Actions)
- [ ] Will create `data-YYYYMMDD` tag after merge (every code release includes data release)

---

## Automated Data-Release PR Exemption
<!-- If this is an automated data-release PR created by tooling, it may bypass manual review if:
  - Created by authorized machine user
  - CI checks pass
  - Merged by GitHub Actions workflow
Otherwise, complete the Data Release PR Checklist above.
-->

---

## Additional Notes
<!-- Any other context, concerns, or follow-up items -->
