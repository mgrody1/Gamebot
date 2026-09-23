# Known issues

Last updated 2026-09-21. Items 1-3 and 5-9 are fixed in the models and in the gamebot-lite snapshot
rebuilt on 2026-09-21 (0.2.2). `tests/test_snapshot_integrity.py` and the dbt `unique` tests guard them,
including two jury tests that compare the snapshot to the ballots in `bronze.jury_votes`.

| # | Issue | Cause | State |
|---|-------|-------|-------|
| 1 | Challenge wins were never counted, so every gold challenge feature was 0. | `won_flag` matched `'%win%'`; survivoR writes `Won`, `Won (reward only)`, `Won (immunity only)`. | Fixed (`'%won%'`). |
| 2 | `challenge_format` was always `other`, so individual/team splits were empty. | The model read `challenge_type` (Immunity, Reward). The format lives in `challenge_results.outcome_type`. | Fixed. Mixed outcomes such as `Team / Individual` count as individual. |
| 3 | Gold tables repeated keys (4,248 rows for 1,382 castaway-seasons). | `castaway_profile` joined `season_summary` on `season`, which repeats across versions (US01, AU01, ...). Gold joined `castaway_profile` on `castaway_id` alone. | Fixed. Both joins use `version_season`. Gold now has one row per castaway-season. |
| 4 | Column-level descriptions were absent from `dbt/models/*/schema.yml`. | Never written. | Fixed 2026-09-20: `scripts/write_column_docs.py` writes 476 one-line descriptions for every silver and gold column, checked against the model SQL (thresholds, categories); rerun it after adding a column, and edit its DOCS dict rather than the yml. Bronze columns are survivoR's and stay documented upstream. |
| 5 | The bronze load failed on current survivoR (2.4.0). | Upstream drift. `challenge_description.turn_based` became `rounds`, and `maze` and `all_names` were added. `castaway_scores` dropped its `n_*` count columns and added `p_score_*`, `r_score_*`, and `threat_*`. `season_summary.viewers_premiere` and `viewers_finale` hold fractional values. | Fixed. The bronze DDL mirrors upstream. |
| 6 | `dbt build` without `--select` failed. | `dbt/models/analysis_notes.sql` held only comments. | Fixed. The notes moved to `docs/feature_ideas.md`. |
| 7 | `silver.castaway_profile.season_name` held the winner's name. | The model aliased `season_summary.full_name` (the winner) as `season_name`. | Fixed in 0.2.1. The model reads `season_summary.season_name`. |
| 8 | `gold.jury_votes_received` counted jurors, not votes (Kim Spradlin 9, true 7; Parvati 17 career, true 8). | The gold CTE counted every `jury_analysis` row (one per juror x finalist). | Fixed (`where voted_for_finalist_name = '1.0'`), rebuilt 2026-09-21: the column sums to the 636 ballots. The site's `silver.jury_tally` browser view counts the same ballots and stays as the simplest source. |
| 9 | `silver.jury_analysis.voted_for_winner` was 1 on every winner row regardless of the vote, and `voted_against_winner` was 1 on every other finalist's row. | Both tested only which finalist the row belonged to. | Fixed (both require `voted_for_finalist_name = '1.0'`), rebuilt 2026-09-21. `voted_for_finalist_name` holds the vote ('1.0'/'0.0') and is misnamed. survivoR records one juror (SA0077, SA05) with two ballots; the test allows it. |
| 10 | Career totals that join `bronze.castaways` split a person who changed name between seasons (Kim Spradlin / Kim Spradlin-Wolfe). | `castaways` stores the per-season name; `castaway_details` holds one canonical name per `castaway_id`. | Not a model bug; the site's `silver.people` view and the model's notes point career aggregates at `castaway_details`. |
| 11 | Four advantage events were `recieved` in `silver.advantage_strategy.event_type` and fell into `event_category = 'other'`, beside 92 spelled `received`. | survivoR's `advantage_movement.event` spells them `Recieved`. Bronze keeps the upstream value. | Fixed in silver 2026-09-22 (`replace(lower(event), 'recieved', 'received')`), guarded by `dbt/tests/advantage_event_spelling.sql`. Takes effect at the next rebuild. |
| 12 | `bronze.dataset_versions` was empty in the 0.2.x snapshots. | Only the Airflow DAG's `persist_dataset_metadata` task wrote it, and `scripts/run_lite.py` recreates bronze without that task. | Fixed 2026-09-22: `run_lite.py` runs `scripts/record_dataset_versions.py` after the bronze load. Set `GITHUB_TOKEN` to record upstream commits too. Takes effect at the next rebuild. |

## Breaking changes in gamebot-lite 0.2.0

- `challenge_description.turn_based` is now `rounds`.
- `castaway_scores` follows the upstream 2.3.12 layout.
- `castaway_profile` gains `version_season` and `version`, with one row per castaway-season.
- `challenge_performance` gains `outcome_type`.
- Gold tables drop from 4,248 duplicated rows to 1,441 unique rows.

## Upstream columns the bronze layer still drops

The loader logs these and ignores them: `won*` flags on `challenge_results`, `index_count` and
`index_time` on `confessionals`, `description` on `season_summary`, and `sog_id` on `challenge_summary`. Add them to
`Database/create_tables.sql` to keep them.
