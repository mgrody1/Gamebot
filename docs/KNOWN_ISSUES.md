# Known issues

Last updated 2026-09-17. Items 1-3 and 5-6 are fixed in the models and in the gamebot-lite 0.2.0
snapshot. `tests/test_snapshot_integrity.py` and the dbt `unique` tests guard them. The site demo
needs a fresh `scripts/export_site.py` run to pick up the new snapshot.

| # | Issue | Cause | State |
|---|-------|-------|-------|
| 1 | Challenge wins were never counted, so every gold challenge feature was 0. | `won_flag` matched `'%win%'`; survivoR writes `Won`, `Won (reward only)`, `Won (immunity only)`. | Fixed (`'%won%'`). |
| 2 | `challenge_format` was always `other`, so individual/team splits were empty. | The model read `challenge_type` (Immunity, Reward). The format lives in `challenge_results.outcome_type`. | Fixed. Mixed outcomes such as `Team / Individual` count as individual. |
| 3 | Gold tables repeated keys (4,248 rows for 1,382 castaway-seasons). | `castaway_profile` joined `season_summary` on `season`, which repeats across versions (US01, AU01, ...). Gold joined `castaway_profile` on `castaway_id` alone. | Fixed. Both joins use `version_season`. Gold now has one row per castaway-season. |
| 4 | Column-level descriptions are absent from `dbt/models/*/schema.yml`. | Never written. | Open. |
| 5 | The bronze load failed on current survivoR (2.4.0). | Upstream drift. `challenge_description.turn_based` became `rounds`, and `maze` and `all_names` were added. `castaway_scores` dropped its `n_*` count columns and added `p_score_*`, `r_score_*`, and `threat_*`. `season_summary.viewers_premiere` and `viewers_finale` hold fractional values. | Fixed. The bronze DDL mirrors upstream. |
| 6 | `dbt build` without `--select` failed. | `dbt/models/analysis_notes.sql` held only comments. | Fixed. The notes moved to `docs/feature_ideas.md`. |

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
