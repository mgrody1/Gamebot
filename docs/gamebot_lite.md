# Gamebot Lite

Gamebot Lite is a lightweight snapshot of the Survivor warehouse bundled as a SQLite database. It mirrors the `survivoR` R package experience so analysts can explore the data from a notebook without running the full pipeline.

## Layers

- **Bronze (default)** – Direct exports of the survivoR tables (e.g., `castaway_details`, `season_summary`). This matches the original schema names.
- **Silver** – Curated tables with friendlier names (e.g., `castaway_profile`, `challenge_results_curated`). These are optional and focus on analytics-ready features.
- **Gold** – Machine-learning feature sets (e.g., `features_castaway_episode`). Included when the gold layer is selected.

The packaged SQLite file includes the `gamebot_ingestion_metadata` table with the latest run metadata (environment, git branch/commit, timestamp) so you know how fresh the data is.

## Usage

```python
from gamebot_lite import load_table, duckdb_query

# Pandas example (bronze castaway details)
df_castaways = load_table("castaway_details")
print(df_castaways.head())

# DuckDB SQL example (requires `pip install duckdb`)
sql = """
SELECT original_tribe, COUNT(*) AS castaway_count
FROM castaway_season_profile
GROUP BY original_tribe
ORDER BY castaway_count DESC
"""

df_tribes = duckdb_query(sql)
print(df_tribes)
```

### Available table names (partial)

| Layer | Table | Description |
| --- | --- | --- |
| Bronze | `castaway_details` | Master castaway information (matches survivoR). |
| Bronze | `season_summary` | Season-level metadata. |
| Silver | `castaway_profile` | Dimension table with enriched castaway attributes. |
| Silver | `challenge_results_curated` | Curated challenge results with consistent keys. |
| Silver | `vote_history_curated` | Curated vote history with castaway keys. |
| Gold | `features_castaway_episode` | Episode-level feature JSON for ML experiments. |

See the ERD (`docs/erd/warehouse.png`) for full lineage across layers.

## Data Dictionary

Most bronze columns match the survivoR dataset. For silver/gold tables, refer to:

- `castaway_profile`: demographic fields (`gender`, `race`, `personality_type`), boolean race/identity flags, etc.
- `challenge_catalog`: challenge meta data (type, name, recurring_name).
- `challenge_results_curated`: performance metrics (`result`, `chosen_for_reward`, `sit_out`, `order_of_finish`).
- `vote_history_curated`: vote context (`tribe_status`, `vote`, `vote_event_outcome`, `immunity`).
- `confessional_summary`: aggregated confessional counts and time.
- `features_castaway_episode`: JSON payload keyed by episode with cumulative metrics used for ML.

Refer back to the ERD for relationships and join keys.

## Keeping Data Fresh

1. Run `pipenv run python scripts/export_sqlite.py --layer silver --package --output gamebot_lite/data/gamebot.sqlite`.
2. Bump `pyproject.toml` version, build (`pipenv run python -m build`), and upload via twine.
3. Users can simply run `python -m pip install --upgrade gamebot-lite` to fetch the latest snapshot.
