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

### Available table names

#### Bronze tables

| Table | Description |
| --- | --- |
| `advantage_details` | Advantage inventory with type, owner, and metadata. |
| `advantage_movement` | Advantage lifecycle events (found, passed, played). |
| `boot_mapping` | Episode boot mapping to castaways and outcomes. |
| `castaway_details` | Master castaway information (matches survivoR). |
| `castaways` | Castaway-season relationship table. |
| `challenge_description` | Challenge catalog with type, recurring name, and description. |
| `challenge_results` | Raw challenge outcomes (team/individual). |
| `confessionals` | Raw confessional transcripts and metadata. |
| `episodes` | Episode metadata including numbers and air dates. |
| `jury_votes` | Raw jury vote outcomes. |
| `season_summary` | Season-level metadata. |
| `tribe_mapping` | Tribe membership timeline per castaway. |
| `vote_history` | Vote outcomes with round-by-round details. |
| `vote_history_extended` | Extended vote context (revotes, idols, etc.). |
| `gamebot_ingestion_metadata` | Loader run metadata (environment, git details). |

#### Silver tables

| Table | Description |
| --- | --- |
| `dim_advantage` | Advantage dimension with canonical attributes. |
| `dim_castaway` | Dimension table with enriched castaway attributes. |
| `dim_challenge` | Challenge dimension with analytics-friendly columns. |
| `dim_episode` | Episode dimension with air dates and numbering. |
| `dim_season` | Season dimension capturing themes, twists, and geography. |
| `fact_advantage_movement` | Advantage lifecycle events (found, passed, played). |
| `fact_boot_mapping` | Mapping of episode boot events to castaways. |
| `fact_challenge_results` | Curated challenge results with keys to dimensions. |
| `fact_confessionals` | Aggregated confessional counts and airtime. |
| `fact_jury_votes` | Jury vote outcomes tied to castaway dimension keys. |
| `fact_tribe_membership` | Tribe membership timeline per castaway. |
| `fact_vote_history` | Curated vote history with consistent castaway keys. |
| `challenge_skill_bridge` | Bridge table connecting challenges to skill types. |
| `challenge_skill_lookup` | Lookup table of challenge skill taxonomy. |
| `bridge_castaway_season` | Bridge table linking castaways to seasons with roles. |

#### Gold tables

| Table | Description |
| --- | --- |
| `features_castaway_episode` | Episode-level feature JSON for ML experiments. |
| `features_castaway_season` | Season-level feature JSON per castaway. |
| `features_season` | Season-wide feature JSON. |

Planned development: enhanced confessional text tables (ingestion + NLP features) will appear as additional silver models once complete.

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
